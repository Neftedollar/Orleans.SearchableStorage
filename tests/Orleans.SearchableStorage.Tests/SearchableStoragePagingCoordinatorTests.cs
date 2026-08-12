using System.Collections.Concurrent;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStoragePagingCoordinatorTests
{
    private const string ProviderName = "paging-coordinator";

    [Fact]
    public async Task GlobalMinimumFrontierProducesAShortPageAndRescansDiscardedItems()
    {
        var first = CreateId("a");
        var second = CreateId("b");
        var discardedThenRescanned = CreateId("d");
        var callOrder = new ConcurrentQueue<int>();
        var owner0 = new PagePartition(request =>
        {
            callOrder.Enqueue(0);
            return Task.FromResult(request.HasAfter
                ? Result(request, [discardedThenRescanned], exhausted: true)
                : Result(request, [first, discardedThenRescanned], frontier: discardedThenRescanned));
        });
        var owner1 = new PagePartition(request =>
        {
            callOrder.Enqueue(1);
            return Task.FromResult(request.HasAfter
                ? Result(request, [], exhausted: true)
                : Result(request, [second], frontier: second));
        });
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 0, 1, 1),
            new Dictionary<int, PagePartition>
            {
                [0] = owner0,
                [1] = owner1,
            });
        var query = CreateQuery(client);

        var page1 = await query.ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));
        var page2 = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10, page1.ContinuationToken));

        page1.Items.Should().Equal(first, second);
        page1.Items.Should().NotContain(discardedThenRescanned);
        page1.ContinuationToken.Should().NotBeNull();
        page2.Items.Should().ContainSingle().Which.Should().Be(discardedThenRescanned);
        page2.ContinuationToken.Should().BeNull();
        owner0.Requests[1].After.Should().Be(second);
        owner1.Requests[1].After.Should().Be(second);
        callOrder.Should().Equal(0, 1, 0, 1);
    }

    [Fact]
    public async Task EmptyNonTerminalPageAdvancesTheGlobalFrontier()
    {
        var firstFrontier = CreateId("b");
        var secondFrontier = CreateId("c");
        var options = CreateOptions();
        options.PartitionWorkBudget = 5;
        var owner0 = new PagePartition(request => Task.FromResult(request.HasAfter
            ? Result(request, [], exhausted: true)
            : Result(request, [], frontier: firstFrontier)));
        var owner1 = new PagePartition(request => Task.FromResult(request.HasAfter
            ? Result(request, [], exhausted: true)
            : Result(request, [], frontier: secondFrontier)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 },
            options);
        var query = CreateQuery(client);

        var page1 = await query.ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));
        var page2 = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10, page1.ContinuationToken));

        page1.Items.Should().BeEmpty();
        page1.ContinuationToken.Should().NotBeNull();
        owner0.Requests[1].After.Should().Be(firstFrontier);
        owner1.Requests[1].After.Should().Be(firstFrontier);
        page2.Items.Should().BeEmpty();
        page2.ContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task PublicPageByteLimitTruncatesAtTheLastCompleteItemAndResumes()
    {
        var first = CreateId("byte-a");
        var second = CreateId("byte-b");
        var options = CreateOptions();
        options.PageByteLimit = GrainIdCanonicalOrder.GetEncodedLength(first);
        var partition = new PagePartition(request => Task.FromResult(
            request.HasAfter
                ? Result(request, [second], exhausted: true)
                : Result(request, [first, second], exhausted: true)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);
        var query = CreateQuery(client);

        var page1 = await query.ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(2));
        var page2 = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(2, page1.ContinuationToken));

        page1.Items.Should().ContainSingle().Which.Should().Be(first);
        page1.ContinuationToken.Should().NotBeNull();
        page2.Items.Should().ContainSingle().Which.Should().Be(second);
        page2.ContinuationToken.Should().BeNull();
        partition.Requests[1].After.Should().Be(first);
    }

    [Fact]
    public async Task PublicPageByteLimitRejectsAnItemWhichCannotFit()
    {
        var item = CreateId("oversized-for-page");
        var options = CreateOptions();
        options.PageByteLimit = GrainIdCanonicalOrder.GetEncodedLength(item) - 1;
        var partition = new PagePartition(request => Task.FromResult(
            Result(request, [item], exhausted: true)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(1));

        await execute.Should().ThrowAsync<SearchableStorageQueryLimitExceededException>()
            .WithMessage("*cannot fit*page-byte limit*");
        partition.Requests.Should().ContainSingle();
    }

    [Fact]
    public async Task ChangesAtOrBeforeTheFrontierMayBeMissedWhileEarlierItemsRemainReturned()
    {
        var insertedBeforeFrontier = CreateId("a");
        var deletedAfterReturn = CreateId("b");
        var changedToMatchBeforeFrontier = CreateId("c");
        var changedAwayAfterReturn = CreateId("d");
        var laterMatch = CreateId("f");
        var matches = new MutableMatchSet(deletedAfterReturn, changedAwayAfterReturn, laterMatch);
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = matches.Partition });
        var query = CreateQuery(client);

        var first = await query.ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(2));
        first.Items.Should().Equal(deletedAfterReturn, changedAwayAfterReturn);
        first.ContinuationToken.Should().NotBeNull();

        // These changes happen after the first turn. New matches at or before its exclusive
        // continuation boundary are allowed to be missed, while already-returned values remain in
        // the immutable first page even after deletion or mutation away from the predicate.
        matches.Add(insertedBeforeFrontier);
        matches.Add(changedToMatchBeforeFrontier);
        matches.Remove(deletedAfterReturn);
        matches.Remove(changedAwayAfterReturn);

        var second = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(2, first.ContinuationToken));
        var concatenated = first.Items.Concat(second.Items).ToArray();

        matches.Partition.Requests[1].After.Should().Be(changedAwayAfterReturn);
        second.Items.Should().ContainSingle().Which.Should().Be(laterMatch);
        second.ContinuationToken.Should().BeNull();
        concatenated.Should().Equal(deletedAfterReturn, changedAwayAfterReturn, laterMatch);
        concatenated.Should().NotContain(insertedBeforeFrontier);
        concatenated.Should().NotContain(changedToMatchBeforeFrontier);
    }

    [Fact]
    public async Task InsertAndMutationToMatchAfterTheFrontierCanAppearOnLaterPages()
    {
        var firstMatch = CreateId("b");
        var frontier = CreateId("d");
        var insertedAfterFrontier = CreateId("e");
        var existingLaterMatch = CreateId("f");
        var changedToMatchAfterFrontier = CreateId("g");
        var matches = new MutableMatchSet(firstMatch, frontier, existingLaterMatch);
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = matches.Partition });
        var query = CreateQuery(client);

        var first = await query.ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(2));
        first.Items.Should().Equal(firstMatch, frontier);

        matches.Add(insertedAfterFrontier);
        matches.Add(changedToMatchAfterFrontier);

        var second = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(2, first.ContinuationToken));
        var third = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(2, second.ContinuationToken));

        second.Items.Should().Equal(insertedAfterFrontier, existingLaterMatch);
        second.ContinuationToken.Should().NotBeNull();
        third.Items.Should().ContainSingle().Which.Should().Be(changedToMatchAfterFrontier);
        third.ContinuationToken.Should().BeNull();
        first.Items.Concat(second.Items).Concat(third.Items)
            .Should().Equal(
                firstMatch,
                frontier,
                insertedAfterFrontier,
                existingLaterMatch,
                changedToMatchAfterFrontier);
    }

    [Fact]
    public async Task ReplayingTheSameInputTokenIsAllowedAndCanObserveDifferentLaterState()
    {
        var firstMatch = CreateId("b");
        var frontier = CreateId("d");
        var firstReplayOnly = CreateId("e");
        var stableLaterMatch = CreateId("f");
        var secondReplayOnly = CreateId("g");
        var matches = new MutableMatchSet(firstMatch, frontier, stableLaterMatch);
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = matches.Partition });
        var query = CreateQuery(client);

        var first = await query.ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(2));
        var replayToken = first.ContinuationToken;
        replayToken.Should().NotBeNull();

        matches.Add(firstReplayOnly);
        var firstReplay = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(2, replayToken));

        matches.Remove(firstReplayOnly);
        matches.Add(secondReplayOnly);
        var secondReplay = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(2, replayToken));

        firstReplay.Items.Should().Equal(firstReplayOnly, stableLaterMatch);
        secondReplay.Items.Should().Equal(stableLaterMatch, secondReplayOnly);
        firstReplay.Items.Should().NotEqual(secondReplay.Items);
        firstReplay.ContinuationToken.Should().BeNull();
        secondReplay.ContinuationToken.Should().BeNull();
        matches.Partition.Requests[1].After.Should().Be(frontier);
        matches.Partition.Requests[2].After.Should().Be(frontier);
    }

    [Fact]
    public async Task NoWriteConcatenationEqualsTheFullSortedDistinctCoordinatorResult()
    {
        var a = CreateId("a");
        var b = CreateId("b");
        var c = CreateId("c");
        var d = CreateId("d");
        var e = CreateId("e");
        var f = CreateId("f");
        var g = CreateId("g");
        var owner0 = new MutableMatchSet(a, c, e, g);
        var owner1 = new MutableMatchSet(b, c, d, f);
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition>
            {
                [0] = owner0.Partition,
                [1] = owner1.Partition,
            });
        var query = CreateQuery(client);
        var concatenated = new List<GrainId>();
        string? continuation = null;

        do
        {
            var page = await query.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(2, continuation));
            concatenated.AddRange(page.Items);
            continuation = page.ContinuationToken;
        }
        while (continuation is not null);

        concatenated.Should().Equal(a, b, c, d, e, f, g);
        var independentlySortedDistinct = concatenated
            .Distinct(GrainIdCanonicalOrder.EqualityComparer)
            .Order(GrainIdCanonicalOrder.Comparer)
            .ToArray();
        concatenated.Should().Equal(independentlySortedDistinct);
        owner0.Partition.Requests.Should().HaveCount(4);
        owner1.Partition.Requests.Should().HaveCount(4);
    }

    [Fact]
    public async Task FirstPageRouteMismatchRefreshesTheWholeAttemptOnce()
    {
        var firstLayout = CreateLayout(epoch: 1, 0);
        var refreshedLayout = CreateLayout(epoch: 2, 1);
        var loadCount = 0;
        var owner0 = new PagePartition(request => Task.FromException<PartitionQueryPageResult>(
            CreateMismatch(request.Epoch, currentEpoch: 2, requestedOwner: 0)));
        var expected = CreateId("after-refresh");
        var owner1 = new PagePartition(request => Task.FromResult(
            Result(request, [expected], exhausted: true)));
        var client = CreateClient(
            new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(
                Interlocked.Increment(ref loadCount) == 1 ? firstLayout : refreshedLayout)),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 });

        var page = await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        page.Items.Should().ContainSingle().Which.Should().Be(expected);
        page.ContinuationToken.Should().BeNull();
        loadCount.Should().Be(2);
        owner0.Requests.Should().ContainSingle().Which.Epoch.Should().Be(1);
        owner1.Requests.Should().ContainSingle().Which.Epoch.Should().Be(2);
    }

    [Fact]
    public async Task ResumedRouteMismatchIsStaleAndDoesNotRefresh()
    {
        var layout = CreateLayout(epoch: 1, 0);
        var loadCount = 0;
        var frontier = CreateId("frontier");
        var options = CreateOptions();
        options.PartitionWorkBudget = 5;
        var partition = new PagePartition(request => request.HasAfter
            ? Task.FromException<PartitionQueryPageResult>(
                CreateMismatch(request.Epoch, currentEpoch: 2, requestedOwner: 0))
            : Task.FromResult(Result(request, [], frontier: frontier)));
        var client = CreateClient(
            new StorageLayoutCache(() =>
            {
                Interlocked.Increment(ref loadCount);
                return Task.FromResult<StorageLayoutSnapshot?>(layout);
            }),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);
        var query = CreateQuery(client);
        var first = await query.ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        Func<Task> resume = async () => await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10, first.ContinuationToken));

        await resume.Should().ThrowAsync<SearchableStorageStaleContinuationTokenException>();
        loadCount.Should().Be(1);
        partition.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task FanoutWaitsForEveryOwnerAndSelectsFailureBySortedOwner()
    {
        var owner0Completion = new TaskCompletionSource<PartitionQueryPageResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var owner0Failure = new InvalidOperationException("owner zero");
        var owner1Failure = new InvalidOperationException("owner one");
        var owner0 = new PagePartition(_ => owner0Completion.Task);
        var owner1 = new PagePartition(_ => Task.FromException<PartitionQueryPageResult>(owner1Failure));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 });

        var execution = CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));
        await owner1.Started.WaitAsync(TimeSpan.FromSeconds(5));

        execution.IsCompleted.Should().BeFalse();
        owner0Completion.SetException(owner0Failure);
        Func<Task> wait = async () => await execution;
        (await wait.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(owner0Failure);
    }

    [Fact]
    public async Task CancellationDelegatesTheWholeFanoutForLateObservation()
    {
        var owner0Completion = new TaskCompletionSource<PartitionQueryPageResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var owner1Completion = new TaskCompletionSource<PartitionQueryPageResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var owner0Failure = new InvalidOperationException("late owner zero");
        var owner1Failure = new InvalidOperationException("late owner one");
        var owner0 = new PagePartition(_ => owner0Completion.Task);
        var owner1 = new PagePartition(_ => owner1Completion.Task);
        Task? observedFanout = null;
        var observationCompleted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Action<Task> observer = task =>
        {
            observedFanout = task;
            _ = ObserveForTestAsync(task, observationCompleted);
        };
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 },
            detachedFanoutObserver: observer);
        using var cancellation = new CancellationTokenSource();

        var execution = CreateQuery(client).ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10),
            cancellation.Token);
        await Task.WhenAll(owner0.Started, owner1.Started).WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();

        Func<Task> wait = async () => await execution;
        await wait.Should().ThrowAsync<OperationCanceledException>();
        observedFanout.Should().NotBeNull();
        observedFanout!.IsCompleted.Should().BeFalse();

        owner0Completion.SetException(owner0Failure);
        owner1Completion.SetException(owner1Failure);
        await observationCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        observedFanout.IsFaulted.Should().BeTrue();
        observedFanout.Exception!.Flatten().InnerExceptions
            .Should().Contain(owner0Failure).And.Contain(owner1Failure);
    }

    [Fact]
    public async Task RequestFingerprintsAreIsolatedFromInProcessPartitionMutation()
    {
        byte originalFirstByte = 0;
        byte secondOwnerFirstByte = 0;
        var owner0 = new PagePartition(request =>
        {
            originalFirstByte = request.QueryFingerprint[0];
            request.QueryFingerprint[0] ^= 0xff;
            return Task.FromResult(Result(request, [], exhausted: true));
        });
        var owner1 = new PagePartition(request =>
        {
            secondOwnerFirstByte = request.QueryFingerprint[0];
            return Task.FromResult(Result(request, [], exhausted: true));
        });
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*mismatched query fingerprint*");
        secondOwnerFirstByte.Should().Be(originalFirstByte);
    }

    [Theory]
    [InlineData("candidate")]
    [InlineData("record")]
    [InlineData("predicate")]
    [InlineData("entry")]
    [InlineData("ownership")]
    [InlineData("seek")]
    [InlineData("range-bucket")]
    [InlineData("materialization")]
    [InlineData("range-merge")]
    [InlineData("planner-node")]
    [InlineData("planner-metadata")]
    [InlineData("posting-candidate")]
    [InlineData("catalog-candidate")]
    [InlineData("heap")]
    [InlineData("union")]
    public async Task NegativeWorkComponentCannotBeHiddenByAPositiveComponent(string component)
    {
        var work = component switch
        {
            "candidate" => WorkWith(orderedCandidate: -1),
            "record" => WorkWith(record: -1),
            "predicate" => WorkWith(predicate: -1),
            "entry" => WorkWith(entry: -1),
            "ownership" => WorkWith(ownership: -1),
            "seek" => WorkWith(seek: -1),
            "range-bucket" => WorkWith(rangeBucket: -1),
            "materialization" => WorkWith(materialization: -1),
            "range-merge" => WorkWith(rangeMerge: -1),
            "planner-node" => WorkWith(plannerNode: -1),
            "planner-metadata" => WorkWith(plannerMetadata: -1),
            "posting-candidate" => WorkWith(postingCandidate: -1),
            "catalog-candidate" => WorkWith(catalogCandidate: -1),
            "heap" => WorkWith(heap: -1),
            "union" => WorkWith(union: -1),
            _ => throw new ArgumentOutOfRangeException(nameof(component)),
        };
        var partition = new PagePartition(request => Task.FromResult(
            Result(request, [], exhausted: true, work: work)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*effective response policy*");
    }

    [Theory]
    [InlineData((int)PartitionQueryAccessPath.None)]
    [InlineData(int.MaxValue)]
    public async Task InvalidScalarAccessPathFailsClosed(int rawAccessPath)
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: new PartitionQueryPageWork
            {
                AccessPath = (PartitionQueryAccessPath)rawAccessPath,
            })));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*invalid scalar access path*");
    }

    [Fact]
    public async Task ScalarAccessPathWhichContradictsItsWorkEvidenceFailsClosed()
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: new PartitionQueryPageWork
            {
                AccessPath = PartitionQueryAccessPath.ExactPosting,
                CatalogCandidateVisitCount = 1,
                PostingSeekCount = 2,
                PlannerNodeVisitCount = 1,
                PlannerMetadataReadCount = 1,
            })));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData("exact-source")]
    [InlineData("range-source")]
    [InlineData("range-heap")]
    [InlineData("range-merge")]
    [InlineData("union-source")]
    [InlineData("union-operation")]
    [InlineData("catalog-source")]
    public async Task ScalarAccessPathRejectsUnderreportedSourceWork(string evidence)
    {
        var work = evidence switch
        {
            "exact-source" => WorkWith(
                orderedCandidate: 1,
                ownership: 1,
                seek: 2,
                plannerNode: 3,
                plannerMetadata: 1,
                accessPath: PartitionQueryAccessPath.ExactPosting),
            "range-source" => WorkWith(
                orderedCandidate: 1,
                ownership: 1,
                seek: 2,
                rangeBucket: 1,
                rangeMerge: 1,
                plannerNode: 3,
                plannerMetadata: 1,
                heap: 1,
                accessPath: PartitionQueryAccessPath.RangeMerge),
            "range-heap" => WorkWith(
                orderedCandidate: 1,
                ownership: 1,
                seek: 2,
                rangeBucket: 1,
                rangeMerge: 1,
                plannerNode: 3,
                plannerMetadata: 1,
                postingCandidate: 1,
                accessPath: PartitionQueryAccessPath.RangeMerge),
            "range-merge" => WorkWith(
                orderedCandidate: 1,
                ownership: 1,
                seek: 2,
                rangeBucket: 1,
                plannerNode: 3,
                plannerMetadata: 1,
                postingCandidate: 1,
                heap: 1,
                accessPath: PartitionQueryAccessPath.RangeMerge),
            "union-source" => WorkWith(
                orderedCandidate: 1,
                ownership: 1,
                seek: 4,
                plannerNode: 3,
                plannerMetadata: 2,
                union: 1,
                accessPath: PartitionQueryAccessPath.Union),
            "union-operation" => WorkWith(
                orderedCandidate: 1,
                ownership: 1,
                seek: 4,
                plannerNode: 3,
                plannerMetadata: 2,
                postingCandidate: 1,
                accessPath: PartitionQueryAccessPath.Union),
            "catalog-source" => WorkWith(
                orderedCandidate: 1,
                ownership: 1,
                plannerNode: 3),
            _ => throw new ArgumentOutOfRangeException(nameof(evidence)),
        };
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: work)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateBooleanQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    [InlineData(true, 4)]
    public async Task PlannerNodeVisitsMustEqualTheWirePlanNodeCount(
        bool useBooleanQuery,
        long reportedNodeCount)
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: CreateZeroCandidateWork(
                PartitionQueryAccessPath.Catalog,
                reportedNodeCount))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });
        var query = useBooleanQuery ? CreateBooleanQuery(client) : CreateQuery(client);

        Func<Task> execute = async () => await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData("catalog-seek")]
    [InlineData("exact-seek")]
    [InlineData("exact-metadata")]
    [InlineData("range-seek")]
    [InlineData("range-metadata")]
    [InlineData("range-bucket")]
    [InlineData("union-seek")]
    [InlineData("union-metadata")]
    public async Task ScalarAccessPathRejectsUnderreportedVersionTwoMinimums(
        string evidence)
    {
        var work = evidence switch
        {
            "catalog-seek" => WorkWith(seek: 0, plannerNode: 3),
            "exact-seek" => WorkWith(
                seek: 1,
                plannerNode: 3,
                plannerMetadata: 1,
                accessPath: PartitionQueryAccessPath.ExactPosting),
            "exact-metadata" => WorkWith(
                seek: 2,
                plannerNode: 3,
                plannerMetadata: 0,
                accessPath: PartitionQueryAccessPath.ExactPosting),
            "range-seek" => WorkWith(
                seek: 1,
                rangeBucket: 1,
                plannerNode: 3,
                plannerMetadata: 1,
                accessPath: PartitionQueryAccessPath.RangeMerge),
            "range-metadata" => WorkWith(
                seek: 2,
                rangeBucket: 1,
                plannerNode: 3,
                plannerMetadata: 0,
                accessPath: PartitionQueryAccessPath.RangeMerge),
            "range-bucket" => WorkWith(
                seek: 2,
                rangeBucket: 0,
                plannerNode: 3,
                plannerMetadata: 1,
                accessPath: PartitionQueryAccessPath.RangeMerge),
            "union-seek" => WorkWith(
                seek: 3,
                plannerNode: 3,
                plannerMetadata: 2,
                accessPath: PartitionQueryAccessPath.Union),
            "union-metadata" => WorkWith(
                seek: 4,
                plannerNode: 3,
                plannerMetadata: 1,
                accessPath: PartitionQueryAccessPath.Union),
            _ => throw new ArgumentOutOfRangeException(nameof(evidence)),
        };
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: work)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateBooleanQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ResumedMixedUnionPlanningLowerBoundRequiresItsRangeBucketVisit(
        bool reportRangeBucket)
    {
        var firstItem = CreateId($"mixed-union-bucket-{reportRangeBucket}");
        var partition = new PagePartition(request => Task.FromResult(request.HasAfter
            ? Result(
                request,
                [],
                exhausted: true,
                work: WorkWith(
                    seek: 4,
                    rangeBucket: reportRangeBucket ? 1 : 0,
                    plannerNode: 3,
                    plannerMetadata: 2,
                    accessPath: PartitionQueryAccessPath.Union))
            : Result(
                request,
                [firstItem],
                frontier: firstItem,
                work: WorkWith(
                    orderedCandidate: 1,
                    record: 1,
                    predicate: 2,
                    entry: 1,
                    ownership: 1,
                    materialization: 1,
                    plannerNode: 3,
                    catalogCandidate: 1))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });
        var query = CreateMixedBooleanQuery(client);
        var first = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10));

        if (reportRangeBucket)
        {
            var page = await query.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(10, first.ContinuationToken));

            page.Items.Should().BeEmpty();
            page.ContinuationToken.Should().BeNull();
            return;
        }

        Func<Task> execute = async () => await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10, first.ContinuationToken));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData((int)PartitionQueryAccessPath.RangeMerge)]
    [InlineData((int)PartitionQueryAccessPath.Union)]
    public async Task ExactRootRejectsAnIncompatibleScalarAccessPath(int rawAccessPath)
    {
        var accessPath = (PartitionQueryAccessPath)rawAccessPath;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: CreateZeroCandidateWork(accessPath))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData((int)PartitionQueryAccessPath.Empty)]
    [InlineData((int)PartitionQueryAccessPath.Catalog)]
    public async Task ExactRootAcceptsFirstPageNonSelectiveZeroCandidateAccessPaths(
        int rawAccessPath)
    {
        var accessPath = (PartitionQueryAccessPath)rawAccessPath;
        var work = accessPath == PartitionQueryAccessPath.Empty
            ? WorkWith(
                plannerMetadata: 1,
                accessPath: PartitionQueryAccessPath.Empty)
            : CreateZeroCandidateWork(accessPath);
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: work)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        var page = await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        page.Items.Should().BeEmpty();
        page.ContinuationToken.Should().BeNull();
    }

    [Theory]
    [InlineData("seek")]
    [InlineData("metadata")]
    public async Task ExactEmptyProofRequiresItsLookupAndMetadataCharges(string missing)
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: WorkWith(
                seek: missing == "seek" ? 0 : 1,
                plannerMetadata: missing == "metadata" ? 0 : 1,
                accessPath: PartitionQueryAccessPath.Empty))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task RangeRootRejectsAnExactPostingAccessPath()
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: CreateZeroCandidateWork(PartitionQueryAccessPath.ExactPosting))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await client.RangeAsync<PagingState, int>(
            "state",
            state => state.Rank,
            0,
            10);

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task RangeEmptyProofRequiresAChargedLookup()
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: WorkWith(
                seek: 0,
                accessPath: PartitionQueryAccessPath.Empty))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await client.RangeAsync<PagingState, int>(
            "state",
            state => state.Rank,
            0,
            10);

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task RangeRootAcceptsAChargedEmptyProof()
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: WorkWith(accessPath: PartitionQueryAccessPath.Empty))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        var result = await client.RangeAsync<PagingState, int>(
            "state",
            state => state.Rank,
            0,
            10);

        result.Should().BeEmpty();
    }

    [Theory]
    [InlineData("record-without-ownership")]
    [InlineData("predicate-without-record")]
    [InlineData("index-entry-without-predicate")]
    public async Task ScalarWorkRejectsMissingPrerequisiteCharges(string evidence)
    {
        var work = evidence switch
        {
            "record-without-ownership" => WorkWith(
                record: 1,
                predicate: 1),
            "predicate-without-record" => WorkWith(
                orderedCandidate: 1,
                predicate: 1,
                ownership: 1,
                catalogCandidate: 1),
            "index-entry-without-predicate" => WorkWith(entry: 1),
            _ => throw new ArgumentOutOfRangeException(nameof(evidence)),
        };
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: work)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task FrontierRequiresCompletedOwnershipEvidence()
    {
        var frontier = CreateId("frontier-without-ownership");
        var options = CreateOptions();
        options.PartitionWorkBudget = 2;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            frontier: frontier,
            work: WorkWith())));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task ResumedPostingSourceVisitRequiresAnOrderedCandidate()
    {
        var firstItem = CreateId("posting-source-resume");
        var partition = new PagePartition(request => Task.FromResult(request.HasAfter
            ? Result(
                request,
                [],
                exhausted: true,
                work: WorkWith(
                    seek: 2,
                    plannerMetadata: 1,
                    postingCandidate: 1,
                    accessPath: PartitionQueryAccessPath.ExactPosting))
            : Result(
                request,
                [firstItem],
                frontier: firstItem)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });
        var query = CreateQuery(client);
        var first = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10));

        Func<Task> resume = async () => await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10, first.ContinuationToken));

        await resume.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task ExhaustedCatalogSourceVisitRequiresAnOrderedCandidate()
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: WorkWith(catalogCandidate: 1))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExactAndRangeItemsRequireAtLeastOneIndexProbePerResult(
        bool useRangeRoot)
    {
        var item = CreateId(useRangeRoot ? "range-without-index-probe" : "exact-without-index-probe");
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            exhausted: true,
            work: WorkWith(
                orderedCandidate: 1,
                record: 1,
                predicate: 1,
                ownership: 1,
                materialization: 1,
                catalogCandidate: 1))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = useRangeRoot
            ? async () => await client.RangeAsync<PagingState, int>(
                "state",
                state => state.Rank,
                0,
                10)
            : async () => await CreateQuery(client)
                .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task CheckedWorkSumOverflowFailsClosedAsCoordinatorAccounting()
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: WorkWith(
                orderedCandidate: long.MaxValue,
                record: 1))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        var failure = (await execute.Should().ThrowAsync<InvalidOperationException>())
            .Which;
        failure.Message.Should().Contain("overflowed coordinator accounting");
        failure.InnerException.Should().BeOfType<OverflowException>();
    }

    [Fact]
    public async Task WorkBudgetStopMustConsumeTheRequestedPartitionBudget()
    {
        var frontier = CreateId("underreported-work-budget");
        var options = CreateOptions();
        options.PartitionWorkBudget = 6;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            frontier: frontier,
            work: CreateValidCatalogWork(itemCount: 0, hasCompletedFrontier: true),
            stopReason: PartitionQueryPageStopReason.WorkBudget)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task ItemLimitStopMustReturnTheConfiguredNumberOfItems()
    {
        var item = CreateId("underreported-item-limit");
        var options = CreateOptions();
        options.PartitionResponseItemLimit = 2;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            frontier: item,
            stopReason: PartitionQueryPageStopReason.ItemLimit)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task ByteLimitStopCannotReturnAnEmptyItemPrefix()
    {
        var frontier = CreateId("impossible-empty-byte-limit");
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            frontier: frontier,
            stopReason: PartitionQueryPageStopReason.ByteLimit)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task BooleanMaterializationIncludesTheBooleanRootPredicateCharge()
    {
        var item = CreateId("underreported-boolean-root");
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            exhausted: true,
            work: WorkWith(
                orderedCandidate: 1,
                record: 1,
                predicate: 1,
                entry: 1,
                ownership: 1,
                materialization: 1,
                plannerNode: 3,
                catalogCandidate: 1))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateBooleanQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task ResultMaterializationCountMustEqualReturnedItemCount(
        long reportedMaterializationCount)
    {
        var item = CreateId("mismatched-materialization");
        var candidateCount = Math.Max(1, reportedMaterializationCount);
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            exhausted: true,
            work: WorkWith(
                orderedCandidate: candidateCount,
                record: candidateCount,
                predicate: candidateCount,
                entry: candidateCount,
                ownership: candidateCount,
                materialization: reportedMaterializationCount,
                catalogCandidate: candidateCount))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData("ordered")]
    [InlineData("ownership")]
    [InlineData("record")]
    [InlineData("predicate")]
    [InlineData("source")]
    public async Task MaterializedResultsCannotBeHiddenByUnderreportedCandidateWork(
        string component)
    {
        var item = CreateId($"underreported-{component}");
        var work = component switch
        {
            "ordered" => WorkWith(
                record: 1,
                predicate: 1,
                entry: 1,
                ownership: 1,
                materialization: 1,
                catalogCandidate: 1),
            "ownership" => WorkWith(
                orderedCandidate: 1,
                record: 1,
                predicate: 1,
                entry: 1,
                materialization: 1,
                catalogCandidate: 1),
            "record" => WorkWith(
                orderedCandidate: 1,
                predicate: 1,
                entry: 1,
                ownership: 1,
                materialization: 1,
                catalogCandidate: 1),
            "predicate" => WorkWith(
                orderedCandidate: 1,
                record: 1,
                entry: 1,
                ownership: 1,
                materialization: 1,
                catalogCandidate: 1),
            "source" => WorkWith(
                orderedCandidate: 1,
                record: 1,
                predicate: 1,
                entry: 1,
                ownership: 1,
                materialization: 1),
            _ => throw new ArgumentOutOfRangeException(nameof(component)),
        };
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            exhausted: true,
            work: work)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData(false, 3, 1)]
    [InlineData(true, 2, 0)]
    [InlineData(true, 2, 1)]
    public async Task AtMostOneUnownedCandidateIsAllowedOnlyForAWorkBudgetStop(
        bool exhausted,
        long orderedCandidate,
        long ownership)
    {
        var frontier = CreateId("partial-candidate-frontier");
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted,
            frontier,
            WorkWith(
                orderedCandidate: orderedCandidate,
                ownership: ownership,
                catalogCandidate: orderedCandidate))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task WorkBudgetStopMayIncludeOneCandidateWhoseOwnershipChargeDidNotFit()
    {
        var frontier = CreateId("completed-before-partial-candidate");
        var options = CreateOptions();
        options.PartitionWorkBudget = 7;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: false,
            frontier: frontier,
            work: WorkWith(
                orderedCandidate: 2,
                ownership: 1,
                catalogCandidate: 2))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        var page = await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        page.Items.Should().BeEmpty();
        page.ContinuationToken.Should().NotBeNull();
    }

    [Fact]
    public async Task WorkBudgetStopMayIncludeOneRecordWhoseRootPredicateChargeDidNotFit()
    {
        var frontier = CreateId("completed-before-partial-record");
        var options = CreateOptions();
        options.PartitionWorkBudget = 11;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: false,
            frontier: frontier,
            work: WorkWith(
                orderedCandidate: 2,
                record: 2,
                predicate: 1,
                ownership: 2,
                catalogCandidate: 2))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        var page = await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        page.Items.Should().BeEmpty();
        page.ContinuationToken.Should().NotBeNull();
    }

    [Fact]
    public async Task WorkBudgetStopCannotReportTwoSimultaneouslyIncompleteStages()
    {
        var frontier = CreateId("forged-multiple-partial-stages");
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: false,
            frontier: frontier,
            work: WorkWith(
                orderedCandidate: 2,
                record: 1,
                ownership: 1,
                catalogCandidate: 2))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData((int)PartitionQueryAccessPath.Empty)]
    [InlineData((int)PartitionQueryAccessPath.Catalog)]
    public async Task FirstPageNonSelectiveAccessPathMayHaveNoCandidate(
        int rawAccessPath)
    {
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: CreateBooleanZeroCandidateWork(
                (PartitionQueryAccessPath)rawAccessPath,
                plannerNodeCount: 3))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        var page = await CreateBooleanQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        page.Items.Should().BeEmpty();
        page.ContinuationToken.Should().BeNull();
    }

    [Theory]
    [InlineData((int)PartitionQueryAccessPath.ExactPosting)]
    [InlineData((int)PartitionQueryAccessPath.RangeMerge)]
    [InlineData((int)PartitionQueryAccessPath.Union)]
    public async Task ResumedSelectiveAccessPathMayHaveNoCandidateAfterItsBoundary(
        int rawAccessPath)
    {
        var accessPath = (PartitionQueryAccessPath)rawAccessPath;
        var firstItem = CreateId($"selective-resume-{accessPath}");
        var partition = new PagePartition(request => Task.FromResult(request.HasAfter
            ? Result(
                request,
                [],
                exhausted: true,
                work: CreateMixedBooleanZeroCandidateWork(accessPath))
            : Result(
                request,
                [firstItem],
                frontier: firstItem,
                work: WorkWith(
                    orderedCandidate: 1,
                    record: 1,
                    predicate: 2,
                    entry: 1,
                    ownership: 1,
                    materialization: 1,
                    plannerNode: 3,
                    catalogCandidate: 1))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });
        var query = CreateMixedBooleanQuery(client);

        var first = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10));
        var second = await query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(10, first.ContinuationToken));

        first.Items.Should().ContainSingle().Which.Should().Be(firstItem);
        first.ContinuationToken.Should().NotBeNull();
        second.Items.Should().BeEmpty();
        second.ContinuationToken.Should().BeNull();
        partition.Requests.Should().HaveCount(2);
        partition.Requests[1].HasAfter.Should().BeTrue();
        partition.Requests[1].After.Should().Be(firstItem);
    }

    [Theory]
    [InlineData((int)PartitionQueryAccessPath.ExactPosting)]
    [InlineData((int)PartitionQueryAccessPath.RangeMerge)]
    [InlineData((int)PartitionQueryAccessPath.Union)]
    public async Task FirstPageSelectiveAccessPathCannotReportZeroCandidates(
        int rawAccessPath)
    {
        var accessPath = (PartitionQueryAccessPath)rawAccessPath;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: CreateMixedBooleanZeroCandidateWork(accessPath))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateMixedBooleanQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Theory]
    [InlineData("frontier")]
    [InlineData("candidate")]
    [InlineData("posting-source")]
    [InlineData("catalog-source")]
    [InlineData("record")]
    [InlineData("predicate")]
    [InlineData("index-entry")]
    [InlineData("materialization")]
    [InlineData("heap")]
    [InlineData("union")]
    [InlineData("range-merge")]
    public async Task EmptyAccessPathRejectsExecutionEvidence(string evidence)
    {
        var frontier = CreateId("empty-path-frontier");
        var work = WorkWith(
            orderedCandidate: evidence == "candidate" ? 1 : 0,
            record: evidence == "record" ? 1 : 0,
            predicate: evidence == "predicate" ? 1 : 0,
            entry: evidence == "index-entry" ? 1 : 0,
            ownership: evidence == "candidate" ? 1 : 0,
            materialization: evidence == "materialization" ? 1 : 0,
            rangeMerge: evidence == "range-merge" ? 1 : 0,
            postingCandidate: evidence == "posting-source" ? 1 : 0,
            catalogCandidate: evidence == "catalog-source" ? 1 : 0,
            heap: evidence == "heap" ? 1 : 0,
            union: evidence == "union" ? 1 : 0,
            plannerMetadata: 1,
            accessPath: PartitionQueryAccessPath.Empty);
        var items = evidence == "materialization" ? new[] { CreateId("empty-item") } : [];
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            items,
            exhausted: evidence != "frontier",
            frontier: evidence == "frontier" ? frontier : null,
            work: work)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*inconsistent scalar work evidence*");
    }

    [Fact]
    public async Task PublicPageMapsPartitionBudgetFailureToThePublicLimitException()
    {
        PartitionQueryBudgetTooSmallException? partitionFailure = null;
        var partition = new PagePartition(request =>
        {
            partitionFailure = new PartitionQueryBudgetTooSmallException(
                request.WorkBudget,
                checked(request.WorkBudget + 1),
                PartitionQueryPageStopReason.WorkBudget);
            return Task.FromException<PartitionQueryPageResult>(partitionFailure);
        });
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        var failure = (await execute.Should()
            .ThrowAsync<SearchableStorageQueryLimitExceededException>()).Which;
        failure.InnerException.Should().BeSameAs(partitionFailure);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DefaultGrainIdsAreRejectedInItemsAndFrontiers(bool defaultIsItem)
    {
        var partition = new PagePartition(request => Task.FromResult(defaultIsItem
            ? Result(request, [default], exhausted: true)
            : Result(request, [], frontier: default(GrainId))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition });

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(10));

        await execute.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task OversizedPageRequestIsRejectedBeforeUninitializedLayoutLookup()
    {
        var layoutLoadCount = 0;
        var options = new SearchableStorageQueryOptions { PageSizeLimit = 2 };
        var client = CreateClient(
            new StorageLayoutCache(() =>
            {
                Interlocked.Increment(ref layoutLoadCount);
                return Task.FromResult<StorageLayoutSnapshot?>(null);
            }),
            new Dictionary<int, PagePartition>(),
            options);

        Func<Task> execute = async () => await CreateQuery(client)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(3));

        await execute.Should().ThrowAsync<ArgumentOutOfRangeException>();
        layoutLoadCount.Should().Be(0);
    }

    [Fact]
    public async Task PreCanceledPageWinsBeforeMissingKeyAndLayoutValidation()
    {
        var layoutLoadCount = 0;
        var client = CreateClient(
            new StorageLayoutCache(() =>
            {
                Interlocked.Increment(ref layoutLoadCount);
                return Task.FromResult<StorageLayoutSnapshot?>(null);
            }),
            new Dictionary<int, PagePartition>(),
            new SearchableStorageQueryOptions());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Func<Task> execute = async () => await CreateQuery(client).ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(1),
            cancellation.Token);

        await execute.Should().ThrowAsync<OperationCanceledException>();
        layoutLoadCount.Should().Be(0);
    }

    [Theory]
    [InlineData("items")]
    [InlineData("bytes")]
    [InlineData("work")]
    [InlineData("rounds")]
    public async Task LegacyQueryThrowsWithoutPartialResultsAtEveryAggregateCeiling(string ceiling)
    {
        var first = CreateId("a");
        var second = CreateId("b");
        var options = CreateOptions();
        options.LegacyResultItemLimit = ceiling == "items" ? 1 : 10;
        options.LegacyResultByteLimit = ceiling == "bytes" ? 1 : 1_024;
        options.LegacyAggregateWorkLimit = ceiling == "work" ? 5 : 1_024;
        options.LegacyRoundLimit = ceiling == "rounds" ? 1 : 10;
        if (ceiling == "rounds")
        {
            options.PartitionWorkBudget = 5;
        }

        var partition = new PagePartition(request => Task.FromResult(ceiling switch
        {
            "rounds" => Result(request, [], frontier: first),
            "items" => Result(request, [first], frontier: first),
            "work" => Result(
                request,
                [],
                frontier: first,
                work: CreateValidCatalogWork(
                    itemCount: 0,
                    hasCompletedFrontier: true)),
            _ => Result(
                request,
                [first, second],
                exhausted: true,
                work: CreateValidCatalogWork(itemCount: 2)),
        }));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        Func<Task> execute = async () => await client.FindAsync<PagingState, string>(
            "state",
            state => state.Value,
            "match");

        await execute.Should().ThrowAsync<SearchableStorageQueryLimitExceededException>();
        partition.UnboundedQueryCallCount.Should().Be(0);
        partition.UnboundedFindCallCount.Should().Be(0);
        partition.UnboundedRangeCallCount.Should().Be(0);
    }

    [Theory]
    [InlineData("items")]
    [InlineData("bytes")]
    [InlineData("work")]
    [InlineData("rounds")]
    public async Task LegacyTerminalPageMayFinishExactlyAtEveryAggregateCeiling(string ceiling)
    {
        var item = CreateId("terminal-at-limit");
        var itemBytes = GrainIdCanonicalOrder.GetEncodedLength(item);
        var options = CreateOptions();
        options.LegacyResultItemLimit = ceiling == "items" ? 1 : 10;
        options.LegacyResultByteLimit = ceiling == "bytes" ? itemBytes : 1_024;
        options.LegacyAggregateWorkLimit = ceiling == "work" ? 9 : 1_024;
        options.LegacyRoundLimit = ceiling == "rounds" ? 1 : 10;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            exhausted: true,
            work: CreateValidCatalogWork(itemCount: 1))));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        var result = await client.FindAsync<PagingState, string>(
            "state",
            state => state.Value,
            "match");

        result.Should().ContainSingle().Which.Should().Be(item);
        partition.Requests.Should().ContainSingle();
    }

    [Theory]
    [InlineData("items")]
    [InlineData("bytes")]
    [InlineData("work")]
    [InlineData("rounds")]
    public async Task LegacyNonTerminalPageFailsExactlyAtEveryAggregateCeiling(string ceiling)
    {
        var item = CreateId("nonterminal-at-limit");
        var itemBytes = GrainIdCanonicalOrder.GetEncodedLength(item);
        var options = CreateOptions();
        options.LegacyResultItemLimit = ceiling == "items" ? 1 : 10;
        options.LegacyResultByteLimit = ceiling == "bytes" ? itemBytes : 1_024;
        options.LegacyAggregateWorkLimit = ceiling == "work" ? 9 : 1_024;
        options.LegacyRoundLimit = ceiling == "rounds" ? 1 : 10;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            frontier: item)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0),
            new Dictionary<int, PagePartition> { [0] = partition },
            options);

        Func<Task> execute = async () => await client.FindAsync<PagingState, string>(
            "state",
            state => state.Value,
            "match");

        await execute.Should().ThrowAsync<SearchableStorageQueryLimitExceededException>();
        partition.Requests.Should().ContainSingle();
    }

    [Fact]
    public async Task LegacyWorkBudgetIsApportionedBeforeFanout()
    {
        var options = CreateOptions();
        options.LegacyAggregateWorkLimit = 5;
        var owner0 = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: WorkWith())));
        var owner1 = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: WorkWith())));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 },
            options);

        var result = await client.FindAsync<PagingState, string>(
            "state",
            state => state.Value,
            "match");

        result.Should().BeEmpty();
        owner0.Requests.Should().ContainSingle().Which.WorkBudget.Should().Be(2);
        owner1.Requests.Should().ContainSingle().Which.WorkBudget.Should().Be(2);
    }

    private static IQueryable<PagingState> CreateQuery(SearchableStorageClient client)
    {
        return client.Query<PagingState>("state").Where(state => state.Value == "match");
    }

    private static IQueryable<PagingState> CreateBooleanQuery(SearchableStorageClient client)
    {
        return client.Query<PagingState>("state").Where(
            state => state.Value == "match" || state.Value == "alternate");
    }

    private static IQueryable<PagingState> CreateMixedBooleanQuery(
        SearchableStorageClient client)
    {
        return client.Query<PagingState>("state").Where(
            state => state.Value == "match" || state.Rank >= 0);
    }

    private static SearchableStorageClient CreateClient(
        StorageLayoutSnapshot layout,
        IReadOnlyDictionary<int, PagePartition> partitions,
        SearchableStorageQueryOptions? options = null,
        Action<Task>? detachedFanoutObserver = null)
    {
        return CreateClient(
            new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(layout)),
            partitions,
            options,
            detachedFanoutObserver);
    }

    private static SearchableStorageClient CreateClient(
        StorageLayoutCache cache,
        IReadOnlyDictionary<int, PagePartition> partitions,
        SearchableStorageQueryOptions? options = null,
        Action<Task>? detachedFanoutObserver = null)
    {
        return new SearchableStorageClient(
            ProviderName,
            cache,
            owner => partitions[owner],
            options ?? CreateOptions(),
            detachedFanoutObserver: detachedFanoutObserver);
    }

    private static async Task ObserveForTestAsync(Task task, TaskCompletionSource completion)
    {
        await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        completion.TrySetResult();
    }

    private static SearchableStorageQueryOptions CreateOptions()
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "coordinator-tests",
            Enumerable.Range(1, 32).Select(static value => (byte)value).ToArray());
        return options;
    }

    private static StorageLayoutSnapshot CreateLayout(long epoch, params int[] assignments)
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = ProviderName,
            PartitionCount = assignments.Distinct().Count(),
            VirtualSlotCount = assignments.Length,
            SlotAssignments = assignments,
            Epoch = epoch,
        });
    }

    private static PartitionQueryPageResult Result(
        RoutedPartitionQueryPageRequest request,
        GrainId[] items,
        bool exhausted = false,
        GrainId? frontier = null,
        PartitionQueryPageWork? work = null,
        PartitionQueryPageStopReason? stopReason = null)
    {
        return new PartitionQueryPageResult
        {
            Items = items,
            HasFrontier = !exhausted,
            Frontier = exhausted ? default : frontier ?? throw new ArgumentNullException(nameof(frontier)),
            Exhausted = exhausted,
            StopReason = exhausted
                ? PartitionQueryPageStopReason.Exhausted
                : stopReason ?? (items.Length > 0
                    ? PartitionQueryPageStopReason.ByteLimit
                    : PartitionQueryPageStopReason.WorkBudget),
            Work = work ?? CreateValidCatalogWork(
                items.Length,
                hasCompletedFrontier: !exhausted),
            ItemByteCount = items.Any(static item => item.IsDefault)
                ? 0
                : items.Sum(GrainIdCanonicalOrder.GetEncodedLength),
            ProtocolVersion = request.ProtocolVersion,
            OrderingVersion = request.OrderingVersion,
            WorkPolicyVersion = request.WorkPolicyVersion,
            ResponseFamily = request.ResponseFamily,
            Epoch = request.Epoch,
            QueryFingerprint = [.. request.QueryFingerprint],
            LayoutFormatVersion = request.LayoutFormatVersion,
            LayoutFingerprint = [.. request.LayoutFingerprint],
        };
    }

    private static PartitionQueryPageWork WorkWith(
        long orderedCandidate = 0,
        long record = 0,
        long predicate = 0,
        long entry = 0,
        long ownership = 0,
        long seek = 1,
        long rangeBucket = 0,
        long materialization = 0,
        long rangeMerge = 0,
        long plannerNode = 1,
        long plannerMetadata = 0,
        long postingCandidate = 0,
        long catalogCandidate = 0,
        long heap = 0,
        long union = 0,
        PartitionQueryAccessPath accessPath = PartitionQueryAccessPath.Catalog)
    {
        return new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = orderedCandidate,
            RecordProbeCount = record,
            PredicateNodeProbeCount = predicate,
            IndexEntryProbeCount = entry,
            OwnershipProbeCount = ownership,
            PostingSeekCount = seek,
            RangeBucketVisitCount = rangeBucket,
            ResultMaterializationCount = materialization,
            RangeMergeOperationCount = rangeMerge,
            PlannerNodeVisitCount = plannerNode,
            PlannerMetadataReadCount = plannerMetadata,
            PostingCandidateVisitCount = postingCandidate,
            CatalogCandidateVisitCount = catalogCandidate,
            HeapOperationCount = heap,
            UnionOperationCount = union,
            AccessPath = accessPath,
        };
    }

    private static PartitionQueryPageWork CreateValidCatalogWork(
        int itemCount,
        bool hasCompletedFrontier = false)
    {
        var candidateCount = Math.Max(itemCount, hasCompletedFrontier ? 1 : 0);
        return WorkWith(
            orderedCandidate: candidateCount,
            record: itemCount,
            predicate: itemCount,
            entry: itemCount,
            ownership: candidateCount,
            materialization: itemCount,
            catalogCandidate: candidateCount);
    }

    private static PartitionQueryPageWork CreateZeroCandidateWork(
        PartitionQueryAccessPath accessPath,
        long plannerNodeCount = 1)
    {
        return accessPath switch
        {
            PartitionQueryAccessPath.Empty => WorkWith(
                seek: 0,
                plannerNode: plannerNodeCount,
                plannerMetadata: 0,
                accessPath: accessPath),
            PartitionQueryAccessPath.ExactPosting => WorkWith(
                seek: 2,
                plannerNode: plannerNodeCount,
                plannerMetadata: 1,
                accessPath: accessPath),
            PartitionQueryAccessPath.RangeMerge => WorkWith(
                seek: 2,
                rangeBucket: 1,
                plannerNode: plannerNodeCount,
                plannerMetadata: 1,
                accessPath: accessPath),
            PartitionQueryAccessPath.Union => WorkWith(
                seek: 4,
                plannerNode: plannerNodeCount,
                plannerMetadata: 2,
                accessPath: accessPath),
            PartitionQueryAccessPath.Catalog => WorkWith(
                plannerNode: plannerNodeCount,
                accessPath: accessPath),
            _ => throw new ArgumentOutOfRangeException(nameof(accessPath)),
        };
    }

    private static PartitionQueryPageWork CreateBooleanZeroCandidateWork(
        PartitionQueryAccessPath accessPath,
        long plannerNodeCount)
    {
        return accessPath switch
        {
            PartitionQueryAccessPath.Empty => WorkWith(
                seek: 2,
                plannerNode: plannerNodeCount,
                plannerMetadata: 2,
                accessPath: accessPath),
            PartitionQueryAccessPath.ExactPosting => WorkWith(
                seek: 3,
                plannerNode: plannerNodeCount,
                plannerMetadata: 2,
                accessPath: accessPath),
            PartitionQueryAccessPath.Union => WorkWith(
                seek: 4,
                plannerNode: plannerNodeCount,
                plannerMetadata: 2,
                accessPath: accessPath),
            PartitionQueryAccessPath.Catalog => WorkWith(
                plannerNode: plannerNodeCount,
                accessPath: accessPath),
            _ => throw new ArgumentOutOfRangeException(nameof(accessPath)),
        };
    }

    private static PartitionQueryPageWork CreateMixedBooleanZeroCandidateWork(
        PartitionQueryAccessPath accessPath)
    {
        return accessPath switch
        {
            PartitionQueryAccessPath.ExactPosting => WorkWith(
                seek: 3,
                plannerNode: 3,
                plannerMetadata: 1,
                accessPath: accessPath),
            PartitionQueryAccessPath.RangeMerge => WorkWith(
                seek: 3,
                rangeBucket: 1,
                plannerNode: 3,
                plannerMetadata: 2,
                accessPath: accessPath),
            PartitionQueryAccessPath.Union => WorkWith(
                seek: 4,
                rangeBucket: 1,
                plannerNode: 3,
                plannerMetadata: 2,
                accessPath: accessPath),
            _ => throw new ArgumentOutOfRangeException(nameof(accessPath)),
        };
    }

    private static StorageRouteMismatchException CreateMismatch(
        long expectedEpoch,
        long currentEpoch,
        int requestedOwner)
    {
        return new StorageRouteMismatchException(
            expectedEpoch,
            currentEpoch,
            requestedOwner);
    }

    private static GrainId CreateId(string key) => GrainId.Create("paging", key);

    private sealed class PagingState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Value { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Rank { get; init; }
    }

    private sealed class MutableMatchSet
    {
        private readonly object _lock = new();
        private readonly SortedSet<GrainId> _matches = new(GrainIdCanonicalOrder.Comparer);

        public MutableMatchSet(params GrainId[] matches)
        {
            foreach (var match in matches)
            {
                if (!_matches.Add(match))
                {
                    throw new ArgumentException("Initial matches must be distinct.", nameof(matches));
                }
            }

            Partition = new PagePartition(QueryAsync);
        }

        public PagePartition Partition { get; }

        public void Add(GrainId match)
        {
            lock (_lock)
            {
                if (!_matches.Add(match))
                {
                    throw new InvalidOperationException("The match already exists in the test set.");
                }
            }
        }

        public void Remove(GrainId match)
        {
            lock (_lock)
            {
                if (!_matches.Remove(match))
                {
                    throw new InvalidOperationException("The match does not exist in the test set.");
                }
            }
        }

        private Task<PartitionQueryPageResult> QueryAsync(
            RoutedPartitionQueryPageRequest request)
        {
            GrainId[] remaining;
            lock (_lock)
            {
                remaining = _matches
                    .Where(match => !request.HasAfter
                        || GrainIdCanonicalOrder.Compare(match, request.After) > 0)
                    .ToArray();
            }

            var items = remaining.Take(request.ItemLimit).ToArray();
            return Task.FromResult(items.Length == remaining.Length
                ? Result(request, items, exhausted: true)
                : Result(request, items, frontier: items[^1]));
        }
    }

    private sealed class PagePartition : StoragePartitionGrainMovementTestDouble, IStoragePartitionGrain
    {
        private readonly Func<RoutedPartitionQueryPageRequest, Task<PartitionQueryPageResult>> _query;
        private readonly TaskCompletionSource _started = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public PagePartition(
            Func<RoutedPartitionQueryPageRequest, Task<PartitionQueryPageResult>> query)
        {
            _query = query;
        }

        public List<RoutedPartitionQueryPageRequest> Requests { get; } = [];

        public int UnboundedQueryCallCount { get; private set; }

        public int UnboundedFindCallCount { get; private set; }

        public int UnboundedRangeCallCount { get; private set; }

        public Task Started => _started.Task;

        public Task<PartitionQueryPageResult> QueryPageRoutedAsync(
            RoutedPartitionQueryPageRequest request)
        {
            Requests.Add(request);
            _started.TrySetResult();
            return _query(request);
        }

        public Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
            RoutedPartitionDistinctFacetPageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
            RoutedPartitionFacetCandidatePageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
            RoutedPartitionFacetCountSliceRequest request) => throw new NotSupportedException();

        public Task<GrainId[]> FindAsync(ExactIndexQuery query) =>
            throw new NotSupportedException();

        public Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query)
        {
            UnboundedFindCallCount++;
            throw new NotSupportedException();
        }

        public Task<GrainId[]> RangeAsync(RangeIndexQuery query) =>
            throw new NotSupportedException();

        public Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query)
        {
            UnboundedRangeCallCount++;
            throw new NotSupportedException();
        }

        public Task<GrainId[]> QueryAsync(PartitionQueryPlan query) =>
            throw new NotSupportedException();

        public Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query)
        {
            UnboundedQueryCallCount++;
            throw new NotSupportedException();
        }

        public Task<StorageReadResult> ReadAsync(string recordKey) => throw new NotSupportedException();

        public Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request) =>
            throw new NotSupportedException();

        public Task<string> WriteAsync(StorageWriteRequest request) => throw new NotSupportedException();

        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request) =>
            throw new NotSupportedException();

        public Task ClearAsync(StorageClearRequest request) => throw new NotSupportedException();

        public Task ClearRoutedAsync(RoutedStorageClearRequest request) =>
            throw new NotSupportedException();

        public Task CompactAsync() => throw new NotSupportedException();

        public Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync() =>
            throw new NotSupportedException();
    }
}
