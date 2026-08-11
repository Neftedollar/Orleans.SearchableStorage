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
        var owner0 = new PagePartition(request => Task.FromResult(request.HasAfter
            ? Result(request, [], exhausted: true)
            : Result(request, [], frontier: firstFrontier)));
        var owner1 = new PagePartition(request => Task.FromResult(request.HasAfter
            ? Result(request, [], exhausted: true)
            : Result(request, [], frontier: secondFrontier)));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 });
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
            new Dictionary<int, PagePartition> { [0] = partition });
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
    public async Task NegativeWorkComponentCannotBeHiddenByAPositiveComponent(string component)
    {
        var work = component switch
        {
            "candidate" => new PartitionQueryPageWork { OrderedCandidateVisitCount = -1 },
            "record" => new PartitionQueryPageWork { RecordProbeCount = -1 },
            "predicate" => new PartitionQueryPageWork { PredicateNodeProbeCount = -1 },
            "entry" => new PartitionQueryPageWork { IndexEntryProbeCount = -1 },
            "ownership" => new PartitionQueryPageWork { OwnershipProbeCount = -1 },
            "seek" => new PartitionQueryPageWork { PostingSeekCount = -1 },
            "range-bucket" => new PartitionQueryPageWork { RangeBucketVisitCount = -1 },
            "materialization" => new PartitionQueryPageWork { ResultMaterializationCount = -1 },
            "range-merge" => new PartitionQueryPageWork { RangeMergeOperationCount = -1 },
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
        options.LegacyAggregateWorkLimit = ceiling == "work" ? 1 : 1_024;
        options.LegacyRoundLimit = ceiling == "rounds" ? 1 : 10;
        var partition = new PagePartition(request => Task.FromResult(ceiling switch
        {
            "rounds" => Result(request, [], frontier: first),
            "items" => Result(request, [first], frontier: first),
            "work" => Result(
                request,
                [],
                frontier: first,
                work: new PartitionQueryPageWork { RecordProbeCount = request.WorkBudget }),
            _ => Result(
                request,
                [first, second],
                exhausted: true,
                work: new PartitionQueryPageWork { RecordProbeCount = 2 }),
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
        options.LegacyAggregateWorkLimit = ceiling == "work" ? 2 : 1_024;
        options.LegacyRoundLimit = ceiling == "rounds" ? 1 : 10;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            exhausted: true,
            work: new PartitionQueryPageWork
            {
                RecordProbeCount = ceiling == "work" ? 2 : 0,
            })));
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
        options.LegacyAggregateWorkLimit = ceiling == "work" ? 2 : 1_024;
        options.LegacyRoundLimit = ceiling == "rounds" ? 1 : 10;
        var partition = new PagePartition(request => Task.FromResult(Result(
            request,
            [item],
            frontier: item,
            work: new PartitionQueryPageWork
            {
                RecordProbeCount = ceiling == "work" ? 2 : 0,
            })));
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
        options.LegacyAggregateWorkLimit = 3;
        var owner0 = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: new PartitionQueryPageWork { RecordProbeCount = request.WorkBudget })));
        var owner1 = new PagePartition(request => Task.FromResult(Result(
            request,
            [],
            exhausted: true,
            work: new PartitionQueryPageWork { RecordProbeCount = request.WorkBudget })));
        var client = CreateClient(
            CreateLayout(epoch: 1, 0, 1),
            new Dictionary<int, PagePartition> { [0] = owner0, [1] = owner1 },
            options);

        var result = await client.FindAsync<PagingState, string>(
            "state",
            state => state.Value,
            "match");

        result.Should().BeEmpty();
        owner0.Requests.Should().ContainSingle().Which.WorkBudget.Should().Be(1);
        owner1.Requests.Should().ContainSingle().Which.WorkBudget.Should().Be(1);
    }

    private static IQueryable<PagingState> CreateQuery(SearchableStorageClient client)
    {
        return client.Query<PagingState>("state").Where(state => state.Value == "match");
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
            FormatVersion = StorageLayout.CurrentFormatVersion,
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
        PartitionQueryPageWork? work = null)
    {
        return new PartitionQueryPageResult
        {
            Items = items,
            HasFrontier = !exhausted,
            Frontier = exhausted ? default : frontier ?? throw new ArgumentNullException(nameof(frontier)),
            Exhausted = exhausted,
            StopReason = exhausted
                ? PartitionQueryPageStopReason.Exhausted
                : PartitionQueryPageStopReason.WorkBudget,
            Work = work ?? new PartitionQueryPageWork(),
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
