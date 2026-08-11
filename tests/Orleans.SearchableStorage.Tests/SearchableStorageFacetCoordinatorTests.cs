using System.Collections.Concurrent;
using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageFacetCoordinatorTests
{
    private const string ProviderName = "facet-coordinator";
    private const string StateName = "facet-state";

    [Fact]
    public async Task ExactTopNFindsAFilteredGlobalWinnerHiddenBeyondThreeValueOrderedTurns()
    {
        var fixture = CreateFixture(ownerCount: 2);
        fixture.Partitions[0].AddRows("a", 5, included: false);
        fixture.Partitions[0].AddRows("c", 4, included: false);
        fixture.Partitions[0].AddRows("z", 3, included: true);
        fixture.Partitions[1].AddRows("b", 5, included: false);
        fixture.Partitions[1].AddRows("d", 4, included: false);
        fixture.Partitions[1].AddRows("z", 3, included: true);
        var query = fixture.Client.Query<FacetState>(StateName)
            .Where(state => state.Included == true);

        var result = await query.ToFacetValueCountsAsync(
            state => state.Category,
            new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact));

        result.IsExact.Should().BeTrue();
        result.MaximumOmittedCount.Should().Be(0);
        result.Items.Should().ContainSingle();
        result.Items[0].Value.Should().Be("z");
        result.Items[0].Count.Should().Be(6);
        fixture.Partitions.Sum(static partition => partition.CandidateRequests.Count)
            .Should().BeGreaterThanOrEqualTo(6);
        fixture.Partitions.SelectMany(static partition => partition.CountRequests)
            .Count(request => request.Value.Equals(IndexValue.Create("z")))
            .Should().Be(2);
    }

    [Fact]
    public async Task ApproximateTopNReturnsExactNominatedCountsAndBoundsTheHiddenOracleWinner()
    {
        var fixture = CreateFixture(ownerCount: 2);
        fixture.Partitions[0].AddRows("a", 5, included: false);
        fixture.Partitions[0].AddRows("z", 3, included: true);
        fixture.Partitions[1].AddRows("b", 5, included: false);
        fixture.Partitions[1].AddRows("z", 3, included: true);
        var query = fixture.Client.Query<FacetState>(StateName)
            .Where(state => state.Included == true);

        var result = await query.ToFacetValueCountsAsync(
            state => state.Category,
            new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Approximate));

        result.IsExact.Should().BeFalse();
        result.Items.Should().BeEmpty();
        result.MaximumOmittedCount.Should().BeGreaterThanOrEqualTo(6);
        fixture.Partitions.Sum(static partition => partition.CandidateRequests.Count).Should().Be(2);
    }

    [Fact]
    public async Task ExactThresholdStopsBeforeExhaustionOnlyWhenNthStrictlyExceedsRemainingRawCount()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRows("a", 10, included: true);
        fixture.Partitions[0].AddRow("b", included: true);
        fixture.Partitions[0].AddRow("c", included: true);

        var result = await fixture.Client.Query<FacetState>(StateName)
            .ToFacetValueCountsAsync(
                state => state.Category,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact));

        result.IsExact.Should().BeTrue();
        result.Items.Should().ContainSingle().Which.Count.Should().Be(10);
        fixture.Partitions[0].CandidateRequests.Should().ContainSingle();
    }

    [Fact]
    public async Task ExactThresholdDeepensWhenNthEqualsTheCertifiedUnseenBound()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRows("a", 2, included: true);
        fixture.Partitions[0].AddRows("b", 2, included: true);

        var result = await fixture.Client.Query<FacetState>(StateName)
            .ToFacetValueCountsAsync(
                state => state.Category,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact));

        result.Items.Should().ContainSingle();
        result.Items[0].Value.Should().Be("a");
        result.Items[0].Count.Should().Be(2);
        fixture.Partitions[0].CandidateRequests.Count.Should().Be(2);
    }

    [Fact]
    public async Task ApproximateOmittedBoundIncludesAnExactNominatedRunnerUp()
    {
        var fixture = CreateFixture(ownerCount: 2);
        fixture.Partitions[0].AddRows("a", 5, included: true);
        fixture.Partitions[1].AddRows("b", 4, included: true);
        fixture.Partitions[1].AddRow("z", included: true);

        var result = await fixture.Client.Query<FacetState>(StateName)
            .ToFacetValueCountsAsync(
                state => state.Category,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Approximate));

        result.Items.Should().ContainSingle();
        result.Items[0].Value.Should().Be("a");
        result.Items[0].Count.Should().Be(5);
        result.MaximumOmittedCount.Should().Be(4);
    }

    [Fact]
    public async Task ExactCountsUseCanonicalValueTieBreakAndIgnoreCopiedPostingsOnTheWrongOwner()
    {
        var fixture = CreateFixture(ownerCount: 2);
        fixture.Partitions[0].AddRows("alpha", 2, included: true);
        fixture.Partitions[0].AddRows("beta", 1, included: true);
        fixture.Partitions[1].AddRows("alpha", 1, included: true);
        fixture.Partitions[1].AddRows("beta", 2, included: true);
        var copied = fixture.Partitions[0].AddRow("copied", included: true);
        fixture.Partitions[1].AddCopiedRecord(copied.RecordKey, copied.Record);

        var result = await fixture.Client.Query<FacetState>(StateName)
            .ToFacetValueCountsAsync(
                state => state.Category,
                new SearchableStorageFacetRequest(3, SearchableStorageFacetAccuracy.Exact));

        result.Items.Select(static item => (item.Value, item.Count)).Should().Equal(
            ("alpha", 3L),
            ("beta", 3L),
            ("copied", 1L));
    }

    [Fact]
    public async Task DistinctPagingCanReturnEmptyNonterminalPagesAndResumesWeaklyByValue()
    {
        var fixture = CreateFixture(ownerCount: 2);
        fixture.Partitions[0].AddRow("a", included: false);
        fixture.Partitions[0].AddRow("z", included: true);
        fixture.Partitions[1].AddRow("b", included: false);
        fixture.Partitions[1].AddRow("c", included: true);
        var query = fixture.Client.Query<FacetState>(StateName)
            .Where(state => state.Included == true);

        var first = await query.ToDistinctFacetValuePageAsync(
            state => state.Category,
            new SearchableStorageFacetPageRequest(1));

        first.Items.Should().BeEmpty();
        first.ContinuationToken.Should().NotBeNull();
        fixture.Partitions[0].AddRow("0", included: true);
        fixture.Partitions[0].AddRow("aa", included: true);

        var values = new List<string>();
        var token = first.ContinuationToken;
        while (token is not null)
        {
            var page = await query.ToDistinctFacetValuePageAsync(
                state => state.Category,
                new SearchableStorageFacetPageRequest(1, token));
            values.AddRange(page.Items);
            token = page.ContinuationToken;
        }

        values.Should().BeInAscendingOrder(StringComparer.Ordinal);
        values.Should().Contain(["aa", "c", "z"]);
        values.Should().NotContain("0", "a new value before the exclusive weak frontier may be missed");
        values.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task DistinctTokenRejectsTamperCrossQueryCrossScopePolicyAndLayout()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: false);
        fixture.Partitions[0].AddRow("b", included: true);
        var query = fixture.Client.Query<FacetState>(StateName)
            .Where(state => state.Included == true);
        var first = await query.ToDistinctFacetValuePageAsync(
            state => state.Category,
            new SearchableStorageFacetPageRequest(1));
        var token = first.ContinuationToken!;
        var tampered = token[..^1] + (token[^1] == 'A' ? 'B' : 'A');

        await Awaiting(() => query.ToDistinctFacetValuePageAsync(
                state => state.Category,
                new SearchableStorageFacetPageRequest(1, tampered)))
            .Should().ThrowAsync<SearchableStorageInvalidContinuationTokenException>();
        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .Where(state => state.Included == false)
                .ToDistinctFacetValuePageAsync(
                    state => state.Category,
                    new SearchableStorageFacetPageRequest(1, token)))
            .Should().ThrowAsync<SearchableStorageInvalidContinuationTokenException>();
        await Awaiting(() => query.ToDistinctFacetValuePageAsync(
                state => state.Score,
                new SearchableStorageFacetPageRequest(1, token)))
            .Should().ThrowAsync<SearchableStorageInvalidContinuationTokenException>();

        var changedOptions = CreateOptions();
        changedOptions.FacetAggregateWorkLimit--;
        var changedPolicyClient = CreateClient(fixture.Layout, fixture.Partitions, changedOptions);
        await Awaiting(() => changedPolicyClient.Query<FacetState>(StateName)
                .Where(state => state.Included == true)
                .ToDistinctFacetValuePageAsync(
                    state => state.Category,
                    new SearchableStorageFacetPageRequest(1, token)))
            .Should().ThrowAsync<SearchableStorageInvalidContinuationTokenException>();

        var movedLayout = CreateLayout(epoch: fixture.Layout.Epoch + 1, ownerCount: 1);
        var movedClient = CreateClient(movedLayout, fixture.Partitions, CreateOptions());
        await Awaiting(() => movedClient.Query<FacetState>(StateName)
                .Where(state => state.Included == true)
                .ToDistinctFacetValuePageAsync(
                    state => state.Category,
                    new SearchableStorageFacetPageRequest(1, token)))
            .Should().ThrowAsync<SearchableStorageStaleContinuationTokenException>();
    }

    [Fact]
    public async Task MinMaxIsFilteredExactNullableOnEmptyAndAllOrThrowsAtAggregateLimit()
    {
        var fixture = CreateFixture(ownerCount: 2);
        fixture.Partitions[0].AddRow("a", included: false, score: -100);
        fixture.Partitions[0].AddRow("b", included: true, score: -5);
        fixture.Partitions[1].AddRow("c", included: true, score: 100);
        fixture.Partitions[1].AddRow("d", included: false, score: 500);

        var minMax = await fixture.Client.Query<FacetState>(StateName)
            .Where(state => state.Included == true)
            .ToFacetMinMaxAsync(state => state.Score);
        var empty = await fixture.Client.Query<FacetState>(StateName)
            .Where(state => state.Score > 8 && state.Score < 5)
            .ToFacetMinMaxAsync(state => state.Score);

        minMax.Should().NotBeNull();
        minMax!.Minimum.Should().Be(-5);
        minMax.Maximum.Should().Be(100);
        empty.Should().BeNull();

        var constrained = CreateOptions();
        constrained.FacetAggregateWorkLimit = 1;
        var constrainedClient = CreateClient(fixture.Layout, fixture.Partitions, constrained);
        await Awaiting(() => constrainedClient.Query<FacetState>(StateName)
                .ToFacetMinMaxAsync(state => state.Score))
            .Should().ThrowAsync<SearchableStorageQueryLimitExceededException>();
    }

    [Fact]
    public async Task ADataVersionChangeRestartsTheWholeExactAttemptOnce()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRows("winner", 2, included: true);
        fixture.Partitions[0].ChangeOnFirstExpectedCount = true;

        var result = await fixture.Client.Query<FacetState>(StateName)
            .ToFacetValueCountsAsync(
                state => state.Category,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact));

        result.Items.Should().ContainSingle();
        result.Items[0].Count.Should().Be(2);
        fixture.Partitions[0].CandidateRequests.Count.Should().Be(2);
        fixture.Partitions[0].CandidateRequests.All(static request => request.AfterValue is null)
            .Should().BeTrue();
    }

    [Fact]
    public async Task ASecondDataVersionChangeSurfacesThePublicConcurrentChangeFailure()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRows("winner", 2, included: true);
        fixture.Partitions[0].ChangeOnEveryExpectedCount = true;

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<SearchableStorageFacetConcurrentChangeException>();

        fixture.Partitions[0].CandidateRequests.Count.Should().Be(2);
        fixture.Partitions[0].CountRequests.Count.Should().Be(2);
    }

    [Fact]
    public async Task DistinctReturnsAFullPageWithoutProbingTheNextNominatedValue()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: true);
        fixture.Partitions[0].AddRow("b", included: true);
        fixture.Partitions[0].CountHandler = (request, next) =>
            request.Value.Equals(IndexValue.Create("b"))
                ? Task.FromException<PartitionFacetCountSliceResult>(
                    new PartitionQueryBudgetTooSmallException(
                        1,
                        2,
                        PartitionQueryPageStopReason.WorkBudget))
                : Task.FromResult(next());

        var page = await fixture.Client.Query<FacetState>(StateName)
            .ToDistinctFacetValuePageAsync(
                state => state.Category,
                new SearchableStorageFacetPageRequest(1));

        page.Items.Should().Equal("a");
        page.ContinuationToken.Should().NotBeNull();
        fixture.Partitions[0].CountRequests.Select(static request => request.Value.Text)
            .Should().Equal("a");
    }

    [Theory]
    [InlineData("family")]
    [InlineData("fingerprint")]
    [InlineData("protocol")]
    public async Task IncompatibleCandidateMetadataFailsClosed(string mutation)
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: true);
        fixture.Partitions[0].CandidateHandler = (_, next) =>
        {
            var result = next();
            return Task.FromResult(CopyCandidate(
                result,
                responseFamily: mutation == "family"
                    ? PartitionQueryResponseFamily.DistinctFacetValuePage
                    : null,
                requestFingerprint: mutation == "fingerprint" ? new byte[32] : null,
                protocolVersion: mutation == "protocol" ? result.ProtocolVersion + 1 : null));
        };

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*incompatible facet metadata*");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MalformedCandidateWorkNeverLeaksCheckedOverflow(bool overflow)
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: true);
        fixture.Partitions[0].CandidateHandler = (_, next) =>
        {
            var result = next();
            var work = overflow
                ? new PartitionFacetWork
                {
                    ValueSeekCount = long.MaxValue,
                    ValueVisitCount = 1,
                }
                : new PartitionFacetWork { ValueSeekCount = -1 };
            return Task.FromResult(CopyCandidate(result, work: work));
        };

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task MalformedCandidateRawSumNeverLeaksCheckedOverflow()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: true);
        fixture.Partitions[0].AddRow("b", included: true);
        fixture.Partitions[0].CandidateHandler = (_, next) =>
        {
            var result = next();
            return Task.FromResult(CopyCandidate(
                result,
                items:
                [
                    new PartitionFacetCandidate
                    {
                        Value = IndexValue.Create("a"),
                        RawCount = long.MaxValue,
                    },
                    new PartitionFacetCandidate
                    {
                        Value = IndexValue.Create("b"),
                        RawCount = 1,
                    },
                ],
                pageRawCount: long.MaxValue,
                totalRawCount: long.MaxValue));
        };

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(2, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*candidate accounting*");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NullOrWrongDomainCandidateValuesFailClosed(bool nullItem)
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: true);
        fixture.Partitions[0].CandidateHandler = (_, next) =>
        {
            var result = next();
            PartitionFacetCandidate[] items = nullItem
                ? [null!]
                :
                [
                    new PartitionFacetCandidate
                    {
                        Value = IndexValue.Create(42),
                        RawCount = 1,
                    },
                ];
            return Task.FromResult(CopyCandidate(result, items: items));
        };

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task CertifiedOwnerBoundOverflowMapsToThePublicFacetLimit()
    {
        var fixture = CreateFixture(ownerCount: 2);
        fixture.Partitions[0].AddRow("a", included: true);
        fixture.Partitions[1].AddRow("b", included: true);
        foreach (var partition in fixture.Partitions)
        {
            partition.CandidateHandler = (_, next) =>
            {
                var result = next();
                return Task.FromResult(CopyCandidate(
                    result,
                    exhausted: false,
                    frontierValue: result.Items[^1].Value,
                    pageRawCount: 1,
                    totalRawCount: long.MaxValue,
                    stopReason: PartitionQueryPageStopReason.ItemLimit));
            };
        }

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Approximate)))
            .Should().ThrowAsync<SearchableStorageQueryLimitExceededException>()
            .WithMessage("*omitted-count bound*");
    }

    [Fact]
    public async Task MalformedAndNonProgressingCountFrontiersFailClosed()
    {
        var malformed = CreateFixture(ownerCount: 1);
        malformed.Partitions[0].AddRow("a", included: true);
        malformed.Partitions[0].CountHandler = (_, next) =>
        {
            var result = next();
            var oversized = GrainId.Create(
                new GrainType(new byte[GrainIdCanonicalOrder.MaximumTypeBytes + 1]),
                new IdSpan([1]));
            return Task.FromResult(CopyCount(
                result,
                countDelta: 0,
                hasFrontier: true,
                frontier: oversized,
                exhausted: false,
                stopReason: PartitionQueryPageStopReason.WorkBudget));
        };
        await Awaiting(() => malformed.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*malformed count-slice frontier*");

        var nonProgressing = CreateFixture(ownerCount: 1);
        nonProgressing.Partitions[0].AddRow("a", included: true);
        var fixedFrontier = GrainId.Create("facet-frontier", "fixed");
        nonProgressing.Partitions[0].CountHandler = (_, next) =>
        {
            var result = next();
            return Task.FromResult(CopyCount(
                result,
                countDelta: 0,
                hasFrontier: true,
                frontier: fixedFrontier,
                exhausted: false,
                stopReason: PartitionQueryPageStopReason.WorkBudget));
        };
        await Awaiting(() => nonProgressing.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*invalid count-slice frontier*");
        nonProgressing.Partitions[0].CountRequests.Count.Should().Be(2);
    }

    [Fact]
    public async Task CountDeltaCannotExceedItsChargedIncrementWork()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: true);
        fixture.Partitions[0].CountHandler = (_, next) =>
        {
            var result = next();
            return Task.FromResult(CopyCount(
                result,
                countDelta: result.Work.CountIncrementCount + 1));
        };

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*invalid count-slice frontier*");
    }

    [Fact]
    public async Task NullFacetResponseAndImpossibleCountStopReasonFailClosed()
    {
        var nullResponse = CreateFixture(ownerCount: 1);
        nullResponse.Partitions[0].CandidateHandler = (_, _) =>
            Task.FromResult<PartitionFacetCandidatePageResult>(null!);
        await Awaiting(() => nullResponse.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*null response*");

        var impossibleStop = CreateFixture(ownerCount: 1);
        impossibleStop.Partitions[0].AddRow("a", included: true);
        impossibleStop.Partitions[0].CountHandler = (_, next) =>
        {
            var result = next();
            return Task.FromResult(CopyCount(
                result,
                countDelta: 0,
                hasFrontier: true,
                frontier: GrainId.Create("facet-frontier", "item-limit"),
                exhausted: false,
                stopReason: PartitionQueryPageStopReason.ItemLimit));
        };
        await Awaiting(() => impossibleStop.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*invalid count-slice frontier*");
    }

    [Fact]
    public async Task UnsupportedStoredTextFailsWholePublicFacetOperationsWithoutPartialResults()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow(
            new string('x', IndexValueCanonicalEncoding.MaximumTextBytes + 1),
            included: true);

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToDistinctFacetValuePageAsync(
                    state => state.Category,
                    new SearchableStorageFacetPageRequest(1)))
            .Should().ThrowAsync<SearchableStorageQueryLimitExceededException>();
        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<SearchableStorageQueryLimitExceededException>();
        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetMinMaxAsync(state => state.Category))
            .Should().ThrowAsync<SearchableStorageQueryLimitExceededException>();
    }

    [Fact]
    public async Task FanoutObservesAllOwnersAndTerminalFailurePrecedesDataChangeAndRouteMismatch()
    {
        var fixture = CreateFixture(ownerCount: 3);
        var terminal = new InvalidOperationException("terminal owner failure");
        fixture.Partitions[0].CandidateHandler = (_, _) =>
            throw new StorageFacetDataChangedException(1, 2);
        fixture.Partitions[1].CandidateHandler = (_, _) =>
            Task.FromException<PartitionFacetCandidatePageResult>(
                new StorageRouteMismatchException(1, 2, 1));
        fixture.Partitions[2].CandidateHandler = (_, _) =>
            Task.FromException<PartitionFacetCandidatePageResult>(terminal);

        var failure = await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<InvalidOperationException>();

        failure.Which.Should().BeSameAs(terminal);
        fixture.Partitions.Should().OnlyContain(static partition => partition.CandidateRequests.Count == 1);
    }

    [Fact]
    public async Task ChildCancellationPrecedesRetryableFacetFailuresWhenCallerWasNotCanceled()
    {
        var fixture = CreateFixture(ownerCount: 2);
        using var childCancellation = new CancellationTokenSource();
        childCancellation.Cancel();
        fixture.Partitions[0].CandidateHandler = (_, _) =>
            Task.FromException<PartitionFacetCandidatePageResult>(
                new StorageFacetDataChangedException(1, 2));
        fixture.Partitions[1].CandidateHandler = (_, _) =>
            Task.FromCanceled<PartitionFacetCandidatePageResult>(childCancellation.Token);

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<TaskCanceledException>();
    }

    [Fact]
    public async Task CallerCancellationDelegatesTheWholeFacetFanoutForLateObservation()
    {
        var layout = CreateLayout(epoch: 1, ownerCount: 2);
        var partitions = Enumerable.Range(0, 2)
            .Select(owner => new FacetPartition(owner, layout))
            .ToArray();
        var completions = partitions.Select(_ =>
            new TaskCompletionSource<PartitionFacetCandidatePageResult>(
                TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        for (var index = 0; index < partitions.Length; index++)
        {
            var captured = index;
            partitions[index].CandidateHandler = (_, _) => completions[captured].Task;
        }

        Task? observedFanout = null;
        var observationCompleted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Action<Task> observer = task =>
        {
            observedFanout = task;
            _ = ObserveForTestAsync(task, observationCompleted);
        };
        var client = CreateClient(layout, partitions, CreateOptions(), observer);
        using var cancellation = new CancellationTokenSource();
        var execution = client.Query<FacetState>(StateName)
            .ToFacetValueCountsAsync(
                state => state.Category,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact),
                cancellation.Token);
        await WaitUntilAsync(() => partitions.All(
            static partition => partition.CandidateRequests.Count == 1));

        await cancellation.CancelAsync();
        await Awaiting(async () => await execution)
            .Should().ThrowAsync<OperationCanceledException>();
        observedFanout.Should().NotBeNull();
        observedFanout!.IsCompleted.Should().BeFalse();

        completions[0].SetException(new InvalidOperationException("late zero"));
        completions[1].SetException(new InvalidOperationException("late one"));
        await observationCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        observedFanout.IsFaulted.Should().BeTrue();
    }

    [Fact]
    public async Task DataChangeThenRouteRefreshDoesNotGrantASecondDataRestart()
    {
        var fixture = CreateFixture(ownerCount: 1);
        fixture.Partitions[0].AddRow("a", included: true);
        var candidateCall = 0;
        fixture.Partitions[0].CandidateHandler = (request, next) =>
            Interlocked.Increment(ref candidateCall) == 2
                ? Task.FromException<PartitionFacetCandidatePageResult>(
                    new StorageRouteMismatchException(request.Epoch, request.Epoch + 1, 0))
                : Task.FromResult(next());
        fixture.Partitions[0].CountHandler = (_, _) =>
            Task.FromException<PartitionFacetCountSliceResult>(
                new StorageFacetDataChangedException(1, 2));

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<SearchableStorageFacetConcurrentChangeException>();

        candidateCall.Should().Be(3);
        fixture.Partitions[0].CountRequests.Count.Should().Be(2);
    }

    [Fact]
    public async Task CallerCancellationAtFinalFacetCompletionCannotReturnSuccess()
    {
        var fixture = CreateFixture(ownerCount: 1);
        using var cancellation = new CancellationTokenSource();
        fixture.Partitions[0].CandidateHandler = (_, next) =>
        {
            var result = next();
            cancellation.Cancel();
            return Task.FromResult(result);
        };

        await Awaiting(() => fixture.Client.Query<FacetState>(StateName)
                .ToFacetValueCountsAsync(
                    state => state.Category,
                    new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact),
                    cancellation.Token))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ConvertedOrUnindexedFacetSelectorsFailBeforePartitionWork()
    {
        var fixture = CreateFixture(ownerCount: 1);
        Expression<Func<FacetState, object>> boxed = state => state.Score;
        Expression<Func<FacetState, long>> widened = state => state.Score;
        var query = fixture.Client.Query<FacetState>(StateName);

        await Awaiting(() => query.ToFacetValueCountsAsync(
                boxed,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<ArgumentException>().WithMessage("*exactly match*");
        await Awaiting(() => query.ToFacetValueCountsAsync(
                widened,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<ArgumentException>().WithMessage("*exactly match*");
        await Awaiting(() => query.ToFacetValueCountsAsync(
                state => state.Description,
                new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact)))
            .Should().ThrowAsync<ArgumentException>().WithMessage("*not marked*");
        fixture.Partitions[0].CandidateRequests.Should().BeEmpty();
    }

    [Fact]
    public async Task KnownEmptyFacetFormsReturnWithoutLayoutOrPartitionFanout()
    {
        var fixture = CreateFixture(ownerCount: 1);
        var query = fixture.Client.Query<FacetState>(StateName)
            .Where(state => state.Score > 8 && state.Score < 5);

        var distinct = await query.ToDistinctFacetValuePageAsync(
            state => state.Category,
            new SearchableStorageFacetPageRequest(10));
        var counts = await query.ToFacetValueCountsAsync(
            state => state.Category,
            new SearchableStorageFacetRequest(10, SearchableStorageFacetAccuracy.Exact));
        var minMax = await query.ToFacetMinMaxAsync(state => state.Score);

        distinct.Items.Should().BeEmpty();
        distinct.ContinuationToken.Should().BeNull();
        counts.Items.Should().BeEmpty();
        counts.IsExact.Should().BeTrue();
        counts.MaximumOmittedCount.Should().Be(0);
        minMax.Should().BeNull();
        fixture.Partitions[0].TotalFacetCalls.Should().Be(0);
    }

    private static PartitionFacetCandidatePageResult CopyCandidate(
        PartitionFacetCandidatePageResult source,
        PartitionFacetCandidate[]? items = null,
        IndexValue? frontierValue = null,
        bool? exhausted = null,
        long? pageRawCount = null,
        long? totalRawCount = null,
        PartitionQueryPageStopReason? stopReason = null,
        PartitionFacetWork? work = null,
        int? protocolVersion = null,
        PartitionQueryResponseFamily? responseFamily = null,
        byte[]? requestFingerprint = null)
    {
        return new PartitionFacetCandidatePageResult
        {
            Items = items ?? source.Items,
            FrontierValue = frontierValue ?? source.FrontierValue,
            Exhausted = exhausted ?? source.Exhausted,
            PageRawCount = pageRawCount ?? source.PageRawCount,
            TotalRawCount = totalRawCount ?? source.TotalRawCount,
            StopReason = stopReason ?? source.StopReason,
            Work = work ?? source.Work,
            ItemByteCount = source.ItemByteCount,
            ProtocolVersion = protocolVersion ?? source.ProtocolVersion,
            OrderingVersion = source.OrderingVersion,
            WorkPolicyVersion = source.WorkPolicyVersion,
            ResponseFamily = responseFamily ?? source.ResponseFamily,
            Epoch = source.Epoch,
            RequestFingerprint = requestFingerprint ?? source.RequestFingerprint,
            LayoutFormatVersion = source.LayoutFormatVersion,
            LayoutFingerprint = source.LayoutFingerprint,
            DataVersion = source.DataVersion,
        };
    }

    private static PartitionFacetCountSliceResult CopyCount(
        PartitionFacetCountSliceResult source,
        long? countDelta = null,
        bool? hasFrontier = null,
        GrainId? frontier = null,
        bool? exhausted = null,
        PartitionQueryPageStopReason? stopReason = null,
        PartitionFacetWork? work = null)
    {
        return new PartitionFacetCountSliceResult
        {
            CountDelta = countDelta ?? source.CountDelta,
            HasFrontier = hasFrontier ?? source.HasFrontier,
            Frontier = frontier ?? source.Frontier,
            Exhausted = exhausted ?? source.Exhausted,
            StopReason = stopReason ?? source.StopReason,
            Work = work ?? source.Work,
            ProtocolVersion = source.ProtocolVersion,
            OrderingVersion = source.OrderingVersion,
            WorkPolicyVersion = source.WorkPolicyVersion,
            ResponseFamily = source.ResponseFamily,
            Epoch = source.Epoch,
            RequestFingerprint = source.RequestFingerprint,
            LayoutFormatVersion = source.LayoutFormatVersion,
            LayoutFingerprint = source.LayoutFingerprint,
            DataVersion = source.DataVersion,
        };
    }

    private static Fixture CreateFixture(int ownerCount)
    {
        var layout = CreateLayout(epoch: 1, ownerCount);
        var partitions = Enumerable.Range(0, ownerCount)
            .Select(owner => new FacetPartition(owner, layout))
            .ToArray();
        return new Fixture(layout, partitions, CreateClient(layout, partitions, CreateOptions()));
    }

    private static Func<Task<T>> Awaiting<T>(Func<Task<T>> action) => action;

    private static async Task ObserveForTestAsync(Task task, TaskCompletionSource completion)
    {
        await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        completion.TrySetResult();
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    private static SearchableStorageClient CreateClient(
        StorageLayoutSnapshot layout,
        IReadOnlyList<FacetPartition> partitions,
        SearchableStorageQueryOptions options,
        Action<Task>? detachedFanoutObserver = null)
    {
        return new SearchableStorageClient(
            ProviderName,
            new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(layout)),
            owner => partitions[owner],
            options,
            detachedFanoutObserver: detachedFanoutObserver);
    }

    private static SearchableStorageQueryOptions CreateOptions()
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "facet-tests",
            Enumerable.Range(1, 32).Select(static value => (byte)value).ToArray());
        return options;
    }

    private static StorageLayoutSnapshot CreateLayout(long epoch, int ownerCount)
    {
        const int slotsPerOwner = 8;
        var assignments = Enumerable.Range(0, ownerCount * slotsPerOwner)
            .Select(index => index % ownerCount)
            .ToArray();
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = ProviderName,
            PartitionCount = ownerCount,
            VirtualSlotCount = assignments.Length,
            SlotAssignments = assignments,
            Epoch = epoch,
        });
    }

    private sealed record Fixture(
        StorageLayoutSnapshot Layout,
        FacetPartition[] Partitions,
        SearchableStorageClient Client);

    private sealed class FacetState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Category { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Score { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public bool Included { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public int? Optional { get; init; }

        public string Description { get; init; } = string.Empty;
    }

    private sealed class FacetPartition : IStoragePartitionGrain
    {
        private static readonly SelectedIndex CategoryIndex =
            IndexMetadataProvider.GetSelectedIndex<FacetState, string>(StateName, state => state.Category);
        private static readonly SelectedIndex ScoreIndex =
            IndexMetadataProvider.GetSelectedIndex<FacetState, int>(StateName, state => state.Score);
        private static readonly SelectedIndex IncludedIndex =
            IndexMetadataProvider.GetSelectedIndex<FacetState, bool>(StateName, state => state.Included);

        private readonly int _owner;
        private readonly StorageLayoutSnapshot _layout;
        private readonly StoragePartitionView _view = new(new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
        private int _nextRecord;
        private long _dataVersion = 1;

        public FacetPartition(int owner, StorageLayoutSnapshot layout)
        {
            _owner = owner;
            _layout = layout;
        }

        public ConcurrentQueue<RoutedPartitionDistinctFacetPageRequest> DistinctRequests { get; } = new();
        public ConcurrentQueue<RoutedPartitionFacetCandidatePageRequest> CandidateRequests { get; } = new();
        public ConcurrentQueue<RoutedPartitionFacetCountSliceRequest> CountRequests { get; } = new();
        public Func<
            RoutedPartitionDistinctFacetPageRequest,
            Func<PartitionDistinctFacetPageResult>,
            Task<PartitionDistinctFacetPageResult>>? DistinctHandler { get; set; }
        public Func<
            RoutedPartitionFacetCandidatePageRequest,
            Func<PartitionFacetCandidatePageResult>,
            Task<PartitionFacetCandidatePageResult>>? CandidateHandler { get; set; }
        public Func<
            RoutedPartitionFacetCountSliceRequest,
            Func<PartitionFacetCountSliceResult>,
            Task<PartitionFacetCountSliceResult>>? CountHandler { get; set; }
        public bool ChangeOnFirstExpectedCount { get; set; }
        public bool ChangeOnEveryExpectedCount { get; set; }
        public int TotalFacetCalls => DistinctRequests.Count + CandidateRequests.Count + CountRequests.Count;

        public void AddRows(string category, int count, bool included)
        {
            for (var index = 0; index < count; index++)
            {
                AddRow(category, included, score: _nextRecord);
            }
        }

        public (string RecordKey, StoredRecord Record) AddRow(
            string category,
            bool included,
            int score = 0)
        {
            var sequence = _nextRecord++;
            var grainId = CreateOwnedGrainId(_owner, sequence);
            var recordKey = $"{StateName}/record-{_owner}-{sequence}";
            var record = new StoredRecord
            {
                GrainId = grainId,
                Payload = [],
                ETag = "1",
                IndexEntries =
                [
                    new IndexEntry
                    {
                        Scope = CategoryIndex.Scope,
                        Kind = CategoryIndex.Kind,
                        Value = IndexValue.Create(category),
                    },
                    new IndexEntry
                    {
                        Scope = ScoreIndex.Scope,
                        Kind = ScoreIndex.Kind,
                        Value = IndexValue.Create(score),
                    },
                    new IndexEntry
                    {
                        Scope = IncludedIndex.Scope,
                        Kind = IncludedIndex.Kind,
                        Value = IndexValue.Create(included),
                    },
                ],
            };
            _view.ApplyUpsert(recordKey, record);
            _dataVersion++;
            return (recordKey, record);
        }

        public void AddCopiedRecord(string sourceKey, StoredRecord record)
        {
            _view.ApplyUpsert($"{sourceKey}-copy-on-{_owner}", record);
            _dataVersion++;
        }

        public Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
            RoutedPartitionDistinctFacetPageRequest request)
        {
            DistinctRequests.Enqueue(request);
            return DistinctHandler is null
                ? Task.FromResult(Evaluate())
                : DistinctHandler(request, Evaluate);

            PartitionDistinctFacetPageResult Evaluate()
            {
                ValidateVersion(request.HasExpectedDataVersion, request.ExpectedDataVersion);
                var result = StoragePartitionFacetEvaluator.EvaluateDistinctPageValidated(
                    request,
                    _view,
                    _layout,
                    request.RequestFingerprint,
                    StorageLayoutFingerprint.Compute(_layout));
                result.DataVersion = _dataVersion;
                return result;
            }
        }

        public Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
            RoutedPartitionFacetCandidatePageRequest request)
        {
            CandidateRequests.Enqueue(request);
            return CandidateHandler is null
                ? Task.FromResult(Evaluate())
                : CandidateHandler(request, Evaluate);

            PartitionFacetCandidatePageResult Evaluate()
            {
                ValidateVersion(request.HasExpectedDataVersion, request.ExpectedDataVersion);
                var result = StoragePartitionFacetEvaluator.EvaluateCandidatePageValidated(
                    request,
                    _view,
                    _layout,
                    request.RequestFingerprint,
                    StorageLayoutFingerprint.Compute(_layout));
                result.DataVersion = _dataVersion;
                return result;
            }
        }

        public Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
            RoutedPartitionFacetCountSliceRequest request)
        {
            CountRequests.Enqueue(request);
            return CountHandler is null
                ? Task.FromResult(Evaluate())
                : CountHandler(request, Evaluate);

            PartitionFacetCountSliceResult Evaluate()
            {
                if (request.HasExpectedDataVersion
                    && (ChangeOnEveryExpectedCount || ChangeOnFirstExpectedCount))
                {
                    ChangeOnFirstExpectedCount = false;
                    _dataVersion++;
                }

                ValidateVersion(request.HasExpectedDataVersion, request.ExpectedDataVersion);
                var result = StoragePartitionFacetEvaluator.EvaluateCountSliceValidated(
                    request,
                    _view,
                    _layout,
                    _owner,
                    request.RequestFingerprint,
                    StorageLayoutFingerprint.Compute(_layout));
                result.DataVersion = _dataVersion;
                return result;
            }
        }

        private void ValidateVersion(bool hasExpected, long expected)
        {
            if (hasExpected && expected != _dataVersion)
            {
                throw new StorageFacetDataChangedException(expected, _dataVersion);
            }
        }

        private GrainId CreateOwnedGrainId(int owner, int sequence)
        {
            for (var attempt = 0; ; attempt++)
            {
                var grainId = GrainId.Create("facet", $"{owner}-{sequence}-{attempt}");
                var slot = StorageLayout.GetSlot(grainId, _layout.VirtualSlotCount);
                if (_layout.GetOwner(slot) == owner)
                {
                    return grainId;
                }
            }
        }

        public Task<StorageReadResult> ReadAsync(string recordKey) => throw new NotSupportedException();
        public Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request) => throw new NotSupportedException();
        public Task<string> WriteAsync(StorageWriteRequest request) => throw new NotSupportedException();
        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request) => throw new NotSupportedException();
        public Task ClearAsync(StorageClearRequest request) => throw new NotSupportedException();
        public Task ClearRoutedAsync(RoutedStorageClearRequest request) => throw new NotSupportedException();
        public Task<GrainId[]> FindAsync(ExactIndexQuery query) => throw new NotSupportedException();
        public Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query) => throw new NotSupportedException();
        public Task<GrainId[]> RangeAsync(RangeIndexQuery query) => throw new NotSupportedException();
        public Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query) => throw new NotSupportedException();
        public Task<GrainId[]> QueryAsync(PartitionQueryPlan query) => throw new NotSupportedException();
        public Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query) => throw new NotSupportedException();
        public Task<PartitionQueryPageResult> QueryPageRoutedAsync(RoutedPartitionQueryPageRequest request) => throw new NotSupportedException();
        public Task CompactAsync() => throw new NotSupportedException();
        public Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync() => throw new NotSupportedException();
    }
}
