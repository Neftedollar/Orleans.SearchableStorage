using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryApiContractTests
{
    [Fact]
    public void ExistingDirectClientImplementationsDoNotNeedTheQuerySurface()
    {
        ISearchableStorageClient client = new DirectClientImplementation();

        client.Should().NotBeAssignableTo<ISearchableStorageQueryClient>();
    }

    [Fact]
    public async Task ExternalAsyncProviderCanUseThePublicTerminalOperation()
    {
        var provider = new ExternalQueryProvider<QueryState>();
        var query = new ExternalQuery<QueryState>(provider)
            .Where(state => state.Value == 7);
        using var cancellation = new CancellationTokenSource();

        var results = await query.ToGrainIdsAsync(cancellation.Token);

        results.Should().BeEmpty();
        provider.Expression.Should().BeSameAs(query.Expression);
        provider.CancellationToken.Should().Be(cancellation.Token);
    }

    [Fact]
    public async Task ExistingExternalProviderNeedsNoFacetInterfaceAndFacetTerminalFailsClearly()
    {
        var provider = new ExternalQueryProvider<QueryState>();
        var query = new ExternalQuery<QueryState>(provider);

        Func<Task> execute = async () => await query.ToFacetValueCountsAsync(
            state => state.Value,
            new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact));

        await execute.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*facet-enabled*");
    }

    [Fact]
    public async Task OptInFacetProviderReceivesExactExpressionSelectorRequestAndCancellation()
    {
        var provider = new ExternalFacetQueryProvider<QueryState>();
        var query = new ExternalQuery<QueryState>(provider)
            .Where(state => state.Value == 7);
        Expression<Func<QueryState, int>> selector = state => state.Value;
        var request = new SearchableStorageFacetRequest(
            3,
            SearchableStorageFacetAccuracy.Approximate);
        using var cancellation = new CancellationTokenSource();

        var result = await query.ToFacetValueCountsAsync(
            selector,
            request,
            cancellation.Token);

        result.Items.Should().BeEmpty();
        provider.FacetExpression.Should().BeSameAs(query.Expression);
        provider.FacetSelector.Should().BeSameAs(selector);
        provider.FacetRequest.Should().BeSameAs(request);
        provider.FacetCancellationToken.Should().Be(cancellation.Token);
    }

    [Fact]
    public void FacetDtosValidateDomainsAndDefensivelyCopyCollections()
    {
        Action zeroTopN = () => _ = new SearchableStorageFacetRequest(
            0,
            SearchableStorageFacetAccuracy.Exact);
        Action unknownAccuracy = () => _ = new SearchableStorageFacetRequest(
            1,
            (SearchableStorageFacetAccuracy)42);
        Action zeroPage = () => _ = new SearchableStorageFacetPageRequest(0);
        Action blankToken = () => _ = new SearchableStorageFacetPageRequest(1, " ");
        Action zeroCount = () => _ = new SearchableStorageFacetValueCount<int>(1, 0);
        Action nullValue = () => _ = new SearchableStorageFacetValueCount<string>(null!, 1);
        Action nullResultItem = () => _ = new SearchableStorageFacetResult<int>(
            [null!],
            isExact: false,
            maximumOmittedCount: 0);
        Action negativeBound = () => _ = new SearchableStorageFacetResult<int>(
            [],
            isExact: false,
            maximumOmittedCount: -1);

        zeroTopN.Should().Throw<ArgumentOutOfRangeException>();
        unknownAccuracy.Should().Throw<ArgumentOutOfRangeException>();
        zeroPage.Should().Throw<ArgumentOutOfRangeException>();
        blankToken.Should().Throw<ArgumentException>();
        zeroCount.Should().Throw<ArgumentOutOfRangeException>();
        nullValue.Should().Throw<ArgumentNullException>();
        nullResultItem.Should().Throw<ArgumentException>();
        negativeBound.Should().Throw<ArgumentOutOfRangeException>();

        var counts = new List<SearchableStorageFacetValueCount<int>>
        {
            new(7, 2),
        };
        var result = new SearchableStorageFacetResult<int>(counts, true, 0);
        counts.Clear();
        result.Items.Should().ContainSingle().Which.Value.Should().Be(7);

        var values = new List<string> { "a" };
        var page = new SearchableStorageDistinctFacetPage<string>(values, continuationToken: null);
        values[0] = "changed";
        page.Items.Should().Equal("a");
    }

    [Fact]
    public void BoundedRangeWireMessageKeepsRequiredNonNullableBounds()
    {
        var nullability = new NullabilityInfoContext();
        var scope = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.Scope))!;
        var lowerBound = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.LowerBound))!;
        var upperBound = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.UpperBound))!;
        var includeLower = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.IncludeLowerBound))!;
        var includeUpper = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.IncludeUpperBound))!;

        scope.GetCustomAttribute<RequiredMemberAttribute>().Should().NotBeNull();
        lowerBound.GetCustomAttribute<RequiredMemberAttribute>().Should().NotBeNull();
        upperBound.GetCustomAttribute<RequiredMemberAttribute>().Should().NotBeNull();
        nullability.Create(scope).ReadState.Should().Be(NullabilityState.NotNull);
        nullability.Create(lowerBound).ReadState.Should().Be(NullabilityState.NotNull);
        nullability.Create(upperBound).ReadState.Should().Be(NullabilityState.NotNull);
        scope.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(0);
        lowerBound.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(1);
        upperBound.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(2);
        includeLower.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(3);
        includeUpper.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(4);
    }

    [Fact]
    public void PartitionQueryWireMessageKeepsStableFieldsAndNullableOpenBounds()
    {
        var expectedFields = new Dictionary<string, uint>
        {
            [nameof(PartitionQueryPlan.Operation)] = 0,
            [nameof(PartitionQueryPlan.Scope)] = 1,
            [nameof(PartitionQueryPlan.IndexKind)] = 2,
            [nameof(PartitionQueryPlan.Value)] = 3,
            [nameof(PartitionQueryPlan.LowerBound)] = 4,
            [nameof(PartitionQueryPlan.UpperBound)] = 5,
            [nameof(PartitionQueryPlan.IncludeLowerBound)] = 6,
            [nameof(PartitionQueryPlan.IncludeUpperBound)] = 7,
            [nameof(PartitionQueryPlan.Left)] = 8,
            [nameof(PartitionQueryPlan.Right)] = 9,
        };

        foreach (var field in expectedFields)
        {
            typeof(PartitionQueryPlan).GetProperty(field.Key)!
                .GetCustomAttribute<IdAttribute>()!.Id.Should().Be(field.Value);
        }

        var nullability = new NullabilityInfoContext();
        var lowerBound = typeof(PartitionQueryPlan).GetProperty(nameof(PartitionQueryPlan.LowerBound))!;
        var upperBound = typeof(PartitionQueryPlan).GetProperty(nameof(PartitionQueryPlan.UpperBound))!;
        nullability.Create(lowerBound).ReadState.Should().Be(NullabilityState.Nullable);
        nullability.Create(upperBound).ReadState.Should().Be(NullabilityState.Nullable);
        ((int)PartitionQueryOperation.Empty).Should().Be(0);
        ((int)PartitionQueryOperation.Exact).Should().Be(1);
        ((int)PartitionQueryOperation.Range).Should().Be(2);
        ((int)PartitionQueryOperation.And).Should().Be(3);
        ((int)PartitionQueryOperation.Or).Should().Be(4);
        ((int)PartitionQueryOperation.All).Should().Be(5);
    }

    [Fact]
    public void VirtualRoutingWireAndLayoutMessagesKeepStableFieldIds()
    {
        typeof(IStorageLayoutGrain)
            .GetMethod(nameof(IStorageLayoutGrain.GetCurrentLayoutAsync))!
            .GetCustomAttribute<Orleans.Concurrency.AlwaysInterleaveAttribute>()
            .Should().NotBeNull("partition ownership validation must be able to read the layout during orchestration");

        typeof(StorageLayoutState).GetProperty(
                "PartitionCount",
                BindingFlags.Instance | BindingFlags.Public)
            .Should().NotBeNull("the JSON property name is part of persisted version-3 state");

        AssertFieldIds<RoutedStorageReadRequest>(
            (nameof(RoutedStorageReadRequest.RecordKey), 0),
            (nameof(RoutedStorageReadRequest.Slot), 1),
            (nameof(RoutedStorageReadRequest.Epoch), 2),
            (nameof(RoutedStorageReadRequest.GrainId), 3));
        AssertFieldIds<RoutedStorageWriteRequest>(
            (nameof(RoutedStorageWriteRequest.Request), 0),
            (nameof(RoutedStorageWriteRequest.Slot), 1),
            (nameof(RoutedStorageWriteRequest.Epoch), 2));
        AssertFieldIds<RoutedStorageClearRequest>(
            (nameof(RoutedStorageClearRequest.Request), 0),
            (nameof(RoutedStorageClearRequest.Slot), 1),
            (nameof(RoutedStorageClearRequest.Epoch), 2),
            (nameof(RoutedStorageClearRequest.GrainId), 3));
        AssertFieldIds<RoutedExactIndexQuery>(
            (nameof(RoutedExactIndexQuery.Query), 0),
            (nameof(RoutedExactIndexQuery.Epoch), 1),
            (nameof(RoutedExactIndexQuery.StateName), 2),
            (nameof(RoutedExactIndexQuery.IndexSchemaFingerprint), 3),
            (nameof(RoutedExactIndexQuery.IndexSchemaProtocolVersion), 4));
        AssertFieldIds<RoutedRangeIndexQuery>(
            (nameof(RoutedRangeIndexQuery.Query), 0),
            (nameof(RoutedRangeIndexQuery.Epoch), 1),
            (nameof(RoutedRangeIndexQuery.StateName), 2),
            (nameof(RoutedRangeIndexQuery.IndexSchemaFingerprint), 3),
            (nameof(RoutedRangeIndexQuery.IndexSchemaProtocolVersion), 4));
        AssertFieldIds<RoutedPartitionQuery>(
            (nameof(RoutedPartitionQuery.Query), 0),
            (nameof(RoutedPartitionQuery.Epoch), 1),
            (nameof(RoutedPartitionQuery.StateName), 2),
            (nameof(RoutedPartitionQuery.IndexSchemaFingerprint), 3),
            (nameof(RoutedPartitionQuery.IndexSchemaProtocolVersion), 4));
        AssertFieldIds<RoutedPartitionQueryPageRequest>(
            (nameof(RoutedPartitionQueryPageRequest.Query), 0),
            (nameof(RoutedPartitionQueryPageRequest.Epoch), 1),
            (nameof(RoutedPartitionQueryPageRequest.HasAfter), 2),
            (nameof(RoutedPartitionQueryPageRequest.After), 3),
            (nameof(RoutedPartitionQueryPageRequest.WorkBudget), 4),
            (nameof(RoutedPartitionQueryPageRequest.ItemLimit), 5),
            (nameof(RoutedPartitionQueryPageRequest.ByteLimit), 6),
            (nameof(RoutedPartitionQueryPageRequest.ProtocolVersion), 7),
            (nameof(RoutedPartitionQueryPageRequest.OrderingVersion), 8),
            (nameof(RoutedPartitionQueryPageRequest.WorkPolicyVersion), 9),
            (nameof(RoutedPartitionQueryPageRequest.ResponseFamily), 10),
            (nameof(RoutedPartitionQueryPageRequest.QueryFingerprint), 11),
            (nameof(RoutedPartitionQueryPageRequest.LayoutFormatVersion), 12),
            (nameof(RoutedPartitionQueryPageRequest.LayoutFingerprint), 13),
            (nameof(RoutedPartitionQueryPageRequest.StateName), 14),
            (nameof(RoutedPartitionQueryPageRequest.IndexSchemaFingerprint), 15),
            (nameof(RoutedPartitionQueryPageRequest.IndexSchemaProtocolVersion), 16));
        AssertFieldIds<PartitionQueryPageResult>(
            (nameof(PartitionQueryPageResult.Items), 0),
            (nameof(PartitionQueryPageResult.HasFrontier), 1),
            (nameof(PartitionQueryPageResult.Frontier), 2),
            (nameof(PartitionQueryPageResult.Exhausted), 3),
            (nameof(PartitionQueryPageResult.StopReason), 4),
            (nameof(PartitionQueryPageResult.Work), 5),
            (nameof(PartitionQueryPageResult.ItemByteCount), 6),
            (nameof(PartitionQueryPageResult.ProtocolVersion), 7),
            (nameof(PartitionQueryPageResult.OrderingVersion), 8),
            (nameof(PartitionQueryPageResult.WorkPolicyVersion), 9),
            (nameof(PartitionQueryPageResult.ResponseFamily), 10),
            (nameof(PartitionQueryPageResult.Epoch), 11),
            (nameof(PartitionQueryPageResult.QueryFingerprint), 12),
            (nameof(PartitionQueryPageResult.LayoutFormatVersion), 13),
            (nameof(PartitionQueryPageResult.LayoutFingerprint), 14));
        AssertFieldIds<PartitionQueryPageWork>(
            (nameof(PartitionQueryPageWork.OrderedCandidateVisitCount), 0),
            (nameof(PartitionQueryPageWork.RecordProbeCount), 1),
            (nameof(PartitionQueryPageWork.PredicateNodeProbeCount), 2),
            (nameof(PartitionQueryPageWork.IndexEntryProbeCount), 3),
            (nameof(PartitionQueryPageWork.OwnershipProbeCount), 4),
            (nameof(PartitionQueryPageWork.PostingSeekCount), 5),
            (nameof(PartitionQueryPageWork.RangeBucketVisitCount), 6),
            (nameof(PartitionQueryPageWork.ResultMaterializationCount), 7),
            (nameof(PartitionQueryPageWork.RangeMergeOperationCount), 8));
        AssertFieldIds<PartitionQueryBudgetTooSmallException>(
            (nameof(PartitionQueryBudgetTooSmallException.RequestedLimit), 0),
            (nameof(PartitionQueryBudgetTooSmallException.MinimumRequired), 1),
            (nameof(PartitionQueryBudgetTooSmallException.Reason), 2));
        AssertFieldIds<StorageRouteMismatchException>(
            (nameof(StorageRouteMismatchException.ExpectedEpoch), 0),
            (nameof(StorageRouteMismatchException.CurrentEpoch), 1),
            (nameof(StorageRouteMismatchException.RequestedPartition), 2),
            (nameof(StorageRouteMismatchException.Slot), 3),
            (nameof(StorageRouteMismatchException.CurrentOwner), 4));
        AssertFieldIds<RoutedPartitionDistinctFacetPageRequest>(
            (nameof(RoutedPartitionDistinctFacetPageRequest.Query), 0),
            (nameof(RoutedPartitionDistinctFacetPageRequest.FacetScope), 1),
            (nameof(RoutedPartitionDistinctFacetPageRequest.FacetKind), 2),
            (nameof(RoutedPartitionDistinctFacetPageRequest.Epoch), 3),
            (nameof(RoutedPartitionDistinctFacetPageRequest.After), 4),
            (nameof(RoutedPartitionDistinctFacetPageRequest.WorkBudget), 5),
            (nameof(RoutedPartitionDistinctFacetPageRequest.ItemLimit), 6),
            (nameof(RoutedPartitionDistinctFacetPageRequest.ByteLimit), 7),
            (nameof(RoutedPartitionDistinctFacetPageRequest.ProtocolVersion), 8),
            (nameof(RoutedPartitionDistinctFacetPageRequest.OrderingVersion), 9),
            (nameof(RoutedPartitionDistinctFacetPageRequest.WorkPolicyVersion), 10),
            (nameof(RoutedPartitionDistinctFacetPageRequest.ResponseFamily), 11),
            (nameof(RoutedPartitionDistinctFacetPageRequest.RequestFingerprint), 12),
            (nameof(RoutedPartitionDistinctFacetPageRequest.LayoutFormatVersion), 13),
            (nameof(RoutedPartitionDistinctFacetPageRequest.LayoutFingerprint), 14),
            (nameof(RoutedPartitionDistinctFacetPageRequest.StateName), 15),
            (nameof(RoutedPartitionDistinctFacetPageRequest.HasExpectedDataVersion), 16),
            (nameof(RoutedPartitionDistinctFacetPageRequest.ExpectedDataVersion), 17),
            (nameof(RoutedPartitionDistinctFacetPageRequest.IndexSchemaFingerprint), 18),
            (nameof(RoutedPartitionDistinctFacetPageRequest.IndexSchemaProtocolVersion), 19));
        AssertFieldIds<PartitionDistinctFacetPageResult>(
            (nameof(PartitionDistinctFacetPageResult.Items), 0),
            (nameof(PartitionDistinctFacetPageResult.Frontier), 1),
            (nameof(PartitionDistinctFacetPageResult.Exhausted), 2),
            (nameof(PartitionDistinctFacetPageResult.StopReason), 3),
            (nameof(PartitionDistinctFacetPageResult.Work), 4),
            (nameof(PartitionDistinctFacetPageResult.ItemByteCount), 5),
            (nameof(PartitionDistinctFacetPageResult.ProtocolVersion), 6),
            (nameof(PartitionDistinctFacetPageResult.OrderingVersion), 7),
            (nameof(PartitionDistinctFacetPageResult.WorkPolicyVersion), 8),
            (nameof(PartitionDistinctFacetPageResult.ResponseFamily), 9),
            (nameof(PartitionDistinctFacetPageResult.Epoch), 10),
            (nameof(PartitionDistinctFacetPageResult.RequestFingerprint), 11),
            (nameof(PartitionDistinctFacetPageResult.LayoutFormatVersion), 12),
            (nameof(PartitionDistinctFacetPageResult.LayoutFingerprint), 13),
            (nameof(PartitionDistinctFacetPageResult.DataVersion), 14));
        AssertFieldIds<RoutedPartitionFacetCandidatePageRequest>(
            (nameof(RoutedPartitionFacetCandidatePageRequest.Query), 0),
            (nameof(RoutedPartitionFacetCandidatePageRequest.FacetScope), 1),
            (nameof(RoutedPartitionFacetCandidatePageRequest.FacetKind), 2),
            (nameof(RoutedPartitionFacetCandidatePageRequest.Epoch), 3),
            (nameof(RoutedPartitionFacetCandidatePageRequest.AfterValue), 4),
            (nameof(RoutedPartitionFacetCandidatePageRequest.WorkBudget), 5),
            (nameof(RoutedPartitionFacetCandidatePageRequest.ItemLimit), 6),
            (nameof(RoutedPartitionFacetCandidatePageRequest.ByteLimit), 7),
            (nameof(RoutedPartitionFacetCandidatePageRequest.ProtocolVersion), 8),
            (nameof(RoutedPartitionFacetCandidatePageRequest.OrderingVersion), 9),
            (nameof(RoutedPartitionFacetCandidatePageRequest.WorkPolicyVersion), 10),
            (nameof(RoutedPartitionFacetCandidatePageRequest.ResponseFamily), 11),
            (nameof(RoutedPartitionFacetCandidatePageRequest.RequestFingerprint), 12),
            (nameof(RoutedPartitionFacetCandidatePageRequest.LayoutFormatVersion), 13),
            (nameof(RoutedPartitionFacetCandidatePageRequest.LayoutFingerprint), 14),
            (nameof(RoutedPartitionFacetCandidatePageRequest.StateName), 15),
            (nameof(RoutedPartitionFacetCandidatePageRequest.HasExpectedDataVersion), 16),
            (nameof(RoutedPartitionFacetCandidatePageRequest.ExpectedDataVersion), 17),
            (nameof(RoutedPartitionFacetCandidatePageRequest.IndexSchemaFingerprint), 18),
            (nameof(RoutedPartitionFacetCandidatePageRequest.IndexSchemaProtocolVersion), 19));
        AssertFieldIds<PartitionFacetCandidate>(
            (nameof(PartitionFacetCandidate.Value), 0),
            (nameof(PartitionFacetCandidate.RawCount), 1));
        AssertFieldIds<PartitionFacetCandidatePageResult>(
            (nameof(PartitionFacetCandidatePageResult.Items), 0),
            (nameof(PartitionFacetCandidatePageResult.FrontierValue), 1),
            (nameof(PartitionFacetCandidatePageResult.Exhausted), 2),
            (nameof(PartitionFacetCandidatePageResult.PageRawCount), 3),
            (nameof(PartitionFacetCandidatePageResult.TotalRawCount), 4),
            (nameof(PartitionFacetCandidatePageResult.StopReason), 5),
            (nameof(PartitionFacetCandidatePageResult.Work), 6),
            (nameof(PartitionFacetCandidatePageResult.ItemByteCount), 7),
            (nameof(PartitionFacetCandidatePageResult.ProtocolVersion), 8),
            (nameof(PartitionFacetCandidatePageResult.OrderingVersion), 9),
            (nameof(PartitionFacetCandidatePageResult.WorkPolicyVersion), 10),
            (nameof(PartitionFacetCandidatePageResult.ResponseFamily), 11),
            (nameof(PartitionFacetCandidatePageResult.Epoch), 12),
            (nameof(PartitionFacetCandidatePageResult.RequestFingerprint), 13),
            (nameof(PartitionFacetCandidatePageResult.LayoutFormatVersion), 14),
            (nameof(PartitionFacetCandidatePageResult.LayoutFingerprint), 15),
            (nameof(PartitionFacetCandidatePageResult.DataVersion), 16));
        AssertFieldIds<RoutedPartitionFacetCountSliceRequest>(
            (nameof(RoutedPartitionFacetCountSliceRequest.Query), 0),
            (nameof(RoutedPartitionFacetCountSliceRequest.FacetScope), 1),
            (nameof(RoutedPartitionFacetCountSliceRequest.FacetKind), 2),
            (nameof(RoutedPartitionFacetCountSliceRequest.Value), 3),
            (nameof(RoutedPartitionFacetCountSliceRequest.Epoch), 4),
            (nameof(RoutedPartitionFacetCountSliceRequest.HasAfter), 5),
            (nameof(RoutedPartitionFacetCountSliceRequest.After), 6),
            (nameof(RoutedPartitionFacetCountSliceRequest.WorkBudget), 7),
            (nameof(RoutedPartitionFacetCountSliceRequest.ProtocolVersion), 8),
            (nameof(RoutedPartitionFacetCountSliceRequest.OrderingVersion), 9),
            (nameof(RoutedPartitionFacetCountSliceRequest.WorkPolicyVersion), 10),
            (nameof(RoutedPartitionFacetCountSliceRequest.ResponseFamily), 11),
            (nameof(RoutedPartitionFacetCountSliceRequest.RequestFingerprint), 12),
            (nameof(RoutedPartitionFacetCountSliceRequest.LayoutFormatVersion), 13),
            (nameof(RoutedPartitionFacetCountSliceRequest.LayoutFingerprint), 14),
            (nameof(RoutedPartitionFacetCountSliceRequest.StateName), 15),
            (nameof(RoutedPartitionFacetCountSliceRequest.HasExpectedDataVersion), 16),
            (nameof(RoutedPartitionFacetCountSliceRequest.ExpectedDataVersion), 17),
            (nameof(RoutedPartitionFacetCountSliceRequest.IndexSchemaFingerprint), 18),
            (nameof(RoutedPartitionFacetCountSliceRequest.IndexSchemaProtocolVersion), 19));
        AssertFieldIds<PartitionFacetCountSliceResult>(
            (nameof(PartitionFacetCountSliceResult.CountDelta), 0),
            (nameof(PartitionFacetCountSliceResult.HasFrontier), 1),
            (nameof(PartitionFacetCountSliceResult.Frontier), 2),
            (nameof(PartitionFacetCountSliceResult.Exhausted), 3),
            (nameof(PartitionFacetCountSliceResult.StopReason), 4),
            (nameof(PartitionFacetCountSliceResult.Work), 5),
            (nameof(PartitionFacetCountSliceResult.ProtocolVersion), 6),
            (nameof(PartitionFacetCountSliceResult.OrderingVersion), 7),
            (nameof(PartitionFacetCountSliceResult.WorkPolicyVersion), 8),
            (nameof(PartitionFacetCountSliceResult.ResponseFamily), 9),
            (nameof(PartitionFacetCountSliceResult.Epoch), 10),
            (nameof(PartitionFacetCountSliceResult.RequestFingerprint), 11),
            (nameof(PartitionFacetCountSliceResult.LayoutFormatVersion), 12),
            (nameof(PartitionFacetCountSliceResult.LayoutFingerprint), 13),
            (nameof(PartitionFacetCountSliceResult.DataVersion), 14));
        AssertFieldIds<PartitionFacetWork>(
            (nameof(PartitionFacetWork.ValueSeekCount), 0),
            (nameof(PartitionFacetWork.ValueVisitCount), 1),
            (nameof(PartitionFacetWork.GrainGroupVisitCount), 2),
            (nameof(PartitionFacetWork.OwnershipProbeCount), 3),
            (nameof(PartitionFacetWork.RecordProbeCount), 4),
            (nameof(PartitionFacetWork.PredicateNodeProbeCount), 5),
            (nameof(PartitionFacetWork.IndexEntryProbeCount), 6),
            (nameof(PartitionFacetWork.CountIncrementCount), 7),
            (nameof(PartitionFacetWork.ResultMaterializationCount), 8));
        AssertFieldIds<StorageFacetDataChangedException>(
            (nameof(StorageFacetDataChangedException.ExpectedVersion), 0),
            (nameof(StorageFacetDataChangedException.CurrentVersion), 1));

        ((int)PartitionQueryResponseFamily.GrainIdPage).Should().Be(1);
        ((int)PartitionQueryResponseFamily.DistinctFacetValuePage).Should().Be(2);
        ((int)PartitionQueryResponseFamily.FacetValueCountCandidates).Should().Be(3);
        ((int)PartitionQueryResponseFamily.FacetValueCountProbe).Should().Be(4);

        ((int)PartitionQueryPageStopReason.Exhausted).Should().Be(0);
        ((int)PartitionQueryPageStopReason.WorkBudget).Should().Be(1);
        ((int)PartitionQueryPageStopReason.ItemLimit).Should().Be(2);
        ((int)PartitionQueryPageStopReason.ByteLimit).Should().Be(3);

        AssertFieldIds<StorageLayoutDescriptor>(
            (nameof(StorageLayoutDescriptor.FormatVersion), 0),
            (nameof(StorageLayoutDescriptor.ProviderName), 1),
            (nameof(StorageLayoutDescriptor.PartitionCount), 2),
            (nameof(StorageLayoutDescriptor.JournalSegmentCapacity), 3),
            (nameof(StorageLayoutDescriptor.MaximumJournalReplayEntries), 4),
            (nameof(StorageLayoutDescriptor.VirtualSlotTargetCount), 5));
        AssertFieldIds<StorageLayoutIdentity>(
            (nameof(StorageLayoutIdentity.FormatVersion), 0),
            (nameof(StorageLayoutIdentity.ProviderName), 1),
            (nameof(StorageLayoutIdentity.PartitionCount), 2));
        AssertFieldIds<StorageLayoutSnapshot>(
            (nameof(StorageLayoutSnapshot.FormatVersion), 0),
            (nameof(StorageLayoutSnapshot.ProviderName), 1),
            (nameof(StorageLayoutSnapshot.InitialPartitionCount), 2),
            (nameof(StorageLayoutSnapshot.VirtualSlotCount), 3),
            (nameof(StorageLayoutSnapshot.Epoch), 4),
            ("SlotAssignments", 5),
            (nameof(StorageLayoutSnapshot.MovementProtocolVersion), 6),
            ("MovementEnablement", 7),
            ("MoveIntent", 8),
            ("LastMoveReceipt", 9),
            (nameof(StorageLayoutSnapshot.IndexSchemaProtocolVersion), 10),
            ("IndexSchemaEnablement", 11));
        AssertFieldIds<StorageLayoutState>(
            (nameof(StorageLayoutState.Initialized), 0),
            (nameof(StorageLayoutState.FormatVersion), 1),
            (nameof(StorageLayoutState.ProviderName), 2),
            (nameof(StorageLayoutState.PartitionCount), 3),
            (nameof(StorageLayoutState.JournalSegmentCapacity), 4),
            (nameof(StorageLayoutState.MaximumJournalReplayEntries), 5),
            (nameof(StorageLayoutState.VirtualSlotCount), 6),
            (nameof(StorageLayoutState.SlotAssignments), 7),
            (nameof(StorageLayoutState.Epoch), 8),
            (nameof(StorageLayoutState.MovementProtocolVersion), 9),
            (nameof(StorageLayoutState.MovementEnablement), 10),
            (nameof(StorageLayoutState.MoveIntent), 11),
            (nameof(StorageLayoutState.LastMoveReceipt), 12),
            (nameof(StorageLayoutState.IndexSchemaProtocolVersion), 13),
            (nameof(StorageLayoutState.IndexSchemaEnablement), 14));
        AssertFieldIds<StorageIndexSchemaEnableIntent>(
            (nameof(StorageIndexSchemaEnableIntent.EnablementId), 0),
            (nameof(StorageIndexSchemaEnableIntent.ProtocolVersion), 1),
            (nameof(StorageIndexSchemaEnableIntent.LayoutEpoch), 2),
            (nameof(StorageIndexSchemaEnableIntent.LayoutFingerprint), 3));
        AssertFieldIds<StorageMovementEnableIntent>(
            (nameof(StorageMovementEnableIntent.EnablementId), 0),
            (nameof(StorageMovementEnableIntent.SourceEpoch), 1),
            (nameof(StorageMovementEnableIntent.PlannedEpoch), 2),
            (nameof(StorageMovementEnableIntent.Owners), 3),
            (nameof(StorageMovementEnableIntent.NextOwnerIndex), 4));
        AssertFieldIds<StorageSlotMoveIntent>(
            (nameof(StorageSlotMoveIntent.MoveId), 0),
            (nameof(StorageSlotMoveIntent.Slot), 1),
            (nameof(StorageSlotMoveIntent.SourceOwner), 2),
            (nameof(StorageSlotMoveIntent.TargetOwner), 3),
            (nameof(StorageSlotMoveIntent.SourceEpoch), 4),
            (nameof(StorageSlotMoveIntent.Phase), 5),
            (nameof(StorageSlotMoveIntent.TransferPageRecordLimit), 6),
            (nameof(StorageSlotMoveIntent.TransferPageByteTarget), 7),
            (nameof(StorageSlotMoveIntent.ExportedRecordCount), 8),
            (nameof(StorageSlotMoveIntent.ExportedByteCount), 9),
            (nameof(StorageSlotMoveIntent.DeletedRecordCount), 10),
            (nameof(StorageSlotMoveIntent.DeletedByteCount), 11));
        AssertFieldIds<StorageSlotMoveReceipt>(
            (nameof(StorageSlotMoveReceipt.MoveId), 0),
            (nameof(StorageSlotMoveReceipt.Slot), 1),
            (nameof(StorageSlotMoveReceipt.SourceOwner), 2),
            (nameof(StorageSlotMoveReceipt.TargetOwner), 3),
            (nameof(StorageSlotMoveReceipt.SourceEpoch), 4),
            (nameof(StorageSlotMoveReceipt.CompletionEpoch), 5),
            (nameof(StorageSlotMoveReceipt.TerminalPhase), 6),
            (nameof(StorageSlotMoveReceipt.ExportedRecordCount), 7),
            (nameof(StorageSlotMoveReceipt.ExportedByteCount), 8),
            (nameof(StorageSlotMoveReceipt.DeletedRecordCount), 9),
            (nameof(StorageSlotMoveReceipt.DeletedByteCount), 10));
        AssertFieldIds<StorageSlotMovePlanRequest>(
            (nameof(StorageSlotMovePlanRequest.Slot), 0),
            (nameof(StorageSlotMovePlanRequest.TargetOwner), 1),
            (nameof(StorageSlotMovePlanRequest.MovementProtocolVersion), 2),
            (nameof(StorageSlotMovePlanRequest.TransferPageRecordLimit), 3),
            (nameof(StorageSlotMovePlanRequest.TransferPageByteTarget), 4));
        AssertFieldIds<StorageSlotMoveCommand>(
            (nameof(StorageSlotMoveCommand.MoveId), 0),
            (nameof(StorageSlotMoveCommand.MovementProtocolVersion), 1));
        AssertFieldIds<StorageSlotMoveProgressSnapshot>(
            (nameof(StorageSlotMoveProgressSnapshot.Intent), 0),
            (nameof(StorageSlotMoveProgressSnapshot.CurrentEpoch), 1),
            (nameof(StorageSlotMoveProgressSnapshot.ExportedRecordCount), 2),
            (nameof(StorageSlotMoveProgressSnapshot.ExportedByteCount), 3),
            (nameof(StorageSlotMoveProgressSnapshot.DeletedRecordCount), 4),
            (nameof(StorageSlotMoveProgressSnapshot.DeletedByteCount), 5));

        StorageLayout.CurrentMovementProtocolVersion.Should().Be(1);
        ((int)SearchableStorageMovementState.Disabled).Should().Be(0);
        ((int)SearchableStorageMovementState.Enabling).Should().Be(1);
        ((int)SearchableStorageMovementState.Enabled).Should().Be(2);
        ((int)SearchableStorageSlotMovePhase.Planned).Should().Be(0);
        ((int)SearchableStorageSlotMovePhase.SourceFrozen).Should().Be(1);
        ((int)SearchableStorageSlotMovePhase.TargetVersionFenced).Should().Be(2);
        ((int)SearchableStorageSlotMovePhase.Copying).Should().Be(3);
        ((int)SearchableStorageSlotMovePhase.CopyComplete).Should().Be(4);
        ((int)SearchableStorageSlotMovePhase.OwnershipCommitted).Should().Be(5);
        ((int)SearchableStorageSlotMovePhase.SourceVisibilityFenced).Should().Be(6);
        ((int)SearchableStorageSlotMovePhase.TargetEnabled).Should().Be(7);
        ((int)SearchableStorageSlotMovePhase.DeletingSource).Should().Be(8);
        ((int)SearchableStorageSlotMovePhase.Retiring).Should().Be(9);
        ((int)SearchableStorageSlotMovePhase.Aborting).Should().Be(10);
        ((int)SearchableStorageSlotMovePhase.Completed).Should().Be(11);
        ((int)SearchableStorageSlotMovePhase.Aborted).Should().Be(12);

    }

    private static void AssertFieldIds<T>(params (string MemberName, uint Id)[] expected)
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
        var actual = typeof(T).GetProperties(flags)
            .Select(static property => (
                property.Name,
                Attribute: property.GetCustomAttribute<IdAttribute>()))
            .Where(static item => item.Attribute is not null)
            .Select(static item => (item.Name, item.Attribute!.Id))
            .OrderBy(static item => item.Id)
            .ToArray();

        actual.Should().Equal(expected.Select(static item => (item.MemberName, item.Id)));
        actual.Select(static item => item.Id).Should().Equal(
            Enumerable.Range(0, expected.Length).Select(static value => (uint)value));
    }

    private static uint[] GetFieldIds<T>()
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
        return typeof(T).GetProperties(flags)
            .Select(static property => property.GetCustomAttribute<IdAttribute>())
            .Where(static attribute => attribute is not null)
            .Select(static attribute => attribute!.Id)
            .ToArray();
    }

    private sealed class QueryState
    {
        public int Value { get; init; }
    }

    private sealed class DirectClientImplementation : ISearchableStorageClient
    {
        public Task<IReadOnlyList<GrainId>> FindAsync<TState, TValue>(
            string stateName,
            Expression<Func<TState, TValue>> propertySelector,
            TValue value,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IReadOnlyList<GrainId>>([]);
        }

        public Task<IReadOnlyList<GrainId>> RangeAsync<TState, TValue>(
            string stateName,
            Expression<Func<TState, TValue>> propertySelector,
            TValue lowerBound,
            TValue upperBound,
            bool includeLowerBound = true,
            bool includeUpperBound = true,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IReadOnlyList<GrainId>>([]);
        }
    }

    private class ExternalQueryProvider<TState> : IQueryProvider, ISearchableStorageAsyncQueryProvider
    {
        public Expression? Expression { get; private set; }

        public CancellationToken CancellationToken { get; private set; }

        public IQueryable CreateQuery(Expression expression)
        {
            return new ExternalQuery<TState>(this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (typeof(TElement) != typeof(TState))
            {
                throw new NotSupportedException();
            }

            return (IQueryable<TElement>)(object)new ExternalQuery<TState>(this, expression);
        }

        public object? Execute(Expression expression)
        {
            throw new NotSupportedException();
        }

        public TResult Execute<TResult>(Expression expression)
        {
            throw new NotSupportedException();
        }

        public Task<IReadOnlyList<GrainId>> ExecuteToGrainIdsAsync(
            Expression expression,
            CancellationToken cancellationToken)
        {
            Expression = expression;
            CancellationToken = cancellationToken;
            return Task.FromResult<IReadOnlyList<GrainId>>([]);
        }
    }

    private sealed class ExternalFacetQueryProvider<TState>
        : ExternalQueryProvider<TState>, ISearchableStorageFacetQueryProvider
    {
        public Expression? FacetExpression { get; private set; }
        public LambdaExpression? FacetSelector { get; private set; }
        public SearchableStorageFacetRequest? FacetRequest { get; private set; }
        public CancellationToken FacetCancellationToken { get; private set; }

        public Task<SearchableStorageDistinctFacetPage<TValue>> ExecuteDistinctFacetValuePageAsync<TValue>(
            Expression queryExpression,
            LambdaExpression propertySelector,
            SearchableStorageFacetPageRequest request,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(new SearchableStorageDistinctFacetPage<TValue>([], null));
        }

        public Task<SearchableStorageFacetResult<TValue>> ExecuteFacetValueCountsAsync<TValue>(
            Expression queryExpression,
            LambdaExpression propertySelector,
            SearchableStorageFacetRequest request,
            CancellationToken cancellationToken)
        {
            FacetExpression = queryExpression;
            FacetSelector = propertySelector;
            FacetRequest = request;
            FacetCancellationToken = cancellationToken;
            return Task.FromResult(new SearchableStorageFacetResult<TValue>([], true, 0));
        }

        public Task<SearchableStorageFacetMinMax<TValue>?> ExecuteFacetMinMaxAsync<TValue>(
            Expression queryExpression,
            LambdaExpression propertySelector,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<SearchableStorageFacetMinMax<TValue>?>(null);
        }
    }

    private sealed class ExternalQuery<TState> : IOrderedQueryable<TState>
    {
        public ExternalQuery(IQueryProvider provider)
        {
            Provider = provider;
            Expression = System.Linq.Expressions.Expression.Constant(this);
        }

        public ExternalQuery(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public Type ElementType => typeof(TState);

        public Expression Expression { get; }

        public IQueryProvider Provider { get; }

        public IEnumerator<TState> GetEnumerator()
        {
            throw new NotSupportedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
