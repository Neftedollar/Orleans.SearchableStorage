using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageMovementFacetConservatismTests
{
    private const string StateName = "movement-facet";
    private const string FacetScope = "movement-facet/value";

    [Theory]
    [InlineData(0, 1)] // The target has a staged copy while the source still owns the slot.
    [InlineData(1, 0)] // The source has a hidden copy after ownership moved to the target.
    public void NonAuthoritativeCopyOnlyInflatesRawMetadataAndExactProbeReturnsZero(
        int authoritativeOwner,
        int physicalPartition)
    {
        var layout = CreateLayout(authoritativeOwner);
        var grainId = GrainId.Create("movement-facet", "copy-only");
        var view = new StoragePartitionView(new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["copy-only"] = new StoredRecord
            {
                GrainId = grainId,
                Payload = [],
                ETag = "1",
                IndexEntries =
                [
                    new IndexEntry
                    {
                        Scope = FacetScope,
                        Kind = SearchableIndexKind.Hash,
                        Value = IndexValue.Create("copy-only"),
                    },
                ],
            },
        });
        var query = new PartitionQueryPlan { Operation = PartitionQueryOperation.All };
        var fingerprint = FacetQueryFingerprint.Compute(
            StateName,
            query,
            FacetScope,
            SearchableIndexKind.Hash);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
        var candidateRequest = new RoutedPartitionFacetCandidatePageRequest
        {
            StateName = StateName,
            Query = query,
            FacetScope = FacetScope,
            FacetKind = SearchableIndexKind.Hash,
            Epoch = layout.Epoch,
            WorkBudget = 100,
            ItemLimit = 1,
            ByteLimit = 1_000,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
            WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.FacetValueCountCandidates,
            RequestFingerprint = fingerprint,
            LayoutFormatVersion = layout.FormatVersion,
            LayoutFingerprint = layoutFingerprint,
        };
        var candidates = StoragePartitionFacetEvaluator.EvaluateCandidatePageValidated(
            candidateRequest,
            view,
            layout,
            fingerprint,
            layoutFingerprint);
        var countRequest = new RoutedPartitionFacetCountSliceRequest
        {
            StateName = StateName,
            Query = query,
            FacetScope = FacetScope,
            FacetKind = SearchableIndexKind.Hash,
            Value = IndexValue.Create("copy-only"),
            Epoch = layout.Epoch,
            WorkBudget = 100,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
            WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.FacetValueCountProbe,
            RequestFingerprint = fingerprint,
            LayoutFormatVersion = layout.FormatVersion,
            LayoutFingerprint = layoutFingerprint,
        };
        var exact = StoragePartitionFacetEvaluator.EvaluateCountSliceValidated(
            countRequest,
            view,
            layout,
            physicalPartition,
            fingerprint,
            layoutFingerprint);

        candidates.Items.Should().ContainSingle();
        candidates.Items[0].RawCount.Should().Be(1);
        candidates.PageRawCount.Should().Be(1);
        candidates.TotalRawCount.Should().Be(1);
        exact.CountDelta.Should().Be(0);
        exact.Exhausted.Should().BeTrue();
        candidates.TotalRawCount.Should().BeGreaterThanOrEqualTo(exact.CountDelta);
    }

    private static StorageLayoutSnapshot CreateLayout(int authoritativeOwner)
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = StateName,
            PartitionCount = 2,
            VirtualSlotCount = 1,
            SlotAssignments = [authoritativeOwner],
            Epoch = 2,
        });
    }
}
