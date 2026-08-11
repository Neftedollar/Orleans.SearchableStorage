using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageMovementVisibilityFenceTests
{
    private readonly MemoryStorageFixture _fixture;

    public StorageMovementVisibilityFenceTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SourceVisibilityFenceSurvivesReactivationBeforeTargetEnableAndMoveResumes()
    {
        const int partitionCount = 2;
        const int virtualSlotCount = 2;
        const int slot = 0;
        const int sourceOwner = 0;
        const int targetOwner = 1;
        const string stateName = "visibility-fence";
        var providerName = $"movement-fence-{Guid.NewGuid():N}";
        var layoutGrain = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var initialLayout = await layoutGrain.InitializeRoutingAsync(StorageLayout.CreateDescriptor(
            providerName,
            partitionCount,
            journalSegmentCapacity: 8,
            maximumJournalReplayEntries: 64,
            virtualSlotTargetCount: virtualSlotCount));
        var grainId = CreateGrainInSlot(slot, virtualSlotCount);
        var recordKey = CreateRecordKey(stateName, grainId);
        var source = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, sourceOwner));
        var target = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, targetOwner));
        var settings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = 8,
            MaximumJournalReplayEntries = 64,
            CompactionThreshold = 64,
        };
        var etag = await source.WriteRoutedAsync(new RoutedStorageWriteRequest
        {
            Request = new StorageWriteRequest
            {
                RecordKey = recordKey,
                GrainId = grainId,
                Payload = [1, 2, 3],
                IndexEntries = [],
                Persistence = settings,
            },
            Slot = slot,
            Epoch = initialLayout.Epoch,
        });
        etag.Should().Be("1");

        var admin = new SearchableStorageAdminClient(
            _fixture.Cluster.GrainFactory,
            providerName,
            partitionCount,
            new SearchableStorageMovementOptions
            {
                TransferPageRecordLimit = 1,
                TransferPageByteTarget = StorageMoveProtocol.MaximumPageBytes,
            });
        _ = await admin.EnableMovementAsync();
        var progress = await admin.PlanMoveAsync(slot, targetOwner);
        for (var attempt = 0;
             attempt < 24 && progress.Phase != SearchableStorageSlotMovePhase.SourceVisibilityFenced;
             attempt++)
        {
            progress = await admin.AdvanceMoveAsync(progress.MoveId);
        }

        progress.Phase.Should().Be(SearchableStorageSlotMovePhase.SourceVisibilityFenced);
        var committedEpoch = checked(progress.SourceEpoch + 1);
        progress.CurrentEpoch.Should().Be(committedEpoch);
        var targetBeforeEnable = await target.GetMovementStateAsync();
        targetBeforeEnable.MoveControl.Phase.Should().Be(
            StoragePartitionMovePhase.TargetImportComplete);

        await _fixture.Cluster.DeactivateAsync(source);
        var staleRead = new RoutedStorageReadRequest
        {
            RecordKey = recordKey,
            GrainId = grainId,
            Slot = slot,
            Epoch = progress.SourceEpoch,
        };
        var staleQuery = new RoutedPartitionQuery
        {
            Query = new PartitionQueryPlan { Operation = PartitionQueryOperation.All },
            Epoch = progress.SourceEpoch,
        };
        var currentLayout = await layoutGrain.GetCurrentLayoutAsync();
        currentLayout.Should().NotBeNull();
        var pagePlan = new PartitionQueryPlan { Operation = PartitionQueryOperation.All };
        var stalePage = new RoutedPartitionQueryPageRequest
        {
            Query = pagePlan,
            Epoch = progress.SourceEpoch,
            WorkBudget = SearchableStorageQueryOptions.DefaultPartitionWorkBudget,
            ItemLimit = SearchableStorageQueryOptions.DefaultPartitionResponseItems,
            ByteLimit = SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.OrderingVersion,
            WorkPolicyVersion = QueryProtocol.WorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.GrainIdPage,
            QueryFingerprint = QueryPlanFingerprint.Compute(stateName, pagePlan),
            LayoutFormatVersion = currentLayout!.FormatVersion,
            LayoutFingerprint = StorageLayoutFingerprint.Compute(currentLayout),
            StateName = stateName,
        };

        await AssertStaleEpochAsync(
            () => source.ReadRoutedAsync(staleRead),
            progress.SourceEpoch,
            committedEpoch,
            sourceOwner);
        await AssertStaleEpochAsync(
            () => source.QueryRoutedAsync(staleQuery),
            progress.SourceEpoch,
            committedEpoch,
            sourceOwner);
        await AssertStaleEpochAsync(
            () => source.QueryPageRoutedAsync(stalePage),
            progress.SourceEpoch,
            committedEpoch,
            sourceOwner);

        Func<Task> targetMutationBeforeEnable = async () => _ = await target.WriteRoutedAsync(
            new RoutedStorageWriteRequest
            {
                Request = new StorageWriteRequest
                {
                    RecordKey = recordKey,
                    GrainId = grainId,
                    Payload = [9],
                    ExpectedETag = etag,
                    IndexEntries = [],
                    Persistence = settings,
                },
                Slot = slot,
                Epoch = committedEpoch,
            });
        await targetMutationBeforeEnable.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*mutation-frozen*");

        var completed = await admin.ExecuteMoveAsync(progress.MoveId);
        completed.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
        completed.IsComplete.Should().BeTrue();
        var targetRead = await target.ReadRoutedAsync(new RoutedStorageReadRequest
        {
            RecordKey = recordKey,
            GrainId = grainId,
            Slot = slot,
            Epoch = committedEpoch,
        });
        targetRead.Found.Should().BeTrue();
        targetRead.ETag.Should().Be(etag);
    }

    private static async Task AssertStaleEpochAsync<T>(
        Func<Task<T>> operation,
        long expectedEpoch,
        long currentEpoch,
        int requestedPartition)
    {
        var mismatch = (await operation.Should()
            .ThrowAsync<StorageRouteMismatchException>()).Which;
        mismatch.ExpectedEpoch.Should().Be(expectedEpoch);
        mismatch.CurrentEpoch.Should().Be(currentEpoch);
        mismatch.RequestedPartition.Should().Be(requestedPartition);
    }

    private static GrainId CreateGrainInSlot(int slot, int virtualSlotCount)
    {
        for (var candidate = 0; ; candidate++)
        {
            var grainId = GrainId.Create("movement-fence", $"record-{candidate}");
            if (StorageLayout.GetSlot(grainId, virtualSlotCount) == slot)
            {
                return grainId;
            }
        }
    }

    private static string CreateRecordKey(string stateName, GrainId grainId)
    {
        return string.Concat(
            stateName,
            "/",
            Convert.ToHexString(grainId.Type.AsSpan()),
            "/",
            Convert.ToHexString(grainId.Key.AsSpan()));
    }
}
