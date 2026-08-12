using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

internal static class CollectionMembershipProviderContract
{
    public static async Task AssertPersistenceAndMovementAsync(ISearchableStorageFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        await AssertJournalSnapshotRecoveryAsync(fixture);
        await AssertMovementRoundTripAsync(fixture);
    }

    private static async Task AssertJournalSnapshotRecoveryAsync(ISearchableStorageFixture fixture)
    {
        var stateName = $"collection-provider-{Guid.NewGuid():N}";
        var services = GetPrimaryServices(fixture);
        var storage = services.GetRequiredKeyedService<IGrainStorage>(VacancyGrain.StorageProviderName);
        var client = CreateClient(
            fixture,
            VacancyGrain.StorageProviderName,
            fixture.PartitionCount,
            "collection-provider-contract");
        var grainId = GrainId.Create("collection-provider", Guid.NewGuid().ToString("N"));
        var oldTag = $"old-{Guid.NewGuid():N}";
        var currentTag = $"current-{Guid.NewGuid():N}";
        var state = new GrainState<CollectionMembershipState>
        {
            State = new CollectionMembershipState
            {
                Tags = [oldTag, null, oldTag],
                AudienceIds = [10, null, 10],
                City = "Netanya",
                Salary = 10,
            },
        };

        try
        {
            await storage.WriteStateAsync(stateName, grainId, state);
            state.State.Tags = [currentTag, null, currentTag];
            state.State.AudienceIds = [20, null, 20];
            state.State.City = "Rishon-LeZion";
            state.State.Salary = 20;
            await storage.WriteStateAsync(stateName, grainId, state);

            var layout = await fixture.Cluster.GrainFactory
                .GetGrain<IStorageLayoutGrain>(VacancyGrain.StorageProviderName)
                .GetCurrentLayoutAsync();
            layout.Should().NotBeNull();
            var owner = layout!.GetOwner(StorageLayout.GetSlot(grainId, layout.VirtualSlotCount));
            var partition = fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(VacancyGrain.StorageProviderName, owner));
            await partition.CompactAsync();
            await fixture.Cluster.DeactivateAsync(partition);

            var loaded = new GrainState<CollectionMembershipState>();
            await storage.ReadStateAsync(stateName, grainId, loaded);
            var currentArray = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(candidate => Enumerable.Contains(candidate.Tags!, currentTag))
                .ToGrainIdsAsync();
            var currentList = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(candidate => candidate.AudienceIds!.Contains(20))
                .ToGrainIdsAsync();
            var oldArray = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(candidate => Enumerable.Contains(candidate.Tags!, oldTag))
                .ToGrainIdsAsync();
            var oldList = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(candidate => candidate.AudienceIds!.Contains(10))
                .ToGrainIdsAsync();

            loaded.RecordExists.Should().BeTrue();
            loaded.ETag.Should().Be(state.ETag);
            loaded.State.Tags.Should().Equal(currentTag, null, currentTag);
            loaded.State.AudienceIds.Should().Equal(20, null, 20);
            currentArray.Should().ContainSingle().Which.Should().Be(grainId);
            currentList.Should().ContainSingle().Which.Should().Be(grainId);
            oldArray.Should().BeEmpty();
            oldList.Should().BeEmpty();

            await storage.ClearStateAsync(stateName, grainId, loaded);
            state.ETag = loaded.ETag;
            state.RecordExists = loaded.RecordExists;
        }
        finally
        {
            if (state.RecordExists)
            {
                await storage.ClearStateAsync(stateName, grainId, state);
            }
        }
    }

    private static async Task AssertMovementRoundTripAsync(ISearchableStorageFixture fixture)
    {
        const int initialPartitionCount = 2;
        const int virtualSlotCount = 8;
        var providerName = $"collection-move-{Guid.NewGuid():N}";
        var stateName = $"collection-move-state-{Guid.NewGuid():N}";
        var services = GetPrimaryServices(fixture);
        var configured = services.GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(VacancyGrain.StorageProviderName);
        var storage = ActivatorUtilities.CreateInstance<SearchableGrainStorage>(
            services,
            providerName,
            new SearchableStorageOptions
            {
                PartitionCount = initialPartitionCount,
                VirtualSlotTargetCount = virtualSlotCount,
                JournalSegmentCapacity = 8,
                MaximumJournalReplayEntries = 64,
                CompactionThreshold = 32,
                GrainStorageSerializer = configured.GrainStorageSerializer,
            });
        var admin = new SearchableStorageAdminClient(
            fixture.Cluster.GrainFactory,
            providerName,
            initialPartitionCount,
            new SearchableStorageMovementOptions
            {
                TransferPageRecordLimit = 1,
                TransferPageByteTarget = 16 * 1024,
            });
        var client = CreateClient(
            fixture,
            providerName,
            initialPartitionCount,
            "collection-movement-contract");
        var initializerId = GrainId.Create("collection-move", $"{providerName}-initializer");
        var initializer = CreateState("initializer", 0);
        await storage.WriteStateAsync(stateName, initializerId, initializer);
        await admin.EnableMovementAsync();
        var rebalance = await admin.PlanRebalanceAsync(initialPartitionCount + 1);
        rebalance.NextMove.Should().NotBeNull();
        var next = rebalance.NextMove!;
        var grainId = FindGrainIdInSlot(providerName, next.Slot, virtualSlotCount);
        var tag = $"moved-{Guid.NewGuid():N}";
        var state = CreateState(tag, 77);

        try
        {
            state.State.Tags = [tag, null, tag];
            state.State.AudienceIds = [77, null, 77];
            await storage.WriteStateAsync(stateName, grainId, state);
            var planned = await admin.PlanMoveAsync(next.Slot, next.TargetPartitionIndex);
            var completed = await admin.ExecuteMoveAsync(planned.MoveId);
            completed.IsComplete.Should().BeTrue();
            completed.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);

            var source = fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, next.SourcePartitionIndex));
            var target = fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, next.TargetPartitionIndex));
            await fixture.Cluster.DeactivateAsync(source);
            await fixture.Cluster.DeactivateAsync(target);

            var loaded = new GrainState<CollectionMembershipState>();
            await storage.ReadStateAsync(stateName, grainId, loaded);
            var byArray = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(candidate => Enumerable.Contains(candidate.Tags!, tag))
                .ToGrainIdsAsync();
            var byList = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(candidate => candidate.AudienceIds!.Contains(77))
                .ToGrainIdsAsync();

            loaded.RecordExists.Should().BeTrue();
            loaded.State.Tags.Should().Equal(tag, null, tag);
            loaded.State.AudienceIds.Should().Equal(77, null, 77);
            byArray.Should().ContainSingle().Which.Should().Be(grainId);
            byList.Should().ContainSingle().Which.Should().Be(grainId);

            await storage.ClearStateAsync(stateName, grainId, loaded);
            state.ETag = loaded.ETag;
            state.RecordExists = loaded.RecordExists;
        }
        finally
        {
            if (state.RecordExists)
            {
                await storage.ClearStateAsync(stateName, grainId, state);
            }

            if (initializer.RecordExists)
            {
                await storage.ClearStateAsync(stateName, initializerId, initializer);
            }
        }
    }

    private static GrainId FindGrainIdInSlot(
        string providerName,
        int slot,
        int virtualSlotCount)
    {
        for (var ordinal = 0; ordinal < 100_000; ordinal++)
        {
            var grainId = GrainId.Create("collection-move", $"{providerName}-{ordinal:D8}");
            if (StorageLayout.GetSlot(grainId, virtualSlotCount) == slot)
            {
                return grainId;
            }
        }

        throw new InvalidOperationException($"Could not create a grain id in virtual slot {slot}.");
    }

    private static GrainState<CollectionMembershipState> CreateState(string value, int number)
    {
        return new GrainState<CollectionMembershipState>
        {
            State = new CollectionMembershipState
            {
                Tags = [value],
                AudienceIds = [number],
                City = value,
                Salary = number,
            },
        };
    }

    private static SearchableStorageClient CreateClient(
        ISearchableStorageFixture fixture,
        string providerName,
        int partitionCount,
        string keyId)
    {
        var queryOptions = new SearchableStorageQueryOptions();
        queryOptions.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            keyId,
            Enumerable.Range(1, 32).Select(static value => checked((byte)value)).ToArray());
        return new SearchableStorageClient(
            fixture.Cluster.GrainFactory,
            providerName,
            partitionCount,
            queryOptions);
    }

    private static IServiceProvider GetPrimaryServices(ISearchableStorageFixture fixture)
    {
        return Assert.IsType<InProcessSiloHandle>(fixture.Cluster.Primary).ServiceProvider;
    }
}
