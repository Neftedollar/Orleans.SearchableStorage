using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageIndexSchemaControlRecoveryTests
{
    private const string ControlStateName = "index-schema";
    private readonly MemoryStorageFixture _fixture;

    public StorageIndexSchemaControlRecoveryTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData(
        PhysicalWriteFaultStage.BeforeCommit,
        MemoryStorageFixture.SchemaBeginBeforeProviderName,
        SearchableStorageIndexSchemaState.Uninitialized)]
    [InlineData(
        PhysicalWriteFaultStage.AfterCommit,
        MemoryStorageFixture.SchemaBeginAfterProviderName,
        SearchableStorageIndexSchemaState.Rebuilding)]
    public async Task BeginRecoversFromAControlWriteFailureBeforeOrAfterCommit(
        PhysicalWriteFaultStage stage,
        string providerName,
        SearchableStorageIndexSchemaState durableStateAfterFailure)
    {
        var stateName = MemoryStorageFixture.SchemaFaultStateName;
        var admin = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var (control, request) = CreateControl(providerName, stateName);
        await ScheduleNextControlWriteFaultAsync(control, stage);

        Func<Task> begin = async () => await control.BeginRebuildAsync(request);
        await AssertInjectedFailureAsync(begin);
        await _fixture.Cluster.DeactivateAsync(control);

        var recovered = await admin.GetIndexSchemaAsync<VacancyState>(stateName);
        recovered.State.Should().Be(durableStateAfterFailure);
        recovered.ProcessedRecordCount.Should().Be(0);

        var completed = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(0);
    }

    [Theory]
    [InlineData(
        PhysicalWriteFaultStage.BeforeCommit,
        MemoryStorageFixture.SchemaProgressBeforeProviderName,
        0)]
    [InlineData(
        PhysicalWriteFaultStage.AfterCommit,
        MemoryStorageFixture.SchemaProgressAfterProviderName,
        StorageIndexSchema.RebuildPageSize)]
    public async Task PageProgressRecoversFromAControlWriteFailureBeforeOrAfterCommit(
        PhysicalWriteFaultStage stage,
        string providerName,
        long durableCountAfterFailure)
    {
        var stateName = MemoryStorageFixture.SchemaFaultStateName;
        var seeded = await SeedLegacyRecordsAsync(providerName, stateName, 65);
        var admin = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var (control, request) = CreateControl(providerName, stateName);
        var snapshot = await PrepareFirstRecordPageAsync(control, request);
        var rebuildId = snapshot.Rebuild!.RebuildId;
        await ScheduleNextControlWriteFaultAsync(control, stage);

        Func<Task> advance = async () => await control.AdvanceRebuildAsync(
            new StorageIndexSchemaCommand
            {
                Schema = request,
                RebuildId = rebuildId,
            });
        await AssertInjectedFailureAsync(advance);
        await _fixture.Cluster.DeactivateAsync(seeded.Partition);
        await _fixture.Cluster.DeactivateAsync(control);

        var recovered = await admin.GetIndexSchemaAsync<VacancyState>(stateName);
        recovered.State.Should().Be(SearchableStorageIndexSchemaState.Rebuilding);
        recovered.RebuildId.Should().Be(rebuildId);
        recovered.ProcessedRecordCount.Should().Be(durableCountAfterFailure);

        var completed = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(65);
        var query = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var found = await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            seeded.City);
        found.Should().BeEquivalentTo(seeded.GrainIds);
    }

    [Theory]
    [InlineData(
        PhysicalWriteFaultStage.BeforeCommit,
        MemoryStorageFixture.SchemaPublicationBeforeProviderName,
        0,
        false)]
    [InlineData(
        PhysicalWriteFaultStage.AfterCommit,
        MemoryStorageFixture.SchemaPublicationAfterProviderName,
        1,
        true)]
    public async Task LayoutPublicationCheckpointRecoversFromAControlWriteFailureBeforeOrAfterCommit(
        PhysicalWriteFaultStage stage,
        string providerName,
        long durableCountAfterFailure,
        bool durablePublicationCheckpoint)
    {
        var stateName = MemoryStorageFixture.SchemaFaultStateName;
        var seeded = await SeedLegacyRecordsAsync(providerName, stateName, 1);
        var admin = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var (control, request) = CreateControl(providerName, stateName);
        var snapshot = await PrepareFirstRecordPageAsync(control, request);
        var rebuildId = snapshot.Rebuild!.RebuildId;
        await ScheduleNextControlWriteFaultAsync(control, stage);

        Func<Task> advance = async () => await control.AdvanceRebuildAsync(
            new StorageIndexSchemaCommand
            {
                Schema = request,
                RebuildId = rebuildId,
            });
        await AssertInjectedFailureAsync(advance);

        var layout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(providerName)
            .GetCurrentLayoutAsync();
        layout!.IndexSchemaProtocolVersion.Should().Be(StorageIndexSchema.ProtocolVersion);
        layout.CopyIndexSchemaEnablement().Should().BeNull(
            "layout publication commits before its control checkpoint");

        await _fixture.Cluster.DeactivateAsync(seeded.Partition);
        await _fixture.Cluster.DeactivateAsync(control);
        var durable = await control.GetAsync(request);
        durable.Rebuild.Should().NotBeNull();
        durable.Rebuild!.RebuildId.Should().Be(rebuildId);
        durable.Rebuild.ProcessedRecordCount.Should().Be(durableCountAfterFailure);
        durable.Rebuild.LayoutProtocolPublished.Should().Be(durablePublicationCheckpoint);

        var recovered = await admin.GetIndexSchemaAsync<VacancyState>(stateName);
        recovered.State.Should().Be(SearchableStorageIndexSchemaState.Rebuilding);
        recovered.ProcessedRecordCount.Should().Be(durableCountAfterFailure);

        var completed = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(1);
        var query = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var found = await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            seeded.City);
        found.Should().ContainSingle().Which.Should().Be(seeded.GrainIds.Single());
    }

    [Theory]
    [InlineData(
        PhysicalWriteFaultStage.BeforeCommit,
        MemoryStorageFixture.SchemaFinalBeforeProviderName,
        SearchableStorageIndexSchemaState.Rebuilding)]
    [InlineData(
        PhysicalWriteFaultStage.AfterCommit,
        MemoryStorageFixture.SchemaFinalAfterProviderName,
        SearchableStorageIndexSchemaState.Active)]
    public async Task FinalActivationRecoversFromAControlWriteFailureBeforeOrAfterCommit(
        PhysicalWriteFaultStage stage,
        string providerName,
        SearchableStorageIndexSchemaState durableStateAfterFailure)
    {
        var stateName = MemoryStorageFixture.SchemaFaultStateName;
        var seeded = await SeedLegacyRecordsAsync(providerName, stateName, 1);
        var admin = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var (control, request) = CreateControl(providerName, stateName);
        var snapshot = await PrepareFirstRecordPageAsync(control, request);
        snapshot = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
        {
            Schema = request,
            RebuildId = snapshot.Rebuild!.RebuildId,
        });
        snapshot.Rebuild.Should().NotBeNull();
        snapshot.Rebuild!.ProcessedRecordCount.Should().Be(1);
        snapshot.Rebuild.LayoutProtocolPublished.Should().BeTrue();
        await ScheduleNextControlWriteFaultAsync(control, stage);

        Func<Task> advance = async () => await control.AdvanceRebuildAsync(
            new StorageIndexSchemaCommand
            {
                Schema = request,
                RebuildId = snapshot.Rebuild!.RebuildId,
            });
        await AssertInjectedFailureAsync(advance);
        await _fixture.Cluster.DeactivateAsync(seeded.Partition);
        await _fixture.Cluster.DeactivateAsync(control);

        var recovered = await admin.GetIndexSchemaAsync<VacancyState>(stateName);
        recovered.State.Should().Be(durableStateAfterFailure);
        recovered.ProcessedRecordCount.Should().Be(1);

        var completed = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(1);
        var record = seeded.GrainIds.Single();
        var read = await seeded.Partition.ReadRoutedAsync(new RoutedStorageReadRequest
        {
            RecordKey = CreateRecordKey(stateName, record),
            GrainId = record,
            Slot = StorageLayout.GetSlot(record, seeded.Layout.VirtualSlotCount),
            Epoch = seeded.Layout.Epoch,
        });
        read.ETag.Should().Be(seeded.ETags.Single());
    }

    private static async Task<StorageIndexSchemaSnapshot> PrepareFirstRecordPageAsync(
        IStorageIndexSchemaGrain control,
        StorageIndexSchemaRequest request)
    {
        var snapshot = await control.BeginRebuildAsync(request);
        snapshot.Rebuild.Should().NotBeNull();
        var rebuildId = snapshot.Rebuild!.RebuildId;
        do
        {
            snapshot = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
            {
                Schema = request,
                RebuildId = rebuildId,
            });
        }
        while (snapshot.Rebuild is { } progress
               && progress.NextProtocolOwnerIndex < progress.OwnerCount);

        snapshot.Rebuild.Should().NotBeNull();
        snapshot.Rebuild!.ProcessedRecordCount.Should().Be(0);
        snapshot.Rebuild.NextProtocolOwnerIndex.Should().Be(snapshot.Rebuild.OwnerCount);
        snapshot.Rebuild.LayoutProtocolPublished.Should().BeFalse();
        snapshot.Rebuild.NextOwnerIndex.Should().Be(0);
        snapshot.Rebuild.HasAfter.Should().BeFalse();
        return snapshot;
    }

    private async Task<SeededProvider> SeedLegacyRecordsAsync(
        string providerName,
        string stateName,
        int recordCount)
    {
        var options = GetPrimaryServices()
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(providerName);
        var layout = await _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName)
            .InitializeRoutingAsync(StorageLayout.CreateDescriptor(
                providerName,
                options.PartitionCount,
                options.JournalSegmentCapacity,
                options.MaximumJournalReplayEntries,
                options.VirtualSlotTargetCount));
        layout.GetDistinctOwners().Should().ContainSingle();
        var owner = layout.GetDistinctOwners().Single();
        var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, owner));
        var serializer = options.GrainStorageSerializer!;
        var persistence = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = options.JournalSegmentCapacity,
            MaximumJournalReplayEntries = options.MaximumJournalReplayEntries,
            CompactionThreshold = options.CompactionThreshold,
        };
        var city = $"control-recovery-{Guid.NewGuid():N}";
        var grainIds = new List<GrainId>(recordCount);
        var etags = new List<string>(recordCount);
        for (var index = 0; index < recordCount; index++)
        {
            var grainId = GrainId.Create(
                "schema-control-recovery",
                $"{index:D3}-{Guid.NewGuid():N}");
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            layout.GetOwner(slot).Should().Be(owner);
            var state = new VacancyState { City = city, Salary = 40_000 + index };
            var etag = await partition.WriteRoutedAsync(new RoutedStorageWriteRequest
            {
                Slot = slot,
                Epoch = layout.Epoch,
                Request = new StorageWriteRequest
                {
                    RecordKey = CreateRecordKey(stateName, grainId),
                    GrainId = grainId,
                    Payload = serializer.Serialize(state).ToArray(),
                    IndexEntries = [.. IndexMetadataProvider.Extract(stateName, state)],
                    Persistence = persistence,
                },
            });
            grainIds.Add(grainId);
            etags.Add(etag);
        }

        return new SeededProvider(layout, partition, city, grainIds, etags);
    }

    private (IStorageIndexSchemaGrain Control, StorageIndexSchemaRequest Request) CreateControl(
        string providerName,
        string stateName)
    {
        var definition = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(stateName);
        var request = StorageIndexSchema.CreateRequest(providerName, definition);
        var control = _fixture.Cluster.GrainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(providerName, stateName));
        return (control, request);
    }

    private async Task ScheduleNextControlWriteFaultAsync(
        IStorageIndexSchemaGrain control,
        PhysicalWriteFaultStage stage)
    {
        await WriteFaultInjectingGrainStorage.AddWriteFaultAsync(
            _fixture.Cluster.GrainFactory,
            control.GetGrainId(),
            ControlStateName,
            stage,
            call: 1);
    }

    private static async Task AssertInjectedFailureAsync(Func<Task> action)
    {
        var exception = await action.Should().ThrowAsync<Exception>();
        exception.Which.ToString().Should().Contain(
            WriteFaultInjectingGrainStorage.InjectedFailureMessage);
    }

    private IServiceProvider GetPrimaryServices()
    {
        return Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary).ServiceProvider;
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

    private sealed record SeededProvider(
        StorageLayoutSnapshot Layout,
        IStoragePartitionGrain Partition,
        string City,
        IReadOnlyList<GrainId> GrainIds,
        IReadOnlyList<string> ETags);
}
