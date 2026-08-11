using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageIndexSchemaLifecycleTests
{
    private readonly MemoryStorageFixture _fixture;

    public StorageIndexSchemaLifecycleTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void SearchableStorageIndexSchemaExceptionRoundTripsThroughOrleansAsException()
    {
        var serializer = GetPrimaryServices().GetRequiredService<Serializer>();
        Exception original = new SearchableStorageIndexSchemaException(
            "The registered schema is rebuilding.",
            new InvalidOperationException("materialization failed"));

        var payload = serializer.SerializeToArray(original);
        var copy = serializer.Deserialize<Exception>(payload);

        copy.Should().BeOfType<SearchableStorageIndexSchemaException>();
        copy.Message.Should().Be(original.Message);
        copy.InnerException.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Be("materialization failed");
    }

    [Fact]
    public async Task LegacyRecordIsRebuiltWithoutChangingEtagAndManagedQueriesFailClosed()
    {
        var providerName = MemoryStorageFixture.ManagedSchemaProviderName;
        var stateName = MemoryStorageFixture.ManagedSchemaStateName;
        var services = GetPrimaryServices();
        var storage = services.GetRequiredKeyedService<IGrainStorage>(providerName);
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var query = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var seeded = await SeedLegacyRecordsAsync(providerName, stateName, 1, "Moscow");
        var record = seeded.Records.Single();

        Func<Task> beforeRebuild = async () => await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            "Moscow");
        await beforeRebuild.Should().ThrowAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*not active*");

        var rebuilt = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        rebuilt.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        rebuilt.ProcessedRecordCount.Should().Be(1);
        rebuilt.Fingerprint.Should().NotBeNullOrWhiteSpace();

        var read = await record.Partition.ReadRoutedAsync(new RoutedStorageReadRequest
        {
            RecordKey = record.RecordKey,
            GrainId = record.GrainId,
            Slot = record.Slot,
            Epoch = seeded.Layout.Epoch,
        });
        read.ETag.Should().Be(
            record.ETag,
            "reindexing is derived-state maintenance, not an object write");
        var found = await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            "Moscow");
        found.Should().ContainSingle().Which.Should().Be(record.GrainId);

        var writable = new GrainState<VacancyState>
        {
            State = new VacancyState { City = "Kazan", Salary = 60_000 },
            ETag = record.ETag,
            RecordExists = true,
        };
        await storage.WriteStateAsync(stateName, record.GrainId, writable);
        var updated = await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            "Kazan");
        updated.Should().ContainSingle().Which.Should().Be(record.GrainId);
    }

    [Fact]
    public async Task BoundedRebuildPersistsARealPageCursorAndResumesAfterDeactivation()
    {
        var providerName = MemoryStorageFixture.PagedManagedSchemaProviderName;
        var stateName = MemoryStorageFixture.PagedManagedSchemaStateName;
        var services = GetPrimaryServices();
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var query = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var city = $"paged-{Guid.NewGuid():N}";
        var seeded = await SeedLegacyRecordsAsync(providerName, stateName, 70, city);
        var owners = seeded.Records.Select(static record => record.Owner).Distinct().ToArray();
        owners.Should().ContainSingle("all 70 records must exercise one partition cursor");

        var definition = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(stateName);
        var request = StorageIndexSchema.CreateRequest(providerName, definition);
        var control = _fixture.Cluster.GrainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(providerName, stateName));
        var snapshot = await control.BeginRebuildAsync(request);
        snapshot.Rebuild.Should().NotBeNull();
        var rebuildId = snapshot.Rebuild!.RebuildId;

        while (snapshot.Rebuild is { ProcessedRecordCount: 0 })
        {
            snapshot = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
            {
                Schema = request,
                RebuildId = rebuildId,
            });
        }

        snapshot.Rebuild.Should().NotBeNull();
        snapshot.Rebuild!.ProcessedRecordCount.Should().Be(StorageIndexSchema.RebuildPageSize);
        snapshot.Rebuild.HasAfter.Should().BeTrue();
        snapshot.Rebuild.After.IsDefault.Should().BeFalse();
        snapshot.Rebuild.NextOwnerIndex.Should().Be(0);

        var owner = owners.Single();
        var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, owner));
        await _fixture.Cluster.DeactivateAsync(partition);
        await _fixture.Cluster.DeactivateAsync(control);

        var resumed = await admin.GetIndexSchemaAsync<VacancyState>(stateName);
        resumed.State.Should().Be(SearchableStorageIndexSchemaState.Rebuilding);
        resumed.RebuildId.Should().Be(rebuildId);
        resumed.ProcessedRecordCount.Should().Be(StorageIndexSchema.RebuildPageSize);

        Func<Task> duringRebuild = async () => await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            city);
        await duringRebuild.Should().ThrowAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*still running*");

        var completed = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(70);
        var found = await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            city);
        found.Should().BeEquivalentTo(seeded.Records.Select(static record => record.GrainId));
    }

    [Fact]
    public async Task FreshRebuildBootstrapsLayoutAndProviderGateRejectsEveryUnregisteredPath()
    {
        var providerName = MemoryStorageFixture.FreshSchemaProviderName;
        var stateName = MemoryStorageFixture.FreshSchemaStateName;
        var services = GetPrimaryServices();
        var storage = services.GetRequiredKeyedService<IGrainStorage>(providerName);
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var query = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);

        (await admin.GetLayoutAsync()).Should().BeNull();
        var before = await admin.GetIndexSchemaAsync<VacancyState>(stateName);
        before.State.Should().Be(SearchableStorageIndexSchemaState.Uninitialized);
        (await admin.GetLayoutAsync()).Should().BeNull(
            "status reads do not create an empty provider namespace");

        var active = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        active.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        active.ProcessedRecordCount.Should().Be(0);
        (await admin.GetLayoutAsync()).Should().NotBeNull(
            "the first rebuild is the namespace bootstrap operation");
        var internalLayout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(providerName)
            .GetCurrentLayoutAsync();
        internalLayout!.IndexSchemaProtocolVersion.Should().Be(StorageIndexSchema.ProtocolVersion);

        const string unregisteredStateName = "unregistered-vacancy";
        var grainId = GrainId.Create("schema-gate", Guid.NewGuid().ToString("N"));
        var unwritten = new GrainState<VacancyState>
        {
            State = new VacancyState { City = "Moscow", Salary = 52_000 },
        };
        var pointRead = new GrainState<VacancyState>();
        await storage.ReadStateAsync(unregisteredStateName, grainId, pointRead);
        pointRead.RecordExists.Should().BeFalse("point reads do not interpret index entries");

        Func<Task> write = async () => await storage.WriteStateAsync(
            unregisteredStateName,
            grainId,
            unwritten);
        Func<Task> clear = async () => await storage.ClearStateAsync(
            unregisteredStateName,
            grainId,
            unwritten);
        Func<Task> find = async () => await query.FindAsync<VacancyState, string>(
            unregisteredStateName,
            state => state.City,
            "Moscow");
        var unregisteredQuery = query.Query<VacancyState>(unregisteredStateName)
            .Where(state => state.City == "Moscow");
        Func<Task> page = async () => await unregisteredQuery.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(1));
        Func<Task> facet = async () => await unregisteredQuery.ToDistinctFacetValuePageAsync(
            state => state.City,
            new SearchableStorageFacetPageRequest(1));

        foreach (var operation in new[] { write, clear, find, page, facet })
        {
            await operation.Should().ThrowAsync<SearchableStorageIndexSchemaException>();
        }

        var oldClient = new SearchableStorageClient(
            _fixture.Cluster.GrainFactory,
            providerName,
            partitionCount: 1);
        Func<Task> oldClientQuery = async () => await oldClient.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            "Moscow");
        await oldClientQuery.Should().ThrowAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*managed index schemas enabled*");
    }

    [Fact]
    public async Task FirstRebuildAdoptsALegacyLayoutBeforeEnablingManagedSchemas()
    {
        var providerName = MemoryStorageFixture.LegacyLayoutSchemaProviderName;
        var stateName = MemoryStorageFixture.LegacyLayoutSchemaStateName;
        var services = GetPrimaryServices();
        var options = services.GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(providerName);
        var layout = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var physical = services.GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
        await physical.WriteStateAsync(
            "layout",
            layout.GetGrainId(),
            new GrainState<StorageLayoutState>
            {
                State = new StorageLayoutState
                {
                    Initialized = true,
                    FormatVersion = StorageLayout.LegacyFormatVersion,
                    ProviderName = providerName,
                    PartitionCount = options.PartitionCount,
                    JournalSegmentCapacity = options.JournalSegmentCapacity,
                    MaximumJournalReplayEntries = options.MaximumJournalReplayEntries,
                },
            });

        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var active = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);

        active.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        active.ProcessedRecordCount.Should().Be(0);
        var adopted = await layout.GetCurrentLayoutAsync();
        adopted.Should().NotBeNull();
        adopted!.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
        adopted.Epoch.Should().Be(1);
        adopted.IndexSchemaProtocolVersion.Should().Be(StorageIndexSchema.ProtocolVersion);
    }

    [Fact]
    public async Task ApplicationSchemaVersionRebuildFailsDuringMovementAndConvergesAfterRescan()
    {
        var providerName = MemoryStorageFixture.VersionedSchemaProviderName;
        var stateName = MemoryStorageFixture.VersionedSchemaStateName;
        var services = GetPrimaryServices();
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var query = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var versionOne = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(
            stateName,
            applicationSchemaVersion: 1);
        var seeded = await SeedLegacyRecordsAsync(
            providerName,
            stateName,
            1,
            "Oslo",
            versionOne.Fingerprint);
        await EnableSchemaCapabilityAsync(providerName, seeded.Layout);

        var control = _fixture.Cluster.GrainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(providerName, stateName));
        var physical = services.GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
        await physical.WriteStateAsync(
            "index-schema",
            control.GetGrainId(),
            new GrainState<StorageIndexSchemaState>
            {
                State = new StorageIndexSchemaState
                {
                    Initialized = true,
                    ProtocolVersion = StorageIndexSchema.ProtocolVersion,
                    ProviderName = providerName,
                    StateName = stateName,
                    ActiveFingerprint = [.. versionOne.Fingerprint],
                    LastCompletedRecordCount = 1,
                },
            });

        Func<Task> oldStatus = async () => await admin.GetIndexSchemaAsync<VacancyState>(
            stateName,
            applicationSchemaVersion: 2,
            CancellationToken.None);
        await oldStatus.Should().ThrowAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*does not match*");

        var versionTwo = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(
            stateName,
            applicationSchemaVersion: 2);
        var request = StorageIndexSchema.CreateRequest(providerName, versionTwo);
        var started = await control.BeginRebuildAsync(request);
        var rebuildId = started.Rebuild!.RebuildId;
        var originalLayoutEpoch = started.Rebuild.LayoutEpoch;

        await admin.EnableMovementAsync(CancellationToken.None);

        var resetAfterMovementEnablement = await control.AdvanceRebuildAsync(
            new StorageIndexSchemaCommand
            {
                Schema = request,
                RebuildId = rebuildId,
            });
        resetAfterMovementEnablement.Rebuild.Should().NotBeNull();
        resetAfterMovementEnablement.Rebuild!.RebuildId.Should().Be(rebuildId);
        resetAfterMovementEnablement.Rebuild.LayoutEpoch.Should()
            .BeGreaterThan(originalLayoutEpoch);
        resetAfterMovementEnablement.Rebuild.ProcessedRecordCount.Should().Be(0);
        resetAfterMovementEnablement.Rebuild.NextProtocolOwnerIndex.Should().Be(0);
        resetAfterMovementEnablement.Rebuild.LayoutProtocolPublished.Should().BeFalse();
        resetAfterMovementEnablement.Rebuild.NextOwnerIndex.Should().Be(0);
        resetAfterMovementEnablement.Rebuild.OwnerCount.Should().Be(1);
        resetAfterMovementEnablement.Rebuild.HasAfter.Should().BeFalse(
            "a completed movement-enablement epoch change restarts the durable owner scan");

        var ownersEnabled = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
        {
            Schema = request,
            RebuildId = rebuildId,
        });
        ownersEnabled.Rebuild.Should().NotBeNull();
        ownersEnabled.Rebuild!.NextProtocolOwnerIndex.Should()
            .Be(ownersEnabled.Rebuild.OwnerCount);
        ownersEnabled.Rebuild.NextOwnerIndex.Should().Be(0);
        ownersEnabled.Rebuild.LayoutProtocolPublished.Should().BeFalse();

        // This call publishes the provider capability after the scan, then deliberately stops
        // before the separate control commit which activates the target generation.
        var published = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
        {
            Schema = request,
            RebuildId = rebuildId,
        });

        published.Rebuild.Should().NotBeNull(
            "the final activation is a separate durable control commit");
        published.Rebuild!.ProcessedRecordCount.Should().Be(1);
        published.Rebuild.NextOwnerIndex.Should().Be(published.Rebuild.OwnerCount);
        published.Rebuild.LayoutProtocolPublished.Should().BeTrue();
        published.ActiveFingerprint.Should().NotBeNull();
        published.ActiveFingerprint!.Should().Equal(versionOne.Fingerprint);

        var record = seeded.Records.Single();
        record.Owner.Should().Be(0);
        const int targetOwner = 1;
        var move = await admin.PlanMoveAsync(
            record.Slot,
            targetOwner,
            CancellationToken.None);

        Func<Task> resumeDuringMovement = async () => await admin
            .RebuildIndexSchemaAsync<VacancyState>(
                stateName,
                applicationSchemaVersion: 2,
                CancellationToken.None);
        await resumeDuringMovement.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot run at the same time*");

        await _fixture.Cluster.DeactivateAsync(control);
        var durableDuringMovement = await control.GetAsync(request);
        durableDuringMovement.Rebuild.Should().NotBeNull();
        durableDuringMovement.Rebuild!.RebuildId.Should().Be(rebuildId);
        durableDuringMovement.Rebuild.ProcessedRecordCount.Should().Be(1);
        durableDuringMovement.Rebuild.LayoutProtocolPublished.Should().BeTrue();

        var completedMove = await admin.ExecuteMoveAsync(move.MoveId, CancellationToken.None);
        completedMove.IsComplete.Should().BeTrue();
        completedMove.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
        var movedLayout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(providerName)
            .GetCurrentLayoutAsync();
        movedLayout.Should().NotBeNull();
        movedLayout!.Epoch.Should().BeGreaterThan(published.Rebuild.LayoutEpoch);
        movedLayout.GetOwner(record.Slot).Should().Be(targetOwner);
        movedLayout.CopyMoveIntent().Should().BeNull();
        var target = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, targetOwner));
        var targetAfterMove = await target.GetMovementStateAsync();

        // The target was activated while the move intent existed at this epoch. Retiring that
        // intent does not advance the epoch, so its ordinary layout cache still contains a stale
        // same-epoch intent. Protocol enablement must bypass that cache with an authoritative read.
        var restartedAfterMove = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
        {
            Schema = request,
            RebuildId = rebuildId,
        });
        restartedAfterMove.Rebuild.Should().NotBeNull();
        restartedAfterMove.Rebuild!.RebuildId.Should().Be(rebuildId);
        restartedAfterMove.Rebuild.LayoutEpoch.Should().Be(movedLayout.Epoch);
        restartedAfterMove.Rebuild.OwnerCount.Should()
            .Be(movedLayout.GetDistinctOwners().Length);
        restartedAfterMove.Rebuild.ProcessedRecordCount.Should().Be(0);
        restartedAfterMove.Rebuild.NextProtocolOwnerIndex.Should().Be(0);
        restartedAfterMove.Rebuild.LayoutProtocolPublished.Should().BeFalse();
        restartedAfterMove.Rebuild.NextOwnerIndex.Should().Be(0);
        restartedAfterMove.Rebuild.HasAfter.Should().BeFalse(
            "a completed slot move changes the routing boundary and restarts the durable owner scan");

        var active = await admin.RebuildIndexSchemaAsync<VacancyState>(
            stateName,
            applicationSchemaVersion: 2,
            CancellationToken.None);
        active.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        active.ProcessedRecordCount.Should().Be(1);
        active.Fingerprint.Should().Be(Convert.ToHexString(versionTwo.Fingerprint));
        var targetAfterRescan = await target.GetMovementStateAsync();
        targetAfterRescan.CommittedSequence.Should().Be(
            targetAfterMove.CommittedSequence,
            "the moved record already carries the target fingerprint and must not be reindexed again");
        var found = await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            "Oslo");
        found.Should().ContainSingle().Which.Should().Be(seeded.Records.Single().GrainId);

        var read = await target.ReadRoutedAsync(new RoutedStorageReadRequest
        {
            RecordKey = record.RecordKey,
            GrainId = record.GrainId,
            Slot = record.Slot,
            Epoch = movedLayout.Epoch,
        });
        read.ETag.Should().Be(record.ETag);
    }

    [Fact]
    public async Task AdoptionInvalidatesOldContinuationsAndExternalRegistryRestoresQueries()
    {
        var providerName = MemoryStorageFixture.ContinuationSchemaProviderName;
        var stateName = MemoryStorageFixture.ContinuationSchemaStateName;
        var services = GetPrimaryServices();
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var managed = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var city = $"continuation-{Guid.NewGuid():N}";
        await SeedLegacyRecordsAsync(providerName, stateName, 3, city);
        var options = CreateSchemaQueryOptions();
        var legacy = new SearchableStorageClient(
            _fixture.Cluster.GrainFactory,
            providerName,
            partitionCount: 1,
            options);
        var legacyQuery = legacy.Query<VacancyState>(stateName)
            .Where(state => state.City == city);
        var oldPage = await legacyQuery.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(1));
        oldPage.Items.Should().ContainSingle();
        oldPage.ContinuationToken.Should().NotBeNull();

        await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);
        var managedQuery = managed.Query<VacancyState>(stateName)
            .Where(state => state.City == city);
        Func<Task> resumeOldPage = async () => await managedQuery.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(1, oldPage.ContinuationToken));
        await resumeOldPage.Should().ThrowAsync<SearchableStorageInvalidContinuationTokenException>();

        Func<Task> oldClientAfterGate = async () => await legacyQuery.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(1));
        await oldClientAfterGate.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*explicit managed schema binding*");

        var registry = new SearchableStorageSchemaRegistry()
            .AddState<VacancyState>(stateName);
        var external = new SearchableStorageClient(
            _fixture.Cluster.GrainFactory,
            providerName,
            partitionCount: 1,
            options,
            registry);
        var currentPage = await external.Query<VacancyState>(stateName)
            .Where(state => state.City == city)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(3));
        currentPage.Items.Should().HaveCount(3);
        currentPage.ContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task MaterializationFailurePreservesProgressAndTheSameRebuildCanResume()
    {
        var providerName = MemoryStorageFixture.SchemaMaterializationFailureProviderName;
        var stateName = MemoryStorageFixture.SchemaMaterializationFailureStateName;
        const string rawIndexedValueCanary = "RAW-INDEX-VALUE-CANARY-4B982D2D";
        SchemaMaterializationFailureState.ThrowOnIndexAccess = false;

        try
        {
            var seeded = await SeedSchemaMaterializationRecordAsync(
                providerName,
                stateName,
                rawIndexedValueCanary);
            var record = seeded.Records.Single();
            var services = GetPrimaryServices();
            var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
            var query = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
            var definition = IndexMetadataProvider
                .GetSchemaDefinition<SchemaMaterializationFailureState>(stateName);
            var request = StorageIndexSchema.CreateRequest(providerName, definition);
            var control = _fixture.Cluster.GrainFactory.GetGrain<IStorageIndexSchemaGrain>(
                StorageIndexSchema.CreateGrainKey(providerName, stateName));

            SchemaMaterializationFailureState.ThrowOnIndexAccess = true;
            var snapshot = await control.BeginRebuildAsync(request);
            var rebuildId = snapshot.Rebuild!.RebuildId;
            while (snapshot.Rebuild is { } progress
                   && progress.NextProtocolOwnerIndex < progress.OwnerCount)
            {
                snapshot = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
                {
                    Schema = request,
                    RebuildId = rebuildId,
                });
            }

            var beforeFailure = snapshot.Rebuild!;
            beforeFailure.NextProtocolOwnerIndex.Should().Be(beforeFailure.OwnerCount);
            beforeFailure.ProcessedRecordCount.Should().Be(0);
            beforeFailure.NextOwnerIndex.Should().Be(0);
            beforeFailure.HasAfter.Should().BeFalse();

            Func<Task> rebuild = async () => await admin
                .RebuildIndexSchemaAsync<SchemaMaterializationFailureState>(stateName);
            var failure = await rebuild.Should().ThrowExactlyAsync<InvalidOperationException>();
            failure.Which.Message.Should().Contain(providerName)
                .And.Contain(stateName)
                .And.Contain(record.GrainId.ToString())
                .And.Contain($"physical owner {record.Owner}")
                .And.Contain(typeof(InvalidDataException).FullName!)
                .And.Contain("Application exception details are intentionally omitted")
                .And.NotContain(SchemaMaterializationFailureState.FailureMessagePrefix)
                .And.NotContain(rawIndexedValueCanary);
            failure.Which.InnerException.Should().BeNull();
            failure.Which.ToString().Should()
                .NotContain(SchemaMaterializationFailureState.FailureMessagePrefix)
                .And.NotContain(rawIndexedValueCanary);

            await _fixture.Cluster.DeactivateAsync(record.Partition);
            await _fixture.Cluster.DeactivateAsync(control);
            var durable = await control.GetAsync(request);
            durable.Rebuild.Should().NotBeNull();
            durable.Rebuild!.RebuildId.Should().Be(rebuildId);
            durable.Rebuild.NextProtocolOwnerIndex.Should()
                .Be(beforeFailure.NextProtocolOwnerIndex);
            durable.Rebuild.ProcessedRecordCount.Should()
                .Be(beforeFailure.ProcessedRecordCount);
            durable.Rebuild.NextOwnerIndex.Should().Be(beforeFailure.NextOwnerIndex);
            durable.Rebuild.HasAfter.Should().Be(beforeFailure.HasAfter);
            durable.Rebuild.After.Should().Be(beforeFailure.After);
            durable.Rebuild.LayoutProtocolPublished.Should()
                .Be(beforeFailure.LayoutProtocolPublished);

            SchemaMaterializationFailureState.ThrowOnIndexAccess = false;
            var resumable = await admin.GetIndexSchemaAsync<SchemaMaterializationFailureState>(
                stateName);
            resumable.State.Should().Be(SearchableStorageIndexSchemaState.Rebuilding);
            resumable.RebuildId.Should().Be(rebuildId);
            resumable.ProcessedRecordCount.Should().Be(0);

            var completed = await admin
                .RebuildIndexSchemaAsync<SchemaMaterializationFailureState>(stateName);
            completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
            completed.ProcessedRecordCount.Should().Be(1);
            var found = await query.FindAsync<SchemaMaterializationFailureState, string>(
                stateName,
                state => state.City,
                rawIndexedValueCanary);
            found.Should().ContainSingle().Which.Should().Be(record.GrainId);
        }
        finally
        {
            SchemaMaterializationFailureState.ThrowOnIndexAccess = false;
        }
    }

    private async Task<SeededRecords> SeedLegacyRecordsAsync(
        string providerName,
        string stateName,
        int recordCount,
        string city,
        byte[]? legacyScopeFingerprint = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordCount);
        var services = GetPrimaryServices();
        var options = services.GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(providerName);
        var serializer = options.GrainStorageSerializer!;
        var layoutGrain = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var layout = await layoutGrain.InitializeRoutingAsync(StorageLayout.CreateDescriptor(
            providerName,
            options.PartitionCount,
            options.JournalSegmentCapacity,
            options.MaximumJournalReplayEntries,
            options.VirtualSlotTargetCount));
        var persistence = CreatePersistence(options);
        var records = new List<SeededRecord>(recordCount);
        for (var index = 0; index < recordCount; index++)
        {
            var grainId = GrainId.Create(
                "managed-schema",
                $"record-{index:D3}-{Guid.NewGuid():N}");
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            var owner = layout.GetOwner(slot);
            var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, owner));
            var state = new VacancyState { City = city, Salary = 50_000 + index };
            var recordKey = CreateRecordKey(stateName, grainId);
            var etag = await partition.WriteRoutedAsync(new RoutedStorageWriteRequest
            {
                Slot = slot,
                Epoch = layout.Epoch,
                Request = new StorageWriteRequest
                {
                    RecordKey = recordKey,
                    GrainId = grainId,
                    Payload = serializer.Serialize(state).ToArray(),
                    IndexEntries =
                    [
                        .. IndexMetadataProvider.Extract(
                            stateName,
                            state,
                            legacyScopeFingerprint),
                    ],
                    Persistence = persistence,
                },
            });
            records.Add(new SeededRecord(grainId, recordKey, slot, owner, partition, etag));
        }

        return new SeededRecords(layout, records);
    }

    private async Task<SeededRecords> SeedSchemaMaterializationRecordAsync(
        string providerName,
        string stateName,
        string city)
    {
        var services = GetPrimaryServices();
        var options = services.GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(providerName);
        var layout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(providerName)
            .InitializeRoutingAsync(StorageLayout.CreateDescriptor(
                providerName,
                options.PartitionCount,
                options.JournalSegmentCapacity,
                options.MaximumJournalReplayEntries,
                options.VirtualSlotTargetCount));
        layout.GetDistinctOwners().Should().ContainSingle();
        var grainId = GrainId.Create(
            "schema-materialization",
            Guid.NewGuid().ToString("N"));
        var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
        var owner = layout.GetOwner(slot);
        var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, owner));
        var state = new SchemaMaterializationFailureState { StoredCity = city };
        var recordKey = CreateRecordKey(stateName, grainId);
        var etag = await partition.WriteRoutedAsync(new RoutedStorageWriteRequest
        {
            Slot = slot,
            Epoch = layout.Epoch,
            Request = new StorageWriteRequest
            {
                RecordKey = recordKey,
                GrainId = grainId,
                Payload = options.GrainStorageSerializer!.Serialize(state).ToArray(),
                IndexEntries = [.. IndexMetadataProvider.Extract(stateName, state)],
                Persistence = CreatePersistence(options),
            },
        });
        return new SeededRecords(
            layout,
            [new SeededRecord(grainId, recordKey, slot, owner, partition, etag)]);
    }

    private async Task EnableSchemaCapabilityAsync(
        string providerName,
        StorageLayoutSnapshot layout)
    {
        var options = GetPrimaryServices()
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(providerName);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
        var layoutGrain = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var layoutRequest = new StorageIndexSchemaLayoutProtocolRequest
        {
            ProtocolVersion = StorageIndexSchema.ProtocolVersion,
            LayoutEpoch = layout.Epoch,
            LayoutFingerprint = [.. layoutFingerprint],
            EnablementId = Guid.NewGuid(),
        };
        await layoutGrain.BeginIndexSchemaProtocolEnablementAsync(layoutRequest);
        foreach (var owner in layout.GetDistinctOwners())
        {
            var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, owner));
            await partition.EnableIndexSchemaProtocolAsync(
                new StorageIndexSchemaPartitionProtocolRequest
                {
                    ProtocolVersion = StorageIndexSchema.ProtocolVersion,
                    ProviderName = providerName,
                    LayoutEpoch = layout.Epoch,
                    LayoutFingerprint = [.. layoutFingerprint],
                    Persistence = CreatePersistence(options),
                });
        }

        await layoutGrain.EnableIndexSchemaProtocolAsync(layoutRequest);
    }

    private IServiceProvider GetPrimaryServices()
    {
        return Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary).ServiceProvider;
    }

    private static SearchableStorageQueryOptions CreateSchemaQueryOptions()
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "schema-lifecycle-tests",
            Enumerable.Range(1, 32).Select(static value => checked((byte)value)).ToArray());
        return options;
    }

    private static StoragePersistenceSettings CreatePersistence(SearchableStorageOptions options)
    {
        return new StoragePersistenceSettings
        {
            JournalSegmentCapacity = options.JournalSegmentCapacity,
            MaximumJournalReplayEntries = options.MaximumJournalReplayEntries,
            CompactionThreshold = options.CompactionThreshold,
        };
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

    private sealed record SeededRecords(
        StorageLayoutSnapshot Layout,
        IReadOnlyList<SeededRecord> Records);

    private sealed record SeededRecord(
        GrainId GrainId,
        string RecordKey,
        int Slot,
        int Owner,
        IStoragePartitionGrain Partition,
        string ETag);
}
