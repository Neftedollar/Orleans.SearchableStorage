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
public sealed class StorageIndexSchemaAcceptanceTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);
    private readonly MemoryStorageFixture _fixture;

    public StorageIndexSchemaAcceptanceTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ProviderFenceCoversNoIndexAndNullStatesWithoutInventingIndexEntries()
    {
        var providerName = MemoryStorageFixture.MultiStateSchemaProviderName;
        var noIndexStateName = MemoryStorageFixture.NoIndexSchemaStateName;
        var nullableStateName = MemoryStorageFixture.NullableSchemaStateName;
        var noIndexRecord = (await SeedLegacyRecordsAsync(
            providerName,
            noIndexStateName,
            [new NoIndexSchemaState { Value = "stored without an index" }])).Single();
        var nullableRecord = (await SeedLegacyRecordsAsync(
            providerName,
            nullableStateName,
            [new NullableQueryState { Score = null }])).Single();
        var services = GetPrimaryServices();
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        var storage = services.GetRequiredKeyedService<IGrainStorage>(providerName);
        var omittedState = new GrainState<NoIndexSchemaState>
        {
            State = new NoIndexSchemaState { Value = "must not be written" },
        };
        var omittedGrainId = GrainId.Create("omitted-schema-state", Guid.NewGuid().ToString("N"));
        Func<Task>[] omittedMutations =
        [
            async () => await storage.WriteStateAsync("omitted", omittedGrainId, omittedState),
            async () => await storage.ClearStateAsync("omitted", omittedGrainId, omittedState),
        ];

        foreach (var mutate in omittedMutations)
        {
            await mutate.Should().ThrowExactlyAsync<SearchableStorageIndexSchemaException>()
                .WithMessage("*managed schema declarations*not registered*");
        }

        var noIndex = await admin.RebuildIndexSchemaAsync<NoIndexSchemaState>(noIndexStateName);

        noIndex.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        noIndex.ProcessedRecordCount.Should().Be(1);
        var noIndexDefinition = IndexMetadataProvider
            .GetSchemaDefinition<NoIndexSchemaState>(noIndexStateName);
        noIndexDefinition.Indexes.Should().BeEmpty();
        AssertReindexedWithoutValues(
            await ReadLatestJournalRecordAsync(noIndexRecord),
            noIndexDefinition.Fingerprint);

        var queryClient = services
            .GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var noIndexRead = new GrainState<NoIndexSchemaState>();
        await storage.ReadStateAsync(noIndexStateName, noIndexRecord.GrainId, noIndexRead);
        noIndexRead.RecordExists.Should().BeTrue();
        noIndexRead.State.Value.Should().Be("stored without an index");
        Func<Task> queryBeforeNullableActivation = async () => await queryClient
            .FindAsync<NullableQueryState, int?>(
                nullableStateName,
                state => state.Score,
                17);
        await queryBeforeNullableActivation.Should()
            .ThrowAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*not active*");

        var nullableDefinition = IndexMetadataProvider
            .GetSchemaDefinition<NullableQueryState>(nullableStateName);
        var nullableRequest = StorageIndexSchema.CreateRequest(providerName, nullableDefinition);
        var nullableControl = GetSchemaControl(providerName, nullableStateName);
        var started = await nullableControl.BeginRebuildAsync(nullableRequest);
        var rebuildId = started.Rebuild!.RebuildId;

        var fenced = await nullableControl.AdvanceRebuildAsync(new StorageIndexSchemaCommand
        {
            Schema = nullableRequest,
            RebuildId = rebuildId,
        });
        fenced.Rebuild.Should().NotBeNull();
        fenced.Rebuild!.NextProtocolOwnerIndex.Should().Be(1);
        var sharedLayout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(providerName)
            .GetCurrentLayoutAsync();
        sharedLayout!.IndexSchemaProtocolVersion.Should().Be(StorageIndexSchema.ProtocolVersion);
        sharedLayout.CopyIndexSchemaEnablement()!.EnablementId.Should().Be(rebuildId);

        var nullable = await admin.RebuildIndexSchemaAsync<NullableQueryState>(nullableStateName);

        nullable.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        nullable.ProcessedRecordCount.Should().Be(1);
        AssertReindexedWithoutValues(
            await ReadLatestJournalRecordAsync(nullableRecord),
            nullableDefinition.Fingerprint);
        var releasedLayout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(providerName)
            .GetCurrentLayoutAsync();
        releasedLayout!.IndexSchemaProtocolVersion.Should().Be(StorageIndexSchema.ProtocolVersion);
        releasedLayout.CopyIndexSchemaEnablement().Should().BeNull();
        (await admin.GetIndexSchemaAsync<NoIndexSchemaState>(noIndexStateName))
            .State.Should().Be(SearchableStorageIndexSchemaState.Active);
        (await admin.GetIndexSchemaAsync<NullableQueryState>(nullableStateName))
            .State.Should().Be(SearchableStorageIndexSchemaState.Active);
        var nullableRead = new GrainState<NullableQueryState>();
        await storage.ReadStateAsync(nullableStateName, nullableRecord.GrainId, nullableRead);
        nullableRead.RecordExists.Should().BeTrue();
        nullableRead.State.Score.Should().BeNull();
        (await queryClient.FindAsync<NullableQueryState, int?>(
                nullableStateName,
                state => state.Score,
                17))
            .Should().BeEmpty();
    }

    [Fact]
    public async Task IncompatibleSerializedPayloadPreservesTheRebuildIdAndResumesAfterRepair()
    {
        var providerName = MemoryStorageFixture.CorruptPayloadSchemaProviderName;
        var stateName = MemoryStorageFixture.CorruptPayloadSchemaStateName;
        var record = (await SeedLegacyRecordsAsync(
            providerName,
            stateName,
            [new VacancyState { City = "Tallinn", Salary = 75_000 }])).Single();
        var options = GetOptions(providerName);
        var incompatiblePayload = options.GrainStorageSerializer!
            .Serialize("this is not a VacancyState")
            .ToArray();
        await ReplaceJournalPayloadAsync(record, incompatiblePayload);
        var definition = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(stateName);
        var request = StorageIndexSchema.CreateRequest(providerName, definition);
        var control = GetSchemaControl(providerName, stateName);
        var started = await control.BeginRebuildAsync(request);
        var rebuildId = started.Rebuild!.RebuildId;
        var services = GetPrimaryServices();
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);

        Func<Task> rebuild = async () => await admin.RebuildIndexSchemaAsync<VacancyState>(
            stateName);
        await rebuild.Should().ThrowAsync<InvalidOperationException>();

        var failed = await control.GetAsync(request);
        failed.Rebuild.Should().NotBeNull();
        failed.Rebuild!.RebuildId.Should().Be(rebuildId);
        failed.Rebuild.ProcessedRecordCount.Should().Be(0);
        failed.Rebuild.NextOwnerIndex.Should().Be(0);

        await ReplaceJournalPayloadAsync(record, record.SerializedPayload);

        var completed = await admin.RebuildIndexSchemaAsync<VacancyState>(stateName);

        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(1);
        var query = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var found = await query.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            "Tallinn");
        found.Should().ContainSingle().Which.Should().Be(record.GrainId);
    }

    [Fact]
    public async Task CancelingAnInflightRebuildTurnLeavesTheSameDurableRebuildResumable()
    {
        var providerName = MemoryStorageFixture.CancelableSchemaProviderName;
        var stateName = MemoryStorageFixture.CancelableSchemaStateName;
        var record = (await SeedLegacyRecordsAsync(
            providerName,
            stateName,
            [new BlockingSchemaState { StoredCity = "Helsinki" }])).Single();
        var definition = IndexMetadataProvider.GetSchemaDefinition<BlockingSchemaState>(stateName);
        var request = StorageIndexSchema.CreateRequest(providerName, definition);
        var control = GetSchemaControl(providerName, stateName);
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

        var admin = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);
        using var cancellation = new CancellationTokenSource();
        BlockingSchemaState.BeginBlocking();
        try
        {
            var rebuild = admin.RebuildIndexSchemaAsync<BlockingSchemaState>(
                stateName,
                cancellation.Token);
            await BlockingSchemaState.WaitUntilBlockedAsync(TestTimeout);
            await cancellation.CancelAsync();

            Func<Task> wait = async () => await rebuild;
            await wait.Should().ThrowAsync<OperationCanceledException>();
        }
        finally
        {
            BlockingSchemaState.ReleaseBlockedGetter();
        }

        var durable = await control.GetAsync(request).WaitAsync(TestTimeout);
        durable.Rebuild.Should().NotBeNull();
        durable.Rebuild!.RebuildId.Should().Be(rebuildId);
        durable.Rebuild.ProcessedRecordCount.Should().Be(1);

        var completed = await admin.RebuildIndexSchemaAsync<BlockingSchemaState>(stateName);

        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(1);
        var query = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var found = await query.FindAsync<BlockingSchemaState, string>(
            stateName,
            state => state.City,
            "Helsinki");
        found.Should().ContainSingle().Which.Should().Be(record.GrainId);
    }

    [Fact]
    public async Task DistinctFacetContinuationIsRejectedWhenLegacyStateAdoptsManagedV2()
    {
        var providerName = MemoryStorageFixture.FacetGenerationSchemaProviderName;
        var stateName = MemoryStorageFixture.FacetGenerationSchemaStateName;
        await SeedLegacyRecordsAsync(
            providerName,
            stateName,
            [
                new VacancyState { City = "Amsterdam", Salary = 70_000 },
                new VacancyState { City = "Berlin", Salary = 80_000 },
                new VacancyState { City = "Copenhagen", Salary = 90_000 },
            ]);
        var legacy = new SearchableStorageClient(
            _fixture.Cluster.GrainFactory,
            providerName,
            partitionCount: 1,
            CreateSchemaQueryOptions());
        var legacyQuery = legacy.Query<VacancyState>(stateName);
        var first = await legacyQuery.ToDistinctFacetValuePageAsync(
            state => state.City,
            new SearchableStorageFacetPageRequest(1));
        first.Items.Should().ContainSingle();
        first.ContinuationToken.Should().NotBeNull();
        var services = GetPrimaryServices();
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(providerName);

        await admin.RebuildIndexSchemaAsync<VacancyState>(
            stateName,
            applicationSchemaVersion: 2,
            cancellationToken: CancellationToken.None);

        var managed = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
        var managedQuery = managed.Query<VacancyState>(stateName);
        Func<Task> resumeOldFacet = async () => await managedQuery
            .ToDistinctFacetValuePageAsync(
                state => state.City,
                new SearchableStorageFacetPageRequest(1, first.ContinuationToken));
        await resumeOldFacet.Should()
            .ThrowAsync<SearchableStorageInvalidContinuationTokenException>();

        var current = await managedQuery.ToDistinctFacetValuePageAsync(
            state => state.City,
            new SearchableStorageFacetPageRequest(4));
        current.Items.Should().BeEquivalentTo(["Amsterdam", "Berlin", "Copenhagen"]);
        current.ContinuationToken.Should().BeNull();
    }

    private async Task<IReadOnlyList<SeededRecord>> SeedLegacyRecordsAsync<TState>(
        string providerName,
        string stateName,
        IReadOnlyList<TState> states)
    {
        states.Should().NotBeEmpty();
        var options = GetOptions(providerName);
        var layout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(providerName)
            .InitializeRoutingAsync(StorageLayout.CreateDescriptor(
                providerName,
                options.PartitionCount,
                options.JournalSegmentCapacity,
                options.MaximumJournalReplayEntries,
                options.VirtualSlotTargetCount));
        var records = new List<SeededRecord>(states.Count);
        foreach (var state in states)
        {
            var grainId = GrainId.Create("schema-acceptance", Guid.NewGuid().ToString("N"));
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            var owner = layout.GetOwner(slot);
            var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, owner));
            var recordKey = CreateRecordKey(stateName, grainId);
            var payload = options.GrainStorageSerializer!.Serialize(state).ToArray();
            await partition.WriteRoutedAsync(new RoutedStorageWriteRequest
            {
                Slot = slot,
                Epoch = layout.Epoch,
                Request = new StorageWriteRequest
                {
                    RecordKey = recordKey,
                    GrainId = grainId,
                    Payload = payload,
                    IndexEntries = [.. IndexMetadataProvider.Extract(stateName, state)],
                    Persistence = CreatePersistence(options),
                },
            });
            records.Add(new SeededRecord(
                providerName,
                grainId,
                recordKey,
                owner,
                partition,
                payload));
        }

        return records;
    }

    private async Task ReplaceJournalPayloadAsync(SeededRecord record, byte[] payload)
    {
        var options = GetOptions(record.ProviderName);
        var journal = GetFirstJournalSegment(record.ProviderName, record.Owner, options);
        await _fixture.Cluster.DeactivateAsync(record.Partition);
        await _fixture.Cluster.DeactivateAsync(journal);
        var physical = GetPrimaryServices().GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
        var durable = new GrainState<StorageJournalSegmentState>();
        await physical.ReadStateAsync("journal", journal.GetGrainId(), durable);
        durable.RecordExists.Should().BeTrue();
        var entryIndex = durable.State.Entries.FindIndex(
            entry => entry.Operation == StorageJournalOperation.Upsert
                && string.Equals(entry.RecordKey, record.RecordKey, StringComparison.Ordinal));
        entryIndex.Should().BeGreaterThanOrEqualTo(0);
        durable.State.Entries[entryIndex] = CopyWithPayload(
            durable.State.Entries[entryIndex],
            payload);
        await physical.WriteStateAsync("journal", journal.GetGrainId(), durable);
    }

    private async Task<StoredRecord> ReadLatestJournalRecordAsync(SeededRecord record)
    {
        var options = GetOptions(record.ProviderName);
        var journal = GetFirstJournalSegment(record.ProviderName, record.Owner, options);
        var state = await journal.ReadAsync();
        return state.Entries
            .Where(entry => string.Equals(
                entry.RecordKey,
                record.RecordKey,
                StringComparison.Ordinal))
            .OrderBy(static entry => entry.Sequence)
            .Last()
            .Record!;
    }

    private IStorageJournalSegmentGrain GetFirstJournalSegment(
        string providerName,
        int owner,
        SearchableStorageOptions options)
    {
        var partitionKey = StorageLayout.CreatePartitionKey(providerName, owner);
        var slotCount = StoragePersistence.GetJournalSlotCount(
            options.MaximumJournalReplayEntries,
            options.JournalSegmentCapacity);
        return _fixture.Cluster.GrainFactory.GetGrain<IStorageJournalSegmentGrain>(
            StoragePersistence.CreateJournalSlotKey(partitionKey, slotIndex: 0, slotCount));
    }

    private IStorageIndexSchemaGrain GetSchemaControl(string providerName, string stateName)
    {
        return _fixture.Cluster.GrainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(providerName, stateName));
    }

    private SearchableStorageOptions GetOptions(string providerName)
    {
        return GetPrimaryServices()
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(providerName);
    }

    private IServiceProvider GetPrimaryServices()
    {
        return Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary).ServiceProvider;
    }

    private static void AssertReindexedWithoutValues(
        StoredRecord record,
        byte[] expectedFingerprint)
    {
        record.IndexEntries.Should().BeEmpty();
        record.IndexSchemaFingerprint.Should().Equal(expectedFingerprint);
    }

    private static StorageJournalEntry CopyWithPayload(
        StorageJournalEntry entry,
        byte[] payload)
    {
        var record = entry.Record
            ?? throw new InvalidOperationException("The durable upsert omitted its record.");
        var copiedRecord = StoragePersistenceStateCopy.CopyRecord(record)!;
        return new StorageJournalEntry
        {
            Sequence = entry.Sequence,
            WriterEpoch = entry.WriterEpoch,
            OperationId = entry.OperationId,
            PreviousOperationId = entry.PreviousOperationId,
            Operation = entry.Operation,
            RecordKey = entry.RecordKey,
            ExpectedETag = entry.ExpectedETag,
            Record = new StoredRecord
            {
                GrainId = record.GrainId,
                Payload = [.. payload],
                ETag = record.ETag,
                IndexEntries = copiedRecord.IndexEntries,
                IndexSchemaFingerprint = record.IndexSchemaFingerprint is null
                    ? null
                    : [.. record.IndexSchemaFingerprint],
            },
            NextVersionAfter = entry.NextVersionAfter,
            Move = entry.Move?.Copy(),
        };
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

    private sealed record SeededRecord(
        string ProviderName,
        GrainId GrainId,
        string RecordKey,
        int Owner,
        IStoragePartitionGrain Partition,
        byte[] SerializedPayload);
}
