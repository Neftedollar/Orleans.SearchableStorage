using System.Collections;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.Serialization;
using Orleans.Serialization.Activators;
using Orleans.Serialization.Serializers;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageCapacityGuardrailTests
{
    [Fact]
    public void RecordPayloadAndKeyUseStableCanonicalByteBoundaries()
    {
        var exactPayload = CreateRecord(
            payload: new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes]);
        var oversizedPayload = CreateRecord(
            payload: new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1]);
        var oversizedKey = new string(
            'k',
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes / sizeof(char));

        Action validatePayload = () => StorageCapacityGuardrails.ValidateRecord("record", oversizedPayload);
        Action validateKey = () => StorageCapacityGuardrails.ValidateRecord(oversizedKey, CreateRecord());

        StorageCapacityGuardrails.ValidateRecord("record", exactPayload).Should().BePositive();
        AssertCapacityFailure(
            validatePayload,
            StorageCapacityGuardrails.RecordPayloadBytes,
            SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1L,
            SearchableStorageCapacityLimits.MaximumRecordPayloadBytes);
        validateKey.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordKeyBytes);
    }

    [Fact]
    public void GrainIdTypeAndKeyAcceptExactLimitsAndRejectOneAdditionalByte()
    {
        var exactType = GrainId.Create(
            new GrainType(new byte[SearchableStorageCapacityLimits.MaximumGrainTypeBytes]),
            new IdSpan([1]));
        var exactKey = GrainId.Create(
            new GrainType([1]),
            new IdSpan(new byte[SearchableStorageCapacityLimits.MaximumGrainKeyBytes]));
        var oversizedType = GrainId.Create(
            new GrainType(new byte[SearchableStorageCapacityLimits.MaximumGrainTypeBytes + 1]),
            new IdSpan([1]));
        var oversizedKey = GrainId.Create(
            new GrainType([1]),
            new IdSpan(new byte[SearchableStorageCapacityLimits.MaximumGrainKeyBytes + 1]));
        var emptyType = GrainId.Create(new GrainType([]), new IdSpan([1]));
        var emptyKey = GrainId.Create(new GrainType([1]), new IdSpan([]));

        StorageCapacityGuardrails.ValidateRecord("exact-type", CreateRecord(grainId: exactType));
        StorageCapacityGuardrails.ValidateRecord("exact-key", CreateRecord(grainId: exactKey));
        Action validateType = () => StorageCapacityGuardrails.ValidateRecord(
            "oversized-type",
            CreateRecord(grainId: oversizedType));
        Action validateKey = () => StorageCapacityGuardrails.ValidateRecord(
            "oversized-key",
            CreateRecord(grainId: oversizedKey));
        Action validateEmptyType = () => StorageCapacityGuardrails.ValidateRecord(
            "empty-type",
            CreateRecord(grainId: emptyType));
        Action validateEmptyKey = () => StorageCapacityGuardrails.ValidateRecord(
            "empty-key",
            CreateRecord(grainId: emptyKey));

        AssertCapacityFailure(
            validateType,
            StorageCapacityGuardrails.GrainTypeBytes,
            SearchableStorageCapacityLimits.MaximumGrainTypeBytes + 1L,
            SearchableStorageCapacityLimits.MaximumGrainTypeBytes);
        AssertCapacityFailure(
            validateKey,
            StorageCapacityGuardrails.GrainKeyBytes,
            SearchableStorageCapacityLimits.MaximumGrainKeyBytes + 1L,
            SearchableStorageCapacityLimits.MaximumGrainKeyBytes);
        validateEmptyType.Should().Throw<ArgumentException>()
            .WithMessage("*non-empty type and key*");
        validateEmptyKey.Should().Throw<ArgumentException>()
            .WithMessage("*non-empty type and key*");

        var movedExact = StorageMoveRecordCodec.Encode("moved", CreateRecord(grainId: exactType));
        StorageCapacityGuardrails.ValidateMoveRecord(movedExact).Should().BePositive();
        var movedOversized = new StorageMoveRecord
        {
            RecordKey = [.. movedExact.RecordKey],
            Record = new StorageMoveStoredRecord
            {
                GrainType = new byte[SearchableStorageCapacityLimits.MaximumGrainTypeBytes + 1],
                GrainKey = [.. movedExact.Record.GrainKey],
                Payload = [.. movedExact.Record.Payload!],
                ETag = [.. movedExact.Record.ETag],
                IndexEntries = movedExact.Record.IndexEntries.Select(static entry => new StorageMoveIndexEntry
                {
                    Scope = [.. entry.Scope],
                    Kind = entry.Kind,
                    Value = new StorageMoveIndexValue
                    {
                        Kind = entry.Value.Kind,
                        Text = StorageMoveRecordCodec.CopyText(entry.Value.Text),
                        PrimitiveBits = [.. entry.Value.PrimitiveBits],
                    },
                }).ToList(),
                IndexSchemaFingerprint = movedExact.Record.IndexSchemaFingerprint is null
                    ? null
                    : [.. movedExact.Record.IndexSchemaFingerprint],
            },
        };
        Action validateMoved = () => StorageCapacityGuardrails.ValidateMoveRecord(movedOversized);
        validateMoved.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.GrainTypeBytes);
    }

    [Fact]
    public void SchemaRebuildRejectsOversizedDurableAndRequestFrontiers()
    {
        var oversized = GrainId.Create(
            new GrainType(new byte[SearchableStorageCapacityLimits.MaximumGrainTypeBytes + 1]),
            new IdSpan([1]));
        var state = new StorageIndexSchemaState
        {
            Initialized = true,
            ProtocolVersion = StorageIndexSchema.ProtocolVersion,
            ProviderName = "capacity",
            StateName = "state",
            Rebuild = new StorageIndexSchemaRebuildIntent
            {
                RebuildId = Guid.NewGuid(),
                SchemaKey = new byte[IndexSchemaDefinition.FingerprintLength],
                TargetFingerprint = new byte[IndexSchemaDefinition.FingerprintLength],
                LayoutEpoch = 1,
                LayoutFingerprint = new byte[IndexSchemaDefinition.FingerprintLength],
                OwnerCount = 1,
                NextProtocolOwnerIndex = 1,
                HasAfter = true,
                After = oversized,
            },
        };
        var request = new StorageIndexSchemaRebuildPageRequest
        {
            ProviderName = "capacity",
            StateName = "state",
            SchemaKey = new byte[IndexSchemaDefinition.FingerprintLength],
            TargetFingerprint = new byte[IndexSchemaDefinition.FingerprintLength],
            HasAfter = true,
            After = oversized,
            PageSize = 1,
            Persistence = new StoragePersistenceSettings(),
        };

        Action validateDurable = () => StorageIndexSchemaGrain.ValidateState(state);
        Action validateRequest = () => StoragePartitionGrain.ValidateRebuildPageFrontier(request);

        validateDurable.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.GrainTypeBytes);
        validateRequest.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.GrainTypeBytes);
    }

    [Fact]
    public void IndexEntryCountIsBoundedPerScopeAndPerRecord()
    {
        var sameScope = Enumerable.Range(
                0,
                SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope + 1)
            .Select(_ => CreateIndexEntry("same"))
            .ToList();
        var distinctScopes = Enumerable.Range(
                0,
                SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord + 1)
            .Select(index => CreateIndexEntry($"scope-{index}"))
            .ToList();

        Action validateScope = () => StorageCapacityGuardrails.ValidateRecord(
            "scope-record",
            CreateRecord(indexEntries: sameScope));
        Action validateTotal = () => StorageCapacityGuardrails.ValidateRecord(
            "total-record",
            CreateRecord(indexEntries: distinctScopes));

        AssertCapacityFailure(
            validateScope,
            StorageCapacityGuardrails.RecordScopeIndexEntries,
            SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope + 1L,
            SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope);
        AssertCapacityFailure(
            validateTotal,
            StorageCapacityGuardrails.RecordIndexEntries,
            SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord + 1L,
            SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord);
    }

    [Fact]
    public void ElementCapsPrecedeTraversalOfMalformedStoredAndMovedEntries()
    {
        var stored = CreateRecord(indexEntries: Enumerable.Repeat<IndexEntry>(
                null!,
                SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord + 1)
            .ToList());
        var moved = StorageMoveRecordCodec.Encode("moved", CreateRecord());
        var malformedMoved = new StorageMoveRecord
        {
            RecordKey = [.. moved.RecordKey],
            Record = new StorageMoveStoredRecord
            {
                GrainType = [.. moved.Record.GrainType],
                GrainKey = [.. moved.Record.GrainKey],
                Payload = [.. moved.Record.Payload!],
                ETag = [.. moved.Record.ETag],
                IndexEntries = Enumerable.Repeat<StorageMoveIndexEntry>(
                        null!,
                        SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord + 1)
                    .ToList(),
                IndexSchemaFingerprint = null,
            },
        };

        Action validateStored = () => StorageCapacityGuardrails.ValidateRecord("stored", stored);
        Action validateMoved = () => StorageCapacityGuardrails.ValidateMoveRecord(malformedMoved);

        validateStored.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordIndexEntries);
        validateMoved.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordIndexEntries);
    }

    [Fact]
    public void IndexEntriesHaveIndividualAndAggregateCanonicalByteCeilings()
    {
        var indexEntryFixedBytes = StorageMovePageDigest.GetIndexEntryEncodedByteCount(
            CreateIndexEntry("a", string.Empty));
        var exactValueLength = checked((int)(
            (SearchableStorageCapacityLimits.MaximumIndexEntryCanonicalBytes - indexEntryFixedBytes)
            / sizeof(char)));
        var exactEntries = Enumerable.Range(0, 8)
            .Select(index => CreateIndexEntry(
                ((char)('a' + index)).ToString(),
                new string('v', exactValueLength)))
            .ToList();
        var oversizedValue = new string('v', exactValueLength + 1);
        var aggregateEntries = Enumerable.Range(0, 9)
            .Select(index => CreateIndexEntry($"scope-{index}", new string('v', 30_000)))
            .ToList();

        Action validateEntry = () => StorageCapacityGuardrails.ValidateRecord(
            "entry-record",
            CreateRecord(indexEntries: [CreateIndexEntry("scope", oversizedValue)]));
        Action validateAggregate = () => StorageCapacityGuardrails.ValidateRecord(
            "aggregate-record",
            CreateRecord(indexEntries: aggregateEntries));

        exactEntries.Sum(StorageMovePageDigest.GetIndexEntryEncodedByteCount)
            .Should().Be(SearchableStorageCapacityLimits.MaximumIndexBytesPerRecord);
        StorageCapacityGuardrails.ValidateRecord(
            "exact-index-record",
            CreateRecord(indexEntries: exactEntries)).Should().BePositive();
        validateEntry.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.IndexEntryBytes);
        validateAggregate.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordIndexBytes);
    }

    [Fact]
    public void ProjectedRebuildAndImportCannotCrossTheSnapshotEnvelope()
    {
        var sharedPayload = new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes];
        var records = Enumerable.Range(0, 127)
            .ToDictionary(
                index => $"large-{index:D3}",
                _ => CreateRecord(sharedPayload),
                StringComparer.Ordinal);
        records.Add("replacement", CreateRecord());
        var view = new StoragePartitionView(records);
        var replacement = CreateRecord(sharedPayload);
        var imported = StorageMoveRecordCodec.Encode("imported", replacement);

        Action rebuild = () => view.ValidateProjectedUpsert("replacement", replacement);
        Action import = () => view.ValidateProjectedImports([imported]);

        rebuild.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.SnapshotBytes);
        import.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.SnapshotBytes);
        view.Records.Should().HaveCount(128);
        view.Records["replacement"].Payload.Should().BeEmpty();
    }

    [Fact]
    public void SnapshotRecordAndCanonicalByteCountsFailBeforeMaterialization()
    {
        var tooManyRecords = new CountOnlyRecords(
            SearchableStorageCapacityLimits.MaximumSnapshotRecords + 1);
        var sharedPayload = new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes];
        var tooManyBytes = Enumerable.Range(0, 129)
            .ToDictionary(
                index => $"record-{index:D3}",
                _ => CreateRecord(sharedPayload),
                StringComparer.Ordinal);

        Action validateCount = () => StorageCapacityGuardrails.ValidateSnapshotRecords(tooManyRecords);
        Action validateBytes = () => StorageCapacityGuardrails.ValidateSnapshotRecords(tooManyBytes);

        validateCount.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.SnapshotRecords);
        validateBytes.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.SnapshotBytes);
    }

    [Fact]
    public void SnapshotCanonicalBytesAcceptTheExactLimitAndRejectOneAdditionalByte()
    {
        var sharedPayload = new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes];
        var records = Enumerable.Range(0, 127)
            .ToDictionary(
                index => $"large-{index:D3}",
                _ => CreateRecord(sharedPayload),
                StringComparer.Ordinal);
        var currentBytes = records.Sum(
            pair => StorageCapacityGuardrails.ValidateRecord(pair.Key, pair.Value));
        const string finalKey = "final";
        var emptyFinalBytes = StorageCapacityGuardrails.ValidateRecord(finalKey, CreateRecord());
        var finalPayloadLength = checked((int)(
            SearchableStorageCapacityLimits.MaximumSnapshotCanonicalBytes
            - currentBytes
            - emptyFinalBytes));
        finalPayloadLength.Should().BeInRange(
            0,
            SearchableStorageCapacityLimits.MaximumRecordPayloadBytes - 1);
        records.Add(finalKey, CreateRecord(new byte[finalPayloadLength]));

        StorageCapacityGuardrails.ValidateSnapshotRecords(records)
            .Should().Be(SearchableStorageCapacityLimits.MaximumSnapshotCanonicalBytes);

        records[finalKey] = CreateRecord(new byte[finalPayloadLength + 1]);
        Action validateOverLimit = () => StorageCapacityGuardrails.ValidateSnapshotRecords(records);
        validateOverLimit.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.SnapshotBytes);
    }

    [Fact]
    public async Task SnapshotChildRejectsOversizedPayloadWithoutWritingOrPoisoning()
    {
        var state = new TestPersistentState<StorageSnapshotState>();
        var deactivationRequested = false;
        var grain = new StorageSnapshotGrain(state, () => deactivationRequested = true);
        var snapshot = CreateSnapshot(CreateRecord(
            payload: new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1]));

        Func<Task> store = () => grain.StoreAsync(snapshot);

        var failure = await store.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>();
        failure.Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordPayloadBytes);
        state.WriteCount.Should().Be(0);
        deactivationRequested.Should().BeFalse();
        (await grain.ReadAsync()).Initialized.Should().BeFalse();
    }

    [Fact]
    public async Task JournalChildRejectsOversizedEntryWithoutWritingOrPoisoning()
    {
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var deactivationRequested = false;
        var grain = new StorageJournalSegmentGrain(state, () => deactivationRequested = true);
        var entry = CreateUpsertEntry(CreateRecord(
            payload: new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1]));

        Func<Task> store = () => grain.StoreAsync(
            entry,
            committedSequence: 0,
            committedOperationId: Guid.Empty,
            absoluteSegmentIndex: 0,
            segmentCapacity: 1);

        var failure = await store.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>();
        failure.Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordPayloadBytes);
        state.WriteCount.Should().Be(0);
        deactivationRequested.Should().BeFalse();
        (await grain.ReadAsync()).Initialized.Should().BeFalse();
    }

    [Fact]
    public async Task ChildReadsFailClosedOnOversizedDurablePayloads()
    {
        var oversizedRecord = CreateRecord(
            payload: new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1]);
        var snapshotState = new TestPersistentState<StorageSnapshotState>
        {
            State = CreateSnapshot(oversizedRecord),
        };
        var journalState = new TestPersistentState<StorageJournalSegmentState>
        {
            State = new StorageJournalSegmentState
            {
                Initialized = true,
                Capacity = 1,
                AbsoluteSegmentIndex = 0,
                HighestWriterEpoch = 1,
                Entries = [CreateUpsertEntry(oversizedRecord)],
            },
        };
        var snapshotDeactivationRequested = false;
        var journalDeactivationRequested = false;
        var snapshot = new StorageSnapshotGrain(
            snapshotState,
            () => snapshotDeactivationRequested = true);
        var journal = new StorageJournalSegmentGrain(
            journalState,
            () => journalDeactivationRequested = true);

        Func<Task> readSnapshot = () => snapshot.ReadAsync();
        Func<Task> readJournal = () => journal.ReadAsync();

        (await readSnapshot.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordPayloadBytes);
        (await readJournal.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordPayloadBytes);
        snapshotState.WriteCount.Should().Be(0);
        journalState.WriteCount.Should().Be(0);
        snapshotDeactivationRequested.Should().BeTrue();
        journalDeactivationRequested.Should().BeTrue();

        await readSnapshot.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after invalid durable state*");
        await readJournal.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after invalid durable state*");
    }

    [Fact]
    public async Task CorruptRetiredAndUninitializedPayloadsCannotBypassLoadedStateChecks()
    {
        var oversizedRecord = CreateRecord(
            payload: new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1]);
        var snapshotState = new TestPersistentState<StorageSnapshotState>
        {
            State = CreateSnapshot(oversizedRecord),
        };
        snapshotState.State.Tombstoned = true;
        var journalState = new TestPersistentState<StorageJournalSegmentState>
        {
            State = new StorageJournalSegmentState
            {
                Initialized = false,
                Capacity = 1,
                Entries = [CreateUpsertEntry(oversizedRecord)],
            },
        };
        var snapshotDeactivationRequested = false;
        var journalDeactivationRequested = false;
        var snapshot = new StorageSnapshotGrain(
            snapshotState,
            () => snapshotDeactivationRequested = true);
        var journal = new StorageJournalSegmentGrain(
            journalState,
            () => journalDeactivationRequested = true);

        Func<Task> readSnapshot = () => snapshot.ReadAsync();
        Func<Task> readJournal = () => journal.ReadAsync();

        await readSnapshot.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>();
        await readJournal.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>();
        snapshotDeactivationRequested.Should().BeTrue();
        journalDeactivationRequested.Should().BeTrue();
    }

    [Fact]
    public async Task StructurallyInvalidDurableChildMetadataRequestsDeactivation()
    {
        var journalState = new TestPersistentState<StorageJournalSegmentState>
        {
            State = new StorageJournalSegmentState
            {
                Initialized = true,
                Capacity = 0,
                AbsoluteSegmentIndex = 0,
                HighestWriterEpoch = 1,
                Entries = [CreateUpsertEntry(CreateRecord())],
            },
        };
        var snapshotState = new TestPersistentState<StorageSnapshotState>
        {
            State = new StorageSnapshotState
            {
                Initialized = true,
                Tombstoned = true,
            },
        };
        var journalDeactivationRequested = false;
        var snapshotDeactivationRequested = false;
        var journal = new StorageJournalSegmentGrain(
            journalState,
            () => journalDeactivationRequested = true);
        var snapshot = new StorageSnapshotGrain(
            snapshotState,
            () => snapshotDeactivationRequested = true);

        Func<Task> readJournal = () => journal.ReadAsync();
        Func<Task> readSnapshot = () => snapshot.ReadAsync();

        await readJournal.Should().ThrowAsync<InvalidOperationException>();
        await readSnapshot.Should().ThrowAsync<ArgumentOutOfRangeException>();
        journalDeactivationRequested.Should().BeTrue();
        snapshotDeactivationRequested.Should().BeTrue();
        await readJournal.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after invalid durable state*");
        await readSnapshot.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after invalid durable state*");
    }

    [Fact]
    public async Task StorageBridgeRejectsPayloadBeforeRoutingOrPartitionAuthority()
    {
        var layoutLoadCount = 0;
        var partition = new CountingPartition();
        var storage = new SearchableGrainStorage(
            "capacity-client",
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                VirtualSlotTargetCount = 1,
                GrainStorageSerializer = OversizedSerializer.Instance,
            },
            TestActivatorProvider.Instance,
            new StorageLayoutCache(() =>
            {
                layoutLoadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());
            }),
            _ => partition);
        var state = new GrainState<CapacityState> { State = new CapacityState() };

        Func<Task> write = () => storage.WriteStateAsync(
            "state",
            GrainId.Create("capacity", "client"),
            state);

        (await write.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordPayloadBytes);
        layoutLoadCount.Should().Be(0);
        partition.WriteCount.Should().Be(0);
        state.ETag.Should().BeNull();
        state.RecordExists.Should().BeFalse();
    }

    [Fact]
    public async Task StorageBridgeRejectsRecordKeyBeforeApplicationSerialization()
    {
        var layoutLoadCount = 0;
        var serializer = new CountingSerializer();
        var storage = new SearchableGrainStorage(
            "capacity-client",
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                VirtualSlotTargetCount = 1,
                GrainStorageSerializer = serializer,
            },
            TestActivatorProvider.Instance,
            new StorageLayoutCache(() =>
            {
                layoutLoadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());
            }),
            _ => new CountingPartition());
        var state = new GrainState<CapacityState> { State = new CapacityState() };
        var oversizedStateName = new string(
            's',
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes / sizeof(char));

        Func<Task> write = () => storage.WriteStateAsync(
            oversizedStateName,
            GrainId.Create("capacity", "client"),
            state);

        (await write.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordKeyBytes);
        serializer.SerializeCount.Should().Be(0);
        layoutLoadCount.Should().Be(0);
    }

    [Fact]
    public async Task ManagedWriteRejectsOversizedExtractedIndexBeforeLayoutOrPartitionAuthority()
    {
        var layoutLoadCount = 0;
        var partition = new CountingPartition();
        var registration = new SearchableStateRegistration<CapacityManagedState>(
            "capacity-client",
            "state");
        var registry = new SearchableStateRegistry([registration], options: null);
        var schema = new ActiveCapacitySchema();
        var storage = new SearchableGrainStorage(
            "capacity-client",
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                VirtualSlotTargetCount = 1,
                GrainStorageSerializer = SmallSerializer.Instance,
            },
            TestActivatorProvider.Instance,
            new StorageLayoutCache(() =>
            {
                layoutLoadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());
            }),
            _ => partition,
            registry,
            _ => schema);
        var state = new GrainState<CapacityManagedState>
        {
            State = new CapacityManagedState { Value = new string('v', 40_000) },
        };

        Func<Task> write = () => storage.WriteStateAsync(
            "state",
            GrainId.Create("capacity", "managed"),
            state);

        (await write.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.IndexEntryBytes);
        schema.GetCount.Should().Be(1);
        layoutLoadCount.Should().Be(0);
        partition.WriteCount.Should().Be(0);
    }

    [Fact]
    public async Task LegacyWriteRejectsOversizedExtractedIndexBeforeLayoutInitialization()
    {
        var layoutLoadCount = 0;
        var partition = new CountingPartition();
        var storage = new SearchableGrainStorage(
            "capacity-client",
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                VirtualSlotTargetCount = 1,
                GrainStorageSerializer = SmallSerializer.Instance,
            },
            TestActivatorProvider.Instance,
            new StorageLayoutCache(() =>
            {
                layoutLoadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());
            }),
            _ => partition);
        var state = new GrainState<CapacityManagedState>
        {
            State = new CapacityManagedState { Value = new string('v', 40_000) },
        };

        Func<Task> write = () => storage.WriteStateAsync(
            "state",
            GrainId.Create("capacity", "legacy"),
            state);

        (await write.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.IndexEntryBytes);
        layoutLoadCount.Should().Be(0);
        partition.WriteCount.Should().Be(0);
    }

    [Fact]
    public async Task OversizedGrainIdFailsBeforeWriteAuthorityAndDuringRecovery()
    {
        var layoutLoadCount = 0;
        var partition = new CountingPartition();
        var storage = new SearchableGrainStorage(
            "capacity-client",
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                VirtualSlotTargetCount = 1,
                GrainStorageSerializer = SmallSerializer.Instance,
            },
            TestActivatorProvider.Instance,
            new StorageLayoutCache(() =>
            {
                layoutLoadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());
            }),
            _ => partition);
        var oversizedGrainId = GrainId.Create(
            new GrainType(new byte[SearchableStorageCapacityLimits.MaximumGrainTypeBytes + 1]),
            new IdSpan([1]));
        var state = new GrainState<CapacityState> { State = new CapacityState() };

        Func<Task> write = () => storage.WriteStateAsync("state", oversizedGrainId, state);
        Action recover = () => _ = new StorageCapacityTracker(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["record"] = CreateRecord(grainId: oversizedGrainId),
            });

        (await write.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.GrainTypeBytes);
        recover.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.GrainTypeBytes);
        layoutLoadCount.Should().Be(0);
        partition.WriteCount.Should().Be(0);
    }

    [Fact]
    public async Task StorageBridgeRejectsOversizedClearKeyBeforeRoutingOrPartitionAuthority()
    {
        var layoutLoadCount = 0;
        var partition = new CountingPartition();
        var storage = new SearchableGrainStorage(
            "capacity-client",
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                VirtualSlotTargetCount = 1,
                GrainStorageSerializer = SmallSerializer.Instance,
            },
            TestActivatorProvider.Instance,
            new StorageLayoutCache(() =>
            {
                layoutLoadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());
            }),
            _ => partition);
        var state = new GrainState<CapacityState>
        {
            State = new CapacityState(),
            ETag = "1",
            RecordExists = true,
        };
        var oversizedStateName = new string(
            's',
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes / sizeof(char));

        Func<Task> clear = () => storage.ClearStateAsync(
            oversizedStateName,
            GrainId.Create("capacity", "client"),
            state);

        (await clear.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordKeyBytes);
        layoutLoadCount.Should().Be(0);
        partition.ClearCount.Should().Be(0);
        state.ETag.Should().Be("1");
        state.RecordExists.Should().BeTrue();
    }

    [Fact]
    public void JournalEntryAndSegmentHaveIndependentHardCeilings()
    {
        var largeExpectedEtag = new string('e', 600_000);
        var largeEntry = CreateUpsertEntry(
            CreateRecord(new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes]),
            expectedEtag: largeExpectedEtag);
        var tooManyEntries = Enumerable.Repeat(
                CreateUpsertEntry(CreateRecord()),
                SearchableStorageCapacityLimits.MaximumJournalSegmentEntries + 1)
            .ToList();

        Action validateEntry = () => StorageCapacityGuardrails.ValidateJournalEntry(largeEntry);
        Action validateSegment = () => StorageCapacityGuardrails.ValidateJournalSegment(
            new StorageJournalSegmentState { Entries = tooManyEntries });

        validateEntry.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.JournalEntryBytes);
        validateSegment.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.JournalSegmentEntries);
    }

    [Fact]
    public void JournalCanonicalBytesAcceptTheExactLimitAndRejectTheNextTextCodeUnit()
    {
        var record = CreateRecord();
        var baseline = CreateUpsertEntry(record, expectedEtag: string.Empty);
        var baselineBytes = StorageCapacityGuardrails.ValidateJournalEntry(baseline);
        if (((SearchableStorageCapacityLimits.MaximumJournalEntryCanonicalBytes - baselineBytes) & 1) != 0)
        {
            record = CreateRecord([0]);
            baseline = CreateUpsertEntry(record, expectedEtag: string.Empty);
            baselineBytes = StorageCapacityGuardrails.ValidateJournalEntry(baseline);
        }

        var exactEtagLength = checked((int)(
            (SearchableStorageCapacityLimits.MaximumJournalEntryCanonicalBytes - baselineBytes)
            / sizeof(char)));
        var exact = CreateUpsertEntry(record, new string('e', exactEtagLength));
        var over = CreateUpsertEntry(record, new string('e', exactEtagLength + 1));

        StorageCapacityGuardrails.ValidateJournalEntry(exact)
            .Should().Be(SearchableStorageCapacityLimits.MaximumJournalEntryCanonicalBytes);
        Action validateOver = () => StorageCapacityGuardrails.ValidateJournalEntry(over);
        validateOver.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.JournalEntryBytes);
    }

    [Fact]
    public async Task MutationAdmissionRejectsBeforeAuthorityWithoutPoisoningAndAllowsRetry()
    {
        var authorityCount = 0;
        var poisonCount = 0;
        var oversized = CreateUpsertEntry(
            CreateRecord(),
            new string('e', 3_000_000));

        Func<Task> reject = () => StorageMutationAdmission.PrepareAsync(
            oversized,
            () =>
            {
                authorityCount++;
                return Task.CompletedTask;
            },
            () => poisonCount++);

        (await reject.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.JournalEntryBytes);
        authorityCount.Should().Be(0);
        poisonCount.Should().Be(0);

        await StorageMutationAdmission.PrepareAsync(
            CreateUpsertEntry(CreateRecord()),
            () =>
            {
                authorityCount++;
                return Task.CompletedTask;
            },
            () => poisonCount++);
        authorityCount.Should().Be(1);
        poisonCount.Should().Be(0);
    }

    [Fact]
    public void MovementJournalCursorsUseTheRecordKeyCanonicalCeiling()
    {
        var exactCursor = new byte[
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes - sizeof(int)];
        var oversizedCursor = new byte[exactCursor.Length + sizeof(char)];
        var exact = CreateTerminalImportEntry(exactCursor);
        var oversized = CreateTerminalImportEntry(oversizedCursor);

        StorageCapacityGuardrails.ValidateJournalEntry(exact).Should().BePositive();
        Action validateOversized = () => StorageCapacityGuardrails.ValidateJournalEntry(oversized);
        AssertCapacityFailure(
            validateOversized,
            StorageCapacityGuardrails.RecordKeyBytes,
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes + (long)sizeof(char),
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes);
    }

    [Fact]
    public async Task DeleteRecordKeyCapIsEnforcedBeforeTheJournalChildWrites()
    {
        var oversizedKey = new string(
            'k',
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes / sizeof(char));
        var entry = new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.Delete,
            RecordKey = oversizedKey,
            NextVersionAfter = 1,
        };
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state);

        Func<Task> store = () => grain.StoreAsync(
            entry,
            committedSequence: 0,
            committedOperationId: Guid.Empty,
            absoluteSegmentIndex: 0,
            segmentCapacity: 1);

        (await store.Should().ThrowExactlyAsync<SearchableStorageCapacityExceededException>())
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordKeyBytes);
        state.WriteCount.Should().Be(0);
        (await grain.ReadAsync()).Initialized.Should().BeFalse();
    }

    [Fact]
    public void OversizedReplayRecordFailsBeforeChangingRecoveredRecords()
    {
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var entry = CreateUpsertEntry(CreateRecord(
            payload: new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1]));
        var recoveredOperationIds = new HashSet<Guid>();
        var nextVersion = 1L;
        var operationId = Guid.Empty;

        Action replay = () => StorageJournalReplay.ApplyEntry(
            records,
            entry,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            recoveredOperationIds,
            ref nextVersion,
            ref operationId,
            new StorageCapacityTracker(records));

        replay.Should().ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordPayloadBytes);
        records.Should().BeEmpty();
        nextVersion.Should().Be(1);
        operationId.Should().BeEmpty();
    }

    [Fact]
    public void PersistenceConfigurationUsesFixedOneZeroCeilings()
    {
        var valid = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = SearchableStorageCapacityLimits.MaximumJournalSegmentEntries,
            MaximumJournalReplayEntries = SearchableStorageCapacityLimits.MaximumJournalReplayEntries,
            CompactionThreshold = 1,
        };
        var tooManySegmentEntries = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = SearchableStorageCapacityLimits.MaximumJournalSegmentEntries + 1,
            MaximumJournalReplayEntries = 1,
            CompactionThreshold = 1,
        };
        var tooManyReplayEntries = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = 1,
            MaximumJournalReplayEntries = SearchableStorageCapacityLimits.MaximumJournalReplayEntries + 1,
            CompactionThreshold = 1,
        };

        StoragePartitionPersistence.ValidateSettings(valid);
        Action validateSegment = () => StoragePartitionPersistence.ValidateSettings(tooManySegmentEntries);
        Action validateReplay = () => StoragePartitionPersistence.ValidateSettings(tooManyReplayEntries);

        validateSegment.Should().Throw<ArgumentOutOfRangeException>();
        validateReplay.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void CapacityDiagnosticsNeverContainRecordOrIndexValues()
    {
        const string secret = "do-not-leak-this-index-value";
        var entries = Enumerable.Range(
                0,
                SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope + 1)
            .Select(_ => CreateIndexEntry("secret-scope", secret))
            .ToList();

        Action validate = () => StorageCapacityGuardrails.ValidateRecord(
            "secret-record-key",
            CreateRecord(indexEntries: entries));

        var failure = validate.Should().ThrowExactly<SearchableStorageCapacityExceededException>().Which;
        failure.Message.Should().NotContain(secret);
        failure.Message.Should().NotContain("secret-scope");
        failure.Message.Should().NotContain("secret-record-key");
        failure.Boundary.Should().Be(StorageCapacityGuardrails.RecordScopeIndexEntries);
    }

    private static void AssertCapacityFailure(
        Action action,
        string boundary,
        long actual,
        long limit)
    {
        var failure = action.Should().ThrowExactly<SearchableStorageCapacityExceededException>().Which;
        failure.Boundary.Should().Be(boundary);
        failure.Actual.Should().Be(actual);
        failure.Limit.Should().Be(limit);
    }

    private static StorageSnapshotState CreateSnapshot(StoredRecord record)
    {
        return new StorageSnapshotState
        {
            Initialized = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = Guid.NewGuid(),
            Sequence = 1,
            OperationId = Guid.NewGuid(),
            NextVersion = 2,
            Records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["record"] = record,
            },
        };
    }

    private static StorageJournalEntry CreateUpsertEntry(
        StoredRecord record,
        string? expectedEtag = null)
    {
        return new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = "record",
            ExpectedETag = expectedEtag,
            Record = record,
            NextVersionAfter = 2,
        };
    }

    private static StorageJournalEntry CreateTerminalImportEntry(byte[] cursor)
    {
        var move = StorageMovementProtocolTests.CreateMoveIdentity();
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            AfterRecordKey = [.. cursor],
            NextRecordKey = [.. cursor],
            Exhausted = true,
            FrozenNextVersion = 1,
            ItemLimit = 1,
            ByteTarget = 1,
        };
        var payload = new StorageMoveJournalPayload
        {
            MoveId = unsigned.MoveId,
            Slot = unsigned.Slot,
            VirtualSlotCount = unsigned.VirtualSlotCount,
            SourceEpoch = unsigned.SourceEpoch,
            SourceOwner = unsigned.SourceOwner,
            TargetOwner = unsigned.TargetOwner,
            AfterRecordKey = unsigned.AfterRecordKey,
            NextRecordKey = unsigned.NextRecordKey,
            Exhausted = unsigned.Exhausted,
            PageDigest = StorageMovePageDigest.Compute(StorageJournalOperation.Import, unsigned),
            FrozenNextVersion = unsigned.FrozenNextVersion,
            ItemLimit = unsigned.ItemLimit,
            ByteTarget = unsigned.ByteTarget,
        };
        return new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.Import,
            RecordKey = string.Empty,
            NextVersionAfter = 1,
            Move = payload,
        };
    }

    private static StoredRecord CreateRecord(
        byte[]? payload = null,
        List<IndexEntry>? indexEntries = null,
        GrainId? grainId = null)
    {
        return new StoredRecord
        {
            GrainId = grainId ?? GrainId.Create("capacity", "record"),
            Payload = payload ?? [],
            ETag = "1",
            IndexEntries = indexEntries ?? [],
        };
    }

    private static IndexEntry CreateIndexEntry(string scope, string value = "value")
    {
        return new IndexEntry
        {
            Scope = scope,
            Kind = SearchableIndexKind.Hash,
            Value = new IndexValue
            {
                Kind = IndexValueKind.String,
                Text = value,
            },
        };
    }

    private static StorageLayoutSnapshot CreateLayout()
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = "capacity-client",
            PartitionCount = 1,
            VirtualSlotCount = 1,
            SlotAssignments = [0],
            Epoch = 1,
        });
    }

    private sealed class CapacityState
    {
    }

    private sealed class CapacityManagedState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Value { get; init; } = string.Empty;
    }

    private sealed class OversizedSerializer : IGrainStorageSerializer
    {
        public static OversizedSerializer Instance { get; } = new();

        public BinaryData Serialize<T>(T input) => BinaryData.FromBytes(
            new byte[SearchableStorageCapacityLimits.MaximumRecordPayloadBytes + 1]);

        public T Deserialize<T>(BinaryData input) => throw new NotSupportedException();
    }

    private sealed class SmallSerializer : IGrainStorageSerializer
    {
        public static SmallSerializer Instance { get; } = new();

        public BinaryData Serialize<T>(T input) => BinaryData.FromBytes([1]);

        public T Deserialize<T>(BinaryData input) => throw new NotSupportedException();
    }

    private sealed class CountingSerializer : IGrainStorageSerializer
    {
        public int SerializeCount { get; private set; }

        public BinaryData Serialize<T>(T input)
        {
            SerializeCount++;
            return BinaryData.FromBytes([1]);
        }

        public T Deserialize<T>(BinaryData input) => throw new NotSupportedException();
    }

    private sealed class ActiveCapacitySchema : IStorageIndexSchemaGrain
    {
        public int GetCount { get; private set; }

        public Task<StorageIndexSchemaSnapshot> GetAsync(StorageIndexSchemaRequest request)
        {
            GetCount++;
            return Task.FromResult(new StorageIndexSchemaSnapshot
            {
                ProviderName = request.ProviderName,
                StateName = request.StateName,
                ActiveFingerprint = [.. request.Fingerprint],
            });
        }

        public Task<StorageIndexSchemaSnapshot> BeginRebuildAsync(
            StorageIndexSchemaRequest request) => throw new NotSupportedException();

        public Task<StorageIndexSchemaSnapshot> AdvanceRebuildAsync(
            StorageIndexSchemaCommand command) => throw new NotSupportedException();
    }

    private sealed class TestActivatorProvider : IActivatorProvider
    {
        public static TestActivatorProvider Instance { get; } = new();

        public IActivator<T> GetActivator<T>() => TestActivator<T>.Instance;
    }

    private sealed class TestActivator<T> : IActivator<T>
    {
        public static TestActivator<T> Instance { get; } = new();

        public T Create() => Activator.CreateInstance<T>();
    }

    private sealed class CountingPartition : StoragePartitionGrainMovementTestDouble, IStoragePartitionGrain
    {
        public int WriteCount { get; private set; }

        public int ClearCount { get; private set; }

        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request)
        {
            WriteCount++;
            return Task.FromResult("1");
        }

        public Task ClearRoutedAsync(RoutedStorageClearRequest request)
        {
            ClearCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class CountOnlyRecords : IReadOnlyDictionary<string, StoredRecord>
    {
        public CountOnlyRecords(int count)
        {
            Count = count;
        }

        public int Count { get; }

        public IEnumerable<string> Keys => throw new NotSupportedException();

        public IEnumerable<StoredRecord> Values => throw new NotSupportedException();

        public StoredRecord this[string key] => throw new NotSupportedException();

        public bool ContainsKey(string key) => false;

        public IEnumerator<KeyValuePair<string, StoredRecord>> GetEnumerator() =>
            throw new NotSupportedException();

        public bool TryGetValue(string key, out StoredRecord value)
        {
            value = null!;
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageCapacitySerializationTests
{
    private readonly MemoryStorageFixture _fixture;

    public StorageCapacitySerializationTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void CapacityExceptionRoundTripsThroughOrleansAsException()
    {
        var services = Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary).ServiceProvider;
        var serializer = services.GetRequiredService<Serializer>();
        Exception original = new SearchableStorageCapacityExceededException(
            StorageCapacityGuardrails.RecordPayloadBytes,
            actual: 11,
            limit: 10);

        var payload = serializer.SerializeToArray(original);
        var copy = serializer.Deserialize<Exception>(payload);

        var capacity = copy.Should().BeOfType<SearchableStorageCapacityExceededException>().Which;
        capacity.Message.Should().Be(original.Message);
        capacity.Boundary.Should().Be(StorageCapacityGuardrails.RecordPayloadBytes);
        capacity.Actual.Should().Be(11);
        capacity.Limit.Should().Be(10);
    }

}
