using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageMovementRecoveryTests
{
    private readonly MemoryStorageFixture _fixture;

    public StorageMovementRecoveryTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void EmptySlotVersionFenceAndTargetAheadImportRecoverWithoutRenumberingEtags()
    {
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var recoveredIds = new HashSet<Guid>();
        var nextVersion = 1L;
        var operationId = Guid.Empty;
        var capacity = new StorageCapacityTracker(records);
        var move = StorageMovementProtocolTests.CreateMoveIdentity();
        var advance = CreateAdvanceEntry(
            move,
            sequence: 1,
            previousOperationId: operationId,
            frozenNextVersion: 50,
            nextVersionAfter: 50);

        StorageJournalReplay.ApplyEntry(
            records,
            advance,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            recoveredIds,
            ref nextVersion,
            ref operationId,
            capacity);

        records.Should().BeEmpty();
        nextVersion.Should().Be(50);

        // A target can already be ahead because versions are partition-wide. The source ETag is
        // preserved and the target allocator remains at its higher value.
        nextVersion = 100;
        operationId = Guid.Empty;
        recoveredIds.Clear();
        var import = CreateImportEntry(
            move,
            sequence: 1,
            previousOperationId: operationId,
            frozenNextVersion: 50,
            nextVersionAfter: 100,
            recordKey: "record-49",
            etag: "49");

        StorageJournalReplay.ApplyEntry(
            records,
            import,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            recoveredIds,
            ref nextVersion,
            ref operationId,
            capacity);

        records["record-49"].ETag.Should().Be("49");
        nextVersion.Should().Be(100);
    }

    [Fact]
    public void ImportRejectsAnEtagAtOrAboveTheFrozenSourceHighWaterEvenWhenTargetIsAhead()
    {
        var move = StorageMovementProtocolTests.CreateMoveIdentity();
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var recoveredIds = new HashSet<Guid>();
        var nextVersion = 100L;
        var operationId = Guid.Empty;
        var capacity = new StorageCapacityTracker(records);
        var import = CreateImportEntry(
            move,
            sequence: 1,
            previousOperationId: operationId,
            frozenNextVersion: 50,
            nextVersionAfter: 100,
            recordKey: "impossible-source-version",
            etag: "50");

        Action replay = () => StorageJournalReplay.ApplyEntry(
            records,
            import,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            recoveredIds,
            ref nextVersion,
            ref operationId,
            capacity);

        replay.Should().Throw<InvalidOperationException>()
            .WithMessage("*invalid imported record*");
        records.Should().BeEmpty();
        nextVersion.Should().Be(100);
    }

    [Fact]
    public async Task LostChildAcknowledgementAcceptsOnlyTheExactImportPageOnReactivation()
    {
        var injected = new InvalidOperationException("Lost child acknowledgement.");
        var state = new TestPersistentState<StorageJournalSegmentState>
        {
            WriteException = injected,
        };
        var firstActivation = new StorageJournalSegmentGrain(
            state,
            requestDeactivation: static () => { });
        var move = StorageMovementProtocolTests.CreateMoveIdentity();
        var entry = CreateImportEntry(
            move,
            sequence: 1,
            previousOperationId: Guid.Empty,
            frozenNextVersion: 10,
            nextVersionAfter: 10,
            recordKey: "page-a",
            etag: "9");

        Func<Task> firstStore = () => firstActivation.StoreAsync(
            entry,
            committedSequence: 0,
            committedOperationId: Guid.Empty,
            absoluteSegmentIndex: 0,
            segmentCapacity: 2);
        await firstStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);
        state.LastWriteState!.Entries.Should().ContainSingle();

        state.WriteException = null;
        state.State = state.LastWriteState!;
        var recoveredActivation = new StorageJournalSegmentGrain(state);
        await recoveredActivation.StoreAsync(
            entry.Copy(),
            committedSequence: 0,
            committedOperationId: Guid.Empty,
            absoluteSegmentIndex: 0,
            segmentCapacity: 2);
        state.WriteCount.Should().Be(1);

        var conflict = CreateImportEntry(
            move,
            sequence: 1,
            previousOperationId: Guid.Empty,
            frozenNextVersion: 10,
            nextVersionAfter: 10,
            recordKey: "page-b",
            etag: "9",
            operationId: entry.OperationId);
        Func<Task> conflictingStore = () => recoveredActivation.StoreAsync(
            conflict,
            committedSequence: 0,
            committedOperationId: Guid.Empty,
            absoluteSegmentIndex: 0,
            segmentCapacity: 2);

        await conflictingStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*exactly the same durable representation*");
        state.WriteCount.Should().Be(1);
        state.State.Entries.Should().ContainSingle()
            .Which.Move!.PageDigest.Should().Equal(entry.Move!.PageDigest);
    }

    [Fact]
    public async Task LostManifestAcknowledgementRecoversAnEmptySlotVersionFenceAndMovePhase()
    {
        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var poisonCount = 0;
        var partitionKey = $"movement-recovery-{Guid.NewGuid():N}:00000000";
        var persistence = CreatePersistence(manifest, partitionKey, () => poisonCount++);
        var settings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            CompactionThreshold = 4,
        };
        var move = StorageMovementProtocolTests.CreateMoveIdentity();
        await persistence.EnableMovementProtocolAsync(settings, minimumRoutingEpoch: 1);
        var prepared = CreateTargetControl(
            move,
            StoragePartitionMovePhase.TargetPrepared,
            frozenNextVersion: 40);
        await persistence.SetMoveControlAsync(prepared);
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        await persistence.PrepareForProtocolMutationAsync(records);

        var importing = prepared.Copy();
        importing.Phase = StoragePartitionMovePhase.TargetImporting;
        var advance = CreateAdvanceEntry(
            move,
            sequence: persistence.NextSequence,
            previousOperationId: persistence.CommittedOperationId,
            frozenNextVersion: 40,
            nextVersionAfter: 40,
            writerEpoch: persistence.WriterEpoch);
        manifest.WriteException = new InvalidOperationException("Lost manifest acknowledgement.");

        Func<Task> commit = () => persistence.CommitAsync(advance, importing);
        await commit.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(manifest.WriteException.Message);
        poisonCount.Should().Be(1);
        manifest.State = manifest.LastWriteState!;
        manifest.State.CommittedSequence.Should().Be(1);
        manifest.State.NextVersion.Should().Be(40);

        manifest.WriteException = null;
        var recovered = CreatePersistence(manifest, partitionKey, static () => { });
        var recoveredRecords = await recovered.ActivateAsync();

        recoveredRecords.Should().BeEmpty();
        recovered.CommittedSequence.Should().Be(1);
        recovered.NextVersion.Should().Be(40);
        recovered.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetImporting);
        recovered.MoveControl.FrozenNextVersion.Should().Be(40);
    }

    [Fact]
    public async Task V3ActivationAndOrdinaryMutationStayV3UntilExplicitEnablement()
    {
        var settings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            CompactionThreshold = 4,
        };
        var existingManifest = new TestPersistentState<StoragePartitionManifestState>
        {
            State = new StoragePartitionManifestState
            {
                Initialized = true,
                PersistenceFormatVersion = StoragePersistence.PreviousPersistenceFormatVersion,
                JournalSegmentCapacity = settings.JournalSegmentCapacity,
                MaximumJournalReplayEntries = settings.MaximumJournalReplayEntries,
                NextVersion = 1,
            },
        };
        var partitionKey = $"movement-v3-{Guid.NewGuid():N}:00000000";
        var persistence = CreatePersistence(existingManifest, partitionKey, static () => { });

        (await persistence.ActivateAsync()).Should().BeEmpty();
        existingManifest.WriteCount.Should().Be(0);
        existingManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.PreviousPersistenceFormatVersion);

        await persistence.PrepareForMutationAsync(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            settings);
        existingManifest.WriteCount.Should().Be(1);
        existingManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.PreviousPersistenceFormatVersion);

        await persistence.EnableMovementProtocolAsync(settings, minimumRoutingEpoch: 1);
        existingManifest.WriteCount.Should().Be(2);
        existingManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.MovementPersistenceFormatVersion);
        existingManifest.State.MovementProtocolVersion.Should().Be(StorageMoveProtocol.Version);

        await persistence.EnableMovementProtocolAsync(settings, minimumRoutingEpoch: 1);
        existingManifest.WriteCount.Should().Be(2);

        var newManifest = new TestPersistentState<StoragePartitionManifestState>();
        var newPersistence = CreatePersistence(
            newManifest,
            $"movement-v3-new-{Guid.NewGuid():N}:00000000",
            static () => { });
        await newPersistence.PrepareForMutationAsync(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            settings);
        newManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.PreviousPersistenceFormatVersion);
    }

    [Fact]
    public async Task ExplicitEnablementAdoptsAV3SnapshotWithoutRewritingTheChildPayload()
    {
        var partitionKey = $"movement-v3-snapshot-{Guid.NewGuid():N}:00000000";
        var operationId = Guid.NewGuid();
        var snapshot = new StorageSnapshotState
        {
            Initialized = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = Guid.NewGuid(),
            Sequence = 1,
            OperationId = operationId,
            NextVersion = 2,
            Records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["v3-record"] = StorageMovementProtocolTests.CreateRecord("v3-record", "1"),
            },
        };
        var descriptor = StorageSnapshotDescriptor.FromSnapshot(snapshot);
        var snapshotGrain = _fixture.Cluster.GrainFactory.GetGrain<IStorageSnapshotGrain>(
            StoragePersistence.CreateSnapshotSlotKey(partitionKey, slotIndex: 0));
        await snapshotGrain.StoreAsync(snapshot);

        var manifest = new TestPersistentState<StoragePartitionManifestState>
        {
            State = new StoragePartitionManifestState
            {
                Initialized = true,
                PersistenceFormatVersion = StoragePersistence.PreviousPersistenceFormatVersion,
                JournalSegmentCapacity = 2,
                MaximumJournalReplayEntries = 4,
                WriterEpoch = 1,
                CommittedSequence = 1,
                CommittedOperationId = operationId,
                NextVersion = 2,
                ActiveSnapshot = descriptor,
                SnapshotGenerationHighWatermark = 1,
                SnapshotSequence = 1,
            },
        };
        var persistence = CreatePersistence(manifest, partitionKey, static () => { });
        var recovered = await persistence.ActivateAsync();

        recovered.Should().ContainKey("v3-record");
        manifest.WriteCount.Should().Be(0);
        manifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.PreviousPersistenceFormatVersion);

        var settings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            CompactionThreshold = 4,
        };
        await persistence.EnableMovementProtocolAsync(settings, minimumRoutingEpoch: 1);

        manifest.WriteCount.Should().Be(1);
        manifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.MovementPersistenceFormatVersion);
        var childAfterEnable = await snapshotGrain.ReadAsync();
        StoragePersistenceStateEquality.SnapshotEquals(childAfterEnable, snapshot).Should().BeTrue();

        var upgradedActivation = CreatePersistence(manifest, partitionKey, static () => { });
        var upgradedRecords = await upgradedActivation.ActivateAsync();
        upgradedRecords.Should().ContainSingle().Which.Key.Should().Be("v3-record");
        upgradedRecords["v3-record"].ETag.Should().Be("1");
        childAfterEnable.RecordEncodingVersion.Should()
            .Be(StorageSnapshotFactory.LegacyRecordEncodingVersion);
        childAfterEnable.LosslessRecords.Should().BeEmpty();
    }

    [Fact]
    public async Task FrozenSourceWatermarkSurvivesWritesAndClearsInAnotherSlot()
    {
        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var partitionKey = $"movement-source-watermark-{Guid.NewGuid():N}:00000000";
        var persistence = CreatePersistence(manifest, partitionKey, static () => { });
        var settings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            CompactionThreshold = 4,
        };
        var move = new StorageMoveIdentity
        {
            ProtocolVersion = StorageMoveProtocol.Version,
            MoveId = Guid.NewGuid(),
            Slot = 0,
            VirtualSlotCount = 2,
            SourceEpoch = 1,
            SourceOwner = 0,
            TargetOwner = 1,
        };
        await persistence.EnableMovementProtocolAsync(settings, minimumRoutingEpoch: 1);
        var source = new StoragePartitionMoveControl
        {
            IsPresent = true,
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            Role = StoragePartitionMoveRole.Source,
            Phase = StoragePartitionMovePhase.SourceFrozen,
            FrozenNextVersion = 1,
        };
        await persistence.SetMoveControlAsync(source);

        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        await persistence.PrepareForProtocolMutationAsync(records);
        var grainId = CreateGrainInSlot(slot: 1, virtualSlotCount: 2);
        var record = new StoredRecord
        {
            GrainId = grainId,
            Payload = [1, 2, 3],
            ETag = "1",
            IndexEntries = [],
        };
        var upsertId = Guid.NewGuid();
        await persistence.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = upsertId,
            PreviousOperationId = persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = "other-slot",
            Record = record,
            NextVersionAfter = 2,
        });
        records.Add("other-slot", record);

        await persistence.PrepareForProtocolMutationAsync(records);
        await persistence.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = upsertId,
            Operation = StorageJournalOperation.Delete,
            RecordKey = "other-slot",
            ExpectedETag = "1",
            NextVersionAfter = 2,
        });
        records.Clear();

        persistence.MoveControl.FrozenNextVersion.Should().Be(1);
        persistence.NextVersion.Should().Be(2);

        var recovered = CreatePersistence(manifest, partitionKey, static () => { });
        var recoveredRecords = await recovered.ActivateAsync();
        recoveredRecords.Should().BeEmpty();
        recovered.NextVersion.Should().Be(2);
        recovered.MoveControl.FrozenNextVersion.Should().Be(1);

        var view = new StoragePartitionView(recoveredRecords, virtualSlotCount: 2);
        var page = StorageMovePageOperations.CreateExportRecords(
            view,
            slot: 0,
            afterRecordKey: null,
            itemLimit: 1,
            byteTarget: StorageMoveProtocol.DefaultPageBytes,
            out var cursor,
            out var exhausted,
            out var bytes);
        page.Should().BeEmpty();
        cursor.Should().BeNull();
        exhausted.Should().BeTrue();
        bytes.Should().Be(0);
    }

    private StoragePartitionPersistence CreatePersistence(
        TestPersistentState<StoragePartitionManifestState> manifest,
        string partitionKey,
        Action poisonActivation)
    {
        return new StoragePartitionPersistence(
            manifest,
            _fixture.Cluster.GrainFactory,
            partitionKey,
            poisonActivation,
            NullLogger<StoragePartitionPersistence>.Instance);
    }

    private static GrainId CreateGrainInSlot(int slot, int virtualSlotCount)
    {
        for (var candidate = 0; ; candidate++)
        {
            var grainId = GrainId.Create("movement-watermark", $"record-{candidate}");
            if (StorageLayout.GetSlot(grainId, virtualSlotCount) == slot)
            {
                return grainId;
            }
        }
    }

    private static StoragePartitionMoveControl CreateTargetControl(
        StorageMoveIdentity move,
        StoragePartitionMovePhase phase,
        long frozenNextVersion)
    {
        return new StoragePartitionMoveControl
        {
            IsPresent = true,
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            Role = StoragePartitionMoveRole.Target,
            Phase = phase,
            FrozenNextVersion = frozenNextVersion,
        };
    }

    private static StorageJournalEntry CreateAdvanceEntry(
        StorageMoveIdentity move,
        long sequence,
        Guid previousOperationId,
        long frozenNextVersion,
        long nextVersionAfter,
        long writerEpoch = 1)
    {
        return new StorageJournalEntry
        {
            Sequence = sequence,
            WriterEpoch = writerEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = previousOperationId,
            Operation = StorageJournalOperation.AdvanceVersion,
            RecordKey = string.Empty,
            NextVersionAfter = nextVersionAfter,
            Move = new StorageMoveJournalPayload
            {
                MoveId = move.MoveId,
                Slot = move.Slot,
                VirtualSlotCount = move.VirtualSlotCount,
                SourceEpoch = move.SourceEpoch,
                SourceOwner = move.SourceOwner,
                TargetOwner = move.TargetOwner,
                FrozenNextVersion = frozenNextVersion,
            },
        };
    }

    private static StorageJournalEntry CreateImportEntry(
        StorageMoveIdentity move,
        long sequence,
        Guid previousOperationId,
        long frozenNextVersion,
        long nextVersionAfter,
        string recordKey,
        string etag,
        Guid? operationId = null)
    {
        var item = StorageMoveRecordCodec.Encode(
            recordKey,
            StorageMovementProtocolTests.CreateRecord(recordKey, etag));
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            PageOrdinal = 0,
            NextRecordKey = StorageMoveRecordCodec.EncodeText(recordKey),
            Exhausted = true,
            FrozenNextVersion = frozenNextVersion,
            Imports = [item],
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.MaximumPageBytes,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(item),
        };
        var payload = new StorageMoveJournalPayload
        {
            MoveId = unsigned.MoveId,
            Slot = unsigned.Slot,
            VirtualSlotCount = unsigned.VirtualSlotCount,
            SourceEpoch = unsigned.SourceEpoch,
            SourceOwner = unsigned.SourceOwner,
            TargetOwner = unsigned.TargetOwner,
            PageOrdinal = unsigned.PageOrdinal,
            AfterRecordKey = unsigned.AfterRecordKey,
            NextRecordKey = unsigned.NextRecordKey,
            Exhausted = unsigned.Exhausted,
            PageDigest = StorageMovePageDigest.Compute(StorageJournalOperation.Import, unsigned),
            FrozenNextVersion = unsigned.FrozenNextVersion,
            Imports = unsigned.Imports,
            ItemLimit = unsigned.ItemLimit,
            ByteTarget = unsigned.ByteTarget,
            EncodedByteCount = unsigned.EncodedByteCount,
        };
        return new StorageJournalEntry
        {
            Sequence = sequence,
            WriterEpoch = 1,
            OperationId = operationId ?? Guid.NewGuid(),
            PreviousOperationId = previousOperationId,
            Operation = StorageJournalOperation.Import,
            RecordKey = string.Empty,
            NextVersionAfter = nextVersionAfter,
            Move = payload,
        };
    }
}
