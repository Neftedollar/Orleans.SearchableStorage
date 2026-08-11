using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageMovementSnapshotCompactionTests
{
    private static readonly StoragePersistenceSettings Settings = new()
    {
        JournalSegmentCapacity = 2,
        MaximumJournalReplayEntries = 2,
        CompactionThreshold = 2,
    };

    private readonly MemoryStorageFixture _fixture;

    public StorageMovementSnapshotCompactionTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ImportHardBoundaryCompactsLosslesslyAndRecoversTheNextPage()
    {
        const string firstKey = "a-import-\ud800";
        const string secondKey = "b-import-\udc00";
        var move = CreateMoveIdentity();
        var firstRecord = CreateRecord(firstKey, "1", "\ud801", move);
        var secondRecord = CreateRecord(secondKey, "2", "\udc01", move, keySeed: 10_000);
        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var partitionKey = $"movement-v4-import-snapshot-{Guid.NewGuid():N}:00000001";
        var persistence = CreatePersistence(manifest, partitionKey);
        await persistence.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);
        var prepared = CreateControl(
            move,
            StoragePartitionMoveRole.Target,
            StoragePartitionMovePhase.TargetPrepared,
            frozenNextVersion: 3);
        await persistence.SetMoveControlAsync(prepared);

        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        await persistence.PrepareForProtocolMutationAsync(records);
        var importing = prepared.Copy();
        importing.Phase = StoragePartitionMovePhase.TargetImporting;
        await persistence.CommitAsync(
            CreateAdvanceEntry(persistence, move, frozenNextVersion: 3),
            importing);

        await persistence.PrepareForProtocolMutationAsync(records);
        var firstPayload = CreatePagePayload(
            StorageJournalOperation.Import,
            move,
            pageOrdinal: 0,
            afterRecordKey: null,
            nextRecordKey: firstKey,
            exhausted: false,
            frozenNextVersion: 3,
            imports: [StorageMoveRecordCodec.Encode(firstKey, firstRecord)],
            deletes: []);
        var firstProgress = CreateProgressControl(
            importing,
            StoragePartitionMovePhase.TargetImporting,
            firstPayload,
            importedRecordCount: 1,
            importedByteCount: firstPayload.EncodedByteCount,
            deletedRecordCount: 0,
            deletedByteCount: 0);
        await persistence.CommitAsync(
            CreatePageEntry(persistence, StorageJournalOperation.Import, firstPayload),
            firstProgress);
        records.Add(firstKey, StoragePersistenceStateCopy.CopyRecord(firstRecord)!);

        // AdvanceVersion + page zero reaches the hard replay boundary. Preparing page one must
        // publish a v4 lossless snapshot and prune those WAL entries before allocating sequence 3.
        await persistence.PrepareForProtocolMutationAsync(records);
        manifest.State.SnapshotSequence.Should().Be(2);
        manifest.State.PrunedSequence.Should().Be(2);
        var compacted = await GetActiveSnapshotAsync(manifest.State, partitionKey);
        compacted.RecordEncodingVersion.Should()
            .Be(StorageSnapshotFactory.LosslessRecordEncodingVersion);
        compacted.Records.Should().BeEmpty();
        compacted.LosslessRecords.Should().ContainSingle();
        StorageMoveRecordCodec.DecodeRecordKey(compacted.LosslessRecords[0]).Should().Be(firstKey);
        AssertRecord(compacted.LosslessRecords[0].Record, firstRecord);

        var secondPayload = CreatePagePayload(
            StorageJournalOperation.Import,
            move,
            pageOrdinal: 1,
            afterRecordKey: firstKey,
            nextRecordKey: secondKey,
            exhausted: true,
            frozenNextVersion: 3,
            imports: [StorageMoveRecordCodec.Encode(secondKey, secondRecord)],
            deletes: []);
        var complete = CreateProgressControl(
            firstProgress,
            StoragePartitionMovePhase.TargetImportComplete,
            secondPayload,
            importedRecordCount: 2,
            importedByteCount: checked(
                firstPayload.EncodedByteCount + secondPayload.EncodedByteCount),
            deletedRecordCount: 0,
            deletedByteCount: 0);
        await persistence.CommitAsync(
            CreatePageEntry(persistence, StorageJournalOperation.Import, secondPayload),
            complete);
        records.Add(secondKey, StoragePersistenceStateCopy.CopyRecord(secondRecord)!);

        var recoveredPersistence = CreatePersistence(manifest, partitionKey);
        var recovered = await recoveredPersistence.ActivateAsync();
        recovered.Keys.Should().Equal(firstKey, secondKey);
        AssertRecord(recovered[firstKey], firstRecord);
        AssertRecord(recovered[secondKey], secondRecord);
        recoveredPersistence.MoveControl.Phase.Should()
            .Be(StoragePartitionMovePhase.TargetImportComplete);

        // Every later v4 compaction remains binary, including a page which was replayed after the
        // first snapshot boundary.
        await recoveredPersistence.CompactAsync(recovered);
        var fullyCompacted = await GetActiveSnapshotAsync(manifest.State, partitionKey);
        fullyCompacted.RecordEncodingVersion.Should()
            .Be(StorageSnapshotFactory.LosslessRecordEncodingVersion);
        fullyCompacted.Records.Should().BeEmpty();
        fullyCompacted.LosslessRecords.Select(StorageMoveRecordCodec.DecodeRecordKey)
            .Should().Equal(firstKey, secondKey);
    }

    [Fact]
    public async Task SourceCleanupUsesExactSurrogateKeysAfterV4CompactionAndReactivation()
    {
        const string firstKey = "a-source-\ud800";
        const string secondKey = "b-source-\udc00";
        var move = CreateMoveIdentity();
        var firstRecord = CreateRecord(firstKey, "1", "\ud802", move);
        var secondRecord = CreateRecord(secondKey, "2", "\udc02", move, keySeed: 10_000);
        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var partitionKey = $"movement-v4-source-snapshot-{Guid.NewGuid():N}:00000000";
        var persistence = CreatePersistence(manifest, partitionKey);
        await persistence.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);

        await CommitUpsertAsync(persistence, records, firstKey, firstRecord, nextVersion: 2);
        await CommitUpsertAsync(persistence, records, secondKey, secondRecord, nextVersion: 3);
        var hidden = CreateControl(
            move,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceHidden,
            frozenNextVersion: 3);
        await persistence.SetMoveControlAsync(hidden, minimumRoutingEpoch: 2);

        // The ordinary writes fill the replay budget. Cleanup preparation must compact the exact
        // in-memory source records before the first destructive page is allowed to commit.
        await persistence.PrepareForProtocolMutationAsync(records);
        var compacted = await GetActiveSnapshotAsync(manifest.State, partitionKey);
        compacted.RecordEncodingVersion.Should()
            .Be(StorageSnapshotFactory.LosslessRecordEncodingVersion);
        compacted.LosslessRecords.Select(StorageMoveRecordCodec.DecodeRecordKey)
            .Should().Equal(firstKey, secondKey);

        var view = new StoragePartitionView(records, move.VirtualSlotCount);
        var firstDeletes = StorageMovePageOperations.CreateDeleteRecords(
            view,
            move.Slot,
            afterRecordKey: null,
            itemLimit: 1,
            byteTarget: StorageMoveProtocol.DefaultPageBytes,
            out var firstCursor,
            out var firstExhausted,
            out var firstBytes);
        firstExhausted.Should().BeFalse();
        firstDeletes.Should().ContainSingle();
        StorageMoveRecordCodec.DecodeRecordKey(firstDeletes[0]).Should().Be(firstKey);
        var firstPayload = CreatePagePayload(
            StorageJournalOperation.MoveDelete,
            move,
            pageOrdinal: 0,
            afterRecordKey: null,
            nextRecordKey: StorageMoveRecordCodec.DecodeNullableText(
                firstCursor,
                nameof(firstCursor))!,
            exhausted: firstExhausted,
            frozenNextVersion: 3,
            imports: [],
            deletes: firstDeletes);
        firstPayload.EncodedByteCount.Should().Be(firstBytes);
        var deleting = CreateProgressControl(
            hidden,
            StoragePartitionMovePhase.SourceDeleting,
            firstPayload,
            importedRecordCount: 0,
            importedByteCount: 0,
            deletedRecordCount: 1,
            deletedByteCount: firstBytes);
        await persistence.CommitAsync(
            CreatePageEntry(persistence, StorageJournalOperation.MoveDelete, firstPayload),
            deleting);
        StorageMovePageOperations.ApplyDeletes(view, firstDeletes);

        var recoveredPersistence = CreatePersistence(manifest, partitionKey);
        var recovered = await recoveredPersistence.ActivateAsync();
        recovered.Should().ContainSingle().Which.Key.Should().Be(secondKey);
        AssertRecord(recovered[secondKey], secondRecord);

        var recoveredView = new StoragePartitionView(recovered, move.VirtualSlotCount);
        await recoveredPersistence.PrepareForProtocolMutationAsync(recovered);
        var secondDeletes = StorageMovePageOperations.CreateDeleteRecords(
            recoveredView,
            move.Slot,
            firstCursor,
            itemLimit: 1,
            byteTarget: StorageMoveProtocol.DefaultPageBytes,
            out var secondCursor,
            out var secondExhausted,
            out var secondBytes);
        secondExhausted.Should().BeTrue();
        secondDeletes.Should().ContainSingle();
        StorageMoveRecordCodec.DecodeRecordKey(secondDeletes[0]).Should().Be(secondKey);
        var secondPayload = CreatePagePayload(
            StorageJournalOperation.MoveDelete,
            move,
            pageOrdinal: 1,
            afterRecordKey: StorageMoveRecordCodec.DecodeNullableText(
                firstCursor,
                nameof(firstCursor)),
            nextRecordKey: StorageMoveRecordCodec.DecodeNullableText(
                secondCursor,
                nameof(secondCursor))!,
            exhausted: secondExhausted,
            frozenNextVersion: 3,
            imports: [],
            deletes: secondDeletes);
        secondPayload.EncodedByteCount.Should().Be(secondBytes);
        var complete = CreateProgressControl(
            deleting,
            StoragePartitionMovePhase.SourceDeleteComplete,
            secondPayload,
            importedRecordCount: 0,
            importedByteCount: 0,
            deletedRecordCount: 2,
            deletedByteCount: checked(firstBytes + secondBytes));
        await recoveredPersistence.CommitAsync(
            CreatePageEntry(
                recoveredPersistence,
                StorageJournalOperation.MoveDelete,
                secondPayload),
            complete);
        StorageMovePageOperations.ApplyDeletes(recoveredView, secondDeletes);

        var finalPersistence = CreatePersistence(manifest, partitionKey);
        (await finalPersistence.ActivateAsync()).Should().BeEmpty();
        finalPersistence.MoveControl.Phase.Should()
            .Be(StoragePartitionMovePhase.SourceDeleteComplete);
        finalPersistence.MoveControl.DeletedRecordCount.Should().Be(2);
    }

    private StoragePartitionPersistence CreatePersistence(
        TestPersistentState<StoragePartitionManifestState> manifest,
        string partitionKey)
    {
        return new StoragePartitionPersistence(
            manifest,
            _fixture.Cluster.GrainFactory,
            partitionKey,
            static () => { },
            NullLogger<StoragePartitionPersistence>.Instance);
    }

    private async Task<StorageSnapshotState> GetActiveSnapshotAsync(
        StoragePartitionManifestState manifest,
        string partitionKey)
    {
        manifest.ActiveSnapshot.IsPresent.Should().BeTrue();
        return await _fixture.Cluster.GrainFactory.GetGrain<IStorageSnapshotGrain>(
                StoragePersistence.CreateSnapshotSlotKey(
                    partitionKey,
                    manifest.ActiveSnapshot.Slot))
            .ReadAsync();
    }

    private static async Task CommitUpsertAsync(
        StoragePartitionPersistence persistence,
        Dictionary<string, StoredRecord> records,
        string recordKey,
        StoredRecord record,
        long nextVersion)
    {
        await persistence.PrepareForProtocolMutationAsync(records);
        await persistence.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = recordKey,
            Record = StoragePersistenceStateCopy.CopyRecord(record),
            NextVersionAfter = nextVersion,
        });
        records.Add(recordKey, StoragePersistenceStateCopy.CopyRecord(record)!);
    }

    private static StorageJournalEntry CreateAdvanceEntry(
        StoragePartitionPersistence persistence,
        StorageMoveIdentity move,
        long frozenNextVersion)
    {
        return new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = persistence.CommittedOperationId,
            Operation = StorageJournalOperation.AdvanceVersion,
            RecordKey = string.Empty,
            NextVersionAfter = frozenNextVersion,
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

    private static StorageJournalEntry CreatePageEntry(
        StoragePartitionPersistence persistence,
        StorageJournalOperation operation,
        StorageMoveJournalPayload payload)
    {
        return new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = persistence.CommittedOperationId,
            Operation = operation,
            RecordKey = string.Empty,
            NextVersionAfter = persistence.NextVersion,
            Move = payload,
        };
    }

    private static StorageMoveJournalPayload CreatePagePayload(
        StorageJournalOperation operation,
        StorageMoveIdentity move,
        long pageOrdinal,
        string? afterRecordKey,
        string nextRecordKey,
        bool exhausted,
        long frozenNextVersion,
        List<StorageMoveRecord> imports,
        List<StorageMoveDeleteRecord> deletes)
    {
        var encodedByteCount = imports.Count == 0
            ? StorageMovePageDigest.GetEncodedByteCount(deletes)
            : StorageMovePageDigest.GetEncodedByteCount(imports);
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            PageOrdinal = pageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.EncodeNullableText(afterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.EncodeText(nextRecordKey),
            Exhausted = exhausted,
            FrozenNextVersion = frozenNextVersion,
            Imports = imports,
            Deletes = deletes,
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.DefaultPageBytes,
            EncodedByteCount = encodedByteCount,
        };
        return new StorageMoveJournalPayload
        {
            MoveId = unsigned.MoveId,
            Slot = unsigned.Slot,
            VirtualSlotCount = unsigned.VirtualSlotCount,
            SourceEpoch = unsigned.SourceEpoch,
            SourceOwner = unsigned.SourceOwner,
            TargetOwner = unsigned.TargetOwner,
            PageOrdinal = unsigned.PageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(unsigned.AfterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.CopyText(unsigned.NextRecordKey),
            Exhausted = unsigned.Exhausted,
            PageDigest = StorageMovePageDigest.Compute(operation, unsigned),
            FrozenNextVersion = unsigned.FrozenNextVersion,
            Imports = unsigned.Imports.Select(StorageMoveRecordCodec.Copy).ToList(),
            Deletes = unsigned.Deletes.Select(StorageMoveRecordCodec.Copy).ToList(),
            ItemLimit = unsigned.ItemLimit,
            ByteTarget = unsigned.ByteTarget,
            EncodedByteCount = unsigned.EncodedByteCount,
        };
    }

    private static StoragePartitionMoveControl CreateProgressControl(
        StoragePartitionMoveControl previous,
        StoragePartitionMovePhase phase,
        StorageMoveJournalPayload payload,
        long importedRecordCount,
        long importedByteCount,
        long deletedRecordCount,
        long deletedByteCount)
    {
        var result = previous.Copy();
        result.Phase = phase;
        result.ProgressAfterRecordKey = StorageMoveRecordCodec.CopyText(payload.NextRecordKey);
        result.NextPageOrdinal = checked(payload.PageOrdinal + 1);
        result.LastPageDigest = [.. payload.PageDigest];
        result.ImportedRecordCount = importedRecordCount;
        result.ImportedByteCount = importedByteCount;
        result.DeletedRecordCount = deletedRecordCount;
        result.DeletedByteCount = deletedByteCount;
        result.LastPageRequestAfterRecordKey = StorageMoveRecordCodec.CopyText(
            payload.AfterRecordKey);
        result.LastPageItemLimit = payload.ItemLimit;
        result.LastPageByteTarget = payload.ByteTarget;
        result.LastPageEncodedByteCount = payload.EncodedByteCount;
        return result;
    }

    private static StoragePartitionMoveControl CreateControl(
        StorageMoveIdentity move,
        StoragePartitionMoveRole role,
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
            Role = role,
            Phase = phase,
            FrozenNextVersion = frozenNextVersion,
        };
    }

    private static StorageMoveIdentity CreateMoveIdentity()
    {
        return new StorageMoveIdentity
        {
            ProtocolVersion = StorageMoveProtocol.Version,
            MoveId = Guid.NewGuid(),
            Slot = 0,
            VirtualSlotCount = 2,
            SourceEpoch = 1,
            SourceOwner = 0,
            TargetOwner = 1,
        };
    }

    private static StoredRecord CreateRecord(
        string recordKey,
        string etag,
        string surrogate,
        StorageMoveIdentity move,
        int keySeed = 0)
    {
        GrainId grainId;
        do
        {
            grainId = GrainId.Create("movement-snapshot", $"record-{keySeed++}");
        }
        while (StorageLayout.GetSlot(grainId, move.VirtualSlotCount) != move.Slot);

        return new StoredRecord
        {
            GrainId = grainId,
            Payload = [1, 2, 3, checked((byte)etag[0])],
            ETag = etag,
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = $"scope/{recordKey}/{surrogate}",
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create($"text/{surrogate}"),
                },
            ],
        };
    }

    private static void AssertRecord(StorageMoveStoredRecord actual, StoredRecord expected)
    {
        AssertRecord(StorageMoveRecordCodec.Decode(actual), expected);
    }

    private static void AssertRecord(StoredRecord actual, StoredRecord expected)
    {
        actual.GrainId.Should().Be(expected.GrainId);
        actual.Payload.Should().Equal(expected.Payload);
        actual.ETag.Should().Be(expected.ETag);
        actual.IndexEntries.Should().ContainSingle();
        actual.IndexEntries[0].Scope.Should().Be(expected.IndexEntries[0].Scope);
        actual.IndexEntries[0].Value.Text.Should().Be(expected.IndexEntries[0].Value.Text);
    }
}
