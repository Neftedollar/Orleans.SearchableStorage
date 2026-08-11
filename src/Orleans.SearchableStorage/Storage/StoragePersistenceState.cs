using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Stores the constant-size durable commit point for one storage partition.
/// </summary>
[GenerateSerializer]
internal sealed class StoragePartitionManifestState
{
    [Id(0)]
    public bool Initialized { get; set; }

    [Id(1)]
    public int PersistenceFormatVersion { get; set; }

    [Id(2)]
    public int JournalSegmentCapacity { get; set; }

    [Id(3)]
    public int MaximumJournalReplayEntries { get; set; }

    [Id(4)]
    public long WriterEpoch { get; set; }

    [Id(5)]
    public long CommittedSequence { get; set; }

    [Id(6)]
    public Guid CommittedOperationId { get; set; }

    [Id(7)]
    public long NextVersion { get; set; } = 1;

    [Id(8)]
    public StorageSnapshotDescriptor ActiveSnapshot { get; set; } = new();

    [Id(9)]
    public StorageSnapshotDescriptor PendingSnapshot { get; set; } = new();

    [Id(10)]
    public StorageSnapshotDescriptor RetiringSnapshot { get; set; } = new();

    [Id(11)]
    public long SnapshotGenerationHighWatermark { get; set; }

    [Id(12)]
    public long SnapshotSequence { get; set; }

    [Id(13)]
    public long PrunedSequence { get; set; }

    /// <summary>
    /// Identifies the movement protocol understood and durably enabled by this partition.
    /// </summary>
    [Id(14)]
    public int MovementProtocolVersion { get; set; }

    /// <summary>
    /// Requires every caller to use an epoch-bound routed operation. This is enabled only after
    /// the quiesced movement-protocol participant sweep has upgraded this manifest.
    /// </summary>
    [Id(15)]
    public bool RoutedOperationsRequired { get; set; }

    /// <summary>
    /// Rejects routed calls from epochs which can still consider a demoted slot authoritative.
    /// </summary>
    [Id(16)]
    public long MinimumRoutingEpoch { get; set; } = 1;

    /// <summary>
    /// Holds the single bounded source or target control record permitted by the provider-wide
    /// movement protocol.
    /// </summary>
    [Id(17)]
    public StoragePartitionMoveControl MoveControl { get; set; } = new();

    public StoragePartitionManifestState Copy()
    {
        return new StoragePartitionManifestState
        {
            Initialized = Initialized,
            PersistenceFormatVersion = PersistenceFormatVersion,
            JournalSegmentCapacity = JournalSegmentCapacity,
            MaximumJournalReplayEntries = MaximumJournalReplayEntries,
            WriterEpoch = WriterEpoch,
            CommittedSequence = CommittedSequence,
            CommittedOperationId = CommittedOperationId,
            NextVersion = NextVersion,
            ActiveSnapshot = ActiveSnapshot.Copy(),
            PendingSnapshot = PendingSnapshot.Copy(),
            RetiringSnapshot = RetiringSnapshot.Copy(),
            SnapshotGenerationHighWatermark = SnapshotGenerationHighWatermark,
            SnapshotSequence = SnapshotSequence,
            PrunedSequence = PrunedSequence,
            MovementProtocolVersion = MovementProtocolVersion,
            RoutedOperationsRequired = RoutedOperationsRequired,
            MinimumRoutingEpoch = MinimumRoutingEpoch,
            MoveControl = MoveControl.Copy(),
        };
    }
}

internal enum StoragePartitionMoveRole
{
    None = 0,
    Source = 1,
    Target = 2,
}

internal enum StoragePartitionMovePhase
{
    None = 0,
    SourceFrozen = 1,
    SourceHidden = 2,
    SourceDeleting = 3,
    SourceDeleteComplete = 4,
    TargetPrepared = 5,
    TargetImporting = 6,
    TargetImportComplete = 7,
    TargetEnabled = 8,
    TargetAbortDeleting = 9,
    TargetAbortComplete = 10,
}

/// <summary>
/// Stores constant-cardinality move identity and progress in the partition manifest. The one
/// ordinal record-key cursor can be as large as the largest accepted record identity, but no
/// record collection is embedded in the manifest.
/// </summary>
[GenerateSerializer]
internal sealed class StoragePartitionMoveControl
{
    [Id(0)]
    public bool IsPresent { get; set; }

    [Id(1)]
    public Guid MoveId { get; set; }

    [Id(2)]
    public int Slot { get; set; }

    [Id(3)]
    public int VirtualSlotCount { get; set; }

    [Id(4)]
    public long SourceEpoch { get; set; }

    [Id(5)]
    public int SourceOwner { get; set; }

    [Id(6)]
    public int TargetOwner { get; set; }

    [Id(7)]
    public StoragePartitionMoveRole Role { get; set; }

    [Id(8)]
    public StoragePartitionMovePhase Phase { get; set; }

    [Id(9)]
    public long FrozenNextVersion { get; set; }

    [Id(10)]
    public byte[]? ProgressAfterRecordKey { get; set; }

    [Id(11)]
    public long NextPageOrdinal { get; set; }

    [Id(12)]
    public byte[] LastPageDigest { get; set; } = [];

    [Id(13)]
    public long ImportedRecordCount { get; set; }

    [Id(14)]
    public long ImportedByteCount { get; set; }

    [Id(15)]
    public long DeletedRecordCount { get; set; }

    [Id(16)]
    public long DeletedByteCount { get; set; }

    [Id(17)]
    public byte[]? LastPageRequestAfterRecordKey { get; set; }

    [Id(18)]
    public int LastPageItemLimit { get; set; }

    [Id(19)]
    public int LastPageByteTarget { get; set; }

    [Id(20)]
    public long LastPageEncodedByteCount { get; set; }

    public StoragePartitionMoveControl Copy()
    {
        return new StoragePartitionMoveControl
        {
            IsPresent = IsPresent,
            MoveId = MoveId,
            Slot = Slot,
            VirtualSlotCount = VirtualSlotCount,
            SourceEpoch = SourceEpoch,
            SourceOwner = SourceOwner,
            TargetOwner = TargetOwner,
            Role = Role,
            Phase = Phase,
            FrozenNextVersion = FrozenNextVersion,
            ProgressAfterRecordKey = StorageMoveRecordCodec.CopyText(ProgressAfterRecordKey),
            NextPageOrdinal = NextPageOrdinal,
            LastPageDigest = [.. LastPageDigest],
            ImportedRecordCount = ImportedRecordCount,
            ImportedByteCount = ImportedByteCount,
            DeletedRecordCount = DeletedRecordCount,
            DeletedByteCount = DeletedByteCount,
            LastPageRequestAfterRecordKey = StorageMoveRecordCodec.CopyText(LastPageRequestAfterRecordKey),
            LastPageItemLimit = LastPageItemLimit,
            LastPageByteTarget = LastPageByteTarget,
            LastPageEncodedByteCount = LastPageEncodedByteCount,
        };
    }
}

/// <summary>
/// Identifies one immutable snapshot without embedding its record payload in the manifest.
/// </summary>
[GenerateSerializer]
internal sealed class StorageSnapshotDescriptor
{
    [Id(0)]
    public bool IsPresent { get; set; }

    [Id(1)]
    public int Slot { get; set; }

    [Id(2)]
    public long Generation { get; set; }

    [Id(3)]
    public Guid SnapshotId { get; set; }

    [Id(4)]
    public long Sequence { get; set; }

    [Id(5)]
    public Guid OperationId { get; set; }

    [Id(6)]
    public long NextVersion { get; set; }

    public StorageSnapshotDescriptor Copy()
    {
        return new StorageSnapshotDescriptor
        {
            IsPresent = IsPresent,
            Slot = Slot,
            Generation = Generation,
            SnapshotId = SnapshotId,
            Sequence = Sequence,
            OperationId = OperationId,
            NextVersion = NextVersion,
        };
    }

    public static StorageSnapshotDescriptor FromSnapshot(StorageSnapshotState snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = snapshot.Slot,
            Generation = snapshot.Generation,
            SnapshotId = snapshot.SnapshotId,
            Sequence = snapshot.Sequence,
            OperationId = snapshot.OperationId,
            NextVersion = snapshot.NextVersion,
        };
    }
}

[GenerateSerializer]
internal sealed class StorageJournalEntry
{
    [Id(0)]
    public long Sequence { get; init; }

    [Id(1)]
    public long WriterEpoch { get; init; }

    [Id(2)]
    public Guid OperationId { get; init; }

    [Id(3)]
    public Guid PreviousOperationId { get; init; }

    [Id(4)]
    public StorageJournalOperation Operation { get; init; }

    [Id(5)]
    public required string RecordKey { get; init; }

    [Id(6)]
    public string? ExpectedETag { get; init; }

    [Id(7)]
    public StoredRecord? Record { get; init; }

    [Id(8)]
    public long NextVersionAfter { get; init; }

    /// <summary>
    /// Carries the bounded payload for a version fence, import page, or move-delete page. Existing
    /// upsert and delete entries leave this appended field absent.
    /// </summary>
    [Id(9)]
    public StorageMoveJournalPayload? Move { get; init; }

    public StorageJournalEntry Copy()
    {
        return new StorageJournalEntry
        {
            Sequence = Sequence,
            WriterEpoch = WriterEpoch,
            OperationId = OperationId,
            PreviousOperationId = PreviousOperationId,
            Operation = Operation,
            RecordKey = RecordKey,
            ExpectedETag = ExpectedETag,
            Record = StoragePersistenceStateCopy.CopyRecord(Record),
            NextVersionAfter = NextVersionAfter,
            Move = Move?.Copy(),
        };
    }
}

internal enum StorageJournalOperation
{
    Upsert = 0,
    Delete = 1,
    AdvanceVersion = 2,
    Import = 3,
    MoveDelete = 4,
}

[GenerateSerializer]
internal sealed class StorageMoveJournalPayload
{
    [Id(0)]
    public Guid MoveId { get; init; }

    [Id(1)]
    public int Slot { get; init; }

    [Id(2)]
    public int VirtualSlotCount { get; init; }

    [Id(3)]
    public long SourceEpoch { get; init; }

    [Id(4)]
    public int SourceOwner { get; init; }

    [Id(5)]
    public int TargetOwner { get; init; }

    [Id(6)]
    public long PageOrdinal { get; init; }

    [Id(7)]
    public byte[]? AfterRecordKey { get; init; }

    [Id(8)]
    public byte[]? NextRecordKey { get; init; }

    [Id(9)]
    public bool Exhausted { get; init; }

    [Id(10)]
    public byte[] PageDigest { get; init; } = [];

    [Id(11)]
    public long FrozenNextVersion { get; init; }

    [Id(12)]
    public List<StorageMoveRecord> Imports { get; init; } = [];

    [Id(13)]
    public List<StorageMoveDeleteRecord> Deletes { get; init; } = [];

    [Id(14)]
    public int ItemLimit { get; init; }

    [Id(15)]
    public int ByteTarget { get; init; }

    [Id(16)]
    public long EncodedByteCount { get; init; }

    public StorageMoveJournalPayload Copy()
    {
        return new StorageMoveJournalPayload
        {
            MoveId = MoveId,
            Slot = Slot,
            VirtualSlotCount = VirtualSlotCount,
            SourceEpoch = SourceEpoch,
            SourceOwner = SourceOwner,
            TargetOwner = TargetOwner,
            PageOrdinal = PageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(AfterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.CopyText(NextRecordKey),
            Exhausted = Exhausted,
            PageDigest = [.. PageDigest],
            FrozenNextVersion = FrozenNextVersion,
            Imports = Imports.Select(static item => item.Copy()).ToList(),
            Deletes = Deletes.Select(static item => item.Copy()).ToList(),
            ItemLimit = ItemLimit,
            ByteTarget = ByteTarget,
            EncodedByteCount = EncodedByteCount,
        };
    }
}

[GenerateSerializer]
internal sealed class StorageMoveRecord
{
    [Id(0)]
    public required byte[] RecordKey { get; init; }

    [Id(1)]
    public required StorageMoveStoredRecord Record { get; init; }

    public StorageMoveRecord Copy()
    {
        return StorageMoveRecordCodec.Copy(this);
    }
}

[GenerateSerializer]
internal sealed class StorageMoveDeleteRecord
{
    [Id(0)]
    public required byte[] RecordKey { get; init; }

    [Id(1)]
    public required byte[] ExpectedETag { get; init; }

    public StorageMoveDeleteRecord Copy()
    {
        return StorageMoveRecordCodec.Copy(this);
    }
}

/// <summary>
/// Stores one reusable physical slot in the bounded journal ring.
/// </summary>
[GenerateSerializer]
internal sealed class StorageJournalSegmentState
{
    [Id(0)]
    public bool Initialized { get; set; }

    [Id(1)]
    public int Capacity { get; set; }

    [Id(2)]
    public long AbsoluteSegmentIndex { get; set; }

    [Id(3)]
    public long HighestWriterEpoch { get; set; }

    [Id(4)]
    public bool Tombstoned { get; set; }

    [Id(5)]
    public List<StorageJournalEntry> Entries { get; set; } = [];

    public StorageJournalSegmentState Copy()
    {
        return new StorageJournalSegmentState
        {
            Initialized = Initialized,
            Capacity = Capacity,
            AbsoluteSegmentIndex = AbsoluteSegmentIndex,
            HighestWriterEpoch = HighestWriterEpoch,
            Tombstoned = Tombstoned,
            Entries = Entries.Select(static entry => entry.Copy()).ToList(),
        };
    }
}

/// <summary>
/// Stores one immutable snapshot generation in one of two physical slots.
/// </summary>
[GenerateSerializer]
internal sealed class StorageSnapshotState
{
    [Id(0)]
    public bool Initialized { get; set; }

    [Id(1)]
    public bool Tombstoned { get; set; }

    [Id(2)]
    public int Slot { get; set; }

    [Id(3)]
    public long Generation { get; set; }

    [Id(4)]
    public Guid SnapshotId { get; set; }

    [Id(5)]
    public long Sequence { get; set; }

    [Id(6)]
    public Guid OperationId { get; set; }

    [Id(7)]
    public long NextVersion { get; set; } = 1;

    [Id(8)]
    public Dictionary<string, StoredRecord> Records { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Identifies the snapshot record payload representation. Version zero is the original
    /// string-based dictionary. Version one stores lossless movement records so persistence-v4
    /// compaction preserves every UTF-16 code unit accepted by the storage write domain.
    /// </summary>
    [Id(9)]
    public int RecordEncodingVersion { get; set; }

    [Id(10)]
    public List<StorageMoveRecord> LosslessRecords { get; set; } = [];

    public StorageSnapshotState Copy()
    {
        return new StorageSnapshotState
        {
            Initialized = Initialized,
            Tombstoned = Tombstoned,
            Slot = Slot,
            Generation = Generation,
            SnapshotId = SnapshotId,
            Sequence = Sequence,
            OperationId = OperationId,
            NextVersion = NextVersion,
            Records = Records.ToDictionary(
                static pair => pair.Key,
                static pair => StoragePersistenceStateCopy.CopyRecord(pair.Value)!,
                StringComparer.Ordinal),
            RecordEncodingVersion = RecordEncodingVersion,
            LosslessRecords = LosslessRecords.Select(StorageMoveRecordCodec.Copy).ToList(),
        };
    }
}

internal static class StoragePersistenceStateCopy
{
    public static StoredRecord? CopyRecord(StoredRecord? record)
    {
        if (record is null)
        {
            return null;
        }

        StoragePersistenceStateValidation.ValidateRecord(record, nameof(record));

        return new StoredRecord
        {
            GrainId = record.GrainId,
            Payload = [.. record.Payload],
            ETag = record.ETag,
            IndexEntries = record.IndexEntries.Select(CopyIndexEntry).ToList(),
        };
    }

    private static IndexEntry CopyIndexEntry(IndexEntry entry)
    {
        return new IndexEntry
        {
            Scope = entry.Scope,
            Kind = entry.Kind,
            Value = CopyIndexValue(entry.Value),
        };
    }

    private static IndexValue CopyIndexValue(IndexValue value)
    {
        return new IndexValue
        {
            Kind = value.Kind,
            Text = value.Text,
            SignedInteger = value.SignedInteger,
            UnsignedInteger = value.UnsignedInteger,
            Decimal = value.Decimal,
            FloatingPoint = value.FloatingPoint,
            UtcTicks = value.UtcTicks,
            Guid = value.Guid,
            Boolean = value.Boolean,
        };
    }
}

internal static class StoragePersistenceStateValidation
{
    public static void ValidateJournalEntry(StorageJournalEntry entry, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(entry, parameterName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(entry.Sequence, parameterName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(entry.WriterEpoch, parameterName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(entry.NextVersionAfter, parameterName);

        if (entry.OperationId == Guid.Empty)
        {
            throw new ArgumentException("A journal operation id must not be empty.", parameterName);
        }

        if (entry.OperationId == entry.PreviousOperationId)
        {
            throw new ArgumentException("A journal operation cannot refer to itself as its predecessor.", parameterName);
        }

        if ((entry.Sequence == 1) != (entry.PreviousOperationId == Guid.Empty))
        {
            throw new ArgumentException(
                "Only the first journal operation may have an empty previous operation id.",
                parameterName);
        }

        if (!Enum.IsDefined(entry.Operation))
        {
            throw new ArgumentOutOfRangeException(parameterName, entry.Operation, "Unknown journal operation.");
        }

        switch (entry.Operation)
        {
            case StorageJournalOperation.Upsert:
                ArgumentException.ThrowIfNullOrWhiteSpace(entry.RecordKey, parameterName);
                if (entry.Record is null
                    || entry.Move is not null)
                {
                    throw new ArgumentException(
                        "An upsert journal entry requires one record and cannot contain a move payload.",
                        parameterName);
                }

                ValidateRecord(entry.Record, parameterName);
                break;
            case StorageJournalOperation.Delete:
                ArgumentException.ThrowIfNullOrWhiteSpace(entry.RecordKey, parameterName);
                if (entry.Record is not null
                    || entry.Move is not null)
                {
                    throw new ArgumentException(
                        "A delete journal entry cannot contain a record or move payload.",
                        parameterName);
                }

                break;
            case StorageJournalOperation.AdvanceVersion:
            case StorageJournalOperation.Import:
            case StorageJournalOperation.MoveDelete:
                if (!string.IsNullOrEmpty(entry.RecordKey)
                    || entry.ExpectedETag is not null
                    || entry.Record is not null
                    || entry.Move is null)
                {
                    throw new ArgumentException(
                        "A movement journal entry must use only its bounded move payload.",
                        parameterName);
                }

                ValidateMoveJournalPayload(entry.Operation, entry.Move, parameterName);
                break;
            default:
                throw new ArgumentOutOfRangeException(parameterName, entry.Operation, "Unknown journal operation.");
        }
    }

    private static void ValidateMoveJournalPayload(
        StorageJournalOperation operation,
        StorageMoveJournalPayload move,
        string parameterName)
    {
        if (move.MoveId == Guid.Empty
            || move.Slot < 0
            || move.VirtualSlotCount <= 0
            || move.VirtualSlotCount > StorageLayout.MaximumVirtualSlotCount
            || move.Slot >= move.VirtualSlotCount
            || move.SourceEpoch <= 0
            || move.SourceOwner < 0
            || move.SourceOwner >= StorageLayout.MaximumVirtualSlotCount
            || move.TargetOwner < 0
            || move.TargetOwner >= StorageLayout.MaximumVirtualSlotCount
            || move.SourceOwner == move.TargetOwner
            || move.PageOrdinal < 0
            || move.FrozenNextVersion <= 0
            || move.Imports is null
            || move.Deletes is null
            || move.PageDigest is null)
        {
            throw new ArgumentException("A movement journal payload has invalid identity or bounds.", parameterName);
        }

        switch (operation)
        {
            case StorageJournalOperation.AdvanceVersion:
                if (move.PageOrdinal != 0
                    || move.AfterRecordKey is not null
                    || move.NextRecordKey is not null
                    || move.Exhausted
                    || move.PageDigest.Length != 0
                    || move.Imports.Count != 0
                    || move.Deletes.Count != 0
                    || move.ItemLimit != 0
                    || move.ByteTarget != 0
                    || move.EncodedByteCount != 0)
                {
                    throw new ArgumentException("A version-advance payload cannot contain page data.", parameterName);
                }

                return;
            case StorageJournalOperation.Import:
                if (move.Deletes.Count != 0)
                {
                    throw new ArgumentException("An import payload cannot contain delete entries.", parameterName);
                }

                foreach (var item in move.Imports)
                {
                    if (item is null)
                    {
                        throw new ArgumentException("An import payload cannot contain a null record.", parameterName);
                    }

                    StorageMoveRecordCodec.Validate(item, parameterName);
                }

                break;
            case StorageJournalOperation.MoveDelete:
                if (move.Imports.Count != 0)
                {
                    throw new ArgumentException("A move-delete payload cannot contain import entries.", parameterName);
                }

                foreach (var item in move.Deletes)
                {
                    if (item is null)
                    {
                        throw new ArgumentException("A move-delete payload cannot contain a null entry.", parameterName);
                    }

                    StorageMoveRecordCodec.Validate(item, parameterName);
                }

                break;
        }

        var itemCount = operation == StorageJournalOperation.Import
            ? move.Imports.Count
            : move.Deletes.Count;
        if (move.PageDigest.Length != StorageMovePageDigest.DigestLength
            || move.ItemLimit <= 0
            || move.ItemLimit > StorageMoveProtocol.MaximumPageRecords
            || move.ByteTarget <= 0
            || move.ByteTarget > StorageMoveProtocol.MaximumPageBytes
            || move.EncodedByteCount < 0
            || itemCount > move.ItemLimit
            || (move.Imports.Count == 0 && move.Deletes.Count == 0 && !move.Exhausted))
        {
            throw new ArgumentException("A movement page contains invalid progress metadata.", parameterName);
        }

        ValidateMovePage(operation, move, parameterName);
    }

    private static void ValidateMovePage(
        StorageJournalOperation operation,
        StorageMoveJournalPayload move,
        string parameterName)
    {
        var keys = operation == StorageJournalOperation.Import
            ? move.Imports.Select(static item => item.RecordKey)
            : move.Deletes.Select(static item => item.RecordKey);
        byte[]? previous = move.AfterRecordKey;
        var count = 0;
        foreach (var key in keys)
        {
            if (previous is not null
                && StorageMoveRecordCodec.CompareText(previous, key) >= 0)
            {
                throw new ArgumentException(
                    "A movement page must contain strictly increasing record keys after its cursor.",
                    parameterName);
            }

            previous = key;
            count++;
        }

        if (count == 0)
        {
            if (!move.Exhausted
                || !StorageMoveRecordCodec.TextEquals(
                    move.NextRecordKey,
                    move.AfterRecordKey))
            {
                throw new ArgumentException(
                    "Only a terminal movement page may be empty, and it must preserve its cursor.",
                    parameterName);
            }
        }
        else if (!StorageMoveRecordCodec.TextEquals(previous, move.NextRecordKey))
        {
            throw new ArgumentException(
                "A movement page cursor must identify its last record key.",
                parameterName);
        }

        var encodedByteCount = operation == StorageJournalOperation.Import
            ? StorageMovePageDigest.GetEncodedByteCount(move.Imports)
            : StorageMovePageDigest.GetEncodedByteCount(move.Deletes);
        if (move.EncodedByteCount != encodedByteCount
            || (encodedByteCount > move.ByteTarget && count != 1)
            || !StorageMovePageDigest.Equals(
                move.PageDigest,
                StorageMovePageDigest.Compute(operation, move)))
        {
            throw new ArgumentException(
                "A movement page has invalid byte accounting or canonical digest.",
                parameterName);
        }

        if (operation == StorageJournalOperation.Import)
        {
            foreach (var item in move.Imports)
            {
                if (StorageLayout.GetSlot(
                        StorageMoveRecordCodec.Decode(item.Record).GrainId,
                        move.VirtualSlotCount)
                    != move.Slot)
                {
                    throw new ArgumentException(
                        "An imported record does not belong to the movement slot.",
                        parameterName);
                }
            }
        }
    }

    public static void ValidateRecord(StoredRecord record, string parameterName)
    {
        if (record.GrainId.IsDefault)
        {
            throw new ArgumentException("A stored record must identify a grain.", parameterName);
        }

        ArgumentNullException.ThrowIfNull(record.Payload, parameterName);
        ArgumentException.ThrowIfNullOrWhiteSpace(record.ETag, parameterName);
        ArgumentNullException.ThrowIfNull(record.IndexEntries, parameterName);

        foreach (var indexEntry in record.IndexEntries)
        {
            if (indexEntry is null)
            {
                throw new ArgumentException("A stored record cannot contain a null index entry.", parameterName);
            }

            ArgumentException.ThrowIfNullOrWhiteSpace(indexEntry.Scope, parameterName);
            ArgumentNullException.ThrowIfNull(indexEntry.Value, parameterName);
            if (!Enum.IsDefined(indexEntry.Kind))
            {
                throw new ArgumentException($"Unknown searchable index kind '{indexEntry.Kind}'.", parameterName);
            }

            if (!Enum.IsDefined(indexEntry.Value.Kind))
            {
                throw new ArgumentException($"Unknown persisted index value kind '{indexEntry.Value.Kind}'.", parameterName);
            }

            if (indexEntry.Value.Kind == IndexValueKind.String && indexEntry.Value.Text is null)
            {
                throw new ArgumentException("A persisted string index value must not be null.", parameterName);
            }
        }
    }
}

internal static class StoragePersistenceStateEquality
{
    public static bool SnapshotEquals(StorageSnapshotState left, StorageSnapshotState right)
    {
        if (left.Initialized != right.Initialized
            || left.Tombstoned != right.Tombstoned
            || left.Slot != right.Slot
            || left.Generation != right.Generation
            || left.SnapshotId != right.SnapshotId
            || left.Sequence != right.Sequence
            || left.OperationId != right.OperationId
            || left.NextVersion != right.NextVersion
            || left.RecordEncodingVersion != right.RecordEncodingVersion
            || left.Records.Count != right.Records.Count
            || left.LosslessRecords.Count != right.LosslessRecords.Count)
        {
            return false;
        }

        foreach (var (recordKey, leftRecord) in left.Records)
        {
            if (!right.Records.TryGetValue(recordKey, out var rightRecord)
                || !RecordEquals(leftRecord, rightRecord))
            {
                return false;
            }
        }

        for (var index = 0; index < left.LosslessRecords.Count; index++)
        {
            if (!StorageMoveRecordCodec.BinaryEquals(
                    left.LosslessRecords[index],
                    right.LosslessRecords[index]))
            {
                return false;
            }
        }

        return true;
    }

    public static bool DescriptorEquals(StorageSnapshotDescriptor descriptor, StorageSnapshotState snapshot)
    {
        return descriptor.IsPresent
            && descriptor.Slot == snapshot.Slot
            && descriptor.Generation == snapshot.Generation
            && descriptor.SnapshotId == snapshot.SnapshotId
            && descriptor.Sequence == snapshot.Sequence
            && descriptor.OperationId == snapshot.OperationId
            && descriptor.NextVersion == snapshot.NextVersion;
    }

    public static bool JournalEntryEquals(StorageJournalEntry left, StorageJournalEntry right)
    {
        return left.Sequence == right.Sequence
            && left.WriterEpoch == right.WriterEpoch
            && left.OperationId == right.OperationId
            && left.PreviousOperationId == right.PreviousOperationId
            && left.Operation == right.Operation
            && string.Equals(left.RecordKey, right.RecordKey, StringComparison.Ordinal)
            && string.Equals(left.ExpectedETag, right.ExpectedETag, StringComparison.Ordinal)
            && left.NextVersionAfter == right.NextVersionAfter
            && RecordsEqual(left.Record, right.Record)
            && MovePayloadEquals(left.Move, right.Move);
    }

    private static bool MovePayloadEquals(
        StorageMoveJournalPayload? left,
        StorageMoveJournalPayload? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        if (left.MoveId != right.MoveId
            || left.Slot != right.Slot
            || left.VirtualSlotCount != right.VirtualSlotCount
            || left.SourceEpoch != right.SourceEpoch
            || left.SourceOwner != right.SourceOwner
            || left.TargetOwner != right.TargetOwner
            || left.PageOrdinal != right.PageOrdinal
            || !StorageMoveRecordCodec.TextEquals(left.AfterRecordKey, right.AfterRecordKey)
            || !StorageMoveRecordCodec.TextEquals(left.NextRecordKey, right.NextRecordKey)
            || left.Exhausted != right.Exhausted
            || !left.PageDigest.AsSpan().SequenceEqual(right.PageDigest)
            || left.FrozenNextVersion != right.FrozenNextVersion
            || left.Imports.Count != right.Imports.Count
            || left.Deletes.Count != right.Deletes.Count
            || left.ItemLimit != right.ItemLimit
            || left.ByteTarget != right.ByteTarget
            || left.EncodedByteCount != right.EncodedByteCount)
        {
            return false;
        }

        for (var index = 0; index < left.Imports.Count; index++)
        {
            if (!StorageMoveRecordCodec.TextEquals(
                    left.Imports[index].RecordKey,
                    right.Imports[index].RecordKey)
                || !RecordEquals(
                    StorageMoveRecordCodec.Decode(left.Imports[index].Record),
                    StorageMoveRecordCodec.Decode(right.Imports[index].Record)))
            {
                return false;
            }
        }

        for (var index = 0; index < left.Deletes.Count; index++)
        {
            if (!StorageMoveRecordCodec.TextEquals(
                    left.Deletes[index].RecordKey,
                    right.Deletes[index].RecordKey)
                || !StorageMoveRecordCodec.TextEquals(
                    left.Deletes[index].ExpectedETag,
                    right.Deletes[index].ExpectedETag))
            {
                return false;
            }
        }

        return true;
    }

    private static bool RecordsEqual(StoredRecord? left, StoredRecord? right)
    {
        return left is null ? right is null : right is not null && RecordEquals(left, right);
    }

    private static bool RecordEquals(StoredRecord left, StoredRecord right)
    {
        if (!left.GrainId.Equals(right.GrainId)
            || !left.Payload.AsSpan().SequenceEqual(right.Payload)
            || !string.Equals(left.ETag, right.ETag, StringComparison.Ordinal)
            || left.IndexEntries.Count != right.IndexEntries.Count)
        {
            return false;
        }

        for (var index = 0; index < left.IndexEntries.Count; index++)
        {
            var leftEntry = left.IndexEntries[index];
            var rightEntry = right.IndexEntries[index];
            if (!string.Equals(leftEntry.Scope, rightEntry.Scope, StringComparison.Ordinal)
                || leftEntry.Kind != rightEntry.Kind
                || !leftEntry.Value.Equals(rightEntry.Value))
            {
                return false;
            }
        }

        return true;
    }
}
