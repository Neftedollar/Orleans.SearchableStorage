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
        };
    }
}

internal enum StorageJournalOperation
{
    Upsert = 0,
    Delete = 1,
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
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.RecordKey, parameterName);
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

        if ((entry.Operation == StorageJournalOperation.Upsert) != (entry.Record is not null))
        {
            throw new ArgumentException(
                "An upsert journal entry requires a record, while a delete entry must not contain one.",
                parameterName);
        }

        if (entry.Record is not null)
        {
            ValidateRecord(entry.Record, parameterName);
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
            || left.Records.Count != right.Records.Count)
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
            && RecordsEqual(left.Record, right.Record);
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
