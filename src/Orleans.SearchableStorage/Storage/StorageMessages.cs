using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

[GenerateSerializer]
internal sealed class IndexEntry
{
    [Id(0)]
    public required string Scope { get; init; }

    [Id(1)]
    public SearchableIndexKind Kind { get; init; }

    [Id(2)]
    public required IndexValue Value { get; init; }
}

[GenerateSerializer]
internal sealed class StorageWriteRequest
{
    [Id(0)]
    public required string RecordKey { get; init; }

    [Id(1)]
    public required GrainId GrainId { get; init; }

    [Id(2)]
    public required byte[] Payload { get; init; }

    [Id(3)]
    public string? ExpectedETag { get; init; }

    [Id(4)]
    public required List<IndexEntry> IndexEntries { get; init; }

    [Id(5)]
    public required StoragePersistenceSettings Persistence { get; init; }
}

[GenerateSerializer]
internal sealed class StorageClearRequest
{
    [Id(0)]
    public required string RecordKey { get; init; }

    [Id(1)]
    public string? ExpectedETag { get; init; }

    [Id(2)]
    public required StoragePersistenceSettings Persistence { get; init; }
}

[GenerateSerializer]
internal sealed class StoragePersistenceSettings
{
    [Id(0)]
    public int JournalSegmentCapacity { get; init; }

    [Id(1)]
    public int MaximumJournalReplayEntries { get; init; }

    [Id(2)]
    public int CompactionThreshold { get; init; }
}

[GenerateSerializer]
internal sealed class StorageReadResult
{
    [Id(0)]
    public bool Found { get; init; }

    [Id(1)]
    public byte[]? Payload { get; init; }

    [Id(2)]
    public string? ETag { get; init; }
}

/// <summary>
/// Carries a point read together with the routing decision used by its caller.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedStorageReadRequest
{
    [Id(0)]
    public required string RecordKey { get; init; }

    [Id(1)]
    public int Slot { get; init; }

    [Id(2)]
    public long Epoch { get; init; }

    [Id(3)]
    public required GrainId GrainId { get; init; }
}

/// <summary>
/// Carries a point write together with the routing decision used by its caller.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedStorageWriteRequest
{
    [Id(0)]
    public required StorageWriteRequest Request { get; init; }

    [Id(1)]
    public int Slot { get; init; }

    [Id(2)]
    public long Epoch { get; init; }
}

/// <summary>
/// Carries a point clear together with the routing decision used by its caller.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedStorageClearRequest
{
    [Id(0)]
    public required StorageClearRequest Request { get; init; }

    [Id(1)]
    public int Slot { get; init; }

    [Id(2)]
    public long Epoch { get; init; }

    [Id(3)]
    public required GrainId GrainId { get; init; }
}

[GenerateSerializer]
internal sealed class StoragePartitionPersistenceInfo
{
    [Id(0)]
    public bool Initialized { get; init; }

    [Id(1)]
    public int JournalSegmentCapacity { get; init; }

    [Id(2)]
    public int MaximumJournalReplayEntries { get; init; }

    [Id(3)]
    public long WriterEpoch { get; init; }

    [Id(4)]
    public long CommittedSequence { get; init; }

    [Id(5)]
    public long SnapshotSequence { get; init; }

    [Id(6)]
    public long PrunedSequence { get; init; }

    [Id(7)]
    public long ActiveSnapshotGeneration { get; init; }

    [Id(8)]
    public long PendingSnapshotGeneration { get; init; }

    [Id(9)]
    public long RetiringSnapshotGeneration { get; init; }

    [Id(10)]
    public int RecordCount { get; init; }
}

[GenerateSerializer]
internal sealed class ExactIndexQuery
{
    [Id(0)]
    public required string Scope { get; init; }

    [Id(1)]
    public SearchableIndexKind Kind { get; init; }

    [Id(2)]
    public required IndexValue Value { get; init; }
}

[GenerateSerializer]
internal sealed class RangeIndexQuery
{
    [Id(0)]
    public required string Scope { get; init; }

    [Id(1)]
    public required IndexValue LowerBound { get; init; }

    [Id(2)]
    public required IndexValue UpperBound { get; init; }

    [Id(3)]
    public bool IncludeLowerBound { get; init; }

    [Id(4)]
    public bool IncludeUpperBound { get; init; }
}

/// <summary>
/// Carries an exact-index query for one routing epoch.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedExactIndexQuery
{
    [Id(0)]
    public required ExactIndexQuery Query { get; init; }

    [Id(1)]
    public long Epoch { get; init; }
}

/// <summary>
/// Carries a range-index query for one routing epoch.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedRangeIndexQuery
{
    [Id(0)]
    public required RangeIndexQuery Query { get; init; }

    [Id(1)]
    public long Epoch { get; init; }
}

/// <summary>
/// Carries one complete query to a partition. This message is serialized between Orleans
/// participants but is never part of persisted storage state.
/// </summary>
[GenerateSerializer]
internal sealed class PartitionQueryPlan
{
    [Id(0)]
    public PartitionQueryOperation Operation { get; init; }

    [Id(1)]
    public string? Scope { get; init; }

    [Id(2)]
    public SearchableIndexKind IndexKind { get; init; }

    [Id(3)]
    public IndexValue? Value { get; init; }

    [Id(4)]
    public IndexValue? LowerBound { get; init; }

    [Id(5)]
    public IndexValue? UpperBound { get; init; }

    [Id(6)]
    public bool IncludeLowerBound { get; init; }

    [Id(7)]
    public bool IncludeUpperBound { get; init; }

    [Id(8)]
    public PartitionQueryPlan? Left { get; init; }

    [Id(9)]
    public PartitionQueryPlan? Right { get; init; }
}

/// <summary>
/// Carries a complete partition query plan for one routing epoch.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedPartitionQuery
{
    [Id(0)]
    public required PartitionQueryPlan Query { get; init; }

    [Id(1)]
    public long Epoch { get; init; }
}

/// <summary>
/// Reports that a partition call was routed using a layout which is no longer authoritative.
/// </summary>
[GenerateSerializer]
internal sealed class StorageRouteMismatchException : Exception
{
    public StorageRouteMismatchException(
        long expectedEpoch,
        long currentEpoch,
        int requestedPartition,
        int? slot = null,
        int? currentOwner = null)
        : base(CreateMessage(expectedEpoch, currentEpoch, requestedPartition, slot, currentOwner))
    {
        ExpectedEpoch = expectedEpoch;
        CurrentEpoch = currentEpoch;
        RequestedPartition = requestedPartition;
        Slot = slot;
        CurrentOwner = currentOwner;
    }

    [Id(0)]
    public long ExpectedEpoch { get; private set; }

    [Id(1)]
    public long CurrentEpoch { get; private set; }

    [Id(2)]
    public int RequestedPartition { get; private set; }

    [Id(3)]
    public int? Slot { get; private set; }

    [Id(4)]
    public int? CurrentOwner { get; private set; }

    private static string CreateMessage(
        long expectedEpoch,
        long currentEpoch,
        int requestedPartition,
        int? slot,
        int? currentOwner)
    {
        if (slot is not null && currentOwner is not null)
        {
            return $"Routing epoch {expectedEpoch} sent virtual slot {slot} to physical partition "
                + $"{requestedPartition}, but layout epoch {currentEpoch} assigns that slot to partition {currentOwner}.";
        }

        return $"Routing epoch {expectedEpoch} sent a query to physical partition {requestedPartition}, "
            + $"but the current routing layout is at epoch {currentEpoch}.";
    }
}

internal enum PartitionQueryOperation
{
    Empty = 0,
    Exact = 1,
    Range = 2,
    And = 3,
    Or = 4,
}
