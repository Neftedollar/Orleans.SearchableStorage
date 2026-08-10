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

internal enum PartitionQueryOperation
{
    Empty = 0,
    Exact = 1,
    Range = 2,
    And = 3,
    Or = 4,
}
