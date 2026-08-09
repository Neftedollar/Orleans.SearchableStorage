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
    public IndexValue? LowerBound { get; init; }

    [Id(2)]
    public IndexValue? UpperBound { get; init; }

    [Id(3)]
    public bool IncludeLowerBound { get; init; }

    [Id(4)]
    public bool IncludeUpperBound { get; init; }
}
