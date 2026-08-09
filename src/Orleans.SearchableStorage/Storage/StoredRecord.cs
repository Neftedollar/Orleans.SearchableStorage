using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

[GenerateSerializer]
internal sealed class StoredRecord
{
    [Id(0)]
    public required GrainId GrainId { get; init; }

    [Id(1)]
    public required byte[] Payload { get; init; }

    [Id(2)]
    public required string ETag { get; init; }

    [Id(3)]
    public required List<IndexEntry> IndexEntries { get; init; }
}
