using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

[GenerateSerializer]
internal sealed class StoragePartitionState
{
    [Id(0)]
    public long NextVersion { get; set; } = 1;

    [Id(1)]
    public Dictionary<string, StoredRecord> Records { get; set; } = new(StringComparer.Ordinal);

    public StoragePartitionState Copy()
    {
        return new StoragePartitionState
        {
            NextVersion = NextVersion,
            Records = Records.ToDictionary(
                pair => pair.Key,
                pair => pair.Value.Copy(),
                StringComparer.Ordinal),
        };
    }
}

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

    public StoredRecord Copy()
    {
        return new StoredRecord
        {
            GrainId = GrainId,
            Payload = Payload,
            ETag = ETag,
            IndexEntries = [.. IndexEntries],
        };
    }
}
