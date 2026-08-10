namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Owns one activation's authoritative records and the indexes derived from them.
/// </summary>
internal sealed class StoragePartitionView
{
    public StoragePartitionView(Dictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);
        Records = records;
        Indexes = StoragePartitionIndexes.Build(records);
        OrderedIndexes = StoragePartitionOrderedIndexes.Build(records);
    }

    public Dictionary<string, StoredRecord> Records { get; }

    public StoragePartitionIndexes Indexes { get; }

    public StoragePartitionOrderedIndexes OrderedIndexes { get; }

    public void ApplyUpsert(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);

        if (Records.TryGetValue(recordKey, out var current))
        {
            Indexes.RemoveRecord(recordKey, current);
            OrderedIndexes.RemoveRecord(recordKey, current);
        }

        Indexes.AddRecord(recordKey, record);
        OrderedIndexes.AddRecord(recordKey, record);
        Records[recordKey] = record;
    }

    public void ApplyDelete(string recordKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        if (!Records.TryGetValue(recordKey, out var current))
        {
            return;
        }

        Indexes.RemoveRecord(recordKey, current);
        OrderedIndexes.RemoveRecord(recordKey, current);
        Records.Remove(recordKey);
    }
}
