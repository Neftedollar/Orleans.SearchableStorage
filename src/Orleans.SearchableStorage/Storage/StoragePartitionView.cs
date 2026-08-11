namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Owns one activation's authoritative records and the indexes derived from them.
/// </summary>
internal sealed class StoragePartitionView
{
    public StoragePartitionView(
        Dictionary<string, StoredRecord> records,
        int? virtualSlotCount = null)
    {
        ArgumentNullException.ThrowIfNull(records);
        Records = records;
        Indexes = StoragePartitionIndexes.Build(records);
        OrderedIndexes = StoragePartitionOrderedIndexes.Build(records);
        SlotCatalog = virtualSlotCount is null
            ? null
            : new StoragePartitionSlotCatalog(records, virtualSlotCount.Value);
    }

    public Dictionary<string, StoredRecord> Records { get; }

    public StoragePartitionIndexes Indexes { get; }

    public StoragePartitionOrderedIndexes OrderedIndexes { get; }

    public StoragePartitionSlotCatalog? SlotCatalog { get; }

    public void ApplyUpsert(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);

        if (Records.TryGetValue(recordKey, out var current))
        {
            Indexes.RemoveRecord(recordKey, current);
            OrderedIndexes.RemoveRecord(recordKey, current);
            SlotCatalog?.Remove(recordKey, current);
        }

        Indexes.AddRecord(recordKey, record);
        OrderedIndexes.AddRecord(recordKey, record);
        SlotCatalog?.Add(recordKey, record);
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
        SlotCatalog?.Remove(recordKey, current);
        Records.Remove(recordKey);
    }
}
