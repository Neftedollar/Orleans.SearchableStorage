namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Owns one activation's authoritative records and the indexes derived from them.
/// </summary>
internal sealed class StoragePartitionView
{
    private readonly StorageCapacityTracker _capacity;

    public StoragePartitionView(
        Dictionary<string, StoredRecord> records,
        int? virtualSlotCount = null)
    {
        ArgumentNullException.ThrowIfNull(records);
        Records = records;
        _capacity = new StorageCapacityTracker(records);
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

    public long CanonicalByteCount => _capacity.CanonicalByteCount;

    public void ValidateProjectedUpsert(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        _capacity.ValidateProjectedUpsert(Records, recordKey, record);
    }

    public void ValidateProjectedImports(IReadOnlyList<StorageMoveRecord> imports)
    {
        ArgumentNullException.ThrowIfNull(imports);
        _capacity.ValidateProjectedImports(Records, imports);
    }

    public void ApplyUpsert(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        _capacity.ValidateProjectedUpsert(Records, recordKey, record);

        if (Records.TryGetValue(recordKey, out var current))
        {
            Indexes.RemoveRecord(recordKey, current);
            OrderedIndexes.RemoveRecord(recordKey, current);
            SlotCatalog?.Remove(recordKey, current);
        }

        Indexes.AddRecord(recordKey, record);
        OrderedIndexes.AddRecord(recordKey, record);
        SlotCatalog?.Add(recordKey, record);
        _capacity.ApplyUpsert(Records, recordKey, record);
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
        _capacity.ApplyDelete(Records, recordKey);
        Records.Remove(recordKey);
    }
}
