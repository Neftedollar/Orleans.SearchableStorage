namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Maintains the activation-local record-key order for each persisted virtual slot. It is rebuilt
/// from durable records on activation and never changes the snapshot or journal layout.
/// </summary>
internal sealed class StoragePartitionSlotCatalog
{
    private readonly Dictionary<int, SortedSet<string>> _recordKeys = [];

    public StoragePartitionSlotCatalog(
        IReadOnlyDictionary<string, StoredRecord> records,
        int virtualSlotCount)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualSlotCount);
        if (virtualSlotCount > StorageLayout.MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(virtualSlotCount),
                virtualSlotCount,
                $"A slot catalog cannot exceed {StorageLayout.MaximumVirtualSlotCount} virtual slots.");
        }

        VirtualSlotCount = virtualSlotCount;
        foreach (var (recordKey, record) in records)
        {
            Add(recordKey, record);
        }
    }

    public int VirtualSlotCount { get; }

    public int GetRecordCount(int slot)
    {
        ValidateSlot(slot);
        return _recordKeys.TryGetValue(slot, out var keys) ? keys.Count : 0;
    }

    public IEnumerable<string> EnumerateAfter(int slot, string? afterRecordKey)
    {
        ValidateSlot(slot);
        if (!_recordKeys.TryGetValue(slot, out var keys) || keys.Count == 0)
        {
            return [];
        }

        if (afterRecordKey is null)
        {
            return keys;
        }

        var maximum = keys.Max!;
        if (StringComparer.Ordinal.Compare(afterRecordKey, maximum) >= 0)
        {
            return [];
        }

        // The slot is mutation-frozen while transfer cursors are used. Its actual maximum is a
        // valid inclusive upper bound and lets SortedSet seek the lower boundary in O(log N).
        return keys.GetViewBetween(afterRecordKey, maximum)
            .Where(key => !string.Equals(key, afterRecordKey, StringComparison.Ordinal));
    }

    public void Add(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        var slot = StorageLayout.GetSlot(record.GrainId, VirtualSlotCount);
        if (!_recordKeys.TryGetValue(slot, out var keys))
        {
            keys = new SortedSet<string>(StringComparer.Ordinal);
            _recordKeys.Add(slot, keys);
        }

        if (!keys.Add(recordKey))
        {
            throw new InvalidOperationException(
                $"Virtual-slot catalog {slot} already contains record key '{recordKey}'.");
        }
    }

    public void Remove(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        var slot = StorageLayout.GetSlot(record.GrainId, VirtualSlotCount);
        if (!_recordKeys.TryGetValue(slot, out var keys) || !keys.Remove(recordKey))
        {
            throw new InvalidOperationException(
                $"Virtual-slot catalog {slot} does not contain record key '{recordKey}'.");
        }

        if (keys.Count == 0)
        {
            _recordKeys.Remove(slot);
        }
    }

    private void ValidateSlot(int slot)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(slot);
        if (slot >= VirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slot),
                slot,
                $"A slot must be less than the catalog's {VirtualSlotCount} virtual slots.");
        }
    }
}
