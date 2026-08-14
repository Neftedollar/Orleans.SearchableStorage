namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Assigns compact activation-local references to live records. References are never persisted,
/// exposed in continuations, or used as a query ordering key.
/// </summary>
internal sealed class StoragePartitionRecordRefs
{
    private readonly Dictionary<string, int> _refsByRecordKey = new(StringComparer.Ordinal);
    private readonly List<string?> _recordKeys = [];
    private readonly List<StoredRecord?> _records = [];
    private readonly Stack<int> _freeRefs = [];

    private StoragePartitionRecordRefs()
    {
        RecordKeyComparer = new RecordRefKeyComparer(this);
    }

    public int Count => _refsByRecordKey.Count;

    internal IComparer<int> RecordKeyComparer { get; }

    public static StoragePartitionRecordRefs Build(
        IReadOnlyDictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        var result = new StoragePartitionRecordRefs();
        foreach (var (recordKey, record) in records)
        {
            result.Add(recordKey, record);
        }

        return result;
    }

    public int Add(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        if (_refsByRecordKey.ContainsKey(recordKey))
        {
            throw new InvalidOperationException(
                $"The activation-local record table already contains '{recordKey}'.");
        }

        int recordRef;
        if (_freeRefs.TryPop(out var available))
        {
            recordRef = available;
            _recordKeys[recordRef] = recordKey;
            _records[recordRef] = record;
        }
        else
        {
            recordRef = _records.Count;
            _recordKeys.Add(recordKey);
            _records.Add(record);
        }

        _refsByRecordKey.Add(recordKey, recordRef);
        return recordRef;
    }

    public void Update(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        var recordRef = GetRequiredRef(recordKey);
        _records[recordRef] = record;
    }

    public void Remove(string recordKey, StoredRecord expectedRecord)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(expectedRecord);
        var recordRef = GetRequiredRef(recordKey);
        if (!ReferenceEquals(_records[recordRef], expectedRecord))
        {
            throw new InvalidOperationException(
                $"The activation-local record reference for '{recordKey}' identifies another record.");
        }

        _refsByRecordKey.Remove(recordKey);
        _recordKeys[recordRef] = null;
        _records[recordRef] = null;
        _freeRefs.Push(recordRef);
    }

    public bool TryGetRef(string recordKey, out int recordRef)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        return _refsByRecordKey.TryGetValue(recordKey, out recordRef);
    }

    public int GetRequiredRef(string recordKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        return _refsByRecordKey.TryGetValue(recordKey, out var recordRef)
            ? recordRef
            : throw new InvalidOperationException(
                $"The activation-local record table does not contain '{recordKey}'.");
    }

    public string GetRecordKey(int recordRef)
    {
        ValidateRef(recordRef);
        return _recordKeys[recordRef]
            ?? throw new InvalidOperationException(
                $"Activation-local record reference {recordRef} is not live.");
    }

    public StoredRecord GetRecord(int recordRef)
    {
        ValidateRef(recordRef);
        return _records[recordRef]
            ?? throw new InvalidOperationException(
                $"Activation-local record reference {recordRef} is not live.");
    }

    public IReadOnlyCollection<string> ResolveRecordKeys(
        OrderedRecordRefCollection recordRefs)
    {
        return new RecordKeyCollection(this, recordRefs);
    }

    private void ValidateRef(int recordRef)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(recordRef);
        if (recordRef >= _records.Count)
        {
            throw new ArgumentOutOfRangeException(
                nameof(recordRef),
                recordRef,
                "The activation-local record reference is outside the allocated table.");
        }
    }

    private sealed class RecordRefKeyComparer(StoragePartitionRecordRefs records) : IComparer<int>
    {
        public int Compare(int left, int right)
        {
            if (left == right)
            {
                return 0;
            }

            return StringComparer.Ordinal.Compare(
                records.GetRecordKey(left),
                records.GetRecordKey(right));
        }
    }

    private sealed class RecordKeyCollection(
        StoragePartitionRecordRefs records,
        OrderedRecordRefCollection recordRefs) : IReadOnlyCollection<string>
    {
        public int Count => recordRefs.Count;

        public IEnumerator<string> GetEnumerator()
        {
            foreach (var recordRef in recordRefs)
            {
                yield return records.GetRecordKey(recordRef);
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }
}
