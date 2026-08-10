using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Owns the mutable, partition-local projections derived from durable records.
/// </summary>
internal sealed class StoragePartitionIndexes
{
    private readonly Dictionary<string, Dictionary<IndexValue, HashSet<string>>> _hash =
        new(StringComparer.Ordinal);
    private readonly Dictionary<string, RangeIndex> _range = new(StringComparer.Ordinal);

    public static StoragePartitionIndexes Build(IReadOnlyDictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        var indexes = new StoragePartitionIndexes();
        foreach (var pair in records)
        {
            indexes.AddRecord(pair.Key, pair.Value);
        }

        return indexes;
    }

    public static void ValidateRecord(StoredRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(record.IndexEntries);

        foreach (var entry in record.IndexEntries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentException.ThrowIfNullOrWhiteSpace(entry.Scope);
            ArgumentNullException.ThrowIfNull(entry.Value);
            if (entry.Kind is not SearchableIndexKind.Hash and not SearchableIndexKind.Range)
            {
                throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
            }
        }
    }

    public void AddRecord(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ValidateRecord(record);

        foreach (var entry in record.IndexEntries)
        {
            switch (entry.Kind)
            {
                case SearchableIndexKind.Hash:
                    AddHashEntry(entry.Scope, entry.Value, recordKey);
                    break;
                case SearchableIndexKind.Range:
                    AddRangeEntry(entry.Scope, entry.Value, recordKey);
                    break;
                default:
                    throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
            }
        }
    }

    public void RemoveRecord(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ValidateRecord(record);

        foreach (var entry in record.IndexEntries)
        {
            switch (entry.Kind)
            {
                case SearchableIndexKind.Hash:
                    RemoveHashEntry(entry.Scope, entry.Value, recordKey);
                    break;
                case SearchableIndexKind.Range:
                    RemoveRangeEntry(entry.Scope, entry.Value, recordKey);
                    break;
                default:
                    throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
            }
        }
    }

    public HashSet<string> FindHashEntries(string scope, IndexValue value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ArgumentNullException.ThrowIfNull(value);

        return _hash.TryGetValue(scope, out var index)
            && index.TryGetValue(value, out var bucket)
                ? bucket
                : [];
    }

    public HashSet<string> FindRangeEntries(string scope, IndexValue value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ArgumentNullException.ThrowIfNull(value);

        return _range.TryGetValue(scope, out var index)
            && index.TryGetValue(value, out var bucket)
                ? bucket
                : [];
    }

    public void UnionRange(
        string scope,
        IndexValue? lowerBound,
        IndexValue? upperBound,
        bool includeLowerBound,
        bool includeUpperBound,
        HashSet<string> destination)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ArgumentNullException.ThrowIfNull(destination);

        if (_range.TryGetValue(scope, out var index))
        {
            index.UnionRange(
                lowerBound,
                upperBound,
                includeLowerBound,
                includeUpperBound,
                destination);
        }
    }

    private void AddHashEntry(string scope, IndexValue value, string recordKey)
    {
        if (!_hash.TryGetValue(scope, out var index))
        {
            index = new Dictionary<IndexValue, HashSet<string>>();
            _hash.Add(scope, index);
        }

        if (!index.TryGetValue(value, out var bucket))
        {
            bucket = new HashSet<string>(StringComparer.Ordinal);
            index.Add(value, bucket);
        }

        bucket.Add(recordKey);
    }

    private void RemoveHashEntry(string scope, IndexValue value, string recordKey)
    {
        if (!_hash.TryGetValue(scope, out var index)
            || !index.TryGetValue(value, out var bucket))
        {
            return;
        }

        bucket.Remove(recordKey);
        if (bucket.Count == 0)
        {
            index.Remove(value);
        }

        if (index.Count == 0)
        {
            _hash.Remove(scope);
        }
    }

    private void AddRangeEntry(string scope, IndexValue value, string recordKey)
    {
        if (!_range.TryGetValue(scope, out var index))
        {
            index = new RangeIndex(new Dictionary<IndexValue, HashSet<string>>());
            _range.Add(scope, index);
        }

        index.Add(value, recordKey);
    }

    private void RemoveRangeEntry(string scope, IndexValue value, string recordKey)
    {
        if (!_range.TryGetValue(scope, out var index))
        {
            return;
        }

        index.Remove(value, recordKey);
        if (index.IsEmpty)
        {
            _range.Remove(scope);
        }
    }
}
