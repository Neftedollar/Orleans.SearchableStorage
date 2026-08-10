using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Owns activation-local ordered catalogs and postings derived from durable records.
/// None of these nodes or collections are persisted or exposed in continuations.
/// </summary>
internal sealed class StoragePartitionOrderedIndexes
{
    private static readonly OrderedGrainGroups EmptyGroups = new(isReadOnly: true);
    private readonly Dictionary<string, OrderedGrainGroups> _catalogs =
        new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<IndexValue, OrderedGrainGroups>> _hash =
        new(StringComparer.Ordinal);
    private readonly Dictionary<string, OrderedRangeIndex> _range =
        new(StringComparer.Ordinal);

    internal static OrderedGrainGroups EmptyPosting => EmptyGroups;

    public static StoragePartitionOrderedIndexes Build(
        IReadOnlyDictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        // Every live insertion is logarithmic in its catalog/posting size. Reusing that path also
        // makes activation rebuild and incremental mutation exercise identical invariants.
        var indexes = new StoragePartitionOrderedIndexes();
        foreach (var pair in records)
        {
            indexes.AddRecord(pair.Key, pair.Value);
        }

        return indexes;
    }

    public void AddRecord(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        StoragePartitionIndexes.ValidateRecord(record);

        var stateName = GetStateName(recordKey);
        if (!_catalogs.TryGetValue(stateName, out var catalog))
        {
            catalog = new OrderedGrainGroups();
            _catalogs.Add(stateName, catalog);
        }

        catalog.Add(record.GrainId, recordKey);
        foreach (var entry in record.IndexEntries)
        {
            GetOrAddPosting(entry).Add(record.GrainId, recordKey);
        }
    }

    public void RemoveRecord(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        StoragePartitionIndexes.ValidateRecord(record);

        var stateName = GetStateName(recordKey);
        if (_catalogs.TryGetValue(stateName, out var catalog))
        {
            catalog.Remove(record.GrainId, recordKey);
            if (catalog.Count == 0)
            {
                _catalogs.Remove(stateName);
            }
        }

        foreach (var entry in record.IndexEntries)
        {
            RemovePostingEntry(entry, record.GrainId, recordKey);
        }
    }

    public OrderedGrainGroups GetStateCatalog(string stateName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        return _catalogs.TryGetValue(stateName, out var catalog) ? catalog : EmptyGroups;
    }

    public OrderedGrainGroups GetExactPosting(
        string scope,
        SearchableIndexKind kind,
        IndexValue value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ArgumentNullException.ThrowIfNull(value);

        return kind switch
        {
            SearchableIndexKind.Hash => _hash.TryGetValue(scope, out var hash)
                && hash.TryGetValue(value, out var hashPosting)
                    ? hashPosting
                    : EmptyGroups,
            SearchableIndexKind.Range => _range.TryGetValue(scope, out var range)
                ? range.GetExactPosting(value)
                : EmptyGroups,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown index kind."),
        };
    }

    public OrderedRangeBucketSelection CreateRangeBucketCursor(
        string scope,
        IndexValue? lowerBound,
        IndexValue? upperBound)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        if (!_range.TryGetValue(scope, out var range))
        {
            return new OrderedRangeBucketSelection(0, OrderedRangeBucketCursor.Empty());
        }

        return new OrderedRangeBucketSelection(
            range.Count,
            range.CreateCursor(lowerBound, upperBound));
    }

    private OrderedGrainGroups GetOrAddPosting(IndexEntry entry)
    {
        switch (entry.Kind)
        {
            case SearchableIndexKind.Hash:
                if (!_hash.TryGetValue(entry.Scope, out var hash))
                {
                    hash = new Dictionary<IndexValue, OrderedGrainGroups>();
                    _hash.Add(entry.Scope, hash);
                }

                if (!hash.TryGetValue(entry.Value, out var hashPosting))
                {
                    hashPosting = new OrderedGrainGroups();
                    hash.Add(entry.Value, hashPosting);
                }

                return hashPosting;
            case SearchableIndexKind.Range:
                if (!_range.TryGetValue(entry.Scope, out var range))
                {
                    range = new OrderedRangeIndex();
                    _range.Add(entry.Scope, range);
                }

                return range.GetOrAddPosting(entry.Value);
            default:
                throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
        }
    }

    private void RemovePostingEntry(
        IndexEntry entry,
        GrainId grainId,
        string recordKey)
    {
        switch (entry.Kind)
        {
            case SearchableIndexKind.Hash:
                if (!_hash.TryGetValue(entry.Scope, out var hash)
                    || !hash.TryGetValue(entry.Value, out var hashPosting))
                {
                    return;
                }

                hashPosting.Remove(grainId, recordKey);
                if (hashPosting.Count == 0)
                {
                    hash.Remove(entry.Value);
                }

                if (hash.Count == 0)
                {
                    _hash.Remove(entry.Scope);
                }

                return;
            case SearchableIndexKind.Range:
                if (!_range.TryGetValue(entry.Scope, out var range))
                {
                    return;
                }

                range.Remove(entry.Value, grainId, recordKey);
                if (range.Count == 0)
                {
                    _range.Remove(entry.Scope);
                }

                return;
            default:
                throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
        }
    }

    private static string GetStateName(string recordKey)
    {
        // The durable key is state-name/type-hex/key-hex. Split from the end so state names may
        // themselves contain '/'. Legacy tests and manually injected records which predate this
        // derived representation are kept in an isolated empty-name catalog.
        var keySeparator = recordKey.LastIndexOf('/');
        if (keySeparator <= 0)
        {
            return string.Empty;
        }

        var typeSeparator = recordKey.LastIndexOf('/', keySeparator - 1);
        return typeSeparator > 0 ? recordKey[..typeSeparator] : string.Empty;
    }
}

/// <summary>
/// Mutable canonical GrainId grouping backed by a balanced tree. Both live mutation and exclusive
/// continuation seeking are O(log N); enumeration after the seek is O(K).
/// </summary>
internal sealed class OrderedGrainGroups
{
    private readonly SortedSet<OrderedGrainGroup> _groups = new(OrderedGrainGroupComparer.Instance);
    private readonly bool _isReadOnly;

    public OrderedGrainGroups(bool isReadOnly = false)
    {
        _isReadOnly = isReadOnly;
    }

    public int Count => _groups.Count;

    public void Add(GrainId grainId, string recordKey)
    {
        EnsureMutable();
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);

        var candidate = new OrderedGrainGroup(grainId);
        if (!_groups.TryGetValue(candidate, out var group))
        {
            group = candidate;
            _groups.Add(group);
        }

        group.RecordKeys.Add(recordKey);
    }

    public void Remove(GrainId grainId, string recordKey)
    {
        EnsureMutable();
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);

        var candidate = new OrderedGrainGroup(grainId);
        if (!_groups.TryGetValue(candidate, out var group))
        {
            return;
        }

        group.RecordKeys.Remove(recordKey);
        if (group.RecordKeys.Count == 0)
        {
            _groups.Remove(group);
        }
    }

    public bool TryGetRecordKeys(GrainId grainId, out IReadOnlyCollection<string> recordKeys)
    {
        if (_groups.TryGetValue(new OrderedGrainGroup(grainId), out var group))
        {
            recordKeys = group.RecordKeys;
            return true;
        }

        recordKeys = Array.Empty<string>();
        return false;
    }

    public OrderedGrainGroupCursor CreateCursorAfter(bool hasAfter, GrainId after)
    {
        if (_groups.Count == 0)
        {
            return OrderedGrainGroupCursor.Empty();
        }

        if (!hasAfter)
        {
            return new OrderedGrainGroupCursor(_groups.GetEnumerator(), hasAfter: false, after);
        }

        var last = _groups.Max!;
        if (GrainIdCanonicalOrder.Compare(after, last.GrainId) >= 0)
        {
            return OrderedGrainGroupCursor.Empty();
        }

        var view = _groups.GetViewBetween(new OrderedGrainGroup(after), last);
        return new OrderedGrainGroupCursor(view.GetEnumerator(), hasAfter: true, after);
    }

    internal GrainId[] CopyGrainIds()
    {
        var result = new GrainId[_groups.Count];
        var index = 0;
        foreach (var group in _groups)
        {
            result[index++] = group.GrainId;
        }

        return result;
    }

    private void EnsureMutable()
    {
        if (_isReadOnly)
        {
            throw new InvalidOperationException("The shared empty ordered posting is immutable.");
        }
    }
}

internal sealed class OrderedGrainGroupCursor : IDisposable
{
    private readonly IEnumerator<OrderedGrainGroup> _enumerator;
    private bool _hasCurrent;

    public OrderedGrainGroupCursor(
        IEnumerator<OrderedGrainGroup> enumerator,
        bool hasAfter,
        GrainId after)
    {
        ArgumentNullException.ThrowIfNull(enumerator);
        _enumerator = enumerator;
        _hasCurrent = _enumerator.MoveNext();
        if (_hasCurrent
            && hasAfter
            && GrainIdCanonicalOrder.Compare(_enumerator.Current.GrainId, after) <= 0)
        {
            _hasCurrent = _enumerator.MoveNext();
        }
    }

    public bool HasCurrent => _hasCurrent;

    public bool TakeCurrentAndAdvance(out GrainId grainId)
    {
        if (!_hasCurrent)
        {
            grainId = default;
            return false;
        }

        grainId = _enumerator.Current.GrainId;
        _hasCurrent = _enumerator.MoveNext();
        return true;
    }

    public void Dispose() => _enumerator.Dispose();

    public static OrderedGrainGroupCursor Empty()
    {
        return new OrderedGrainGroupCursor(
            ((IEnumerable<OrderedGrainGroup>)Array.Empty<OrderedGrainGroup>()).GetEnumerator(),
            hasAfter: false,
            after: default);
    }
}

internal sealed class OrderedGrainGroup
{
    public OrderedGrainGroup(GrainId grainId)
    {
        GrainId = grainId;
    }

    public GrainId GrainId { get; }

    public SortedSet<string> RecordKeys { get; } = new(StringComparer.Ordinal);
}

internal sealed class OrderedGrainGroupComparer : IComparer<OrderedGrainGroup>
{
    public static OrderedGrainGroupComparer Instance { get; } = new();

    public int Compare(OrderedGrainGroup? left, OrderedGrainGroup? right)
    {
        if (ReferenceEquals(left, right))
        {
            return 0;
        }

        if (left is null)
        {
            return -1;
        }

        if (right is null)
        {
            return 1;
        }

        return GrainIdCanonicalOrder.Compare(left.GrainId, right.GrainId);
    }
}

internal readonly record struct OrderedRangeBucketSelection(
    int TotalBucketCount,
    OrderedRangeBucketCursor Cursor);

internal sealed class OrderedRangeIndex
{
    private readonly SortedSet<OrderedRangeBucket> _buckets =
        new(OrderedRangeBucketComparer.Instance);

    public int Count => _buckets.Count;

    public OrderedGrainGroups GetExactPosting(IndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return _buckets.TryGetValue(new OrderedRangeBucket(value), out var bucket)
            ? bucket.Posting
            : StoragePartitionOrderedIndexes.EmptyPosting;
    }

    public OrderedGrainGroups GetOrAddPosting(IndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var candidate = new OrderedRangeBucket(value);
        if (!_buckets.TryGetValue(candidate, out var bucket))
        {
            bucket = candidate;
            _buckets.Add(bucket);
        }

        return bucket.Posting;
    }

    public void Remove(IndexValue value, GrainId grainId, string recordKey)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (!_buckets.TryGetValue(new OrderedRangeBucket(value), out var bucket))
        {
            return;
        }

        bucket.Posting.Remove(grainId, recordKey);
        if (bucket.Posting.Count == 0)
        {
            _buckets.Remove(bucket);
        }
    }

    public OrderedRangeBucketCursor CreateCursor(
        IndexValue? lowerBound,
        IndexValue? upperBound)
    {
        if (_buckets.Count == 0)
        {
            return OrderedRangeBucketCursor.Empty();
        }

        var first = _buckets.Min!;
        var last = _buckets.Max!;
        if ((lowerBound is not null && lowerBound.CompareTo(last.Value) > 0)
            || (upperBound is not null && upperBound.CompareTo(first.Value) < 0))
        {
            return OrderedRangeBucketCursor.Empty();
        }

        var lower = lowerBound is null || lowerBound.CompareTo(first.Value) < 0
            ? first
            : new OrderedRangeBucket(lowerBound);
        var upper = upperBound is null || upperBound.CompareTo(last.Value) > 0
            ? last
            : new OrderedRangeBucket(upperBound);
        if (OrderedRangeBucketComparer.Instance.Compare(lower, upper) > 0)
        {
            return OrderedRangeBucketCursor.Empty();
        }

        return new OrderedRangeBucketCursor(
            _buckets.GetViewBetween(lower, upper).GetEnumerator());
    }
}

internal sealed class OrderedRangeBucketCursor : IDisposable
{
    private readonly IEnumerator<OrderedRangeBucket> _enumerator;
    private bool _hasCurrent;

    public OrderedRangeBucketCursor(IEnumerator<OrderedRangeBucket> enumerator)
    {
        ArgumentNullException.ThrowIfNull(enumerator);
        _enumerator = enumerator;
        _hasCurrent = _enumerator.MoveNext();
    }

    public bool HasCurrent => _hasCurrent;

    public bool TakeCurrentAndAdvance(out OrderedRangeBucket bucket)
    {
        if (!_hasCurrent)
        {
            bucket = null!;
            return false;
        }

        bucket = _enumerator.Current;
        _hasCurrent = _enumerator.MoveNext();
        return true;
    }

    public void Dispose() => _enumerator.Dispose();

    public static OrderedRangeBucketCursor Empty()
    {
        return new OrderedRangeBucketCursor(
            ((IEnumerable<OrderedRangeBucket>)Array.Empty<OrderedRangeBucket>()).GetEnumerator());
    }
}

internal sealed class OrderedRangeBucket
{
    public OrderedRangeBucket(IndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Value = value;
    }

    public IndexValue Value { get; }

    public OrderedGrainGroups Posting { get; } = new();
}

internal sealed class OrderedRangeBucketComparer : IComparer<OrderedRangeBucket>
{
    public static OrderedRangeBucketComparer Instance { get; } = new();

    public int Compare(OrderedRangeBucket? left, OrderedRangeBucket? right)
    {
        if (ReferenceEquals(left, right))
        {
            return 0;
        }

        if (left is null)
        {
            return -1;
        }

        if (right is null)
        {
            return 1;
        }

        return left.Value.CompareTo(right.Value);
    }
}
