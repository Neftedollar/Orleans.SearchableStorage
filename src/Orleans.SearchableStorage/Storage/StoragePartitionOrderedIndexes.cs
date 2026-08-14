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
    private static readonly StoragePartitionRecordRefs EmptyRecordRefs =
        StoragePartitionRecordRefs.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
    private static readonly OrderedGrainGroups EmptyGroups =
        new(EmptyRecordRefs, isReadOnly: true);
    private readonly StoragePartitionRecordRefs _recordRefs;
    private readonly bool _ownsRecordRefs;
    private readonly Dictionary<string, OrderedGrainGroups> _catalogs =
        new(StringComparer.Ordinal);
    private readonly Dictionary<string, OrderedRangeIndex> _hash =
        new(StringComparer.Ordinal);
    private readonly Dictionary<string, OrderedRangeIndex> _range =
        new(StringComparer.Ordinal);

    private StoragePartitionOrderedIndexes(
        StoragePartitionRecordRefs recordRefs,
        bool ownsRecordRefs)
    {
        _recordRefs = recordRefs;
        _ownsRecordRefs = ownsRecordRefs;
    }

    internal static OrderedGrainGroups EmptyPosting => EmptyGroups;

    public static StoragePartitionOrderedIndexes Build(
        IReadOnlyDictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);

        return BuildCore(records, StoragePartitionRecordRefs.Build(records), ownsRecordRefs: true);
    }

    public static StoragePartitionOrderedIndexes Build(
        IReadOnlyDictionary<string, StoredRecord> records,
        StoragePartitionRecordRefs recordRefs)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(recordRefs);

        return BuildCore(records, recordRefs, ownsRecordRefs: false);
    }

    private static StoragePartitionOrderedIndexes BuildCore(
        IReadOnlyDictionary<string, StoredRecord> records,
        StoragePartitionRecordRefs recordRefs,
        bool ownsRecordRefs)
    {
        if (recordRefs.Count != records.Count)
        {
            throw new InvalidOperationException(
                "The activation-local record-reference table does not match the live record count.");
        }

        // Every live insertion is logarithmic in its catalog/posting size. Reusing that path also
        // makes activation rebuild and incremental mutation exercise identical invariants.
        var indexes = new StoragePartitionOrderedIndexes(recordRefs, ownsRecordRefs);
        foreach (var pair in records)
        {
            indexes.AddRecord(pair.Key, pair.Value);
        }

        return indexes;
    }

    public void AddRecord(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        StoragePartitionIndexValidation.ValidateRecord(record);

        if (!_recordRefs.TryGetRef(recordKey, out var recordRef))
        {
            if (!_ownsRecordRefs)
            {
                throw new InvalidOperationException(
                    $"No activation-local record reference exists for '{recordKey}'.");
            }

            recordRef = _recordRefs.Add(recordKey, record);
        }
        else if (!ReferenceEquals(_recordRefs.GetRecord(recordRef), record))
        {
            throw new InvalidOperationException(
                $"The activation-local record reference for '{recordKey}' identifies another record.");
        }

        var stateName = GetStateName(recordKey);
        if (!_catalogs.TryGetValue(stateName, out var catalog))
        {
            catalog = new OrderedGrainGroups(_recordRefs);
            _catalogs.Add(stateName, catalog);
        }

        catalog.Add(record.GrainId, recordRef);
        for (var index = 0; index < record.IndexEntries.Count; index++)
        {
            var entry = record.IndexEntries[index];
            var canonical = AddPostingEntry(entry, record.GrainId, recordRef);
            var canonicalValue = HasExactDurableRepresentation(entry.Value, canonical.Value)
                ? canonical.Value
                : entry.Value;
            if (!ReferenceEquals(entry.Scope, canonical.Scope)
                || !ReferenceEquals(entry.Value, canonicalValue))
            {
                // This reconstruction must copy every durable IndexEntry field. Sharing is
                // permitted only while the movement and snapshot representation stays byte-exact.
                record.IndexEntries[index] = new IndexEntry
                {
                    Scope = canonical.Scope,
                    Kind = entry.Kind,
                    Value = canonicalValue,
                };
            }
        }
    }

    public void RemoveRecord(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        StoragePartitionIndexValidation.ValidateRecord(record);
        var recordRef = _recordRefs.GetRequiredRef(recordKey);
        if (!ReferenceEquals(_recordRefs.GetRecord(recordRef), record))
        {
            throw new InvalidOperationException(
                $"The activation-local record reference for '{recordKey}' identifies another record.");
        }

        var stateName = GetStateName(recordKey);
        if (_catalogs.TryGetValue(stateName, out var catalog))
        {
            catalog.Remove(record.GrainId, recordRef);
            if (catalog.Count == 0)
            {
                _catalogs.Remove(stateName);
            }
        }

        foreach (var entry in record.IndexEntries)
        {
            RemovePostingEntry(entry, record.GrainId, recordRef);
        }

        if (_ownsRecordRefs)
        {
            _recordRefs.Remove(recordKey, record);
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
                ? hash.GetExactPosting(value)
                : EmptyGroups,
            SearchableIndexKind.Range => _range.TryGetValue(scope, out var range)
                ? range.GetExactPosting(value)
                : EmptyGroups,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown index kind."),
        };
    }

    public OrderedRangeBucketCursor CreateFacetValueCursor(
        string scope,
        SearchableIndexKind kind,
        IndexValue? after)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        return kind switch
        {
            SearchableIndexKind.Hash => _hash.TryGetValue(scope, out var hash)
                ? hash.CreateCursorAfter(after)
                : OrderedRangeBucketCursor.Empty(),
            SearchableIndexKind.Range => _range.TryGetValue(scope, out var range)
                ? range.CreateCursorAfter(after)
                : OrderedRangeBucketCursor.Empty(),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown index kind."),
        };
    }

    public long GetFacetRecordCount(string scope, SearchableIndexKind kind)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        return kind switch
        {
            SearchableIndexKind.Hash => _hash.TryGetValue(scope, out var hash)
                ? hash.TotalRecordCount
                : 0,
            SearchableIndexKind.Range => _range.TryGetValue(scope, out var range)
                ? range.TotalRecordCount
                : 0,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown index kind."),
        };
    }

    /// <summary>
    /// Materializes one exact posting as activation-local record references. The returned set is
    /// caller-owned and can be changed by legacy Boolean evaluation.
    /// </summary>
    public HashSet<int> FindExactRecordRefs(
        string scope,
        SearchableIndexKind kind,
        IndexValue value)
    {
        var posting = GetExactPosting(scope, kind, value);
        var result = new HashSet<int>();
        result.UnionWith(posting.EnumerateRecordRefs());
        return result;
    }

    public HashSet<string> ResolveRecordKeys(IEnumerable<int> recordRefs)
    {
        ArgumentNullException.ThrowIfNull(recordRefs);
        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (var recordRef in recordRefs)
        {
            result.Add(_recordRefs.GetRecordKey(recordRef));
        }

        return result;
    }

    public void UnionRangeRecordRefs(
        string scope,
        IndexValue? lowerBound,
        IndexValue? upperBound,
        bool includeLowerBound,
        bool includeUpperBound,
        HashSet<int> destination)
    {
        var work = default(NoPartitionQueryWorkSink);
        UnionRangeRecordRefs(
            scope,
            lowerBound,
            upperBound,
            includeLowerBound,
            includeUpperBound,
            destination,
            ref work);
    }

    internal void UnionRangeRecordRefs<TWorkSink>(
        string scope,
        IndexValue? lowerBound,
        IndexValue? upperBound,
        bool includeLowerBound,
        bool includeUpperBound,
        HashSet<int> destination,
        ref TWorkSink work)
        where TWorkSink : struct, IPartitionQueryWorkSink
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ArgumentNullException.ThrowIfNull(destination);
        if (lowerBound is not null
            && upperBound is not null
            && lowerBound.CompareTo(upperBound) > 0)
        {
            throw new ArgumentException(
                "The lower range bound must not be greater than the upper range bound.",
                nameof(lowerBound));
        }

        if (!_range.TryGetValue(scope, out var range))
        {
            return;
        }

        using var cursor = range.CreateCursor(lowerBound, upperBound);
        while (cursor.HasCurrent)
        {
            if (!cursor.TakeCurrentAndAdvance(out var bucket))
            {
                throw new InvalidOperationException(
                    "An ordered range cursor lost its prefetched bucket.");
            }

            if ((lowerBound is not null
                    && !includeLowerBound
                    && bucket.Value.CompareTo(lowerBound) == 0)
                || (upperBound is not null
                    && !includeUpperBound
                    && bucket.Value.CompareTo(upperBound) == 0))
            {
                work.RecordRangeBucket(candidateCount: 0);
                continue;
            }

            work.RecordRangeBucket(bucket.Posting.RecordCount);
            destination.UnionWith(bucket.Posting.EnumerateRecordRefs());
        }
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

    private (string Scope, IndexValue Value) AddPostingEntry(
        IndexEntry entry,
        GrainId grainId,
        int recordRef)
    {
        switch (entry.Kind)
        {
            case SearchableIndexKind.Hash:
                if (!_hash.TryGetValue(entry.Scope, out var hash))
                {
                    hash = new OrderedRangeIndex(entry.Scope, _recordRefs);
                    _hash.Add(hash.Scope, hash);
                }

                return (hash.Scope, hash.Add(entry.Value, grainId, recordRef));
            case SearchableIndexKind.Range:
                if (!_range.TryGetValue(entry.Scope, out var range))
                {
                    range = new OrderedRangeIndex(entry.Scope, _recordRefs);
                    _range.Add(range.Scope, range);
                }

                return (range.Scope, range.Add(entry.Value, grainId, recordRef));
            default:
                throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
        }
    }

    private void RemovePostingEntry(
        IndexEntry entry,
        GrainId grainId,
        int recordRef)
    {
        switch (entry.Kind)
        {
            case SearchableIndexKind.Hash:
                if (!_hash.TryGetValue(entry.Scope, out var hash)
                    || hash.GetExactPosting(entry.Value) == EmptyGroups)
                {
                    return;
                }

                hash.Remove(entry.Value, grainId, recordRef);

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

                range.Remove(entry.Value, grainId, recordRef);
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

    private static bool HasExactDurableRepresentation(IndexValue left, IndexValue right)
    {
        Span<int> leftDecimalBits = stackalloc int[4];
        Span<int> rightDecimalBits = stackalloc int[4];
        decimal.GetBits(left.Decimal, leftDecimalBits);
        decimal.GetBits(right.Decimal, rightDecimalBits);
        return left.Kind == right.Kind
            && string.Equals(left.Text, right.Text, StringComparison.Ordinal)
            && left.SignedInteger == right.SignedInteger
            && left.UnsignedInteger == right.UnsignedInteger
            && leftDecimalBits.SequenceEqual(rightDecimalBits)
            && BitConverter.DoubleToInt64Bits(left.FloatingPoint)
                == BitConverter.DoubleToInt64Bits(right.FloatingPoint)
            && left.UtcTicks == right.UtcTicks
            && left.Guid == right.Guid
            && left.Boolean == right.Boolean;
    }
}

/// <summary>
/// Mutable canonical GrainId grouping backed by a balanced tree. Both live mutation and exclusive
/// continuation seeking are O(log N); enumeration after the seek is O(K).
/// </summary>
internal sealed class OrderedGrainGroups
{
    private readonly SortedSet<OrderedGrainGroup> _groups = new(OrderedGrainGroupComparer.Instance);
    private readonly StoragePartitionRecordRefs _recordRefs;
    private readonly bool _isReadOnly;
    private int _recordCount;

    public OrderedGrainGroups(
        StoragePartitionRecordRefs recordRefs,
        bool isReadOnly = false)
    {
        ArgumentNullException.ThrowIfNull(recordRefs);
        _recordRefs = recordRefs;
        _isReadOnly = isReadOnly;
    }

    public int Count => _groups.Count;

    public int RecordCount => _recordCount;

    public void Add(GrainId grainId, int recordRef)
    {
        EnsureMutable();
        _ = _recordRefs.GetRecord(recordRef);

        var candidate = new OrderedGrainGroup(grainId);
        if (!_groups.TryGetValue(candidate, out var group))
        {
            group = candidate;
            _groups.Add(group);
        }

        if (group.AddRecordRef(recordRef, _recordRefs.RecordKeyComparer))
        {
            _recordCount = checked(_recordCount + 1);
        }
    }

    public void Remove(GrainId grainId, int recordRef)
    {
        EnsureMutable();
        _ = _recordRefs.GetRecord(recordRef);

        var candidate = new OrderedGrainGroup(grainId);
        if (!_groups.TryGetValue(candidate, out var group))
        {
            return;
        }

        if (!group.RemoveRecordRef(recordRef))
        {
            return;
        }

        _recordCount--;
        if (group.RecordRefCount == 0)
        {
            _groups.Remove(group);
        }
    }

    public bool TryGetRecordRefs(
        GrainId grainId,
        out OrderedRecordRefCollection recordRefs)
    {
        if (_groups.TryGetValue(new OrderedGrainGroup(grainId), out var group))
        {
            recordRefs = group.RecordRefs;
            return true;
        }

        recordRefs = default;
        return false;
    }

    public bool TryGetRecordKeys(GrainId grainId, out IReadOnlyCollection<string> recordKeys)
    {
        if (TryGetRecordRefs(grainId, out var refs))
        {
            recordKeys = _recordRefs.ResolveRecordKeys(refs);
            return true;
        }

        recordKeys = Array.Empty<string>();
        return false;
    }

    public IEnumerable<int> EnumerateRecordRefs()
    {
        foreach (var group in _groups)
        {
            foreach (var recordRef in group.RecordRefs)
            {
                yield return recordRef;
            }
        }
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
    private const int NoRecordRef = -1;
    private int _singleRecordRef = NoRecordRef;
    private SortedSet<int>? _multipleRecordRefs;

    public OrderedGrainGroup(GrainId grainId)
    {
        GrainId = grainId;
    }

    public GrainId GrainId { get; }

    public int RecordRefCount => _multipleRecordRefs?.Count
        ?? (_singleRecordRef == NoRecordRef ? 0 : 1);

    public OrderedRecordRefCollection RecordRefs => new(this);

    internal int SingleRecordRef => _singleRecordRef;

    internal SortedSet<int>? MultipleRecordRefs => _multipleRecordRefs;

    public bool AddRecordRef(int recordRef, IComparer<int> recordKeyComparer)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(recordRef);
        ArgumentNullException.ThrowIfNull(recordKeyComparer);
        if (_multipleRecordRefs is not null)
        {
            return _multipleRecordRefs.Add(recordRef);
        }

        if (_singleRecordRef == recordRef)
        {
            return false;
        }

        if (_singleRecordRef == NoRecordRef)
        {
            _singleRecordRef = recordRef;
            return true;
        }

        var multiple = new SortedSet<int>(recordKeyComparer)
        {
            _singleRecordRef,
        };
        if (!multiple.Add(recordRef))
        {
            throw new InvalidOperationException(
                "Distinct record references compared as the same durable record key.");
        }

        _singleRecordRef = NoRecordRef;
        _multipleRecordRefs = multiple;
        return true;
    }

    public bool RemoveRecordRef(int recordRef)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(recordRef);
        if (_multipleRecordRefs is null)
        {
            if (_singleRecordRef != recordRef)
            {
                return false;
            }

            _singleRecordRef = NoRecordRef;
            return true;
        }

        if (!_multipleRecordRefs.Remove(recordRef))
        {
            return false;
        }

        if (_multipleRecordRefs.Count == 1)
        {
            _singleRecordRef = _multipleRecordRefs.Min;
            _multipleRecordRefs = null;
        }

        return true;
    }
}

/// <summary>
/// Allocation-free view over the inline singleton or rare ordered overflow references in one
/// canonical GrainId group.
/// </summary>
internal readonly struct OrderedRecordRefCollection : IReadOnlyCollection<int>
{
    private readonly OrderedGrainGroup? _group;

    public OrderedRecordRefCollection(OrderedGrainGroup group)
    {
        ArgumentNullException.ThrowIfNull(group);
        _group = group;
    }

    public int Count => _group?.RecordRefCount ?? 0;

    public Enumerator GetEnumerator() => new(_group);

    IEnumerator<int> IEnumerable<int>.GetEnumerator() => GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
        GetEnumerator();

    internal struct Enumerator : IEnumerator<int>
    {
        private readonly bool _usesMultiple;
        private readonly int _singleRecordRef;
        private bool _singlePending;
        private SortedSet<int>.Enumerator _multiple;

        public Enumerator(OrderedGrainGroup? group)
        {
            if (group?.MultipleRecordRefs is { } multiple)
            {
                _usesMultiple = true;
                _singleRecordRef = -1;
                _singlePending = false;
                _multiple = multiple.GetEnumerator();
            }
            else
            {
                _usesMultiple = false;
                _singleRecordRef = group?.SingleRecordRef ?? -1;
                _singlePending = _singleRecordRef >= 0;
                _multiple = default;
            }
        }

        public int Current => _usesMultiple ? _multiple.Current : _singleRecordRef;

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_usesMultiple)
            {
                return _multiple.MoveNext();
            }

            if (!_singlePending)
            {
                return false;
            }

            _singlePending = false;
            return true;
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            if (_usesMultiple)
            {
                _multiple.Dispose();
            }
        }
    }
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
    private readonly StoragePartitionRecordRefs _recordRefs;
    private readonly SortedSet<OrderedRangeBucket> _buckets =
        new(OrderedRangeBucketComparer.Instance);

    public OrderedRangeIndex(
        string scope,
        StoragePartitionRecordRefs recordRefs)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ArgumentNullException.ThrowIfNull(recordRefs);
        Scope = scope;
        _recordRefs = recordRefs;
    }

    public string Scope { get; }

    public int Count => _buckets.Count;

    public long TotalRecordCount { get; private set; }

    public OrderedGrainGroups GetExactPosting(IndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return _buckets.TryGetValue(new OrderedRangeBucket(value), out var bucket)
            ? bucket.Posting
            : StoragePartitionOrderedIndexes.EmptyPosting;
    }

    private OrderedRangeBucket GetOrAddBucket(IndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var candidate = new OrderedRangeBucket(value);
        if (!_buckets.TryGetValue(candidate, out var bucket))
        {
            bucket = new OrderedRangeBucket(value, _recordRefs);
            _buckets.Add(bucket);
        }

        return bucket;
    }

    public IndexValue Add(IndexValue value, GrainId grainId, int recordRef)
    {
        var bucket = GetOrAddBucket(value);
        var posting = bucket.Posting;
        var before = posting.RecordCount;
        posting.Add(grainId, recordRef);
        if (posting.RecordCount != before)
        {
            TotalRecordCount = checked(TotalRecordCount + 1);
        }

        return bucket.Value;
    }

    public void Remove(IndexValue value, GrainId grainId, int recordRef)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (!_buckets.TryGetValue(new OrderedRangeBucket(value), out var bucket))
        {
            return;
        }

        var before = bucket.Posting.RecordCount;
        bucket.Posting.Remove(grainId, recordRef);
        if (bucket.Posting.RecordCount != before)
        {
            TotalRecordCount--;
        }

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

    public OrderedRangeBucketCursor CreateCursorAfter(IndexValue? after)
    {
        if (_buckets.Count == 0)
        {
            return OrderedRangeBucketCursor.Empty();
        }

        if (after is null)
        {
            return new OrderedRangeBucketCursor(_buckets.GetEnumerator());
        }

        var last = _buckets.Max!;
        if (after.CompareTo(last.Value) >= 0)
        {
            return OrderedRangeBucketCursor.Empty();
        }

        return new OrderedRangeBucketCursor(
            _buckets.GetViewBetween(new OrderedRangeBucket(after), last).GetEnumerator(),
            after);
    }

}

internal sealed class OrderedRangeBucketCursor : IDisposable
{
    private readonly IEnumerator<OrderedRangeBucket> _enumerator;
    private bool _hasCurrent;

    public OrderedRangeBucketCursor(IEnumerator<OrderedRangeBucket> enumerator)
        : this(enumerator, after: null)
    {
    }

    public OrderedRangeBucketCursor(
        IEnumerator<OrderedRangeBucket> enumerator,
        IndexValue? after)
    {
        ArgumentNullException.ThrowIfNull(enumerator);
        _enumerator = enumerator;
        _hasCurrent = _enumerator.MoveNext();
        if (_hasCurrent && after is not null && _enumerator.Current.Value.CompareTo(after) <= 0)
        {
            _hasCurrent = _enumerator.MoveNext();
        }
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
    private readonly OrderedGrainGroups? _posting;

    public OrderedRangeBucket(IndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Value = value;
    }

    public OrderedRangeBucket(
        IndexValue value,
        StoragePartitionRecordRefs recordRefs)
        : this(value)
    {
        ArgumentNullException.ThrowIfNull(recordRefs);
        _posting = new OrderedGrainGroups(recordRefs);
    }

    public IndexValue Value { get; }

    public OrderedGrainGroups Posting => _posting
        ?? throw new InvalidOperationException("A range search key has no posting.");
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
