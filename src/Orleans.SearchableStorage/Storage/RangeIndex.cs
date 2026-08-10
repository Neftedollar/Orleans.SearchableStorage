using System.Diagnostics.CodeAnalysis;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

internal sealed class RangeIndex
{
    private readonly SortedSet<Bucket> _buckets;
    private readonly IComparer<IndexValue> _valueComparer;

    public bool IsEmpty => _buckets.Count == 0;

    public RangeIndex(
        IDictionary<IndexValue, HashSet<string>> buckets,
        IComparer<IndexValue>? comparer = null)
    {
        ArgumentNullException.ThrowIfNull(buckets);

        _valueComparer = comparer ?? Comparer<IndexValue>.Default;
        _buckets = new SortedSet<Bucket>(new BucketComparer(_valueComparer));
        foreach (var pair in buckets)
        {
            ArgumentNullException.ThrowIfNull(pair.Key);
            ArgumentNullException.ThrowIfNull(pair.Value);

            var candidate = new Bucket(pair.Key, pair.Value);
            if (_buckets.TryGetValue(candidate, out var existing))
            {
                // Persisted values are canonicalized by ordering before the indexed representation
                // is created. Activation must not depend on hash equality remaining identical to
                // ordering equality as new IndexValue kinds are added.
                existing.RecordKeys.UnionWith(candidate.RecordKeys);
            }
            else
            {
                _buckets.Add(candidate);
            }
        }
    }

    public bool TryGetValue(
        IndexValue value,
        [NotNullWhen(true)] out HashSet<string>? recordKeys)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (_buckets.TryGetValue(new Bucket(value), out var bucket))
        {
            recordKeys = bucket.RecordKeys;
            return true;
        }

        recordKeys = null;
        return false;
    }

    public bool Add(IndexValue value, string recordKey)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(recordKey);

        var candidate = new Bucket(value);
        if (_buckets.TryGetValue(candidate, out var bucket))
        {
            return bucket.RecordKeys.Add(recordKey);
        }

        candidate.RecordKeys.Add(recordKey);
        _buckets.Add(candidate);
        return true;
    }

    public bool Remove(IndexValue value, string recordKey)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(recordKey);

        var candidate = new Bucket(value);
        if (!_buckets.TryGetValue(candidate, out var bucket)
            || !bucket.RecordKeys.Remove(recordKey))
        {
            return false;
        }

        if (bucket.RecordKeys.Count == 0)
        {
            _buckets.Remove(bucket);
        }

        return true;
    }

    public void UnionRange(
        IndexValue? lowerBound,
        IndexValue? upperBound,
        bool includeLowerBound,
        bool includeUpperBound,
        HashSet<string> destination)
    {
        ArgumentNullException.ThrowIfNull(destination);

        if (lowerBound is not null
            && upperBound is not null
            && _valueComparer.Compare(lowerBound, upperBound) > 0)
        {
            throw new ArgumentException(
                "The lower range bound must not be greater than the upper range bound.",
                nameof(lowerBound));
        }

        if (_buckets.Count == 0)
        {
            return;
        }

        var first = _buckets.Min!;
        var last = _buckets.Max!;
        if ((lowerBound is not null && _valueComparer.Compare(lowerBound, last.Value) > 0)
            || (upperBound is not null && _valueComparer.Compare(upperBound, first.Value) < 0))
        {
            return;
        }

        var lowerBucket = lowerBound is null
            || _valueComparer.Compare(lowerBound, first.Value) < 0
                ? first
                : new Bucket(lowerBound);
        var upperBucket = upperBound is null
            || _valueComparer.Compare(upperBound, last.Value) > 0
                ? last
                : new Bucket(upperBound);

        // The set view seeks to its first bucket through the tree and then enumerates only the
        // requested window. SortedSet views are inclusive, so endpoint equality is filtered here.
        foreach (var bucket in _buckets.GetViewBetween(lowerBucket, upperBucket))
        {
            if (lowerBound is not null
                && !includeLowerBound
                && _valueComparer.Compare(bucket.Value, lowerBound) == 0)
            {
                continue;
            }

            if (upperBound is not null
                && !includeUpperBound
                && _valueComparer.Compare(bucket.Value, upperBound) == 0)
            {
                continue;
            }

            destination.UnionWith(bucket.RecordKeys);
        }
    }

    private sealed class Bucket
    {
        public Bucket(IndexValue value)
        {
            Value = value;
            RecordKeys = new HashSet<string>(StringComparer.Ordinal);
        }

        public Bucket(IndexValue value, IEnumerable<string> recordKeys)
        {
            Value = value;
            RecordKeys = new HashSet<string>(recordKeys, StringComparer.Ordinal);
        }

        public IndexValue Value { get; }

        public HashSet<string> RecordKeys { get; }
    }

    private sealed class BucketComparer(IComparer<IndexValue> valueComparer) : IComparer<Bucket>
    {
        public int Compare(Bucket? x, Bucket? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            if (x is null)
            {
                return -1;
            }

            if (y is null)
            {
                return 1;
            }

            return valueComparer.Compare(x.Value, y.Value);
        }
    }
}
