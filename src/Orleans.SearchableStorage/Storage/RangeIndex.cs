using System.Diagnostics.CodeAnalysis;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

internal sealed class RangeIndex
{
    private readonly SortedList<IndexValue, HashSet<string>> _buckets;

    public RangeIndex(
        IDictionary<IndexValue, HashSet<string>> buckets,
        IComparer<IndexValue>? comparer = null)
    {
        ArgumentNullException.ThrowIfNull(buckets);

        var effectiveComparer = comparer ?? Comparer<IndexValue>.Default;
        var orderedBuckets = new SortedDictionary<IndexValue, HashSet<string>>(effectiveComparer);
        foreach (var pair in buckets)
        {
            ArgumentNullException.ThrowIfNull(pair.Key);
            ArgumentNullException.ThrowIfNull(pair.Value);

            // Persisted values are canonicalized by ordering before the indexed representation is
            // created. Activation must not depend on hash equality remaining identical to ordering
            // equality as new IndexValue kinds are added.
            if (orderedBuckets.TryGetValue(pair.Key, out var existing))
            {
                existing.UnionWith(pair.Value);
            }
            else
            {
                orderedBuckets.Add(
                    pair.Key,
                    new HashSet<string>(pair.Value, StringComparer.Ordinal));
            }
        }

        _buckets = new SortedList<IndexValue, HashSet<string>>(
            orderedBuckets,
            effectiveComparer);
    }

    public bool TryGetValue(
        IndexValue value,
        [NotNullWhen(true)] out HashSet<string>? recordKeys)
    {
        ArgumentNullException.ThrowIfNull(value);
        return _buckets.TryGetValue(value, out recordKeys);
    }

    public void UnionRange(
        IndexValue lowerBound,
        IndexValue upperBound,
        bool includeLowerBound,
        bool includeUpperBound,
        HashSet<string> destination)
    {
        ArgumentNullException.ThrowIfNull(lowerBound);
        ArgumentNullException.ThrowIfNull(upperBound);
        ArgumentNullException.ThrowIfNull(destination);

        if (_buckets.Comparer.Compare(lowerBound, upperBound) > 0)
        {
            throw new ArgumentException(
                "The lower range bound must not be greater than the upper range bound.",
                nameof(lowerBound));
        }

        // SortedList exposes indexed keys, so binary search can seek directly to the first
        // eligible bucket instead of enumerating every key below the lower bound.
        var startIndex = FindLowerBound(lowerBound, includeLowerBound);
        for (var index = startIndex; index < _buckets.Count; index++)
        {
            var comparison = _buckets.Comparer.Compare(_buckets.Keys[index], upperBound);
            if (comparison > 0 || (comparison == 0 && !includeUpperBound))
            {
                break;
            }

            destination.UnionWith(_buckets.Values[index]);
        }
    }

    private int FindLowerBound(IndexValue lowerBound, bool includeLowerBound)
    {
        var lowerIndex = 0;
        var upperIndex = _buckets.Count;
        while (lowerIndex < upperIndex)
        {
            var middleIndex = lowerIndex + ((upperIndex - lowerIndex) / 2);
            var comparison = _buckets.Comparer.Compare(_buckets.Keys[middleIndex], lowerBound);
            if (comparison < 0 || (comparison == 0 && !includeLowerBound))
            {
                lowerIndex = middleIndex + 1;
            }
            else
            {
                upperIndex = middleIndex;
            }
        }

        return lowerIndex;
    }
}
