using System.Collections.ObjectModel;

namespace Orleans.SearchableStorage;

/// <summary>
/// Selects the correctness contract for a bounded top-N facet query.
/// </summary>
public enum SearchableStorageFacetAccuracy
{
    /// <summary>
    /// Continue bounded candidate pages and exact-count probes until the global top N is proven.
    /// </summary>
    Exact = 0,

    /// <summary>
    /// Stop after the first bounded candidate turn and report a certified omitted-count bound.
    /// </summary>
    Approximate = 1,
}

/// <summary>
/// Describes a bounded top-N value-count facet request.
/// </summary>
public sealed class SearchableStorageFacetRequest
{
    /// <summary>
    /// Initializes a facet request. Accuracy is deliberately explicit at every call site.
    /// </summary>
    /// <param name="topN">The maximum number of values to return.</param>
    /// <param name="accuracy">The requested exact or bounded-approximate contract.</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is outside its supported domain.</exception>
    public SearchableStorageFacetRequest(int topN, SearchableStorageFacetAccuracy accuracy)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(topN);
        if (!Enum.IsDefined(accuracy))
        {
            throw new ArgumentOutOfRangeException(nameof(accuracy), accuracy, "Unknown facet accuracy.");
        }

        TopN = topN;
        Accuracy = accuracy;
    }

    /// <summary>Gets the maximum number of values to return.</summary>
    public int TopN { get; }

    /// <summary>Gets the explicitly selected correctness contract.</summary>
    public SearchableStorageFacetAccuracy Accuracy { get; }
}

/// <summary>
/// Describes one value-ordered page request for a distinct indexed-value facet.
/// </summary>
public sealed class SearchableStorageFacetPageRequest
{
    /// <summary>Initializes a distinct-value page request.</summary>
    /// <param name="pageSize">The maximum number of distinct values requested.</param>
    /// <param name="continuationToken">The preceding opaque continuation, or <see langword="null"/>.</param>
    public SearchableStorageFacetPageRequest(int pageSize, string? continuationToken = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        if (continuationToken is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(continuationToken);
        }

        PageSize = pageSize;
        ContinuationToken = continuationToken;
    }

    /// <summary>Gets the requested maximum number of distinct values.</summary>
    public int PageSize { get; }

    /// <summary>Gets the preceding opaque continuation, or <see langword="null"/>.</summary>
    public string? ContinuationToken { get; }
}

/// <summary>Contains one indexed facet value and its exact global count.</summary>
public sealed class SearchableStorageFacetValueCount<TValue>
{
    /// <summary>Initializes a value-count pair.</summary>
    /// <param name="value">The non-null indexed value.</param>
    /// <param name="count">The positive exact global count under the query predicate.</param>
    public SearchableStorageFacetValueCount(TValue value, long count)
    {
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value), "Null values are not indexed.");
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        Value = value;
        Count = count;
    }

    /// <summary>Gets the indexed value.</summary>
    public TValue Value { get; }

    /// <summary>Gets the exact global count for <see cref="Value"/> under the predicate.</summary>
    public long Count { get; }
}

/// <summary>Contains a bounded top-N value-count facet result.</summary>
public sealed class SearchableStorageFacetResult<TValue>
{
    /// <summary>Initializes a facet result.</summary>
    /// <param name="items">Exact positive counts in facet result order.</param>
    /// <param name="isExact">Whether <paramref name="items"/> is the proven global top N.</param>
    /// <param name="maximumOmittedCount">
    /// An inclusive certified upper bound for every omitted value count.
    /// </param>
    public SearchableStorageFacetResult(
        IReadOnlyList<SearchableStorageFacetValueCount<TValue>> items,
        bool isExact,
        long maximumOmittedCount)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentOutOfRangeException.ThrowIfNegative(maximumOmittedCount);
        var copy = new SearchableStorageFacetValueCount<TValue>[items.Count];
        for (var index = 0; index < copy.Length; index++)
        {
            copy[index] = items[index]
                ?? throw new ArgumentException(
                    "Facet result items cannot contain null elements.",
                    nameof(items));
        }

        Items = new ReadOnlyCollection<SearchableStorageFacetValueCount<TValue>>(copy);
        IsExact = isExact;
        MaximumOmittedCount = maximumOmittedCount;
    }

    /// <summary>
    /// Gets exact counts ordered by count descending and then by the indexed value's canonical order.
    /// </summary>
    public IReadOnlyList<SearchableStorageFacetValueCount<TValue>> Items { get; }

    /// <summary>Gets whether these items are the proven global top N.</summary>
    public bool IsExact { get; }

    /// <summary>
    /// Gets a certified inclusive upper bound on the count of every value omitted from
    /// <see cref="Items"/>. It is zero when no positive-count value can be omitted.
    /// </summary>
    public long MaximumOmittedCount { get; }
}

/// <summary>Contains one stable value-ordered page of distinct indexed values.</summary>
public sealed class SearchableStorageDistinctFacetPage<TValue>
{
    /// <summary>Initializes a distinct-value page.</summary>
    /// <param name="items">The non-null values in canonical value order.</param>
    /// <param name="continuationToken">The next opaque weak continuation, or <see langword="null"/>.</param>
    public SearchableStorageDistinctFacetPage(
        IReadOnlyList<TValue> items,
        string? continuationToken)
    {
        ArgumentNullException.ThrowIfNull(items);
        if (continuationToken is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(continuationToken);
        }

        var copy = new TValue[items.Count];
        for (var index = 0; index < copy.Length; index++)
        {
            copy[index] = items[index];
            if (copy[index] is null)
            {
                throw new ArgumentException("Null values are not indexed.", nameof(items));
            }
        }

        Items = new ReadOnlyCollection<TValue>(copy);
        ContinuationToken = continuationToken;
    }

    /// <summary>Gets the canonical value-ordered distinct values.</summary>
    public IReadOnlyList<TValue> Items { get; }

    /// <summary>Gets the next opaque continuation, or <see langword="null"/>.</summary>
    public string? ContinuationToken { get; }
}

/// <summary>Contains the exact minimum and maximum indexed values under a predicate.</summary>
public sealed class SearchableStorageFacetMinMax<TValue>
{
    /// <summary>Initializes an exact minimum/maximum pair.</summary>
    /// <param name="minimum">The non-null exact minimum indexed value.</param>
    /// <param name="maximum">The non-null exact maximum indexed value.</param>
    public SearchableStorageFacetMinMax(TValue minimum, TValue maximum)
    {
        if (minimum is null)
        {
            throw new ArgumentNullException(nameof(minimum), "Null values are not indexed.");
        }

        if (maximum is null)
        {
            throw new ArgumentNullException(nameof(maximum), "Null values are not indexed.");
        }

        Minimum = minimum;
        Maximum = maximum;
    }

    /// <summary>Gets the exact minimum indexed value.</summary>
    public TValue Minimum { get; }

    /// <summary>Gets the exact maximum indexed value.</summary>
    public TValue Maximum { get; }
}
