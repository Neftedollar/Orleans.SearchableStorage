using System.Collections.ObjectModel;
using Orleans.Runtime;

namespace Orleans.SearchableStorage;

/// <summary>
/// Describes one bounded page request for a searchable-storage query.
/// </summary>
public sealed class SearchableStorageQueryPageRequest
{
    /// <summary>
    /// Initializes a new query page request.
    /// </summary>
    /// <param name="pageSize">The maximum number of grain identifiers requested in the page.</param>
    /// <param name="continuationToken">An opaque token returned by the preceding page, or <see langword="null"/> for the first page.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    /// <exception cref="ArgumentException"><paramref name="continuationToken"/> is empty or whitespace.</exception>
    public SearchableStorageQueryPageRequest(int pageSize, string? continuationToken = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        if (continuationToken is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(continuationToken);
        }

        PageSize = pageSize;
        ContinuationToken = continuationToken;
    }

    /// <summary>
    /// Gets the requested maximum item count.
    /// </summary>
    public int PageSize { get; }

    /// <summary>
    /// Gets the opaque continuation token, or <see langword="null"/> for the first page.
    /// </summary>
    public string? ContinuationToken { get; }
}

/// <summary>
/// Contains one bounded page of matching grain identifiers.
/// </summary>
/// <remarks>
/// A non-terminal page can be short or empty. Callers must continue until
/// <see cref="ContinuationToken"/> is <see langword="null"/>.
/// </remarks>
public sealed class SearchableStorageQueryPage
{
    /// <summary>
    /// Initializes a query page.
    /// </summary>
    /// <param name="items">The sorted, distinct grain identifiers in this page.</param>
    /// <param name="continuationToken">The opaque token for the next page, or <see langword="null"/> for the final page.</param>
    /// <exception cref="ArgumentNullException"><paramref name="items"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="continuationToken"/> is empty or whitespace.</exception>
    public SearchableStorageQueryPage(
        IReadOnlyList<GrainId> items,
        string? continuationToken)
    {
        ArgumentNullException.ThrowIfNull(items);
        if (continuationToken is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(continuationToken);
        }

        var copy = new GrainId[items.Count];
        for (var index = 0; index < copy.Length; index++)
        {
            copy[index] = items[index];
        }

        Items = new ReadOnlyCollection<GrainId>(copy);
        ContinuationToken = continuationToken;
    }

    /// <summary>
    /// Gets the sorted, distinct grain identifiers in this page.
    /// </summary>
    public IReadOnlyList<GrainId> Items { get; }

    /// <summary>
    /// Gets the opaque token for the next page, or <see langword="null"/> for the final page.
    /// </summary>
    public string? ContinuationToken { get; }
}
