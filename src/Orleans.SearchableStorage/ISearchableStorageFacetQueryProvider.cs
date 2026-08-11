using System.Linq.Expressions;

namespace Orleans.SearchableStorage;

/// <summary>
/// Opts an <see cref="IQueryable"/> provider into indexed-only facet execution.
/// </summary>
/// <remarks>
/// This contract is separate from the existing id terminals so external providers remain source
/// and binary compatible. Implementations must execute only indexed, non-null values. They must
/// preserve canonical value ordering, return exact counts for every emitted value, certify the
/// omitted-count bound of approximate results, treat distinct continuations as exclusive weak
/// value frontiers, and return no partial result when a bounded operation cannot complete.
/// </remarks>
public interface ISearchableStorageFacetQueryProvider
{
    /// <summary>Executes one stable value-ordered page of distinct indexed values.</summary>
    /// <typeparam name="TValue">The indexed property's exact CLR type.</typeparam>
    /// <param name="queryExpression">The complete query expression whose predicate scopes the facet.</param>
    /// <param name="propertySelector">A direct selector for the indexed property.</param>
    /// <param name="request">The bounded page request and optional weak continuation.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>
    /// A canonical value-ordered page. A continuation resumes exclusively after the last certified
    /// frontier and may miss values concurrently inserted before that weak frontier.
    /// </returns>
    Task<SearchableStorageDistinctFacetPage<TValue>> ExecuteDistinctFacetValuePageAsync<TValue>(
        Expression queryExpression,
        LambdaExpression propertySelector,
        SearchableStorageFacetPageRequest request,
        CancellationToken cancellationToken);

    /// <summary>Executes a bounded exact or approximate top-N value-count facet.</summary>
    /// <typeparam name="TValue">The indexed property's exact CLR type.</typeparam>
    /// <param name="queryExpression">The complete query expression whose predicate scopes the facet.</param>
    /// <param name="propertySelector">A direct selector for the indexed property.</param>
    /// <param name="request">The top-N limit and explicit accuracy contract.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>
    /// Exact counts ordered by count descending then canonical value ascending. Approximate results
    /// must provide an inclusive certified upper bound for every omitted value.
    /// </returns>
    Task<SearchableStorageFacetResult<TValue>> ExecuteFacetValueCountsAsync<TValue>(
        Expression queryExpression,
        LambdaExpression propertySelector,
        SearchableStorageFacetRequest request,
        CancellationToken cancellationToken);

    /// <summary>Executes an exact minimum/maximum indexed-value facet.</summary>
    /// <typeparam name="TValue">The indexed property's exact CLR type.</typeparam>
    /// <param name="queryExpression">The complete query expression whose predicate scopes the facet.</param>
    /// <param name="propertySelector">A direct selector for the indexed property.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>
    /// The exact minimum and maximum, or <see langword="null"/> when no indexed value matches. The
    /// operation must throw rather than return a partial pair when its bounded ceilings are reached.
    /// </returns>
    Task<SearchableStorageFacetMinMax<TValue>?> ExecuteFacetMinMaxAsync<TValue>(
        Expression queryExpression,
        LambdaExpression propertySelector,
        CancellationToken cancellationToken);
}
