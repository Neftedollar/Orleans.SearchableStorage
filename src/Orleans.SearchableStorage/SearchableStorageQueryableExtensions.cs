using Orleans.Runtime;

namespace Orleans.SearchableStorage;

/// <summary>
/// Executes deferred searchable-storage queries.
/// </summary>
public static class SearchableStorageQueryableExtensions
{
    /// <summary>
    /// Executes one stable value-ordered page of distinct values from an indexed property.
    /// </summary>
    /// <remarks>
    /// Null values are not indexed and therefore never appear. Pages are weakly consistent; a
    /// non-terminal page can be short or empty, so continue until the token is null.
    /// </remarks>
    /// <exception cref="ArgumentException">The selector is not a directly typed indexed property, or an argument is invalid.</exception>
    /// <exception cref="NotSupportedException">The query or provider does not support facets.</exception>
    /// <exception cref="SearchableStorageQueryConfigurationException">Continuation protection or a bounded policy is invalid.</exception>
    /// <exception cref="SearchableStorageInvalidContinuationTokenException">The continuation is invalid or belongs to another facet/policy.</exception>
    /// <exception cref="SearchableStorageStaleContinuationTokenException">The continuation names an obsolete layout.</exception>
    /// <exception cref="SearchableStorageQueryLimitExceededException">The page cannot make progress within its ceilings.</exception>
    /// <exception cref="SearchableStorageFacetConcurrentChangeException">Partition data changed repeatedly within this page.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static Task<SearchableStorageDistinctFacetPage<TValue>> ToDistinctFacetValuePageAsync<TState, TValue>(
        this IQueryable<TState> source,
        System.Linq.Expressions.Expression<Func<TState, TValue>> propertySelector,
        SearchableStorageFacetPageRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(propertySelector);
        ArgumentNullException.ThrowIfNull(request);
        var provider = GetFacetProvider(source.Provider);
        return provider.ExecuteDistinctFacetValuePageAsync<TValue>(
            source.Expression,
            propertySelector,
            request,
            cancellationToken);
    }

    /// <summary>
    /// Executes a bounded top-N count facet over an indexed property.
    /// </summary>
    /// <remarks>
    /// Returned counts are always exact. Approximate mode may omit a winner, but reports a
    /// certified inclusive upper bound for every omitted value through
    /// <see cref="SearchableStorageFacetResult{TValue}.MaximumOmittedCount"/>.
    /// </remarks>
    /// <exception cref="ArgumentException">The selector is not a directly typed indexed property, or an argument is invalid.</exception>
    /// <exception cref="NotSupportedException">The query or provider does not support facets.</exception>
    /// <exception cref="SearchableStorageQueryConfigurationException">The bounded facet policy is invalid.</exception>
    /// <exception cref="SearchableStorageQueryLimitExceededException">The terminal cannot complete within its aggregate ceilings.</exception>
    /// <exception cref="SearchableStorageFacetConcurrentChangeException">Partition data changed repeatedly within the attempt.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static Task<SearchableStorageFacetResult<TValue>> ToFacetValueCountsAsync<TState, TValue>(
        this IQueryable<TState> source,
        System.Linq.Expressions.Expression<Func<TState, TValue>> propertySelector,
        SearchableStorageFacetRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(propertySelector);
        ArgumentNullException.ThrowIfNull(request);
        var provider = GetFacetProvider(source.Provider);
        return provider.ExecuteFacetValueCountsAsync<TValue>(
            source.Expression,
            propertySelector,
            request,
            cancellationToken);
    }

    /// <summary>
    /// Executes an exact minimum/maximum facet over an indexed property.
    /// </summary>
    /// <returns>A minimum/maximum pair, or <see langword="null"/> when no non-null value matches.</returns>
    /// <exception cref="ArgumentException">The selector is not a directly typed indexed property, or an argument is invalid.</exception>
    /// <exception cref="NotSupportedException">The query or provider does not support facets.</exception>
    /// <exception cref="SearchableStorageQueryConfigurationException">The bounded facet policy is invalid.</exception>
    /// <exception cref="SearchableStorageQueryLimitExceededException">The terminal cannot complete within its aggregate ceilings.</exception>
    /// <exception cref="SearchableStorageFacetConcurrentChangeException">Partition data changed repeatedly within the attempt.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static Task<SearchableStorageFacetMinMax<TValue>?> ToFacetMinMaxAsync<TState, TValue>(
        this IQueryable<TState> source,
        System.Linq.Expressions.Expression<Func<TState, TValue>> propertySelector,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(propertySelector);
        var provider = GetFacetProvider(source.Provider);
        return provider.ExecuteFacetMinMaxAsync<TValue>(
            source.Expression,
            propertySelector,
            cancellationToken);
    }

    /// <summary>
    /// Executes one bounded page of a supported indexed predicate.
    /// </summary>
    /// <typeparam name="TState">The persisted state type being queried.</typeparam>
    /// <param name="source">A searchable-storage query.</param>
    /// <param name="request">The requested page size and optional continuation.</param>
    /// <param name="cancellationToken">A token which cancels waiting for the distributed query.</param>
    /// <returns>One sorted, distinct page and its optional continuation.</returns>
    /// <remarks>
    /// Pages are weakly consistent rather than a distributed snapshot. A non-terminal page can be
    /// short or empty; continue until its continuation token is <see langword="null"/>.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="request"/> is null.</exception>
    /// <exception cref="NotSupportedException">The query provider has not opted into bounded paging.</exception>
    /// <exception cref="SearchableStorageQueryConfigurationException">The built-in provider has no current continuation-protection key.</exception>
    /// <exception cref="SearchableStorageInvalidContinuationTokenException">The continuation is malformed, unauthenticated, or does not match this query.</exception>
    /// <exception cref="SearchableStorageStaleContinuationTokenException">The continuation names an obsolete routing layout.</exception>
    /// <exception cref="SearchableStorageQueryLimitExceededException">The next bounded candidate cannot make progress within the configured partition limits.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static Task<SearchableStorageQueryPage> ToGrainIdPageAsync<TState>(
        this IQueryable<TState> source,
        SearchableStorageQueryPageRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(request);
        if (source.Provider is not ISearchableStoragePagedQueryProvider provider)
        {
            throw new NotSupportedException(
                "ToGrainIdPageAsync requires a query created by a paging-enabled "
                + "ISearchableStorageQueryClient or an IQueryable provider which implements "
                + "ISearchableStoragePagedQueryProvider.");
        }

        return provider.ExecuteToGrainIdPageAsync(source.Expression, request, cancellationToken);
    }

    /// <summary>
    /// Executes a supported indexed predicate and returns matching grain identifiers.
    /// </summary>
    /// <typeparam name="TState">The persisted state type being queried.</typeparam>
    /// <param name="source">A query created by <see cref="ISearchableStorageQueryClient.Query{TState}(string)"/>.</param>
    /// <param name="cancellationToken">A token which cancels waiting for the distributed query.</param>
    /// <returns>A sorted, distinct list of matching grain identifiers.</returns>
    /// <remarks>
    /// Supported predicates use <c>==</c>, <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>, or
    /// <c>&gt;=</c> on indexed properties and combine them with <c>&amp;&amp;</c> or <c>||</c>.
    /// The built-in provider collects bounded pages and returns the complete result or throws
    /// <see cref="SearchableStorageQueryLimitExceededException"/> without a partial list. Use
    /// <see cref="ToGrainIdPageAsync{TState}(IQueryable{TState}, SearchableStorageQueryPageRequest, CancellationToken)"/>
    /// when a result can exceed the compatibility terminal's aggregate ceilings. External query
    /// providers own and must document their execution and bounding semantics.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    /// <exception cref="NotSupportedException">The query contains an unsupported expression or provider.</exception>
    /// <exception cref="SearchableStorageQueryLimitExceededException">The built-in provider cannot complete the result within its aggregate ceilings.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static Task<IReadOnlyList<GrainId>> ToGrainIdsAsync<TState>(
        this IQueryable<TState> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (source.Provider is not ISearchableStorageAsyncQueryProvider provider)
        {
            throw new NotSupportedException(
                "ToGrainIdsAsync requires a query created by ISearchableStorageQueryClient.Query " +
                "or an IQueryable provider which implements ISearchableStorageAsyncQueryProvider.");
        }

        return provider.ExecuteToGrainIdsAsync(source.Expression, cancellationToken);
    }

    private static ISearchableStorageFacetQueryProvider GetFacetProvider(IQueryProvider provider)
    {
        return provider as ISearchableStorageFacetQueryProvider
            ?? throw new NotSupportedException(
                "Facet terminals require a query created by a facet-enabled "
                + "ISearchableStorageQueryClient or an IQueryable provider which implements "
                + "ISearchableStorageFacetQueryProvider.");
    }
}
