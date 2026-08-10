using Orleans.Runtime;

namespace Orleans.SearchableStorage;

/// <summary>
/// Executes deferred searchable-storage queries.
/// </summary>
public static class SearchableStorageQueryableExtensions
{
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
}
