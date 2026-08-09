using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage;

/// <summary>
/// Executes deferred searchable-storage queries.
/// </summary>
public static class SearchableStorageQueryableExtensions
{
    /// <summary>
    /// Executes a supported indexed predicate and returns matching grain identifiers.
    /// </summary>
    /// <typeparam name="TState">The persisted state type being queried.</typeparam>
    /// <param name="source">A query created by <see cref="ISearchableStorageClient.Query{TState}(string)"/>.</param>
    /// <param name="cancellationToken">A token which cancels waiting for the distributed query.</param>
    /// <returns>A sorted, distinct list of matching grain identifiers.</returns>
    /// <remarks>
    /// Supported predicates use <c>==</c>, <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>, or
    /// <c>&gt;=</c> on indexed properties and combine them with <c>&amp;&amp;</c> or <c>||</c>.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    /// <exception cref="NotSupportedException">The query contains an unsupported expression or provider.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    public static Task<IReadOnlyList<GrainId>> ToGrainIdsAsync<TState>(
        this IQueryable<TState> source,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (source.Provider is not ISearchableStorageQueryProvider provider)
        {
            throw new NotSupportedException(
                "ToGrainIdsAsync can only execute queries created by ISearchableStorageClient.Query.");
        }

        return provider.ExecuteAsync(source.Expression, cancellationToken);
    }
}
