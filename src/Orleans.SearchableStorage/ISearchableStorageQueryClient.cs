namespace Orleans.SearchableStorage;

/// <summary>
/// Provides the opt-in LINQ expression surface in addition to direct index operations.
/// </summary>
public interface ISearchableStorageQueryClient : ISearchableStorageClient
{
    /// <summary>
    /// Creates a deferred query over indexed properties of one persisted state type.
    /// </summary>
    /// <typeparam name="TState">The persisted state type which declares the indexes.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <returns>A query root which can be filtered and executed with <see cref="SearchableStorageQueryableExtensions.ToGrainIdsAsync{TState}(IQueryable{TState}, CancellationToken)"/>.</returns>
    /// <remarks>
    /// The query supports indexed comparisons combined with boolean AND and OR. It does not support
    /// synchronous enumeration or the general LINQ operator set.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="stateName"/> is empty.</exception>
    IQueryable<TState> Query<TState>(string stateName);
}
