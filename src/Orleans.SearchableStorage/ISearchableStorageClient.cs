using System.Linq.Expressions;
using Orleans.Runtime;

namespace Orleans.SearchableStorage;

/// <summary>
/// Queries secondary indexes maintained by a searchable grain-storage provider.
/// </summary>
public interface ISearchableStorageClient
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

    /// <summary>
    /// Finds states whose indexed property equals <paramref name="value"/>.
    /// </summary>
    /// <typeparam name="TState">The persisted state type which declares the index.</typeparam>
    /// <typeparam name="TValue">The indexed property type.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="propertySelector">An expression selecting one indexed state property.</param>
    /// <param name="value">The exact value to find. Null values are not indexed.</param>
    /// <param name="cancellationToken">A token which cancels waiting for the distributed query.</param>
    /// <returns>A sorted, distinct list of matching grain identifiers.</returns>
    /// <remarks>The result is assembled from consistent partition-local reads, not from a cross-partition snapshot.</remarks>
    /// <exception cref="ArgumentException">The selector or value does not match a declared index.</exception>
    /// <exception cref="InvalidOperationException">The configured client layout differs from the persisted layout.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    Task<IReadOnlyList<GrainId>> FindAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue value,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Finds states whose range-indexed property lies between two bounds.
    /// </summary>
    /// <typeparam name="TState">The persisted state type which declares the index.</typeparam>
    /// <typeparam name="TValue">The indexed property type.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="propertySelector">An expression selecting one range-indexed state property.</param>
    /// <param name="lowerBound">The lower range bound.</param>
    /// <param name="upperBound">The upper range bound.</param>
    /// <param name="includeLowerBound">Whether values equal to <paramref name="lowerBound"/> are included.</param>
    /// <param name="includeUpperBound">Whether values equal to <paramref name="upperBound"/> are included.</param>
    /// <param name="cancellationToken">A token which cancels waiting for the distributed query.</param>
    /// <returns>A sorted, distinct list of matching grain identifiers.</returns>
    /// <remarks>The result is assembled from consistent partition-local reads, not from a cross-partition snapshot.</remarks>
    /// <exception cref="ArgumentException">The selector, value type, or range bounds are invalid.</exception>
    /// <exception cref="InvalidOperationException">The configured client layout differs from the persisted layout.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled.</exception>
    Task<IReadOnlyList<GrainId>> RangeAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue lowerBound,
        TValue upperBound,
        bool includeLowerBound = true,
        bool includeUpperBound = true,
        CancellationToken cancellationToken = default);
}
