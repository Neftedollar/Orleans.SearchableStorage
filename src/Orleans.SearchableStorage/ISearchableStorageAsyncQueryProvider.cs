using System.Linq.Expressions;
using Orleans.Runtime;

namespace Orleans.SearchableStorage;

/// <summary>
/// Executes searchable-storage expression trees through the public async terminal operation.
/// </summary>
/// <remarks>
/// An external query implementation can expose an <see cref="IQueryable"/> whose
/// <see cref="IQueryable.Provider"/> implements this contract.
/// </remarks>
public interface ISearchableStorageAsyncQueryProvider
{
    /// <summary>
    /// Executes <paramref name="expression"/> and returns matching grain identifiers.
    /// </summary>
    /// <param name="expression">The complete query expression to execute.</param>
    /// <param name="cancellationToken">A token which cancels waiting for execution.</param>
    /// <returns>A task containing matching grain identifiers in sorted, duplicate-free order.</returns>
    /// <remarks>Implementations must return a sorted, distinct result.</remarks>
    Task<IReadOnlyList<GrainId>> ExecuteToGrainIdsAsync(
        Expression expression,
        CancellationToken cancellationToken);
}
