using System.Linq.Expressions;
using Orleans.Runtime;

namespace Orleans.SearchableStorage;

/// <summary>
/// Executes searchable-storage expression trees through the public async terminal operation.
/// </summary>
/// <remarks>
/// An external query implementation can expose an <see cref="IQueryable"/> whose
/// <see cref="IQueryable.Provider"/> implements this contract. Implementing this interface does
/// not opt an external provider into the built-in bounded distributed protocol. An external
/// provider must document whether it enforces equivalent hard work, item, byte, and round ceilings;
/// callers must not assume the built-in bounds apply when it does not.
/// </remarks>
public interface ISearchableStorageAsyncQueryProvider
{
    /// <summary>
    /// Executes <paramref name="expression"/> and returns matching grain identifiers.
    /// </summary>
    /// <param name="expression">The complete query expression to execute.</param>
    /// <param name="cancellationToken">A token which cancels waiting for execution.</param>
    /// <returns>A task containing matching grain identifiers in sorted, duplicate-free order.</returns>
    /// <remarks>
    /// Implementations must return a sorted, distinct result. The built-in provider executes
    /// through bounded pages and returns all results or throws
    /// <see cref="SearchableStorageQueryLimitExceededException"/> without returning a partial
    /// list. External providers own and must document their execution and bounding semantics.
    /// </remarks>
    Task<IReadOnlyList<GrainId>> ExecuteToGrainIdsAsync(
        Expression expression,
        CancellationToken cancellationToken);
}
