using System.Linq.Expressions;

namespace Orleans.SearchableStorage;

/// <summary>
/// Opts an <see cref="IQueryable"/> provider into bounded searchable-storage paging.
/// </summary>
/// <remarks>
/// This contract is separate from <see cref="ISearchableStorageAsyncQueryProvider"/> so existing
/// external providers remain binary and source compatible. External implementations own their
/// execution protocol and must provide equivalent bounds if they implement this interface.
/// </remarks>
public interface ISearchableStoragePagedQueryProvider
{
    /// <summary>
    /// Executes one bounded page for <paramref name="expression"/>.
    /// </summary>
    /// <param name="expression">The complete query expression to execute.</param>
    /// <param name="request">The requested page size and optional continuation.</param>
    /// <param name="cancellationToken">A token which cancels waiting for execution.</param>
    /// <returns>A task containing one page and its optional continuation.</returns>
    Task<SearchableStorageQueryPage> ExecuteToGrainIdPageAsync(
        Expression expression,
        SearchableStorageQueryPageRequest request,
        CancellationToken cancellationToken);
}
