using System.Collections;
using System.Linq.Expressions;
using Orleans.Runtime;

namespace Orleans.SearchableStorage.Querying;

internal sealed class SearchableStorageQueryProvider<TState>(
    SearchableStorageClient client,
    string stateName) :
    IQueryProvider,
    ISearchableStorageAsyncQueryProvider,
    ISearchableStoragePagedQueryProvider,
    ISearchableStorageFacetQueryProvider
{
    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        if (!typeof(IQueryable<TState>).IsAssignableFrom(expression.Type))
        {
            throw UnsupportedElementType(expression.Type);
        }

        return new SearchableStorageQuery<TState>(this, expression);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        if (typeof(TElement) != typeof(TState))
        {
            throw UnsupportedElementType(typeof(TElement));
        }

        return (IQueryable<TElement>)(object)new SearchableStorageQuery<TState>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        throw SynchronousExecutionNotSupported();
    }

    public TResult Execute<TResult>(Expression expression)
    {
        throw SynchronousExecutionNotSupported();
    }

    public Task<IReadOnlyList<GrainId>> ExecuteToGrainIdsAsync(
        Expression expression,
        CancellationToken cancellationToken)
    {
        return client.ExecuteQueryAsync<TState>(stateName, expression, cancellationToken);
    }

    public Task<SearchableStorageQueryPage> ExecuteToGrainIdPageAsync(
        Expression expression,
        SearchableStorageQueryPageRequest request,
        CancellationToken cancellationToken)
    {
        return client.ExecuteQueryPageAsync<TState>(
            stateName,
            expression,
            request,
            cancellationToken);
    }

    public Task<SearchableStorageDistinctFacetPage<TValue>> ExecuteDistinctFacetValuePageAsync<TValue>(
        Expression queryExpression,
        LambdaExpression propertySelector,
        SearchableStorageFacetPageRequest request,
        CancellationToken cancellationToken)
    {
        return client.ExecuteDistinctFacetPageAsync<TState, TValue>(
            stateName,
            queryExpression,
            propertySelector,
            request,
            cancellationToken);
    }

    public Task<SearchableStorageFacetResult<TValue>> ExecuteFacetValueCountsAsync<TValue>(
        Expression queryExpression,
        LambdaExpression propertySelector,
        SearchableStorageFacetRequest request,
        CancellationToken cancellationToken)
    {
        return client.ExecuteFacetValueCountsAsync<TState, TValue>(
            stateName,
            queryExpression,
            propertySelector,
            request,
            cancellationToken);
    }

    public Task<SearchableStorageFacetMinMax<TValue>?> ExecuteFacetMinMaxAsync<TValue>(
        Expression queryExpression,
        LambdaExpression propertySelector,
        CancellationToken cancellationToken)
    {
        return client.ExecuteFacetMinMaxAsync<TState, TValue>(
            stateName,
            queryExpression,
            propertySelector,
            cancellationToken);
    }

    private static NotSupportedException UnsupportedElementType(Type elementType)
    {
        return new NotSupportedException(
            $"LINQ projections and element type '{elementType}' are not supported. " +
            "Searchable storage queries return GrainId values through ToGrainIdPageAsync "
            + "or ToGrainIdsAsync.");
    }

    private static NotSupportedException SynchronousExecutionNotSupported()
    {
        return new NotSupportedException(
            "Synchronous query execution is not supported. Use an asynchronous id or facet terminal.");
    }
}

internal sealed class SearchableStorageQuery<TState> : IOrderedQueryable<TState>
{
    public SearchableStorageQuery(IQueryProvider provider)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = Expression.Constant(this);
    }

    public SearchableStorageQuery(IQueryProvider provider, Expression expression)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
    }

    public Type ElementType => typeof(TState);

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<TState> GetEnumerator()
    {
        throw new NotSupportedException(
            "Synchronous query enumeration is not supported. Use ToGrainIdPageAsync or "
            + "ToGrainIdsAsync.");
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
