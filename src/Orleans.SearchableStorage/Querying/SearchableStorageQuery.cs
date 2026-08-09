using System.Collections;
using System.Linq.Expressions;
using Orleans.Runtime;

namespace Orleans.SearchableStorage.Querying;

internal interface ISearchableStorageQueryProvider
{
    Task<IReadOnlyList<GrainId>> ExecuteAsync(
        Expression expression,
        CancellationToken cancellationToken);
}

internal sealed class SearchableStorageQueryProvider<TState>(
    SearchableStorageClient client,
    string stateName) : IQueryProvider, ISearchableStorageQueryProvider
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

    public Task<IReadOnlyList<GrainId>> ExecuteAsync(
        Expression expression,
        CancellationToken cancellationToken)
    {
        return client.ExecuteQueryAsync<TState>(stateName, expression, cancellationToken);
    }

    private static NotSupportedException UnsupportedElementType(Type elementType)
    {
        return new NotSupportedException(
            $"LINQ projections and element type '{elementType}' are not supported. " +
            "Searchable storage queries return GrainId values through ToGrainIdsAsync.");
    }

    private static NotSupportedException SynchronousExecutionNotSupported()
    {
        return new NotSupportedException(
            "Synchronous query execution is not supported. Use ToGrainIdsAsync.");
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
            "Synchronous query enumeration is not supported. Use ToGrainIdsAsync.");
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
