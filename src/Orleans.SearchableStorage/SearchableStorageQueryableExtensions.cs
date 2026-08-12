using System.Linq.Expressions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage;

/// <summary>
/// Executes deferred searchable-storage queries.
/// </summary>
public static class SearchableStorageQueryableExtensions
{
    /// <summary>
    /// Filters a searchable-storage query to records whose scalar indexed property equals one of
    /// a bounded set of values.
    /// </summary>
    /// <typeparam name="TState">The persisted state type being queried.</typeparam>
    /// <typeparam name="TValue">The exact scalar property and value type.</typeparam>
    /// <param name="source">A searchable-storage query.</param>
    /// <param name="propertySelector">A direct scalar Hash or Range indexed property.</param>
    /// <param name="values">At most 64 non-null values. The values are snapshotted immediately.</param>
    /// <returns>A deferred query lowered to existing exact-match and OR query nodes.</returns>
    /// <remarks>
    /// Duplicate values are removed using the index's canonical equality and the remaining values
    /// are ordered canonically, so input order does not affect the query fingerprint. This is a
    /// bounded convenience operator, not general LINQ <c>Contains</c> translation.
    /// </remarks>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    /// <exception cref="ArgumentException">
    /// The selector is not one directly typed scalar index, or <paramref name="values"/> contains null.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="values"/> contains more than 64 items.</exception>
    public static IQueryable<TState> WhereIn<TState, TValue>(
        this IQueryable<TState> source,
        Expression<Func<TState, TValue>> propertySelector,
        IReadOnlyList<TValue> values)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(propertySelector);
        ArgumentNullException.ThrowIfNull(values);

        if (propertySelector.Parameters.Count != 1
            || propertySelector.Body is not MemberExpression
            {
                Member: System.Reflection.PropertyInfo,
                Expression: ParameterExpression parameter,
            }
            || parameter != propertySelector.Parameters[0])
        {
            throw new ArgumentException(
                "WhereIn requires a directly typed scalar indexed property selector.",
                nameof(propertySelector));
        }

        var selected = IndexMetadataProvider.GetSelectedIndex(
            "where-in-selector-validation",
            propertySelector);
        if (selected.Converter.ValueType != typeof(TValue))
        {
            throw new ArgumentException(
                $"WhereIn selector type '{typeof(TValue)}' must exactly match scalar indexed property type "
                + $"'{selected.Converter.ValueType}'; conversions and boxing are not supported.",
                nameof(propertySelector));
        }

        if (values.Count > SearchableStorageCapacityLimits.MaximumWhereInValues)
        {
            throw new ArgumentOutOfRangeException(
                nameof(values),
                values.Count,
                $"WhereIn accepts at most {SearchableStorageCapacityLimits.MaximumWhereInValues} raw values.");
        }

        var canonical = new SortedDictionary<IndexValue, TValue>();
        for (var index = 0; index < values.Count; index++)
        {
            var value = values[index];
            if (value is null)
            {
                throw new ArgumentException("WhereIn values cannot contain null.", nameof(values));
            }

            var converted = selected.Converter.ConvertObject(value)
                ?? throw new InvalidOperationException("A non-null WhereIn value unexpectedly converted to null.");
            canonical.TryAdd(converted, value);
        }

        var predicateBody = BuildWhereInPredicate(propertySelector.Body, canonical.Values);
        var predicate = Expression.Lambda<Func<TState, bool>>(
            predicateBody,
            propertySelector.Parameters);
        return Queryable.Where(source, predicate);
    }

    private static Expression BuildWhereInPredicate<TValue>(
        Expression property,
        IEnumerable<TValue> values)
    {
        var current = values
            .Select(value => Expression.Equal(property, CreateTypedConstant(value)))
            .Cast<Expression>()
            .ToList();
        if (current.Count == 0)
        {
            return Expression.Constant(false);
        }

        while (current.Count > 1)
        {
            var next = new List<Expression>((current.Count + 1) / 2);
            for (var index = 0; index < current.Count; index += 2)
            {
                next.Add(index + 1 < current.Count
                    ? Expression.OrElse(current[index], current[index + 1])
                    : current[index]);
            }

            current = next;
        }

        return current[0];
    }

    private static Expression CreateTypedConstant<TValue>(TValue value)
    {
        var targetType = typeof(TValue);
        var runtimeConstant = Expression.Constant(value);
        return runtimeConstant.Type == targetType
            ? runtimeConstant
            : Expression.Convert(runtimeConstant, targetType);
    }

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
