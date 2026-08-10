using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryApiContractTests
{
    [Fact]
    public void ExistingDirectClientImplementationsDoNotNeedTheQuerySurface()
    {
        ISearchableStorageClient client = new DirectClientImplementation();

        client.Should().NotBeAssignableTo<ISearchableStorageQueryClient>();
    }

    [Fact]
    public async Task ExternalAsyncProviderCanUseThePublicTerminalOperation()
    {
        var provider = new ExternalQueryProvider<QueryState>();
        var query = new ExternalQuery<QueryState>(provider)
            .Where(state => state.Value == 7);
        using var cancellation = new CancellationTokenSource();

        var results = await query.ToGrainIdsAsync(cancellation.Token);

        results.Should().BeEmpty();
        provider.Expression.Should().BeSameAs(query.Expression);
        provider.CancellationToken.Should().Be(cancellation.Token);
    }

    [Fact]
    public void BoundedRangeWireMessageKeepsRequiredNonNullableBounds()
    {
        var nullability = new NullabilityInfoContext();
        var scope = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.Scope))!;
        var lowerBound = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.LowerBound))!;
        var upperBound = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.UpperBound))!;
        var includeLower = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.IncludeLowerBound))!;
        var includeUpper = typeof(RangeIndexQuery).GetProperty(nameof(RangeIndexQuery.IncludeUpperBound))!;

        scope.GetCustomAttribute<RequiredMemberAttribute>().Should().NotBeNull();
        lowerBound.GetCustomAttribute<RequiredMemberAttribute>().Should().NotBeNull();
        upperBound.GetCustomAttribute<RequiredMemberAttribute>().Should().NotBeNull();
        nullability.Create(scope).ReadState.Should().Be(NullabilityState.NotNull);
        nullability.Create(lowerBound).ReadState.Should().Be(NullabilityState.NotNull);
        nullability.Create(upperBound).ReadState.Should().Be(NullabilityState.NotNull);
        scope.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(0);
        lowerBound.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(1);
        upperBound.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(2);
        includeLower.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(3);
        includeUpper.GetCustomAttribute<IdAttribute>()!.Id.Should().Be(4);
    }

    [Fact]
    public void PartitionQueryWireMessageKeepsStableFieldsAndNullableOpenBounds()
    {
        var expectedFields = new Dictionary<string, uint>
        {
            [nameof(PartitionQueryPlan.Operation)] = 0,
            [nameof(PartitionQueryPlan.Scope)] = 1,
            [nameof(PartitionQueryPlan.IndexKind)] = 2,
            [nameof(PartitionQueryPlan.Value)] = 3,
            [nameof(PartitionQueryPlan.LowerBound)] = 4,
            [nameof(PartitionQueryPlan.UpperBound)] = 5,
            [nameof(PartitionQueryPlan.IncludeLowerBound)] = 6,
            [nameof(PartitionQueryPlan.IncludeUpperBound)] = 7,
            [nameof(PartitionQueryPlan.Left)] = 8,
            [nameof(PartitionQueryPlan.Right)] = 9,
        };

        foreach (var field in expectedFields)
        {
            typeof(PartitionQueryPlan).GetProperty(field.Key)!
                .GetCustomAttribute<IdAttribute>()!.Id.Should().Be(field.Value);
        }

        var nullability = new NullabilityInfoContext();
        var lowerBound = typeof(PartitionQueryPlan).GetProperty(nameof(PartitionQueryPlan.LowerBound))!;
        var upperBound = typeof(PartitionQueryPlan).GetProperty(nameof(PartitionQueryPlan.UpperBound))!;
        nullability.Create(lowerBound).ReadState.Should().Be(NullabilityState.Nullable);
        nullability.Create(upperBound).ReadState.Should().Be(NullabilityState.Nullable);
        ((int)PartitionQueryOperation.Empty).Should().Be(0);
        ((int)PartitionQueryOperation.Exact).Should().Be(1);
        ((int)PartitionQueryOperation.Range).Should().Be(2);
        ((int)PartitionQueryOperation.And).Should().Be(3);
        ((int)PartitionQueryOperation.Or).Should().Be(4);
    }

    private sealed class QueryState
    {
        public int Value { get; init; }
    }

    private sealed class DirectClientImplementation : ISearchableStorageClient
    {
        public Task<IReadOnlyList<GrainId>> FindAsync<TState, TValue>(
            string stateName,
            Expression<Func<TState, TValue>> propertySelector,
            TValue value,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IReadOnlyList<GrainId>>([]);
        }

        public Task<IReadOnlyList<GrainId>> RangeAsync<TState, TValue>(
            string stateName,
            Expression<Func<TState, TValue>> propertySelector,
            TValue lowerBound,
            TValue upperBound,
            bool includeLowerBound = true,
            bool includeUpperBound = true,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IReadOnlyList<GrainId>>([]);
        }
    }

    private sealed class ExternalQueryProvider<TState> : IQueryProvider, ISearchableStorageAsyncQueryProvider
    {
        public Expression? Expression { get; private set; }

        public CancellationToken CancellationToken { get; private set; }

        public IQueryable CreateQuery(Expression expression)
        {
            return new ExternalQuery<TState>(this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (typeof(TElement) != typeof(TState))
            {
                throw new NotSupportedException();
            }

            return (IQueryable<TElement>)(object)new ExternalQuery<TState>(this, expression);
        }

        public object? Execute(Expression expression)
        {
            throw new NotSupportedException();
        }

        public TResult Execute<TResult>(Expression expression)
        {
            throw new NotSupportedException();
        }

        public Task<IReadOnlyList<GrainId>> ExecuteToGrainIdsAsync(
            Expression expression,
            CancellationToken cancellationToken)
        {
            Expression = expression;
            CancellationToken = cancellationToken;
            return Task.FromResult<IReadOnlyList<GrainId>>([]);
        }
    }

    private sealed class ExternalQuery<TState> : IOrderedQueryable<TState>
    {
        public ExternalQuery(IQueryProvider provider)
        {
            Provider = provider;
            Expression = System.Linq.Expressions.Expression.Constant(this);
        }

        public ExternalQuery(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public Type ElementType => typeof(TState);

        public Expression Expression { get; }

        public IQueryProvider Provider { get; }

        public IEnumerator<TState> GetEnumerator()
        {
            throw new NotSupportedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
