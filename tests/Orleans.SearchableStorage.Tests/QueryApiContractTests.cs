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

    [Fact]
    public void VirtualRoutingWireAndLayoutMessagesKeepStableFieldIds()
    {
        typeof(StorageLayoutState).GetProperty(
                "PartitionCount",
                BindingFlags.Instance | BindingFlags.Public)
            .Should().NotBeNull("the JSON property name is part of persisted version-3 state");

        AssertFieldIds<RoutedStorageReadRequest>(
            (nameof(RoutedStorageReadRequest.RecordKey), 0),
            (nameof(RoutedStorageReadRequest.Slot), 1),
            (nameof(RoutedStorageReadRequest.Epoch), 2),
            (nameof(RoutedStorageReadRequest.GrainId), 3));
        AssertFieldIds<RoutedStorageWriteRequest>(
            (nameof(RoutedStorageWriteRequest.Request), 0),
            (nameof(RoutedStorageWriteRequest.Slot), 1),
            (nameof(RoutedStorageWriteRequest.Epoch), 2));
        AssertFieldIds<RoutedStorageClearRequest>(
            (nameof(RoutedStorageClearRequest.Request), 0),
            (nameof(RoutedStorageClearRequest.Slot), 1),
            (nameof(RoutedStorageClearRequest.Epoch), 2),
            (nameof(RoutedStorageClearRequest.GrainId), 3));
        AssertFieldIds<RoutedExactIndexQuery>(
            (nameof(RoutedExactIndexQuery.Query), 0),
            (nameof(RoutedExactIndexQuery.Epoch), 1));
        AssertFieldIds<RoutedRangeIndexQuery>(
            (nameof(RoutedRangeIndexQuery.Query), 0),
            (nameof(RoutedRangeIndexQuery.Epoch), 1));
        AssertFieldIds<RoutedPartitionQuery>(
            (nameof(RoutedPartitionQuery.Query), 0),
            (nameof(RoutedPartitionQuery.Epoch), 1));
        AssertFieldIds<StorageRouteMismatchException>(
            (nameof(StorageRouteMismatchException.ExpectedEpoch), 0),
            (nameof(StorageRouteMismatchException.CurrentEpoch), 1),
            (nameof(StorageRouteMismatchException.RequestedPartition), 2),
            (nameof(StorageRouteMismatchException.Slot), 3),
            (nameof(StorageRouteMismatchException.CurrentOwner), 4));

        AssertFieldIds<StorageLayoutDescriptor>(
            (nameof(StorageLayoutDescriptor.FormatVersion), 0),
            (nameof(StorageLayoutDescriptor.ProviderName), 1),
            (nameof(StorageLayoutDescriptor.PartitionCount), 2),
            (nameof(StorageLayoutDescriptor.JournalSegmentCapacity), 3),
            (nameof(StorageLayoutDescriptor.MaximumJournalReplayEntries), 4),
            (nameof(StorageLayoutDescriptor.VirtualSlotTargetCount), 5));
        AssertFieldIds<StorageLayoutIdentity>(
            (nameof(StorageLayoutIdentity.FormatVersion), 0),
            (nameof(StorageLayoutIdentity.ProviderName), 1),
            (nameof(StorageLayoutIdentity.PartitionCount), 2));
        AssertFieldIds<StorageLayoutSnapshot>(
            (nameof(StorageLayoutSnapshot.FormatVersion), 0),
            (nameof(StorageLayoutSnapshot.ProviderName), 1),
            (nameof(StorageLayoutSnapshot.InitialPartitionCount), 2),
            (nameof(StorageLayoutSnapshot.VirtualSlotCount), 3),
            (nameof(StorageLayoutSnapshot.Epoch), 4),
            ("SlotAssignments", 5));
        AssertFieldIds<StorageLayoutState>(
            (nameof(StorageLayoutState.Initialized), 0),
            (nameof(StorageLayoutState.FormatVersion), 1),
            (nameof(StorageLayoutState.ProviderName), 2),
            (nameof(StorageLayoutState.PartitionCount), 3),
            (nameof(StorageLayoutState.JournalSegmentCapacity), 4),
            (nameof(StorageLayoutState.MaximumJournalReplayEntries), 5),
            (nameof(StorageLayoutState.VirtualSlotCount), 6),
            (nameof(StorageLayoutState.SlotAssignments), 7),
            (nameof(StorageLayoutState.Epoch), 8));

    }

    private static void AssertFieldIds<T>(params (string MemberName, uint Id)[] expected)
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
        foreach (var (memberName, id) in expected)
        {
            typeof(T).GetProperty(memberName, flags)!
                .GetCustomAttribute<IdAttribute>()!.Id.Should().Be(id);
        }
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
