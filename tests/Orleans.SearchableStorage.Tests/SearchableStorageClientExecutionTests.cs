using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageClientExecutionTests
{
    [Fact]
    public async Task CompoundPlanUsesOneAtomicRequestPerPartition()
    {
        var firstId = GrainId.Create("vacancy", "first");
        var secondId = GrainId.Create("vacancy", "second");
        var oldVersion = GrainId.Create("vacancy", "old-version");
        var newVersion = GrainId.Create("vacancy", "new-version");
        // The legacy leaf seams expose mutually exclusive versions. A client-side combination
        // would produce an anomaly, while one complete partition request returns one version.
        var first = new ControlledPartition(
            _ => Task.FromResult(new[] { secondId }),
            _ => Task.FromResult(new[] { oldVersion }),
            _ => Task.FromResult(new[] { newVersion }));
        var second = new ControlledPartition(
            _ => Task.FromResult(new[] { firstId }),
            _ => Task.FromResult(new[] { newVersion }),
            _ => Task.FromResult(new[] { oldVersion }));
        var client = CreateClient(first, second);
        var lowerBound = 5;
        var upperBound = 8;

        var matches = await client
            .Query<QueryState>("state")
            .Where(state => (state.City == "Helsinki" || state.City == "Tampere")
                && state.Salary > lowerBound
                && state.Salary < upperBound)
            .ToGrainIdsAsync();

        matches.Should().Equal(firstId, secondId);
        first.QueryCallCount.Should().Be(1);
        second.QueryCallCount.Should().Be(1);
        first.FindCallCount.Should().Be(0);
        second.FindCallCount.Should().Be(0);
        first.RangeCallCount.Should().Be(0);
        second.RangeCallCount.Should().Be(0);
        first.Plans.Should().ContainSingle()
            .Which.Operation.Should().Be(PartitionQueryOperation.And);
        second.Plans.Should().ContainSingle()
            .Which.Operation.Should().Be(PartitionQueryOperation.And);
    }

    [Fact]
    public async Task RangeApiUsesTheSharedBoundedPageMessage()
    {
        var match = GrainId.Create("vacancy", "match");
        PartitionQueryPlan? sentQuery = null;
        var partition = new ControlledPartition(query =>
            {
                sentQuery = query;
                return Task.FromResult(new[] { match });
            });

        var matches = await CreateClient(partition).RangeAsync<QueryState, int>(
            "state",
            state => state.Salary,
            5,
            8,
            includeLowerBound: false,
            includeUpperBound: true);

        matches.Should().ContainSingle().Which.Should().Be(match);
        partition.RangeCallCount.Should().Be(0);
        partition.QueryCallCount.Should().Be(1);
        partition.UnboundedQueryCallCount.Should().Be(0);
        sentQuery.Should().NotBeNull();
        sentQuery!.LowerBound!.SignedInteger.Should().Be(5);
        sentQuery.UpperBound!.SignedInteger.Should().Be(8);
        sentQuery.IncludeLowerBound.Should().BeFalse();
        sentQuery.IncludeUpperBound.Should().BeTrue();
    }

    [Fact]
    public async Task ExactFanoutReturnsSortedDistinctIds()
    {
        var firstId = GrainId.Create("vacancy", "a");
        var secondId = GrainId.Create("vacancy", "b");
        var first = new ControlledPartition(_ => Task.FromResult(new[] { secondId, firstId }));
        var second = new ControlledPartition(_ => Task.FromResult(new[] { secondId }));

        var matches = await CreateClient(first, second)
            .Query<QueryState>("state")
            .Where(state => state.City == "Helsinki")
            .ToGrainIdsAsync();

        matches.Should().Equal(firstId, secondId);
    }

    [Fact]
    public async Task CapturedValueIsEvaluatedForEveryExecutionOfADeferredQuery()
    {
        var firstId = GrainId.Create("vacancy", "first");
        var secondId = GrainId.Create("vacancy", "second");
        var partition = new ControlledPartition(plan => Task.FromResult(
            plan.Value!.Text == "Helsinki"
                ? new[] { firstId }
                : new[] { secondId }));
        var client = CreateClient(partition);
        var city = "Helsinki";
        var query = client
            .Query<QueryState>("state")
            .Where(state => state.City == city);

        var firstMatches = await query.ToGrainIdsAsync();
        city = "Tampere";
        var secondMatches = await query.ToGrainIdsAsync();

        firstMatches.Should().ContainSingle().Which.Should().Be(firstId);
        secondMatches.Should().ContainSingle().Which.Should().Be(secondId);
        partition.Plans.Select(static plan => plan.Value!.Text)
            .Should().Equal("Helsinki", "Tampere");
    }

    [Fact]
    public async Task EqualInclusiveBoundsAndNullableValuesReachTheWirePlan()
    {
        var partition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var client = CreateClient(partition);
        var bound = 7;
        int? optional = 9;

        await client
            .Query<QueryState>("state")
            .Where(state => state.Salary >= bound && state.Salary <= bound)
            .ToGrainIdsAsync();
        await client
            .Query<QueryState>("state")
            .Where(state => state.Optional == optional)
            .ToGrainIdsAsync();

        var range = partition.Plans[0];
        range.Operation.Should().Be(PartitionQueryOperation.Range);
        range.LowerBound!.SignedInteger.Should().Be(bound);
        range.UpperBound!.SignedInteger.Should().Be(bound);
        range.IncludeLowerBound.Should().BeTrue();
        range.IncludeUpperBound.Should().BeTrue();
        var exact = partition.Plans[1];
        exact.Operation.Should().Be(PartitionQueryOperation.Exact);
        exact.Value!.SignedInteger.Should().Be(optional.Value);
    }

    [Fact]
    public async Task FanoutDoesNotFailFastAndObservesImmediateAndLateFailures()
    {
        var immediateFailure = new InvalidOperationException("immediate partition failed");
        var lateFailure = new InvalidOperationException("late partition failed");
        var success = new ControlledPartition(_ => Task.FromResult(new[]
        {
            GrainId.Create("vacancy", "partial-result"),
        }));
        var failed = new ControlledPartition(_ => Task.FromException<GrainId[]>(immediateFailure));
        var blocked = ControlledPartition.CreateBlocking();
        var query = CreateClient(success, failed, blocked)
            .Query<QueryState>("state")
            .Where(state => state.City == "Helsinki");

        var execution = query.ToGrainIdsAsync();
        await blocked.Started.WaitAsync(TimeSpan.FromSeconds(5));

        execution.IsCompleted.Should().BeFalse(
            "fan-out must wait for every partition instead of returning an exception or partial result early");
        success.LastQueryTask!.IsCompletedSuccessfully.Should().BeTrue();
        failed.LastQueryTask!.IsFaulted.Should().BeTrue();

        blocked.Fail(lateFailure);
        Func<Task> waitForExecution = async () => await execution;

        await waitForExecution.Should().ThrowAsync<InvalidOperationException>();
        success.QueryCallCount.Should().Be(1);
        failed.QueryCallCount.Should().Be(1);
        blocked.QueryCallCount.Should().Be(1);
        failed.LastQueryTask.Exception!.InnerExceptions.Should().Contain(immediateFailure);
        blocked.LastQueryTask!.Exception!.InnerExceptions.Should().Contain(lateFailure);
    }

    [Fact]
    public async Task CancellationInterruptsAnInFlightPartitionFanout()
    {
        var first = ControlledPartition.CreateBlocking();
        var second = ControlledPartition.CreateBlocking();
        var query = CreateClient(first, second)
            .Query<QueryState>("state")
            .Where(state => state.City == "Helsinki");
        using var cancellation = new CancellationTokenSource();

        var execution = query.ToGrainIdsAsync(cancellation.Token);
        await Task.WhenAll(first.Started, second.Started).WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();

        Func<Task> waitForExecution = async () => await execution;
        await waitForExecution.Should().ThrowAsync<OperationCanceledException>();
        first.Fail(new InvalidOperationException("first canceled call later failed"));
        second.Fail(new InvalidOperationException("second canceled call later failed"));
        first.LastQueryTask!.IsFaulted.Should().BeTrue();
        second.LastQueryTask!.IsFaulted.Should().BeTrue();
    }

    [Fact]
    public async Task UnsupportedPublicOperatorsProduceFocusedDiagnostics()
    {
        var partition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var query = CreateClient(partition).Query<QueryState>("state");

        Action select = () => _ = query.Select(state => state.City);
        Action count = () => _ = query.Count();
        Action first = () => _ = query.First();
        Func<Task> orderBy = () => query
            .Where(state => state.Salary >= 5)
            .OrderBy(state => state.Salary)
            .ToGrainIdsAsync();

        select.Should().Throw<NotSupportedException>().WithMessage("*projections*");
        count.Should().Throw<NotSupportedException>().WithMessage("*Synchronous query execution*");
        first.Should().Throw<NotSupportedException>().WithMessage("*Synchronous query execution*");
        await orderBy.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*operator 'OrderBy' is not supported*");
    }

    [Fact]
    public async Task ContradictoryPlanValidatesLayoutButSkipsPartitionFanout()
    {
        var layoutCallCount = 0;
        var partition = new ControlledPartition(
            _ => Task.FromException<GrainId[]>(new InvalidOperationException("must not be called")));
        var client = CreateClient(
            () =>
            {
                layoutCallCount++;
                return Task.FromResult(true);
            },
            partition);

        var matches = await client
            .Query<QueryState>("state")
            .Where(state => state.Salary > 8 && state.Salary < 5)
            .ToGrainIdsAsync();

        matches.Should().BeEmpty();
        layoutCallCount.Should().Be(1);
        partition.QueryCallCount.Should().Be(0);
        partition.FindCallCount.Should().Be(0);
        partition.RangeCallCount.Should().Be(0);
    }

    [Fact]
    public async Task ContradictoryPlanDoesNotHideLayoutFailureOrPreCancellation()
    {
        var canceledLayoutCallCount = 0;
        var partition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var layoutFailure = new InvalidOperationException("layout failed");
        var failedQuery = CreateClient(
                () => Task.FromException<bool>(layoutFailure),
                partition)
            .Query<QueryState>("state")
            .Where(state => state.Salary > 8 && state.Salary < 5);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var canceledQuery = CreateClient(
                () =>
                {
                    canceledLayoutCallCount++;
                    return Task.FromResult(true);
                },
                partition)
            .Query<QueryState>("state")
            .Where(state => state.Salary > 8 && state.Salary < 5);

        Func<Task> fail = () => failedQuery.ToGrainIdsAsync();
        Func<Task> cancel = () => canceledQuery.ToGrainIdsAsync(cancellation.Token);

        (await fail.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(layoutFailure);
        await cancel.Should().ThrowAsync<OperationCanceledException>();
        canceledLayoutCallCount.Should().Be(0);
        partition.QueryCallCount.Should().Be(0);
    }

    [Fact]
    public async Task LayoutFailurePreventsEveryPartitionQueryApi()
    {
        var layoutFailure = new InvalidOperationException("legacy layout");
        var partition = new ControlledPartition(
            _ => Task.FromException<GrainId[]>(new InvalidOperationException("must not be called")));
        var client = CreateClient(
            () => Task.FromException<bool>(layoutFailure),
            partition);

        Func<Task> find = () => client.FindAsync<QueryState, string>(
            "state",
            state => state.City,
            "Helsinki");
        Func<Task> range = () => client.RangeAsync<QueryState, int>(
            "state",
            state => state.Salary,
            5,
            8);
        Func<Task> query = () => client
            .Query<QueryState>("state")
            .Where(state => state.City == "Helsinki")
            .ToGrainIdsAsync();

        (await find.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(layoutFailure);
        (await range.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(layoutFailure);
        (await query.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(layoutFailure);
        partition.FindCallCount.Should().Be(0);
        partition.RangeCallCount.Should().Be(0);
        partition.QueryCallCount.Should().Be(0);
    }

    [Fact]
    public async Task WhereChainAtTheDepthBoundaryIsAcceptedAndTheNextLevelIsRejected()
    {
        var acceptedPartition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var accepted = CreateClient(acceptedPartition).Query<QueryState>("state");
        for (var index = 0; index < QueryPlanLimits.MaximumDepth - 1; index++)
        {
            accepted = accepted.Where(state => state.City == "Helsinki");
        }

        var acceptedMatches = await accepted.ToGrainIdsAsync();

        var rejectedPartition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var layoutCallCount = 0;
        var rejected = CreateClient(
            () =>
            {
                layoutCallCount++;
                return Task.FromResult(true);
            },
            rejectedPartition).Query<QueryState>("state");
        for (var index = 0; index < QueryPlanLimits.MaximumDepth; index++)
        {
            rejected = rejected.Where(state => state.City == "Helsinki");
        }

        Func<Task> executeRejected = () => rejected.ToGrainIdsAsync();

        acceptedMatches.Should().BeEmpty();
        acceptedPartition.QueryCallCount.Should().Be(1);
        await executeRejected.Should().ThrowAsync<NotSupportedException>()
            .WithMessage($"*maximum supported depth of {QueryPlanLimits.MaximumDepth}*");
        layoutCallCount.Should().Be(0);
        rejectedPartition.QueryCallCount.Should().Be(0);
    }

    [Fact]
    public async Task BalancedPredicateAtTheNodeBoundaryIsAcceptedAndTheNextLeafIsRejected()
    {
        // One Where and its source consume two visits. Each OR predicate leaf contributes one
        // comparison, one closed value, and all but one leaf contributes a boolean node.
        var acceptedLeafCount = (QueryPlanLimits.MaximumNodeCount - 1) / 3;
        var rejectedLeafCount = acceptedLeafCount + 1;
        ((3 * acceptedLeafCount) + 1).Should().BeLessThanOrEqualTo(QueryPlanLimits.MaximumNodeCount);
        ((3 * rejectedLeafCount) + 1).Should().BeGreaterThan(QueryPlanLimits.MaximumNodeCount);
        var acceptedPartition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var acceptedQuery = CreateClient(acceptedPartition)
            .Query<QueryState>("state")
            .Where(CreateBalancedCityPredicate(acceptedLeafCount));
        var acceptedMatches = await acceptedQuery.ToGrainIdsAsync();

        var rejectedPartition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var layoutCallCount = 0;
        var rejectedClient = CreateClient(
            () =>
            {
                layoutCallCount++;
                return Task.FromResult(true);
            },
            rejectedPartition);
        var rejectedQuery = rejectedClient
            .Query<QueryState>("state")
            .Where(CreateBalancedCityPredicate(rejectedLeafCount));

        Func<Task> execute = () => rejectedQuery.ToGrainIdsAsync();

        acceptedMatches.Should().BeEmpty();
        acceptedPartition.QueryCallCount.Should().Be(1);
        await execute.Should().ThrowAsync<NotSupportedException>()
            .WithMessage($"*maximum supported node count of {QueryPlanLimits.MaximumNodeCount}*");
        layoutCallCount.Should().Be(0);
        rejectedPartition.QueryCallCount.Should().Be(0);
    }

    [Fact]
    public async Task LeftSkewedPredicateAtTheDepthBoundaryIsBalancedWithoutReorderingLeaves()
    {
        var leafCount = QueryPlanLimits.MaximumDepth;
        var expectedValues = Enumerable.Range(0, leafCount)
            .Select(static index => $"city-{index}")
            .ToArray();
        var acceptedPartition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var acceptedQuery = CreateClient(acceptedPartition)
            .Query<QueryState>("state")
            .Where(CreateLeftSkewedCityPredicate(leafCount));

        var acceptedMatches = await acceptedQuery.ToGrainIdsAsync();

        var rejectedPartition = new ControlledPartition(_ => Task.FromResult(Array.Empty<GrainId>()));
        var layoutCallCount = 0;
        var rejectedQuery = CreateClient(
                () =>
                {
                    layoutCallCount++;
                    return Task.FromResult(true);
                },
                rejectedPartition)
            .Query<QueryState>("state")
            .Where(CreateLeftSkewedCityPredicate(leafCount + 1));
        Func<Task> executeRejected = () => rejectedQuery.ToGrainIdsAsync();

        acceptedMatches.Should().BeEmpty();
        var wirePlan = acceptedPartition.Plans.Should().ContainSingle().Which;
        GetExactLeafValues(wirePlan).Should().Equal(expectedValues);
        var maximumBalancedDepth = 1 + (int)Math.Ceiling(Math.Log2(leafCount));
        GetPlanDepth(wirePlan).Should().BeLessThanOrEqualTo(maximumBalancedDepth);
        await executeRejected.Should().ThrowAsync<NotSupportedException>()
            .WithMessage($"*maximum supported depth of {QueryPlanLimits.MaximumDepth}*");
        layoutCallCount.Should().Be(0);
        rejectedPartition.QueryCallCount.Should().Be(0);
    }

    private static SearchableStorageClient CreateClient(params ControlledPartition[] partitions)
    {
        return CreateClient(static () => Task.FromResult(true), partitions);
    }

    private static SearchableStorageClient CreateClient(
        Func<Task<bool>> validateLayout,
        params ControlledPartition[] partitions)
    {
        return new SearchableStorageClient(
            "test",
            partitions,
            validateLayout);
    }

    private static Expression<Func<QueryState, bool>> CreateBalancedCityPredicate(int leafCount)
    {
        var parameter = Expression.Parameter(typeof(QueryState), "state");
        var city = Expression.Property(parameter, nameof(QueryState.City));
        var current = Enumerable.Range(0, leafCount)
            .Select(index => (Expression)Expression.Equal(city, Expression.Constant($"city-{index}")))
            .ToList();
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

        return Expression.Lambda<Func<QueryState, bool>>(current[0], parameter);
    }

    private static Expression<Func<QueryState, bool>> CreateLeftSkewedCityPredicate(int leafCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(leafCount);

        var parameter = Expression.Parameter(typeof(QueryState), "state");
        var city = Expression.Property(parameter, nameof(QueryState.City));
        Expression predicate = Expression.Equal(city, Expression.Constant("city-0"));
        for (var index = 1; index < leafCount; index++)
        {
            predicate = Expression.OrElse(
                predicate,
                Expression.Equal(city, Expression.Constant($"city-{index}")));
        }

        return Expression.Lambda<Func<QueryState, bool>>(predicate, parameter);
    }

    private static List<string> GetExactLeafValues(PartitionQueryPlan plan)
    {
        var values = new List<string>();
        var pending = new Stack<PartitionQueryPlan>();
        pending.Push(plan);
        while (pending.TryPop(out var current))
        {
            if (current.Operation == PartitionQueryOperation.Or)
            {
                pending.Push(current.Right!);
                pending.Push(current.Left!);
                continue;
            }

            if (current.Operation != PartitionQueryOperation.Exact || current.Value?.Text is not { } value)
            {
                throw new InvalidOperationException("Expected a wire plan containing only OR and exact string nodes.");
            }

            values.Add(value);
        }

        return values;
    }

    private static int GetPlanDepth(PartitionQueryPlan plan)
    {
        var maximumDepth = 0;
        var pending = new Stack<(PartitionQueryPlan Plan, int Depth)>();
        pending.Push((plan, 1));
        while (pending.TryPop(out var current))
        {
            maximumDepth = Math.Max(maximumDepth, current.Depth);
            if (current.Plan.Left is not null)
            {
                pending.Push((current.Plan.Left, current.Depth + 1));
            }

            if (current.Plan.Right is not null)
            {
                pending.Push((current.Plan.Right, current.Depth + 1));
            }
        }

        return maximumDepth;
    }

    private sealed class QueryState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Salary { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public int? Optional { get; init; }
    }

    private sealed class ControlledPartition : StoragePartitionGrainMovementTestDouble, IStoragePartitionGrain
    {
        private static readonly Func<ExactIndexQuery, Task<GrainId[]>> EmptyFind =
            static _ => Task.FromResult(Array.Empty<GrainId>());
        private static readonly Func<RangeIndexQuery, Task<GrainId[]>> EmptyRange =
            static _ => Task.FromResult(Array.Empty<GrainId>());

        private readonly Func<ExactIndexQuery, Task<GrainId[]>> _find;
        private readonly Func<PartitionQueryPlan, Task<GrainId[]>> _query;
        private readonly Func<RangeIndexQuery, Task<GrainId[]>> _range;
        private readonly TaskCompletionSource<GrainId[]>? _completion;
        private readonly TaskCompletionSource _started = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public ControlledPartition(
            Func<PartitionQueryPlan, Task<GrainId[]>> query,
            Func<ExactIndexQuery, Task<GrainId[]>>? find = null,
            Func<RangeIndexQuery, Task<GrainId[]>>? range = null)
        {
            _query = query;
            _find = find ?? EmptyFind;
            _range = range ?? EmptyRange;
        }

        private ControlledPartition(TaskCompletionSource<GrainId[]> completion)
        {
            _completion = completion;
            _query = _ => completion.Task;
            _find = EmptyFind;
            _range = EmptyRange;
        }

        public int FindCallCount { get; private set; }

        public int RangeCallCount { get; private set; }

        public int QueryCallCount { get; private set; }

        public int UnboundedQueryCallCount { get; private set; }

        public List<PartitionQueryPlan> Plans { get; } = [];

        public Task<GrainId[]>? LastQueryTask { get; private set; }

        public Task Started => _started.Task;

        public static ControlledPartition CreateBlocking()
        {
            return new ControlledPartition(new TaskCompletionSource<GrainId[]>(
                TaskCreationOptions.RunContinuationsAsynchronously));
        }

        public void Fail(Exception exception)
        {
            _completion!.TrySetException(exception).Should().BeTrue();
        }

        public Task<StorageReadResult> ReadAsync(string recordKey)
        {
            throw new NotSupportedException();
        }

        public Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request)
        {
            throw new NotSupportedException();
        }

        public Task<string> WriteAsync(StorageWriteRequest request)
        {
            throw new NotSupportedException();
        }

        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request)
        {
            throw new NotSupportedException();
        }

        public Task ClearAsync(StorageClearRequest request)
        {
            throw new NotSupportedException();
        }

        public Task ClearRoutedAsync(RoutedStorageClearRequest request)
        {
            throw new NotSupportedException();
        }

        public Task CompactAsync()
        {
            throw new NotSupportedException();
        }

        public Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync()
        {
            throw new NotSupportedException();
        }

        public Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
            RoutedPartitionDistinctFacetPageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
            RoutedPartitionFacetCandidatePageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
            RoutedPartitionFacetCountSliceRequest request) => throw new NotSupportedException();

        public Task<GrainId[]> FindAsync(ExactIndexQuery query)
        {
            FindCallCount++;
            return _find(query);
        }

        public Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query)
        {
            return FindAsync(query.Query);
        }

        public Task<GrainId[]> RangeAsync(RangeIndexQuery query)
        {
            RangeCallCount++;
            return _range(query);
        }

        public Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query)
        {
            return RangeAsync(query.Query);
        }

        public Task<GrainId[]> QueryAsync(PartitionQueryPlan query)
        {
            UnboundedQueryCallCount++;
            return _query(query);
        }

        public Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query)
        {
            return QueryAsync(query.Query);
        }

        public Task<PartitionQueryPageResult> QueryPageRoutedAsync(
            RoutedPartitionQueryPageRequest request)
        {
            QueryCallCount++;
            Plans.Add(request.Query);
            _started.TrySetResult();
            LastQueryTask = _query(request.Query);
            return CreatePageResultAsync(LastQueryTask, request);
        }

        private static async Task<PartitionQueryPageResult> CreatePageResultAsync(
            Task<GrainId[]> task,
            RoutedPartitionQueryPageRequest request)
        {
            var items = (await task)
                .Where(item => !request.HasAfter
                    || GrainIdCanonicalOrder.Compare(item, request.After) > 0)
                .Distinct(GrainIdCanonicalOrder.EqualityComparer)
                .Order(GrainIdCanonicalOrder.Comparer)
                .ToArray();
            return new PartitionQueryPageResult
            {
                Items = items,
                Exhausted = true,
                StopReason = PartitionQueryPageStopReason.Exhausted,
                Work = new PartitionQueryPageWork(),
                ItemByteCount = items.Sum(GrainIdCanonicalOrder.GetEncodedLength),
                ProtocolVersion = request.ProtocolVersion,
                OrderingVersion = request.OrderingVersion,
                WorkPolicyVersion = request.WorkPolicyVersion,
                ResponseFamily = request.ResponseFamily,
                Epoch = request.Epoch,
                QueryFingerprint = [.. request.QueryFingerprint],
                LayoutFormatVersion = request.LayoutFormatVersion,
                LayoutFingerprint = [.. request.LayoutFingerprint],
            };
        }
    }
}
