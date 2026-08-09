using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.Runtime;
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
    public async Task BoundedRangeApiKeepsUsingItsRequiredBoundedMessage()
    {
        var match = GrainId.Create("vacancy", "match");
        RangeIndexQuery? sentQuery = null;
        var partition = new ControlledPartition(
            _ => Task.FromResult(Array.Empty<GrainId>()),
            range: query =>
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
        partition.RangeCallCount.Should().Be(1);
        partition.QueryCallCount.Should().Be(0);
        sentQuery.Should().NotBeNull();
        sentQuery!.LowerBound.SignedInteger.Should().Be(5);
        sentQuery.UpperBound.SignedInteger.Should().Be(8);
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

    private static SearchableStorageClient CreateClient(params ControlledPartition[] partitions)
    {
        return new SearchableStorageClient(
            "test",
            partitions,
            static () => Task.FromResult(true));
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

    private sealed class ControlledPartition : IStoragePartitionGrain
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

        public Task<string> WriteAsync(StorageWriteRequest request)
        {
            throw new NotSupportedException();
        }

        public Task ClearAsync(string recordKey, string? expectedETag)
        {
            throw new NotSupportedException();
        }

        public Task<GrainId[]> FindAsync(ExactIndexQuery query)
        {
            FindCallCount++;
            return _find(query);
        }

        public Task<GrainId[]> RangeAsync(RangeIndexQuery query)
        {
            RangeCallCount++;
            return _range(query);
        }

        public Task<GrainId[]> QueryAsync(PartitionQueryPlan query)
        {
            QueryCallCount++;
            Plans.Add(query);
            _started.TrySetResult();
            LastQueryTask = _query(query);
            return LastQueryTask;
        }
    }
}
