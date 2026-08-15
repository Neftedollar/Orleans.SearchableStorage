using System.Diagnostics.CodeAnalysis;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Runtime.Tests;

public sealed class DurableProjectionRuntimeTests
{
    [Fact]
    public async Task StartupReplaysUpsertAndRemovalBeforeReadiness()
    {
        var coordinator = new FakeLockCoordinator();
        var store = new FakeStore(coordinator);
        var upsert = Projection("startup-upsert", 1, ProjectionOperation.Upsert);
        var removal = Projection("startup-removal", 2, ProjectionOperation.Remove);
        store.Desired.AddRange([upsert, removal]);
        var index = new FakeIndex();
        DurableProjectionRuntime? runtime = null;
        index.BeforeExternalCall = () => Assert.False(runtime!.IsReady);
        runtime = CreateRuntime(store, index);

        await runtime.StartAsync();

        Assert.True(runtime.IsReady);
        Assert.Equal("ready", runtime.Status);
        Assert.True(IndexOf(store.Events, $"materialize:Upsert:{upsert.Version}")
            < IndexOf(store.Events, $"index-upsert:{upsert.Version}"));
        Assert.True(IndexOf(store.Events, $"index-upsert:{upsert.Version}")
            < IndexOf(store.Events, $"rebuild-finalize:{upsert.Version}"));
        Assert.True(IndexOf(store.Events, $"index-remove:{removal.AccountKey}")
            < IndexOf(store.Events, $"materialize:Remove:{removal.Version}"));
        Assert.True(IndexOf(store.Events, $"materialize:Remove:{removal.Version}")
            < IndexOf(store.Events, $"rebuild-finalize:{removal.Version}"));
        await runtime.DisposeAsync();
    }

    [Fact]
    public async Task OnlyOneRuntimeCanHoldTheDispatcherIncarnation()
    {
        var coordinator = new FakeLockCoordinator();
        var first = CreateRuntime(new FakeStore(coordinator), new FakeIndex());
        var second = CreateRuntime(new FakeStore(coordinator), new FakeIndex());

        await first.StartAsync();
        await Assert.ThrowsAsync<ProjectionDispatcherAlreadyActiveException>(() => second.StartAsync());

        Assert.True(first.IsReady);
        Assert.False(second.IsReady);
        await first.DisposeAsync();
        await second.DisposeAsync();
    }

    [Fact]
    public async Task RuntimeDispatchUsesHydrationIndexFinalizeOrder()
    {
        var store = new FakeStore(new FakeLockCoordinator());
        var index = new FakeIndex();
        var runtime = CreateRuntime(store, index);
        await runtime.StartAsync();
        store.Events.Clear();

        var upsert = Projection("dispatch-upsert", 3, ProjectionOperation.Upsert);
        var removal = Projection("dispatch-removal", 7, ProjectionOperation.Remove);
        store.LeaseBatches.Enqueue(
        [
            new ProjectionOutboxLease(Guid.NewGuid(), upsert, 0),
            new ProjectionOutboxLease(Guid.NewGuid(), removal, 0),
        ]);

        var completed = await runtime.DispatchOnceAsync();

        Assert.Equal(2, completed);
        Assert.True(IndexOf(store.Events, $"prepare:{upsert.Version}")
            < IndexOf(store.Events, $"index-upsert:{upsert.Version}"));
        Assert.True(IndexOf(store.Events, $"index-upsert:{upsert.Version}")
            < IndexOf(store.Events, $"finalize:{upsert.Version}"));
        Assert.True(IndexOf(store.Events, $"index-remove:{removal.AccountKey}")
            < IndexOf(store.Events, $"finalize:{removal.Version}"));
        await runtime.DisposeAsync();
    }

    [Fact]
    public async Task PreIndexFailureIsReleasedWithoutTouchingIndex()
    {
        var store = new FakeStore(new FakeLockCoordinator())
        {
            PrepareException = new InvalidOperationException("deterministic preparation failure"),
        };
        var index = new FakeIndex();
        var runtime = CreateRuntime(store, index);
        await runtime.StartAsync();
        var projection = Projection("prepare-failure", 1, ProjectionOperation.Upsert);
        store.LeaseBatches.Enqueue(
            [new ProjectionOutboxLease(Guid.NewGuid(), projection, 0)]);

        Assert.Equal(0, await runtime.DispatchOnceAsync());

        Assert.Equal(1, store.FailCount);
        Assert.Equal(0, index.ExternalCallCount);
        Assert.True(runtime.IsReady);
        await runtime.DisposeAsync();
    }

    [Fact]
    public async Task IndexExceptionIsAmbiguousAndRequestsFatalTerminationOnce()
    {
        var store = new FakeStore(new FakeLockCoordinator());
        var index = new FakeIndex { Exception = new OperationCanceledException("unknown index outcome") };
        var terminator = new FakeTerminator();
        var runtime = CreateRuntime(store, index, terminator);
        await runtime.StartAsync();
        var projection = Projection("ambiguous-index", 1, ProjectionOperation.Upsert);
        store.LeaseBatches.Enqueue(
            [new ProjectionOutboxLease(Guid.NewGuid(), projection, 0)]);

        await Assert.ThrowsAsync<FatalTerminationRequestedException>(() => runtime.DispatchOnceAsync());

        Assert.Equal(1, terminator.CallCount);
        Assert.Equal(0, store.FinalizeCount);
        Assert.Equal(0, store.FailCount);
        Assert.False(runtime.IsReady);
        await runtime.DisposeAsync();
    }

    [Fact]
    public async Task FalseFinalizeAfterIndexCallIsAmbiguousAndCannotRetryInProcess()
    {
        var store = new FakeStore(new FakeLockCoordinator()) { FinalizeResult = false };
        var terminator = new FakeTerminator();
        var runtime = CreateRuntime(store, new FakeIndex(), terminator);
        await runtime.StartAsync();
        var projection = Projection("lost-lease", 1, ProjectionOperation.Remove);
        store.LeaseBatches.Enqueue(
            [new ProjectionOutboxLease(Guid.NewGuid(), projection, 0)]);

        await Assert.ThrowsAsync<FatalTerminationRequestedException>(() => runtime.DispatchOnceAsync());

        Assert.Equal(1, terminator.CallCount);
        Assert.Equal(1, store.FinalizeCount);
        Assert.Equal(0, store.FailCount);
        Assert.False(runtime.IsReady);
        await runtime.DisposeAsync();
    }

    [Fact]
    public async Task MissingRemovalAndExactRestartReplayConverge()
    {
        var coordinator = new FakeLockCoordinator();
        var store = new FakeStore(coordinator);
        var removal = Projection("already-missing", 9, ProjectionOperation.Remove);
        store.Desired.Add(removal);
        var index = new FakeIndex();

        var first = CreateRuntime(store, index);
        await first.StartAsync();
        Assert.True(first.IsReady);
        await first.DisposeAsync();

        var second = CreateRuntime(store, index);
        await second.StartAsync();

        Assert.True(second.IsReady);
        Assert.Equal(2, index.RemoveCount);
        Assert.Equal(2, store.Events.Count(value => value == $"materialize:Remove:{removal.Version}"));
        await second.DisposeAsync();
    }

    [Fact]
    public async Task ChangedRebuildUpsertNeverTouchesIndexOrOpensReadiness()
    {
        var store = new FakeStore(new FakeLockCoordinator()) { MaterializeResult = false };
        store.Desired.Add(Projection("changed", 4, ProjectionOperation.Upsert));
        var index = new FakeIndex();
        var terminator = new FakeTerminator();
        var runtime = CreateRuntime(store, index, terminator);

        await Assert.ThrowsAsync<ProjectionChangedDuringRebuildException>(() => runtime.StartAsync());

        Assert.Equal(0, index.ExternalCallCount);
        Assert.Equal(0, terminator.CallCount);
        Assert.False(runtime.IsReady);
        await runtime.DisposeAsync();
    }

    private static DurableProjectionRuntime CreateRuntime(
        FakeStore store,
        FakeIndex index,
        FakeTerminator? terminator = null)
    {
        index.Events = store.Events;
        return new DurableProjectionRuntime(
            store,
            index,
            terminator ?? new FakeTerminator(),
            new DurableProjectionRuntimeOptions
            {
                RebuildPageSize = 2,
                DispatchBatchSize = 10,
                DispatchLeaseDuration = TimeSpan.FromMinutes(1),
                PreIndexFailureDelay = TimeSpan.Zero,
            });
    }

    private static ProjectionSnapshot Projection(
        string seed,
        long version,
        ProjectionOperation operation)
        => new(
            AccountKey.FromDid($"did:plc:{seed}"),
            version,
            operation,
            isComplete: true,
            projectionCutMinuteUtc: 100,
            nextRecalculationMinuteUtc: operation == ProjectionOperation.Upsert ? 200 : null,
            lastActivityMinuteUtc: 99,
            createdRecordCount1Day: 1,
            createdRecordCount7Days: 2,
            createdRecordCount30Days: 3,
            updatedRecordCount1Day: 1,
            updatedRecordCount7Days: 2,
            updatedRecordCount30Days: 3,
            deletedRecordCount1Day: 0,
            deletedRecordCount7Days: 1,
            deletedRecordCount30Days: 1,
            currentPostCount: operation == ProjectionOperation.Upsert ? 4 : 0,
            currentFollowingCount: operation == ProjectionOperation.Upsert ? 5 : 0,
            currentFollowerCount: operation == ProjectionOperation.Upsert ? 6 : 0,
            postCreates1Day: 1,
            postCreates7Days: 1,
            postCreates30Days: 2,
            receivedEngagementCreates30Days: 7);

    private static int IndexOf(List<string> values, string expected)
    {
        for (var index = 0; index < values.Count; index++)
        {
            if (string.Equals(values[index], expected, StringComparison.Ordinal))
            {
                return index;
            }
        }

        return -1;
    }

    private sealed class FakeStore(FakeLockCoordinator coordinator) : IDurableProjectionDispatchStore
    {
        public List<ProjectionSnapshot> Desired { get; } = [];

        public Queue<IReadOnlyList<ProjectionOutboxLease>> LeaseBatches { get; } = new();

        public List<string> Events { get; } = [];

        public Exception? PrepareException { get; init; }

        public bool MaterializeResult { get; init; } = true;

        public bool FinalizeResult { get; init; } = true;

        public int FinalizeCount { get; private set; }

        public int FailCount { get; private set; }

        public Task<IProjectionDispatcherIncarnation?> TryAcquireIncarnationAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(coordinator.TryAcquire(Events));
        }

        public Task<IReadOnlyList<ProjectionSnapshot>> ReadDesiredProjectionPageAsync(
            AccountKey? afterAccountKeyExclusive,
            int batchSize,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = Desired
                .Where(projection => afterAccountKeyExclusive is null
                    || projection.AccountKey > afterAccountKeyExclusive.Value)
                .OrderBy(static projection => projection.AccountKey)
                .Take(batchSize)
                .ToArray();
            return Task.FromResult<IReadOnlyList<ProjectionSnapshot>>(page);
        }

        public Task<bool> MaterializeDesiredProjectionAsync(
            ProjectionSnapshot projection,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Events.Add($"materialize:{projection.Operation}:{projection.Version}");
            return Task.FromResult(MaterializeResult);
        }

        public Task<bool> FinalizeRebuildProjectionAsync(
            ProjectionSnapshot projection,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Events.Add($"rebuild-finalize:{projection.Version}");
            return Task.FromResult(true);
        }

        public Task<IReadOnlyList<ProjectionOutboxLease>> LeaseProjectionsAsync(
            int batchSize,
            TimeSpan leaseDuration,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Events.Add("lease");
            return Task.FromResult(
                LeaseBatches.Count == 0
                    ? (IReadOnlyList<ProjectionOutboxLease>)Array.Empty<ProjectionOutboxLease>()
                    : LeaseBatches.Dequeue());
        }

        public Task<bool> PrepareProjectionHydrationAsync(
            ProjectionOutboxLease lease,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Events.Add($"prepare:{lease.Projection.Version}");
            return PrepareException is null
                ? Task.FromResult(true)
                : Task.FromException<bool>(PrepareException);
        }

        public Task<bool> FinalizeProjectionAsync(
            ProjectionOutboxLease lease,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            FinalizeCount++;
            Events.Add($"finalize:{lease.Projection.Version}");
            return Task.FromResult(FinalizeResult);
        }

        public Task<bool> FailProjectionAsync(
            ProjectionOutboxLease lease,
            DateTimeOffset availableAtUtc,
            string errorCode,
            string errorMessage,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            FailCount++;
            Events.Add($"fail:{lease.Projection.Version}");
            return Task.FromResult(true);
        }
    }

    private sealed class FakeIndex : IRuntimeProjectionIndexWriter
    {
        public List<string> Events { get; set; } = [];

        public Action? BeforeExternalCall { get; set; }

        public Exception? Exception { get; init; }

        public int ExternalCallCount { get; private set; }

        public int RemoveCount { get; private set; }

        public ValueTask UpsertAsync(
            ProjectionSnapshot projection,
            CancellationToken cancellationToken = default)
        {
            BeforeExternalCall?.Invoke();
            ExternalCallCount++;
            Events.Add($"index-upsert:{projection.Version}");
            return Exception is null
                ? ValueTask.CompletedTask
                : ValueTask.FromException(Exception);
        }

        public ValueTask RemoveAsync(
            AccountKey accountKey,
            CancellationToken cancellationToken = default)
        {
            BeforeExternalCall?.Invoke();
            ExternalCallCount++;
            RemoveCount++;
            Events.Add($"index-remove:{accountKey}");
            return Exception is null
                ? ValueTask.CompletedTask
                : ValueTask.FromException(Exception);
        }
    }

    private sealed class FakeLockCoordinator
    {
        private int _held;

        public IProjectionDispatcherIncarnation? TryAcquire(List<string> events)
            => Interlocked.CompareExchange(ref _held, 1, 0) == 0
                ? new FakeIncarnation(this, events)
                : null;

        private sealed class FakeIncarnation(
            FakeLockCoordinator owner,
            List<string> events) : IProjectionDispatcherIncarnation
        {
            private int _disposed;

            public ValueTask<bool> IsHeldAsync(CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                events.Add("lock-check");
                return ValueTask.FromResult(
                    Volatile.Read(ref _disposed) == 0
                    && Volatile.Read(ref owner._held) == 1);
            }

            public ValueTask DisposeAsync()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    Volatile.Write(ref owner._held, 0);
                }

                return ValueTask.CompletedTask;
            }
        }
    }

    private sealed class FakeTerminator : IFatalProcessTerminator
    {
        public int CallCount { get; private set; }

        [DoesNotReturn]
        public void Terminate(string message, Exception? exception = null)
        {
            CallCount++;
            throw new FatalTerminationRequestedException(message, exception);
        }
    }

    private sealed class FatalTerminationRequestedException(string message, Exception? innerException)
        : Exception(message, innerException);
}
