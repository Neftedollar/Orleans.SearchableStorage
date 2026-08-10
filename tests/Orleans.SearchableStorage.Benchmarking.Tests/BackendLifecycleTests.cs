using System.Diagnostics;
using Microsoft.Extensions.Hosting;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class BackendLifecycleTests
{
    [Fact]
    public async Task ProvisioningLostAcknowledgementRunsCompensationAndPreservesReport()
    {
        var resourceExists = false;

        var exception = await Assert.ThrowsAsync<BackendProvisioningException>(() =>
            BackendProvisioningGuard.RunAsync<object>(
                "delete-test-resource",
                _ =>
                {
                    resourceExists = true;
                    return Task.FromException<object>(new IOException("acknowledgement lost"));
                },
                () =>
                {
                    resourceExists = false;
                    return Task.CompletedTask;
                },
                CancellationToken.None));

        Assert.False(resourceExists);
        Assert.Equal(
            new BackendCleanupReport(
                "delete-test-resource",
                Attempted: true,
                Succeeded: true,
                Error: null),
            exception.CleanupReport);
        Assert.IsType<IOException>(exception.InnerException);
    }

    [Fact]
    public async Task ProvisioningCancellationIsFailureWithTruthfulCompensationReport()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var compensationCalls = 0;

        var exception = await Assert.ThrowsAsync<BackendProvisioningException>(() =>
            BackendProvisioningGuard.RunAsync<object>(
                "delete-test-resource",
                token => Task.FromCanceled<object>(token),
                () =>
                {
                    compensationCalls++;
                    return Task.CompletedTask;
                },
                cancellation.Token));

        Assert.Equal(1, compensationCalls);
        Assert.True(exception.CleanupReport.Attempted);
        Assert.True(exception.CleanupReport.Succeeded);
        Assert.IsType<TaskCanceledException>(exception.InnerException);
        Assert.True(Program.ContainsCancellation(exception));
    }

    [Fact]
    public async Task ProvisioningCompensationFailurePreservesBothFailures()
    {
        var exception = await Assert.ThrowsAsync<BackendProvisioningException>(() =>
            BackendProvisioningGuard.RunAsync<object>(
                "delete-test-resource",
                _ => Task.FromException<object>(new IOException("provision failed")),
                () => Task.FromException(new InvalidOperationException("cleanup failed")),
                CancellationToken.None));

        Assert.True(exception.CleanupReport.Attempted);
        Assert.False(exception.CleanupReport.Succeeded);
        Assert.Contains("cleanup failed", exception.CleanupReport.Error, StringComparison.Ordinal);
        var failures = Assert.IsType<AggregateException>(exception.InnerException).Flatten().InnerExceptions;
        Assert.Contains(failures, static failure => failure is IOException);
        Assert.Contains(failures, static failure => failure is InvalidOperationException);
    }

    [Fact]
    public async Task NonOwnerProvisioningFailureDoesNotClaimCleanupWasAttempted()
    {
        var exception = await Assert.ThrowsAsync<BackendProvisioningException>(() =>
            BackendProvisioningGuard.RunAsync<object>(
                "shared-silo-non-owner",
                _ => Task.FromException<object>(new IOException("provision failed")),
                compensate: null,
                cancellationToken: CancellationToken.None));

        Assert.Equal(
            new BackendCleanupReport(
                "shared-silo-non-owner",
                Attempted: false,
                Succeeded: false,
                Error: null),
            exception.CleanupReport);
    }

    [Fact]
    public async Task HostReleaseDeadlinesDoNotBlockLaterHostsOrBackendCleanup()
    {
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var laterHostStops = 0;
        var laterHostDisposals = 0;
        var backendCleanupCalls = 0;
        var stalledHost = new TestHost(
            _ => never.Task,
            () => new ValueTask(never.Task));
        var laterHost = new TestHost(
            _ =>
            {
                laterHostStops++;
                return Task.CompletedTask;
            },
            () =>
            {
                laterHostDisposals++;
                return ValueTask.CompletedTask;
            });
        var backend = new BackendLease(
            string.Empty,
            "test-service",
            "delete-test-resource",
            () =>
            {
                backendCleanupCalls++;
                return Task.CompletedTask;
            });
        var stopwatch = Stopwatch.StartNew();

        var failures = await HostRelease.ReleaseAsync(
            [stalledHost, laterHost],
            backend,
            stopTimeout: TimeSpan.FromMilliseconds(50),
            disposeTimeout: TimeSpan.FromMilliseconds(50));

        Assert.Equal(2, failures.Count);
        Assert.All(failures, static failure => Assert.IsType<TimeoutException>(failure));
        Assert.Equal(1, laterHostStops);
        Assert.Equal(1, laterHostDisposals);
        Assert.Equal(1, backendCleanupCalls);
        Assert.True(backend.CleanupReport.Attempted);
        Assert.True(backend.CleanupReport.Succeeded);
        Assert.InRange(stopwatch.Elapsed, TimeSpan.Zero, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task HostReleaseBoundsSynchronousDisposeWithoutSkippingBackendCleanup()
    {
        using var unblockDispose = new ManualResetEventSlim(initialState: false);
        var backendCleanupCalls = 0;
        var host = new SynchronousTestHost(() => unblockDispose.Wait());
        var backend = new BackendLease(
            string.Empty,
            "test-service",
            "delete-test-resource",
            () =>
            {
                backendCleanupCalls++;
                return Task.CompletedTask;
            });

        try
        {
            var failures = await HostRelease.ReleaseAsync(
                [host],
                backend,
                stopTimeout: TimeSpan.FromMilliseconds(50),
                disposeTimeout: TimeSpan.FromMilliseconds(50));

            Assert.Single(failures);
            Assert.IsType<TimeoutException>(failures[0]);
            Assert.Equal(1, backendCleanupCalls);
        }
        finally
        {
            unblockDispose.Set();
        }
    }

    [Fact]
    public void RedisCleanupPatternUsesAnExactSafeServiceNamespace()
    {
        Assert.Equal(
            "oss-benchmark-run-42/state/*",
            BackendNamespace.CreateRedisStateKeyPattern("oss-benchmark-run-42"));
    }

    [Theory]
    [InlineData("unsafe*")]
    [InlineData("unsafe?")]
    [InlineData("unsafe[abc]")]
    [InlineData("unsafe\\escape")]
    [InlineData("-leading")]
    [InlineData("trailing-")]
    [InlineData("double--hyphen")]
    [InlineData("юникод")]
    public void RedisCleanupPatternRejectsUnsafeServiceNamespaces(string serviceId)
    {
        Assert.Throws<InvalidDataException>(() => BackendNamespace.CreateRedisStateKeyPattern(serviceId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TopologyRejectsNonPortableBarrierTimeouts(bool primaryBarrier)
    {
        var topology = primaryBarrier
            ? new TopologySpec { BarrierTimeoutSeconds = TopologySpec.MaximumBarrierTimeoutSeconds + 1 }
            : new TopologySpec { BarrierLateCallDrainTimeoutSeconds = TopologySpec.MaximumBarrierTimeoutSeconds + 1 };

        Assert.Throws<ArgumentOutOfRangeException>(topology.Validate);
    }

    [Fact]
    public void OrleansResponseTimeoutOutlivesEveryDriverDeadlineAndDrainWindow()
    {
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "timeout-test",
            Population = new PopulationSpec
            {
                OperationTimeoutSeconds = 11,
                LateCallDrainTimeoutSeconds = 13,
            },
            Audit = new CorrectnessAuditSpec
            {
                OperationTimeoutSeconds = 17,
                LateCallDrainTimeoutSeconds = 19,
            },
            Topology = new TopologySpec
            {
                BarrierTimeoutSeconds = 23,
                BarrierLateCallDrainTimeoutSeconds = 29,
            },
        };
        var workload = new WorkloadSpec
        {
            Id = "timeout-test",
            OperationTimeoutSeconds = 31,
            LateCallDrainTimeoutSeconds = 37,
        };
        var spec = new BenchmarkSpec(scenario, new DatasetSpec(), workload);

        var timeout = BenchmarkHosting.GetResponseTimeout(spec);

        Assert.True(timeout > TimeSpan.FromSeconds(11 + 13));
        Assert.True(timeout > TimeSpan.FromSeconds(17 + 19));
        Assert.True(timeout > TimeSpan.FromSeconds(31 + 37));
        Assert.True(timeout > TimeSpan.FromSeconds(
            23 + 29 + BenchmarkRecordConstants.BarrierResultDeliveryMarginSeconds));
        Assert.Equal(TimeSpan.FromSeconds(112), timeout);
    }

    private sealed class TestHost(
        Func<CancellationToken, Task> stop,
        Func<ValueTask> disposeAsync) : IHost, IAsyncDisposable
    {
        public IServiceProvider Services => null!;

        public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken = default) => stop(cancellationToken);

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => disposeAsync();
    }

    private sealed class SynchronousTestHost(Action dispose) : IHost
    {
        public IServiceProvider Services => null!;

        public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose() => dispose();
    }
}
