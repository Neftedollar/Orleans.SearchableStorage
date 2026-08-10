using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

public sealed class ExternalStorageFixtureLifecycleTests
{
    private static readonly IReadOnlyDictionary<string, string?> EmptySettings =
        new Dictionary<string, string?>();

    [Fact]
    public async Task PrepareFailureCleansUpExactlyOnceAndPreservesOriginalException()
    {
        var expected = new InvalidOperationException("prepare failed");
        var factory = new RecordingClusterFactory();
        var fixture = new TestExternalStorageFixture(
            factory,
            () => Task.FromException<IReadOnlyDictionary<string, string?>>(expected));

        Func<Task> initialize = fixture.InitializeAsync;

        var exception = await initialize.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.BuildCount.Should().Be(0);
        fixture.CleanupCount.Should().Be(1);

        await fixture.DisposeAsync();
        fixture.CleanupCount.Should().Be(1);
    }

    [Fact]
    public async Task BuildFailureCleansUpExactlyOnceAndPreservesOriginalException()
    {
        var expected = new InvalidOperationException("build failed");
        var factory = new RecordingClusterFactory { BuildException = expected };
        var fixture = new TestExternalStorageFixture(factory);

        Func<Task> initialize = fixture.InitializeAsync;

        var exception = await initialize.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.BuildCount.Should().Be(1);
        factory.Cluster.StopCount.Should().Be(0);
        fixture.CleanupCount.Should().Be(1);

        await fixture.DisposeAsync();
        fixture.CleanupCount.Should().Be(1);
    }

    [Fact]
    public async Task DeployFailureStopsAndCleansUpWhilePreservingOriginalException()
    {
        var expected = new InvalidOperationException("deploy failed");
        var cleanupFailure = new IOException("cleanup failed");
        var disposeFailure = new IOException("dispose failed");
        var stopFailure = new IOException("stop failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.DeployException = expected;
        factory.Cluster.DisposeException = disposeFailure;
        factory.Cluster.StopException = stopFailure;
        var fixture = new TestExternalStorageFixture(
            factory,
            cleanup: () => Task.FromException(cleanupFailure),
            lifecycleEvents: factory.Events);

        Func<Task> initialize = fixture.InitializeAsync;

        var exception = await initialize.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.StopFailureDataKey]
            .Should().BeSameAs(stopFailure);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.DisposeFailureDataKey]
            .Should().BeSameAs(disposeFailure);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.CleanupFailureDataKey]
            .Should().BeSameAs(cleanupFailure);
        factory.Cluster.DeployCount.Should().Be(1);
        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("deploy", "stop", "dispose", "cleanup");
    }

    [Fact]
    public async Task SuccessfulReleaseAfterDeployFailureMakesRunnerTeardownIdempotent()
    {
        var expected = new InvalidOperationException("deploy failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.DeployException = expected;
        var fixture = new TestExternalStorageFixture(
            factory,
            lifecycleEvents: factory.Events);

        Func<Task> initialize = fixture.InitializeAsync;

        var exception = await initialize.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("deploy", "stop", "dispose", "cleanup");

        await fixture.DisposeAsync();

        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("deploy", "stop", "dispose", "cleanup");
    }

    [Fact]
    public async Task StopFailureStillDisposesAndCleansUpInOneCall()
    {
        var expected = new InvalidOperationException("stop failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.StopException = expected;
        var fixture = new TestExternalStorageFixture(
            factory,
            lifecycleEvents: factory.Events);
        await fixture.InitializeAsync();
        factory.Events.Clear();

        Func<Task> dispose = fixture.DisposeAsync;

        var exception = await dispose.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("stop", "dispose", "cleanup");
    }

    [Fact]
    public async Task SuccessfulTeardownStopsDisposesAndCleansUpInOrder()
    {
        var factory = new RecordingClusterFactory();
        var fixture = new TestExternalStorageFixture(
            factory,
            lifecycleEvents: factory.Events);
        await fixture.InitializeAsync();
        factory.Events.Clear();

        await fixture.DisposeAsync();

        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("stop", "dispose", "cleanup");
    }

    [Fact]
    public async Task DisposeFailureStillCleansUpAndRemainsThePrimaryException()
    {
        var expected = new InvalidOperationException("dispose failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.DisposeException = expected;
        var fixture = new TestExternalStorageFixture(
            factory,
            lifecycleEvents: factory.Events);
        await fixture.InitializeAsync();
        factory.Events.Clear();

        Func<Task> dispose = fixture.DisposeAsync;

        var exception = await dispose.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("stop", "dispose", "cleanup");
    }

    [Fact]
    public async Task CleanupFailureAfterClusterReleaseRemainsThePrimaryException()
    {
        var expected = new InvalidOperationException("cleanup failed");
        var factory = new RecordingClusterFactory();
        var fixture = new TestExternalStorageFixture(
            factory,
            cleanup: () => Task.FromException(expected),
            lifecycleEvents: factory.Events);
        await fixture.InitializeAsync();
        factory.Events.Clear();

        Func<Task> dispose = fixture.DisposeAsync;

        var exception = await dispose.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("stop", "dispose", "cleanup");
    }

    [Fact]
    public async Task CombinedTeardownFailuresPreserveStopAsPrimaryAndAttachLaterFailures()
    {
        var expected = new InvalidOperationException("stop failed");
        var disposeFailure = new IOException("dispose failed");
        var cleanupFailure = new IOException("cleanup failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.StopException = expected;
        factory.Cluster.DisposeException = disposeFailure;
        var fixture = new TestExternalStorageFixture(
            factory,
            cleanup: () => Task.FromException(cleanupFailure),
            lifecycleEvents: factory.Events);
        await fixture.InitializeAsync();
        factory.Events.Clear();

        Func<Task> dispose = fixture.DisposeAsync;

        var exception = await dispose.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.DisposeFailureDataKey]
            .Should().BeSameAs(disposeFailure);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.CleanupFailureDataKey]
            .Should().BeSameAs(cleanupFailure);
        factory.Cluster.StopCount.Should().Be(1);
        factory.Cluster.DisposeCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);
        factory.Events.Should().Equal("stop", "dispose", "cleanup");
    }

    private sealed class TestExternalStorageFixture : ExternalStorageFixture<EmptyHostConfigurator>
    {
        private readonly Func<Task> _cleanup;
        private readonly ICollection<string>? _lifecycleEvents;
        private readonly Func<Task<IReadOnlyDictionary<string, string?>>> _prepare;

        public TestExternalStorageFixture(
            IExternalStorageClusterFactory factory,
            Func<Task<IReadOnlyDictionary<string, string?>>>? prepare = null,
            Func<Task>? cleanup = null,
            ICollection<string>? lifecycleEvents = null)
            : base("lifecycle", isEnabled: true, factory)
        {
            _prepare = prepare ?? (() => Task.FromResult(EmptySettings));
            _cleanup = cleanup ?? (() => Task.CompletedTask);
            _lifecycleEvents = lifecycleEvents;
        }

        public int CleanupCount { get; private set; }

        protected override Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
        {
            return _prepare();
        }

        protected override Task CleanupBackendAsync()
        {
            CleanupCount++;
            _lifecycleEvents?.Add("cleanup");
            return _cleanup();
        }
    }

    private sealed class RecordingClusterFactory : IExternalStorageClusterFactory
    {
        public RecordingClusterFactory()
        {
            Cluster = new RecordingCluster(Events);
        }

        public RecordingCluster Cluster { get; }

        public List<string> Events { get; } = [];

        public Exception? BuildException { get; init; }

        public int BuildCount { get; private set; }

        public IExternalStorageCluster Build<TSiloConfigurator>(
            string serviceId,
            IReadOnlyDictionary<string, string?> settings)
            where TSiloConfigurator : IHostConfigurator, new()
        {
            BuildCount++;
            if (BuildException is not null)
            {
                throw BuildException;
            }

            return Cluster;
        }
    }

    private sealed class RecordingCluster(ICollection<string> events) : IExternalStorageCluster
    {
        public TestCluster Cluster => throw new NotSupportedException();

        public Exception? DeployException { get; set; }

        public Exception? DisposeException { get; set; }

        public Exception? StopException { get; set; }

        public int DeployCount { get; private set; }

        public int DisposeCount { get; private set; }

        public int StopCount { get; private set; }

        public Task DeployAsync()
        {
            DeployCount++;
            events.Add("deploy");
            return DeployException is null
                ? Task.CompletedTask
                : Task.FromException(DeployException);
        }

        public Task StopAsync()
        {
            StopCount++;
            events.Add("stop");
            return StopException is null
                ? Task.CompletedTask
                : Task.FromException(StopException);
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            events.Add("dispose");
            return DisposeException is null
                ? ValueTask.CompletedTask
                : ValueTask.FromException(DisposeException);
        }
    }

    private sealed class EmptyHostConfigurator : IHostConfigurator
    {
        public void Configure(IHostBuilder hostBuilder)
        {
        }
    }
}
