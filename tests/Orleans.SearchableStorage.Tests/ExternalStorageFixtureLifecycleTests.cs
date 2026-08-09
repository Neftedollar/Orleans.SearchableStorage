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
        var stopFailure = new IOException("stop failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.DeployException = expected;
        factory.Cluster.StopException = stopFailure;
        var failCleanup = true;
        var fixture = new TestExternalStorageFixture(
            factory,
            cleanup: () =>
            {
                if (failCleanup)
                {
                    failCleanup = false;
                    return Task.FromException(cleanupFailure);
                }

                return Task.CompletedTask;
            });

        Func<Task> initialize = fixture.InitializeAsync;

        var exception = await initialize.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.StopFailureDataKey]
            .Should().BeSameAs(stopFailure);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.CleanupFailureDataKey]
            .Should().BeSameAs(cleanupFailure);
        factory.Cluster.DeployCount.Should().Be(1);
        factory.Cluster.StopCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);

        factory.Cluster.StopException = null;
        await fixture.DisposeAsync();
        factory.Cluster.StopCount.Should().Be(2);
        fixture.CleanupCount.Should().Be(2);
    }

    [Fact]
    public async Task StopFailureStillCleansUpAndRemainsThePrimaryException()
    {
        var expected = new InvalidOperationException("stop failed");
        var cleanupFailure = new IOException("cleanup failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.StopException = expected;
        var failCleanup = true;
        var fixture = new TestExternalStorageFixture(
            factory,
            cleanup: () =>
            {
                if (failCleanup)
                {
                    failCleanup = false;
                    return Task.FromException(cleanupFailure);
                }

                return Task.CompletedTask;
            });
        await fixture.InitializeAsync();

        Func<Task> dispose = fixture.DisposeAsync;

        var exception = await dispose.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        exception.Which.Data[ExternalStorageFixture<EmptyHostConfigurator>.CleanupFailureDataKey]
            .Should().BeSameAs(cleanupFailure);
        factory.Cluster.StopCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);

        factory.Cluster.StopException = null;
        await fixture.DisposeAsync();
        factory.Cluster.StopCount.Should().Be(2);
        fixture.CleanupCount.Should().Be(2);
    }

    [Fact]
    public async Task CleanupFailureAfterSuccessfulStopRetriesOnlyCleanup()
    {
        var expected = new InvalidOperationException("cleanup failed");
        var factory = new RecordingClusterFactory();
        var failCleanup = true;
        var fixture = new TestExternalStorageFixture(
            factory,
            cleanup: () =>
            {
                if (failCleanup)
                {
                    failCleanup = false;
                    return Task.FromException(expected);
                }

                return Task.CompletedTask;
            });
        await fixture.InitializeAsync();

        Func<Task> firstDispose = fixture.DisposeAsync;

        var exception = await firstDispose.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.Cluster.StopCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);

        await fixture.DisposeAsync();
        factory.Cluster.StopCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(2);
    }

    [Fact]
    public async Task StopFailureAfterSuccessfulCleanupRetriesOnlyStop()
    {
        var expected = new InvalidOperationException("stop failed");
        var factory = new RecordingClusterFactory();
        factory.Cluster.StopException = expected;
        var fixture = new TestExternalStorageFixture(factory);
        await fixture.InitializeAsync();

        Func<Task> firstDispose = fixture.DisposeAsync;

        var exception = await firstDispose.Should().ThrowAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expected);
        factory.Cluster.StopCount.Should().Be(1);
        fixture.CleanupCount.Should().Be(1);

        factory.Cluster.StopException = null;
        await fixture.DisposeAsync();
        factory.Cluster.StopCount.Should().Be(2);
        fixture.CleanupCount.Should().Be(1);
    }

    private sealed class TestExternalStorageFixture : ExternalStorageFixture<EmptyHostConfigurator>
    {
        private readonly Func<Task> _cleanup;
        private readonly Func<Task<IReadOnlyDictionary<string, string?>>> _prepare;

        public TestExternalStorageFixture(
            IExternalStorageClusterFactory factory,
            Func<Task<IReadOnlyDictionary<string, string?>>>? prepare = null,
            Func<Task>? cleanup = null)
            : base("lifecycle", isEnabled: true, factory)
        {
            _prepare = prepare ?? (() => Task.FromResult(EmptySettings));
            _cleanup = cleanup ?? (() => Task.CompletedTask);
        }

        public int CleanupCount { get; private set; }

        protected override Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
        {
            return _prepare();
        }

        protected override Task CleanupBackendAsync()
        {
            CleanupCount++;
            return _cleanup();
        }
    }

    private sealed class RecordingClusterFactory : IExternalStorageClusterFactory
    {
        public RecordingCluster Cluster { get; } = new();

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

    private sealed class RecordingCluster : IExternalStorageCluster
    {
        public TestCluster Cluster => throw new NotSupportedException();

        public Exception? DeployException { get; set; }

        public Exception? StopException { get; set; }

        public int DeployCount { get; private set; }

        public int StopCount { get; private set; }

        public Task DeployAsync()
        {
            DeployCount++;
            return DeployException is null
                ? Task.CompletedTask
                : Task.FromException(DeployException);
        }

        public Task StopAsync()
        {
            StopCount++;
            return StopException is null
                ? Task.CompletedTask
                : Task.FromException(StopException);
        }
    }

    private sealed class EmptyHostConfigurator : IHostConfigurator
    {
        public void Configure(IHostBuilder hostBuilder)
        {
        }
    }
}
