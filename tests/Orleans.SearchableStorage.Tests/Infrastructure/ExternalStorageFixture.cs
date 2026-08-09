using Microsoft.Extensions.Configuration;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

internal static class BackendStorageTestConstants
{
    public const int PartitionCount = 8;
}

public abstract class ExternalStorageFixture<TSiloConfigurator> : ISearchableStorageFixture, IAsyncLifetime
    where TSiloConfigurator : IHostConfigurator, new()
{
    // Capture opt-in once so fixture initialization and per-test skip decisions cannot diverge.
    private readonly bool _isEnabled = BackendTestEnvironment.ShouldRunBackendTests();
    private TestCluster? _cluster;

    protected ExternalStorageFixture(string backendName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(backendName);
        BackendName = backendName;
        ServiceId = $"oss-{backendName}-{Guid.NewGuid():N}";
    }

    public TestCluster Cluster => _cluster
        ?? throw new InvalidOperationException($"The {BackendName} test cluster has not been initialized.");

    public int PartitionCount => BackendStorageTestConstants.PartitionCount;

    public string ServiceId { get; }

    protected string BackendName { get; }

    public async Task InitializeAsync()
    {
        if (!_isEnabled)
        {
            return;
        }

        IReadOnlyDictionary<string, string?> settings;
        try
        {
            settings = await PrepareBackendAsync();
        }
        catch
        {
            await CleanupBackendAsync();
            throw;
        }

        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.Options.ServiceId = ServiceId;
        builder.Options.ClusterId = $"{ServiceId}-cluster";
        builder.ConfigureHostConfiguration(configuration => configuration.AddInMemoryCollection(settings));
        builder.AddSiloBuilderConfigurator<TSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ExternalStorageClientConfigurator>();
        _cluster = builder.Build();

        try
        {
            await _cluster.DeployAsync();
        }
        catch
        {
            try
            {
                await _cluster.StopAllSilosAsync();
            }
            finally
            {
                await CleanupBackendAsync();
            }

            throw;
        }
    }

    public async Task DisposeAsync()
    {
        if (!_isEnabled)
        {
            return;
        }

        try
        {
            if (_cluster is not null)
            {
                await _cluster.StopAllSilosAsync();
            }
        }
        finally
        {
            await CleanupBackendAsync();
        }
    }

    public void EnsurePreconditionsMet()
    {
        Skip.IfNot(
            _isEnabled,
            $"Set {BackendTestEnvironment.RunBackendTestsVariable}=true to run the {BackendName} storage contract.");
    }

    protected abstract Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync();

    protected abstract Task CleanupBackendAsync();
}
