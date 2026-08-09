using System.Runtime.ExceptionServices;
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
    internal const string CleanupFailureDataKey = "Orleans.SearchableStorage.ExternalFixture.CleanupFailure";
    internal const string StopFailureDataKey = "Orleans.SearchableStorage.ExternalFixture.StopFailure";

    private readonly IExternalStorageClusterFactory _clusterFactory;
    private readonly bool _isEnabled;
    private IExternalStorageCluster? _cluster;
    private bool _cleanupCompleted;
    private bool _stopCompleted;

    protected ExternalStorageFixture(string backendName)
        : this(
            backendName,
            BackendTestEnvironment.ShouldRunBackendTests(),
            ExternalStorageClusterFactory.Instance)
    {
    }

    private protected ExternalStorageFixture(
        string backendName,
        bool isEnabled,
        IExternalStorageClusterFactory clusterFactory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(backendName);
        ArgumentNullException.ThrowIfNull(clusterFactory);
        BackendName = backendName;
        ServiceId = $"oss-{backendName}-{Guid.NewGuid():N}";
        _isEnabled = isEnabled;
        _clusterFactory = clusterFactory;
    }

    public TestCluster Cluster => _cluster?.Cluster
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

        try
        {
            var settings = await PrepareBackendAsync();
            _cluster = _clusterFactory.Build<TSiloConfigurator>(ServiceId, settings);
            await _cluster.DeployAsync();
        }
        catch (Exception exception)
        {
            await ReleaseAfterInitializationFailureAsync(exception);
            ExceptionDispatchInfo.Capture(exception).Throw();
        }
    }

    public async Task DisposeAsync()
    {
        if (!_isEnabled)
        {
            return;
        }

        Exception? failure = null;
        try
        {
            await StopClusterAsync();
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        try
        {
            await CompleteBackendCleanupAsync();
        }
        catch (Exception exception) when (failure is not null)
        {
            AttachSecondaryFailure(failure, CleanupFailureDataKey, exception);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        if (failure is not null)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
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

    private async Task ReleaseAfterInitializationFailureAsync(Exception initializationFailure)
    {
        try
        {
            await StopClusterAsync();
        }
        catch (Exception exception)
        {
            AttachSecondaryFailure(initializationFailure, StopFailureDataKey, exception);
        }

        try
        {
            await CompleteBackendCleanupAsync();
        }
        catch (Exception exception)
        {
            AttachSecondaryFailure(initializationFailure, CleanupFailureDataKey, exception);
        }
    }

    private async Task StopClusterAsync()
    {
        if (_cluster is null || _stopCompleted)
        {
            return;
        }

        await _cluster.StopAsync();
        _stopCompleted = true;
    }

    private async Task CompleteBackendCleanupAsync()
    {
        if (_cleanupCompleted)
        {
            return;
        }

        await CleanupBackendAsync();
        _cleanupCompleted = true;
    }

    private static void AttachSecondaryFailure(
        Exception primary,
        string key,
        Exception secondary)
    {
        primary.Data[key] = secondary;
    }
}

internal interface IExternalStorageCluster
{
    TestCluster Cluster { get; }

    Task DeployAsync();

    Task StopAsync();
}

internal interface IExternalStorageClusterFactory
{
    IExternalStorageCluster Build<TSiloConfigurator>(
        string serviceId,
        IReadOnlyDictionary<string, string?> settings)
        where TSiloConfigurator : IHostConfigurator, new();
}

internal sealed class ExternalStorageClusterFactory : IExternalStorageClusterFactory
{
    public static ExternalStorageClusterFactory Instance { get; } = new();

    private ExternalStorageClusterFactory()
    {
    }

    public IExternalStorageCluster Build<TSiloConfigurator>(
        string serviceId,
        IReadOnlyDictionary<string, string?> settings)
        where TSiloConfigurator : IHostConfigurator, new()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.Options.ServiceId = serviceId;
        builder.Options.ClusterId = $"{serviceId}-cluster";
        builder.ConfigureHostConfiguration(configuration => configuration.AddInMemoryCollection(settings));
        builder.AddSiloBuilderConfigurator<TSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ExternalStorageClientConfigurator>();
        return new ExternalStorageCluster(builder.Build());
    }
}

internal sealed class ExternalStorageCluster(TestCluster cluster) : IExternalStorageCluster
{
    public TestCluster Cluster { get; } = cluster;

    public Task DeployAsync()
    {
        return Cluster.DeployAsync();
    }

    public Task StopAsync()
    {
        return Cluster.StopAllSilosAsync();
    }
}
