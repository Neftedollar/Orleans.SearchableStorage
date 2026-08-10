using System.Net;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence;
using Orleans.Serialization;
using Orleans.Storage;
using StackExchange.Redis;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkHosting
{
    private const int ResponseDeliveryMarginSeconds = 30;

    public static async Task<BenchmarkCluster> StartClientClusterAsync(
        BenchmarkSpec spec,
        CancellationToken cancellationToken)
    {
        return spec.Topology.Mode switch
        {
            TopologyMode.Embedded => await StartEmbeddedClusterAsync(spec, cancellationToken),
            TopologyMode.External => await StartExternalClientAsync(spec, cancellationToken),
            _ => throw new InvalidOperationException($"Unsupported topology '{spec.Topology.Mode}'."),
        };
    }

    public static async Task<ServeOutcome> ServeAsync(BenchmarkSpec spec, CancellationToken cancellationToken)
    {
        BackendLease? backend = null;
        IHost? host = null;
        Exception? failure = null;
        BackendCleanupReport? provisioningCleanup = null;
        var waitingForShutdown = false;
        try
        {
            var preparedBackend = await BackendLease.PrepareAsync(
                spec.Storage,
                spec.Topology,
                cancellationToken);
            backend = preparedBackend;
            var advertisedAddress = await EndpointResolver.ResolveAddressAsync(
                spec.Topology.AdvertisedAddress,
                cancellationToken);
            var primaryEndpoint = string.IsNullOrWhiteSpace(spec.Topology.PrimarySiloEndpoint)
                ? new IPEndPoint(advertisedAddress, spec.Topology.SiloPort)
                : await EndpointResolver.ResolveEndpointAsync(
                    spec.Topology.PrimarySiloEndpoint,
                    spec.Topology.SiloPort,
                    cancellationToken);

            var builder = Host.CreateApplicationBuilder();
            ConfigureLogging(builder.Logging);
            builder.UseOrleans(siloBuilder =>
            {
                ConfigureClusterIdentity(siloBuilder, spec.Topology);
                ConfigureResponseTimeout(siloBuilder, spec);
                siloBuilder
                    .UseDevelopmentClustering(primaryEndpoint)
                    .ConfigureEndpoints(
                        advertisedAddress,
                        spec.Topology.SiloPort,
                        spec.Topology.GatewayPort,
                        listenOnAnyHostAddress: true);
                ConfigureStorage(siloBuilder, spec.Storage, preparedBackend);
            });

            host = builder.Build();
            await host.StartAsync(cancellationToken);
            Console.WriteLine(
                $"Orleans searchable-storage benchmark silo started. Gateway={advertisedAddress}:{spec.Topology.GatewayPort}");
            waitingForShutdown = true;
            await host.WaitForShutdownAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested && waitingForShutdown)
        {
            // The controller stops the silo after the load job exits. Continue into the
            // ordered host-stop and backend-cleanup path and emit cleanup evidence.
        }
        catch (Exception exception)
        {
            failure = exception;
            provisioningCleanup = FindProvisioningCleanup(exception);
        }
        finally
        {
            var releaseFailures = await HostRelease.ReleaseAsync(
                host is null ? [] : [host],
                backend);
            foreach (var releaseFailure in releaseFailures)
            {
                failure = failure is null ? releaseFailure : new AggregateException(failure, releaseFailure);
            }

            Console.WriteLine($"Backend cleanup policy: {backend?.CleanupReport.Policy ?? "provisioning-failed"}");
        }

        var cleanup = backend?.CleanupReport
            ?? provisioningCleanup
            ?? new BackendCleanupReport(
                "backend-provisioning-failed",
                Attempted: false,
                Succeeded: false,
                Error: "Backend provisioning did not produce a lease, so no cleanup target was available.");
        return new ServeOutcome(cleanup, failure);
    }

    private static async Task<BenchmarkCluster> StartEmbeddedClusterAsync(
        BenchmarkSpec spec,
        CancellationToken cancellationToken)
    {
        BackendLease? backend = null;
        var silos = new List<IHost>(spec.Topology.EmbeddedSiloCount);
        IHost? clientHost = null;
        try
        {
            var preparedBackend = await BackendLease.PrepareAsync(
                spec.Storage,
                spec.Topology,
                cancellationToken);
            backend = preparedBackend;
            for (var index = 0; index < spec.Topology.EmbeddedSiloCount; index++)
            {
                var siloIndex = index;
                var builder = Host.CreateApplicationBuilder();
                ConfigureLogging(builder.Logging);
                builder.UseOrleans(siloBuilder =>
                {
                    ConfigureClusterIdentity(siloBuilder, spec.Topology);
                    ConfigureResponseTimeout(siloBuilder, spec);
                    siloBuilder.UseLocalhostClustering(
                        siloPort: checked(spec.Topology.SiloPort + siloIndex),
                        gatewayPort: checked(spec.Topology.GatewayPort + siloIndex),
                        primarySiloEndpoint: siloIndex == 0
                            ? null
                            : new IPEndPoint(IPAddress.Loopback, spec.Topology.SiloPort));
                    ConfigureStorage(siloBuilder, spec.Storage, preparedBackend);
                });

                var host = builder.Build();
                silos.Add(host);
                await host.StartAsync(cancellationToken);
            }

            var clientBuilder = Host.CreateApplicationBuilder();
            ConfigureLogging(clientBuilder.Logging);
            clientBuilder.UseOrleansClient(client =>
            {
                client.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = spec.Topology.ClusterId;
                    options.ServiceId = spec.Topology.ServiceId;
                });
                client.Configure<ClientMessagingOptions>(options =>
                {
                    options.LocalAddress = IPAddress.Loopback;
                    options.ResponseTimeout = GetResponseTimeout(spec);
                });
                client.UseStaticClustering(
                    Enumerable.Range(spec.Topology.GatewayPort, spec.Topology.EmbeddedSiloCount)
                        .Select(static port => new IPEndPoint(IPAddress.Loopback, port))
                        .ToArray());
            });
            clientHost = clientBuilder.Build();
            await clientHost.StartAsync(cancellationToken);
            return new BenchmarkCluster(silos, clientHost, preparedBackend);
        }
        catch (Exception startupFailure)
        {
            var releaseFailures = await HostRelease.ReleaseAsync(
                clientHost is null
                    ? silos.AsEnumerable().Reverse()
                    : [clientHost, .. silos.AsEnumerable().Reverse()],
                backend);
            var failure = releaseFailures.Count == 0
                ? startupFailure
                : new AggregateException([startupFailure, .. releaseFailures]);
            var cleanup = backend?.CleanupReport
                ?? FindProvisioningCleanup(startupFailure)
                ?? new BackendCleanupReport(
                    "backend-provisioning-failed",
                    Attempted: false,
                    Succeeded: false,
                    Error: "Backend provisioning did not produce a lease, so no cleanup target was available.");
            throw new BenchmarkClusterStartException(cleanup, failure);
        }
    }

    private static BackendCleanupReport? FindProvisioningCleanup(Exception exception)
    {
        if (exception is BackendProvisioningException provisioning)
        {
            return provisioning.CleanupReport;
        }

        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.Flatten().InnerExceptions)
            {
                if (FindProvisioningCleanup(inner) is { } report)
                {
                    return report;
                }
            }
        }

        return exception.InnerException is null ? null : FindProvisioningCleanup(exception.InnerException);
    }

    private static async Task<BenchmarkCluster> StartExternalClientAsync(
        BenchmarkSpec spec,
        CancellationToken cancellationToken)
    {
        var gateways = new List<IPEndPoint>(spec.Topology.GatewayEndpoints.Count);
        foreach (var value in spec.Topology.GatewayEndpoints)
        {
            gateways.Add(await EndpointResolver.ResolveEndpointAsync(
                value,
                spec.Topology.GatewayPort,
                cancellationToken));
        }

        var builder = Host.CreateApplicationBuilder();
        ConfigureLogging(builder.Logging);
        builder.UseOrleansClient(client =>
        {
            client.Configure<ClusterOptions>(options =>
            {
                options.ClusterId = spec.Topology.ClusterId;
                options.ServiceId = spec.Topology.ServiceId;
            });
            client.Configure<ClientMessagingOptions>(options =>
            {
                options.LocalAddress = IPAddress.Loopback;
                options.ResponseTimeout = GetResponseTimeout(spec);
            });
            client.UseStaticClustering(gateways.ToArray());
        });

        var clientHost = builder.Build();
        try
        {
            await clientHost.StartAsync(cancellationToken);
        }
        catch (Exception startupFailure)
        {
            var releaseFailures = await HostRelease.ReleaseAsync([clientHost], backend: null);
            var failure = releaseFailures.Count == 0
                ? startupFailure
                : new AggregateException([startupFailure, .. releaseFailures]);
            throw new BenchmarkClusterStartException(
                new BackendCleanupReport(
                    "external-client-startup-release",
                    Attempted: true,
                    Succeeded: releaseFailures.Count == 0,
                    Error: releaseFailures.Count == 0 ? null : string.Join("; ", releaseFailures.Select(static value => value.Message))),
                failure);
        }

        return new BenchmarkCluster([], clientHost, backend: null);
    }

    private static void ConfigureClusterIdentity(ISiloBuilder siloBuilder, TopologySpec topology)
    {
        siloBuilder.Configure<ClusterOptions>(options =>
        {
            options.ClusterId = topology.ClusterId;
            options.ServiceId = topology.ServiceId;
        });
    }

    internal static TimeSpan GetResponseTimeout(BenchmarkSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        var maximumDeadlineSeconds = new[]
        {
            checked(spec.Population.OperationTimeoutSeconds + spec.Population.LateCallDrainTimeoutSeconds),
            checked(spec.Audit.OperationTimeoutSeconds + spec.Audit.LateCallDrainTimeoutSeconds),
            checked(spec.Workload.OperationTimeoutSeconds + spec.Workload.LateCallDrainTimeoutSeconds),
            checked(
                spec.Topology.BarrierTimeoutSeconds
                + BenchmarkRecordConstants.BarrierResultDeliveryMarginSeconds
                + spec.Topology.BarrierLateCallDrainTimeoutSeconds),
        }.Max();
        return TimeSpan.FromSeconds(checked(maximumDeadlineSeconds + ResponseDeliveryMarginSeconds));
    }

    private static void ConfigureResponseTimeout(ISiloBuilder siloBuilder, BenchmarkSpec spec)
    {
        siloBuilder.Configure<SiloMessagingOptions>(options =>
            options.ResponseTimeout = GetResponseTimeout(spec));
    }

    private static void ConfigureStorage(
        ISiloBuilder siloBuilder,
        StorageSpec storage,
        BackendLease backend)
    {
        var physicalProvider = storage.Path is StoragePath.Searchable
            ? SearchableStorageConstants.PhysicalStorageProviderName
            : BenchmarkRecordConstants.PlainStorageProviderName;
        AddPhysicalStorage(siloBuilder, physicalProvider, storage, backend.ProviderConnectionString);

        if (storage.Path is StoragePath.Plain)
        {
            return;
        }

        siloBuilder.AddSearchableGrainStorage(
            BenchmarkRecordConstants.StorageProviderName,
            options =>
            {
                options.PartitionCount = storage.PartitionCount;
                options.VirtualSlotTargetCount = storage.VirtualSlotTargetCount;
                options.JournalSegmentCapacity = storage.JournalSegmentCapacity;
                options.MaximumJournalReplayEntries = storage.MaximumJournalReplayEntries;
                options.CompactionThreshold = storage.CompactionThreshold;
            });
    }

    private static void AddPhysicalStorage(
        ISiloBuilder siloBuilder,
        string providerName,
        StorageSpec storage,
        string providerConnectionString)
    {
        switch (storage.Backend)
        {
            case StorageBackend.Memory:
                siloBuilder.AddMemoryGrainStorage(
                    providerName,
                    (OptionsBuilder<MemoryGrainStorageOptions> optionsBuilder) =>
                        optionsBuilder.Configure<OrleansJsonSerializer>(UseJsonSerializer));
                break;
            case StorageBackend.PostgreSql:
                siloBuilder.AddAdoNetGrainStorage(
                    providerName,
                    (OptionsBuilder<AdoNetGrainStorageOptions> optionsBuilder) =>
                        optionsBuilder.Configure<OrleansJsonSerializer>((options, serializer) =>
                        {
                            options.ConnectionString = providerConnectionString;
                            options.Invariant = "Npgsql";
                            options.DeleteStateOnClear = true;
                            UseJsonSerializer(options, serializer);
                        }));
                break;
            case StorageBackend.Redis:
                siloBuilder.AddRedisGrainStorage(
                    providerName,
                    (OptionsBuilder<RedisStorageOptions> optionsBuilder) =>
                        optionsBuilder.Configure<OrleansJsonSerializer>((options, serializer) =>
                        {
                            options.ConfigurationOptions = ConfigurationOptions.Parse(providerConnectionString);
                            options.DeleteStateOnClear = true;
                            UseJsonSerializer(options, serializer);
                        }));
                break;
            case StorageBackend.AzureBlob:
                siloBuilder.AddAzureBlobGrainStorage(
                    providerName,
                    (OptionsBuilder<AzureBlobStorageOptions> optionsBuilder) =>
                        optionsBuilder.Configure<OrleansJsonSerializer>((options, serializer) =>
                        {
                            options.BlobServiceClient = new BlobServiceClient(providerConnectionString);
                            options.ContainerName = storage.AzureBlobContainer;
                            options.DeleteStateOnClear = true;
                            UseJsonSerializer(options, serializer);
                        }));
                break;
            default:
                throw new InvalidOperationException($"Unsupported storage backend '{storage.Backend}'.");
        }
    }

    private static void UseJsonSerializer<TOptions>(TOptions options, OrleansJsonSerializer serializer)
        where TOptions : IStorageProviderSerializerOptions
    {
        options.GrainStorageSerializer = new JsonGrainStorageSerializer(serializer);
    }

    private static void ConfigureLogging(ILoggingBuilder logging)
    {
        // Provider exceptions can contain credentials. The driver emits its own sanitized
        // status and machine-readable failure evidence, so framework console providers are removed.
        logging.ClearProviders();
    }

}

internal sealed record ServeOutcome(BackendCleanupReport Cleanup, Exception? Failure);

internal sealed class BenchmarkClusterStartException(
    BackendCleanupReport cleanupReport,
    Exception innerException)
    : Exception("Benchmark cluster startup failed; the best-effort release path completed.", innerException)
{
    public BackendCleanupReport CleanupReport { get; } = cleanupReport;
}

internal static class HostRelease
{
    internal static readonly TimeSpan DefaultStopTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultDisposeTimeout = TimeSpan.FromSeconds(15);

    public static async Task<IReadOnlyList<Exception>> ReleaseAsync(
        IEnumerable<IHost> hosts,
        BackendLease? backend,
        TimeSpan? stopTimeout = null,
        TimeSpan? disposeTimeout = null)
    {
        var effectiveStopTimeout = stopTimeout ?? DefaultStopTimeout;
        var effectiveDisposeTimeout = disposeTimeout ?? DefaultDisposeTimeout;
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(effectiveStopTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(effectiveDisposeTimeout, TimeSpan.Zero);

        var failures = new List<Exception>();
        foreach (var host in hosts)
        {
            try
            {
                await RunBoundedAsync(
                    token => host.StopAsync(token),
                    effectiveStopTimeout,
                    "stop");
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }

            try
            {
                await RunBoundedAsync(
                    _ => host is IAsyncDisposable asyncDisposable
                        ? asyncDisposable.DisposeAsync().AsTask()
                        : DisposeSynchronouslyAsync(host),
                    effectiveDisposeTimeout,
                    "dispose");
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        if (backend is not null)
        {
            try
            {
                await backend.DisposeAsync();
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        return failures;
    }

    private static async Task RunBoundedAsync(
        Func<CancellationToken, Task> operation,
        TimeSpan timeout,
        string phase)
    {
        using var deadline = new CancellationTokenSource(timeout);
        var operationTask = Task.Run(() => operation(deadline.Token));
        try
        {
            await operationTask.WaitAsync(deadline.Token);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            ObserveCompletion(operationTask);
            throw new TimeoutException($"Host {phase} exceeded its {timeout.TotalSeconds:F0}-second deadline.");
        }
    }

    private static Task DisposeSynchronouslyAsync(IDisposable disposable)
    {
        disposable.Dispose();
        return Task.CompletedTask;
    }

    private static void ObserveCompletion(Task task)
    {
        _ = task.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
}

internal sealed class BenchmarkCluster(
    IReadOnlyList<IHost> silos,
    IHost clientHost,
    BackendLease? backend) : IAsyncDisposable
{
    private int _disposed;

    public IClusterClient Client => clientHost.Services.GetRequiredService<IClusterClient>();

    public BackendCleanupReport CleanupReport => backend?.CleanupReport
        ?? new BackendCleanupReport("silo-owned-on-shutdown", Attempted: false, Succeeded: false, Error: null);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        var failures = await HostRelease.ReleaseAsync(
            [clientHost, .. silos.AsEnumerable().Reverse()],
            backend);
        if (failures.Count > 0)
        {
            throw new AggregateException(failures);
        }
    }
}

internal static class EndpointResolver
{
    public static void ValidateAddressSyntax(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!IPAddress.TryParse(value, out _) && Uri.CheckHostName(value) is UriHostNameType.Unknown)
        {
            throw new InvalidDataException($"Address '{value}' is not a valid IP address or DNS host name.");
        }
    }

    public static void ValidateEndpointSyntax(string value, int defaultPort)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        ArgumentOutOfRangeException.ThrowIfLessThan(defaultPort, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(defaultPort, 65_535);
        if (IPAddress.TryParse(value.Trim('[', ']'), out _))
        {
            return;
        }

        if (IPEndPoint.TryParse(value, out var parsed) && parsed.Port is >= 1 and <= 65_535)
        {
            return;
        }

        var candidate = value.Contains(':', StringComparison.Ordinal) ? value : $"{value}:{defaultPort}";
        if (!Uri.TryCreate($"tcp://{candidate}", UriKind.Absolute, out var uri) ||
            uri.Port is < 1 or > 65_535 || Uri.CheckHostName(uri.Host) is UriHostNameType.Unknown)
        {
            throw new InvalidDataException($"Endpoint '{value}' is not a valid host:port value.");
        }
    }

    public static async Task<IPAddress> ResolveAddressAsync(string value, CancellationToken cancellationToken)
    {
        ValidateAddressSyntax(value);
        if (IPAddress.TryParse(value, out var parsed))
        {
            return parsed;
        }

        var addresses = await Dns.GetHostAddressesAsync(value, cancellationToken);
        return addresses.FirstOrDefault(static address => address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
            ?? addresses.FirstOrDefault()
            ?? throw new InvalidOperationException($"Host '{value}' did not resolve to an IP address.");
    }

    public static async Task<IPEndPoint> ResolveEndpointAsync(
        string value,
        int defaultPort,
        CancellationToken cancellationToken)
    {
        ValidateEndpointSyntax(value, defaultPort);
        if (IPAddress.TryParse(value.Trim('[', ']'), out var bareAddress))
        {
            return new IPEndPoint(bareAddress, defaultPort);
        }

        if (IPEndPoint.TryParse(value, out var endpoint) && endpoint.Port is >= 1 and <= 65_535)
        {
            return endpoint;
        }

        var candidate = value.Contains(':', StringComparison.Ordinal) ? value : $"{value}:{defaultPort}";
        if (!Uri.TryCreate($"tcp://{candidate}", UriKind.Absolute, out var uri) || uri.Port is < 1 or > 65_535)
        {
            throw new ArgumentException($"Endpoint '{value}' is not a valid host:port value.", nameof(value));
        }

        var address = await ResolveAddressAsync(uri.Host, cancellationToken);
        return new IPEndPoint(address, uri.Port);
    }
}
