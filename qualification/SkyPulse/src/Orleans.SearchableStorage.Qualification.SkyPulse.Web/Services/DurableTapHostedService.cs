using System.Net.WebSockets;
using Npgsql;
using Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;
using Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Waits for the exact Memory-index rebuild and rolling catch-up, verifies and bootstraps the
/// frozen corpus, and only then opens the authenticated TAP acknowledgement stream.
/// </summary>
internal sealed class DurableTapHostedService(
    DurableProjectionHostedService projection,
    IDurableTapBackend backend,
    DurableCorpusCapacityManager capacity,
    SkyPulseDurableConfiguration configuration,
    TimeProvider timeProvider) : BackgroundService
{
    private const int WaitingForProjection = 0;
    private const int VerifyingCorpus = 1;
    private const int BootstrappingCorpus = 2;
    private const int ValidatingRepositoryProvisioner = 3;
    private const int Connecting = 4;
    private const int ProvisioningRepositories = 5;
    private const int Running = 6;
    private const int Reconnecting = 7;
    private const int Faulted = 8;
    private const int Stopped = 9;
    private int _state;

    public bool IsReady => Volatile.Read(ref _state) == Running && projection.IsReady;

    public string Status => Volatile.Read(ref _state) switch
    {
        WaitingForProjection => "waiting-for-projection",
        VerifyingCorpus => "verifying-corpus",
        BootstrappingCorpus => "bootstrapping-corpus",
        ValidatingRepositoryProvisioner => "validating-tap-repository-provisioner",
        Connecting => "connecting-tap",
        ProvisioningRepositories => "provisioning-tap-repositories",
        Running => "tap-connected",
        Reconnecting => "reconnecting-tap",
        Faulted => "faulted",
        Stopped => "stopped",
        _ => "invalid",
    };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await WaitForProjectionAsync(stoppingToken).ConfigureAwait(false);
            Volatile.Write(ref _state, VerifyingCorpus);
            Volatile.Write(ref _state, BootstrappingCorpus);
            await capacity.InitializeAsync(stoppingToken).ConfigureAwait(false);

            var processor = new DurableTapDeliveryProcessor(
                configuration.SourceInstanceId,
                backend,
                capacity.Admission,
                configuration.CreateIngestionOptions());
            var runner = new DurableTapSessionRunner(processor, timeProvider);
            var sessionFactory = new TapWebSocketSessionFactory(configuration.CreateTapOptions());
            var repositoriesProvisioned = false;
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    Volatile.Write(ref _state, Connecting);
                    await using var session = await sessionFactory
                        .ConnectAsync(stoppingToken)
                        .ConfigureAwait(false);
                    if (!repositoriesProvisioned)
                    {
                        Volatile.Write(ref _state, ProvisioningRepositories);
                        repositoriesProvisioned = await RunProvisioningSessionAsync(
                            session,
                            runner,
                            stoppingToken).ConfigureAwait(false);
                    }
                    else
                    {
                        Volatile.Write(ref _state, Running);
                        await RunConnectedSessionAsync(
                            session,
                            runner,
                            stoppingToken).ConfigureAwait(false);
                    }
                }
                catch (Exception exception) when (IsTransient(exception, stoppingToken))
                {
                    // The exact delivery remains unacknowledged. TAP will redeliver it after the
                    // authenticated session reconnects; no source-controlled detail is logged.
                }

                Volatile.Write(ref _state, Reconnecting);
                await Task.Delay(configuration.IngestionReconnectDelay, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            Volatile.Write(ref _state, Stopped);
        }
        catch
        {
            Volatile.Write(ref _state, Faulted);
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _state, Stopped);
    }

    private async Task WaitForProjectionAsync(CancellationToken cancellationToken)
    {
        Volatile.Write(ref _state, WaitingForProjection);
        while (!projection.IsReady)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (projection.Status is "faulted" or "stopped")
            {
                throw new InvalidOperationException(
                    "The durable projection runtime stopped before TAP ingestion could start.");
            }

            await Task.Delay(
                configuration.IngestionStartupPollDelay,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<bool> RunProvisioningSessionAsync(
        IDurableTapSession session,
        DurableTapSessionRunner runner,
        CancellationToken stoppingToken)
    {
        using var sessionCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var receiveTask = runner.RunAsync(session, sessionCancellation.Token);
        var provisionTask = capacity.EnsureCurrentProvisionedAsync(sessionCancellation.Token);
        Task? growthTask = null;
        try
        {
            if (await Task.WhenAny(receiveTask, provisionTask).ConfigureAwait(false) == receiveTask)
            {
                _ = await receiveTask.ConfigureAwait(false);
                return false;
            }

            await provisionTask.ConfigureAwait(false);

            Volatile.Write(ref _state, Running);
            growthTask = capacity.RunGrowthLoopAsync(sessionCancellation.Token);
            if (await Task.WhenAny(receiveTask, growthTask).ConfigureAwait(false) == receiveTask)
            {
                _ = await receiveTask.ConfigureAwait(false);
            }
            else
            {
                await growthTask.ConfigureAwait(false);
            }

            return true;
        }
        finally
        {
            await sessionCancellation.CancelAsync().ConfigureAwait(false);
            await ObserveShutdownAsync(receiveTask).ConfigureAwait(false);
            await ObserveShutdownAsync(provisionTask).ConfigureAwait(false);
            if (growthTask is not null)
            {
                await ObserveShutdownAsync(growthTask).ConfigureAwait(false);
            }
        }
    }

    private async Task RunConnectedSessionAsync(
        IDurableTapSession session,
        DurableTapSessionRunner runner,
        CancellationToken stoppingToken)
    {
        using var sessionCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var receiveTask = runner.RunAsync(session, sessionCancellation.Token);
        var growthTask = capacity.RunGrowthLoopAsync(sessionCancellation.Token);
        try
        {
            if (await Task.WhenAny(receiveTask, growthTask).ConfigureAwait(false) == receiveTask)
            {
                _ = await receiveTask.ConfigureAwait(false);
                return;
            }

            await growthTask.ConfigureAwait(false);
        }
        finally
        {
            await sessionCancellation.CancelAsync().ConfigureAwait(false);
            await ObserveShutdownAsync(receiveTask).ConfigureAwait(false);
            await ObserveShutdownAsync(growthTask).ConfigureAwait(false);
        }
    }

    private static async Task ObserveShutdownAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            // The primary task outcome above controls reconnect/fault. This await only observes
            // cancellation or a concurrent failure without exposing route or delivery details.
        }
    }

    private static bool IsTransient(Exception exception, CancellationToken stoppingToken)
    {
        if (stoppingToken.IsCancellationRequested)
        {
            return false;
        }

        return exception switch
        {
            TapConnectionClosedException => true,
            WebSocketException { InnerException: HttpRequestException http } => IsTransientHttp(http),
            WebSocketException => true,
            NpgsqlException postgres => postgres.IsTransient,
            HttpRequestException http => IsTransientHttp(http),
            TimeoutException => true,
            _ => false,
        };
    }

    private static bool IsTransientHttp(HttpRequestException exception)
    {
        if (exception.StatusCode is not { } status)
        {
            return true;
        }

        return status is System.Net.HttpStatusCode.RequestTimeout
                or System.Net.HttpStatusCode.TooManyRequests
            || (int)status >= 500;
    }
}

/// <summary>
/// Keeps API readiness closed unless both the rebuilt projection runtime and authenticated TAP
/// ingestion loop are ready.
/// </summary>
internal sealed class DurableCompositeReadiness(
    DurableProjectionHostedService projection,
    DurableTapHostedService ingestion) : IProjectionReadiness
{
    public bool IsReady => projection.IsReady && ingestion.IsReady;

    public string Status => !projection.IsReady
        ? $"projection-{projection.Status}"
        : !ingestion.IsReady
            ? $"ingestion-{ingestion.Status}"
            : "durable-ready";
}
