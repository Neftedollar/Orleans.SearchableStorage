using Microsoft.Extensions.DependencyInjection;
using Orleans.SearchableStorage;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;
using Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

internal sealed class LocalFunctionalProjectionReadiness : IProjectionReadiness
{
    public bool IsReady => true;

    public string Status => "local-functional-ready";
}

/// <summary>
/// Initializes the durable database identity, rebuilds the ephemeral Memory index, and advances
/// the ordered projection outbox in bounded batches.
/// </summary>
internal sealed class DurableProjectionHostedService(
    [FromKeyedServices(SkyPulseIndexContract.ProviderName)] ISearchableStorageAdminClient indexAdmin,
    PostgreSqlSchemaManager schemaManager,
    PostgreSqlRuntimeManifestStore manifestStore,
    DurableProjectionRuntime runtime,
    RollingWindowRecalculationWorker recalculationWorker,
    SkyPulseDurableConfiguration configuration) : BackgroundService, IProjectionReadiness
{
    private const int Starting = 0;
    private const int CatchingUp = 1;
    private const int Running = 2;
    private const int Faulted = 3;
    private const int Stopped = 4;
    private int _state;

    public bool IsReady => Volatile.Read(ref _state) == Running && runtime.IsReady;

    public string Status => Volatile.Read(ref _state) switch
    {
        Starting => runtime.Status,
        CatchingUp => "rolling-window-catch-up",
        Running => runtime.Status,
        Faulted => "faulted",
        Stopped => "stopped",
        _ => "invalid",
    };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await schemaManager.ApplyMigrationsAsync(stoppingToken).ConfigureAwait(false);
            var validation = await schemaManager.ValidateAsync(stoppingToken).ConfigureAwait(false);
            if (!validation.IsValid)
            {
                throw new InvalidOperationException(
                    "The PostgreSQL schema does not match the reviewed contract: "
                    + string.Join("; ", validation.Errors));
            }

            var indexSchema = await indexAdmin
                .GetIndexSchemaAsync<AccountIndexState>(
                    SkyPulseIndexContract.StateName,
                    SkyPulseIndexContract.ApplicationSchemaVersion,
                    stoppingToken)
                .ConfigureAwait(false);
            if (indexSchema.State != SearchableStorageIndexSchemaState.Active
                || string.IsNullOrWhiteSpace(indexSchema.Fingerprint))
            {
                throw new InvalidOperationException(
                    "The package-backed SkyPulse index schema must be active and fingerprinted before durable rebuild.");
            }

            var manifest = configuration.CreateManifest(indexSchema.Fingerprint);
            await manifestStore.BindAsync(manifest, stoppingToken).ConfigureAwait(false);
            await runtime.StartAsync(stoppingToken).ConfigureAwait(false);
            Volatile.Write(ref _state, CatchingUp);
            await CatchUpBeforeReadinessAsync(stoppingToken).ConfigureAwait(false);
            Volatile.Write(ref _state, Running);

            while (!stoppingToken.IsCancellationRequested)
            {
                var recalculations = await recalculationWorker
                    .ProcessOnceAsync(stoppingToken)
                    .ConfigureAwait(false);
                EnsureRecalculationBatchSucceeded(recalculations);
                var completed = await runtime.DispatchOnceAsync(stoppingToken).ConfigureAwait(false);
                if (recalculations.LeasedCount < recalculationWorker.BatchSize
                    && completed < configuration.DispatchBatchSize)
                {
                    await Task.Delay(configuration.DispatchIdleDelay, stoppingToken).ConfigureAwait(false);
                }
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
        await runtime.DisposeAsync().ConfigureAwait(false);
        Volatile.Write(ref _state, Stopped);
    }

    private async Task CatchUpBeforeReadinessAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var recalculations = await recalculationWorker
                .ProcessOnceAsync(cancellationToken)
                .ConfigureAwait(false);
            EnsureRecalculationBatchSucceeded(recalculations);
            var dispatched = await runtime.DispatchOnceAsync(cancellationToken).ConfigureAwait(false);
            if (recalculations.LeasedCount == 0 && dispatched == 0)
            {
                return;
            }
        }
    }

    private static void EnsureRecalculationBatchSucceeded(
        RollingWindowRecalculationBatchResult result)
    {
        if (result.FailedCount != 0)
        {
            throw new InvalidOperationException(
                $"{result.FailedCount} rolling-window recalculation(s) failed and were released for retry.");
        }
    }
}
