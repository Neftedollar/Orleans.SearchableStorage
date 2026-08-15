using System.Diagnostics.CodeAnalysis;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

/// <summary>
/// Rebuilds and advances the one co-located ephemeral Memory index from durable PostgreSQL state.
/// </summary>
/// <remarks>
/// The searchable-index API is a blind, non-transactional boundary. Consequently, any failure
/// after invoking it hard-stops this process; a fresh process must rebuild the entire index before
/// becoming ready. This class deliberately does not support multiple silos or dispatchers.
/// </remarks>
public sealed class DurableProjectionRuntime : IProjectionReadiness, IAsyncDisposable
{
    private const int NotStarted = 0;
    private const int Rebuilding = 1;
    private const int Ready = 2;
    private const int Faulted = 3;
    private const int Disposed = 4;

    private readonly IDurableProjectionDispatchStore _store;
    private readonly IRuntimeProjectionIndexWriter _indexWriter;
    private readonly IFatalProcessTerminator _terminator;
    private readonly DurableProjectionRuntimeOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _dispatchGate = new(1, 1);
    private IProjectionDispatcherIncarnation? _incarnation;
    private int _state;

    public DurableProjectionRuntime(
        IDurableProjectionDispatchStore store,
        IRuntimeProjectionIndexWriter indexWriter,
        IFatalProcessTerminator terminator,
        DurableProjectionRuntimeOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(indexWriter);
        ArgumentNullException.ThrowIfNull(terminator);
        _options = options ?? new DurableProjectionRuntimeOptions();
        _options.Validate();
        _store = store;
        _indexWriter = indexWriter;
        _terminator = terminator;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public bool IsReady => Volatile.Read(ref _state) == Ready;

    public string Status => Volatile.Read(ref _state) switch
    {
        NotStarted => "not-started",
        Rebuilding => "rebuilding",
        Ready => "ready",
        Faulted => "faulted",
        Disposed => "disposed",
        _ => "invalid",
    };

    /// <summary>
    /// Acquires the one PostgreSQL incarnation lock and fully rebuilds the Memory index before
    /// opening readiness.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _state, Rebuilding, NotStarted) != NotStarted)
        {
            throw new InvalidOperationException("This projection runtime can be started exactly once.");
        }

        IProjectionDispatcherIncarnation? acquired = null;
        try
        {
            acquired = await _store.TryAcquireIncarnationAsync(cancellationToken).ConfigureAwait(false);
            if (acquired is null)
            {
                throw new ProjectionDispatcherAlreadyActiveException();
            }

            _incarnation = acquired;
            await EnsureIncarnationHeldOrTerminateAsync(
                    "The dispatcher advisory lock was lost before index rebuild.",
                    exception: null)
                .ConfigureAwait(false);
            await RebuildAsync(cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _state, Ready);
        }
        catch
        {
            Volatile.Write(ref _state, Faulted);
            _incarnation = null;
            if (acquired is not null)
            {
                await acquired.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <summary>
    /// Leases and dispatches one bounded batch. The caller owns loop cadence and shutdown.
    /// </summary>
    /// <returns>The number of exact outbox rows finalized by this call.</returns>
    public async Task<int> DispatchOnceAsync(CancellationToken cancellationToken = default)
    {
        EnsureReady();
        await _dispatchGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureReady();
            cancellationToken.ThrowIfCancellationRequested();
            await EnsureIncarnationHeldOrTerminateAsync(
                    "The dispatcher advisory lock was lost before leasing projections.",
                    exception: null)
                .ConfigureAwait(false);

            var leases = await _store
                .LeaseProjectionsAsync(
                    _options.DispatchBatchSize,
                    _options.DispatchLeaseDuration,
                    cancellationToken)
                .ConfigureAwait(false);
            ValidateLeaseBatch(leases);

            var completed = 0;
            foreach (var lease in leases)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await DispatchLeaseAsync(lease, cancellationToken).ConfigureAwait(false))
                {
                    completed++;
                }
            }

            return completed;
        }
        finally
        {
            _dispatchGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var previous = Interlocked.Exchange(ref _state, Disposed);
        if (previous == Disposed)
        {
            return;
        }

        await _dispatchGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            var incarnation = Interlocked.Exchange(ref _incarnation, null);
            if (incarnation is not null)
            {
                await incarnation.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _dispatchGate.Release();
        }
    }

    private async Task RebuildAsync(CancellationToken cancellationToken)
    {
        AccountKey? cursor = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await _store
                .ReadDesiredProjectionPageAsync(
                    cursor,
                    _options.RebuildPageSize,
                    cancellationToken)
                .ConfigureAwait(false);
            ValidateRebuildPage(page, cursor);

            foreach (var projection in page)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await RebuildProjectionAsync(projection, cancellationToken).ConfigureAwait(false);
                cursor = projection.AccountKey;
            }

            if (page.Count < _options.RebuildPageSize)
            {
                return;
            }
        }
    }

    private async Task RebuildProjectionAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken)
    {
        if (projection.Operation == ProjectionOperation.Upsert)
        {
            if (!await _store
                    .MaterializeDesiredProjectionAsync(projection, cancellationToken)
                    .ConfigureAwait(false))
            {
                throw new ProjectionChangedDuringRebuildException(projection.AccountKey, projection.Version);
            }

            cancellationToken.ThrowIfCancellationRequested();
            await EnsureIncarnationHeldOrTerminateAsync(
                    "The dispatcher advisory lock was lost before a rebuild upsert.",
                    exception: null)
                .ConfigureAwait(false);
            try
            {
                await _indexWriter.UpsertAsync(projection, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                TerminateAmbiguous("rebuild upsert", projection, exception);
            }

            await EnsureHeldAfterExternalOrTerminateAsync("rebuild upsert", projection).ConfigureAwait(false);
            await FinalizeRebuildAfterExternalAsync(projection).ConfigureAwait(false);
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        await EnsureIncarnationHeldOrTerminateAsync(
                "The dispatcher advisory lock was lost before a rebuild removal.",
                exception: null)
            .ConfigureAwait(false);
        try
        {
            await _indexWriter.RemoveAsync(projection.AccountKey, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            TerminateAmbiguous("rebuild removal", projection, exception);
        }

        await EnsureHeldAfterExternalOrTerminateAsync("rebuild removal", projection).ConfigureAwait(false);
        var materialized = false;
        try
        {
            materialized = await _store
                .MaterializeDesiredProjectionAsync(projection, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            TerminateAmbiguous("rebuild removal materialization", projection, exception);
        }

        if (!materialized)
        {
            TerminateAmbiguous("rebuild removal materialization", projection, exception: null);
        }

        await FinalizeRebuildAfterExternalAsync(projection).ConfigureAwait(false);
    }

    private async Task FinalizeRebuildAfterExternalAsync(ProjectionSnapshot projection)
    {
        var finalized = false;
        try
        {
            finalized = await _store
                .FinalizeRebuildProjectionAsync(projection, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            TerminateAmbiguous("rebuild checkpoint", projection, exception);
        }

        if (!finalized)
        {
            TerminateAmbiguous("rebuild checkpoint", projection, exception: null);
        }
    }

    private async Task<bool> DispatchLeaseAsync(
        ProjectionOutboxLease lease,
        CancellationToken cancellationToken)
    {
        if (lease.Projection.Operation == ProjectionOperation.Upsert)
        {
            bool prepared;
            try
            {
                prepared = await _store
                    .PrepareProjectionHydrationAsync(lease, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                await ReleasePreIndexFailureAsync(lease, exception).ConfigureAwait(false);
                return false;
            }

            if (!prepared)
            {
                return false;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        await EnsureIncarnationHeldOrTerminateAsync(
                "The dispatcher advisory lock was lost before an outbox index call.",
                exception: null)
            .ConfigureAwait(false);

        try
        {
            if (lease.Projection.Operation == ProjectionOperation.Upsert)
            {
                await _indexWriter
                    .UpsertAsync(lease.Projection, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await _indexWriter
                    .RemoveAsync(lease.Projection.AccountKey, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            TerminateAmbiguous("outbox index call", lease.Projection, exception);
        }

        await EnsureHeldAfterExternalOrTerminateAsync("outbox index call", lease.Projection).ConfigureAwait(false);
        var finalized = false;
        try
        {
            finalized = await _store
                .FinalizeProjectionAsync(lease, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            TerminateAmbiguous("outbox finalization", lease.Projection, exception);
        }


        if (!finalized)
        {
            TerminateAmbiguous("outbox finalization", lease.Projection, exception: null);
        }

        return true;
    }

    private async Task ReleasePreIndexFailureAsync(
        ProjectionOutboxLease lease,
        Exception exception)
    {
        var availableAt = _timeProvider.GetUtcNow() + _options.PreIndexFailureDelay;
        var errorMessage = $"{exception.GetType().Name} before searchable-index invocation.";
        await _store
            .FailProjectionAsync(
                lease,
                availableAt,
                "pre_index_failure",
                errorMessage,
                CancellationToken.None)
            .ConfigureAwait(false);
    }

    private async Task EnsureHeldAfterExternalOrTerminateAsync(
        string stage,
        ProjectionSnapshot projection)
    {
        var held = false;
        try
        {
            var incarnation = _incarnation;
            held = incarnation is not null
                && await incarnation.IsHeldAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            TerminateAmbiguous($"{stage} advisory-lock verification", projection, exception);
        }

        if (!held)
        {
            TerminateAmbiguous($"{stage} advisory-lock verification", projection, exception: null);
        }
    }

    private async Task EnsureIncarnationHeldOrTerminateAsync(string message, Exception? exception)
    {
        var held = false;
        try
        {
            var incarnation = _incarnation;
            held = incarnation is not null
                && await incarnation.IsHeldAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception lockException)
        {
            TerminateProcess(message, lockException);
        }

        if (!held)
        {
            TerminateProcess(message, exception);
        }
    }

    [DoesNotReturn]
    private void TerminateAmbiguous(
        string stage,
        ProjectionSnapshot projection,
        Exception? exception)
    {
        var message = $"Ambiguous {stage} for account {projection.AccountKey} projection version {projection.Version}; a fresh process must rebuild the Memory index.";
        TerminateProcess(message, exception);
    }

    [DoesNotReturn]
    private void TerminateProcess(string message, Exception? exception)
    {
        Volatile.Write(ref _state, Faulted);
        _terminator.Terminate(message, exception);
        throw new InvalidOperationException("The fatal-process terminator returned unexpectedly.");
    }

    private void EnsureReady()
    {
        if (!IsReady)
        {
            throw new InvalidOperationException($"Projection dispatch is unavailable while runtime status is '{Status}'.");
        }
    }

    private static void ValidateRebuildPage(
        IReadOnlyList<ProjectionSnapshot> page,
        AccountKey? previousCursor)
    {
        AccountKey? cursor = previousCursor;
        foreach (var projection in page)
        {
            ArgumentNullException.ThrowIfNull(projection);
            if (!projection.IsComplete)
            {
                throw new InvalidOperationException("The rebuild store returned an incomplete desired projection.");
            }

            if (cursor is { } previous && projection.AccountKey <= previous)
            {
                throw new InvalidOperationException("The rebuild store returned a non-canonical keyset page.");
            }

            cursor = projection.AccountKey;
        }
    }

    private static void ValidateLeaseBatch(IReadOnlyList<ProjectionOutboxLease> leases)
    {
        ArgumentNullException.ThrowIfNull(leases);
        var accounts = new HashSet<AccountKey>();
        foreach (var lease in leases)
        {
            ArgumentNullException.ThrowIfNull(lease);
            if (!accounts.Add(lease.Projection.AccountKey))
            {
                throw new InvalidOperationException(
                    "The durable store leased multiple unfinished versions for one account in one batch.");
            }
        }
    }
}

public sealed class ProjectionDispatcherAlreadyActiveException()
    : InvalidOperationException("Another projection-dispatcher incarnation already holds the PostgreSQL advisory lock.");

public sealed class ProjectionChangedDuringRebuildException(AccountKey accountKey, long projectionVersion)
    : InvalidOperationException(
        $"Desired projection {accountKey} version {projectionVersion} changed during rebuild; restart before readiness.");
