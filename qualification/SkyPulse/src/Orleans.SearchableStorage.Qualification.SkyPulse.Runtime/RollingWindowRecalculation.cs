using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

/// <summary>
/// Bounds one pass over durable rolling-window expirations.
/// </summary>
public sealed class RollingWindowRecalculationOptions
{
    public int BatchSize { get; init; } = 64;

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(5);

    public TimeSpan FailureDelay { get; init; } = TimeSpan.FromSeconds(5);

    internal void Validate()
    {
        if (BatchSize is < 1 or > DurableProjectionRuntimeOptions.MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(BatchSize),
                BatchSize,
                $"A batch size must be between 1 and {DurableProjectionRuntimeOptions.MaximumBatchSize}.");
        }

        if (LeaseDuration < TimeSpan.FromSeconds(1)
            || LeaseDuration > TimeSpan.FromMinutes(15))
        {
            throw new ArgumentOutOfRangeException(
                nameof(LeaseDuration),
                LeaseDuration,
                "The recalculation lease must be between one second and fifteen minutes.");
        }

        if (FailureDelay < TimeSpan.Zero || FailureDelay > TimeSpan.FromHours(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(FailureDelay),
                FailureDelay,
                "The recalculation retry delay must be between zero and one hour.");
        }
    }
}

/// <summary>
/// Defines the durable reads and writes around a rolling-window recalculation.
/// </summary>
public interface IRollingWindowRecalculationStore
{
    Task<IReadOnlyList<ProjectionRecalculationLease>> LeaseRecalculationsAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task<AccountStateSnapshot?> ReadAccountAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default);

    Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default);

    Task<ActivityWindowAggregateSnapshot> ReadActivityWindowAggregateAsync(
        AccountKey accountKey,
        long expectedAccountStateVersion,
        long repositoryGeneration,
        long cutMinuteUtc,
        CancellationToken cancellationToken = default);

    Task<bool> CommitRecalculationAsync(
        ProjectionRecalculationLease lease,
        AccountStateMutation accountState,
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default);

    Task<bool> FailRecalculationAsync(
        ProjectionRecalculationLease lease,
        DateTimeOffset availableAtUtc,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Adapts the PostgreSQL planning and dispatch stores to the bounded recalculation worker.
/// </summary>
public sealed class PostgreSqlRollingWindowRecalculationStore : IRollingWindowRecalculationStore
{
    private readonly PostgreSqlPlanningStore _planningStore;
    private readonly PostgreSqlDispatchStore _dispatchStore;

    public PostgreSqlRollingWindowRecalculationStore(
        PostgreSqlPlanningStore planningStore,
        PostgreSqlDispatchStore dispatchStore)
    {
        ArgumentNullException.ThrowIfNull(planningStore);
        ArgumentNullException.ThrowIfNull(dispatchStore);
        _planningStore = planningStore;
        _dispatchStore = dispatchStore;
    }

    public Task<IReadOnlyList<ProjectionRecalculationLease>> LeaseRecalculationsAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
        => _dispatchStore.LeaseRecalculationsAsync(batchSize, leaseDuration, cancellationToken);

    public Task<AccountStateSnapshot?> ReadAccountAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
        => _planningStore.ReadAccountAsync(accountKey, cancellationToken);

    public Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
        => _planningStore.ReadDesiredProjectionAsync(accountKey, cancellationToken);

    public Task<ActivityWindowAggregateSnapshot> ReadActivityWindowAggregateAsync(
        AccountKey accountKey,
        long expectedAccountStateVersion,
        long repositoryGeneration,
        long cutMinuteUtc,
        CancellationToken cancellationToken = default)
        => _planningStore.ReadActivityWindowAggregateAsync(
            accountKey,
            expectedAccountStateVersion,
            repositoryGeneration,
            cutMinuteUtc,
            cancellationToken);

    public Task<bool> CommitRecalculationAsync(
        ProjectionRecalculationLease lease,
        AccountStateMutation accountState,
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default)
        => _dispatchStore.CommitRecalculationAsync(
            lease,
            accountState,
            projection,
            cancellationToken);

    public Task<bool> FailRecalculationAsync(
        ProjectionRecalculationLease lease,
        DateTimeOffset availableAtUtc,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken = default)
        => _dispatchStore.FailRecalculationAsync(
            lease,
            availableAtUtc,
            errorCode,
            errorMessage,
            cancellationToken);
}

/// <summary>
/// Carries the one optimistic account replacement and complete desired projection produced by an
/// exact rolling-window aggregate.
/// </summary>
public sealed record RollingWindowRecalculationTransition(
    AccountStateMutation AccountState,
    ProjectionSnapshot Projection);

/// <summary>
/// Purely derives a projection-only time transition without changing account stock counters.
/// </summary>
public static class RollingWindowRecalculationPlanner
{
    public static RollingWindowRecalculationTransition Plan(
        ProjectionRecalculationLease lease,
        AccountStateSnapshot account,
        ProjectionSnapshot desiredProjection,
        ActivityWindowAggregateSnapshot aggregate,
        long cutMinuteUtc)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(account);
        ArgumentNullException.ThrowIfNull(desiredProjection);
        ArgumentNullException.ThrowIfNull(aggregate);
        ArgumentOutOfRangeException.ThrowIfNegative(cutMinuteUtc);

        if (account.AccountKey != lease.AccountKey
            || desiredProjection.AccountKey != lease.AccountKey
            || aggregate.AccountKey != lease.AccountKey)
        {
            throw new RollingWindowRecalculationEvidenceException(
                "The recalculation evidence does not belong to the leased account.");
        }

        if (account.StateVersion != lease.SourceProjectionVersion
            || desiredProjection.Version != lease.SourceProjectionVersion
            || aggregate.AccountStateVersion != lease.SourceProjectionVersion)
        {
            throw new RollingWindowRecalculationEvidenceException(
                "The recalculation evidence is not fenced by the leased source version.");
        }

        if (account.RepositoryGeneration != aggregate.RepositoryGeneration)
        {
            throw new RollingWindowRecalculationEvidenceException(
                "The activity aggregate is not fenced by the account repository generation.");
        }

        if (desiredProjection.Operation != ProjectionOperation.Upsert
            || !desiredProjection.IsComplete
            || desiredProjection.NextRecalculationMinuteUtc != lease.DueMinuteUtc)
        {
            throw new RollingWindowRecalculationEvidenceException(
                "The leased due minute is not the exact complete visible desired projection.");
        }

        if (account.Lifecycle != DurableAccountLifecycle.Active)
        {
            throw new RollingWindowRecalculationEvidenceException(
                "Only an active visible account can retain a recalculation due row.");
        }

        if (cutMinuteUtc < lease.DueMinuteUtc
            || cutMinuteUtc < desiredProjection.ProjectionCutMinuteUtc
            || aggregate.CutMinuteUtc != cutMinuteUtc)
        {
            throw new RollingWindowRecalculationEvidenceException(
                "The aggregate cut must be monotonic and must reach the leased due minute.");
        }

        if (desiredProjection.LastActivityMinuteUtc != account.LastActivityMinuteUtc
            || desiredProjection.CurrentPostCount != account.CurrentPostCount
            || desiredProjection.CurrentFollowingCount != account.CurrentFollowingCount
            || desiredProjection.CurrentFollowerCount != account.CurrentFollowerCount)
        {
            throw new RollingWindowRecalculationEvidenceException(
                "The desired projection stock fields do not match durable account state.");
        }

        var nextVersion = checked(account.StateVersion + 1);
        var accountMutation = new AccountStateMutation(
            account.AccountKey,
            account.StateVersion,
            nextVersion,
            account.Lifecycle,
            account.RepositoryGeneration,
            account.CompletedSyncRevision,
            account.SynchronizationComplete,
            account.LastActivityMinuteUtc,
            account.CurrentPostCount,
            account.CurrentFollowingCount,
            account.CurrentFollowerCount,
            account.LastAppliedRevision);
        var projection = new ProjectionSnapshot(
            account.AccountKey,
            nextVersion,
            ProjectionOperation.Upsert,
            isComplete: true,
            cutMinuteUtc,
            aggregate.NextExpiryMinuteUtc,
            account.LastActivityMinuteUtc,
            aggregate.RecordCreates.OneDay,
            aggregate.RecordCreates.SevenDays,
            aggregate.RecordCreates.ThirtyDays,
            aggregate.RecordUpdates.OneDay,
            aggregate.RecordUpdates.SevenDays,
            aggregate.RecordUpdates.ThirtyDays,
            aggregate.RecordDeletes.OneDay,
            aggregate.RecordDeletes.SevenDays,
            aggregate.RecordDeletes.ThirtyDays,
            account.CurrentPostCount,
            account.CurrentFollowingCount,
            account.CurrentFollowerCount,
            aggregate.PostCreates.OneDay,
            aggregate.PostCreates.SevenDays,
            aggregate.PostCreates.ThirtyDays,
            aggregate.ReceivedEngagementCreatesThirtyDays);
        return new RollingWindowRecalculationTransition(accountMutation, projection);
    }
}

/// <summary>
/// Reports one bounded worker pass. Every lease has exactly one terminal classification.
/// </summary>
public sealed record RollingWindowRecalculationBatchResult
{
    public RollingWindowRecalculationBatchResult(
        int leasedCount,
        int committedCount,
        int supersededCount,
        int failedCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(leasedCount);
        ArgumentOutOfRangeException.ThrowIfNegative(committedCount);
        ArgumentOutOfRangeException.ThrowIfNegative(supersededCount);
        ArgumentOutOfRangeException.ThrowIfNegative(failedCount);
        if (committedCount + supersededCount + failedCount != leasedCount)
        {
            throw new ArgumentException("Every leased recalculation must have one terminal classification.");
        }

        LeasedCount = leasedCount;
        CommittedCount = committedCount;
        SupersededCount = supersededCount;
        FailedCount = failedCount;
    }

    public int LeasedCount { get; }

    public int CommittedCount { get; }

    public int SupersededCount { get; }

    public int FailedCount { get; }
}

/// <summary>
/// Advances due rolling projections in bounded batches. PostgreSQL commits the new account state,
/// desired projection, next due row, and ordered outbox row atomically.
/// </summary>
public sealed class RollingWindowRecalculationWorker
{
    private const string FailureCode = "rolling-recalculation-failed";
    private const string FailureMessage = "Rolling-window recalculation failed before a durable commit.";
    private readonly IRollingWindowRecalculationStore _store;
    private readonly RollingWindowRecalculationOptions _options;
    private readonly TimeProvider _timeProvider;

    public RollingWindowRecalculationWorker(
        IRollingWindowRecalculationStore store,
        RollingWindowRecalculationOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        _options = options ?? new RollingWindowRecalculationOptions();
        _options.Validate();
        _store = store;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public int BatchSize => _options.BatchSize;

    /// <summary>
    /// Leases and evaluates one bounded due batch. Failures are released with a sanitized durable
    /// reason and surfaced in the result so the owning host can fail readiness closed.
    /// </summary>
    public async Task<RollingWindowRecalculationBatchResult> ProcessOnceAsync(
        CancellationToken cancellationToken = default)
    {
        var leases = await _store
            .LeaseRecalculationsAsync(
                _options.BatchSize,
                _options.LeaseDuration,
                cancellationToken)
            .ConfigureAwait(false);
        ValidateLeaseBatch(leases, _options.BatchSize);

        var committed = 0;
        var superseded = 0;
        var failed = 0;
        foreach (var lease in leases)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var outcome = await ProcessLeaseAsync(lease, cancellationToken).ConfigureAwait(false);
                if (outcome)
                {
                    committed++;
                }
                else
                {
                    superseded++;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                var released = await _store
                    .FailRecalculationAsync(
                        lease,
                        _timeProvider.GetUtcNow().ToUniversalTime() + _options.FailureDelay,
                        FailureCode,
                        FailureMessage,
                        CancellationToken.None)
                    .ConfigureAwait(false);
                var canProveSuperseded = exception is RollingWindowRecalculationEvidenceException
                    or PlanningStateChangedException;
                if (released || !canProveSuperseded)
                {
                    failed++;
                }
                else
                {
                    superseded++;
                }
            }
        }

        return new RollingWindowRecalculationBatchResult(
            leases.Count,
            committed,
            superseded,
            failed);
    }

    private async Task<bool> ProcessLeaseAsync(
        ProjectionRecalculationLease lease,
        CancellationToken cancellationToken)
    {
        var account = await _store
            .ReadAccountAsync(lease.AccountKey, cancellationToken)
            .ConfigureAwait(false)
            ?? throw new RollingWindowRecalculationEvidenceException(
                "The leased account state is missing.");
        var desired = await _store
            .ReadDesiredProjectionAsync(lease.AccountKey, cancellationToken)
            .ConfigureAwait(false)
            ?? throw new RollingWindowRecalculationEvidenceException(
                "The leased desired projection is missing.");
        var cutMinute = Math.Max(
            Math.Max(lease.EvaluationMinuteUtc, lease.DueMinuteUtc),
            desired.ProjectionCutMinuteUtc);
        var aggregate = await _store
            .ReadActivityWindowAggregateAsync(
                lease.AccountKey,
                lease.SourceProjectionVersion,
                account.RepositoryGeneration,
                cutMinute,
                cancellationToken)
            .ConfigureAwait(false);
        var transition = RollingWindowRecalculationPlanner.Plan(
            lease,
            account,
            desired,
            aggregate,
            cutMinute);
        return await _store
            .CommitRecalculationAsync(
                lease,
                transition.AccountState,
                transition.Projection,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static void ValidateLeaseBatch(
        IReadOnlyList<ProjectionRecalculationLease> leases,
        int requestedBatchSize)
    {
        ArgumentNullException.ThrowIfNull(leases);
        if (leases.Count > requestedBatchSize)
        {
            throw new InvalidOperationException("The recalculation store returned an unbounded lease batch.");
        }

        var accounts = new HashSet<AccountKey>();
        foreach (var lease in leases)
        {
            ArgumentNullException.ThrowIfNull(lease);
            if (lease.LeaseId == Guid.Empty
                || !lease.AccountKey.IsValid
                || lease.SourceProjectionVersion <= 0
                || lease.DueMinuteUtc < 0
                || lease.EvaluationMinuteUtc < lease.DueMinuteUtc
                || lease.AttemptCount < 0
                || !accounts.Add(lease.AccountKey))
            {
                throw new InvalidOperationException("The recalculation store returned an invalid or duplicate lease.");
            }
        }
    }
}

public sealed class RollingWindowRecalculationEvidenceException : InvalidOperationException
{
    public RollingWindowRecalculationEvidenceException(string message)
        : base(message)
    {
    }
}
