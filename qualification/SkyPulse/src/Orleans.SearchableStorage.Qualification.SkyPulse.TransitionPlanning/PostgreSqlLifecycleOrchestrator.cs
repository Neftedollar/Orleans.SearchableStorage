using System.Data;
using Npgsql;
using NpgsqlTypes;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

public enum LifecycleWorkPhase : short
{
    OutgoingFollows = 1,
    OwnedRecords = 2,
    OwnedActivity = 3,
    ReconciliationDependencies = 4,
}

public enum LifecycleAdvanceDisposition
{
    Pending,
    Completed,
    Retry,
}

/// <summary>
/// Reports one bounded durable lifecycle step. Only a completed result permits source acknowledgement.
/// </summary>
public sealed record LifecycleAdvanceResult
{
    private LifecycleAdvanceResult(
        LifecycleAdvanceDisposition disposition,
        bool acknowledgementAllowed,
        int processedRows,
        LifecyclePagedWorkKind? workKind,
        LifecycleWorkPhase? phase,
        string? retryMessage)
    {
        if (acknowledgementAllowed != (disposition == LifecycleAdvanceDisposition.Completed))
        {
            throw new ArgumentException("Only a completed durable lifecycle transition permits acknowledgement.", nameof(acknowledgementAllowed));
        }

        Disposition = disposition;
        AcknowledgementAllowed = acknowledgementAllowed;
        ProcessedRows = processedRows;
        WorkKind = workKind;
        Phase = phase;
        RetryMessage = retryMessage;
    }

    public LifecycleAdvanceDisposition Disposition { get; }

    public bool AcknowledgementAllowed { get; }

    public int ProcessedRows { get; }

    public LifecyclePagedWorkKind? WorkKind { get; }

    public LifecycleWorkPhase? Phase { get; }

    public string? RetryMessage { get; }

    internal static LifecycleAdvanceResult Pending(
        LifecyclePagedWorkKind kind,
        LifecycleWorkPhase phase,
        int processedRows = 0)
        => new(LifecycleAdvanceDisposition.Pending, false, processedRows, kind, phase, null);

    internal static LifecycleAdvanceResult Completed(int processedRows = 0)
        => new(LifecycleAdvanceDisposition.Completed, true, processedRows, null, null, null);

    internal static LifecycleAdvanceResult Retry(string message)
        => new(LifecycleAdvanceDisposition.Retry, false, 0, null, null, message);
}

/// <summary>
/// Advances restartable lifecycle and repository-sync transitions in bounded PostgreSQL pages.
/// </summary>
/// <remarks>
/// No raw AT Protocol frame or record content crosses this boundary. The TAP delivery stays
/// <see cref="DurableDeliveryOutcome.Pending"/> until the final page and semantic event commit in
/// the same transaction. Callers must never acknowledge a Pending or Retry result.
/// </remarks>
public sealed class PostgreSqlLifecycleOrchestrator
{
    public const int MaximumPageSize = 1_000;

    internal const string InsertWorkSql = """
        INSERT INTO skypulse.lifecycle_transition_work (
            source_instance_id, delivery_id, delivery_digest, semantic_digest, account_key,
            repository_generation, event_kind, observed_at_minute_utc, repository_revision,
            lifecycle, is_live, phase)
        VALUES (
            @source_instance_id, @delivery_id, @delivery_digest, @semantic_digest, @account_key,
            @repository_generation, @event_kind, @observed_at_minute_utc, @repository_revision,
            @lifecycle, @is_live, @phase)
        ON CONFLICT DO NOTHING;
        """;

    internal const string ReadWorkForUpdateSql = """
        SELECT delivery_digest, semantic_digest, account_key, repository_generation, event_kind,
            observed_at_minute_utc, repository_revision, lifecycle, is_live, phase
        FROM skypulse.lifecycle_transition_work
        WHERE source_instance_id = @source_instance_id
          AND delivery_id = @delivery_id
        FOR UPDATE;
        """;

    internal const string ReadWorkSql = """
        SELECT delivery_digest, semantic_digest, account_key, repository_generation, event_kind,
            observed_at_minute_utc, repository_revision, lifecycle, is_live, phase
        FROM skypulse.lifecycle_transition_work
        WHERE source_instance_id = @source_instance_id
          AND delivery_id = @delivery_id;
        """;

    internal const string ReadOutgoingFollowPageSql = """
        SELECT target_account_key
        FROM skypulse.follow_pair
        WHERE source_account_key = @account_key
        ORDER BY target_account_key
        LIMIT @page_size
        FOR UPDATE;
        """;

    internal const string ReadOutgoingFollowBarrierPageSql = """
        SELECT target_account_key
        FROM skypulse.follow_pair
        WHERE source_account_key = @account_key
        ORDER BY target_account_key
        LIMIT @page_size;
        """;

    internal const string DeleteOwnedRecordPageSql = """
        WITH page AS MATERIALIZED (
            SELECT account_key, repository_generation, collection, record_key
            FROM skypulse.record_state
            WHERE account_key = @account_key
              AND repository_generation <= @repository_generation
            ORDER BY repository_generation, collection, record_key
            LIMIT @page_size
            FOR UPDATE
        )
        DELETE FROM skypulse.record_state AS owned
        USING page
        WHERE owned.account_key = page.account_key
          AND owned.repository_generation = page.repository_generation
          AND owned.collection = page.collection
          AND owned.record_key = page.record_key;
        """;

    internal const string DeleteOwnedActivityPageSql = """
        WITH page AS MATERIALIZED (
            SELECT account_key, repository_generation, minute_utc
            FROM skypulse.activity_minute_bucket
            WHERE account_key = @account_key
              AND repository_generation <= @repository_generation
            ORDER BY repository_generation, minute_utc
            LIMIT @page_size
            FOR UPDATE
        )
        DELETE FROM skypulse.activity_minute_bucket AS owned
        USING page
        WHERE owned.account_key = page.account_key
          AND owned.repository_generation = page.repository_generation
          AND owned.minute_utc = page.minute_utc;
        """;

    internal const string ReadDependencyPageSql = """
        SELECT owner_repository_generation, affected_account_key
        FROM skypulse.reconciliation_dependency
        WHERE owner_account_key = @account_key
          AND ((@all_generations AND owner_repository_generation <= @repository_generation)
            OR (NOT @all_generations AND owner_repository_generation = @repository_generation))
        ORDER BY owner_repository_generation, affected_account_key
        LIMIT @page_size
        FOR UPDATE;
        """;

    internal const string ReadDependencyBarrierPageSql = """
        SELECT affected_account_key
        FROM skypulse.reconciliation_dependency
        WHERE owner_account_key = @account_key
          AND ((@all_generations AND owner_repository_generation <= @repository_generation)
            OR (NOT @all_generations AND owner_repository_generation = @repository_generation))
        ORDER BY owner_repository_generation, affected_account_key
        LIMIT @page_size;
        """;

    internal const string ReadProjectionAggregateSql = """
        WITH bounded AS MATERIALIZED (
            SELECT minute_utc, record_creates, record_updates, record_deletes,
                post_creates, received_engagement_creates
            FROM skypulse.activity_minute_bucket
            WHERE account_key = @account_key
              AND repository_generation = @repository_generation
              AND minute_utc BETWEEN @first_thirty_day_minute AND @cut_minute
        ), due AS (
            SELECT minute_utc + 1440 AS due_minute
            FROM bounded
            WHERE minute_utc + 1440 > @cut_minute
              AND (record_creates > 0 OR record_updates > 0 OR record_deletes > 0 OR post_creates > 0)
            UNION ALL
            SELECT minute_utc + 10080 AS due_minute
            FROM bounded
            WHERE minute_utc + 10080 > @cut_minute
              AND (record_creates > 0 OR record_updates > 0 OR record_deletes > 0 OR post_creates > 0)
            UNION ALL
            SELECT minute_utc + 43200 AS due_minute
            FROM bounded
            WHERE minute_utc + 43200 > @cut_minute
              AND (record_creates > 0 OR record_updates > 0 OR record_deletes > 0
                  OR post_creates > 0 OR received_engagement_creates > 0)
        )
        SELECT
            COALESCE(sum(record_creates) FILTER (WHERE minute_utc >= @first_one_day_minute), 0)::bigint,
            COALESCE(sum(record_creates) FILTER (WHERE minute_utc >= @first_seven_day_minute), 0)::bigint,
            COALESCE(sum(record_creates), 0)::bigint,
            COALESCE(sum(record_updates) FILTER (WHERE minute_utc >= @first_one_day_minute), 0)::bigint,
            COALESCE(sum(record_updates) FILTER (WHERE minute_utc >= @first_seven_day_minute), 0)::bigint,
            COALESCE(sum(record_updates), 0)::bigint,
            COALESCE(sum(record_deletes) FILTER (WHERE minute_utc >= @first_one_day_minute), 0)::bigint,
            COALESCE(sum(record_deletes) FILTER (WHERE minute_utc >= @first_seven_day_minute), 0)::bigint,
            COALESCE(sum(record_deletes), 0)::bigint,
            COALESCE(sum(post_creates) FILTER (WHERE minute_utc >= @first_one_day_minute), 0)::bigint,
            COALESCE(sum(post_creates) FILTER (WHERE minute_utc >= @first_seven_day_minute), 0)::bigint,
            COALESCE(sum(post_creates), 0)::bigint,
            COALESCE(sum(received_engagement_creates), 0)::bigint,
            (SELECT min(due_minute) FROM due)
        FROM bounded;
        """;

    private const string ReadDeliveryForUpdateSql = """
        SELECT delivery_digest, observed_at_minute_utc, outcome
        FROM skypulse.tap_delivery
        WHERE source_instance_id = @source_instance_id
          AND delivery_id = @delivery_id
        FOR UPDATE;
        """;

    private const string ReadAccountForUpdateSql = """
        SELECT state_version, lifecycle, repository_generation, completed_sync_revision,
            last_applied_revision, synchronization_complete, last_activity_minute_utc,
            current_post_count, current_following_count, current_follower_count
        FROM skypulse.account_state
        WHERE account_key = @account_key
        FOR UPDATE;
        """;

    private const string UpdateWorkPhaseSql = """
        UPDATE skypulse.lifecycle_transition_work
        SET phase = @phase,
            updated_at_utc = clock_timestamp()
        WHERE source_instance_id = @source_instance_id
          AND delivery_id = @delivery_id;
        """;

    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlIngestionStore _ingestion;
    private readonly PostgreSqlPlanningStore _planning;

    public PostgreSqlLifecycleOrchestrator(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        _ingestion = new PostgreSqlIngestionStore(dataSource);
        _planning = new PostgreSqlPlanningStore(dataSource);
    }

    /// <summary>
    /// Plans and durably starts a transition. A paged start always returns without ACK permission.
    /// </summary>
    public async Task<LifecycleAdvanceResult> StartAsync(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        ArgumentNullException.ThrowIfNull(envelope);
        if (!reservation.IsPending)
        {
            return LifecycleAdvanceResult.Completed();
        }

        var account = await _planning.ReadAccountAsync(envelope.AccountKey, cancellationToken).ConfigureAwait(false);
        var desired = await _planning.ReadDesiredProjectionAsync(envelope.AccountKey, cancellationToken).ConfigureAwait(false);
        var decision = LifecycleTransitionPlanner.Plan(
            new LifecycleStartPlanningInput(reservation, envelope, account, desired));
        switch (decision.Kind)
        {
            case LifecycleStartDecisionKind.ImmediateCommit:
                return FromCommit(await _ingestion.CommitAsync(
                    reservation,
                    decision.ImmediateCommit!,
                    cancellationToken).ConfigureAwait(false));
            case LifecycleStartDecisionKind.ValidatedNoOp:
                return FromCommit(await _ingestion.CommitValidatedNoOpAsync(
                    reservation,
                    decision.ValidatedNoOp!,
                    cancellationToken).ConfigureAwait(false));
            case LifecycleStartDecisionKind.Quarantine:
                return FromCommit(await _ingestion.CommitQuarantineAsync(
                    reservation,
                    decision.Quarantine!,
                    cancellationToken).ConfigureAwait(false));
            case LifecycleStartDecisionKind.Retry:
                return LifecycleAdvanceResult.Retry(decision.RetryMessage!);
            case LifecycleStartDecisionKind.DeliveryAlreadyCompleted:
                return LifecycleAdvanceResult.Completed();
            case LifecycleStartDecisionKind.StartPagedWork:
                return await StartPagedWorkAsync(
                    reservation,
                    envelope,
                    decision,
                    cancellationToken).ConfigureAwait(false);
            default:
                throw new InvalidOperationException("The lifecycle start decision is not defined.");
        }
    }

    /// <summary>
    /// Commits at most one bounded page. Repeated calls resume from PostgreSQL until completion.
    /// </summary>
    public async Task<LifecycleAdvanceResult> AdvanceAsync(
        DurableDeliveryReservation reservation,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        ValidatePageSize(pageSize);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        // READ COMMITTED is intentional here: the page-target preflight runs before potentially
        // waiting for advisory locks, and the coverage recheck must observe commits made while it
        // waited. The account advisory locks, delivery/work row locks, and exact owner fence provide
        // the serialization contract for the bounded page itself.
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var delivery = await ReadDeliveryForUpdateAsync(connection, transaction, reservation, cancellationToken).ConfigureAwait(false);
        if (delivery.Outcome != DurableDeliveryOutcome.Pending)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Completed();
        }

        var candidateWork = await ReadWorkAsync(
            connection,
            transaction,
            reservation,
            cancellationToken).ConfigureAwait(false);
        if (candidateWork is null)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry("The pending delivery has no durable lifecycle work row.");
        }

        var pageBarrierTargets = await ReadPageBarrierTargetsAsync(
            connection,
            transaction,
            candidateWork,
            pageSize,
            cancellationToken).ConfigureAwait(false);
        var barrierAccounts = await PostgreSqlAccountTransactionBarrier.AcquireAsync(
            connection,
            transaction,
            pageBarrierTargets.Prepend(candidateWork.AccountKey),
            cancellationToken).ConfigureAwait(false);
        var work = await ReadWorkForUpdateAsync(connection, transaction, reservation, cancellationToken).ConfigureAwait(false);
        if (work is null)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry("The pending delivery has no durable lifecycle work row.");
        }

        if (work != candidateWork)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry(
                "The durable lifecycle work changed before its complete account barrier was acquired.");
        }

        if (!await WorkOwnsExactAccountBarrierAsync(
            connection,
            transaction,
            work,
            cancellationToken).ConfigureAwait(false))
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry(
                "The lifecycle work no longer owns its exact account generation and synchronization barrier.");
        }

        if (!await PageBarrierStillCoversCurrentTargetsAsync(
            connection,
            transaction,
            work,
            pageSize,
            barrierAccounts,
            cancellationToken).ConfigureAwait(false))
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry(
                "The lifecycle page changed before every affected account barrier could be acquired.");
        }

        LifecycleAdvanceResult result;
        if (work.Kind == LifecyclePagedWorkKind.InactiveAccountPurge)
        {
            result = await AdvanceInactivePurgeAsync(
                connection,
                transaction,
                reservation,
                work,
                pageSize,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            result = await AdvanceRepositorySyncAsync(
                connection,
                transaction,
                reservation,
                work,
                pageSize,
                cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return result;
    }

    private async Task<LifecycleAdvanceResult> StartPagedWorkAsync(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        LifecycleStartDecision decision,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        // Match ordinary commits: a work check made after waiting for the account barrier must see
        // the transaction which previously owned that barrier.
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var delivery = await ReadDeliveryForUpdateAsync(connection, transaction, reservation, cancellationToken).ConfigureAwait(false);
        if (delivery.Outcome != DurableDeliveryOutcome.Pending)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Completed();
        }

        var barrierAccounts = await PostgreSqlAccountTransactionBarrier.AcquireAsync(
            connection,
            transaction,
            [envelope.AccountKey],
            cancellationToken).ConfigureAwait(false);

        var currentWork = await ReadWorkForUpdateAsync(connection, transaction, reservation, cancellationToken).ConfigureAwait(false);
        if (currentWork is not null)
        {
            EnsureWorkMatchesEnvelope(currentWork, envelope);
            if (!await WorkOwnsExactAccountBarrierAsync(
                connection,
                transaction,
                currentWork,
                cancellationToken).ConfigureAwait(false))
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return LifecycleAdvanceResult.Retry(
                    "The lifecycle work no longer owns its exact account generation and synchronization barrier.");
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Pending(currentWork.Kind, currentWork.Phase);
        }

        if (await PostgreSqlAccountTransactionBarrier.HasPendingWorkAsync(
            connection,
            transaction,
            barrierAccounts,
            cancellationToken).ConfigureAwait(false))
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry(
                "Another pending delivery owns this account's lifecycle transition barrier.");
        }

        if (await SemanticEventExistsAsync(connection, transaction, envelope, cancellationToken).ConfigureAwait(false))
        {
            await PostgreSqlIngestionStore.CompleteDeliveryAsync(
                connection,
                transaction,
                reservation,
                DurableDeliveryOutcome.SemanticDuplicate,
                envelope.SemanticDigest,
                envelope.AccountKey,
                cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Completed();
        }

        AccountStateMutation? repositorySyncFence = null;
        if (decision.PagedWorkKind == LifecyclePagedWorkKind.RepositorySynchronization)
        {
            var lockedState = await ReadAccountForUpdateAsync(
                connection,
                transaction,
                envelope.AccountKey,
                cancellationToken).ConfigureAwait(false);
            if (lockedState is null
                || lockedState.RepositoryGeneration != envelope.RepositoryGeneration
                || lockedState.Lifecycle != DurableAccountLifecycle.Active
                || lockedState.SynchronizationComplete)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return LifecycleAdvanceResult.Retry(
                    "The repository reconciliation barrier changed before durable work could start.");
            }

            var maximumRecordRevision = await ReadMaximumRecordRevisionAsync(
                connection,
                transaction,
                envelope.AccountKey,
                envelope.RepositoryGeneration,
                cancellationToken).ConfigureAwait(false);
            if (maximumRecordRevision is not null
                && string.CompareOrdinal(maximumRecordRevision, envelope.RepositoryRevision) > 0)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return FromCommit(await _ingestion.CommitQuarantineAsync(
                    reservation,
                    new DurableQuarantine(
                        reservation.SourceInstanceId,
                        reservation.TapDeliveryId,
                        reservation.DeliveryDigest,
                        DurableQuarantineReason.ReconciliationRevisionConflict,
                        reservation.FirstObservedAtMinuteUtc,
                        envelope.SemanticDigest,
                        envelope.AccountKey),
                    cancellationToken).ConfigureAwait(false));
            }

            repositorySyncFence = CopyState(lockedState);
        }

        if (!await TryInsertWorkAsync(connection, transaction, envelope, cancellationToken).ConfigureAwait(false))
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry(
                "Another pending delivery owns this account's lifecycle transition barrier.");
        }

        var initialState = decision.InitialAccountState ?? repositorySyncFence;
        if (initialState is { } state
            && !await PostgreSqlIngestionStore.TryReplaceAccountStateAsync(
                connection,
                transaction,
                state,
                cancellationToken).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Retry("The lifecycle account state changed before durable work could start.");
        }

        if (decision.InitialRemoval is { } removal)
        {
            await PostgreSqlIngestionStore.SaveProjectionAndOutboxAsync(
                connection,
                transaction,
                removal,
                cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return LifecycleAdvanceResult.Pending(decision.PagedWorkKind!.Value, LifecycleWorkPhase.OutgoingFollows);
    }

    private static async Task<LifecycleAdvanceResult> AdvanceInactivePurgeAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        LifecycleWorkRow work,
        int pageSize,
        CancellationToken cancellationToken)
    {
        switch (work.Phase)
        {
            case LifecycleWorkPhase.OutgoingFollows:
            {
                var processed = await DeleteOutgoingFollowPageAsync(
                    connection,
                    transaction,
                    work,
                    pageSize,
                    cancellationToken).ConfigureAwait(false);
                var phase = processed < pageSize ? LifecycleWorkPhase.OwnedRecords : work.Phase;
                if (phase != work.Phase)
                {
                    await UpdateWorkPhaseAsync(connection, transaction, reservation, phase, cancellationToken).ConfigureAwait(false);
                }

                return LifecycleAdvanceResult.Pending(work.Kind, phase, processed);
            }
            case LifecycleWorkPhase.OwnedRecords:
            {
                var processed = await DeleteOwnedPageAsync(
                    connection,
                    transaction,
                    work,
                    pageSize,
                    DeleteOwnedRecordPageSql,
                    cancellationToken).ConfigureAwait(false);
                var phase = processed < pageSize ? LifecycleWorkPhase.OwnedActivity : work.Phase;
                if (phase != work.Phase)
                {
                    await UpdateWorkPhaseAsync(connection, transaction, reservation, phase, cancellationToken).ConfigureAwait(false);
                }

                return LifecycleAdvanceResult.Pending(work.Kind, phase, processed);
            }
            case LifecycleWorkPhase.OwnedActivity:
            {
                var processed = await DeleteOwnedPageAsync(
                    connection,
                    transaction,
                    work,
                    pageSize,
                    DeleteOwnedActivityPageSql,
                    cancellationToken).ConfigureAwait(false);
                var phase = processed < pageSize ? LifecycleWorkPhase.ReconciliationDependencies : work.Phase;
                if (phase != work.Phase)
                {
                    await UpdateWorkPhaseAsync(connection, transaction, reservation, phase, cancellationToken).ConfigureAwait(false);
                }

                return LifecycleAdvanceResult.Pending(work.Kind, phase, processed);
            }
            case LifecycleWorkPhase.ReconciliationDependencies:
            {
                var processed = await DeleteDependencyPageAsync(
                    connection,
                    transaction,
                    work,
                    pageSize,
                    allGenerations: true,
                    cancellationToken).ConfigureAwait(false);
                if (processed == pageSize)
                {
                    return LifecycleAdvanceResult.Pending(work.Kind, work.Phase, processed);
                }

                await FinalizeInactivePurgeAsync(
                    connection,
                    transaction,
                    reservation,
                    work,
                    cancellationToken).ConfigureAwait(false);
                return LifecycleAdvanceResult.Completed(processed);
            }
            default:
                throw new InvalidOperationException("The inactive purge work phase is not defined.");
        }
    }

    private static async Task<LifecycleAdvanceResult> AdvanceRepositorySyncAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        LifecycleWorkRow work,
        int pageSize,
        CancellationToken cancellationToken)
    {
        if (work.Phase != LifecycleWorkPhase.OutgoingFollows
            && work.Phase != LifecycleWorkPhase.ReconciliationDependencies)
        {
            throw new InvalidOperationException("Repository synchronization has an invalid durable phase.");
        }

        if (work.Phase == LifecycleWorkPhase.OutgoingFollows)
        {
            await UpdateWorkPhaseAsync(
                connection,
                transaction,
                reservation,
                LifecycleWorkPhase.ReconciliationDependencies,
                cancellationToken).ConfigureAwait(false);
            return LifecycleAdvanceResult.Pending(
                work.Kind,
                LifecycleWorkPhase.ReconciliationDependencies);
        }

        var processed = await DeleteDependencyPageAsync(
            connection,
            transaction,
            work,
            pageSize,
            allGenerations: false,
            cancellationToken).ConfigureAwait(false);
        if (processed == pageSize)
        {
            return LifecycleAdvanceResult.Pending(work.Kind, work.Phase, processed);
        }

        await FinalizeRepositorySyncAsync(
            connection,
            transaction,
            reservation,
            work,
            cancellationToken).ConfigureAwait(false);
        return LifecycleAdvanceResult.Completed(processed);
    }

    private static LifecycleAdvanceResult FromCommit(DurableCommitResult result)
        => result.AcknowledgementAllowed
            ? LifecycleAdvanceResult.Completed()
            : LifecycleAdvanceResult.Retry($"The durable commit returned {result.Outcome}.");

    private static void ValidatePageSize(int pageSize)
    {
        if (pageSize is < 1 or > MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                pageSize,
                $"The lifecycle page size must be between 1 and {MaximumPageSize}.");
        }
    }

    private static async Task<DeliveryRow> ReadDeliveryForUpdateAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReadDeliveryForUpdateSql;
        PostgreSqlCommands.AddDeliveryIdentity(command, reservation.SourceInstanceId, reservation.TapDeliveryId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new TapDeliveryReservationMismatchException(reservation.SourceInstanceId, reservation.TapDeliveryId);
        }

        var digest = Convert.ToHexString(reader.GetFieldValue<byte[]>(0)).ToLowerInvariant();
        var observedMinute = reader.GetInt64(1);
        if (!string.Equals(digest, reservation.DeliveryDigest, StringComparison.Ordinal)
            || observedMinute != reservation.FirstObservedAtMinuteUtc)
        {
            throw new TapDeliveryReservationMismatchException(reservation.SourceInstanceId, reservation.TapDeliveryId);
        }

        return new DeliveryRow((DurableDeliveryOutcome)reader.GetInt16(2));
    }

    private static async Task<LifecycleWorkRow?> ReadWorkForUpdateAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        CancellationToken cancellationToken)
        => await ReadWorkRowAsync(
            connection,
            transaction,
            reservation,
            ReadWorkForUpdateSql,
            cancellationToken).ConfigureAwait(false);

    private static async Task<LifecycleWorkRow?> ReadWorkAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        CancellationToken cancellationToken)
        => await ReadWorkRowAsync(
            connection,
            transaction,
            reservation,
            ReadWorkSql,
            cancellationToken).ConfigureAwait(false);

    private static async Task<LifecycleWorkRow?> ReadWorkRowAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        PostgreSqlCommands.AddDeliveryIdentity(command, reservation.SourceInstanceId, reservation.TapDeliveryId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var eventKind = (DurableEventKind)reader.GetInt16(4);
        return new LifecycleWorkRow(
            Convert.ToHexString(reader.GetFieldValue<byte[]>(0)).ToLowerInvariant(),
            Convert.ToHexString(reader.GetFieldValue<byte[]>(1)).ToLowerInvariant(),
            PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(2)),
            reader.GetInt64(3),
            eventKind,
            reader.GetInt64(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : (DurableAccountLifecycle)reader.GetInt16(7),
            reader.GetBoolean(8),
            (LifecycleWorkPhase)reader.GetInt16(9));
    }

    private static async Task<IReadOnlyList<AccountKey>> ReadPageBarrierTargetsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        LifecycleWorkRow work,
        int pageSize,
        CancellationToken cancellationToken)
    {
        if (work.Kind == LifecyclePagedWorkKind.InactiveAccountPurge
            && work.Phase == LifecycleWorkPhase.OutgoingFollows)
        {
            return await ReadOutgoingFollowBarrierTargetsAsync(
                connection,
                transaction,
                work.AccountKey,
                pageSize,
                cancellationToken).ConfigureAwait(false);
        }

        if (work.Phase == LifecycleWorkPhase.ReconciliationDependencies)
        {
            return await ReadDependencyBarrierTargetsAsync(
                connection,
                transaction,
                work,
                pageSize,
                allGenerations: work.Kind == LifecyclePagedWorkKind.InactiveAccountPurge,
                cancellationToken).ConfigureAwait(false);
        }

        return [];
    }

    private static async Task<bool> PageBarrierStillCoversCurrentTargetsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        LifecycleWorkRow work,
        int pageSize,
        IReadOnlyList<AccountKey> barrierAccounts,
        CancellationToken cancellationToken)
    {
        var currentTargets = await ReadPageBarrierTargetsAsync(
            connection,
            transaction,
            work,
            pageSize,
            cancellationToken).ConfigureAwait(false);
        var barriers = barrierAccounts.ToHashSet();
        return currentTargets.All(barriers.Contains);
    }

    private static async Task<IReadOnlyList<AccountKey>> ReadOutgoingFollowBarrierTargetsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountKey accountKey,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var targets = new List<AccountKey>(pageSize);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReadOutgoingFollowBarrierPageSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        command.Parameters.AddWithValue("page_size", NpgsqlDbType.Integer, pageSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            targets.Add(PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(0)));
        }

        return targets;
    }

    private static async Task<IReadOnlyList<AccountKey>> ReadDependencyBarrierTargetsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        LifecycleWorkRow work,
        int pageSize,
        bool allGenerations,
        CancellationToken cancellationToken)
    {
        var targets = new List<AccountKey>(pageSize);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReadDependencyBarrierPageSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(work.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, work.RepositoryGeneration);
        command.Parameters.AddWithValue("all_generations", NpgsqlDbType.Boolean, allGenerations);
        command.Parameters.AddWithValue("page_size", NpgsqlDbType.Integer, pageSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            targets.Add(PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(0)));
        }

        return targets;
    }

    private static async Task<bool> WorkOwnsExactAccountBarrierAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        LifecycleWorkRow work,
        CancellationToken cancellationToken)
    {
        var state = await ReadAccountForUpdateAsync(
            connection,
            transaction,
            work.AccountKey,
            cancellationToken).ConfigureAwait(false);
        if (state is null
            || state.RepositoryGeneration != work.RepositoryGeneration
            || state.SynchronizationComplete)
        {
            return false;
        }

        return work.Kind switch
        {
            LifecyclePagedWorkKind.InactiveAccountPurge => work.Lifecycle is { } lifecycle
                && lifecycle != DurableAccountLifecycle.Active
                && state.Lifecycle == lifecycle,
            LifecyclePagedWorkKind.RepositorySynchronization => work.Lifecycle is null
                && state.Lifecycle == DurableAccountLifecycle.Active,
            _ => false,
        };
    }

    private static void EnsureWorkMatchesEnvelope(LifecycleWorkRow work, DurableEventEnvelope envelope)
    {
        if (!string.Equals(work.DeliveryDigest, envelope.DeliveryDigest, StringComparison.Ordinal)
            || !string.Equals(work.SemanticDigest, envelope.SemanticDigest, StringComparison.Ordinal)
            || work.AccountKey != envelope.AccountKey
            || work.RepositoryGeneration != envelope.RepositoryGeneration
            || work.EventKind != envelope.EventKind
            || work.ObservedAtMinuteUtc != envelope.ObservedAtMinuteUtc
            || !string.Equals(work.RepositoryRevision, envelope.RepositoryRevision, StringComparison.Ordinal)
            || work.Lifecycle != envelope.Lifecycle
            || work.IsLive != envelope.IsLive)
        {
            throw new InvalidOperationException("The durable lifecycle work row does not match its sanitized event envelope.");
        }
    }

    private static async Task<bool> TryInsertWorkAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = InsertWorkSql;
        PostgreSqlCommands.AddDeliveryIdentity(command, envelope.SourceInstanceId, envelope.TapDeliveryId);
        command.Parameters.AddWithValue("delivery_digest", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(envelope.DeliveryDigest));
        command.Parameters.AddWithValue("semantic_digest", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(envelope.SemanticDigest));
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(envelope.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, envelope.RepositoryGeneration);
        command.Parameters.AddWithValue("event_kind", NpgsqlDbType.Smallint, (short)envelope.EventKind);
        command.Parameters.AddWithValue("observed_at_minute_utc", NpgsqlDbType.Bigint, envelope.ObservedAtMinuteUtc);
        PostgreSqlCommands.AddNullable(command, "repository_revision", NpgsqlDbType.Text, envelope.RepositoryRevision);
        PostgreSqlCommands.AddNullable(
            command,
            "lifecycle",
            NpgsqlDbType.Smallint,
            envelope.Lifecycle is { } lifecycle ? (short)lifecycle : null);
        command.Parameters.AddWithValue("is_live", NpgsqlDbType.Boolean, envelope.IsLive);
        command.Parameters.AddWithValue("phase", NpgsqlDbType.Smallint, (short)LifecycleWorkPhase.OutgoingFollows);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    private static async Task<bool> SemanticEventExistsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT EXISTS (
                SELECT 1
                FROM skypulse.semantic_event
                WHERE account_key = @account_key
                  AND repository_generation = @repository_generation
                  AND semantic_digest = @semantic_digest);
            """;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(envelope.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, envelope.RepositoryGeneration);
        command.Parameters.AddWithValue("semantic_digest", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(envelope.SemanticDigest));
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    private static async Task<int> DeleteOutgoingFollowPageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        LifecycleWorkRow work,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var targets = new List<AccountKey>(pageSize);
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = ReadOutgoingFollowPageSql;
            read.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(work.AccountKey));
            read.Parameters.AddWithValue("page_size", NpgsqlDbType.Integer, pageSize);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                targets.Add(PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(0)));
            }
        }

        foreach (var target in targets)
        {
            var targetState = await ReadAccountForUpdateAsync(connection, transaction, target, cancellationToken).ConfigureAwait(false);
            await using var delete = connection.CreateCommand();
            delete.Transaction = transaction;
            delete.CommandText = """
                DELETE FROM skypulse.follow_pair
                WHERE source_account_key = @source_account_key
                  AND target_account_key = @target_account_key;
                """;
            delete.Parameters.AddWithValue("source_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(work.AccountKey));
            delete.Parameters.AddWithValue("target_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(target));
            if (await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                throw new InvalidOperationException("An outgoing follow page changed inside its locked transaction.");
            }

            if (targetState is null)
            {
                continue;
            }

            if (targetState.CurrentFollowerCount == 0)
            {
                throw new InvalidOperationException("Deleting a durable follow pair would underflow its target follower count.");
            }

            var mutation = CopyState(
                targetState,
                currentFollowerCount: targetState.CurrentFollowerCount - 1);
            if (!await PostgreSqlIngestionStore.TryReplaceAccountStateAsync(
                connection,
                transaction,
                mutation,
                cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException("A locked follower target failed its optimistic replacement.");
            }

            if (target != work.AccountKey)
            {
                await SaveProjectionForCurrentStateAsync(
                    connection,
                    transaction,
                    Snapshot(mutation),
                    work.ObservedAtMinuteUtc,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        return targets.Count;
    }

    private static async Task<int> DeleteOwnedPageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        LifecycleWorkRow work,
        int pageSize,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(work.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, work.RepositoryGeneration);
        command.Parameters.AddWithValue("page_size", NpgsqlDbType.Integer, pageSize);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<int> DeleteDependencyPageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        LifecycleWorkRow work,
        int pageSize,
        bool allGenerations,
        CancellationToken cancellationToken)
    {
        var dependencies = new List<DependencyAddress>(pageSize);
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = ReadDependencyPageSql;
            read.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(work.AccountKey));
            read.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, work.RepositoryGeneration);
            read.Parameters.AddWithValue("all_generations", NpgsqlDbType.Boolean, allGenerations);
            read.Parameters.AddWithValue("page_size", NpgsqlDbType.Integer, pageSize);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                dependencies.Add(new DependencyAddress(
                    reader.GetInt64(0),
                    PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(1))));
            }
        }

        foreach (var dependency in dependencies)
        {
            await using var delete = connection.CreateCommand();
            delete.Transaction = transaction;
            delete.CommandText = """
                DELETE FROM skypulse.reconciliation_dependency
                WHERE owner_account_key = @owner_account_key
                  AND owner_repository_generation = @owner_repository_generation
                  AND affected_account_key = @affected_account_key;
                """;
            delete.Parameters.AddWithValue("owner_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(work.AccountKey));
            delete.Parameters.AddWithValue("owner_repository_generation", NpgsqlDbType.Bigint, dependency.RepositoryGeneration);
            delete.Parameters.AddWithValue("affected_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(dependency.AffectedAccountKey));
            if (await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                throw new InvalidOperationException("A reconciliation dependency page changed inside its locked transaction.");
            }

            if (dependency.AffectedAccountKey == work.AccountKey
                || await IsProjectionBlockedAsync(
                    connection,
                    transaction,
                    dependency.AffectedAccountKey,
                    cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            var state = await ReadAccountForUpdateAsync(
                connection,
                transaction,
                dependency.AffectedAccountKey,
                cancellationToken).ConfigureAwait(false);
            if (state is null)
            {
                continue;
            }

            await RefreshWithNewVersionAsync(
                connection,
                transaction,
                state,
                work.ObservedAtMinuteUtc,
                cancellationToken).ConfigureAwait(false);
        }

        return dependencies.Count;
    }

    private static async Task FinalizeInactivePurgeAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        LifecycleWorkRow work,
        CancellationToken cancellationToken)
    {
        var state = await ReadAccountForUpdateAsync(connection, transaction, work.AccountKey, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("An inactive purge lost its durable account state.");
        if (state.RepositoryGeneration != work.RepositoryGeneration
            || state.Lifecycle != work.Lifecycle
            || state.Lifecycle == DurableAccountLifecycle.Active)
        {
            throw new InvalidOperationException("An inactive purge no longer owns the exact lifecycle generation.");
        }

        var finalState = CopyState(
            state,
            lastActivityMinuteUtc: 0,
            currentPostCount: 0,
            currentFollowingCount: 0);
        if (!await PostgreSqlIngestionStore.TryReplaceAccountStateAsync(
            connection,
            transaction,
            finalState,
            cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("The final inactive account replacement failed its durable guard.");
        }

        await CompleteWorkAsync(connection, transaction, reservation, work, cancellationToken).ConfigureAwait(false);
    }

    private static async Task FinalizeRepositorySyncAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        LifecycleWorkRow work,
        CancellationToken cancellationToken)
    {
        var revision = work.RepositoryRevision
            ?? throw new InvalidOperationException("Repository-sync work must retain its revision.");
        var state = await ReadAccountForUpdateAsync(connection, transaction, work.AccountKey, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("Repository synchronization lost its durable account state.");
        if (state.RepositoryGeneration != work.RepositoryGeneration
            || state.Lifecycle != DurableAccountLifecycle.Active
            || state.SynchronizationComplete)
        {
            throw new InvalidOperationException("Repository synchronization no longer owns the exact open generation.");
        }

        var maximumRecordRevision = await ReadMaximumRecordRevisionAsync(
            connection,
            transaction,
            work.AccountKey,
            work.RepositoryGeneration,
            cancellationToken).ConfigureAwait(false);
        if (maximumRecordRevision is not null
            && string.CompareOrdinal(maximumRecordRevision, revision) > 0)
        {
            throw new InvalidOperationException("The repository-sync revision precedes a stored historical record revision.");
        }

        if (await HasOwnedDependenciesAsync(
            connection,
            transaction,
            work.AccountKey,
            work.RepositoryGeneration,
            cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("Repository synchronization cannot close before every dependency page commits.");
        }

        var finalState = new AccountStateMutation(
            state.AccountKey,
            state.StateVersion,
            checked(state.StateVersion + 1),
            state.Lifecycle,
            state.RepositoryGeneration,
            revision,
            synchronizationComplete: true,
            state.LastActivityMinuteUtc,
            state.CurrentPostCount,
            state.CurrentFollowingCount,
            state.CurrentFollowerCount,
            lastAppliedRevision: revision);
        if (!await PostgreSqlIngestionStore.TryReplaceAccountStateAsync(
            connection,
            transaction,
            finalState,
            cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("The final repository-sync account replacement failed its durable guard.");
        }

        await SaveProjectionForCurrentStateAsync(
            connection,
            transaction,
            Snapshot(finalState),
            work.ObservedAtMinuteUtc,
            cancellationToken).ConfigureAwait(false);
        await CompleteWorkAsync(connection, transaction, reservation, work, cancellationToken).ConfigureAwait(false);
    }

    private static async Task CompleteWorkAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        LifecycleWorkRow work,
        CancellationToken cancellationToken)
    {
        var envelope = work.ToEnvelope(reservation.SourceInstanceId, reservation.TapDeliveryId);
        if (!await PostgreSqlIngestionStore.TryInsertSemanticEventAsync(
            connection,
            transaction,
            envelope,
            cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("A paged lifecycle transition lost its unique semantic-event ownership.");
        }

        await using (var delete = connection.CreateCommand())
        {
            delete.Transaction = transaction;
            delete.CommandText = """
                DELETE FROM skypulse.lifecycle_transition_work
                WHERE source_instance_id = @source_instance_id
                  AND delivery_id = @delivery_id;
                """;
            PostgreSqlCommands.AddDeliveryIdentity(delete, reservation.SourceInstanceId, reservation.TapDeliveryId);
            if (await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                throw new InvalidOperationException("The final lifecycle transaction lost its durable work row.");
            }
        }

        await PostgreSqlIngestionStore.CompleteDeliveryAsync(
            connection,
            transaction,
            reservation,
            DurableDeliveryOutcome.Applied,
            work.SemanticDigest,
            work.AccountKey,
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task RefreshWithNewVersionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountStateSnapshot state,
        long cutMinuteUtc,
        CancellationToken cancellationToken)
    {
        var desired = await ReadDesiredProjectionAsync(connection, transaction, state.AccountKey, cancellationToken).ConfigureAwait(false);
        var visible = state.Lifecycle == DurableAccountLifecycle.Active && state.SynchronizationComplete;
        if (!visible && desired is not { Operation: ProjectionOperation.Upsert })
        {
            return;
        }

        var mutation = CopyState(state);
        if (!await PostgreSqlIngestionStore.TryReplaceAccountStateAsync(
            connection,
            transaction,
            mutation,
            cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("A locked reconciliation target failed its version advance.");
        }

        await SaveProjectionForCurrentStateAsync(
            connection,
            transaction,
            Snapshot(mutation),
            cutMinuteUtc,
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task SaveProjectionForCurrentStateAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountStateSnapshot state,
        long requestedCutMinuteUtc,
        CancellationToken cancellationToken)
    {
        var desired = await ReadDesiredProjectionAsync(connection, transaction, state.AccountKey, cancellationToken).ConfigureAwait(false);
        if (desired is not null && desired.Version >= state.StateVersion)
        {
            return;
        }

        var blocked = await IsProjectionBlockedAsync(connection, transaction, state.AccountKey, cancellationToken).ConfigureAwait(false);
        ProjectionSnapshot? projection = null;
        if (state.Lifecycle == DurableAccountLifecycle.Active && state.SynchronizationComplete && !blocked)
        {
            var cutMinute = Math.Max(requestedCutMinuteUtc, desired?.ProjectionCutMinuteUtc ?? 0);
            projection = await BuildProjectionAsync(
                connection,
                transaction,
                state,
                cutMinute,
                cancellationToken).ConfigureAwait(false);
        }
        else if (desired is { Operation: ProjectionOperation.Upsert } current)
        {
            projection = CopyRemoval(current, state.StateVersion, requestedCutMinuteUtc);
        }

        if (projection is not null)
        {
            await PostgreSqlIngestionStore.SaveProjectionAndOutboxAsync(
                connection,
                transaction,
                projection,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task<ProjectionSnapshot> BuildProjectionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountStateSnapshot state,
        long cutMinuteUtc,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReadProjectionAggregateSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(state.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, state.RepositoryGeneration);
        command.Parameters.AddWithValue("cut_minute", NpgsqlDbType.Bigint, cutMinuteUtc);
        command.Parameters.AddWithValue("first_one_day_minute", NpgsqlDbType.Bigint, Math.Max(0, cutMinuteUtc - 1_439));
        command.Parameters.AddWithValue("first_seven_day_minute", NpgsqlDbType.Bigint, Math.Max(0, cutMinuteUtc - 10_079));
        command.Parameters.AddWithValue("first_thirty_day_minute", NpgsqlDbType.Bigint, Math.Max(0, cutMinuteUtc - 43_199));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("PostgreSQL did not return the projection aggregate row.");
        }

        return new ProjectionSnapshot(
            state.AccountKey,
            state.StateVersion,
            ProjectionOperation.Upsert,
            isComplete: true,
            cutMinuteUtc,
            reader.IsDBNull(13) ? null : reader.GetInt64(13),
            state.LastActivityMinuteUtc,
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetInt64(2),
            reader.GetInt64(3),
            reader.GetInt64(4),
            reader.GetInt64(5),
            reader.GetInt64(6),
            reader.GetInt64(7),
            reader.GetInt64(8),
            state.CurrentPostCount,
            state.CurrentFollowingCount,
            state.CurrentFollowerCount,
            reader.GetInt64(9),
            reader.GetInt64(10),
            reader.GetInt64(11),
            reader.GetInt64(12));
    }

    private static ProjectionSnapshot CopyRemoval(
        ProjectionSnapshot desired,
        long version,
        long requestedCutMinuteUtc)
        => new(
            desired.AccountKey,
            version,
            ProjectionOperation.Remove,
            isComplete: true,
            Math.Max(desired.ProjectionCutMinuteUtc, requestedCutMinuteUtc),
            nextRecalculationMinuteUtc: null,
            desired.LastActivityMinuteUtc,
            desired.CreatedRecordCount1Day,
            desired.CreatedRecordCount7Days,
            desired.CreatedRecordCount30Days,
            desired.UpdatedRecordCount1Day,
            desired.UpdatedRecordCount7Days,
            desired.UpdatedRecordCount30Days,
            desired.DeletedRecordCount1Day,
            desired.DeletedRecordCount7Days,
            desired.DeletedRecordCount30Days,
            desired.CurrentPostCount,
            desired.CurrentFollowingCount,
            desired.CurrentFollowerCount,
            desired.PostCreates1Day,
            desired.PostCreates7Days,
            desired.PostCreates30Days,
            desired.ReceivedEngagementCreates30Days);

    private static async Task<AccountStateSnapshot?> ReadAccountForUpdateAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountKey accountKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReadAccountForUpdateSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return new AccountStateSnapshot(
            accountKey,
            reader.GetInt64(0),
            (DurableAccountLifecycle)reader.GetInt16(1),
            reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.GetBoolean(5),
            reader.GetInt64(6),
            reader.GetInt64(7),
            reader.GetInt64(8),
            reader.GetInt64(9),
            reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    private static async Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountKey accountKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlPlanningStore.ReadDesiredProjectionSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return new ProjectionSnapshot(
            accountKey,
            reader.GetInt64(0),
            (ProjectionOperation)reader.GetInt16(1),
            reader.GetBoolean(2),
            reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4),
            reader.GetInt64(5),
            reader.GetInt64(6),
            reader.GetInt64(7),
            reader.GetInt64(8),
            reader.GetInt64(9),
            reader.GetInt64(10),
            reader.GetInt64(11),
            reader.GetInt64(12),
            reader.GetInt64(13),
            reader.GetInt64(14),
            reader.GetInt64(15),
            reader.GetInt64(16),
            reader.GetInt64(17),
            reader.GetInt64(18),
            reader.GetInt64(19),
            reader.GetInt64(20),
            reader.GetInt64(21));
    }

    private static async Task<bool> IsProjectionBlockedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountKey accountKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT EXISTS (
                SELECT 1
                FROM skypulse.reconciliation_dependency
                WHERE affected_account_key = @account_key);
            """;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    private static async Task<bool> HasOwnedDependenciesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountKey accountKey,
        long repositoryGeneration,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT EXISTS (
                SELECT 1
                FROM skypulse.reconciliation_dependency
                WHERE owner_account_key = @account_key
                  AND owner_repository_generation = @repository_generation);
            """;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, repositoryGeneration);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    private static async Task<string?> ReadMaximumRecordRevisionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountKey accountKey,
        long repositoryGeneration,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT latest_revision
            FROM skypulse.record_state
            WHERE account_key = @account_key
              AND repository_generation = @repository_generation
            ORDER BY convert_to(latest_revision, 'UTF8') DESC
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, repositoryGeneration);
        return (string?)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task UpdateWorkPhaseAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        LifecycleWorkPhase phase,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = UpdateWorkPhaseSql;
        PostgreSqlCommands.AddDeliveryIdentity(command, reservation.SourceInstanceId, reservation.TapDeliveryId);
        command.Parameters.AddWithValue("phase", NpgsqlDbType.Smallint, (short)phase);
        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
        {
            throw new InvalidOperationException("The lifecycle work phase could not be advanced.");
        }
    }

    private static AccountStateMutation CopyState(
        AccountStateSnapshot state,
        long? lastActivityMinuteUtc = null,
        long? currentPostCount = null,
        long? currentFollowingCount = null,
        long? currentFollowerCount = null)
        => new(
            state.AccountKey,
            state.StateVersion,
            checked(state.StateVersion + 1),
            state.Lifecycle,
            state.RepositoryGeneration,
            state.CompletedSyncRevision,
            state.SynchronizationComplete,
            lastActivityMinuteUtc ?? state.LastActivityMinuteUtc,
            currentPostCount ?? state.CurrentPostCount,
            currentFollowingCount ?? state.CurrentFollowingCount,
            currentFollowerCount ?? state.CurrentFollowerCount,
            state.LastAppliedRevision);

    private static AccountStateSnapshot Snapshot(AccountStateMutation mutation)
        => new(
            mutation.AccountKey,
            mutation.NextVersion,
            mutation.Lifecycle,
            mutation.RepositoryGeneration,
            mutation.CompletedSyncRevision,
            mutation.SynchronizationComplete,
            mutation.LastActivityMinuteUtc,
            mutation.CurrentPostCount,
            mutation.CurrentFollowingCount,
            mutation.CurrentFollowerCount,
            mutation.LastAppliedRevision);

    private sealed record DeliveryRow(DurableDeliveryOutcome Outcome);

    private sealed record DependencyAddress(long RepositoryGeneration, AccountKey AffectedAccountKey);

    private sealed record LifecycleWorkRow(
        string DeliveryDigest,
        string SemanticDigest,
        AccountKey AccountKey,
        long RepositoryGeneration,
        DurableEventKind EventKind,
        long ObservedAtMinuteUtc,
        string? RepositoryRevision,
        DurableAccountLifecycle? Lifecycle,
        bool IsLive,
        LifecycleWorkPhase Phase)
    {
        internal LifecyclePagedWorkKind Kind => EventKind == DurableEventKind.RepositorySync
            ? LifecyclePagedWorkKind.RepositorySynchronization
            : LifecyclePagedWorkKind.InactiveAccountPurge;

        internal DurableEventEnvelope ToEnvelope(Guid sourceInstanceId, ulong tapDeliveryId)
            => new(
                sourceInstanceId,
                tapDeliveryId,
                DeliveryDigest,
                SemanticDigest,
                AccountKey,
                RepositoryGeneration,
                EventKind,
                ObservedAtMinuteUtc,
                RepositoryRevision,
                lifecycle: Lifecycle,
                isLive: IsLive);
    }
}
