using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Thrown when a TAP delivery identifier is reused for different sanitized metadata.
/// </summary>
public sealed class TapDeliveryIdentityConflictException : InvalidOperationException
{
    public TapDeliveryIdentityConflictException(Guid sourceInstanceId, ulong deliveryId)
        : base($"TAP source {sourceInstanceId} delivery {deliveryId} was already reserved with a different sanitized digest.")
    {
        SourceInstanceId = sourceInstanceId;
        DeliveryId = deliveryId;
    }

    public Guid SourceInstanceId { get; }

    public ulong DeliveryId { get; }
}

/// <summary>
/// Thrown when a commit does not bind the exact independently persisted reservation.
/// </summary>
public sealed class TapDeliveryReservationMismatchException : InvalidOperationException
{
    public TapDeliveryReservationMismatchException(Guid sourceInstanceId, ulong deliveryId)
        : base($"TAP source {sourceInstanceId} delivery {deliveryId} does not match its exact durable reservation.")
    {
        SourceInstanceId = sourceInstanceId;
        DeliveryId = deliveryId;
    }

    public Guid SourceInstanceId { get; }

    public ulong DeliveryId { get; }
}

/// <summary>
/// Thrown when the database cannot prove a requested acknowledgement-safe stale no-op.
/// </summary>
public sealed class ValidatedNoOpProofFailedException : InvalidOperationException
{
    public ValidatedNoOpProofFailedException(ValidatedNoOpReason reason)
        : base($"The current durable state does not prove validated no-op reason {reason}.")
    {
        Reason = reason;
    }

    public ValidatedNoOpReason Reason { get; }
}

/// <summary>
/// Reserves deliveries before planning and atomically persists typed transitions and projection outbox rows.
/// </summary>
/// <remarks>
/// The source transport may acknowledge a delivery only when the returned reservation or commit result
/// allows it. The store never accepts raw AT Protocol JSON, post text, handles, or media.
/// </remarks>
public sealed class PostgreSqlIngestionStore
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlIngestionStore(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
    }

    /// <summary>
    /// Persists a Pending delivery identity before the caller reads state or plans a transition.
    /// </summary>
    public async Task<DurableDeliveryReservation> ReserveDeliveryAsync(
        DurableDeliveryReservationRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        await EnsureManifestSourceAsync(connection, transaction, request.SourceInstanceId, cancellationToken).ConfigureAwait(false);

        await using (var reserve = PostgreSqlCommands.CreateReserveDeliveryCommand(connection, transaction, request))
        {
            await reserve.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var actual = await ReadDeliveryForUpdateAsync(
            connection,
            transaction,
            request.SourceInstanceId,
            request.TapDeliveryId,
            cancellationToken).ConfigureAwait(false);
        if (!string.Equals(actual.DeliveryDigest, request.DeliveryDigest, StringComparison.Ordinal))
        {
            throw new TapDeliveryIdentityConflictException(request.SourceInstanceId, request.TapDeliveryId);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new DurableDeliveryReservation(
            request.SourceInstanceId,
            request.TapDeliveryId,
            actual.DeliveryDigest,
            actual.FirstObservedAtMinuteUtc,
            actual.Outcome);
    }

    public async Task<DurableCommitResult> CommitAsync(
        DurableDeliveryReservation reservation,
        DurableIngestionCommit commit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        ArgumentNullException.ThrowIfNull(commit);
        ValidateEnvelopeBinding(reservation, commit.Envelope);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        // The barrier can wait for a lifecycle transaction which commits after this transaction's
        // first statement. READ COMMITTED makes the subsequent durable-work check observe that
        // commit; the sorted account locks and optimistic state versions serialize all mutations.
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var actual = await LockAndValidateReservationAsync(connection, transaction, reservation, cancellationToken).ConfigureAwait(false);
        if (actual.Outcome != DurableDeliveryOutcome.Pending)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new DurableCommitResult(DurableCommitOutcome.DeliveryDuplicate, true);
        }

        var mutationAccounts = await PostgreSqlAccountTransactionBarrier.AcquireAsync(
            connection,
            transaction,
            GetMutationAccountKeys(commit),
            cancellationToken).ConfigureAwait(false);
        if (await PostgreSqlAccountTransactionBarrier.HasPendingWorkAsync(
            connection,
            transaction,
            mutationAccounts,
            cancellationToken).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new DurableCommitResult(DurableCommitOutcome.OptimisticConflict, false);
        }

        if (!await TryInsertSemanticEventAsync(connection, transaction, commit.Envelope, cancellationToken).ConfigureAwait(false))
        {
            await CompleteDeliveryAsync(
                connection,
                transaction,
                reservation,
                DurableDeliveryOutcome.SemanticDuplicate,
                commit.Envelope.SemanticDigest,
                commit.Envelope.AccountKey,
                cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new DurableCommitResult(DurableCommitOutcome.SemanticDuplicate, true);
        }

        foreach (var accountState in commit.AccountStates.OrderBy(static state => state.AccountKey))
        {
            if (!await TryReplaceAccountStateAsync(connection, transaction, accountState, cancellationToken).ConfigureAwait(false))
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new DurableCommitResult(DurableCommitOutcome.OptimisticConflict, false);
            }
        }

        foreach (var record in commit.Records
            .OrderBy(static value => value.AccountKey)
            .ThenBy(static value => value.Collection)
            .ThenBy(static value => value.RecordKey, StringComparer.Ordinal))
        {
            if (!await TryUpsertRecordAsync(connection, transaction, record, cancellationToken).ConfigureAwait(false))
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new DurableCommitResult(DurableCommitOutcome.RevisionConflict, false);
            }
        }

        foreach (var pair in commit.FollowPairs.OrderBy(static value => value.SourceAccountKey).ThenBy(static value => value.TargetAccountKey))
        {
            await ReplaceFollowPairAsync(connection, transaction, pair, cancellationToken).ConfigureAwait(false);
        }

        foreach (var activity in commit.Activity.OrderBy(static value => value.AccountKey).ThenBy(static value => value.MinuteUtc))
        {
            await AddActivityAsync(connection, transaction, activity, cancellationToken).ConfigureAwait(false);
        }

        foreach (var dependency in commit.ReconciliationDependencies
            .OrderBy(static value => value.OwnerAccountKey)
            .ThenBy(static value => value.OwnerRepositoryGeneration)
            .ThenBy(static value => value.AffectedAccountKey))
        {
            await ReplaceReconciliationDependencyAsync(connection, transaction, dependency, cancellationToken).ConfigureAwait(false);
        }

        foreach (var projection in commit.Projections.OrderBy(static value => value.AccountKey).ThenBy(static value => value.Version))
        {
            await SaveProjectionAndOutboxAsync(connection, transaction, projection, cancellationToken).ConfigureAwait(false);
        }

        await CompleteDeliveryAsync(
            connection,
            transaction,
            reservation,
            DurableDeliveryOutcome.Applied,
            commit.Envelope.SemanticDigest,
            commit.Envelope.AccountKey,
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new DurableCommitResult(DurableCommitOutcome.Applied, true);
    }

    internal static IReadOnlyList<AccountKey> GetMutationAccountKeys(DurableIngestionCommit commit)
    {
        ArgumentNullException.ThrowIfNull(commit);
        var accounts = new List<AccountKey>
        {
            commit.Envelope.AccountKey,
        };
        accounts.AddRange(commit.AccountStates.Select(static value => value.AccountKey));
        foreach (var record in commit.Records)
        {
            accounts.Add(record.AccountKey);
            if (record.TargetAccountKey is { } target)
            {
                accounts.Add(target);
            }
        }

        foreach (var pair in commit.FollowPairs)
        {
            accounts.Add(pair.SourceAccountKey);
            accounts.Add(pair.TargetAccountKey);
        }

        accounts.AddRange(commit.Activity.Select(static value => value.AccountKey));
        accounts.AddRange(commit.Projections.Select(static value => value.AccountKey));
        foreach (var dependency in commit.ReconciliationDependencies)
        {
            accounts.Add(dependency.OwnerAccountKey);
            accounts.Add(dependency.AffectedAccountKey);
        }

        return PostgreSqlAccountTransactionBarrier.Canonicalize(accounts);
    }

    public async Task<DurableCommitResult> CommitValidatedNoOpAsync(
        DurableDeliveryReservation reservation,
        DurableValidatedNoOp noOp,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        ArgumentNullException.ThrowIfNull(noOp);
        ValidateEnvelopeBinding(reservation, noOp.Envelope);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        var actual = await LockAndValidateReservationAsync(connection, transaction, reservation, cancellationToken).ConfigureAwait(false);
        if (actual.Outcome != DurableDeliveryOutcome.Pending)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new DurableCommitResult(DurableCommitOutcome.DeliveryDuplicate, true);
        }

        if (!await TryInsertSemanticEventAsync(connection, transaction, noOp.Envelope, cancellationToken).ConfigureAwait(false))
        {
            await CompleteDeliveryAsync(
                connection,
                transaction,
                reservation,
                DurableDeliveryOutcome.SemanticDuplicate,
                noOp.Envelope.SemanticDigest,
                noOp.Envelope.AccountKey,
                cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new DurableCommitResult(DurableCommitOutcome.SemanticDuplicate, true);
        }

        if (!await ProveValidatedNoOpAsync(connection, transaction, noOp, cancellationToken).ConfigureAwait(false))
        {
            throw new ValidatedNoOpProofFailedException(noOp.Reason);
        }

        await CompleteDeliveryAsync(
            connection,
            transaction,
            reservation,
            DurableDeliveryOutcome.Applied,
            noOp.Envelope.SemanticDigest,
            noOp.Envelope.AccountKey,
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new DurableCommitResult(DurableCommitOutcome.ValidatedNoOp, true);
    }

    public async Task<DurableCommitResult> CommitQuarantineAsync(
        DurableDeliveryReservation reservation,
        DurableQuarantine quarantine,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        ArgumentNullException.ThrowIfNull(quarantine);
        ValidateQuarantineBinding(reservation, quarantine);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        var actual = await LockAndValidateReservationAsync(connection, transaction, reservation, cancellationToken).ConfigureAwait(false);
        if (actual.Outcome != DurableDeliveryOutcome.Pending)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new DurableCommitResult(DurableCommitOutcome.DeliveryDuplicate, true);
        }

        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = PostgreSqlCommands.InsertQuarantineSql;
            PostgreSqlCommands.AddDeliveryIdentity(insert, quarantine.SourceInstanceId, quarantine.TapDeliveryId);
            insert.Parameters.AddWithValue("delivery_digest", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(quarantine.DeliveryDigest));
            PostgreSqlCommands.AddNullable(insert, "semantic_digest", NpgsqlDbType.Bytea, quarantine.SemanticDigest is null ? null : PostgreSqlSchema.DecodeDigest(quarantine.SemanticDigest));
            PostgreSqlCommands.AddNullable(insert, "account_key", NpgsqlDbType.Bytea, quarantine.AccountKey is { } account ? PostgreSqlSchema.EncodeAccountKey(account) : null);
            insert.Parameters.AddWithValue("observed_at_minute_utc", NpgsqlDbType.Bigint, quarantine.ObservedAtMinuteUtc);
            insert.Parameters.AddWithValue("quarantine_code", NpgsqlDbType.Text, quarantine.Code);
            insert.Parameters.AddWithValue("quarantine_message", NpgsqlDbType.Text, quarantine.Message);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await CompleteDeliveryAsync(
            connection,
            transaction,
            reservation,
            DurableDeliveryOutcome.Quarantined,
            quarantine.SemanticDigest,
            quarantine.AccountKey,
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new DurableCommitResult(DurableCommitOutcome.Quarantined, true);
    }

    private static void ValidateEnvelopeBinding(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope)
    {
        if (reservation.SourceInstanceId != envelope.SourceInstanceId
            || reservation.TapDeliveryId != envelope.TapDeliveryId
            || !string.Equals(reservation.DeliveryDigest, envelope.DeliveryDigest, StringComparison.Ordinal)
            || reservation.FirstObservedAtMinuteUtc != envelope.ObservedAtMinuteUtc)
        {
            throw new TapDeliveryReservationMismatchException(reservation.SourceInstanceId, reservation.TapDeliveryId);
        }
    }

    private static void ValidateQuarantineBinding(
        DurableDeliveryReservation reservation,
        DurableQuarantine quarantine)
    {
        if (reservation.SourceInstanceId != quarantine.SourceInstanceId
            || reservation.TapDeliveryId != quarantine.TapDeliveryId
            || !string.Equals(reservation.DeliveryDigest, quarantine.DeliveryDigest, StringComparison.Ordinal)
            || reservation.FirstObservedAtMinuteUtc != quarantine.ObservedAtMinuteUtc)
        {
            throw new TapDeliveryReservationMismatchException(reservation.SourceInstanceId, reservation.TapDeliveryId);
        }
    }

    private static async Task EnsureManifestSourceAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid sourceInstanceId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlCommands.ReadManifestSourceSql;
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (value is not Guid manifestSource || manifestSource != sourceInstanceId)
        {
            throw new InvalidOperationException("The runtime manifest is absent or bound to a different source instance.");
        }
    }

    private static async Task<DeliveryRow> LockAndValidateReservationAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        CancellationToken cancellationToken)
    {
        var actual = await ReadDeliveryForUpdateAsync(
            connection,
            transaction,
            reservation.SourceInstanceId,
            reservation.TapDeliveryId,
            cancellationToken).ConfigureAwait(false);
        if (!string.Equals(actual.DeliveryDigest, reservation.DeliveryDigest, StringComparison.Ordinal)
            || actual.FirstObservedAtMinuteUtc != reservation.FirstObservedAtMinuteUtc)
        {
            throw new TapDeliveryReservationMismatchException(reservation.SourceInstanceId, reservation.TapDeliveryId);
        }

        return actual;
    }

    private static async Task<DeliveryRow> ReadDeliveryForUpdateAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid sourceInstanceId,
        ulong deliveryId,
        CancellationToken cancellationToken)
    {
        await using var read = connection.CreateCommand();
        read.Transaction = transaction;
        read.CommandText = PostgreSqlCommands.ReadDeliverySql;
        PostgreSqlCommands.AddDeliveryIdentity(read, sourceInstanceId, deliveryId);
        await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new TapDeliveryReservationMismatchException(sourceInstanceId, deliveryId);
        }

        return new DeliveryRow(
            Convert.ToHexString(reader.GetFieldValue<byte[]>(0)).ToLowerInvariant(),
            reader.GetInt64(1),
            (DurableDeliveryOutcome)reader.GetInt16(2));
    }

    internal static async Task<bool> TryInsertSemanticEventAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlCommands.InsertSemanticEventSql;
        command.Parameters.AddWithValue("semantic_digest", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(envelope.SemanticDigest));
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(envelope.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, envelope.RepositoryGeneration);
        command.Parameters.AddWithValue("event_kind", NpgsqlDbType.Smallint, (short)envelope.EventKind);
        command.Parameters.AddWithValue("observed_at_minute_utc", NpgsqlDbType.Bigint, envelope.ObservedAtMinuteUtc);
        PostgreSqlCommands.AddNullable(command, "repository_revision", NpgsqlDbType.Text, envelope.RepositoryRevision);
        PostgreSqlCommands.AddNullable(command, "lifecycle", NpgsqlDbType.Smallint, envelope.Lifecycle is { } lifecycle ? (short)lifecycle : null);
        PostgreSqlCommands.AddNullable(command, "collection", NpgsqlDbType.Smallint, envelope.Collection is { } collection ? (short)collection : null);
        PostgreSqlCommands.AddNullable(command, "action", NpgsqlDbType.Smallint, envelope.Action is { } action ? (short)action : null);
        PostgreSqlCommands.AddNullable(command, "record_key", NpgsqlDbType.Text, envelope.RecordKey);
        PostgreSqlCommands.AddNullable(command, "cid", NpgsqlDbType.Text, envelope.Cid);
        PostgreSqlCommands.AddNullable(command, "target_account_key", NpgsqlDbType.Bytea, envelope.TargetAccountKey is { } target ? PostgreSqlSchema.EncodeAccountKey(target) : null);
        command.Parameters.AddWithValue("is_direct_reply", NpgsqlDbType.Boolean, envelope.IsDirectReply);
        command.Parameters.AddWithValue("is_live", NpgsqlDbType.Boolean, envelope.IsLive);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    private static async Task<bool> ProveValidatedNoOpAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableValidatedNoOp noOp,
        CancellationToken cancellationToken)
    {
        var envelope = noOp.Envelope;
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = noOp.Reason switch
        {
            ValidatedNoOpReason.RecordRevisionAlreadyObserved => PostgreSqlCommands.ProveRecordRevisionAlreadyObservedSql,
            ValidatedNoOpReason.RepositoryGenerationSuperseded => PostgreSqlCommands.ProveRepositoryGenerationSupersededSql,
            ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted => PostgreSqlCommands.ProveRepositorySyncRevisionAlreadyCompletedSql,
            ValidatedNoOpReason.RepositoryRevisionAlreadyApplied => PostgreSqlCommands.ProveRepositoryRevisionAlreadyAppliedSql,
            _ => throw new InvalidOperationException("The validated no-op reason was already checked by its model."),
        };
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(envelope.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, envelope.RepositoryGeneration);
        if (noOp.Reason == ValidatedNoOpReason.RecordRevisionAlreadyObserved)
        {
            command.Parameters.AddWithValue("collection", NpgsqlDbType.Smallint, (short)envelope.Collection!.Value);
            command.Parameters.AddWithValue("record_key", NpgsqlDbType.Text, envelope.RecordKey!);
        }

        if (noOp.Reason != ValidatedNoOpReason.RepositoryGenerationSuperseded)
        {
            command.Parameters.AddWithValue("repository_revision", NpgsqlDbType.Text, envelope.RepositoryRevision!);
        }

        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    internal static async Task<bool> TryReplaceAccountStateAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AccountStateMutation state,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = state.ExpectedVersion == 0
            ? PostgreSqlCommands.InsertAccountStateSql
            : PostgreSqlCommands.UpdateAccountStateSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(state.AccountKey));
        if (state.ExpectedVersion != 0)
        {
            command.Parameters.AddWithValue("expected_version", NpgsqlDbType.Bigint, state.ExpectedVersion);
        }

        command.Parameters.AddWithValue("next_version", NpgsqlDbType.Bigint, state.NextVersion);
        command.Parameters.AddWithValue("lifecycle", NpgsqlDbType.Smallint, (short)state.Lifecycle);
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, state.RepositoryGeneration);
        PostgreSqlCommands.AddNullable(command, "completed_sync_revision", NpgsqlDbType.Text, state.CompletedSyncRevision);
        PostgreSqlCommands.AddNullable(command, "last_applied_revision", NpgsqlDbType.Text, state.LastAppliedRevision);
        command.Parameters.AddWithValue("synchronization_complete", NpgsqlDbType.Boolean, state.SynchronizationComplete);
        command.Parameters.AddWithValue("last_activity_minute_utc", NpgsqlDbType.Bigint, state.LastActivityMinuteUtc);
        command.Parameters.AddWithValue("current_post_count", NpgsqlDbType.Bigint, state.CurrentPostCount);
        command.Parameters.AddWithValue("current_following_count", NpgsqlDbType.Bigint, state.CurrentFollowingCount);
        command.Parameters.AddWithValue("current_follower_count", NpgsqlDbType.Bigint, state.CurrentFollowerCount);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    private static async Task<bool> TryUpsertRecordAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RecordStateMutation record,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlCommands.UpsertRecordStateSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(record.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, record.RepositoryGeneration);
        command.Parameters.AddWithValue("collection", NpgsqlDbType.Smallint, (short)record.Collection);
        command.Parameters.AddWithValue("record_key", NpgsqlDbType.Text, record.RecordKey);
        command.Parameters.AddWithValue("latest_revision", NpgsqlDbType.Text, record.LatestRevision);
        command.Parameters.AddWithValue("is_deleted", NpgsqlDbType.Boolean, record.IsDeleted);
        PostgreSqlCommands.AddNullable(command, "cid", NpgsqlDbType.Text, record.Cid);
        PostgreSqlCommands.AddNullable(command, "target_account_key", NpgsqlDbType.Bytea, record.TargetAccountKey is { } target ? PostgreSqlSchema.EncodeAccountKey(target) : null);
        command.Parameters.AddWithValue("is_direct_reply", NpgsqlDbType.Boolean, record.IsDirectReply);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    private static async Task ReplaceFollowPairAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        FollowPairMutation pair,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = pair.Multiplicity == 0
            ? PostgreSqlCommands.DeleteFollowPairSql
            : PostgreSqlCommands.UpsertFollowPairSql;
        command.Parameters.AddWithValue("source_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(pair.SourceAccountKey));
        command.Parameters.AddWithValue("target_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(pair.TargetAccountKey));
        if (pair.Multiplicity != 0)
        {
            command.Parameters.AddWithValue("multiplicity", NpgsqlDbType.Integer, pair.Multiplicity);
        }

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task AddActivityAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ActivityMinuteDelta activity,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlCommands.AddActivitySql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(activity.AccountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, activity.RepositoryGeneration);
        command.Parameters.AddWithValue("minute_utc", NpgsqlDbType.Bigint, activity.MinuteUtc);
        command.Parameters.AddWithValue("record_creates", NpgsqlDbType.Bigint, activity.RecordCreates);
        command.Parameters.AddWithValue("record_updates", NpgsqlDbType.Bigint, activity.RecordUpdates);
        command.Parameters.AddWithValue("record_deletes", NpgsqlDbType.Bigint, activity.RecordDeletes);
        command.Parameters.AddWithValue("post_creates", NpgsqlDbType.Bigint, activity.PostCreates);
        command.Parameters.AddWithValue("received_engagement_creates", NpgsqlDbType.Bigint, activity.ReceivedEngagementCreates);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task ReplaceReconciliationDependencyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ReconciliationDependencyMutation dependency,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = dependency.Action == ReconciliationDependencyAction.Add
            ? PostgreSqlCommands.AddReconciliationDependencySql
            : PostgreSqlCommands.RemoveReconciliationDependencySql;
        command.Parameters.AddWithValue("owner_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(dependency.OwnerAccountKey));
        command.Parameters.AddWithValue("owner_repository_generation", NpgsqlDbType.Bigint, dependency.OwnerRepositoryGeneration);
        command.Parameters.AddWithValue("affected_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(dependency.AffectedAccountKey));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static async Task SaveProjectionAndOutboxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ProjectionSnapshot projection,
        CancellationToken cancellationToken)
    {
        await using (var desired = connection.CreateCommand())
        {
            desired.Transaction = transaction;
            desired.CommandText = PostgreSqlCommands.UpsertDesiredProjectionSql;
            PostgreSqlCommands.AddProjectionParameters(desired, projection);
            if (await desired.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                throw new InvalidOperationException("A desired projection did not advance monotonically.");
            }
        }

        if (projection.IsComplete)
        {
            await using var outbox = connection.CreateCommand();
            outbox.Transaction = transaction;
            outbox.CommandText = PostgreSqlCommands.InsertOutboxSql;
            PostgreSqlCommands.AddProjectionParameters(outbox, projection);
            await outbox.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var due = connection.CreateCommand();
        due.Transaction = transaction;
        if (projection.IsComplete
            && projection.Operation == ProjectionOperation.Upsert
            && projection.NextRecalculationMinuteUtc is { } dueMinute)
        {
            due.CommandText = PostgreSqlCommands.UpsertRecalculationDueSql;
            due.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(projection.AccountKey));
            due.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, projection.Version);
            due.Parameters.AddWithValue("due_minute_utc", NpgsqlDbType.Bigint, dueMinute);
        }
        else
        {
            due.CommandText = PostgreSqlCommands.DeleteRecalculationDueSql;
            due.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(projection.AccountKey));
            due.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, projection.Version);
        }

        await due.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static async Task CompleteDeliveryAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DurableDeliveryReservation reservation,
        DurableDeliveryOutcome outcome,
        string? semanticDigest,
        SkyPulse.AccountKey? accountKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlCommands.CompleteDeliverySql;
        PostgreSqlCommands.AddDeliveryIdentity(command, reservation.SourceInstanceId, reservation.TapDeliveryId);
        command.Parameters.AddWithValue("delivery_digest", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(reservation.DeliveryDigest));
        command.Parameters.AddWithValue("first_observed_at_minute_utc", NpgsqlDbType.Bigint, reservation.FirstObservedAtMinuteUtc);
        command.Parameters.AddWithValue("outcome", NpgsqlDbType.Smallint, (short)outcome);
        PostgreSqlCommands.AddNullable(command, "semantic_digest", NpgsqlDbType.Bytea, semanticDigest is null ? null : PostgreSqlSchema.DecodeDigest(semanticDigest));
        PostgreSqlCommands.AddNullable(command, "account_key", NpgsqlDbType.Bytea, accountKey is { } account ? PostgreSqlSchema.EncodeAccountKey(account) : null);
        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
        {
            throw new TapDeliveryReservationMismatchException(reservation.SourceInstanceId, reservation.TapDeliveryId);
        }
    }

    private sealed record DeliveryRow(
        string DeliveryDigest,
        long FirstObservedAtMinuteUtc,
        DurableDeliveryOutcome Outcome);
}
