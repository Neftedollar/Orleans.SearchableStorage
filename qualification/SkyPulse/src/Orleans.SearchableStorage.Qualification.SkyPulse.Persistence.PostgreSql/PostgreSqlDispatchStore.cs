using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Leases projection publications and rolling-window recalculations without violating per-account order.
/// </summary>
public sealed class PostgreSqlDispatchStore
{
    private const int MaximumBatchSize = 1_000;
    private static readonly TimeSpan MaximumLeaseDuration = TimeSpan.FromMinutes(15);
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlDispatchStore(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
    }

    /// <summary>
    /// Leases only the earliest unfinished projection version for each account.
    /// </summary>
    public async Task<IReadOnlyList<ProjectionOutboxLease>> LeaseProjectionsAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ValidateLease(batchSize, leaseDuration);
        var leaseId = Guid.NewGuid();
        var result = new List<ProjectionOutboxLease>(batchSize);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = PostgreSqlCommands.LeaseOutboxSql;
        command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, batchSize);
        command.Parameters.AddWithValue("lease_id", NpgsqlDbType.Uuid, leaseId);
        command.Parameters.AddWithValue("lease_duration", NpgsqlDbType.Interval, leaseDuration);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(new ProjectionOutboxLease(leaseId, ReadProjectionSnapshot(reader), reader.GetInt32(23)));
        }

        return result;
    }

    /// <summary>
    /// Prepares hydration for an upsert while deliberately leaving its outbox row unfinished.
    /// </summary>
    /// <remarks>
    /// After this succeeds, the caller writes the same projection to Orleans.SearchableStorage and
    /// then calls <see cref="FinalizeProjectionAsync"/>. Repeating preparation is idempotent.
    /// </remarks>
    public async Task<bool> PrepareProjectionHydrationAsync(
        ProjectionOutboxLease lease,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        if (lease.Projection.Operation != ProjectionOperation.Upsert)
        {
            throw new ArgumentException("Only an upsert prepares hydration before index publication.", nameof(lease));
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = PostgreSqlCommands.PrepareHydrationSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(lease.Projection.AccountKey));
        command.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, lease.Projection.Version);
        command.Parameters.AddWithValue("lease_id", NpgsqlDbType.Uuid, lease.LeaseId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null;
    }

    /// <summary>
    /// Finalizes an exact live lease after its external index-only operation has succeeded.
    /// </summary>
    /// <remarks>
    /// An upsert can finalize only when same-version hydration was prepared. For a removal, the
    /// caller removes the external index entry first; this transaction then retains an exact
    /// removal tombstone and completes the outbox row. A lost or expired lease returns
    /// <see langword="false"/>.
    /// </remarks>
    public async Task<bool> FinalizeProjectionAsync(
        ProjectionOutboxLease lease,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);

        if (lease.Projection.Operation == ProjectionOperation.Remove)
        {
            await using var materializeRemoval = connection.CreateCommand();
            materializeRemoval.Transaction = transaction;
            materializeRemoval.CommandText = PostgreSqlCommands.MaterializeRemovalSql;
            materializeRemoval.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(lease.Projection.AccountKey));
            materializeRemoval.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, lease.Projection.Version);
            materializeRemoval.Parameters.AddWithValue("lease_id", NpgsqlDbType.Uuid, lease.LeaseId);
            await materializeRemoval.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var complete = connection.CreateCommand();
        complete.Transaction = transaction;
        complete.CommandText = PostgreSqlCommands.CompleteOutboxSql;
        complete.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(lease.Projection.AccountKey));
        complete.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, lease.Projection.Version);
        complete.Parameters.AddWithValue("lease_id", NpgsqlDbType.Uuid, lease.LeaseId);
        var result = await complete.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (result is not true)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Releases a failed projection for a caller-selected retry instant.
    /// </summary>
    public Task<bool> FailProjectionAsync(
        ProjectionOutboxLease lease,
        DateTimeOffset availableAtUtc,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        return FailAsync(
            PostgreSqlCommands.FailOutboxSql,
            lease.Projection.AccountKey,
            lease.Projection.Version,
            "projection_version",
            lease.LeaseId,
            availableAtUtc,
            errorCode,
            errorMessage,
            cancellationToken);
    }

    /// <summary>
    /// Leases bounded rolling-window recalculations whose UTC due time has arrived and returns the
    /// authoritative PostgreSQL UTC minute from the same statement.
    /// </summary>
    public async Task<IReadOnlyList<ProjectionRecalculationLease>> LeaseRecalculationsAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ValidateLease(batchSize, leaseDuration);
        var leaseId = Guid.NewGuid();
        var result = new List<ProjectionRecalculationLease>(batchSize);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = PostgreSqlCommands.LeaseRecalculationsSql;
        command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, batchSize);
        command.Parameters.AddWithValue("lease_id", NpgsqlDbType.Uuid, leaseId);
        command.Parameters.AddWithValue("lease_duration", NpgsqlDbType.Interval, leaseDuration);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(
                new ProjectionRecalculationLease(
                    leaseId,
                    PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(0)),
                    reader.GetInt64(1),
                    reader.GetInt64(2),
                    reader.GetInt64(3),
                    reader.GetInt32(4)));
        }

        return result;
    }

    /// <summary>
    /// Atomically advances account state, writes the recalculated projection and outbox row, and
    /// completes the exact due lease.
    /// </summary>
    public async Task<bool> CommitRecalculationAsync(
        ProjectionRecalculationLease lease,
        AccountStateMutation accountState,
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(accountState);
        ArgumentNullException.ThrowIfNull(projection);
        if (accountState.AccountKey != lease.AccountKey
            || projection.AccountKey != lease.AccountKey
            || accountState.ExpectedVersion != lease.SourceProjectionVersion
            || projection.Version != accountState.NextVersion
            || !projection.IsComplete)
        {
            throw new ArgumentException("The recalculation lease, account state, and projection versions must form one monotonic transition.", nameof(projection));
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        await using (var complete = connection.CreateCommand())
        {
            complete.Transaction = transaction;
            complete.CommandText = PostgreSqlCommands.CompleteRecalculationSql;
            complete.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(lease.AccountKey));
            complete.Parameters.AddWithValue("source_projection_version", NpgsqlDbType.Bigint, lease.SourceProjectionVersion);
            complete.Parameters.AddWithValue("lease_id", NpgsqlDbType.Uuid, lease.LeaseId);
            if (await complete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }
        }

        if (!await PostgreSqlIngestionStore.TryReplaceAccountStateAsync(connection, transaction, accountState, cancellationToken).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }

        await PostgreSqlIngestionStore.SaveProjectionAndOutboxAsync(connection, transaction, projection, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Releases a failed recalculation for a caller-selected retry instant.
    /// </summary>
    public Task<bool> FailRecalculationAsync(
        ProjectionRecalculationLease lease,
        DateTimeOffset availableAtUtc,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        return FailAsync(
            PostgreSqlCommands.FailRecalculationSql,
            lease.AccountKey,
            lease.SourceProjectionVersion,
            "source_projection_version",
            lease.LeaseId,
            availableAtUtc,
            errorCode,
            errorMessage,
            cancellationToken);
    }

    private async Task<bool> FailAsync(
        string sql,
        AccountKey accountKey,
        long version,
        string versionParameterName,
        Guid leaseId,
        DateTimeOffset availableAtUtc,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken)
    {
        if (availableAtUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("The retry instant must use the UTC offset.", nameof(availableAtUtc));
        }

        Guard.RequiredBounded(errorCode, 64, nameof(errorCode));
        Guard.RequiredBounded(errorMessage, 512, nameof(errorMessage));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        command.Parameters.AddWithValue(versionParameterName, NpgsqlDbType.Bigint, version);
        command.Parameters.AddWithValue("lease_id", NpgsqlDbType.Uuid, leaseId);
        command.Parameters.AddWithValue("available_at_utc", NpgsqlDbType.TimestampTz, availableAtUtc);
        command.Parameters.AddWithValue("error_code", NpgsqlDbType.Text, errorCode);
        command.Parameters.AddWithValue("error_message", NpgsqlDbType.Text, errorMessage);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    internal static ProjectionSnapshot ReadProjectionSnapshot(NpgsqlDataReader reader)
        => new(
            PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(0)),
            reader.GetInt64(1),
            (ProjectionOperation)reader.GetInt16(2),
            reader.GetBoolean(3),
            reader.GetInt64(4),
            reader.IsDBNull(5) ? null : reader.GetInt64(5),
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
            reader.GetInt64(21),
            reader.GetInt64(22));

    private static void ValidateLease(int batchSize, TimeSpan leaseDuration)
    {
        if (batchSize is < 1 or > MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, $"The batch size must be between 1 and {MaximumBatchSize}.");
        }

        if (leaseDuration < TimeSpan.FromSeconds(1) || leaseDuration > MaximumLeaseDuration)
        {
            throw new ArgumentOutOfRangeException(nameof(leaseDuration), leaseDuration, "The lease duration must be between one second and fifteen minutes.");
        }
    }
}
