using System.Collections.ObjectModel;
using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Owns the PostgreSQL side of the single-dispatcher index rebuild and hydration boundary.
/// </summary>
public sealed class PostgreSqlProjectionRuntimeStore
{
    internal const long DispatcherAdvisoryLockKey = 1_561_301_132_475_359_920L;
    internal const int MaximumPageSize = 1_000;
    internal const string TryAcquireDispatcherSql = "SELECT pg_try_advisory_lock(@lock_key);";
    internal const string ReleaseDispatcherSql = "SELECT pg_advisory_unlock(@lock_key);";
    internal const string IsDispatcherLockHeldSql = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_locks
            WHERE locktype = 'advisory'
              AND pid = pg_backend_pid()
              AND granted
              AND mode = 'ExclusiveLock'
              AND classid::bigint = ((@lock_key >> 32) & 4294967295)
              AND objid::bigint = (@lock_key & 4294967295)
              AND objsubid = 1);
        """;

    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlProjectionRuntimeStore(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
    }

    /// <summary>
    /// Tries to reserve the one allowed projection-dispatcher incarnation for this database.
    /// The returned session must remain undisposed for the complete process incarnation.
    /// </summary>
    public async Task<PostgreSqlDispatcherIncarnationLock?> TryAcquireDispatcherIncarnationAsync(
        CancellationToken cancellationToken = default)
    {
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = TryAcquireDispatcherSql;
            command.Parameters.AddWithValue("lock_key", NpgsqlDbType.Bigint, DispatcherAdvisoryLockKey);
            var acquired = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (acquired is not true)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                return null;
            }

            return new PostgreSqlDispatcherIncarnationLock(connection);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Reads one canonical keyset page of complete current desired projections.
    /// </summary>
    public async Task<IReadOnlyList<ProjectionSnapshot>> ReadDesiredProjectionPageAsync(
        AccountKey? afterAccountKeyExclusive,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        ValidateBatchSize(batchSize);
        if (afterAccountKeyExclusive is { IsValid: false })
        {
            throw new ArgumentException("A valid account-key cursor is required when supplied.", nameof(afterAccountKeyExclusive));
        }

        var result = new List<ProjectionSnapshot>(batchSize);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = afterAccountKeyExclusive is null
            ? PostgreSqlCommands.ReadDesiredProjectionFirstPageSql
            : PostgreSqlCommands.ReadDesiredProjectionNextPageSql;
        command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, batchSize);
        if (afterAccountKeyExclusive is { } cursor)
        {
            command.Parameters.AddWithValue(
                "after_account_key",
                NpgsqlDbType.Bytea,
                PostgreSqlSchema.EncodeAccountKey(cursor));
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(PostgreSqlDispatchStore.ReadProjectionSnapshot(reader));
        }

        return result;
    }

    /// <summary>
    /// Materializes the exact current desired projection as published hydration.
    /// </summary>
    /// <remarks>
    /// Rebuild calls this before an external upsert and after an external removal. A false result
    /// means the desired version or operation changed before the materialization boundary.
    /// </remarks>
    public async Task<bool> MaterializeDesiredProjectionAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(projection);
        if (!projection.IsComplete)
        {
            throw new ArgumentException("Only a complete desired projection can be materialized.", nameof(projection));
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = PostgreSqlCommands.MaterializeDesiredProjectionSql;
        command.Parameters.AddWithValue(
            "account_key",
            NpgsqlDbType.Bytea,
            PostgreSqlSchema.EncodeAccountKey(projection.AccountKey));
        command.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, projection.Version);
        command.Parameters.AddWithValue("operation", NpgsqlDbType.Smallint, (short)projection.Operation);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null;
    }

    /// <summary>
    /// Checkpoints an exact rebuilt desired version and supersedes all older outbox work for its
    /// account after the external Memory index call has completed.
    /// </summary>
    public async Task<bool> FinalizeRebuildProjectionAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(projection);
        if (!projection.IsComplete)
        {
            throw new ArgumentException("Only a complete desired projection can finish rebuild.", nameof(projection));
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = PostgreSqlCommands.FinalizeRebuildProjectionSql;
        command.Parameters.AddWithValue(
            "account_key",
            NpgsqlDbType.Bytea,
            PostgreSqlSchema.EncodeAccountKey(projection.AccountKey));
        command.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, projection.Version);
        command.Parameters.AddWithValue("operation", NpgsqlDbType.Smallint, (short)projection.Operation);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    /// <summary>
    /// Batch-reads only complete published upserts. Removal tombstones are never hydration results.
    /// </summary>
    public async Task<IReadOnlyDictionary<AccountKey, ProjectionSnapshot>> ReadPublishedUpsertsAsync(
        IReadOnlyCollection<AccountKey> accountKeys,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(accountKeys);
        if (accountKeys.Count > MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(accountKeys),
                accountKeys.Count,
                $"At most {MaximumPageSize} account keys can be hydrated at once.");
        }

        if (accountKeys.Count == 0)
        {
            return new ReadOnlyDictionary<AccountKey, ProjectionSnapshot>(
                new Dictionary<AccountKey, ProjectionSnapshot>());
        }

        var distinctKeys = accountKeys.Distinct().ToArray();
        if (distinctKeys.Any(static key => !key.IsValid))
        {
            throw new ArgumentException("Every hydration key must be valid.", nameof(accountKeys));
        }

        var encodedKeys = distinctKeys
            .Select(PostgreSqlSchema.EncodeAccountKey)
            .ToArray();
        var result = new Dictionary<AccountKey, ProjectionSnapshot>(distinctKeys.Length);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = PostgreSqlCommands.ReadPublishedUpsertsSql;
        command.Parameters.AddWithValue(
            "account_keys",
            NpgsqlDbType.Array | NpgsqlDbType.Bytea,
            encodedKeys);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var projection = PostgreSqlDispatchStore.ReadProjectionSnapshot(reader);
            result.Add(projection.AccountKey, projection);
        }

        return new ReadOnlyDictionary<AccountKey, ProjectionSnapshot>(result);
    }

    private static void ValidateBatchSize(int batchSize)
    {
        if (batchSize is < 1 or > MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize),
                batchSize,
                $"The batch size must be between 1 and {MaximumPageSize}.");
        }
    }
}

/// <summary>
/// Holds the PostgreSQL session advisory lock for one projection-dispatcher incarnation.
/// </summary>
public sealed class PostgreSqlDispatcherIncarnationLock : IAsyncDisposable
{
    private NpgsqlConnection? _connection;

    internal PostgreSqlDispatcherIncarnationLock(NpgsqlConnection connection)
    {
        _connection = connection;
    }

    /// <summary>
    /// Confirms on the owning PostgreSQL session that the exact advisory lock is still held.
    /// A broken session raises an exception and must stop the owning process before another write.
    /// </summary>
    public async ValueTask<bool> IsHeldAsync(CancellationToken cancellationToken = default)
    {
        var connection = Volatile.Read(ref _connection);
        if (connection is null)
        {
            return false;
        }

        await using var command = connection.CreateCommand();
        command.CommandText = PostgreSqlProjectionRuntimeStore.IsDispatcherLockHeldSql;
        command.Parameters.AddWithValue(
            "lock_key",
            NpgsqlDbType.Bigint,
            PostgreSqlProjectionRuntimeStore.DispatcherAdvisoryLockKey);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    public async ValueTask DisposeAsync()
    {
        var connection = Interlocked.Exchange(ref _connection, null);
        if (connection is null)
        {
            return;
        }

        try
        {
            // A session advisory lock survives returning the pooled physical connection; Npgsql
            // resets it only when that physical connection is next reused. Release explicitly so
            // a successor incarnation can acquire the lock immediately after disposal.
            await using var command = connection.CreateCommand();
            command.CommandText = PostgreSqlProjectionRuntimeStore.ReleaseDispatcherSql;
            command.Parameters.AddWithValue(
                "lock_key",
                NpgsqlDbType.Bigint,
                PostgreSqlProjectionRuntimeStore.DispatcherAdvisoryLockKey);
            _ = await command.ExecuteScalarAsync().ConfigureAwait(false);
        }
        catch (NpgsqlException)
        {
            // A broken session cannot be returned to the pool; closing its physical connection
            // releases the advisory lock at the server.
        }
        finally
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
