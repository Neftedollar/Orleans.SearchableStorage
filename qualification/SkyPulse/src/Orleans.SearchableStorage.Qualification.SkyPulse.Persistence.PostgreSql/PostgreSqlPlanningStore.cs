using System.Data;
using System.Globalization;
using Npgsql;
using NpgsqlTypes;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Thrown when a bounded planning read cannot prove that all returned rows belong to one account state.
/// </summary>
public sealed class PlanningStateChangedException : InvalidOperationException
{
    public PlanningStateChangedException(AccountKey accountKey, long expectedRepositoryGeneration)
        : this(accountKey, expectedAccountStateVersion: null, expectedRepositoryGeneration)
    {
    }

    public PlanningStateChangedException(
        AccountKey accountKey,
        long expectedAccountStateVersion,
        long expectedRepositoryGeneration)
        : this(accountKey, (long?)expectedAccountStateVersion, expectedRepositoryGeneration)
    {
    }

    private PlanningStateChangedException(
        AccountKey accountKey,
        long? expectedAccountStateVersion,
        long expectedRepositoryGeneration)
        : base(expectedAccountStateVersion is { } version
            ? $"Account {accountKey} no longer has state version {version} and repository generation {expectedRepositoryGeneration} in the planning snapshot."
            : $"Account {accountKey} no longer has repository generation {expectedRepositoryGeneration} in the planning snapshot.")
    {
        AccountKey = accountKey;
        ExpectedAccountStateVersion = expectedAccountStateVersion;
        ExpectedRepositoryGeneration = expectedRepositoryGeneration;
    }

    public AccountKey AccountKey { get; }

    public long? ExpectedAccountStateVersion { get; }

    public long ExpectedRepositoryGeneration { get; }
}

/// <summary>
/// Provides exact, bounded read models from which a pure transition planner can derive mutations.
/// </summary>
public sealed class PostgreSqlPlanningStore
{
    public const int MaximumReadPageSize = 1_000;

    public const int MaximumActivityWindowMinutes = 30 * 24 * 60;

    internal const string ReadAccountSql = """
        SELECT state_version, lifecycle, repository_generation, completed_sync_revision,
            last_applied_revision, synchronization_complete, last_activity_minute_utc, current_post_count,
            current_following_count, current_follower_count
        FROM skypulse.account_state
        WHERE account_key = @account_key;
        """;

    internal const string ReadDesiredProjectionSql = """
        SELECT projection_version, operation, is_complete,
            projection_cut_minute_utc, next_recalculation_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days
        FROM skypulse.desired_projection
        WHERE account_key = @account_key;
        """;

    internal const string ReadRecordSql = """
        SELECT latest_revision, is_deleted, cid, target_account_key, is_direct_reply
        FROM skypulse.record_state
        WHERE account_key = @account_key
          AND repository_generation = @repository_generation
          AND collection = @collection
          AND record_key = @record_key;
        """;

    internal const string ReadFollowPairSql = """
        SELECT multiplicity
        FROM skypulse.follow_pair
        WHERE source_account_key = @source_account_key
          AND target_account_key = @target_account_key;
        """;

    internal const string ReadActivitySql = """
        SELECT minute_utc, record_creates, record_updates, record_deletes,
            post_creates, received_engagement_creates
        FROM skypulse.activity_minute_bucket
        WHERE account_key = @account_key
          AND repository_generation = @repository_generation
          AND minute_utc >= @first_minute_utc_inclusive
          AND minute_utc <= @last_minute_utc_inclusive
          AND (@after_minute_utc IS NULL OR minute_utc > @after_minute_utc)
        ORDER BY minute_utc
        LIMIT @read_limit;
        """;

    internal const string ReadActivityWindowAggregateSql = """
        WITH fenced_account AS (
            SELECT state_version, repository_generation
            FROM skypulse.account_state
            WHERE account_key = @account_key
              AND state_version = @expected_state_version
              AND repository_generation = @repository_generation
        ),
        bounded_buckets AS (
            SELECT bucket.minute_utc, bucket.record_creates, bucket.record_updates,
                bucket.record_deletes, bucket.post_creates,
                bucket.received_engagement_creates
            FROM fenced_account AS account
            JOIN skypulse.activity_minute_bucket AS bucket
              ON bucket.account_key = @account_key
             AND bucket.repository_generation = account.repository_generation
             AND bucket.minute_utc > (@cut_minute_utc - 43200)
             AND bucket.minute_utc <= @cut_minute_utc
        ),
        aggregate_values AS (
            SELECT
                COALESCE(SUM(record_creates) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 1440)), 0)::bigint AS record_creates_1_day,
                COALESCE(SUM(record_creates) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 10080)), 0)::bigint AS record_creates_7_days,
                COALESCE(SUM(record_creates), 0)::bigint AS record_creates_30_days,
                COALESCE(SUM(record_updates) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 1440)), 0)::bigint AS record_updates_1_day,
                COALESCE(SUM(record_updates) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 10080)), 0)::bigint AS record_updates_7_days,
                COALESCE(SUM(record_updates), 0)::bigint AS record_updates_30_days,
                COALESCE(SUM(record_deletes) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 1440)), 0)::bigint AS record_deletes_1_day,
                COALESCE(SUM(record_deletes) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 10080)), 0)::bigint AS record_deletes_7_days,
                COALESCE(SUM(record_deletes), 0)::bigint AS record_deletes_30_days,
                COALESCE(SUM(post_creates) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 1440)), 0)::bigint AS post_creates_1_day,
                COALESCE(SUM(post_creates) FILTER (
                    WHERE minute_utc > (@cut_minute_utc - 10080)), 0)::bigint AS post_creates_7_days,
                COALESCE(SUM(post_creates), 0)::bigint AS post_creates_30_days,
                COALESCE(SUM(received_engagement_creates), 0)::bigint
                    AS received_engagement_creates_30_days
            FROM bounded_buckets
        ),
        next_expiry AS (
            SELECT MIN(candidate.expiry_minute_utc) AS next_expiry_minute_utc
            FROM bounded_buckets AS bucket
            CROSS JOIN LATERAL (VALUES
                (CASE WHEN bucket.record_creates > 0 OR bucket.record_updates > 0
                    OR bucket.record_deletes > 0 OR bucket.post_creates > 0
                    THEN bucket.minute_utc + 1440 END),
                (CASE WHEN bucket.record_creates > 0 OR bucket.record_updates > 0
                    OR bucket.record_deletes > 0 OR bucket.post_creates > 0
                    THEN bucket.minute_utc + 10080 END),
                (CASE WHEN bucket.record_creates > 0 OR bucket.record_updates > 0
                    OR bucket.record_deletes > 0 OR bucket.post_creates > 0
                    OR bucket.received_engagement_creates > 0
                    THEN bucket.minute_utc + 43200 END)
            ) AS candidate(expiry_minute_utc)
            WHERE candidate.expiry_minute_utc > @cut_minute_utc
        )
        SELECT account.state_version, account.repository_generation,
            CAST(@cut_minute_utc AS bigint),
            aggregate_values.record_creates_1_day,
            aggregate_values.record_creates_7_days,
            aggregate_values.record_creates_30_days,
            aggregate_values.record_updates_1_day,
            aggregate_values.record_updates_7_days,
            aggregate_values.record_updates_30_days,
            aggregate_values.record_deletes_1_day,
            aggregate_values.record_deletes_7_days,
            aggregate_values.record_deletes_30_days,
            aggregate_values.post_creates_1_day,
            aggregate_values.post_creates_7_days,
            aggregate_values.post_creates_30_days,
            aggregate_values.received_engagement_creates_30_days,
            next_expiry.next_expiry_minute_utc
        FROM fenced_account AS account
        CROSS JOIN aggregate_values
        CROSS JOIN next_expiry;
        """;

    internal const string ReadDependenciesSql = """
        SELECT affected_account_key
        FROM skypulse.reconciliation_dependency
        WHERE owner_account_key = @owner_account_key
          AND owner_repository_generation = @owner_repository_generation
          AND (@after_affected_account_key IS NULL OR affected_account_key > @after_affected_account_key)
        ORDER BY affected_account_key
        LIMIT @read_limit;
        """;

    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlPlanningStore(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
    }

    public async Task<AccountStateSnapshot?> ReadAccountAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await ReadAccountAsync(connection, transaction: null, accountKey, cancellationToken).ConfigureAwait(false);
    }

    public async Task<RecordStateSnapshot?> ReadRecordAsync(
        AccountKey accountKey,
        long repositoryGeneration,
        DurableRecordKind collection,
        string recordKey,
        CancellationToken cancellationToken = default)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.DefinedEnum(collection, nameof(collection));
        Guard.RequiredBounded(recordKey, 512, nameof(recordKey));

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = ReadRecordSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, repositoryGeneration);
        command.Parameters.AddWithValue("collection", NpgsqlDbType.Smallint, (short)collection);
        command.Parameters.AddWithValue("record_key", NpgsqlDbType.Text, recordKey);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return new RecordStateSnapshot(
            accountKey,
            repositoryGeneration,
            collection,
            recordKey,
            reader.GetString(0),
            reader.GetBoolean(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(3)),
            reader.GetBoolean(4));
    }

    /// <summary>
    /// Reads the current desired projection snapshot for planner version/cut fencing.
    /// A missing row is returned as <see langword="null"/> and is distinct from a removal snapshot.
    /// </summary>
    public async Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = ReadDesiredProjectionSql;
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

    public async Task<FollowPairSnapshot?> ReadFollowPairAsync(
        AccountKey sourceAccountKey,
        AccountKey targetAccountKey,
        CancellationToken cancellationToken = default)
    {
        Guard.ValidAccountKey(sourceAccountKey, nameof(sourceAccountKey));
        Guard.ValidAccountKey(targetAccountKey, nameof(targetAccountKey));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = ReadFollowPairSql;
        command.Parameters.AddWithValue("source_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(sourceAccountKey));
        command.Parameters.AddWithValue("target_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(targetAccountKey));
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is null or DBNull
            ? null
            : new FollowPairSnapshot(
                sourceAccountKey,
                targetAccountKey,
                Convert.ToInt32(value, CultureInfo.InvariantCulture));
    }

    public async Task<ActivityMinuteBucketPage> ReadActivityMinuteBucketsAsync(
        AccountKey accountKey,
        long repositoryGeneration,
        long firstMinuteUtcInclusive,
        long lastMinuteUtcInclusive,
        long? afterMinuteUtc,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        ActivityMinuteBucketPage.ValidateWindow(firstMinuteUtcInclusive, lastMinuteUtcInclusive);
        ActivityMinuteBucketPage.ValidatePageSize(pageSize);
        if (afterMinuteUtc is { } after &&
            (after < firstMinuteUtcInclusive || after >= lastMinuteUtcInclusive))
        {
            throw new ArgumentOutOfRangeException(nameof(afterMinuteUtc), after, "The activity cursor must lie inside the requested window and before its last minute.");
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);
        var account = await ReadAccountAsync(connection, transaction, accountKey, cancellationToken).ConfigureAwait(false);
        if (account is null || account.RepositoryGeneration != repositoryGeneration)
        {
            throw new PlanningStateChangedException(accountKey, repositoryGeneration);
        }

        var values = new List<ActivityMinuteBucketSnapshot>(checked(pageSize + 1));
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = ReadActivitySql;
            command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
            command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, repositoryGeneration);
            command.Parameters.AddWithValue("first_minute_utc_inclusive", NpgsqlDbType.Bigint, firstMinuteUtcInclusive);
            command.Parameters.AddWithValue("last_minute_utc_inclusive", NpgsqlDbType.Bigint, lastMinuteUtcInclusive);
            PostgreSqlCommands.AddNullable(command, "after_minute_utc", NpgsqlDbType.Bigint, afterMinuteUtc);
            command.Parameters.AddWithValue("read_limit", NpgsqlDbType.Integer, checked(pageSize + 1));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                values.Add(new ActivityMinuteBucketSnapshot(
                    accountKey,
                    repositoryGeneration,
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetInt64(2),
                    reader.GetInt64(3),
                    reader.GetInt64(4),
                    reader.GetInt64(5)));
            }
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        var hasMore = values.Count > pageSize;
        if (hasMore)
        {
            values.RemoveAt(pageSize);
        }

        return new ActivityMinuteBucketPage(
            accountKey,
            account.StateVersion,
            repositoryGeneration,
            firstMinuteUtcInclusive,
            lastMinuteUtcInclusive,
            pageSize,
            values,
            hasMore);
    }

    /// <summary>
    /// Reads one exact fixed-size rolling activity aggregate at a monotonic projection cut.
    /// </summary>
    /// <remarks>
    /// The account-version and repository-generation fence is evaluated in the same PostgreSQL
    /// statement as the aggregate. A concurrent state change therefore produces no row and is
    /// surfaced as <see cref="PlanningStateChangedException"/> instead of returning mixed evidence.
    /// </remarks>
    public async Task<ActivityWindowAggregateSnapshot> ReadActivityWindowAggregateAsync(
        AccountKey accountKey,
        long expectedAccountStateVersion,
        long repositoryGeneration,
        long cutMinuteUtc,
        CancellationToken cancellationToken = default)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        if (expectedAccountStateVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedAccountStateVersion),
                expectedAccountStateVersion,
                "The expected account state version must be positive.");
        }

        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.NonNegative(cutMinuteUtc, nameof(cutMinuteUtc));

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = ReadActivityWindowAggregateSql;
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(accountKey));
        command.Parameters.AddWithValue("expected_state_version", NpgsqlDbType.Bigint, expectedAccountStateVersion);
        command.Parameters.AddWithValue("repository_generation", NpgsqlDbType.Bigint, repositoryGeneration);
        command.Parameters.AddWithValue("cut_minute_utc", NpgsqlDbType.Bigint, cutMinuteUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new PlanningStateChangedException(
                accountKey,
                expectedAccountStateVersion,
                repositoryGeneration);
        }

        return new ActivityWindowAggregateSnapshot(
            accountKey,
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetInt64(2),
            new ActivityRollingCounts(reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5)),
            new ActivityRollingCounts(reader.GetInt64(6), reader.GetInt64(7), reader.GetInt64(8)),
            new ActivityRollingCounts(reader.GetInt64(9), reader.GetInt64(10), reader.GetInt64(11)),
            new ActivityRollingCounts(reader.GetInt64(12), reader.GetInt64(13), reader.GetInt64(14)),
            reader.GetInt64(15),
            reader.IsDBNull(16) ? null : reader.GetInt64(16));
    }

    public async Task<ReconciliationDependencyPage> ReadReconciliationDependenciesAsync(
        AccountKey ownerAccountKey,
        long ownerRepositoryGeneration,
        AccountKey? afterAffectedAccountKey,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        Guard.ValidAccountKey(ownerAccountKey, nameof(ownerAccountKey));
        Guard.NonNegative(ownerRepositoryGeneration, nameof(ownerRepositoryGeneration));
        if (afterAffectedAccountKey is { } after)
        {
            Guard.ValidAccountKey(after, nameof(afterAffectedAccountKey));
        }

        ActivityMinuteBucketPage.ValidatePageSize(pageSize);
        var values = new List<AccountKey>(checked(pageSize + 1));
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = ReadDependenciesSql;
        command.Parameters.AddWithValue("owner_account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(ownerAccountKey));
        command.Parameters.AddWithValue("owner_repository_generation", NpgsqlDbType.Bigint, ownerRepositoryGeneration);
        PostgreSqlCommands.AddNullable(
            command,
            "after_affected_account_key",
            NpgsqlDbType.Bytea,
            afterAffectedAccountKey is { } cursor ? PostgreSqlSchema.EncodeAccountKey(cursor) : null);
        command.Parameters.AddWithValue("read_limit", NpgsqlDbType.Integer, checked(pageSize + 1));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            values.Add(PostgreSqlSchema.DecodeAccountKey(reader.GetFieldValue<byte[]>(0)));
        }

        var hasMore = values.Count > pageSize;
        if (hasMore)
        {
            values.RemoveAt(pageSize);
        }

        return new ReconciliationDependencyPage(
            ownerAccountKey,
            ownerRepositoryGeneration,
            pageSize,
            values,
            hasMore);
    }

    private static async Task<AccountStateSnapshot?> ReadAccountAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        AccountKey accountKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReadAccountSql;
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
}
