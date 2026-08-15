using System.Data;
using System.Globalization;
using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Identifies one reviewed prefix of the immutable parent corpus.
/// </summary>
public sealed record CorpusCapacityProfile
{
    public CorpusCapacityProfile(
        string profileId,
        int profileVersion,
        long corpusCap,
        string prefixSha256)
    {
        ProfileId = RuntimeManifestGuard.CanonicalIdentifier(profileId, nameof(profileId));
        ProfileVersion = RuntimeManifestGuard.Positive(profileVersion, nameof(profileVersion));
        CorpusCap = RuntimeManifestGuard.Positive(corpusCap, nameof(corpusCap));
        PrefixSha256 = RuntimeManifestGuard.Sha256(prefixSha256, nameof(prefixSha256));
    }

    public string ProfileId { get; }

    public int ProfileVersion { get; }

    public long CorpusCap { get; }

    public string PrefixSha256 { get; }
}

/// <summary>
/// Describes the durable monotonic corpus-cap state. The target is non-null only while an online
/// expansion is being bootstrapped or provisioned.
/// </summary>
public sealed record CorpusCapacityState(
    CorpusCapacityProfile Base,
    CorpusCapacityProfile Active,
    CorpusCapacityProfile? Target,
    long OperationVersion);

public sealed record CorpusCapacityStatistics(long AccountCount, long SynchronizedAccountCount);

public enum CorpusGrowthRequestOutcome
{
    Accepted = 1,
    AlreadyActive = 2,
    AlreadyRequested = 3,
    GrowthInProgress = 4,
    NonMonotonic = 5,
}

public sealed record CorpusGrowthRequestResult(
    CorpusGrowthRequestOutcome Outcome,
    CorpusCapacityState State);

/// <summary>
/// Thrown when a database is already bound to a different immutable base corpus profile.
/// </summary>
public sealed class CorpusCapacityIdentityMismatchException : InvalidOperationException
{
    public CorpusCapacityIdentityMismatchException()
        : base("The PostgreSQL corpus-capacity base identity does not match this process.")
    {
    }
}

/// <summary>
/// Persists one restartable, monotonic online corpus expansion at a time.
/// </summary>
public sealed class PostgreSqlCorpusCapacityStore
{
    // "SKYPCAPA" as a stable transaction-lock namespace.
    private const long CapacityLockKey = 0x534B595043415041;

    internal const string InsertBaseSql = """
        INSERT INTO skypulse.corpus_capacity (
            capacity_id, base_profile_id, base_profile_version, base_corpus_cap,
            base_prefix_sha256, active_profile_id, active_corpus_cap,
            active_prefix_sha256, target_profile_id, target_corpus_cap,
            target_prefix_sha256, operation_version)
        VALUES (
            1, @profile_id, @profile_version, @corpus_cap,
            @prefix_sha256, @profile_id, @corpus_cap,
            @prefix_sha256, NULL, NULL, NULL, 1)
        ON CONFLICT (capacity_id) DO NOTHING;
        """;

    internal const string ReadSql = """
        SELECT base_profile_id, base_profile_version, base_corpus_cap, base_prefix_sha256,
            active_profile_id, active_corpus_cap, active_prefix_sha256,
            target_profile_id, target_corpus_cap, target_prefix_sha256, operation_version
        FROM skypulse.corpus_capacity
        WHERE capacity_id = 1;
        """;

    internal const string ReadForUpdateSql = """
        SELECT base_profile_id, base_profile_version, base_corpus_cap, base_prefix_sha256,
            active_profile_id, active_corpus_cap, active_prefix_sha256,
            target_profile_id, target_corpus_cap, target_prefix_sha256, operation_version
        FROM skypulse.corpus_capacity
        WHERE capacity_id = 1
        FOR UPDATE;
        """;

    internal const string RequestGrowthSql = """
        UPDATE skypulse.corpus_capacity
        SET target_profile_id = @profile_id,
            target_corpus_cap = @corpus_cap,
            target_prefix_sha256 = @prefix_sha256,
            operation_version = operation_version + 1,
            updated_at_utc = clock_timestamp()
        WHERE capacity_id = 1
          AND operation_version = @expected_version
          AND target_profile_id IS NULL
          AND active_corpus_cap < @corpus_cap;
        """;

    internal const string CompleteGrowthSql = """
        UPDATE skypulse.corpus_capacity
        SET active_profile_id = target_profile_id,
            active_corpus_cap = target_corpus_cap,
            active_prefix_sha256 = target_prefix_sha256,
            target_profile_id = NULL,
            target_corpus_cap = NULL,
            target_prefix_sha256 = NULL,
            operation_version = operation_version + 1,
            updated_at_utc = clock_timestamp()
        WHERE capacity_id = 1
          AND operation_version = @expected_version
          AND target_profile_id = @profile_id
          AND target_corpus_cap = @corpus_cap
          AND target_prefix_sha256 = @prefix_sha256;
        """;

    internal const string StatisticsSql = """
        SELECT count(*)::bigint,
            count(*) FILTER (WHERE synchronization_complete)::bigint
        FROM skypulse.account_state;
        """;

    internal const string AcquireLockSql = "SELECT pg_advisory_xact_lock(@lock_key);";

    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlCorpusCapacityStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    public async Task<CorpusCapacityState> BindBaseAsync(
        CorpusCapacityProfile baseProfile,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(baseProfile);
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken)
            .ConfigureAwait(false);
        await AcquireLockAsync(connection, transaction, cancellationToken).ConfigureAwait(false);

        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = InsertBaseSql;
            AddProfileParameters(insert, baseProfile);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var state = await ReadAsync(connection, transaction, forUpdate: true, cancellationToken)
            .ConfigureAwait(false)
            ?? throw new InvalidOperationException("The corpus-capacity state could not be read after binding.");
        if (!ProfilesEqual(state.Base, baseProfile))
        {
            throw new CorpusCapacityIdentityMismatchException();
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return state;
    }

    public async Task<CorpusCapacityState> ReadAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        return await ReadAsync(connection, transaction: null, forUpdate: false, cancellationToken)
            .ConfigureAwait(false)
            ?? throw new InvalidOperationException("The PostgreSQL corpus-capacity state is not bound.");
    }

    public async Task<CorpusGrowthRequestResult> RequestGrowthAsync(
        CorpusCapacityProfile target,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken)
            .ConfigureAwait(false);
        await AcquireLockAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        var state = await ReadAsync(connection, transaction, forUpdate: true, cancellationToken)
            .ConfigureAwait(false)
            ?? throw new InvalidOperationException("The PostgreSQL corpus-capacity state is not bound.");

        CorpusGrowthRequestOutcome outcome;
        if (ProfilesEqual(state.Active, target))
        {
            outcome = CorpusGrowthRequestOutcome.AlreadyActive;
        }
        else if (target.ProfileVersion != state.Base.ProfileVersion
            || target.CorpusCap <= state.Active.CorpusCap)
        {
            outcome = CorpusGrowthRequestOutcome.NonMonotonic;
        }
        else if (state.Target is { } pending)
        {
            outcome = ProfilesEqual(pending, target)
                ? CorpusGrowthRequestOutcome.AlreadyRequested
                : CorpusGrowthRequestOutcome.GrowthInProgress;
        }
        else
        {
            await using var update = connection.CreateCommand();
            update.Transaction = transaction;
            update.CommandText = RequestGrowthSql;
            AddProfileParameters(update, target);
            update.Parameters.AddWithValue("expected_version", NpgsqlDbType.Bigint, state.OperationVersion);
            if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                throw new DBConcurrencyException("The corpus-growth request lost its serialized state transition.");
            }

            state = await ReadAsync(connection, transaction, forUpdate: true, cancellationToken)
                .ConfigureAwait(false)
                ?? throw new InvalidOperationException("The requested corpus-capacity state disappeared.");
            outcome = CorpusGrowthRequestOutcome.Accepted;
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new CorpusGrowthRequestResult(outcome, state);
    }

    public async Task<CorpusCapacityState> CompleteGrowthAsync(
        CorpusCapacityProfile target,
        long expectedOperationVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(expectedOperationVersion);
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken)
            .ConfigureAwait(false);
        await AcquireLockAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        await using (var update = connection.CreateCommand())
        {
            update.Transaction = transaction;
            update.CommandText = CompleteGrowthSql;
            AddProfileParameters(update, target);
            update.Parameters.AddWithValue("expected_version", NpgsqlDbType.Bigint, expectedOperationVersion);
            if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                throw new DBConcurrencyException("The corpus-growth completion no longer matches its durable target.");
            }
        }

        var state = await ReadAsync(connection, transaction, forUpdate: true, cancellationToken)
            .ConfigureAwait(false)
            ?? throw new InvalidOperationException("The completed corpus-capacity state disappeared.");
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return state;
    }

    public async Task<CorpusCapacityStatistics> ReadStatisticsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = StatisticsSql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("PostgreSQL did not return corpus-capacity statistics.");
        }

        return new CorpusCapacityStatistics(reader.GetInt64(0), reader.GetInt64(1));
    }

    private static async Task<CorpusCapacityState?> ReadAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        bool forUpdate,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = forUpdate ? ReadForUpdateSql : ReadSql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var version = reader.GetInt32(1);
        var baseProfile = new CorpusCapacityProfile(
            reader.GetString(0),
            version,
            reader.GetInt64(2),
            ToHex(reader.GetFieldValue<byte[]>(3)));
        var activeProfile = new CorpusCapacityProfile(
            reader.GetString(4),
            version,
            reader.GetInt64(5),
            ToHex(reader.GetFieldValue<byte[]>(6)));
        CorpusCapacityProfile? targetProfile = null;
        if (!reader.IsDBNull(7))
        {
            targetProfile = new CorpusCapacityProfile(
                reader.GetString(7),
                version,
                reader.GetInt64(8),
                ToHex(reader.GetFieldValue<byte[]>(9)));
        }

        return new CorpusCapacityState(baseProfile, activeProfile, targetProfile, reader.GetInt64(10));
    }

    private static async Task AcquireLockAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = AcquireLockSql;
        command.Parameters.AddWithValue("lock_key", NpgsqlDbType.Bigint, CapacityLockKey);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddProfileParameters(NpgsqlCommand command, CorpusCapacityProfile profile)
    {
        command.Parameters.AddWithValue("profile_id", NpgsqlDbType.Text, profile.ProfileId);
        command.Parameters.AddWithValue("profile_version", NpgsqlDbType.Integer, profile.ProfileVersion);
        command.Parameters.AddWithValue("corpus_cap", NpgsqlDbType.Bigint, profile.CorpusCap);
        command.Parameters.AddWithValue(
            "prefix_sha256",
            NpgsqlDbType.Bytea,
            PostgreSqlSchema.DecodeDigest(profile.PrefixSha256));
    }

    private static bool ProfilesEqual(CorpusCapacityProfile left, CorpusCapacityProfile right)
        => left.ProfileVersion == right.ProfileVersion
            && left.CorpusCap == right.CorpusCap
            && string.Equals(left.ProfileId, right.ProfileId, StringComparison.Ordinal)
            && string.Equals(left.PrefixSha256, right.PrefixSha256, StringComparison.Ordinal);

    private static string ToHex(byte[] value)
        => Convert.ToHexString(value).ToLower(CultureInfo.InvariantCulture);
}
