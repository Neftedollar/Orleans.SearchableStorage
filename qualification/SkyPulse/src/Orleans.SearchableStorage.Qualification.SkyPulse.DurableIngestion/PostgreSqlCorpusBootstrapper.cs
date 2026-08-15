using System.Data;
using System.Globalization;
using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

/// <summary>
/// Idempotently creates the exact selected-account baseline before TAP ingestion can run. This
/// first version deliberately scans all admitted keys on every startup.
/// </summary>
public sealed class PostgreSqlCorpusBootstrapper
{
    public const int MaximumPageSize = FileBackedCorpusAdmission.MaximumReadPageSize;

    internal const string BootstrapPageSql = """
        WITH admitted AS MATERIALIZED (
            SELECT account_key
            FROM unnest(@account_keys::bytea[]) AS input(account_key)
        ), inserted AS (
            INSERT INTO skypulse.account_state (
                account_key, state_version, lifecycle, repository_generation,
                completed_sync_revision, last_applied_revision, synchronization_complete,
                last_activity_minute_utc, current_post_count,
                current_following_count, current_follower_count)
            SELECT account_key, 1, 1, 0, NULL, NULL, FALSE, 0, 0, 0, 0
            FROM admitted
            ON CONFLICT (account_key) DO NOTHING
            RETURNING account_key, repository_generation, lifecycle, synchronization_complete
        ), dependency_candidates AS (
            SELECT account_key, repository_generation
            FROM inserted
            WHERE lifecycle = 1 AND NOT synchronization_complete
            UNION ALL
            SELECT state.account_key, state.repository_generation
            FROM skypulse.account_state AS state
            JOIN admitted USING (account_key)
            WHERE state.lifecycle = 1
              AND NOT state.synchronization_complete
              AND NOT EXISTS (
                  SELECT 1 FROM inserted WHERE inserted.account_key = state.account_key)
        )
        INSERT INTO skypulse.reconciliation_dependency (
            owner_account_key, owner_repository_generation, affected_account_key)
        SELECT account_key, repository_generation, account_key
        FROM dependency_candidates
        ON CONFLICT DO NOTHING;
        """;

    internal const string CountSql = "SELECT count(*)::bigint FROM skypulse.account_state;";

    // "SKYPULSE" as a stable signed advisory-lock namespace.
    private const long BootstrapLockKey = 0x534B5950554C5345;
    private readonly NpgsqlDataSource _dataSource;
    private readonly VerifiedCorpusAdmission _admission;
    private readonly int _pageSize;

    public PostgreSqlCorpusBootstrapper(
        NpgsqlDataSource dataSource,
        VerifiedCorpusAdmission admission,
        int pageSize = 1_000)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _admission = admission ?? throw new ArgumentNullException(nameof(admission));
        if (pageSize is < 1 or > MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                pageSize,
                $"The corpus bootstrap page size must be between 1 and {MaximumPageSize}.");
        }

        _pageSize = pageSize;
    }

    public Task BootstrapAsync(CancellationToken cancellationToken = default)
        => BootstrapRangeAsync(startIndex: 0, cancellationToken);

    /// <summary>
    /// Idempotently inserts the suffix beginning at <paramref name="startIndex"/> and then proves
    /// that PostgreSQL contains exactly the selected prefix. Repeating a partially completed
    /// expansion after process loss is safe.
    /// </summary>
    public async Task BootstrapRangeAsync(
        int startIndex,
        CancellationToken cancellationToken = default)
    {
        if (startIndex < 0 || startIndex > _admission.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(startIndex));
        }

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await AcquireLockAsync(connection, cancellationToken).ConfigureAwait(false);
        try
        {
            for (var start = startIndex; start < _admission.Count;)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var page = _admission.ReadPage(start, _pageSize);
                await BootstrapPageAsync(connection, page, cancellationToken).ConfigureAwait(false);
                start = checked(start + page.Count);
            }

            await using var count = connection.CreateCommand();
            count.CommandText = CountSql;
            var observed = Convert.ToInt64(
                await count.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            if (observed != _admission.Count)
            {
                throw new InvalidOperationException(
                    "PostgreSQL account state does not equal the exact frozen corpus cap after bootstrap.");
            }
        }
        finally
        {
            await ReleaseLockAsync(connection).ConfigureAwait(false);
        }
    }

    private static async Task BootstrapPageAsync(
        NpgsqlConnection connection,
        IReadOnlyList<AccountKey> page,
        CancellationToken cancellationToken)
    {
        if (page.Count == 0)
        {
            return;
        }

        var encoded = new byte[page.Count][];
        for (var index = 0; index < page.Count; index++)
        {
            encoded[index] = Convert.FromHexString(page[index].ToString());
        }

        await using var transaction = await connection
            .BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken)
            .ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BootstrapPageSql;
        command.Parameters.AddWithValue(
            "account_keys",
            NpgsqlDbType.Array | NpgsqlDbType.Bytea,
            encoded);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task AcquireLockAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_advisory_lock(@lock_key);";
        command.Parameters.AddWithValue("lock_key", NpgsqlDbType.Bigint, BootstrapLockKey);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task ReleaseLockAsync(NpgsqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_advisory_unlock(@lock_key);";
        command.Parameters.AddWithValue("lock_key", NpgsqlDbType.Bigint, BootstrapLockKey);
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }
}
