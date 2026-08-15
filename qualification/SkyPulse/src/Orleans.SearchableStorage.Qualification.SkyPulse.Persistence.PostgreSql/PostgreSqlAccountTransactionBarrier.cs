using System.Buffers.Binary;
using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Serializes every durable mutation of one account with restartable lifecycle work for that
/// account. Transaction-scoped advisory locks cover accounts which do not have an
/// <c>account_state</c> row yet.
/// </summary>
internal static class PostgreSqlAccountTransactionBarrier
{
    // PostgreSQL keeps the two-int advisory-lock key space separate from the bigint key space used
    // by the projection-dispatcher incarnation lock. The XOR values domain-separate this use while
    // preserving all 64 bits derived from the already-hashed account key.
    private const int ClassDomain = unchecked((int)0x53504C53);
    private const int ObjectDomain = unchecked((int)0x41434354);

    internal const string AcquireSql =
        "SELECT pg_advisory_xact_lock(@account_lock_class_id, @account_lock_object_id);";

    internal const string HasPendingWorkSql = """
        SELECT EXISTS (
            SELECT 1
            FROM skypulse.lifecycle_transition_work
            WHERE account_key = @account_key);
        """;

    internal static IReadOnlyList<AccountKey> Canonicalize(IEnumerable<AccountKey> accountKeys)
    {
        ArgumentNullException.ThrowIfNull(accountKeys);
        var ordered = accountKeys.Distinct().Order().ToArray();
        if (ordered.Length == 0)
        {
            throw new ArgumentException("At least one account transaction barrier is required.", nameof(accountKeys));
        }

        if (ordered.Any(static accountKey => !accountKey.IsValid))
        {
            throw new ArgumentException("Every account transaction barrier key must be valid.", nameof(accountKeys));
        }

        return ordered;
    }

    internal static async Task<IReadOnlyList<AccountKey>> AcquireAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IEnumerable<AccountKey> accountKeys,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        var ordered = Canonicalize(accountKeys);
        foreach (var accountKey in ordered)
        {
            var (classId, objectId) = GetLockIdentity(accountKey);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = AcquireSql;
            command.Parameters.AddWithValue("account_lock_class_id", NpgsqlDbType.Integer, classId);
            command.Parameters.AddWithValue("account_lock_object_id", NpgsqlDbType.Integer, objectId);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return ordered;
    }

    internal static async Task<bool> HasPendingWorkAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IEnumerable<AccountKey> orderedAccountKeys,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(orderedAccountKeys);
        foreach (var accountKey in orderedAccountKeys)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = HasPendingWorkSql;
            command.Parameters.AddWithValue(
                "account_key",
                NpgsqlDbType.Bytea,
                PostgreSqlSchema.EncodeAccountKey(accountKey));
            if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true)
            {
                return true;
            }
        }

        return false;
    }

    internal static (int ClassId, int ObjectId) GetLockIdentity(AccountKey accountKey)
    {
        var encoded = PostgreSqlSchema.EncodeAccountKey(accountKey);
        return (
            BinaryPrimitives.ReadInt32BigEndian(encoded) ^ ClassDomain,
            BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(sizeof(int))) ^ ObjectDomain);
    }
}
