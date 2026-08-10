using Azure.Storage.Blobs;
using Npgsql;
using StackExchange.Redis;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.SearchableStorage.Benchmarks;

internal sealed class BackendLease : IAsyncDisposable
{
    private readonly Func<Task>? _cleanup;
    private int _disposed;

    internal BackendLease(
        string providerConnectionString,
        string storageNamespace,
        string cleanupPolicy,
        Func<Task>? cleanup)
    {
        ProviderConnectionString = providerConnectionString;
        StorageNamespace = storageNamespace;
        CleanupReport = new BackendCleanupReport(cleanupPolicy, Attempted: false, Succeeded: false, Error: null);
        _cleanup = cleanup;
    }

    public string ProviderConnectionString { get; }

    public string StorageNamespace { get; }

    public BackendCleanupReport CleanupReport { get; private set; }

    public static async Task<BackendLease> PrepareAsync(
        StorageSpec storage,
        TopologySpec topology,
        CancellationToken cancellationToken)
    {
        var cleanupOwner = !string.Equals(
            Environment.GetEnvironmentVariable("OSS_BENCHMARK_CLEANUP_OWNER"),
            "false",
            StringComparison.OrdinalIgnoreCase);
        return storage.Backend switch
        {
            StorageBackend.Memory => new BackendLease(
                string.Empty,
                topology.ServiceId,
                "process-memory",
                cleanup: null),
            StorageBackend.PostgreSql => await PreparePostgreSqlAsync(
                storage,
                topology,
                cleanupOwner,
                cancellationToken),
            StorageBackend.Redis => await PrepareRedisAsync(storage, topology, cleanupOwner),
            StorageBackend.AzureBlob => await PrepareAzureBlobAsync(
                storage,
                cleanupOwner,
                cancellationToken),
            _ => throw new InvalidOperationException($"Unsupported backend '{storage.Backend}'."),
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (_cleanup is null)
        {
            if (string.Equals(CleanupReport.Policy, "process-memory", StringComparison.Ordinal))
            {
                CleanupReport = CleanupReport with { Attempted = true, Succeeded = true };
            }

            return;
        }

        CleanupReport = CleanupReport with { Attempted = true, Succeeded = false };
        try
        {
            await _cleanup();
            CleanupReport = CleanupReport with { Succeeded = true, Error = null };
        }
        catch (Exception exception)
        {
            CleanupReport = CleanupReport with
            {
                Attempted = true,
                Succeeded = false,
                Error = $"{exception.GetType().Name}: {exception.Message}",
            };
            throw;
        }
    }

    private static async Task<BackendLease> PreparePostgreSqlAsync(
        StorageSpec storage,
        TopologySpec topology,
        bool cleanupOwner,
        CancellationToken cancellationToken)
    {
        var administrativeConnectionString = GetRequiredConnectionString(storage);
        var schemaName = BackendNamespace.CreatePostgreSqlIdentifier(topology.ServiceId);
        var cleanupPolicy = cleanupOwner ? "drop-schema-on-silo-exit" : "shared-silo-non-owner";
        Func<Task>? cleanup = cleanupOwner
            ? async () =>
            {
                await using var connection = new NpgsqlConnection(administrativeConnectionString);
                await connection.OpenAsync();
                await ExecutePostgreSqlAsync(
                    connection,
                    $"DROP SCHEMA IF EXISTS {QuoteIdentifier(schemaName)} CASCADE",
                    CancellationToken.None);
            }
            : null;

        return await BackendProvisioningGuard.RunAsync(
            cleanupPolicy,
            async token =>
            {
                await using (var connection = new NpgsqlConnection(administrativeConnectionString))
                {
                    await connection.OpenAsync(token);
                    await ExecutePostgreSqlAsync(
                        connection,
                        $"SELECT pg_advisory_lock(hashtext('{EscapeLiteral(schemaName)}'))",
                        token);
                    try
                    {
                        await ExecutePostgreSqlAsync(
                            connection,
                            $"CREATE SCHEMA IF NOT EXISTS {QuoteIdentifier(schemaName)}; SET search_path TO {QuoteIdentifier(schemaName)}",
                            token);
                        await using var exists = connection.CreateCommand();
                        exists.CommandText = "SELECT to_regclass('\"OrleansQuery\"') IS NOT NULL";
                        var initialized = (bool)(await exists.ExecuteScalarAsync(token) ?? false);
                        if (!initialized)
                        {
                            await ExecutePostgreSqlFileAsync(connection, "PostgreSQL-Main.sql", token);
                            await ExecutePostgreSqlFileAsync(connection, "PostgreSQL-Persistence.sql", token);
                        }
                    }
                    finally
                    {
                        await ExecutePostgreSqlAsync(
                            connection,
                            $"SELECT pg_advisory_unlock(hashtext('{EscapeLiteral(schemaName)}'))",
                            CancellationToken.None);
                    }
                }

                var providerConnectionString = new NpgsqlConnectionStringBuilder(administrativeConnectionString)
                {
                    SearchPath = schemaName,
                }.ConnectionString;
                return new BackendLease(
                    providerConnectionString,
                    schemaName,
                    cleanupPolicy,
                    cleanup);
            },
            cleanup,
            cancellationToken);
    }

    private static Task<BackendLease> PrepareRedisAsync(
        StorageSpec storage,
        TopologySpec topology,
        bool cleanupOwner)
    {
        var connectionString = GetRequiredConnectionString(storage);
        var keyPattern = BackendNamespace.CreateRedisStateKeyPattern(topology.ServiceId);
        Func<Task>? cleanup = null;
        if (cleanupOwner)
        {
            cleanup = async () =>
            {
                var configuration = ConfigurationOptions.Parse(connectionString);
                await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
                var database = multiplexer.GetDatabase();
                var keys = multiplexer.GetEndPoints()
                    .SelectMany(endpoint => multiplexer.GetServer(endpoint)
                        .Keys(pattern: keyPattern)
                        .ToArray())
                    .Distinct()
                    .ToArray();
                foreach (var batch in keys.Chunk(128))
                {
                    await Task.WhenAll(batch.Select(key => database.KeyDeleteAsync(key)));
                }
            };
        }

        return Task.FromResult(new BackendLease(
            connectionString,
            topology.ServiceId,
            cleanupOwner ? "delete-service-state-keys-on-silo-exit" : "shared-silo-non-owner",
            cleanup));
    }

    private static async Task<BackendLease> PrepareAzureBlobAsync(
        StorageSpec storage,
        bool cleanupOwner,
        CancellationToken cancellationToken)
    {
        var connectionString = GetRequiredConnectionString(storage);
        var container = new BlobServiceClient(connectionString).GetBlobContainerClient(storage.AzureBlobContainer);
        var cleanupPolicy = cleanupOwner ? "delete-container-on-silo-exit" : "shared-silo-non-owner";
        Func<Task>? cleanup = cleanupOwner
            ? async () => await container.DeleteIfExistsAsync(cancellationToken: CancellationToken.None)
            : null;
        return await BackendProvisioningGuard.RunAsync(
            cleanupPolicy,
            async token =>
            {
                await container.CreateIfNotExistsAsync(cancellationToken: token);
                return new BackendLease(
                    connectionString,
                    storage.AzureBlobContainer,
                    cleanupPolicy,
                    cleanup);
            },
            cleanup,
            cancellationToken);
    }

    private static string GetRequiredConnectionString(StorageSpec storage)
    {
        return Environment.GetEnvironmentVariable(storage.ConnectionStringEnvironment) is { Length: > 0 } value
            ? value
            : throw new InvalidOperationException(
                $"Storage backend '{storage.Backend}' requires environment variable '{storage.ConnectionStringEnvironment}'.");
    }

    private static async Task ExecutePostgreSqlFileAsync(
        NpgsqlConnection connection,
        string fileName,
        CancellationToken cancellationToken)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "BackendAssets", fileName);
        var sql = await File.ReadAllTextAsync(path, cancellationToken);
        await ExecutePostgreSqlAsync(connection, sql, cancellationToken);
    }

    private static async Task ExecutePostgreSqlAsync(
        NpgsqlConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string QuoteIdentifier(string value)
    {
        return $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }

    private static string EscapeLiteral(string value)
    {
        return value.Replace("'", "''", StringComparison.Ordinal);
    }
}

internal sealed record BackendCleanupReport(string Policy, bool Attempted, bool Succeeded, string? Error);

internal sealed class BackendProvisioningException(
    BackendCleanupReport cleanupReport,
    Exception innerException)
    : Exception("Backend provisioning failed; the best-effort compensation path completed.", innerException)
{
    public BackendCleanupReport CleanupReport { get; } = cleanupReport;
}

internal static class BackendProvisioningGuard
{
    public static async Task<T> RunAsync<T>(
        string cleanupPolicy,
        Func<CancellationToken, Task<T>> provision,
        Func<Task>? compensate,
        CancellationToken cancellationToken)
    {
        try
        {
            return await provision(cancellationToken);
        }
        catch (Exception provisioningFailure)
        {
            if (compensate is null)
            {
                throw new BackendProvisioningException(
                    new BackendCleanupReport(
                        cleanupPolicy,
                        Attempted: false,
                        Succeeded: false,
                        Error: null),
                    provisioningFailure);
            }

            BackendCleanupReport cleanupReport;
            Exception combinedFailure = provisioningFailure;
            try
            {
                await compensate();
                cleanupReport = new BackendCleanupReport(
                    cleanupPolicy,
                    Attempted: true,
                    Succeeded: true,
                    Error: null);
            }
            catch (Exception cleanupFailure)
            {
                cleanupReport = new BackendCleanupReport(
                    cleanupPolicy,
                    Attempted: true,
                    Succeeded: false,
                    Error: $"{cleanupFailure.GetType().Name}: {cleanupFailure.Message}");
                combinedFailure = new AggregateException(provisioningFailure, cleanupFailure);
            }

            throw new BackendProvisioningException(cleanupReport, combinedFailure);
        }
    }
}

internal static class BackendNamespace
{
    public const int MaximumServiceIdLength = 150;

    public static void ValidateServiceId(string serviceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceId);
        if (serviceId.Length > MaximumServiceIdLength ||
            !char.IsAsciiLetterOrDigit(serviceId[0]) ||
            !char.IsAsciiLetterOrDigit(serviceId[^1]) ||
            serviceId.Any(static character => !char.IsAsciiLetterOrDigit(character) && character != '-') ||
            serviceId.Contains("--", StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"serviceId must be 1-{MaximumServiceIdLength} ASCII letters, digits, or non-consecutive hyphens, and start/end with a letter or digit.");
        }
    }

    public static string CreateRedisStateKeyPattern(string serviceId)
    {
        ValidateServiceId(serviceId);
        return $"{serviceId}/state/*";
    }

    public static string CreatePostgreSqlIdentifier(string serviceId)
    {
        ValidateServiceId(serviceId);
        var safe = new string(serviceId
            .ToLowerInvariant()
            .Select(static character => char.IsAsciiLetterOrDigit(character) ? character : '_')
            .ToArray());
        var hash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(serviceId)))[..12];
        const int maximumIdentifierLength = 63;
        var prefixLength = maximumIdentifierLength - hash.Length - 1;
        var prefix = ("oss_" + safe);
        if (prefix.Length > prefixLength)
        {
            prefix = prefix[..prefixLength];
        }

        return $"{prefix}_{hash}";
    }
}
