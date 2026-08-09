using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

public sealed class PostgreSqlStorageFixture : ExternalStorageFixture<PostgreSqlSiloConfigurator>
{
    private string? _administrativeConnectionString;
    private PostgreSqlSchemaManager? _schemaManager;
    private string? _schemaName;

    public PostgreSqlStorageFixture()
        : base("postgresql")
    {
    }

    internal PostgreSqlSchemaManager SchemaManager => _schemaManager
        ?? throw new InvalidOperationException("The PostgreSQL test resource has not been prepared.");

    protected override async Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
    {
        _administrativeConnectionString = BackendTestEnvironment.GetConnectionString(
            BackendTestEnvironment.PostgreSqlConnectionStringVariable,
            BackendTestEnvironment.DefaultPostgreSqlConnectionString);
        // Orleans' PostgreSQL queries are unqualified, so a private search path isolates each run.
        _schemaName = $"oss_{Guid.NewGuid():N}";
        _schemaManager = new PostgreSqlSchemaManager(_administrativeConnectionString);
        await _schemaManager.CreateSchemaAsync(_schemaName);
        var schemaIdentifier = PostgreSqlSchemaManager.QuoteIdentifier(_schemaName);

        await using var connection = new NpgsqlConnection(_administrativeConnectionString);
        await connection.OpenAsync();
        await ExecuteAsync(connection, $"SET search_path TO {schemaIdentifier}");
        await ExecuteScriptAsync(connection, "PostgreSql/PostgreSQL-Main.sql");
        await ExecuteScriptAsync(connection, "PostgreSql/PostgreSQL-Persistence.sql");

        var providerConnectionString = new NpgsqlConnectionStringBuilder(_administrativeConnectionString)
        {
            SearchPath = _schemaName,
        }.ConnectionString;

        return new Dictionary<string, string?>
        {
            [PostgreSqlSiloConfigurator.ConnectionStringKey] = providerConnectionString,
        };
    }

    protected override async Task CleanupBackendAsync()
    {
        if (_schemaManager is null || _schemaName is null)
        {
            return;
        }

        await _schemaManager.DropSchemaAsync(_schemaName);
    }

    private static async Task ExecuteScriptAsync(NpgsqlConnection connection, string relativePath)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Infrastructure", relativePath);
        var commandText = await File.ReadAllTextAsync(path);
        await ExecuteAsync(connection, commandText);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string commandText)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        await command.ExecuteNonQueryAsync();
    }

}

internal sealed class PostgreSqlSchemaManager(string connectionString)
{
    private const string SentinelTableName = "cleanup_sentinel";

    public async Task CreateSchemaAsync(string schemaName)
    {
        await ExecuteAsync($"CREATE SCHEMA {QuoteIdentifier(schemaName)}");
    }

    public async Task CreateSchemaWithSentinelAsync(string schemaName)
    {
        var schemaIdentifier = QuoteIdentifier(schemaName);
        await ExecuteAsync(
            $"CREATE SCHEMA {schemaIdentifier}; " +
            $"CREATE TABLE {schemaIdentifier}.{QuoteIdentifier(SentinelTableName)} (value integer NOT NULL)");
    }

    public async Task DropSchemaAsync(string schemaName)
    {
        await ExecuteAsync($"DROP SCHEMA IF EXISTS {QuoteIdentifier(schemaName)} CASCADE");
    }

    public Task<bool> SchemaExistsAsync(string schemaName)
    {
        return ExistsAsync(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = @name)",
            schemaName);
    }

    public Task<bool> SentinelExistsAsync(string schemaName)
    {
        return ExistsAsync(
            "SELECT EXISTS (" +
            "SELECT 1 FROM information_schema.tables " +
            "WHERE table_schema = @name AND table_name = 'cleanup_sentinel')",
            schemaName);
    }

    internal static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }

    private async Task ExecuteAsync(string commandText)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<bool> ExistsAsync(string commandText, string schemaName)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Parameters.AddWithValue("name", schemaName);
        return (bool)(await command.ExecuteScalarAsync()
            ?? throw new InvalidOperationException("PostgreSQL did not return an existence result."));
    }
}

public sealed class PostgreSqlSiloConfigurator : IHostConfigurator
{
    public const string ConnectionStringKey = "BackendTests:PostgreSql:ConnectionString";

    public void Configure(IHostBuilder hostBuilder)
    {
        hostBuilder.UseOrleans((context, siloBuilder) =>
        {
            var connectionString = ExternalStorageSiloConfiguration.GetRequiredSetting(
                context.Configuration,
                ConnectionStringKey);
            siloBuilder.AddAdoNetGrainStorage(
                ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName,
                (OptionsBuilder<AdoNetGrainStorageOptions> optionsBuilder) =>
                    optionsBuilder.Configure<OrleansJsonSerializer>((options, serializer) =>
                    {
                        options.ConnectionString = connectionString;
                        options.Invariant = "Npgsql";
                        options.DeleteStateOnClear = true;
                        ExternalStorageSiloConfiguration.UseJsonSerializer(options, serializer);
                    }));
            ExternalStorageSiloConfiguration.AddSearchableStorage(siloBuilder);
        });
    }
}
