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
    private string? _schemaName;

    public PostgreSqlStorageFixture()
        : base("postgresql")
    {
    }

    protected override async Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
    {
        _administrativeConnectionString = BackendTestEnvironment.GetConnectionString(
            BackendTestEnvironment.PostgreSqlConnectionStringVariable,
            BackendTestEnvironment.DefaultPostgreSqlConnectionString);
        // Orleans' PostgreSQL queries are unqualified, so a private search path isolates each run.
        _schemaName = $"oss_{Guid.NewGuid():N}";
        var schemaIdentifier = QuoteIdentifier(_schemaName);

        await using var connection = new NpgsqlConnection(_administrativeConnectionString);
        await connection.OpenAsync();
        await ExecuteAsync(connection, $"CREATE SCHEMA {schemaIdentifier}");
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
        if (_administrativeConnectionString is null || _schemaName is null)
        {
            return;
        }

        await using var connection = new NpgsqlConnection(_administrativeConnectionString);
        await connection.OpenAsync();
        await ExecuteAsync(connection, $"DROP SCHEMA IF EXISTS {QuoteIdentifier(_schemaName)} CASCADE");
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

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
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
