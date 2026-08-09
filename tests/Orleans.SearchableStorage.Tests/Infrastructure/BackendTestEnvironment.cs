namespace Orleans.SearchableStorage.Tests.Infrastructure;

internal static class BackendTestEnvironment
{
    public const string RunBackendTestsVariable = "ORLEANS_SEARCHABLE_STORAGE_RUN_BACKEND_TESTS";
    public const string PostgreSqlConnectionStringVariable = "ORLEANS_SEARCHABLE_STORAGE_POSTGRES_CONNECTION_STRING";
    public const string RedisConnectionStringVariable = "ORLEANS_SEARCHABLE_STORAGE_REDIS_CONNECTION_STRING";
    public const string AzureBlobConnectionStringVariable = "ORLEANS_SEARCHABLE_STORAGE_AZURE_BLOB_CONNECTION_STRING";

    public const string DefaultPostgreSqlConnectionString =
        "Host=127.0.0.1;Port=5432;Database=postgres;Username=postgres;Password=postgres";

    public const string DefaultRedisConnectionString = "127.0.0.1:6379,abortConnect=false";
    public const string DefaultAzureBlobConnectionString = "UseDevelopmentStorage=true";

    public static bool ShouldRunBackendTests()
    {
        var value = Environment.GetEnvironmentVariable(RunBackendTestsVariable);
        return string.Equals(value, "1", StringComparison.Ordinal)
            || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    public static string GetConnectionString(string variableName, string defaultValue)
    {
        return Environment.GetEnvironmentVariable(variableName) is { Length: > 0 } configured
            ? configured
            : defaultValue;
    }
}
