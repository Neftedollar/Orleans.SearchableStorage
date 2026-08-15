namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

/// <summary>
/// Requires an explicitly configured PostgreSQL database because every test drops and recreates
/// the dedicated <c>skypulse</c> schema.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
internal sealed class PostgreSqlIntegrationFactAttribute : FactAttribute
{
    internal const string ConnectionStringEnvironmentVariable = "SKYPULSE_POSTGRES_CONNECTION_STRING";

    public PostgreSqlIntegrationFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable)))
        {
            Skip = $"Set {ConnectionStringEnvironmentVariable} to a disposable PostgreSQL database to run this test.";
        }
    }
}
