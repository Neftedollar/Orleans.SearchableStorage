using Microsoft.Extensions.DependencyInjection;
using Orleans.SearchableStorage;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Activates the empty index-only schema before ASP.NET Core accepts traffic.
/// </summary>
internal sealed partial class SkyPulseIndexSchemaBootstrapService(
    [FromKeyedServices(SkyPulseIndexContract.ProviderName)] ISearchableStorageAdminClient admin,
    ILogger<SkyPulseIndexSchemaBootstrapService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var status = await admin.RebuildIndexSchemaAsync<AccountIndexState>(
                SkyPulseIndexContract.StateName,
                SkyPulseIndexContract.ApplicationSchemaVersion,
                cancellationToken)
            .ConfigureAwait(false);

        LogSchemaState(
            logger,
            SkyPulseIndexContract.ProviderName,
            SkyPulseIndexContract.StateName,
            status.State);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "SkyPulse index schema {Provider}/{StateName} is {SchemaState}.")]
    private static partial void LogSchemaState(
        ILogger logger,
        string provider,
        string stateName,
        SearchableStorageIndexSchemaState schemaState);
}
