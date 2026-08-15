using Microsoft.Extensions.Options;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

internal sealed class QuerySessionCleanupService(
    QuerySessionRegistry sessions,
    IOptions<QuerySessionOptions> options,
    TimeProvider timeProvider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Value.CleanupInterval, timeProvider);
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            sessions.RemoveExpired();
        }
    }
}
