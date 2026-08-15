namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Stores one real source-derived projection and publishes it to matching current-page sessions.
/// </summary>
/// <remarks>
/// This direct path is limited to local functional checks. Qualification ingestion must use the
/// PostgreSQL outbox dispatcher so that an ambiguous index write terminates the current Memory
/// index incarnation and is recovered by a full authoritative replay.
/// </remarks>
public sealed class ProjectionUpdatePublisher(
    IProjectionStore projectionStore,
    IProjectionIndexWriter indexWriter,
    QuerySessionRegistry sessions)
{
    public async ValueTask<int> PublishAsync(
        AccountProjection projection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(projection);

        await projectionStore.UpsertAsync(projection, cancellationToken).ConfigureAwait(false);
        await indexWriter.UpsertAsync(projection, cancellationToken).ConfigureAwait(false);
        return sessions.Publish(SkyPulseQueryRow.FromProjection(projection));
    }
}
