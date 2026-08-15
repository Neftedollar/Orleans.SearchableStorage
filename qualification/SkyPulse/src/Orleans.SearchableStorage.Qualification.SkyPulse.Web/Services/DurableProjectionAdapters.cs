using System.Collections.ObjectModel;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;
using Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Adapts complete durable snapshots to the existing package-backed index writer and current-page
/// notification surface.
/// </summary>
internal sealed class RuntimeProjectionIndexWriterAdapter(
    IProjectionIndexWriter indexWriter,
    QuerySessionRegistry sessions) : IRuntimeProjectionIndexWriter
{
    public async ValueTask UpsertAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default)
    {
        var hydrated = DurableProjectionMapper.ToAccountProjection(projection);
        await indexWriter.UpsertAsync(hydrated, cancellationToken).ConfigureAwait(false);
        sessions.Publish(SkyPulseQueryRow.FromProjection(hydrated));
    }

    public async ValueTask RemoveAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
    {
        await indexWriter.RemoveAsync(accountKey, cancellationToken).ConfigureAwait(false);
        sessions.PublishRemoval(accountKey);
    }
}

/// <summary>
/// Hydrates bounded result pages only from complete PostgreSQL-published upserts.
/// </summary>
internal sealed class PostgreSqlPublishedProjectionStore(
    PostgreSqlProjectionRuntimeStore store) : IProjectionStore
{
    public async ValueTask<AccountProjection?> GetAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        var snapshots = await store
            .ReadPublishedUpsertsAsync([accountKey], cancellationToken)
            .ConfigureAwait(false);
        return snapshots.TryGetValue(accountKey, out var snapshot)
            ? DurableProjectionMapper.ToAccountProjection(snapshot)
            : null;
    }

    public async ValueTask<IReadOnlyDictionary<AccountKey, AccountProjection>> GetManyAsync(
        IReadOnlyList<AccountKey> accountKeys,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(accountKeys);
        if (accountKeys.Any(static accountKey => !accountKey.IsValid))
        {
            throw new ArgumentException("Every account key must be valid.", nameof(accountKeys));
        }

        var snapshots = await store
            .ReadPublishedUpsertsAsync(accountKeys, cancellationToken)
            .ConfigureAwait(false);
        var projections = snapshots.ToDictionary(
            static pair => pair.Key,
            static pair => DurableProjectionMapper.ToAccountProjection(pair.Value));
        return new ReadOnlyDictionary<AccountKey, AccountProjection>(projections);
    }

    public ValueTask UpsertAsync(
        AccountProjection projection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(projection);
        cancellationToken.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "Direct projection writes are disabled in Durable mode; only the PostgreSQL transition and outbox path may publish state.");
    }
}

internal static class DurableProjectionMapper
{
    internal static AccountProjection ToAccountProjection(ProjectionSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (!snapshot.IsComplete || snapshot.Operation != ProjectionOperation.Upsert)
        {
            throw new ArgumentException(
                "Only a complete durable upsert can be hydrated as an account projection.",
                nameof(snapshot));
        }

        return new AccountProjection(
            snapshot.AccountKey,
            snapshot.LastActivityMinuteUtc,
            new RollingWindowCounts(
                snapshot.CreatedRecordCount1Day,
                snapshot.CreatedRecordCount7Days,
                snapshot.CreatedRecordCount30Days),
            new RollingWindowCounts(
                snapshot.UpdatedRecordCount1Day,
                snapshot.UpdatedRecordCount7Days,
                snapshot.UpdatedRecordCount30Days),
            new RollingWindowCounts(
                snapshot.DeletedRecordCount1Day,
                snapshot.DeletedRecordCount7Days,
                snapshot.DeletedRecordCount30Days),
            snapshot.CurrentPostCount,
            snapshot.CurrentFollowingCount,
            snapshot.CurrentFollowerCount,
            new RollingWindowCounts(
                snapshot.PostCreates1Day,
                snapshot.PostCreates7Days,
                snapshot.PostCreates30Days),
            snapshot.ReceivedEngagementCreates30Days);
    }
}
