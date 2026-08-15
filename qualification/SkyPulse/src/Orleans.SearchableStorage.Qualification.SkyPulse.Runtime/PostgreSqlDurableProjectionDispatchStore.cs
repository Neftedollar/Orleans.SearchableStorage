using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

/// <summary>
/// Adapts the reviewed PostgreSQL stores to the durable runtime boundary.
/// </summary>
public sealed class PostgreSqlDurableProjectionDispatchStore : IDurableProjectionDispatchStore
{
    private readonly PostgreSqlProjectionRuntimeStore _runtimeStore;
    private readonly PostgreSqlDispatchStore _dispatchStore;

    public PostgreSqlDurableProjectionDispatchStore(
        PostgreSqlProjectionRuntimeStore runtimeStore,
        PostgreSqlDispatchStore dispatchStore)
    {
        ArgumentNullException.ThrowIfNull(runtimeStore);
        ArgumentNullException.ThrowIfNull(dispatchStore);
        _runtimeStore = runtimeStore;
        _dispatchStore = dispatchStore;
    }

    public async Task<IProjectionDispatcherIncarnation?> TryAcquireIncarnationAsync(
        CancellationToken cancellationToken = default)
    {
        var incarnation = await _runtimeStore
            .TryAcquireDispatcherIncarnationAsync(cancellationToken)
            .ConfigureAwait(false);
        return incarnation is null ? null : new PostgreSqlIncarnationAdapter(incarnation);
    }

    public Task<IReadOnlyList<ProjectionSnapshot>> ReadDesiredProjectionPageAsync(
        AccountKey? afterAccountKeyExclusive,
        int batchSize,
        CancellationToken cancellationToken = default)
        => _runtimeStore.ReadDesiredProjectionPageAsync(
            afterAccountKeyExclusive,
            batchSize,
            cancellationToken);

    public Task<bool> MaterializeDesiredProjectionAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default)
        => _runtimeStore.MaterializeDesiredProjectionAsync(projection, cancellationToken);

    public Task<bool> FinalizeRebuildProjectionAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default)
        => _runtimeStore.FinalizeRebuildProjectionAsync(projection, cancellationToken);

    public Task<IReadOnlyList<ProjectionOutboxLease>> LeaseProjectionsAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
        => _dispatchStore.LeaseProjectionsAsync(batchSize, leaseDuration, cancellationToken);

    public Task<bool> PrepareProjectionHydrationAsync(
        ProjectionOutboxLease lease,
        CancellationToken cancellationToken = default)
        => _dispatchStore.PrepareProjectionHydrationAsync(lease, cancellationToken);

    public Task<bool> FinalizeProjectionAsync(
        ProjectionOutboxLease lease,
        CancellationToken cancellationToken = default)
        => _dispatchStore.FinalizeProjectionAsync(lease, cancellationToken);

    public Task<bool> FailProjectionAsync(
        ProjectionOutboxLease lease,
        DateTimeOffset availableAtUtc,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken = default)
        => _dispatchStore.FailProjectionAsync(
            lease,
            availableAtUtc,
            errorCode,
            errorMessage,
            cancellationToken);

    private sealed class PostgreSqlIncarnationAdapter(PostgreSqlDispatcherIncarnationLock inner)
        : IProjectionDispatcherIncarnation
    {
        public ValueTask<bool> IsHeldAsync(CancellationToken cancellationToken = default)
            => inner.IsHeldAsync(cancellationToken);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
