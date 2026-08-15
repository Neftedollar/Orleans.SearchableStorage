namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Executes one bounded page query against the searchable index.
/// </summary>
public interface ISkyPulsePageQuery
{
    Task<SkyPulseQueryPage> QueryAsync(
        SkyPulseQueryRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Reads and writes the current metadata-only account projection.
/// </summary>
public interface IProjectionStore
{
    ValueTask<AccountProjection?> GetAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyDictionary<AccountKey, AccountProjection>> GetManyAsync(
        IReadOnlyList<AccountKey> accountKeys,
        CancellationToken cancellationToken = default);

    ValueTask UpsertAsync(
        AccountProjection projection,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Replaces or removes the searchable, payload-free projection for one account.
/// </summary>
/// <remarks>
/// This is deliberately separate from <see cref="IProjectionStore"/>: the external payload and
/// the Orleans index have independent consistency boundaries. The durable dispatcher owns their
/// ordering and crash policy.
/// </remarks>
public interface IProjectionIndexWriter
{
    ValueTask UpsertAsync(
        AccountProjection projection,
        CancellationToken cancellationToken = default);

    ValueTask RemoveAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default);
}
