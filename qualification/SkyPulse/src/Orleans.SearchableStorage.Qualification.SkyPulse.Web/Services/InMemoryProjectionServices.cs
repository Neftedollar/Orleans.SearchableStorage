using System.Collections.Concurrent;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Stores only projections supplied by an ingestion adapter; it never generates demo records.
/// </summary>
public sealed class InMemoryProjectionStore : IProjectionStore
{
    private readonly ConcurrentDictionary<AccountKey, AccountProjection> _projections = new();

    public ValueTask<AccountProjection?> GetAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _projections.TryGetValue(accountKey, out var projection);
        return ValueTask.FromResult(projection);
    }

    public ValueTask UpsertAsync(
        AccountProjection projection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(projection);
        cancellationToken.ThrowIfCancellationRequested();
        _projections[projection.AccountKey] = projection;
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyDictionary<AccountKey, AccountProjection>> GetManyAsync(
        IReadOnlyList<AccountKey> accountKeys,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(accountKeys);
        cancellationToken.ThrowIfCancellationRequested();

        var found = new Dictionary<AccountKey, AccountProjection>(accountKeys.Count);
        foreach (var accountKey in accountKeys)
        {
            if (!accountKey.IsValid)
            {
                throw new ArgumentException("Every account key must be valid.", nameof(accountKeys));
            }

            if (_projections.TryGetValue(accountKey, out var projection))
            {
                found[accountKey] = projection;
            }
        }

        return ValueTask.FromResult<IReadOnlyDictionary<AccountKey, AccountProjection>>(found);
    }

    internal AccountProjection[] Snapshot() => _projections.Values.ToArray();
}

/// <summary>
/// Provides a deterministic in-process adapter for local wiring and contract tests.
/// </summary>
/// <remarks>
/// Qualification runs replace this service with the package-only SearchableStorage adapter.
/// The in-memory service only exposes projections already supplied by the real source adapter.
/// </remarks>
public sealed class InMemorySkyPulsePageQuery(InMemoryProjectionStore store) : ISkyPulsePageQuery
{
    public Task<SkyPulseQueryPage> QueryAsync(
        SkyPulseQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        request.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        var orderedRows = store.Snapshot()
            .Where(request.Matches)
            .Select(SkyPulseQueryRow.FromProjection)
            .OrderBy(static row => row.GrainId, StringComparer.Ordinal);

        if (!string.IsNullOrEmpty(request.ContinuationToken))
        {
            orderedRows = orderedRows
                .Where(row => string.CompareOrdinal(row.GrainId, request.ContinuationToken) > 0)
                .OrderBy(static row => row.GrainId, StringComparer.Ordinal);
        }

        var lookahead = orderedRows.Take(request.PageSize + 1).ToArray();
        var rows = lookahead.Take(request.PageSize).ToArray();
        var continuationToken = lookahead.Length > request.PageSize ? rows[^1].GrainId : null;

        return Task.FromResult<SkyPulseQueryPage>(new(rows, continuationToken));
    }
}
