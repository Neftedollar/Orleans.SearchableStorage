using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.SearchableStorage;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Executes the fixed browser query surface against the package-only index and hydrates only the
/// returned bounded page from the application-owned projection store.
/// </summary>
public sealed class SearchableStorageSkyPulsePageQuery(
    [FromKeyedServices(SkyPulseIndexContract.ProviderName)] ISearchableStorageQueryClient search,
    IProjectionStore projectionStore) : ISkyPulsePageQuery
{
    private static readonly GrainType ExpectedGrainType = GrainType.Create(SkyPulseIndexContract.GrainType);

    public async Task<SkyPulseQueryPage> QueryAsync(
        SkyPulseQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        request.Validate();

        var query = search.Query<AccountIndexState>(SkyPulseIndexContract.StateName);
        query = ApplyFilters(query, request);

        var page = await query.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(request.PageSize, request.ContinuationToken),
                cancellationToken)
            .ConfigureAwait(false);

        var accountKeys = new AccountKey[page.Items.Count];
        for (var index = 0; index < page.Items.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (page.Items[index].Type != ExpectedGrainType)
            {
                throw new InvalidOperationException(
                    "The searchable index returned an unexpected SkyPulse grain type.");
            }

            if (!AccountKey.TryParse(page.Items[index].Key.ToString(), out var accountKey))
            {
                throw new InvalidOperationException(
                    "The searchable index returned a non-canonical SkyPulse account key.");
            }

            accountKeys[index] = accountKey;
        }

        var projections = await projectionStore.GetManyAsync(accountKeys, cancellationToken)
            .ConfigureAwait(false);
        var rows = new List<SkyPulseQueryRow>(accountKeys.Length);
        foreach (var accountKey in accountKeys)
        {
            if (projections.TryGetValue(accountKey, out var projection)
                && request.Matches(projection))
            {
                rows.Add(SkyPulseQueryRow.FromProjection(projection));
            }
        }

        return new SkyPulseQueryPage(rows.AsReadOnly(), page.ContinuationToken);
    }

    private static IQueryable<AccountIndexState> ApplyFilters(
        IQueryable<AccountIndexState> query,
        SkyPulseQueryRequest request)
    {
        if (!request.HasAnyRangeBound)
        {
            // Shipping identifier queries intentionally reject an unfiltered root. Every admitted
            // projection has a non-negative minute, so this is the explicit bounded "all records"
            // predicate used by the default UI rather than a hidden full-index fallback.
            query = query.Where(state => state.LastActivityMinuteUtc >= 0);
        }

        if (request.LastActivityMinuteUtc?.Minimum is { } lastActivityMinimum)
        {
            query = query.Where(state => state.LastActivityMinuteUtc >= lastActivityMinimum);
        }

        if (request.LastActivityMinuteUtc?.Maximum is { } lastActivityMaximum)
        {
            query = query.Where(state => state.LastActivityMinuteUtc <= lastActivityMaximum);
        }

        if (request.CreatedRecordCount1Day?.Minimum is { } created1DayMinimum)
        {
            query = query.Where(state => state.CreatedRecordCount1Day >= created1DayMinimum);
        }

        if (request.CreatedRecordCount1Day?.Maximum is { } created1DayMaximum)
        {
            query = query.Where(state => state.CreatedRecordCount1Day <= created1DayMaximum);
        }

        if (request.CreatedRecordCount7Days?.Minimum is { } created7DaysMinimum)
        {
            query = query.Where(state => state.CreatedRecordCount7Days >= created7DaysMinimum);
        }

        if (request.CreatedRecordCount7Days?.Maximum is { } created7DaysMaximum)
        {
            query = query.Where(state => state.CreatedRecordCount7Days <= created7DaysMaximum);
        }

        if (request.CreatedRecordCount30Days?.Minimum is { } created30DaysMinimum)
        {
            query = query.Where(state => state.CreatedRecordCount30Days >= created30DaysMinimum);
        }

        if (request.CreatedRecordCount30Days?.Maximum is { } created30DaysMaximum)
        {
            query = query.Where(state => state.CreatedRecordCount30Days <= created30DaysMaximum);
        }

        if (request.UpdatedRecordCount1Day?.Minimum is { } updated1DayMinimum)
        {
            query = query.Where(state => state.UpdatedRecordCount1Day >= updated1DayMinimum);
        }

        if (request.UpdatedRecordCount1Day?.Maximum is { } updated1DayMaximum)
        {
            query = query.Where(state => state.UpdatedRecordCount1Day <= updated1DayMaximum);
        }

        if (request.UpdatedRecordCount7Days?.Minimum is { } updated7DaysMinimum)
        {
            query = query.Where(state => state.UpdatedRecordCount7Days >= updated7DaysMinimum);
        }

        if (request.UpdatedRecordCount7Days?.Maximum is { } updated7DaysMaximum)
        {
            query = query.Where(state => state.UpdatedRecordCount7Days <= updated7DaysMaximum);
        }

        if (request.UpdatedRecordCount30Days?.Minimum is { } updated30DaysMinimum)
        {
            query = query.Where(state => state.UpdatedRecordCount30Days >= updated30DaysMinimum);
        }

        if (request.UpdatedRecordCount30Days?.Maximum is { } updated30DaysMaximum)
        {
            query = query.Where(state => state.UpdatedRecordCount30Days <= updated30DaysMaximum);
        }

        if (request.DeletedRecordCount1Day?.Minimum is { } deleted1DayMinimum)
        {
            query = query.Where(state => state.DeletedRecordCount1Day >= deleted1DayMinimum);
        }

        if (request.DeletedRecordCount1Day?.Maximum is { } deleted1DayMaximum)
        {
            query = query.Where(state => state.DeletedRecordCount1Day <= deleted1DayMaximum);
        }

        if (request.DeletedRecordCount7Days?.Minimum is { } deleted7DaysMinimum)
        {
            query = query.Where(state => state.DeletedRecordCount7Days >= deleted7DaysMinimum);
        }

        if (request.DeletedRecordCount7Days?.Maximum is { } deleted7DaysMaximum)
        {
            query = query.Where(state => state.DeletedRecordCount7Days <= deleted7DaysMaximum);
        }

        if (request.DeletedRecordCount30Days?.Minimum is { } deleted30DaysMinimum)
        {
            query = query.Where(state => state.DeletedRecordCount30Days >= deleted30DaysMinimum);
        }

        if (request.DeletedRecordCount30Days?.Maximum is { } deleted30DaysMaximum)
        {
            query = query.Where(state => state.DeletedRecordCount30Days <= deleted30DaysMaximum);
        }

        if (request.CurrentPostCount?.Minimum is { } postCountMinimum)
        {
            query = query.Where(state => state.CurrentPostCount >= postCountMinimum);
        }

        if (request.CurrentPostCount?.Maximum is { } postCountMaximum)
        {
            query = query.Where(state => state.CurrentPostCount <= postCountMaximum);
        }

        if (request.CurrentFollowingCount?.Minimum is { } followingCountMinimum)
        {
            query = query.Where(state => state.CurrentFollowingCount >= followingCountMinimum);
        }

        if (request.CurrentFollowingCount?.Maximum is { } followingCountMaximum)
        {
            query = query.Where(state => state.CurrentFollowingCount <= followingCountMaximum);
        }

        if (request.CurrentFollowerCount?.Minimum is { } followerCountMinimum)
        {
            query = query.Where(state => state.CurrentFollowerCount >= followerCountMinimum);
        }

        if (request.CurrentFollowerCount?.Maximum is { } followerCountMaximum)
        {
            query = query.Where(state => state.CurrentFollowerCount <= followerCountMaximum);
        }

        if (request.PostCreates1Day?.Minimum is { } postCreates1DayMinimum)
        {
            query = query.Where(state => state.PostCreates1Day >= postCreates1DayMinimum);
        }

        if (request.PostCreates1Day?.Maximum is { } postCreates1DayMaximum)
        {
            query = query.Where(state => state.PostCreates1Day <= postCreates1DayMaximum);
        }

        if (request.PostCreates7Days?.Minimum is { } postCreates7DaysMinimum)
        {
            query = query.Where(state => state.PostCreates7Days >= postCreates7DaysMinimum);
        }

        if (request.PostCreates7Days?.Maximum is { } postCreates7DaysMaximum)
        {
            query = query.Where(state => state.PostCreates7Days <= postCreates7DaysMaximum);
        }

        if (request.PostCreates30Days?.Minimum is { } postCreates30DaysMinimum)
        {
            query = query.Where(state => state.PostCreates30Days >= postCreates30DaysMinimum);
        }

        if (request.PostCreates30Days?.Maximum is { } postCreates30DaysMaximum)
        {
            query = query.Where(state => state.PostCreates30Days <= postCreates30DaysMaximum);
        }

        if (request.ReceivedEngagementCreates30Days?.Minimum is { } engagementMinimum)
        {
            query = query.Where(state => state.ReceivedEngagementCreates30Days >= engagementMinimum);
        }

        if (request.ReceivedEngagementCreates30Days?.Maximum is { } engagementMaximum)
        {
            query = query.Where(state => state.ReceivedEngagementCreates30Days <= engagementMaximum);
        }

        return query;
    }
}
