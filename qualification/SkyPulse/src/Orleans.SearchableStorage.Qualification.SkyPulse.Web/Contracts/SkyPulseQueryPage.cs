namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Contains the browser-safe metadata fields for one grain returned by a SkyPulse query.
/// </summary>
public sealed record SkyPulseQueryRow
{
    private SkyPulseQueryRow()
    {
    }

    public required string GrainId { get; init; }

    public long LastActivityMinuteUtc { get; init; }

    public long CreatedRecordCount1Day { get; init; }

    public long CreatedRecordCount7Days { get; init; }

    public long CreatedRecordCount30Days { get; init; }

    public long UpdatedRecordCount1Day { get; init; }

    public long UpdatedRecordCount7Days { get; init; }

    public long UpdatedRecordCount30Days { get; init; }

    public long DeletedRecordCount1Day { get; init; }

    public long DeletedRecordCount7Days { get; init; }

    public long DeletedRecordCount30Days { get; init; }

    public long CurrentPostCount { get; init; }

    public long CurrentFollowingCount { get; init; }

    public long CurrentFollowerCount { get; init; }

    public long PostCreates1Day { get; init; }

    public long PostCreates7Days { get; init; }

    public long PostCreates30Days { get; init; }

    public long ReceivedEngagementCreates30Days { get; init; }

    public static SkyPulseQueryRow FromProjection(AccountProjection projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        return new SkyPulseQueryRow
        {
            GrainId = projection.AccountKey.ToString(),
            LastActivityMinuteUtc = projection.LastActivityMinuteUtc,
            CreatedRecordCount1Day = projection.CreatedRecordCount1Day,
            CreatedRecordCount7Days = projection.CreatedRecordCount7Days,
            CreatedRecordCount30Days = projection.CreatedRecordCount30Days,
            UpdatedRecordCount1Day = projection.UpdatedRecordCount1Day,
            UpdatedRecordCount7Days = projection.UpdatedRecordCount7Days,
            UpdatedRecordCount30Days = projection.UpdatedRecordCount30Days,
            DeletedRecordCount1Day = projection.DeletedRecordCount1Day,
            DeletedRecordCount7Days = projection.DeletedRecordCount7Days,
            DeletedRecordCount30Days = projection.DeletedRecordCount30Days,
            CurrentPostCount = projection.CurrentPostCount,
            CurrentFollowingCount = projection.CurrentFollowingCount,
            CurrentFollowerCount = projection.CurrentFollowerCount,
            PostCreates1Day = projection.PostCreates1Day,
            PostCreates7Days = projection.PostCreates7Days,
            PostCreates30Days = projection.PostCreates30Days,
            ReceivedEngagementCreates30Days = projection.ReceivedEngagementCreates30Days,
        };
    }
}

/// <summary>
/// Contains one canonically ordered, bounded page of SkyPulse grain IDs and projections.
/// </summary>
public sealed record SkyPulseQueryPage(IReadOnlyList<SkyPulseQueryRow> Rows, string? ContinuationToken);

/// <summary>
/// Describes a query session and its current materialized page.
/// </summary>
public sealed record QuerySessionSnapshot(Guid SessionId, SkyPulseQueryPage Page, DateTimeOffset ExpiresAtUtc);
