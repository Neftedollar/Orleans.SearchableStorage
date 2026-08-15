using Orleans.SearchableStorage;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Contains exactly the 17 scalar values retained by the payload-free SkyPulse index.
/// </summary>
/// <remarks>
/// The account key is the Orleans grain identifier, not another posting. This type contains no
/// DID, handle, post text, profile data, media metadata, or other application payload.
/// </remarks>
[GenerateSerializer]
public sealed class AccountIndexState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long LastActivityMinuteUtc { get; set; }

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long CreatedRecordCount1Day { get; set; }

    [Id(2)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long CreatedRecordCount7Days { get; set; }

    [Id(3)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long CreatedRecordCount30Days { get; set; }

    [Id(4)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long UpdatedRecordCount1Day { get; set; }

    [Id(5)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long UpdatedRecordCount7Days { get; set; }

    [Id(6)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long UpdatedRecordCount30Days { get; set; }

    [Id(7)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long DeletedRecordCount1Day { get; set; }

    [Id(8)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long DeletedRecordCount7Days { get; set; }

    [Id(9)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long DeletedRecordCount30Days { get; set; }

    [Id(10)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long CurrentPostCount { get; set; }

    [Id(11)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long CurrentFollowingCount { get; set; }

    [Id(12)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long CurrentFollowerCount { get; set; }

    [Id(13)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long PostCreates1Day { get; set; }

    [Id(14)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long PostCreates7Days { get; set; }

    [Id(15)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long PostCreates30Days { get; set; }

    [Id(16)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public long ReceivedEngagementCreates30Days { get; set; }

    internal static AccountIndexState FromProjection(AccountProjection projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        return new AccountIndexState
        {
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
