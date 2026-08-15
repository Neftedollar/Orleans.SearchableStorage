namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Defines the metadata-only searchable projection for one admitted AT Protocol account.
/// </summary>
/// <remarks>
/// The projection deliberately contains no post text, profile data, media metadata, handles,
/// or raw DIDs. Post counters include every <c>app.bsky.feed.post</c> record: ordinary posts,
/// replies, quote posts, and quote replies are not split into separate indexed fields.
/// </remarks>
public sealed class AccountProjection
{
    internal AccountProjection(
        AccountKey accountKey,
        long lastActivityMinuteUtc,
        RollingWindowCounts createdRecordCounts,
        RollingWindowCounts updatedRecordCounts,
        RollingWindowCounts deletedRecordCounts,
        long currentPostCount,
        long currentFollowingCount,
        long currentFollowerCount,
        RollingWindowCounts postCreateCounts,
        long receivedEngagementCreates30Days)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        ValidateNonNegative(lastActivityMinuteUtc, nameof(lastActivityMinuteUtc));
        ValidateNonNegative(currentPostCount, nameof(currentPostCount));
        ValidateNonNegative(currentFollowingCount, nameof(currentFollowingCount));
        ValidateNonNegative(currentFollowerCount, nameof(currentFollowerCount));
        ValidateNonNegative(receivedEngagementCreates30Days, nameof(receivedEngagementCreates30Days));

        if (postCreateCounts.OneDay > createdRecordCounts.OneDay
            || postCreateCounts.SevenDays > createdRecordCounts.SevenDays
            || postCreateCounts.ThirtyDays > createdRecordCounts.ThirtyDays)
        {
            throw new ArgumentException(
                "Post creates must be a subset of all record creates in every time window.",
                nameof(postCreateCounts));
        }

        AccountKey = accountKey;
        LastActivityMinuteUtc = lastActivityMinuteUtc;
        CreatedRecordCount1Day = createdRecordCounts.OneDay;
        CreatedRecordCount7Days = createdRecordCounts.SevenDays;
        CreatedRecordCount30Days = createdRecordCounts.ThirtyDays;
        UpdatedRecordCount1Day = updatedRecordCounts.OneDay;
        UpdatedRecordCount7Days = updatedRecordCounts.SevenDays;
        UpdatedRecordCount30Days = updatedRecordCounts.ThirtyDays;
        DeletedRecordCount1Day = deletedRecordCounts.OneDay;
        DeletedRecordCount7Days = deletedRecordCounts.SevenDays;
        DeletedRecordCount30Days = deletedRecordCounts.ThirtyDays;
        CurrentPostCount = currentPostCount;
        CurrentFollowingCount = currentFollowingCount;
        CurrentFollowerCount = currentFollowerCount;
        PostCreates1Day = postCreateCounts.OneDay;
        PostCreates7Days = postCreateCounts.SevenDays;
        PostCreates30Days = postCreateCounts.ThirtyDays;
        ReceivedEngagementCreates30Days = receivedEngagementCreates30Days;
    }

    /// <summary>
    /// Gets the stable SHA-256 account identifier used as the grain key.
    /// </summary>
    public AccountKey AccountKey { get; }

    /// <summary>
    /// Gets the UTC Unix minute containing the most recently observed admitted-account activity.
    /// </summary>
    public long LastActivityMinuteUtc { get; }

    public long CreatedRecordCount1Day { get; }

    public long CreatedRecordCount7Days { get; }

    public long CreatedRecordCount30Days { get; }

    public long UpdatedRecordCount1Day { get; }

    public long UpdatedRecordCount7Days { get; }

    public long UpdatedRecordCount30Days { get; }

    public long DeletedRecordCount1Day { get; }

    public long DeletedRecordCount7Days { get; }

    public long DeletedRecordCount30Days { get; }

    /// <summary>
    /// Gets the current number of all feed-post records, including replies and quote posts.
    /// </summary>
    public long CurrentPostCount { get; }

    public long CurrentFollowingCount { get; }

    public long CurrentFollowerCount { get; }

    /// <summary>
    /// Gets all feed-post creates in the trailing one-day window, including replies and quotes.
    /// </summary>
    public long PostCreates1Day { get; }

    /// <summary>
    /// Gets all feed-post creates in the trailing seven-day window, including replies and quotes.
    /// </summary>
    public long PostCreates7Days { get; }

    /// <summary>
    /// Gets all feed-post creates in the trailing thirty-day window, including replies and quotes.
    /// </summary>
    public long PostCreates30Days { get; }

    public long ReceivedEngagementCreates30Days { get; }

    private static void ValidateNonNegative(long value, string parameterName)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "A projection value cannot be negative.");
        }
    }
}
