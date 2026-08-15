namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Defines an inclusive minimum and maximum for one fixed numeric projection field.
/// </summary>
public sealed record LongRangeFilter
{
    public long? Minimum { get; init; }

    public long? Maximum { get; init; }

    internal bool Matches(long value)
        => (!Minimum.HasValue || value >= Minimum.Value)
            && (!Maximum.HasValue || value <= Maximum.Value);

    internal void Validate(string fieldName)
    {
        if (Minimum < 0)
        {
            throw new ArgumentOutOfRangeException(fieldName, Minimum, "A minimum cannot be negative.");
        }

        if (Maximum < 0)
        {
            throw new ArgumentOutOfRangeException(fieldName, Maximum, "A maximum cannot be negative.");
        }

        if (Minimum > Maximum)
        {
            throw new ArgumentException("The minimum cannot exceed the maximum.", fieldName);
        }
    }
}

/// <summary>
/// Defines one bounded SkyPulse query over the fixed searchable projection.
/// </summary>
/// <remarks>
/// The request is intentionally not an expression tree or arbitrary LINQ surface. Each property
/// corresponds to one reviewed index field and every returned page is capped at 100 grain IDs.
/// </remarks>
public sealed record SkyPulseQueryRequest
{
    public const int MaximumPageSize = 100;

    public const int MaximumContinuationTokenLength = 2_048;

    public int PageSize { get; init; } = 50;

    public string? ContinuationToken { get; init; }

    public LongRangeFilter? LastActivityMinuteUtc { get; init; }

    public LongRangeFilter? CreatedRecordCount1Day { get; init; }

    public LongRangeFilter? CreatedRecordCount7Days { get; init; }

    public LongRangeFilter? CreatedRecordCount30Days { get; init; }

    public LongRangeFilter? UpdatedRecordCount1Day { get; init; }

    public LongRangeFilter? UpdatedRecordCount7Days { get; init; }

    public LongRangeFilter? UpdatedRecordCount30Days { get; init; }

    public LongRangeFilter? DeletedRecordCount1Day { get; init; }

    public LongRangeFilter? DeletedRecordCount7Days { get; init; }

    public LongRangeFilter? DeletedRecordCount30Days { get; init; }

    public LongRangeFilter? CurrentPostCount { get; init; }

    public LongRangeFilter? CurrentFollowingCount { get; init; }

    public LongRangeFilter? CurrentFollowerCount { get; init; }

    public LongRangeFilter? PostCreates1Day { get; init; }

    public LongRangeFilter? PostCreates7Days { get; init; }

    public LongRangeFilter? PostCreates30Days { get; init; }

    public LongRangeFilter? ReceivedEngagementCreates30Days { get; init; }

    internal bool HasAnyRangeBound
        => HasBound(LastActivityMinuteUtc)
            || HasBound(CreatedRecordCount1Day)
            || HasBound(CreatedRecordCount7Days)
            || HasBound(CreatedRecordCount30Days)
            || HasBound(UpdatedRecordCount1Day)
            || HasBound(UpdatedRecordCount7Days)
            || HasBound(UpdatedRecordCount30Days)
            || HasBound(DeletedRecordCount1Day)
            || HasBound(DeletedRecordCount7Days)
            || HasBound(DeletedRecordCount30Days)
            || HasBound(CurrentPostCount)
            || HasBound(CurrentFollowingCount)
            || HasBound(CurrentFollowerCount)
            || HasBound(PostCreates1Day)
            || HasBound(PostCreates7Days)
            || HasBound(PostCreates30Days)
            || HasBound(ReceivedEngagementCreates30Days);

    public void Validate()
    {
        if (PageSize is < 1 or > MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(PageSize),
                PageSize,
                $"Page size must be between 1 and {MaximumPageSize}.");
        }

        if (ContinuationToken is { Length: > MaximumContinuationTokenLength })
        {
            throw new ArgumentException("The continuation token is too long.", nameof(ContinuationToken));
        }

        LastActivityMinuteUtc?.Validate(nameof(LastActivityMinuteUtc));
        CreatedRecordCount1Day?.Validate(nameof(CreatedRecordCount1Day));
        CreatedRecordCount7Days?.Validate(nameof(CreatedRecordCount7Days));
        CreatedRecordCount30Days?.Validate(nameof(CreatedRecordCount30Days));
        UpdatedRecordCount1Day?.Validate(nameof(UpdatedRecordCount1Day));
        UpdatedRecordCount7Days?.Validate(nameof(UpdatedRecordCount7Days));
        UpdatedRecordCount30Days?.Validate(nameof(UpdatedRecordCount30Days));
        DeletedRecordCount1Day?.Validate(nameof(DeletedRecordCount1Day));
        DeletedRecordCount7Days?.Validate(nameof(DeletedRecordCount7Days));
        DeletedRecordCount30Days?.Validate(nameof(DeletedRecordCount30Days));
        CurrentPostCount?.Validate(nameof(CurrentPostCount));
        CurrentFollowingCount?.Validate(nameof(CurrentFollowingCount));
        CurrentFollowerCount?.Validate(nameof(CurrentFollowerCount));
        PostCreates1Day?.Validate(nameof(PostCreates1Day));
        PostCreates7Days?.Validate(nameof(PostCreates7Days));
        PostCreates30Days?.Validate(nameof(PostCreates30Days));
        ReceivedEngagementCreates30Days?.Validate(nameof(ReceivedEngagementCreates30Days));
    }

    internal bool Matches(AccountProjection projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        return Matches(LastActivityMinuteUtc, projection.LastActivityMinuteUtc)
            && Matches(CreatedRecordCount1Day, projection.CreatedRecordCount1Day)
            && Matches(CreatedRecordCount7Days, projection.CreatedRecordCount7Days)
            && Matches(CreatedRecordCount30Days, projection.CreatedRecordCount30Days)
            && Matches(UpdatedRecordCount1Day, projection.UpdatedRecordCount1Day)
            && Matches(UpdatedRecordCount7Days, projection.UpdatedRecordCount7Days)
            && Matches(UpdatedRecordCount30Days, projection.UpdatedRecordCount30Days)
            && Matches(DeletedRecordCount1Day, projection.DeletedRecordCount1Day)
            && Matches(DeletedRecordCount7Days, projection.DeletedRecordCount7Days)
            && Matches(DeletedRecordCount30Days, projection.DeletedRecordCount30Days)
            && Matches(CurrentPostCount, projection.CurrentPostCount)
            && Matches(CurrentFollowingCount, projection.CurrentFollowingCount)
            && Matches(CurrentFollowerCount, projection.CurrentFollowerCount)
            && Matches(PostCreates1Day, projection.PostCreates1Day)
            && Matches(PostCreates7Days, projection.PostCreates7Days)
            && Matches(PostCreates30Days, projection.PostCreates30Days)
            && Matches(ReceivedEngagementCreates30Days, projection.ReceivedEngagementCreates30Days);
    }

    internal bool Matches(SkyPulseQueryRow projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        return Matches(LastActivityMinuteUtc, projection.LastActivityMinuteUtc)
            && Matches(CreatedRecordCount1Day, projection.CreatedRecordCount1Day)
            && Matches(CreatedRecordCount7Days, projection.CreatedRecordCount7Days)
            && Matches(CreatedRecordCount30Days, projection.CreatedRecordCount30Days)
            && Matches(UpdatedRecordCount1Day, projection.UpdatedRecordCount1Day)
            && Matches(UpdatedRecordCount7Days, projection.UpdatedRecordCount7Days)
            && Matches(UpdatedRecordCount30Days, projection.UpdatedRecordCount30Days)
            && Matches(DeletedRecordCount1Day, projection.DeletedRecordCount1Day)
            && Matches(DeletedRecordCount7Days, projection.DeletedRecordCount7Days)
            && Matches(DeletedRecordCount30Days, projection.DeletedRecordCount30Days)
            && Matches(CurrentPostCount, projection.CurrentPostCount)
            && Matches(CurrentFollowingCount, projection.CurrentFollowingCount)
            && Matches(CurrentFollowerCount, projection.CurrentFollowerCount)
            && Matches(PostCreates1Day, projection.PostCreates1Day)
            && Matches(PostCreates7Days, projection.PostCreates7Days)
            && Matches(PostCreates30Days, projection.PostCreates30Days)
            && Matches(ReceivedEngagementCreates30Days, projection.ReceivedEngagementCreates30Days);
    }

    private static bool Matches(LongRangeFilter? filter, long value) => filter?.Matches(value) ?? true;

    private static bool HasBound(LongRangeFilter? filter)
        => filter is { Minimum: not null } or { Maximum: not null };
}
