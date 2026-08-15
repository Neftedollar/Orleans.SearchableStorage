namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

public sealed class QuerySessionOptions
{
    public int MaximumConcurrentSessions { get; set; } = 256;

    public int UpdateBufferCapacity { get; set; } = 256;

    public TimeSpan SessionTimeToLive { get; set; } = TimeSpan.FromMinutes(10);

    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(1);

    public void Validate()
    {
        if (MaximumConcurrentSessions <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaximumConcurrentSessions),
                MaximumConcurrentSessions,
                "The concurrent session limit must be positive.");
        }

        if (UpdateBufferCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(UpdateBufferCapacity),
                UpdateBufferCapacity,
                "The update buffer capacity must be positive.");
        }

        ValidatePositive(SessionTimeToLive, nameof(SessionTimeToLive));
        ValidatePositive(HeartbeatInterval, nameof(HeartbeatInterval));
        ValidatePositive(CleanupInterval, nameof(CleanupInterval));
    }

    private static void ValidatePositive(TimeSpan value, string propertyName)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(propertyName, value, "The duration must be positive.");
        }
    }
}
