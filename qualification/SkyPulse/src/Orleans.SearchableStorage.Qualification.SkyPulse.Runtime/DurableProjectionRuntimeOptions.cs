namespace Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

/// <summary>
/// Bounds rebuild and dispatch work for the single co-located Memory-silo process.
/// </summary>
public sealed class DurableProjectionRuntimeOptions
{
    public const int MaximumBatchSize = 1_000;

    public int RebuildPageSize { get; init; } = 256;

    public int DispatchBatchSize { get; init; } = 64;

    public TimeSpan DispatchLeaseDuration { get; init; } = TimeSpan.FromMinutes(5);

    public TimeSpan PreIndexFailureDelay { get; init; } = TimeSpan.FromSeconds(5);

    internal void Validate()
    {
        ValidateBatch(RebuildPageSize, nameof(RebuildPageSize));
        ValidateBatch(DispatchBatchSize, nameof(DispatchBatchSize));
        if (DispatchLeaseDuration < TimeSpan.FromSeconds(1)
            || DispatchLeaseDuration > TimeSpan.FromMinutes(15))
        {
            throw new ArgumentOutOfRangeException(
                nameof(DispatchLeaseDuration),
                DispatchLeaseDuration,
                "The dispatch lease must be between one second and fifteen minutes.");
        }

        if (PreIndexFailureDelay < TimeSpan.Zero
            || PreIndexFailureDelay > TimeSpan.FromHours(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(PreIndexFailureDelay),
                PreIndexFailureDelay,
                "The pre-index retry delay must be between zero and one hour.");
        }
    }

    private static void ValidateBatch(int value, string parameterName)
    {
        if (value is < 1 or > MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                value,
                $"A batch size must be between 1 and {MaximumBatchSize}.");
        }
    }
}
