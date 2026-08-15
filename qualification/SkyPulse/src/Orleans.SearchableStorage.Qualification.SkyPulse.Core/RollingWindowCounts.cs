namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Contains exact event counts for nested trailing one-day, seven-day, and thirty-day windows.
/// </summary>
public readonly record struct RollingWindowCounts
{
    public RollingWindowCounts(long oneDay, long sevenDays, long thirtyDays)
    {
        ValidateNonNegative(oneDay, nameof(oneDay));
        ValidateNonNegative(sevenDays, nameof(sevenDays));
        ValidateNonNegative(thirtyDays, nameof(thirtyDays));

        if (oneDay > sevenDays || sevenDays > thirtyDays)
        {
            throw new ArgumentException(
                "Counts must be monotonic across the nested one-day, seven-day, and thirty-day windows.");
        }

        OneDay = oneDay;
        SevenDays = sevenDays;
        ThirtyDays = thirtyDays;
    }

    public long OneDay { get; }

    public long SevenDays { get; }

    public long ThirtyDays { get; }

    private static void ValidateNonNegative(long value, string parameterName)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "A count cannot be negative.");
        }
    }
}
