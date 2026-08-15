namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Defines the standard bounded SkyPulse qualification profiles.
/// </summary>
public static class SkyPulseProfiles
{
    public static CappedCorpusProfile OneMillion { get; } = new("skypulse-1m-v1", 1_000_000);

    public static CappedCorpusProfile TenMillion { get; } = new("skypulse-10m-v1", 10_000_000);
}
