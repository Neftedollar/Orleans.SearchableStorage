namespace Orleans.SearchableStorage.Qualification.SkyPulse;

/// <summary>
/// Names a reproducible prefix of a frozen account allowlist.
/// </summary>
public sealed record CappedCorpusProfile
{
    public CappedCorpusProfile(string name, int maximumAccounts)
    {
        if (string.IsNullOrWhiteSpace(name) || !string.Equals(name, name.Trim(), StringComparison.Ordinal))
        {
            throw new ArgumentException("A non-empty canonical profile name is required.", nameof(name));
        }

        if (maximumAccounts <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumAccounts),
                maximumAccounts,
                "The maximum account count must be greater than zero.");
        }

        Name = name;
        MaximumAccounts = maximumAccounts;
    }

    public string Name { get; }

    public int MaximumAccounts { get; }
}
