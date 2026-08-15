namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

public sealed record CorpusProfileRequest
{
    public CorpusProfileRequest(string name, long accountCount)
    {
        if (string.IsNullOrWhiteSpace(name)
            || !string.Equals(name, name.Trim(), StringComparison.Ordinal)
            || name.Length > 80
            || name.Any(static character => !IsProfileCharacter(character)))
        {
            throw new ArgumentException(
                "A profile name must be 1-80 lowercase ASCII letters, digits, dots, dashes, or underscores.",
                nameof(name));
        }

        if (accountCount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(accountCount),
                accountCount,
                "A profile must contain at least one account.");
        }

        Name = name;
        AccountCount = accountCount;
    }

    public string Name { get; }

    public long AccountCount { get; }

    private static bool IsProfileCharacter(char character)
        => character is >= 'a' and <= 'z'
            or >= '0' and <= '9'
            or '.' or '-' or '_';
}

public sealed record CorpusFreezeOptions
{
    public required string JournalPath { get; init; }

    public required string OutputDirectory { get; init; }

    /// <summary>
    /// Maximum encoded observation payload retained in one in-memory sort batch.
    /// </summary>
    public long MemoryBudgetBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// Maximum number of sorted runs opened by one merge operation.
    /// </summary>
    public int MergeFanIn { get; init; } = 32;

    public bool WriteHumanReadableHex { get; init; }

    /// <summary>
    /// Exact prefixes to freeze. With no entries, one profile named <c>parent</c> is emitted.
    /// </summary>
    public IReadOnlyList<CorpusProfileRequest> Profiles { get; init; } = [];
}

public sealed record CorpusFreezeResult(
    string OutputDirectory,
    string ManifestPath,
    long AccountCount,
    int InitialSpillRunCount,
    CorpusManifest Manifest);

public sealed record CorpusVerificationResult(
    string ManifestPath,
    long AccountCount,
    bool DeepVerification,
    bool SourceJournalVerified);

public sealed record CorpusManifest(
    string Format,
    AccountKeyManifest AccountKey,
    SourceJournalManifest SourceJournal,
    ParentCorpusManifest Parent,
    IReadOnlyList<CorpusProfileManifest> Profiles,
    TextArtifactManifest? HumanReadableArtifact);

public sealed record AccountKeyManifest(string Algorithm, int ByteLength);

public sealed record SourceJournalManifest(string Format, long ByteLength, string Sha256);

public sealed record ParentCorpusManifest(
    string Artifact,
    long AccountCount,
    long ByteLength,
    string Sha256,
    string CorpusFingerprint);

public sealed record CorpusProfileManifest(
    string Name,
    long AccountCount,
    long ByteLength,
    string PrefixSha256);

public sealed record TextArtifactManifest(string Artifact, long ByteLength, string Sha256);
