namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

/// <summary>
/// Versioned, public format constants for a frozen SkyPulse account corpus.
/// </summary>
public static class CorpusFormat
{
    public const string ManifestFormat = "orleans-searchable-storage-skypulse-corpus-manifest-v1";

    public const string JournalFormat = "skypulse-sanitized-lifecycle-observation-ndjson-v1";

    public const string AccountKeyAlgorithm = "sha256-exact-utf8-canonical-did";

    public const string BinaryArtifactName = "accounts.ak32";

    public const string HumanArtifactName = "accounts.hex";

    public const string ManifestName = "corpus.manifest.json";

    public const int AccountKeyByteLength = 32;

    // This is deliberately kept byte-for-byte compatible with the versioned Core allowlist
    // format. The compatibility test opens builder output through FileBackedCorpusAdmission.
    internal const string FingerprintDomain = "orleans-searchable-storage-skypulse-corpus-v1\0";
}
