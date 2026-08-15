using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

/// <summary>
/// Answers frozen-corpus membership without exposing the source DID or materializing the corpus.
/// </summary>
public interface IAccountAdmission
{
    bool IsAdmitted(AccountKey accountKey);
}

/// <summary>
/// Owns an exact, manifest-verified prefix of <c>accounts.ak32</c>.
/// </summary>
public sealed class VerifiedCorpusAdmission : IAccountAdmission, IDisposable
{
    private const int MaximumManifestBytes = 1024 * 1024;
    private const string ManifestFormat = "orleans-searchable-storage-skypulse-corpus-manifest-v1";
    private const string JournalFormat = "skypulse-sanitized-lifecycle-observation-ndjson-v1";
    private const string AccountKeyAlgorithm = "sha256-exact-utf8-canonical-did";
    private const string BinaryArtifactName = "accounts.ak32";
    private const string ManifestName = "corpus.manifest.json";

    private static readonly JsonSerializerOptions ManifestJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        NumberHandling = JsonNumberHandling.Strict,
        RespectNullableAnnotations = true,
        WriteIndented = false,
    };

    private readonly FileBackedCorpusAdmission _admission;

    private VerifiedCorpusAdmission(FileBackedCorpusAdmission admission)
    {
        _admission = admission;
    }

    public CappedCorpusProfile Profile => _admission.Profile;

    public string ProfilePrefixSha256 => _admission.ProfilePrefixSha256;

    public int Count => _admission.Count;

    public int ParentAccountCount => _admission.ParentAccountCount;

    public string ParentArtifactSha256 => _admission.ArtifactSha256;

    public string ParentCorpusFingerprint => _admission.AllowlistFingerprint;

    /// <summary>
    /// Opens the canonical manifest and its sibling binary corpus only after every configured
    /// profile identity and all parent/prefix hashes have been verified.
    /// </summary>
    public static VerifiedCorpusAdmission Open(
        string manifestPath,
        string expectedProfileId,
        long expectedCorpusCap,
        string expectedProfilePrefixSha256)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(manifestPath);
        ValidateProfileId(expectedProfileId);
        if (expectedCorpusCap is <= 0 or > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedCorpusCap),
                expectedCorpusCap,
                $"The file-backed qualification corpus cap must be between 1 and {int.MaxValue}.");
        }

        ValidateSha256(expectedProfilePrefixSha256, nameof(expectedProfilePrefixSha256));
        var canonicalManifestPath = Path.GetFullPath(manifestPath);
        if (!string.Equals(Path.GetFileName(canonicalManifestPath), ManifestName, StringComparison.Ordinal))
        {
            throw new InvalidDataException($"The frozen corpus manifest must be named '{ManifestName}'.");
        }

        var manifestInfo = new FileInfo(canonicalManifestPath);
        if (!manifestInfo.Exists)
        {
            throw new FileNotFoundException("The frozen corpus manifest does not exist.", canonicalManifestPath);
        }

        if (manifestInfo.Length is <= 0 or > MaximumManifestBytes)
        {
            throw new InvalidDataException(
                $"The frozen corpus manifest must contain between 1 and {MaximumManifestBytes} bytes.");
        }

        var manifestBytes = File.ReadAllBytes(canonicalManifestPath);
        FrozenCorpusManifest manifest;
        try
        {
            manifest = JsonSerializer.Deserialize<FrozenCorpusManifest>(manifestBytes, ManifestJson)
                ?? throw new InvalidDataException("The frozen corpus manifest is empty.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("The frozen corpus manifest is not valid canonical JSON.", exception);
        }

        ValidateManifest(manifest);
        var canonicalBytes = JsonSerializer.SerializeToUtf8Bytes(manifest, ManifestJson);
        if (manifestBytes.Length != canonicalBytes.Length + 1
            || manifestBytes[^1] != (byte)'\n'
            || !manifestBytes.AsSpan(0, canonicalBytes.Length).SequenceEqual(canonicalBytes))
        {
            throw new InvalidDataException("The frozen corpus manifest is not in its unique canonical JSON form.");
        }

        var matchingProfiles = manifest.Profiles
            .Where(profile => string.Equals(profile.Name, expectedProfileId, StringComparison.Ordinal))
            .ToArray();
        if (matchingProfiles.Length != 1)
        {
            throw new InvalidDataException("The configured profile identity is not unique in the frozen corpus manifest.");
        }

        var selected = matchingProfiles[0];
        if (selected.AccountCount != expectedCorpusCap
            || !string.Equals(
                selected.PrefixSha256,
                expectedProfilePrefixSha256,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException("The configured corpus profile does not match the frozen manifest.");
        }

        var binaryPath = Path.Combine(
            Path.GetDirectoryName(canonicalManifestPath)!,
            BinaryArtifactName);
        var admission = FileBackedCorpusAdmission.OpenVerified(
            binaryPath,
            checked((int)manifest.Parent.AccountCount),
            manifest.Parent.Sha256,
            manifest.Parent.CorpusFingerprint,
            new CappedCorpusProfile(selected.Name, checked((int)selected.AccountCount)),
            selected.PrefixSha256);
        return new VerifiedCorpusAdmission(admission);
    }

    public bool IsAdmitted(AccountKey accountKey) => _admission.IsAdmitted(accountKey);

    public IReadOnlyList<AccountKey> ReadPage(int startIndex, int pageSize)
        => _admission.ReadPage(startIndex, pageSize);

    public void Dispose() => _admission.Dispose();

    private static void ValidateManifest(FrozenCorpusManifest manifest)
    {
        if (!string.Equals(manifest.Format, ManifestFormat, StringComparison.Ordinal)
            || !string.Equals(manifest.AccountKey.Algorithm, AccountKeyAlgorithm, StringComparison.Ordinal)
            || manifest.AccountKey.ByteLength != AccountKey.ByteLength
            || !string.Equals(manifest.SourceJournal.Format, JournalFormat, StringComparison.Ordinal)
            || manifest.SourceJournal.ByteLength <= 0)
        {
            throw new InvalidDataException("The frozen corpus manifest contract is not supported.");
        }

        ValidateSha256(manifest.SourceJournal.Sha256, "sourceJournal.sha256");
        if (!string.Equals(manifest.Parent.Artifact, BinaryArtifactName, StringComparison.Ordinal)
            || manifest.Parent.AccountCount is <= 0 or > int.MaxValue
            || manifest.Parent.ByteLength != checked(manifest.Parent.AccountCount * AccountKey.ByteLength))
        {
            throw new InvalidDataException("The frozen parent corpus identity is invalid.");
        }

        ValidateSha256(manifest.Parent.Sha256, "parent.sha256");
        ValidateSha256(manifest.Parent.CorpusFingerprint, "parent.corpusFingerprint");
        if (manifest.Profiles is null || manifest.Profiles.Count == 0)
        {
            throw new InvalidDataException("The frozen corpus manifest has no profiles.");
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        FrozenCorpusProfile? previous = null;
        foreach (var profile in manifest.Profiles)
        {
            ValidateProfileId(profile.Name);
            if (!names.Add(profile.Name)
                || profile.AccountCount <= 0
                || profile.AccountCount > manifest.Parent.AccountCount
                || profile.ByteLength != checked(profile.AccountCount * AccountKey.ByteLength))
            {
                throw new InvalidDataException("The frozen corpus contains an invalid or duplicate profile.");
            }

            ValidateSha256(profile.PrefixSha256, "profiles.prefixSha256");
            if (previous is not null
                && (previous.AccountCount > profile.AccountCount
                    || previous.AccountCount == profile.AccountCount
                        && string.CompareOrdinal(previous.Name, profile.Name) >= 0))
            {
                throw new InvalidDataException("Frozen corpus profiles are not in canonical order.");
            }

            previous = profile;
        }

        if (manifest.HumanReadableArtifact is { } human)
        {
            if (!string.Equals(human.Artifact, "accounts.hex", StringComparison.Ordinal)
                || human.ByteLength != checked(manifest.Parent.AccountCount * 65))
            {
                throw new InvalidDataException("The optional human-readable corpus identity is invalid.");
            }

            ValidateSha256(human.Sha256, "humanReadableArtifact.sha256");
        }
    }

    private static void ValidateProfileId(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length is < 1 or > 80
            || !string.Equals(value, value.Trim(), StringComparison.Ordinal)
            || value.Any(static character => character is not (>= 'a' and <= 'z')
                and not (>= '0' and <= '9')
                and not '.' and not '-' and not '_'))
        {
            throw new ArgumentException("A corpus profile ID must use the canonical 1-80 character form.", nameof(value));
        }
    }

    private static void ValidateSha256(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if (value.Length != 64
            || value.Any(static character => character is not (>= '0' and <= '9')
                and not (>= 'a' and <= 'f')))
        {
            throw new InvalidDataException($"{parameterName} is not a canonical lowercase SHA-256 value.");
        }
    }

    private sealed record FrozenCorpusManifest(
        string Format,
        FrozenAccountKeyManifest AccountKey,
        FrozenSourceJournalManifest SourceJournal,
        FrozenParentCorpusManifest Parent,
        IReadOnlyList<FrozenCorpusProfile> Profiles,
        FrozenTextArtifactManifest? HumanReadableArtifact);

    private sealed record FrozenAccountKeyManifest(string Algorithm, int ByteLength);

    private sealed record FrozenSourceJournalManifest(string Format, long ByteLength, string Sha256);

    private sealed record FrozenParentCorpusManifest(
        string Artifact,
        long AccountCount,
        long ByteLength,
        string Sha256,
        string CorpusFingerprint);

    private sealed record FrozenCorpusProfile(
        string Name,
        long AccountCount,
        long ByteLength,
        string PrefixSha256);

    private sealed record FrozenTextArtifactManifest(string Artifact, long ByteLength, string Sha256);
}
