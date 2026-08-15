using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Identifies the immutable base qualification profile bound to a runtime database. Monotonic
/// online increases are recorded separately in <c>corpus_capacity</c>; this identity never moves.
/// </summary>
public sealed class RuntimeProfileIdentity
{
    public RuntimeProfileIdentity(
        string profileId,
        int profileVersion,
        long corpusCap,
        string allowlistSha256)
    {
        ProfileId = RuntimeManifestGuard.CanonicalIdentifier(profileId, nameof(profileId));
        ProfileVersion = RuntimeManifestGuard.Positive(profileVersion, nameof(profileVersion));
        CorpusCap = RuntimeManifestGuard.Positive(corpusCap, nameof(corpusCap));
        AllowlistSha256 = RuntimeManifestGuard.Sha256(allowlistSha256, nameof(allowlistSha256));
    }

    public string ProfileId { get; }

    public int ProfileVersion { get; }

    public long CorpusCap { get; }

    public string AllowlistSha256 { get; }
}

/// <summary>
/// Identifies the immutable index namespace and schema used by a runtime database.
/// </summary>
public sealed class RuntimeIndexIdentity
{
    public RuntimeIndexIdentity(
        string indexNamespace,
        string providerName,
        string schemaId,
        int schemaVersion,
        string schemaFingerprint)
    {
        IndexNamespace = RuntimeManifestGuard.CanonicalIdentifier(indexNamespace, nameof(indexNamespace));
        ProviderName = RuntimeManifestGuard.CanonicalIdentifier(providerName, nameof(providerName));
        SchemaId = RuntimeManifestGuard.CanonicalIdentifier(schemaId, nameof(schemaId));
        SchemaVersion = RuntimeManifestGuard.Positive(schemaVersion, nameof(schemaVersion));
        SchemaFingerprint = RuntimeManifestGuard.Sha256(schemaFingerprint, nameof(schemaFingerprint));
    }

    public string IndexNamespace { get; }

    public string ProviderName { get; }

    public string SchemaId { get; }

    public int SchemaVersion { get; }

    public string SchemaFingerprint { get; }
}

/// <summary>
/// Identifies the exact package bytes and source/build provenance used by a runtime database.
/// </summary>
public sealed class RuntimePackageIdentity
{
    public RuntimePackageIdentity(
        string packageId,
        string packageVersion,
        string nupkgSha256,
        string canonicalManifestSha256,
        string repositoryUrl,
        string repositoryCommit,
        string buildSdkVersion)
    {
        PackageId = RuntimeManifestGuard.CanonicalIdentifier(packageId, nameof(packageId));
        PackageVersion = RuntimeManifestGuard.SemVer2(packageVersion, nameof(packageVersion));
        NupkgSha256 = RuntimeManifestGuard.Sha256(nupkgSha256, nameof(nupkgSha256));
        CanonicalManifestSha256 = RuntimeManifestGuard.Sha256(
            canonicalManifestSha256,
            nameof(canonicalManifestSha256));
        RepositoryUrl = RuntimeManifestGuard.GitHubRepositoryUrl(repositoryUrl, nameof(repositoryUrl));
        RepositoryCommit = RuntimeManifestGuard.RepositoryCommit(repositoryCommit, nameof(repositoryCommit));
        BuildSdkVersion = RuntimeManifestGuard.SdkVersion(buildSdkVersion, nameof(buildSdkVersion));
    }

    public string PackageId { get; }

    public string PackageVersion { get; }

    public string NupkgSha256 { get; }

    public string CanonicalManifestSha256 { get; }

    public string RepositoryUrl { get; }

    public string RepositoryCommit { get; }

    public string BuildSdkVersion { get; }
}

/// <summary>
/// Defines the exact immutable runtime identity which must be bound before ingestion starts.
/// </summary>
public sealed class RuntimeManifest
{
    private static readonly byte[] FingerprintDomain =
        Encoding.UTF8.GetBytes("orleans-searchable-storage-skypulse-runtime-manifest-v1\0");

    public RuntimeManifest(
        RuntimeProfileIdentity profile,
        Guid sourceInstanceId,
        RuntimeIndexIdentity index,
        RuntimePackageIdentity package)
    {
        Profile = profile ?? throw new ArgumentNullException(nameof(profile));
        SourceInstanceId = RuntimeManifestGuard.NonEmpty(sourceInstanceId, nameof(sourceInstanceId));
        Index = index ?? throw new ArgumentNullException(nameof(index));
        Package = package ?? throw new ArgumentNullException(nameof(package));
        Fingerprint = ComputeFingerprint();
    }

    public RuntimeProfileIdentity Profile { get; }

    public Guid SourceInstanceId { get; }

    public RuntimeIndexIdentity Index { get; }

    public RuntimePackageIdentity Package { get; }

    public string Fingerprint { get; }

    private string ComputeFingerprint()
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(FingerprintDomain);

        AppendString(hash, Profile.ProfileId);
        AppendInt32(hash, Profile.ProfileVersion);
        AppendInt64(hash, Profile.CorpusCap);
        AppendString(hash, Profile.AllowlistSha256);
        AppendString(hash, SourceInstanceId.ToString("D"));

        AppendString(hash, Index.IndexNamespace);
        AppendString(hash, Index.ProviderName);
        AppendString(hash, Index.SchemaId);
        AppendInt32(hash, Index.SchemaVersion);
        AppendString(hash, Index.SchemaFingerprint);

        AppendString(hash, Package.PackageId);
        AppendString(hash, Package.PackageVersion);
        AppendString(hash, Package.NupkgSha256);
        AppendString(hash, Package.CanonicalManifestSha256);
        AppendString(hash, Package.RepositoryUrl);
        AppendString(hash, Package.RepositoryCommit);
        AppendString(hash, Package.BuildSdkVersion);

        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static void AppendString(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }

    private static void AppendInt32(IncrementalHash hash, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private static void AppendInt64(IncrementalHash hash, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        hash.AppendData(bytes);
    }
}

internal static class RuntimeManifestGuard
{
    private const int MaximumIdentifierLength = 256;

    internal static string CanonicalIdentifier(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);

        if (value.Length is 0 or > MaximumIdentifierLength ||
            !IsAscii(value) ||
            char.IsWhiteSpace(value[0]) ||
            char.IsWhiteSpace(value[^1]))
        {
            throw new ArgumentException(
                "The value must be a non-empty, bounded canonical ASCII identifier.",
                parameterName);
        }

        foreach (var character in value)
        {
            if (char.IsControl(character))
            {
                throw new ArgumentException("The value must not contain control characters.", parameterName);
            }
        }

        return value;
    }

    internal static int Positive(int value, string parameterName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "The value must be positive.");
        }

        return value;
    }

    internal static long Positive(long value, string parameterName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "The value must be positive.");
        }

        return value;
    }

    internal static Guid NonEmpty(Guid value, string parameterName)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException("The value must not be an empty UUID.", parameterName);
        }

        return value;
    }

    internal static string Sha256(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);

        if (value.Length != 64 || !value.All(IsLowerHex))
        {
            throw new ArgumentException(
                "The value must be a lower-case 64-character SHA-256 digest.",
                parameterName);
        }

        return value;
    }

    internal static string RepositoryCommit(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);

        if (value.Length is not (40 or 64) || !value.All(IsLowerHex))
        {
            throw new ArgumentException(
                "The value must be a full lower-case 40- or 64-character Git object ID.",
                parameterName);
        }

        return value;
    }

    internal static string SdkVersion(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        var components = value.Split('.', StringSplitOptions.None);
        if (components.Length != 3 || components.Any(static component => !IsCanonicalNumber(component)))
        {
            throw new ArgumentException(
                "The value must be an exact canonical three-component SDK version.",
                parameterName);
        }

        return value;
    }

    internal static string GitHubRepositoryUrl(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) ||
            uri.Scheme != Uri.UriSchemeHttps ||
            uri.Host != "github.com" ||
            !uri.IsDefaultPort ||
            !string.IsNullOrEmpty(uri.UserInfo) ||
            !string.IsNullOrEmpty(uri.Query) ||
            !string.IsNullOrEmpty(uri.Fragment) ||
            value.EndsWith('/'))
        {
            throw new ArgumentException("The value must be a canonical HTTPS GitHub repository URL.", parameterName);
        }

        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length != 2 || segments.Any(static segment => !IsGitHubSegment(segment)))
        {
            throw new ArgumentException("The URL must identify exactly one GitHub owner and repository.", parameterName);
        }

        var canonical = $"https://github.com/{segments[0]}/{segments[1]}";
        if (!string.Equals(value, canonical, StringComparison.Ordinal))
        {
            throw new ArgumentException("The value must use canonical GitHub URL spelling.", parameterName);
        }

        return value;
    }

    internal static string SemVer2(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if (value.Length is 0 or > MaximumIdentifierLength || !IsAscii(value))
        {
            throw new ArgumentException("The value must be a bounded ASCII SemVer 2 version.", parameterName);
        }

        var plus = value.IndexOf('+');
        if (plus >= 0 && value.IndexOf('+', plus + 1) >= 0)
        {
            throw new ArgumentException("The value must be a canonical SemVer 2 version.", parameterName);
        }

        var withoutBuild = plus >= 0 ? value[..plus] : value;
        var build = plus >= 0 ? value[(plus + 1)..] : null;
        var hyphen = withoutBuild.IndexOf('-');
        var core = hyphen >= 0 ? withoutBuild[..hyphen] : withoutBuild;
        var prerelease = hyphen >= 0 ? withoutBuild[(hyphen + 1)..] : null;

        var coreParts = core.Split('.', StringSplitOptions.None);
        if (coreParts.Length != 3 || coreParts.Any(static part => !IsCanonicalNumber(part)) ||
            !IsValidDotIdentifiers(prerelease, numericIdentifiersMustBeCanonical: true) ||
            !IsValidDotIdentifiers(build, numericIdentifiersMustBeCanonical: false))
        {
            throw new ArgumentException("The value must be a canonical SemVer 2 version.", parameterName);
        }

        return value;
    }

    private static bool IsValidDotIdentifiers(string? value, bool numericIdentifiersMustBeCanonical)
    {
        if (value is null)
        {
            return true;
        }

        if (value.Length == 0)
        {
            return false;
        }

        foreach (var identifier in value.Split('.', StringSplitOptions.None))
        {
            if (identifier.Length == 0 ||
                !identifier.All(static character =>
                    char.IsAsciiLetterOrDigit(character) || character == '-'))
            {
                return false;
            }

            if (numericIdentifiersMustBeCanonical &&
                identifier.All(char.IsAsciiDigit) &&
                !IsCanonicalNumber(identifier))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsCanonicalNumber(string value) =>
        value.Length > 0 &&
        value.All(char.IsAsciiDigit) &&
        (value.Length == 1 || value[0] != '0');

    private static bool IsGitHubSegment(string value) =>
        value.Length is > 0 and <= 100 &&
        char.IsAsciiLetterOrDigit(value[0]) &&
        char.IsAsciiLetterOrDigit(value[^1]) &&
        value.All(static character =>
            char.IsAsciiLetterOrDigit(character) || character is '.' or '_' or '-');

    private static bool IsAscii(string value) => value.All(char.IsAscii);

    private static bool IsLowerHex(char value) =>
        value is >= '0' and <= '9' or >= 'a' and <= 'f';
}
