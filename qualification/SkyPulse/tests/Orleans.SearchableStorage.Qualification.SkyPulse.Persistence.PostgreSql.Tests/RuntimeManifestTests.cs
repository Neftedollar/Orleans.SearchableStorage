using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class RuntimeManifestTests
{
    private const string RepositoryUrl = "https://github.com/Neftedollar/Orleans.SearchableStorage";

    [Fact]
    public void CompleteIdentityHasPinnedDomainSeparatedFingerprint()
    {
        var manifest = CreateManifest();

        Assert.Equal("e67ee08692cde5bd073c4d91e1e21bcfb0d0f4c3a13d909728d7c466486a33ee", manifest.Fingerprint);
    }

    [Fact]
    public void EveryMaterialIdentityGroupChangesFingerprint()
    {
        var baseline = CreateManifest();
        var changed = new[]
        {
            CreateManifest(profile: new("other-profile", 1, 1_000_000, Digest('a'))),
            CreateManifest(profile: new("skypulse-million-v1", 2, 1_000_000, Digest('a'))),
            CreateManifest(profile: new("skypulse-million-v1", 1, 10_000_000, Digest('a'))),
            CreateManifest(profile: new("skypulse-million-v1", 1, 1_000_000, Digest('f'))),
            CreateManifest(sourceInstanceId: Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")),
            CreateManifest(index: new("other-index", "SkyPulseIndex", "skypulse-account-v1", 1, Digest('b'))),
            CreateManifest(index: new("skypulse-1m", "OtherProvider", "skypulse-account-v1", 1, Digest('b'))),
            CreateManifest(index: new("skypulse-1m", "SkyPulseIndex", "other-schema", 1, Digest('b'))),
            CreateManifest(index: new("skypulse-1m", "SkyPulseIndex", "skypulse-account-v1", 2, Digest('b'))),
            CreateManifest(index: new("skypulse-1m", "SkyPulseIndex", "skypulse-account-v1", 1, Digest('f'))),
            CreateManifest(package: new("Other.Package", "1.0.0-rc.2", Digest('c'), Digest('d'), RepositoryUrl, new string('e', 40), "10.0.303")),
            CreateManifest(package: new("Orleans.SearchableStorage", "1.0.0-rc.3", Digest('c'), Digest('d'), RepositoryUrl, new string('e', 40), "10.0.303")),
            CreateManifest(package: new("Orleans.SearchableStorage", "1.0.0-rc.2", Digest('f'), Digest('d'), RepositoryUrl, new string('e', 40), "10.0.303")),
            CreateManifest(package: new("Orleans.SearchableStorage", "1.0.0-rc.2", Digest('c'), Digest('f'), RepositoryUrl, new string('e', 40), "10.0.303")),
            CreateManifest(package: new("Orleans.SearchableStorage", "1.0.0-rc.2", Digest('c'), Digest('d'), "https://github.com/Neftedollar/Other", new string('e', 40), "10.0.303")),
            CreateManifest(package: new("Orleans.SearchableStorage", "1.0.0-rc.2", Digest('c'), Digest('d'), RepositoryUrl, new string('f', 40), "10.0.303")),
            CreateManifest(package: new("Orleans.SearchableStorage", "1.0.0-rc.2", Digest('c'), Digest('d'), RepositoryUrl, new string('e', 40), "10.0.304")),
        };

        Assert.All(changed, value => Assert.NotEqual(baseline.Fingerprint, value.Fingerprint));
    }

    [Theory]
    [InlineData("")]
    [InlineData("ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789")]
    [InlineData("abcdef")]
    public void Sha256MustBeExactLowercaseHex(string digest)
    {
        Assert.Throws<ArgumentException>(() => new RuntimeProfileIdentity("profile", 1, 1, digest));
    }

    [Theory]
    [InlineData("1")]
    [InlineData("1.0")]
    [InlineData("01.0.0")]
    [InlineData("1.0.0-01")]
    [InlineData("1.0.0-")]
    [InlineData("1.0.0+")]
    [InlineData("1.0.0+build+other")]
    public void PackageVersionMustBeCanonicalSemVer2(string version)
    {
        Assert.Throws<ArgumentException>(() => Package(version: version));
    }

    [Theory]
    [InlineData("http://github.com/Neftedollar/Orleans.SearchableStorage")]
    [InlineData("https://GitHub.com/Neftedollar/Orleans.SearchableStorage")]
    [InlineData("https://github.com/Neftedollar/Orleans.SearchableStorage/")]
    [InlineData("https://github.com/Neftedollar/Orleans.SearchableStorage?x=1")]
    [InlineData("https://github.com/Neftedollar")]
    [InlineData("https://user@github.com/Neftedollar/Orleans.SearchableStorage")]
    public void RepositoryUrlMustBeCanonicalAndRepositorySpecific(string url)
    {
        Assert.Throws<ArgumentException>(() => Package(repositoryUrl: url));
    }

    [Theory]
    [InlineData("abcdef")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeg")]
    public void RepositoryCommitMustBeFullLowercaseObjectId(string commit)
    {
        Assert.Throws<ArgumentException>(() => Package(repositoryCommit: commit));
    }

    [Fact]
    public void RepositoryCommitAcceptsFullSha1AndSha256ObjectIds()
    {
        Assert.Equal(40, Package(repositoryCommit: new string('a', 40)).RepositoryCommit.Length);
        Assert.Equal(64, Package(repositoryCommit: new string('a', 64)).RepositoryCommit.Length);
    }

    [Theory]
    [InlineData("10.0")]
    [InlineData("10.0.303-preview")]
    [InlineData("010.0.303")]
    [InlineData("10.0.0303")]
    [InlineData("10.0.303.1")]
    public void BuildSdkMustBeExactCanonicalThreeComponentVersion(string version)
    {
        Assert.Throws<ArgumentException>(() => Package(buildSdkVersion: version));
    }

    [Fact]
    public void PositiveVersionsCapAndSourceAreRequired()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuntimeProfileIdentity("profile", 0, 1, Digest('a')));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuntimeProfileIdentity("profile", 1, 0, Digest('a')));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RuntimeIndexIdentity("index", "provider", "schema", 0, Digest('a')));
        Assert.Throws<ArgumentException>(() => new RuntimeManifest(
            new RuntimeProfileIdentity("profile", 1, 1, Digest('a')),
            Guid.Empty,
            new RuntimeIndexIdentity("index", "provider", "schema", 1, Digest('b')),
            Package()));
    }

    private static RuntimeManifest CreateManifest(
        RuntimeProfileIdentity? profile = null,
        Guid? sourceInstanceId = null,
        RuntimeIndexIdentity? index = null,
        RuntimePackageIdentity? package = null)
        => new(
            profile ?? new RuntimeProfileIdentity("skypulse-million-v1", 1, 1_000_000, Digest('a')),
            sourceInstanceId ?? Guid.Parse("11111111-2222-3333-4444-555555555555"),
            index ?? new RuntimeIndexIdentity("skypulse-1m", "SkyPulseIndex", "skypulse-account-v1", 1, Digest('b')),
            package ?? Package());

    private static RuntimePackageIdentity Package(
        string version = "1.0.0-rc.2",
        string repositoryUrl = RepositoryUrl,
        string repositoryCommit = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        string buildSdkVersion = "10.0.303")
        => new(
            "Orleans.SearchableStorage",
            version,
            Digest('c'),
            Digest('d'),
            repositoryUrl,
            repositoryCommit,
            buildSdkVersion);

    private static string Digest(char value) => new(value, 64);
}
