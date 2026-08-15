using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion.Tests;

public sealed class VerifiedCorpusAdmissionTests
{
    [Fact]
    public void OpensOnlyTheExactConfiguredManifestPrefixAndReadsBoundedPages()
    {
        using var fixture = new CorpusFixture();
        var frozen = fixture.Freeze();
        var profile = frozen.Manifest.Profiles.Single(value => value.Name == "two");

        using var admission = VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            profile.Name,
            profile.AccountCount,
            profile.PrefixSha256);

        Assert.Equal(2, admission.Count);
        Assert.Equal(profile.PrefixSha256, admission.ProfilePrefixSha256);
        var page = admission.ReadPage(0, 1);
        Assert.Single(page);
        Assert.True(admission.IsAdmitted(page[0]));
        Assert.Single(admission.ReadPage(1, 10));
        Assert.Empty(admission.ReadPage(2, 10));
    }

    [Fact]
    public void MismatchedConfiguredPrefixFailsBeforeAdmission()
    {
        using var fixture = new CorpusFixture();
        var frozen = fixture.Freeze();
        var profile = frozen.Manifest.Profiles.Single(value => value.Name == "two");

        Assert.Throws<InvalidDataException>(() => VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            profile.Name,
            profile.AccountCount,
            new string('0', 64)));
    }

    [Fact]
    public void BootstrapSqlNeverOverwritesProgressedStateAndRequiresAnExactFinalCount()
    {
        var sql = PostgreSqlCorpusBootstrapper.BootstrapPageSql;

        Assert.Contains("ON CONFLICT (account_key) DO NOTHING", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DO UPDATE", sql, StringComparison.Ordinal);
        Assert.Contains("NOT state.synchronization_complete", sql, StringComparison.Ordinal);
        Assert.Contains("reconciliation_dependency", sql, StringComparison.Ordinal);
        Assert.Equal(
            "SELECT count(*)::bigint FROM skypulse.account_state;",
            PostgreSqlCorpusBootstrapper.CountSql);
    }

    [Fact]
    public void AdmissionAdvancesOnlyToALargerPrefixOfTheSameParent()
    {
        using var fixture = new CorpusFixture();
        var frozen = fixture.Freeze();
        var two = frozen.Manifest.Profiles.Single(value => value.Name == "two");
        var all = frozen.Manifest.Profiles.Single(value => value.Name == "all");
        using var admission = new MonotonicCorpusAdmission();
        admission.Initialize(VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            two.Name,
            two.AccountCount,
            two.PrefixSha256));
        var firstPage = VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            all.Name,
            all.AccountCount,
            all.PrefixSha256);

        admission.Advance(firstPage);

        Assert.Equal(3, admission.Count);
        Assert.Equal("all", admission.ProfileId);
        Assert.Equal(all.PrefixSha256, admission.ProfilePrefixSha256);
        Assert.True(admission.IsAdmitted(AccountKey.FromDid("did:plc:corpus-c")));
    }

    [Fact]
    public void AdmissionRejectsASecondInitializationAndNonIncreasingPrefix()
    {
        using var fixture = new CorpusFixture();
        var frozen = fixture.Freeze();
        var two = frozen.Manifest.Profiles.Single(value => value.Name == "two");
        using var admission = new MonotonicCorpusAdmission();
        admission.Initialize(VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            two.Name,
            two.AccountCount,
            two.PrefixSha256));
        using var duplicate = VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            two.Name,
            two.AccountCount,
            two.PrefixSha256);
        using var secondInitialization = VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            two.Name,
            two.AccountCount,
            two.PrefixSha256);

        Assert.Throws<InvalidOperationException>(() => admission.Advance(duplicate));
        Assert.Throws<InvalidOperationException>(() => admission.Initialize(secondInitialization));
    }

    private sealed class CorpusFixture : IDisposable
    {
        private readonly string _directory = Path.Combine(
            Path.GetTempPath(),
            $"skypulse-durable-ingestion-{Guid.NewGuid():N}");

        internal CorpusFreezeResult Freeze()
        {
            Directory.CreateDirectory(_directory);
            var journal = Path.Combine(_directory, "observations.ndjson");
            File.WriteAllText(
                journal,
                """
                {"ordinal":1,"did":"did:plc:corpus-a","status":"active","sourcePosition":"a"}
                {"ordinal":2,"did":"did:plc:corpus-b","status":"active","sourcePosition":"b"}
                {"ordinal":3,"did":"did:plc:corpus-c","status":"active","sourcePosition":"c"}

                """,
                new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(journal, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }

            return CorpusFreezer.Freeze(new CorpusFreezeOptions
            {
                JournalPath = journal,
                OutputDirectory = Path.Combine(_directory, "frozen"),
                MemoryBudgetBytes = 4 * 1024,
                MergeFanIn = 2,
                Profiles =
                [
                    new CorpusProfileRequest("two", 2),
                    new CorpusProfileRequest("all", 3),
                ],
            });
        }

        public void Dispose()
        {
            if (Directory.Exists(_directory))
            {
                Directory.Delete(_directory, recursive: true);
            }
        }
    }
}
