using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder.Tests;

public sealed class CorpusBuilderTests
{
    [Fact]
    public void ShuffledDuplicateAccountsAndStatusTransitionsSelectLatestExplicitActive()
    {
        using var fixture = new CorpusFixture();
        var alpha = "did:plc:alpha-account";
        var bravo = "did:plc:bravo-account";
        var charlie = "did:plc:charlie-account";
        var journal = fixture.WriteJournal(
            (alpha, "active"),
            (bravo, "active"),
            (alpha, "inactive"),
            (charlie, "inactive"),
            (alpha, "active"),
            (charlie, "active"),
            (bravo, "inactive"),
            (alpha, "active"));

        var result = fixture.Freeze(journal, "selected", memoryBudgetBytes: 4096);

        Assert.Equal(2, result.AccountCount);
        Assert.Equal(
            OrderedKeyHex(alpha, charlie),
            CorpusFixture.ReadKeyHex(result.OutputDirectory));
        var verified = CorpusVerifier.Verify(result.ManifestPath, deep: true, journal);
        Assert.True(verified.DeepVerification);
        Assert.True(verified.SourceJournalVerified);
    }

    [Fact]
    public void ForcedTinySpillsMatchAnIndependentInMemoryOracle()
    {
        using var fixture = new CorpusFixture();
        var observations = new List<(string Did, string Status)>();
        var latest = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var round = 0; round < 4; round++)
        {
            for (var offset = 0; offset < 180; offset++)
            {
                var index = (offset * 73 + round * 29) % 180;
                var did = $"did:plc:spill-{index:D4}";
                var status = (index + round) % 5 == 0 ? "inactive" : "active";
                observations.Add((did, status));
                latest[did] = status;
            }
        }

        var journal = fixture.WriteJournal(observations.ToArray());
        var result = fixture.Freeze(
            journal,
            "tiny-spills",
            memoryBudgetBytes: 4096,
            mergeFanIn: 3);
        var expected = latest
            .Where(static pair => pair.Value == "active")
            .Select(static pair => AccountKey.FromDid(pair.Key).ToString())
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.True(result.InitialSpillRunCount > 10);
        Assert.Equal(expected, CorpusFixture.ReadKeyHex(result.OutputDirectory));
        Assert.Equal(expected.Length, result.AccountCount);
        _ = CorpusVerifier.Verify(result.ManifestPath, deep: true, journal);
    }

    [Fact]
    public void PrefixHashesAndCoreFileBackedAdmissionUseTheExactSameFormat()
    {
        using var fixture = new CorpusFixture();
        var dids = Enumerable.Range(0, 12)
            .Select(static index => $"did:plc:profile-{index:D3}")
            .ToArray();
        var journal = fixture.WriteJournal(dids.Select(static did => (did, "active")).ToArray());
        var result = fixture.Freeze(
            journal,
            "profiles",
            profiles:
            [
                new CorpusProfileRequest("small", 3),
                new CorpusProfileRequest("middle", 7),
                new CorpusProfileRequest("all", 12),
            ]);
        var binary = Path.Combine(result.OutputDirectory, CorpusFormat.BinaryArtifactName);

        foreach (var profile in result.Manifest.Profiles)
        {
            var prefix = File.ReadAllBytes(binary).AsSpan(0, checked((int)profile.ByteLength));
            Assert.Equal(LowerHex(SHA256.HashData(prefix)), profile.PrefixSha256);
        }

        using var admission = FileBackedCorpusAdmission.OpenVerified(
            binary,
            checked((int)result.Manifest.Parent.AccountCount),
            result.Manifest.Parent.Sha256,
            result.Manifest.Parent.CorpusFingerprint,
            new CappedCorpusProfile("compatibility-7", 7));
        Assert.Equal(7, admission.Count);
        Assert.Equal(result.Manifest.Parent.CorpusFingerprint, admission.AllowlistFingerprint);
        Assert.Equal(
            CorpusFixture.ReadKeyHex(result.OutputDirectory).Take(7),
            Enumerable.Range(0, admission.Count).Select(index => admission[index].ToString()));

        var overTenMillion = new CorpusProfileRequest("over-10m", 10_000_001);
        Assert.Equal(10_000_001, overTenMillion.AccountCount);
    }

    [Fact]
    public void RepeatFreezeIsByteForByteDeterministicAndPublishesNoDidText()
    {
        using var fixture = new CorpusFixture();
        var journal = fixture.WriteJournal(
            ("did:plc:deterministic-c", "active"),
            ("did:plc:deterministic-a", "active"),
            ("did:plc:deterministic-b", "inactive"),
            ("did:plc:deterministic-b", "active"));
        var first = fixture.Freeze(journal, "repeat-a", writeHex: true, memoryBudgetBytes: 4096);
        var second = fixture.Freeze(journal, "repeat-b", writeHex: true, memoryBudgetBytes: 4096);

        var firstFiles = Directory.GetFiles(first.OutputDirectory)
            .Select(Path.GetFileName)
            .Order(StringComparer.Ordinal)
            .ToArray();
        var secondFiles = Directory.GetFiles(second.OutputDirectory)
            .Select(Path.GetFileName)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(firstFiles, secondFiles);
        foreach (var name in firstFiles)
        {
            var firstBytes = File.ReadAllBytes(Path.Combine(first.OutputDirectory, name!));
            var secondBytes = File.ReadAllBytes(Path.Combine(second.OutputDirectory, name!));
            Assert.Equal(firstBytes, secondBytes);
            Assert.False(Contains(firstBytes, "did:"u8));
        }

        var manifestText = File.ReadAllText(first.ManifestPath);
        Assert.DoesNotContain(journal, manifestText, StringComparison.Ordinal);
        Assert.EndsWith("\n", manifestText, StringComparison.Ordinal);
        Assert.Equal(1, manifestText.Count(static character => character == '\n'));
    }

    [Fact]
    public void DidBearingSpillRunsUseAPrivateDirectoryAndPrivateFiles()
    {
        using var fixture = new CorpusFixture();
        var journal = fixture.WriteJournal(
            ("did:plc:private-spill-a", "active"),
            ("did:plc:private-spill-b", "active"));
        var work = fixture.OutputPath("private-spill-work");
        PrivateWorkspacePermissions.CreateDirectory(work);

        var sorted = ExternalObservationSorter.Sort(journal, work, 4096, 2);

        if (!OperatingSystem.IsWindows())
        {
            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute,
                File.GetUnixFileMode(work));
            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite,
                File.GetUnixFileMode(sorted.SortedRunPath));
        }
    }

    [Fact]
    public void SorterRejectsAWorldReadablePrivateWorkspace()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var fixture = new CorpusFixture();
        var journal = fixture.WriteJournal(("did:plc:unsafe-spill", "active"));
        var work = fixture.OutputPath("unsafe-spill-work");
        Directory.CreateDirectory(work);
        File.SetUnixFileMode(
            work,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute
                | UnixFileMode.GroupRead | UnixFileMode.GroupExecute
                | UnixFileMode.OtherRead | UnixFileMode.OtherExecute);

        Assert.Throws<IOException>(() => ExternalObservationSorter.Sort(journal, work, 4096, 2));
    }

    [Fact]
    public void SorterRejectsASymbolicLinkWorkspace()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var fixture = new CorpusFixture();
        var journal = fixture.WriteJournal(("did:plc:linked-spill", "active"));
        var target = fixture.OutputPath("private-spill-target");
        PrivateWorkspacePermissions.CreateDirectory(target);
        var link = fixture.OutputPath("private-spill-link");
        Directory.CreateSymbolicLink(link, target);

        Assert.Throws<IOException>(() => ExternalObservationSorter.Sort(journal, link, 4096, 2));
    }

    [Fact]
    public void FreezeRejectsAWorldReadableOrLinkedDidJournal()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var fixture = new CorpusFixture();
        var journal = fixture.WriteJournal(("did:plc:unsafe-journal", "active"));
        File.SetUnixFileMode(
            journal,
            UnixFileMode.UserRead | UnixFileMode.UserWrite
                | UnixFileMode.GroupRead | UnixFileMode.OtherRead);
        Assert.Throws<IOException>(() => fixture.Freeze(journal, "unsafe-journal-output"));

        File.SetUnixFileMode(journal, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        var link = fixture.OutputPath("linked-private-journal.ndjson");
        File.CreateSymbolicLink(link, journal);
        Assert.Throws<IOException>(() => fixture.Freeze(link, "linked-journal-output"));
    }

    [Theory]
    [InlineData("corrupt")]
    [InlineData("unsorted")]
    [InlineData("truncated")]
    public void DeepVerificationRejectsCorruptUnsortedAndTruncatedArtifacts(string mutation)
    {
        using var fixture = new CorpusFixture();
        var journal = fixture.WriteJournal(
            ("did:plc:verify-a", "active"),
            ("did:plc:verify-b", "active"),
            ("did:plc:verify-c", "active"));
        var result = fixture.Freeze(journal, $"invalid-{mutation}");
        var binary = Path.Combine(result.OutputDirectory, CorpusFormat.BinaryArtifactName);
        var bytes = File.ReadAllBytes(binary);
        switch (mutation)
        {
            case "corrupt":
                bytes[^1] ^= 0x80;
                File.WriteAllBytes(binary, bytes);
                break;
            case "unsorted":
                var first = bytes.AsSpan(0, 32).ToArray();
                bytes.AsSpan(32, 32).CopyTo(bytes.AsSpan(0, 32));
                first.CopyTo(bytes.AsSpan(32, 32));
                File.WriteAllBytes(binary, bytes);
                break;
            case "truncated":
                using (var stream = File.OpenWrite(binary))
                {
                    stream.SetLength(stream.Length - 1);
                }

                break;
        }

        Assert.Throws<InvalidDataException>(
            () => CorpusVerifier.Verify(result.ManifestPath, deep: true, journal));
    }

    [Theory]
    [InlineData("{\"ordinal\":1,\"did\":\"did:plc:a\",\"status\":\"unknown\",\"sourcePosition\":\"p1\"}\n")]
    [InlineData("{\"ordinal\":1,\"did\":\"did:plc:a\",\"status\":\"active\",\"sourcePosition\":\"p1\",\"handle\":\"not-allowed\"}\n")]
    [InlineData("{\"ordinal\":1,\"ordinal\":2,\"did\":\"did:plc:a\",\"status\":\"active\",\"sourcePosition\":\"p1\"}\n")]
    [InlineData("{\"ordinal\":2,\"did\":\"did:plc:a\",\"status\":\"active\",\"sourcePosition\":\"p2\"}\n{\"ordinal\":1,\"did\":\"did:plc:b\",\"status\":\"active\",\"sourcePosition\":\"p1\"}\n")]
    public void UnknownOrInconsistentJournalInputFailsClosed(string content)
    {
        using var fixture = new CorpusFixture();
        var journal = fixture.WriteRawJournal(content);
        var output = fixture.OutputPath("rejected");

        Assert.Throws<InvalidDataException>(
            () => CorpusFreezer.Freeze(
                new CorpusFreezeOptions
                {
                    JournalPath = journal,
                    OutputDirectory = output,
                    MemoryBudgetBytes = 4096,
                }));
        Assert.False(Directory.Exists(output));
    }

    [Fact]
    public void CliRequiresDeepVerificationAndCanCheckTheSourceJournal()
    {
        using var fixture = new CorpusFixture();
        var journal = fixture.WriteJournal(("did:plc:cli", "active"));
        var outputDirectory = fixture.OutputPath("cli");
        using var stdout = new StringWriter();
        using var stderr = new StringWriter();

        Assert.Equal(
            0,
            CorpusBuilderCli.Run(
                [
                    "freeze",
                    "--journal",
                    journal,
                    "--output",
                    outputDirectory,
                    "--memory-bytes",
                    "4096",
                ],
                stdout,
                stderr));
        Assert.Equal(
            2,
            CorpusBuilderCli.Run(
                ["verify", "--manifest", Path.Combine(outputDirectory, CorpusFormat.ManifestName)],
                stdout,
                stderr));
        Assert.Equal(
            0,
            CorpusBuilderCli.Run(
                [
                    "verify",
                    "--manifest",
                    Path.Combine(outputDirectory, CorpusFormat.ManifestName),
                    "--deep",
                    "--journal",
                    journal,
                ],
                stdout,
                stderr));
        Assert.Contains("Deep verification passed", stdout.ToString(), StringComparison.Ordinal);
    }

    private static string[] OrderedKeyHex(params string[] dids)
        => dids.Select(AccountKey.FromDid).Order().Select(static key => key.ToString()).ToArray();

    private static string LowerHex(ReadOnlySpan<byte> bytes)
        => Convert.ToHexString(bytes).ToLowerInvariant();

    private static bool Contains(ReadOnlySpan<byte> haystack, ReadOnlySpan<byte> needle)
        => haystack.IndexOf(needle) >= 0;

    private sealed class CorpusFixture : IDisposable
    {
        private readonly string _root = Path.Combine(
            Path.GetTempPath(),
            $"skypulse-corpus-tests-{Guid.NewGuid():N}");
        private int _journalNumber;

        public CorpusFixture() => Directory.CreateDirectory(_root);

        public string WriteJournal(params (string Did, string Status)[] observations)
        {
            var builder = new StringBuilder();
            for (var index = 0; index < observations.Length; index++)
            {
                builder.Append("{\"ordinal\":");
                builder.Append(index + 1);
                builder.Append(",\"did\":");
                builder.Append(JsonSerializer.Serialize(observations[index].Did));
                builder.Append(",\"status\":");
                builder.Append(JsonSerializer.Serialize(observations[index].Status));
                builder.Append(",\"sourcePosition\":\"fixture-");
                builder.Append(index + 1);
                builder.Append("\"}\n");
            }

            return WriteRawJournal(builder.ToString());
        }

        public string WriteRawJournal(string content)
        {
            var path = Path.Combine(_root, $"journal-{_journalNumber++:D3}.ndjson");
            File.WriteAllText(path, content, new UTF8Encoding(false));
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }

            return path;
        }

        public string OutputPath(string name) => Path.Combine(_root, name);

        public CorpusFreezeResult Freeze(
            string journal,
            string outputName,
            long memoryBudgetBytes = 64 * 1024,
            int mergeFanIn = 4,
            bool writeHex = false,
            IReadOnlyList<CorpusProfileRequest>? profiles = null)
            => CorpusFreezer.Freeze(
                new CorpusFreezeOptions
                {
                    JournalPath = journal,
                    OutputDirectory = OutputPath(outputName),
                    MemoryBudgetBytes = memoryBudgetBytes,
                    MergeFanIn = mergeFanIn,
                    WriteHumanReadableHex = writeHex,
                    Profiles = profiles ?? [],
                });

        public static string[] ReadKeyHex(string outputDirectory)
        {
            var bytes = File.ReadAllBytes(Path.Combine(outputDirectory, CorpusFormat.BinaryArtifactName));
            Assert.Equal(0, bytes.Length % CorpusFormat.AccountKeyByteLength);
            return Enumerable.Range(0, bytes.Length / CorpusFormat.AccountKeyByteLength)
                .Select(index => LowerHex(
                    bytes.AsSpan(
                        index * CorpusFormat.AccountKeyByteLength,
                        CorpusFormat.AccountKeyByteLength)))
                .ToArray();
        }

        public void Dispose() => Directory.Delete(_root, recursive: true);
    }
}
