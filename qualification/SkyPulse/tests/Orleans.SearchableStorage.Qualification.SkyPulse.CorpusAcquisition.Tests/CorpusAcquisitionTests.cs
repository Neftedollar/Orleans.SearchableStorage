using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition.Tests;

public sealed class CorpusAcquisitionTests
{
    [Fact]
    public void ListReposTransportIsBoundToTheConfiguredOrigin()
    {
        using var handler = HttpListReposSource.CreateHandler();

        Assert.False(handler.AllowAutoRedirect);
        Assert.Equal(System.Net.DecompressionMethods.None, handler.AutomaticDecompression);
        Assert.False(handler.UseCookies);
        Assert.False(handler.UseProxy);
        Assert.Equal(1, handler.MaxConnectionsPerServer);
    }

    [Fact]
    public async Task SyntheticCaptureProducesAFreezeEligibleBoundedObservedCensus()
    {
        using var fixture = new AcquisitionFixture();
        var jetstream = AcquisitionFixture.Session(
            new JetstreamLifecycleObservation("jet-a", 100, JetstreamLifecycleKind.Identity, Did("identity"), null),
            new JetstreamLifecycleObservation("jet-a", 101, JetstreamLifecycleKind.Account, Did("alpha"), true),
            new JetstreamLifecycleObservation("jet-a", 101, JetstreamLifecycleKind.Account, Did("alpha"), true),
            new JetstreamLifecycleObservation("jet-a", 105, JetstreamLifecycleKind.Sync, Did("sync"), null));
        var pages = new ScriptedListReposSource(
            Page("relay-a", "s1-next", ("alpha", true), ("bravo", true)),
            Page("relay-a", null, ("charlie", false)),
            Page("relay-a", "s2-next", ("alpha", true), ("bravo", false)),
            Page("relay-a", null, ("charlie", true)));

        var result = await CorpusAcquisitionRunner.RunAsync(fixture.Options, jetstream, pages);

        Assert.True(result.Manifest.FreezeEligible);
        Assert.Equal("bounded-observed-census-not-atomic-not-global", result.Manifest.Claim);
        Assert.Equal(2, result.Manifest.Sweeps.Count);
        Assert.Equal(100UL, result.Manifest.StartCursor);
        Assert.Equal(105UL, result.Manifest.CloseCursor);
        Assert.Equal(1, result.Manifest.Counts.JetstreamAccountEvents);
        Assert.Equal(1, result.Manifest.Counts.JetstreamIdentityEvents);
        Assert.Equal(1, result.Manifest.Counts.JetstreamSyncEvents);
        Assert.Equal(7, result.Manifest.Counts.JournalObservations);
        Assert.Equal("kinds=account&kinds=identity&kinds=sync", result.Manifest.JetstreamFilter);
        Assert.Equal(AcquisitionContract.JetstreamCommit, result.Manifest.Contracts[0].Commit);

        var journal = await File.ReadAllTextAsync(result.JournalPath);
        Assert.DoesNotContain("handle", journal, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("record", journal, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("content", journal, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("media", journal, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("identity", journal, StringComparison.Ordinal);
        Assert.Contains("\"status\":\"active\"", journal, StringComparison.Ordinal);
        AssertStrictlyIncreasingOrdinals(journal);
        AssertPrivateDirectoryMode(fixture.Options.OutputDirectory);
        AssertPrivateMode(result.JournalPath);
        AssertPrivateMode(result.ManifestPath);
        AssertPrivateMode(Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.CheckpointFileName));
        AssertPrivateMode(Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.CursorLedgerFileName));
    }

    [Fact]
    public async Task CursorLoopPoisonsRunAndNeverPublishesFreezableJournal()
    {
        using var fixture = new AcquisitionFixture();
        var jetstream = AcquisitionFixture.Session(
            new JetstreamLifecycleObservation("jet-a", 20, JetstreamLifecycleKind.Account, Did("live"), true));
        var pages = new ScriptedListReposSource(
            Page("relay-a", "loop", ("one", true)),
            Page("relay-a", "loop", ("two", true)));

        var exception = await Assert.ThrowsAsync<AcquisitionContractException>(
            () => CorpusAcquisitionRunner.RunAsync(fixture.Options, jetstream, pages));

        Assert.Equal("list-repos-cursor-loop", exception.ReasonCode);
        Assert.False(File.Exists(Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.JournalFileName)));
        Assert.False(File.Exists(Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.ManifestFileName)));
        var checkpoint = PrivateArtifactIO.ReadCanonical<AcquisitionCheckpoint>(Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.CheckpointFileName));
        Assert.Equal(AcquisitionPhase.Poisoned, checkpoint.Phase);
    }

    [Fact]
    public async Task UnknownListReposLifecycleStatusPoisonsRun()
    {
        using var fixture = new AcquisitionFixture();
        var jetstream = AcquisitionFixture.Session(
            new JetstreamLifecycleObservation("jet-a", 30, JetstreamLifecycleKind.Account, Did("live"), true));
        var pages = new ScriptedListReposSource(
            new ListReposPage(
                "relay-a",
                [new ListReposObservation(Did("unknown"), null)],
                null));

        var exception = await Assert.ThrowsAsync<AcquisitionContractException>(
            () => CorpusAcquisitionRunner.RunAsync(fixture.Options, jetstream, pages));

        Assert.Equal("unknown-lifecycle", exception.ReasonCode);
        Assert.False(File.Exists(Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.JournalFileName)));
    }

    [Fact]
    public void JetstreamParserDiscardsPrivatePayloadsAndRejectsCommitOrGap()
    {
        var identity = Encoding.UTF8.GetBytes(
            "{\"$type\":\"message\",\"payload\":{"
            + "\"$type\":\"network.bsky.jetstream.subscribeEvents#identity\","
            + "\"seq\":42,\"did\":\"did:plc:outer\",\"time\":\"2026-08-15T00:00:00.000000Z\","
            + "\"identity\":{\"did\":\"did:plc:outer\",\"handle\":\"private.example\","
            + "\"seq\":7,\"time\":\"2026-08-15T00:00:00Z\"}}}");

        var parsed = JetstreamV2FrameParser.Parse(identity, "jet-a");

        Assert.Equal(JetstreamLifecycleKind.Identity, parsed.Kind);
        Assert.Equal("did:plc:outer", parsed.Did);
        Assert.Null(parsed.Active);
        Assert.DoesNotContain("private.example", parsed.ToString(), StringComparison.Ordinal);

        var sync = Encoding.UTF8.GetBytes(
            "{\"$type\":\"message\",\"payload\":{"
            + "\"$type\":\"network.bsky.jetstream.subscribeEvents#sync\","
            + "\"seq\":43,\"did\":\"did:plc:outer\",\"time\":\"2026-08-15T00:00:00.000000Z\","
            + "\"sync\":{\"did\":\"did:plc:outer\",\"rev\":\"3abc\",\"seq\":8,"
            + "\"time\":\"2026-08-15T00:00:00Z\",\"blocks\":{\"$bytes\":\"AQID\"}}}}" );
        var parsedSync = JetstreamV2FrameParser.Parse(sync, "jet-a");
        Assert.Equal(JetstreamLifecycleKind.Sync, parsedSync.Kind);
        Assert.DoesNotContain("AQID", parsedSync.ToString(), StringComparison.Ordinal);

        var commit = Encoding.UTF8.GetBytes(
            "{\"$type\":\"message\",\"payload\":{"
            + "\"$type\":\"network.bsky.jetstream.subscribeEvents#commit\","
            + "\"seq\":43,\"did\":\"did:plc:outer\",\"time\":\"2026-08-15T00:00:00.000000Z\","
            + "\"commit\":{\"text\":\"must never be accepted\"}}}");
        var exception = Assert.Throws<AcquisitionContractException>(
            () => JetstreamV2FrameParser.Parse(commit, "jet-a"));
        Assert.Equal("jetstream-commit-leak", exception.ReasonCode);

        var info = Encoding.UTF8.GetBytes(
            "{\"$type\":\"message\",\"payload\":{"
            + "\"$type\":\"network.bsky.jetstream.subscribeEvents#info\","
            + "\"name\":\"OutdatedCursor\",\"message\":\"starting at a later cursor\"}}" );
        var gap = Assert.Throws<AcquisitionContractException>(
            () => JetstreamV2FrameParser.Parse(info, "jet-a"));
        Assert.Equal("jetstream-gap", gap.ReasonCode);
    }

    [Fact]
    public void ListReposParserKeepsOnlyDidAndExplicitLifecycleStatus()
    {
        var response = Encoding.UTF8.GetBytes(
            "{\"cursor\":\"opaque\",\"repos\":[{\"did\":\"did:plc:repo\","
            + "\"head\":\"bafy-head\",\"rev\":\"3abc\",\"active\":false,\"status\":\"deleted\"}]}");

        var page = HttpListReposSource.Parse(response, "relay-a", 1000);

        var repository = Assert.Single(page.Repositories);
        Assert.Equal("did:plc:repo", repository.Did);
        Assert.False(repository.Active);
        Assert.DoesNotContain("bafy-head", repository.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("deleted", repository.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResumeTruncatesUncheckpointedTailAndDeduplicatesInclusiveReplay()
    {
        using var fixture = new AcquisitionFixture();
        long committedLength;
        long nextOrdinal;
        await using (var first = AcquisitionWorkspace.Open(fixture.Options))
        {
            await first.MarkSweepingAsync(CancellationToken.None);
            _ = await first.CommitJetstreamAsync(
                new JetstreamLifecycleObservation(
                    "jet-a",
                    40,
                    JetstreamLifecycleKind.Account,
                    Did("resume"),
                    true),
                CancellationToken.None);
            await first.CommitListReposPageAsync(
                1,
                1,
                null,
                Page("relay-a", "next", ("resume", true)),
                CancellationToken.None);
            committedLength = first.Checkpoint.JournalByteLength;
            nextOrdinal = first.Checkpoint.NextOrdinal;
        }

        var partial = Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.PartialJournalFileName);
        await File.AppendAllTextAsync(partial, "uncommitted-private-tail");
        Assert.True(new FileInfo(partial).Length > committedLength);

        await using var resumed = AcquisitionWorkspace.Open(fixture.Options);
        Assert.Equal(committedLength, new FileInfo(partial).Length);
        var shouldContinue = await resumed.CommitJetstreamAsync(
            new JetstreamLifecycleObservation(
                "jet-a",
                40,
                JetstreamLifecycleKind.Account,
                Did("resume"),
                true),
            CancellationToken.None);

        Assert.True(shouldContinue);
        Assert.Equal(nextOrdinal, resumed.Checkpoint.NextOrdinal);
        Assert.Equal(committedLength, resumed.Checkpoint.JournalByteLength);
    }

    [Fact]
    public async Task ResumeRejectsAWorldReadableCheckpointInsteadOfRepairingIt()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var fixture = new AcquisitionFixture();
        await using (var workspace = AcquisitionWorkspace.Open(fixture.Options))
        {
            await workspace.MarkSweepingAsync(CancellationToken.None);
        }

        var checkpoint = Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.CheckpointFileName);
        File.SetUnixFileMode(
            checkpoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite
                | UnixFileMode.GroupRead | UnixFileMode.OtherRead);

        Assert.Throws<IOException>(() => AcquisitionWorkspace.Open(fixture.Options));
        Assert.Equal(
            UnixFileMode.UserRead | UnixFileMode.UserWrite
                | UnixFileMode.GroupRead | UnixFileMode.OtherRead,
            File.GetUnixFileMode(checkpoint));
    }

    [Fact]
    public async Task ResumeRejectsASymbolicLinkCheckpoint()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var fixture = new AcquisitionFixture();
        await using (var workspace = AcquisitionWorkspace.Open(fixture.Options))
        {
            await workspace.MarkSweepingAsync(CancellationToken.None);
        }

        var checkpoint = Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.CheckpointFileName);
        var target = Path.Combine(fixture.Options.OutputDirectory, "checkpoint-private-target.json");
        File.Move(checkpoint, target);
        File.CreateSymbolicLink(checkpoint, target);

        Assert.Throws<IOException>(() => AcquisitionWorkspace.Open(fixture.Options));
    }

    [Fact]
    public async Task InstanceSwitchPoisonsRun()
    {
        using var fixture = new AcquisitionFixture();
        var jetstream = AcquisitionFixture.Session(
            new JetstreamLifecycleObservation(
                "different-instance",
                60,
                JetstreamLifecycleKind.Account,
                Did("switch"),
                true));
        var pages = new ScriptedListReposSource(
            Page("relay-a", null, ("one", true)),
            Page("relay-a", null, ("one", true)));

        var exception = await Assert.ThrowsAsync<AcquisitionContractException>(
            () => CorpusAcquisitionRunner.RunAsync(fixture.Options, jetstream, pages));

        Assert.Equal("jetstream-instance-switch", exception.ReasonCode);
        Assert.False(File.Exists(Path.Combine(
            fixture.Options.OutputDirectory,
            AcquisitionContract.JournalFileName)));
    }

    [Fact]
    public async Task PrivateRouteProvesExactOrderedProfileAndBoundedBatches()
    {
        using var fixture = new AcquisitionFixture();
        var jetstream = AcquisitionFixture.Session(
            new JetstreamLifecycleObservation("jet-a", 50, JetstreamLifecycleKind.Account, Did("live"), true));
        var pages = new ScriptedListReposSource(
            Page("relay-a", null, ("alpha", true), ("bravo", true), ("charlie", true)),
            Page("relay-a", null, ("alpha", true), ("bravo", true), ("charlie", true)));
        var acquisition = await CorpusAcquisitionRunner.RunAsync(fixture.Options, jetstream, pages);
        var corpus = CorpusFreezer.Freeze(
            new CorpusFreezeOptions
            {
                JournalPath = acquisition.JournalPath,
                OutputDirectory = fixture.Path("corpus"),
                MemoryBudgetBytes = 4096,
                MergeFanIn = 2,
                Profiles = [new CorpusProfileRequest("two", 2)],
            });

        var route = PrivateRoutingExporter.Export(
            new PrivateRoutingExportOptions
            {
                AcquisitionManifestPath = acquisition.ManifestPath,
                CorpusManifestPath = corpus.ManifestPath,
                ProfileName = "two",
                OutputDirectory = fixture.Path("route"),
                MemoryBudgetBytes = 4096,
                MergeFanIn = 2,
                BatchRecordLimit = 1,
            });
        var verified = PrivateRoutingExporter.Verify(route.ManifestPath);
        var independentlyBound = PrivateRoutingExporter.Verify(
            route.ManifestPath,
            new PrivateRoutingExpectedProfile(
                verified.Profile.Name,
                verified.Profile.AccountCount,
                verified.Profile.PrefixSha256));

        Assert.Equal(2, verified.Routing.AccountCount);
        Assert.Equal(2, verified.Batches.Count);
        Assert.All(verified.Batches, static batch => Assert.Equal(1, batch.RecordCount));
        Assert.Equal(verified.Profile.PrefixSha256, verified.Routing.AccountKeyProjectionSha256);
        Assert.Equal(verified.Profile, independentlyBound.Profile);
        Assert.Equal(verified.Routing, independentlyBound.Routing);
        Assert.Throws<InvalidDataException>(
            () => PrivateRoutingExporter.Verify(
                route.ManifestPath,
                new PrivateRoutingExpectedProfile("two", 2, new string('0', 64))));
        AssertPrivateMode(route.RoutingPath);
        AssertPrivateMode(route.ManifestPath);
        AssertPrivateDirectoryMode(route.OutputDirectory);
        var publicBytes = Directory.GetFiles(corpus.OutputDirectory)
            .SelectMany(File.ReadAllBytes)
            .ToArray();
        Assert.DoesNotContain("did:"u8.ToArray(), publicBytes);
    }

    private static string Did(string suffix) => $"did:plc:test-{suffix}";

    private static ListReposPage Page(
        string instance,
        string? cursor,
        params (string Suffix, bool Active)[] repositories)
        => new(
            instance,
            repositories.Select(static repository => new ListReposObservation(
                Did(repository.Suffix),
                repository.Active)).ToArray(),
            cursor);

    private static void AssertStrictlyIncreasingOrdinals(string journal)
    {
        long prior = 0;
        foreach (var line in journal.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            using var document = System.Text.Json.JsonDocument.Parse(line);
            var ordinal = document.RootElement.GetProperty("ordinal").GetInt64();
            Assert.True(ordinal > prior);
            prior = ordinal;
        }
    }

    private static void AssertPrivateMode(string path)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite,
                File.GetUnixFileMode(path));
        }
    }

    private static void AssertPrivateDirectoryMode(string path)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute,
                File.GetUnixFileMode(path));
        }
    }

    private sealed class AcquisitionFixture : IDisposable
    {
        private readonly string _root = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"skypulse-acquisition-{Guid.NewGuid():N}");

        public AcquisitionFixture() => Directory.CreateDirectory(_root);

        public AcquisitionOptions Options => new()
        {
            OutputDirectory = Path("capture"),
            JetstreamEndpoint = new Uri("wss://jet.example"),
            JetstreamInstanceId = "jet-a",
            RelayEndpoint = new Uri("https://relay.example"),
            RelayInstanceId = "relay-a",
            FullSweepCount = 2,
            ListReposPageLimit = 1000,
            MaximumPagesPerSweep = 20,
            MaximumLifecycleFrames = 100,
            CloseCursorWaitTimeout = TimeSpan.FromSeconds(5),
        };

        public string Path(string name) => System.IO.Path.Combine(_root, name);

        public static ScriptedJetstreamSource Session(params JetstreamLifecycleObservation[] observations)
            => new(observations);

        public void Dispose() => Directory.Delete(_root, recursive: true);
    }

    private sealed class ScriptedListReposSource(params ListReposPage[] pages) : IListReposSource
    {
        private readonly Queue<ListReposPage> _pages = new(pages);

        public ValueTask<ListReposPage> GetPageAsync(
            ListReposRequest request,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_pages.Count == 0)
            {
                throw new InvalidOperationException("No scripted listRepos page remains.");
            }

            return ValueTask.FromResult(_pages.Dequeue());
        }
    }

    private sealed class ScriptedJetstreamSource(
        IReadOnlyList<JetstreamLifecycleObservation> observations) : IJetstreamLifecycleSource
    {
        public ValueTask<IJetstreamLifecycleSession> OpenAsync(
            JetstreamOpenRequest request,
            CancellationToken cancellationToken)
            => ValueTask.FromResult<IJetstreamLifecycleSession>(new Session(observations));

        private sealed class Session(
            IReadOnlyList<JetstreamLifecycleObservation> observations) : IJetstreamLifecycleSession
        {
            private readonly TaskCompletionSource<ulong> _cursor = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private ulong? _latest;

            public string InstanceId => observations[0].InstanceId;

            public ulong? LatestReceivedCursor => _latest;

            public async IAsyncEnumerable<JetstreamLifecycleObservation> ReadAllAsync(
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                foreach (var observation in observations)
                {
                    _latest = observation.Cursor;
                    _cursor.TrySetResult(observation.Cursor);
                    yield return observation;
                    await Task.Yield();
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            public async ValueTask<ulong> WaitForCloseCursorAsync(
                TimeSpan timeout,
                CancellationToken cancellationToken)
            {
                while (_latest != observations[^1].Cursor)
                {
                    _ = await _cursor.Task.WaitAsync(timeout, cancellationToken);
                    await Task.Yield();
                }

                return _latest.Value;
            }

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
