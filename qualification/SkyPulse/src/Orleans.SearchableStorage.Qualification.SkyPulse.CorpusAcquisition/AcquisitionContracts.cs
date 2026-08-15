namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

public static class AcquisitionContract
{
    public const string ManifestFormat = "skypulse-atproto-observed-census-manifest-v1";
    public const string CheckpointFormat = "skypulse-atproto-acquisition-checkpoint-v1";
    public const string RoutingManifestFormat = "skypulse-private-routing-manifest-v1";
    public const string RoutingArtifactFormat = "skypulse-private-routing-ndjson-v1";
    public const string JournalFormat = "skypulse-sanitized-lifecycle-observation-ndjson-v1";
    public const string JournalFileName = "observations.private.ndjson";
    public const string PartialJournalFileName = "observations.private.ndjson.partial";
    public const string ManifestFileName = "acquisition.manifest.json";
    public const string CheckpointFileName = "acquisition.checkpoint.json";
    public const string CursorLedgerFileName = "acquisition.cursors.private.bin";
    public const string RoutingFileName = "routing.private.ndjson";
    public const string RoutingManifestFileName = "routing.private.manifest.json";

    // Contract evidence captured from the official upstream repositories on 2026-08-15.
    public const string AtProtoRepository = "https://github.com/bluesky-social/atproto";
    public const string AtProtoCommit = "02f6e227bbb35da2596c476fdf2711d14036ef0b";
    public const string ListReposLexiconPath = "lexicons/com/atproto/sync/listRepos.json";
    public const string ListReposLexiconSha256 = "afb3599e6075c8b413cf3431e3d0ce0e66aa7eff681bcdfd0bf88aefdf0b52d1";
    public const string SubscribeReposLexiconPath = "lexicons/com/atproto/sync/subscribeRepos.json";
    public const string SubscribeReposLexiconSha256 = "bfc3e22bfeae701736fbbbd68a56f0b4b8b66ef4e0e10f1c281b2de61c3328ae";

    public const string JetstreamRepository = "https://github.com/bluesky-social/jetstream";
    public const string JetstreamCommit = "9a30defd224e9058814a7d6ce8d9e4fc48d5493c";
    public const string JetstreamLexiconPath = "lexicons/network/bsky/jetstream/subscribeEvents.json";
    public const string JetstreamLexiconSha256 = "fcc7532518a896771d69c71462f57e94d454f96bc6d63e951d10285d9f8f37be";
    public const string JetstreamSubprotocol = "xrpc.v1.json";
    public const string JetstreamXrpcPath = "/xrpc/network.bsky.jetstream.subscribeEvents";
    public const string ListReposXrpcPath = "/xrpc/com.atproto.sync.listRepos";
}

public enum UnknownLifecyclePolicy
{
    FailRun = 0,
    QuarantineRun = 1,
}

public enum JetstreamLifecycleKind
{
    Account = 0,
    Identity = 1,
    Sync = 2,
}

public sealed record AcquisitionOptions
{
    public required string OutputDirectory { get; init; }

    public required Uri JetstreamEndpoint { get; init; }

    /// <summary>
    /// Operator-pinned identity for the exact Jetstream deployment. Jetstream cursors are
    /// instance-local and the wire currently exposes no cryptographic server instance ID.
    /// </summary>
    public required string JetstreamInstanceId { get; init; }

    public required Uri RelayEndpoint { get; init; }

    public required string RelayInstanceId { get; init; }

    public int FullSweepCount { get; init; } = 2;

    public int ListReposPageLimit { get; init; } = 1000;

    public int MaximumPagesPerSweep { get; init; } = 100_000;

    public long MaximumLifecycleFrames { get; init; } = 10_000_000;

    public int MaximumJetstreamFrameBytes { get; init; } = 16 * 1024;

    public int MaximumListReposResponseBytes { get; init; } = 8 * 1024 * 1024;

    public TimeSpan CloseCursorWaitTimeout { get; init; } = TimeSpan.FromMinutes(5);

    public UnknownLifecyclePolicy UnknownLifecyclePolicy { get; init; } = UnknownLifecyclePolicy.FailRun;
}

public sealed record JetstreamOpenRequest(
    Uri Endpoint,
    string ExpectedInstanceId,
    ulong? InclusiveCursor,
    int MaximumFrameBytes);

public sealed record JetstreamLifecycleObservation(
    string InstanceId,
    ulong Cursor,
    JetstreamLifecycleKind Kind,
    string Did,
    bool? Active);

public interface IJetstreamLifecycleSource
{
    ValueTask<IJetstreamLifecycleSession> OpenAsync(
        JetstreamOpenRequest request,
        CancellationToken cancellationToken);
}

public interface IJetstreamLifecycleSession : IAsyncDisposable
{
    string InstanceId { get; }

    ulong? LatestReceivedCursor { get; }

    IAsyncEnumerable<JetstreamLifecycleObservation> ReadAllAsync(
        CancellationToken cancellationToken);

    ValueTask<ulong> WaitForCloseCursorAsync(TimeSpan timeout, CancellationToken cancellationToken);
}

public sealed record ListReposRequest(
    Uri Endpoint,
    string ExpectedInstanceId,
    int Limit,
    string? Cursor);

public sealed record ListReposObservation(string Did, bool? Active);

public sealed record ListReposPage(
    string InstanceId,
    IReadOnlyList<ListReposObservation> Repositories,
    string? NextCursor);

public interface IListReposSource
{
    ValueTask<ListReposPage> GetPageAsync(
        ListReposRequest request,
        CancellationToken cancellationToken);
}

public sealed record AcquisitionResult(
    string OutputDirectory,
    string JournalPath,
    string ManifestPath,
    AcquisitionManifest Manifest);

public sealed record ContractIdentity(
    string Repository,
    string Commit,
    string Path,
    string Sha256);

public sealed record EndpointIdentity(string Uri, string InstanceId);

public sealed record SweepEvidence(
    int Sweep,
    int PageCount,
    long RepositoryCount,
    string TerminalCursorSha256);

public sealed record AcquisitionCountEvidence(
    long JournalObservations,
    long JetstreamAccountEvents,
    long JetstreamIdentityEvents,
    long JetstreamSyncEvents,
    long ListReposRepositories);

public sealed record PrivateArtifactEvidence(long ByteLength, string Sha256);

public sealed record AcquisitionManifest(
    string Format,
    string Claim,
    bool FreezeEligible,
    EndpointIdentity Jetstream,
    EndpointIdentity Relay,
    IReadOnlyList<ContractIdentity> Contracts,
    string JetstreamFilter,
    ulong StartCursor,
    ulong CloseCursor,
    int RequiredFullSweeps,
    int PageLimit,
    IReadOnlyList<SweepEvidence> Sweeps,
    AcquisitionCountEvidence Counts,
    PrivateArtifactEvidence Journal);

public sealed class AcquisitionContractException : IOException
{
    public AcquisitionContractException(string reasonCode, string message)
        : base(message)
    {
        ReasonCode = reasonCode;
    }

    public AcquisitionContractException(string reasonCode, string message, Exception innerException)
        : base(message, innerException)
    {
        ReasonCode = reasonCode;
    }

    public string ReasonCode { get; }
}
