namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

internal enum AcquisitionPhase
{
    Capturing = 0,
    Sweeping = 1,
    Draining = 2,
    Complete = 3,
    Poisoned = 4,
}

internal sealed record MutableSweepEvidence
{
    public required int Sweep { get; init; }

    public int PageCount { get; set; }

    public long RepositoryCount { get; set; }

    public string TerminalCursorSha256 { get; set; } = PrivateArtifactIO.Sha256Text("end");
}

internal sealed record AcquisitionCheckpoint
{
    public required string Format { get; init; }

    public required string JetstreamUri { get; init; }

    public required string JetstreamInstanceId { get; init; }

    public required string RelayUri { get; init; }

    public required string RelayInstanceId { get; init; }

    public required int FullSweepCount { get; init; }

    public required int PageLimit { get; init; }

    public required UnknownLifecyclePolicy UnknownLifecyclePolicy { get; init; }

    public AcquisitionPhase Phase { get; set; }

    public long NextOrdinal { get; set; } = 1;

    public long JournalByteLength { get; set; }

    public long CursorLedgerByteLength { get; set; }

    public ulong? FirstJetstreamCursor { get; set; }

    public ulong? LastJetstreamCursor { get; set; }

    public string? LastJetstreamFingerprint { get; set; }

    public ulong? CloseCursor { get; set; }

    public int CurrentSweep { get; set; } = 1;

    public string? NextPageCursor { get; set; }

    public int CurrentSweepPageCount { get; set; }

    public long CurrentSweepRepositoryCount { get; set; }

    public List<MutableSweepEvidence> CompletedSweeps { get; init; } = [];

    public long JournalObservations { get; set; }

    public long JetstreamAccountEvents { get; set; }

    public long JetstreamIdentityEvents { get; set; }

    public long JetstreamSyncEvents { get; set; }

    public long ListReposRepositories { get; set; }

    public long LifecycleFrames { get; set; }

    public string? PoisonReasonCode { get; set; }
}
