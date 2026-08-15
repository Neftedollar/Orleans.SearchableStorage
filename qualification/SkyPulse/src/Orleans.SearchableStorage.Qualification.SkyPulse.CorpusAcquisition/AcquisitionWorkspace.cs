using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

internal sealed class AcquisitionWorkspace : IAsyncDisposable
{
    private const int CursorLedgerRecordBytes = sizeof(int) + 32;
    private readonly AcquisitionOptions _options;
    private readonly string _checkpointPath;
    private readonly string _partialJournalPath;
    private readonly string _finalJournalPath;
    private readonly string _manifestPath;
    private readonly FileStream _journal;
    private readonly FileStream _cursorLedger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly HashSet<string> _currentSweepCursorHashes = new(StringComparer.Ordinal);
    private bool _faulted;
    private bool _disposed;
    private bool _journalClosed;

    private AcquisitionWorkspace(
        AcquisitionOptions options,
        AcquisitionCheckpoint checkpoint,
        FileStream journal,
        FileStream cursorLedger)
    {
        _options = options;
        Checkpoint = checkpoint;
        _checkpointPath = Path.Combine(options.OutputDirectory, AcquisitionContract.CheckpointFileName);
        _partialJournalPath = Path.Combine(options.OutputDirectory, AcquisitionContract.PartialJournalFileName);
        _finalJournalPath = Path.Combine(options.OutputDirectory, AcquisitionContract.JournalFileName);
        _manifestPath = Path.Combine(options.OutputDirectory, AcquisitionContract.ManifestFileName);
        _journal = journal;
        _cursorLedger = cursorLedger;
    }

    public AcquisitionCheckpoint Checkpoint { get; }

    public static AcquisitionWorkspace Open(AcquisitionOptions options)
    {
        ValidateOptions(options);
        var output = Path.GetFullPath(options.OutputDirectory);
        PrivateArtifactIO.EnsurePrivateDirectory(output);
        var normalizedOptions = options with { OutputDirectory = output };

        var checkpointPath = Path.Combine(output, AcquisitionContract.CheckpointFileName);
        AcquisitionCheckpoint checkpoint;
        if (File.Exists(checkpointPath))
        {
            checkpoint = PrivateArtifactIO.ReadCanonical<AcquisitionCheckpoint>(checkpointPath);
            ValidateCheckpoint(checkpoint, normalizedOptions);
            if (checkpoint.Phase == AcquisitionPhase.Poisoned)
            {
                throw new AcquisitionContractException(
                    checkpoint.PoisonReasonCode ?? "poisoned",
                    "This acquisition run is poisoned and cannot be frozen or resumed.");
            }

            if (checkpoint.Phase == AcquisitionPhase.Complete)
            {
                throw new InvalidOperationException("A completed workspace must be opened through ReadCompleted.");
            }
        }
        else
        {
            if (Directory.EnumerateFileSystemEntries(output).Any())
            {
                throw new IOException("A new acquisition output directory must be empty.");
            }

            checkpoint = NewCheckpoint(normalizedOptions);
            PrivateArtifactIO.AtomicWriteCanonical(checkpointPath, checkpoint);
        }

        var partial = Path.Combine(output, AcquisitionContract.PartialJournalFileName);
        var final = Path.Combine(output, AcquisitionContract.JournalFileName);
        if (!File.Exists(partial) && File.Exists(final))
        {
            File.Move(final, partial);
        }
        else if (File.Exists(partial) && File.Exists(final))
        {
            throw new InvalidDataException("Both partial and final private journals exist.");
        }

        var journal = PrivateArtifactIO.OpenPrivateAppend(partial);
        RecoverLength(journal, checkpoint.JournalByteLength, "journal");
        var cursorLedger = PrivateArtifactIO.OpenPrivateAppend(
            Path.Combine(output, AcquisitionContract.CursorLedgerFileName));
        RecoverLength(cursorLedger, checkpoint.CursorLedgerByteLength, "cursor ledger");

        var workspace = new AcquisitionWorkspace(normalizedOptions, checkpoint, journal, cursorLedger);
        workspace.LoadCursorLedger();
        return workspace;
    }

    public static AcquisitionResult ReadCompleted(AcquisitionOptions options)
    {
        ValidateOptions(options);
        var output = Path.GetFullPath(options.OutputDirectory);
        PrivateArtifactIO.EnsurePrivateDirectory(output);
        var normalizedOptions = options with { OutputDirectory = output };
        var checkpoint = PrivateArtifactIO.ReadCanonical<AcquisitionCheckpoint>(
            Path.Combine(output, AcquisitionContract.CheckpointFileName));
        ValidateCheckpoint(checkpoint, normalizedOptions);
        if (checkpoint.Phase != AcquisitionPhase.Complete)
        {
            throw new InvalidOperationException("The acquisition workspace is not complete.");
        }

        var manifestPath = Path.Combine(output, AcquisitionContract.ManifestFileName);
        var manifest = PrivateArtifactIO.ReadCanonical<AcquisitionManifest>(manifestPath);
        AcquisitionManifestValidation.Validate(manifest);
        var journalPath = Path.Combine(output, AcquisitionContract.JournalFileName);
        var evidence = PrivateArtifactIO.InspectPrivate(journalPath);
        if (evidence != manifest.Journal)
        {
            throw new InvalidDataException("The completed private journal no longer matches its manifest.");
        }

        PrivateArtifactIO.EnsurePrivateMode(journalPath);
        return new AcquisitionResult(output, journalPath, manifestPath, manifest);
    }

    public async ValueTask MarkSweepingAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable();
            if (Checkpoint.Phase == AcquisitionPhase.Capturing)
            {
                Checkpoint.Phase = AcquisitionPhase.Sweeping;
                PersistCheckpoint();
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<bool> CommitJetstreamAsync(
        JetstreamLifecycleObservation observation,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable();
            ValidateInstance(observation.InstanceId, _options.JetstreamInstanceId, "jetstream-instance-switch");
            if (observation.Cursor == 0)
            {
                throw Contract("invalid-jetstream-cursor", "Jetstream delivered reserved cursor zero.");
            }

            ValidateDid(observation.Did, "invalid-jetstream-did");
            var fingerprint = Fingerprint(observation);
            if (Checkpoint.LastJetstreamCursor is { } prior)
            {
                if (observation.Cursor < prior)
                {
                    throw Contract("jetstream-cursor-regression", "Jetstream cursor regressed.");
                }

                if (observation.Cursor == prior)
                {
                    if (!string.Equals(
                            Checkpoint.LastJetstreamFingerprint,
                            fingerprint,
                            StringComparison.Ordinal))
                    {
                        throw Contract(
                            "jetstream-inclusive-replay-mismatch",
                            "Inclusive replay changed the sanitized event at the same cursor.");
                    }

                    return Checkpoint.CloseCursor is null || prior < Checkpoint.CloseCursor.Value;
                }
            }

            if (Checkpoint.CloseCursor is { } closeCursor && observation.Cursor > closeCursor)
            {
                return false;
            }

            Checkpoint.LifecycleFrames = checked(Checkpoint.LifecycleFrames + 1);
            if (Checkpoint.LifecycleFrames > _options.MaximumLifecycleFrames)
            {
                throw Contract("jetstream-frame-bound", "Jetstream lifecycle frame bound was exceeded.");
            }

            var ordinal = TakeOrdinal();
            switch (observation.Kind)
            {
                case JetstreamLifecycleKind.Account:
                    Checkpoint.JetstreamAccountEvents = checked(Checkpoint.JetstreamAccountEvents + 1);
                    if (observation.Active is null)
                    {
                        throw UnknownLifecycle("Jetstream account event omitted its explicit active state.");
                    }

                    AppendObservation(
                        ordinal,
                        observation.Did,
                        observation.Active.Value,
                        $"jetstream:{observation.Cursor}");
                    break;

                case JetstreamLifecycleKind.Identity:
                    Checkpoint.JetstreamIdentityEvents = checked(Checkpoint.JetstreamIdentityEvents + 1);
                    if (observation.Active is not null)
                    {
                        throw Contract("identity-status", "Identity events cannot assert lifecycle status.");
                    }

                    break;

                case JetstreamLifecycleKind.Sync:
                    Checkpoint.JetstreamSyncEvents = checked(Checkpoint.JetstreamSyncEvents + 1);
                    if (observation.Active is not null)
                    {
                        throw Contract("sync-status", "Sync events cannot assert lifecycle status.");
                    }

                    break;

                default:
                    throw Contract("unknown-jetstream-kind", "Jetstream delivered an unknown lifecycle kind.");
            }

            Checkpoint.FirstJetstreamCursor ??= observation.Cursor;
            Checkpoint.LastJetstreamCursor = observation.Cursor;
            Checkpoint.LastJetstreamFingerprint = fingerprint;
            CommitOutputThenCheckpoint();
            return Checkpoint.CloseCursor is null || observation.Cursor < Checkpoint.CloseCursor.Value;
        }
        catch (AcquisitionContractException exception)
        {
            PoisonUnderLock(exception.ReasonCode);
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask CommitListReposPageAsync(
        int sweep,
        int pageNumber,
        string? requestCursor,
        ListReposPage page,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable();
            ValidateInstance(page.InstanceId, _options.RelayInstanceId, "relay-instance-switch");
            if (Checkpoint.Phase != AcquisitionPhase.Sweeping
                || sweep != Checkpoint.CurrentSweep
                || pageNumber != Checkpoint.CurrentSweepPageCount + 1
                || !string.Equals(requestCursor, Checkpoint.NextPageCursor, StringComparison.Ordinal))
            {
                throw Contract("list-repos-state", "listRepos page does not match the durable sweep position.");
            }

            if (page.Repositories.Count > _options.ListReposPageLimit)
            {
                throw Contract("list-repos-page-size", "listRepos returned more repositories than requested.");
            }

            if (pageNumber > _options.MaximumPagesPerSweep)
            {
                throw Contract("list-repos-page-bound", "listRepos exceeded the configured page bound.");
            }

            ValidateOpaqueCursor(page.NextCursor);
            var requestCursorHash = CursorHash(requestCursor);
            if (!_currentSweepCursorHashes.Add(requestCursorHash))
            {
                throw Contract("list-repos-cursor-loop", "listRepos repeated an opaque request cursor.");
            }

            if (page.NextCursor is not null
                && _currentSweepCursorHashes.Contains(CursorHash(page.NextCursor)))
            {
                throw Contract("list-repos-cursor-loop", "listRepos returned an opaque cursor loop.");
            }

            foreach (var repository in page.Repositories)
            {
                ValidateDid(repository.Did, "invalid-list-repos-did");
                if (repository.Active is null)
                {
                    throw UnknownLifecycle("listRepos omitted an explicit active state.");
                }
            }

            var sourcePrefix = $"listrepos:s{sweep}:p{pageNumber}:c{requestCursorHash}";
            for (var index = 0; index < page.Repositories.Count; index++)
            {
                var repository = page.Repositories[index];
                var ordinal = TakeOrdinal();
                AppendObservation(
                    ordinal,
                    repository.Did,
                    repository.Active!.Value,
                    $"{sourcePrefix}:i{index + 1}");
            }

            AppendCursorLedger(sweep, requestCursorHash);
            Checkpoint.CurrentSweepPageCount = pageNumber;
            Checkpoint.CurrentSweepRepositoryCount = checked(
                Checkpoint.CurrentSweepRepositoryCount + page.Repositories.Count);
            Checkpoint.ListReposRepositories = checked(
                Checkpoint.ListReposRepositories + page.Repositories.Count);
            Checkpoint.NextPageCursor = page.NextCursor;

            if (page.NextCursor is null)
            {
                Checkpoint.CompletedSweeps.Add(
                    new MutableSweepEvidence
                    {
                        Sweep = sweep,
                        PageCount = Checkpoint.CurrentSweepPageCount,
                        RepositoryCount = Checkpoint.CurrentSweepRepositoryCount,
                        TerminalCursorSha256 = requestCursorHash,
                    });
                Checkpoint.CurrentSweep = checked(sweep + 1);
                Checkpoint.CurrentSweepPageCount = 0;
                Checkpoint.CurrentSweepRepositoryCount = 0;
                Checkpoint.NextPageCursor = null;
                _currentSweepCursorHashes.Clear();
            }

            CommitOutputThenCheckpoint();
        }
        catch (AcquisitionContractException exception)
        {
            PoisonUnderLock(exception.ReasonCode);
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<ulong> RecordCloseCursorAsync(
        ulong closeCursor,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable();
            if (Checkpoint.CompletedSweeps.Count < _options.FullSweepCount)
            {
                throw Contract("incomplete-sweeps", "The close cursor cannot be recorded before all sweeps drain.");
            }

            if (Checkpoint.LastJetstreamCursor is not { } last)
            {
                throw Contract("invalid-close-cursor", "No durable Jetstream cursor exists.");
            }

            var effectiveCloseCursor = Math.Max(closeCursor, last);
            Checkpoint.CloseCursor ??= effectiveCloseCursor;
            if (Checkpoint.CloseCursor != effectiveCloseCursor)
            {
                throw Contract("close-cursor-change", "The recorded close cursor changed across resume.");
            }

            Checkpoint.Phase = AcquisitionPhase.Draining;
            PersistCheckpoint();
            return effectiveCloseCursor;
        }
        catch (AcquisitionContractException exception)
        {
            PoisonUnderLock(exception.ReasonCode);
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<AcquisitionResult> FinalizeAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable();
            if (Checkpoint.Phase != AcquisitionPhase.Draining
                || Checkpoint.CloseCursor is not { } close
                || Checkpoint.LastJetstreamCursor is not { } last
                || last < close
                || Checkpoint.CompletedSweeps.Count != _options.FullSweepCount)
            {
                throw Contract("incomplete-census", "The bounded observed census has not fully drained.");
            }

            _journal.Flush(flushToDisk: true);
            _journal.Dispose();
            _journalClosed = true;
            _cursorLedger.Flush(flushToDisk: true);
            var journalEvidence = PrivateArtifactIO.InspectPrivate(_partialJournalPath);
            if (journalEvidence.ByteLength <= 0 || journalEvidence.ByteLength != Checkpoint.JournalByteLength)
            {
                throw Contract("journal-evidence", "The private journal evidence is incomplete.");
            }

            var manifest = BuildManifest(journalEvidence);
            AcquisitionManifestValidation.Validate(manifest);
            if (File.Exists(_finalJournalPath))
            {
                throw new IOException("The final private journal already exists.");
            }

            File.Move(_partialJournalPath, _finalJournalPath);
            PrivateArtifactIO.EnsurePrivateMode(_finalJournalPath);
            PrivateArtifactIO.AtomicWriteCanonical(_manifestPath, manifest);
            Checkpoint.Phase = AcquisitionPhase.Complete;
            PersistCheckpoint();
            return new AcquisitionResult(
                _options.OutputDirectory,
                _finalJournalPath,
                _manifestPath,
                manifest);
        }
        catch (AcquisitionContractException exception)
        {
            PoisonUnderLock(exception.ReasonCode);
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask PoisonAsync(string reasonCode, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            PoisonUnderLock(reasonCode);
        }
        finally
        {
            _gate.Release();
        }
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            if (!_journalClosed)
            {
                _journal.Dispose();
            }
            _cursorLedger.Dispose();
            _gate.Dispose();
        }

        return ValueTask.CompletedTask;
    }

    private AcquisitionManifest BuildManifest(PrivateArtifactEvidence journalEvidence)
        => new(
            AcquisitionContract.ManifestFormat,
            "bounded-observed-census-not-atomic-not-global",
            true,
            new EndpointIdentity(CanonicalUri(_options.JetstreamEndpoint), _options.JetstreamInstanceId),
            new EndpointIdentity(CanonicalUri(_options.RelayEndpoint), _options.RelayInstanceId),
            [
                new ContractIdentity(
                    AcquisitionContract.JetstreamRepository,
                    AcquisitionContract.JetstreamCommit,
                    AcquisitionContract.JetstreamLexiconPath,
                    AcquisitionContract.JetstreamLexiconSha256),
                new ContractIdentity(
                    AcquisitionContract.AtProtoRepository,
                    AcquisitionContract.AtProtoCommit,
                    AcquisitionContract.ListReposLexiconPath,
                    AcquisitionContract.ListReposLexiconSha256),
                new ContractIdentity(
                    AcquisitionContract.AtProtoRepository,
                    AcquisitionContract.AtProtoCommit,
                    AcquisitionContract.SubscribeReposLexiconPath,
                    AcquisitionContract.SubscribeReposLexiconSha256),
            ],
            "kinds=account&kinds=identity&kinds=sync",
            Checkpoint.FirstJetstreamCursor!.Value,
            Checkpoint.CloseCursor!.Value,
            _options.FullSweepCount,
            _options.ListReposPageLimit,
            Checkpoint.CompletedSweeps
                .Select(static sweep => new SweepEvidence(
                    sweep.Sweep,
                    sweep.PageCount,
                    sweep.RepositoryCount,
                    sweep.TerminalCursorSha256))
                .ToArray(),
            new AcquisitionCountEvidence(
                Checkpoint.JournalObservations,
                Checkpoint.JetstreamAccountEvents,
                Checkpoint.JetstreamIdentityEvents,
                Checkpoint.JetstreamSyncEvents,
                Checkpoint.ListReposRepositories),
            journalEvidence);

    private void AppendObservation(long ordinal, string did, bool active, string sourcePosition)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            writer.WriteStartObject();
            writer.WriteNumber("ordinal", ordinal);
            writer.WriteString("did", did);
            writer.WriteString("status", active ? "active" : "inactive");
            writer.WriteString("sourcePosition", sourcePosition);
            writer.WriteEndObject();
        }

        _journal.Write(buffer.WrittenSpan);
        _journal.WriteByte((byte)'\n');
        Checkpoint.JournalByteLength = checked(
            Checkpoint.JournalByteLength + buffer.WrittenCount + 1);
        Checkpoint.JournalObservations = checked(Checkpoint.JournalObservations + 1);
    }

    private void AppendCursorLedger(int sweep, string cursorHash)
    {
        Span<byte> record = stackalloc byte[CursorLedgerRecordBytes];
        BinaryPrimitives.WriteInt32BigEndian(record, sweep);
        Convert.FromHexString(cursorHash).CopyTo(record[sizeof(int)..]);
        _cursorLedger.Write(record);
        Checkpoint.CursorLedgerByteLength = checked(
            Checkpoint.CursorLedgerByteLength + CursorLedgerRecordBytes);
    }

    private void CommitOutputThenCheckpoint()
    {
        try
        {
            if (!_journalClosed)
            {
                _journal.Flush(flushToDisk: true);
            }
            _cursorLedger.Flush(flushToDisk: true);
            PersistCheckpoint();
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private void PersistCheckpoint()
        => PrivateArtifactIO.AtomicWriteCanonical(_checkpointPath, Checkpoint);

    private void PoisonUnderLock(string reasonCode)
    {
        if (Checkpoint.Phase == AcquisitionPhase.Complete)
        {
            return;
        }

        Checkpoint.Phase = AcquisitionPhase.Poisoned;
        Checkpoint.PoisonReasonCode = SanitizeReasonCode(reasonCode);
        try
        {
            if (!_journalClosed)
            {
                _journal.Flush(flushToDisk: true);
            }
            _cursorLedger.Flush(flushToDisk: true);
            PersistCheckpoint();
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private void LoadCursorLedger()
    {
        _cursorLedger.Position = 0;
        Span<byte> record = stackalloc byte[CursorLedgerRecordBytes];
        while (_cursorLedger.Position < _cursorLedger.Length)
        {
            _cursorLedger.ReadExactly(record);
            var sweep = BinaryPrimitives.ReadInt32BigEndian(record);
            if (sweep == Checkpoint.CurrentSweep)
            {
                _currentSweepCursorHashes.Add(
                    Convert.ToHexString(record[sizeof(int)..]).ToLowerInvariant());
            }
        }

        _cursorLedger.Position = _cursorLedger.Length;
    }

    private long TakeOrdinal()
    {
        var ordinal = Checkpoint.NextOrdinal;
        Checkpoint.NextOrdinal = checked(ordinal + 1);
        return ordinal;
    }

    private AcquisitionContractException UnknownLifecycle(string message)
        => Contract(
            _options.UnknownLifecyclePolicy == UnknownLifecyclePolicy.QuarantineRun
                ? "unknown-lifecycle-quarantined"
                : "unknown-lifecycle",
            message);

    private static AcquisitionContractException Contract(string reasonCode, string message)
        => new(reasonCode, message);

    private void ThrowIfUnavailable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted)
        {
            throw new InvalidOperationException("The workspace faulted after an uncertain durable write.");
        }

        if (Checkpoint.Phase is AcquisitionPhase.Poisoned or AcquisitionPhase.Complete)
        {
            throw new InvalidOperationException("The workspace no longer accepts observations.");
        }
    }

    private static string Fingerprint(JetstreamLifecycleObservation observation)
    {
        var active = observation.Active switch
        {
            true => "active",
            false => "inactive",
            null => "none",
        };
        return PrivateArtifactIO.Sha256Text(
            $"{observation.Cursor}\0{observation.Kind}\0{observation.Did}\0{active}");
    }

    private static string CursorHash(string? cursor)
        => PrivateArtifactIO.Sha256Text(cursor is null ? "<start>" : $"cursor\0{cursor}");

    private static void ValidateOpaqueCursor(string? cursor)
    {
        if (cursor is null)
        {
            return;
        }

        if (cursor.Length is 0 or > 2048
            || Encoding.UTF8.GetByteCount(cursor) > 2048
            || cursor.Any(static character => char.IsControl(character)))
        {
            throw Contract("invalid-list-repos-cursor", "listRepos returned an invalid opaque cursor.");
        }
    }

    private static void ValidateInstance(string actual, string expected, string reasonCode)
    {
        if (!string.Equals(actual, expected, StringComparison.Ordinal))
        {
            throw Contract(reasonCode, "The configured source instance identity changed during acquisition.");
        }
    }

    private static void ValidateDid(string did, string reasonCode)
    {
        try
        {
            _ = AccountKey.FromDid(did);
        }
        catch (ArgumentException exception)
        {
            throw new AcquisitionContractException(
                reasonCode,
                "A source returned a non-canonical repository DID.",
                exception);
        }
    }

    private static string SanitizeReasonCode(string reasonCode)
    {
        if (string.IsNullOrWhiteSpace(reasonCode)
            || reasonCode.Length > 80
            || reasonCode.Any(static character => character is not (>= 'a' and <= 'z')
                and not (>= '0' and <= '9') and not '-'))
        {
            return "unspecified-contract-failure";
        }

        return reasonCode;
    }

    private static void RecoverLength(FileStream stream, long committedLength, string artifactName)
    {
        if (committedLength < 0 || stream.Length < committedLength)
        {
            stream.Dispose();
            throw new InvalidDataException($"The private {artifactName} is shorter than its durable checkpoint.");
        }

        if (stream.Length > committedLength)
        {
            stream.SetLength(committedLength);
            stream.Flush(flushToDisk: true);
        }

        stream.Position = committedLength;
    }

    private static AcquisitionCheckpoint NewCheckpoint(AcquisitionOptions options)
        => new()
        {
            Format = AcquisitionContract.CheckpointFormat,
            JetstreamUri = CanonicalUri(options.JetstreamEndpoint),
            JetstreamInstanceId = options.JetstreamInstanceId,
            RelayUri = CanonicalUri(options.RelayEndpoint),
            RelayInstanceId = options.RelayInstanceId,
            FullSweepCount = options.FullSweepCount,
            PageLimit = options.ListReposPageLimit,
            UnknownLifecyclePolicy = options.UnknownLifecyclePolicy,
            Phase = AcquisitionPhase.Capturing,
        };

    private static void ValidateCheckpoint(AcquisitionCheckpoint checkpoint, AcquisitionOptions options)
    {
        if (!string.Equals(checkpoint.Format, AcquisitionContract.CheckpointFormat, StringComparison.Ordinal)
            || !string.Equals(checkpoint.JetstreamUri, CanonicalUri(options.JetstreamEndpoint), StringComparison.Ordinal)
            || !string.Equals(checkpoint.JetstreamInstanceId, options.JetstreamInstanceId, StringComparison.Ordinal)
            || !string.Equals(checkpoint.RelayUri, CanonicalUri(options.RelayEndpoint), StringComparison.Ordinal)
            || !string.Equals(checkpoint.RelayInstanceId, options.RelayInstanceId, StringComparison.Ordinal)
            || checkpoint.FullSweepCount != options.FullSweepCount
            || checkpoint.PageLimit != options.ListReposPageLimit
            || checkpoint.UnknownLifecyclePolicy != options.UnknownLifecyclePolicy)
        {
            throw new InvalidDataException("Acquisition options do not match the durable checkpoint identity.");
        }

        if (checkpoint.NextOrdinal <= 0
            || checkpoint.JournalByteLength < 0
            || checkpoint.CursorLedgerByteLength < 0
            || checkpoint.CursorLedgerByteLength % CursorLedgerRecordBytes != 0
            || checkpoint.CurrentSweep <= 0
            || checkpoint.CompletedSweeps.Count > options.FullSweepCount)
        {
            throw new InvalidDataException("The acquisition checkpoint is internally inconsistent.");
        }
    }

    private static void ValidateOptions(AcquisitionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.OutputDirectory))
        {
            throw new ArgumentException("An acquisition output directory is required.", nameof(options));
        }

        ValidateEndpoint(options.JetstreamEndpoint, websocket: true, nameof(options.JetstreamEndpoint));
        ValidateEndpoint(options.RelayEndpoint, websocket: false, nameof(options.RelayEndpoint));
        ValidateIdentity(options.JetstreamInstanceId, nameof(options.JetstreamInstanceId));
        ValidateIdentity(options.RelayInstanceId, nameof(options.RelayInstanceId));
        if (options.FullSweepCount < 2 || options.FullSweepCount > 16)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Two to sixteen full sweeps are required.");
        }

        if (options.ListReposPageLimit is < 1 or > 1000
            || options.MaximumPagesPerSweep is < 1 or > 1_000_000
            || options.MaximumLifecycleFrames <= 0
            || options.MaximumJetstreamFrameBytes is < 1024 or > 1024 * 1024
            || options.MaximumListReposResponseBytes is < 1024 or > 64 * 1024 * 1024
            || options.CloseCursorWaitTimeout <= TimeSpan.Zero
            || options.CloseCursorWaitTimeout > TimeSpan.FromHours(1))
        {
            throw new ArgumentOutOfRangeException(nameof(options), "One or more acquisition bounds are invalid.");
        }
    }

    private static void ValidateEndpoint(Uri endpoint, bool websocket, string name)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (!endpoint.IsAbsoluteUri
            || !string.IsNullOrEmpty(endpoint.UserInfo)
            || !string.IsNullOrEmpty(endpoint.Fragment)
            || !string.IsNullOrEmpty(endpoint.Query)
            || !string.Equals(endpoint.AbsolutePath, "/", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The endpoint must be an absolute origin URI without credentials, path, query, or fragment.",
                name);
        }

        var secure = websocket ? endpoint.Scheme == Uri.UriSchemeWss : endpoint.Scheme == Uri.UriSchemeHttps;
        var loopback = endpoint.IsLoopback
            && (websocket ? endpoint.Scheme == Uri.UriSchemeWs : endpoint.Scheme == Uri.UriSchemeHttp);
        if (!secure && !loopback)
        {
            throw new ArgumentException("Remote acquisition endpoints must use transport encryption.", name);
        }
    }

    private static void ValidateIdentity(string identity, string name)
    {
        if (string.IsNullOrWhiteSpace(identity)
            || identity.Length > 200
            || !string.Equals(identity, identity.Trim(), StringComparison.Ordinal)
            || identity.Any(static character => char.IsControl(character)))
        {
            throw new ArgumentException("A bounded canonical source instance identity is required.", name);
        }
    }

    internal static string CanonicalUri(Uri value)
        => value.GetComponents(UriComponents.HttpRequestUrl, UriFormat.UriEscaped);
}

internal static class AcquisitionManifestValidation
{
    public static void Validate(AcquisitionManifest manifest)
    {
        if (!string.Equals(manifest.Format, AcquisitionContract.ManifestFormat, StringComparison.Ordinal)
            || !string.Equals(manifest.Claim, "bounded-observed-census-not-atomic-not-global", StringComparison.Ordinal)
            || !manifest.FreezeEligible
            || manifest.RequiredFullSweeps < 2
            || manifest.Sweeps.Count != manifest.RequiredFullSweeps
            || manifest.StartCursor == 0
            || manifest.CloseCursor < manifest.StartCursor
            || manifest.Journal.ByteLength <= 0)
        {
            throw new InvalidDataException("The acquisition manifest is not a successful bounded observed census.");
        }

        PrivateArtifactIO.ValidateSha256(manifest.Journal.Sha256, "private journal SHA-256");
        for (var index = 0; index < manifest.Sweeps.Count; index++)
        {
            var sweep = manifest.Sweeps[index];
            if (sweep.Sweep != index + 1 || sweep.PageCount <= 0 || sweep.RepositoryCount < 0)
            {
                throw new InvalidDataException("The acquisition sweep evidence is inconsistent.");
            }

            PrivateArtifactIO.ValidateSha256(sweep.TerminalCursorSha256, "terminal cursor SHA-256");
        }

        var expected = new Dictionary<string, (string Repository, string Commit, string Sha256)>(
            StringComparer.Ordinal)
        {
            [AcquisitionContract.JetstreamLexiconPath] = (
                AcquisitionContract.JetstreamRepository,
                AcquisitionContract.JetstreamCommit,
                AcquisitionContract.JetstreamLexiconSha256),
            [AcquisitionContract.ListReposLexiconPath] = (
                AcquisitionContract.AtProtoRepository,
                AcquisitionContract.AtProtoCommit,
                AcquisitionContract.ListReposLexiconSha256),
            [AcquisitionContract.SubscribeReposLexiconPath] = (
                AcquisitionContract.AtProtoRepository,
                AcquisitionContract.AtProtoCommit,
                AcquisitionContract.SubscribeReposLexiconSha256),
        };
        if (manifest.Contracts.Count != expected.Count
            || manifest.Contracts.Any(contract => !expected.TryGetValue(contract.Path, out var identity)
                || !string.Equals(identity.Repository, contract.Repository, StringComparison.Ordinal)
                || !string.Equals(identity.Commit, contract.Commit, StringComparison.Ordinal)
                || !string.Equals(identity.Sha256, contract.Sha256, StringComparison.Ordinal)))
        {
            throw new InvalidDataException("The acquisition manifest contract identities are unknown.");
        }
    }
}
