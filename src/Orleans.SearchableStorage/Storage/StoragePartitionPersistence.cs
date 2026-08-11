using System.Globalization;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Coordinates the manifest commit point with bounded journal and snapshot slots for one partition.
/// </summary>
internal sealed class StoragePartitionPersistence
{
    private static readonly Action<ILogger, string, Exception?> LogAutomaticCompactionFailure =
        LoggerMessage.Define<string>(
            LogLevel.Warning,
            new EventId(1, nameof(LogAutomaticCompactionFailure)),
            "Automatic compaction failed for searchable storage partition {PartitionKey}; the committed partition state remains authoritative.");

    private static readonly Action<ILogger, string, Exception?> LogCleanupFailure =
        LoggerMessage.Define<string>(
            LogLevel.Warning,
            new EventId(2, nameof(LogCleanupFailure)),
            "Persistence cleanup failed for searchable storage partition {PartitionKey}; authoritative data is unchanged and cleanup will be retried.");

    private readonly IGrainFactory _grainFactory;
    private readonly ILogger _logger;
    private readonly IPersistentState<StoragePartitionManifestState> _manifest;
    private readonly string _partitionKey;
    private readonly Action _poisonActivation;
    private bool _manifestWriteOutcomeAmbiguous;
    private bool _writerEpochAcquired;

    public StoragePartitionPersistence(
        IPersistentState<StoragePartitionManifestState> manifest,
        IGrainFactory grainFactory,
        string partitionKey,
        Action poisonActivation,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        ArgumentNullException.ThrowIfNull(poisonActivation);
        ArgumentNullException.ThrowIfNull(logger);

        _manifest = manifest;
        _grainFactory = grainFactory;
        _partitionKey = partitionKey;
        _logger = logger;
        _poisonActivation = poisonActivation;
    }

    public long CommittedSequence
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.Initialized ? _manifest.State.CommittedSequence : 0;
        }
    }

    public Guid CommittedOperationId
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.Initialized
                ? _manifest.State.CommittedOperationId
                : Guid.Empty;
        }
    }

    public long NextSequence => checked(CommittedSequence + 1);

    public long NextVersion
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.Initialized ? _manifest.State.NextVersion : 1;
        }
    }

    public long WriterEpoch
    {
        get
        {
            EnsureCoordinatorUsable();
            if (!_writerEpochAcquired)
            {
                throw new InvalidOperationException("A writer epoch has not been acquired for this activation.");
            }

            return _manifest.State.WriterEpoch;
        }
    }

    public bool IsInitialized
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.Initialized;
        }
    }

    public bool RoutedOperationsRequired
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.Initialized && _manifest.State.RoutedOperationsRequired;
        }
    }

    public long MinimumRoutingEpoch
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.Initialized ? _manifest.State.MinimumRoutingEpoch : 1;
        }
    }

    public int IndexSchemaProtocolVersion
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.Initialized
                ? _manifest.State.IndexSchemaProtocolVersion
                : 0;
        }
    }

    public StoragePartitionMoveControl MoveControl
    {
        get
        {
            EnsureCoordinatorUsable();
            return _manifest.State.MoveControl.Copy();
        }
    }

    public async Task<Dictionary<string, StoredRecord>> ActivateAsync()
    {
        EnsureCoordinatorUsable();
        var records = await RecoverAsync();
        if (_manifest.State.PendingSnapshot.IsPresent)
        {
            await PublishPendingSnapshotAsync(records);
        }

        try
        {
            await CompleteCleanupAsync();
        }
        catch (Exception exception)
        {
            _poisonActivation();
            LogCleanupFailure(_logger, _partitionKey, exception);
            throw;
        }

        return records;
    }

    public async Task EnableMovementProtocolAsync(
        StoragePersistenceSettings settings,
        long minimumRoutingEpoch,
        int indexSchemaProtocolVersion = 0)
    {
        EnsureCoordinatorUsable();
        ValidateSettings(settings);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumRoutingEpoch);
        if (indexSchemaProtocolVersion is not 0 and not StorageIndexSchema.ProtocolVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(indexSchemaProtocolVersion),
                indexSchemaProtocolVersion,
                "Unknown index-schema protocol version.");
        }

        var candidate = _manifest.State.Initialized
            ? _manifest.State.Copy()
            : CreateInitialManifest(settings);
        EnsureSettingsMatch(settings);
        if (candidate.MoveControl.IsPresent)
        {
            throw new InvalidOperationException(
                "The movement protocol cannot be enabled while this partition participates in a move.");
        }

        if (candidate.MovementProtocolVersion == StorageMoveProtocol.Version
            && candidate.RoutedOperationsRequired
            && candidate.MinimumRoutingEpoch == minimumRoutingEpoch
            && candidate.IndexSchemaProtocolVersion >= indexSchemaProtocolVersion)
        {
            return;
        }

        if (candidate.MovementProtocolVersion is not 0 and not StorageMoveProtocol.Version)
        {
            throw new InvalidOperationException(
                $"Movement protocol version {candidate.MovementProtocolVersion} is not supported.");
        }

        if (candidate.RoutedOperationsRequired
            && minimumRoutingEpoch < candidate.MinimumRoutingEpoch)
        {
            throw new InvalidOperationException("A durable minimum routing epoch cannot move backwards.");
        }

        candidate.PersistenceFormatVersion = candidate.IndexSchemaProtocolVersion == StorageIndexSchema.ProtocolVersion
            || indexSchemaProtocolVersion == StorageIndexSchema.ProtocolVersion
                ? StoragePersistence.CurrentPersistenceFormatVersion
                : StoragePersistence.MovementPersistenceFormatVersion;
        candidate.MovementProtocolVersion = StorageMoveProtocol.Version;
        candidate.RoutedOperationsRequired = true;
        candidate.MinimumRoutingEpoch = minimumRoutingEpoch;
        candidate.IndexSchemaProtocolVersion = Math.Max(
            candidate.IndexSchemaProtocolVersion,
            indexSchemaProtocolVersion);
        await PersistManifestAsync(candidate);
    }

    public async Task EnableIndexSchemaProtocolAsync(StoragePersistenceSettings settings)
    {
        EnsureCoordinatorUsable();
        ValidateSettings(settings);

        var candidate = _manifest.State.Initialized
            ? _manifest.State.Copy()
            : CreateInitialManifest(settings);
        EnsureSettingsMatch(settings);
        if (candidate.MoveControl.IsPresent)
        {
            throw new InvalidOperationException(
                "The index-schema protocol cannot be enabled while this partition participates in a move.");
        }

        if (candidate.PersistenceFormatVersion == StoragePersistence.CurrentPersistenceFormatVersion
            && candidate.IndexSchemaProtocolVersion == StorageIndexSchema.ProtocolVersion)
        {
            return;
        }

        if (!StoragePersistence.IsSupportedFormat(candidate.PersistenceFormatVersion)
            || candidate.IndexSchemaProtocolVersion is not 0 and not StorageIndexSchema.ProtocolVersion)
        {
            throw new InvalidOperationException(
                "The partition has an unsupported persistence or index-schema protocol version.");
        }

        candidate.PersistenceFormatVersion = StoragePersistence.CurrentPersistenceFormatVersion;
        candidate.IndexSchemaProtocolVersion = StorageIndexSchema.ProtocolVersion;
        candidate.MinimumRoutingEpoch = Math.Max(candidate.MinimumRoutingEpoch, 1);
        await PersistManifestAsync(candidate);
    }

    public async Task SetMoveControlAsync(
        StoragePartitionMoveControl moveControl,
        long? minimumRoutingEpoch = null)
    {
        EnsureCoordinatorUsable();
        ArgumentNullException.ThrowIfNull(moveControl);
        if (!_manifest.State.Initialized
            || !StoragePersistence.SupportsMovement(_manifest.State.PersistenceFormatVersion)
            || _manifest.State.MovementProtocolVersion != StorageMoveProtocol.Version
            || !_manifest.State.RoutedOperationsRequired)
        {
            throw new InvalidOperationException(
                "The partition movement protocol has not been durably enabled.");
        }

        var candidate = _manifest.State.Copy();
        candidate.MoveControl = moveControl.Copy();
        if (minimumRoutingEpoch is not null)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumRoutingEpoch.Value);
            if (minimumRoutingEpoch.Value < candidate.MinimumRoutingEpoch)
            {
                throw new InvalidOperationException("A durable minimum routing epoch cannot move backwards.");
            }

            candidate.MinimumRoutingEpoch = minimumRoutingEpoch.Value;
        }

        await PersistManifestAsync(candidate);
    }

    public Task ClearMoveControlAsync()
    {
        return SetMoveControlAsync(new StoragePartitionMoveControl());
    }

    public Task PrepareForProtocolMutationAsync(IReadOnlyDictionary<string, StoredRecord> records)
    {
        EnsureCoordinatorUsable();
        if (!_manifest.State.Initialized
            || !StoragePersistence.SupportsMovement(_manifest.State.PersistenceFormatVersion)
            || _manifest.State.MovementProtocolVersion != StorageMoveProtocol.Version
            || !_manifest.State.RoutedOperationsRequired)
        {
            throw new InvalidOperationException(
                "The partition movement protocol has not been durably enabled.");
        }

        return PrepareForMutationAsync(records, CreateProtocolPersistenceSettings());
    }

    public async Task PrepareForMutationAsync(
        IReadOnlyDictionary<string, StoredRecord> records,
        StoragePersistenceSettings settings)
    {
        EnsureCoordinatorUsable();
        ArgumentNullException.ThrowIfNull(records);
        ValidateSettings(settings);
        EnsureSettingsMatch(settings);

        if (_manifest.State.Initialized
            && _manifest.State.CommittedSequence - _manifest.State.SnapshotSequence
                >= settings.MaximumJournalReplayEntries)
        {
            // The hard replay bound is checked before allocating a journal slot. A compaction
            // failure therefore backpressures this mutation without extending the durable tail.
            await CompactCoreAsync(records);
            if (_manifest.State.CommittedSequence - _manifest.State.SnapshotSequence
                >= settings.MaximumJournalReplayEntries)
            {
                throw new InvalidOperationException(
                    "The partition journal reached its hard replay limit and compaction made no progress.");
            }
        }

        await AcquireWriterEpochAsync(settings);
    }

    public async Task CommitAsync(
        StorageJournalEntry entry,
        StoragePartitionMoveControl? moveControl = null)
    {
        EnsureCoordinatorUsable();
        ArgumentNullException.ThrowIfNull(entry);
        StoragePersistenceStateValidation.ValidateJournalEntry(entry, nameof(entry));
        ValidateJournalCapability(
            entry,
            _manifest.State.PersistenceFormatVersion,
            _manifest.State.IndexSchemaProtocolVersion,
            "The journal entry");
        if (!_writerEpochAcquired
            || entry.WriterEpoch != _manifest.State.WriterEpoch
            || entry.Sequence != checked(_manifest.State.CommittedSequence + 1)
            || entry.PreviousOperationId != _manifest.State.CommittedOperationId)
        {
            throw new InvalidOperationException(
                "A journal entry must extend the current activation's durable manifest commit point.");
        }

        if (_manifest.State.PendingSnapshot.IsPresent)
        {
            throw new InvalidOperationException("A mutation cannot commit while a snapshot publication is pending.");
        }

        var absoluteSegmentIndex = StoragePersistence.GetAbsoluteSegmentIndex(
            entry.Sequence,
            _manifest.State.JournalSegmentCapacity);
        try
        {
            await GetJournalSegment(absoluteSegmentIndex).StoreAsync(
                entry,
                _manifest.State.CommittedSequence,
                _manifest.State.CommittedOperationId,
                absoluteSegmentIndex,
                _manifest.State.JournalSegmentCapacity);
        }
        catch
        {
            _poisonActivation();
            throw;
        }

        var candidate = _manifest.State.Copy();
        candidate.CommittedSequence = entry.Sequence;
        candidate.CommittedOperationId = entry.OperationId;
        candidate.NextVersion = entry.NextVersionAfter;
        if (moveControl is not null)
        {
            candidate.MoveControl = moveControl.Copy();
        }

        await PersistManifestAsync(candidate);
    }

    public async Task CompactIfRequiredAsync(
        IReadOnlyDictionary<string, StoredRecord> records,
        int compactionThreshold)
    {
        EnsureCoordinatorUsable();
        ArgumentNullException.ThrowIfNull(records);
        if (!_manifest.State.Initialized)
        {
            return;
        }

        ValidateCompactionThreshold(compactionThreshold, _manifest.State.MaximumJournalReplayEntries);
        if (_manifest.State.CommittedSequence - _manifest.State.SnapshotSequence < compactionThreshold)
        {
            return;
        }

        try
        {
            await CompactCoreAsync(records);
        }
        catch (Exception exception)
        {
            // The user mutation was acknowledged by the manifest before maintenance began. Its
            // result must not be converted into a reported write failure by optional compaction.
            _poisonActivation();
            LogAutomaticCompactionFailure(_logger, _partitionKey, exception);
        }
    }

    public Task CompactAsync(IReadOnlyDictionary<string, StoredRecord> records)
    {
        EnsureCoordinatorUsable();
        ArgumentNullException.ThrowIfNull(records);
        return CompactCoreAsync(records);
    }

    public StoragePartitionPersistenceInfo CreateInfo(int recordCount)
    {
        EnsureCoordinatorUsable();
        ArgumentOutOfRangeException.ThrowIfNegative(recordCount);
        var state = _manifest.State;
        return new StoragePartitionPersistenceInfo
        {
            Initialized = state.Initialized,
            JournalSegmentCapacity = state.JournalSegmentCapacity,
            MaximumJournalReplayEntries = state.MaximumJournalReplayEntries,
            WriterEpoch = state.WriterEpoch,
            CommittedSequence = state.CommittedSequence,
            SnapshotSequence = state.SnapshotSequence,
            PrunedSequence = state.PrunedSequence,
            ActiveSnapshotGeneration = state.ActiveSnapshot.IsPresent ? state.ActiveSnapshot.Generation : 0,
            PendingSnapshotGeneration = state.PendingSnapshot.IsPresent ? state.PendingSnapshot.Generation : 0,
            RetiringSnapshotGeneration = state.RetiringSnapshot.IsPresent ? state.RetiringSnapshot.Generation : 0,
            RecordCount = recordCount,
        };
    }

    public StoragePartitionProtocolState CreateProtocolState()
    {
        EnsureCoordinatorUsable();
        var state = _manifest.State;
        return new StoragePartitionProtocolState
        {
            PersistenceFormatVersion = state.Initialized ? state.PersistenceFormatVersion : 0,
            MovementProtocolVersion = state.Initialized ? state.MovementProtocolVersion : 0,
            RoutedOperationsRequired = state.Initialized && state.RoutedOperationsRequired,
            MinimumRoutingEpoch = state.Initialized ? state.MinimumRoutingEpoch : 1,
            CommittedSequence = state.Initialized ? state.CommittedSequence : 0,
            NextVersion = state.Initialized ? state.NextVersion : 1,
            MoveControl = state.Initialized
                ? state.MoveControl.Copy()
                : new StoragePartitionMoveControl(),
            IndexSchemaProtocolVersion = state.Initialized
                ? state.IndexSchemaProtocolVersion
                : 0,
        };
    }

    public static void ValidateSettings(StoragePersistenceSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        StoragePersistence.ValidateOptions(
            settings.JournalSegmentCapacity,
            settings.MaximumJournalReplayEntries);
        ValidateCompactionThreshold(
            settings.CompactionThreshold,
            settings.MaximumJournalReplayEntries);
    }

    private static void ValidateCompactionThreshold(int threshold, int maximumReplayEntries)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(threshold);
        if (threshold > maximumReplayEntries)
        {
            throw new ArgumentOutOfRangeException(
                nameof(threshold),
                threshold,
                "The compaction threshold must not exceed the hard journal replay limit.");
        }
    }

    private StoragePersistenceSettings CreateProtocolPersistenceSettings()
    {
        return new StoragePersistenceSettings
        {
            JournalSegmentCapacity = _manifest.State.JournalSegmentCapacity,
            MaximumJournalReplayEntries = _manifest.State.MaximumJournalReplayEntries,
            // Protocol pages compact only at the hard replay boundary. This retains the existing
            // whole-partition snapshot boundary without coupling an admin operation to one silo's
            // mutable compaction preference.
            CompactionThreshold = _manifest.State.MaximumJournalReplayEntries,
        };
    }

    private async Task AcquireWriterEpochAsync(StoragePersistenceSettings settings)
    {
        if (_writerEpochAcquired)
        {
            return;
        }

        var candidate = _manifest.State.Initialized
            ? _manifest.State.Copy()
            : CreateInitialManifest(settings);
        candidate.WriterEpoch = checked(candidate.WriterEpoch + 1);
        await PersistManifestAsync(candidate);
        _writerEpochAcquired = true;
    }

    private async Task CompactCoreAsync(IReadOnlyDictionary<string, StoredRecord> records)
    {
        if (!_manifest.State.Initialized)
        {
            return;
        }

        if (_manifest.State.PendingSnapshot.IsPresent)
        {
            await PublishPendingSnapshotAsync(records);
        }

        await CompleteCleanupAsync();
        if (_manifest.State.CommittedSequence == _manifest.State.SnapshotSequence)
        {
            return;
        }

        var state = _manifest.State;
        ValidateRecordCapabilities(
            records.Values,
            state.PersistenceFormatVersion,
            state.IndexSchemaProtocolVersion,
            "The compacted partition");
        var generation = checked(state.SnapshotGenerationHighWatermark + 1);
        var targetSlot = state.ActiveSnapshot.IsPresent ? 1 - state.ActiveSnapshot.Slot : 0;
        var pending = new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = targetSlot,
            Generation = generation,
            SnapshotId = Guid.NewGuid(),
            Sequence = state.CommittedSequence,
            OperationId = state.CommittedOperationId,
            NextVersion = state.NextVersion,
        };

        var candidate = state.Copy();
        candidate.SnapshotGenerationHighWatermark = generation;
        candidate.PendingSnapshot = pending;
        await PersistManifestAsync(candidate);
        await PublishPendingSnapshotAsync(records);
        await CompleteCleanupAsync();
    }

    private async Task PublishPendingSnapshotAsync(IReadOnlyDictionary<string, StoredRecord> records)
    {
        var state = _manifest.State;
        var pending = state.PendingSnapshot;
        if (!pending.IsPresent)
        {
            return;
        }

        if (pending.Sequence != state.CommittedSequence
            || pending.OperationId != state.CommittedOperationId
            || pending.NextVersion != state.NextVersion
            || state.RetiringSnapshot.IsPresent)
        {
            throw new InvalidOperationException(
                "The pending snapshot does not identify the current committed partition state.");
        }

        var snapshot = StorageSnapshotFactory.Create(
            pending,
            records,
            state.PersistenceFormatVersion);

        try
        {
            await GetSnapshot(pending.Slot).StoreAsync(snapshot);
        }
        catch
        {
            _poisonActivation();
            throw;
        }

        var candidate = state.Copy();
        candidate.RetiringSnapshot = state.ActiveSnapshot.Copy();
        candidate.ActiveSnapshot = pending.Copy();
        candidate.PendingSnapshot = new StorageSnapshotDescriptor();
        candidate.SnapshotSequence = pending.Sequence;
        await PersistManifestAsync(candidate);
    }

    private async Task<Dictionary<string, StoredRecord>> RecoverAsync()
    {
        if (!_manifest.State.Initialized)
        {
            if (_manifest.RecordExists)
            {
                throw new InvalidOperationException(
                    "The partition manifest exists but does not identify a supported persistence format.");
            }

            return new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        }

        ValidateManifest(_manifest.State, allowPreviousFormat: true);
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var recoveredNextVersion = 1L;
        var recoveredOperationId = Guid.Empty;
        var recoveredOperationIds = new HashSet<Guid>();
        if (_manifest.State.ActiveSnapshot.IsPresent)
        {
            var descriptor = _manifest.State.ActiveSnapshot;
            StorageSnapshotState snapshot;
            try
            {
                snapshot = await GetSnapshot(descriptor.Slot).ReadAsync();
            }
            catch
            {
                _poisonActivation();
                throw;
            }

            records = ValidateActiveSnapshot(
                descriptor,
                snapshot,
                _manifest.State.PersistenceFormatVersion,
                _manifest.State.IndexSchemaProtocolVersion);
            recoveredNextVersion = snapshot.NextVersion;
            recoveredOperationId = snapshot.OperationId;
            recoveredOperationIds.Add(snapshot.OperationId);
        }

        var sequence = checked(_manifest.State.SnapshotSequence + 1);
        while (sequence <= _manifest.State.CommittedSequence)
        {
            var absoluteSegmentIndex = StoragePersistence.GetAbsoluteSegmentIndex(
                sequence,
                _manifest.State.JournalSegmentCapacity);
            StorageJournalSegmentState segment;
            try
            {
                segment = await GetJournalSegment(absoluteSegmentIndex).ReadAsync();
            }
            catch
            {
                _poisonActivation();
                throw;
            }

            ValidateJournalSegment(
                segment,
                absoluteSegmentIndex,
                _manifest.State.JournalSegmentCapacity,
                _manifest.State.WriterEpoch,
                _manifest.State.CommittedSequence,
                _manifest.State.PersistenceFormatVersion,
                _manifest.State.IndexSchemaProtocolVersion);
            var segmentEnd = Math.Min(
                StoragePersistence.GetSegmentEndSequence(
                    absoluteSegmentIndex,
                    _manifest.State.JournalSegmentCapacity),
                _manifest.State.CommittedSequence);
            var entries = new Dictionary<long, StorageJournalEntry>();
            foreach (var entry in segment.Entries)
            {
                if (entry.Sequence < sequence || entry.Sequence > segmentEnd)
                {
                    continue;
                }

                if (!entries.TryAdd(entry.Sequence, entry))
                {
                    throw new InvalidOperationException(
                        $"Committed journal segment {absoluteSegmentIndex} contains duplicate sequence {entry.Sequence}.");
                }
            }

            while (sequence <= segmentEnd)
            {
                if (!entries.TryGetValue(sequence, out var entry))
                {
                    throw new InvalidOperationException($"Committed journal entry {sequence} is missing.");
                }

                StorageJournalReplay.ApplyEntry(
                    records,
                    entry,
                    sequence,
                    _manifest.State.WriterEpoch,
                    recoveredOperationIds,
                    ref recoveredNextVersion,
                    ref recoveredOperationId);
                sequence++;
            }
        }

        if (recoveredNextVersion != _manifest.State.NextVersion
            || recoveredOperationId != _manifest.State.CommittedOperationId)
        {
            throw new InvalidOperationException(
                "The partition manifest commit point does not match its snapshot and committed journal chain.");
        }

        return records;
    }

    private async Task CompleteCleanupAsync()
    {
        var state = _manifest.State;
        if (!state.Initialized || state.PendingSnapshot.IsPresent)
        {
            return;
        }

        if (state.RetiringSnapshot.IsPresent)
        {
            try
            {
                await GetSnapshot(state.RetiringSnapshot.Slot).RetireAsync(state.RetiringSnapshot);
            }
            catch
            {
                _poisonActivation();
                throw;
            }
        }

        var targetPrunedSequence = StoragePersistence.GetPrunableSequence(
            state.SnapshotSequence,
            state.JournalSegmentCapacity);
        var firstSegmentIndex = state.PrunedSequence / state.JournalSegmentCapacity;
        var segmentCount = targetPrunedSequence / state.JournalSegmentCapacity;
        for (var absoluteSegmentIndex = firstSegmentIndex;
             absoluteSegmentIndex < segmentCount;
             absoluteSegmentIndex++)
        {
            try
            {
                await GetJournalSegment(absoluteSegmentIndex).RetireAsync(absoluteSegmentIndex);
            }
            catch
            {
                _poisonActivation();
                throw;
            }
        }

        if (!state.RetiringSnapshot.IsPresent && state.PrunedSequence == targetPrunedSequence)
        {
            return;
        }

        var candidate = state.Copy();
        candidate.RetiringSnapshot = new StorageSnapshotDescriptor();
        candidate.PrunedSequence = targetPrunedSequence;
        await PersistManifestAsync(candidate);
    }

    private async Task PersistManifestAsync(StoragePartitionManifestState candidate)
    {
        EnsureCoordinatorUsable();
        // Ordinary mutations deliberately preserve their persistence format during a rolling
        // deploy. Only an explicit protocol enablement changes the durable capability gate.
        ValidateManifest(candidate, allowPreviousFormat: true);

        var previous = _manifest.State;
        _manifest.State = candidate;
        try
        {
            // The physical provider ETag makes this small manifest write the sole compare-and-swap
            // commit point. Child slots remain non-authoritative until their descriptor is published.
            await _manifest.WriteStateAsync();
        }
        catch
        {
            // A provider can commit and update its ETag before reporting a lost acknowledgement.
            // The restored value must never participate in another compare-and-swap.
            _manifestWriteOutcomeAmbiguous = true;
            _manifest.State = previous;
            _poisonActivation();
            throw;
        }
    }

    public void EnsureSettingsMatch(StoragePersistenceSettings settings)
    {
        EnsureCoordinatorUsable();
        if (!_manifest.State.Initialized)
        {
            return;
        }

        if (_manifest.State.JournalSegmentCapacity != settings.JournalSegmentCapacity
            || _manifest.State.MaximumJournalReplayEntries != settings.MaximumJournalReplayEntries)
        {
            throw new InvalidOperationException(
                "The configured journal layout does not match the persisted partition manifest.");
        }
    }

    private IStorageJournalSegmentGrain GetJournalSegment(long absoluteSegmentIndex)
    {
        var slotCount = StoragePersistence.GetJournalSlotCount(
            _manifest.State.MaximumJournalReplayEntries,
            _manifest.State.JournalSegmentCapacity);
        var slotIndex = StoragePersistence.GetJournalSlotIndex(
            absoluteSegmentIndex,
            _manifest.State.MaximumJournalReplayEntries,
            _manifest.State.JournalSegmentCapacity);
        return _grainFactory.GetGrain<IStorageJournalSegmentGrain>(
            StoragePersistence.CreateJournalSlotKey(_partitionKey, slotIndex, slotCount));
    }

    private IStorageSnapshotGrain GetSnapshot(int slot)
    {
        return _grainFactory.GetGrain<IStorageSnapshotGrain>(
            StoragePersistence.CreateSnapshotSlotKey(_partitionKey, slot));
    }

    private static StoragePartitionManifestState CreateInitialManifest(StoragePersistenceSettings settings)
    {
        return new StoragePartitionManifestState
        {
            Initialized = true,
            PersistenceFormatVersion = StoragePersistence.PreviousPersistenceFormatVersion,
            JournalSegmentCapacity = settings.JournalSegmentCapacity,
            MaximumJournalReplayEntries = settings.MaximumJournalReplayEntries,
            NextVersion = 1,
            MinimumRoutingEpoch = 1,
            MoveControl = new StoragePartitionMoveControl(),
        };
    }

    internal static void ValidateManifest(
        StoragePartitionManifestState state,
        bool allowPreviousFormat = false)
    {
        var isLegacyFormat =
            state.PersistenceFormatVersion == StoragePersistence.LegacyPersistenceFormatVersion;
        if (state.PersistenceFormatVersion != StoragePersistence.CurrentPersistenceFormatVersion
            && (!allowPreviousFormat
                || !StoragePersistence.IsSupportedFormat(state.PersistenceFormatVersion)))
        {
            throw new InvalidOperationException(
                $"Partition persistence format {state.PersistenceFormatVersion} is not supported; "
                + $"format {StoragePersistence.CurrentPersistenceFormatVersion} is required for new capabilities.");
        }

        ArgumentNullException.ThrowIfNull(state.MoveControl);
        if (isLegacyFormat)
        {
            ValidatePreviousFormatMovementFields(state);
        }
        else
        {
            ValidateMovementFields(state);
        }

        ValidateIndexSchemaFields(state);

        StoragePersistence.ValidateOptions(
            state.JournalSegmentCapacity,
            state.MaximumJournalReplayEntries);
        if (state.WriterEpoch < 0
            || state.CommittedSequence < 0
            || state.NextVersion <= 0
            || (state.CommittedSequence == 0) != (state.CommittedOperationId == Guid.Empty)
            || (state.CommittedSequence > 0 && state.WriterEpoch == 0)
            || state.SnapshotGenerationHighWatermark < 0
            || state.SnapshotSequence < 0
            || state.SnapshotSequence > state.CommittedSequence
            || state.PrunedSequence < 0
            || state.PrunedSequence > state.SnapshotSequence
            || state.CommittedSequence - state.SnapshotSequence > state.MaximumJournalReplayEntries)
        {
            throw new InvalidOperationException("The partition manifest contains invalid persistence boundaries.");
        }

        if (state.CommittedSequence == 0)
        {
            if (state.NextVersion != 1
                || state.SnapshotGenerationHighWatermark != 0
                || state.SnapshotSequence != 0
                || state.PrunedSequence != 0)
            {
                throw new InvalidOperationException(
                    "An empty partition manifest contains non-initial persistence state.");
            }
        }
        else if (state.NextVersion < (isLegacyFormat ? 2 : 1)
            || (isLegacyFormat && state.NextVersion - 1 > state.CommittedSequence)
            || state.SnapshotGenerationHighWatermark > state.CommittedSequence)
        {
            throw new InvalidOperationException(
                "A committed partition manifest contains invalid version or snapshot-generation state.");
        }

        if (StoragePersistence.GetPrunableSequence(
                state.PrunedSequence,
                state.JournalSegmentCapacity) != state.PrunedSequence)
        {
            throw new InvalidOperationException("The partition manifest contains an unaligned prune boundary.");
        }

        ValidateSnapshotDescriptors(state, isLegacyFormat);
    }

    private static void ValidateIndexSchemaFields(StoragePartitionManifestState state)
    {
        if (StoragePersistence.SupportsIndexSchemas(state.PersistenceFormatVersion))
        {
            if (state.IndexSchemaProtocolVersion != StorageIndexSchema.ProtocolVersion)
            {
                throw new InvalidOperationException(
                    "A persistence-v5 manifest lacks its required index-schema protocol capability.");
            }

            return;
        }

        if (state.IndexSchemaProtocolVersion != 0)
        {
            throw new InvalidOperationException(
                "A persistence-v3/v4 manifest contains index-schema protocol state.");
        }
    }

    private static void ValidatePreviousFormatMovementFields(StoragePartitionManifestState state)
    {
        if (state.MovementProtocolVersion != 0
            || state.RoutedOperationsRequired
            || state.MinimumRoutingEpoch is not 0 and not 1)
        {
            throw new InvalidOperationException(
                "A persistence-v3 manifest contains unsupported movement-protocol fields.");
        }

        ValidateAbsentMoveControl(state.MoveControl);
    }

    private static void ValidateMovementFields(StoragePartitionManifestState state)
    {
        if (state.MinimumRoutingEpoch <= 0
            || (state.MovementProtocolVersion == 0
                && (state.RoutedOperationsRequired || state.MinimumRoutingEpoch != 1))
            || (state.MovementProtocolVersion == StorageMoveProtocol.Version
                && !state.RoutedOperationsRequired)
            || state.MovementProtocolVersion is not 0 and not StorageMoveProtocol.Version)
        {
            throw new InvalidOperationException(
                "The partition manifest contains invalid movement-protocol boundaries.");
        }

        var move = state.MoveControl;
        if (!move.IsPresent)
        {
            ValidateAbsentMoveControl(move);
            return;
        }

        if (state.MovementProtocolVersion != StorageMoveProtocol.Version
            || !state.RoutedOperationsRequired
            || move.MoveId == Guid.Empty
            || move.Slot < 0
            || move.VirtualSlotCount <= 0
            || move.VirtualSlotCount > StorageLayout.MaximumVirtualSlotCount
            || move.Slot >= move.VirtualSlotCount
            || move.SourceEpoch <= 0
            || move.SourceOwner < 0
            || move.SourceOwner >= StorageLayout.MaximumVirtualSlotCount
            || move.TargetOwner < 0
            || move.TargetOwner >= StorageLayout.MaximumVirtualSlotCount
            || move.SourceOwner == move.TargetOwner
            || move.FrozenNextVersion <= 0
            || move.NextPageOrdinal < 0
            || move.ImportedRecordCount < 0
            || move.ImportedByteCount < 0
            || move.DeletedRecordCount < 0
            || move.DeletedByteCount < 0
            || move.LastPageItemLimit < 0
            || move.LastPageByteTarget < 0
            || move.LastPageEncodedByteCount < 0
            || move.LastPageDigest is null
            || (move.NextPageOrdinal == 0
                && (move.ProgressAfterRecordKey is not null
                    || move.LastPageDigest.Length != 0
                    || move.LastPageRequestAfterRecordKey is not null
                    || move.LastPageItemLimit != 0
                    || move.LastPageByteTarget != 0
                    || move.LastPageEncodedByteCount != 0))
            || (move.NextPageOrdinal > 0
                && (move.LastPageDigest.Length != StorageMovePageDigest.DigestLength
                    || move.LastPageItemLimit <= 0
                    || move.LastPageItemLimit > StorageMoveProtocol.MaximumPageRecords
                    || move.LastPageByteTarget <= 0
                    || move.LastPageByteTarget > StorageMoveProtocol.MaximumPageBytes)))
        {
            throw new InvalidOperationException("The partition manifest contains invalid move control.");
        }

        try
        {
            ValidateMoveCursor(move.ProgressAfterRecordKey, nameof(move.ProgressAfterRecordKey));
            ValidateMoveCursor(
                move.LastPageRequestAfterRecordKey,
                nameof(move.LastPageRequestAfterRecordKey));
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException(
                "The partition manifest contains an invalid lossless move cursor.",
                exception);
        }

        var sourcePhase = move.Phase is StoragePartitionMovePhase.SourceFrozen
            or StoragePartitionMovePhase.SourceHidden
            or StoragePartitionMovePhase.SourceDeleting
            or StoragePartitionMovePhase.SourceDeleteComplete;
        var targetPhase = move.Phase is StoragePartitionMovePhase.TargetPrepared
            or StoragePartitionMovePhase.TargetImporting
            or StoragePartitionMovePhase.TargetImportComplete
            or StoragePartitionMovePhase.TargetEnabled
            or StoragePartitionMovePhase.TargetAbortDeleting
            or StoragePartitionMovePhase.TargetAbortComplete;
        if ((!sourcePhase && !targetPhase)
            || (move.Role == StoragePartitionMoveRole.Source) != sourcePhase
            || (move.Role == StoragePartitionMoveRole.Target) != targetPhase)
        {
            throw new InvalidOperationException(
                "The partition move role and phase are inconsistent.");
        }

        if (sourcePhase)
        {
            if (move.ImportedRecordCount != 0
                || move.ImportedByteCount != 0
                || (move.Phase is StoragePartitionMovePhase.SourceFrozen
                        or StoragePartitionMovePhase.SourceHidden
                    && (move.NextPageOrdinal != 0
                        || move.DeletedRecordCount != 0
                        || move.DeletedByteCount != 0))
                || (move.Phase is StoragePartitionMovePhase.SourceDeleting
                        or StoragePartitionMovePhase.SourceDeleteComplete
                    && move.NextPageOrdinal == 0)
                || move.FrozenNextVersion > state.NextVersion
                || (move.Phase == StoragePartitionMovePhase.SourceFrozen
                    && state.MinimumRoutingEpoch > move.SourceEpoch)
                || (move.Phase != StoragePartitionMovePhase.SourceFrozen
                    && state.MinimumRoutingEpoch <= move.SourceEpoch))
            {
                throw new InvalidOperationException(
                    "The source move control is inconsistent with its version or visibility fence.");
            }
        }
        else if ((move.Phase == StoragePartitionMovePhase.TargetPrepared
                && (move.NextPageOrdinal != 0
                    || move.ImportedRecordCount != 0
                    || move.ImportedByteCount != 0
                    || move.DeletedRecordCount != 0
                    || move.DeletedByteCount != 0))
            || (move.Phase is StoragePartitionMovePhase.TargetImporting
                    or StoragePartitionMovePhase.TargetImportComplete
                    or StoragePartitionMovePhase.TargetEnabled
                && (move.DeletedRecordCount != 0 || move.DeletedByteCount != 0))
            || (move.Phase is StoragePartitionMovePhase.TargetImportComplete
                    or StoragePartitionMovePhase.TargetEnabled
                    or StoragePartitionMovePhase.TargetAbortDeleting
                    or StoragePartitionMovePhase.TargetAbortComplete
                && move.NextPageOrdinal == 0)
            || (move.Phase != StoragePartitionMovePhase.TargetPrepared
                && state.NextVersion < move.FrozenNextVersion))
        {
            throw new InvalidOperationException(
                "The target move control has invalid progress or lacks its source version fence.");
        }
    }

    private static void ValidateAbsentMoveControl(StoragePartitionMoveControl move)
    {
        if (move.IsPresent
            || move.MoveId != Guid.Empty
            || move.Slot != 0
            || move.VirtualSlotCount != 0
            || move.SourceEpoch != 0
            || move.SourceOwner != 0
            || move.TargetOwner != 0
            || move.Role != StoragePartitionMoveRole.None
            || move.Phase != StoragePartitionMovePhase.None
            || move.FrozenNextVersion != 0
            || move.ProgressAfterRecordKey is not null
            || move.NextPageOrdinal != 0
            || move.LastPageDigest is null
            || move.LastPageDigest.Length != 0
            || move.ImportedRecordCount != 0
            || move.ImportedByteCount != 0
            || move.DeletedRecordCount != 0
            || move.DeletedByteCount != 0
            || move.LastPageRequestAfterRecordKey is not null
            || move.LastPageItemLimit != 0
            || move.LastPageByteTarget != 0
            || move.LastPageEncodedByteCount != 0)
        {
            throw new InvalidOperationException("An absent partition move control contains persisted data.");
        }
    }

    private static void ValidateMoveCursor(byte[]? cursor, string parameterName)
    {
        if (cursor is null)
        {
            return;
        }

        var decoded = StorageMoveRecordCodec.DecodeText(cursor, parameterName);
        ArgumentException.ThrowIfNullOrWhiteSpace(decoded, parameterName);
    }

    private static void ValidateSnapshotDescriptors(
        StoragePartitionManifestState state,
        bool isPreviousFormat)
    {
        ArgumentNullException.ThrowIfNull(state.ActiveSnapshot);
        ArgumentNullException.ThrowIfNull(state.PendingSnapshot);
        ArgumentNullException.ThrowIfNull(state.RetiringSnapshot);

        ValidateAbsentDescriptor(state.ActiveSnapshot, nameof(state.ActiveSnapshot));
        ValidateAbsentDescriptor(state.PendingSnapshot, nameof(state.PendingSnapshot));
        ValidateAbsentDescriptor(state.RetiringSnapshot, nameof(state.RetiringSnapshot));

        if (state.ActiveSnapshot.IsPresent)
        {
            ValidateDescriptor(
                state.ActiveSnapshot,
                nameof(state.ActiveSnapshot),
                isPreviousFormat);
            if (state.ActiveSnapshot.Sequence != state.SnapshotSequence
                || state.ActiveSnapshot.Generation > state.SnapshotGenerationHighWatermark
                || state.ActiveSnapshot.NextVersion > state.NextVersion
                || ((state.ActiveSnapshot.Sequence == state.CommittedSequence)
                    != (state.ActiveSnapshot.OperationId == state.CommittedOperationId)))
            {
                throw new InvalidOperationException("The active snapshot descriptor does not match the manifest boundary.");
            }
        }
        else if (state.SnapshotSequence != 0 || state.RetiringSnapshot.IsPresent)
        {
            throw new InvalidOperationException(
                "A snapshot boundary or retiring snapshot requires an active snapshot descriptor.");
        }

        if (state.PendingSnapshot.IsPresent)
        {
            ValidateDescriptor(
                state.PendingSnapshot,
                nameof(state.PendingSnapshot),
                isPreviousFormat);
            if (state.PendingSnapshot.Generation != state.SnapshotGenerationHighWatermark
                || state.PendingSnapshot.Sequence != state.CommittedSequence
                || state.PendingSnapshot.OperationId != state.CommittedOperationId
                || state.PendingSnapshot.NextVersion != state.NextVersion
                || state.RetiringSnapshot.IsPresent
                || (state.ActiveSnapshot.IsPresent
                    && (state.PendingSnapshot.Slot == state.ActiveSnapshot.Slot
                        || state.PendingSnapshot.Generation != checked(state.ActiveSnapshot.Generation + 1)
                        || state.PendingSnapshot.Sequence <= state.ActiveSnapshot.Sequence
                        || state.PendingSnapshot.NextVersion < state.ActiveSnapshot.NextVersion
                        || state.PendingSnapshot.SnapshotId == state.ActiveSnapshot.SnapshotId
                        || state.PendingSnapshot.OperationId == state.ActiveSnapshot.OperationId))
                || (!state.ActiveSnapshot.IsPresent
                    && (state.PendingSnapshot.Generation != 1 || state.PendingSnapshot.Slot != 0)))
            {
                throw new InvalidOperationException("The pending snapshot descriptor is inconsistent with the manifest.");
            }
        }

        if (state.RetiringSnapshot.IsPresent)
        {
            ValidateDescriptor(
                state.RetiringSnapshot,
                nameof(state.RetiringSnapshot),
                isPreviousFormat);
            if (!state.ActiveSnapshot.IsPresent
                || state.RetiringSnapshot.Slot == state.ActiveSnapshot.Slot
                || state.ActiveSnapshot.Generation <= 1
                || state.RetiringSnapshot.Generation != state.ActiveSnapshot.Generation - 1
                || state.RetiringSnapshot.Sequence >= state.ActiveSnapshot.Sequence
                || state.RetiringSnapshot.NextVersion > state.ActiveSnapshot.NextVersion
                || state.RetiringSnapshot.SnapshotId == state.ActiveSnapshot.SnapshotId
                || state.RetiringSnapshot.OperationId == state.ActiveSnapshot.OperationId)
            {
                throw new InvalidOperationException("The retiring snapshot descriptor is inconsistent with the active snapshot.");
            }
        }

        var authoritativeGeneration = state.PendingSnapshot.IsPresent
            ? state.PendingSnapshot.Generation
            : state.ActiveSnapshot.IsPresent
                ? state.ActiveSnapshot.Generation
                : 0;
        if (state.SnapshotGenerationHighWatermark != authoritativeGeneration)
        {
            throw new InvalidOperationException(
                "The snapshot generation high-water mark does not identify the active or pending generation.");
        }
    }

    private static void ValidateDescriptor(
        StorageSnapshotDescriptor descriptor,
        string name,
        bool isPreviousFormat)
    {
        StoragePersistence.ValidateSnapshotSlot(descriptor.Slot, name);
        if (descriptor.Generation <= 0
            || descriptor.Slot != (descriptor.Generation - 1) % StoragePersistence.SnapshotSlotCount
            || descriptor.SnapshotId == Guid.Empty
            || descriptor.Sequence <= 0
            || descriptor.Generation > descriptor.Sequence
            || descriptor.OperationId == Guid.Empty
            || descriptor.NextVersion < (isPreviousFormat ? 2 : 1)
            || (isPreviousFormat && descriptor.NextVersion - 1 > descriptor.Sequence))
        {
            throw new InvalidOperationException($"Snapshot descriptor '{name}' is invalid.");
        }
    }

    private static void ValidateAbsentDescriptor(StorageSnapshotDescriptor descriptor, string name)
    {
        if (!descriptor.IsPresent
            && (descriptor.Slot != 0
                || descriptor.Generation != 0
                || descriptor.SnapshotId != Guid.Empty
                || descriptor.Sequence != 0
                || descriptor.OperationId != Guid.Empty
                || descriptor.NextVersion != 0))
        {
            throw new InvalidOperationException($"Absent snapshot descriptor '{name}' contains persisted data.");
        }
    }

    private static Dictionary<string, StoredRecord> ValidateActiveSnapshot(
        StorageSnapshotDescriptor descriptor,
        StorageSnapshotState snapshot,
        int persistenceFormatVersion,
        int indexSchemaProtocolVersion)
    {
        if (!snapshot.Initialized
            || snapshot.Tombstoned
            || !StoragePersistenceStateEquality.DescriptorEquals(descriptor, snapshot))
        {
            throw new InvalidOperationException(
                $"Committed snapshot generation {descriptor.Generation} is missing, retired, or has mismatched identity.");
        }

        var records = StorageSnapshotFactory.DecodeRecords(snapshot, persistenceFormatVersion);
        ValidateRecordCapabilities(
            records.Values,
            persistenceFormatVersion,
            indexSchemaProtocolVersion,
            $"Committed snapshot generation {descriptor.Generation}");
        foreach (var record in records.Values)
        {
            ValidateRecordVersion(record, snapshot.NextVersion, descriptor.Generation);
        }

        return records;
    }

    private static void ValidateJournalSegment(
        StorageJournalSegmentState segment,
        long absoluteSegmentIndex,
        int capacity,
        long maximumWriterEpoch,
        long committedSequence,
        int persistenceFormatVersion,
        int indexSchemaProtocolVersion)
    {
        if (!segment.Initialized
            || segment.Tombstoned
            || segment.AbsoluteSegmentIndex != absoluteSegmentIndex
            || segment.Capacity != capacity
            || segment.HighestWriterEpoch <= 0
            || segment.HighestWriterEpoch > maximumWriterEpoch
            || segment.Entries is null
            || segment.Entries.Count == 0
            || segment.Entries.Count > capacity)
        {
            throw new InvalidOperationException(
                $"Committed journal segment {absoluteSegmentIndex} is missing, retired, or invalid.");
        }

        var startSequence = StoragePersistence.GetSegmentStartSequence(absoluteSegmentIndex, capacity);
        var endSequence = StoragePersistence.GetSegmentEndSequence(absoluteSegmentIndex, capacity);
        var previousSequence = startSequence - 1;
        var operationIds = new HashSet<Guid>();
        var uncommittedEntryCount = 0;
        var highestEntryWriterEpoch = 0L;
        foreach (var entry in segment.Entries)
        {
            try
            {
                StoragePersistenceStateValidation.ValidateJournalEntry(entry, nameof(segment));
            }
            catch (ArgumentException exception)
            {
                throw new InvalidOperationException(
                    $"Committed journal segment {absoluteSegmentIndex} contains an invalid entry.",
                    exception);
            }

            if (entry.Sequence < startSequence
                || entry.Sequence > endSequence
                || entry.Sequence <= previousSequence
                || entry.WriterEpoch > segment.HighestWriterEpoch
                || entry.WriterEpoch > maximumWriterEpoch
                || !operationIds.Add(entry.OperationId))
            {
                throw new InvalidOperationException(
                    $"Committed journal segment {absoluteSegmentIndex} contains invalid entry boundaries.");
            }


            ValidateJournalCapability(
                entry,
                persistenceFormatVersion,
                indexSchemaProtocolVersion,
                $"Journal segment {absoluteSegmentIndex}");

            if (entry.Sequence > committedSequence
                && (entry.Sequence != checked(committedSequence + 1)
                    || ++uncommittedEntryCount > 1))
            {
                throw new InvalidOperationException(
                    $"Committed journal segment {absoluteSegmentIndex} contains an invalid uncommitted tail.");
            }

            previousSequence = entry.Sequence;
            highestEntryWriterEpoch = Math.Max(highestEntryWriterEpoch, entry.WriterEpoch);
        }

        if (segment.HighestWriterEpoch != highestEntryWriterEpoch)
        {
            throw new InvalidOperationException(
                $"Committed journal segment {absoluteSegmentIndex} contains inconsistent writer-epoch metadata.");
        }
    }

    internal static void ValidateJournalCapability(
        StorageJournalEntry entry,
        int persistenceFormatVersion,
        int indexSchemaProtocolVersion,
        string context)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(context);

        if (persistenceFormatVersion == StoragePersistence.LegacyPersistenceFormatVersion
            && entry.Operation is not StorageJournalOperation.Upsert
                and not StorageJournalOperation.Delete)
        {
            throw new InvalidOperationException(
                $"{context} contains an operation unavailable in persistence-v3.");
        }

        if (persistenceFormatVersion == StoragePersistence.MovementPersistenceFormatVersion
            && entry.Operation == StorageJournalOperation.Reindex)
        {
            throw new InvalidOperationException(
                $"{context} contains an index-schema operation unavailable in persistence-v4.");
        }

        var hasSchemaCapability =
            StoragePersistence.SupportsIndexSchemas(persistenceFormatVersion)
            && indexSchemaProtocolVersion == StorageIndexSchema.ProtocolVersion;
        var containsManagedRecord = entry.Record?.IndexSchemaFingerprint is not null
            || (entry.Move?.Imports.Any(
                static item => item.Record.IndexSchemaFingerprint is not null) ?? false);
        if (!hasSchemaCapability && containsManagedRecord)
        {
            throw new InvalidOperationException(
                $"{context} contains a managed record without its durable schema capability.");
        }

        if (!hasSchemaCapability && entry.Operation == StorageJournalOperation.Reindex)
        {
            throw new InvalidOperationException(
                $"{context} contains a Reindex entry without its durable schema capability.");
        }
    }

    private static void ValidateRecordCapabilities(
        IEnumerable<StoredRecord> records,
        int persistenceFormatVersion,
        int indexSchemaProtocolVersion,
        string context)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentException.ThrowIfNullOrWhiteSpace(context);
        if (StoragePersistence.SupportsIndexSchemas(persistenceFormatVersion)
            && indexSchemaProtocolVersion == StorageIndexSchema.ProtocolVersion)
        {
            return;
        }

        if (records.Any(static record => record.IndexSchemaFingerprint is not null))
        {
            throw new InvalidOperationException(
                $"{context} contains a managed record without its durable schema capability.");
        }
    }

    private static void ValidateRecordVersion(StoredRecord record, long nextVersion, long snapshotGeneration)
    {
        if (!long.TryParse(
                record.ETag,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var version)
            || version <= 0
            || version >= nextVersion)
        {
            throw new InvalidOperationException(
                $"Committed snapshot generation {snapshotGeneration} contains an invalid record version.");
        }
    }

    private void EnsureCoordinatorUsable()
    {
        if (_manifestWriteOutcomeAmbiguous)
        {
            throw new InvalidOperationException(
                "The partition persistence coordinator cannot be reused after an ambiguous manifest write.");
        }
    }
}
