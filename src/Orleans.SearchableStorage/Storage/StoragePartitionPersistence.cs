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

    public async Task CommitAsync(StorageJournalEntry entry)
    {
        EnsureCoordinatorUsable();
        ArgumentNullException.ThrowIfNull(entry);
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

        var snapshot = new StorageSnapshotState
        {
            Initialized = true,
            Slot = pending.Slot,
            Generation = pending.Generation,
            SnapshotId = pending.SnapshotId,
            Sequence = pending.Sequence,
            OperationId = pending.OperationId,
            NextVersion = pending.NextVersion,
            Records = records.ToDictionary(
                static pair => pair.Key,
                static pair => StoragePersistenceStateCopy.CopyRecord(pair.Value)!,
                StringComparer.Ordinal),
        };

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

        ValidateManifest(_manifest.State);
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

            ValidateActiveSnapshot(descriptor, snapshot);
            records = snapshot.Records.ToDictionary(
                static pair => pair.Key,
                static pair => StoragePersistenceStateCopy.CopyRecord(pair.Value)!,
                StringComparer.Ordinal);
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
                _manifest.State.CommittedSequence);
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

                ApplyEntry(
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
        ValidateManifest(candidate);

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
            PersistenceFormatVersion = StoragePersistence.CurrentPersistenceFormatVersion,
            JournalSegmentCapacity = settings.JournalSegmentCapacity,
            MaximumJournalReplayEntries = settings.MaximumJournalReplayEntries,
            NextVersion = 1,
        };
    }

    private static void ValidateManifest(StoragePartitionManifestState state)
    {
        if (state.PersistenceFormatVersion != StoragePersistence.CurrentPersistenceFormatVersion)
        {
            throw new InvalidOperationException(
                $"Partition persistence format {state.PersistenceFormatVersion} is not supported; "
                + $"format {StoragePersistence.CurrentPersistenceFormatVersion} is required.");
        }

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
        else if (state.NextVersion < 2
            || state.NextVersion - 1 > state.CommittedSequence
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

        ValidateSnapshotDescriptors(state);
    }

    private static void ValidateSnapshotDescriptors(StoragePartitionManifestState state)
    {
        ArgumentNullException.ThrowIfNull(state.ActiveSnapshot);
        ArgumentNullException.ThrowIfNull(state.PendingSnapshot);
        ArgumentNullException.ThrowIfNull(state.RetiringSnapshot);

        ValidateAbsentDescriptor(state.ActiveSnapshot, nameof(state.ActiveSnapshot));
        ValidateAbsentDescriptor(state.PendingSnapshot, nameof(state.PendingSnapshot));
        ValidateAbsentDescriptor(state.RetiringSnapshot, nameof(state.RetiringSnapshot));

        if (state.ActiveSnapshot.IsPresent)
        {
            ValidateDescriptor(state.ActiveSnapshot, nameof(state.ActiveSnapshot));
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
            ValidateDescriptor(state.PendingSnapshot, nameof(state.PendingSnapshot));
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
            ValidateDescriptor(state.RetiringSnapshot, nameof(state.RetiringSnapshot));
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

    private static void ValidateDescriptor(StorageSnapshotDescriptor descriptor, string name)
    {
        StoragePersistence.ValidateSnapshotSlot(descriptor.Slot, name);
        if (descriptor.Generation <= 0
            || descriptor.Slot != (descriptor.Generation - 1) % StoragePersistence.SnapshotSlotCount
            || descriptor.SnapshotId == Guid.Empty
            || descriptor.Sequence <= 0
            || descriptor.Generation > descriptor.Sequence
            || descriptor.OperationId == Guid.Empty
            || descriptor.NextVersion < 2
            || descriptor.NextVersion - 1 > descriptor.Sequence)
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

    private static void ValidateActiveSnapshot(
        StorageSnapshotDescriptor descriptor,
        StorageSnapshotState snapshot)
    {
        if (!snapshot.Initialized
            || snapshot.Tombstoned
            || !StoragePersistenceStateEquality.DescriptorEquals(descriptor, snapshot))
        {
            throw new InvalidOperationException(
                $"Committed snapshot generation {descriptor.Generation} is missing, retired, or has mismatched identity.");
        }

        foreach (var (recordKey, record) in snapshot.Records)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
            StoragePersistenceStateValidation.ValidateRecord(record, nameof(snapshot));
            ValidateRecordVersion(record, snapshot.NextVersion, descriptor.Generation);
        }
    }

    private static void ValidateJournalSegment(
        StorageJournalSegmentState segment,
        long absoluteSegmentIndex,
        int capacity,
        long maximumWriterEpoch,
        long committedSequence)
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

    private static void ApplyEntry(
        Dictionary<string, StoredRecord> records,
        StorageJournalEntry entry,
        long expectedSequence,
        long maximumWriterEpoch,
        HashSet<Guid> recoveredOperationIds,
        ref long nextVersion,
        ref Guid operationId)
    {
        ArgumentNullException.ThrowIfNull(recoveredOperationIds);
        if (entry.Sequence != expectedSequence
            || entry.WriterEpoch <= 0
            || entry.WriterEpoch > maximumWriterEpoch
            || entry.OperationId == Guid.Empty
            || entry.PreviousOperationId != operationId
            || !recoveredOperationIds.Add(entry.OperationId)
            || entry.NextVersionAfter <= 0
            || string.IsNullOrWhiteSpace(entry.RecordKey))
        {
            throw new InvalidOperationException($"Journal entry {expectedSequence} is invalid.");
        }

        records.TryGetValue(entry.RecordKey, out var currentRecord);
        if (!string.Equals(currentRecord?.ETag, entry.ExpectedETag, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Journal entry {entry.Sequence} does not follow the committed record version.");
        }

        switch (entry.Operation)
        {
            case StorageJournalOperation.Upsert when entry.Record is not null:
                StoragePersistenceStateValidation.ValidateRecord(entry.Record, nameof(entry));
                if (!string.Equals(
                        entry.Record.ETag,
                        nextVersion.ToString(CultureInfo.InvariantCulture),
                        StringComparison.Ordinal)
                    || entry.NextVersionAfter != checked(nextVersion + 1))
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} does not contain the next record version.");
                }

                records[entry.RecordKey] = StoragePersistenceStateCopy.CopyRecord(entry.Record)!;
                break;
            case StorageJournalOperation.Delete when entry.Record is null && currentRecord is not null:
                if (entry.NextVersionAfter != nextVersion)
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} changes the version during a delete.");
                }

                records.Remove(entry.RecordKey);
                break;
            default:
                throw new InvalidOperationException(
                    $"Journal entry {entry.Sequence} has an invalid operation payload.");
        }

        nextVersion = entry.NextVersionAfter;
        operationId = entry.OperationId;
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
