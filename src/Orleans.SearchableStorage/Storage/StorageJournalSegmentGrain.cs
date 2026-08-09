using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Owns one reusable physical slot in a partition's bounded journal ring.
/// </summary>
internal sealed class StorageJournalSegmentGrain : Grain, IStorageJournalSegmentGrain
{
    private readonly IPersistentState<StorageJournalSegmentState> _state;
    private readonly Action _requestDeactivation;
    private bool _persistenceOutcomeAmbiguous;

    public StorageJournalSegmentGrain(
        [PersistentState("journal", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StorageJournalSegmentState> state)
        : this(state, requestDeactivation: null)
    {
    }

    internal StorageJournalSegmentGrain(
        IPersistentState<StorageJournalSegmentState> state,
        Action? requestDeactivation)
    {
        _state = state;
        _requestDeactivation = requestDeactivation ?? DeactivateOnIdle;
    }

    public async Task StoreAsync(
        StorageJournalEntry entry,
        long committedSequence,
        Guid committedOperationId,
        long absoluteSegmentIndex,
        int segmentCapacity)
    {
        EnsureUsable();
        ValidateStoreRequest(
            entry,
            committedSequence,
            committedOperationId,
            absoluteSegmentIndex,
            segmentCapacity);

        var expectedSequence = checked(committedSequence + 1);
        if (entry.Sequence != expectedSequence
            || entry.PreviousOperationId != committedOperationId)
        {
            throw new InvalidOperationException(
                "A journal append must extend the durable manifest commit point by exactly one operation.");
        }

        var candidate = PrepareSlot(absoluteSegmentIndex, segmentCapacity, entry.WriterEpoch);
        var existingIndex = candidate.Entries.FindIndex(stored => stored.Sequence == entry.Sequence);
        if (existingIndex >= 0
            && StoragePersistenceStateEquality.JournalEntryEquals(candidate.Entries[existingIndex], entry))
        {
            return;
        }

        if (entry.WriterEpoch < candidate.HighestWriterEpoch)
        {
            throw new InvalidOperationException(
                $"Writer epoch {entry.WriterEpoch} is stale; journal slot {absoluteSegmentIndex} has observed "
                + $"writer epoch {candidate.HighestWriterEpoch}.");
        }

        if (existingIndex >= 0)
        {
            var existing = candidate.Entries[existingIndex];
            if (entry.OperationId == existing.OperationId)
            {
                throw new InvalidOperationException(
                    "A repeated journal operation id must have exactly the same durable representation.");
            }

            if (entry.WriterEpoch <= candidate.HighestWriterEpoch
                || entry.WriterEpoch <= existing.WriterEpoch)
            {
                throw new InvalidOperationException(
                    "Only a higher writer epoch may replace the single uncommitted operation after the manifest commit point.");
            }

            candidate.Entries[existingIndex] = entry.Copy();
        }
        else
        {
            if (candidate.Entries.Any(stored => stored.OperationId == entry.OperationId))
            {
                throw new InvalidOperationException(
                    $"Journal operation id '{entry.OperationId}' is already assigned to another sequence.");
            }

            if (candidate.Entries.Any(stored => stored.Sequence > committedSequence))
            {
                throw new InvalidOperationException(
                    "A journal slot cannot contain more than one operation after the durable manifest commit point.");
            }

            if (candidate.Entries.Count >= candidate.Capacity)
            {
                throw new InvalidOperationException(
                    $"Journal slot {absoluteSegmentIndex} already contains its configured {candidate.Capacity} entries.");
            }

            candidate.Entries.Add(entry.Copy());
            candidate.Entries.Sort(static (left, right) => left.Sequence.CompareTo(right.Sequence));
        }

        candidate.HighestWriterEpoch = Math.Max(candidate.HighestWriterEpoch, entry.WriterEpoch);
        await PersistAsync(candidate);
    }

    public Task<StorageJournalSegmentState> ReadAsync()
    {
        EnsureUsable();
        return Task.FromResult(_state.State.Copy());
    }

    public async Task RetireAsync(long absoluteSegmentIndex)
    {
        EnsureUsable();
        ArgumentOutOfRangeException.ThrowIfNegative(absoluteSegmentIndex);

        if (!_state.State.Initialized)
        {
            await PersistAsync(new StorageJournalSegmentState
            {
                Initialized = true,
                AbsoluteSegmentIndex = absoluteSegmentIndex,
                Tombstoned = true,
            });
            return;
        }

        if (_state.State.AbsoluteSegmentIndex > absoluteSegmentIndex)
        {
            // A delayed retirement must not affect a newer absolute segment which reused this slot.
            return;
        }

        if (_state.State.AbsoluteSegmentIndex < absoluteSegmentIndex)
        {
            throw new InvalidOperationException(
                $"Journal slot contains absolute segment {_state.State.AbsoluteSegmentIndex} and cannot skip directly "
                + $"to retirement fence {absoluteSegmentIndex}.");
        }

        if (_state.State.Tombstoned)
        {
            return;
        }

        var candidate = _state.State.Copy();
        candidate.Tombstoned = true;
        candidate.Entries.Clear();
        await PersistAsync(candidate);
    }

    private StorageJournalSegmentState PrepareSlot(
        long absoluteSegmentIndex,
        int segmentCapacity,
        long writerEpoch)
    {
        if (!_state.State.Initialized)
        {
            return new StorageJournalSegmentState
            {
                Initialized = true,
                Capacity = segmentCapacity,
                AbsoluteSegmentIndex = absoluteSegmentIndex,
                HighestWriterEpoch = writerEpoch,
            };
        }

        var candidate = _state.State.Copy();
        if (candidate.Capacity != 0 && candidate.Capacity != segmentCapacity)
        {
            throw new InvalidOperationException(
                $"Journal slot capacity {candidate.Capacity} does not match the immutable manifest capacity "
                + $"{segmentCapacity}.");
        }

        if (absoluteSegmentIndex < candidate.AbsoluteSegmentIndex)
        {
            throw new InvalidOperationException(
                $"Absolute segment {absoluteSegmentIndex} is stale; this journal slot is fenced at "
                + $"absolute segment {candidate.AbsoluteSegmentIndex}.");
        }

        if (absoluteSegmentIndex == candidate.AbsoluteSegmentIndex)
        {
            if (candidate.Tombstoned)
            {
                throw new InvalidOperationException(
                    $"Retired absolute segment {absoluteSegmentIndex} cannot be resurrected.");
            }

            return candidate;
        }

        if (!candidate.Tombstoned)
        {
            throw new InvalidOperationException(
                $"Journal slot must be retired before absolute segment {absoluteSegmentIndex} can reuse it.");
        }

        if (writerEpoch < candidate.HighestWriterEpoch)
        {
            throw new InvalidOperationException(
                $"Writer epoch {writerEpoch} is stale; the reusable journal slot has observed writer epoch "
                + $"{candidate.HighestWriterEpoch}.");
        }

        candidate.Capacity = segmentCapacity;
        candidate.AbsoluteSegmentIndex = absoluteSegmentIndex;
        candidate.Tombstoned = false;
        candidate.Entries.Clear();
        return candidate;
    }

    private static void ValidateStoreRequest(
        StorageJournalEntry entry,
        long committedSequence,
        Guid committedOperationId,
        long absoluteSegmentIndex,
        int segmentCapacity)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentOutOfRangeException.ThrowIfNegative(committedSequence);
        ArgumentOutOfRangeException.ThrowIfNegative(absoluteSegmentIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(segmentCapacity);

        if ((committedSequence == 0) != (committedOperationId == Guid.Empty))
        {
            throw new ArgumentException(
                "The initial commit point must have an empty operation id, and every positive commit point must have one.",
                nameof(committedOperationId));
        }

        StoragePersistenceStateValidation.ValidateJournalEntry(entry, nameof(entry));

        var derivedSegmentIndex = StoragePersistence.GetAbsoluteSegmentIndex(entry.Sequence, segmentCapacity);
        if (derivedSegmentIndex != absoluteSegmentIndex)
        {
            throw new ArgumentException(
                $"Journal sequence {entry.Sequence} belongs to absolute segment {derivedSegmentIndex}, not "
                + $"{absoluteSegmentIndex}.",
                nameof(absoluteSegmentIndex));
        }
    }

    private async Task PersistAsync(StorageJournalSegmentState candidate)
    {
        var previous = _state.State;
        _state.State = candidate;
        try
        {
            // IPersistentState retains the provider ETag, so this write is a compare-and-swap.
            await _state.WriteStateAsync();
        }
        catch
        {
            _persistenceOutcomeAmbiguous = true;
            _state.State = previous;
            _requestDeactivation();
            throw;
        }
    }

    private void EnsureUsable()
    {
        if (_persistenceOutcomeAmbiguous)
        {
            throw new InvalidOperationException(
                "The journal slot activation cannot be reused after an ambiguous persistence write.");
        }
    }
}
