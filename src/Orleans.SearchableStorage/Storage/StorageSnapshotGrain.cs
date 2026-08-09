using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Owns one of the two reusable immutable snapshot slots for a storage partition.
/// </summary>
internal sealed class StorageSnapshotGrain : Grain, IStorageSnapshotGrain
{
    private readonly IPersistentState<StorageSnapshotState> _state;
    private readonly Action _requestDeactivation;
    private bool _persistenceOutcomeAmbiguous;

    public StorageSnapshotGrain(
        [PersistentState("snapshot", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StorageSnapshotState> state)
        : this(state, requestDeactivation: null)
    {
    }

    internal StorageSnapshotGrain(
        IPersistentState<StorageSnapshotState> state,
        Action? requestDeactivation)
    {
        _state = state;
        _requestDeactivation = requestDeactivation ?? DeactivateOnIdle;
    }

    public async Task StoreAsync(StorageSnapshotState snapshot)
    {
        EnsureUsable();
        ValidateSnapshot(snapshot);

        if (!_state.State.Initialized)
        {
            await PersistAsync(snapshot.Copy());
            return;
        }

        EnsureSameSlot(snapshot.Slot);
        if (snapshot.Generation < _state.State.Generation)
        {
            throw new InvalidOperationException(
                $"Snapshot generation {snapshot.Generation} is stale; slot {snapshot.Slot} is fenced at "
                + $"generation {_state.State.Generation}.");
        }

        if (snapshot.Generation == _state.State.Generation)
        {
            if (StoragePersistenceStateEquality.SnapshotEquals(_state.State, snapshot))
            {
                return;
            }

            throw new InvalidOperationException(
                $"Snapshot generation {snapshot.Generation} already exists with different metadata or payload.");
        }

        if (!_state.State.Tombstoned)
        {
            throw new InvalidOperationException(
                $"Live snapshot generation {_state.State.Generation} must be retired before slot {snapshot.Slot} "
                + $"accepts generation {snapshot.Generation}.");
        }

        if (snapshot.Sequence <= _state.State.Sequence
            || snapshot.NextVersion < _state.State.NextVersion
            || snapshot.SnapshotId == _state.State.SnapshotId
            || snapshot.OperationId == _state.State.OperationId)
        {
            throw new InvalidOperationException(
                $"Snapshot generation {snapshot.Generation} does not advance the retired slot identity and boundary.");
        }

        await PersistAsync(snapshot.Copy());
    }

    public Task<StorageSnapshotState> ReadAsync()
    {
        EnsureUsable();
        return Task.FromResult(_state.State.Copy());
    }

    public async Task RetireAsync(StorageSnapshotDescriptor descriptor)
    {
        EnsureUsable();
        ValidateDescriptor(descriptor);

        if (!_state.State.Initialized)
        {
            await PersistAsync(CreateTombstone(descriptor));
            return;
        }

        EnsureSameSlot(descriptor.Slot);
        if (descriptor.Generation < _state.State.Generation)
        {
            // A delayed retirement must not erase a newer generation which reused this slot.
            return;
        }

        if (descriptor.Generation > _state.State.Generation)
        {
            throw new InvalidOperationException(
                $"Snapshot slot {descriptor.Slot} contains generation {_state.State.Generation} and cannot skip "
                + $"directly to retirement fence {descriptor.Generation}.");
        }

        if (!StoragePersistenceStateEquality.DescriptorEquals(descriptor, _state.State))
        {
            throw new InvalidOperationException(
                $"Snapshot generation {descriptor.Generation} cannot be retired using mismatched identity metadata.");
        }

        if (_state.State.Tombstoned)
        {
            return;
        }

        var candidate = _state.State.Copy();
        candidate.Tombstoned = true;
        candidate.Records.Clear();
        await PersistAsync(candidate);
    }

    private void EnsureSameSlot(int slot)
    {
        if (_state.State.Slot != slot)
        {
            throw new InvalidOperationException(
                $"Snapshot grain contains slot {_state.State.Slot} and cannot store or retire slot {slot}.");
        }
    }

    private static StorageSnapshotState CreateTombstone(StorageSnapshotDescriptor descriptor)
    {
        return new StorageSnapshotState
        {
            Initialized = true,
            Tombstoned = true,
            Slot = descriptor.Slot,
            Generation = descriptor.Generation,
            SnapshotId = descriptor.SnapshotId,
            Sequence = descriptor.Sequence,
            OperationId = descriptor.OperationId,
            NextVersion = descriptor.NextVersion,
        };
    }

    private static void ValidateSnapshot(StorageSnapshotState snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (!snapshot.Initialized || snapshot.Tombstoned)
        {
            throw new ArgumentException(
                "A stored snapshot must be initialized and contain a live payload.",
                nameof(snapshot));
        }

        StoragePersistence.ValidateSnapshotSlot(snapshot.Slot, nameof(snapshot));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(snapshot.Generation, nameof(snapshot));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(snapshot.Sequence, nameof(snapshot));
        if (snapshot.NextVersion < 2 || snapshot.NextVersion - 1 > snapshot.Sequence)
        {
            throw new ArgumentOutOfRangeException(
                nameof(snapshot),
                snapshot.NextVersion,
                "A snapshot next version must be representable by its positive committed sequence.");
        }

        if (snapshot.SnapshotId == Guid.Empty)
        {
            throw new ArgumentException("A snapshot id must not be empty.", nameof(snapshot));
        }

        if (snapshot.OperationId == Guid.Empty)
        {
            throw new ArgumentException(
                "A durable snapshot operation id must not be empty.",
                nameof(snapshot));
        }

        ArgumentNullException.ThrowIfNull(snapshot.Records, nameof(snapshot));
        foreach (var (recordKey, record) in snapshot.Records)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(recordKey, nameof(snapshot));
            if (record is null)
            {
                throw new ArgumentException("A snapshot cannot contain a null record.", nameof(snapshot));
            }

            StoragePersistenceStateValidation.ValidateRecord(record, nameof(snapshot));
        }
    }

    private static void ValidateDescriptor(StorageSnapshotDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        if (!descriptor.IsPresent)
        {
            throw new ArgumentException("Only a present snapshot descriptor can be retired.", nameof(descriptor));
        }

        StoragePersistence.ValidateSnapshotSlot(descriptor.Slot, nameof(descriptor));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(descriptor.Generation, nameof(descriptor));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(descriptor.Sequence, nameof(descriptor));
        if (descriptor.NextVersion < 2 || descriptor.NextVersion - 1 > descriptor.Sequence)
        {
            throw new ArgumentOutOfRangeException(
                nameof(descriptor),
                descriptor.NextVersion,
                "A snapshot next version must be representable by its positive committed sequence.");
        }

        if (descriptor.SnapshotId == Guid.Empty)
        {
            throw new ArgumentException("A snapshot id must not be empty.", nameof(descriptor));
        }

        if (descriptor.OperationId == Guid.Empty)
        {
            throw new ArgumentException(
                "A durable snapshot operation id must not be empty.",
                nameof(descriptor));
        }
    }

    private async Task PersistAsync(StorageSnapshotState candidate)
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
                "The snapshot slot activation cannot be reused after an ambiguous persistence write.");
        }
    }
}
