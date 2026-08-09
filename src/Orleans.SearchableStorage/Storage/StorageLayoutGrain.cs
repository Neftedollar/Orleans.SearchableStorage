using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StorageLayoutGrain : Grain, IStorageLayoutGrain
{
    private readonly IPersistentState<StorageLayoutState> _state;
    private bool _usable = true;

    public StorageLayoutGrain(
        [PersistentState("layout", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StorageLayoutState> state)
    {
        _state = state;
    }

    public async Task InitializeAsync(StorageLayoutDescriptor descriptor)
    {
        EnsureUsable();
        ValidateDescriptor(descriptor);

        if (_state.State.Initialized)
        {
            EnsureMatches(descriptor);
            return;
        }

        var previous = _state.State;
        _state.State = new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = descriptor.FormatVersion,
            ProviderName = descriptor.ProviderName,
            PartitionCount = descriptor.PartitionCount,
            JournalSegmentCapacity = descriptor.JournalSegmentCapacity,
            MaximumJournalReplayEntries = descriptor.MaximumJournalReplayEntries,
        };

        try
        {
            await _state.WriteStateAsync();
        }
        catch
        {
            _state.State = previous;
            PoisonActivation();
            throw;
        }
    }

    public Task<bool> ValidateAsync(StorageLayoutDescriptor descriptor)
    {
        EnsureUsable();
        ValidateDescriptor(descriptor);

        if (!_state.State.Initialized)
        {
            return Task.FromResult(false);
        }

        EnsureMatches(descriptor);
        return Task.FromResult(true);
    }

    public Task<bool> ValidateIdentityAsync(StorageLayoutIdentity identity)
    {
        EnsureUsable();
        ValidateIdentity(identity);

        if (!_state.State.Initialized)
        {
            return Task.FromResult(false);
        }

        EnsureIdentityMatches(identity);
        return Task.FromResult(true);
    }

    private void ValidateDescriptor(StorageLayoutDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ValidateIdentityValues(
            descriptor.FormatVersion,
            descriptor.ProviderName,
            descriptor.PartitionCount,
            nameof(descriptor),
            "layout descriptor");
        StoragePersistence.ValidateOptions(
            descriptor.JournalSegmentCapacity,
            descriptor.MaximumJournalReplayEntries);
    }

    private void ValidateIdentity(StorageLayoutIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        ValidateIdentityValues(
            identity.FormatVersion,
            identity.ProviderName,
            identity.PartitionCount,
            nameof(identity),
            "layout identity");
    }

    private void ValidateIdentityValues(
        int formatVersion,
        string providerName,
        int partitionCount,
        string parameterName,
        string description)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);

        if (!string.Equals(this.GetPrimaryKeyString(), providerName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"The {description} provider name must match the layout grain key.",
                parameterName);
        }

        if (formatVersion != StorageLayout.CurrentFormatVersion)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                formatVersion,
                $"Storage format version {StorageLayout.CurrentFormatVersion} is required.");
        }
    }

    private void EnsureMatches(StorageLayoutDescriptor descriptor)
    {
        if (_state.State.FormatVersion == descriptor.FormatVersion
            && string.Equals(_state.State.ProviderName, descriptor.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == descriptor.PartitionCount
            && _state.State.JournalSegmentCapacity == descriptor.JournalSegmentCapacity
            && _state.State.MaximumJournalReplayEntries == descriptor.MaximumJournalReplayEntries)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Searchable storage provider '{descriptor.ProviderName}' is configured for layout "
            + $"version {descriptor.FormatVersion} with {descriptor.PartitionCount} partitions, journal capacity "
            + $"{descriptor.JournalSegmentCapacity}, and replay limit {descriptor.MaximumJournalReplayEntries}, but its persisted "
            + $"layout is version {_state.State.FormatVersion} for provider '{_state.State.ProviderName}' "
            + $"with {_state.State.PartitionCount} partitions, journal capacity {_state.State.JournalSegmentCapacity}, "
            + $"and replay limit {_state.State.MaximumJournalReplayEntries}. Restore the persisted configuration or migrate the data.");
    }

    private void EnsureIdentityMatches(StorageLayoutIdentity identity)
    {
        if (_state.State.FormatVersion == identity.FormatVersion
            && string.Equals(_state.State.ProviderName, identity.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == identity.PartitionCount)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Searchable storage provider '{identity.ProviderName}' is configured for layout "
            + $"version {identity.FormatVersion} with {identity.PartitionCount} partitions, but its persisted "
            + $"layout is version {_state.State.FormatVersion} for provider '{_state.State.ProviderName}' "
            + $"with {_state.State.PartitionCount} partitions. Restore the persisted configuration or migrate the data.");
    }

    private void EnsureUsable()
    {
        if (!_usable)
        {
            throw new InvalidOperationException(
                "The storage layout activation is retiring after an ambiguous persistence outcome.");
        }
    }

    private void PoisonActivation()
    {
        _usable = false;
        DeactivateOnIdle();
    }
}
