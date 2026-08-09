using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StorageLayoutGrain : Grain, IStorageLayoutGrain
{
    private readonly IPersistentState<StorageLayoutState> _state;

    public StorageLayoutGrain(
        [PersistentState("layout", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StorageLayoutState> state)
    {
        _state = state;
    }

    public async Task InitializeAsync(StorageLayoutDescriptor descriptor)
    {
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
        };

        try
        {
            await _state.WriteStateAsync();
        }
        catch
        {
            _state.State = previous;
            DeactivateOnIdle();
            throw;
        }
    }

    public Task<bool> ValidateAsync(StorageLayoutDescriptor descriptor)
    {
        ValidateDescriptor(descriptor);

        if (!_state.State.Initialized)
        {
            return Task.FromResult(false);
        }

        EnsureMatches(descriptor);
        return Task.FromResult(true);
    }

    private void ValidateDescriptor(StorageLayoutDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentException.ThrowIfNullOrWhiteSpace(descriptor.ProviderName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(descriptor.PartitionCount);

        if (!string.Equals(this.GetPrimaryKeyString(), descriptor.ProviderName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The layout descriptor provider name must match the layout grain key.",
                nameof(descriptor));
        }

        if (descriptor.FormatVersion != StorageLayout.CurrentFormatVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(descriptor),
                descriptor.FormatVersion,
                $"Storage format version {StorageLayout.CurrentFormatVersion} is required.");
        }
    }

    private void EnsureMatches(StorageLayoutDescriptor descriptor)
    {
        if (_state.State.FormatVersion == descriptor.FormatVersion
            && string.Equals(_state.State.ProviderName, descriptor.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == descriptor.PartitionCount)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Searchable storage provider '{descriptor.ProviderName}' is configured for layout "
            + $"version {descriptor.FormatVersion} with {descriptor.PartitionCount} partitions, but its persisted "
            + $"layout is version {_state.State.FormatVersion} for provider '{_state.State.ProviderName}' "
            + $"with {_state.State.PartitionCount} partitions. Restore the persisted configuration or migrate the data.");
    }
}
