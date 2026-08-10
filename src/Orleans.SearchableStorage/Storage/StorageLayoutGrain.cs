using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StorageLayoutGrain : Grain, IStorageLayoutGrain
{
    private const long InitialRoutingEpoch = 1;

    private readonly string? _providerName;
    private readonly Action _requestDeactivation;
    private readonly IPersistentState<StorageLayoutState> _state;
    private StorageLayoutSnapshot? _routingSnapshot;
    private bool _routingStateValidated;
    private bool _usable = true;

    public StorageLayoutGrain(
        [PersistentState("layout", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StorageLayoutState> state)
        : this(state, providerName: null, requestDeactivation: null)
    {
    }

    internal StorageLayoutGrain(
        IPersistentState<StorageLayoutState> state,
        string? providerName,
        Action? requestDeactivation)
    {
        ArgumentNullException.ThrowIfNull(state);
        if (providerName is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        }

        _state = state;
        _providerName = providerName;
        _requestDeactivation = requestDeactivation ?? DeactivateOnIdle;
    }

    private string ProviderName => _providerName ?? this.GetPrimaryKeyString();

    public async Task InitializeAsync(StorageLayoutDescriptor descriptor)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(descriptor);

        if (descriptor.FormatVersion == StorageLayout.CurrentFormatVersion)
        {
            _ = await InitializeRoutingAsync(descriptor);
            return;
        }

        ValidateLegacyDescriptor(descriptor);
        if (_state.State.Initialized)
        {
            if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
            {
                EnsureLegacyMatches(descriptor);
                return;
            }

            if (_state.State.FormatVersion == StorageLayout.CurrentFormatVersion)
            {
                EnsureLegacyCompatibleWithRouting(descriptor);
                return;
            }

            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        await PersistAsync(CreateLegacyState(descriptor));
    }

    public async Task<StorageLayoutSnapshot> InitializeRoutingAsync(StorageLayoutDescriptor descriptor)
    {
        EnsureUsable();
        ValidateRoutingDescriptorBase(descriptor);

        if (_state.State.Initialized)
        {
            if (_state.State.FormatVersion == StorageLayout.CurrentFormatVersion)
            {
                EnsureRoutingMatches(descriptor);
                return CreateSnapshot();
            }

            if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
            {
                EnsureLegacyCanMigrate(descriptor);
                await PersistAsync(CreateRoutingState(descriptor));
                return CreateSnapshot();
            }

            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        await PersistAsync(CreateRoutingState(descriptor));
        return CreateSnapshot();
    }

    public Task<bool> ValidateAsync(StorageLayoutDescriptor descriptor)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(descriptor);
        ValidateSupportedDescriptor(descriptor);

        if (!_state.State.Initialized)
        {
            if (descriptor.FormatVersion == StorageLayout.CurrentFormatVersion)
            {
                ValidateRoutingSeed(descriptor);
            }

            return Task.FromResult(false);
        }

        if (descriptor.FormatVersion == StorageLayout.PreviousFormatVersion)
        {
            if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
            {
                EnsureLegacyMatches(descriptor);
            }
            else if (_state.State.FormatVersion == StorageLayout.CurrentFormatVersion)
            {
                EnsureLegacyCompatibleWithRouting(descriptor);
            }
            else
            {
                ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
            }
        }
        else if (_state.State.FormatVersion == StorageLayout.CurrentFormatVersion)
        {
            EnsureRoutingMatches(descriptor);
        }
        else if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
        {
            throw CreateRoutingInitializationRequiredException();
        }
        else
        {
            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        return Task.FromResult(true);
    }

    public Task<bool> ValidateIdentityAsync(StorageLayoutIdentity identity)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(identity);
        ValidateSupportedIdentity(identity);

        if (!_state.State.Initialized)
        {
            return Task.FromResult(false);
        }

        if (identity.FormatVersion == StorageLayout.PreviousFormatVersion)
        {
            if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
            {
                EnsureLegacyIdentityMatches(identity);
            }
            else if (_state.State.FormatVersion == StorageLayout.CurrentFormatVersion)
            {
                EnsureLegacyIdentityCompatibleWithRouting(identity);
            }
            else
            {
                ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
            }
        }
        else if (_state.State.FormatVersion == StorageLayout.CurrentFormatVersion)
        {
            EnsureRoutingIdentityMatches(identity);
        }
        else if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
        {
            throw CreateRoutingInitializationRequiredException();
        }
        else
        {
            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        return Task.FromResult(true);
    }

    public Task<StorageLayoutSnapshot?> GetLayoutAsync(StorageLayoutIdentity identity)
    {
        EnsureUsable();
        ValidateRoutingIdentity(identity);

        if (!_state.State.Initialized)
        {
            return Task.FromResult<StorageLayoutSnapshot?>(null);
        }

        if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
        {
            throw CreateRoutingInitializationRequiredException();
        }

        if (_state.State.FormatVersion != StorageLayout.CurrentFormatVersion)
        {
            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        EnsureRoutingIdentityMatches(identity);
        return Task.FromResult<StorageLayoutSnapshot?>(CreateSnapshot());
    }

    public Task<StorageLayoutSnapshot?> GetCurrentLayoutAsync()
    {
        EnsureUsable();
        if (!_state.State.Initialized)
        {
            return Task.FromResult<StorageLayoutSnapshot?>(null);
        }

        if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion)
        {
            throw CreateRoutingInitializationRequiredException();
        }

        if (_state.State.FormatVersion != StorageLayout.CurrentFormatVersion)
        {
            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        ValidateRoutingState();
        return Task.FromResult<StorageLayoutSnapshot?>(CreateSnapshot());
    }

    private static StorageLayoutState CreateLegacyState(StorageLayoutDescriptor descriptor)
    {
        return new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.PreviousFormatVersion,
            ProviderName = descriptor.ProviderName,
            PartitionCount = descriptor.PartitionCount,
            JournalSegmentCapacity = descriptor.JournalSegmentCapacity,
            MaximumJournalReplayEntries = descriptor.MaximumJournalReplayEntries,
        };
    }

    private static StorageLayoutState CreateRoutingState(StorageLayoutDescriptor descriptor)
    {
        var virtualSlotCount = StorageLayout.DeriveVirtualSlotCount(
            descriptor.PartitionCount,
            descriptor.VirtualSlotTargetCount);
        return new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = descriptor.ProviderName,
            PartitionCount = descriptor.PartitionCount,
            JournalSegmentCapacity = descriptor.JournalSegmentCapacity,
            MaximumJournalReplayEntries = descriptor.MaximumJournalReplayEntries,
            VirtualSlotCount = virtualSlotCount,
            SlotAssignments = StorageLayout.CreateIdentityAssignments(
                descriptor.PartitionCount,
                virtualSlotCount),
            Epoch = InitialRoutingEpoch,
        };
    }

    private StorageLayoutSnapshot CreateSnapshot()
    {
        ValidateRoutingState();
        return _routingSnapshot ??= StorageLayoutSnapshot.FromState(_state.State);
    }

    private void ValidateSupportedDescriptor(StorageLayoutDescriptor descriptor)
    {
        if (descriptor.FormatVersion == StorageLayout.PreviousFormatVersion)
        {
            ValidateLegacyDescriptor(descriptor);
            return;
        }

        ValidateRoutingDescriptorBase(descriptor);
    }

    private void ValidateLegacyDescriptor(StorageLayoutDescriptor descriptor)
    {
        ValidateIdentityValues(
            descriptor.FormatVersion,
            descriptor.ProviderName,
            descriptor.PartitionCount,
            nameof(descriptor),
            "layout descriptor",
            StorageLayout.PreviousFormatVersion);
        StoragePersistence.ValidateOptions(
            descriptor.JournalSegmentCapacity,
            descriptor.MaximumJournalReplayEntries);
        if (descriptor.VirtualSlotTargetCount != 0)
        {
            throw new ArgumentException(
                "A version-3 layout descriptor cannot contain virtual-slot settings.",
                nameof(descriptor));
        }
    }

    private void ValidateRoutingDescriptorBase(StorageLayoutDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ValidateIdentityValues(
            descriptor.FormatVersion,
            descriptor.ProviderName,
            descriptor.PartitionCount,
            nameof(descriptor),
            "layout descriptor",
            StorageLayout.CurrentFormatVersion);
        StoragePersistence.ValidateOptions(
            descriptor.JournalSegmentCapacity,
            descriptor.MaximumJournalReplayEntries);
    }

    private static void ValidateRoutingSeed(StorageLayoutDescriptor descriptor)
    {
        _ = StorageLayout.DeriveVirtualSlotCount(
            descriptor.PartitionCount,
            descriptor.VirtualSlotTargetCount);
    }

    private void ValidateSupportedIdentity(StorageLayoutIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        if (identity.FormatVersion is not StorageLayout.PreviousFormatVersion
            and not StorageLayout.CurrentFormatVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(identity),
                identity.FormatVersion,
                $"Layout format version {StorageLayout.CurrentFormatVersion} or placement-compatible version "
                + $"{StorageLayout.PreviousFormatVersion} is required.");
        }

        ValidateIdentityValues(
            identity.FormatVersion,
            identity.ProviderName,
            identity.PartitionCount,
            nameof(identity),
            "layout identity",
            identity.FormatVersion);
    }

    private void ValidateRoutingIdentity(StorageLayoutIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        ValidateIdentityValues(
            identity.FormatVersion,
            identity.ProviderName,
            identity.PartitionCount,
            nameof(identity),
            "layout identity",
            StorageLayout.CurrentFormatVersion);
    }

    private void ValidateIdentityValues(
        int formatVersion,
        string providerName,
        int partitionCount,
        string parameterName,
        string description,
        int requiredFormatVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);

        if (!string.Equals(ProviderName, providerName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"The {description} provider name must match the layout grain key.",
                parameterName);
        }

        if (formatVersion != requiredFormatVersion)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                formatVersion,
                $"Layout format version {requiredFormatVersion} is required.");
        }
    }

    private void EnsureLegacyCanMigrate(StorageLayoutDescriptor descriptor)
    {
        EnsureLegacyBaseMatches(descriptor);
        if (_state.State.VirtualSlotCount != 0
            || (_state.State.SlotAssignments is not null && _state.State.SlotAssignments.Length != 0)
            || _state.State.Epoch != 0)
        {
            throw new InvalidOperationException(
                "The persisted version-3 layout contains unexpected virtual-routing state and cannot be migrated.");
        }
    }

    private void EnsureLegacyMatches(StorageLayoutDescriptor descriptor)
    {
        if (_state.State.FormatVersion != StorageLayout.PreviousFormatVersion)
        {
            ThrowLayoutMismatch(descriptor);
        }

        EnsureLegacyBaseMatches(descriptor);
        if (_state.State.VirtualSlotCount != 0
            || (_state.State.SlotAssignments is not null && _state.State.SlotAssignments.Length != 0)
            || _state.State.Epoch != 0)
        {
            throw new InvalidOperationException("The persisted version-3 layout contains invalid routing fields.");
        }
    }

    private void EnsureLegacyBaseMatches(StorageLayoutDescriptor descriptor)
    {
        if (string.Equals(_state.State.ProviderName, descriptor.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == descriptor.PartitionCount
            && _state.State.JournalSegmentCapacity == descriptor.JournalSegmentCapacity
            && _state.State.MaximumJournalReplayEntries == descriptor.MaximumJournalReplayEntries)
        {
            return;
        }

        ThrowLayoutMismatch(descriptor);
    }

    private void EnsureLegacyCompatibleWithRouting(StorageLayoutDescriptor descriptor)
    {
        ValidateRoutingState();
        if (string.Equals(_state.State.ProviderName, descriptor.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == descriptor.PartitionCount
            && _state.State.JournalSegmentCapacity == descriptor.JournalSegmentCapacity
            && _state.State.MaximumJournalReplayEntries == descriptor.MaximumJournalReplayEntries)
        {
            return;
        }

        ThrowLayoutMismatch(descriptor);
    }

    private void EnsureRoutingMatches(StorageLayoutDescriptor descriptor)
    {
        ValidateRoutingState();
        if (string.Equals(_state.State.ProviderName, descriptor.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == descriptor.PartitionCount
            && _state.State.JournalSegmentCapacity == descriptor.JournalSegmentCapacity
            && _state.State.MaximumJournalReplayEntries == descriptor.MaximumJournalReplayEntries)
        {
            // VirtualSlotTargetCount is a seed for new layouts, not an immutable runtime setting.
            return;
        }

        ThrowLayoutMismatch(descriptor);
    }

    private void EnsureLegacyIdentityMatches(StorageLayoutIdentity identity)
    {
        if (_state.State.FormatVersion == StorageLayout.PreviousFormatVersion
            && string.Equals(_state.State.ProviderName, identity.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == identity.PartitionCount)
        {
            return;
        }

        ThrowIdentityMismatch(identity);
    }

    private void EnsureLegacyIdentityCompatibleWithRouting(StorageLayoutIdentity identity)
    {
        ValidateRoutingState();
        if (string.Equals(_state.State.ProviderName, identity.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == identity.PartitionCount)
        {
            return;
        }

        ThrowIdentityMismatch(identity);
    }

    private void EnsureRoutingIdentityMatches(StorageLayoutIdentity identity)
    {
        ValidateRoutingState();
        if (string.Equals(_state.State.ProviderName, identity.ProviderName, StringComparison.Ordinal)
            && _state.State.PartitionCount == identity.PartitionCount)
        {
            return;
        }

        ThrowIdentityMismatch(identity);
    }

    private void ValidateRoutingState()
    {
        if (_routingStateValidated)
        {
            return;
        }

        var state = _state.State;
        if (!state.Initialized
            || state.FormatVersion != StorageLayout.CurrentFormatVersion
            || string.IsNullOrWhiteSpace(state.ProviderName)
            || !string.Equals(state.ProviderName, ProviderName, StringComparison.Ordinal)
            || state.PartitionCount <= 0
            || state.VirtualSlotCount < state.PartitionCount
            || state.VirtualSlotCount > StorageLayout.MaximumVirtualSlotCount
            || state.VirtualSlotCount % state.PartitionCount != 0
            || state.SlotAssignments is null
            || state.SlotAssignments.Length != state.VirtualSlotCount
            || state.Epoch != InitialRoutingEpoch)
        {
            throw new InvalidOperationException("The persisted version-4 layout contains invalid routing boundaries.");
        }

        StoragePersistence.ValidateOptions(
            state.JournalSegmentCapacity,
            state.MaximumJournalReplayEntries);
        for (var slot = 0; slot < state.SlotAssignments.Length; slot++)
        {
            if (state.SlotAssignments[slot] != slot % state.PartitionCount)
            {
                throw new InvalidOperationException(
                    "The phase-1 version-4 layout must contain the zero-movement identity assignment.");
            }
        }

        _routingStateValidated = true;
    }

    private async Task PersistAsync(StorageLayoutState candidate)
    {
        var previous = _state.State;
        _state.State = candidate;
        ResetRoutingSnapshot();
        try
        {
            // The physical provider ETag makes the single layout document the routing commit point.
            await _state.WriteStateAsync();
        }
        catch
        {
            // A provider may commit before losing the acknowledgement. Restored in-memory state
            // must never participate in another compare-and-swap on this activation.
            _state.State = previous;
            ResetRoutingSnapshot();
            PoisonActivation();
            throw;
        }
    }

    private void ResetRoutingSnapshot()
    {
        _routingSnapshot = null;
        _routingStateValidated = false;
    }

    private static InvalidOperationException CreateRoutingInitializationRequiredException()
    {
        return new InvalidOperationException(
            "The persisted version-3 layout must be initialized through InitializeRoutingAsync before routing can be served.");
    }

    private static void ThrowUnsupportedPersistedVersion(int formatVersion)
    {
        throw new InvalidOperationException(
            $"Persisted layout format version {formatVersion} is not supported; migrate it before accessing this namespace.");
    }

    private void ThrowLayoutMismatch(StorageLayoutDescriptor descriptor)
    {
        throw new InvalidOperationException(
            $"Searchable storage provider '{descriptor.ProviderName}' is configured for layout "
            + $"version {descriptor.FormatVersion} with {descriptor.PartitionCount} initial partitions, journal capacity "
            + $"{descriptor.JournalSegmentCapacity}, and replay limit {descriptor.MaximumJournalReplayEntries}, but its persisted "
            + $"layout is version {_state.State.FormatVersion} for provider '{_state.State.ProviderName}' "
            + $"with {_state.State.PartitionCount} initial partitions, journal capacity {_state.State.JournalSegmentCapacity}, "
            + $"and replay limit {_state.State.MaximumJournalReplayEntries}. Restore the persisted configuration or migrate the data.");
    }

    private void ThrowIdentityMismatch(StorageLayoutIdentity identity)
    {
        throw new InvalidOperationException(
            $"Searchable storage provider '{identity.ProviderName}' is configured for layout "
            + $"version {identity.FormatVersion} with {identity.PartitionCount} initial partitions, but its persisted "
            + $"layout is version {_state.State.FormatVersion} for provider '{_state.State.ProviderName}' "
            + $"with {_state.State.PartitionCount} initial partitions. Restore the persisted configuration or migrate the data.");
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
        _requestDeactivation();
    }
}
