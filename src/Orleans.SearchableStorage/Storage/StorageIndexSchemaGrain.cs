using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StorageIndexSchemaGrain : Grain, IStorageIndexSchemaGrain
{
    private readonly IPersistentState<StorageIndexSchemaState> _state;
    private readonly SearchableStateRegistry _registrations;
    private readonly IOptionsMonitor<SearchableStorageOptions> _options;
    private readonly Action _requestDeactivation;
    private bool _usable = true;

    public StorageIndexSchemaGrain(
        [PersistentState("index-schema", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StorageIndexSchemaState> state,
        SearchableStateRegistry registrations,
        IOptionsMonitor<SearchableStorageOptions> options)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(registrations);
        ArgumentNullException.ThrowIfNull(options);
        _state = state;
        _registrations = registrations;
        _options = options;
        _requestDeactivation = DeactivateOnIdle;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        ValidateState(_state.State);
        return base.OnActivateAsync(cancellationToken);
    }

    public Task<StorageIndexSchemaSnapshot> GetAsync(StorageIndexSchemaRequest request)
    {
        EnsureUsable();
        ValidateRequest(request);
        ValidateRegistration(request);
        ValidatePersistedIdentity(request);
        return Task.FromResult(CreateSnapshot(request));
    }

    public async Task<StorageIndexSchemaSnapshot> BeginRebuildAsync(
        StorageIndexSchemaRequest request)
    {
        EnsureUsable();
        ValidateRequest(request);
        ValidateRegistration(request);
        ValidatePersistedIdentity(request);

        if (_state.State.Rebuild is { } active)
        {
            if (!IndexSchemaIdentity.FixedTimeEquals(active.SchemaKey, request.SchemaKey)
                || !IndexSchemaIdentity.FixedTimeEquals(
                    active.TargetFingerprint,
                    request.Fingerprint))
            {
                throw new InvalidOperationException(
                    "A different index-schema rebuild is already active for this state.");
            }

            return CreateSnapshot(request);
        }

        if (_state.State.ActiveFingerprint is { } fingerprint
            && IndexSchemaIdentity.FixedTimeEquals(fingerprint, request.Fingerprint))
        {
            return CreateSnapshot(request);
        }

        var layout = await GetRequiredStableLayoutAsync(
            request.ProviderName,
            initializeIfMissing: true);
        var owners = layout.GetDistinctOwners();
        var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
        var candidate = _state.State.Copy();
        candidate.Initialized = true;
        candidate.ProtocolVersion = StorageIndexSchema.ProtocolVersion;
        candidate.ProviderName = request.ProviderName;
        candidate.StateName = request.StateName;
        candidate.Rebuild = new StorageIndexSchemaRebuildIntent
        {
            RebuildId = Guid.NewGuid(),
            SchemaKey = [.. request.SchemaKey],
            TargetFingerprint = [.. request.Fingerprint],
            LayoutEpoch = layout.Epoch,
            LayoutFingerprint = layoutFingerprint,
            OwnerCount = owners.Length,
        };
        await PersistAsync(candidate);
        return CreateSnapshot(request);
    }

    public async Task<StorageIndexSchemaSnapshot> AdvanceRebuildAsync(
        StorageIndexSchemaCommand command)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(command);
        ValidateRequest(command.Schema);
        ValidateRegistration(command.Schema);
        ValidatePersistedIdentity(command.Schema);
        var rebuild = _state.State.Rebuild;
        if (rebuild is null)
        {
            if (_state.State.ActiveFingerprint is { } active
                && IndexSchemaIdentity.FixedTimeEquals(active, command.Schema.Fingerprint))
            {
                return CreateSnapshot(command.Schema);
            }

            throw new InvalidOperationException("No index-schema rebuild is active for this state.");
        }

        if (command.RebuildId == Guid.Empty || command.RebuildId != rebuild.RebuildId)
        {
            throw new InvalidOperationException("The index-schema rebuild identifier is stale.");
        }

        var layout = await GetRequiredStableLayoutAsync(
            command.Schema.ProviderName,
            initializeIfMissing: false);
        var owners = layout.GetDistinctOwners();
        var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
        var candidate = _state.State.Copy();
        if (layout.Epoch != rebuild.LayoutEpoch
            || owners.Length != rebuild.OwnerCount
            || !StorageLayoutFingerprint.Equals(
                layoutFingerprint,
                rebuild.LayoutFingerprint))
        {
            // A completed move can leave records under a different owner while preserving the
            // target fingerprint on copied records. Restarting the owner scan is bounded and
            // idempotent: already rebuilt records are skipped, and no generation is activated
            // until the new layout has been covered completely.
            candidate.Rebuild!.LayoutEpoch = layout.Epoch;
            candidate.Rebuild.LayoutFingerprint = layoutFingerprint;
            candidate.Rebuild.OwnerCount = owners.Length;
            candidate.Rebuild.NextProtocolOwnerIndex = 0;
            candidate.Rebuild.LayoutProtocolPublished = false;
            candidate.Rebuild.NextOwnerIndex = 0;
            candidate.Rebuild.HasAfter = false;
            candidate.Rebuild.After = default;
            candidate.Rebuild.ProcessedRecordCount = 0;
            await PersistAsync(candidate);
            return CreateSnapshot(command.Schema);
        }

        var progress = candidate.Rebuild!;
        var layoutGrain = GrainFactory.GetGrain<IStorageLayoutGrain>(
            command.Schema.ProviderName);
        var enablementRequest = new StorageIndexSchemaLayoutProtocolRequest
        {
            ProtocolVersion = StorageIndexSchema.ProtocolVersion,
            LayoutEpoch = progress.LayoutEpoch,
            LayoutFingerprint = [.. progress.LayoutFingerprint],
            EnablementId = progress.RebuildId,
        };
        if (!progress.LayoutProtocolPublished)
        {
            var fencedLayout = await layoutGrain.BeginIndexSchemaProtocolEnablementAsync(
                enablementRequest);
            var activeEnablement = fencedLayout.CopyIndexSchemaEnablement();
            if (fencedLayout.FormatVersion != StorageLayout.IndexSchemaFormatVersion
                || fencedLayout.Epoch != progress.LayoutEpoch
                || !StorageLayoutFingerprint.Equals(
                    StorageLayoutFingerprint.Compute(fencedLayout),
                    progress.LayoutFingerprint)
                || activeEnablement?.EnablementId != progress.RebuildId
                || (fencedLayout.IndexSchemaProtocolVersion
                        != StorageLayout.CurrentIndexSchemaProtocolVersion
                    && fencedLayout.IndexSchemaProtocolVersion != 0))
            {
                throw new InvalidOperationException(
                    "The layout did not durably fence index-schema enablement.");
            }
        }

        var settings = CreatePersistenceSettings(command.Schema.ProviderName);
        if (progress.NextProtocolOwnerIndex < owners.Length)
        {
            var owner = owners[progress.NextProtocolOwnerIndex];
            var partition = GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(command.Schema.ProviderName, owner));
            var protocol = await partition.EnableIndexSchemaProtocolAsync(
                new StorageIndexSchemaPartitionProtocolRequest
                {
                    ProtocolVersion = StorageIndexSchema.ProtocolVersion,
                    ProviderName = command.Schema.ProviderName,
                    LayoutEpoch = progress.LayoutEpoch,
                    LayoutFingerprint = [.. progress.LayoutFingerprint],
                    Persistence = settings,
                });
            if (protocol.PersistenceFormatVersion
                    != StoragePersistence.CurrentPersistenceFormatVersion
                || protocol.IndexSchemaProtocolVersion != StorageIndexSchema.ProtocolVersion
                || protocol.MoveControl.IsPresent)
            {
                throw new InvalidOperationException(
                    $"Storage owner {owner} did not durably enable the index-schema capability.");
            }

            progress.NextProtocolOwnerIndex++;
            await PersistAsync(candidate);
            return CreateSnapshot(command.Schema);
        }

        if (progress.NextOwnerIndex < owners.Length)
        {
            var owner = owners[progress.NextOwnerIndex];
            var partition = GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(command.Schema.ProviderName, owner));
            var result = await partition.RebuildIndexSchemaPageAsync(
                new StorageIndexSchemaRebuildPageRequest
                {
                    ProviderName = command.Schema.ProviderName,
                    StateName = command.Schema.StateName,
                    SchemaKey = [.. command.Schema.SchemaKey],
                    TargetFingerprint = [.. command.Schema.Fingerprint],
                    LayoutEpoch = progress.LayoutEpoch,
                    HasAfter = progress.HasAfter,
                    After = progress.After,
                    PageSize = StorageIndexSchema.RebuildPageSize,
                    Persistence = settings,
                });
            if (result.ProcessedRecordCount < 0
                || result.ProcessedRecordCount > StorageIndexSchema.RebuildPageSize
                || result.HasAfter == result.After.IsDefault
                || (!result.Exhausted && (!result.HasAfter || result.ProcessedRecordCount == 0)))
            {
                throw new InvalidOperationException(
                    $"Storage owner {owner} returned invalid schema-rebuild progress.");
            }

            if (result.HasAfter)
            {
                StorageCapacityGuardrails.ValidateGrainId(result.After);
            }

            progress.ProcessedRecordCount = checked(
                progress.ProcessedRecordCount + result.ProcessedRecordCount);
            if (result.Exhausted)
            {
                progress.NextOwnerIndex++;
                progress.HasAfter = false;
                progress.After = default;
            }
            else
            {
                progress.HasAfter = result.HasAfter;
                progress.After = result.After;
            }
        }

        if (progress.NextOwnerIndex == owners.Length)
        {
            if (!progress.LayoutProtocolPublished)
            {
                var published = await layoutGrain.EnableIndexSchemaProtocolAsync(
                    enablementRequest);
                if (published.IndexSchemaProtocolVersion != StorageIndexSchema.ProtocolVersion
                    || published.CopyIndexSchemaEnablement() is not null
                    || published.Epoch != progress.LayoutEpoch
                    || !StorageLayoutFingerprint.Equals(
                        StorageLayoutFingerprint.Compute(published),
                        progress.LayoutFingerprint))
                {
                    throw new InvalidOperationException(
                        "The layout did not publish the expected index-schema capability.");
                }

                progress.LayoutProtocolPublished = true;
                await PersistAsync(candidate);
                return CreateSnapshot(command.Schema);
            }

            candidate.ActiveFingerprint = [.. progress.TargetFingerprint];
            candidate.LastCompletedRecordCount = progress.ProcessedRecordCount;
            candidate.Rebuild = null;
        }

        await PersistAsync(candidate);
        return CreateSnapshot(command.Schema);
    }

    private async Task<StorageLayoutSnapshot> GetRequiredStableLayoutAsync(
        string providerName,
        bool initializeIfMissing)
    {
        var layoutGrain = GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        StorageLayoutSnapshot? layout;
        if (initializeIfMissing)
        {
            var options = _options.Get(providerName);
            layout = await layoutGrain.InitializeRoutingAsync(StorageLayout.CreateDescriptor(
                providerName,
                options.PartitionCount,
                options.JournalSegmentCapacity,
                options.MaximumJournalReplayEntries,
                options.VirtualSlotTargetCount));
        }
        else
        {
            layout = await layoutGrain.GetCurrentLayoutAsync();
        }

        if (layout is null)
        {
            throw new InvalidOperationException(
                "The storage layout must be initialized before advancing an index-schema rebuild.");
        }
        if (layout.CopyMovementEnablement() is not null || layout.CopyMoveIntent() is not null)
        {
            throw new InvalidOperationException(
                "Index-schema rebuild and virtual-slot movement cannot run at the same time.");
        }

        return layout;
    }

    private StoragePersistenceSettings CreatePersistenceSettings(string providerName)
    {
        var options = _options.Get(providerName);
        return new StoragePersistenceSettings
        {
            JournalSegmentCapacity = options.JournalSegmentCapacity,
            MaximumJournalReplayEntries = options.MaximumJournalReplayEntries,
            CompactionThreshold = options.CompactionThreshold,
        };
    }

    private void ValidateRegistration(StorageIndexSchemaRequest request)
    {
        var registration = _registrations.Find(request.ProviderName, request.StateName)
            ?? throw new InvalidOperationException(
                $"No searchable state registration exists for provider '{request.ProviderName}' "
                + $"and state '{request.StateName}'.");
        if (!IndexSchemaIdentity.FixedTimeEquals(registration.Schema.SchemaKey, request.SchemaKey)
            || !IndexSchemaIdentity.FixedTimeEquals(
                registration.Schema.Fingerprint,
                request.Fingerprint))
        {
            throw new InvalidOperationException(
                "The requested index schema does not match the registered schema declaration "
                + "(state type, index metadata, or application schema version).");
        }
    }

    private void ValidatePersistedIdentity(StorageIndexSchemaRequest request)
    {
        if (!_state.State.Initialized)
        {
            return;
        }

        if (_state.State.ProtocolVersion != StorageIndexSchema.ProtocolVersion
            || !string.Equals(
                _state.State.ProviderName,
                request.ProviderName,
                StringComparison.Ordinal)
            || !string.Equals(_state.State.StateName, request.StateName, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "The persisted index-schema control has an incompatible identity or version.");
        }

        if (_state.State.Rebuild is { } rebuild
            && (!IndexSchemaIdentity.FixedTimeEquals(rebuild.SchemaKey, request.SchemaKey)
                || !IndexSchemaIdentity.FixedTimeEquals(
                    rebuild.TargetFingerprint,
                    request.Fingerprint)))
        {
            throw new InvalidOperationException(
                "The active index-schema rebuild targets a different registered generation.");
        }
    }

    private void ValidateRequest(StorageIndexSchemaRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.ProviderName);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.StateName);
        if (request.ProtocolVersion != StorageIndexSchema.ProtocolVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ProtocolVersion,
                "Unknown index-schema protocol version.");
        }

        IndexSchemaIdentity.ValidateIdentity(request.SchemaKey, nameof(request));
        IndexSchemaIdentity.ValidateIdentity(request.Fingerprint, nameof(request));
        if (!string.Equals(
                this.GetPrimaryKeyString(),
                StorageIndexSchema.CreateGrainKey(request.ProviderName, request.StateName),
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The index-schema request does not match this control grain.",
                nameof(request));
        }
    }

    private StorageIndexSchemaSnapshot CreateSnapshot(StorageIndexSchemaRequest request)
    {
        return new StorageIndexSchemaSnapshot
        {
            ProviderName = request.ProviderName,
            StateName = request.StateName,
            ActiveFingerprint = _state.State.ActiveFingerprint is null
                ? null
                : [.. _state.State.ActiveFingerprint],
            Rebuild = _state.State.Rebuild?.Copy(),
            LastCompletedRecordCount = _state.State.LastCompletedRecordCount,
        };
    }

    private async Task PersistAsync(StorageIndexSchemaState candidate)
    {
        EnsureUsable();
        ValidateState(candidate);
        var previous = _state.State;
        _state.State = candidate;
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

    internal static void ValidateState(StorageIndexSchemaState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        if (!state.Initialized)
        {
            if (state.ProtocolVersion != 0
                || !string.IsNullOrEmpty(state.ProviderName)
                || !string.IsNullOrEmpty(state.StateName)
                || state.ActiveFingerprint is not null
                || state.Rebuild is not null
                || state.LastCompletedRecordCount != 0)
            {
                throw new InvalidOperationException(
                    "An uninitialized index-schema control contains persisted data.");
            }

            return;
        }

        if (state.ProtocolVersion != StorageIndexSchema.ProtocolVersion
            || string.IsNullOrWhiteSpace(state.ProviderName)
            || string.IsNullOrWhiteSpace(state.StateName)
            || state.LastCompletedRecordCount < 0)
        {
            throw new InvalidOperationException(
                "The persisted index-schema control contains invalid identity or counters.");
        }

        if (state.ActiveFingerprint is not null)
        {
            ValidatePersistedIdentityBytes(state.ActiveFingerprint, "active fingerprint");
        }

        if (state.Rebuild is not { } rebuild)
        {
            return;
        }

        ValidatePersistedIdentityBytes(rebuild.SchemaKey, "schema key");
        ValidatePersistedIdentityBytes(rebuild.TargetFingerprint, "target fingerprint");
        ValidatePersistedIdentityBytes(rebuild.LayoutFingerprint, "layout fingerprint");
        if (rebuild.HasAfter)
        {
            StorageCapacityGuardrails.ValidateGrainId(rebuild.After);
        }

        if (rebuild.RebuildId == Guid.Empty
            || rebuild.LayoutEpoch <= 0
            || rebuild.OwnerCount <= 0
            || rebuild.OwnerCount > StorageLayout.MaximumVirtualSlotCount
            || rebuild.NextProtocolOwnerIndex < 0
            || rebuild.NextProtocolOwnerIndex > rebuild.OwnerCount
            || rebuild.NextOwnerIndex < 0
            || rebuild.NextOwnerIndex > rebuild.OwnerCount
            || rebuild.ProcessedRecordCount < 0
            || (rebuild.NextProtocolOwnerIndex != rebuild.OwnerCount
                && (rebuild.NextOwnerIndex != 0
                    || rebuild.HasAfter
                    || !rebuild.After.IsDefault
                    || rebuild.ProcessedRecordCount != 0
                    || rebuild.LayoutProtocolPublished))
            || (rebuild.LayoutProtocolPublished
                && (rebuild.NextProtocolOwnerIndex != rebuild.OwnerCount
                    || rebuild.NextOwnerIndex != rebuild.OwnerCount))
            || (rebuild.NextOwnerIndex == rebuild.OwnerCount
                && (!rebuild.LayoutProtocolPublished
                    || rebuild.HasAfter
                    || !rebuild.After.IsDefault))
            || rebuild.HasAfter == rebuild.After.IsDefault
            || (state.ActiveFingerprint is not null
                && IndexSchemaIdentity.FixedTimeEquals(
                    state.ActiveFingerprint,
                    rebuild.TargetFingerprint)))
        {
            throw new InvalidOperationException(
                "The persisted index-schema rebuild intent contains invalid progress.");
        }
    }

    private static void ValidatePersistedIdentityBytes(byte[]? identity, string description)
    {
        if (identity is null || identity.Length != IndexSchemaDefinition.FingerprintLength)
        {
            throw new InvalidOperationException(
                $"The persisted index-schema {description} has an invalid length.");
        }
    }

    private void EnsureUsable()
    {
        if (!_usable)
        {
            throw new InvalidOperationException(
                "The index-schema control activation is retiring after an ambiguous persistence outcome.");
        }
    }

    private void PoisonActivation()
    {
        _usable = false;
        _requestDeactivation();
    }
}
