using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StorageLayoutGrain : Grain, IStorageLayoutGrain
{
    private const long InitialRoutingEpoch = 1;

    private readonly string? _providerName;
    private readonly Func<int, IStoragePartitionGrain> _getPartition;
    private readonly Action _requestDeactivation;
    private readonly IPersistentState<StorageLayoutState> _state;
    private StorageLayoutSnapshot? _routingSnapshot;
    private bool _routingStateValidated;
    private bool _layoutWriteInProgress;
    private bool _durableLayoutInitializedDuringWrite;
    private int _durableLayoutFormatVersionDuringWrite;
    private bool _usable = true;

    public StorageLayoutGrain(
        [PersistentState("layout", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StorageLayoutState> state)
        : this(
            state,
            providerName: null,
            requestDeactivation: null,
            getPartition: null)
    {
    }

    internal StorageLayoutGrain(
        IPersistentState<StorageLayoutState> state,
        string? providerName,
        Action? requestDeactivation,
        Func<int, IStoragePartitionGrain>? getPartition = null)
    {
        ArgumentNullException.ThrowIfNull(state);
        if (providerName is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        }

        _state = state;
        _providerName = providerName;
        _requestDeactivation = requestDeactivation ?? DeactivateOnIdle;
        _getPartition = getPartition ?? (partitionIndex => GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(ProviderName, partitionIndex)));
    }

    private string ProviderName => _providerName ?? this.GetPrimaryKeyString();

    public async Task InitializeAsync(StorageLayoutDescriptor descriptor)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(descriptor);

        if (descriptor.FormatVersion == StorageLayout.MovementFormatVersion)
        {
            _ = await InitializeRoutingAsync(descriptor);
            return;
        }

        ValidateLegacyDescriptor(descriptor);
        if (_state.State.Initialized)
        {
            if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
            {
                EnsureLegacyMatches(descriptor);
                return;
            }

            if (StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
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
            if (StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
            {
                EnsureRoutingMatches(descriptor);
                return CreateSnapshot();
            }

            if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
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
            if (descriptor.FormatVersion == StorageLayout.MovementFormatVersion)
            {
                ValidateRoutingSeed(descriptor);
            }

            return Task.FromResult(false);
        }

        if (descriptor.FormatVersion == StorageLayout.LegacyFormatVersion)
        {
            if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
            {
                EnsureLegacyMatches(descriptor);
            }
            else if (StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
            {
                EnsureLegacyCompatibleWithRouting(descriptor);
            }
            else
            {
                ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
            }
        }
        else if (StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
        {
            EnsureRoutingMatches(descriptor);
        }
        else if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
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

        if (identity.FormatVersion == StorageLayout.LegacyFormatVersion)
        {
            if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
            {
                EnsureLegacyIdentityMatches(identity);
            }
            else if (StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
            {
                EnsureLegacyIdentityCompatibleWithRouting(identity);
            }
            else
            {
                ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
            }
        }
        else if (StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
        {
            EnsureRoutingIdentityMatches(identity);
        }
        else if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
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

        if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
        {
            throw CreateRoutingInitializationRequiredException();
        }

        if (!StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
        {
            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        EnsureRoutingIdentityMatches(identity);
        return Task.FromResult<StorageLayoutSnapshot?>(CreateSnapshot());
    }

    public Task<StorageLayoutSnapshot?> GetCurrentLayoutAsync()
    {
        EnsureUsable();
        if (_layoutWriteInProgress)
        {
            if (!_durableLayoutInitializedDuringWrite)
            {
                return Task.FromResult<StorageLayoutSnapshot?>(null);
            }

            if (_durableLayoutFormatVersionDuringWrite == StorageLayout.LegacyFormatVersion)
            {
                throw CreateRoutingInitializationRequiredException();
            }

            if (!StorageLayout.IsRoutingFormatVersion(_durableLayoutFormatVersionDuringWrite))
            {
                ThrowUnsupportedPersistedVersion(_durableLayoutFormatVersionDuringWrite);
            }

            return Task.FromResult<StorageLayoutSnapshot?>(_routingSnapshot
                ?? throw new InvalidOperationException(
                    "The last durable routing snapshot is unavailable during a layout write."));
        }

        if (!_state.State.Initialized)
        {
            return Task.FromResult<StorageLayoutSnapshot?>(null);
        }

        if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
        {
            throw CreateRoutingInitializationRequiredException();
        }

        if (!StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
        {
            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        ValidateRoutingState();
        return Task.FromResult<StorageLayoutSnapshot?>(CreateSnapshot());
    }

    public async Task<StorageLayoutSnapshot> BeginIndexSchemaProtocolEnablementAsync(
        StorageIndexSchemaLayoutProtocolRequest request)
    {
        EnsureUsable();
        EnsureCurrentRoutingState();
        ValidateIndexSchemaEnablementRequest(request);
        var current = CreateSnapshot();
        EnsureIndexSchemaRequestMatchesLayout(request, current);

        if (_state.State.MovementEnablement is not null || _state.State.MoveIntent is not null)
        {
            throw new InvalidOperationException(
                "Index-schema enablement and virtual-slot movement cannot run at the same time.");
        }

        if (_state.State.IndexSchemaProtocolVersion is not 0
            and not StorageLayout.CurrentIndexSchemaProtocolVersion)
        {
            throw new InvalidOperationException(
                $"Layout index-schema protocol version {_state.State.IndexSchemaProtocolVersion} is not supported.");
        }

        if (_state.State.IndexSchemaEnablement is { } active)
        {
            EnsureSameIndexSchemaEnablement(active, request);
            return current;
        }

        var candidate = _state.State.Copy();
        candidate.FormatVersion = StorageLayout.IndexSchemaFormatVersion;
        candidate.IndexSchemaEnablement = new StorageIndexSchemaEnableIntent
        {
            EnablementId = request.EnablementId,
            ProtocolVersion = request.ProtocolVersion,
            LayoutEpoch = request.LayoutEpoch,
            LayoutFingerprint = [.. request.LayoutFingerprint],
        };
        await PersistAsync(candidate);
        return CreateSnapshot();
    }

    public async Task<StorageLayoutSnapshot> EnableIndexSchemaProtocolAsync(
        StorageIndexSchemaLayoutProtocolRequest request)
    {
        EnsureUsable();
        EnsureCurrentRoutingState();
        ValidateIndexSchemaEnablementRequest(request);
        var current = CreateSnapshot();
        EnsureIndexSchemaRequestMatchesLayout(request, current);

        if (_state.State.MovementEnablement is not null || _state.State.MoveIntent is not null)
        {
            throw new InvalidOperationException(
                "Index-schema enablement and virtual-slot movement cannot run at the same time.");
        }

        if (_state.State.IndexSchemaProtocolVersion
                == StorageLayout.CurrentIndexSchemaProtocolVersion
            && _state.State.IndexSchemaEnablement is null)
        {
            // The publish CAS may have committed before its acknowledgement was lost.
            return current;
        }

        if (_state.State.IndexSchemaProtocolVersion is not 0
            and not StorageLayout.CurrentIndexSchemaProtocolVersion)
        {
            throw new InvalidOperationException(
                $"Layout index-schema protocol version {_state.State.IndexSchemaProtocolVersion} is not supported.");
        }

        var active = _state.State.IndexSchemaEnablement
            ?? throw new InvalidOperationException(
                "Index-schema enablement must be durably begun before it can be published.");
        EnsureSameIndexSchemaEnablement(active, request);

        var candidate = _state.State.Copy();
        candidate.IndexSchemaProtocolVersion = StorageLayout.CurrentIndexSchemaProtocolVersion;
        candidate.IndexSchemaEnablement = null;
        await PersistAsync(candidate);
        return CreateSnapshot();
    }

    public async Task<StorageLayoutSnapshot> BeginMovementEnablementAsync()
    {
        EnsureUsable();
        EnsureCurrentRoutingState();
        EnsureNoIndexSchemaEnablement();
        if (_state.State.MovementProtocolVersion == StorageLayout.CurrentMovementProtocolVersion)
        {
            return CreateSnapshot();
        }

        if (_state.State.MovementEnablement is not null)
        {
            return CreateSnapshot();
        }

        var candidate = _state.State.Copy();
        candidate.MovementEnablement = new StorageMovementEnableIntent
        {
            EnablementId = Guid.NewGuid(),
            SourceEpoch = candidate.Epoch,
            PlannedEpoch = checked(candidate.Epoch + 1),
            Owners = candidate.SlotAssignments.Distinct().Order().ToArray(),
        };
        await PersistAsync(candidate);
        return CreateSnapshot();
    }

    public async Task<StorageLayoutSnapshot> AdvanceMovementEnablementAsync(Guid enablementId)
    {
        EnsureUsable();
        if (enablementId == Guid.Empty)
        {
            throw new ArgumentException("An enablement id must not be empty.", nameof(enablementId));
        }

        EnsureCurrentRoutingState();
        EnsureNoIndexSchemaEnablement();
        if (_state.State.MovementProtocolVersion == StorageLayout.CurrentMovementProtocolVersion
            && _state.State.MovementEnablement is null)
        {
            // The final layout CAS may have committed before its acknowledgement was lost.
            return CreateSnapshot();
        }

        var enablement = _state.State.MovementEnablement
            ?? throw new InvalidOperationException("No movement enablement is in progress.");
        if (enablement.EnablementId != enablementId)
        {
            throw new InvalidOperationException(
                $"Movement enablement '{enablement.EnablementId}' is active; '{enablementId}' cannot advance it.");
        }

        if (enablement.NextOwnerIndex < enablement.Owners.Length)
        {
            var owner = enablement.Owners[enablement.NextOwnerIndex];
            var participant = await _getPartition(owner).EnableMovementProtocolAsync(
                CreateProtocolRequest(enablement.PlannedEpoch));
            ValidateProtocolState(participant, enablement.PlannedEpoch, allowMove: false);

            var candidate = _state.State.Copy();
            candidate.MovementEnablement!.NextOwnerIndex = checked(
                candidate.MovementEnablement.NextOwnerIndex + 1);
            await PersistAsync(candidate);
            return CreateSnapshot();
        }

        var enabled = _state.State.Copy();
        enabled.Epoch = enablement.PlannedEpoch;
        enabled.MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion;
        enabled.MovementEnablement = null;
        await PersistAsync(enabled);
        return CreateSnapshot();
    }

    public async Task<StorageSlotMoveProgressSnapshot> PlanMoveAsync(
        StorageSlotMovePlanRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ValidateMoveProtocolVersion(request.MovementProtocolVersion, nameof(request));
        ArgumentOutOfRangeException.ThrowIfNegative(request.Slot);
        StorageLayout.ValidateOwnerIndex(request.TargetOwner, nameof(request));
        StorageMoveProtocol.ValidatePageLimits(
            request.TransferPageRecordLimit,
            request.TransferPageByteTarget,
            nameof(request));
        EnsureNoIndexSchemaEnablement();
        EnsureMovementEnabled();
        if (request.Slot >= _state.State.VirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.Slot,
                $"A move slot must be less than the persisted virtual-slot count {_state.State.VirtualSlotCount}.");
        }

        if (_state.State.Epoch == long.MaxValue)
        {
            throw new InvalidOperationException(
                "A slot move cannot be planned because the routing epoch cannot advance further.");
        }

        if (_state.State.MoveIntent is { } active)
        {
            if (active.Slot == request.Slot
                && active.TargetOwner == request.TargetOwner
                && active.TransferPageRecordLimit == request.TransferPageRecordLimit
                && active.TransferPageByteTarget == request.TransferPageByteTarget)
            {
                return await CreateMoveProgressAsync(active);
            }

            throw new InvalidOperationException(
                $"Move '{active.MoveId}' is already active for slot {active.Slot}.");
        }

        var sourceOwner = _state.State.SlotAssignments[request.Slot];
        if (sourceOwner == request.TargetOwner)
        {
            throw new ArgumentException(
                $"Virtual slot {request.Slot} is already assigned to partition {request.TargetOwner}.",
                nameof(request));
        }

        var candidate = _state.State.Copy();
        candidate.MoveIntent = new StorageSlotMoveIntent
        {
            MoveId = Guid.NewGuid(),
            Slot = request.Slot,
            SourceOwner = sourceOwner,
            TargetOwner = request.TargetOwner,
            SourceEpoch = candidate.Epoch,
            Phase = SearchableStorageSlotMovePhase.Planned,
            TransferPageRecordLimit = request.TransferPageRecordLimit,
            TransferPageByteTarget = request.TransferPageByteTarget,
        };
        await PersistAsync(candidate);
        return await CreateMoveProgressAsync(candidate.MoveIntent);
    }

    public async Task<StorageSlotMoveProgressSnapshot?> GetMoveProgressAsync()
    {
        EnsureUsable();
        EnsureCurrentRoutingState();
        return _state.State.MoveIntent is null
            ? null
            : await CreateMoveProgressAsync(_state.State.MoveIntent);
    }

    public async Task<StorageSlotMoveProgressSnapshot> AdvanceMoveAsync(
        StorageSlotMoveCommand command)
    {
        EnsureUsable();
        ValidateMoveCommand(command);
        EnsureMovementEnabled();
        var move = GetMoveOrTerminalReceipt(command.MoveId, out var terminal);
        if (terminal is not null)
        {
            return CreateTerminalProgress(terminal, _state.State.Epoch);
        }

        return move!.Phase switch
        {
            SearchableStorageSlotMovePhase.Planned => await AdvancePlannedMoveAsync(move),
            SearchableStorageSlotMovePhase.SourceFrozen => await AdvanceFrozenMoveAsync(move),
            SearchableStorageSlotMovePhase.TargetVersionFenced
                or SearchableStorageSlotMovePhase.Copying => await AdvanceCopyAsync(move),
            SearchableStorageSlotMovePhase.CopyComplete => await CommitOwnershipAsync(move),
            SearchableStorageSlotMovePhase.OwnershipCommitted => await FenceSourceVisibilityAsync(move),
            SearchableStorageSlotMovePhase.SourceVisibilityFenced => await EnableTargetAsync(move),
            SearchableStorageSlotMovePhase.TargetEnabled
                or SearchableStorageSlotMovePhase.DeletingSource => await AdvanceSourceCleanupAsync(move),
            SearchableStorageSlotMovePhase.Retiring => await AdvanceRetirementAsync(move),
            SearchableStorageSlotMovePhase.Aborting => await AdvanceAbortAsync(move),
            _ => throw new InvalidOperationException(
                $"Move '{move.MoveId}' has unsupported durable phase {move.Phase}."),
        };
    }

    public async Task<StorageSlotMoveProgressSnapshot> RequestMoveAbortAsync(
        StorageSlotMoveCommand command)
    {
        EnsureUsable();
        ValidateMoveCommand(command);
        EnsureMovementEnabled();
        var move = GetMoveOrTerminalReceipt(command.MoveId, out var terminal);
        if (terminal is not null)
        {
            if (terminal.TerminalPhase == SearchableStorageSlotMovePhase.Aborted)
            {
                return CreateTerminalProgress(terminal, _state.State.Epoch);
            }

            throw new InvalidOperationException(
                $"Move '{command.MoveId}' completed its ownership commit and cannot be aborted.");
        }

        if (move!.Phase == SearchableStorageSlotMovePhase.Aborting)
        {
            return await CreateMoveProgressAsync(move);
        }

        if (move.Phase >= SearchableStorageSlotMovePhase.OwnershipCommitted)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' committed ownership in epoch {checked(move.SourceEpoch + 1)} and cannot be aborted.");
        }

        var participants = await ReadParticipantsAsync(move);
        return await PersistMovePhaseAsync(
            move,
            SearchableStorageSlotMovePhase.Aborting,
            participants);
    }

    private async Task<StorageSlotMoveProgressSnapshot> AdvancePlannedMoveAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var target = await _getPartition(move.TargetOwner).GetMovementStateAsync();
        EnsureParticipantControlMatches(target.MoveControl, identity, StoragePartitionMoveRole.Target);
        if (target.MoveControl.IsPresent)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' has target progress before its source was durably frozen.");
        }

        if (!IsProtocolReady(target, move.SourceEpoch))
        {
            target = await _getPartition(move.TargetOwner).EnableMovementProtocolAsync(
                CreateProtocolRequest(move.SourceEpoch));
            ValidateProtocolState(target, move.SourceEpoch, allowMove: false);
            return await CreateMoveProgressAsync(move);
        }

        var source = await _getPartition(move.SourceOwner).GetMovementStateAsync();
        EnsureParticipantControlMatches(source.MoveControl, identity, StoragePartitionMoveRole.Source);
        if (source.MoveControl.IsPresent)
        {
            ValidateMoveParticipant(source, identity, StoragePartitionMoveRole.Source);
            if (source.MoveControl.Phase != StoragePartitionMovePhase.SourceFrozen)
            {
                throw new InvalidOperationException(
                    $"Move '{move.MoveId}' has an invalid source phase {source.MoveControl.Phase} while planned.");
            }

            return await PersistMovePhaseAsync(
                move,
                SearchableStorageSlotMovePhase.SourceFrozen,
                new MoveParticipants(source, target));
        }

        if (!IsProtocolReady(source, move.SourceEpoch))
        {
            source = await _getPartition(move.SourceOwner).EnableMovementProtocolAsync(
                CreateProtocolRequest(move.SourceEpoch));
            ValidateProtocolState(source, move.SourceEpoch, allowMove: false);
            return await CreateMoveProgressAsync(move);
        }

        source = await _getPartition(move.SourceOwner).FreezeMoveSourceAsync(identity);
        ValidateMoveParticipant(source, identity, StoragePartitionMoveRole.Source);
        if (source.MoveControl.Phase != StoragePartitionMovePhase.SourceFrozen)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' did not durably freeze its source.");
        }

        return await PersistMovePhaseAsync(
            move,
            SearchableStorageSlotMovePhase.SourceFrozen,
            new MoveParticipants(source, target));
    }

    private async Task<StorageSlotMoveProgressSnapshot> AdvanceFrozenMoveAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        var sourceControl = RequireParticipantPhase(
            participants.Source,
            identity,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceFrozen);
        ValidateProtocolState(participants.Target, move.SourceEpoch, allowMove: true);
        EnsureParticipantControlMatches(
            participants.Target.MoveControl,
            identity,
            StoragePartitionMoveRole.Target);

        var target = participants.Target;
        if (target.MoveControl.IsPresent)
        {
            var reconciledPhase = target.MoveControl.Phase switch
            {
                StoragePartitionMovePhase.TargetImporting =>
                    SearchableStorageSlotMovePhase.TargetVersionFenced,
                StoragePartitionMovePhase.TargetImportComplete =>
                    SearchableStorageSlotMovePhase.CopyComplete,
                StoragePartitionMovePhase.TargetPrepared =>
                    SearchableStorageSlotMovePhase.Planned,
                _ => throw new InvalidOperationException(
                    $"Move '{move.MoveId}' has invalid target phase {target.MoveControl.Phase} while source-frozen."),
            };
            if (reconciledPhase != SearchableStorageSlotMovePhase.Planned)
            {
                return await PersistMovePhaseAsync(
                    move,
                    reconciledPhase,
                    participants);
            }
        }

        target = await _getPartition(move.TargetOwner).PrepareMoveTargetAsync(
            new StorageMoveTargetPrepareRequest
            {
                Move = identity,
                FrozenNextVersion = sourceControl.FrozenNextVersion,
            });
        ValidateMoveParticipant(target, identity, StoragePartitionMoveRole.Target);
        var nextPhase = target.MoveControl.Phase switch
        {
            StoragePartitionMovePhase.TargetImporting =>
                SearchableStorageSlotMovePhase.TargetVersionFenced,
            StoragePartitionMovePhase.TargetImportComplete =>
                SearchableStorageSlotMovePhase.CopyComplete,
            _ => throw new InvalidOperationException(
                $"Move '{move.MoveId}' did not durably fence its target version sequence."),
        };
        return await PersistMovePhaseAsync(
            move,
            nextPhase,
            new MoveParticipants(participants.Source, target));
    }

    private async Task<StorageSlotMoveProgressSnapshot> AdvanceCopyAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        var sourceControl = RequireParticipantPhase(
            participants.Source,
            identity,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceFrozen);
        var targetControl = RequireParticipantPhase(
            participants.Target,
            identity,
            StoragePartitionMoveRole.Target,
            StoragePartitionMovePhase.TargetImporting,
            StoragePartitionMovePhase.TargetImportComplete);
        if (targetControl.FrozenNextVersion != sourceControl.FrozenNextVersion
            || participants.Target.NextVersion < sourceControl.FrozenNextVersion)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' target is not fenced to at least source high-water mark "
                + $"{sourceControl.FrozenNextVersion}.");
        }

        if (targetControl.Phase == StoragePartitionMovePhase.TargetImportComplete)
        {
            return await PersistMovePhaseAsync(
                move,
                SearchableStorageSlotMovePhase.CopyComplete,
                participants);
        }

        var pageRequest = new StorageMovePageRequest
        {
            Move = identity,
            PageOrdinal = targetControl.NextPageOrdinal,
            AfterRecordKey = targetControl.ProgressAfterRecordKey,
            ItemLimit = move.TransferPageRecordLimit,
            ByteTarget = move.TransferPageByteTarget,
        };
        var page = await _getPartition(move.SourceOwner).ExportMovePageAsync(pageRequest);
        ValidateExportPage(page, pageRequest, sourceControl.FrozenNextVersion);
        var committed = await _getPartition(move.TargetOwner).ImportMovePageAsync(
            new StorageMoveImportPageRequest { Page = page });
        ValidateMoveParticipant(committed.State, identity, StoragePartitionMoveRole.Target);
        var committedControl = committed.State.MoveControl;
        var nextPhase = committedControl.Phase switch
        {
            StoragePartitionMovePhase.TargetImporting => SearchableStorageSlotMovePhase.Copying,
            StoragePartitionMovePhase.TargetImportComplete => SearchableStorageSlotMovePhase.CopyComplete,
            _ => throw new InvalidOperationException(
                $"Move '{move.MoveId}' import returned invalid target phase {committedControl.Phase}."),
        };
        return await PersistMovePhaseAsync(
            move,
            nextPhase,
            new MoveParticipants(participants.Source, committed.State));
    }

    private async Task<StorageSlotMoveProgressSnapshot> CommitOwnershipAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        var sourceControl = RequireParticipantPhase(
            participants.Source,
            identity,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceFrozen);
        var targetControl = RequireParticipantPhase(
            participants.Target,
            identity,
            StoragePartitionMoveRole.Target,
            StoragePartitionMovePhase.TargetImportComplete);
        if (targetControl.FrozenNextVersion != sourceControl.FrozenNextVersion
            || participants.Target.NextVersion < sourceControl.FrozenNextVersion)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' cannot commit before the target reaches source high-water mark "
                + $"{sourceControl.FrozenNextVersion}.");
        }

        var candidate = _state.State.Copy();
        var candidateMove = candidate.MoveIntent
            ?? throw new InvalidOperationException("The active move intent disappeared before ownership commit.");
        EnsureSameMove(candidateMove, move);
        if (candidate.Epoch != move.SourceEpoch
            || candidate.SlotAssignments[move.Slot] != move.SourceOwner)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' no longer owns its source routing boundary.");
        }

        CaptureMoveCounters(candidateMove, participants);
        candidateMove.Phase = SearchableStorageSlotMovePhase.OwnershipCommitted;
        candidate.SlotAssignments[move.Slot] = move.TargetOwner;
        candidate.Epoch = checked(move.SourceEpoch + 1);
        await PersistAsync(candidate);
        return CreateProgress(candidateMove, candidate.Epoch);
    }

    private async Task<StorageSlotMoveProgressSnapshot> FenceSourceVisibilityAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        _ = RequireParticipantPhase(
            participants.Target,
            identity,
            StoragePartitionMoveRole.Target,
            StoragePartitionMovePhase.TargetImportComplete);
        EnsureParticipantControlMatches(
            participants.Source.MoveControl,
            identity,
            StoragePartitionMoveRole.Source);
        var source = participants.Source;
        if (!source.MoveControl.IsPresent
            || source.MoveControl.Phase == StoragePartitionMovePhase.SourceFrozen)
        {
            source = await _getPartition(move.SourceOwner).HideMoveSourceAsync(
                new StorageMoveVisibilityFenceRequest
                {
                    Move = identity,
                    CommittedEpoch = checked(move.SourceEpoch + 1),
                });
        }

        _ = RequireParticipantPhase(
            source,
            identity,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceHidden,
            StoragePartitionMovePhase.SourceDeleting,
            StoragePartitionMovePhase.SourceDeleteComplete);
        if (source.MinimumRoutingEpoch < checked(move.SourceEpoch + 1))
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' source lacks its committed-epoch visibility fence.");
        }

        return await PersistMovePhaseAsync(
            move,
            SearchableStorageSlotMovePhase.SourceVisibilityFenced,
            new MoveParticipants(source, participants.Target));
    }

    private async Task<StorageSlotMoveProgressSnapshot> EnableTargetAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        _ = RequireParticipantPhase(
            participants.Source,
            identity,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceHidden,
            StoragePartitionMovePhase.SourceDeleting,
            StoragePartitionMovePhase.SourceDeleteComplete);
        EnsureParticipantControlMatches(
            participants.Target.MoveControl,
            identity,
            StoragePartitionMoveRole.Target);
        var target = participants.Target;
        if (!target.MoveControl.IsPresent
            || target.MoveControl.Phase == StoragePartitionMovePhase.TargetImportComplete)
        {
            target = await _getPartition(move.TargetOwner).EnableMoveTargetAsync(identity);
        }

        _ = RequireParticipantPhase(
            target,
            identity,
            StoragePartitionMoveRole.Target,
            StoragePartitionMovePhase.TargetEnabled);
        return await PersistMovePhaseAsync(
            move,
            SearchableStorageSlotMovePhase.TargetEnabled,
            new MoveParticipants(participants.Source, target));
    }

    private async Task<StorageSlotMoveProgressSnapshot> AdvanceSourceCleanupAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        _ = RequireParticipantPhase(
            participants.Target,
            identity,
            StoragePartitionMoveRole.Target,
            StoragePartitionMovePhase.TargetEnabled);
        var sourceControl = RequireParticipantPhase(
            participants.Source,
            identity,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceHidden,
            StoragePartitionMovePhase.SourceDeleting,
            StoragePartitionMovePhase.SourceDeleteComplete);
        if (sourceControl.Phase == StoragePartitionMovePhase.SourceDeleteComplete)
        {
            return await PersistMovePhaseAsync(
                move,
                SearchableStorageSlotMovePhase.Retiring,
                participants);
        }

        var deleted = await _getPartition(move.SourceOwner).DeleteMovePageAsync(
            new StorageMoveDeletePageRequest
            {
                Move = identity,
                Mode = StorageMoveDeleteMode.SourceCleanup,
                PageOrdinal = sourceControl.NextPageOrdinal,
                AfterRecordKey = sourceControl.ProgressAfterRecordKey,
                ItemLimit = move.TransferPageRecordLimit,
                ByteTarget = move.TransferPageByteTarget,
            });
        ValidateMoveParticipant(deleted.State, identity, StoragePartitionMoveRole.Source);
        var nextPhase = deleted.State.MoveControl.Phase switch
        {
            StoragePartitionMovePhase.SourceDeleting => SearchableStorageSlotMovePhase.DeletingSource,
            StoragePartitionMovePhase.SourceDeleteComplete => SearchableStorageSlotMovePhase.Retiring,
            _ => throw new InvalidOperationException(
                $"Move '{move.MoveId}' cleanup returned invalid source phase "
                + $"{deleted.State.MoveControl.Phase}."),
        };
        return await PersistMovePhaseAsync(
            move,
            nextPhase,
            new MoveParticipants(deleted.State, participants.Target));
    }

    private async Task<StorageSlotMoveProgressSnapshot> AdvanceRetirementAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        EnsureParticipantControlMatches(
            participants.Source.MoveControl,
            identity,
            StoragePartitionMoveRole.Source);
        EnsureParticipantControlMatches(
            participants.Target.MoveControl,
            identity,
            StoragePartitionMoveRole.Target);

        if (participants.Source.MoveControl.IsPresent)
        {
            if (participants.Source.MoveControl.Phase != StoragePartitionMovePhase.SourceDeleteComplete)
            {
                throw new InvalidOperationException(
                    $"Move '{move.MoveId}' cannot retire source phase {participants.Source.MoveControl.Phase}.");
            }

            _ = await _getPartition(move.SourceOwner).RetireMoveParticipantAsync(
                new StorageMoveRetireRequest
                {
                    Move = identity,
                    Kind = StorageMoveRetirementKind.Completed,
                });
            return await CreateMoveProgressAsync(move);
        }

        if (participants.Target.MoveControl.IsPresent)
        {
            if (participants.Target.MoveControl.Phase != StoragePartitionMovePhase.TargetEnabled)
            {
                throw new InvalidOperationException(
                    $"Move '{move.MoveId}' cannot retire target phase {participants.Target.MoveControl.Phase}.");
            }

            _ = await _getPartition(move.TargetOwner).RetireMoveParticipantAsync(
                new StorageMoveRetireRequest
                {
                    Move = identity,
                    Kind = StorageMoveRetirementKind.Completed,
                });
            return await CreateMoveProgressAsync(move);
        }

        return await CompleteMoveAsync(move, SearchableStorageSlotMovePhase.Completed);
    }

    private async Task<StorageSlotMoveProgressSnapshot> AdvanceAbortAsync(
        StorageSlotMoveIntent move)
    {
        var identity = CreateMoveIdentity(move);
        var participants = await ReadParticipantsAsync(move);
        EnsureParticipantControlMatches(
            participants.Source.MoveControl,
            identity,
            StoragePartitionMoveRole.Source);
        EnsureParticipantControlMatches(
            participants.Target.MoveControl,
            identity,
            StoragePartitionMoveRole.Target);

        if (participants.Target.MoveControl.IsPresent)
        {
            var targetControl = participants.Target.MoveControl;
            if (targetControl.Phase == StoragePartitionMovePhase.TargetPrepared)
            {
                _ = await _getPartition(move.TargetOwner).PrepareMoveTargetAsync(
                    new StorageMoveTargetPrepareRequest
                    {
                        Move = identity,
                        FrozenNextVersion = targetControl.FrozenNextVersion,
                    });
                return await CreateMoveProgressAsync(move);
            }

            if (targetControl.Phase is StoragePartitionMovePhase.TargetImporting
                or StoragePartitionMovePhase.TargetImportComplete
                or StoragePartitionMovePhase.TargetAbortDeleting)
            {
                var resetCursor = targetControl.Phase is StoragePartitionMovePhase.TargetImporting
                    or StoragePartitionMovePhase.TargetImportComplete;
                _ = await _getPartition(move.TargetOwner).DeleteMovePageAsync(
                    new StorageMoveDeletePageRequest
                    {
                        Move = identity,
                        Mode = StorageMoveDeleteMode.TargetAbort,
                        PageOrdinal = resetCursor ? 0 : targetControl.NextPageOrdinal,
                        AfterRecordKey = resetCursor ? null : targetControl.ProgressAfterRecordKey,
                        ItemLimit = move.TransferPageRecordLimit,
                        ByteTarget = move.TransferPageByteTarget,
                    });
                return await CreateMoveProgressAsync(move);
            }

            if (targetControl.Phase != StoragePartitionMovePhase.TargetAbortComplete)
            {
                throw new InvalidOperationException(
                    $"Move '{move.MoveId}' cannot abort target phase {targetControl.Phase}.");
            }

            _ = await _getPartition(move.TargetOwner).RetireMoveParticipantAsync(
                new StorageMoveRetireRequest
                {
                    Move = identity,
                    Kind = StorageMoveRetirementKind.Aborted,
                });
            return await CreateMoveProgressAsync(move);
        }

        if (participants.Source.MoveControl.IsPresent)
        {
            if (participants.Source.MoveControl.Phase != StoragePartitionMovePhase.SourceFrozen)
            {
                throw new InvalidOperationException(
                    $"Move '{move.MoveId}' cannot abort source phase {participants.Source.MoveControl.Phase}.");
            }

            _ = await _getPartition(move.SourceOwner).RetireMoveParticipantAsync(
                new StorageMoveRetireRequest
                {
                    Move = identity,
                    Kind = StorageMoveRetirementKind.Aborted,
                });
            return await CreateMoveProgressAsync(move);
        }

        return await CompleteMoveAsync(move, SearchableStorageSlotMovePhase.Aborted);
    }

    private void EnsureCurrentRoutingState()
    {
        if (!_state.State.Initialized)
        {
            throw new InvalidOperationException(
                "The searchable-storage layout must be initialized before routing protocols can be administered.");
        }

        if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion)
        {
            throw CreateRoutingInitializationRequiredException();
        }

        if (!StorageLayout.IsRoutingFormatVersion(_state.State.FormatVersion))
        {
            ThrowUnsupportedPersistedVersion(_state.State.FormatVersion);
        }

        ValidateRoutingState();
    }

    private static void ValidateIndexSchemaEnablementRequest(
        StorageIndexSchemaLayoutProtocolRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.ProtocolVersion != StorageLayout.CurrentIndexSchemaProtocolVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ProtocolVersion,
                "Unknown layout index-schema protocol version.");
        }

        if (request.EnablementId == Guid.Empty)
        {
            throw new ArgumentException(
                "An index-schema enablement id must not be empty.",
                nameof(request));
        }

        Indexing.IndexSchemaIdentity.ValidateIdentity(
            request.LayoutFingerprint,
            nameof(request));
    }

    private static void EnsureIndexSchemaRequestMatchesLayout(
        StorageIndexSchemaLayoutProtocolRequest request,
        StorageLayoutSnapshot current)
    {
        if (request.LayoutEpoch != current.Epoch
            || !Indexing.IndexSchemaIdentity.FixedTimeEquals(
                request.LayoutFingerprint,
                StorageLayoutFingerprint.Compute(current)))
        {
            throw new InvalidOperationException(
                "The index-schema capability request does not match the current routing layout.");
        }
    }

    private static void EnsureSameIndexSchemaEnablement(
        StorageIndexSchemaEnableIntent active,
        StorageIndexSchemaLayoutProtocolRequest request)
    {
        if (active.EnablementId != request.EnablementId
            || active.ProtocolVersion != request.ProtocolVersion
            || active.LayoutEpoch != request.LayoutEpoch
            || !Indexing.IndexSchemaIdentity.FixedTimeEquals(
                active.LayoutFingerprint,
                request.LayoutFingerprint))
        {
            throw new InvalidOperationException(
                $"Index-schema enablement '{active.EnablementId}' is active; "
                + $"'{request.EnablementId}' cannot address it.");
        }
    }

    private void EnsureNoIndexSchemaEnablement()
    {
        if (_state.State.IndexSchemaEnablement is { } active)
        {
            throw new InvalidOperationException(
                $"Index-schema enablement '{active.EnablementId}' is active; "
                + "virtual-slot movement cannot run until it is published.");
        }
    }

    private void EnsureMovementEnabled()
    {
        EnsureCurrentRoutingState();
        if (_state.State.MovementProtocolVersion != StorageLayout.CurrentMovementProtocolVersion)
        {
            var state = _state.State.MovementEnablement is null ? "disabled" : "still enabling";
            throw new InvalidOperationException(
                $"Live virtual-slot movement is {state}; complete EnableMovementAsync first.");
        }
    }

    private static void ValidateMoveProtocolVersion(int version, string parameterName)
    {
        if (version != StorageLayout.CurrentMovementProtocolVersion)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                version,
                $"Movement protocol version {StorageLayout.CurrentMovementProtocolVersion} is required.");
        }
    }

    private static void ValidateMoveCommand(StorageSlotMoveCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        ValidateMoveProtocolVersion(command.MovementProtocolVersion, nameof(command));
        if (command.MoveId == Guid.Empty)
        {
            throw new ArgumentException("A move id must not be empty.", nameof(command));
        }
    }

    private StorageSlotMoveIntent? GetMoveOrTerminalReceipt(
        Guid moveId,
        out StorageSlotMoveReceipt? terminal)
    {
        var move = _state.State.MoveIntent;
        if (move?.MoveId == moveId)
        {
            terminal = null;
            return move;
        }

        terminal = _state.State.LastMoveReceipt;
        if (terminal?.MoveId == moveId)
        {
            return null;
        }

        if (move is not null)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' is active; command '{moveId}' cannot address it.");
        }

        throw new InvalidOperationException($"Move '{moveId}' is not active and has no terminal receipt.");
    }

    private StoragePartitionProtocolRequest CreateProtocolRequest(long minimumRoutingEpoch)
    {
        return new StoragePartitionProtocolRequest
        {
            ProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
            VirtualSlotCount = _state.State.VirtualSlotCount,
            MinimumRoutingEpoch = minimumRoutingEpoch,
            JournalSegmentCapacity = _state.State.JournalSegmentCapacity,
            MaximumJournalReplayEntries = _state.State.MaximumJournalReplayEntries,
            IndexSchemaProtocolVersion = _state.State.IndexSchemaProtocolVersion,
        };
    }

    private StorageMoveIdentity CreateMoveIdentity(StorageSlotMoveIntent move)
    {
        return new StorageMoveIdentity
        {
            ProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = _state.State.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
        };
    }

    private async Task<MoveParticipants> ReadParticipantsAsync(StorageSlotMoveIntent move)
    {
        var sourceTask = _getPartition(move.SourceOwner).GetMovementStateAsync();
        var targetTask = _getPartition(move.TargetOwner).GetMovementStateAsync();
        await Task.WhenAll(sourceTask, targetTask);
        return new MoveParticipants(
            await sourceTask,
            await targetTask);
    }

    private bool IsProtocolReady(
        StoragePartitionProtocolState state,
        long minimumRoutingEpoch)
    {
        return StoragePersistence.SupportsMovement(state.PersistenceFormatVersion)
            && state.MovementProtocolVersion == StorageLayout.CurrentMovementProtocolVersion
            && state.RoutedOperationsRequired
            && state.MinimumRoutingEpoch == minimumRoutingEpoch
            && state.IndexSchemaProtocolVersion == _state.State.IndexSchemaProtocolVersion;
    }

    private void ValidateProtocolState(
        StoragePartitionProtocolState state,
        long minimumRoutingEpoch,
        bool allowMove)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(state.MoveControl);
        if (!IsProtocolReady(state, minimumRoutingEpoch)
            || (!allowMove && state.MoveControl.IsPresent))
        {
            throw new InvalidOperationException(
                $"A movement participant is not durably enabled and fenced at routing epoch "
                + $"{minimumRoutingEpoch}.");
        }
    }

    private void ValidateMoveParticipant(
        StoragePartitionProtocolState state,
        StorageMoveIdentity identity,
        StoragePartitionMoveRole role)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(state.MoveControl);
        if (!StoragePersistence.SupportsMovement(state.PersistenceFormatVersion)
            || state.MovementProtocolVersion != identity.ProtocolVersion
            || !state.RoutedOperationsRequired
            || state.IndexSchemaProtocolVersion != _state.State.IndexSchemaProtocolVersion
            || state.MinimumRoutingEpoch < identity.SourceEpoch
            || state.MinimumRoutingEpoch > checked(identity.SourceEpoch + 1))
        {
            throw new InvalidOperationException(
                $"Move '{identity.MoveId}' participant is not fenced for the active routing boundary.");
        }

        EnsureParticipantControlMatches(state.MoveControl, identity, role);
    }

    private static void EnsureParticipantControlMatches(
        StoragePartitionMoveControl control,
        StorageMoveIdentity identity,
        StoragePartitionMoveRole role)
    {
        ArgumentNullException.ThrowIfNull(control);
        if (!control.IsPresent)
        {
            return;
        }

        if (control.MoveId != identity.MoveId
            || control.Slot != identity.Slot
            || control.VirtualSlotCount != identity.VirtualSlotCount
            || control.SourceEpoch != identity.SourceEpoch
            || control.SourceOwner != identity.SourceOwner
            || control.TargetOwner != identity.TargetOwner
            || control.Role != role)
        {
            throw new InvalidOperationException(
                $"A {role} participant control does not match move '{identity.MoveId}'.");
        }
    }

    private StoragePartitionMoveControl RequireParticipantPhase(
        StoragePartitionProtocolState state,
        StorageMoveIdentity identity,
        StoragePartitionMoveRole role,
        params StoragePartitionMovePhase[] allowedPhases)
    {
        ValidateMoveParticipant(state, identity, role);
        var control = state.MoveControl;
        if (!control.IsPresent || !allowedPhases.Contains(control.Phase))
        {
            throw new InvalidOperationException(
                $"Move '{identity.MoveId}' requires {role} phase "
                + $"{string.Join(" or ", allowedPhases)}, but found "
                + (control.IsPresent ? control.Phase : StoragePartitionMovePhase.None) + ".");
        }

        return control;
    }

    private static void ValidateExportPage(
        StorageMoveExportPage page,
        StorageMovePageRequest request,
        long frozenNextVersion)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(page.Move);
        ArgumentNullException.ThrowIfNull(page.Records);
        ArgumentNullException.ThrowIfNull(page.PageDigest);
        if (page.Move.MoveId != request.Move.MoveId
            || page.Move.Slot != request.Move.Slot
            || page.Move.VirtualSlotCount != request.Move.VirtualSlotCount
            || page.Move.SourceEpoch != request.Move.SourceEpoch
            || page.Move.SourceOwner != request.Move.SourceOwner
            || page.Move.TargetOwner != request.Move.TargetOwner
            || page.PageOrdinal != request.PageOrdinal
            || !StorageMoveRecordCodec.TextEquals(page.AfterRecordKey, request.AfterRecordKey)
            || page.FrozenNextVersion != frozenNextVersion
            || page.ItemLimit != request.ItemLimit
            || page.ByteTarget != request.ByteTarget
            || page.Records.Count > request.ItemLimit
            || page.EncodedByteCount < 0
            || page.PageDigest.Length != StorageMovePageDigest.DigestLength
            || (!page.Exhausted && page.Records.Count == 0))
        {
            throw new InvalidOperationException(
                $"Move '{request.Move.MoveId}' source returned an invalid transfer page.");
        }
    }

    private async Task<StorageSlotMoveProgressSnapshot> PersistMovePhaseAsync(
        StorageSlotMoveIntent move,
        SearchableStorageSlotMovePhase phase,
        MoveParticipants participants)
    {
        var candidate = _state.State.Copy();
        var candidateMove = candidate.MoveIntent
            ?? throw new InvalidOperationException("The active move intent disappeared while advancing.");
        EnsureSameMove(candidateMove, move);
        CaptureMoveCounters(candidateMove, participants);
        candidateMove.Phase = phase;
        await PersistAsync(candidate);
        return CreateProgress(candidateMove, candidate.Epoch);
    }

    private async Task<StorageSlotMoveProgressSnapshot> CreateMoveProgressAsync(
        StorageSlotMoveIntent move)
    {
        var participants = await ReadParticipantsAsync(move);
        var snapshot = move.Copy();
        CaptureMoveCounters(snapshot, participants);
        return CreateProgress(snapshot, _state.State.Epoch);
    }

    private static void CaptureMoveCounters(
        StorageSlotMoveIntent move,
        MoveParticipants participants)
    {
        var target = participants.Target.MoveControl;
        if (target.IsPresent
            && target.MoveId == move.MoveId
            && target.Role == StoragePartitionMoveRole.Target)
        {
            move.ExportedRecordCount = Math.Max(
                move.ExportedRecordCount,
                target.ImportedRecordCount);
            move.ExportedByteCount = Math.Max(
                move.ExportedByteCount,
                target.ImportedByteCount);
        }

        var source = participants.Source.MoveControl;
        if (source.IsPresent
            && source.MoveId == move.MoveId
            && source.Role == StoragePartitionMoveRole.Source)
        {
            move.DeletedRecordCount = Math.Max(
                move.DeletedRecordCount,
                source.DeletedRecordCount);
            move.DeletedByteCount = Math.Max(
                move.DeletedByteCount,
                source.DeletedByteCount);
        }
    }

    private async Task<StorageSlotMoveProgressSnapshot> CompleteMoveAsync(
        StorageSlotMoveIntent move,
        SearchableStorageSlotMovePhase terminalPhase)
    {
        if (terminalPhase is not SearchableStorageSlotMovePhase.Completed
            and not SearchableStorageSlotMovePhase.Aborted)
        {
            throw new ArgumentOutOfRangeException(nameof(terminalPhase));
        }

        var candidate = _state.State.Copy();
        var candidateMove = candidate.MoveIntent
            ?? throw new InvalidOperationException("The active move intent disappeared before completion.");
        EnsureSameMove(candidateMove, move);
        var completionEpoch = terminalPhase == SearchableStorageSlotMovePhase.Completed
            ? checked(move.SourceEpoch + 1)
            : move.SourceEpoch;
        candidate.LastMoveReceipt = new StorageSlotMoveReceipt
        {
            MoveId = candidateMove.MoveId,
            Slot = candidateMove.Slot,
            SourceOwner = candidateMove.SourceOwner,
            TargetOwner = candidateMove.TargetOwner,
            SourceEpoch = candidateMove.SourceEpoch,
            CompletionEpoch = completionEpoch,
            TerminalPhase = terminalPhase,
            ExportedRecordCount = candidateMove.ExportedRecordCount,
            ExportedByteCount = candidateMove.ExportedByteCount,
            DeletedRecordCount = candidateMove.DeletedRecordCount,
            DeletedByteCount = candidateMove.DeletedByteCount,
        };
        candidate.MoveIntent = null;
        await PersistAsync(candidate);
        return CreateTerminalProgress(candidate.LastMoveReceipt, candidate.Epoch);
    }

    private static void EnsureSameMove(
        StorageSlotMoveIntent candidate,
        StorageSlotMoveIntent expected)
    {
        if (candidate.MoveId != expected.MoveId)
        {
            throw new InvalidOperationException(
                $"Move '{expected.MoveId}' is no longer the active layout intent.");
        }
    }

    private static StorageSlotMoveProgressSnapshot CreateProgress(
        StorageSlotMoveIntent move,
        long currentEpoch)
    {
        return new StorageSlotMoveProgressSnapshot
        {
            Intent = move.Copy(),
            CurrentEpoch = currentEpoch,
            ExportedRecordCount = move.ExportedRecordCount,
            ExportedByteCount = move.ExportedByteCount,
            DeletedRecordCount = move.DeletedRecordCount,
            DeletedByteCount = move.DeletedByteCount,
        };
    }

    private static StorageSlotMoveProgressSnapshot CreateTerminalProgress(
        StorageSlotMoveReceipt receipt,
        long currentEpoch)
    {
        var terminal = new StorageSlotMoveIntent
        {
            MoveId = receipt.MoveId,
            Slot = receipt.Slot,
            SourceOwner = receipt.SourceOwner,
            TargetOwner = receipt.TargetOwner,
            SourceEpoch = receipt.SourceEpoch,
            Phase = receipt.TerminalPhase,
            TransferPageRecordLimit = SearchableStorageMovementOptions.DefaultTransferPageRecordLimit,
            TransferPageByteTarget = SearchableStorageMovementOptions.DefaultTransferPageByteTarget,
            ExportedRecordCount = receipt.ExportedRecordCount,
            ExportedByteCount = receipt.ExportedByteCount,
            DeletedRecordCount = receipt.DeletedRecordCount,
            DeletedByteCount = receipt.DeletedByteCount,
        };
        return CreateProgress(terminal, currentEpoch);
    }

    private readonly record struct MoveParticipants(
        StoragePartitionProtocolState Source,
        StoragePartitionProtocolState Target);

    private static StorageLayoutState CreateLegacyState(StorageLayoutDescriptor descriptor)
    {
        return new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.LegacyFormatVersion,
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
            FormatVersion = StorageLayout.MovementFormatVersion,
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
        if (descriptor.FormatVersion == StorageLayout.LegacyFormatVersion)
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
            StorageLayout.LegacyFormatVersion);
        StoragePersistence.ValidateOptions(
            descriptor.JournalSegmentCapacity,
            descriptor.MaximumJournalReplayEntries);
        StorageCapacityGuardrails.ValidatePersistenceConfiguration(
            descriptor.JournalSegmentCapacity,
            descriptor.MaximumJournalReplayEntries,
            nameof(descriptor.JournalSegmentCapacity),
            nameof(descriptor.MaximumJournalReplayEntries));
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
            StorageLayout.MovementFormatVersion);
        StoragePersistence.ValidateOptions(
            descriptor.JournalSegmentCapacity,
            descriptor.MaximumJournalReplayEntries);
        StorageCapacityGuardrails.ValidatePersistenceConfiguration(
            descriptor.JournalSegmentCapacity,
            descriptor.MaximumJournalReplayEntries,
            nameof(descriptor.JournalSegmentCapacity),
            nameof(descriptor.MaximumJournalReplayEntries));
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
        if (identity.FormatVersion is not StorageLayout.LegacyFormatVersion
            and not StorageLayout.MovementFormatVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(identity),
                identity.FormatVersion,
                $"Layout format version {StorageLayout.MovementFormatVersion} or placement-compatible version "
                + $"{StorageLayout.LegacyFormatVersion} is required.");
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
            StorageLayout.MovementFormatVersion);
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
            || _state.State.Epoch != 0
            || _state.State.IndexSchemaProtocolVersion != 0
            || _state.State.IndexSchemaEnablement is not null)
        {
            throw new InvalidOperationException(
                "The persisted version-3 layout contains unexpected virtual-routing state and cannot be migrated.");
        }
    }

    private void EnsureLegacyMatches(StorageLayoutDescriptor descriptor)
    {
        if (_state.State.FormatVersion != StorageLayout.LegacyFormatVersion)
        {
            ThrowLayoutMismatch(descriptor);
        }

        EnsureLegacyBaseMatches(descriptor);
        if (_state.State.VirtualSlotCount != 0
            || (_state.State.SlotAssignments is not null && _state.State.SlotAssignments.Length != 0)
            || _state.State.Epoch != 0
            || _state.State.IndexSchemaProtocolVersion != 0
            || _state.State.IndexSchemaEnablement is not null)
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
        if (_state.State.MovementProtocolVersion == 0
            && _state.State.MovementEnablement is null
            && _state.State.MoveIntent is null
            && _state.State.IndexSchemaProtocolVersion == 0
            && _state.State.IndexSchemaEnablement is null
            && _state.State.Epoch == InitialRoutingEpoch
            && string.Equals(_state.State.ProviderName, descriptor.ProviderName, StringComparison.Ordinal)
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
        if (_state.State.FormatVersion == StorageLayout.LegacyFormatVersion
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
        if (_state.State.MovementProtocolVersion == 0
            && _state.State.MovementEnablement is null
            && _state.State.MoveIntent is null
            && _state.State.IndexSchemaProtocolVersion == 0
            && _state.State.IndexSchemaEnablement is null
            && _state.State.Epoch == InitialRoutingEpoch
            && string.Equals(_state.State.ProviderName, identity.ProviderName, StringComparison.Ordinal)
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
            || !StorageLayout.IsRoutingFormatVersion(state.FormatVersion)
            || string.IsNullOrWhiteSpace(state.ProviderName)
            || !string.Equals(state.ProviderName, ProviderName, StringComparison.Ordinal)
            || state.PartitionCount <= 0
            || state.VirtualSlotCount < state.PartitionCount
            || state.VirtualSlotCount > StorageLayout.MaximumVirtualSlotCount
            || state.VirtualSlotCount % state.PartitionCount != 0
            || state.SlotAssignments is null
            || state.SlotAssignments.Length != state.VirtualSlotCount
            || state.Epoch <= 0)
        {
            throw new InvalidOperationException(
                "The persisted routing layout contains invalid format or assignment boundaries.");
        }

        StoragePersistence.ValidateOptions(
            state.JournalSegmentCapacity,
            state.MaximumJournalReplayEntries);
        StorageCapacityGuardrails.ValidatePersistenceConfiguration(
            state.JournalSegmentCapacity,
            state.MaximumJournalReplayEntries,
            nameof(state.JournalSegmentCapacity),
            nameof(state.MaximumJournalReplayEntries));
        if (state.IndexSchemaProtocolVersion is not 0
            and not StorageLayout.CurrentIndexSchemaProtocolVersion)
        {
            throw new InvalidOperationException(
                $"The persisted layout index-schema protocol version {state.IndexSchemaProtocolVersion} is not supported.");
        }

        if (state.FormatVersion == StorageLayout.MovementFormatVersion)
        {
            if (state.IndexSchemaProtocolVersion != 0 || state.IndexSchemaEnablement is not null)
            {
                throw new InvalidOperationException(
                    "A version-4 layout cannot contain managed index-schema capability state.");
            }
        }
        else
        {
            var schemaMaintenanceActive = state.IndexSchemaEnablement is not null
                && (state.IndexSchemaProtocolVersion is 0
                    or StorageLayout.CurrentIndexSchemaProtocolVersion);
            var schemaEnabled = state.IndexSchemaEnablement is null
                && state.IndexSchemaProtocolVersion
                    == StorageLayout.CurrentIndexSchemaProtocolVersion;
            if (!schemaMaintenanceActive && !schemaEnabled)
            {
                throw new InvalidOperationException(
                    "A version-5 layout must contain an active or published managed index-schema capability.");
            }
        }

        switch (state.MovementProtocolVersion)
        {
            case 0:
                ValidateMovementDisabledState(state);
                break;
            case StorageLayout.CurrentMovementProtocolVersion:
                ValidateMovementEnabledState(state);
                break;
            default:
                throw new InvalidOperationException(
                    $"The persisted layout movement protocol version {state.MovementProtocolVersion} is not supported.");
        }

        if (state.IndexSchemaEnablement is { } indexSchemaEnablement)
        {
            ValidateIndexSchemaEnablement(state, indexSchemaEnablement);
        }

        _routingStateValidated = true;
    }

    private static void ValidateIndexSchemaEnablement(
        StorageLayoutState state,
        StorageIndexSchemaEnableIntent enablement)
    {
        if (state.IndexSchemaProtocolVersion is not 0
                and not StorageLayout.CurrentIndexSchemaProtocolVersion
            || state.MovementEnablement is not null
            || state.MoveIntent is not null
            || enablement.EnablementId == Guid.Empty
            || enablement.ProtocolVersion != StorageLayout.CurrentIndexSchemaProtocolVersion
            || enablement.LayoutEpoch != state.Epoch
            || enablement.LayoutFingerprint is null
            || !StorageLayoutFingerprint.Equals(
                enablement.LayoutFingerprint,
                StorageLayoutFingerprint.Compute(StorageLayoutSnapshot.FromState(state))))
        {
            throw new InvalidOperationException(
                "The persisted index-schema enablement intent contains invalid routing boundaries.");
        }
    }

    private static void ValidateMovementDisabledState(StorageLayoutState state)
    {
        if (state.Epoch != InitialRoutingEpoch
            || state.MoveIntent is not null
            || state.LastMoveReceipt is not null)
        {
            throw new InvalidOperationException(
                "A movement-disabled version-4 layout must retain the epoch-one identity boundary.");
        }

        for (var slot = 0; slot < state.SlotAssignments.Length; slot++)
        {
            if (state.SlotAssignments[slot] != slot % state.PartitionCount)
            {
                throw new InvalidOperationException(
                    "A movement-disabled version-4 layout must contain the zero-movement identity assignment.");
            }
        }

        if (state.MovementEnablement is not null)
        {
            ValidateMovementEnablement(state, state.MovementEnablement);
        }
    }

    private static void ValidateMovementEnabledState(StorageLayoutState state)
    {
        if (state.Epoch <= InitialRoutingEpoch || state.MovementEnablement is not null)
        {
            throw new InvalidOperationException(
                "A movement-enabled layout requires an advanced epoch and no enablement intent.");
        }

        foreach (var owner in state.SlotAssignments)
        {
            if (owner < 0 || owner >= StorageLayout.MaximumVirtualSlotCount)
            {
                throw new InvalidOperationException(
                    "A movement-enabled layout contains an out-of-range physical owner.");
            }
        }

        if (state.MoveIntent is not null)
        {
            ValidateMoveIntent(state, state.MoveIntent);
        }

        if (state.LastMoveReceipt is not null)
        {
            ValidateMoveReceipt(state, state.LastMoveReceipt);
        }
    }

    private static void ValidateMovementEnablement(
        StorageLayoutState state,
        StorageMovementEnableIntent enablement)
    {
        if (enablement.EnablementId == Guid.Empty
            || enablement.SourceEpoch != state.Epoch
            || enablement.PlannedEpoch != checked(enablement.SourceEpoch + 1)
            || enablement.Owners is null
            || enablement.Owners.Length == 0
            || enablement.NextOwnerIndex < 0
            || enablement.NextOwnerIndex > enablement.Owners.Length)
        {
            throw new InvalidOperationException(
                "The persisted movement-enablement intent contains invalid progress boundaries.");
        }

        var expectedOwners = state.SlotAssignments.Distinct().Order().ToArray();
        if (!enablement.Owners.SequenceEqual(expectedOwners))
        {
            throw new InvalidOperationException(
                "The persisted movement-enablement owner sweep does not match the current layout.");
        }
    }

    private static void ValidateMoveIntent(StorageLayoutState state, StorageSlotMoveIntent move)
    {
        if (move.MoveId == Guid.Empty
            || move.Slot < 0
            || move.Slot >= state.VirtualSlotCount
            || move.SourceOwner < 0
            || move.SourceOwner >= StorageLayout.MaximumVirtualSlotCount
            || move.TargetOwner < 0
            || move.TargetOwner >= StorageLayout.MaximumVirtualSlotCount
            || move.SourceOwner == move.TargetOwner
            || move.SourceEpoch <= InitialRoutingEpoch
            || move.ExportedRecordCount < 0
            || move.ExportedByteCount < 0
            || move.DeletedRecordCount < 0
            || move.DeletedByteCount < 0
            || !Enum.IsDefined(move.Phase)
            || move.Phase is SearchableStorageSlotMovePhase.Completed
                or SearchableStorageSlotMovePhase.Aborted)
        {
            throw new InvalidOperationException("The persisted slot-move intent is invalid.");
        }

        StorageMoveProtocol.ValidatePageLimits(
            move.TransferPageRecordLimit,
            move.TransferPageByteTarget,
            nameof(move));

        var ownershipCommitted = move.Phase is >= SearchableStorageSlotMovePhase.OwnershipCommitted
            and < SearchableStorageSlotMovePhase.Aborting;
        var expectedEpoch = ownershipCommitted
            ? checked(move.SourceEpoch + 1)
            : move.SourceEpoch;
        var expectedOwner = ownershipCommitted ? move.TargetOwner : move.SourceOwner;
        if (state.Epoch != expectedEpoch || state.SlotAssignments[move.Slot] != expectedOwner)
        {
            throw new InvalidOperationException(
                "The persisted slot-move phase does not match its authoritative assignment boundary.");
        }
    }

    private static void ValidateMoveReceipt(StorageLayoutState state, StorageSlotMoveReceipt receipt)
    {
        if (receipt.MoveId == Guid.Empty
            || receipt.Slot < 0
            || receipt.Slot >= state.VirtualSlotCount
            || receipt.SourceOwner < 0
            || receipt.SourceOwner >= StorageLayout.MaximumVirtualSlotCount
            || receipt.TargetOwner < 0
            || receipt.TargetOwner >= StorageLayout.MaximumVirtualSlotCount
            || receipt.SourceOwner == receipt.TargetOwner
            || receipt.SourceEpoch <= InitialRoutingEpoch
            || receipt.TerminalPhase is not SearchableStorageSlotMovePhase.Completed
                and not SearchableStorageSlotMovePhase.Aborted
            || receipt.CompletionEpoch != (receipt.TerminalPhase == SearchableStorageSlotMovePhase.Completed
                ? checked(receipt.SourceEpoch + 1)
                : receipt.SourceEpoch)
            || receipt.CompletionEpoch != (state.MoveIntent is null
                ? state.Epoch
                : state.MoveIntent.SourceEpoch)
            || receipt.ExportedRecordCount < 0
            || receipt.ExportedByteCount < 0
            || receipt.DeletedRecordCount < 0
            || receipt.DeletedByteCount < 0)
        {
            throw new InvalidOperationException("The persisted terminal slot-move receipt is invalid.");
        }
    }

    private async Task PersistAsync(StorageLayoutState candidate)
    {
        var previous = _state.State;
        var previousSnapshot = _routingSnapshot;
        var previousValidated = _routingStateValidated;
        if (previous.Initialized && StorageLayout.IsRoutingFormatVersion(previous.FormatVersion))
        {
            ValidateRoutingState();
            previousSnapshot ??= StorageLayoutSnapshot.FromState(previous);
            _routingSnapshot = previousSnapshot;
        }

        _durableLayoutInitializedDuringWrite = previous.Initialized;
        _durableLayoutFormatVersionDuringWrite = previous.FormatVersion;
        _layoutWriteInProgress = true;
        _state.State = candidate;
        try
        {
            // The physical provider ETag makes the single layout document the routing commit point.
            await _state.WriteStateAsync();
            _routingSnapshot = candidate.Initialized
                && StorageLayout.IsRoutingFormatVersion(candidate.FormatVersion)
                    ? StorageLayoutSnapshot.FromState(candidate)
                    : null;
            _routingStateValidated = false;
        }
        catch
        {
            // A provider may commit before losing the acknowledgement. Restored in-memory state
            // must never participate in another compare-and-swap on this activation.
            _state.State = previous;
            _routingSnapshot = previousSnapshot;
            _routingStateValidated = previousValidated;
            PoisonActivation();
            throw;
        }
        finally
        {
            _layoutWriteInProgress = false;
            _durableLayoutInitializedDuringWrite = false;
            _durableLayoutFormatVersionDuringWrite = 0;
        }
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
