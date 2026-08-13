using Microsoft.Extensions.Logging;
using Orleans.SearchableStorage.Diagnostics;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage;

/// <summary>
/// Reads and administers managed index schemas, routing, and live movement through Orleans grains.
/// </summary>
public sealed class SearchableStorageAdminClient : ISearchableStorageAdminClient
{
    private readonly StorageLayoutCache _layoutCache;
    private readonly IStorageLayoutGrain? _layoutGrain;
    private readonly StorageLayoutIdentity? _layoutIdentity;
    private readonly int _transferPageRecordLimit;
    private readonly int _transferPageByteTarget;
    private readonly IGrainFactory? _grainFactory;
    private readonly string? _providerName;
    private readonly ILogger<SearchableStorageAdminClient>? _logger;

    /// <summary>
    /// Initializes a client for one searchable-storage provider.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to access layout and partition grains.</param>
    /// <param name="providerName">The searchable-storage provider name.</param>
    /// <param name="partitionCount">The provider's initial physical partition count.</param>
    /// <remarks>
    /// This direct constructor targets integrated <c>IGrainStorage</c> namespaces. Configure an
    /// index-only external client through <see cref="SearchableStorageSiloBuilderExtensions.AddSearchableIndex(Microsoft.Extensions.DependencyInjection.IServiceCollection, string, Action{SearchableStorageOptions}?)"/>
    /// and resolve its keyed admin client instead.
    /// </remarks>
    public SearchableStorageAdminClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount)
        : this(
            grainFactory,
            providerName,
            partitionCount,
            new SearchableStorageMovementOptions())
    {
    }

    /// <summary>
    /// Initializes a client for one searchable-storage provider with explicit movement limits.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to access layout and partition grains.</param>
    /// <param name="providerName">The searchable-storage provider name.</param>
    /// <param name="partitionCount">The provider's initial physical partition count.</param>
    /// <param name="movementOptions">The bounded transfer-page settings captured by planned moves.</param>
    /// <remarks>
    /// This direct constructor targets integrated <c>IGrainStorage</c> namespaces. Configure an
    /// index-only external client through <see cref="SearchableStorageSiloBuilderExtensions.AddSearchableIndex(Microsoft.Extensions.DependencyInjection.IServiceCollection, string, Action{SearchableStorageOptions}?)"/>
    /// and resolve its keyed admin client instead.
    /// </remarks>
    public SearchableStorageAdminClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount,
        SearchableStorageMovementOptions movementOptions)
        : this(
            grainFactory,
            providerName,
            partitionCount,
            movementOptions,
            StorageNamespaceMode.Integrated,
            logger: null)
    {
    }

    internal SearchableStorageAdminClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount,
        SearchableStorageMovementOptions movementOptions,
        StorageNamespaceMode namespaceMode,
        ILogger<SearchableStorageAdminClient>? logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(movementOptions);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);
        if (!Enum.IsDefined(namespaceMode))
        {
            throw new ArgumentOutOfRangeException(nameof(namespaceMode));
        }

        StorageMoveProtocol.ValidatePageLimits(
            movementOptions.TransferPageRecordLimit,
            movementOptions.TransferPageByteTarget,
            nameof(movementOptions));

        var layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var layoutIdentity = StorageLayout.CreateIdentity(
            providerName,
            partitionCount,
            namespaceMode);
        _layoutGrain = layoutGrain;
        _grainFactory = grainFactory;
        _providerName = providerName;
        _layoutIdentity = layoutIdentity;
        _layoutCache = new StorageLayoutCache(
            () => layoutGrain.GetLayoutAsync(layoutIdentity));
        _transferPageRecordLimit = movementOptions.TransferPageRecordLimit;
        _transferPageByteTarget = movementOptions.TransferPageByteTarget;
        _logger = logger;
    }

    internal SearchableStorageAdminClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount,
        SearchableStorageMovementOptions movementOptions,
        ILogger<SearchableStorageAdminClient>? logger)
        : this(
            grainFactory,
            providerName,
            partitionCount,
            movementOptions,
            StorageNamespaceMode.Integrated,
            logger)
    {
    }

    internal SearchableStorageAdminClient(StorageLayoutCache layoutCache)
    {
        ArgumentNullException.ThrowIfNull(layoutCache);
        _layoutCache = layoutCache;
        _transferPageRecordLimit = StorageMoveProtocol.DefaultPageRecords;
        _transferPageByteTarget = StorageMoveProtocol.DefaultPageBytes;
    }

    internal SearchableStorageAdminClient(
        IStorageLayoutGrain layoutGrain,
        StorageLayoutIdentity layoutIdentity,
        SearchableStorageMovementOptions movementOptions)
    {
        ArgumentNullException.ThrowIfNull(layoutGrain);
        ArgumentNullException.ThrowIfNull(layoutIdentity);
        ArgumentNullException.ThrowIfNull(movementOptions);
        ArgumentException.ThrowIfNullOrWhiteSpace(layoutIdentity.ProviderName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(layoutIdentity.PartitionCount);
        StorageMoveProtocol.ValidatePageLimits(
            movementOptions.TransferPageRecordLimit,
            movementOptions.TransferPageByteTarget,
            nameof(movementOptions));

        _layoutGrain = layoutGrain;
        _layoutIdentity = layoutIdentity;
        _layoutCache = new StorageLayoutCache(
            () => layoutGrain.GetLayoutAsync(layoutIdentity));
        _providerName = layoutIdentity.ProviderName;
        _transferPageRecordLimit = movementOptions.TransferPageRecordLimit;
        _transferPageByteTarget = movementOptions.TransferPageByteTarget;
    }

    /// <inheritdoc />
    public async Task<SearchableStorageLayout?> GetLayoutAsync(
        CancellationToken cancellationToken = default)
    {
        var layout = await GetLayoutSnapshotAsync(cancellationToken);
        if (layout is null)
        {
            return null;
        }

        var activeMove = CreateSnapshotProgress(layout);

        return CreatePublicLayout(layout, activeMove);
    }

    /// <inheritdoc />
    public Task<SearchableStorageIndexSchemaStatus> GetIndexSchemaAsync<TState>(
        string stateName,
        CancellationToken cancellationToken = default)
    {
        return GetIndexSchemaAsync<TState>(
            stateName,
            applicationSchemaVersion: 1,
            cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task<SearchableStorageIndexSchemaStatus> GetIndexSchemaAsync<TState>(
        string stateName,
        int applicationSchemaVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applicationSchemaVersion);
        cancellationToken.ThrowIfCancellationRequested();
        var (grain, request) = GetSchemaControl<TState>(
            stateName,
            applicationSchemaVersion);
        var snapshot = await WaitForCallAsync(grain.GetAsync(request), cancellationToken);
        return CreatePublicSchemaStatus(snapshot, request.Fingerprint);
    }

    /// <inheritdoc />
    public Task<SearchableStorageIndexSchemaStatus> RebuildIndexSchemaAsync<TState>(
        string stateName,
        CancellationToken cancellationToken = default)
    {
        return RebuildIndexSchemaAsync<TState>(
            stateName,
            applicationSchemaVersion: 1,
            cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public Task<SearchableStorageIndexSchemaStatus> RebuildIndexSchemaAsync<TState>(
        string stateName,
        int applicationSchemaVersion,
        CancellationToken cancellationToken = default)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            RequiredDiagnosticsProviderName,
            "schema.rebuild",
            "orchestrate",
            _logger,
            lifecycle: true,
            () => RebuildIndexSchemaCoreAsync<TState>(
                stateName,
                applicationSchemaVersion,
                cancellationToken),
            static result => result.ProcessedRecordCount);
    }

    private async Task<SearchableStorageIndexSchemaStatus> RebuildIndexSchemaCoreAsync<TState>(
        string stateName,
        int applicationSchemaVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applicationSchemaVersion);
        cancellationToken.ThrowIfCancellationRequested();
        var (grain, request) = GetSchemaControl<TState>(
            stateName,
            applicationSchemaVersion);
        var snapshot = await WaitForCallAsync(grain.BeginRebuildAsync(request), cancellationToken);
        while (snapshot.Rebuild is { } rebuild)
        {
            cancellationToken.ThrowIfCancellationRequested();
            snapshot = await WaitForCallAsync(
                grain.AdvanceRebuildAsync(new StorageIndexSchemaCommand
                {
                    Schema = request,
                    RebuildId = rebuild.RebuildId,
                }),
                cancellationToken);
        }

        var result = CreatePublicSchemaStatus(snapshot, request.Fingerprint);
        if (result.State != SearchableStorageIndexSchemaState.Active)
        {
            throw new InvalidOperationException(
                "The index schema rebuild ended without activating its target fingerprint.");
        }

        return result;
    }

    /// <inheritdoc />
    public Task<SearchableStorageLayout> EnableMovementAsync(
        CancellationToken cancellationToken = default)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            RequiredDiagnosticsProviderName,
            "movement.enable",
            "orchestrate",
            _logger,
            lifecycle: true,
            () => EnableMovementCoreAsync(cancellationToken));
    }

    private async Task<SearchableStorageLayout> EnableMovementCoreAsync(
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var layoutGrain = GetRequiredLayoutGrain();
        var layout = await WaitForCallAsync(
            layoutGrain.BeginMovementEnablementAsync(),
            cancellationToken);
        while (layout.MovementState == SearchableStorageMovementState.Enabling)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var enablement = layout.CopyMovementEnablement()
                ?? throw new InvalidOperationException(
                    "An enabling layout omitted its durable enablement identity.");
            layout = await WaitForCallAsync(
                layoutGrain.AdvanceMovementEnablementAsync(enablement.EnablementId),
                cancellationToken);
        }

        if (layout.MovementState != SearchableStorageMovementState.Enabled)
        {
            throw new InvalidOperationException(
                "The storage layout did not converge to the enabled movement protocol.");
        }

        var activeMove = CreateSnapshotProgress(layout);
        return CreatePublicLayout(layout, activeMove);
    }

    /// <inheritdoc />
    public async Task<SearchableStorageSlotMoveProgress> PlanMoveAsync(
        int slot,
        int targetPartitionIndex,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(slot);
        StorageLayout.ValidateOwnerIndex(targetPartitionIndex, nameof(targetPartitionIndex));
        cancellationToken.ThrowIfCancellationRequested();
        var progress = await WaitForCallAsync(
            GetRequiredLayoutGrain().PlanMoveAsync(new StorageSlotMovePlanRequest
            {
                Slot = slot,
                TargetOwner = targetPartitionIndex,
                MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
                TransferPageRecordLimit = _transferPageRecordLimit,
                TransferPageByteTarget = _transferPageByteTarget,
            }),
            cancellationToken);
        return CreatePublicProgress(progress);
    }

    /// <inheritdoc />
    public async Task<SearchableStorageSlotMoveProgress?> GetMoveAsync(
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var progress = await WaitForCallAsync(
            GetRequiredLayoutGrain().GetMoveProgressAsync(),
            cancellationToken);
        return progress is null ? null : CreatePublicProgress(progress);
    }

    /// <inheritdoc />
    public async Task<SearchableStorageSlotMoveProgress> AdvanceMoveAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        ValidateMoveId(moveId);
        cancellationToken.ThrowIfCancellationRequested();
        var progress = await WaitForCallAsync(
            GetRequiredLayoutGrain().AdvanceMoveAsync(CreateMoveCommand(moveId)),
            cancellationToken);
        return CreatePublicProgress(progress);
    }

    /// <inheritdoc />
    public Task<SearchableStorageSlotMoveProgress> ExecuteMoveAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            RequiredDiagnosticsProviderName,
            "movement.execute",
            "orchestrate",
            _logger,
            lifecycle: true,
            () => ExecuteMoveCoreAsync(moveId, cancellationToken),
            static progress => Math.Max(
                progress.ExportedRecordCount,
                progress.DeletedRecordCount));
    }

    private async Task<SearchableStorageSlotMoveProgress> ExecuteMoveCoreAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        ValidateMoveId(moveId);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var progress = await AdvanceMoveAsync(moveId, cancellationToken);
            if (progress.IsComplete)
            {
                return progress;
            }
        }
    }

    /// <inheritdoc />
    public Task<SearchableStorageSlotMoveProgress> AbortMoveAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            RequiredDiagnosticsProviderName,
            "movement.abort",
            "orchestrate",
            _logger,
            lifecycle: true,
            () => AbortMoveCoreAsync(moveId, cancellationToken),
            static progress => Math.Max(
                progress.ExportedRecordCount,
                progress.DeletedRecordCount));
    }

    private async Task<SearchableStorageSlotMoveProgress> AbortMoveCoreAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        ValidateMoveId(moveId);
        cancellationToken.ThrowIfCancellationRequested();
        var progress = CreatePublicProgress(await WaitForCallAsync(
            GetRequiredLayoutGrain().RequestMoveAbortAsync(CreateMoveCommand(moveId)),
            cancellationToken));
        while (!progress.IsComplete)
        {
            cancellationToken.ThrowIfCancellationRequested();
            progress = await AdvanceMoveAsync(moveId, cancellationToken);
        }

        return progress;
    }

    /// <inheritdoc />
    public async Task<SearchableStorageRebalancePlan> PlanRebalanceAsync(
        int targetPartitionCount,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(targetPartitionCount);
        cancellationToken.ThrowIfCancellationRequested();
        var layout = await GetLayoutSnapshotAsync(cancellationToken)
            ?? throw new InvalidOperationException(
                "A storage layout must be initialized before a rebalance can be planned.");
        if (layout.MovementState != SearchableStorageMovementState.Enabled)
        {
            var state = layout.MovementState == SearchableStorageMovementState.Enabling
                ? "still enabling"
                : "disabled";
            throw new InvalidOperationException(
                $"Live virtual-slot movement is {state}; complete EnableMovementAsync before planning a rebalance.");
        }

        if (targetPartitionCount > layout.VirtualSlotCount
            || targetPartitionCount > StorageLayout.MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(targetPartitionCount),
                targetPartitionCount,
                $"A rebalance target must be between 1 and the persisted virtual-slot count {layout.VirtualSlotCount}.");
        }

        var activeMove = CreateSnapshotProgress(layout);
        return CreateRebalancePlan(layout, targetPartitionCount, activeMove);
    }

    /// <inheritdoc />
    public async Task<SearchableStorageRebalancePlan> ExecuteRebalanceAsync(
        int targetPartitionCount,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var plan = await PlanRebalanceAsync(targetPartitionCount, cancellationToken);
            if (plan.ActiveMove is not null)
            {
                _ = await ExecuteMoveAsync(plan.ActiveMove.MoveId, cancellationToken);
                continue;
            }

            if (plan.NextMove is null)
            {
                return plan;
            }

            var move = await PlanMoveAsync(
                plan.NextMove.Slot,
                plan.NextMove.TargetPartitionIndex,
                cancellationToken);
            _ = await ExecuteMoveAsync(move.MoveId, cancellationToken);
        }
    }

    private async Task<StorageLayoutSnapshot?> GetLayoutSnapshotAsync(
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _layoutGrain is null
            ? await _layoutCache.GetAsync(cancellationToken)
            : await WaitForCallAsync(
                _layoutGrain.GetLayoutAsync(_layoutIdentity!),
                cancellationToken);
    }

    private static SearchableStorageLayout CreatePublicLayout(
        StorageLayoutSnapshot layout,
        SearchableStorageSlotMoveProgress? activeMove)
    {
        ArgumentNullException.ThrowIfNull(layout);

        var assignments = layout.CopySlotAssignments();
        var partitions = Array.AsReadOnly(assignments
            .GroupBy(static owner => owner)
            .OrderBy(static group => group.Key)
            .Select(static group => new SearchableStoragePartitionLayout
            {
                PartitionIndex = group.Key,
                SlotCount = group.Count(),
            })
            .ToArray());

        return new SearchableStorageLayout
        {
            Epoch = layout.Epoch,
            InitialPartitionCount = layout.InitialPartitionCount,
            VirtualSlotCount = layout.VirtualSlotCount,
            Partitions = partitions,
            MovementProtocolVersion = layout.MovementProtocolVersion,
            IndexSchemaProtocolVersion = layout.IndexSchemaProtocolVersion,
            MovementState = layout.MovementState,
            ActiveMove = activeMove,
        };
    }

    private static SearchableStorageSlotMoveProgress CreatePublicProgress(
        StorageSlotMoveProgressSnapshot progress)
    {
        ArgumentNullException.ThrowIfNull(progress);
        ArgumentNullException.ThrowIfNull(progress.Intent);
        var intent = progress.Intent;
        var isComplete = intent.Phase is SearchableStorageSlotMovePhase.Completed
            or SearchableStorageSlotMovePhase.Aborted;
        return new SearchableStorageSlotMoveProgress
        {
            MoveId = intent.MoveId,
            Slot = intent.Slot,
            SourcePartitionIndex = intent.SourceOwner,
            TargetPartitionIndex = intent.TargetOwner,
            SourceEpoch = intent.SourceEpoch,
            CurrentEpoch = progress.CurrentEpoch,
            Phase = intent.Phase,
            ExportedRecordCount = progress.ExportedRecordCount,
            ExportedByteCount = progress.ExportedByteCount,
            DeletedRecordCount = progress.DeletedRecordCount,
            DeletedByteCount = progress.DeletedByteCount,
            CanAbort = intent.Phase <= SearchableStorageSlotMovePhase.CopyComplete
                || intent.Phase == SearchableStorageSlotMovePhase.Aborting,
            IsComplete = isComplete,
        };
    }

    private static SearchableStorageSlotMoveProgress? CreateSnapshotProgress(
        StorageLayoutSnapshot layout)
    {
        var intent = layout.CopyMoveIntent();
        if (intent is null)
        {
            return null;
        }

        return CreatePublicProgress(new StorageSlotMoveProgressSnapshot
        {
            Intent = intent,
            CurrentEpoch = layout.Epoch,
            ExportedRecordCount = intent.ExportedRecordCount,
            ExportedByteCount = intent.ExportedByteCount,
            DeletedRecordCount = intent.DeletedRecordCount,
            DeletedByteCount = intent.DeletedByteCount,
        });
    }

    private static SearchableStorageRebalancePlan CreateRebalancePlan(
        StorageLayoutSnapshot layout,
        int targetPartitionCount,
        SearchableStorageSlotMoveProgress? activeMove)
    {
        var assignments = layout.CopySlotAssignments();
        var activeOwnershipCommitPending = activeMove is not null
            && activeMove.Phase <= SearchableStorageSlotMovePhase.CopyComplete;
        if (activeOwnershipCommitPending)
        {
            assignments[activeMove!.Slot] = activeMove.TargetPartitionIndex;
        }

        var quotas = new int[targetPartitionCount];
        var retained = new int[targetPartitionCount];
        var baseQuota = assignments.Length / targetPartitionCount;
        var remainder = assignments.Length % targetPartitionCount;
        var currentOwnerCounts = new int[targetPartitionCount];
        foreach (var owner in assignments)
        {
            if (owner < targetPartitionCount)
            {
                currentOwnerCounts[owner]++;
            }
        }

        // A balanced layout has `remainder` owners at base+1. Assign those extra slots to
        // owners which can actually retain one more current slot; this maximizes retained
        // ownership (and therefore minimizes moves). Owner index is the stable tie-break.
        var extraQuotaOwners = Enumerable.Range(0, targetPartitionCount)
            .OrderByDescending(owner => currentOwnerCounts[owner] > baseQuota)
            .ThenBy(static owner => owner)
            .Take(remainder)
            .ToHashSet();
        for (var owner = 0; owner < quotas.Length; owner++)
        {
            quotas[owner] = baseQuota + (extraQuotaOwners.Contains(owner) ? 1 : 0);
        }

        var excess = new List<(int Slot, int SourceOwner)>();
        for (var slot = 0; slot < assignments.Length; slot++)
        {
            var owner = assignments[slot];
            if (owner < targetPartitionCount && retained[owner] < quotas[owner])
            {
                retained[owner]++;
            }
            else
            {
                excess.Add((slot, owner));
            }
        }

        SearchableStorageSlotMovePlan? next = null;
        if (activeMove is null && excess.Count > 0)
        {
            var target = 0;
            while (target < quotas.Length && retained[target] >= quotas[target])
            {
                target++;
            }

            if (target >= quotas.Length)
            {
                throw new InvalidOperationException(
                    "The rebalance excess set has no corresponding target deficit.");
            }

            next = new SearchableStorageSlotMovePlan
            {
                Slot = excess[0].Slot,
                SourcePartitionIndex = excess[0].SourceOwner,
                TargetPartitionIndex = target,
            };
        }

        return new SearchableStorageRebalancePlan
        {
            Epoch = layout.Epoch,
            TargetPartitionCount = targetPartitionCount,
            RequiredMoveCount = checked(excess.Count + (activeOwnershipCommitPending ? 1 : 0)),
            NextMove = next,
            ActiveMove = activeMove,
        };
    }

    private IStorageLayoutGrain GetRequiredLayoutGrain()
    {
        return _layoutGrain
            ?? throw new InvalidOperationException(
                "Movement operations require an Orleans-backed searchable-storage admin client.");
    }

    private string RequiredDiagnosticsProviderName => _providerName ?? "unknown";

    private (IStorageIndexSchemaGrain Grain, StorageIndexSchemaRequest Request)
        GetSchemaControl<TState>(string stateName, int applicationSchemaVersion)
    {
        var grainFactory = _grainFactory
            ?? throw new InvalidOperationException(
                "Index-schema operations require an Orleans-backed searchable-storage admin client.");
        var providerName = _providerName
            ?? throw new InvalidOperationException("The searchable-storage provider is unavailable.");
        var definition = IndexMetadataProvider.GetSchemaDefinition<TState>(
            stateName,
            applicationSchemaVersion);
        var request = StorageIndexSchema.CreateRequest(providerName, definition);
        var grain = grainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(providerName, stateName));
        return (grain, request);
    }

    internal static SearchableStorageIndexSchemaStatus CreatePublicSchemaStatus(
        StorageIndexSchemaSnapshot snapshot,
        byte[] configuredFingerprint)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(configuredFingerprint);
        if (snapshot.Rebuild is { } rebuild)
        {
            return new SearchableStorageIndexSchemaStatus
            {
                StateName = snapshot.StateName,
                State = SearchableStorageIndexSchemaState.Rebuilding,
                RebuildId = rebuild.RebuildId,
                RebuildPhase = CreatePublicRebuildPhase(rebuild),
                TotalOwnerCount = rebuild.OwnerCount,
                SchemaEnabledOwnerCount = rebuild.NextProtocolOwnerIndex,
                ScannedOwnerCount = rebuild.NextOwnerIndex,
                ProcessedRecordCount = rebuild.ProcessedRecordCount,
                Fingerprint = Convert.ToHexString(rebuild.TargetFingerprint),
            };
        }

        if (snapshot.ActiveFingerprint is null)
        {
            return new SearchableStorageIndexSchemaStatus
            {
                StateName = snapshot.StateName,
                State = SearchableStorageIndexSchemaState.Uninitialized,
                ProcessedRecordCount = 0,
            };
        }

        if (!IndexSchemaIdentity.FixedTimeEquals(
                snapshot.ActiveFingerprint,
                configuredFingerprint))
        {
            throw new SearchableStorageIndexSchemaException(
                $"The active index schema for state '{snapshot.StateName}' does not match the "
                + "registered schema declaration (state type, index metadata, or application "
                + "schema version). Keep traffic quiesced and rebuild the registered schema.");
        }

        return new SearchableStorageIndexSchemaStatus
        {
            StateName = snapshot.StateName,
            State = SearchableStorageIndexSchemaState.Active,
            ProcessedRecordCount = snapshot.LastCompletedRecordCount,
            Fingerprint = Convert.ToHexString(snapshot.ActiveFingerprint),
        };
    }

    private static SearchableStorageIndexSchemaRebuildPhase CreatePublicRebuildPhase(
        StorageIndexSchemaRebuildIntent rebuild)
    {
        if (rebuild.NextProtocolOwnerIndex < rebuild.OwnerCount)
        {
            return SearchableStorageIndexSchemaRebuildPhase.EnablingOwners;
        }

        if (rebuild.NextOwnerIndex < rebuild.OwnerCount)
        {
            return SearchableStorageIndexSchemaRebuildPhase.ScanningRecords;
        }

        return SearchableStorageIndexSchemaRebuildPhase.ActivatingGeneration;
    }

    private static StorageSlotMoveCommand CreateMoveCommand(Guid moveId)
    {
        return new StorageSlotMoveCommand
        {
            MoveId = moveId,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
        };
    }

    private static void ValidateMoveId(Guid moveId)
    {
        if (moveId == Guid.Empty)
        {
            throw new ArgumentException("A move id must not be empty.", nameof(moveId));
        }
    }

    private static async Task<T> WaitForCallAsync<T>(
        Task<T> call,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        try
        {
            return await call.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ = ObserveCompletionAsync(call);
            throw;
        }
    }

    private static async Task ObserveCompletionAsync(Task call)
    {
        await call.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
