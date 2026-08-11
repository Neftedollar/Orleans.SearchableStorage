using System.Globalization;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StoragePartitionGrain : Grain, IStoragePartitionGrain
{
    private readonly ILogger<StoragePartitionGrain> _logger;
    private readonly StorageLayoutCacheRegistry _layoutCaches;
    private readonly IPersistentState<StoragePartitionManifestState> _manifest;
    private StorageLayoutCache? _routingCache;
    private StoragePartitionView _view = new(
        new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
    private StoragePartitionPersistence? _persistence;
    private string _providerName = string.Empty;
    private int _partitionIndex = -1;
    private bool _usable;

    public StoragePartitionGrain(
        [PersistentState("manifest", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StoragePartitionManifestState> manifest,
        ILogger<StoragePartitionGrain> logger,
        StorageLayoutCacheRegistry layoutCaches)
    {
        _manifest = manifest;
        _logger = logger;
        _layoutCaches = layoutCaches;
    }

    private StoragePartitionPersistence Persistence => _persistence
        ?? throw new InvalidOperationException("The storage partition has not completed activation.");

    private StorageLayoutCache RoutingCache => _routingCache
        ?? throw new InvalidOperationException("The storage partition routing layout has not been initialized.");

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var partitionKey = this.GetPrimaryKeyString();
        (_providerName, _partitionIndex) = ParsePartitionKey(partitionKey);
        _routingCache = _layoutCaches.Get(_providerName);
        _persistence = new StoragePartitionPersistence(
            _manifest,
            GrainFactory,
            partitionKey,
            PoisonActivation,
            _logger);
        var records = await _persistence.ActivateAsync();
        if (_persistence.RoutedOperationsRequired)
        {
            var routing = await GetRequiredRoutingSnapshotAsync();
            var move = _persistence.MoveControl;
            if (move.IsPresent && move.VirtualSlotCount != routing.VirtualSlotCount)
            {
                throw new InvalidOperationException(
                    "The durable partition move control does not match the immutable virtual-slot layout.");
            }

            _view = new StoragePartitionView(records, routing.VirtualSlotCount);
        }
        else
        {
            // Persistence-v3 activation remains independent of routing so a rolling deployment
            // can recover legacy partitions before the layout is initialized or upgraded. Routed
            // calls hydrate routing lazily; the explicit protocol gate builds the slot catalog.
            _view = new StoragePartitionView(records);
        }

        _usable = true;
        await base.OnActivateAsync(cancellationToken);
    }

    public Task<StorageReadResult> ReadAsync(string recordKey)
    {
        EnsureUsable();
        EnsureLegacyOperationAllowed();
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);

        return Task.FromResult(ReadCore(recordKey));
    }

    private StorageReadResult ReadCore(string recordKey)
    {
        if (!_view.Records.TryGetValue(recordKey, out var record))
        {
            return new StorageReadResult { Found = false };
        }

        return new StorageReadResult
        {
            Found = true,
            Payload = [.. record.Payload],
            ETag = record.ETag,
        };
    }

    public async Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RecordKey);

        var snapshot = await ValidatePointRouteAsync(request.Slot, request.Epoch);
        ValidateRoutedRecordIdentity(
            request.RecordKey,
            request.GrainId,
            request.Slot,
            snapshot,
            nameof(request));
        return ReadCore(request.RecordKey);
    }

    public async Task<string> WriteAsync(StorageWriteRequest request)
    {
        EnsureUsable();
        EnsureLegacyOperationAllowed();
        return await WriteCoreAsync(request);
    }

    private async Task<string> WriteCoreAsync(StorageWriteRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RecordKey);
        ArgumentNullException.ThrowIfNull(request.Payload);
        ArgumentNullException.ThrowIfNull(request.IndexEntries);
        StoragePartitionPersistence.ValidateSettings(request.Persistence);
        Persistence.EnsureSettingsMatch(request.Persistence);

        _view.Records.TryGetValue(request.RecordKey, out var currentRecord);
        EnsureETagMatches(request.RecordKey, currentRecord?.ETag, request.ExpectedETag, "write");

        var nextVersion = Persistence.NextVersion;
        var etag = nextVersion.ToString(CultureInfo.InvariantCulture);
        var storedRecord = StoragePersistenceStateCopy.CopyRecord(new StoredRecord
        {
            GrainId = request.GrainId,
            Payload = request.Payload,
            ETag = etag,
            IndexEntries = request.IndexEntries,
        })!;
        StoragePartitionIndexes.ValidateRecord(storedRecord);

        await PrepareForMutationAsync(request.Persistence);
        var entry = new StorageJournalEntry
        {
            Sequence = Persistence.NextSequence,
            WriterEpoch = Persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = request.RecordKey,
            ExpectedETag = request.ExpectedETag,
            Record = storedRecord,
            NextVersionAfter = checked(nextVersion + 1),
        };

        await CommitAsync(entry);
        ApplyCommittedUpsert(request.RecordKey, storedRecord);
        await Persistence.CompactIfRequiredAsync(_view.Records, request.Persistence.CompactionThreshold);
        return etag;
    }

    public async Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Request);

        var snapshot = await ValidatePointRouteAsync(request.Slot, request.Epoch);
        ValidateRoutedRecordIdentity(
            request.Request.RecordKey,
            request.Request.GrainId,
            request.Slot,
            snapshot,
            nameof(request));

        EnsureSlotMutationAllowed(request.Slot);

        return await WriteCoreAsync(request.Request);
    }

    public async Task ClearAsync(StorageClearRequest request)
    {
        EnsureUsable();
        EnsureLegacyOperationAllowed();
        await ClearCoreAsync(request);
    }

    private async Task ClearCoreAsync(StorageClearRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RecordKey);
        StoragePartitionPersistence.ValidateSettings(request.Persistence);
        Persistence.EnsureSettingsMatch(request.Persistence);

        if (!_view.Records.TryGetValue(request.RecordKey, out var currentRecord))
        {
            EnsureETagMatches(request.RecordKey, null, request.ExpectedETag, "clear");
            return;
        }

        EnsureETagMatches(request.RecordKey, currentRecord.ETag, request.ExpectedETag, "clear");
        await PrepareForMutationAsync(request.Persistence);
        var entry = new StorageJournalEntry
        {
            Sequence = Persistence.NextSequence,
            WriterEpoch = Persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Delete,
            RecordKey = request.RecordKey,
            ExpectedETag = request.ExpectedETag,
            NextVersionAfter = Persistence.NextVersion,
        };

        await CommitAsync(entry);
        ApplyCommittedDelete(request.RecordKey);
        await Persistence.CompactIfRequiredAsync(_view.Records, request.Persistence.CompactionThreshold);
    }

    public async Task ClearRoutedAsync(RoutedStorageClearRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Request);

        // Route validation deliberately precedes the missing-record fast path in ClearAsync. A
        // stale owner must not acknowledge a clear while the authoritative record lives elsewhere.
        var snapshot = await ValidatePointRouteAsync(request.Slot, request.Epoch);
        ValidateRoutedRecordIdentity(
            request.Request.RecordKey,
            request.GrainId,
            request.Slot,
            snapshot,
            nameof(request));
        EnsureSlotMutationAllowed(request.Slot);
        await ClearCoreAsync(request.Request);
    }

    public Task<GrainId[]> FindAsync(ExactIndexQuery query)
    {
        EnsureUsable();
        EnsureLegacyOperationAllowed();
        ArgumentNullException.ThrowIfNull(query);
        return Task.FromResult(ResolveGrainIds(FindRecordKeys(query)));
    }

    public async Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(query.Query);

        var snapshot = await ValidateQueryRouteAsync(query.Epoch);
        return ResolveGrainIds(FindRecordKeys(query.Query), snapshot);
    }

    public Task<GrainId[]> RangeAsync(RangeIndexQuery query)
    {
        EnsureUsable();
        EnsureLegacyOperationAllowed();
        ArgumentNullException.ThrowIfNull(query);
        return Task.FromResult(ResolveGrainIds(FindRangeRecordKeys(query)));
    }

    public async Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(query.Query);

        var snapshot = await ValidateQueryRouteAsync(query.Epoch);
        return ResolveGrainIds(FindRangeRecordKeys(query.Query), snapshot);
    }

    public Task<GrainId[]> QueryAsync(PartitionQueryPlan query)
    {
        EnsureUsable();
        EnsureLegacyOperationAllowed();
        ArgumentNullException.ThrowIfNull(query);
        // StoragePartitionGrain is non-reentrant. Evaluating the complete plan synchronously in
        // this call gives AND and OR one serially consistent partition-local view.
        var recordKeys = StoragePartitionQueryEvaluator.Evaluate(query, _view.Indexes);
        return Task.FromResult(ResolveGrainIds(recordKeys));
    }

    public async Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(query.Query);
        QueryPlanValidator.Validate(query.Query);

        var snapshot = await ValidateQueryRouteAsync(query.Epoch);
        // Ownership filtering is part of the same non-reentrant call as plan evaluation, so the
        // result cannot mix two activation-local record views.
        return ResolveGrainIds(
            StoragePartitionQueryEvaluator.EvaluateValidated(query.Query, _view.Indexes),
            snapshot);
    }

    public async Task<PartitionQueryPageResult> QueryPageRoutedAsync(
        RoutedPartitionQueryPageRequest request)
    {
        EnsureUsable();
        ValidatePageRequest(request);

        // Recompute both bindings in the partition. Caller-supplied fingerprints are routing and
        // continuation assertions, never authoritative descriptions of the plan or layout.
        var queryFingerprint = QueryPlanFingerprint.Compute(request.StateName, request.Query);
        if (!QueryPlanFingerprint.Equals(queryFingerprint, request.QueryFingerprint))
        {
            throw new ArgumentException(
                "The partition query fingerprint does not match the state name and query plan.",
                nameof(request));
        }

        var snapshot = await ValidateQueryRouteAsync(request.Epoch);
        if (request.LayoutFormatVersion != snapshot.FormatVersion)
        {
            throw new ArgumentException(
                "The partition query layout format does not match the authoritative routing layout.",
                nameof(request));
        }

        var layoutFingerprint = StorageLayoutFingerprint.Compute(snapshot);
        if (!StorageLayoutFingerprint.Equals(layoutFingerprint, request.LayoutFingerprint))
        {
            throw new ArgumentException(
                "The partition query layout fingerprint does not match the authoritative routing layout.",
                nameof(request));
        }

        // The grain is non-reentrant. Candidate grouping, predicate probes, ownership filtering,
        // and frontier advancement therefore observe one serially consistent local view.
        return StoragePartitionQueryPageEvaluator.EvaluateValidated(
            request,
            _view,
            snapshot,
            _partitionIndex,
            queryFingerprint,
            layoutFingerprint);
    }

    public async Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
        RoutedPartitionDistinctFacetPageRequest request)
    {
        EnsureUsable();
        ValidateDistinctFacetPageRequest(request);
        var requestFingerprint = ValidateFacetFingerprint(request.StateName, request.Query, request.FacetScope, request.FacetKind, request.RequestFingerprint, request);
        var snapshot = await ValidateQueryRouteAsync(request.Epoch);
        ValidateFacetLayout(request.LayoutFormatVersion, request.LayoutFingerprint, snapshot, request);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(snapshot);
        var dataVersion = ValidateFacetDataVersion(request.HasExpectedDataVersion, request.ExpectedDataVersion);
        var result = StoragePartitionFacetEvaluator.EvaluateDistinctPageValidated(
            request,
            _view,
            snapshot,
            requestFingerprint,
            layoutFingerprint);
        result.DataVersion = dataVersion;
        return result;
    }

    public async Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
        RoutedPartitionFacetCandidatePageRequest request)
    {
        EnsureUsable();
        ValidateFacetCandidatePageRequest(request);
        var requestFingerprint = ValidateFacetFingerprint(request.StateName, request.Query, request.FacetScope, request.FacetKind, request.RequestFingerprint, request);
        var snapshot = await ValidateQueryRouteAsync(request.Epoch);
        ValidateFacetLayout(request.LayoutFormatVersion, request.LayoutFingerprint, snapshot, request);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(snapshot);
        var dataVersion = ValidateFacetDataVersion(request.HasExpectedDataVersion, request.ExpectedDataVersion);
        var result = StoragePartitionFacetEvaluator.EvaluateCandidatePageValidated(
            request,
            _view,
            snapshot,
            requestFingerprint,
            layoutFingerprint);
        result.DataVersion = dataVersion;
        return result;
    }

    public async Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
        RoutedPartitionFacetCountSliceRequest request)
    {
        EnsureUsable();
        ValidateFacetCountSliceRequest(request);
        var requestFingerprint = ValidateFacetFingerprint(request.StateName, request.Query, request.FacetScope, request.FacetKind, request.RequestFingerprint, request);
        var snapshot = await ValidateQueryRouteAsync(request.Epoch);
        ValidateFacetLayout(request.LayoutFormatVersion, request.LayoutFingerprint, snapshot, request);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(snapshot);
        var dataVersion = ValidateFacetDataVersion(request.HasExpectedDataVersion, request.ExpectedDataVersion);
        var result = StoragePartitionFacetEvaluator.EvaluateCountSliceValidated(
            request,
            _view,
            snapshot,
            _partitionIndex,
            requestFingerprint,
            layoutFingerprint);
        result.DataVersion = dataVersion;
        return result;
    }

    public async Task<StoragePartitionProtocolState> EnableMovementProtocolAsync(
        StoragePartitionProtocolRequest request)
    {
        EnsureUsable();
        ValidateProtocolRequest(request);
        var routing = await GetRequiredRoutingSnapshotAsync();
        if (routing.VirtualSlotCount != request.VirtualSlotCount
            || request.MinimumRoutingEpoch < routing.Epoch)
        {
            throw new ArgumentException(
                "The partition protocol request does not match the persisted routing layout.",
                nameof(request));
        }

        var settings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = request.JournalSegmentCapacity,
            MaximumJournalReplayEntries = request.MaximumJournalReplayEntries,
            CompactionThreshold = request.MaximumJournalReplayEntries,
        };
        await Persistence.EnableMovementProtocolAsync(settings, request.MinimumRoutingEpoch);
        if (_view.SlotCatalog is null)
        {
            try
            {
                _view = new StoragePartitionView(_view.Records, routing.VirtualSlotCount);
            }
            catch
            {
                // The capability gate is already durable. A fresh activation must reconstruct
                // the catalog rather than serving protocol-1 operations with a partial view.
                PoisonActivation();
                throw;
            }
        }
        else if (_view.SlotCatalog.VirtualSlotCount != routing.VirtualSlotCount)
        {
            PoisonActivation();
            throw new InvalidOperationException(
                "The activation slot catalog does not match the immutable routing layout.");
        }

        return Persistence.CreateProtocolState();
    }

    public Task<StoragePartitionProtocolState> GetMovementStateAsync()
    {
        EnsureUsable();
        return Task.FromResult(Persistence.CreateProtocolState());
    }

    public async Task<StoragePartitionProtocolState> FreezeMoveSourceAsync(StorageMoveIdentity move)
    {
        EnsureUsable();
        ValidateMoveIdentity(move, StoragePartitionMoveRole.Source);
        await ValidateMoveLayoutBeforeCommitAsync(move);

        var current = Persistence.MoveControl;
        if (current.IsPresent)
        {
            EnsureMoveControlMatches(current, move, StoragePartitionMoveRole.Source);
            if (current.Phase != StoragePartitionMovePhase.SourceFrozen)
            {
                throw new InvalidOperationException(
                    $"Move '{move.MoveId}' cannot freeze a source in phase {current.Phase}.");
            }

            return Persistence.CreateProtocolState();
        }

        var control = CreateMoveControl(
            move,
            StoragePartitionMoveRole.Source,
            StoragePartitionMovePhase.SourceFrozen,
            Persistence.NextVersion);
        await Persistence.SetMoveControlAsync(control);
        return Persistence.CreateProtocolState();
    }

    public async Task<StoragePartitionProtocolState> PrepareMoveTargetAsync(
        StorageMoveTargetPrepareRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Move);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(request.FrozenNextVersion);
        ValidateMoveIdentity(request.Move, StoragePartitionMoveRole.Target);
        await ValidateMoveLayoutBeforeCommitAsync(request.Move);

        var catalog = GetSlotCatalog();
        var current = Persistence.MoveControl;
        if (!current.IsPresent)
        {
            if (catalog.GetRecordCount(request.Move.Slot) != 0)
            {
                throw new InvalidOperationException(
                    $"Target partition {_partitionIndex} already contains records for virtual slot {request.Move.Slot}.");
            }

            current = CreateMoveControl(
                request.Move,
                StoragePartitionMoveRole.Target,
                StoragePartitionMovePhase.TargetPrepared,
                request.FrozenNextVersion);
            await Persistence.SetMoveControlAsync(current);
        }
        else
        {
            EnsureMoveControlMatches(current, request.Move, StoragePartitionMoveRole.Target);
            if (current.FrozenNextVersion != request.FrozenNextVersion)
            {
                throw new InvalidOperationException(
                    "A repeated target preparation must use the same source version high-water mark.");
            }
        }

        if (current.Phase is StoragePartitionMovePhase.TargetImporting
            or StoragePartitionMovePhase.TargetImportComplete)
        {
            return Persistence.CreateProtocolState();
        }

        if (current.Phase != StoragePartitionMovePhase.TargetPrepared)
        {
            throw new InvalidOperationException(
                $"Move '{request.Move.MoveId}' cannot prepare a target in phase {current.Phase}.");
        }

        await PrepareForProtocolMutationAsync();
        var advanced = current.Copy();
        advanced.Phase = StoragePartitionMovePhase.TargetImporting;
        var entry = CreateMoveJournalEntry(
            StorageJournalOperation.AdvanceVersion,
            CreateAdvancePayload(request.Move, request.FrozenNextVersion),
            Math.Max(Persistence.NextVersion, request.FrozenNextVersion));
        await CommitAsync(entry, advanced);
        return Persistence.CreateProtocolState();
    }

    public async Task<StorageMoveExportPage> ExportMovePageAsync(StorageMovePageRequest request)
    {
        EnsureUsable();
        ValidatePageRequest(request);
        ValidateMoveIdentity(request.Move, StoragePartitionMoveRole.Source);
        await ValidateMoveLayoutBeforeCommitAsync(request.Move);

        var control = Persistence.MoveControl;
        EnsureMoveControlMatches(control, request.Move, StoragePartitionMoveRole.Source);
        if (control.Phase != StoragePartitionMovePhase.SourceFrozen)
        {
            throw new InvalidOperationException(
                $"Move '{request.Move.MoveId}' cannot export in phase {control.Phase}.");
        }

        var records = StorageMovePageOperations.CreateExportRecords(
            _view,
            request.Move.Slot,
            request.AfterRecordKey,
            request.ItemLimit,
            request.ByteTarget,
            out var nextRecordKey,
            out var exhausted,
            out var encodedByteCount);
        var payload = CreatePagePayload(
            request.Move,
            request.PageOrdinal,
            request.AfterRecordKey,
            nextRecordKey,
            exhausted,
            control.FrozenNextVersion,
            records,
            deletes: [],
            request.ItemLimit,
            request.ByteTarget,
            encodedByteCount,
            StorageJournalOperation.Import);
        return new StorageMoveExportPage
        {
            Move = request.Move.Copy(),
            PageOrdinal = request.PageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(request.AfterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.CopyText(nextRecordKey),
            Exhausted = exhausted,
            EncodedByteCount = encodedByteCount,
            Records = records.Select(static item => item.Copy()).ToList(),
            PageDigest = [.. payload.PageDigest],
            FrozenNextVersion = control.FrozenNextVersion,
            ItemLimit = request.ItemLimit,
            ByteTarget = request.ByteTarget,
        };
    }

    public async Task<StorageMovePageCommitResult> ImportMovePageAsync(
        StorageMoveImportPageRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Page);
        ArgumentNullException.ThrowIfNull(request.Page.Move);
        ValidateMoveIdentity(request.Page.Move, StoragePartitionMoveRole.Target);
        ValidateExportPage(request.Page);
        await ValidateMoveLayoutBeforeCommitAsync(request.Page.Move);

        var current = Persistence.MoveControl;
        EnsureMoveControlMatches(current, request.Page.Move, StoragePartitionMoveRole.Target);
        if (IsDuplicatePage(current, request.Page.PageOrdinal, request.Page.PageDigest))
        {
            return CreatePageCommitResult(
                current,
                checked(current.NextPageOrdinal - 1),
                current.ProgressAfterRecordKey,
                current.Phase == StoragePartitionMovePhase.TargetImportComplete,
                current.LastPageDigest,
                current.LastPageEncodedByteCount);
        }

        if (current.Phase != StoragePartitionMovePhase.TargetImporting
            || request.Page.PageOrdinal != current.NextPageOrdinal
            || !StorageMoveRecordCodec.TextEquals(
                request.Page.AfterRecordKey,
                current.ProgressAfterRecordKey)
            || request.Page.FrozenNextVersion != current.FrozenNextVersion)
        {
            throw new InvalidOperationException(
                "An import page does not extend the target's durable movement cursor.");
        }

        StorageMovePageOperations.ValidateImportAgainstCurrentView(
            _view,
            request.Page,
            current,
            Persistence.NextVersion);
        var payload = CreateImportPayload(request.Page);
        var advanced = AdvanceImportPageControl(
            current,
            request.Page.NextRecordKey,
            request.Page.PageDigest,
            request.Page.Records.Count,
            request.Page.EncodedByteCount,
            request.Page.AfterRecordKey,
            request.Page.ItemLimit,
            request.Page.ByteTarget,
            request.Page.Exhausted
                ? StoragePartitionMovePhase.TargetImportComplete
                : StoragePartitionMovePhase.TargetImporting);
        await PrepareForProtocolMutationAsync();
        var entry = CreateMoveJournalEntry(
            StorageJournalOperation.Import,
            payload,
            Persistence.NextVersion);
        await CommitAsync(entry, advanced);
        ApplyCommittedImports(request.Page.Records);
        return CreatePageCommitResult(
            advanced,
            request.Page.PageOrdinal,
            request.Page.NextRecordKey,
            request.Page.Exhausted,
            request.Page.PageDigest,
            request.Page.EncodedByteCount);
    }

    public async Task<StoragePartitionProtocolState> HideMoveSourceAsync(
        StorageMoveVisibilityFenceRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Move);
        ValidateMoveIdentity(request.Move, StoragePartitionMoveRole.Source);
        if (request.CommittedEpoch != checked(request.Move.SourceEpoch + 1))
        {
            throw new ArgumentException(
                "The visibility fence must identify the epoch immediately after the move source epoch.",
                nameof(request));
        }

        await ValidateMoveLayoutAfterCommitAsync(request.Move, request.CommittedEpoch);
        var current = Persistence.MoveControl;
        EnsureMoveControlMatches(current, request.Move, StoragePartitionMoveRole.Source);
        if (current.Phase is StoragePartitionMovePhase.SourceHidden
            or StoragePartitionMovePhase.SourceDeleting
            or StoragePartitionMovePhase.SourceDeleteComplete)
        {
            if (Persistence.MinimumRoutingEpoch < request.CommittedEpoch)
            {
                throw new InvalidOperationException("The source visibility phase lacks its durable routing fence.");
            }

            return Persistence.CreateProtocolState();
        }

        if (current.Phase != StoragePartitionMovePhase.SourceFrozen)
        {
            throw new InvalidOperationException(
                $"Move '{request.Move.MoveId}' cannot hide a source in phase {current.Phase}.");
        }

        var hidden = current.Copy();
        hidden.Phase = StoragePartitionMovePhase.SourceHidden;
        await Persistence.SetMoveControlAsync(hidden, request.CommittedEpoch);
        return Persistence.CreateProtocolState();
    }

    public async Task<StoragePartitionProtocolState> EnableMoveTargetAsync(StorageMoveIdentity move)
    {
        EnsureUsable();
        ValidateMoveIdentity(move, StoragePartitionMoveRole.Target);
        await ValidateMoveLayoutAfterCommitAsync(move, checked(move.SourceEpoch + 1));
        var current = Persistence.MoveControl;
        EnsureMoveControlMatches(current, move, StoragePartitionMoveRole.Target);
        if (current.Phase == StoragePartitionMovePhase.TargetEnabled)
        {
            return Persistence.CreateProtocolState();
        }

        if (current.Phase != StoragePartitionMovePhase.TargetImportComplete)
        {
            throw new InvalidOperationException(
                $"Move '{move.MoveId}' cannot enable a target in phase {current.Phase}.");
        }

        var enabled = current.Copy();
        enabled.Phase = StoragePartitionMovePhase.TargetEnabled;
        await Persistence.SetMoveControlAsync(enabled);
        return Persistence.CreateProtocolState();
    }

    public async Task<StorageMovePageCommitResult> DeleteMovePageAsync(
        StorageMoveDeletePageRequest request)
    {
        EnsureUsable();
        ValidateDeletePageRequest(request);
        var expectedRole = request.Mode == StorageMoveDeleteMode.SourceCleanup
            ? StoragePartitionMoveRole.Source
            : StoragePartitionMoveRole.Target;
        ValidateMoveIdentity(request.Move, expectedRole);
        if (request.Mode == StorageMoveDeleteMode.SourceCleanup)
        {
            await ValidateMoveLayoutAfterCommitAsync(
                request.Move,
                checked(request.Move.SourceEpoch + 1));
        }
        else
        {
            await ValidateMoveLayoutBeforeCommitAsync(request.Move);
        }

        var current = Persistence.MoveControl;
        EnsureMoveControlMatches(current, request.Move, expectedRole);
        if (request.Mode == StorageMoveDeleteMode.TargetAbort
            && current.Phase is StoragePartitionMovePhase.TargetImporting
                or StoragePartitionMovePhase.TargetImportComplete)
        {
            current = ResetTargetAbortProgress(current);
        }

        if (current.NextPageOrdinal > 0
            && request.PageOrdinal == checked(current.NextPageOrdinal - 1))
        {
            if (!IsExactDuplicateDeleteRequest(current, request))
            {
                throw new InvalidOperationException(
                    "A repeated move-delete page does not match the durable request receipt.");
            }

            return CreatePageCommitResult(
                current,
                request.PageOrdinal,
                current.ProgressAfterRecordKey,
                current.Phase is StoragePartitionMovePhase.SourceDeleteComplete
                    or StoragePartitionMovePhase.TargetAbortComplete,
                current.LastPageDigest,
                current.LastPageEncodedByteCount);
        }

        ValidateDeletePhase(request.Mode, current);
        if (request.PageOrdinal != current.NextPageOrdinal
            || !StorageMoveRecordCodec.TextEquals(
                request.AfterRecordKey,
                current.ProgressAfterRecordKey))
        {
            throw new InvalidOperationException(
                "A move-delete page does not extend the participant's durable movement cursor.");
        }

        var deletes = StorageMovePageOperations.CreateDeleteRecords(
            _view,
            request.Move.Slot,
            request.AfterRecordKey,
            request.ItemLimit,
            request.ByteTarget,
            out var nextRecordKey,
            out var exhausted,
            out var encodedByteCount);
        var payload = CreatePagePayload(
            request.Move,
            request.PageOrdinal,
            request.AfterRecordKey,
            nextRecordKey,
            exhausted,
            current.FrozenNextVersion,
            imports: [],
            deletes,
            request.ItemLimit,
            request.ByteTarget,
            encodedByteCount,
            StorageJournalOperation.MoveDelete);
        var nextPhase = request.Mode switch
        {
            StorageMoveDeleteMode.SourceCleanup when exhausted =>
                StoragePartitionMovePhase.SourceDeleteComplete,
            StorageMoveDeleteMode.SourceCleanup => StoragePartitionMovePhase.SourceDeleting,
            StorageMoveDeleteMode.TargetAbort when exhausted =>
                StoragePartitionMovePhase.TargetAbortComplete,
            StorageMoveDeleteMode.TargetAbort => StoragePartitionMovePhase.TargetAbortDeleting,
            _ => throw new ArgumentOutOfRangeException(nameof(request), request.Mode, "Unknown delete mode."),
        };
        var advanced = AdvanceDeletePageControl(
            current,
            nextRecordKey,
            payload.PageDigest,
            deletes.Count,
            encodedByteCount,
            request.AfterRecordKey,
            request.ItemLimit,
            request.ByteTarget,
            nextPhase);
        await PrepareForProtocolMutationAsync();
        var entry = CreateMoveJournalEntry(
            StorageJournalOperation.MoveDelete,
            payload,
            Persistence.NextVersion);
        await CommitAsync(entry, advanced);
        ApplyCommittedMoveDeletes(deletes);
        return CreatePageCommitResult(
            advanced,
            request.PageOrdinal,
            nextRecordKey,
            exhausted,
            payload.PageDigest,
            encodedByteCount);
    }

    public async Task<StoragePartitionProtocolState> RetireMoveParticipantAsync(
        StorageMoveRetireRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Move);
        if (!Enum.IsDefined(request.Kind))
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.Kind, "Unknown retirement kind.");
        }

        var current = Persistence.MoveControl;
        if (!current.IsPresent)
        {
            return Persistence.CreateProtocolState();
        }

        var expectedRole = _partitionIndex == request.Move.SourceOwner
            ? StoragePartitionMoveRole.Source
            : StoragePartitionMoveRole.Target;
        ValidateMoveIdentity(request.Move, expectedRole);
        EnsureMoveControlMatches(current, request.Move, expectedRole);
        var allowed = request.Kind switch
        {
            StorageMoveRetirementKind.Completed =>
                current.Phase is StoragePartitionMovePhase.SourceDeleteComplete
                    or StoragePartitionMovePhase.TargetEnabled,
            StorageMoveRetirementKind.Aborted =>
                current.Phase is StoragePartitionMovePhase.SourceFrozen
                    or StoragePartitionMovePhase.TargetAbortComplete,
            _ => false,
        };
        if (!allowed)
        {
            throw new InvalidOperationException(
                $"Move '{request.Move.MoveId}' cannot retire {current.Role} in phase {current.Phase}.");
        }

        if ((current.Phase is StoragePartitionMovePhase.SourceDeleteComplete
                or StoragePartitionMovePhase.TargetAbortComplete)
            && GetSlotCatalog().GetRecordCount(current.Slot) != 0)
        {
            throw new InvalidOperationException(
                "A move participant cannot retire while cleanup records remain in its slot.");
        }

        await Persistence.ClearMoveControlAsync();
        return Persistence.CreateProtocolState();
    }

    public async Task CompactAsync()
    {
        EnsureUsable();
        try
        {
            await Persistence.CompactAsync(_view.Records);
        }
        catch
        {
            PoisonActivation();
            throw;
        }
    }

    public Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync()
    {
        EnsureUsable();
        return Task.FromResult(Persistence.CreateInfo(_view.Records.Count));
    }

    private HashSet<string> FindRecordKeys(ExactIndexQuery query)
    {
        return query.Kind switch
        {
            SearchableIndexKind.Hash => _view.Indexes.FindHashEntries(query.Scope, query.Value),
            SearchableIndexKind.Range => _view.Indexes.FindRangeEntries(query.Scope, query.Value),
            _ => throw new ArgumentOutOfRangeException(nameof(query), query.Kind, "Unknown index kind."),
        };
    }

    private HashSet<string> FindRangeRecordKeys(RangeIndexQuery query)
    {
        if (query.LowerBound is null || query.UpperBound is null)
        {
            throw new ArgumentException(
                "A bounded range query requires both lower and upper bounds.",
                nameof(query));
        }

        if (query.LowerBound.CompareTo(query.UpperBound) > 0)
        {
            throw new ArgumentException(
                "The lower range bound must not be greater than the upper range bound.",
                nameof(query));
        }

        var recordKeys = new HashSet<string>(StringComparer.Ordinal);
        _view.Indexes.UnionRange(
            query.Scope,
            query.LowerBound,
            query.UpperBound,
            query.IncludeLowerBound,
            query.IncludeUpperBound,
            recordKeys);
        return recordKeys;
    }

    private static void ValidateProtocolRequest(StoragePartitionProtocolRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.ProtocolVersion != StorageMoveProtocol.Version)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ProtocolVersion,
                "Unknown storage movement protocol version.");
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(request.VirtualSlotCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(request.MinimumRoutingEpoch);
        StoragePersistence.ValidateOptions(
            request.JournalSegmentCapacity,
            request.MaximumJournalReplayEntries);
    }

    private void ValidateMoveIdentity(
        StorageMoveIdentity move,
        StoragePartitionMoveRole expectedRole)
    {
        ValidateMoveIdentityBounds(move);

        var expectedOwner = expectedRole switch
        {
            StoragePartitionMoveRole.Source => move.SourceOwner,
            StoragePartitionMoveRole.Target => move.TargetOwner,
            _ => throw new ArgumentOutOfRangeException(nameof(expectedRole)),
        };
        if (_partitionIndex != expectedOwner)
        {
            throw new ArgumentException(
                $"Move '{move.MoveId}' addresses partition {expectedOwner}, not partition {_partitionIndex}.",
                nameof(move));
        }

        var protocol = Persistence.CreateProtocolState();
        if (protocol.PersistenceFormatVersion != StoragePersistence.CurrentPersistenceFormatVersion
            || protocol.MovementProtocolVersion != StorageMoveProtocol.Version
            || !protocol.RoutedOperationsRequired
            || protocol.MinimumRoutingEpoch < move.SourceEpoch
            || protocol.MinimumRoutingEpoch > checked(move.SourceEpoch + 1)
            || GetSlotCatalog().VirtualSlotCount != move.VirtualSlotCount)
        {
            throw new InvalidOperationException(
                "The partition is not fenced for this movement protocol and routing epoch.");
        }
    }

    internal static void ValidateMoveIdentityBounds(StorageMoveIdentity move)
    {
        ArgumentNullException.ThrowIfNull(move);
        if (move.ProtocolVersion != StorageMoveProtocol.Version
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
            || move.SourceOwner == move.TargetOwner)
        {
            throw new ArgumentException("A storage move identity is invalid.", nameof(move));
        }
    }

    private static void EnsureMoveControlMatches(
        StoragePartitionMoveControl control,
        StorageMoveIdentity move,
        StoragePartitionMoveRole role)
    {
        ArgumentNullException.ThrowIfNull(control);
        if (!control.IsPresent
            || control.MoveId != move.MoveId
            || control.Slot != move.Slot
            || control.VirtualSlotCount != move.VirtualSlotCount
            || control.SourceEpoch != move.SourceEpoch
            || control.SourceOwner != move.SourceOwner
            || control.TargetOwner != move.TargetOwner
            || control.Role != role)
        {
            throw new InvalidOperationException(
                $"Partition move control does not match move '{move.MoveId}'.");
        }
    }

    private static StoragePartitionMoveControl CreateMoveControl(
        StorageMoveIdentity move,
        StoragePartitionMoveRole role,
        StoragePartitionMovePhase phase,
        long frozenNextVersion)
    {
        return new StoragePartitionMoveControl
        {
            IsPresent = true,
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            Role = role,
            Phase = phase,
            FrozenNextVersion = frozenNextVersion,
        };
    }

    private async Task ValidateMoveLayoutBeforeCommitAsync(StorageMoveIdentity move)
    {
        var routing = await GetRoutingSnapshotAsync(move.SourceEpoch);
        if (routing.Epoch != move.SourceEpoch
            || routing.VirtualSlotCount != move.VirtualSlotCount
            || routing.GetOwner(move.Slot) != move.SourceOwner)
        {
            throw new StorageRouteMismatchException(
                move.SourceEpoch,
                routing.Epoch,
                move.SourceOwner,
                move.Slot,
                routing.GetOwner(move.Slot));
        }
    }

    private async Task ValidateMoveLayoutAfterCommitAsync(
        StorageMoveIdentity move,
        long committedEpoch)
    {
        var routing = await GetRoutingSnapshotAsync(committedEpoch);
        if (routing.Epoch != committedEpoch
            || routing.VirtualSlotCount != move.VirtualSlotCount
            || routing.GetOwner(move.Slot) != move.TargetOwner)
        {
            throw new StorageRouteMismatchException(
                committedEpoch,
                routing.Epoch,
                move.TargetOwner,
                move.Slot,
                routing.GetOwner(move.Slot));
        }
    }

    private static void ValidatePageRequest(StorageMovePageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Move);
        ArgumentOutOfRangeException.ThrowIfNegative(request.PageOrdinal);
        StorageMoveProtocol.ValidatePageLimits(
            request.ItemLimit,
            request.ByteTarget,
            nameof(request));
    }

    private static void ValidateDeletePageRequest(StorageMoveDeletePageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Move);
        if (!Enum.IsDefined(request.Mode))
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.Mode, "Unknown delete mode.");
        }

        ArgumentOutOfRangeException.ThrowIfNegative(request.PageOrdinal);
        StorageMoveProtocol.ValidatePageLimits(
            request.ItemLimit,
            request.ByteTarget,
            nameof(request));
    }

    private static void ValidateExportPage(StorageMoveExportPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(page.Move);
        ArgumentNullException.ThrowIfNull(page.Records);
        ArgumentNullException.ThrowIfNull(page.PageDigest);
        var payload = CreateImportPayload(page);
        var validationEntry = new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.Import,
            RecordKey = string.Empty,
            NextVersionAfter = 1,
            Move = payload,
        };
        StoragePersistenceStateValidation.ValidateJournalEntry(validationEntry, nameof(page));
    }

    private static StorageMoveJournalPayload CreateAdvancePayload(
        StorageMoveIdentity move,
        long frozenNextVersion)
    {
        return new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            FrozenNextVersion = frozenNextVersion,
        };
    }

    private static StorageMoveJournalPayload CreateImportPayload(StorageMoveExportPage page)
    {
        return new StorageMoveJournalPayload
        {
            MoveId = page.Move.MoveId,
            Slot = page.Move.Slot,
            VirtualSlotCount = page.Move.VirtualSlotCount,
            SourceEpoch = page.Move.SourceEpoch,
            SourceOwner = page.Move.SourceOwner,
            TargetOwner = page.Move.TargetOwner,
            PageOrdinal = page.PageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(page.AfterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.CopyText(page.NextRecordKey),
            Exhausted = page.Exhausted,
            PageDigest = [.. page.PageDigest],
            FrozenNextVersion = page.FrozenNextVersion,
            Imports = page.Records.Select(static item => item.Copy()).ToList(),
            ItemLimit = page.ItemLimit,
            ByteTarget = page.ByteTarget,
            EncodedByteCount = page.EncodedByteCount,
        };
    }

    private static StorageMoveJournalPayload CreatePagePayload(
        StorageMoveIdentity move,
        long pageOrdinal,
        byte[]? afterRecordKey,
        byte[]? nextRecordKey,
        bool exhausted,
        long frozenNextVersion,
        List<StorageMoveRecord> imports,
        List<StorageMoveDeleteRecord> deletes,
        int itemLimit,
        int byteTarget,
        long encodedByteCount,
        StorageJournalOperation operation)
    {
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            PageOrdinal = pageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(afterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.CopyText(nextRecordKey),
            Exhausted = exhausted,
            FrozenNextVersion = frozenNextVersion,
            Imports = imports.Select(static item => item.Copy()).ToList(),
            Deletes = deletes.Select(static item => item.Copy()).ToList(),
            ItemLimit = itemLimit,
            ByteTarget = byteTarget,
            EncodedByteCount = encodedByteCount,
        };
        return new StorageMoveJournalPayload
        {
            MoveId = unsigned.MoveId,
            Slot = unsigned.Slot,
            VirtualSlotCount = unsigned.VirtualSlotCount,
            SourceEpoch = unsigned.SourceEpoch,
            SourceOwner = unsigned.SourceOwner,
            TargetOwner = unsigned.TargetOwner,
            PageOrdinal = unsigned.PageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(unsigned.AfterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.CopyText(unsigned.NextRecordKey),
            Exhausted = unsigned.Exhausted,
            PageDigest = StorageMovePageDigest.Compute(operation, unsigned),
            FrozenNextVersion = unsigned.FrozenNextVersion,
            Imports = unsigned.Imports,
            Deletes = unsigned.Deletes,
            ItemLimit = unsigned.ItemLimit,
            ByteTarget = unsigned.ByteTarget,
            EncodedByteCount = unsigned.EncodedByteCount,
        };
    }

    private StorageJournalEntry CreateMoveJournalEntry(
        StorageJournalOperation operation,
        StorageMoveJournalPayload payload,
        long nextVersionAfter)
    {
        return new StorageJournalEntry
        {
            Sequence = Persistence.NextSequence,
            WriterEpoch = Persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Persistence.CommittedOperationId,
            Operation = operation,
            RecordKey = string.Empty,
            NextVersionAfter = nextVersionAfter,
            Move = payload,
        };
    }

    internal static StoragePartitionMoveControl AdvanceImportPageControl(
        StoragePartitionMoveControl current,
        byte[]? nextRecordKey,
        byte[] pageDigest,
        int recordCount,
        long encodedByteCount,
        byte[]? requestAfterRecordKey,
        int itemLimit,
        int byteTarget,
        StoragePartitionMovePhase phase)
    {
        var advanced = current.Copy();
        advanced.Phase = phase;
        advanced.ProgressAfterRecordKey = StorageMoveRecordCodec.CopyText(nextRecordKey);
        advanced.NextPageOrdinal = checked(current.NextPageOrdinal + 1);
        advanced.LastPageDigest = [.. pageDigest];
        advanced.ImportedRecordCount = checked(current.ImportedRecordCount + recordCount);
        advanced.ImportedByteCount = checked(current.ImportedByteCount + encodedByteCount);
        SetLastPageReceipt(
            advanced,
            requestAfterRecordKey,
            itemLimit,
            byteTarget,
            encodedByteCount);
        return advanced;
    }

    internal static StoragePartitionMoveControl AdvanceDeletePageControl(
        StoragePartitionMoveControl current,
        byte[]? nextRecordKey,
        byte[] pageDigest,
        int recordCount,
        long encodedByteCount,
        byte[]? requestAfterRecordKey,
        int itemLimit,
        int byteTarget,
        StoragePartitionMovePhase phase)
    {
        var advanced = current.Copy();
        advanced.Phase = phase;
        advanced.ProgressAfterRecordKey = StorageMoveRecordCodec.CopyText(nextRecordKey);
        advanced.NextPageOrdinal = checked(current.NextPageOrdinal + 1);
        advanced.LastPageDigest = [.. pageDigest];
        advanced.DeletedRecordCount = checked(current.DeletedRecordCount + recordCount);
        advanced.DeletedByteCount = checked(current.DeletedByteCount + encodedByteCount);
        SetLastPageReceipt(
            advanced,
            requestAfterRecordKey,
            itemLimit,
            byteTarget,
            encodedByteCount);
        return advanced;
    }

    private static void SetLastPageReceipt(
        StoragePartitionMoveControl control,
        byte[]? requestAfterRecordKey,
        int itemLimit,
        int byteTarget,
        long encodedByteCount)
    {
        control.LastPageRequestAfterRecordKey = StorageMoveRecordCodec.CopyText(requestAfterRecordKey);
        control.LastPageItemLimit = itemLimit;
        control.LastPageByteTarget = byteTarget;
        control.LastPageEncodedByteCount = encodedByteCount;
    }

    private static bool IsDuplicatePage(
        StoragePartitionMoveControl control,
        long pageOrdinal,
        byte[] pageDigest)
    {
        return control.NextPageOrdinal > 0
            && pageOrdinal == control.NextPageOrdinal - 1
            && StorageMovePageDigest.Equals(control.LastPageDigest, pageDigest);
    }

    internal static bool IsExactDuplicateDeleteRequest(
        StoragePartitionMoveControl control,
        StorageMoveDeletePageRequest request)
    {
        return StorageMoveRecordCodec.TextEquals(
                control.LastPageRequestAfterRecordKey,
                request.AfterRecordKey)
            && control.LastPageItemLimit == request.ItemLimit
            && control.LastPageByteTarget == request.ByteTarget;
    }

    private static void ValidateDeletePhase(
        StorageMoveDeleteMode mode,
        StoragePartitionMoveControl control)
    {
        var valid = mode switch
        {
            StorageMoveDeleteMode.SourceCleanup =>
                control.Phase is StoragePartitionMovePhase.SourceHidden
                    or StoragePartitionMovePhase.SourceDeleting,
            StorageMoveDeleteMode.TargetAbort =>
                control.Phase is StoragePartitionMovePhase.TargetImporting
                    or StoragePartitionMovePhase.TargetImportComplete
                    or StoragePartitionMovePhase.TargetAbortDeleting,
            _ => false,
        };
        if (!valid)
        {
            throw new InvalidOperationException(
                $"Move-delete mode {mode} is invalid in participant phase {control.Phase}.");
        }
    }

    internal static StoragePartitionMoveControl ResetTargetAbortProgress(
        StoragePartitionMoveControl current)
    {
        var reset = current.Copy();
        reset.Phase = StoragePartitionMovePhase.TargetAbortDeleting;
        reset.ProgressAfterRecordKey = null;
        reset.NextPageOrdinal = 0;
        reset.LastPageDigest = [];
        reset.DeletedRecordCount = 0;
        reset.DeletedByteCount = 0;
        reset.LastPageRequestAfterRecordKey = null;
        reset.LastPageItemLimit = 0;
        reset.LastPageByteTarget = 0;
        reset.LastPageEncodedByteCount = 0;
        return reset;
    }

    private StorageMovePageCommitResult CreatePageCommitResult(
        StoragePartitionMoveControl control,
        long pageOrdinal,
        byte[]? afterRecordKey,
        bool exhausted,
        byte[] pageDigest,
        long encodedByteCount)
    {
        _ = control;
        return new StorageMovePageCommitResult
        {
            State = Persistence.CreateProtocolState(),
            PageOrdinal = pageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(afterRecordKey),
            Exhausted = exhausted,
            PageDigest = [.. pageDigest],
            EncodedByteCount = encodedByteCount,
        };
    }

    private StoragePartitionSlotCatalog GetSlotCatalog()
    {
        return _view.SlotCatalog
            ?? throw new InvalidOperationException(
                "The partition virtual-slot catalog has not been initialized.");
    }

    private async Task PrepareForProtocolMutationAsync()
    {
        try
        {
            await Persistence.PrepareForProtocolMutationAsync(_view.Records);
        }
        catch
        {
            PoisonActivation();
            throw;
        }
    }

    private async Task PrepareForMutationAsync(StoragePersistenceSettings settings)
    {
        try
        {
            await Persistence.PrepareForMutationAsync(_view.Records, settings);
        }
        catch
        {
            PoisonActivation();
            throw;
        }
    }

    private static void ValidatePageRequest(RoutedPartitionQueryPageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Query);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.StateName);
        ArgumentNullException.ThrowIfNull(request.QueryFingerprint);
        ArgumentNullException.ThrowIfNull(request.LayoutFingerprint);
        QueryPlanValidator.Validate(request.Query);

        if (request.QueryFingerprint.Length != 32)
        {
            throw new ArgumentException(
                "A partition query fingerprint must contain exactly 32 bytes.",
                nameof(request));
        }

        if (request.LayoutFingerprint.Length != 32)
        {
            throw new ArgumentException(
                "A partition layout fingerprint must contain exactly 32 bytes.",
                nameof(request));
        }

        if (request.ProtocolVersion != QueryProtocol.PagingVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ProtocolVersion,
                "Unknown partition paging protocol version.");
        }

        if (request.OrderingVersion != QueryProtocol.OrderingVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.OrderingVersion,
                "Unknown canonical GrainId ordering version.");
        }

        if (request.WorkPolicyVersion != QueryProtocol.WorkPolicyVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.WorkPolicyVersion,
                "Unknown partition work-policy version.");
        }

        if (request.ResponseFamily != PartitionQueryResponseFamily.GrainIdPage)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ResponseFamily,
                "Unknown partition query response family.");
        }

        if (request.LayoutFormatVersion != StorageLayout.CurrentFormatVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.LayoutFormatVersion,
                "Unknown storage layout format version.");
        }

        if (request.HasAfter == request.After.IsDefault)
        {
            throw new ArgumentException(
                "HasAfter must be true exactly when a non-default exclusive boundary is supplied.",
                nameof(request));
        }

        if (request.WorkBudget <= 0
            || request.WorkBudget > SearchableStorageQueryOptions.MaximumPartitionWorkBudget)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.WorkBudget,
                $"A partition work budget must be between 1 and {SearchableStorageQueryOptions.MaximumPartitionWorkBudget}.");
        }

        if (request.ItemLimit <= 0
            || request.ItemLimit > SearchableStorageQueryOptions.MaximumPartitionResponseItems)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ItemLimit,
                $"A partition item limit must be between 1 and {SearchableStorageQueryOptions.MaximumPartitionResponseItems}.");
        }

        if (request.ByteLimit <= 0
            || request.ByteLimit > SearchableStorageQueryOptions.MaximumPartitionResponseBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ByteLimit,
                $"A partition byte limit must be between 1 and {SearchableStorageQueryOptions.MaximumPartitionResponseBytes}.");
        }
    }

    private static void ValidateDistinctFacetPageRequest(
        RoutedPartitionDistinctFacetPageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateFacetRequestCommon(
            request.Query,
            request.StateName,
            request.FacetScope,
            request.FacetKind,
            request.RequestFingerprint,
            request.LayoutFingerprint,
            request.ProtocolVersion,
            request.OrderingVersion,
            request.WorkPolicyVersion,
            request.LayoutFormatVersion,
            request.WorkBudget,
            request.ByteLimit,
            request);
        if (request.ResponseFamily != PartitionQueryResponseFamily.DistinctFacetValuePage)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request), request.ResponseFamily, "Unknown distinct facet response family.");
        }

        ValidateFacetItemLimit(request.ItemLimit, request);
        ValidateFacetIndexValue(request.After, request);
        ValidateExpectedDataVersion(
            request.HasExpectedDataVersion,
            request.ExpectedDataVersion,
            request);
    }

    private static void ValidateFacetCandidatePageRequest(
        RoutedPartitionFacetCandidatePageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateFacetRequestCommon(
            request.Query,
            request.StateName,
            request.FacetScope,
            request.FacetKind,
            request.RequestFingerprint,
            request.LayoutFingerprint,
            request.ProtocolVersion,
            request.OrderingVersion,
            request.WorkPolicyVersion,
            request.LayoutFormatVersion,
            request.WorkBudget,
            request.ByteLimit,
            request);
        if (request.ResponseFamily != PartitionQueryResponseFamily.FacetValueCountCandidates)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request), request.ResponseFamily, "Unknown facet candidate response family.");
        }

        ValidateFacetItemLimit(request.ItemLimit, request);
        ValidateFacetIndexValue(request.AfterValue, request);
        if (request.AfterValue is not null && !request.HasExpectedDataVersion)
        {
            throw new ArgumentException(
                "A resumed facet candidate page must be pinned to an expected data version.",
                nameof(request));
        }

        ValidateExpectedDataVersion(
            request.HasExpectedDataVersion,
            request.ExpectedDataVersion,
            request);
    }

    private static void ValidateFacetCountSliceRequest(
        RoutedPartitionFacetCountSliceRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Value);
        ValidateFacetRequestCommon(
            request.Query,
            request.StateName,
            request.FacetScope,
            request.FacetKind,
            request.RequestFingerprint,
            request.LayoutFingerprint,
            request.ProtocolVersion,
            request.OrderingVersion,
            request.WorkPolicyVersion,
            request.LayoutFormatVersion,
            request.WorkBudget,
            byteLimit: 1,
            request);
        if (request.ResponseFamily != PartitionQueryResponseFamily.FacetValueCountProbe)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request), request.ResponseFamily, "Unknown facet count-slice response family.");
        }

        if (!request.HasExpectedDataVersion)
        {
            throw new ArgumentException(
                "A facet count slice must be pinned to an expected data version.",
                nameof(request));
        }

        if (request.HasAfter == request.After.IsDefault)
        {
            throw new ArgumentException(
                "HasAfter must be true exactly when a GrainId frontier is supplied.",
                nameof(request));
        }

        if (request.HasAfter)
        {
            GrainIdCanonicalOrder.Validate(request.After, nameof(request));
        }

        ValidateFacetIndexValue(request.Value, request);

        ValidateExpectedDataVersion(
            request.HasExpectedDataVersion,
            request.ExpectedDataVersion,
            request);
    }

    private static void ValidateFacetItemLimit(int itemLimit, object request)
    {
        if (itemLimit <= 0
            || itemLimit > SearchableStorageQueryOptions.MaximumPartitionResponseItems)
        {
            throw new ArgumentOutOfRangeException(nameof(request), itemLimit, "Invalid facet item limit.");
        }
    }

    internal static void ValidateFacetIndexValue(IndexValue? value, object request)
    {
        if (value is null)
        {
            return;
        }

        try
        {
            IndexValueCanonicalEncoding.Validate(value, nameof(value));
            _ = IndexValueCanonicalEncoding.GetEncodedLength(value);
        }
        catch (Exception exception) when (exception is ArgumentException
            or CanonicalEncodingLimitExceededException
            or InvalidOperationException
            or OverflowException)
        {
            throw new ArgumentException(
                "The facet request contains an invalid canonical index value.",
                nameof(request));
        }
    }

    private static void ValidateExpectedDataVersion(
        bool hasExpected,
        long expected,
        object request)
    {
        if (expected < 0 || (!hasExpected && expected != 0))
        {
            throw new ArgumentException("The expected facet data version is invalid.", nameof(request));
        }
    }

    private static void ValidateFacetRequestCommon(
        PartitionQueryPlan query,
        string stateName,
        string facetScope,
        SearchableIndexKind facetKind,
        byte[] requestFingerprint,
        byte[] layoutFingerprint,
        int protocolVersion,
        int orderingVersion,
        int workPolicyVersion,
        int layoutFormatVersion,
        long workBudget,
        int byteLimit,
        object request)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentException.ThrowIfNullOrWhiteSpace(facetScope);
        ArgumentNullException.ThrowIfNull(requestFingerprint);
        ArgumentNullException.ThrowIfNull(layoutFingerprint);
        QueryPlanValidator.Validate(query);
        if (facetKind is not SearchableIndexKind.Hash and not SearchableIndexKind.Range)
        {
            throw new ArgumentOutOfRangeException(nameof(request), facetKind, "Unknown facet index kind.");
        }

        if (requestFingerprint.Length != 32 || layoutFingerprint.Length != 32)
        {
            throw new ArgumentException("Facet fingerprints must contain exactly 32 bytes.", nameof(request));
        }

        if (protocolVersion != QueryProtocol.PagingVersion
            || orderingVersion != QueryProtocol.FacetValueOrderingVersion
            || workPolicyVersion != QueryProtocol.FacetWorkPolicyVersion
            || layoutFormatVersion != StorageLayout.CurrentFormatVersion)
        {
            throw new ArgumentException("Facet request protocol metadata is incompatible.", nameof(request));
        }

        if (workBudget <= 0 || workBudget > SearchableStorageQueryOptions.MaximumPartitionWorkBudget)
        {
            throw new ArgumentOutOfRangeException(nameof(request), workBudget, "Invalid facet work budget.");
        }

        if (byteLimit <= 0 || byteLimit > SearchableStorageQueryOptions.MaximumPartitionResponseBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(request), byteLimit, "Invalid facet byte limit.");
        }
    }

    private static void ValidateFacetLayout(
        int requestedFormatVersion,
        byte[] requestedFingerprint,
        StorageLayoutSnapshot snapshot,
        object request)
    {
        var actual = StorageLayoutFingerprint.Compute(snapshot);
        if (requestedFormatVersion != snapshot.FormatVersion
            || !StorageLayoutFingerprint.Equals(actual, requestedFingerprint))
        {
            throw new ArgumentException(
                "The partition facet layout binding does not match authoritative routing.",
                nameof(request));
        }
    }

    private static byte[] ValidateFacetFingerprint(
        string stateName,
        PartitionQueryPlan query,
        string facetScope,
        SearchableIndexKind facetKind,
        byte[] supplied,
        object request)
    {
        var computed = FacetQueryFingerprint.Compute(stateName, query, facetScope, facetKind);
        if (!QueryPlanFingerprint.Equals(computed, supplied))
        {
            throw new ArgumentException(
                "The partition facet fingerprint does not match its query and selected index.",
                nameof(request));
        }

        return computed;
    }

    private long ValidateFacetDataVersion(bool hasExpected, long expected)
    {
        var current = Persistence.CommittedSequence;
        if (hasExpected && current != expected)
        {
            throw new StorageFacetDataChangedException(expected, current);
        }

        return current;
    }

    private async Task CommitAsync(
        StorageJournalEntry entry,
        StoragePartitionMoveControl? moveControl = null)
    {
        try
        {
            await Persistence.CommitAsync(entry, moveControl);
        }
        catch
        {
            PoisonActivation();
            throw;
        }
    }

    private void ApplyCommittedImports(IReadOnlyList<StorageMoveRecord> imports)
    {
        try
        {
            StorageMovePageOperations.ApplyImports(_view, imports);
        }
        catch
        {
            // The manifest is already durable. Reconstruct the whole activation rather than
            // exposing a partially applied import page.
            PoisonActivation();
            throw;
        }
    }

    private void ApplyCommittedMoveDeletes(IReadOnlyList<StorageMoveDeleteRecord> deletes)
    {
        try
        {
            StorageMovePageOperations.ApplyDeletes(_view, deletes);
        }
        catch
        {
            // The manifest is already durable. Reconstruct the whole activation rather than
            // exposing a partially applied delete page.
            PoisonActivation();
            throw;
        }
    }

    private void ApplyCommittedUpsert(
        string recordKey,
        StoredRecord storedRecord)
    {
        try
        {
            _view.ApplyUpsert(recordKey, storedRecord);
        }
        catch
        {
            // The manifest is already durable. Any unexpected local apply failure poisons this
            // activation so the next call reconstructs records and indexes from committed storage.
            PoisonActivation();
            throw;
        }
    }

    private void ApplyCommittedDelete(string recordKey)
    {
        try
        {
            _view.ApplyDelete(recordKey);
        }
        catch
        {
            PoisonActivation();
            throw;
        }
    }

    private async Task<StorageLayoutSnapshot> ValidatePointRouteAsync(int slot, long expectedEpoch)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(slot);
        EnsureRoutingEpochAccepted(expectedEpoch);
        var snapshot = await GetRoutingSnapshotAsync(expectedEpoch);
        if (slot >= snapshot.VirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slot),
                slot,
                $"A routed slot must be less than the persisted virtual-slot count {snapshot.VirtualSlotCount}.");
        }

        var currentOwner = snapshot.GetOwner(slot);
        if (snapshot.Epoch != expectedEpoch || currentOwner != _partitionIndex)
        {
            throw new StorageRouteMismatchException(
                expectedEpoch,
                snapshot.Epoch,
                _partitionIndex,
                slot,
                currentOwner);
        }

        return snapshot;
    }

    private void ValidateRoutedRecordIdentity(
        string recordKey,
        GrainId grainId,
        int routedSlot,
        StorageLayoutSnapshot snapshot,
        string parameterName)
    {
        if (grainId.IsDefault)
        {
            throw new ArgumentException("A routed point operation must identify a grain.", parameterName);
        }

        var derivedSlot = StorageLayout.GetSlot(grainId, snapshot.VirtualSlotCount);
        if (derivedSlot != routedSlot)
        {
            throw new ArgumentException(
                $"The routed slot {routedSlot} does not match the grain's derived slot {derivedSlot}.",
                parameterName);
        }

        if (_view.Records.TryGetValue(recordKey, out var existing)
            && !existing.GrainId.Equals(grainId))
        {
            throw new InvalidOperationException(
                $"Stored record '{recordKey}' identifies a different grain than the routed request.");
        }
    }

    private async Task<StorageLayoutSnapshot> ValidateQueryRouteAsync(long expectedEpoch)
    {
        EnsureRoutingEpochAccepted(expectedEpoch);
        var snapshot = await GetRoutingSnapshotAsync(expectedEpoch);
        var isCurrentOwner = snapshot.ContainsOwner(_partitionIndex);
        if (snapshot.Epoch != expectedEpoch || !isCurrentOwner)
        {
            throw new StorageRouteMismatchException(
                expectedEpoch,
                snapshot.Epoch,
                _partitionIndex);
        }

        return snapshot;
    }

    private async Task<StorageLayoutSnapshot> GetRoutingSnapshotAsync(long expectedEpoch)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(expectedEpoch);
        var current = await GetRequiredRoutingSnapshotAsync();
        if (expectedEpoch != current.Epoch)
        {
            RoutingCache.Invalidate(current);
            current = await GetRequiredRoutingSnapshotAsync();
        }

        return current;
    }

    private async Task<StorageLayoutSnapshot> GetRequiredRoutingSnapshotAsync()
    {
        var current = await RoutingCache.GetAsync()
            ?? throw new InvalidOperationException(
                $"Searchable storage provider '{_providerName}' has no initialized routing layout.");
        ValidateRoutingSnapshot(current);
        return current;
    }

    private void ValidateRoutingSnapshot(StorageLayoutSnapshot snapshot)
    {
        if (snapshot.FormatVersion != StorageLayout.CurrentFormatVersion
            || !string.Equals(snapshot.ProviderName, _providerName, StringComparison.Ordinal)
            || snapshot.InitialPartitionCount <= 0
            || snapshot.VirtualSlotCount <= 0
            || snapshot.VirtualSlotCount > StorageLayout.MaximumVirtualSlotCount
            || snapshot.Epoch <= 0)
        {
            throw new InvalidOperationException(
                $"Searchable storage partition '{this.GetPrimaryKeyString()}' received an invalid routing snapshot.");
        }
    }

    private GrainId[] ResolveGrainIds(
        IEnumerable<string> recordKeys,
        StorageLayoutSnapshot? routing = null)
    {
        var records = recordKeys.Select(GetIndexedRecord);
        if (routing is not null)
        {
            records = records.Where(record =>
            {
                var slot = StorageLayout.GetSlot(record.GrainId, routing.VirtualSlotCount);
                return routing.GetOwner(slot) == _partitionIndex;
            });
        }

        return records
            .Select(static record => record.GrainId)
            .Distinct()
            .ToArray();
    }

    private StoredRecord GetIndexedRecord(string recordKey)
    {
        if (_view.Records.TryGetValue(recordKey, out var record))
        {
            return record;
        }

        throw new InvalidOperationException(
            $"A derived index in partition '{this.GetPrimaryKeyString()}' references missing record '{recordKey}'.");
    }

    internal static (string ProviderName, int PartitionIndex) ParsePartitionKey(string partitionKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        var separator = partitionKey.LastIndexOf(':');
        var suffixLength = partitionKey.Length - separator - 1;
        if (separator <= 0
            || suffixLength != 8
            || !int.TryParse(
                partitionKey.AsSpan(separator + 1),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var partitionIndex))
        {
            throw new InvalidOperationException(
                $"Storage partition key '{partitionKey}' must end with a colon and an eight-digit partition index.");
        }

        return (partitionKey[..separator], partitionIndex);
    }

    private void EnsureUsable()
    {
        if (!_usable)
        {
            throw new InvalidOperationException(
                "The storage partition activation is retiring after an ambiguous persistence outcome.");
        }
    }

    private void EnsureLegacyOperationAllowed()
    {
        if (Persistence.RoutedOperationsRequired)
        {
            throw new InvalidOperationException(
                "Legacy placement-based partition operations are disabled after live slot movement is enabled.");
        }
    }

    private void EnsureRoutingEpochAccepted(long expectedEpoch)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(expectedEpoch);
        var minimumEpoch = Persistence.MinimumRoutingEpoch;
        if (expectedEpoch < minimumEpoch)
        {
            throw new StorageRouteMismatchException(
                expectedEpoch,
                minimumEpoch,
                _partitionIndex);
        }
    }

    private void EnsureSlotMutationAllowed(int slot)
    {
        var move = Persistence.MoveControl;
        if (!move.IsPresent || move.Slot != slot)
        {
            return;
        }

        if (move.Role == StoragePartitionMoveRole.Target
            && move.Phase == StoragePartitionMovePhase.TargetEnabled)
        {
            return;
        }

        throw new InvalidOperationException(
            $"Virtual slot {slot} is mutation-frozen by move '{move.MoveId}'.");
    }

    private void PoisonActivation()
    {
        _usable = false;
        DeactivateOnIdle();
    }

    private static void EnsureETagMatches(
        string recordKey,
        string? storedETag,
        string? expectedETag,
        string operation)
    {
        if (string.Equals(storedETag, expectedETag, StringComparison.Ordinal))
        {
            return;
        }

        throw new InconsistentStateException(
            $"Version conflict during {operation} for searchable storage record '{recordKey}'.",
            storedETag,
            expectedETag);
    }
}
