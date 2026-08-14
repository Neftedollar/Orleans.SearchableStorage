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
    private readonly SearchableStateRegistry _stateRegistry;
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
        StorageLayoutCacheRegistry layoutCaches,
        SearchableStateRegistry stateRegistry)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(layoutCaches);
        ArgumentNullException.ThrowIfNull(stateRegistry);
        _manifest = manifest;
        _logger = logger;
        _layoutCaches = layoutCaches;
        _stateRegistry = stateRegistry;
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
            _logger,
            _providerName);
        var records = await _persistence.ActivateAsync();
        StoredRecordNamespaceValidation.ValidateAll(records.Values, _persistence.NamespaceMode);
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
        EnsureNamespaceMode(StorageNamespaceMode.Integrated);
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        StorageCapacityGuardrails.ValidateRecordKey(recordKey);

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
            Payload = [.. (record.Payload
                ?? throw new InvalidOperationException(
                    "An integrated storage record is missing its application payload."))],
            ETag = record.ETag,
        };
    }

    public async Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        StorageCapacityGuardrails.ValidateGrainId(request.GrainId);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RecordKey);
        StorageCapacityGuardrails.ValidateRecordKey(request.RecordKey);

        var snapshot = await ValidatePointRouteAsync(request.Slot, request.Epoch);
        EnsureNamespaceMode(snapshot, StorageNamespaceMode.Integrated);
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
        if (request.Persistence.NamespaceMode != StorageNamespaceMode.Integrated)
        {
            throw new InvalidOperationException(
                "Index-only mutations require the routed writer protocol.");
        }
        EnsureNamespaceMode(StorageNamespaceMode.Integrated);
        return await WriteCoreAsync(request);
    }

    private async Task<string> WriteCoreAsync(StorageWriteRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RecordKey);
        ArgumentNullException.ThrowIfNull(request.IndexEntries);
        if (request.Unconditional != (request.Persistence.NamespaceMode == StorageNamespaceMode.IndexOnly))
        {
            throw new ArgumentException(
                "Only an index-only writer can issue an unconditional payload-free write.",
                nameof(request));
        }
        if (request.Persistence.NamespaceMode == StorageNamespaceMode.Integrated)
        {
            ArgumentNullException.ThrowIfNull(request.Payload);
        }
        StorageCapacityGuardrails.ValidateWriteRequest(request);
        StoragePartitionPersistence.ValidateSettings(request.Persistence);
        Persistence.EnsureSettingsMatch(request.Persistence);
        ValidateManagedSchemaBinding(
            request.StateName,
            request.IndexSchemaFingerprint,
            request.IndexSchemaProtocolVersion,
            request.RecordKey);
        if (request.IndexSchemaFingerprint is { } writeFingerprint
            && request.IndexEntries.Any(
                entry => !IndexSchemaIdentity.IsBoundScope(entry.Scope, writeFingerprint)))
        {
            throw new ArgumentException(
                "A managed write contains an index scope from a different schema generation.",
                nameof(request));
        }
        _view.Records.TryGetValue(request.RecordKey, out var currentRecord);
        if (request.IndexSchemaFingerprint is { } activeFingerprint
            && currentRecord is not null
            && (currentRecord.IndexSchemaFingerprint is null
                || !IndexSchemaIdentity.FixedTimeEquals(
                    currentRecord.IndexSchemaFingerprint,
                    activeFingerprint)))
        {
            throw new InvalidOperationException(
                "A managed write encountered a record outside the active schema generation.");
        }

        if (!request.Unconditional)
        {
            EnsureETagMatches(request.RecordKey, currentRecord?.ETag, request.ExpectedETag, "write");
        }

        var nextVersion = Persistence.NextVersion;
        var etag = nextVersion.ToString(CultureInfo.InvariantCulture);
        var storedRecord = StoragePersistenceStateCopy.CopyRecord(new StoredRecord
        {
            GrainId = request.GrainId,
            Payload = request.Payload,
            ETag = etag,
            IndexEntries = request.IndexEntries,
            IndexSchemaFingerprint = request.IndexSchemaFingerprint is null
                ? null
                : [.. request.IndexSchemaFingerprint],
        })!;
        StoredRecordNamespaceValidation.Validate(storedRecord, request.Persistence.NamespaceMode);
        StoragePartitionIndexValidation.ValidateRecord(storedRecord);
        _view.ValidateProjectedUpsert(request.RecordKey, storedRecord);

        var validationEntry = CreateMutationJournalValidationEntry(
            StorageJournalOperation.Upsert,
            request.RecordKey,
            request.Unconditional ? currentRecord?.ETag : request.ExpectedETag,
            storedRecord,
            checked(nextVersion + 1));
        await PrepareForMutationAsync(request.Persistence, validationEntry);
        var entry = new StorageJournalEntry
        {
            Sequence = Persistence.NextSequence,
            WriterEpoch = Persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = request.RecordKey,
            ExpectedETag = request.Unconditional ? currentRecord?.ETag : request.ExpectedETag,
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
        StorageCapacityGuardrails.ValidateWriteRequest(request.Request);

        var snapshot = await ValidatePointRouteAsync(request.Slot, request.Epoch);
        EnsureNamespaceMode(snapshot, request.Request.Persistence.NamespaceMode);
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
        if (request.Persistence.NamespaceMode != StorageNamespaceMode.Integrated)
        {
            throw new InvalidOperationException(
                "Index-only mutations require the routed writer protocol.");
        }
        EnsureNamespaceMode(StorageNamespaceMode.Integrated);
        await ClearCoreAsync(request);
    }

    private async Task ClearCoreAsync(StorageClearRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RecordKey);
        if (request.Unconditional != (request.Persistence.NamespaceMode == StorageNamespaceMode.IndexOnly))
        {
            throw new ArgumentException(
                "Only an index-only writer can issue an unconditional clear.",
                nameof(request));
        }
        StorageCapacityGuardrails.ValidateRecordKey(request.RecordKey);
        StoragePartitionPersistence.ValidateSettings(request.Persistence);
        Persistence.EnsureSettingsMatch(request.Persistence);
        ValidateManagedSchemaBinding(
            request.StateName,
            request.IndexSchemaFingerprint,
            request.IndexSchemaProtocolVersion,
            request.RecordKey);

        if (!_view.Records.TryGetValue(request.RecordKey, out var currentRecord))
        {
            if (!request.Unconditional)
            {
                EnsureETagMatches(request.RecordKey, null, request.ExpectedETag, "clear");
            }
            return;
        }

        if (request.IndexSchemaFingerprint is { } activeFingerprint
            && (currentRecord.IndexSchemaFingerprint is null
                || !IndexSchemaIdentity.FixedTimeEquals(
                    currentRecord.IndexSchemaFingerprint,
                    activeFingerprint)))
        {
            throw new InvalidOperationException(
                "A managed clear encountered a record outside the active schema generation.");
        }

        if (!request.Unconditional)
        {
            EnsureETagMatches(request.RecordKey, currentRecord.ETag, request.ExpectedETag, "clear");
        }
        var validationEntry = CreateMutationJournalValidationEntry(
            StorageJournalOperation.Delete,
            request.RecordKey,
            request.Unconditional ? currentRecord.ETag : request.ExpectedETag,
            record: null,
            Persistence.NextVersion);
        await PrepareForMutationAsync(request.Persistence, validationEntry);
        var entry = new StorageJournalEntry
        {
            Sequence = Persistence.NextSequence,
            WriterEpoch = Persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Delete,
            RecordKey = request.RecordKey,
            ExpectedETag = request.Unconditional ? currentRecord.ETag : request.ExpectedETag,
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
        StorageCapacityGuardrails.ValidateGrainId(request.GrainId);
        StorageCapacityGuardrails.ValidateRecordKey(request.Request.RecordKey);

        // Route validation deliberately precedes the missing-record fast path in ClearAsync. A
        // stale owner must not acknowledge a clear while the authoritative record lives elsewhere.
        var snapshot = await ValidatePointRouteAsync(request.Slot, request.Epoch);
        EnsureNamespaceMode(snapshot, request.Request.Persistence.NamespaceMode);
        ValidateRoutedRecordIdentity(
            request.Request.RecordKey,
            request.GrainId,
            request.Slot,
            snapshot,
            nameof(request));
        EnsureSlotMutationAllowed(request.Slot);
        await ClearCoreAsync(request.Request);
    }

    public async Task<StorageIndexSchemaRebuildPageResult> RebuildIndexSchemaPageAsync(
        StorageIndexSchemaRebuildPageRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.ProviderName);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.StateName);
        ValidateRebuildPageSize(request.PageSize);
        ValidateRebuildPageFrontier(request);

        IndexSchemaIdentity.ValidateIdentity(request.SchemaKey, nameof(request));
        IndexSchemaIdentity.ValidateIdentity(request.TargetFingerprint, nameof(request));
        StoragePartitionPersistence.ValidateSettings(request.Persistence);
        Persistence.EnsureSettingsMatch(request.Persistence);
        if (Persistence.IndexSchemaProtocolVersion != StorageIndexSchema.ProtocolVersion)
        {
            throw new InvalidOperationException(
                "The partition index-schema protocol has not been durably enabled.");
        }
        if (Persistence.MoveControl.IsPresent)
        {
            throw new InvalidOperationException(
                "Index-schema rebuild cannot touch a partition while a slot move is active.");
        }
        if (Persistence.NamespaceMode == StorageNamespaceMode.IndexOnly
            && _view.Records.Count != 0)
        {
            throw new SearchableStorageIndexSchemaException(
                "An index-only partition cannot rebuild indexes from application payloads. "
                + "Replay the authoritative external corpus into a new index namespace.");
        }
        if (!string.Equals(request.ProviderName, _providerName, StringComparison.Ordinal))
        {
            throw new ArgumentException("The schema rebuild targets a different provider.", nameof(request));
        }

        var registration = _stateRegistry.Find(request.ProviderName, request.StateName)
            ?? throw new InvalidOperationException(
                $"No searchable state registration exists for provider '{request.ProviderName}' "
                + $"and state '{request.StateName}'.");
        if (!IndexSchemaIdentity.FixedTimeEquals(registration.Schema.SchemaKey, request.SchemaKey)
            || !IndexSchemaIdentity.FixedTimeEquals(
                registration.Schema.Fingerprint,
                request.TargetFingerprint))
        {
            throw new InvalidOperationException(
                "The registered schema declaration does not match the requested index schema "
                + "(state type, index metadata, or application schema version).");
        }

        _ = await ValidateQueryRouteAsync(request.LayoutEpoch);
        var catalog = _view.OrderedIndexes.GetStateCatalog(request.StateName);
        var page = new List<GrainId>(request.PageSize);
        bool exhausted;
        using (var cursor = catalog.CreateCursorAfter(request.HasAfter, request.After))
        {
            while (page.Count < request.PageSize
                   && cursor.TakeCurrentAndAdvance(out var grainId))
            {
                page.Add(grainId);
            }

            exhausted = !cursor.HasCurrent;
        }

        var hasAfter = request.HasAfter;
        var after = request.After;
        foreach (var grainId in page)
        {
            if (!catalog.TryGetRecordKeys(grainId, out var recordKeys)
                || recordKeys.Count != 1)
            {
                throw new InvalidOperationException(
                    "A managed state catalog must contain exactly one record per GrainId.");
            }

            var recordKey = recordKeys.Single();
            var current = _view.Records[recordKey];
            if (!IndexSchemaIdentity.FixedTimeEquals(
                    current.IndexSchemaFingerprint ?? [],
                    request.TargetFingerprint))
            {
                IReadOnlyList<IndexEntry> indexes;
                try
                {
                    var payload = current.Payload
                        ?? throw new InvalidOperationException(
                            "An integrated schema-rebuild record is missing its application payload.");
                    indexes = _stateRegistry.Extract(
                        request.ProviderName,
                        request.StateName,
                        payload,
                        request.TargetFingerprint);
                }
                catch (Exception exception) when (IsSchemaMaterializationFailure(exception))
                {
                    var partitionKey = StorageLayout.CreatePartitionKey(
                        _providerName,
                        _partitionIndex);
                    // Application serializers and indexed getters control exception messages and
                    // can include the payload or raw index values in them. Do not propagate that
                    // text (or retain the application exception as an inner exception) across the
                    // Orleans boundary. The stable location and failure type are sufficient to
                    // repair the responsible record while preserving data confidentiality.
                    throw new InvalidOperationException(
                        $"Index-schema rebuild could not materialize provider "
                        + $"'{request.ProviderName}', state '{request.StateName}', GrainId "
                        + $"'{current.GrainId}', record key '{recordKey}', physical owner "
                        + $"{_partitionIndex} ('{partitionKey}'). Restore a payload-compatible "
                        + "serializer and indexed state type, then resume the same rebuild. "
                        + $"Underlying failure type: {exception.GetType().FullName}. "
                        + "Application exception details are intentionally omitted.");
                }
                var replacement = new StoredRecord
                {
                    GrainId = current.GrainId,
                    Payload = [.. (current.Payload
                        ?? throw new InvalidOperationException(
                            "An integrated schema-rebuild record is missing its application payload."))],
                    ETag = current.ETag,
                    IndexEntries = [.. indexes],
                    IndexSchemaFingerprint = [.. request.TargetFingerprint],
                };
                StoragePartitionIndexValidation.ValidateRecord(replacement);
                _view.ValidateProjectedUpsert(recordKey, replacement);
                var validationEntry = CreateMutationJournalValidationEntry(
                    StorageJournalOperation.Reindex,
                    recordKey,
                    current.ETag,
                    replacement,
                    Persistence.NextVersion);
                await PrepareForMutationAsync(request.Persistence, validationEntry);
                var entry = new StorageJournalEntry
                {
                    Sequence = Persistence.NextSequence,
                    WriterEpoch = Persistence.WriterEpoch,
                    OperationId = Guid.NewGuid(),
                    PreviousOperationId = Persistence.CommittedOperationId,
                    Operation = StorageJournalOperation.Reindex,
                    RecordKey = recordKey,
                    ExpectedETag = current.ETag,
                    Record = replacement,
                    NextVersionAfter = Persistence.NextVersion,
                };
                await CommitAsync(entry);
                ApplyCommittedUpsert(recordKey, replacement);
            }

            hasAfter = true;
            after = grainId;
        }

        await Persistence.CompactIfRequiredAsync(
            _view.Records,
            request.Persistence.CompactionThreshold);
        return new StorageIndexSchemaRebuildPageResult
        {
            Exhausted = exhausted,
            HasAfter = hasAfter,
            After = after,
            ProcessedRecordCount = page.Count,
        };
    }

    private static bool IsSchemaMaterializationFailure(Exception exception)
    {
        // These failures originate in application serialization or indexed property getters. A
        // plain framework exception is deliberate: arbitrary application exception graphs do not
        // necessarily cross an Orleans proxy, while operators still need enough context to repair
        // the payload or deployment and resume the durable cursor. Fatal process failures retain
        // their original behavior.
        return exception is not OutOfMemoryException
            and not StackOverflowException
            and not AccessViolationException;
    }

    public async Task<StoragePartitionProtocolState> EnableIndexSchemaProtocolAsync(
        StorageIndexSchemaPartitionProtocolRequest request)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.ProviderName);
        if (request.ProtocolVersion != StorageIndexSchema.ProtocolVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.ProtocolVersion,
                "Unknown index-schema protocol version.");
        }

        IndexSchemaIdentity.ValidateIdentity(request.LayoutFingerprint, nameof(request));
        StoragePartitionPersistence.ValidateSettings(request.Persistence);
        if (!string.Equals(request.ProviderName, _providerName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The index-schema protocol request targets a different provider.",
                nameof(request));
        }

        // Movement intents can start and retire without changing the routing epoch. Schema
        // maintenance therefore needs an authoritative read instead of a same-epoch cached
        // snapshot, otherwise a completed move can remain falsely visible until cache eviction.
        var routing = await ValidateFreshQueryRouteAsync(request.LayoutEpoch);
        if (routing.NamespaceMode != request.Persistence.NamespaceMode)
        {
            throw new ArgumentException(
                "The index-schema protocol request namespace mode does not match the persisted routing layout.",
                nameof(request));
        }

        var layoutFingerprint = StorageLayoutFingerprint.Compute(routing);
        if (!IndexSchemaIdentity.FixedTimeEquals(
                layoutFingerprint,
                request.LayoutFingerprint))
        {
            throw new StorageRouteMismatchException(
                request.LayoutEpoch,
                routing.Epoch,
                _partitionIndex);
        }

        if (routing.CopyMovementEnablement() is not null
            || routing.CopyMoveIntent() is not null
            || Persistence.MoveControl.IsPresent)
        {
            throw new InvalidOperationException(
                "Index-schema enablement cannot run while virtual-slot movement is active.");
        }

        await Persistence.EnableIndexSchemaProtocolAsync(request.Persistence);
        return Persistence.CreateProtocolState();
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
        ValidateManagedQueryBinding(
            query.StateName,
            query.IndexSchemaFingerprint,
            query.IndexSchemaProtocolVersion,
            query.Query.Scope);
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
        ValidateManagedQueryBinding(
            query.StateName,
            query.IndexSchemaFingerprint,
            query.IndexSchemaProtocolVersion,
            query.Query.Scope);
        return ResolveGrainIds(FindRangeRecordKeys(query.Query), snapshot);
    }

    public Task<GrainId[]> QueryAsync(PartitionQueryPlan query)
    {
        EnsureUsable();
        EnsureLegacyOperationAllowed();
        ArgumentNullException.ThrowIfNull(query);
        // StoragePartitionGrain is non-reentrant. Evaluating the complete plan synchronously in
        // this call gives AND and OR one serially consistent partition-local view.
        var recordKeys = StoragePartitionQueryEvaluator.Evaluate(query, _view.OrderedIndexes);
        return Task.FromResult(ResolveGrainIds(recordKeys));
    }

    public async Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(query.Query);
        QueryPlanValidator.Validate(query.Query);

        var snapshot = await ValidateQueryRouteAsync(query.Epoch);
        ValidateManagedQueryBinding(
            query.StateName,
            query.IndexSchemaFingerprint,
            query.IndexSchemaProtocolVersion,
            query.Query);
        // Ownership filtering is part of the same non-reentrant call as plan evaluation, so the
        // result cannot mix two activation-local record views.
        return ResolveGrainIds(
            StoragePartitionQueryEvaluator.EvaluateValidated(query.Query, _view.OrderedIndexes),
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
        ValidateManagedQueryBinding(
            request.StateName,
            request.IndexSchemaFingerprint,
            request.IndexSchemaProtocolVersion,
            request.Query);
        if (!StorageLayout.AreRoutingFormatsCompatible(request.LayoutFormatVersion, snapshot.FormatVersion))
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
        ValidateManagedQueryBinding(
            request.StateName,
            request.IndexSchemaFingerprint,
            request.IndexSchemaProtocolVersion,
            request.Query,
            request.FacetScope);
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
        ValidateManagedQueryBinding(
            request.StateName,
            request.IndexSchemaFingerprint,
            request.IndexSchemaProtocolVersion,
            request.Query,
            request.FacetScope);
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
        ValidateManagedQueryBinding(
            request.StateName,
            request.IndexSchemaFingerprint,
            request.IndexSchemaProtocolVersion,
            request.Query,
            request.FacetScope);
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
            || request.MinimumRoutingEpoch < routing.Epoch
            || routing.NamespaceMode != request.NamespaceMode)
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
            NamespaceMode = request.NamespaceMode,
        };
        await Persistence.EnableMovementProtocolAsync(
            settings,
            request.MinimumRoutingEpoch,
            request.IndexSchemaProtocolVersion);
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

        var advancePayload = CreateAdvancePayload(request.Move, request.FrozenNextVersion);
        ValidateMovementJournalPayloadCapacity(
            StorageJournalOperation.AdvanceVersion,
            advancePayload);
        await PrepareForProtocolMutationAsync();
        var advanced = current.Copy();
        advanced.Phase = StoragePartitionMovePhase.TargetImporting;
        var entry = CreateMoveJournalEntry(
            StorageJournalOperation.AdvanceVersion,
            advancePayload,
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


        ValidateImportedSchemaBindings(
            _providerName,
            Persistence.IndexSchemaProtocolVersion,
            _stateRegistry,
            request.Page.Records);
        ValidateImportedNamespaceMode(request.Page.Records, Persistence.NamespaceMode);

        StorageMovePageOperations.ValidateImportAgainstCurrentView(
            _view,
            request.Page,
            current,
            Persistence.NextVersion);
        _view.ValidateProjectedImports(request.Page.Records);
        var payload = CreateImportPayload(request.Page);
        ValidateMovementJournalPayloadCapacity(StorageJournalOperation.Import, payload);
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
        ValidateMovementJournalPayloadCapacity(StorageJournalOperation.MoveDelete, payload);
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
        return _view.OrderedIndexes.ResolveRecordKeys(
            _view.OrderedIndexes.FindExactRecordRefs(query.Scope, query.Kind, query.Value));
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

        var recordRefs = new HashSet<int>();
        _view.OrderedIndexes.UnionRangeRecordRefs(
            query.Scope,
            query.LowerBound,
            query.UpperBound,
            query.IncludeLowerBound,
            query.IncludeUpperBound,
            recordRefs);
        return _view.OrderedIndexes.ResolveRecordKeys(recordRefs);
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
        StorageCapacityGuardrails.ValidatePersistenceConfiguration(
            request.JournalSegmentCapacity,
            request.MaximumJournalReplayEntries,
            nameof(request.JournalSegmentCapacity),
            nameof(request.MaximumJournalReplayEntries));
        if (request.IndexSchemaProtocolVersion is not 0
            and not StorageIndexSchema.ProtocolVersion)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.IndexSchemaProtocolVersion,
                "Unknown index-schema protocol version.");
        }
        if (!Enum.IsDefined(request.NamespaceMode))
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.NamespaceMode,
                "Unknown searchable-storage namespace mode.");
        }
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
        if (!StoragePersistence.SupportsMovement(protocol.PersistenceFormatVersion)
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
        if (request.AfterRecordKey is not null)
        {
            StorageCapacityGuardrails.ValidateRecordKeyBytes(request.AfterRecordKey);
        }
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
        if (request.AfterRecordKey is not null)
        {
            StorageCapacityGuardrails.ValidateRecordKeyBytes(request.AfterRecordKey);
        }
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
        _ = StorageCapacityGuardrails.ValidateJournalEntry(validationEntry);
    }

    private static void ValidateMovementJournalPayloadCapacity(
        StorageJournalOperation operation,
        StorageMoveJournalPayload payload)
    {
        var validationEntry = new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = operation,
            RecordKey = string.Empty,
            NextVersionAfter = 1,
            Move = payload,
        };
        _ = StorageCapacityGuardrails.ValidateJournalEntry(validationEntry);
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

    private async Task PrepareForMutationAsync(
        StoragePersistenceSettings settings,
        StorageJournalEntry prospectiveEntry)
    {
        await StorageMutationAdmission.PrepareAsync(
            prospectiveEntry,
            () => Persistence.PrepareForMutationAsync(_view.Records, settings),
            PoisonActivation);
    }

    private static StorageJournalEntry CreateMutationJournalValidationEntry(
        StorageJournalOperation operation,
        string recordKey,
        string? expectedEtag,
        StoredRecord? record,
        long nextVersionAfter)
    {
        // Sequence, epoch, and operation identifiers have fixed canonical widths. These valid
        // placeholders therefore make the capacity measure byte-for-byte equivalent to the entry
        // built after writer-epoch acquisition, while allowing rejection before durable authority.
        return new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = operation,
            RecordKey = recordKey,
            ExpectedETag = expectedEtag,
            Record = record,
            NextVersionAfter = nextVersionAfter,
        };
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

        if (!StorageLayout.IsRoutingFormatVersion(request.LayoutFormatVersion))
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

        if (request.HasAfter)
        {
            StorageCapacityGuardrails.ValidateGrainId(request.After);
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
            || !StorageLayout.IsRoutingFormatVersion(layoutFormatVersion))
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
        if (!StorageLayout.AreRoutingFormatsCompatible(requestedFormatVersion, snapshot.FormatVersion)
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

    internal static void ValidateRebuildPageSize(int pageSize)
    {
        if (pageSize <= 0 || pageSize > StorageIndexSchema.RebuildPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                pageSize,
                $"A schema rebuild page size must be between 1 and {StorageIndexSchema.RebuildPageSize}.");
        }
    }

    internal static void ValidateRebuildPageFrontier(StorageIndexSchemaRebuildPageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.HasAfter == request.After.IsDefault)
        {
            throw new ArgumentException(
                "A schema rebuild frontier must be present exactly when HasAfter is true.",
                nameof(request));
        }

        if (request.HasAfter)
        {
            StorageCapacityGuardrails.ValidateGrainId(request.After);
        }
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

    private void EnsureNamespaceMode(StorageNamespaceMode expected)
    {
        if (Persistence.NamespaceMode != expected)
        {
            throw new InvalidOperationException(
                $"Partition namespace mode {Persistence.NamespaceMode} cannot serve a {expected} operation.");
        }
    }

    private void EnsureNamespaceMode(
        StorageLayoutSnapshot snapshot,
        StorageNamespaceMode expected)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.NamespaceMode != expected)
        {
            throw new InvalidOperationException(
                $"Layout namespace mode {snapshot.NamespaceMode} cannot serve a {expected} operation.");
        }

        EnsureNamespaceMode(expected);
    }

    private static void ValidateImportedNamespaceMode(
        IReadOnlyList<StorageMoveRecord> imports,
        StorageNamespaceMode namespaceMode)
    {
        ArgumentNullException.ThrowIfNull(imports);
        foreach (var item in imports)
        {
            StoredRecordNamespaceValidation.Validate(
                StorageMoveRecordCodec.Decode(item.Record),
                namespaceMode);
        }
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
        return ValidateQueryRoute(snapshot, expectedEpoch);
    }

    private async Task<StorageLayoutSnapshot> ValidateFreshQueryRouteAsync(long expectedEpoch)
    {
        EnsureRoutingEpochAccepted(expectedEpoch);
        var snapshot = await RoutingCache.ReadFreshAsync()
            ?? throw new InvalidOperationException(
                $"Searchable storage provider '{_providerName}' has no initialized routing layout.");
        ValidateRoutingSnapshot(snapshot);
        return ValidateQueryRoute(snapshot, expectedEpoch);
    }

    private StorageLayoutSnapshot ValidateQueryRoute(
        StorageLayoutSnapshot snapshot,
        long expectedEpoch)
    {
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
        if (!StorageLayout.IsRoutingFormatVersion(snapshot.FormatVersion)
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


        if (Persistence.IndexSchemaProtocolVersion != 0)
        {
            throw new InvalidOperationException(
                "Legacy schema-unbound partition operations are disabled after managed index schemas are enabled.");
        }
    }

    internal static void ValidateImportedSchemaBindings(
        string providerName,
        int protocol,
        SearchableStateRegistry stateRegistry,
        IReadOnlyList<StorageMoveRecord> records)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(stateRegistry);
        ArgumentNullException.ThrowIfNull(records);
        foreach (var item in records)
        {
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            var record = StorageMoveRecordCodec.Decode(item.Record);
            var fingerprint = record.IndexSchemaFingerprint;
            if (protocol == 0)
            {
                if (fingerprint is not null)
                {
                    throw new InvalidOperationException(
                        "A movement target without the schema capability cannot import a managed record.");
                }

                continue;
            }

            var registration = protocol == StorageIndexSchema.ProtocolVersion
                && fingerprint is not null
                    ? stateRegistry.FindByFingerprint(providerName, fingerprint)
                    : null;
            if (registration is null)
            {
                throw new InvalidOperationException(
                    "A managed movement target received a record without a matching local state registration.");
            }

            if (!recordKey.StartsWith(
                    string.Concat(registration.StateName, "/"),
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "A managed movement target received a record key bound to a different state registration.");
            }

            if (record.IndexEntries.Any(
                    entry => !IndexSchemaIdentity.IsBoundScope(entry.Scope, fingerprint!)))
            {
                throw new InvalidOperationException(
                    "A managed movement target received an index scope from a different schema generation.");
            }
        }
    }

    private void ValidateManagedSchemaBinding(
        string? stateName,
        byte[]? fingerprint,
        int protocolVersion,
        string? recordKey = null)
    {
        var durableProtocol = Persistence.IndexSchemaProtocolVersion;
        if (durableProtocol == 0)
        {
            if (protocolVersion != 0 || fingerprint is not null)
            {
                throw new InvalidOperationException(
                    "A managed schema request reached a partition before its capability was enabled.");
            }

            return;
        }

        if (durableProtocol != StorageIndexSchema.ProtocolVersion
            || protocolVersion != durableProtocol
            || string.IsNullOrWhiteSpace(stateName)
            || fingerprint is null)
        {
            throw new InvalidOperationException(
                "The partition requires an explicit managed schema binding for this state.");
        }

        IndexSchemaIdentity.ValidateIdentity(fingerprint, nameof(fingerprint));
        var registration = _stateRegistry.Find(_providerName, stateName)
            ?? throw new InvalidOperationException(
                $"Searchable state '{stateName}' is not registered on this silo for provider '{_providerName}'.");
        if (!IndexSchemaIdentity.FixedTimeEquals(
                registration.Schema.Fingerprint,
                fingerprint))
        {
            throw new InvalidOperationException(
                $"Searchable state '{stateName}' does not match the request's managed schema generation.");
        }

        if (recordKey is not null
            && !recordKey.StartsWith(string.Concat(stateName, "/"), StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The record key does not belong to the managed state name.",
                nameof(recordKey));
        }
    }

    private void ValidateManagedQueryBinding(
        string? stateName,
        byte[]? fingerprint,
        int protocolVersion,
        string scope)
    {
        ValidateManagedSchemaBinding(stateName, fingerprint, protocolVersion);
        if (fingerprint is not null && !IndexSchemaIdentity.IsBoundScope(scope, fingerprint))
        {
            throw new ArgumentException(
                "The query scope does not belong to its managed schema generation.",
                nameof(scope));
        }
    }

    private void ValidateManagedQueryBinding(
        string? stateName,
        byte[]? fingerprint,
        int protocolVersion,
        PartitionQueryPlan query,
        string? additionalScope = null)
    {
        ValidateManagedSchemaBinding(stateName, fingerprint, protocolVersion);
        if (fingerprint is null)
        {
            return;
        }

        ValidateManagedPlanScopes(query, fingerprint);
        if (additionalScope is not null
            && !IndexSchemaIdentity.IsBoundScope(additionalScope, fingerprint))
        {
            throw new ArgumentException(
                "The facet scope does not belong to its managed schema generation.",
                nameof(additionalScope));
        }
    }

    private static void ValidateManagedPlanScopes(PartitionQueryPlan query, byte[] fingerprint)
    {
        if (query.Scope is not null
            && !IndexSchemaIdentity.IsBoundScope(query.Scope, fingerprint))
        {
            throw new ArgumentException(
                "The query plan contains a scope from a different schema generation.",
                nameof(query));
        }

        if (query.Left is not null)
        {
            ValidateManagedPlanScopes(query.Left, fingerprint);
        }

        if (query.Right is not null)
        {
            ValidateManagedPlanScopes(query.Right, fingerprint);
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

/// <summary>Keeps pure request admission outside the persistence-failure poison fence.</summary>
internal static class StorageMutationAdmission
{
    public static async Task PrepareAsync(
        StorageJournalEntry prospectiveEntry,
        Func<Task> prepareAuthority,
        Action poisonAuthorityFailure)
    {
        ArgumentNullException.ThrowIfNull(prospectiveEntry);
        ArgumentNullException.ThrowIfNull(prepareAuthority);
        ArgumentNullException.ThrowIfNull(poisonAuthorityFailure);

        _ = StorageCapacityGuardrails.ValidateJournalEntry(prospectiveEntry);
        try
        {
            await prepareAuthority();
        }
        catch
        {
            poisonAuthorityFailure();
            throw;
        }
    }
}
