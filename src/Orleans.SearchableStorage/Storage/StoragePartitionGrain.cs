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
        _view = new StoragePartitionView(await _persistence.ActivateAsync());
        _usable = true;
        await base.OnActivateAsync(cancellationToken);
    }

    public Task<StorageReadResult> ReadAsync(string recordKey)
    {
        EnsureUsable();
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);

        if (!_view.Records.TryGetValue(recordKey, out var record))
        {
            return Task.FromResult(new StorageReadResult { Found = false });
        }

        return Task.FromResult(new StorageReadResult
        {
            Found = true,
            Payload = [.. record.Payload],
            ETag = record.ETag,
        });
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
        return await ReadAsync(request.RecordKey);
    }

    public async Task<string> WriteAsync(StorageWriteRequest request)
    {
        EnsureUsable();
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

        return await WriteAsync(request.Request);
    }

    public async Task ClearAsync(StorageClearRequest request)
    {
        EnsureUsable();
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
        await ClearAsync(request.Request);
    }

    public Task<GrainId[]> FindAsync(ExactIndexQuery query)
    {
        EnsureUsable();
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
        ArgumentNullException.ThrowIfNull(query);
        QueryPlanValidator.Validate(query);

        // StoragePartitionGrain is non-reentrant. Evaluating the complete plan synchronously in
        // this call gives AND and OR one serially consistent partition-local view.
        var recordKeys = EvaluateQuery(query);
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
        return ResolveGrainIds(EvaluateQuery(query.Query), snapshot);
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

    private HashSet<string> EvaluateQuery(PartitionQueryPlan query)
    {
        return query.Operation switch
        {
            PartitionQueryOperation.Empty => new HashSet<string>(StringComparer.Ordinal),
            PartitionQueryOperation.Exact => EvaluateExactQuery(query),
            PartitionQueryOperation.Range => EvaluateRangeQuery(query),
            PartitionQueryOperation.And => EvaluateAndQuery(query),
            PartitionQueryOperation.Or => EvaluateOrQuery(query),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.Operation,
                "Unknown partition query operation."),
        };
    }

    private HashSet<string> EvaluateExactQuery(PartitionQueryPlan query)
    {
        var scope = query.Scope
            ?? throw new ArgumentException("An exact query requires an index scope.", nameof(query));
        var value = query.Value
            ?? throw new ArgumentException("An exact query requires an index value.", nameof(query));
        var records = query.IndexKind switch
        {
            SearchableIndexKind.Hash => _view.Indexes.FindHashEntries(scope, value),
            SearchableIndexKind.Range => _view.Indexes.FindRangeEntries(scope, value),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.IndexKind,
                "Unknown index kind."),
        };
        // Lookup methods return live buckets. Boolean nodes mutate only their private copy so a
        // query cannot corrupt the derived indexes used by later reads.
        return new HashSet<string>(records, StringComparer.Ordinal);
    }

    private HashSet<string> EvaluateRangeQuery(PartitionQueryPlan query)
    {
        var scope = query.Scope
            ?? throw new ArgumentException("A range query requires an index scope.", nameof(query));
        if (query.LowerBound is null && query.UpperBound is null)
        {
            throw new ArgumentException("A range query requires at least one bound.", nameof(query));
        }

        if (query.LowerBound is not null
            && query.UpperBound is not null
            && query.LowerBound.CompareTo(query.UpperBound) > 0)
        {
            throw new ArgumentException(
                "The lower range bound must not be greater than the upper range bound.",
                nameof(query));
        }

        var records = new HashSet<string>(StringComparer.Ordinal);
        _view.Indexes.UnionRange(
            scope,
            query.LowerBound,
            query.UpperBound,
            query.IncludeLowerBound,
            query.IncludeUpperBound,
            records);
        return records;
    }

    private HashSet<string> EvaluateAndQuery(PartitionQueryPlan query)
    {
        var left = EvaluateQuery(GetRequiredChild(query.Left, "left", query));
        left.IntersectWith(EvaluateQuery(GetRequiredChild(query.Right, "right", query)));
        return left;
    }

    private HashSet<string> EvaluateOrQuery(PartitionQueryPlan query)
    {
        var left = EvaluateQuery(GetRequiredChild(query.Left, "left", query));
        left.UnionWith(EvaluateQuery(GetRequiredChild(query.Right, "right", query)));
        return left;
    }

    private static PartitionQueryPlan GetRequiredChild(
        PartitionQueryPlan? child,
        string side,
        PartitionQueryPlan query)
    {
        return child
            ?? throw new ArgumentException(
                $"A boolean query requires a {side} child plan.",
                nameof(query));
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

    private async Task CommitAsync(StorageJournalEntry entry)
    {
        try
        {
            await Persistence.CommitAsync(entry);
        }
        catch
        {
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
        // PR7a permits only the immutable epoch-one identity map. Any future epoch advancement
        // must replace this cache condition with an authoritative freshness and fencing protocol.
        var current = await GetRequiredRoutingSnapshotAsync();
        if (expectedEpoch > current.Epoch)
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
