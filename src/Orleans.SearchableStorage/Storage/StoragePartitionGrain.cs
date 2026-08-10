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
    private readonly IPersistentState<StoragePartitionManifestState> _manifest;
    private StoragePartitionView _view = new(
        new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
    private StoragePartitionPersistence? _persistence;
    private bool _usable;

    public StoragePartitionGrain(
        [PersistentState("manifest", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StoragePartitionManifestState> manifest,
        ILogger<StoragePartitionGrain> logger)
    {
        _manifest = manifest;
        _logger = logger;
    }

    private StoragePartitionPersistence Persistence => _persistence
        ?? throw new InvalidOperationException("The storage partition has not completed activation.");

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _persistence = new StoragePartitionPersistence(
            _manifest,
            GrainFactory,
            this.GetPrimaryKeyString(),
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

    public Task<GrainId[]> FindAsync(ExactIndexQuery query)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(query);

        IEnumerable<string> recordKeys = query.Kind switch
        {
            SearchableIndexKind.Hash => _view.Indexes.FindHashEntries(query.Scope, query.Value),
            SearchableIndexKind.Range => _view.Indexes.FindRangeEntries(query.Scope, query.Value),
            _ => throw new ArgumentOutOfRangeException(nameof(query), query.Kind, "Unknown index kind."),
        };

        return Task.FromResult(ResolveGrainIds(recordKeys));
    }

    public Task<GrainId[]> RangeAsync(RangeIndexQuery query)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(query);
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
        return Task.FromResult(ResolveGrainIds(recordKeys));
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

    private GrainId[] ResolveGrainIds(IEnumerable<string> recordKeys)
    {
        return recordKeys
            .Select(recordKey => _view.Records[recordKey].GrainId)
            .Distinct()
            .ToArray();
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
