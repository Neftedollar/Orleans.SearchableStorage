using System.Globalization;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StoragePartitionGrain : Grain, IStoragePartitionGrain
{
    private readonly IPersistentState<StoragePartitionState> _state;
    private Dictionary<string, Dictionary<IndexValue, HashSet<string>>> _hashIndexes = new(StringComparer.Ordinal);
    private Dictionary<string, RangeIndex> _rangeIndexes = new(StringComparer.Ordinal);

    public StoragePartitionGrain(
        [PersistentState("partition", SearchableStorageConstants.PhysicalStorageProviderName)]
        IPersistentState<StoragePartitionState> state)
    {
        _state = state;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        SetIndexes(BuildIndexes(_state.State));
        return base.OnActivateAsync(cancellationToken);
    }

    public Task<StorageReadResult> ReadAsync(string recordKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);

        if (!_state.State.Records.TryGetValue(recordKey, out var record))
        {
            return Task.FromResult(new StorageReadResult { Found = false });
        }

        return Task.FromResult(new StorageReadResult
        {
            Found = true,
            Payload = record.Payload,
            ETag = record.ETag,
        });
    }

    public async Task<string> WriteAsync(StorageWriteRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        _state.State.Records.TryGetValue(request.RecordKey, out var currentRecord);
        EnsureETagMatches(request.RecordKey, currentRecord?.ETag, request.ExpectedETag, "write");

        var candidate = _state.State.Copy();
        var etag = candidate.NextVersion.ToString(CultureInfo.InvariantCulture);
        candidate.NextVersion++;
        var storedRecord = new StoredRecord
        {
            GrainId = request.GrainId,
            Payload = request.Payload,
            ETag = etag,
            IndexEntries = request.IndexEntries,
        };
        candidate.Records[request.RecordKey] = storedRecord;

        await PersistAsync(candidate, BuildIndexes(candidate));
        return etag;
    }

    public async Task ClearAsync(string recordKey, string? expectedETag)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);

        if (!_state.State.Records.TryGetValue(recordKey, out var currentRecord))
        {
            EnsureETagMatches(recordKey, null, expectedETag, "clear");
            return;
        }

        EnsureETagMatches(recordKey, currentRecord.ETag, expectedETag, "clear");

        var candidate = _state.State.Copy();
        candidate.Records.Remove(recordKey);
        await PersistAsync(candidate, BuildIndexes(candidate));
    }

    public Task<GrainId[]> FindAsync(ExactIndexQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);

        IEnumerable<string> recordKeys = query.Kind switch
        {
            SearchableIndexKind.Hash => FindHashEntries(query.Scope, query.Value),
            SearchableIndexKind.Range => FindRangeEntries(query.Scope, query.Value),
            _ => throw new ArgumentOutOfRangeException(nameof(query), query.Kind, "Unknown index kind."),
        };

        return Task.FromResult(ResolveGrainIds(recordKeys));
    }

    public Task<GrainId[]> RangeAsync(RangeIndexQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(query.LowerBound);
        ArgumentNullException.ThrowIfNull(query.UpperBound);

        if (query.LowerBound.CompareTo(query.UpperBound) > 0)
        {
            throw new ArgumentException("The lower range bound must not be greater than the upper range bound.", nameof(query));
        }

        if (!_rangeIndexes.TryGetValue(query.Scope, out var index))
        {
            return Task.FromResult(Array.Empty<GrainId>());
        }

        var recordKeys = new HashSet<string>(StringComparer.Ordinal);
        index.UnionRange(
            query.LowerBound,
            query.UpperBound,
            query.IncludeLowerBound,
            query.IncludeUpperBound,
            recordKeys);

        return Task.FromResult(ResolveGrainIds(recordKeys));
    }

    public Task<GrainId[]> QueryAsync(PartitionQueryPlan query)
    {
        ArgumentNullException.ThrowIfNull(query);

        // StoragePartitionGrain is non-reentrant. Evaluating the complete plan synchronously in
        // this call gives AND and OR one serially consistent partition-local view.
        var recordKeys = EvaluateQuery(query);
        return Task.FromResult(ResolveGrainIds(recordKeys));
    }

    private HashSet<string> EvaluateQuery(PartitionQueryPlan query)
    {
        return query.Operation switch
        {
            PartitionQueryOperation.Empty => [],
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
            SearchableIndexKind.Hash => FindHashEntries(scope, value),
            SearchableIndexKind.Range => FindRangeEntries(scope, value),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.IndexKind,
                "Unknown index kind."),
        };
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
        if (_rangeIndexes.TryGetValue(scope, out var index))
        {
            index.UnionRange(
                query.LowerBound,
                query.UpperBound,
                query.IncludeLowerBound,
                query.IncludeUpperBound,
                records);
        }

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

    private static PartitionIndexes BuildIndexes(StoragePartitionState state)
    {
        var hashIndexes = new Dictionary<string, Dictionary<IndexValue, HashSet<string>>>(StringComparer.Ordinal);
        var rangeBuckets = new Dictionary<string, Dictionary<IndexValue, HashSet<string>>>(StringComparer.Ordinal);
        foreach (var pair in state.Records)
        {
            foreach (var entry in pair.Value.IndexEntries)
            {
                switch (entry.Kind)
                {
                    case SearchableIndexKind.Hash:
                        AddIndexEntry(hashIndexes, entry.Scope, entry.Value, pair.Key);
                        break;
                    case SearchableIndexKind.Range:
                        AddIndexEntry(rangeBuckets, entry.Scope, entry.Value, pair.Key);
                        break;
                    default:
                        throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
                }
            }
        }

        var rangeIndexes = rangeBuckets.ToDictionary(
            static pair => pair.Key,
            static pair => new RangeIndex(pair.Value),
            StringComparer.Ordinal);
        return new PartitionIndexes(hashIndexes, rangeIndexes);
    }

    private static void AddIndexEntry(
        Dictionary<string, Dictionary<IndexValue, HashSet<string>>> indexes,
        string scope,
        IndexValue value,
        string recordKey)
    {
        if (!indexes.TryGetValue(scope, out var index))
        {
            index = [];
            indexes.Add(scope, index);
        }

        if (!index.TryGetValue(value, out var bucket))
        {
            bucket = new HashSet<string>(StringComparer.Ordinal);
            index.Add(value, bucket);
        }

        bucket.Add(recordKey);
    }

    private HashSet<string> FindHashEntries(string scope, IndexValue value)
    {
        if (_hashIndexes.TryGetValue(scope, out var index)
            && index.TryGetValue(value, out var bucket))
        {
            return bucket;
        }

        return [];
    }

    private HashSet<string> FindRangeEntries(string scope, IndexValue value)
    {
        if (_rangeIndexes.TryGetValue(scope, out var index)
            && index.TryGetValue(value, out var bucket))
        {
            return bucket;
        }

        return [];
    }

    private GrainId[] ResolveGrainIds(IEnumerable<string> recordKeys)
    {
        return recordKeys
            .Select(recordKey => _state.State.Records[recordKey].GrainId)
            .Distinct()
            .Order()
            .ToArray();
    }

    private async Task PersistAsync(StoragePartitionState candidate, PartitionIndexes indexes)
    {
        var previous = _state.State;
        _state.State = candidate;
        try
        {
            // Records and their local index entries share one physical write. A failed write must
            // never leave this activation serving the uncommitted candidate state.
            await _state.WriteStateAsync();
        }
        catch
        {
            _state.State = previous;
            DeactivateOnIdle();
            throw;
        }

        SetIndexes(indexes);
    }

    private void SetIndexes(PartitionIndexes indexes)
    {
        _hashIndexes = indexes.Hash;
        _rangeIndexes = indexes.Range;
    }

    private static void EnsureETagMatches(string recordKey, string? storedETag, string? expectedETag, string operation)
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

    private sealed class PartitionIndexes(
        Dictionary<string, Dictionary<IndexValue, HashSet<string>>> hash,
        Dictionary<string, RangeIndex> range)
    {
        public Dictionary<string, Dictionary<IndexValue, HashSet<string>>> Hash { get; } = hash;

        public Dictionary<string, RangeIndex> Range { get; } = range;
    }
}
