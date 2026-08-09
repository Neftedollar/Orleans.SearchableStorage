using System.Globalization;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Storage;

internal sealed class StoragePartitionGrain : Grain, IStoragePartitionGrain
{
    private readonly IPersistentState<StoragePartitionState> _state;
    private Dictionary<string, Dictionary<IndexValue, HashSet<string>>> _hashIndexes = new(StringComparer.Ordinal);
    private Dictionary<string, SortedDictionary<IndexValue, HashSet<string>>> _rangeIndexes = new(StringComparer.Ordinal);

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

        if (query.LowerBound.CompareTo(query.UpperBound) > 0)
        {
            throw new ArgumentException("The lower range bound must not be greater than the upper range bound.", nameof(query));
        }

        if (!_rangeIndexes.TryGetValue(query.Scope, out var index))
        {
            return Task.FromResult(Array.Empty<GrainId>());
        }

        var recordKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var pair in index)
        {
            var lowerComparison = pair.Key.CompareTo(query.LowerBound);
            if (lowerComparison < 0 || (lowerComparison == 0 && !query.IncludeLowerBound))
            {
                continue;
            }

            var upperComparison = pair.Key.CompareTo(query.UpperBound);
            if (upperComparison > 0 || (upperComparison == 0 && !query.IncludeUpperBound))
            {
                break;
            }

            recordKeys.UnionWith(pair.Value);
        }

        return Task.FromResult(ResolveGrainIds(recordKeys));
    }

    private static PartitionIndexes BuildIndexes(StoragePartitionState state)
    {
        var indexes = new PartitionIndexes();
        foreach (var pair in state.Records)
        {
            foreach (var entry in pair.Value.IndexEntries)
            {
                switch (entry.Kind)
                {
                    case SearchableIndexKind.Hash:
                        AddHashEntry(indexes.Hash, entry.Scope, entry.Value, pair.Key);
                        break;
                    case SearchableIndexKind.Range:
                        AddRangeEntry(indexes.Range, entry.Scope, entry.Value, pair.Key);
                        break;
                    default:
                        throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
                }
            }
        }

        return indexes;
    }

    private static void AddHashEntry(
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

    private static void AddRangeEntry(
        Dictionary<string, SortedDictionary<IndexValue, HashSet<string>>> indexes,
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

    private sealed class PartitionIndexes
    {
        public Dictionary<string, Dictionary<IndexValue, HashSet<string>>> Hash { get; } = new(StringComparer.Ordinal);

        public Dictionary<string, SortedDictionary<IndexValue, HashSet<string>>> Range { get; } = new(StringComparer.Ordinal);
    }
}
