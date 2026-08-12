using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal enum CandidateTakeResult
{
    Candidate,
    Exhausted,
    WorkBudget,
}

internal interface IOrderedCandidateCursor : IDisposable
{
    bool HasCandidate { get; }

    CandidateTakeResult TryTakeNext(
        ref PageWorkAccumulator work,
        out GrainId candidate);
}

internal struct PageWorkAccumulator(long budget)
{
    private long _totalOperationCount;
    private long _orderedCandidateVisitCount;
    private long _recordProbeCount;
    private long _predicateNodeProbeCount;
    private long _indexEntryProbeCount;
    private long _ownershipProbeCount;
    private long _postingSeekCount;
    private long _rangeBucketVisitCount;
    private long _resultMaterializationCount;
    private long _rangeMergeOperationCount;
    private long _plannerNodeVisitCount;
    private long _plannerMetadataReadCount;
    private long _postingCandidateVisitCount;
    private long _catalogCandidateVisitCount;
    private long _heapOperationCount;
    private long _unionOperationCount;
    private PartitionQueryAccessPath _accessPath;

    public readonly long TotalOperationCount => _totalOperationCount;

    public readonly long RemainingBudget => checked(budget - _totalOperationCount);

    public readonly PartitionQueryPageWork Snapshot => new()
    {
        OrderedCandidateVisitCount = _orderedCandidateVisitCount,
        RecordProbeCount = _recordProbeCount,
        PredicateNodeProbeCount = _predicateNodeProbeCount,
        IndexEntryProbeCount = _indexEntryProbeCount,
        OwnershipProbeCount = _ownershipProbeCount,
        PostingSeekCount = _postingSeekCount,
        RangeBucketVisitCount = _rangeBucketVisitCount,
        ResultMaterializationCount = _resultMaterializationCount,
        RangeMergeOperationCount = _rangeMergeOperationCount,
        PlannerNodeVisitCount = _plannerNodeVisitCount,
        PlannerMetadataReadCount = _plannerMetadataReadCount,
        PostingCandidateVisitCount = _postingCandidateVisitCount,
        CatalogCandidateVisitCount = _catalogCandidateVisitCount,
        HeapOperationCount = _heapOperationCount,
        UnionOperationCount = _unionOperationCount,
        AccessPath = _accessPath,
    };

    public void SetAccessPath(PartitionQueryAccessPath accessPath)
    {
        if (accessPath == PartitionQueryAccessPath.None)
        {
            throw new ArgumentOutOfRangeException(nameof(accessPath));
        }

        if (_accessPath != PartitionQueryAccessPath.None)
        {
            throw new InvalidOperationException("The scalar access path was already selected.");
        }

        _accessPath = accessPath;
    }

    public bool TryRecordOrderedCandidateVisit() => TryRecord(ref _orderedCandidateVisitCount);

    public bool TryRecordRecordProbe() => TryRecord(ref _recordProbeCount);

    public bool TryRecordPredicateNodeProbe() => TryRecord(ref _predicateNodeProbeCount);

    public bool TryRecordIndexEntryProbe() => TryRecord(ref _indexEntryProbeCount);

    public bool TryRecordOwnershipProbe() => TryRecord(ref _ownershipProbeCount);

    public bool TryRecordPostingSeek() => TryRecord(ref _postingSeekCount);

    public bool TryRecordRangeBucketVisit() => TryRecord(ref _rangeBucketVisitCount);

    public bool TryRecordResultMaterialization() => TryRecord(ref _resultMaterializationCount);

    public bool TryRecordRangeMergeOperation() => TryRecord(ref _rangeMergeOperationCount);

    public bool TryRecordPlannerNodeVisit() => TryRecord(ref _plannerNodeVisitCount);

    public bool TryRecordPlannerMetadataRead() => TryRecord(ref _plannerMetadataReadCount);

    public bool TryRecordPostingCandidateVisit() => TryRecord(ref _postingCandidateVisitCount);

    public bool TryRecordCatalogCandidateVisit() => TryRecord(ref _catalogCandidateVisitCount);

    public bool TryRecordHeapOperation() => TryRecord(ref _heapOperationCount);

    public bool TryRecordUnionOperation() => TryRecord(ref _unionOperationCount);

    private bool TryRecord(ref long component)
    {
        if (_totalOperationCount >= budget)
        {
            return false;
        }

        component = checked(component + 1);
        _totalOperationCount = checked(_totalOperationCount + 1);
        return true;
    }
}
