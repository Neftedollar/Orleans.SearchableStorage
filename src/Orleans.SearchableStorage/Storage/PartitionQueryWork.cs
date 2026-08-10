namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Describes deterministic logical work performed while evaluating one partition query.
/// </summary>
internal readonly record struct PartitionQueryWork(
    long EmptyNodeCount,
    long ExactNodeCount,
    long RangeNodeCount,
    long AndNodeCount,
    long OrNodeCount,
    long ExactCandidateCount,
    long RangeBucketVisitCount,
    long RangeCandidateCount,
    long AndCandidateCheckCount,
    long OrCandidateMergeCount)
{
    /// <summary>
    /// Gets the total number of query-plan nodes evaluated.
    /// </summary>
    public long NodeCount => checked(
        EmptyNodeCount
        + ExactNodeCount
        + RangeNodeCount
        + AndNodeCount
        + OrNodeCount);

    /// <summary>
    /// Gets the total number of record-key candidates consumed by leaf copies and boolean nodes.
    /// </summary>
    public long CandidateOperationCount => checked(
        ExactCandidateCount
        + RangeCandidateCount
        + AndCandidateCheckCount
        + OrCandidateMergeCount);

    /// <summary>
    /// Gets the deterministic budget unit for this evaluation. Each visited plan node, visited
    /// range bucket, and consumed record-key candidate contributes one operation.
    /// </summary>
    public long TotalOperationCount => checked(
        NodeCount
        + RangeBucketVisitCount
        + CandidateOperationCount);
}

/// <summary>
/// Couples an evaluation result with the deterministic logical work which produced it.
/// </summary>
internal readonly record struct StoragePartitionQueryEvaluation(
    HashSet<string> RecordKeys,
    PartitionQueryWork Work);

/// <summary>
/// Receives query-work events from the evaluator and range-index traversal. Struct sinks let the
/// ordinary evaluation path use a JIT-specialized no-op implementation without allocating or
/// updating counters.
/// </summary>
internal interface IPartitionQueryWorkSink
{
    void RecordEmpty();

    void RecordExact(int candidateCount);

    void RecordRange();

    void RecordRangeBucket(int candidateCount);

    void RecordAnd(int candidateCount);

    void RecordOr(int candidateCount);
}

internal readonly struct NoPartitionQueryWorkSink : IPartitionQueryWorkSink
{
    public void RecordEmpty()
    {
    }

    public void RecordExact(int candidateCount)
    {
    }

    public void RecordRange()
    {
    }

    public void RecordRangeBucket(int candidateCount)
    {
    }

    public void RecordAnd(int candidateCount)
    {
    }

    public void RecordOr(int candidateCount)
    {
    }
}

internal struct CountingPartitionQueryWorkSink : IPartitionQueryWorkSink
{
    private long _emptyNodeCount;
    private long _exactNodeCount;
    private long _rangeNodeCount;
    private long _andNodeCount;
    private long _orNodeCount;
    private long _exactCandidateCount;
    private long _rangeBucketVisitCount;
    private long _rangeCandidateCount;
    private long _andCandidateCheckCount;
    private long _orCandidateMergeCount;

    public readonly PartitionQueryWork Snapshot => new(
        _emptyNodeCount,
        _exactNodeCount,
        _rangeNodeCount,
        _andNodeCount,
        _orNodeCount,
        _exactCandidateCount,
        _rangeBucketVisitCount,
        _rangeCandidateCount,
        _andCandidateCheckCount,
        _orCandidateMergeCount);

    public void RecordEmpty()
    {
        _emptyNodeCount = checked(_emptyNodeCount + 1);
    }

    public void RecordExact(int candidateCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(candidateCount);
        _exactNodeCount = checked(_exactNodeCount + 1);
        _exactCandidateCount = checked(_exactCandidateCount + candidateCount);
    }

    public void RecordRange()
    {
        _rangeNodeCount = checked(_rangeNodeCount + 1);
    }

    public void RecordRangeBucket(int candidateCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(candidateCount);
        _rangeBucketVisitCount = checked(_rangeBucketVisitCount + 1);
        _rangeCandidateCount = checked(_rangeCandidateCount + candidateCount);
    }

    public void RecordAnd(int candidateCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(candidateCount);
        _andNodeCount = checked(_andNodeCount + 1);
        _andCandidateCheckCount = checked(_andCandidateCheckCount + candidateCount);
    }

    public void RecordOr(int candidateCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(candidateCount);
        _orNodeCount = checked(_orNodeCount + 1);
        _orCandidateMergeCount = checked(_orCandidateMergeCount + candidateCount);
    }
}
