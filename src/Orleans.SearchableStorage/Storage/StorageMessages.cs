using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

[GenerateSerializer]
internal sealed class IndexEntry
{
    [Id(0)]
    public required string Scope { get; init; }

    [Id(1)]
    public SearchableIndexKind Kind { get; init; }

    [Id(2)]
    public required IndexValue Value { get; init; }
}

[GenerateSerializer]
internal sealed class StorageWriteRequest
{
    [Id(0)]
    public required string RecordKey { get; init; }

    [Id(1)]
    public required GrainId GrainId { get; init; }

    [Id(2)]
    public required byte[] Payload { get; init; }

    [Id(3)]
    public string? ExpectedETag { get; init; }

    [Id(4)]
    public required List<IndexEntry> IndexEntries { get; init; }

    [Id(5)]
    public required StoragePersistenceSettings Persistence { get; init; }

    [Id(6)]
    public byte[]? IndexSchemaFingerprint { get; init; }

    [Id(7)]
    public string? StateName { get; init; }

    [Id(8)]
    public int IndexSchemaProtocolVersion { get; init; }
}

[GenerateSerializer]
internal sealed class StorageClearRequest
{
    [Id(0)]
    public required string RecordKey { get; init; }

    [Id(1)]
    public string? ExpectedETag { get; init; }

    [Id(2)]
    public required StoragePersistenceSettings Persistence { get; init; }

    [Id(3)]
    public string? StateName { get; init; }

    [Id(4)]
    public byte[]? IndexSchemaFingerprint { get; init; }

    [Id(5)]
    public int IndexSchemaProtocolVersion { get; init; }
}

[GenerateSerializer]
internal sealed class StoragePersistenceSettings
{
    [Id(0)]
    public int JournalSegmentCapacity { get; init; }

    [Id(1)]
    public int MaximumJournalReplayEntries { get; init; }

    [Id(2)]
    public int CompactionThreshold { get; init; }
}

[GenerateSerializer]
internal sealed class StorageReadResult
{
    [Id(0)]
    public bool Found { get; init; }

    [Id(1)]
    public byte[]? Payload { get; init; }

    [Id(2)]
    public string? ETag { get; init; }
}

/// <summary>
/// Carries a point read together with the routing decision used by its caller.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedStorageReadRequest
{
    [Id(0)]
    public required string RecordKey { get; init; }

    [Id(1)]
    public int Slot { get; init; }

    [Id(2)]
    public long Epoch { get; init; }

    [Id(3)]
    public required GrainId GrainId { get; init; }
}

/// <summary>
/// Carries a point write together with the routing decision used by its caller.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedStorageWriteRequest
{
    [Id(0)]
    public required StorageWriteRequest Request { get; init; }

    [Id(1)]
    public int Slot { get; init; }

    [Id(2)]
    public long Epoch { get; init; }
}

/// <summary>
/// Carries a point clear together with the routing decision used by its caller.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedStorageClearRequest
{
    [Id(0)]
    public required StorageClearRequest Request { get; init; }

    [Id(1)]
    public int Slot { get; init; }

    [Id(2)]
    public long Epoch { get; init; }

    [Id(3)]
    public required GrainId GrainId { get; init; }
}

[GenerateSerializer]
internal sealed class StoragePartitionPersistenceInfo
{
    [Id(0)]
    public bool Initialized { get; init; }

    [Id(1)]
    public int JournalSegmentCapacity { get; init; }

    [Id(2)]
    public int MaximumJournalReplayEntries { get; init; }

    [Id(3)]
    public long WriterEpoch { get; init; }

    [Id(4)]
    public long CommittedSequence { get; init; }

    [Id(5)]
    public long SnapshotSequence { get; init; }

    [Id(6)]
    public long PrunedSequence { get; init; }

    [Id(7)]
    public long ActiveSnapshotGeneration { get; init; }

    [Id(8)]
    public long PendingSnapshotGeneration { get; init; }

    [Id(9)]
    public long RetiringSnapshotGeneration { get; init; }

    [Id(10)]
    public int RecordCount { get; init; }
}

[GenerateSerializer]
internal sealed class ExactIndexQuery
{
    [Id(0)]
    public required string Scope { get; init; }

    [Id(1)]
    public SearchableIndexKind Kind { get; init; }

    [Id(2)]
    public required IndexValue Value { get; init; }
}

[GenerateSerializer]
internal sealed class RangeIndexQuery
{
    [Id(0)]
    public required string Scope { get; init; }

    [Id(1)]
    public required IndexValue LowerBound { get; init; }

    [Id(2)]
    public required IndexValue UpperBound { get; init; }

    [Id(3)]
    public bool IncludeLowerBound { get; init; }

    [Id(4)]
    public bool IncludeUpperBound { get; init; }
}

/// <summary>
/// Carries an exact-index query for one routing epoch.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedExactIndexQuery
{
    [Id(0)]
    public required ExactIndexQuery Query { get; init; }

    [Id(1)]
    public long Epoch { get; init; }

    [Id(2)] public string? StateName { get; init; }
    [Id(3)] public byte[]? IndexSchemaFingerprint { get; init; }
    [Id(4)] public int IndexSchemaProtocolVersion { get; init; }
}

/// <summary>
/// Carries a range-index query for one routing epoch.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedRangeIndexQuery
{
    [Id(0)]
    public required RangeIndexQuery Query { get; init; }

    [Id(1)]
    public long Epoch { get; init; }

    [Id(2)] public string? StateName { get; init; }
    [Id(3)] public byte[]? IndexSchemaFingerprint { get; init; }
    [Id(4)] public int IndexSchemaProtocolVersion { get; init; }
}

/// <summary>
/// Carries one complete query to a partition. This message is serialized between Orleans
/// participants but is never part of persisted storage state.
/// </summary>
[GenerateSerializer]
internal sealed class PartitionQueryPlan
{
    [Id(0)]
    public PartitionQueryOperation Operation { get; init; }

    [Id(1)]
    public string? Scope { get; init; }

    [Id(2)]
    public SearchableIndexKind IndexKind { get; init; }

    [Id(3)]
    public IndexValue? Value { get; init; }

    [Id(4)]
    public IndexValue? LowerBound { get; init; }

    [Id(5)]
    public IndexValue? UpperBound { get; init; }

    [Id(6)]
    public bool IncludeLowerBound { get; init; }

    [Id(7)]
    public bool IncludeUpperBound { get; init; }

    [Id(8)]
    public PartitionQueryPlan? Left { get; init; }

    [Id(9)]
    public PartitionQueryPlan? Right { get; init; }
}

/// <summary>
/// Carries a complete partition query plan for one routing epoch.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedPartitionQuery
{
    [Id(0)]
    public required PartitionQueryPlan Query { get; init; }

    [Id(1)]
    public long Epoch { get; init; }

    [Id(2)] public string? StateName { get; init; }
    [Id(3)] public byte[]? IndexSchemaFingerprint { get; init; }
    [Id(4)] public int IndexSchemaProtocolVersion { get; init; }
}

/// <summary>
/// Carries one bounded, stateless partition-page request for an authoritative routing epoch.
/// </summary>
[GenerateSerializer]
internal sealed class RoutedPartitionQueryPageRequest
{
    [Id(0)]
    public required PartitionQueryPlan Query { get; init; }

    [Id(1)]
    public long Epoch { get; init; }

    [Id(2)]
    public bool HasAfter { get; init; }

    [Id(3)]
    public GrainId After { get; init; }

    [Id(4)]
    public long WorkBudget { get; init; }

    [Id(5)]
    public int ItemLimit { get; init; }

    [Id(6)]
    public int ByteLimit { get; init; }

    [Id(7)]
    public int ProtocolVersion { get; init; }

    [Id(8)]
    public int OrderingVersion { get; init; }

    [Id(9)]
    public int WorkPolicyVersion { get; init; }

    [Id(10)]
    public PartitionQueryResponseFamily ResponseFamily { get; init; }

    [Id(11)]
    public required byte[] QueryFingerprint { get; init; }

    [Id(12)]
    public int LayoutFormatVersion { get; init; }

    [Id(13)]
    public required byte[] LayoutFingerprint { get; init; }

    [Id(14)]
    public required string StateName { get; init; }

    [Id(15)] public byte[]? IndexSchemaFingerprint { get; init; }
    [Id(16)] public int IndexSchemaProtocolVersion { get; init; }
}

/// <summary>
/// Returns one sorted, distinct prefix from a partition-local bounded query turn.
/// </summary>
[GenerateSerializer]
internal sealed class PartitionQueryPageResult
{
    [Id(0)]
    public required GrainId[] Items { get; init; }

    [Id(1)]
    public bool HasFrontier { get; init; }

    [Id(2)]
    public GrainId Frontier { get; init; }

    [Id(3)]
    public bool Exhausted { get; init; }

    [Id(4)]
    public PartitionQueryPageStopReason StopReason { get; init; }

    [Id(5)]
    public required PartitionQueryPageWork Work { get; init; }

    [Id(6)]
    public int ItemByteCount { get; init; }

    [Id(7)]
    public int ProtocolVersion { get; init; }

    [Id(8)]
    public int OrderingVersion { get; init; }

    [Id(9)]
    public int WorkPolicyVersion { get; init; }

    [Id(10)]
    public PartitionQueryResponseFamily ResponseFamily { get; init; }

    [Id(11)]
    public long Epoch { get; init; }

    [Id(12)]
    public required byte[] QueryFingerprint { get; init; }

    [Id(13)]
    public int LayoutFormatVersion { get; init; }

    [Id(14)]
    public required byte[] LayoutFingerprint { get; init; }
}

/// <summary>Carries one bounded canonical value-order turn for a distinct facet.</summary>
[GenerateSerializer]
internal sealed class RoutedPartitionDistinctFacetPageRequest
{
    [Id(0)] public required PartitionQueryPlan Query { get; init; }
    [Id(1)] public required string FacetScope { get; init; }
    [Id(2)] public SearchableIndexKind FacetKind { get; init; }
    [Id(3)] public long Epoch { get; init; }
    [Id(4)] public IndexValue? After { get; init; }
    [Id(5)] public long WorkBudget { get; init; }
    [Id(6)] public int ItemLimit { get; init; }
    [Id(7)] public int ByteLimit { get; init; }
    [Id(8)] public int ProtocolVersion { get; init; }
    [Id(9)] public int OrderingVersion { get; init; }
    [Id(10)] public int WorkPolicyVersion { get; init; }
    [Id(11)] public PartitionQueryResponseFamily ResponseFamily { get; init; }
    [Id(12)] public required byte[] RequestFingerprint { get; init; }
    [Id(13)] public int LayoutFormatVersion { get; init; }
    [Id(14)] public required byte[] LayoutFingerprint { get; init; }
    [Id(15)] public required string StateName { get; init; }
    [Id(16)] public bool HasExpectedDataVersion { get; init; }
    [Id(17)] public long ExpectedDataVersion { get; init; }
    [Id(18)] public byte[]? IndexSchemaFingerprint { get; init; }
    [Id(19)] public int IndexSchemaProtocolVersion { get; init; }
}

/// <summary>Returns one bounded canonical value-order candidate page.</summary>
[GenerateSerializer]
internal sealed class PartitionDistinctFacetPageResult
{
    [Id(0)] public required IndexValue[] Items { get; init; }
    [Id(1)] public IndexValue? Frontier { get; init; }
    [Id(2)] public bool Exhausted { get; init; }
    [Id(3)] public PartitionQueryPageStopReason StopReason { get; init; }
    [Id(4)] public required PartitionFacetWork Work { get; init; }
    [Id(5)] public int ItemByteCount { get; init; }
    [Id(6)] public int ProtocolVersion { get; init; }
    [Id(7)] public int OrderingVersion { get; init; }
    [Id(8)] public int WorkPolicyVersion { get; init; }
    [Id(9)] public PartitionQueryResponseFamily ResponseFamily { get; init; }
    [Id(10)] public long Epoch { get; init; }
    [Id(11)] public required byte[] RequestFingerprint { get; init; }
    [Id(12)] public int LayoutFormatVersion { get; init; }
    [Id(13)] public required byte[] LayoutFingerprint { get; init; }
    [Id(14)] public long DataVersion { get; set; }
}

/// <summary>Carries one bounded canonical value-ordered candidate turn.</summary>
[GenerateSerializer]
internal sealed class RoutedPartitionFacetCandidatePageRequest
{
    [Id(0)] public required PartitionQueryPlan Query { get; init; }
    [Id(1)] public required string FacetScope { get; init; }
    [Id(2)] public SearchableIndexKind FacetKind { get; init; }
    [Id(3)] public long Epoch { get; init; }
    [Id(4)] public IndexValue? AfterValue { get; init; }
    [Id(5)] public long WorkBudget { get; init; }
    [Id(6)] public int ItemLimit { get; init; }
    [Id(7)] public int ByteLimit { get; init; }
    [Id(8)] public int ProtocolVersion { get; init; }
    [Id(9)] public int OrderingVersion { get; init; }
    [Id(10)] public int WorkPolicyVersion { get; init; }
    [Id(11)] public PartitionQueryResponseFamily ResponseFamily { get; init; }
    [Id(12)] public required byte[] RequestFingerprint { get; init; }
    [Id(13)] public int LayoutFormatVersion { get; init; }
    [Id(14)] public required byte[] LayoutFingerprint { get; init; }
    [Id(15)] public required string StateName { get; init; }
    [Id(16)] public bool HasExpectedDataVersion { get; init; }
    [Id(17)] public long ExpectedDataVersion { get; init; }
    [Id(18)] public byte[]? IndexSchemaFingerprint { get; init; }
    [Id(19)] public int IndexSchemaProtocolVersion { get; init; }
}

/// <summary>One metadata-only partition-local facet candidate.</summary>
[GenerateSerializer]
internal sealed class PartitionFacetCandidate
{
    [Id(0)] public required IndexValue Value { get; init; }
    [Id(1)] public long RawCount { get; init; }
}

/// <summary>Returns one bounded canonical value-ordered candidate page.</summary>
[GenerateSerializer]
internal sealed class PartitionFacetCandidatePageResult
{
    [Id(0)] public required PartitionFacetCandidate[] Items { get; init; }
    [Id(1)] public IndexValue? FrontierValue { get; init; }
    [Id(2)] public bool Exhausted { get; init; }
    [Id(3)] public long PageRawCount { get; init; }
    [Id(4)] public long TotalRawCount { get; init; }
    [Id(5)] public PartitionQueryPageStopReason StopReason { get; init; }
    [Id(6)] public required PartitionFacetWork Work { get; init; }
    [Id(7)] public int ItemByteCount { get; init; }
    [Id(8)] public int ProtocolVersion { get; init; }
    [Id(9)] public int OrderingVersion { get; init; }
    [Id(10)] public int WorkPolicyVersion { get; init; }
    [Id(11)] public PartitionQueryResponseFamily ResponseFamily { get; init; }
    [Id(12)] public long Epoch { get; init; }
    [Id(13)] public required byte[] RequestFingerprint { get; init; }
    [Id(14)] public int LayoutFormatVersion { get; init; }
    [Id(15)] public required byte[] LayoutFingerprint { get; init; }
    [Id(16)] public long DataVersion { get; set; }
}

/// <summary>Requests one resumable exact-count slice for one nominated value.</summary>
[GenerateSerializer]
internal sealed class RoutedPartitionFacetCountSliceRequest
{
    [Id(0)] public required PartitionQueryPlan Query { get; init; }
    [Id(1)] public required string FacetScope { get; init; }
    [Id(2)] public SearchableIndexKind FacetKind { get; init; }
    [Id(3)] public required IndexValue Value { get; init; }
    [Id(4)] public long Epoch { get; init; }
    [Id(5)] public bool HasAfter { get; init; }
    [Id(6)] public GrainId After { get; init; }
    [Id(7)] public long WorkBudget { get; init; }
    [Id(8)] public int ProtocolVersion { get; init; }
    [Id(9)] public int OrderingVersion { get; init; }
    [Id(10)] public int WorkPolicyVersion { get; init; }
    [Id(11)] public PartitionQueryResponseFamily ResponseFamily { get; init; }
    [Id(12)] public required byte[] RequestFingerprint { get; init; }
    [Id(13)] public int LayoutFormatVersion { get; init; }
    [Id(14)] public required byte[] LayoutFingerprint { get; init; }
    [Id(15)] public required string StateName { get; init; }
    [Id(16)] public bool HasExpectedDataVersion { get; init; }
    [Id(17)] public long ExpectedDataVersion { get; init; }
    [Id(18)] public byte[]? IndexSchemaFingerprint { get; init; }
    [Id(19)] public int IndexSchemaProtocolVersion { get; init; }
}

/// <summary>Returns one resumable exact-count delta.</summary>
[GenerateSerializer]
internal sealed class PartitionFacetCountSliceResult
{
    [Id(0)] public long CountDelta { get; init; }
    [Id(1)] public bool HasFrontier { get; init; }
    [Id(2)] public GrainId Frontier { get; init; }
    [Id(3)] public bool Exhausted { get; init; }
    [Id(4)] public PartitionQueryPageStopReason StopReason { get; init; }
    [Id(5)] public required PartitionFacetWork Work { get; init; }
    [Id(6)] public int ProtocolVersion { get; init; }
    [Id(7)] public int OrderingVersion { get; init; }
    [Id(8)] public int WorkPolicyVersion { get; init; }
    [Id(9)] public PartitionQueryResponseFamily ResponseFamily { get; init; }
    [Id(10)] public long Epoch { get; init; }
    [Id(11)] public required byte[] RequestFingerprint { get; init; }
    [Id(12)] public int LayoutFormatVersion { get; init; }
    [Id(13)] public required byte[] LayoutFingerprint { get; init; }
    [Id(14)] public long DataVersion { get; set; }
}

/// <summary>Reports that a multi-turn facet observed a changed partition data version.</summary>
[GenerateSerializer]
internal sealed class StorageFacetDataChangedException : Exception
{
    public StorageFacetDataChangedException(long expectedVersion, long currentVersion)
        : base($"Facet data version changed from {expectedVersion} to {currentVersion}.")
    {
        ExpectedVersion = expectedVersion;
        CurrentVersion = currentVersion;
    }

    [Id(0)] public long ExpectedVersion { get; private set; }
    [Id(1)] public long CurrentVersion { get; private set; }
}

/// <summary>Reports a stored facet value outside the bounded canonical facet domain.</summary>
[GenerateSerializer]
internal sealed class StorageFacetValueUnsupportedException : Exception
{
    public StorageFacetValueUnsupportedException()
        : base("A stored facet value is not valid strict UTF-8 or exceeds the canonical facet-value limit.")
    {
    }

    public StorageFacetValueUnsupportedException(Exception exception)
        : this()
    {
        _ = exception;
    }
}

/// <summary>Serializable logical-work vector for one bounded facet turn.</summary>
[GenerateSerializer]
internal sealed class PartitionFacetWork
{
    [Id(0)] public long ValueSeekCount { get; init; }
    [Id(1)] public long ValueVisitCount { get; init; }
    [Id(2)] public long GrainGroupVisitCount { get; init; }
    [Id(3)] public long OwnershipProbeCount { get; init; }
    [Id(4)] public long RecordProbeCount { get; init; }
    [Id(5)] public long PredicateNodeProbeCount { get; init; }
    [Id(6)] public long IndexEntryProbeCount { get; init; }
    [Id(7)] public long CountIncrementCount { get; init; }
    [Id(8)] public long ResultMaterializationCount { get; init; }

    public long TotalOperationCount => checked(
        ValueSeekCount + ValueVisitCount + GrainGroupVisitCount + OwnershipProbeCount
        + RecordProbeCount + PredicateNodeProbeCount + IndexEntryProbeCount
        + CountIncrementCount + ResultMaterializationCount);
}

/// <summary>
/// Serializable logical-work vector for one bounded partition turn.
/// </summary>
[GenerateSerializer]
internal sealed class PartitionQueryPageWork
{
    /// <summary>
    /// One precharge before a complete canonical <see cref="GrainId"/> candidate group is exposed
    /// to ownership and predicate evaluation. A later predicate-budget stop retains this charge.
    /// </summary>
    [Id(0)]
    public long OrderedCandidateVisitCount { get; init; }

    /// <summary>One per live record occurrence inspected within a candidate group.</summary>
    [Id(1)]
    public long RecordProbeCount { get; init; }

    /// <summary>One per query-plan node occurrence evaluated against a record.</summary>
    [Id(2)]
    public long PredicateNodeProbeCount { get; init; }

    /// <summary>One per record index-entry occurrence inspected by a leaf predicate.</summary>
    [Id(3)]
    public long IndexEntryProbeCount { get; init; }

    /// <summary>One routing-ownership probe per visited canonical candidate group.</summary>
    [Id(4)]
    public long OwnershipProbeCount { get; init; }

    /// <summary>
    /// One before each ordered catalog, exact posting, range-bucket view, or selected range-bucket
    /// posting seek.
    /// </summary>
    [Id(5)]
    public long PostingSeekCount { get; init; }

    /// <summary>One per ordered range bucket visited while constructing a candidate stream.</summary>
    [Id(6)]
    public long RangeBucketVisitCount { get; init; }

    /// <summary>One per matching <see cref="GrainId"/> materialized into the response.</summary>
    [Id(7)]
    public long ResultMaterializationCount { get; init; }

    /// <summary>
    /// One per range-posting candidate occurrence loaded into the merge and one per canonical
    /// comparison performed while merging or grouping those occurrences.
    /// </summary>
    [Id(8)]
    public long RangeMergeOperationCount { get; init; }

    /// <summary>Gets the checked sum of every logical-work component.</summary>
    public long TotalOperationCount => checked(
        OrderedCandidateVisitCount
        + RecordProbeCount
        + PredicateNodeProbeCount
        + IndexEntryProbeCount
        + OwnershipProbeCount
        + PostingSeekCount
        + RangeBucketVisitCount
        + ResultMaterializationCount
        + RangeMergeOperationCount);
}

internal enum PartitionQueryPageStopReason
{
    Exhausted = 0,
    WorkBudget = 1,
    ItemLimit = 2,
    ByteLimit = 3,
}

/// <summary>
/// Reports that a positive partition limit cannot complete even one canonical candidate group.
/// </summary>
[GenerateSerializer]
internal sealed class PartitionQueryBudgetTooSmallException : Exception
{
    public PartitionQueryBudgetTooSmallException(
        long requestedLimit,
        long minimumRequired,
        PartitionQueryPageStopReason reason)
        : base(CreateMessage(requestedLimit, minimumRequired, reason))
    {
        RequestedLimit = requestedLimit;
        MinimumRequired = minimumRequired;
        Reason = reason;
    }

    [Id(0)]
    public long RequestedLimit { get; private set; }

    [Id(1)]
    public long MinimumRequired { get; private set; }

    [Id(2)]
    public PartitionQueryPageStopReason Reason { get; private set; }

    private static string CreateMessage(
        long requestedLimit,
        long minimumRequired,
        PartitionQueryPageStopReason reason)
    {
        return $"The partition {reason} limit {requestedLimit} cannot complete the next "
            + $"canonical candidate group; at least {minimumRequired} is required.";
    }
}

/// <summary>
/// Reports that a partition call was routed using a layout which is no longer authoritative.
/// </summary>
[GenerateSerializer]
internal sealed class StorageRouteMismatchException : Exception
{
    public StorageRouteMismatchException(
        long expectedEpoch,
        long currentEpoch,
        int requestedPartition,
        int? slot = null,
        int? currentOwner = null)
        : base(CreateMessage(expectedEpoch, currentEpoch, requestedPartition, slot, currentOwner))
    {
        ExpectedEpoch = expectedEpoch;
        CurrentEpoch = currentEpoch;
        RequestedPartition = requestedPartition;
        Slot = slot;
        CurrentOwner = currentOwner;
    }

    [Id(0)]
    public long ExpectedEpoch { get; private set; }

    [Id(1)]
    public long CurrentEpoch { get; private set; }

    [Id(2)]
    public int RequestedPartition { get; private set; }

    [Id(3)]
    public int? Slot { get; private set; }

    [Id(4)]
    public int? CurrentOwner { get; private set; }

    private static string CreateMessage(
        long expectedEpoch,
        long currentEpoch,
        int requestedPartition,
        int? slot,
        int? currentOwner)
    {
        if (slot is not null && currentOwner is not null)
        {
            return $"Routing epoch {expectedEpoch} sent virtual slot {slot} to physical partition "
                + $"{requestedPartition}, but layout epoch {currentEpoch} assigns that slot to partition {currentOwner}.";
        }

        return $"Routing epoch {expectedEpoch} sent a query to physical partition {requestedPartition}, "
            + $"but the current routing layout is at epoch {currentEpoch}.";
    }
}

internal enum PartitionQueryOperation
{
    Empty = 0,
    Exact = 1,
    Range = 2,
    And = 3,
    Or = 4,
    All = 5,
}
