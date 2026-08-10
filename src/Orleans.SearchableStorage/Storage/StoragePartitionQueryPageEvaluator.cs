using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Evaluates one bounded canonical prefix against an activation-local, serially consistent view.
/// The evaluator retains no cursor state between calls.
/// </summary>
internal static class StoragePartitionQueryPageEvaluator
{
    public static PartitionQueryPageResult EvaluateValidated(
        RoutedPartitionQueryPageRequest request,
        StoragePartitionView view,
        StorageLayoutSnapshot routing,
        int partitionIndex,
        byte[] queryFingerprint,
        byte[] layoutFingerprint)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(view);
        ArgumentNullException.ThrowIfNull(routing);
        ArgumentNullException.ThrowIfNull(queryFingerprint);
        ArgumentNullException.ThrowIfNull(layoutFingerprint);
        ArgumentOutOfRangeException.ThrowIfNegative(partitionIndex);

        var work = new PageWorkAccumulator(request.WorkBudget);
        if (request.Query.Operation == PartitionQueryOperation.Empty
            || HasEmptyConjunct(request.Query))
        {
            return CreateResult(
                request,
                routing,
                queryFingerprint,
                layoutFingerprint,
                items: [],
                itemByteCount: 0,
                exhausted: true,
                hasFrontier: false,
                frontier: default,
                PartitionQueryPageStopReason.Exhausted,
                work.Snapshot);
        }

        using var candidates = CreateCandidateCursor(request, view.OrderedIndexes, ref work);
        if (!candidates.HasCandidate)
        {
            return CreateResult(
                request,
                routing,
                queryFingerprint,
                layoutFingerprint,
                items: [],
                itemByteCount: 0,
                exhausted: true,
                hasFrontier: false,
                frontier: default,
                PartitionQueryPageStopReason.Exhausted,
                work.Snapshot);
        }

        var catalog = view.OrderedIndexes.GetStateCatalog(request.StateName);
        var items = new List<GrainId>(request.ItemLimit);
        var itemByteCount = 0;
        var hasFrontier = false;
        var frontier = default(GrainId);

        while (candidates.HasCandidate)
        {
            if (items.Count == request.ItemLimit)
            {
                return CreateStoppedResult(PartitionQueryPageStopReason.ItemLimit);
            }

            if (itemByteCount == request.ByteLimit)
            {
                return CreateStoppedResult(PartitionQueryPageStopReason.ByteLimit);
            }

            var take = candidates.TryTakeNext(ref work, out var candidate);
            if (take == CandidateTakeResult.WorkBudget)
            {
                return StopForWork();
            }

            if (take != CandidateTakeResult.Candidate)
            {
                throw new InvalidOperationException(
                    "A candidate cursor reported work while it had no current candidate.");
            }

            if (!work.TryRecordOwnershipProbe())
            {
                return StopForWork();
            }

            var slot = StorageLayout.GetSlot(candidate, routing.VirtualSlotCount);
            var isOwned = routing.GetOwner(slot) == partitionIndex;
            var matches = false;
            if (isOwned && catalog.TryGetRecordKeys(candidate, out var recordKeys))
            {
                foreach (var recordKey in recordKeys)
                {
                    if (!work.TryRecordRecordProbe())
                    {
                        return StopForWork();
                    }

                    if (!view.Records.TryGetValue(recordKey, out var record))
                    {
                        throw new InvalidOperationException(
                            $"The ordered state catalog references missing record '{recordKey}'.");
                    }

                    var predicate = EvaluateRecord(request.Query, record, ref work);
                    if (!predicate.Completed)
                    {
                        return StopForWork();
                    }

                    if (predicate.Matches)
                    {
                        matches = true;
                        break;
                    }
                }
            }

            if (matches)
            {
                var encodedLength = GrainIdCanonicalOrder.GetEncodedLength(candidate);
                if (encodedLength > request.ByteLimit)
                {
                    throw new PartitionQueryBudgetTooSmallException(
                        request.ByteLimit,
                        encodedLength,
                        PartitionQueryPageStopReason.ByteLimit);
                }

                var nextByteCount = checked(itemByteCount + encodedLength);
                if (nextByteCount > request.ByteLimit)
                {
                    return CreateStoppedResult(PartitionQueryPageStopReason.ByteLimit);
                }

                if (!work.TryRecordResultMaterialization())
                {
                    return StopForWork();
                }

                items.Add(candidate);
                itemByteCount = nextByteCount;
            }

            // The complete ownership and predicate group has finished. Advancing only here proves
            // that no matching id at or before this canonical boundary was omitted.
            frontier = candidate;
            hasFrontier = true;

            if (candidates.HasCandidate && items.Count == request.ItemLimit)
            {
                return CreateStoppedResult(PartitionQueryPageStopReason.ItemLimit);
            }

            if (candidates.HasCandidate && itemByteCount == request.ByteLimit)
            {
                return CreateStoppedResult(PartitionQueryPageStopReason.ByteLimit);
            }
        }

        return CreateResult(
            request,
            routing,
            queryFingerprint,
            layoutFingerprint,
            [.. items],
            itemByteCount,
            exhausted: true,
            hasFrontier: false,
            frontier: default,
            PartitionQueryPageStopReason.Exhausted,
            work.Snapshot);

        PartitionQueryPageResult StopForWork()
        {
            if (!hasFrontier)
            {
                throw CreateWorkBudgetTooSmall(request, work);
            }

            return CreateStoppedResult(PartitionQueryPageStopReason.WorkBudget);
        }

        PartitionQueryPageResult CreateStoppedResult(PartitionQueryPageStopReason reason)
        {
            if (!hasFrontier)
            {
                throw new InvalidOperationException(
                    "A non-exhausted partition page cannot succeed without advancing its frontier.");
            }

            return CreateResult(
                request,
                routing,
                queryFingerprint,
                layoutFingerprint,
                [.. items],
                itemByteCount,
                exhausted: false,
                hasFrontier: true,
                frontier,
                reason,
                work.Snapshot);
        }
    }

    private static IOrderedCandidateCursor CreateCandidateCursor(
        RoutedPartitionQueryPageRequest request,
        StoragePartitionOrderedIndexes indexes,
        ref PageWorkAccumulator work)
    {
        var exactDriver = FindConjunctiveLeaf(request.Query, PartitionQueryOperation.Exact);
        if (exactDriver is not null)
        {
            // Select the first conjunctive exact leaf by plan order. Unlike cardinality-based
            // selection, this performs no data-dependent posting reads before the budget charge.
            if (!work.TryRecordPostingSeek())
            {
                throw CreateWorkBudgetTooSmall(request, work);
            }

            var posting = indexes.GetExactPosting(
                exactDriver.Scope!,
                exactDriver.IndexKind,
                exactDriver.Value!);
            return new OrderedGroupCandidateCursor(
                posting.CreateCursorAfter(request.HasAfter, request.After));
        }

        var rangeDriver = FindConjunctiveLeaf(request.Query, PartitionQueryOperation.Range);
        if (rangeDriver is not null)
        {
            var rangeCursor = TryCreateRangeMergeCursor(
                request,
                rangeDriver,
                indexes,
                ref work);
            if (rangeCursor is not null)
            {
                return rangeCursor;
            }
        }

        if (!work.TryRecordPostingSeek())
        {
            throw CreateWorkBudgetTooSmall(request, work);
        }

        var catalog = indexes.GetStateCatalog(request.StateName);
        return new OrderedGroupCandidateCursor(
            catalog.CreateCursorAfter(request.HasAfter, request.After));
    }

    private static RangeMergeCandidateCursor? TryCreateRangeMergeCursor(
        RoutedPartitionQueryPageRequest request,
        PartitionQueryPlan rangeDriver,
        StoragePartitionOrderedIndexes indexes,
        ref PageWorkAccumulator work)
    {
        // This charge covers the O(log D) range-bucket tree seek and the first bucket prefetch.
        if (!work.TryRecordPostingSeek())
        {
            throw CreateWorkBudgetTooSmall(request, work);
        }

        var selection = indexes.CreateRangeBucketCursor(
            rangeDriver.Scope!,
            rangeDriver.LowerBound,
            rangeDriver.UpperBound);
        using var buckets = selection.Cursor;
        if (!buckets.HasCurrent)
        {
            return new RangeMergeCandidateCursor(
                new RangeMergeHeap(),
                []);
        }

        // Total scope cardinality is an O(1), conservative upper bound for this range view. If the
        // complete heap cannot be initialized within the remaining budget, fall back before
        // touching a bucket so catalog traversal retains the rest of the turn.
        var maximumInitializationWork = GetMaximumRangeInitializationWork(
            selection.TotalBucketCount);
        if (maximumInitializationWork > work.RemainingBudget)
        {
            return null;
        }

        var heap = new RangeMergeHeap();
        var postingCursors = new List<OrderedGrainGroupCursor>();
        var completed = false;
        try
        {
            var sourceOrdinal = 0;
            while (buckets.HasCurrent)
            {
                if (!work.TryRecordRangeBucketVisit())
                {
                    return null;
                }

                if (!buckets.TakeCurrentAndAdvance(out var bucket))
                {
                    throw new InvalidOperationException(
                        "A range-bucket cursor lost its prefetched bucket.");
                }

                // Inclusive tree views deliberately expose equal open endpoints so those buckets
                // consume work before being excluded from the merge.
                if (!IsWithinRange(bucket.Value, rangeDriver))
                {
                    continue;
                }

                if (!work.TryRecordPostingSeek())
                {
                    return null;
                }

                var posting = bucket.Posting.CreateCursorAfter(
                    request.HasAfter,
                    request.After);
                postingCursors.Add(posting);
                if (!posting.HasCurrent)
                {
                    sourceOrdinal++;
                    continue;
                }

                if (!TryLoadAndEnqueue(
                        posting,
                        sourceOrdinal++,
                        heap,
                        ref work))
                {
                    return null;
                }
            }

            completed = true;
            return new RangeMergeCandidateCursor(heap, postingCursors);
        }
        finally
        {
            if (!completed)
            {
                foreach (var posting in postingCursors)
                {
                    posting.Dispose();
                }
            }
        }
    }

    private static long GetMaximumRangeInitializationWork(int totalBucketCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(totalBucketCount);
        if (totalBucketCount == 0)
        {
            return 0;
        }

        var maximumComparisonsPerInsertion = 0;
        var value = totalBucketCount - 1;
        while (value > 0)
        {
            maximumComparisonsPerInsertion++;
            value >>= 1;
        }

        // Each possible selected bucket consumes one bucket visit, one posting seek, one candidate
        // occurrence load, and at most ceil(log2(D)) canonical heap comparisons.
        return checked(
            (long)totalBucketCount
            * (3L + maximumComparisonsPerInsertion));
    }

    private static PartitionQueryPlan? FindConjunctiveLeaf(
        PartitionQueryPlan query,
        PartitionQueryOperation operation)
    {
        if (query.Operation == operation)
        {
            return query;
        }

        if (query.Operation != PartitionQueryOperation.And)
        {
            return null;
        }

        return FindConjunctiveLeaf(query.Left!, operation)
            ?? FindConjunctiveLeaf(query.Right!, operation);
    }

    private static bool HasEmptyConjunct(PartitionQueryPlan query)
    {
        if (query.Operation == PartitionQueryOperation.Empty)
        {
            return true;
        }

        return query.Operation == PartitionQueryOperation.And
            && (HasEmptyConjunct(query.Left!) || HasEmptyConjunct(query.Right!));
    }

    private static PredicateEvaluation EvaluateRecord(
        PartitionQueryPlan query,
        StoredRecord record,
        ref PageWorkAccumulator work)
    {
        if (!work.TryRecordPredicateNodeProbe())
        {
            return PredicateEvaluation.Incomplete;
        }

        switch (query.Operation)
        {
            case PartitionQueryOperation.Empty:
                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.Exact:
                foreach (var entry in record.IndexEntries)
                {
                    if (!work.TryRecordIndexEntryProbe())
                    {
                        return PredicateEvaluation.Incomplete;
                    }

                    if (entry.Kind == query.IndexKind
                        && string.Equals(entry.Scope, query.Scope, StringComparison.Ordinal)
                        && entry.Value.Equals(query.Value))
                    {
                        return PredicateEvaluation.Match;
                    }
                }

                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.Range:
                foreach (var entry in record.IndexEntries)
                {
                    if (!work.TryRecordIndexEntryProbe())
                    {
                        return PredicateEvaluation.Incomplete;
                    }

                    if (entry.Kind != SearchableIndexKind.Range
                        || !string.Equals(entry.Scope, query.Scope, StringComparison.Ordinal)
                        || !IsWithinRange(entry.Value, query))
                    {
                        continue;
                    }

                    return PredicateEvaluation.Match;
                }

                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.And:
            {
                var left = EvaluateRecord(query.Left!, record, ref work);
                if (!left.Completed || !left.Matches)
                {
                    return left;
                }

                return EvaluateRecord(query.Right!, record, ref work);
            }
            case PartitionQueryOperation.Or:
            {
                var left = EvaluateRecord(query.Left!, record, ref work);
                if (!left.Completed || left.Matches)
                {
                    return left;
                }

                return EvaluateRecord(query.Right!, record, ref work);
            }
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(query),
                    query.Operation,
                    "Unknown partition query operation.");
        }
    }

    private static bool IsWithinRange(IndexValue value, PartitionQueryPlan query)
    {
        if (query.LowerBound is not null)
        {
            var lowerComparison = value.CompareTo(query.LowerBound);
            if (lowerComparison < 0 || (lowerComparison == 0 && !query.IncludeLowerBound))
            {
                return false;
            }
        }

        if (query.UpperBound is not null)
        {
            var upperComparison = value.CompareTo(query.UpperBound);
            if (upperComparison > 0 || (upperComparison == 0 && !query.IncludeUpperBound))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryLoadAndEnqueue(
        OrderedGrainGroupCursor posting,
        int sourceOrdinal,
        RangeMergeHeap heap,
        ref PageWorkAccumulator work)
    {
        if (!posting.HasCurrent)
        {
            return true;
        }

        // Charge before reading and advancing one raw posting occurrence.
        if (!work.TryRecordRangeMergeOperation()
            || !posting.TakeCurrentAndAdvance(out var grainId))
        {
            return false;
        }

        return heap.TryEnqueue(
            new RangeMergeNode(grainId, sourceOrdinal, posting),
            ref work);
    }

    private static PartitionQueryBudgetTooSmallException CreateWorkBudgetTooSmall(
        RoutedPartitionQueryPageRequest request,
        PageWorkAccumulator work)
    {
        return new PartitionQueryBudgetTooSmallException(
            request.WorkBudget,
            checked(work.TotalOperationCount + 1),
            PartitionQueryPageStopReason.WorkBudget);
    }

    private static PartitionQueryPageResult CreateResult(
        RoutedPartitionQueryPageRequest request,
        StorageLayoutSnapshot routing,
        byte[] queryFingerprint,
        byte[] layoutFingerprint,
        GrainId[] items,
        int itemByteCount,
        bool exhausted,
        bool hasFrontier,
        GrainId frontier,
        PartitionQueryPageStopReason stopReason,
        PartitionQueryPageWork work)
    {
        return new PartitionQueryPageResult
        {
            Items = items,
            HasFrontier = hasFrontier,
            Frontier = frontier,
            Exhausted = exhausted,
            StopReason = stopReason,
            Work = work,
            ItemByteCount = itemByteCount,
            ProtocolVersion = request.ProtocolVersion,
            OrderingVersion = request.OrderingVersion,
            WorkPolicyVersion = request.WorkPolicyVersion,
            ResponseFamily = request.ResponseFamily,
            Epoch = routing.Epoch,
            QueryFingerprint = [.. queryFingerprint],
            LayoutFormatVersion = routing.FormatVersion,
            LayoutFingerprint = [.. layoutFingerprint],
        };
    }

    private enum CandidateTakeResult
    {
        Candidate,
        Exhausted,
        WorkBudget,
    }

    private interface IOrderedCandidateCursor : IDisposable
    {
        bool HasCandidate { get; }

        CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate);
    }

    private sealed class OrderedGroupCandidateCursor(OrderedGrainGroupCursor cursor)
        : IOrderedCandidateCursor
    {
        public bool HasCandidate => cursor.HasCurrent;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            if (!cursor.HasCurrent)
            {
                candidate = default;
                return CandidateTakeResult.Exhausted;
            }

            // The charge precedes both reading the current tree node and advancing its enumerator.
            if (!work.TryRecordOrderedCandidateVisit())
            {
                candidate = default;
                return CandidateTakeResult.WorkBudget;
            }

            if (!cursor.TakeCurrentAndAdvance(out candidate))
            {
                throw new InvalidOperationException(
                    "An ordered group cursor lost its prefetched candidate.");
            }

            return CandidateTakeResult.Candidate;
        }

        public void Dispose() => cursor.Dispose();
    }

    private sealed class RangeMergeCandidateCursor(
        RangeMergeHeap heap,
        IReadOnlyList<OrderedGrainGroupCursor> postingCursors)
        : IOrderedCandidateCursor
    {
        public bool HasCandidate => heap.Count > 0;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            candidate = default;
            if (heap.Count == 0)
            {
                return CandidateTakeResult.Exhausted;
            }

            if (!heap.TryDequeue(ref work, out var node))
            {
                return CandidateTakeResult.WorkBudget;
            }

            var grouped = node.GrainId;
            if (!TryLoadAndEnqueue(
                    node.Posting,
                    node.SourceOrdinal,
                    heap,
                    ref work))
            {
                return CandidateTakeResult.WorkBudget;
            }

            while (heap.Count > 0)
            {
                if (!heap.TryCompareRootTo(grouped, ref work, out var comparison))
                {
                    return CandidateTakeResult.WorkBudget;
                }

                if (comparison != 0)
                {
                    break;
                }

                if (!heap.TryDequeue(ref work, out node)
                    || !TryLoadAndEnqueue(
                        node.Posting,
                        node.SourceOrdinal,
                        heap,
                        ref work))
                {
                    return CandidateTakeResult.WorkBudget;
                }
            }

            if (!work.TryRecordOrderedCandidateVisit())
            {
                return CandidateTakeResult.WorkBudget;
            }

            candidate = grouped;
            return CandidateTakeResult.Candidate;
        }

        public void Dispose()
        {
            foreach (var posting in postingCursors)
            {
                posting.Dispose();
            }
        }
    }

    private sealed class RangeMergeHeap
    {
        private readonly List<RangeMergeNode> _nodes = [];

        public int Count => _nodes.Count;

        public bool TryEnqueue(RangeMergeNode node, ref PageWorkAccumulator work)
        {
            _nodes.Add(node);
            var child = _nodes.Count - 1;
            while (child > 0)
            {
                var parent = (child - 1) / 2;
                if (!TryCompareNodes(_nodes[child], _nodes[parent], ref work, out var comparison))
                {
                    return false;
                }

                if (comparison >= 0)
                {
                    break;
                }

                (_nodes[parent], _nodes[child]) = (_nodes[child], _nodes[parent]);
                child = parent;
            }

            return true;
        }

        public bool TryDequeue(
            ref PageWorkAccumulator work,
            out RangeMergeNode node)
        {
            if (_nodes.Count == 0)
            {
                node = default;
                return false;
            }

            node = _nodes[0];
            var lastIndex = _nodes.Count - 1;
            var last = _nodes[lastIndex];
            _nodes.RemoveAt(lastIndex);
            if (_nodes.Count == 0)
            {
                return true;
            }

            _nodes[0] = last;
            var parent = 0;
            while (true)
            {
                var left = checked((parent * 2) + 1);
                if (left >= _nodes.Count)
                {
                    return true;
                }

                var selected = left;
                var right = left + 1;
                if (right < _nodes.Count)
                {
                    if (!TryCompareNodes(
                            _nodes[right],
                            _nodes[left],
                            ref work,
                            out var childComparison))
                    {
                        return false;
                    }

                    if (childComparison < 0)
                    {
                        selected = right;
                    }
                }

                if (!TryCompareNodes(
                        _nodes[selected],
                        _nodes[parent],
                        ref work,
                        out var parentComparison))
                {
                    return false;
                }

                if (parentComparison >= 0)
                {
                    return true;
                }

                (_nodes[parent], _nodes[selected]) = (_nodes[selected], _nodes[parent]);
                parent = selected;
            }
        }

        public bool TryCompareRootTo(
            GrainId grainId,
            ref PageWorkAccumulator work,
            out int comparison)
        {
            if (_nodes.Count == 0)
            {
                comparison = 1;
                return true;
            }

            if (!work.TryRecordRangeMergeOperation())
            {
                comparison = 0;
                return false;
            }

            comparison = GrainIdCanonicalOrder.Compare(_nodes[0].GrainId, grainId);
            return true;
        }

        private static bool TryCompareNodes(
            RangeMergeNode left,
            RangeMergeNode right,
            ref PageWorkAccumulator work,
            out int comparison)
        {
            if (!work.TryRecordRangeMergeOperation())
            {
                comparison = 0;
                return false;
            }

            comparison = GrainIdCanonicalOrder.Compare(left.GrainId, right.GrainId);
            if (comparison == 0)
            {
                comparison = left.SourceOrdinal.CompareTo(right.SourceOrdinal);
            }

            return true;
        }
    }

    private readonly record struct RangeMergeNode(
        GrainId GrainId,
        int SourceOrdinal,
        OrderedGrainGroupCursor Posting);

    private readonly record struct PredicateEvaluation(bool Completed, bool Matches)
    {
        public static PredicateEvaluation Incomplete => new(false, false);

        public static PredicateEvaluation NoMatch => new(true, false);

        public static PredicateEvaluation Match => new(true, true);
    }

    private struct PageWorkAccumulator(long budget)
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
        };

        public bool TryRecordOrderedCandidateVisit() => TryRecord(ref _orderedCandidateVisitCount);

        public bool TryRecordRecordProbe() => TryRecord(ref _recordProbeCount);

        public bool TryRecordPredicateNodeProbe() => TryRecord(ref _predicateNodeProbeCount);

        public bool TryRecordIndexEntryProbe() => TryRecord(ref _indexEntryProbeCount);

        public bool TryRecordOwnershipProbe() => TryRecord(ref _ownershipProbeCount);

        public bool TryRecordPostingSeek() => TryRecord(ref _postingSeekCount);

        public bool TryRecordRangeBucketVisit() => TryRecord(ref _rangeBucketVisitCount);

        public bool TryRecordResultMaterialization() => TryRecord(ref _resultMaterializationCount);

        public bool TryRecordRangeMergeOperation() => TryRecord(ref _rangeMergeOperationCount);

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
}
