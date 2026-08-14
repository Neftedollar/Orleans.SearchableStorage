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
        var query = PreparedScalarQuery.Create(request, ref work);
        using var candidates = ScalarQueryAccessPathPlanner.CreateCandidateCursor(
            request,
            query,
            view.OrderedIndexes,
            ref work);
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
            if (isOwned && catalog.TryGetRecordRefs(candidate, out var recordRefs))
            {
                foreach (var recordRef in recordRefs)
                {
                    if (!work.TryRecordRecordProbe())
                    {
                        return StopForWork();
                    }

                    var record = view.RecordRefs.GetRecord(recordRef);

                    var predicate = EvaluateRecord(query, record, ref work);
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

    private static PredicateEvaluation EvaluateRecord(
        PreparedScalarQuery query,
        StoredRecord record,
        ref PageWorkAccumulator work)
    {
        if (!work.TryRecordPredicateNodeProbe())
        {
            return PredicateEvaluation.Incomplete;
        }

        switch (query.Operation)
        {
            case PartitionQueryOperation.All:
                return PredicateEvaluation.Match;
            case PartitionQueryOperation.Empty:
                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.Exact:
                var exact = query.Leaf!;
                foreach (var entry in record.IndexEntries)
                {
                    if (!work.TryRecordIndexEntryProbe())
                    {
                        return PredicateEvaluation.Incomplete;
                    }

                    if (entry.Kind == exact.IndexKind
                        && string.Equals(entry.Scope, exact.Scope, StringComparison.Ordinal)
                        && entry.Value.Equals(exact.Value))
                    {
                        return PredicateEvaluation.Match;
                    }
                }

                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.Range:
                var range = query.Leaf!;
                foreach (var entry in record.IndexEntries)
                {
                    if (!work.TryRecordIndexEntryProbe())
                    {
                        return PredicateEvaluation.Incomplete;
                    }

                    if (entry.Kind != SearchableIndexKind.Range
                        || !string.Equals(entry.Scope, range.Scope, StringComparison.Ordinal)
                        || !IsWithinRange(entry.Value, range))
                    {
                        continue;
                    }

                    return PredicateEvaluation.Match;
                }

                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.And:
            {
                foreach (var operand in query.Operands)
                {
                    var result = EvaluateRecord(operand, record, ref work);
                    if (!result.Completed || !result.Matches)
                    {
                        return result;
                    }
                }

                return PredicateEvaluation.Match;
            }
            case PartitionQueryOperation.Or:
            {
                foreach (var operand in query.Operands)
                {
                    var result = EvaluateRecord(operand, record, ref work);
                    if (!result.Completed || result.Matches)
                    {
                        return result;
                    }
                }

                return PredicateEvaluation.NoMatch;
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

    private readonly record struct PredicateEvaluation(bool Completed, bool Matches)
    {
        public static PredicateEvaluation Incomplete => new(false, false);

        public static PredicateEvaluation NoMatch => new(true, false);

        public static PredicateEvaluation Match => new(true, true);
    }

}
