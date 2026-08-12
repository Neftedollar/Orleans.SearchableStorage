using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Builds one deterministic, turn-local candidate stream for a validated scalar query plan.
/// Every data-dependent observation is charged before it is made. No planner or cursor state is
/// retained in a continuation.
/// </summary>
internal static partial class ScalarQueryAccessPathPlanner
{
    private const long MinimumFallbackHeadroom = 16;
    // Work-policy 2 retains at least ceil(post-preparation work / 2) for execution.
    private const int MinimumExecutionReserveDivisor = 2;

    public static IOrderedCandidateCursor CreateCandidateCursor(
        RoutedPartitionQueryPageRequest request,
        PreparedScalarQuery query,
        StoragePartitionOrderedIndexes indexes,
        ref PageWorkAccumulator work)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(indexes);

        var fallbackMinimum = GetCatalogFallbackMinimumWork(query);
        var planningReserve = GetPlanningExecutionReserve(
            fallbackMinimum,
            work.RemainingBudget);
        var planned = Plan(
            query,
            indexes,
            fallbackMinimum,
            planningReserve,
            ref work);
        if (planned.IsEmpty)
        {
            work.SetAccessPath(PartitionQueryAccessPath.Empty);
            return new FinalCandidateCursor(EmptyRawCandidateCursor.Instance);
        }

        var path = planned.Path;
        if (path is not null
            && GetSelectiveAdmissionWork(query, path) <= work.RemainingBudget)
        {
            var source = path.Open(request, ref work);
            work.SetAccessPath(path.AccessPath);
            return new FinalCandidateCursor(source);
        }

        // The state catalog is the universal safe candidate source. Its seek is charged only
        // after every selective path has failed admission, so a fallback never inherits a
        // partially initialized cursor.
        Require(request, ref work, static (ref PageWorkAccumulator value) =>
            value.TryRecordPostingSeek());
        var catalog = indexes.GetStateCatalog(request.StateName);
        work.SetAccessPath(PartitionQueryAccessPath.Catalog);
        return new FinalCandidateCursor(
            new CatalogRawCandidateCursor(
                catalog.CreateCursorAfter(request.HasAfter, request.After)));
    }

    private static PlannedNode Plan(
        PreparedScalarQuery query,
        StoragePartitionOrderedIndexes indexes,
        long fallbackMinimum,
        long planningReserve,
        ref PageWorkAccumulator work)
    {
        switch (query.Operation)
        {
            case PartitionQueryOperation.Empty:
                return PlannedNode.Empty;
            case PartitionQueryOperation.All:
                return PlannedNode.Catalog;
            case PartitionQueryOperation.Exact:
                return PlanExact(query, indexes, planningReserve, ref work);
            case PartitionQueryOperation.Range:
                return PlanRange(query, indexes, planningReserve, ref work);
            case PartitionQueryOperation.And:
            case PartitionQueryOperation.Or:
                var operands = PlanOperands(
                    query,
                    indexes,
                    fallbackMinimum,
                    planningReserve,
                    ref work);
                return query.Operation == PartitionQueryOperation.And
                    ? PlanAnd(operands)
                    : PlanOr(query, operands);
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(query),
                    query.Operation,
                    "Unknown partition query operation.");
        }
    }

    private static PlannedNode[] PlanOperands(
        PreparedScalarQuery query,
        StoragePartitionOrderedIndexes indexes,
        long fallbackMinimum,
        long planningReserve,
        ref PageWorkAccumulator work)
    {
        var result = new PlannedNode[query.Operands.Length];
        PreparedPath? bestAndPath = null;
        long sourceHeadroom = 0;
        var unionInputCount = 0;
        var perOperandPlanningAllowance = GetPerOperandPlanningAllowance(
            planningReserve,
            work.RemainingBudget,
            query.Operands.Length);
        for (var index = 0; index < query.Operands.Length; index++)
        {
            var sourceReserve = SaturatingAdd(fallbackMinimum, sourceHeadroom);
            var baseReserve = Math.Max(planningReserve, sourceReserve);
            var childReserve = GetChildPlanningReserve(
                baseReserve,
                work.RemainingBudget,
                perOperandPlanningAllowance);
            var child = Plan(
                query.Operands[index],
                indexes,
                fallbackMinimum,
                childReserve,
                ref work);
            if (child.Path is not null)
            {
                PreparedPath? nextBestAndPath = null;
                var nextSourceHeadroom = query.Operation == PartitionQueryOperation.And
                    ? GetAndSourceHeadroom(bestAndPath, child.Path, out nextBestAndPath)
                    : GetUnionSourceHeadroom(
                        sourceHeadroom,
                        unionInputCount,
                        child.Path);
                if (SaturatingAdd(fallbackMinimum, nextSourceHeadroom)
                    > work.RemainingBudget)
                {
                    child = PlannedNode.Unavailable;
                }
                else
                {
                    sourceHeadroom = nextSourceHeadroom;
                    bestAndPath = nextBestAndPath;
                    if (query.Operation == PartitionQueryOperation.Or)
                    {
                        unionInputCount++;
                    }
                }
            }

            result[index] = child;
        }

        return result;
    }

    private static long GetAndSourceHeadroom(
        PreparedPath? current,
        PreparedPath candidate,
        out PreparedPath best)
    {
        best = current is null
            || PreparedPathComparer.Instance.Compare(candidate, current) < 0
                ? candidate
                : current;
        return best.MaximumSourceInitializationWork;
    }

    private static long GetUnionSourceHeadroom(
        long currentHeadroom,
        int currentInputCount,
        PreparedPath candidate)
    {
        var next = SaturatingAdd(
            currentHeadroom,
            candidate.MaximumSourceInitializationWork);
        // A left-deep union needs one charged fill per side and one charged comparison for every
        // added input before it can expose its first distinct candidate.
        return currentInputCount == 0 ? next : SaturatingAdd(next, 3);
    }

    private static PlannedNode PlanExact(
        PreparedScalarQuery query,
        StoragePartitionOrderedIndexes indexes,
        long planningReserve,
        ref PageWorkAccumulator work)
    {
        if (!TryPlanningCharge(
                ref work,
                planningReserve,
                static (ref PageWorkAccumulator value) => value.TryRecordPostingSeek()))
        {
            return PlannedNode.Unavailable;
        }

        var leaf = query.Leaf!;
        var posting = indexes.GetExactPosting(leaf.Scope!, leaf.IndexKind, leaf.Value!);

        if (!TryPlanningCharge(
                ref work,
                planningReserve,
                static (ref PageWorkAccumulator value) => value.TryRecordPlannerMetadataRead()))
        {
            return PlannedNode.Unavailable;
        }

        var candidateCount = posting.Count;
        return candidateCount == 0
            ? PlannedNode.Empty
            : PlannedNode.From(new ExactPreparedPath(query, posting, candidateCount));
    }

    private static PlannedNode PlanRange(
        PreparedScalarQuery query,
        StoragePartitionOrderedIndexes indexes,
        long planningReserve,
        ref PageWorkAccumulator work)
    {
        if (!TryPlanningCharge(
                ref work,
                planningReserve,
                static (ref PageWorkAccumulator value) => value.TryRecordPostingSeek()))
        {
            return PlannedNode.Unavailable;
        }

        var leaf = query.Leaf!;
        var selection = indexes.CreateRangeBucketCursor(
            leaf.Scope!,
            leaf.LowerBound,
            leaf.UpperBound);

        var selected = new List<OrderedRangeBucket>();
        long candidateUpperBound = 0;
        using (var buckets = selection.Cursor)
        {
            while (buckets.HasCurrent)
            {
                if (!TryPlanningCharge(
                        ref work,
                        planningReserve,
                        static (ref PageWorkAccumulator value) => value.TryRecordRangeBucketVisit()))
                {
                    return PlannedNode.Unavailable;
                }

                if (!buckets.TakeCurrentAndAdvance(out var bucket))
                {
                    throw new InvalidOperationException(
                        "A range-bucket cursor lost its prefetched bucket.");
                }

                // Inclusive tree views expose equal open endpoints. Visiting and rejecting those
                // endpoints is deliberately visible in the work vector.
                if (!IsWithinRange(bucket.Value, leaf))
                {
                    continue;
                }

                if (!TryPlanningCharge(
                        ref work,
                        planningReserve,
                        static (ref PageWorkAccumulator value) =>
                            value.TryRecordPlannerMetadataRead()))
                {
                    return PlannedNode.Unavailable;
                }

                var postingCount = bucket.Posting.Count;
                if (postingCount == 0)
                {
                    continue;
                }

                selected.Add(bucket);
                candidateUpperBound = SaturatingAdd(candidateUpperBound, postingCount);
            }
        }

        return selected.Count == 0
            ? PlannedNode.Empty
            : PlannedNode.From(
                new RangePreparedPath(query, selected, candidateUpperBound));
    }

    private static PlannedNode PlanAnd(IReadOnlyList<PlannedNode> operands)
    {
        if (operands.Any(static operand => operand.IsEmpty))
        {
            return PlannedNode.Empty;
        }

        PreparedPath? best = null;
        foreach (var operand in operands)
        {
            if (operand.Path is not null
                && (best is null
                    || PreparedPathComparer.Instance.Compare(operand.Path, best) < 0))
            {
                best = operand.Path;
            }
        }

        return best is null ? PlannedNode.Catalog : PlannedNode.From(best);
    }

    private static PlannedNode PlanOr(
        PreparedScalarQuery query,
        IReadOnlyList<PlannedNode> operands)
    {
        var active = operands.Where(static operand => !operand.IsEmpty).ToArray();
        if (active.Length == 0)
        {
            return PlannedNode.Empty;
        }

        if (active.Any(static operand =>
                operand.RequiresCatalog || operand.IsUnavailable || operand.Path is null))
        {
            return PlannedNode.Catalog;
        }

        if (active.Length == 1)
        {
            return active[0];
        }

        // Each child exposes its cheapest charged superset path. The N-input union orders those
        // paths deterministically and collapses equal candidates before final predicate work.
        return PlannedNode.From(
            new UnionPreparedPath(
                query,
                active.Select(static operand => operand.Path!).ToArray()));
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

    private static long SaturatingAdd(long left, long right)
    {
        return left > long.MaxValue - right ? long.MaxValue : left + right;
    }

    private static long SaturatingMultiply(long left, long right)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(left);
        ArgumentOutOfRangeException.ThrowIfNegative(right);
        return left != 0 && right > long.MaxValue / left
            ? long.MaxValue
            : left * right;
    }

    private static void Require(
        RoutedPartitionQueryPageRequest request,
        ref PageWorkAccumulator work,
        WorkCharge charge)
    {
        if (!charge(ref work))
        {
            throw CreateWorkBudgetTooSmall(request, work);
        }
    }

    private static bool TryPlanningCharge(
        ref PageWorkAccumulator work,
        long planningReserve,
        WorkCharge charge)
    {
        // An unfinished descriptor is never authoritative. Preserve the caller's deterministic
        // execution/source reserve; an incomplete planner observation cannot spend it.
        return work.RemainingBudget > planningReserve && charge(ref work);
    }

    private static long GetChildPlanningReserve(
        long baseReserve,
        long remainingBudget,
        long planningAllowance)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(planningAllowance);
        if (remainingBudget <= baseReserve)
        {
            return baseReserve;
        }

        var allowance = Math.Min(remainingBudget, planningAllowance);
        return Math.Max(baseReserve, remainingBudget - allowance);
    }

    private static long GetPerOperandPlanningAllowance(
        long planningReserve,
        long remainingBudget,
        int operandCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(operandCount);
        if (remainingBudget <= planningReserve)
        {
            return 0;
        }

        var distributable = remainingBudget - planningReserve;
        var quotient = distributable / operandCount;
        return distributable % operandCount == 0 ? quotient : checked(quotient + 1);
    }

    private static long GetPlanningExecutionReserve(
        long fallbackMinimum,
        long remainingBudget)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fallbackMinimum);
        ArgumentOutOfRangeException.ThrowIfNegative(remainingBudget);

        // At most half of the post-preparation turn may be consumed by speculative descriptor
        // construction. This leaves a deterministic charged execution slice whether planning
        // selects a path or falls back to the catalog.
        var quotient = remainingBudget / MinimumExecutionReserveDivisor;
        var executionReserve = remainingBudget % MinimumExecutionReserveDivisor == 0
            ? quotient
            : checked(quotient + 1);
        return Math.Max(fallbackMinimum, executionReserve);
    }

    private static long GetCatalogFallbackMinimumWork(PreparedScalarQuery query)
    {
        if (query.Operation == PartitionQueryOperation.Empty)
        {
            return 0;
        }

        var predicate = GetMinimumPredicateWork(query);
        // Seek, catalog advance, final ordered-candidate exposure, ownership, record probe, and
        // result materialization surround the least expensive successful predicate evaluation.
        return Math.Max(
            MinimumFallbackHeadroom,
            predicate.Match == long.MaxValue
                ? MinimumFallbackHeadroom
                : SaturatingAdd(6, predicate.Match));
    }

    private static long GetSelectiveAdmissionWork(
        PreparedScalarQuery query,
        PreparedPath path)
    {
        return SaturatingAdd(
            path.MaximumSourceInitializationWork,
            GetMinimumEvaluatorCandidateWork(query));
    }

    private static long GetMinimumEvaluatorCandidateWork(PreparedScalarQuery query)
    {
        var predicate = GetMinimumPredicateWork(query);
        if (predicate.Match == long.MaxValue)
        {
            return long.MaxValue;
        }

        // Final ordered exposure, ownership, one record probe, the least expensive successful
        // predicate, and result materialization complete a safe first frontier.
        return SaturatingAdd(4, predicate.Match);
    }

    private static PredicateWorkBounds GetMinimumPredicateWork(PreparedScalarQuery query)
    {
        switch (query.Operation)
        {
            case PartitionQueryOperation.All:
                return new PredicateWorkBounds(Match: 1, NoMatch: long.MaxValue);
            case PartitionQueryOperation.Empty:
                return new PredicateWorkBounds(Match: long.MaxValue, NoMatch: 1);
            case PartitionQueryOperation.Exact:
            case PartitionQueryOperation.Range:
                return new PredicateWorkBounds(Match: 2, NoMatch: 1);
            case PartitionQueryOperation.And:
            {
                long match = 1;
                long noMatch = long.MaxValue;
                long matchedPrefix = 0;
                foreach (var operand in query.Operands)
                {
                    var child = GetMinimumPredicateWork(operand);
                    noMatch = Math.Min(
                        noMatch,
                        SaturatingAdd(1, SaturatingAdd(matchedPrefix, child.NoMatch)));
                    matchedPrefix = SaturatingAdd(matchedPrefix, child.Match);
                    match = SaturatingAdd(match, child.Match);
                }

                return new PredicateWorkBounds(match, noMatch);
            }
            case PartitionQueryOperation.Or:
            {
                long match = long.MaxValue;
                long noMatch = 1;
                long unmatchedPrefix = 0;
                foreach (var operand in query.Operands)
                {
                    var child = GetMinimumPredicateWork(operand);
                    match = Math.Min(
                        match,
                        SaturatingAdd(1, SaturatingAdd(unmatchedPrefix, child.Match)));
                    unmatchedPrefix = SaturatingAdd(unmatchedPrefix, child.NoMatch);
                    noMatch = SaturatingAdd(noMatch, child.NoMatch);
                }

                return new PredicateWorkBounds(match, noMatch);
            }
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(query),
                    query.Operation,
                    "Unknown partition query operation.");
        }
    }

    private delegate bool WorkCharge(ref PageWorkAccumulator work);

    private readonly record struct PredicateWorkBounds(long Match, long NoMatch);

    private sealed class PlannedNode
    {
        private PlannedNode(
            bool isEmpty,
            bool requiresCatalog,
            bool isUnavailable,
            PreparedPath? path)
        {
            IsEmpty = isEmpty;
            RequiresCatalog = requiresCatalog;
            IsUnavailable = isUnavailable;
            Path = path;
        }

        public static PlannedNode Empty { get; } = new(true, false, false, path: null);

        public static PlannedNode Catalog { get; } = new(false, true, false, path: null);

        public static PlannedNode Unavailable { get; } = new(false, false, true, path: null);

        public bool IsEmpty { get; }

        public bool RequiresCatalog { get; }

        public bool IsUnavailable { get; }

        public PreparedPath? Path { get; }

        public static PlannedNode From(PreparedPath path) => new(false, false, false, path);
    }

    private abstract class PreparedPath(
        PreparedScalarQuery source,
        PartitionQueryAccessPath accessPath,
        long candidateUpperBound,
        long maximumOpenWork,
        long maximumFirstCandidateSourceWork)
    {
        public PreparedScalarQuery Source { get; } = source;

        public PartitionQueryAccessPath AccessPath { get; } = accessPath;

        public long CandidateUpperBound { get; } = candidateUpperBound;

        public long MaximumOpenWork { get; } = maximumOpenWork;

        public long MaximumFirstCandidateSourceWork { get; } =
            maximumFirstCandidateSourceWork;

        public long MaximumSourceInitializationWork { get; } =
            SaturatingAdd(maximumOpenWork, maximumFirstCandidateSourceWork);

        public abstract IRawCandidateCursor Open(
            RoutedPartitionQueryPageRequest request,
            ref PageWorkAccumulator work);
    }

    private sealed class ExactPreparedPath(
        PreparedScalarQuery source,
        OrderedGrainGroups posting,
        long candidateUpperBound)
        : PreparedPath(
            source,
            PartitionQueryAccessPath.ExactPosting,
            candidateUpperBound,
            maximumOpenWork: 1,
            maximumFirstCandidateSourceWork: 1)
    {
        public override IRawCandidateCursor Open(
            RoutedPartitionQueryPageRequest request,
            ref PageWorkAccumulator work)
        {
            if (!work.TryRecordPostingSeek())
            {
                throw CreateWorkBudgetTooSmall(request, work);
            }

            return new PostingRawCandidateCursor(
                posting.CreateCursorAfter(request.HasAfter, request.After));
        }
    }

    private sealed class RangePreparedPath : PreparedPath
    {
        private readonly IReadOnlyList<OrderedRangeBucket> _buckets;

        public RangePreparedPath(
            PreparedScalarQuery source,
            IReadOnlyList<OrderedRangeBucket> buckets,
            long candidateUpperBound)
            : base(
                source,
                PartitionQueryAccessPath.RangeMerge,
                candidateUpperBound,
                GetMaximumOpenWork(buckets.Count),
                GetMaximumFirstCandidateSourceWork(buckets.Count))
        {
            _buckets = buckets;
        }

        public override IRawCandidateCursor Open(
            RoutedPartitionQueryPageRequest request,
            ref PageWorkAccumulator work)
        {
            var heap = new RangeMergeHeap();
            var postingCursors = new List<OrderedGrainGroupCursor>(_buckets.Count);
            var completed = false;
            try
            {
                for (var sourceOrdinal = 0; sourceOrdinal < _buckets.Count; sourceOrdinal++)
                {
                    if (!work.TryRecordPostingSeek())
                    {
                        throw CreateWorkBudgetTooSmall(request, work);
                    }

                    var posting = _buckets[sourceOrdinal].Posting.CreateCursorAfter(
                        request.HasAfter,
                        request.After);
                    postingCursors.Add(posting);
                    if (!posting.HasCurrent)
                    {
                        continue;
                    }

                    if (!RangeMergeRawCandidateCursor.TryLoadAndEnqueue(
                            posting,
                            sourceOrdinal,
                            heap,
                            ref work))
                    {
                        throw CreateWorkBudgetTooSmall(request, work);
                    }
                }

                completed = true;
                return new RangeMergeRawCandidateCursor(heap, postingCursors);
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

        private static long GetMaximumOpenWork(int selectedBucketCount)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(selectedBucketCount);
            if (selectedBucketCount == 0)
            {
                return 0;
            }

            var maximumInsertionDepth = 0;
            for (var value = selectedBucketCount; value > 1; value >>= 1)
            {
                maximumInsertionDepth++;
            }

            // Per selected bucket: posting seek, posting advance, legacy range occurrence, and
            // heap add. Every possible insertion level can add one comparison and one swap.
            return checked(
                (long)selectedBucketCount
                * (4L + (2L * maximumInsertionDepth)));
        }

        private static long GetMaximumFirstCandidateSourceWork(int selectedBucketCount)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(selectedBucketCount);

            var maximumHeapDepth = 0;
            for (var value = selectedBucketCount; value > 1; value >>= 1)
            {
                maximumHeapDepth++;
            }

            // One GrainId can occur once in every selected posting. Before exposing it, the merge
            // must drain all equal heap roots. Each drain can perform a maximum-depth dequeue,
            // load and enqueue the posting's next value, then compare the new root. K=1 has a
            // smaller dequeue; the general bound is intentionally conservative and saturating.
            var maximumPerDuplicate = selectedBucketCount == 1
                ? 5L
                : SaturatingAdd(6, 5L * maximumHeapDepth);
            return SaturatingMultiply(selectedBucketCount, maximumPerDuplicate);
        }
    }

    private sealed class UnionPreparedPath : PreparedPath
    {
        private readonly PreparedPath[] _inputs;

        public UnionPreparedPath(
            PreparedScalarQuery source,
            IReadOnlyCollection<PreparedPath> inputs)
            : base(
                source,
                PartitionQueryAccessPath.Union,
                inputs.Aggregate(
                    0L,
                    static (total, input) => SaturatingAdd(
                        total,
                        input.CandidateUpperBound)),
                inputs.Aggregate(
                    0L,
                    static (total, input) => SaturatingAdd(
                        total,
                        input.MaximumOpenWork)),
                GetMaximumFirstCandidateSourceWork(inputs))
        {
            if (inputs.Count < 2)
            {
                throw new ArgumentException(
                    "A union access path requires at least two inputs.",
                    nameof(inputs));
            }

            _inputs = [.. inputs];
            Array.Sort(_inputs, PreparedPathComparer.Instance);
        }

        private static long GetMaximumFirstCandidateSourceWork(
            IReadOnlyCollection<PreparedPath> inputs)
        {
            var total = inputs.Aggregate(
                0L,
                static (current, input) => SaturatingAdd(
                    current,
                    input.MaximumFirstCandidateSourceWork));
            return SaturatingAdd(total, checked(3L * (inputs.Count - 1)));
        }

        public override IRawCandidateCursor Open(
            RoutedPartitionQueryPageRequest request,
            ref PageWorkAccumulator work)
        {
            var opened = new List<IRawCandidateCursor>(_inputs.Length);
            try
            {
                foreach (var input in _inputs)
                {
                    opened.Add(input.Open(request, ref work));
                }

                IRawCandidateCursor union = opened[0];
                for (var index = 1; index < opened.Count; index++)
                {
                    union = new UnionRawCandidateCursor(union, opened[index]);
                }

                return union;
            }
            catch
            {
                foreach (var input in opened)
                {
                    input.Dispose();
                }

                throw;
            }
        }
    }

    private sealed class PreparedPathComparer : IComparer<PreparedPath>
    {
        public static PreparedPathComparer Instance { get; } = new();

        public int Compare(PreparedPath? left, PreparedPath? right)
        {
            if (ReferenceEquals(left, right))
            {
                return 0;
            }

            if (left is null)
            {
                return -1;
            }

            if (right is null)
            {
                return 1;
            }

            var comparison = left.CandidateUpperBound.CompareTo(right.CandidateUpperBound);
            if (comparison != 0)
            {
                return comparison;
            }

            comparison = left.MaximumSourceInitializationWork.CompareTo(
                right.MaximumSourceInitializationWork);
            if (comparison != 0)
            {
                return comparison;
            }

            comparison = left.AccessPath.CompareTo(right.AccessPath);
            return comparison != 0
                ? comparison
                : left.Source.CanonicalRank.CompareTo(right.Source.CanonicalRank);
        }
    }
}
