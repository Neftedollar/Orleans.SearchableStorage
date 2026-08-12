using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Querying;

/// <summary>
/// Precomputes query-shape requirements used to validate one owner response.
/// </summary>
/// <remarks>
/// The analysis is deliberately independent of partition data and planner allowance order. It
/// validates query-shape compatibility and coarse structural lower bounds; exact work vectors
/// remain the responsibility of the evaluator and its work-policy tests.
/// </remarks>
internal sealed class QueryResponseRequirements
{
    private readonly PlanAnalysis _plan;
    private readonly MatchMinimums _match;

    private QueryResponseRequirements(int wireNodeCount, PlanAnalysis plan, MatchMinimums match)
    {
        WireNodeCount = wireNodeCount;
        _plan = plan;
        _match = match;
    }

    public int WireNodeCount { get; }

    /// <summary>
    /// Gets one reachable minimum-combined successful evaluation proof for a single record.
    /// The concrete pair remains reachable even when the independent component minima come from
    /// different Boolean branches.
    /// </summary>
    public (long PredicateNodeCount, long IndexEntryCount) MinimumCombinedMatchWork =>
        _match.CanMatch
            ? (_match.CombinedPredicateNodeCount, _match.CombinedIndexEntryCount)
            : throw new InvalidOperationException("The query cannot match a record.");

    public static QueryResponseRequirements Create(PartitionQueryPlan root)
    {
        ArgumentNullException.ThrowIfNull(root);

        return new QueryResponseRequirements(
            CountWireNodes(root),
            AnalyzePlan(root),
            AnalyzePreparedMatch(root));
    }

    public bool Allows(PartitionQueryAccessPath accessPath) => accessPath switch
    {
        PartitionQueryAccessPath.Empty => _plan.EmptyLowerBounds.Count > 0,
        PartitionQueryAccessPath.ExactPosting => _plan.ExactLowerBounds.Count > 0,
        PartitionQueryAccessPath.RangeMerge => _plan.RangeLowerBounds.Count > 0,
        PartitionQueryAccessPath.Union => _plan.UnionLowerBounds.Count > 0,
        PartitionQueryAccessPath.Catalog => _plan.CanBeNonEmpty,
        _ => false,
    };

    public bool MeetsEmptyPlanningLowerBound(PartitionQueryPageWork work) =>
        MeetsAnyPlanningLowerBound(_plan.EmptyLowerBounds, work);

    public bool MeetsAccessPathPlanningLowerBound(
        PartitionQueryAccessPath accessPath,
        PartitionQueryPageWork work)
    {
        var proofs = accessPath switch
        {
            PartitionQueryAccessPath.Empty => _plan.EmptyLowerBounds,
            PartitionQueryAccessPath.ExactPosting => _plan.ExactLowerBounds,
            PartitionQueryAccessPath.RangeMerge => _plan.RangeLowerBounds,
            PartitionQueryAccessPath.Union => _plan.UnionLowerBounds,
            _ => [],
        };
        return MeetsAnyPlanningLowerBound(proofs, work);
    }

    public bool HasMinimumMaterializedPredicateWork(
        long materializedItemCount,
        PartitionQueryPageWork work)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(materializedItemCount);
        if (materializedItemCount == 0)
        {
            return true;
        }

        if (!_match.CanMatch)
        {
            return false;
        }

        var minimumPredicate = checked(materializedItemCount * _match.PredicateNodeCount);
        var minimumIndexEntries = checked(materializedItemCount * _match.IndexEntryCount);
        var minimumCombined = checked(materializedItemCount * _match.CombinedCount);
        var reportedCombined = checked(
            work.PredicateNodeProbeCount + work.IndexEntryProbeCount);
        return work.PredicateNodeProbeCount >= minimumPredicate
            && work.IndexEntryProbeCount >= minimumIndexEntries
            && reportedCombined >= minimumCombined;
    }

    private static int CountWireNodes(PartitionQueryPlan root)
    {
        var count = 0;
        var pending = new Stack<PartitionQueryPlan>();
        pending.Push(root);
        while (pending.TryPop(out var current))
        {
            count = checked(count + 1);
            if (current.Right is not null)
            {
                pending.Push(current.Right);
            }

            if (current.Left is not null)
            {
                pending.Push(current.Left);
            }
        }

        return count;
    }

    private static PlanAnalysis AnalyzePlan(PartitionQueryPlan query)
    {
        return query.Operation switch
        {
            PartitionQueryOperation.Empty => PlanAnalysis.Empty,
            PartitionQueryOperation.All => PlanAnalysis.All,
            PartitionQueryOperation.Exact => PlanAnalysis.Exact,
            PartitionQueryOperation.Range => PlanAnalysis.Range,
            PartitionQueryOperation.And => AnalyzeAndPlan(
                AnalyzePlan(query.Left!),
                AnalyzePlan(query.Right!)),
            PartitionQueryOperation.Or => AnalyzeOrPlan(
                AnalyzePlan(query.Left!),
                AnalyzePlan(query.Right!)),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.Operation,
                "Unknown partition query operation."),
        };
    }

    private static PlanAnalysis AnalyzeAndPlan(PlanAnalysis left, PlanAnalysis right)
    {
        var canBeNonEmpty = left.CanBeNonEmpty && right.CanBeNonEmpty;
        return new PlanAnalysis(
            UnionPlanningLowerBounds(left.EmptyLowerBounds, right.EmptyLowerBounds),
            canBeNonEmpty
                ? UnionPlanningLowerBounds(left.ExactLowerBounds, right.ExactLowerBounds)
                : [],
            canBeNonEmpty
                ? UnionPlanningLowerBounds(left.RangeLowerBounds, right.RangeLowerBounds)
                : [],
            canBeNonEmpty
                ? UnionPlanningLowerBounds(left.UnionLowerBounds, right.UnionLowerBounds)
                : [],
            canBeNonEmpty);
    }

    private static PlanAnalysis AnalyzeOrPlan(PlanAnalysis left, PlanAnalysis right)
    {
        var empty = AddPlanningLowerBounds(left.EmptyLowerBounds, right.EmptyLowerBounds);
        var exact = UnionPlanningLowerBounds(
            AddPlanningLowerBounds(left.ExactLowerBounds, right.EmptyLowerBounds),
            AddPlanningLowerBounds(left.EmptyLowerBounds, right.ExactLowerBounds));
        var range = UnionPlanningLowerBounds(
            AddPlanningLowerBounds(left.RangeLowerBounds, right.EmptyLowerBounds),
            AddPlanningLowerBounds(left.EmptyLowerBounds, right.RangeLowerBounds));
        var propagatedUnion = UnionPlanningLowerBounds(
            AddPlanningLowerBounds(left.UnionLowerBounds, right.EmptyLowerBounds),
            AddPlanningLowerBounds(left.EmptyLowerBounds, right.UnionLowerBounds));
        var combinedUnion = AddPlanningLowerBounds(
            left.SelectiveLowerBounds,
            right.SelectiveLowerBounds);
        return new PlanAnalysis(
            empty,
            exact,
            range,
            UnionPlanningLowerBounds(propagatedUnion, combinedUnion),
            left.CanBeNonEmpty || right.CanBeNonEmpty);
    }

    private static MatchMinimums AnalyzePreparedMatch(PartitionQueryPlan query)
    {
        var prepared = PreparedScalarQuery.CreateForAnalysis(query);
        var proofs = AnalyzePreparedEvaluation(prepared).MatchProofs;
        if (proofs.Count == 0)
        {
            return MatchMinimums.NoMatch;
        }

        var minimumCombined = proofs.MinBy(
            static proof => checked(proof.PredicateNodeCount + proof.IndexEntryCount));
        return new MatchMinimums(
            CanMatch: true,
            PredicateNodeCount: proofs.Min(static proof => proof.PredicateNodeCount),
            IndexEntryCount: proofs.Min(static proof => proof.IndexEntryCount),
            CombinedCount: checked(
                minimumCombined.PredicateNodeCount + minimumCombined.IndexEntryCount),
            CombinedPredicateNodeCount: minimumCombined.PredicateNodeCount,
            CombinedIndexEntryCount: minimumCombined.IndexEntryCount);
    }

    private static EvaluationProofs AnalyzePreparedEvaluation(PreparedScalarQuery query)
    {
        return query.Operation switch
        {
            PartitionQueryOperation.Empty => new EvaluationProofs(
                MatchProofs: [],
                NoMatchProofs: [new EvaluationProof(1, 0)]),
            PartitionQueryOperation.All => new EvaluationProofs(
                MatchProofs: [new EvaluationProof(1, 0)],
                NoMatchProofs: []),
            PartitionQueryOperation.Exact or PartitionQueryOperation.Range =>
                new EvaluationProofs(
                    MatchProofs: [new EvaluationProof(1, 1)],
                    NoMatchProofs: [new EvaluationProof(1, 0)]),
            PartitionQueryOperation.And => AnalyzePreparedAndEvaluation(query.Operands),
            PartitionQueryOperation.Or => AnalyzePreparedOrEvaluation(query.Operands),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.Operation,
                "Unknown partition query operation."),
        };
    }

    private static EvaluationProofs AnalyzePreparedAndEvaluation(
        IReadOnlyList<PreparedScalarQuery> operands)
    {
        IReadOnlyList<EvaluationProof> matchedPrefix = [new EvaluationProof(0, 0)];
        IReadOnlyList<EvaluationProof> noMatch = [];
        foreach (var operand in operands)
        {
            var child = AnalyzePreparedEvaluation(operand);
            noMatch = UnionEvaluationProofs(
                noMatch,
                AddEvaluationProofs(matchedPrefix, child.NoMatchProofs));
            matchedPrefix = AddEvaluationProofs(matchedPrefix, child.MatchProofs);
            if (matchedPrefix.Count == 0)
            {
                break;
            }
        }

        return new EvaluationProofs(
            AddEvaluationRoot(matchedPrefix),
            AddEvaluationRoot(noMatch));
    }

    private static EvaluationProofs AnalyzePreparedOrEvaluation(
        IReadOnlyList<PreparedScalarQuery> operands)
    {
        IReadOnlyList<EvaluationProof> unmatchedPrefix = [new EvaluationProof(0, 0)];
        IReadOnlyList<EvaluationProof> match = [];
        foreach (var operand in operands)
        {
            var child = AnalyzePreparedEvaluation(operand);
            match = UnionEvaluationProofs(
                match,
                AddEvaluationProofs(unmatchedPrefix, child.MatchProofs));
            unmatchedPrefix = AddEvaluationProofs(unmatchedPrefix, child.NoMatchProofs);
            if (unmatchedPrefix.Count == 0)
            {
                break;
            }
        }

        return new EvaluationProofs(
            AddEvaluationRoot(match),
            AddEvaluationRoot(unmatchedPrefix));
    }

    private static List<EvaluationProof> AddEvaluationRoot(
        IReadOnlyList<EvaluationProof> proofs)
    {
        var result = new List<EvaluationProof>(proofs.Count);
        foreach (var proof in proofs)
        {
            result.Add(new EvaluationProof(
                checked(1 + proof.PredicateNodeCount),
                proof.IndexEntryCount));
        }

        return result;
    }

    private static List<EvaluationProof> AddEvaluationProofs(
        IReadOnlyList<EvaluationProof> left,
        IReadOnlyList<EvaluationProof> right)
    {
        if (left.Count == 0 || right.Count == 0)
        {
            return [];
        }

        var sums = new List<EvaluationProof>(checked(left.Count * right.Count));
        foreach (var leftProof in left)
        {
            foreach (var rightProof in right)
            {
                sums.Add(new EvaluationProof(
                    checked(leftProof.PredicateNodeCount + rightProof.PredicateNodeCount),
                    checked(leftProof.IndexEntryCount + rightProof.IndexEntryCount)));
            }
        }

        return NormalizeEvaluationProofs(sums);
    }

    private static IReadOnlyList<EvaluationProof> UnionEvaluationProofs(
        IReadOnlyList<EvaluationProof> left,
        IReadOnlyList<EvaluationProof> right)
    {
        if (left.Count == 0)
        {
            return right;
        }

        if (right.Count == 0)
        {
            return left;
        }

        var combined = new List<EvaluationProof>(checked(left.Count + right.Count));
        combined.AddRange(left);
        combined.AddRange(right);
        return NormalizeEvaluationProofs(combined);
    }

    private static List<EvaluationProof> NormalizeEvaluationProofs(
        IEnumerable<EvaluationProof> candidates)
    {
        var ordered = candidates
            .Distinct()
            .OrderBy(static proof => proof.PredicateNodeCount)
            .ThenBy(static proof => proof.IndexEntryCount);
        var result = new List<EvaluationProof>();
        var bestIndexEntryCount = long.MaxValue;
        foreach (var proof in ordered)
        {
            if (proof.IndexEntryCount >= bestIndexEntryCount)
            {
                continue;
            }

            result.Add(proof);
            bestIndexEntryCount = proof.IndexEntryCount;
        }

        return result;
    }

    private static List<PlanningLowerBound> AddPlanningLowerBounds(
        IReadOnlyList<PlanningLowerBound> left,
        IReadOnlyList<PlanningLowerBound> right)
    {
        if (left.Count == 0 || right.Count == 0)
        {
            return [];
        }

        var sums = new List<PlanningLowerBound>(checked(left.Count * right.Count));
        foreach (var leftProof in left)
        {
            foreach (var rightProof in right)
            {
                sums.Add(new PlanningLowerBound(
                    checked(leftProof.PostingSeekCount + rightProof.PostingSeekCount),
                    checked(
                        leftProof.PlannerMetadataReadCount
                        + rightProof.PlannerMetadataReadCount),
                    checked(
                        leftProof.RangeBucketVisitCount
                        + rightProof.RangeBucketVisitCount)));
            }
        }

        return NormalizePlanningLowerBounds(sums);
    }

    private static IReadOnlyList<PlanningLowerBound> UnionPlanningLowerBounds(
        IReadOnlyList<PlanningLowerBound> left,
        IReadOnlyList<PlanningLowerBound> right)
    {
        if (left.Count == 0)
        {
            return right;
        }

        if (right.Count == 0)
        {
            return left;
        }

        var combined = new List<PlanningLowerBound>(checked(left.Count + right.Count));
        combined.AddRange(left);
        combined.AddRange(right);
        return NormalizePlanningLowerBounds(combined);
    }

    private static List<PlanningLowerBound> NormalizePlanningLowerBounds(
        IEnumerable<PlanningLowerBound> candidates)
    {
        var ordered = candidates
            .Distinct()
            .OrderBy(static proof => proof.PostingSeekCount)
            .ThenBy(static proof => proof.PlannerMetadataReadCount)
            .ThenBy(static proof => proof.RangeBucketVisitCount);
        var result = new List<PlanningLowerBound>();
        foreach (var proof in ordered)
        {
            if (result.Any(existing =>
                    existing.PostingSeekCount <= proof.PostingSeekCount
                    && existing.PlannerMetadataReadCount <= proof.PlannerMetadataReadCount
                    && existing.RangeBucketVisitCount <= proof.RangeBucketVisitCount))
            {
                continue;
            }

            result.Add(proof);
        }

        return result;
    }

    private static bool MeetsAnyPlanningLowerBound(
        IReadOnlyList<PlanningLowerBound> lowerBounds,
        PartitionQueryPageWork work)
    {
        foreach (var lowerBound in lowerBounds)
        {
            if (work.PostingSeekCount >= lowerBound.PostingSeekCount
                && work.PlannerMetadataReadCount >= lowerBound.PlannerMetadataReadCount
                && work.RangeBucketVisitCount >= lowerBound.RangeBucketVisitCount)
            {
                return true;
            }
        }

        return false;
    }

    private readonly record struct PlanningLowerBound(
        long PostingSeekCount,
        long PlannerMetadataReadCount,
        long RangeBucketVisitCount);

    private readonly record struct EvaluationProof(
        long PredicateNodeCount,
        long IndexEntryCount);

    private readonly record struct EvaluationProofs(
        IReadOnlyList<EvaluationProof> MatchProofs,
        IReadOnlyList<EvaluationProof> NoMatchProofs);

    private readonly record struct MatchMinimums(
        bool CanMatch,
        long PredicateNodeCount,
        long IndexEntryCount,
        long CombinedCount,
        long CombinedPredicateNodeCount,
        long CombinedIndexEntryCount)
    {
        public static MatchMinimums NoMatch => new(
            CanMatch: false,
            PredicateNodeCount: 0,
            IndexEntryCount: 0,
            CombinedCount: 0,
            CombinedPredicateNodeCount: 0,
            CombinedIndexEntryCount: 0);
    }

    private sealed class PlanAnalysis(
        IReadOnlyList<PlanningLowerBound> emptyLowerBounds,
        IReadOnlyList<PlanningLowerBound> exactLowerBounds,
        IReadOnlyList<PlanningLowerBound> rangeLowerBounds,
        IReadOnlyList<PlanningLowerBound> unionLowerBounds,
        bool canBeNonEmpty)
    {
        public static PlanAnalysis Empty { get; } = new(
            emptyLowerBounds: [new PlanningLowerBound(0, 0, 0)],
            exactLowerBounds: [],
            rangeLowerBounds: [],
            unionLowerBounds: [],
            canBeNonEmpty: false);

        public static PlanAnalysis All { get; } = new(
            emptyLowerBounds: [],
            exactLowerBounds: [],
            rangeLowerBounds: [],
            unionLowerBounds: [],
            canBeNonEmpty: true);

        public static PlanAnalysis Exact { get; } = new(
            emptyLowerBounds: [new PlanningLowerBound(1, 1, 0)],
            exactLowerBounds: [new PlanningLowerBound(2, 1, 0)],
            rangeLowerBounds: [],
            unionLowerBounds: [],
            canBeNonEmpty: true);

        public static PlanAnalysis Range { get; } = new(
            emptyLowerBounds: [new PlanningLowerBound(1, 0, 0)],
            exactLowerBounds: [],
            rangeLowerBounds: [new PlanningLowerBound(2, 1, 1)],
            unionLowerBounds: [],
            canBeNonEmpty: true);

        public IReadOnlyList<PlanningLowerBound> EmptyLowerBounds { get; } = emptyLowerBounds;

        public IReadOnlyList<PlanningLowerBound> ExactLowerBounds { get; } = exactLowerBounds;

        public IReadOnlyList<PlanningLowerBound> RangeLowerBounds { get; } = rangeLowerBounds;

        public IReadOnlyList<PlanningLowerBound> UnionLowerBounds { get; } = unionLowerBounds;

        public bool CanBeNonEmpty { get; } = canBeNonEmpty;

        public IReadOnlyList<PlanningLowerBound> SelectiveLowerBounds =>
            QueryResponseRequirements.UnionPlanningLowerBounds(
                QueryResponseRequirements.UnionPlanningLowerBounds(
                    ExactLowerBounds,
                    RangeLowerBounds),
                UnionLowerBounds);
    }
}
