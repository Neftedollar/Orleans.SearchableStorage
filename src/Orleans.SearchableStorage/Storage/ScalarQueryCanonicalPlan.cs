using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Turn-local scalar query shape shared by access-path planning and final predicate evaluation.
/// Associative Boolean nodes are flattened and ordered once; wire plans remain unchanged.
/// </summary>
internal sealed class PreparedScalarQuery
{
    private PreparedScalarQuery(
        PartitionQueryOperation operation,
        PartitionQueryPlan? leaf,
        PreparedScalarQuery[] operands,
        int height)
    {
        Operation = operation;
        Leaf = leaf;
        Operands = operands;
        Height = height;
    }

    public PartitionQueryOperation Operation { get; }

    public PartitionQueryPlan? Leaf { get; }

    public PreparedScalarQuery[] Operands { get; }

    public int CanonicalRank { get; private set; }

    private int Height { get; }

    public static PreparedScalarQuery Create(
        RoutedPartitionQueryPageRequest request,
        ref PageWorkAccumulator work)
    {
        ArgumentNullException.ThrowIfNull(request);

        var builder = new Builder(request, work);
        var root = builder.Prepare(request.Query);
        builder.AssignCanonicalRanks();
        work = builder.Work;
        return root;
    }

    /// <summary>
    /// Builds the same flattened, canonically ordered shape without recording execution work.
    /// </summary>
    internal static PreparedScalarQuery CreateForAnalysis(PartitionQueryPlan query)
    {
        ArgumentNullException.ThrowIfNull(query);

        var builder = new Builder();
        var root = builder.Prepare(query);
        builder.AssignCanonicalRanks();
        return root;
    }

    private sealed class Builder
    {
        private readonly List<List<PreparedScalarQuery>> _nodesByHeight = [];
        private readonly RoutedPartitionQueryPageRequest? _request;
        private PageWorkAccumulator _work;

        public Builder()
        {
        }

        public Builder(
            RoutedPartitionQueryPageRequest request,
            PageWorkAccumulator work)
        {
            _request = request;
            _work = work;
        }

        public PageWorkAccumulator Work => _work;

        public PreparedScalarQuery Prepare(PartitionQueryPlan query)
        {
            ChargeNode();
            return PrepareCharged(query);
        }

        public void AssignCanonicalRanks()
        {
            var nextRank = 0;
            for (var height = 0; height < _nodesByHeight.Count; height++)
            {
                var atHeight = _nodesByHeight[height];
                if (height > 0)
                {
                    foreach (var node in atHeight)
                    {
                        Array.Sort(node.Operands, CanonicalRankComparer.Instance);
                    }
                }

                atHeight.Sort(StructuralDescriptorComparer.Instance);
                PreparedScalarQuery? previous = null;
                foreach (var node in atHeight)
                {
                    if (previous is not null
                        && StructuralDescriptorComparer.Instance.Compare(previous, node) != 0)
                    {
                        nextRank = checked(nextRank + 1);
                    }

                    node.CanonicalRank = nextRank;
                    previous = node;
                }

                nextRank = checked(nextRank + 1);
            }
        }

        private PreparedScalarQuery PrepareCharged(PartitionQueryPlan query)
        {
            PreparedScalarQuery prepared;
            switch (query.Operation)
            {
                case PartitionQueryOperation.Empty:
                case PartitionQueryOperation.All:
                case PartitionQueryOperation.Exact:
                case PartitionQueryOperation.Range:
                    prepared = new PreparedScalarQuery(query.Operation, query, [], height: 0);
                    break;
                case PartitionQueryOperation.And:
                case PartitionQueryOperation.Or:
                    var operands = new List<PreparedScalarQuery>();
                    AppendAssociativeOperands(query.Left!, query.Operation, operands);
                    AppendAssociativeOperands(query.Right!, query.Operation, operands);
                    prepared = new PreparedScalarQuery(
                        query.Operation,
                        leaf: null,
                        [.. operands],
                        checked(1 + operands.Max(static operand => operand.Height)));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        nameof(query),
                        query.Operation,
                        "Unknown partition query operation.");
            }

            while (_nodesByHeight.Count <= prepared.Height)
            {
                _nodesByHeight.Add([]);
            }

            _nodesByHeight[prepared.Height].Add(prepared);
            return prepared;
        }

        private void AppendAssociativeOperands(
            PartitionQueryPlan query,
            PartitionQueryOperation operation,
            List<PreparedScalarQuery> operands)
        {
            ChargeNode();
            if (query.Operation == operation)
            {
                AppendAssociativeOperands(query.Left!, operation, operands);
                AppendAssociativeOperands(query.Right!, operation, operands);
                return;
            }

            operands.Add(PrepareCharged(query));
        }

        private void ChargeNode()
        {
            if (_request is null)
            {
                return;
            }

            if (!_work.TryRecordPlannerNodeVisit())
            {
                throw new PartitionQueryBudgetTooSmallException(
                    _request.WorkBudget,
                    checked(_work.TotalOperationCount + 1),
                    PartitionQueryPageStopReason.WorkBudget);
            }
        }
    }

    private sealed class CanonicalRankComparer : IComparer<PreparedScalarQuery>
    {
        public static CanonicalRankComparer Instance { get; } = new();

        public int Compare(PreparedScalarQuery? left, PreparedScalarQuery? right)
        {
            if (ReferenceEquals(left, right))
            {
                return 0;
            }

            if (left is null)
            {
                return -1;
            }

            return right is null
                ? 1
                : left.CanonicalRank.CompareTo(right.CanonicalRank);
        }
    }

    private sealed class StructuralDescriptorComparer : IComparer<PreparedScalarQuery>
    {
        public static StructuralDescriptorComparer Instance { get; } = new();

        public int Compare(PreparedScalarQuery? left, PreparedScalarQuery? right)
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

            var comparison = left.Operation.CompareTo(right.Operation);
            if (comparison != 0)
            {
                return comparison;
            }

            switch (left.Operation)
            {
                case PartitionQueryOperation.Empty:
                case PartitionQueryOperation.All:
                    return 0;
                case PartitionQueryOperation.Exact:
                    comparison = string.Compare(
                        left.Leaf!.Scope,
                        right.Leaf!.Scope,
                        StringComparison.Ordinal);
                    if (comparison != 0)
                    {
                        return comparison;
                    }

                    comparison = left.Leaf.IndexKind.CompareTo(right.Leaf.IndexKind);
                    return comparison != 0
                        ? comparison
                        : left.Leaf.Value!.CompareTo(right.Leaf.Value);
                case PartitionQueryOperation.Range:
                    comparison = string.Compare(
                        left.Leaf!.Scope,
                        right.Leaf!.Scope,
                        StringComparison.Ordinal);
                    if (comparison != 0)
                    {
                        return comparison;
                    }

                    comparison = CompareValue(left.Leaf.LowerBound, right.Leaf.LowerBound);
                    if (comparison != 0)
                    {
                        return comparison;
                    }

                    comparison = CompareValue(left.Leaf.UpperBound, right.Leaf.UpperBound);
                    if (comparison != 0)
                    {
                        return comparison;
                    }

                    comparison = left.Leaf.IncludeLowerBound.CompareTo(
                        right.Leaf.IncludeLowerBound);
                    return comparison != 0
                        ? comparison
                        : left.Leaf.IncludeUpperBound.CompareTo(
                            right.Leaf.IncludeUpperBound);
                case PartitionQueryOperation.And:
                case PartitionQueryOperation.Or:
                    comparison = left.Operands.Length.CompareTo(right.Operands.Length);
                    if (comparison != 0)
                    {
                        return comparison;
                    }

                    for (var index = 0; index < left.Operands.Length; index++)
                    {
                        comparison = left.Operands[index].CanonicalRank.CompareTo(
                            right.Operands[index].CanonicalRank);
                        if (comparison != 0)
                        {
                            return comparison;
                        }
                    }

                    return 0;
                default:
                    throw new ArgumentOutOfRangeException(
                        nameof(left),
                        left.Operation,
                        "Unknown partition query operation.");
            }
        }

        private static int CompareValue(IndexValue? left, IndexValue? right)
        {
            if (ReferenceEquals(left, right))
            {
                return 0;
            }

            if (left is null)
            {
                return -1;
            }

            return right is null ? 1 : left.CompareTo(right);
        }
    }
}
