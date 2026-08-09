using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Querying;

internal static class PartitionQueryPlanFactory
{
    public static PartitionQueryPlan Create(QueryPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        return plan switch
        {
            EmptyQueryPlan => new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Empty,
            },
            ExactQueryPlan exact => new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Exact,
                Scope = exact.Index.Scope,
                IndexKind = exact.Index.Kind,
                Value = exact.Value,
            },
            RangeQueryPlan range => new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Range,
                Scope = range.Index.Scope,
                LowerBound = range.LowerBound,
                UpperBound = range.UpperBound,
                IncludeLowerBound = range.IncludeLowerBound,
                IncludeUpperBound = range.IncludeUpperBound,
            },
            AndQueryPlan and => new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = Create(and.Left),
                Right = Create(and.Right),
            },
            OrQueryPlan or => new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Or,
                Left = Create(or.Left),
                Right = Create(or.Right),
            },
            _ => throw new InvalidOperationException($"Unknown query plan '{plan.GetType()}'."),
        };
    }
}
