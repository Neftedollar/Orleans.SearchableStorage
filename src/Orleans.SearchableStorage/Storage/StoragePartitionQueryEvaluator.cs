using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Evaluates a validated wire query plan against one partition's live derived indexes.
/// </summary>
internal static class StoragePartitionQueryEvaluator
{
    public static HashSet<string> Evaluate(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(indexes);
        QueryPlanValidator.Validate(query);
        return EvaluateValidated(query, indexes);
    }

    internal static HashSet<string> EvaluateValidated(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(indexes);
        return EvaluateCore(query, indexes);
    }

    private static HashSet<string> EvaluateCore(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        return query.Operation switch
        {
            PartitionQueryOperation.Empty => new HashSet<string>(StringComparer.Ordinal),
            PartitionQueryOperation.Exact => EvaluateExact(query, indexes),
            PartitionQueryOperation.Range => EvaluateRange(query, indexes),
            PartitionQueryOperation.And => EvaluateAnd(query, indexes),
            PartitionQueryOperation.Or => EvaluateOr(query, indexes),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.Operation,
                "Unknown partition query operation."),
        };
    }

    private static HashSet<string> EvaluateExact(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        var scope = query.Scope
            ?? throw new ArgumentException("An exact query requires an index scope.", nameof(query));
        var value = query.Value
            ?? throw new ArgumentException("An exact query requires an index value.", nameof(query));
        var records = query.IndexKind switch
        {
            SearchableIndexKind.Hash => indexes.FindHashEntries(scope, value),
            SearchableIndexKind.Range => indexes.FindRangeEntries(scope, value),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.IndexKind,
                "Unknown index kind."),
        };

        // Lookup methods return live buckets. Boolean nodes mutate only this private copy so a
        // query cannot corrupt the derived indexes used by later reads.
        return new HashSet<string>(records, StringComparer.Ordinal);
    }

    private static HashSet<string> EvaluateRange(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        var scope = query.Scope
            ?? throw new ArgumentException("A range query requires an index scope.", nameof(query));
        if (query.LowerBound is null && query.UpperBound is null)
        {
            throw new ArgumentException("A range query requires at least one bound.", nameof(query));
        }

        if (query.LowerBound is not null
            && query.UpperBound is not null
            && query.LowerBound.CompareTo(query.UpperBound) > 0)
        {
            throw new ArgumentException(
                "The lower range bound must not be greater than the upper range bound.",
                nameof(query));
        }

        var records = new HashSet<string>(StringComparer.Ordinal);
        indexes.UnionRange(
            scope,
            query.LowerBound,
            query.UpperBound,
            query.IncludeLowerBound,
            query.IncludeUpperBound,
            records);
        return records;
    }

    private static HashSet<string> EvaluateAnd(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        var left = EvaluateCore(GetRequiredChild(query.Left, "left", query), indexes);
        left.IntersectWith(EvaluateCore(GetRequiredChild(query.Right, "right", query), indexes));
        return left;
    }

    private static HashSet<string> EvaluateOr(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        var left = EvaluateCore(GetRequiredChild(query.Left, "left", query), indexes);
        left.UnionWith(EvaluateCore(GetRequiredChild(query.Right, "right", query), indexes));
        return left;
    }

    private static PartitionQueryPlan GetRequiredChild(
        PartitionQueryPlan? child,
        string side,
        PartitionQueryPlan query)
    {
        return child
            ?? throw new ArgumentException(
                $"A boolean query requires a {side} child plan.",
                nameof(query));
    }
}
