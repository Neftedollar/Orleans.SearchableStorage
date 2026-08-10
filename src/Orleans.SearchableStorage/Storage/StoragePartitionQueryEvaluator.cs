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

    /// <summary>
    /// Evaluates a query and reports deterministic logical work without changing the wire plan or
    /// the result-set semantics used by normal partition calls.
    /// </summary>
    internal static StoragePartitionQueryEvaluation EvaluateWithWork(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(indexes);
        QueryPlanValidator.Validate(query);

        var work = default(CountingPartitionQueryWorkSink);
        var recordKeys = EvaluateCore(query, indexes, ref work);
        return new StoragePartitionQueryEvaluation(recordKeys, work.Snapshot);
    }

    internal static HashSet<string> EvaluateValidated(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(indexes);
        var work = default(NoPartitionQueryWorkSink);
        return EvaluateCore(query, indexes, ref work);
    }

    private static HashSet<string> EvaluateCore<TWorkSink>(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes,
        ref TWorkSink work)
        where TWorkSink : struct, IPartitionQueryWorkSink
    {
        return query.Operation switch
        {
            PartitionQueryOperation.Empty => EvaluateEmpty(ref work),
            PartitionQueryOperation.Exact => EvaluateExact(query, indexes, ref work),
            PartitionQueryOperation.Range => EvaluateRange(query, indexes, ref work),
            PartitionQueryOperation.And => EvaluateAnd(query, indexes, ref work),
            PartitionQueryOperation.Or => EvaluateOr(query, indexes, ref work),
            _ => throw new ArgumentOutOfRangeException(
                nameof(query),
                query.Operation,
                "Unknown partition query operation."),
        };
    }

    private static HashSet<string> EvaluateEmpty<TWorkSink>(ref TWorkSink work)
        where TWorkSink : struct, IPartitionQueryWorkSink
    {
        work.RecordEmpty();
        return new HashSet<string>(StringComparer.Ordinal);
    }

    private static HashSet<string> EvaluateExact<TWorkSink>(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes,
        ref TWorkSink work)
        where TWorkSink : struct, IPartitionQueryWorkSink
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

        work.RecordExact(records.Count);
        // Lookup methods return live buckets. Boolean nodes mutate only this private copy so a
        // query cannot corrupt the derived indexes used by later reads.
        return new HashSet<string>(records, StringComparer.Ordinal);
    }

    private static HashSet<string> EvaluateRange<TWorkSink>(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes,
        ref TWorkSink work)
        where TWorkSink : struct, IPartitionQueryWorkSink
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

        work.RecordRange();
        var records = new HashSet<string>(StringComparer.Ordinal);
        indexes.UnionRange(
            scope,
            query.LowerBound,
            query.UpperBound,
            query.IncludeLowerBound,
            query.IncludeUpperBound,
            records,
            ref work);
        return records;
    }

    private static HashSet<string> EvaluateAnd<TWorkSink>(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes,
        ref TWorkSink work)
        where TWorkSink : struct, IPartitionQueryWorkSink
    {
        var left = EvaluateCore(GetRequiredChild(query.Left, "left", query), indexes, ref work);
        var right = EvaluateCore(GetRequiredChild(query.Right, "right", query), indexes, ref work);
        work.RecordAnd(left.Count);
        left.IntersectWith(right);
        return left;
    }

    private static HashSet<string> EvaluateOr<TWorkSink>(
        PartitionQueryPlan query,
        StoragePartitionIndexes indexes,
        ref TWorkSink work)
        where TWorkSink : struct, IPartitionQueryWorkSink
    {
        var left = EvaluateCore(GetRequiredChild(query.Left, "left", query), indexes, ref work);
        var right = EvaluateCore(GetRequiredChild(query.Right, "right", query), indexes, ref work);
        work.RecordOr(right.Count);
        left.UnionWith(right);
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
