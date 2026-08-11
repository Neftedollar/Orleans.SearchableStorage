using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Querying;

internal static class QueryPlanLimits
{
    public const int MaximumDepth = 64;

    public const int MaximumNodeCount = 256;
}

internal static class QueryPlanValidator
{
    public static void Validate(QueryPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        var visited = new HashSet<QueryPlan>(ReferenceEqualityComparer.Instance);
        var pending = new Stack<(QueryPlan Plan, int Depth)>();
        pending.Push((plan, 1));
        while (pending.TryPop(out var current))
        {
            EnsureWithinLimits(current.Depth, visited.Count + 1, "The translated query");
            if (!visited.Add(current.Plan))
            {
                throw new InvalidOperationException(
                    "A translated query plan must be a tree and cannot contain cycles or shared nodes.");
            }

            switch (current.Plan)
            {
                case AllQueryPlan or EmptyQueryPlan or ExactQueryPlan or RangeQueryPlan:
                    break;
                case AndQueryPlan and:
                    pending.Push((and.Right, current.Depth + 1));
                    pending.Push((and.Left, current.Depth + 1));
                    break;
                case OrQueryPlan or:
                    pending.Push((or.Right, current.Depth + 1));
                    pending.Push((or.Left, current.Depth + 1));
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unknown query plan '{current.Plan.GetType()}'.");
            }
        }
    }

    public static void Validate(PartitionQueryPlan query)
    {
        ArgumentNullException.ThrowIfNull(query);

        var visited = new HashSet<PartitionQueryPlan>(ReferenceEqualityComparer.Instance);
        var pending = new Stack<(PartitionQueryPlan Plan, int Depth)>();
        pending.Push((query, 1));
        while (pending.TryPop(out var current))
        {
            EnsureWirePlanWithinLimits(current.Depth, visited.Count + 1, nameof(query));
            if (!visited.Add(current.Plan))
            {
                throw new ArgumentException(
                    "A partition query plan must be a tree and cannot contain cycles or shared nodes.",
                    nameof(query));
            }

            switch (current.Plan.Operation)
            {
                case PartitionQueryOperation.All:
                case PartitionQueryOperation.Empty:
                    ValidateEmpty(current.Plan, query);
                    break;
                case PartitionQueryOperation.Exact:
                    ValidateExact(current.Plan, query);
                    break;
                case PartitionQueryOperation.Range:
                    ValidateRange(current.Plan, query);
                    break;
                case PartitionQueryOperation.And:
                case PartitionQueryOperation.Or:
                    ValidateBoolean(current.Plan, query);
                    if (current.Plan.Left is null || current.Plan.Right is null)
                    {
                        throw new ArgumentException(
                            "A boolean partition query requires both child plans.",
                            nameof(query));
                    }

                    pending.Push((current.Plan.Right, current.Depth + 1));
                    pending.Push((current.Plan.Left, current.Depth + 1));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        nameof(query),
                        current.Plan.Operation,
                        "Unknown partition query operation.");
            }
        }
    }

    private static void ValidateEmpty(PartitionQueryPlan plan, PartitionQueryPlan query)
    {
        if (HasLeafPayload(plan) || plan.Left is not null || plan.Right is not null)
        {
            throw new ArgumentException(
                "An all/empty partition query cannot contain leaf data or child plans.",
                nameof(query));
        }
    }

    private static void ValidateExact(PartitionQueryPlan plan, PartitionQueryPlan query)
    {
        if (plan.Scope is null || plan.Value is null)
        {
            throw new ArgumentException(
                "An exact partition query requires an index scope and value.",
                nameof(query));
        }

        if (plan.IndexKind is not SearchableIndexKind.Hash and not SearchableIndexKind.Range)
        {
            throw new ArgumentOutOfRangeException(
                nameof(query),
                plan.IndexKind,
                "Unknown index kind.");
        }

        if (plan.LowerBound is not null
            || plan.UpperBound is not null
            || plan.IncludeLowerBound
            || plan.IncludeUpperBound
            || plan.Left is not null
            || plan.Right is not null)
        {
            throw new ArgumentException(
                "An exact partition query cannot contain range data or child plans.",
                nameof(query));
        }
    }

    private static void ValidateRange(PartitionQueryPlan plan, PartitionQueryPlan query)
    {
        if (plan.Scope is null || (plan.LowerBound is null && plan.UpperBound is null))
        {
            throw new ArgumentException(
                "A range partition query requires an index scope and at least one bound.",
                nameof(query));
        }

        if (plan.LowerBound is not null
            && plan.UpperBound is not null
            && plan.LowerBound.CompareTo(plan.UpperBound) > 0)
        {
            throw new ArgumentException(
                "The lower range bound must not be greater than the upper range bound.",
                nameof(query));
        }

        if (plan.Value is not null || plan.Left is not null || plan.Right is not null)
        {
            throw new ArgumentException(
                "A range partition query cannot contain an exact value or child plans.",
                nameof(query));
        }

        if ((plan.LowerBound is null && plan.IncludeLowerBound)
            || (plan.UpperBound is null && plan.IncludeUpperBound))
        {
            throw new ArgumentException(
                "A range partition query cannot include a missing bound.",
                nameof(query));
        }
    }

    private static void ValidateBoolean(PartitionQueryPlan plan, PartitionQueryPlan query)
    {
        if (HasLeafPayload(plan))
        {
            throw new ArgumentException(
                "A boolean partition query cannot contain leaf data.",
                nameof(query));
        }
    }

    private static bool HasLeafPayload(PartitionQueryPlan plan)
    {
        return plan.Scope is not null
            || plan.IndexKind != default
            || plan.Value is not null
            || plan.LowerBound is not null
            || plan.UpperBound is not null
            || plan.IncludeLowerBound
            || plan.IncludeUpperBound;
    }

    private static void EnsureWithinLimits(int depth, int nodeCount, string subject)
    {
        if (depth > QueryPlanLimits.MaximumDepth)
        {
            throw new NotSupportedException(
                $"{subject} exceeds the maximum supported depth of {QueryPlanLimits.MaximumDepth}.");
        }

        if (nodeCount > QueryPlanLimits.MaximumNodeCount)
        {
            throw new NotSupportedException(
                $"{subject} exceeds the maximum supported node count of {QueryPlanLimits.MaximumNodeCount}.");
        }
    }

    private static void EnsureWirePlanWithinLimits(
        int depth,
        int nodeCount,
        string parameterName)
    {
        if (depth > QueryPlanLimits.MaximumDepth)
        {
            throw new ArgumentException(
                $"The partition query exceeds the maximum supported depth of {QueryPlanLimits.MaximumDepth}.",
                parameterName);
        }

        if (nodeCount > QueryPlanLimits.MaximumNodeCount)
        {
            throw new ArgumentException(
                $"The partition query exceeds the maximum supported node count of {QueryPlanLimits.MaximumNodeCount}.",
                parameterName);
        }
    }
}
