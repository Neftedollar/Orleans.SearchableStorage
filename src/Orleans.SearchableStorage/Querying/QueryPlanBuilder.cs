using System.Diagnostics;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Querying;

internal static class QueryPlanBuilder
{
    public static QueryPlan And(QueryPlan left, QueryPlan right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        var conjuncts = new List<QueryPlan>();
        AddConjuncts(left, conjuncts);
        AddConjuncts(right, conjuncts);
        if (conjuncts.Any(static plan => plan is EmptyQueryPlan))
        {
            return EmptyQueryPlan.Instance;
        }

        var normalized = new List<QueryPlan>(conjuncts.Count);
        foreach (var conjunct in conjuncts)
        {
            var wasCombined = false;
            for (var index = 0; index < normalized.Count; index++)
            {
                if (!TryCombineSameIndex(normalized[index], conjunct, out var combined))
                {
                    continue;
                }

                if (combined is EmptyQueryPlan)
                {
                    return combined;
                }

                normalized[index] = combined;
                wasCombined = true;
                break;
            }

            if (!wasCombined)
            {
                normalized.Add(conjunct);
            }
        }

        return BuildBalanced(
            normalized,
            static (leftPlan, rightPlan) => new AndQueryPlan(leftPlan, rightPlan));
    }

    public static QueryPlan Or(QueryPlan left, QueryPlan right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        var alternatives = new List<QueryPlan>();
        AddAssociativePlans(left, alternatives, static plan => plan is OrQueryPlan);
        AddAssociativePlans(right, alternatives, static plan => plan is OrQueryPlan);
        alternatives.RemoveAll(static plan => plan is EmptyQueryPlan);
        return alternatives.Count == 0
            ? EmptyQueryPlan.Instance
            : BuildBalanced(
                alternatives,
                static (leftPlan, rightPlan) => new OrQueryPlan(leftPlan, rightPlan));
    }

    private static bool TryCombineSameIndex(
        QueryPlan left,
        QueryPlan right,
        out QueryPlan combined)
    {
        combined = EmptyQueryPlan.Instance;
        var leftIndex = GetLeafIndex(left);
        var rightIndex = GetLeafIndex(right);
        if (leftIndex is null
            || rightIndex is null
            || !string.Equals(leftIndex.Scope, rightIndex.Scope, StringComparison.Ordinal))
        {
            return false;
        }

        combined = (left, right) switch
        {
            (ExactQueryPlan leftExact, ExactQueryPlan rightExact) =>
                CombineExact(leftExact, rightExact),
            (ExactQueryPlan exact, RangeQueryPlan range) =>
                CombineExactAndRange(exact, range),
            (RangeQueryPlan range, ExactQueryPlan exact) =>
                CombineExactAndRange(exact, range),
            (RangeQueryPlan leftRange, RangeQueryPlan rightRange) =>
                CombineRanges(leftRange, rightRange),
            _ => throw new UnreachableException(),
        };
        return true;
    }

    private static void AddConjuncts(QueryPlan plan, List<QueryPlan> destination)
    {
        AddAssociativePlans(plan, destination, static candidate => candidate is AndQueryPlan);
    }

    private static void AddAssociativePlans(
        QueryPlan plan,
        List<QueryPlan> destination,
        Func<QueryPlan, bool> isAssociativeNode)
    {
        var pending = new Stack<QueryPlan>();
        pending.Push(plan);
        while (pending.TryPop(out var current))
        {
            if (isAssociativeNode(current))
            {
                var (left, right) = current switch
                {
                    AndQueryPlan and => (and.Left, and.Right),
                    OrQueryPlan or => (or.Left, or.Right),
                    _ => throw new UnreachableException(),
                };
                pending.Push(right);
                pending.Push(left);
                continue;
            }

            destination.Add(current);
        }
    }

    private static QueryPlan BuildBalanced(
        List<QueryPlan> plans,
        Func<QueryPlan, QueryPlan, QueryPlan> combine)
    {
        if (plans.Count == 0)
        {
            throw new ArgumentException("At least one query plan is required.", nameof(plans));
        }

        var currentLevel = plans.ToList();
        while (currentLevel.Count > 1)
        {
            var nextLevel = new List<QueryPlan>((currentLevel.Count + 1) / 2);
            for (var index = 0; index < currentLevel.Count; index += 2)
            {
                nextLevel.Add(index + 1 < currentLevel.Count
                    ? combine(currentLevel[index], currentLevel[index + 1])
                    : currentLevel[index]);
            }

            currentLevel = nextLevel;
        }

        return currentLevel[0];
    }

    private static SelectedIndex? GetLeafIndex(QueryPlan plan)
    {
        return plan switch
        {
            ExactQueryPlan exact => exact.Index,
            RangeQueryPlan range => range.Index,
            _ => null,
        };
    }

    private static QueryPlan CombineExact(ExactQueryPlan left, ExactQueryPlan right)
    {
        return left.Value.Equals(right.Value)
            ? left
            : EmptyQueryPlan.Instance;
    }

    private static QueryPlan CombineExactAndRange(ExactQueryPlan exact, RangeQueryPlan range)
    {
        return IsInsideRange(exact.Value, range)
            ? exact
            : EmptyQueryPlan.Instance;
    }

    private static QueryPlan CombineRanges(RangeQueryPlan left, RangeQueryPlan right)
    {
        var (lowerBound, includeLowerBound) = SelectStrongerLowerBound(left, right);
        var (upperBound, includeUpperBound) = SelectStrongerUpperBound(left, right);
        if (lowerBound is not null && upperBound is not null)
        {
            var comparison = lowerBound.CompareTo(upperBound);
            if (comparison > 0
                || (comparison == 0 && (!includeLowerBound || !includeUpperBound)))
            {
                return EmptyQueryPlan.Instance;
            }
        }

        return new RangeQueryPlan(
            left.Index,
            lowerBound,
            includeLowerBound,
            upperBound,
            includeUpperBound);
    }

    private static (IndexValue? Bound, bool Inclusive) SelectStrongerLowerBound(
        RangeQueryPlan left,
        RangeQueryPlan right)
    {
        if (left.LowerBound is null)
        {
            return (right.LowerBound, right.IncludeLowerBound);
        }

        if (right.LowerBound is null)
        {
            return (left.LowerBound, left.IncludeLowerBound);
        }

        var comparison = left.LowerBound.CompareTo(right.LowerBound);
        return comparison switch
        {
            > 0 => (left.LowerBound, left.IncludeLowerBound),
            < 0 => (right.LowerBound, right.IncludeLowerBound),
            _ => (left.LowerBound, left.IncludeLowerBound && right.IncludeLowerBound),
        };
    }

    private static (IndexValue? Bound, bool Inclusive) SelectStrongerUpperBound(
        RangeQueryPlan left,
        RangeQueryPlan right)
    {
        if (left.UpperBound is null)
        {
            return (right.UpperBound, right.IncludeUpperBound);
        }

        if (right.UpperBound is null)
        {
            return (left.UpperBound, left.IncludeUpperBound);
        }

        var comparison = left.UpperBound.CompareTo(right.UpperBound);
        return comparison switch
        {
            < 0 => (left.UpperBound, left.IncludeUpperBound),
            > 0 => (right.UpperBound, right.IncludeUpperBound),
            _ => (left.UpperBound, left.IncludeUpperBound && right.IncludeUpperBound),
        };
    }

    private static bool IsInsideRange(IndexValue value, RangeQueryPlan range)
    {
        if (range.LowerBound is not null)
        {
            var lowerComparison = value.CompareTo(range.LowerBound);
            if (lowerComparison < 0 || (lowerComparison == 0 && !range.IncludeLowerBound))
            {
                return false;
            }
        }

        if (range.UpperBound is not null)
        {
            var upperComparison = value.CompareTo(range.UpperBound);
            if (upperComparison > 0 || (upperComparison == 0 && !range.IncludeUpperBound))
            {
                return false;
            }
        }

        return true;
    }
}
