using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryPlanValidatorTests
{
    [Fact]
    public void WirePlanAcceptsTheDepthLimitAndRejectsTheNextLevel()
    {
        var atLimit = CreateWirePlanAtDepth(QueryPlanLimits.MaximumDepth);
        var beyondLimit = CreateWirePlanAtDepth(QueryPlanLimits.MaximumDepth + 1);

        var accepted = () => QueryPlanValidator.Validate(atLimit);
        var rejected = () => QueryPlanValidator.Validate(beyondLimit);

        accepted.Should().NotThrow();
        rejected.Should().Throw<ArgumentException>()
            .WithParameterName("query")
            .WithMessage($"*maximum supported depth of {QueryPlanLimits.MaximumDepth}*");
    }

    [Fact]
    public void WirePlanRejectsABalancedTreeBeyondTheNodeLimit()
    {
        var largestFullTreeWithinLimit = CreateBalancedWirePlan(
            (QueryPlanLimits.MaximumNodeCount + 1) / 2);
        var firstFullTreeBeyondLimit = CreateBalancedWirePlan(
            ((QueryPlanLimits.MaximumNodeCount + 1) / 2) + 1);

        var accepted = () => QueryPlanValidator.Validate(largestFullTreeWithinLimit);
        var rejected = () => QueryPlanValidator.Validate(firstFullTreeBeyondLimit);

        accepted.Should().NotThrow();
        rejected.Should().Throw<ArgumentException>()
            .WithParameterName("query")
            .WithMessage($"*maximum supported node count of {QueryPlanLimits.MaximumNodeCount}*");
    }

    [Fact]
    public void WirePlanRejectsCyclesWithoutRecursiveTraversal()
    {
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Right = EmptyWirePlan(),
        };
        typeof(PartitionQueryPlan)
            .GetProperty(nameof(PartitionQueryPlan.Left))!
            .SetValue(plan, plan);

        var action = () => QueryPlanValidator.Validate(plan);

        action.Should().Throw<ArgumentException>()
            .WithParameterName("query")
            .WithMessage("*cannot contain cycles or shared nodes*");
    }

    [Fact]
    public void WirePlanRejectsSharedSubtrees()
    {
        var shared = EmptyWirePlan();
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = shared,
            Right = shared,
        };

        var action = () => QueryPlanValidator.Validate(plan);

        action.Should().Throw<ArgumentException>()
            .WithParameterName("query")
            .WithMessage("*cannot contain cycles or shared nodes*");
    }

    [Fact]
    public void WirePlanRejectsMalformedExactLeaves()
    {
        var missingScope = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            IndexKind = SearchableIndexKind.Hash,
            Value = IndexValue.Create(1),
        };
        var missingValue = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = "scope",
            IndexKind = SearchableIndexKind.Hash,
        };
        var unknownKind = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = "scope",
            IndexKind = (SearchableIndexKind)int.MaxValue,
            Value = IndexValue.Create(1),
        };

        AssertInvalidWirePlan<ArgumentException>(missingScope, "*requires an index scope and value*");
        AssertInvalidWirePlan<ArgumentException>(missingValue, "*requires an index scope and value*");
        AssertInvalidWirePlan<ArgumentOutOfRangeException>(unknownKind, "*Unknown index kind*");
    }

    [Fact]
    public void WirePlanRejectsMalformedRangeLeaves()
    {
        var missingScope = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            LowerBound = IndexValue.Create(1),
        };
        var missingBounds = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = "scope",
        };
        var reversedBounds = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = "scope",
            LowerBound = IndexValue.Create(2),
            UpperBound = IndexValue.Create(1),
        };

        AssertInvalidWirePlan<ArgumentException>(missingScope, "*requires an index scope and at least one bound*");
        AssertInvalidWirePlan<ArgumentException>(missingBounds, "*requires an index scope and at least one bound*");
        AssertInvalidWirePlan<ArgumentException>(reversedBounds, "*lower range bound must not be greater*");
    }

    [Fact]
    public void WirePlanRejectsEitherMissingBooleanChild()
    {
        var missingLeft = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Right = EmptyWirePlan(),
        };
        var missingRight = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = EmptyWirePlan(),
        };

        AssertInvalidWirePlan<ArgumentException>(missingLeft, "*requires both child plans*");
        AssertInvalidWirePlan<ArgumentException>(missingRight, "*requires both child plans*");
    }

    [Fact]
    public void WirePlanRejectsHiddenChildrenAndPayloadOnTheWrongNodeKind()
    {
        var hiddenChild = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Empty,
            Left = CreateWirePlanAtDepth(QueryPlanLimits.MaximumDepth + 1),
        };
        var emptyWithLeafPayload = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Empty,
            Scope = "scope",
        };
        var exactWithRangePayload = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = "scope",
            IndexKind = SearchableIndexKind.Hash,
            Value = IndexValue.Create(1),
            LowerBound = IndexValue.Create(1),
        };
        var rangeWithChild = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = "scope",
            LowerBound = IndexValue.Create(1),
            Left = EmptyWirePlan(),
        };
        var booleanWithLeafData = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Scope = "scope",
            Left = EmptyWirePlan(),
            Right = EmptyWirePlan(),
        };

        AssertInvalidWirePlan<ArgumentException>(hiddenChild, "*empty partition query cannot contain*");
        AssertInvalidWirePlan<ArgumentException>(emptyWithLeafPayload, "*empty partition query cannot contain*");
        AssertInvalidWirePlan<ArgumentException>(exactWithRangePayload, "*exact partition query cannot contain*");
        AssertInvalidWirePlan<ArgumentException>(rangeWithChild, "*range partition query cannot contain*");
        AssertInvalidWirePlan<ArgumentException>(booleanWithLeafData, "*boolean partition query cannot contain*");
    }

    [Fact]
    public void WirePlanRejectsInclusivityOnMissingBounds()
    {
        var missingLower = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = "scope",
            UpperBound = IndexValue.Create(2),
            IncludeLowerBound = true,
        };
        var missingUpper = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = "scope",
            LowerBound = IndexValue.Create(1),
            IncludeUpperBound = true,
        };

        AssertInvalidWirePlan<ArgumentException>(missingLower, "*cannot include a missing bound*");
        AssertInvalidWirePlan<ArgumentException>(missingUpper, "*cannot include a missing bound*");
    }

    [Fact]
    public void SemanticPlanFactoryAcceptsTheDepthLimitAndRejectsTheNextLevel()
    {
        var atLimit = CreateSemanticPlanAtDepth(QueryPlanLimits.MaximumDepth);
        var beyondLimit = CreateSemanticPlanAtDepth(QueryPlanLimits.MaximumDepth + 1);

        var accepted = () => PartitionQueryPlanFactory.Create(atLimit);
        var rejected = () => PartitionQueryPlanFactory.Create(beyondLimit);

        accepted.Should().NotThrow();
        rejected.Should().Throw<NotSupportedException>()
            .WithMessage($"*maximum supported depth of {QueryPlanLimits.MaximumDepth}*");
    }

    [Fact]
    public void SemanticPlanFactoryRejectsExcessiveNodeCountBelowTheDepthLimit()
    {
        var atLimit = CreateBalancedSemanticPlan(
            (QueryPlanLimits.MaximumNodeCount + 1) / 2);
        var beyondLimit = CreateBalancedSemanticPlan(
            ((QueryPlanLimits.MaximumNodeCount + 1) / 2) + 1);

        var accepted = () => PartitionQueryPlanFactory.Create(atLimit);
        var rejected = () => PartitionQueryPlanFactory.Create(beyondLimit);

        accepted.Should().NotThrow();
        rejected.Should().Throw<NotSupportedException>()
            .WithMessage($"*maximum supported node count of {QueryPlanLimits.MaximumNodeCount}*");
    }

    [Fact]
    public void SemanticPlanFactoryRejectsCyclesAndSharedSubtrees()
    {
        var cycle = new OrQueryPlan(new EmptyQueryPlan(), new EmptyQueryPlan());
        typeof(OrQueryPlan)
            .GetProperty(nameof(OrQueryPlan.Left))!
            .SetValue(cycle, cycle);
        var shared = new OrQueryPlan(new EmptyQueryPlan(), new EmptyQueryPlan());
        var sharedRoot = new AndQueryPlan(shared, shared);

        var createCycle = () => PartitionQueryPlanFactory.Create(cycle);
        var createShared = () => PartitionQueryPlanFactory.Create(sharedRoot);

        createCycle.Should().Throw<InvalidOperationException>()
            .WithMessage("*cannot contain cycles or shared nodes*");
        createShared.Should().Throw<InvalidOperationException>()
            .WithMessage("*cannot contain cycles or shared nodes*");
    }

    private static PartitionQueryPlan CreateWirePlanAtDepth(int depth)
    {
        var plan = EmptyWirePlan();
        for (var currentDepth = 1; currentDepth < depth; currentDepth++)
        {
            plan = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Or,
                Left = EmptyWirePlan(),
                Right = plan,
            };
        }

        return plan;
    }

    private static PartitionQueryPlan CreateBalancedWirePlan(int leafCount)
    {
        var current = Enumerable.Range(0, leafCount)
            .Select(static _ => EmptyWirePlan())
            .ToList();
        while (current.Count > 1)
        {
            var next = new List<PartitionQueryPlan>((current.Count + 1) / 2);
            for (var index = 0; index < current.Count; index += 2)
            {
                next.Add(index + 1 < current.Count
                    ? new PartitionQueryPlan
                    {
                        Operation = PartitionQueryOperation.Or,
                        Left = current[index],
                        Right = current[index + 1],
                    }
                    : current[index]);
            }

            current = next;
        }

        return current[0];
    }

    private static QueryPlan CreateBalancedSemanticPlan(int leafCount)
    {
        var current = Enumerable.Range(0, leafCount)
            .Select(static _ => (QueryPlan)new EmptyQueryPlan())
            .ToList();
        while (current.Count > 1)
        {
            var next = new List<QueryPlan>((current.Count + 1) / 2);
            for (var index = 0; index < current.Count; index += 2)
            {
                next.Add(index + 1 < current.Count
                    ? new OrQueryPlan(current[index], current[index + 1])
                    : current[index]);
            }

            current = next;
        }

        return current[0];
    }

    private static QueryPlan CreateSemanticPlanAtDepth(int depth)
    {
        QueryPlan plan = new EmptyQueryPlan();
        for (var currentDepth = 1; currentDepth < depth; currentDepth++)
        {
            plan = new OrQueryPlan(new EmptyQueryPlan(), plan);
        }

        return plan;
    }

    private static void AssertInvalidWirePlan<TException>(
        PartitionQueryPlan plan,
        string expectedMessage)
        where TException : ArgumentException
    {
        var action = () => QueryPlanValidator.Validate(plan);

        action.Should().Throw<TException>()
            .WithParameterName("query")
            .WithMessage(expectedMessage);
    }

    private static PartitionQueryPlan EmptyWirePlan()
    {
        return new PartitionQueryPlan { Operation = PartitionQueryOperation.Empty };
    }
}
