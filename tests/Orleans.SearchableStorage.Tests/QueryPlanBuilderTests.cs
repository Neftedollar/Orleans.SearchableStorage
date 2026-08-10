using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryPlanBuilderTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AssociativePlansAreBalancedWithoutReorderingLeaves(bool useIntersection)
    {
        const int leafCount = 64;
        IndexValueConverterProvider.TryGetConverter(typeof(string), out var converter).Should().BeTrue();
        var leaves = Enumerable.Range(0, leafCount)
            .Select(index => (QueryPlan)new ExactQueryPlan(
                new SelectedIndex(
                    $"scope-{index}",
                    SearchableIndexKind.Hash,
                    converter!,
                    $"Property{index}"),
                IndexValue.Create($"value-{index}")))
            .ToArray();
        var plan = leaves.Aggregate((current, next) => useIntersection
            ? QueryPlanBuilder.And(current, next)
            : QueryPlanBuilder.Or(current, next));

        var scopes = Flatten(plan, useIntersection).Select(static leaf => leaf.Index.Scope);

        scopes.Should().Equal(leaves.Cast<ExactQueryPlan>().Select(static leaf => leaf.Index.Scope));
        GetDepth(plan).Should().BeLessThanOrEqualTo(1 + (int)Math.Ceiling(Math.Log2(leafCount)));
        QueryPlanValidator.Validate(plan);
    }

    private static List<ExactQueryPlan> Flatten(QueryPlan plan, bool useIntersection)
    {
        var leaves = new List<ExactQueryPlan>();
        var pending = new Stack<QueryPlan>();
        pending.Push(plan);
        while (pending.TryPop(out var current))
        {
            switch (current)
            {
                case AndQueryPlan intersection when useIntersection:
                    pending.Push(intersection.Right);
                    pending.Push(intersection.Left);
                    break;
                case OrQueryPlan union when !useIntersection:
                    pending.Push(union.Right);
                    pending.Push(union.Left);
                    break;
                default:
                    leaves.Add(current.Should().BeOfType<ExactQueryPlan>().Subject);
                    break;
            }
        }

        return leaves;
    }

    private static int GetDepth(QueryPlan plan)
    {
        var maximumDepth = 0;
        var pending = new Stack<(QueryPlan Plan, int Depth)>();
        pending.Push((plan, 1));
        while (pending.TryPop(out var current))
        {
            maximumDepth = Math.Max(maximumDepth, current.Depth);
            switch (current.Plan)
            {
                case AndQueryPlan and:
                    pending.Push((and.Left, current.Depth + 1));
                    pending.Push((and.Right, current.Depth + 1));
                    break;
                case OrQueryPlan or:
                    pending.Push((or.Left, current.Depth + 1));
                    pending.Push((or.Right, current.Depth + 1));
                    break;
            }
        }

        return maximumDepth;
    }
}
