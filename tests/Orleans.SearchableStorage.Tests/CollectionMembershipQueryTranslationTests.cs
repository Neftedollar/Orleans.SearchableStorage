using System.Collections;
using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class CollectionMembershipQueryTranslationTests
{
    [Fact]
    public void ExactListContainsAndTwoArgumentEnumerableContainsArrayLowerToExact()
    {
        var tag = "blue";
        var audience = 42;

        var array = Translate(state => Enumerable.Contains(state.Tags!, tag))
            .Should().BeOfType<ExactQueryPlan>().Subject;
        var list = Translate(state => state.AudienceIds!.Contains(audience))
            .Should().BeOfType<ExactQueryPlan>().Subject;

        array.Index.PropertyName.Should().Be(nameof(QueryState.Tags));
        array.Index.Multiplicity.Should().Be(Indexing.IndexValueMultiplicity.CollectionMembership);
        array.Value.Text.Should().Be(tag);
        list.Index.PropertyName.Should().Be(nameof(QueryState.AudienceIds));
        list.Value.SignedInteger.Should().Be(audience);
    }

    [Fact]
    public void CollectionContainsRejectsWrongMethodsSourcesAndOperands()
    {
        string? nullTag = null;
        Expression<Func<QueryState, bool>> comparer = state =>
            state.Tags!.Contains("blue", StringComparer.OrdinalIgnoreCase);
        Expression<Func<QueryState, bool>> enumerableList = state =>
            Enumerable.Contains(state.AudienceIds!, 42);
        Expression<Func<QueryState, bool>> set = state => state.Set.Contains(42);
        Expression<Func<QueryState, bool>> nested = state => state.Nested.Tags.Contains("blue");
        Expression<Func<QueryState, bool>> produced = state => state.GetTags().Contains("blue");
        Expression<Func<QueryState, bool>> directComparison = state => state.Tags == Array.Empty<string?>();
        Expression<Func<QueryState, bool>> nullOperand = state => state.Tags!.Contains(nullTag);

        foreach (var predicate in new[]
        {
            comparer,
            enumerableList,
            set,
            nested,
            produced,
            directComparison,
            nullOperand,
        })
        {
            Action action = () => Translate(predicate);
            action.Should().ThrowExactly<NotSupportedException>();
        }
    }

    [Fact]
    public void WhereInSnapshotsInputAndCanonicalizesDuplicatesAndOrder()
    {
        var values = new List<int> { 3, 1, 2, 1 };
        var query = Root().WhereIn(state => state.Score, values);
        values.Clear();
        values.Add(99);
        int[] reorderedValues = [2, 3, 1, 2];

        var plan = QueryTranslator.Translate<QueryState>("state", query.Expression);
        var reversed = QueryTranslator.Translate<QueryState>(
            "state",
            Root().WhereIn(state => state.Score, reorderedValues).Expression);
        var wire = PartitionQueryPlanFactory.Create(plan);
        var reversedWire = PartitionQueryPlanFactory.Create(reversed);

        FlattenOr(plan).Cast<ExactQueryPlan>()
            .Select(static exact => exact.Value.SignedInteger)
            .Should().Equal(1, 2, 3);
        QueryPlanFingerprint.Compute("state", wire).Should()
            .Equal(QueryPlanFingerprint.Compute("state", reversedWire));
    }

    [Fact]
    public void WhereInEmptyAndImpossibleNaNValuesUseScalarEqualitySemantics()
    {
        var empty = QueryTranslator.Translate<QueryState>(
            "state",
            Root().WhereIn(state => state.Score, Array.Empty<int>()).Expression);
        var onlyNaN = QueryTranslator.Translate<QueryState>(
            "state",
            Root().WhereIn(state => state.Ratio, new[] { double.NaN }).Expression);
        var mixed = QueryTranslator.Translate<QueryState>(
            "state",
            Root().WhereIn(state => state.Ratio, new[] { double.NaN, 1d }).Expression);

        empty.Should().BeOfType<EmptyQueryPlan>();
        onlyNaN.Should().BeOfType<EmptyQueryPlan>();
        mixed.Should().BeOfType<ExactQueryPlan>()
            .Which.Value.FloatingPoint.Should().Be(1d);
    }

    [Fact]
    public void WhereInAccepts64BalancedLeavesAndRejects65BeforeProviderMutation()
    {
        var maximum = SearchableStorageQueryLimits.MaximumWhereInValues;
        var query = Root().WhereIn(
            state => state.Score,
            Enumerable.Range(0, maximum).ToArray());
        var plan = QueryTranslator.Translate<QueryState>("state", query.Expression);
        var (nodeCount, maximumDepth) = Measure(plan);
        var root = new TrackingQueryable<QueryState>();

        Action oversized = () => root.WhereIn(
            state => state.Score,
            Enumerable.Range(0, maximum + 1).ToArray());

        FlattenOr(plan).Should().HaveCount(maximum);
        nodeCount.Should().Be((maximum * 2) - 1);
        maximumDepth.Should().Be(7);
        oversized.Should().ThrowExactly<ArgumentOutOfRangeException>()
            .WithParameterName("values");
        root.Provider.CreateQueryCount.Should().Be(0);
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(8, 15, 4)]
    public void WhereInBuildsBalancedPlansAtRepresentativeAdmittedSizes(
        int valueCount,
        int expectedNodeCount,
        int expectedDepth)
    {
        var plan = QueryTranslator.Translate<QueryState>(
            "state",
            Root().WhereIn(
                state => state.Score,
                Enumerable.Range(0, valueCount).Reverse().ToArray())
                .Expression);

        FlattenOr(plan).Should().HaveCount(valueCount);
        Measure(plan).Should().Be((expectedNodeCount, expectedDepth));
    }

    [Fact]
    public void WhereInValidatesSelectorAndNullValuesBeforeCreatingAQuery()
    {
        var root = new TrackingQueryable<QueryState>();
        string?[] values = ["one", null];
        Action nullElement = () => root.WhereIn(state => state.City, values);
        Action collectionSelector = () => QueryTranslator.Translate<QueryState>(
            "state",
            Root().WhereIn(state => state.Tags, Array.Empty<string?[]?>()).Expression);
        Action convertedSelector = () => root.WhereIn(state => (object)state.Score, Array.Empty<object>());
        Action unindexedSelector = () => QueryTranslator.Translate<QueryState>(
            "state",
            Root().WhereIn(state => state.Unindexed, Array.Empty<int>()).Expression);

        nullElement.Should().ThrowExactly<ArgumentException>()
            .WithParameterName("values");
        collectionSelector.Should().ThrowExactly<NotSupportedException>();
        convertedSelector.Should().ThrowExactly<ArgumentException>()
            .WithParameterName("propertySelector");
        unindexedSelector.Should().ThrowExactly<NotSupportedException>()
            .WithMessage("*not searchable*");
        root.Provider.CreateQueryCount.Should().Be(0);
    }

    [Fact]
    public void ExternalProviderReceivesTheDeferredWhereInMarker()
    {
        var root = new TrackingQueryable<QueryState>();
        int[] values = [2, 1];

        var query = root.WhereIn(state => state.Unindexed, values);
        var call = query.Expression.Should().BeAssignableTo<MethodCallExpression>().Subject;
        var snapshot = call.Arguments[2].Should().BeAssignableTo<ConstantExpression>()
            .Which.Value.Should().BeAssignableTo<IReadOnlyList<int>>().Subject;

        root.Provider.CreateQueryCount.Should().Be(1);
        call.Method.Name.Should().Be(nameof(SearchableStorageQueryableExtensions.WhereIn));
        snapshot.Should().Equal(2, 1);
        snapshot.Should().NotBeAssignableTo<int[]>();
    }

    [Fact]
    public void OrdinaryConstantFalseWherePredicateRemainsOutsideTheGrammar()
    {
        var expression = Root().Where(static _ => false).Expression;

        Action translate = () => QueryTranslator.Translate<QueryState>("state", expression);

        translate.Should().ThrowExactly<NotSupportedException>()
            .WithMessage("*'Constant'*not supported*");
    }

    private static QueryPlan Translate(Expression<Func<QueryState, bool>> predicate)
    {
        var expression = Array.Empty<QueryState>().AsQueryable().Where(predicate).Expression;
        return QueryTranslator.Translate<QueryState>("state", expression);
    }

    private static IQueryable<QueryState> Root() => Array.Empty<QueryState>().AsQueryable();

    private static List<QueryPlan> FlattenOr(QueryPlan plan)
    {
        var leaves = new List<QueryPlan>();
        var pending = new Stack<QueryPlan>();
        pending.Push(plan);
        while (pending.TryPop(out var current))
        {
            if (current is OrQueryPlan or)
            {
                pending.Push(or.Right);
                pending.Push(or.Left);
            }
            else
            {
                leaves.Add(current);
            }
        }

        return leaves;
    }

    private static (int NodeCount, int MaximumDepth) Measure(QueryPlan plan)
    {
        var nodeCount = 0;
        var maximumDepth = 0;
        var pending = new Stack<(QueryPlan Plan, int Depth)>();
        pending.Push((plan, 1));
        while (pending.TryPop(out var current))
        {
            nodeCount++;
            maximumDepth = Math.Max(maximumDepth, current.Depth);
            if (current.Plan is OrQueryPlan or)
            {
                pending.Push((or.Right, current.Depth + 1));
                pending.Push((or.Left, current.Depth + 1));
            }
        }

        return (nodeCount, maximumDepth);
    }

    private sealed class QueryState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string?[]? Tags { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public List<int?>? AudienceIds { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Score { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public int? OptionalScore { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public double Ratio { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public string? City { get; init; }

        public HashSet<int> Set { get; init; } = [];

        public NestedState Nested { get; init; } = new();

        public int Unindexed { get; init; }

        public string?[] GetTags() => Tags ?? [];
    }

    private sealed class NestedState
    {
        public List<string> Tags { get; init; } = [];
    }

    private sealed class TrackingQueryable<T> : IQueryable<T>
    {
        public TrackingQueryable()
        {
            Provider = new TrackingProvider();
            Expression = Expression.Constant(this);
        }

        public Type ElementType => typeof(T);

        public Expression Expression { get; }

        public TrackingProvider Provider { get; }

        IQueryProvider IQueryable.Provider => Provider;

        public IEnumerator<T> GetEnumerator() => throw new NotSupportedException();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class TrackingProvider : IQueryProvider
    {
        public int CreateQueryCount { get; private set; }

        public IQueryable CreateQuery(Expression expression) =>
            throw new NotSupportedException();

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            CreateQueryCount++;
            return new ExternalExpressionQuery<TElement>(this, expression);
        }

        public object? Execute(Expression expression) => throw new NotSupportedException();

        public TResult Execute<TResult>(Expression expression) => throw new NotSupportedException();
    }

    private sealed class ExternalExpressionQuery<T>(IQueryProvider provider, Expression expression)
        : IQueryable<T>
    {
        public Type ElementType => typeof(T);

        public Expression Expression { get; } = expression;

        public IQueryProvider Provider { get; } = provider;

        public IEnumerator<T> GetEnumerator() => throw new NotSupportedException();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
