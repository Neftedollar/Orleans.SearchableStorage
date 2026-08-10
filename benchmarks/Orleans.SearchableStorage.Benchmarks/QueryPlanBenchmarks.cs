using System.Linq.Expressions;
using BenchmarkDotNet.Attributes;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

[BenchmarkCategory("Querying", "Planning")]
public class QueryPlanConstructionBenchmarks
{
    private Expression _queryExpression = null!;
    private QueryPlan _translatedPlan = null!;

    [Params(2, 16, 64)]
    public int LeafCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _queryExpression = CreateQueryExpression(LeafCount);
        _translatedPlan = QueryTranslator.Translate<QueryState>("benchmark-state", _queryExpression);
        QueryPlanValidator.Validate(_translatedPlan);
        QueryPlanValidator.Validate(PartitionQueryPlanFactory.Create(_translatedPlan));
    }

    [Benchmark]
    public object TranslateExpression() =>
        QueryTranslator.Translate<QueryState>("benchmark-state", _queryExpression);

    [Benchmark]
    public object CreatePartitionWirePlan() =>
        PartitionQueryPlanFactory.Create(_translatedPlan);

    internal void ValidateFixture(object translatedPlan, object wirePlan)
    {
        if (translatedPlan is not QueryPlan translated
            || wirePlan is not PartitionQueryPlan wire)
        {
            throw new InvalidOperationException(
                "The query-plan benchmarks did not return their production plan representations.");
        }

        var cityIndex = IndexMetadataProvider.GetSelectedIndex<QueryState, string>(
            "benchmark-state",
            state => state.City);
        var salaryIndex = IndexMetadataProvider.GetSelectedIndex<QueryState, int>(
            "benchmark-state",
            state => state.Salary);
        if (string.IsNullOrWhiteSpace(cityIndex.Scope)
            || string.IsNullOrWhiteSpace(salaryIndex.Scope)
            || string.Equals(cityIndex.Scope, salaryIndex.Scope, StringComparison.Ordinal)
            || cityIndex.Kind != SearchableIndexKind.Hash
            || salaryIndex.Kind != SearchableIndexKind.Range)
        {
            throw new InvalidOperationException(
                "The query-plan fixture metadata does not match the exact benchmark index contract: "
                + $"city={cityIndex.Scope}/{cityIndex.Kind}, salary={salaryIndex.Scope}/{salaryIndex.Kind}.");
        }

        var translatedLeaves = new List<QueryPlan>(LeafCount);
        CollectTranslatedLeaves(translated, translatedLeaves);
        var wireLeaves = new List<PartitionQueryPlan>(LeafCount);
        CollectWireLeaves(wire, wireLeaves);

        if (translatedLeaves.Count != LeafCount || wireLeaves.Count != LeafCount)
        {
            throw new InvalidOperationException("The query-plan fixture did not preserve the exact leaf count.");
        }

        for (var index = 0; index < LeafCount; index++)
        {
            var expectedValue = index % 2 == 0
                ? IndexValue.Create($"city-{index:D3}")
                : IndexValue.FromSignedInteger(index);
            if (index % 2 == 0)
            {
                if (translatedLeaves[index] is not ExactQueryPlan exactLeaf
                    || !string.Equals(exactLeaf.Index.Scope, cityIndex.Scope, StringComparison.Ordinal)
                    || exactLeaf.Index.Kind != SearchableIndexKind.Hash
                    || !exactLeaf.Value.Equals(expectedValue)
                    || !IsExpectedExactWireLeaf(wireLeaves[index], cityIndex.Scope, expectedValue))
                {
                    throw new InvalidOperationException(
                        $"The query-plan fixture produced an unexpected exact leaf at position {index}.");
                }
            }
            else if (translatedLeaves[index] is not RangeQueryPlan rangeLeaf
                || !string.Equals(rangeLeaf.Index.Scope, salaryIndex.Scope, StringComparison.Ordinal)
                || rangeLeaf.Index.Kind != SearchableIndexKind.Range
                || rangeLeaf.LowerBound is null
                || !rangeLeaf.LowerBound.Equals(expectedValue)
                || !rangeLeaf.IncludeLowerBound
                || rangeLeaf.UpperBound is not null
                || rangeLeaf.IncludeUpperBound
                || !IsExpectedRangeWireLeaf(wireLeaves[index], salaryIndex.Scope, expectedValue))
            {
                throw new InvalidOperationException(
                    $"The query-plan fixture produced an unexpected range leaf at position {index}.");
            }
        }
    }

    private static void CollectTranslatedLeaves(QueryPlan plan, List<QueryPlan> leaves)
    {
        switch (plan)
        {
            case OrQueryPlan or:
                CollectTranslatedLeaves(or.Left, leaves);
                CollectTranslatedLeaves(or.Right, leaves);
                break;
            case ExactQueryPlan or RangeQueryPlan:
                leaves.Add(plan);
                break;
            default:
                throw new InvalidOperationException(
                    $"The query-plan fixture expected only OR nodes and exact/range leaves, not '{plan.GetType()}'.");
        }
    }

    private static void CollectWireLeaves(PartitionQueryPlan plan, List<PartitionQueryPlan> leaves)
    {
        if (plan.Operation == PartitionQueryOperation.Or
            && plan.Left is not null
            && plan.Right is not null)
        {
            CollectWireLeaves(plan.Left, leaves);
            CollectWireLeaves(plan.Right, leaves);
            return;
        }

        if (plan.Operation is PartitionQueryOperation.Exact or PartitionQueryOperation.Range
            && plan.Left is null
            && plan.Right is null)
        {
            leaves.Add(plan);
            return;
        }

        throw new InvalidOperationException(
            "The wire query-plan fixture expected only OR nodes and exact/range leaves.");
    }

    private static bool IsExpectedExactWireLeaf(
        PartitionQueryPlan plan,
        string expectedScope,
        IndexValue expectedValue) =>
        plan.Operation == PartitionQueryOperation.Exact
        && string.Equals(plan.Scope, expectedScope, StringComparison.Ordinal)
        && plan.IndexKind == SearchableIndexKind.Hash
        && plan.Value?.Equals(expectedValue) == true
        && plan.LowerBound is null
        && plan.UpperBound is null
        && !plan.IncludeLowerBound
        && !plan.IncludeUpperBound;

    private static bool IsExpectedRangeWireLeaf(
        PartitionQueryPlan plan,
        string expectedScope,
        IndexValue expectedValue) =>
        plan.Operation == PartitionQueryOperation.Range
        && string.Equals(plan.Scope, expectedScope, StringComparison.Ordinal)
        && plan.Value is null
        && plan.LowerBound?.Equals(expectedValue) == true
        && plan.UpperBound is null
        && plan.IncludeLowerBound
        && !plan.IncludeUpperBound;

    internal static Expression CreateQueryExpression(int leafCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(leafCount);
        var parameter = Expression.Parameter(typeof(QueryState), "state");
        var leaves = new List<Expression>(leafCount);
        for (var index = 0; index < leafCount; index++)
        {
            Expression leaf = index % 2 == 0
                ? Expression.Equal(
                    Expression.Property(parameter, nameof(QueryState.City)),
                    Expression.Constant($"city-{index:D3}"))
                : Expression.GreaterThanOrEqual(
                    Expression.Property(parameter, nameof(QueryState.Salary)),
                    Expression.Constant(index));
            leaves.Add(leaf);
        }

        while (leaves.Count > 1)
        {
            var next = new List<Expression>((leaves.Count + 1) / 2);
            for (var index = 0; index < leaves.Count; index += 2)
            {
                next.Add(index + 1 < leaves.Count
                    ? Expression.OrElse(leaves[index], leaves[index + 1])
                    : leaves[index]);
            }

            leaves = next;
        }

        var predicate = Expression.Lambda<Func<QueryState, bool>>(leaves[0], parameter);
        return Array.Empty<QueryState>()
            .AsQueryable()
            .Where(predicate)
            .Expression;
    }

    internal sealed class QueryState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Salary { get; init; }
    }
}

[BenchmarkCategory("Querying", "Evaluation")]
public class QueryPlanEvaluationBenchmarks
{
    private const int RecordCount = 65_536;
    private StoragePartitionIndexes _indexes = null!;
    private PartitionQueryPlan _plan = null!;

    [Params(2, 16, 64)]
    public int LeafCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _indexes = StoragePartitionIndexes.Build(BenchmarkData.CreateRecords(RecordCount));
        _plan = CreateEvaluationPlan(LeafCount);
        var expectedCount = checked((LeafCount / 2) * 8);
        if (EvaluatePartitionPlan() != expectedCount)
        {
            throw new InvalidOperationException("The deterministic query-evaluation fixture is inconsistent.");
        }
    }

    [Benchmark]
    public int EvaluatePartitionPlan() =>
        StoragePartitionQueryEvaluator.Evaluate(_plan, _indexes).Count;

    internal void ValidateFixture()
    {
        var actual = StoragePartitionQueryEvaluator.Evaluate(_plan, _indexes);
        var expected = new HashSet<string>(StringComparer.Ordinal);
        for (var group = 0; group < LeafCount / 2; group++)
        {
            for (var offset = 0; offset < 8; offset++)
            {
                expected.Add(BenchmarkData.CreateRecordKey(group + (128 * offset)));
            }
        }

        if (!actual.SetEquals(expected))
        {
            throw new InvalidOperationException(
                "The query-evaluation fixture returned the expected count but not the exact record-key set.");
        }
    }

    private static PartitionQueryPlan CreateEvaluationPlan(int leafCount)
    {
        if (leafCount <= 0 || leafCount % 2 != 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(leafCount),
                leafCount,
                "The evaluation fixture requires a positive even leaf count.");
        }

        var groups = new List<PartitionQueryPlan>(leafCount / 2);
        for (var group = 0; group < leafCount / 2; group++)
        {
            groups.Add(new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = new PartitionQueryPlan
                {
                    Operation = PartitionQueryOperation.Exact,
                    Scope = BenchmarkData.CityScope,
                    IndexKind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create($"city-{group:D3}"),
                },
                Right = new PartitionQueryPlan
                {
                    Operation = PartitionQueryOperation.Range,
                    Scope = BenchmarkData.SalaryScope,
                    LowerBound = IndexValue.FromSignedInteger(group),
                    UpperBound = IndexValue.FromSignedInteger(checked(group + (128 * 7))),
                    IncludeLowerBound = true,
                    IncludeUpperBound = true,
                },
            });
        }

        while (groups.Count > 1)
        {
            var next = new List<PartitionQueryPlan>((groups.Count + 1) / 2);
            for (var index = 0; index < groups.Count; index += 2)
            {
                next.Add(index + 1 < groups.Count
                    ? new PartitionQueryPlan
                    {
                        Operation = PartitionQueryOperation.Or,
                        Left = groups[index],
                        Right = groups[index + 1],
                    }
                    : groups[index]);
            }

            groups = next;
        }

        return groups[0];
    }
}
