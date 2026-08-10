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
    private StoragePartitionIndexes _indexes = null!;
    private PartitionQueryPlan _plan = null!;
    private HashSet<string> _expectedRecordKeys = null!;

    [Params(4_096, 65_536)]
    public int RecordCount { get; set; }

    [Params(QueryEvaluationDistribution.Uniform, QueryEvaluationDistribution.HotKeyAndLowRange)]
    public QueryEvaluationDistribution Distribution { get; set; }

    [Params(
        QueryEvaluationScenario.Exact,
        QueryEvaluationScenario.Range,
        QueryEvaluationScenario.SelectiveExactAndBroadRange,
        QueryEvaluationScenario.BroadAnd,
        QueryEvaluationScenario.BroadOr,
        QueryEvaluationScenario.DuplicateHeavyOr)]
    public QueryEvaluationScenario Scenario { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RecordCount);

        var records = CreateEvaluationRecords(RecordCount, Distribution);
        ValidateDistributionShape(records);
        _indexes = StoragePartitionIndexes.Build(records);
        _plan = CreateEvaluationPlan(RecordCount, Scenario);
        _expectedRecordKeys = CreateExpectedRecordKeys(RecordCount, Distribution, Scenario);

        // Correctness and fixture selectivity are checked during setup rather than in the timed
        // method. The benchmark measures only the production partition-query evaluator.
        ValidateFixture();
        ValidateScenarioShape();
    }

    [Benchmark]
    public int EvaluatePartitionPlan() =>
        StoragePartitionQueryEvaluator.Evaluate(_plan, _indexes).Count;

    internal void ValidateFixture()
    {
        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(_plan, _indexes);
        if (!evaluation.RecordKeys.SetEquals(_expectedRecordKeys))
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' query-evaluation fixture returned an unexpected record-key set "
                + $"for '{Distribution}' data with {RecordCount} records.");
        }

        ValidateWorkShape(evaluation.Work);
    }

    internal int ExpectedResultCount => _expectedRecordKeys.Count;

    private void ValidateScenarioShape()
    {
        if (_expectedRecordKeys.Count <= 0 || _expectedRecordKeys.Count >= RecordCount)
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' fixture must select a non-empty proper subset of the records.");
        }

        var selectivity = (double)_expectedRecordKeys.Count / RecordCount;
        var valid = Scenario switch
        {
            QueryEvaluationScenario.Exact => Distribution switch
            {
                QueryEvaluationDistribution.Uniform => selectivity is > 0.003 and < 0.005,
                QueryEvaluationDistribution.HotKeyAndLowRange => selectivity == 0.5,
                _ => false,
            },
            QueryEvaluationScenario.Range => selectivity is > 0.004 and < 0.021,
            QueryEvaluationScenario.SelectiveExactAndBroadRange => selectivity < 0.005,
            QueryEvaluationScenario.BroadAnd => selectivity is > 0.50 and < 0.75,
            QueryEvaluationScenario.BroadOr => selectivity is > 0.65 and < 0.95,
            QueryEvaluationScenario.DuplicateHeavyOr => selectivity is > 0.74 and < 0.95,
            _ => false,
        };
        if (!valid)
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' fixture selectivity {selectivity:P3} is outside its intended band "
                + $"for '{Distribution}' data with {RecordCount} records.");
        }
    }

    private void ValidateWorkShape(PartitionQueryWork work)
    {
        var matchesScenario = Scenario switch
        {
            QueryEvaluationScenario.Exact =>
                work.ExactNodeCount == 1
                && work.RangeNodeCount == 0
                && work.AndNodeCount == 0
                && work.OrNodeCount == 0
                && work.ExactCandidateCount == ExpectedResultCount,
            QueryEvaluationScenario.Range =>
                work.ExactNodeCount == 0
                && work.RangeNodeCount == 1
                && work.AndNodeCount == 0
                && work.OrNodeCount == 0
                && work.RangeBucketVisitCount > 0
                && work.RangeCandidateCount == ExpectedResultCount,
            QueryEvaluationScenario.SelectiveExactAndBroadRange =>
                work.ExactNodeCount == 1
                && work.RangeNodeCount == 1
                && work.AndNodeCount == 1
                && work.OrNodeCount == 0
                && work.ExactCandidateCount > ExpectedResultCount
                && work.ExactCandidateCount < work.RangeCandidateCount
                && work.RangeCandidateCount > RecordCount * 3L / 4
                && work.AndCandidateCheckCount == work.ExactCandidateCount,
            QueryEvaluationScenario.BroadAnd =>
                work.ExactNodeCount == 0
                && work.RangeNodeCount == 2
                && work.AndNodeCount == 1
                && work.OrNodeCount == 0
                && work.RangeCandidateCount > RecordCount
                && work.AndCandidateCheckCount > ExpectedResultCount,
            QueryEvaluationScenario.BroadOr =>
                work.ExactNodeCount == 0
                && work.RangeNodeCount == 2
                && work.AndNodeCount == 0
                && work.OrNodeCount == 1
                && work.RangeCandidateCount == ExpectedResultCount
                && work.OrCandidateMergeCount > 0
                && work.OrCandidateMergeCount < ExpectedResultCount,
            QueryEvaluationScenario.DuplicateHeavyOr =>
                work.ExactNodeCount == 0
                && work.RangeNodeCount == 4
                && work.AndNodeCount == 0
                && work.OrNodeCount == 3
                && work.RangeBucketVisitCount > 0
                && work.RangeCandidateCount >= ExpectedResultCount * 2L
                && work.OrCandidateMergeCount > ExpectedResultCount,
            _ => false,
        };
        if (!matchesScenario)
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' fixture produced an unexpected measured work shape: {work}.");
        }

        var expectedNodeCount = Scenario switch
        {
            QueryEvaluationScenario.Exact or QueryEvaluationScenario.Range => 1,
            QueryEvaluationScenario.SelectiveExactAndBroadRange
                or QueryEvaluationScenario.BroadAnd
                or QueryEvaluationScenario.BroadOr => 3,
            QueryEvaluationScenario.DuplicateHeavyOr => 7,
            _ => throw new InvalidOperationException($"Unknown query scenario '{Scenario}'."),
        };
        if (work.EmptyNodeCount != 0
            || work.NodeCount != expectedNodeCount
            || work.TotalOperationCount <= work.NodeCount)
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' fixture produced inconsistent aggregate work: {work}.");
        }
    }

    private void ValidateDistributionShape(IReadOnlyDictionary<string, StoredRecord> records)
    {
        var distinctSalaries = new HashSet<long>();
        var hotKeyCount = 0;
        var lowRangeCount = 0;
        var correlatedHotKeyCount = 0;
        var lowRangeUpperBound = RecordCount / 16;
        foreach (var record in records.Values)
        {
            var city = record.IndexEntries.Single(
                static entry => string.Equals(
                    entry.Scope,
                    BenchmarkData.CityScope,
                    StringComparison.Ordinal));
            var salary = record.IndexEntries.Single(
                static entry => string.Equals(
                    entry.Scope,
                    BenchmarkData.SalaryScope,
                    StringComparison.Ordinal));
            var isHotKey = string.Equals(city.Value.Text, "city-000", StringComparison.Ordinal);
            var isLowRange = salary.Value.SignedInteger < lowRangeUpperBound;
            distinctSalaries.Add(salary.Value.SignedInteger);
            hotKeyCount = checked(hotKeyCount + (isHotKey ? 1 : 0));
            lowRangeCount = checked(lowRangeCount + (isLowRange ? 1 : 0));
            correlatedHotKeyCount = checked(
                correlatedHotKeyCount + (isHotKey && isLowRange ? 1 : 0));
        }

        var valid = Distribution switch
        {
            QueryEvaluationDistribution.Uniform =>
                hotKeyCount == RecordCount / 256
                && lowRangeCount == RecordCount / 16
                && distinctSalaries.Count == RecordCount
                && correlatedHotKeyCount * 16 == hotKeyCount,
            QueryEvaluationDistribution.HotKeyAndLowRange =>
                hotKeyCount == RecordCount / 2
                && lowRangeCount > RecordCount * 3 / 4
                && distinctSalaries.Count < RecordCount / 3
                && correlatedHotKeyCount == hotKeyCount,
            _ => false,
        };
        if (!valid)
        {
            throw new InvalidOperationException(
                $"The '{Distribution}' fixture does not satisfy its independent distribution "
                + $"contract: hot={hotKeyCount}, low-range={lowRangeCount}, "
                + $"distinct-range={distinctSalaries.Count}, correlated-hot={correlatedHotKeyCount}.");
        }
    }

    private static Dictionary<string, StoredRecord> CreateEvaluationRecords(
        int recordCount,
        QueryEvaluationDistribution distribution)
    {
        var records = new Dictionary<string, StoredRecord>(recordCount, StringComparer.Ordinal);
        for (var index = 0; index < recordCount; index++)
        {
            var recordKey = BenchmarkData.CreateRecordKey(index);
            records.Add(
                recordKey,
                BenchmarkData.CreateRecord(
                    index,
                    salary: CreateSalary(index, recordCount, distribution),
                    city: CreateCity(index, recordCount, distribution)));
        }

        return records;
    }

    private static HashSet<string> CreateExpectedRecordKeys(
        int recordCount,
        QueryEvaluationDistribution distribution,
        QueryEvaluationScenario scenario)
    {
        var expected = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < recordCount; index++)
        {
            var city = CreateCity(index, recordCount, distribution);
            var salary = CreateSalary(index, recordCount, distribution);
            if (MatchesScenario(city, salary, recordCount, scenario))
            {
                expected.Add(BenchmarkData.CreateRecordKey(index));
            }
        }

        return expected;
    }

    private static bool MatchesScenario(
        int city,
        int salary,
        int recordCount,
        QueryEvaluationScenario scenario)
    {
        var narrowLower = recordCount * 49 / 100;
        var narrowUpper = recordCount * 51 / 100;
        var broadLower = recordCount / 32;
        var broadUpper = recordCount * 3 / 4;
        return scenario switch
        {
            QueryEvaluationScenario.Exact => city == 0,
            QueryEvaluationScenario.Range => salary >= narrowLower && salary <= narrowUpper,
            QueryEvaluationScenario.SelectiveExactAndBroadRange =>
                city == 1 && salary <= broadUpper,
            QueryEvaluationScenario.BroadAnd => salary >= broadLower && salary <= broadUpper,
            QueryEvaluationScenario.BroadOr =>
                salary <= recordCount / 3 || salary >= recordCount * 2 / 3,
            QueryEvaluationScenario.DuplicateHeavyOr => salary <= broadUpper,
            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null),
        };
    }

    private static int CreateCity(
        int index,
        int recordCount,
        QueryEvaluationDistribution distribution) => distribution switch
    {
        QueryEvaluationDistribution.Uniform => index % 256,
        QueryEvaluationDistribution.HotKeyAndLowRange => index < recordCount / 2
            ? 0
            : 1 + (index % 255),
        _ => throw new ArgumentOutOfRangeException(nameof(distribution), distribution, null),
    };

    private static int CreateSalary(
        int index,
        int recordCount,
        QueryEvaluationDistribution distribution) => distribution switch
    {
        QueryEvaluationDistribution.Uniform => index,
        QueryEvaluationDistribution.HotKeyAndLowRange => index < recordCount * 3 / 4
            ? index % (recordCount / 16)
            : checked((index - (recordCount * 3 / 4)) * 4),
        _ => throw new ArgumentOutOfRangeException(nameof(distribution), distribution, null),
    };

    private static PartitionQueryPlan CreateEvaluationPlan(
        int recordCount,
        QueryEvaluationScenario scenario)
    {
        var narrowLower = recordCount * 49 / 100;
        var narrowUpper = recordCount * 51 / 100;
        var broadLower = recordCount / 32;
        var broadUpper = recordCount * 3 / 4;
        return scenario switch
        {
            QueryEvaluationScenario.Exact => Exact(0),
            QueryEvaluationScenario.Range => Range(narrowLower, narrowUpper),
            QueryEvaluationScenario.SelectiveExactAndBroadRange => And(
                Exact(1),
                Range(0, broadUpper)),
            QueryEvaluationScenario.BroadAnd => And(
                Range(broadLower, recordCount - 1),
                Range(0, broadUpper)),
            QueryEvaluationScenario.BroadOr => Or(
                Range(0, recordCount / 3),
                Range(recordCount * 2 / 3, recordCount - 1)),
            QueryEvaluationScenario.DuplicateHeavyOr => CreateDuplicateHeavyOrPlan(recordCount),
            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null),
        };
    }

    private static PartitionQueryPlan CreateDuplicateHeavyOrPlan(int recordCount)
    {
        // Every narrower branch is a subset of the previous branch. This keeps the result broad
        // while forcing the current materializing evaluator to union many duplicate candidates.
        return Or(
            Or(
                Range(0, recordCount * 3 / 4),
                Range(0, recordCount / 2)),
            Or(
                Range(0, recordCount / 4),
                Range(0, recordCount / 8)));
    }

    private static PartitionQueryPlan Exact(int city) => new()
    {
        Operation = PartitionQueryOperation.Exact,
        Scope = BenchmarkData.CityScope,
        IndexKind = SearchableIndexKind.Hash,
        Value = IndexValue.Create($"city-{city:D3}"),
    };

    private static PartitionQueryPlan Range(int lowerBound, int upperBound) => new()
    {
        Operation = PartitionQueryOperation.Range,
        Scope = BenchmarkData.SalaryScope,
        LowerBound = IndexValue.FromSignedInteger(lowerBound),
        UpperBound = IndexValue.FromSignedInteger(upperBound),
        IncludeLowerBound = true,
        IncludeUpperBound = true,
    };

    private static PartitionQueryPlan And(PartitionQueryPlan left, PartitionQueryPlan right) => new()
    {
        Operation = PartitionQueryOperation.And,
        Left = left,
        Right = right,
    };

    private static PartitionQueryPlan Or(PartitionQueryPlan left, PartitionQueryPlan right) => new()
    {
        Operation = PartitionQueryOperation.Or,
        Left = left,
        Right = right,
    };
}

public enum QueryEvaluationDistribution
{
    Uniform,
    HotKeyAndLowRange,
}

public enum QueryEvaluationScenario
{
    Exact,
    Range,
    SelectiveExactAndBroadRange,
    BroadAnd,
    BroadOr,
    DuplicateHeavyOr,
}
