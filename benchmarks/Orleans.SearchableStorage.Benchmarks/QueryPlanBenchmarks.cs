using System.Linq.Expressions;
using BenchmarkDotNet.Attributes;
using Orleans.Runtime;
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
    private StoragePartitionView _view = null!;
    private PartitionQueryPlan _plan = null!;
    private HashSet<string> _expectedRecordKeys = null!;
    private GrainId[] _expectedGrainIds = null!;
    private StorageLayoutSnapshot _routing = null!;
    private byte[] _layoutFingerprint = null!;
    private byte[] _queryFingerprint = null!;
    private QueryBenchmarkDiagnostics? _orderedDiagnostics;

    [Params(
        QueryEvaluationDataset.ShortIds4K,
        QueryEvaluationDataset.ShortIds64K,
        QueryEvaluationDataset.LongIds4K)]
    public QueryEvaluationDataset Dataset { get; set; }

    public int RecordCount => Dataset switch
    {
        QueryEvaluationDataset.ShortIds4K or QueryEvaluationDataset.LongIds4K => 4_096,
        QueryEvaluationDataset.ShortIds64K => 65_536,
        _ => throw new ArgumentOutOfRangeException(nameof(Dataset), Dataset, null),
    };

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

    [Params(
        QueryEvaluationVariant.MaterializingWholePlan,
        QueryEvaluationVariant.OrderedDefaultPartitionPage,
        QueryEvaluationVariant.OrderedHardCeilingPartitionTraversal,
        QueryEvaluationVariant.OrderedConstrainedWorkPartitionPage,
        QueryEvaluationVariant.OrderedMaximumPolicyPartitionPage,
        QueryEvaluationVariant.OrderedDefaultRoundWindow)]
    public QueryEvaluationVariant Variant { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RecordCount);

        var records = CreateEvaluationRecords(
            RecordCount,
            Distribution,
            GetMinimumGrainKeyLength(Dataset));
        ValidateDistributionShape(records);
        _view = new StoragePartitionView(records);
        _plan = CreateEvaluationPlan(RecordCount, Scenario);
        _expectedRecordKeys = CreateExpectedRecordKeys(
            RecordCount,
            Distribution,
            Scenario,
            GetMinimumGrainKeyLength(Dataset));
        _expectedGrainIds = CreateExpectedGrainIds(
            RecordCount,
            Distribution,
            Scenario,
            GetMinimumGrainKeyLength(Dataset));
        _routing = StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = "benchmark-provider",
            PartitionCount = 1,
            VirtualSlotCount = 1,
            SlotAssignments = [0],
            Epoch = 1,
        });
        _queryFingerprint = QueryPlanFingerprint.Compute(BenchmarkData.StateName, _plan);
        _layoutFingerprint = StorageLayoutFingerprint.Compute(_routing);

        // Correctness and fixture selectivity are checked during setup rather than in the timed
        // method. The benchmark measures only the production partition-query evaluator.
        ValidateFixture();
        ValidateScenarioShape();
    }

    [Benchmark]
    public int EvaluatePartitionPlan() => Variant switch
    {
        QueryEvaluationVariant.MaterializingWholePlan =>
            StoragePartitionQueryEvaluator.Evaluate(_plan, _view.Indexes).Count,
        QueryEvaluationVariant.OrderedDefaultPartitionPage
            or QueryEvaluationVariant.OrderedConstrainedWorkPartitionPage
            or QueryEvaluationVariant.OrderedMaximumPolicyPartitionPage =>
            EvaluateOrderedPartitionPage(QueryBenchmarkPolicy.For(Variant)).Items.Length,
        QueryEvaluationVariant.OrderedHardCeilingPartitionTraversal
            or QueryEvaluationVariant.OrderedDefaultRoundWindow =>
            EvaluateOrderedTraversal(QueryBenchmarkPolicy.For(Variant), captureItems: false).ItemCount,
        _ => throw new ArgumentOutOfRangeException(nameof(Variant), Variant, null),
    };

    internal void ValidateFixture()
    {
        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(_plan, _view.Indexes);
        if (!evaluation.RecordKeys.SetEquals(_expectedRecordKeys))
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' query-evaluation fixture returned an unexpected record-key set "
                + $"for '{Distribution}' data with {RecordCount} records.");
        }

        ValidateWorkShape(evaluation.Work);
        ValidateOrderedFixture();
    }

    internal int ExpectedResultCount => _expectedRecordKeys.Count;

    internal int ExpectedTimedResultCount => Variant switch
    {
        QueryEvaluationVariant.MaterializingWholePlan => ExpectedResultCount,
        _ => _orderedDiagnostics?.TimedItemCount
            ?? throw new InvalidOperationException("Ordered query diagnostics were not captured."),
    };

    internal QueryBenchmarkDiagnostics? OrderedDiagnostics => _orderedDiagnostics;

    private void ValidateOrderedFixture()
    {
        if (Variant == QueryEvaluationVariant.MaterializingWholePlan)
        {
            _orderedDiagnostics = null;
            return;
        }

        var policy = QueryBenchmarkPolicy.For(Variant);
        var first = EvaluateOrderedPartitionPage(policy);
        ValidateOrderedPage(first, policy, hasAfter: false, after: default);
        var expectedFirst = _expectedGrainIds
            .Where(id => first.Exhausted
                || GrainIdCanonicalOrder.Compare(id, first.Frontier) <= 0)
            .ToArray();
        if (!first.Items.SequenceEqual(expectedFirst))
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' ordered partition page did not return its exact safe prefix.");
        }

        var repeated = EvaluateOrderedPartitionPage(policy);
        if (!repeated.Items.SequenceEqual(first.Items)
            || repeated.Exhausted != first.Exhausted
            || repeated.HasFrontier != first.HasFrontier
            || (repeated.HasFrontier
                && GrainIdCanonicalOrder.Compare(repeated.Frontier, first.Frontier) != 0)
            || BenchmarkWorkVector.From(repeated.Work) != BenchmarkWorkVector.From(first.Work))
        {
            throw new InvalidOperationException(
                $"The '{Scenario}' ordered page did not reproduce its deterministic prefix and work vector.");
        }

        var exactDriverCount = GetSelectiveExactDriverCount();
        if (Scenario == QueryEvaluationScenario.SelectiveExactAndBroadRange
            && (exactDriverCount <= 0
                || exactDriverCount >= RecordCount / 64
                || first.Work.OrderedCandidateVisitCount <= 0
                || first.Work.OrderedCandidateVisitCount > exactDriverCount))
        {
            throw new InvalidOperationException(
                "The selective exact-and-range benchmark did not use its measured bounded exact posting.");
        }

        var rangeExecutionStrategy = GetRangeExecutionStrategy(first.Work);
        ValidateRangeExecutionStrategy(first.Work, rangeExecutionStrategy);

        QueryTraversalDiagnostics? traversalDiagnostics = null;
        var timedItemCount = first.Items.Length;
        if (Variant is QueryEvaluationVariant.OrderedHardCeilingPartitionTraversal
            or QueryEvaluationVariant.OrderedDefaultRoundWindow)
        {
            var traversal = EvaluateOrderedTraversal(policy, captureItems: true);
            timedItemCount = traversal.ItemCount;
            if (!traversal.Items.SequenceEqual(_expectedGrainIds.Take(traversal.ItemCount)))
            {
                throw new InvalidOperationException(
                    $"The '{Scenario}' ordered traversal did not return an exact ordered result prefix.");
            }

            if (Variant == QueryEvaluationVariant.OrderedHardCeilingPartitionTraversal
                && (!traversal.Exhausted || traversal.ItemCount != _expectedGrainIds.Length))
            {
                throw new InvalidOperationException(
                    $"The '{Scenario}' hard-ceiling traversal did not return the complete expected sequence.");
            }

            if (Scenario == QueryEvaluationScenario.SelectiveExactAndBroadRange
                && Variant == QueryEvaluationVariant.OrderedHardCeilingPartitionTraversal
                && traversal.AggregateWork.OrderedCandidateVisitCount != exactDriverCount)
            {
                throw new InvalidOperationException(
                    "The complete selective exact-and-range traversal did not stay on its exact posting driver.");
            }

            traversalDiagnostics = new QueryTraversalDiagnostics(
                traversal.Rounds,
                traversal.Exhausted,
                traversal.ItemCount,
                traversal.ItemByteCount,
                traversal.AggregateWork,
                traversal.MaximumPageWork,
                ComputeSequenceSha256(traversal.Items));
        }

        _orderedDiagnostics = new QueryBenchmarkDiagnostics(
            Dataset,
            Distribution,
            Scenario,
            Variant,
            RecordCount,
            GetMinimumGrainKeyLength(Dataset),
            ExpectedResultCount,
            exactDriverCount,
            rangeExecutionStrategy,
            policy,
            timedItemCount,
            CreatePageDiagnostics(first),
            traversalDiagnostics);
    }

    private PartitionQueryPageResult EvaluateOrderedPartitionPage(
        QueryBenchmarkPolicy policy,
        bool hasAfter = false,
        GrainId after = default)
    {
        return StoragePartitionQueryPageEvaluator.EvaluateValidated(
            new RoutedPartitionQueryPageRequest
            {
                Query = _plan,
                Epoch = _routing.Epoch,
                HasAfter = hasAfter,
                After = after,
                WorkBudget = policy.WorkBudget,
                ItemLimit = policy.PageSize,
                ByteLimit = policy.ResponseByteLimit,
                ProtocolVersion = QueryProtocol.PagingVersion,
                OrderingVersion = QueryProtocol.OrderingVersion,
                WorkPolicyVersion = QueryProtocol.WorkPolicyVersion,
                ResponseFamily = PartitionQueryResponseFamily.GrainIdPage,
                QueryFingerprint = _queryFingerprint,
                LayoutFormatVersion = _routing.FormatVersion,
                LayoutFingerprint = _layoutFingerprint,
                StateName = BenchmarkData.StateName,
            },
            _view,
            _routing,
            partitionIndex: 0,
            _queryFingerprint,
            _layoutFingerprint);
    }

    private QueryTraversalResult EvaluateOrderedTraversal(
        QueryBenchmarkPolicy policy,
        bool captureItems)
    {
        var totalItems = 0;
        var totalBytes = 0;
        var aggregateWork = default(BenchmarkWorkVector);
        var maximumPageWork = default(BenchmarkWorkVector);
        var items = captureItems ? new List<GrainId>() : null;
        var hasAfter = false;
        var after = default(GrainId);
        for (var round = 1; round <= policy.RoundLimit; round++)
        {
            var result = EvaluateOrderedPartitionPage(policy, hasAfter, after);
            ValidateOrderedPage(result, policy, hasAfter, after);
            totalItems = checked(totalItems + result.Items.Length);
            totalBytes = checked(totalBytes + result.ItemByteCount);
            var pageWork = BenchmarkWorkVector.From(result.Work);
            aggregateWork = aggregateWork.Add(pageWork);
            maximumPageWork = BenchmarkWorkVector.Max(maximumPageWork, pageWork);
            items?.AddRange(result.Items);

            if (result.Exhausted)
            {
                return new QueryTraversalResult(
                    totalItems,
                    totalBytes,
                    round,
                    Exhausted: true,
                    aggregateWork,
                    maximumPageWork,
                    items is null ? [] : [.. items]);
            }

            after = result.Frontier;
            hasAfter = true;
        }

        return new QueryTraversalResult(
            totalItems,
            totalBytes,
            policy.RoundLimit,
            Exhausted: false,
            aggregateWork,
            maximumPageWork,
            items is null ? [] : [.. items]);
    }

    private static void ValidateOrderedPage(
        PartitionQueryPageResult page,
        QueryBenchmarkPolicy policy,
        bool hasAfter,
        GrainId after)
    {
        if (page.Items.Length > policy.PageSize
            || page.ItemByteCount < 0
            || page.ItemByteCount > policy.ResponseByteLimit
            || page.Work.TotalOperationCount > policy.WorkBudget
            || page.Exhausted == page.HasFrontier
            || page.Exhausted != (page.StopReason == PartitionQueryPageStopReason.Exhausted))
        {
            throw new InvalidOperationException("The ordered benchmark page violated its configured bounds.");
        }

        var work = BenchmarkWorkVector.From(page.Work);
        if (work.TotalOperationCount != page.Work.TotalOperationCount)
        {
            throw new InvalidOperationException("The ordered benchmark page reported an inconsistent work total.");
        }

        if (page.HasFrontier
            && hasAfter
            && GrainIdCanonicalOrder.Compare(page.Frontier, after) <= 0)
        {
            throw new InvalidOperationException("The ordered benchmark page did not advance its frontier.");
        }

        GrainId? previous = null;
        var encodedBytes = 0;
        foreach (var item in page.Items)
        {
            if ((hasAfter && GrainIdCanonicalOrder.Compare(item, after) <= 0)
                || (page.HasFrontier && GrainIdCanonicalOrder.Compare(item, page.Frontier) > 0)
                || (previous is { } preceding
                    && GrainIdCanonicalOrder.Compare(preceding, item) >= 0))
            {
                throw new InvalidOperationException(
                    "The ordered benchmark page was not a sorted, distinct safe prefix.");
            }

            encodedBytes = checked(encodedBytes + GrainIdCanonicalOrder.GetEncodedLength(item));
            previous = item;
        }

        if (encodedBytes != page.ItemByteCount)
        {
            throw new InvalidOperationException("The ordered benchmark page reported the wrong item-byte count.");
        }
    }

    private int GetSelectiveExactDriverCount()
    {
        if (Scenario != QueryEvaluationScenario.SelectiveExactAndBroadRange)
        {
            return 0;
        }

        return _view.OrderedIndexes.GetExactPosting(
            BenchmarkData.CityScope,
            SearchableIndexKind.Hash,
            IndexValue.Create("city-001")).Count;
    }

    private QueryRangeExecutionStrategy GetRangeExecutionStrategy(PartitionQueryPageWork work) => Scenario switch
    {
        QueryEvaluationScenario.Exact => QueryRangeExecutionStrategy.NoRangePlan,
        QueryEvaluationScenario.SelectiveExactAndBroadRange =>
            QueryRangeExecutionStrategy.ExactPostingDriver,
        QueryEvaluationScenario.BroadOr or QueryEvaluationScenario.DuplicateHeavyOr =>
            QueryRangeExecutionStrategy.CatalogPlanDriver,
        _ when work.RangeMergeOperationCount > 0 => QueryRangeExecutionStrategy.OrderedRangeMerge,
        _ => QueryRangeExecutionStrategy.CatalogFallback,
    };

    private static void ValidateRangeExecutionStrategy(
        PartitionQueryPageWork work,
        QueryRangeExecutionStrategy strategy)
    {
        var valid = strategy switch
        {
            QueryRangeExecutionStrategy.NoRangePlan =>
                work.RangeBucketVisitCount == 0 && work.RangeMergeOperationCount == 0,
            QueryRangeExecutionStrategy.ExactPostingDriver =>
                work.RangeBucketVisitCount == 0 && work.RangeMergeOperationCount == 0,
            QueryRangeExecutionStrategy.CatalogPlanDriver =>
                work.PostingSeekCount >= 1
                && work.RangeBucketVisitCount == 0
                && work.RangeMergeOperationCount == 0,
            QueryRangeExecutionStrategy.OrderedRangeMerge =>
                work.RangeBucketVisitCount > 0 && work.RangeMergeOperationCount > 0,
            QueryRangeExecutionStrategy.CatalogFallback =>
                work.PostingSeekCount >= 2
                && work.RangeBucketVisitCount == 0
                && work.RangeMergeOperationCount == 0,
            _ => false,
        };
        if (!valid)
        {
            throw new InvalidOperationException(
                $"The ordered benchmark reported work inconsistent with '{strategy}': "
                + $"{BenchmarkWorkVector.From(work)}.");
        }
    }

    private static QueryPageDiagnostics CreatePageDiagnostics(PartitionQueryPageResult page) => new(
        page.Items.Length,
        page.ItemByteCount,
        page.Exhausted,
        page.StopReason,
        page.HasFrontier,
        page.HasFrontier ? Convert.ToHexString(page.Frontier.Type.AsSpan()) : null,
        page.HasFrontier ? Convert.ToHexString(page.Frontier.Key.AsSpan()) : null,
        BenchmarkWorkVector.From(page.Work),
        ComputeSequenceSha256(page.Items));

    private static string ComputeSequenceSha256(IEnumerable<GrainId> items)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true);
        foreach (var item in items)
        {
            var type = item.Type.AsSpan();
            var key = item.Key.AsSpan();
            writer.Write(type.Length);
            writer.Write(type);
            writer.Write(key.Length);
            writer.Write(key);
        }

        writer.Flush();
        var encodedSequence = stream.GetBuffer().AsSpan(0, checked((int)stream.Length));
        return Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(encodedSequence));
    }

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
        QueryEvaluationDistribution distribution,
        int minimumGrainKeyLength)
    {
        var records = new Dictionary<string, StoredRecord>(recordCount, StringComparer.Ordinal);
        for (var index = 0; index < recordCount; index++)
        {
            var grainId = CreateGrainId(index, minimumGrainKeyLength);
            var recordKey = CreateStoredRecordKey(grainId);
            records.Add(
                recordKey,
                BenchmarkData.CreateRecord(
                    index,
                    salary: CreateSalary(index, recordCount, distribution),
                    city: CreateCity(index, recordCount, distribution),
                    grainId: grainId));
        }

        return records;
    }

    private static HashSet<string> CreateExpectedRecordKeys(
        int recordCount,
        QueryEvaluationDistribution distribution,
        QueryEvaluationScenario scenario,
        int minimumGrainKeyLength)
    {
        var expected = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < recordCount; index++)
        {
            var city = CreateCity(index, recordCount, distribution);
            var salary = CreateSalary(index, recordCount, distribution);
            if (MatchesScenario(city, salary, recordCount, scenario))
            {
                expected.Add(CreateStoredRecordKey(CreateGrainId(index, minimumGrainKeyLength)));
            }
        }

        return expected;
    }

    private static GrainId[] CreateExpectedGrainIds(
        int recordCount,
        QueryEvaluationDistribution distribution,
        QueryEvaluationScenario scenario,
        int minimumGrainKeyLength)
    {
        var expected = new List<GrainId>();
        for (var index = 0; index < recordCount; index++)
        {
            var city = CreateCity(index, recordCount, distribution);
            var salary = CreateSalary(index, recordCount, distribution);
            if (MatchesScenario(city, salary, recordCount, scenario))
            {
                expected.Add(CreateGrainId(index, minimumGrainKeyLength));
            }
        }

        expected.Sort(GrainIdCanonicalOrder.Comparer);
        return [.. expected];
    }

    private static GrainId CreateGrainId(int index, int minimumKeyLength) =>
        BenchmarkData.CreateGrainId(index, minimumKeyLength);

    private static string CreateStoredRecordKey(GrainId grainId)
    {
        return BenchmarkData.CreateStoredRecordKey(BenchmarkData.StateName, grainId);
    }

    private static int GetMinimumGrainKeyLength(QueryEvaluationDataset dataset) => dataset switch
    {
        QueryEvaluationDataset.ShortIds4K or QueryEvaluationDataset.ShortIds64K => 0,
        QueryEvaluationDataset.LongIds4K => 1_024,
        _ => throw new ArgumentOutOfRangeException(nameof(dataset), dataset, null),
    };

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

public enum QueryEvaluationDataset
{
    ShortIds4K,
    ShortIds64K,
    LongIds4K,
}

public enum QueryEvaluationVariant
{
    MaterializingWholePlan,
    OrderedDefaultPartitionPage,
    OrderedHardCeilingPartitionTraversal,
    OrderedConstrainedWorkPartitionPage,
    OrderedMaximumPolicyPartitionPage,
    OrderedDefaultRoundWindow,
}

public enum QueryRangeExecutionStrategy
{
    NoRangePlan,
    ExactPostingDriver,
    CatalogPlanDriver,
    OrderedRangeMerge,
    CatalogFallback,
}

internal sealed record QueryBenchmarkPolicy(
    int PageSize,
    long WorkBudget,
    int ResponseByteLimit,
    int RoundLimit)
{
    public static QueryBenchmarkPolicy For(QueryEvaluationVariant variant) => variant switch
    {
        QueryEvaluationVariant.OrderedDefaultPartitionPage => new(
            SearchableStorageQueryOptions.DefaultPageSize,
            SearchableStorageQueryOptions.DefaultPartitionWorkBudget,
            SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
            RoundLimit: 1),
        QueryEvaluationVariant.OrderedHardCeilingPartitionTraversal => new(
            SearchableStorageQueryOptions.DefaultPageSize,
            SearchableStorageQueryOptions.DefaultPartitionWorkBudget,
            SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
            SearchableStorageQueryOptions.MaximumLegacyRounds),
        QueryEvaluationVariant.OrderedConstrainedWorkPartitionPage => new(
            SearchableStorageQueryOptions.DefaultPageSize,
            WorkBudget: 4_096,
            SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
            RoundLimit: 1),
        QueryEvaluationVariant.OrderedMaximumPolicyPartitionPage => new(
            SearchableStorageQueryOptions.MaximumPageSize,
            SearchableStorageQueryOptions.MaximumPartitionWorkBudget,
            SearchableStorageQueryOptions.MaximumPartitionResponseBytes,
            RoundLimit: 1),
        QueryEvaluationVariant.OrderedDefaultRoundWindow => new(
            SearchableStorageQueryOptions.DefaultPageSize,
            SearchableStorageQueryOptions.DefaultPartitionWorkBudget,
            SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
            SearchableStorageQueryOptions.DefaultLegacyRounds),
        _ => throw new ArgumentOutOfRangeException(nameof(variant), variant, null),
    };
}

internal readonly record struct BenchmarkWorkVector(
    long OrderedCandidateVisitCount,
    long RecordProbeCount,
    long PredicateNodeProbeCount,
    long IndexEntryProbeCount,
    long OwnershipProbeCount,
    long PostingSeekCount,
    long RangeBucketVisitCount,
    long ResultMaterializationCount,
    long RangeMergeOperationCount)
{
    public long TotalOperationCount => checked(
        OrderedCandidateVisitCount
        + RecordProbeCount
        + PredicateNodeProbeCount
        + IndexEntryProbeCount
        + OwnershipProbeCount
        + PostingSeekCount
        + RangeBucketVisitCount
        + ResultMaterializationCount
        + RangeMergeOperationCount);

    public static BenchmarkWorkVector From(PartitionQueryPageWork work)
    {
        ArgumentNullException.ThrowIfNull(work);
        return new BenchmarkWorkVector(
            work.OrderedCandidateVisitCount,
            work.RecordProbeCount,
            work.PredicateNodeProbeCount,
            work.IndexEntryProbeCount,
            work.OwnershipProbeCount,
            work.PostingSeekCount,
            work.RangeBucketVisitCount,
            work.ResultMaterializationCount,
            work.RangeMergeOperationCount);
    }

    public BenchmarkWorkVector Add(BenchmarkWorkVector other) => new(
        checked(OrderedCandidateVisitCount + other.OrderedCandidateVisitCount),
        checked(RecordProbeCount + other.RecordProbeCount),
        checked(PredicateNodeProbeCount + other.PredicateNodeProbeCount),
        checked(IndexEntryProbeCount + other.IndexEntryProbeCount),
        checked(OwnershipProbeCount + other.OwnershipProbeCount),
        checked(PostingSeekCount + other.PostingSeekCount),
        checked(RangeBucketVisitCount + other.RangeBucketVisitCount),
        checked(ResultMaterializationCount + other.ResultMaterializationCount),
        checked(RangeMergeOperationCount + other.RangeMergeOperationCount));

    public static BenchmarkWorkVector Max(BenchmarkWorkVector left, BenchmarkWorkVector right) =>
        right.TotalOperationCount > left.TotalOperationCount ? right : left;
}

internal sealed record QueryPageDiagnostics(
    int ItemCount,
    int ItemByteCount,
    bool Exhausted,
    PartitionQueryPageStopReason StopReason,
    bool HasFrontier,
    string? FrontierTypeHex,
    string? FrontierKeyHex,
    BenchmarkWorkVector Work,
    string SequenceSha256);

internal sealed record QueryTraversalDiagnostics(
    int Rounds,
    bool Exhausted,
    int ItemCount,
    int ItemByteCount,
    BenchmarkWorkVector AggregateWork,
    BenchmarkWorkVector MaximumPageWork,
    string SequenceSha256);

internal sealed record QueryBenchmarkDiagnostics(
    QueryEvaluationDataset Dataset,
    QueryEvaluationDistribution Distribution,
    QueryEvaluationScenario Scenario,
    QueryEvaluationVariant Variant,
    int RecordCount,
    int MinimumGrainKeyLength,
    int ExpectedResultCount,
    int SelectiveExactDriverCount,
    QueryRangeExecutionStrategy RangeExecutionStrategy,
    QueryBenchmarkPolicy Policy,
    int TimedItemCount,
    QueryPageDiagnostics FirstPage,
    QueryTraversalDiagnostics? Traversal);

internal sealed record QueryTraversalResult(
    int ItemCount,
    int ItemByteCount,
    int Rounds,
    bool Exhausted,
    BenchmarkWorkVector AggregateWork,
    BenchmarkWorkVector MaximumPageWork,
    GrainId[] Items);
