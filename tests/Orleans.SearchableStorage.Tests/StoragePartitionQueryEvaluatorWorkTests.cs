using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePartitionQueryEvaluatorWorkTests
{
    private const string CityScope = "state/city";
    private const string SalaryScope = "state/salary";

    [Fact]
    public void EmptyPlanReportsOneNodeAndNoCandidateWork()
    {
        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(
            new PartitionQueryPlan { Operation = PartitionQueryOperation.Empty },
            CreateIndexes());

        evaluation.RecordKeys.Should().BeEmpty();
        evaluation.Work.Should().Be(new PartitionQueryWork(
            EmptyNodeCount: 1,
            ExactNodeCount: 0,
            RangeNodeCount: 0,
            AndNodeCount: 0,
            OrNodeCount: 0,
            ExactCandidateCount: 0,
            RangeBucketVisitCount: 0,
            RangeCandidateCount: 0,
            AndCandidateCheckCount: 0,
            OrCandidateMergeCount: 0));
        evaluation.Work.NodeCount.Should().Be(1);
        evaluation.Work.CandidateOperationCount.Should().Be(0);
        evaluation.Work.TotalOperationCount.Should().Be(1);
    }

    [Theory]
    [InlineData(SearchableIndexKind.Hash, CityScope, "Helsinki", 2)]
    [InlineData(SearchableIndexKind.Range, SalaryScope, "2", 2)]
    [InlineData(SearchableIndexKind.Hash, CityScope, "missing", 0)]
    public void ExactPlanCountsCopiedBucketCandidates(
        SearchableIndexKind indexKind,
        string scope,
        string value,
        int expectedCandidateCount)
    {
        var indexValue = indexKind == SearchableIndexKind.Range
            ? IndexValue.FromSignedInteger(long.Parse(value, System.Globalization.CultureInfo.InvariantCulture))
            : IndexValue.Create(value);

        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Exact,
                Scope = scope,
                IndexKind = indexKind,
                Value = indexValue,
            },
            CreateIndexes());

        evaluation.RecordKeys.Should().HaveCount(expectedCandidateCount);
        evaluation.Work.ExactNodeCount.Should().Be(1);
        evaluation.Work.ExactCandidateCount.Should().Be(expectedCandidateCount);
        evaluation.Work.NodeCount.Should().Be(1);
        evaluation.Work.CandidateOperationCount.Should().Be(expectedCandidateCount);
        evaluation.Work.TotalOperationCount.Should().Be(1 + expectedCandidateCount);
    }

    [Fact]
    public void RangePlanSeparatesVisitedBucketsFromIncludedCandidates()
    {
        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Range,
                Scope = SalaryScope,
                LowerBound = IndexValue.FromSignedInteger(1),
                UpperBound = IndexValue.FromSignedInteger(3),
                IncludeLowerBound = false,
                IncludeUpperBound = true,
            },
            CreateIndexes());

        evaluation.RecordKeys.Should().BeEquivalentTo(["second", "third", "fourth"]);
        evaluation.Work.RangeNodeCount.Should().Be(1);
        // SortedSet views include equal endpoints. The excluded lower bucket is still visited but
        // its one record key is not presented to the result set.
        evaluation.Work.RangeBucketVisitCount.Should().Be(3);
        evaluation.Work.RangeCandidateCount.Should().Be(3);
        evaluation.Work.NodeCount.Should().Be(1);
        evaluation.Work.CandidateOperationCount.Should().Be(3);
        evaluation.Work.TotalOperationCount.Should().Be(7);
    }

    [Fact]
    public void RangePlanCountsDuplicateRecordKeysFromDifferentBucketsAsSeparateWork()
    {
        var duplicate = CreateRecord(
            "duplicate",
            "Helsinki",
            1,
            new IndexEntry
            {
                Scope = SalaryScope,
                Kind = SearchableIndexKind.Range,
                Value = IndexValue.FromSignedInteger(2),
            });
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["duplicate"] = duplicate,
            });

        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Range,
                Scope = SalaryScope,
                LowerBound = IndexValue.FromSignedInteger(1),
                UpperBound = IndexValue.FromSignedInteger(2),
                IncludeLowerBound = true,
                IncludeUpperBound = true,
            },
            indexes);

        evaluation.RecordKeys.Should().ContainSingle().Which.Should().Be("duplicate");
        evaluation.Work.RangeNodeCount.Should().Be(1);
        evaluation.Work.RangeBucketVisitCount.Should().Be(2);
        evaluation.Work.RangeCandidateCount.Should().Be(2);
        evaluation.Work.TotalOperationCount.Should().Be(5);
    }

    [Fact]
    public void MissingRangeIndexStillCountsTheLookupNode()
    {
        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Range,
                Scope = "state/missing",
                LowerBound = IndexValue.FromSignedInteger(1),
                IncludeLowerBound = true,
            },
            CreateIndexes());

        evaluation.RecordKeys.Should().BeEmpty();
        evaluation.Work.RangeNodeCount.Should().Be(1);
        evaluation.Work.RangeBucketVisitCount.Should().Be(0);
        evaluation.Work.RangeCandidateCount.Should().Be(0);
        evaluation.Work.TotalOperationCount.Should().Be(1);
    }

    [Fact]
    public void CompositePlanAggregatesEveryOperationWithoutChangingResults()
    {
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = ExactCity("Helsinki"),
                Right = ExactSalaryRange(2),
            },
            Right = new PartitionQueryPlan { Operation = PartitionQueryOperation.Empty },
        };
        var indexes = CreateIndexes();

        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(plan, indexes);
        var ordinaryResult = StoragePartitionQueryEvaluator.Evaluate(plan, indexes);

        evaluation.RecordKeys.Should().BeEquivalentTo(ordinaryResult);
        evaluation.RecordKeys.Should().ContainSingle().Which.Should().Be("second");
        evaluation.Work.Should().Be(new PartitionQueryWork(
            EmptyNodeCount: 1,
            ExactNodeCount: 1,
            RangeNodeCount: 1,
            AndNodeCount: 1,
            OrNodeCount: 1,
            ExactCandidateCount: 2,
            RangeBucketVisitCount: 1,
            RangeCandidateCount: 2,
            AndCandidateCheckCount: 2,
            OrCandidateMergeCount: 0));
        evaluation.Work.NodeCount.Should().Be(5);
        evaluation.Work.CandidateOperationCount.Should().Be(6);
        evaluation.Work.TotalOperationCount.Should().Be(12);
    }

    [Fact]
    public void DuplicateHeavyOrCountsEveryRightCandidateMerge()
    {
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = ExactCity("Helsinki"),
            Right = ExactCity("Helsinki"),
        };

        var evaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(plan, CreateIndexes());

        evaluation.RecordKeys.Should().BeEquivalentTo(["first", "second"]);
        evaluation.Work.ExactNodeCount.Should().Be(2);
        evaluation.Work.ExactCandidateCount.Should().Be(4);
        evaluation.Work.OrNodeCount.Should().Be(1);
        evaluation.Work.OrCandidateMergeCount.Should().Be(2);
        evaluation.Work.NodeCount.Should().Be(3);
        evaluation.Work.CandidateOperationCount.Should().Be(6);
        evaluation.Work.TotalOperationCount.Should().Be(9);
    }

    [Fact]
    public void AndCandidateChecksAreDefinedByTheLeftInput()
    {
        var broadLeft = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = ExactCity("Helsinki"),
            Right = ExactSalary(1),
        };
        var selectiveLeft = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = ExactSalary(1),
            Right = ExactCity("Helsinki"),
        };
        var indexes = CreateIndexes();

        var broadLeftEvaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(
            broadLeft,
            indexes);
        var selectiveLeftEvaluation = StoragePartitionQueryEvaluator.EvaluateWithWork(
            selectiveLeft,
            indexes);

        broadLeftEvaluation.RecordKeys.Should().ContainSingle().Which.Should().Be("first");
        selectiveLeftEvaluation.RecordKeys.Should().BeEquivalentTo(broadLeftEvaluation.RecordKeys);
        broadLeftEvaluation.Work.ExactCandidateCount.Should().Be(3);
        selectiveLeftEvaluation.Work.ExactCandidateCount.Should().Be(3);
        broadLeftEvaluation.Work.AndCandidateCheckCount.Should().Be(2);
        selectiveLeftEvaluation.Work.AndCandidateCheckCount.Should().Be(1);
        broadLeftEvaluation.Work.TotalOperationCount.Should().Be(8);
        selectiveLeftEvaluation.Work.TotalOperationCount.Should().Be(7);
    }

    [Fact]
    public void MeasuredEvaluationPreservesWholePlanValidationPrecedence()
    {
        var malformed = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = new PartitionQueryPlan { Operation = PartitionQueryOperation.Empty },
        };

        var evaluate = () => StoragePartitionQueryEvaluator.EvaluateWithWork(
            malformed,
            CreateIndexes());

        evaluate.Should().Throw<ArgumentException>()
            .WithParameterName("query")
            .WithMessage("*requires both child plans*");
    }

    private static PartitionQueryPlan ExactCity(string city)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = CityScope,
            IndexKind = SearchableIndexKind.Hash,
            Value = IndexValue.Create(city),
        };
    }

    private static PartitionQueryPlan ExactSalaryRange(long salary)
    {
        var bound = IndexValue.FromSignedInteger(salary);
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = SalaryScope,
            LowerBound = bound,
            UpperBound = bound,
            IncludeLowerBound = true,
            IncludeUpperBound = true,
        };
    }

    private static PartitionQueryPlan ExactSalary(long salary)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = SalaryScope,
            IndexKind = SearchableIndexKind.Range,
            Value = IndexValue.FromSignedInteger(salary),
        };
    }

    private static StoragePartitionIndexes CreateIndexes()
    {
        return StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["first"] = CreateRecord("first", "Helsinki", 1),
                ["second"] = CreateRecord("second", "Helsinki", 2),
                ["third"] = CreateRecord("third", "London", 2),
                ["fourth"] = CreateRecord("fourth", "London", 3),
            });
    }

    private static StoredRecord CreateRecord(
        string recordKey,
        string city,
        long salary,
        params IndexEntry[] additionalEntries)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("query-work-test", recordKey),
            Payload = [],
            ETag = "1",
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = CityScope,
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create(city),
                },
                new IndexEntry
                {
                    Scope = SalaryScope,
                    Kind = SearchableIndexKind.Range,
                    Value = IndexValue.FromSignedInteger(salary),
                },
                .. additionalEntries,
            ],
        };
    }
}
