using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePartitionQueryPageEvaluatorTests
{
    private const string StateName = "page-state";
    private const string CityScope = "page/city";
    private const string SalaryScope = "page/salary";

    [Fact]
    public void ExactDriverReturnsSortedDistinctPrefixesAndResumesAfterFiniteFrontier()
    {
        var grainIds = Enumerable.Range(0, 5)
            .Select(index => GrainId.Create("page-type", $"key-{index:D2}"))
            .Reverse()
            .ToArray();
        var view = CreateView(grainIds.Select(grainId =>
            (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", 10))));
        var layout = CreateLayout(partitionCount: 1);
        var plan = ExactCity("Helsinki");

        var first = Evaluate(view, layout, partitionIndex: 0, plan, itemLimit: 2);

        var expected = grainIds.Order(GrainIdCanonicalOrder.Comparer).ToArray();
        first.Items.Should().Equal(expected[..2]);
        first.Exhausted.Should().BeFalse();
        first.HasFrontier.Should().BeTrue();
        first.Frontier.Should().Be(expected[1]);
        first.StopReason.Should().Be(PartitionQueryPageStopReason.ItemLimit);
        first.ItemByteCount.Should().Be(first.Items.Sum(GrainIdCanonicalOrder.GetEncodedLength));

        var second = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            plan,
            itemLimit: 8,
            hasAfter: true,
            after: first.Frontier);

        second.Items.Should().Equal(expected[2..]);
        second.Exhausted.Should().BeTrue();
        second.HasFrontier.Should().BeFalse();
        second.StopReason.Should().Be(PartitionQueryPageStopReason.Exhausted);
        first.Items.Concat(second.Items).Should().Equal(expected);
    }

    [Fact]
    public void ExactDriverGroupsDuplicateRecordOccurrencesIntoOneResult()
    {
        var grainId = GrainId.Create("page-type", "duplicate");
        var recordKey = CreateRecordKey(grainId);
        var view = CreateView(
        [
            (recordKey, CreateRecord(grainId, "Helsinki", 10)),
            (string.Concat(recordKey, "-duplicate"), CreateRecord(grainId, "Helsinki", 10)),
        ]);

        var result = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"));

        result.Items.Should().ContainSingle().Which.Should().Be(grainId);
        result.Work.OrderedCandidateVisitCount.Should().Be(1);
        result.Work.OwnershipProbeCount.Should().Be(1);
        result.Work.ResultMaterializationCount.Should().Be(1);
    }

    [Fact]
    public void MissingExactPostingExhaustsAfterOneBoundedSeek()
    {
        var grainId = GrainId.Create("page-type", "present");
        var view = CreateView(
        [
            (CreateRecordKey(grainId), CreateRecord(grainId, "London", 10)),
        ]);

        var result = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("missing"));

        result.Items.Should().BeEmpty();
        result.Exhausted.Should().BeTrue();
        result.Work.PostingSeekCount.Should().Be(1);
        result.Work.TotalOperationCount.Should().Be(1);
    }

    [Fact]
    public void ExactDriverReportsTheCompleteDeterministicWorkVector()
    {
        var grainId = GrainId.Create("page-type", "exact-work");
        var result = Evaluate(
            CreateView(
            [
                (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", 10)),
            ]),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"));

        result.Work.Should().BeEquivalentTo(new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = 1,
            RecordProbeCount = 1,
            PredicateNodeProbeCount = 1,
            IndexEntryProbeCount = 1,
            OwnershipProbeCount = 1,
            PostingSeekCount = 1,
            RangeBucketVisitCount = 0,
            ResultMaterializationCount = 1,
        });
        result.Work.TotalOperationCount.Should().Be(7);
    }

    [Fact]
    public void BoundedRangeMergeReportsTheCompleteDeterministicWorkVector()
    {
        var records = new[]
        {
            (GrainId.Create("page-type", "range-00"), 10),
            (GrainId.Create("page-type", "range-01"), 20),
            (GrainId.Create("page-type", "range-02"), 30),
        };
        var result = Evaluate(
            CreateView(records.Select(pair =>
                (CreateRecordKey(pair.Item1), CreateRecord(pair.Item1, "London", pair.Item2)))),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(15, 35));

        result.Items.Should().Equal(records[1].Item1, records[2].Item1);
        result.Work.Should().BeEquivalentTo(new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = 2,
            RecordProbeCount = 2,
            PredicateNodeProbeCount = 2,
            IndexEntryProbeCount = 4,
            OwnershipProbeCount = 2,
            PostingSeekCount = 3,
            RangeBucketVisitCount = 2,
            ResultMaterializationCount = 2,
            RangeMergeOperationCount = 4,
        });
        result.Work.TotalOperationCount.Should().Be(23);
    }

    [Fact]
    public void RangeMergeChargesOpenEndpointBucketsBeforeExcludingThem()
    {
        var records = new[]
        {
            (GrainId.Create("page-type", "open-10"), 10),
            (GrainId.Create("page-type", "open-20"), 20),
            (GrainId.Create("page-type", "open-30"), 30),
        };
        var result = Evaluate(
            CreateView(records.Select(pair =>
                (CreateRecordKey(pair.Item1), CreateRecord(pair.Item1, "London", pair.Item2)))),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Range,
                Scope = SalaryScope,
                LowerBound = IndexValue.Create(10),
                UpperBound = IndexValue.Create(30),
                IncludeLowerBound = false,
                IncludeUpperBound = false,
            });

        result.Items.Should().ContainSingle().Which.Should().Be(records[1].Item1);
        result.Work.Should().BeEquivalentTo(new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = 1,
            RecordProbeCount = 1,
            PredicateNodeProbeCount = 1,
            IndexEntryProbeCount = 2,
            OwnershipProbeCount = 1,
            PostingSeekCount = 2,
            RangeBucketVisitCount = 3,
            ResultMaterializationCount = 1,
            RangeMergeOperationCount = 1,
        });
        result.Work.TotalOperationCount.Should().Be(13);
    }

    [Fact]
    public void RangeMergeGroupsDuplicateCandidatesAcrossBucketsBeforeAdvancingFrontier()
    {
        var first = GrainId.Create("page-type", "duplicate-range-a");
        var second = GrainId.Create("page-type", "duplicate-range-b");
        var view = CreateView(
        [
            (CreateRecordKey(first), CreateRecordWithSalaries(first, "Helsinki", 10, 20)),
            (CreateRecordKey(second), CreateRecordWithSalaries(second, "Helsinki", 20)),
        ]);

        var result = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(10, 20));

        result.Items.Should().Equal(first, second);
        result.Work.Should().BeEquivalentTo(new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = 2,
            RecordProbeCount = 2,
            PredicateNodeProbeCount = 2,
            IndexEntryProbeCount = 4,
            OwnershipProbeCount = 2,
            PostingSeekCount = 3,
            RangeBucketVisitCount = 2,
            ResultMaterializationCount = 2,
            RangeMergeOperationCount = 6,
        });
        result.Work.TotalOperationCount.Should().Be(25);
    }

    [Fact]
    public void RangeMergeStopsExactlyBeforeAndAfterACompleteDuplicateCandidateGroup()
    {
        var first = GrainId.Create("page-type", "range-boundary-a");
        var second = GrainId.Create("page-type", "range-boundary-b");
        var view = CreateView(
        [
            (CreateRecordKey(first), CreateRecordWithSalaries(first, "Helsinki", 10, 20)),
            (CreateRecordKey(second), CreateRecordWithSalaries(second, "Helsinki", 20)),
        ]);

        var oneBefore = () => Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(10, 20),
            workBudget: 17);
        oneBefore.Should().Throw<PartitionQueryBudgetTooSmallException>()
            .Which.MinimumRequired.Should().Be(18);

        var exact = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(10, 20),
            workBudget: 18);
        exact.Items.Should().ContainSingle().Which.Should().Be(first);
        exact.HasFrontier.Should().BeTrue();
        exact.Frontier.Should().Be(first);
        exact.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
        exact.Work.TotalOperationCount.Should().Be(18);

        var resumed = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(10, 20),
            hasAfter: true,
            after: exact.Frontier);
        resumed.Items.Should().ContainSingle().Which.Should().Be(second);
    }

    [Fact]
    public void RangeMergeUsesWholeScopePreflightAndFallsBackForANarrowView()
    {
        var records = Enumerable.Range(0, 20)
            .Select(index =>
            {
                var grainId = GrainId.Create("page-type", $"fallback-{index:D2}");
                return (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", index));
            })
            .ToArray();
        var view = CreateView(records);
        var selection = view.OrderedIndexes.CreateRangeBucketCursor(
            SalaryScope,
            IndexValue.Create(0),
            IndexValue.Create(1));
        selection.TotalBucketCount.Should().Be(20);
        using (var selectedBuckets = selection.Cursor)
        {
            var selectedCount = 0;
            while (selectedBuckets.HasCurrent)
            {
                selectedBuckets.TakeCurrentAndAdvance(out _).Should().BeTrue();
                selectedCount++;
            }

            selectedCount.Should().Be(2);
        }

        var result = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(0, 1),
            workBudget: 45);

        result.Exhausted.Should().BeFalse();
        result.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
        result.HasFrontier.Should().BeTrue();
        result.Work.PostingSeekCount.Should().Be(2, "range preflight then catalog seek");
        result.Work.RangeBucketVisitCount.Should().Be(0);
        result.Work.RangeMergeOperationCount.Should().Be(0);
        result.Work.TotalOperationCount.Should().Be(45);
        result.Items.Should().Equal(records[0].Item2.GrainId, records[1].Item2.GrainId);
        result.Items.Should().OnlyContain(item =>
            GrainIdCanonicalOrder.Compare(item, result.Frontier) <= 0);
    }

    [Fact]
    public void ExactAndRangeUsesExactPostingAsCandidateDriver()
    {
        var exactAndInRange = GrainId.Create("page-type", "exact-in");
        var exactOutOfRange = GrainId.Create("page-type", "exact-out");
        var broadOnly = Enumerable.Range(0, 12)
            .Select(index => GrainId.Create("page-type", $"broad-{index:D2}"))
            .ToArray();
        var records = new List<(string, StoredRecord)>
        {
            (CreateRecordKey(exactAndInRange), CreateRecord(exactAndInRange, "Helsinki", 10)),
            (CreateRecordKey(exactOutOfRange), CreateRecord(exactOutOfRange, "Helsinki", 100)),
        };
        records.AddRange(broadOnly.Select(grainId =>
            (CreateRecordKey(grainId), CreateRecord(grainId, "London", 10))));
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = ExactCity("Helsinki"),
            Right = SalaryRange(5, 15),
        };

        var result = Evaluate(
            CreateView(records),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            plan);

        result.Items.Should().ContainSingle().Which.Should().Be(exactAndInRange);
        result.Work.OrderedCandidateVisitCount.Should().Be(2, "the exact posting is the driver");
        result.Work.PredicateNodeProbeCount.Should().Be(6);
        result.Work.ResultMaterializationCount.Should().Be(1);
        result.Work.Should().BeEquivalentTo(new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = 2,
            RecordProbeCount = 2,
            PredicateNodeProbeCount = 6,
            IndexEntryProbeCount = 6,
            OwnershipProbeCount = 2,
            PostingSeekCount = 1,
            RangeBucketVisitCount = 0,
            ResultMaterializationCount = 1,
        });
        result.Work.TotalOperationCount.Should().Be(20);
    }

    [Fact]
    public void ExactDriverSelectionUsesPlanOrderWithoutUnchargedCardinalityReads()
    {
        var records = Enumerable.Range(0, 8)
            .Select(index =>
            {
                var grainId = GrainId.Create("page-type", $"exact-order-{index:D2}");
                return (
                    CreateRecordKey(grainId),
                    CreateRecord(grainId, "Helsinki", index == 7 ? 100 : 10));
            })
            .ToArray();
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = ExactCity("Helsinki"),
            Right = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Exact,
                Scope = SalaryScope,
                IndexKind = SearchableIndexKind.Range,
                Value = IndexValue.Create(100),
            },
        };

        var result = Evaluate(
            CreateView(records),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            plan);

        result.Items.Should().ContainSingle();
        result.Work.PostingSeekCount.Should().Be(1);
        result.Work.OrderedCandidateVisitCount.Should().Be(8,
            "the first conjunctive exact posting is chosen without reading competing cardinalities");
    }

    [Fact]
    public void GeneralOrFallsBackToStateCatalogWithoutOmittingEitherBranch()
    {
        var helsinki = GrainId.Create("page-type", "helsinki");
        var highSalary = GrainId.Create("page-type", "high-salary");
        var noMatch = GrainId.Create("page-type", "no-match");
        var view = CreateView(
        [
            (CreateRecordKey(helsinki), CreateRecord(helsinki, "Helsinki", 10)),
            (CreateRecordKey(highSalary), CreateRecord(highSalary, "London", 100)),
            (CreateRecordKey(noMatch), CreateRecord(noMatch, "London", 10)),
        ]);
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = ExactCity("Helsinki"),
            Right = SalaryRange(90, 110),
        };

        var result = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            plan);

        result.Items.Should().Equal(
            new[] { helsinki, highSalary }.Order(GrainIdCanonicalOrder.Comparer));
        result.Work.OrderedCandidateVisitCount.Should().Be(3, "OR requires the catalog superset");
        result.Work.PostingSeekCount.Should().Be(1);
    }

    [Fact]
    public void BroadAndRangeDriverReportsShortCircuitWorkExactly()
    {
        var records = new[]
        {
            (GrainId.Create("page-type", "and-00"), 10),
            (GrainId.Create("page-type", "and-01"), 100),
            (GrainId.Create("page-type", "and-02"), 200),
        };
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = SalaryRange(0, 150),
            Right = SalaryRange(50, 150),
        };

        var result = Evaluate(
            CreateView(records.Select(pair =>
                (CreateRecordKey(pair.Item1), CreateRecord(pair.Item1, "London", pair.Item2)))),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            plan);

        result.Items.Should().ContainSingle().Which.Should().Be(records[1].Item1);
        result.Work.Should().BeEquivalentTo(new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = 2,
            RecordProbeCount = 2,
            PredicateNodeProbeCount = 6,
            IndexEntryProbeCount = 8,
            OwnershipProbeCount = 2,
            PostingSeekCount = 3,
            RangeBucketVisitCount = 2,
            ResultMaterializationCount = 1,
            RangeMergeOperationCount = 4,
        });
        result.Work.TotalOperationCount.Should().Be(30);
    }

    [Fact]
    public void DuplicateHeavyOrCatalogFallbackChargesBothBranchesWhenRequired()
    {
        var match = GrainId.Create("page-type", "or-match");
        var noMatch = GrainId.Create("page-type", "or-no-match");
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = ExactCity("Helsinki"),
            Right = ExactCity("Helsinki"),
        };

        var result = Evaluate(
            CreateView(
            [
                (CreateRecordKey(match), CreateRecord(match, "Helsinki", 10)),
                (CreateRecordKey(noMatch), CreateRecord(noMatch, "London", 10)),
            ]),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            plan);

        result.Items.Should().ContainSingle().Which.Should().Be(match);
        result.Work.Should().BeEquivalentTo(new PartitionQueryPageWork
        {
            OrderedCandidateVisitCount = 2,
            RecordProbeCount = 2,
            PredicateNodeProbeCount = 5,
            IndexEntryProbeCount = 5,
            OwnershipProbeCount = 2,
            PostingSeekCount = 1,
            RangeBucketVisitCount = 0,
            ResultMaterializationCount = 1,
        });
        result.Work.TotalOperationCount.Should().Be(18);
    }

    [Fact]
    public void WorkBudgetStopsAtPreviousCompleteCandidateWhenNextGroupIsPartial()
    {
        var grainIds = Enumerable.Range(0, 3)
            .Select(index => GrainId.Create("page-type", $"work-{index:D2}"))
            .ToArray();
        var view = CreateView(grainIds.Select(grainId =>
            (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", 10))));

        var result = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            workBudget: 9);

        result.Items.Should().ContainSingle().Which.Should().Be(grainIds[0]);
        result.HasFrontier.Should().BeTrue();
        result.Frontier.Should().Be(grainIds[0]);
        result.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
        result.Work.TotalOperationCount.Should().Be(9);
        result.Work.OrderedCandidateVisitCount.Should().Be(2);
        result.Work.OwnershipProbeCount.Should().Be(2);
        result.Work.RecordProbeCount.Should().Be(1, "the second group stopped before its record probe");
    }

    [Fact]
    public void WorkBudgetWhichCannotAdvanceFailsInsteadOfReturningSameFrontier()
    {
        var grainId = GrainId.Create("page-type", "too-expensive");
        var view = CreateView(
        [
            (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", 10)),
        ]);

        var evaluate = () => Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            workBudget: 2);

        var exception = evaluate.Should().Throw<PartitionQueryBudgetTooSmallException>().Which;
        exception.RequestedLimit.Should().Be(2);
        exception.MinimumRequired.Should().Be(3);
        exception.Reason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
    }

    [Fact]
    public void ExactCandidateSucceedsAtItsExactWorkBoundaryAndFailsOneBefore()
    {
        var grainId = GrainId.Create("page-type", "exact-boundary");
        var view = CreateView(
        [
            (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", 10)),
        ]);

        var oneBefore = () => Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            workBudget: 6);
        oneBefore.Should().Throw<PartitionQueryBudgetTooSmallException>()
            .Which.MinimumRequired.Should().Be(7);

        var exact = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            workBudget: 7);
        exact.Exhausted.Should().BeTrue();
        exact.Items.Should().ContainSingle().Which.Should().Be(grainId);
        exact.Work.TotalOperationCount.Should().Be(7);
    }

    [Fact]
    public void PartialMultiRecordCandidateStopsAtThePreviousCompleteFrontier()
    {
        var first = GrainId.Create("page-type", "multi-00");
        var grouped = GrainId.Create("page-type", "multi-01");
        var groupedKey = CreateRecordKey(grouped);
        var view = CreateView(
        [
            (CreateRecordKey(first), CreateRecord(first, "Helsinki", 10)),
            (groupedKey + "-a", CreateRecord(grouped, "London", 10)),
            (groupedKey + "-b", CreateRecord(grouped, "Helsinki", 10)),
        ]);

        var firstPage = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            workBudget: 14);

        firstPage.Items.Should().ContainSingle().Which.Should().Be(first);
        firstPage.HasFrontier.Should().BeTrue();
        firstPage.Frontier.Should().Be(first);
        firstPage.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
        firstPage.Work.TotalOperationCount.Should().Be(14);
        firstPage.Work.OrderedCandidateVisitCount.Should().Be(2);
        firstPage.Work.RecordProbeCount.Should().Be(3);

        var resumed = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            hasAfter: true,
            after: firstPage.Frontier);
        resumed.Items.Should().ContainSingle().Which.Should().Be(grouped);
        firstPage.Items.Concat(resumed.Items).Should().Equal(first, grouped);
    }

    [Fact]
    public void SingleCanonicalItemWhichCannotFitByteLimitFailsExplicitly()
    {
        var grainId = GrainId.Create("page-type", "oversized-item");
        var view = CreateView(
        [
            (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", 10)),
        ]);
        var encodedLength = GrainIdCanonicalOrder.GetEncodedLength(grainId);

        var evaluate = () => Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            byteLimit: encodedLength - 1);

        var exception = evaluate.Should().Throw<PartitionQueryBudgetTooSmallException>().Which;
        exception.RequestedLimit.Should().Be(encodedLength - 1);
        exception.MinimumRequired.Should().Be(encodedLength);
        exception.Reason.Should().Be(PartitionQueryPageStopReason.ByteLimit);
    }

    [Fact]
    public void OversizedLaterItemFailsTheWholeTurnInsteadOfReturningADoomedContinuation()
    {
        var first = GrainId.Create("page-type", "a");
        var oversized = GrainId.Create("page-type", "b" + new string('x', 200));
        var view = CreateView(
        [
            (CreateRecordKey(first), CreateRecord(first, "Helsinki", 10)),
            (CreateRecordKey(oversized), CreateRecord(oversized, "Helsinki", 10)),
        ]);
        var byteLimit = GrainIdCanonicalOrder.GetEncodedLength(first) + 1;
        GrainIdCanonicalOrder.GetEncodedLength(oversized).Should().BeGreaterThan(byteLimit);

        var evaluate = () => Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            byteLimit: byteLimit);

        var exception = evaluate.Should().Throw<PartitionQueryBudgetTooSmallException>().Which;
        exception.RequestedLimit.Should().Be(byteLimit);
        exception.MinimumRequired.Should().Be(GrainIdCanonicalOrder.GetEncodedLength(oversized));
        exception.Reason.Should().Be(PartitionQueryPageStopReason.ByteLimit);
    }

    [Fact]
    public void ExactByteBoundaryReturnsAResumableCompletePrefix()
    {
        var grainIds = new[]
        {
            GrainId.Create("page-type", "byte-00"),
            GrainId.Create("page-type", "byte-01"),
        };
        var view = CreateView(grainIds.Select(grainId =>
            (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", 10))));
        var byteLimit = GrainIdCanonicalOrder.GetEncodedLength(grainIds[0]);

        var first = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            byteLimit: byteLimit);
        first.Items.Should().ContainSingle().Which.Should().Be(grainIds[0]);
        first.StopReason.Should().Be(PartitionQueryPageStopReason.ByteLimit);
        first.Frontier.Should().Be(grainIds[0]);

        var second = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            byteLimit: byteLimit,
            hasAfter: true,
            after: first.Frontier);
        second.Exhausted.Should().BeTrue();
        second.Items.Should().ContainSingle().Which.Should().Be(grainIds[1]);
    }

    [Fact]
    public void OwnershipFilteringOccursInsideTheSameBoundedCanonicalTurn()
    {
        var layout = CreateLayout(partitionCount: 2);
        var owned = CreateGrainOwnedBy(layout, owner: 0, "owned");
        var foreign = CreateGrainOwnedBy(layout, owner: 1, "foreign");
        var view = CreateView(
        [
            (CreateRecordKey(owned), CreateRecord(owned, "Helsinki", 10)),
            (CreateRecordKey(foreign), CreateRecord(foreign, "Helsinki", 10)),
        ]);

        var result = Evaluate(view, layout, partitionIndex: 0, ExactCity("Helsinki"));

        result.Items.Should().ContainSingle().Which.Should().Be(owned);
        result.Work.OrderedCandidateVisitCount.Should().Be(2);
        result.Work.OwnershipProbeCount.Should().Be(2);
        result.Work.RecordProbeCount.Should().Be(1);
        result.Work.ResultMaterializationCount.Should().Be(1);
    }

    [Fact]
    public void ConcatenatedPartitionPrefixesEqualFullEvaluationForEveryDriverFamily()
    {
        var records = Enumerable.Range(0, 48)
            .Select(index =>
            {
                var grainId = GrainId.Create("page-type", $"property-{index:D2}");
                var city = index % 3 == 0 ? "Helsinki" : "London";
                return (CreateRecordKey(grainId), CreateRecord(grainId, city, index));
            })
            .ToArray();
        var view = CreateView(records);
        var layout = CreateLayout(partitionCount: 1);
        var exact = ExactCity("Helsinki");
        var range = SalaryRange(11, 37);
        var plans = new PartitionQueryPlan[]
        {
            exact,
            range,
            new()
            {
                Operation = PartitionQueryOperation.And,
                Left = ExactCity("Helsinki"),
                Right = SalaryRange(11, 37),
            },
            new()
            {
                Operation = PartitionQueryOperation.Or,
                Left = ExactCity("Helsinki"),
                Right = SalaryRange(11, 37),
            },
        };

        foreach (var plan in plans)
        {
            var expected = Evaluate(view, layout, partitionIndex: 0, plan).Items;
            var actual = new List<GrainId>();
            var hasAfter = false;
            var after = default(GrainId);
            for (var round = 0; round < 100; round++)
            {
                var page = Evaluate(
                    view,
                    layout,
                    partitionIndex: 0,
                    plan,
                    itemLimit: 3,
                    hasAfter: hasAfter,
                    after: after);
                if (hasAfter)
                {
                    page.Items.Should().OnlyContain(item =>
                        GrainIdCanonicalOrder.Compare(item, after) > 0);
                }

                if (page.HasFrontier)
                {
                    page.Items.Should().OnlyContain(item =>
                        GrainIdCanonicalOrder.Compare(item, page.Frontier) <= 0);
                }

                actual.AddRange(page.Items);
                if (page.Exhausted)
                {
                    break;
                }

                page.HasFrontier.Should().BeTrue();
                if (hasAfter)
                {
                    GrainIdCanonicalOrder.Compare(page.Frontier, after).Should().BePositive();
                }

                after = page.Frontier;
                hasAfter = true;
            }

            actual.Should().Equal(expected);
        }
    }

    private static PartitionQueryPageResult Evaluate(
        StoragePartitionView view,
        StorageLayoutSnapshot layout,
        int partitionIndex,
        PartitionQueryPlan plan,
        long workBudget = SearchableStorageQueryOptions.DefaultPartitionWorkBudget,
        int itemLimit = SearchableStorageQueryOptions.DefaultPartitionResponseItems,
        int byteLimit = SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
        bool hasAfter = false,
        GrainId after = default)
    {
        var queryFingerprint = QueryPlanFingerprint.Compute(StateName, plan);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
        var request = new RoutedPartitionQueryPageRequest
        {
            Query = plan,
            Epoch = layout.Epoch,
            HasAfter = hasAfter,
            After = after,
            WorkBudget = workBudget,
            ItemLimit = itemLimit,
            ByteLimit = byteLimit,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.OrderingVersion,
            WorkPolicyVersion = QueryProtocol.WorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.GrainIdPage,
            QueryFingerprint = queryFingerprint,
            LayoutFormatVersion = layout.FormatVersion,
            LayoutFingerprint = layoutFingerprint,
            StateName = StateName,
        };
        return StoragePartitionQueryPageEvaluator.EvaluateValidated(
            request,
            view,
            layout,
            partitionIndex,
            queryFingerprint,
            layoutFingerprint);
    }

    private static StoragePartitionView CreateView(
        IEnumerable<(string RecordKey, StoredRecord Record)> records)
    {
        return new StoragePartitionView(records.ToDictionary(
            static pair => pair.RecordKey,
            static pair => pair.Record,
            StringComparer.Ordinal));
    }

    private static StoredRecord CreateRecord(GrainId grainId, string city, int salary)
    {
        return new StoredRecord
        {
            GrainId = grainId,
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
                    Value = IndexValue.Create(salary),
                },
            ],
        };
    }

    private static StoredRecord CreateRecordWithSalaries(
        GrainId grainId,
        string city,
        params int[] salaries)
    {
        return new StoredRecord
        {
            GrainId = grainId,
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
                .. salaries.Select(salary => new IndexEntry
                {
                    Scope = SalaryScope,
                    Kind = SearchableIndexKind.Range,
                    Value = IndexValue.Create(salary),
                }),
            ],
        };
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

    private static PartitionQueryPlan SalaryRange(int lower, int upper)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = SalaryScope,
            LowerBound = IndexValue.Create(lower),
            UpperBound = IndexValue.Create(upper),
            IncludeLowerBound = true,
            IncludeUpperBound = true,
        };
    }

    private static StorageLayoutSnapshot CreateLayout(int partitionCount)
    {
        var virtualSlotCount = partitionCount * 8;
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = "page-provider",
            PartitionCount = partitionCount,
            VirtualSlotCount = virtualSlotCount,
            SlotAssignments = StorageLayout.CreateIdentityAssignments(
                partitionCount,
                virtualSlotCount),
            Epoch = 1,
        });
    }

    private static GrainId CreateGrainOwnedBy(
        StorageLayoutSnapshot layout,
        int owner,
        string prefix)
    {
        for (var index = 0; index < 10_000; index++)
        {
            var grainId = GrainId.Create("page-type", $"{prefix}-{index:D5}");
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            if (layout.GetOwner(slot) == owner)
            {
                return grainId;
            }
        }

        throw new InvalidOperationException($"Could not find a grain owned by partition {owner}.");
    }

    private static string CreateRecordKey(GrainId grainId)
    {
        return string.Concat(
            StateName,
            "/",
            Convert.ToHexString(grainId.Type.AsSpan()),
            "/",
            Convert.ToHexString(grainId.Key.AsSpan()));
    }
}
