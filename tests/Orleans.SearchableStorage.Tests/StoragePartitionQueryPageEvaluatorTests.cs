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
    public void MissingExactPostingIsProvedEmptyByChargedMetadata()
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
        result.Work.PlannerNodeVisitCount.Should().Be(1);
        result.Work.PlannerMetadataReadCount.Should().Be(1);
        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Empty);
        result.Work.TotalOperationCount.Should().Be(3);
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
            PostingSeekCount = 2,
            RangeBucketVisitCount = 0,
            ResultMaterializationCount = 1,
            PlannerNodeVisitCount = 1,
            PlannerMetadataReadCount = 1,
            PostingCandidateVisitCount = 1,
            AccessPath = PartitionQueryAccessPath.ExactPosting,
        });
        result.Work.TotalOperationCount.Should().Be(11);
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
            PlannerNodeVisitCount = 1,
            PlannerMetadataReadCount = 2,
            PostingCandidateVisitCount = 2,
            HeapOperationCount = 5,
            AccessPath = PartitionQueryAccessPath.RangeMerge,
        });
        result.Work.TotalOperationCount.Should().Be(33);
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
            PlannerNodeVisitCount = 1,
            PlannerMetadataReadCount = 1,
            PostingCandidateVisitCount = 1,
            HeapOperationCount = 2,
            AccessPath = PartitionQueryAccessPath.RangeMerge,
        });
        result.Work.TotalOperationCount.Should().Be(18);
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
            PlannerNodeVisitCount = 1,
            PlannerMetadataReadCount = 2,
            PostingCandidateVisitCount = 3,
            HeapOperationCount = 7,
            AccessPath = PartitionQueryAccessPath.RangeMerge,
        });
        result.Work.TotalOperationCount.Should().Be(38);
    }

    [Fact]
    public void RangeSourceAdmissionFallsBackOneBeforeAndAdvancesAtTheTransition()
    {
        var first = GrainId.Create("page-type", "range-boundary-a");
        var second = GrainId.Create("page-type", "range-boundary-b");
        var view = CreateView(
        [
            (CreateRecordKey(first), CreateRecordWithSalaries(first, "Helsinki", 10, 20)),
            (CreateRecordKey(second), CreateRecordWithSalaries(second, "Helsinki", 20)),
        ]);

        var oneBefore = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(10, 20),
            workBudget: 45);
        oneBefore.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Catalog);
        oneBefore.Items.Should().Equal(first, second);

        var admitted = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(10, 20),
            workBudget: 46);
        admitted.Work.AccessPath.Should().Be(PartitionQueryAccessPath.RangeMerge);
        admitted.Items.Should().Equal(first, second);
        admitted.Items.Should().Equal(oneBefore.Items);
    }

    [Fact]
    public void RangeAdmissionUsesOnlyTheTraversedSelectedWindow()
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
            workBudget: 46);

        result.Exhausted.Should().BeTrue();
        result.StopReason.Should().Be(PartitionQueryPageStopReason.Exhausted);
        result.HasFrontier.Should().BeFalse();
        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.RangeMerge);
        result.Work.PostingSeekCount.Should().Be(3);
        result.Work.RangeBucketVisitCount.Should().Be(2);
        result.Work.PlannerMetadataReadCount.Should().Be(2);
        result.Work.RangeMergeOperationCount.Should().Be(4);
        result.Work.TotalOperationCount.Should().Be(33);
        result.Items.Should().Equal(records[0].Item2.GrainId, records[1].Item2.GrainId);
    }

    [Fact]
    public void BroadRangePlanningPreservesAChargedCatalogFallbackAndCompleteFrontier()
    {
        var records = Enumerable.Range(0, 40)
            .Select(index =>
            {
                var grainId = GrainId.Create("page-type", $"catalog-fallback-{index:D2}");
                return (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", index));
            })
            .ToArray();

        var result = Evaluate(
            CreateView(records),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            SalaryRange(0, 39),
            workBudget: 45);

        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Catalog);
        result.Work.CatalogCandidateVisitCount.Should().BePositive();
        result.Work.RangeBucketVisitCount.Should().BePositive(
            "the abandoned range descriptor still reports its charged planning work");
        result.Work.TotalOperationCount.Should().Be(45);
        result.Items.Should().Equal(
            records[0].Item2.GrainId,
            records[1].Item2.GrainId);
        result.HasFrontier.Should().BeTrue();
        result.Frontier.Should().Be(records[1].Item2.GrainId);
        result.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
    }

    [Fact]
    public void TwoBucketRangeSourceTransitionHasNoSetupBudgetCliff()
    {
        var records = new[]
        {
            GrainId.Create("page-type", "range-transition-00"),
            GrainId.Create("page-type", "range-transition-01"),
        };
        var view = CreateView(
        [
            (CreateRecordKey(records[0]), CreateRecord(records[0], "Helsinki", 10)),
            (CreateRecordKey(records[1]), CreateRecord(records[1], "Helsinki", 20)),
        ]);
        var layout = CreateLayout(partitionCount: 1);
        var observedRangeMerge = false;

        for (var budget = 15; budget <= 60; budget++)
        {
            var result = Evaluate(
                view,
                layout,
                partitionIndex: 0,
                SalaryRange(10, 20),
                workBudget: budget);

            (result.Exhausted || result.HasFrontier).Should().BeTrue(
                $"budget {budget} must either exhaust or advance a complete frontier");
            if (result.Work.AccessPath == PartitionQueryAccessPath.RangeMerge)
            {
                observedRangeMerge = true;
            }
            else
            {
                observedRangeMerge.Should().BeFalse(
                    "source admission must not revert to catalog after this setup transition");
                result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Catalog);
            }
        }

        observedRangeMerge.Should().BeTrue();
    }

    [Fact]
    public void DuplicateHeavyRangeSourceTransitionHasNoBudgetCliff()
    {
        var grainId = GrainId.Create("page-type", "range-source-duplicate");
        var salaries = Enumerable.Range(0, 8).ToArray();
        var view = CreateView(
        [
            (CreateRecordKey(grainId),
                CreateRecordWithSalaries(grainId, "Helsinki", salaries)),
        ]);
        var layout = CreateLayout(partitionCount: 1);

        for (var budget = 250; budget <= 285; budget++)
        {
            var result = Evaluate(
                view,
                layout,
                partitionIndex: 0,
                SalaryRange(0, 7),
                workBudget: budget);

            result.Items.Should().ContainSingle().Which.Should().Be(grainId);
            result.Exhausted.Should().BeTrue();
            result.Work.AccessPath.Should().Be(
                budget < 272
                    ? PartitionQueryAccessPath.Catalog
                    : PartitionQueryAccessPath.RangeMerge,
                $"budget {budget} must cross the charged source-admission boundary once");
        }
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
            PostingSeekCount = 3,
            RangeBucketVisitCount = 1,
            ResultMaterializationCount = 1,
            PlannerNodeVisitCount = 3,
            PlannerMetadataReadCount = 2,
            PostingCandidateVisitCount = 2,
            AccessPath = PartitionQueryAccessPath.ExactPosting,
        });
        result.Work.TotalOperationCount.Should().Be(30);
    }

    [Fact]
    public void AndChoosesTheCheapestChargedExactPostingRegardlessOfOperandOrder()
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

        var view = CreateView(records);
        var layout = CreateLayout(partitionCount: 1);
        var result = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            plan);

        var swapped = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = plan.Right,
                Right = plan.Left,
            });

        result.Items.Should().ContainSingle();
        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.ExactPosting);
        result.Work.PostingSeekCount.Should().Be(3);
        result.Work.PlannerMetadataReadCount.Should().Be(2);
        result.Work.OrderedCandidateVisitCount.Should().Be(1);
        swapped.Items.Should().Equal(result.Items);
        swapped.Work.Should().BeEquivalentTo(result.Work);
    }

    [Fact]
    public void AndKeepsACompletedExactDriverWhenBroadSiblingPlanningRunsOutOfBudget()
    {
        var exact = GrainId.Create("page-type", "constrained-exact");
        var records = new List<(string, StoredRecord)>
        {
            (CreateRecordKey(exact), CreateRecord(exact, "Helsinki", 10)),
        };
        records.AddRange(Enumerable.Range(0, 30).Select(index =>
        {
            var grainId = GrainId.Create("page-type", $"constrained-broad-{index:D2}");
            return (CreateRecordKey(grainId), CreateRecord(grainId, "London", index));
        }));
        var exactLeaf = ExactCity("Helsinki");
        var broadLeaf = SalaryRange(0, 100);
        var view = CreateView(records);
        var layout = CreateLayout(partitionCount: 1);

        var leftExact = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = exactLeaf,
                Right = broadLeaf,
            },
            workBudget: 45);
        var rightExact = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = broadLeaf,
                Right = exactLeaf,
            },
            workBudget: 45);

        leftExact.Items.Should().ContainSingle().Which.Should().Be(exact);
        leftExact.Work.AccessPath.Should().Be(PartitionQueryAccessPath.ExactPosting);
        leftExact.Work.RangeBucketVisitCount.Should().BePositive();
        leftExact.Work.CatalogCandidateVisitCount.Should().Be(0);
        rightExact.Items.Should().Equal(leftExact.Items);
        rightExact.Work.Should().BeEquivalentTo(leftExact.Work);
    }

    [Fact]
    public void BroadCanonicalRangeCannotStarveACheaperRangeSibling()
    {
        var records = Enumerable.Range(0, 30)
            .Select(index =>
            {
                var grainId = GrainId.Create("page-type", $"range-sibling-{index:D2}");
                return (CreateRecordKey(grainId), CreateRecord(grainId, "London", index));
            })
            .ToArray();
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = SalaryRange(0, 29),
            Right = SalaryRange(29, 29),
        };

        var result = Evaluate(
            CreateView(records),
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            plan,
            workBudget: 45);

        result.Items.Should().ContainSingle().Which.Should().Be(records[^1].Item2.GrainId);
        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.RangeMerge);
        result.Work.RangeBucketVisitCount.Should().BeGreaterThan(1,
            "the abandoned broad descriptor keeps its charged planning evidence");
        result.Work.PlannerMetadataReadCount.Should().BeGreaterThan(1);
        result.Work.CatalogCandidateVisitCount.Should().Be(0);
    }

    [Fact]
    public void BroadBooleanPlanningReservesHalfOfAnOddTurnForCatalogExecution()
    {
        var records = Enumerable.Range(0, 40)
            .Select(index =>
            {
                var grainId = GrainId.Create("page-type", $"execution-reserve-{index:D2}");
                return (CreateRecordKey(grainId), CreateRecord(grainId, "London", index));
            })
            .ToArray();
        var plan = Boolean(
            PartitionQueryOperation.And,
            SalaryRange(0, 39),
            SalaryRange(0, 39));
        var view = CreateView(records);
        var layout = CreateLayout(partitionCount: 1);

        var oddPostPreparationTurn = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            plan,
            workBudget: 100);
        var swapped = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            Boolean(PartitionQueryOperation.And, plan.Right!, plan.Left!),
            workBudget: 100);

        oddPostPreparationTurn.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Catalog);
        oddPostPreparationTurn.Work.TotalOperationCount.Should().Be(100);
        oddPostPreparationTurn.Items.Should().HaveCount(4,
            "ceil(97 / 2) charged operations remain after three-node preparation");
        oddPostPreparationTurn.HasFrontier.Should().BeTrue();
        swapped.Items.Should().Equal(oddPostPreparationTurn.Items);
        swapped.Work.Should().BeEquivalentTo(oddPostPreparationTurn.Work);
    }

    [Fact]
    public void AssociativeAndUsesOneCanonicalCheapestDriverAcrossEveryGroupingAndPermutation()
    {
        var records = Enumerable.Range(0, 12)
            .Select(index =>
            {
                var grainId = GrainId.Create("page-type", $"associative-and-{index:D2}");
                var salary = index == 11 ? 100 : index;
                return (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", salary));
            })
            .ToArray();
        var view = CreateView(records);
        var layout = CreateLayout(partitionCount: 1);
        var forms = CreateAssociativeForms(
            PartitionQueryOperation.And,
            ExactCity("Helsinki"),
            ExactSalary(100),
            SalaryRange(0, 100));

        var results = forms
            .Select(plan => Evaluate(
                view,
                layout,
                partitionIndex: 0,
                plan,
                workBudget: 45))
            .ToArray();
        var baseline = results[0];

        baseline.Items.Should().ContainSingle().Which.Should().Be(records[^1].Item2.GrainId);
        baseline.Work.AccessPath.Should().Be(PartitionQueryAccessPath.ExactPosting);
        baseline.Work.OrderedCandidateVisitCount.Should().Be(1);
        baseline.Work.PlannerNodeVisitCount.Should().Be(5);
        foreach (var result in results.Skip(1))
        {
            result.Items.Should().Equal(baseline.Items);
            result.Exhausted.Should().Be(baseline.Exhausted);
            result.HasFrontier.Should().Be(baseline.HasFrontier);
            result.StopReason.Should().Be(baseline.StopReason);
            result.Work.Should().BeEquivalentTo(baseline.Work);
        }
    }

    [Fact]
    public void ComposedAssociativeFormsUseStableGlobalRanksAcrossPreparedHeights()
    {
        var target = GrainId.Create("page-type", "composed-rank-00");
        var second = GrainId.Create("page-type", "composed-rank-01");
        var filtered = GrainId.Create("page-type", "composed-rank-02");
        var view = CreateView(
        [
            (CreateRecordKey(target), CreateRecord(target, "Helsinki", 100)),
            (CreateRecordKey(second), CreateRecord(second, "Helsinki", 200)),
            (CreateRecordKey(filtered), CreateRecord(filtered, "Helsinki", 50)),
        ]);
        var layout = CreateLayout(partitionCount: 1);
        var oppositeForms = CreateAssociativeForms(
            PartitionQueryOperation.Or,
            ExactSalary(100),
            ExactSalary(200),
            ExactSalary(999));
        var plans = Enumerable.Range(0, oppositeForms.Length)
            .Select(index => CreateAssociativeForms(
                PartitionQueryOperation.And,
                ExactCity("Helsinki"),
                oppositeForms[index],
                Boolean(
                    PartitionQueryOperation.Or,
                    ExactSalary(100),
                    Boolean(
                        PartitionQueryOperation.And,
                        ExactCity("Helsinki"),
                        SalaryRange(0, 150))))[index])
            .ToArray();

        var results = plans.Select(plan => Evaluate(
            view,
            layout,
            partitionIndex: 0,
            plan,
            itemLimit: 1)).ToArray();
        var baseline = results[0];

        baseline.Items.Should().ContainSingle().Which.Should().Be(target);
        baseline.HasFrontier.Should().BeTrue();
        baseline.Frontier.Should().Be(target);
        baseline.StopReason.Should().Be(PartitionQueryPageStopReason.ItemLimit);
        baseline.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Union);
        baseline.Work.PlannerNodeVisitCount.Should().Be(13);
        foreach (var result in results.Skip(1))
        {
            result.Items.Should().Equal(baseline.Items);
            result.HasFrontier.Should().BeTrue();
            result.Frontier.Should().Be(baseline.Frontier);
            result.StopReason.Should().Be(baseline.StopReason);
            result.Work.Should().BeEquivalentTo(baseline.Work);
        }
    }

    [Fact]
    public void EqualCardinalityAndTieUsesCanonicalPathRegardlessOfOperandOrder()
    {
        var cityCandidate = GrainId.Create("page-type", "tie-city");
        var salaryCandidate = GrainId.Create("page-type", "tie-salary");
        var cityKey = CreateRecordKey(cityCandidate);
        var salaryExact = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = SalaryScope,
            IndexKind = SearchableIndexKind.Range,
            Value = IndexValue.Create(100),
        };
        var view = CreateView(
        [
            (cityKey + "-a", CreateRecord(cityCandidate, "Helsinki", 10)),
            (cityKey + "-b", CreateRecord(cityCandidate, "Helsinki", 20)),
            (CreateRecordKey(salaryCandidate), CreateRecord(salaryCandidate, "London", 100)),
        ]);
        var layout = CreateLayout(partitionCount: 1);

        PartitionQueryPageResult Run(PartitionQueryPlan left, PartitionQueryPlan right) => Evaluate(
            view,
            layout,
            partitionIndex: 0,
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = left,
                Right = right,
            });

        var first = Run(ExactCity("Helsinki"), salaryExact);
        var second = Run(salaryExact, ExactCity("Helsinki"));

        first.Items.Should().BeEmpty();
        first.Work.AccessPath.Should().Be(PartitionQueryAccessPath.ExactPosting);
        first.Work.RecordProbeCount.Should().Be(2,
            "the canonical city path wins the equal-cardinality tie");
        second.Work.Should().BeEquivalentTo(first.Work);
    }

    [Fact]
    public void OrUsesASortedDistinctUnionWithoutOmittingEitherBranch()
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
        var swapped = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Or,
                Left = plan.Right,
                Right = plan.Left,
            });

        result.Items.Should().Equal(
            new[] { helsinki, highSalary }.Order(GrainIdCanonicalOrder.Comparer));
        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Union);
        result.Work.OrderedCandidateVisitCount.Should().Be(2);
        result.Work.CatalogCandidateVisitCount.Should().Be(0);
        result.Work.UnionOperationCount.Should().BePositive();
        swapped.Items.Should().Equal(result.Items);
        swapped.Work.Should().BeEquivalentTo(result.Work);
    }

    [Fact]
    public void TightUnionBudgetStopsAtThePreviousCompleteDistinctCandidate()
    {
        var first = GrainId.Create("page-type", "union-tight-a");
        var second = GrainId.Create("page-type", "union-tight-b");
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = ExactCity("Helsinki"),
            Right = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Exact,
                Scope = SalaryScope,
                IndexKind = SearchableIndexKind.Range,
                Value = IndexValue.Create(100),
            },
        };

        var view = CreateView(
        [
            (CreateRecordKey(first), CreateRecord(first, "Helsinki", 10)),
            (CreateRecordKey(second), CreateRecord(second, "London", 100)),
        ]);
        var layout = CreateLayout(partitionCount: 1);
        var oneBefore = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            plan,
            workBudget: 29);
        var result = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            plan,
            workBudget: 30);

        oneBefore.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Catalog,
            "the 16-operation fallback minimum dominates half of this small turn");
        (oneBefore.Exhausted || oneBefore.HasFrontier).Should().BeTrue();
        result.Items.Should().ContainSingle().Which.Should().Be(first);
        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Union);
        result.Work.TotalOperationCount.Should().Be(30);
        result.HasFrontier.Should().BeTrue();
        result.Frontier.Should().Be(first);
        result.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
    }

    [Fact]
    public void AssociativeOrKeepsOneDistinctFrontierAcrossEveryGroupingAndPermutation()
    {
        var duplicate = GrainId.Create("page-type", "associative-or-00");
        var salaryOnly = GrainId.Create("page-type", "associative-or-01");
        var cityOnly = GrainId.Create("page-type", "associative-or-02");
        var view = CreateView(
        [
            (CreateRecordKey(duplicate), CreateRecord(duplicate, "Helsinki", 100)),
            (CreateRecordKey(salaryOnly), CreateRecord(salaryOnly, "London", 200)),
            (CreateRecordKey(cityOnly), CreateRecord(cityOnly, "Helsinki", 300)),
        ]);
        var layout = CreateLayout(partitionCount: 1);
        var forms = CreateAssociativeForms(
            PartitionQueryOperation.Or,
            ExactCity("Helsinki"),
            ExactSalary(100),
            ExactSalary(200));

        var results = forms
            .Select(plan => Evaluate(
                view,
                layout,
                partitionIndex: 0,
                plan,
                workBudget: 40))
            .ToArray();
        var baseline = results[0];

        baseline.Items.Should().ContainSingle().Which.Should().Be(duplicate);
        baseline.HasFrontier.Should().BeTrue();
        baseline.Frontier.Should().Be(duplicate);
        baseline.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
        baseline.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Union);
        baseline.Work.PlannerNodeVisitCount.Should().Be(5);
        foreach (var result in results.Skip(1))
        {
            result.Items.Should().Equal(baseline.Items);
            result.HasFrontier.Should().BeTrue();
            result.Frontier.Should().Be(baseline.Frontier);
            result.StopReason.Should().Be(baseline.StopReason);
            result.Work.Should().BeEquivalentTo(baseline.Work);
        }

        var resumed = Evaluate(
            view,
            layout,
            partitionIndex: 0,
            forms[0],
            hasAfter: true,
            after: baseline.Frontier);
        baseline.Items.Concat(resumed.Items).Should().Equal(duplicate, salaryOnly, cityOnly);
    }

    [Fact]
    public void EightWayDuplicateUnionSourceTransitionHasNoBudgetCliff()
    {
        var grainId = GrainId.Create("page-type", "union-source-duplicate");
        var values = Enumerable.Range(0, 8)
            .Select(index => $"union-source-{index:D2}")
            .ToArray();
        var plan = values.Skip(1).Aggregate(
            ExactCity(values[0]),
            static (current, value) => Boolean(
                PartitionQueryOperation.Or,
                current,
                ExactCity(value)));
        var view = CreateView(
        [
            (CreateRecordKey(grainId), new StoredRecord
            {
                GrainId = grainId,
                Payload = [],
                ETag = "1",
                IndexEntries = [.. values.Select(value => new IndexEntry
                {
                    Scope = CityScope,
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create(value),
                })],
            }),
        ]);
        var layout = CreateLayout(partitionCount: 1);

        for (var budget = 70; budget <= 95; budget++)
        {
            var result = Evaluate(
                view,
                layout,
                partitionIndex: 0,
                plan,
                workBudget: budget);

            result.Items.Should().ContainSingle().Which.Should().Be(grainId);
            result.Exhausted.Should().BeTrue();
            result.Work.AccessPath.Should().Be(
                budget < 84
                    ? PartitionQueryAccessPath.Catalog
                    : PartitionQueryAccessPath.Union,
                $"budget {budget} must cross the charged source-admission boundary once");
        }
    }

    [Fact]
    public void MaximumDepthCanonicalPreparationHasLinearWorkAndBoundedLargeValueAllocation()
    {
        var largeValue = new string('x', QueryPlanFingerprint.MaximumPlanTextBytes);
        var values = Enumerable.Range(0, QueryPlanLimits.MaximumDepth)
            .Select(index => index == 0 ? largeValue : $"depth-value-{index:D2}")
            .ToArray();
        var plan = CreateAlternatingExactPlan(values);
        var grainId = GrainId.Create("page-type", "maximum-depth");
        var record = new StoredRecord
        {
            GrainId = grainId,
            Payload = [],
            ETag = "1",
            IndexEntries = values.Select(value => new IndexEntry
            {
                Scope = CityScope,
                Kind = SearchableIndexKind.Hash,
                Value = IndexValue.Create(value),
            }).ToList(),
        };
        var view = CreateView([(CreateRecordKey(grainId), record)]);
        var layout = CreateLayout(partitionCount: 1);
        var request = CreateRequest(layout, plan);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);

        _ = StoragePartitionQueryPageEvaluator.EvaluateValidated(
            request,
            view,
            layout,
            partitionIndex: 0,
            request.QueryFingerprint,
            layoutFingerprint);
        var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        var result = StoragePartitionQueryPageEvaluator.EvaluateValidated(
            request,
            view,
            layout,
            partitionIndex: 0,
            request.QueryFingerprint,
            layoutFingerprint);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

        result.Items.Should().ContainSingle().Which.Should().Be(grainId);
        result.Work.PlannerNodeVisitCount.Should().Be(
            checked((2 * QueryPlanLimits.MaximumDepth) - 1));
        result.Work.PredicateNodeProbeCount.Should().BeLessThanOrEqualTo(
            result.Work.PlannerNodeVisitCount);
        allocated.Should().BeLessThan(2_000_000,
            "canonical preparation retains nodes and operand edges, not copied subtree payloads");
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
            OrderedCandidateVisitCount = 1,
            RecordProbeCount = 1,
            PredicateNodeProbeCount = 3,
            IndexEntryProbeCount = 4,
            OwnershipProbeCount = 1,
            PostingSeekCount = 3,
            RangeBucketVisitCount = 3,
            ResultMaterializationCount = 1,
            RangeMergeOperationCount = 1,
            PlannerNodeVisitCount = 3,
            PlannerMetadataReadCount = 3,
            PostingCandidateVisitCount = 1,
            HeapOperationCount = 2,
            AccessPath = PartitionQueryAccessPath.RangeMerge,
        });
        result.Work.TotalOperationCount.Should().Be(27);
    }

    [Fact]
    public void DuplicateHeavyOrUnionDeduplicatesBeforeFinalPredicateEvaluation()
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
            OrderedCandidateVisitCount = 1,
            RecordProbeCount = 1,
            PredicateNodeProbeCount = 2,
            IndexEntryProbeCount = 1,
            OwnershipProbeCount = 1,
            PostingSeekCount = 4,
            RangeBucketVisitCount = 0,
            ResultMaterializationCount = 1,
            PlannerNodeVisitCount = 3,
            PlannerMetadataReadCount = 2,
            PostingCandidateVisitCount = 2,
            UnionOperationCount = 3,
            AccessPath = PartitionQueryAccessPath.Union,
        });
        result.Work.TotalOperationCount.Should().Be(21);
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
            workBudget: 12);

        result.Items.Should().ContainSingle().Which.Should().Be(grainIds[0]);
        result.HasFrontier.Should().BeTrue();
        result.Frontier.Should().Be(grainIds[0]);
        result.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
        result.Work.TotalOperationCount.Should().Be(12);
        result.Work.OrderedCandidateVisitCount.Should().Be(2);
        result.Work.OwnershipProbeCount.Should().Be(2);
        result.Work.RecordProbeCount.Should().Be(1, "the second group stopped before its record probe");
        result.Work.CatalogCandidateVisitCount.Should().Be(2);
        result.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Catalog);
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
            workBudget: 8);
        oneBefore.Should().Throw<PartitionQueryBudgetTooSmallException>()
            .Which.MinimumRequired.Should().Be(9);

        var exact = Evaluate(
            view,
            CreateLayout(partitionCount: 1),
            partitionIndex: 0,
            ExactCity("Helsinki"),
            workBudget: 9);
        exact.Exhausted.Should().BeTrue();
        exact.Items.Should().ContainSingle().Which.Should().Be(grainId);
        exact.Work.TotalOperationCount.Should().Be(9);
        exact.Work.AccessPath.Should().Be(PartitionQueryAccessPath.Catalog);
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
            workBudget: 19);

        firstPage.Items.Should().ContainSingle().Which.Should().Be(first);
        firstPage.HasFrontier.Should().BeTrue();
        firstPage.Frontier.Should().Be(first);
        firstPage.StopReason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
        firstPage.Work.TotalOperationCount.Should().Be(19);
        firstPage.Work.OrderedCandidateVisitCount.Should().Be(2);
        firstPage.Work.RecordProbeCount.Should().Be(3);
        firstPage.Work.AccessPath.Should().Be(PartitionQueryAccessPath.ExactPosting);

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
        var request = CreateRequest(
            layout,
            plan,
            workBudget,
            itemLimit,
            byteLimit,
            hasAfter,
            after);
        return StoragePartitionQueryPageEvaluator.EvaluateValidated(
            request,
            view,
            layout,
            partitionIndex,
            request.QueryFingerprint,
            request.LayoutFingerprint);
    }

    private static RoutedPartitionQueryPageRequest CreateRequest(
        StorageLayoutSnapshot layout,
        PartitionQueryPlan plan,
        long workBudget = SearchableStorageQueryOptions.DefaultPartitionWorkBudget,
        int itemLimit = SearchableStorageQueryOptions.DefaultPartitionResponseItems,
        int byteLimit = SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
        bool hasAfter = false,
        GrainId after = default)
    {
        var queryFingerprint = QueryPlanFingerprint.Compute(StateName, plan);
        var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
        return new RoutedPartitionQueryPageRequest
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
    }

    private static PartitionQueryPlan[] CreateAssociativeForms(
        PartitionQueryOperation operation,
        PartitionQueryPlan first,
        PartitionQueryPlan second,
        PartitionQueryPlan third)
    {
        var operands = new[] { first, second, third };
        var permutations = new[]
        {
            new[] { 0, 1, 2 },
            new[] { 0, 2, 1 },
            new[] { 1, 0, 2 },
            new[] { 1, 2, 0 },
            new[] { 2, 0, 1 },
            new[] { 2, 1, 0 },
        };
        return permutations.SelectMany(order => new[]
        {
            Boolean(
                operation,
                Boolean(operation, operands[order[0]], operands[order[1]]),
                operands[order[2]]),
            Boolean(
                operation,
                operands[order[0]],
                Boolean(operation, operands[order[1]], operands[order[2]])),
        }).ToArray();
    }

    private static PartitionQueryPlan CreateAlternatingExactPlan(string[] values)
    {
        var plan = ExactCity(values[0]);
        for (var index = 1; index < values.Length; index++)
        {
            plan = Boolean(
                index % 2 == 0
                    ? PartitionQueryOperation.Or
                    : PartitionQueryOperation.And,
                ExactCity(values[index]),
                plan);
        }

        return plan;
    }

    private static PartitionQueryPlan Boolean(
        PartitionQueryOperation operation,
        PartitionQueryPlan left,
        PartitionQueryPlan right)
    {
        return new PartitionQueryPlan
        {
            Operation = operation,
            Left = left,
            Right = right,
        };
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

    private static PartitionQueryPlan ExactSalary(int salary)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = SalaryScope,
            IndexKind = SearchableIndexKind.Range,
            Value = IndexValue.Create(salary),
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
            FormatVersion = StorageLayout.MovementFormatVersion,
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
