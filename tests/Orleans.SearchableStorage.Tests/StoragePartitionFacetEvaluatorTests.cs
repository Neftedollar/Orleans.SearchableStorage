using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePartitionFacetEvaluatorTests
{
    private const string StateName = "facet-evaluator";
    private const string ValueScope = "facet/value";
    private const string IncludedScope = "facet/included";

    [Fact]
    public void FacetRequestValueValidationNormalizesInvalidUtf16()
    {
        var value = new IndexValue
        {
            Kind = IndexValueKind.String,
            Text = "\ud800",
        };

        Action validate = () => StoragePartitionGrain.ValidateFacetIndexValue(value, new object());

        validate.Should().Throw<ArgumentException>()
            .WithMessage("*invalid canonical index value*");
    }

    [Fact]
    public void CandidatePagesAreCanonicalMetadataOnlyAndCertifyRawPageAndTotalCounts()
    {
        var layout = CreateLayout(1, [0]);
        var view = CreateView(
            Record(OwnedId(layout, 0, "c-0"), "c", included: true),
            Record(OwnedId(layout, 0, "a-0"), "a", included: true),
            Record(OwnedId(layout, 0, "b-0"), "b", included: false),
            Record(OwnedId(layout, 0, "b-1"), "b", included: true));
        var query = All();

        var first = EvaluateCandidates(view, layout, query, itemLimit: 2);
        var second = EvaluateCandidates(
            view,
            layout,
            query,
            itemLimit: 2,
            after: first.FrontierValue);

        first.Items.Select(static item => (item.Value.Text, item.RawCount))
            .Should().Equal(("a", 1L), ("b", 2L));
        first.Exhausted.Should().BeFalse();
        first.FrontierValue!.Text.Should().Be("b");
        first.PageRawCount.Should().Be(3);
        first.TotalRawCount.Should().Be(4);
        first.Work.RecordProbeCount.Should().Be(0);
        first.Work.PredicateNodeProbeCount.Should().Be(0);
        first.Work.OwnershipProbeCount.Should().Be(0);
        first.Work.CountIncrementCount.Should().Be(0);
        second.Items.Should().ContainSingle();
        second.Items[0].Value.Text.Should().Be("c");
        second.PageRawCount.Should().Be(1);
        second.TotalRawCount.Should().Be(4);
        second.Exhausted.Should().BeTrue();
    }

    [Fact]
    public void DistinctPageExcludesNullByConstructionAndUsesExactValueFrontier()
    {
        var layout = CreateLayout(1, [0]);
        var nullRecord = Record(OwnedId(layout, 0, "null"), value: null, included: true);
        var view = CreateView(
            nullRecord,
            Record(OwnedId(layout, 0, "b"), "b", included: true),
            Record(OwnedId(layout, 0, "a"), "a", included: true));

        var first = EvaluateDistinct(view, layout, itemLimit: 1);
        var second = EvaluateDistinct(view, layout, itemLimit: 1, after: first.Frontier);

        first.Items.Select(static value => value.Text).Should().Equal("a");
        first.Frontier.Should().Be(first.Items[^1]);
        first.Exhausted.Should().BeFalse();
        second.Items.Select(static value => value.Text).Should().Equal("b");
        second.Exhausted.Should().BeTrue();
    }

    [Fact]
    public void CountSlicesResumeByGrainIdAndFilterPredicateAndRoutingOwnershipExactly()
    {
        var layout = CreateLayout(1, [0, 1]);
        var first = OwnedId(layout, 0, "owned-a");
        var second = OwnedId(layout, 0, "owned-b");
        var filtered = OwnedId(layout, 0, "owned-filtered");
        var copiedFromOtherOwner = OwnedId(layout, 1, "copied");
        var view = CreateView(
            Record(first, "x", included: true),
            Record(second, "x", included: true),
            Record(filtered, "x", included: false),
            Record(copiedFromOtherOwner, "x", included: true));
        var query = ExactIncluded(true);
        var slices = new List<PartitionFacetCountSliceResult>();
        var hasAfter = false;
        var after = default(GrainId);
        do
        {
            var slice = EvaluateCount(
                view,
                layout,
                partitionIndex: 0,
                query,
                IndexValue.Create("x"),
                workBudget: 9,
                hasAfter,
                after);
            slices.Add(slice);
            hasAfter = slice.HasFrontier;
            after = slice.Frontier;
        }
        while (!slices[^1].Exhausted);

        slices.Should().HaveCountGreaterThan(1);
        slices.Sum(static slice => slice.CountDelta).Should().Be(2);
        slices.Where(static slice => !slice.Exhausted)
            .Should().OnlyContain(static slice => slice.HasFrontier && !slice.Frontier.IsDefault);
        slices[^1].HasFrontier.Should().BeFalse();
    }

    [Fact]
    public void AtomicFirstGrainGroupWhichCannotFitFailsOnceWithDeterministicMinimum()
    {
        var layout = CreateLayout(1, [0]);
        var grainId = OwnedId(layout, 0, "duplicate");
        var first = Record(grainId, "x", included: true);
        var second = Record(grainId, "x", included: true);
        var view = new StoragePartitionView(new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["first"] = first.Record,
            ["second"] = second.Record,
        });

        Action evaluate = () => _ = EvaluateCount(
            view,
            layout,
            partitionIndex: 0,
            All(),
            IndexValue.Create("x"),
            workBudget: 8,
            hasAfter: false,
            after: default);

        var failure = evaluate.Should().Throw<PartitionQueryBudgetTooSmallException>().Which;
        failure.RequestedLimit.Should().Be(8);
        failure.MinimumRequired.Should().Be(9);
        failure.Reason.Should().Be(PartitionQueryPageStopReason.WorkBudget);
    }

    [Fact]
    public void OversizedGroupAfterCommittedGroupReturnsProgressThenFailsOnExactResume()
    {
        var layout = CreateLayout(1, [0]);
        var ids = new[]
        {
            OwnedId(layout, 0, "atomic-a"),
            OwnedId(layout, 0, "atomic-z"),
        }.Order(GrainIdCanonicalOrder.Comparer).ToArray();
        var small = Record(ids[0], "x", included: true);
        var largeFirst = Record(ids[1], "x", included: true);
        var largeSecond = Record(ids[1], "x", included: true);
        var view = new StoragePartitionView(new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["small"] = small.Record,
            ["large-1"] = largeFirst.Record,
            ["large-2"] = largeSecond.Record,
        });

        var first = EvaluateCount(
            view,
            layout,
            partitionIndex: 0,
            All(),
            IndexValue.Create("x"),
            workBudget: 6,
            hasAfter: false,
            after: default);
        Action resume = () => _ = EvaluateCount(
            view,
            layout,
            partitionIndex: 0,
            All(),
            IndexValue.Create("x"),
            workBudget: 8,
            hasAfter: true,
            after: first.Frontier);

        first.Exhausted.Should().BeFalse();
        first.CountDelta.Should().Be(1);
        first.Frontier.Should().Be(ids[0]);
        resume.Should().Throw<PartitionQueryBudgetTooSmallException>()
            .Which.MinimumRequired.Should().Be(9);
    }

    [Fact]
    public void CandidateSingleValueWhichCannotFitByteLimitFailsWithoutAnUnresumablePage()
    {
        var layout = CreateLayout(1, [0]);
        var view = CreateView(Record(OwnedId(layout, 0, "large"), "large-value", included: true));
        var required = IndexValueCanonicalEncoding.GetEncodedLength(IndexValue.Create("large-value"))
            + sizeof(long);

        Action evaluate = () => _ = EvaluateCandidates(
            view,
            layout,
            All(),
            itemLimit: 1,
            byteLimit: required - 1);

        evaluate.Should().Throw<PartitionQueryBudgetTooSmallException>()
            .Which.Reason.Should().Be(PartitionQueryPageStopReason.ByteLimit);
    }

    [Fact]
    public void UnsupportedStoredTextFailsDistinctAndCandidateTurnsWithoutAResponse()
    {
        var layout = CreateLayout(1, [0]);
        var unsupported = new[]
        {
            new string('x', IndexValueCanonicalEncoding.MaximumTextBytes + 1),
            "\ud800",
        };

        foreach (var text in unsupported)
        {
            var view = CreateView(Record(OwnedId(layout, 0, "unsupported"), text, included: true));

            Action distinct = () => _ = EvaluateDistinct(view, layout, itemLimit: 1);
            Action candidate = () => _ = EvaluateCandidates(view, layout, All(), itemLimit: 1);

            distinct.Should().Throw<StorageFacetValueUnsupportedException>();
            candidate.Should().Throw<StorageFacetValueUnsupportedException>();
        }
    }

    private static PartitionDistinctFacetPageResult EvaluateDistinct(
        StoragePartitionView view,
        StorageLayoutSnapshot layout,
        int itemLimit,
        IndexValue? after = null)
    {
        var query = All();
        var request = new RoutedPartitionDistinctFacetPageRequest
        {
            Query = query,
            FacetScope = ValueScope,
            FacetKind = SearchableIndexKind.Hash,
            Epoch = layout.Epoch,
            After = after,
            WorkBudget = 1_000,
            ItemLimit = itemLimit,
            ByteLimit = 100_000,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
            WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.DistinctFacetValuePage,
            RequestFingerprint = FacetQueryFingerprint.Compute(
                StateName, query, ValueScope, SearchableIndexKind.Hash),
            LayoutFormatVersion = layout.FormatVersion,
            LayoutFingerprint = StorageLayoutFingerprint.Compute(layout),
            StateName = StateName,
        };
        return StoragePartitionFacetEvaluator.EvaluateDistinctPageValidated(
            request,
            view,
            layout,
            request.RequestFingerprint,
            request.LayoutFingerprint);
    }

    private static PartitionFacetCandidatePageResult EvaluateCandidates(
        StoragePartitionView view,
        StorageLayoutSnapshot layout,
        PartitionQueryPlan query,
        int itemLimit,
        IndexValue? after = null,
        int byteLimit = 100_000)
    {
        var request = new RoutedPartitionFacetCandidatePageRequest
        {
            Query = query,
            FacetScope = ValueScope,
            FacetKind = SearchableIndexKind.Hash,
            Epoch = layout.Epoch,
            AfterValue = after,
            WorkBudget = 1_000,
            ItemLimit = itemLimit,
            ByteLimit = byteLimit,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
            WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.FacetValueCountCandidates,
            RequestFingerprint = FacetQueryFingerprint.Compute(
                StateName, query, ValueScope, SearchableIndexKind.Hash),
            LayoutFormatVersion = layout.FormatVersion,
            LayoutFingerprint = StorageLayoutFingerprint.Compute(layout),
            StateName = StateName,
        };
        return StoragePartitionFacetEvaluator.EvaluateCandidatePageValidated(
            request,
            view,
            layout,
            request.RequestFingerprint,
            request.LayoutFingerprint);
    }

    private static PartitionFacetCountSliceResult EvaluateCount(
        StoragePartitionView view,
        StorageLayoutSnapshot layout,
        int partitionIndex,
        PartitionQueryPlan query,
        IndexValue value,
        long workBudget,
        bool hasAfter,
        GrainId after)
    {
        var request = new RoutedPartitionFacetCountSliceRequest
        {
            Query = query,
            FacetScope = ValueScope,
            FacetKind = SearchableIndexKind.Hash,
            Value = value,
            Epoch = layout.Epoch,
            HasAfter = hasAfter,
            After = after,
            WorkBudget = workBudget,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
            WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.FacetValueCountProbe,
            RequestFingerprint = FacetQueryFingerprint.Compute(
                StateName, query, ValueScope, SearchableIndexKind.Hash),
            LayoutFormatVersion = layout.FormatVersion,
            LayoutFingerprint = StorageLayoutFingerprint.Compute(layout),
            StateName = StateName,
        };
        return StoragePartitionFacetEvaluator.EvaluateCountSliceValidated(
            request,
            view,
            layout,
            partitionIndex,
            request.RequestFingerprint,
            request.LayoutFingerprint);
    }

    private static StoragePartitionView CreateView(
        params (string RecordKey, StoredRecord Record)[] records)
    {
        return new StoragePartitionView(records.ToDictionary(
            static pair => pair.RecordKey,
            static pair => pair.Record,
            StringComparer.Ordinal));
    }

    private static (string RecordKey, StoredRecord Record) Record(
        GrainId grainId,
        string? value,
        bool included)
    {
        var entries = new List<IndexEntry>();
        if (value is not null)
        {
            entries.Add(new IndexEntry
            {
                Scope = ValueScope,
                Kind = SearchableIndexKind.Hash,
                Value = IndexValue.Create(value),
            });
        }

        entries.Add(new IndexEntry
        {
            Scope = IncludedScope,
            Kind = SearchableIndexKind.Hash,
            Value = IndexValue.Create(included),
        });
        return ($"record-{Guid.NewGuid():N}", new StoredRecord
        {
            GrainId = grainId,
            Payload = [],
            ETag = "1",
            IndexEntries = [.. entries],
        });
    }

    private static PartitionQueryPlan All() => new() { Operation = PartitionQueryOperation.All };

    private static PartitionQueryPlan ExactIncluded(bool value) => new()
    {
        Operation = PartitionQueryOperation.Exact,
        Scope = IncludedScope,
        IndexKind = SearchableIndexKind.Hash,
        Value = IndexValue.Create(value),
    };

    private static StorageLayoutSnapshot CreateLayout(long epoch, int[] owners)
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = "facet-evaluator",
            PartitionCount = owners.Distinct().Count(),
            VirtualSlotCount = owners.Length,
            SlotAssignments = owners,
            Epoch = epoch,
        });
    }

    private static GrainId OwnedId(StorageLayoutSnapshot layout, int owner, string seed)
    {
        for (var attempt = 0; ; attempt++)
        {
            var grainId = GrainId.Create("facet", $"{seed}-{attempt}");
            if (layout.GetOwner(StorageLayout.GetSlot(grainId, layout.VirtualSlotCount)) == owner)
            {
                return grainId;
            }
        }
    }
}
