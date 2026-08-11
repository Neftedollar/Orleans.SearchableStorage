using BenchmarkDotNet.Attributes;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

[BenchmarkCategory("Querying", "Facets")]
public class FacetPartitionBenchmarks
{
    private const int CandidatePageSize = 16;
    private const int CountGroupsPerSlice = 16;
    private const int ResponseByteLimit = 256 * 1_024;
    private StoragePartitionView _view = null!;
    private StorageLayoutSnapshot _routing = null!;
    private byte[] _layoutFingerprint = null!;
    private byte[] _requestFingerprint = null!;
    private PartitionQueryPlan _query = null!;
    private IndexValue _countedValue = null!;
    private SortedDictionary<string, FacetOracleCount> _oracle = null!;
    private FacetBenchmarkDiagnostics _diagnostics;

    [Params(4_096, 65_536)]
    public int RecordCount { get; set; }

    [Params(FacetValueCardinality.Low8, FacetValueCardinality.High1024)]
    public FacetValueCardinality Cardinality { get; set; }

    [Params(FacetValueDistribution.Uniform, FacetValueDistribution.Skewed)]
    public FacetValueDistribution Distribution { get; set; }

    [Params(FacetPredicate.All, FacetPredicate.SelectiveRange)]
    public FacetPredicate Predicate { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var records = CreateRecords(out _oracle);
        _view = new StoragePartitionView(records);
        _routing = StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = "facet-benchmark-provider",
            PartitionCount = 1,
            VirtualSlotCount = 1,
            SlotAssignments = [0],
            Epoch = 1,
        });
        _layoutFingerprint = StorageLayoutFingerprint.Compute(_routing);
        _query = Predicate switch
        {
            FacetPredicate.All => new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.All,
            },
            FacetPredicate.SelectiveRange => new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Range,
                Scope = BenchmarkData.SalaryScope,
                IndexKind = SearchableIndexKind.Range,
                LowerBound = IndexValue.FromSignedInteger(1),
                IncludeLowerBound = true,
            },
            _ => throw new ArgumentOutOfRangeException(
                nameof(Predicate),
                Predicate,
                "Unknown facet predicate."),
        };
        _requestFingerprint = FacetQueryFingerprint.Compute(
            BenchmarkData.StateName,
            _query,
            BenchmarkData.CityScope,
            SearchableIndexKind.Hash);
        _countedValue = IndexValue.Create(FormatCity(0));

        ValidateFixture();
    }

    [Benchmark]
    public long EvaluateCandidateMetadataPage()
    {
        var result = EvaluateCandidatePage();
        return checked(
            result.Items.Sum(static item => item.RawCount)
            + result.Work.TotalOperationCount
            + result.PageRawCount
            + result.TotalRawCount);
    }

    [Benchmark]
    public long EvaluateResumableFilteredCount()
    {
        var result = EvaluateCountTraversal();
        return checked(result.Count + result.Work.TotalOperationCount + result.Rounds);
    }

    internal FacetBenchmarkDiagnostics Diagnostics => _diagnostics;

    internal void ValidateFixture()
    {
        var candidate = EvaluateCandidatePage();
        var expectedCandidateCount = Math.Min(CandidatePageSize, _oracle.Count);
        var expectedCandidates = _oracle.Take(expectedCandidateCount).ToArray();
        if (candidate.Items.Length != expectedCandidateCount
            || !candidate.Items.Select(static item => item.Value)
                .SequenceEqual(expectedCandidates.Select(static item => IndexValue.Create(item.Key)))
            || !candidate.Items.Select(static item => item.RawCount)
                .SequenceEqual(expectedCandidates.Select(static item => item.Value.RawCount)))
        {
            throw new InvalidOperationException(
                "The facet candidate page did not match the independently generated value/count oracle.");
        }

        var expectedCandidateWork = new FacetBenchmarkWorkVector(
            ValueSeekCount: 1,
            ValueVisitCount: expectedCandidateCount,
            GrainGroupVisitCount: 0,
            OwnershipProbeCount: 0,
            RecordProbeCount: 0,
            PredicateNodeProbeCount: 0,
            IndexEntryProbeCount: 0,
            CountIncrementCount: 0,
            ResultMaterializationCount: expectedCandidateCount);
        var expectedExhausted = _oracle.Count <= CandidatePageSize;
        var expectedPageRawCount = expectedCandidates.Sum(static item => item.Value.RawCount);
        var expectedUnseenBound = checked(RecordCount - expectedPageRawCount);
        if (FacetBenchmarkWorkVector.From(candidate.Work) != expectedCandidateWork
            || candidate.Exhausted != expectedExhausted
            || candidate.Exhausted == (candidate.FrontierValue is not null)
            || candidate.PageRawCount != expectedPageRawCount
            || candidate.TotalRawCount != RecordCount
            || candidate.TotalRawCount - candidate.PageRawCount != expectedUnseenBound
            || (candidate.Exhausted && expectedUnseenBound != 0)
            || (!candidate.Exhausted && expectedUnseenBound <= 0)
            || candidate.ItemByteCount <= 0
            || candidate.ItemByteCount > ResponseByteLimit)
        {
            throw new InvalidOperationException(
                "The facet candidate page violated its exact metadata-only work/bound contract.");
        }

        // This exact vector is the important regression oracle: enumerating posting members while
        // nominating values would make any group/record/predicate component non-zero.
        if (candidate.Work.GrainGroupVisitCount != 0
            || candidate.Work.OwnershipProbeCount != 0
            || candidate.Work.RecordProbeCount != 0
            || candidate.Work.PredicateNodeProbeCount != 0
            || candidate.Work.IndexEntryProbeCount != 0
            || candidate.Work.CountIncrementCount != 0)
        {
            throw new InvalidOperationException(
                "Candidate nomination performed a hidden posting scan.");
        }

        var count = EvaluateCountTraversal();
        var oracleCount = Predicate == FacetPredicate.All
            ? _oracle[FormatCity(0)].RawCount
            : _oracle[FormatCity(0)].SelectiveCount;
        var expectedRounds = checked(
            (int)((_oracle[FormatCity(0)].RawCount + CountGroupsPerSlice - 1)
                / CountGroupsPerSlice));
        var expectedCountWork = CreateExpectedCountWork(
            _oracle[FormatCity(0)].RawCount,
            oracleCount,
            expectedRounds);
        if (count.Count != oracleCount
            || count.Rounds != expectedRounds
            || count.Work != expectedCountWork
            || !count.Exhausted)
        {
            throw new InvalidOperationException(
                "The resumable filtered count did not match its independent count/round/work oracle.");
        }

        if (_oracle[FormatCity(0)].RawCount > CountGroupsPerSlice && count.Rounds <= 1)
        {
            throw new InvalidOperationException(
                "The count fixture which exceeds one slice did not exercise resumption.");
        }

        var repeatedCandidate = EvaluateCandidatePage();
        var repeatedCount = EvaluateCountTraversal();
        var repeatedDiagnostics = CreateDiagnostics(repeatedCandidate, repeatedCount);
        _diagnostics = CreateDiagnostics(candidate, count);
        if (repeatedDiagnostics != _diagnostics)
        {
            throw new InvalidOperationException(
                "The facet fixture did not reproduce its exact result and work vectors.");
        }
    }

    private PartitionFacetCandidatePageResult EvaluateCandidatePage()
    {
        return StoragePartitionFacetEvaluator.EvaluateCandidatePageValidated(
            new RoutedPartitionFacetCandidatePageRequest
            {
                Query = _query,
                FacetScope = BenchmarkData.CityScope,
                FacetKind = SearchableIndexKind.Hash,
                Epoch = _routing.Epoch,
                WorkBudget = 1 + (2L * CandidatePageSize),
                ItemLimit = CandidatePageSize,
                ByteLimit = ResponseByteLimit,
                ProtocolVersion = QueryProtocol.PagingVersion,
                OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
                WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
                ResponseFamily = PartitionQueryResponseFamily.FacetValueCountCandidates,
                RequestFingerprint = _requestFingerprint,
                LayoutFormatVersion = _routing.FormatVersion,
                LayoutFingerprint = _layoutFingerprint,
                StateName = BenchmarkData.StateName,
            },
            _view,
            _routing,
            _requestFingerprint,
            _layoutFingerprint);
    }

    private FacetCountTraversalResult EvaluateCountTraversal()
    {
        var hasAfter = false;
        var after = default(GrainId);
        var aggregate = default(FacetBenchmarkWorkVector);
        long count = 0;
        var workBudget = Predicate switch
        {
            FacetPredicate.All => 1L + (CountGroupsPerSlice * 5L),
            FacetPredicate.SelectiveRange => 1L + (CountGroupsPerSlice / 2 * 13L),
            _ => throw new InvalidOperationException($"Unknown facet predicate '{Predicate}'."),
        };

        for (var round = 1; round <= SearchableStorageQueryOptions.MaximumFacetRounds; round++)
        {
            var result = StoragePartitionFacetEvaluator.EvaluateCountSliceValidated(
                new RoutedPartitionFacetCountSliceRequest
                {
                    Query = _query,
                    FacetScope = BenchmarkData.CityScope,
                    FacetKind = SearchableIndexKind.Hash,
                    Value = _countedValue,
                    Epoch = _routing.Epoch,
                    HasAfter = hasAfter,
                    After = after,
                    WorkBudget = workBudget,
                    ProtocolVersion = QueryProtocol.PagingVersion,
                    OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
                    WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
                    ResponseFamily = PartitionQueryResponseFamily.FacetValueCountProbe,
                    RequestFingerprint = _requestFingerprint,
                    LayoutFormatVersion = _routing.FormatVersion,
                    LayoutFingerprint = _layoutFingerprint,
                    StateName = BenchmarkData.StateName,
                },
                _view,
                _routing,
                partitionIndex: 0,
                _requestFingerprint,
                _layoutFingerprint);
            count = checked(count + result.CountDelta);
            aggregate = aggregate.Add(FacetBenchmarkWorkVector.From(result.Work));
            if (result.Exhausted)
            {
                return new FacetCountTraversalResult(
                    count,
                    round,
                    Exhausted: true,
                    aggregate);
            }

            if (!result.HasFrontier
                || (hasAfter && GrainIdCanonicalOrder.Compare(result.Frontier, after) <= 0))
            {
                throw new InvalidOperationException(
                    "The facet count slice did not advance its canonical GrainId frontier.");
            }

            hasAfter = true;
            after = result.Frontier;
        }

        throw new InvalidOperationException("The facet count traversal exceeded its hard round ceiling.");
    }

    private Dictionary<string, StoredRecord> CreateRecords(
        out SortedDictionary<string, FacetOracleCount> oracle)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RecordCount);
        var cardinality = Cardinality switch
        {
            FacetValueCardinality.Low8 => 8,
            FacetValueCardinality.High1024 => 1_024,
            _ => throw new InvalidOperationException($"Unknown facet cardinality '{Cardinality}'."),
        };
        if (RecordCount < cardinality || RecordCount % cardinality != 0)
        {
            throw new InvalidOperationException(
                "The facet benchmark requires a record count divisible by its value cardinality.");
        }

        oracle = new SortedDictionary<string, FacetOracleCount>(StringComparer.Ordinal);
        var records = new Dictionary<string, StoredRecord>(RecordCount, StringComparer.Ordinal);
        var occurrences = new int[cardinality];
        for (var index = 0; index < RecordCount; index++)
        {
            var city = Distribution switch
            {
                FacetValueDistribution.Uniform => index % cardinality,
                FacetValueDistribution.Skewed when index < RecordCount / 2 => 0,
                FacetValueDistribution.Skewed => 1 + (index % (cardinality - 1)),
                _ => throw new InvalidOperationException(
                    $"Unknown facet distribution '{Distribution}'."),
            };
            var occurrence = occurrences[city]++;
            var selectiveMatch = (occurrence & 1) == 1;
            var grainId = BenchmarkData.CreateGrainId(index);
            var record = BenchmarkData.CreateRecord(
                index,
                salary: selectiveMatch ? 1 : 0,
                city: city,
                grainId: grainId);
            records.Add(
                BenchmarkData.CreateStoredRecordKey(BenchmarkData.StateName, grainId),
                record);

            var value = FormatCity(city);
            oracle.TryGetValue(value, out var previous);
            oracle[value] = new FacetOracleCount(
                checked(previous.RawCount + 1),
                checked(previous.SelectiveCount + (selectiveMatch ? 1 : 0)));
        }

        return records;
    }

    private FacetBenchmarkWorkVector CreateExpectedCountWork(
        long rawCount,
        long exactCount,
        int rounds)
    {
        return Predicate switch
        {
            FacetPredicate.All => new FacetBenchmarkWorkVector(
                ValueSeekCount: rounds,
                ValueVisitCount: 0,
                GrainGroupVisitCount: rawCount,
                OwnershipProbeCount: rawCount,
                RecordProbeCount: rawCount,
                PredicateNodeProbeCount: rawCount,
                IndexEntryProbeCount: 0,
                CountIncrementCount: rawCount,
                ResultMaterializationCount: 0),
            FacetPredicate.SelectiveRange => new FacetBenchmarkWorkVector(
                ValueSeekCount: rounds,
                ValueVisitCount: 0,
                GrainGroupVisitCount: rawCount,
                OwnershipProbeCount: rawCount,
                RecordProbeCount: rawCount,
                PredicateNodeProbeCount: rawCount,
                IndexEntryProbeCount: checked(rawCount * 2),
                CountIncrementCount: exactCount,
                ResultMaterializationCount: 0),
            _ => throw new InvalidOperationException($"Unknown facet predicate '{Predicate}'."),
        };
    }

    private FacetBenchmarkDiagnostics CreateDiagnostics(
        PartitionFacetCandidatePageResult candidate,
        FacetCountTraversalResult count) => new(
        RecordCount,
        Cardinality,
        Distribution,
        Predicate,
        candidate.Items.Length,
        candidate.Exhausted,
        candidate.PageRawCount,
        candidate.TotalRawCount,
        checked(candidate.TotalRawCount - candidate.PageRawCount),
        FacetBenchmarkWorkVector.From(candidate.Work),
        count.Count,
        count.Rounds,
        count.Work);

    private static string FormatCity(int value) => $"city-{value:D3}";
}

public enum FacetValueCardinality
{
    Low8,
    High1024,
}

public enum FacetValueDistribution
{
    Uniform,
    Skewed,
}

public enum FacetPredicate
{
    All,
    SelectiveRange,
}

internal readonly record struct FacetOracleCount(long RawCount, long SelectiveCount);

internal readonly record struct FacetCountTraversalResult(
    long Count,
    int Rounds,
    bool Exhausted,
    FacetBenchmarkWorkVector Work);

internal readonly record struct FacetBenchmarkDiagnostics(
    int RecordCount,
    FacetValueCardinality Cardinality,
    FacetValueDistribution Distribution,
    FacetPredicate Predicate,
    int CandidateCount,
    bool CandidateExhausted,
    long PageRawCount,
    long TotalRawCount,
    long RemainingRawCount,
    FacetBenchmarkWorkVector CandidateWork,
    long ExactCount,
    int CountRounds,
    FacetBenchmarkWorkVector CountWork);

internal readonly record struct FacetBenchmarkWorkVector(
    long ValueSeekCount,
    long ValueVisitCount,
    long GrainGroupVisitCount,
    long OwnershipProbeCount,
    long RecordProbeCount,
    long PredicateNodeProbeCount,
    long IndexEntryProbeCount,
    long CountIncrementCount,
    long ResultMaterializationCount)
{
    public long TotalOperationCount => checked(
        ValueSeekCount + ValueVisitCount + GrainGroupVisitCount + OwnershipProbeCount
        + RecordProbeCount + PredicateNodeProbeCount + IndexEntryProbeCount
        + CountIncrementCount + ResultMaterializationCount);

    public FacetBenchmarkWorkVector Add(FacetBenchmarkWorkVector other) => new(
        checked(ValueSeekCount + other.ValueSeekCount),
        checked(ValueVisitCount + other.ValueVisitCount),
        checked(GrainGroupVisitCount + other.GrainGroupVisitCount),
        checked(OwnershipProbeCount + other.OwnershipProbeCount),
        checked(RecordProbeCount + other.RecordProbeCount),
        checked(PredicateNodeProbeCount + other.PredicateNodeProbeCount),
        checked(IndexEntryProbeCount + other.IndexEntryProbeCount),
        checked(CountIncrementCount + other.CountIncrementCount),
        checked(ResultMaterializationCount + other.ResultMaterializationCount));

    public static FacetBenchmarkWorkVector From(PartitionFacetWork work) => new(
        work.ValueSeekCount,
        work.ValueVisitCount,
        work.GrainGroupVisitCount,
        work.OwnershipProbeCount,
        work.RecordProbeCount,
        work.PredicateNodeProbeCount,
        work.IndexEntryProbeCount,
        work.CountIncrementCount,
        work.ResultMaterializationCount);
}
