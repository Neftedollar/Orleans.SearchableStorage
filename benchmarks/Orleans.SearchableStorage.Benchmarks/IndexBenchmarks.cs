using BenchmarkDotNet.Attributes;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

[BenchmarkCategory("Indexing", "Mutation")]
public class IndexMutationBenchmarks
{
    private Dictionary<string, StoredRecord> _records = null!;
    private StoragePartitionIndexes _materializingIndexes = null!;
    private StoragePartitionView _view = null!;
    private string _targetKey = string.Empty;
    private StoredRecord _firstReplacement = null!;
    private StoredRecord _secondReplacement = null!;
    private StoredRecord _originalRecord = null!;
    private bool _useFirstReplacement;
    private StoredRecord _expectedRecord = null!;

    [Params(1_024, 65_536)]
    public int RecordCount { get; set; }

    [Params(DerivedIndexRepresentation.MaterializingHashSets, DerivedIndexRepresentation.BoundedOrderedView)]
    public DerivedIndexRepresentation Representation { get; set; }

    [Params(BenchmarkIndexDistribution.UniformUniqueRange, BenchmarkIndexDistribution.HotLowCardinality)]
    public BenchmarkIndexDistribution Distribution { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var records = BenchmarkData.CreateProductionRecords(RecordCount, Distribution);
        switch (Representation)
        {
            case DerivedIndexRepresentation.MaterializingHashSets:
                _records = records;
                _materializingIndexes = StoragePartitionIndexes.Build(records);
                break;
            case DerivedIndexRepresentation.BoundedOrderedView:
                _view = new StoragePartitionView(records);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(Representation),
                    Representation,
                    null);
        }

        var targetGrainId = BenchmarkData.CreateGrainId(RecordCount / 2);
        _targetKey = BenchmarkData.CreateStoredRecordKey(BenchmarkData.StateName, targetGrainId);
        _originalRecord = GetRecords()[_targetKey];
        _firstReplacement = BenchmarkData.CreateRecord(
            RecordCount / 2,
            salary: RecordCount + 10,
            city: 201,
            etag: "replacement-a",
            grainId: targetGrainId);
        _secondReplacement = BenchmarkData.CreateRecord(
            RecordCount / 2,
            salary: RecordCount + 20,
            city: 202,
            etag: "replacement-b",
            grainId: targetGrainId);

        ApplyUpsert(_targetKey, _firstReplacement);
        _expectedRecord = _firstReplacement;
        _useFirstReplacement = false;
        ValidateFixture();
    }

    [Benchmark]
    public int ReplaceIndexedRecord()
    {
        var replacement = _useFirstReplacement
            ? _firstReplacement
            : _secondReplacement;
        _useFirstReplacement = !_useFirstReplacement;
        ApplyUpsert(_targetKey, replacement);
        _expectedRecord = replacement;
        return GetRecords().Count;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public int DeleteAndRestoreIndexedRecord()
    {
        ApplyDelete(_targetKey);
        ApplyUpsert(_targetKey, _firstReplacement);
        _expectedRecord = _firstReplacement;
        return GetRecords().Count;
    }

    [GlobalCleanup]
    public void GlobalCleanup() => ValidateFixture();

    internal void ValidateFixture()
    {
        var records = GetRecords();
        var indexes = GetIndexes();
        if (!records.TryGetValue(_targetKey, out var record)
            || !string.Equals(record.ETag, _expectedRecord.ETag, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The indexed mutation fixture did not retain the expected record.");
        }

        var expectedCity = _expectedRecord.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Hash).Value;
        var expectedSalary = _expectedRecord.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Range).Value;
        var alternate = ReferenceEquals(_expectedRecord, _firstReplacement)
            ? _secondReplacement
            : _firstReplacement;
        var alternateCity = alternate.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Hash).Value;
        var alternateSalary = alternate.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Range).Value;
        var originalCity = _originalRecord.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Hash).Value;
        var originalSalary = _originalRecord.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Range).Value;
        if (!indexes.FindHashEntries(BenchmarkData.CityScope, expectedCity).Contains(_targetKey)
            || !indexes.FindRangeEntries(BenchmarkData.SalaryScope, expectedSalary).Contains(_targetKey)
            || indexes.FindHashEntries(BenchmarkData.CityScope, alternateCity).Contains(_targetKey)
            || indexes.FindRangeEntries(BenchmarkData.SalaryScope, alternateSalary).Contains(_targetKey)
            || indexes.FindHashEntries(BenchmarkData.CityScope, originalCity).Contains(_targetKey)
            || indexes.FindRangeEntries(BenchmarkData.SalaryScope, originalSalary).Contains(_targetKey))
        {
            throw new InvalidOperationException(
                "The indexed mutation fixture contains stale or missing index entries.");
        }

        if (Representation == DerivedIndexRepresentation.BoundedOrderedView)
        {
            var catalog = _view.OrderedIndexes.GetStateCatalog(BenchmarkData.StateName);
            if (!catalog.TryGetRecordKeys(record.GrainId, out var orderedRecordKeys))
            {
                throw new InvalidOperationException(
                    "The indexed mutation fixture omitted the production-shaped ordered catalog entry.");
            }

            if (!orderedRecordKeys.Contains(_targetKey, StringComparer.Ordinal))
            {
                throw new InvalidOperationException(
                    "The indexed mutation fixture retained the wrong production-shaped catalog key.");
            }

            if (!PostingContains(
                    _view.OrderedIndexes.GetExactPosting(
                        BenchmarkData.CityScope,
                        SearchableIndexKind.Hash,
                        expectedCity),
                    record.GrainId,
                    _targetKey)
                || !PostingContains(
                    _view.OrderedIndexes.GetExactPosting(
                        BenchmarkData.SalaryScope,
                        SearchableIndexKind.Range,
                        expectedSalary),
                    record.GrainId,
                    _targetKey)
                || PostingContains(
                    _view.OrderedIndexes.GetExactPosting(
                        BenchmarkData.CityScope,
                        SearchableIndexKind.Hash,
                        alternateCity),
                    record.GrainId,
                    _targetKey)
                || PostingContains(
                    _view.OrderedIndexes.GetExactPosting(
                        BenchmarkData.SalaryScope,
                        SearchableIndexKind.Range,
                        alternateSalary),
                    record.GrainId,
                    _targetKey)
                || PostingContains(
                    _view.OrderedIndexes.GetExactPosting(
                        BenchmarkData.CityScope,
                        SearchableIndexKind.Hash,
                        originalCity),
                    record.GrainId,
                    _targetKey)
                || PostingContains(
                    _view.OrderedIndexes.GetExactPosting(
                        BenchmarkData.SalaryScope,
                        SearchableIndexKind.Range,
                        originalSalary),
                    record.GrainId,
                    _targetKey))
            {
                throw new InvalidOperationException(
                    "The indexed mutation fixture contains stale or missing ordered postings.");
            }
        }
    }

    private static bool PostingContains(
        OrderedGrainGroups posting,
        Orleans.Runtime.GrainId grainId,
        string recordKey) =>
        posting.TryGetRecordKeys(grainId, out var recordKeys)
        && recordKeys.Contains(recordKey, StringComparer.Ordinal);

    private Dictionary<string, StoredRecord> GetRecords() =>
        Representation == DerivedIndexRepresentation.MaterializingHashSets
            ? _records
            : _view.Records;

    private StoragePartitionIndexes GetIndexes() =>
        Representation == DerivedIndexRepresentation.MaterializingHashSets
            ? _materializingIndexes
            : _view.Indexes;

    private void ApplyUpsert(string recordKey, StoredRecord record)
    {
        if (Representation == DerivedIndexRepresentation.BoundedOrderedView)
        {
            _view.ApplyUpsert(recordKey, record);
            return;
        }

        if (_records.TryGetValue(recordKey, out var current))
        {
            _materializingIndexes.RemoveRecord(recordKey, current);
        }

        _materializingIndexes.AddRecord(recordKey, record);
        _records[recordKey] = record;
    }

    private void ApplyDelete(string recordKey)
    {
        if (Representation == DerivedIndexRepresentation.BoundedOrderedView)
        {
            _view.ApplyDelete(recordKey);
            return;
        }

        if (!_records.TryGetValue(recordKey, out var current))
        {
            return;
        }

        _materializingIndexes.RemoveRecord(recordKey, current);
        _records.Remove(recordKey);
    }
}

[BenchmarkCategory("Indexing", "Activation")]
public class DerivedIndexBuildBenchmarks
{
    private Dictionary<string, StoredRecord> _records = null!;

    [Params(4_096, 65_536)]
    public int RecordCount { get; set; }

    [Params(DerivedIndexRepresentation.MaterializingHashSets, DerivedIndexRepresentation.BoundedOrderedView)]
    public DerivedIndexRepresentation Representation { get; set; }

    [Params(BenchmarkIndexDistribution.UniformUniqueRange, BenchmarkIndexDistribution.HotLowCardinality)]
    public BenchmarkIndexDistribution Distribution { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _records = BenchmarkData.CreateProductionRecords(RecordCount, Distribution);

        ValidateFixture(BuildDerivedIndexes());
    }

    [Benchmark]
    public object BuildDerivedIndexes() => Representation switch
    {
        DerivedIndexRepresentation.MaterializingHashSets => StoragePartitionIndexes.Build(_records),
        DerivedIndexRepresentation.BoundedOrderedView => new StoragePartitionView(_records),
        _ => throw new ArgumentOutOfRangeException(nameof(Representation), Representation, null),
    };

    internal void ValidateFixture(object result)
    {
        var targetGrainId = BenchmarkData.CreateGrainId(RecordCount / 2);
        var targetKey = BenchmarkData.CreateStoredRecordKey(BenchmarkData.StateName, targetGrainId);
        var targetRecord = _records[targetKey];
        var expectedCity = targetRecord.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Hash).Value;
        var expectedSalary = targetRecord.IndexEntries.Single(
            static entry => entry.Kind == SearchableIndexKind.Range).Value;
        var indexes = result switch
        {
            StoragePartitionIndexes materializing => materializing,
            StoragePartitionView bounded => bounded.Indexes,
            _ => throw new InvalidOperationException(
                "The derived-index build benchmark returned an unexpected representation."),
        };

        if (!indexes.FindHashEntries(BenchmarkData.CityScope, expectedCity).Contains(targetKey)
            || !indexes.FindRangeEntries(BenchmarkData.SalaryScope, expectedSalary).Contains(targetKey))
        {
            throw new InvalidOperationException(
                "The derived-index build benchmark omitted an expected posting.");
        }

        if (result is StoragePartitionView view)
        {
            var catalog = view.OrderedIndexes.GetStateCatalog(BenchmarkData.StateName);
            var orderedCity = view.OrderedIndexes.GetExactPosting(
                BenchmarkData.CityScope,
                SearchableIndexKind.Hash,
                expectedCity);
            var orderedSalary = view.OrderedIndexes.GetExactPosting(
                BenchmarkData.SalaryScope,
                SearchableIndexKind.Range,
                expectedSalary);
            if (catalog.CopyGrainIds().Length != RecordCount
                || !PostingContains(catalog, targetGrainId, targetKey)
                || !PostingContains(orderedCity, targetGrainId, targetKey)
                || !PostingContains(orderedSalary, targetGrainId, targetKey))
            {
                throw new InvalidOperationException(
                    "The bounded derived-index build benchmark omitted ordered catalog/posting entries.");
            }
        }
    }

    private static bool PostingContains(
        OrderedGrainGroups posting,
        Orleans.Runtime.GrainId grainId,
        string recordKey) =>
        posting.TryGetRecordKeys(grainId, out var recordKeys)
        && recordKeys.Contains(recordKey, StringComparer.Ordinal);
}

public enum DerivedIndexRepresentation
{
    MaterializingHashSets,
    BoundedOrderedView,
}

[BenchmarkCategory("Indexing", "Query")]
public class ExactRangeLookupBenchmarks
{
    private StoragePartitionIndexes _indexes = null!;
    private IndexValue _exactValue = null!;

    [Params(4_096, 65_536)]
    public int BucketCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _indexes = StoragePartitionIndexes.Build(BenchmarkData.CreateRecords(BucketCount));
        _exactValue = IndexValue.FromSignedInteger(BucketCount / 2);
    }

    [Benchmark]
    public int ExactRangeValueLookup() =>
        _indexes.FindRangeEntries(BenchmarkData.SalaryScope, _exactValue).Count;
}

[BenchmarkCategory("Indexing", "Query")]
public class RangeQueryBenchmarks
{
    private StoragePartitionIndexes _indexes = null!;
    private IndexValue _lowerBound = null!;
    private IndexValue _upperBound = null!;

    [Params(4_096, 65_536)]
    public int BucketCount { get; set; }

    [Params(1, 256)]
    public int MatchCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        if (MatchCount > BucketCount)
        {
            throw new InvalidOperationException("The requested range window exceeds the dataset.");
        }

        _indexes = StoragePartitionIndexes.Build(BenchmarkData.CreateRecords(BucketCount));
        var first = (BucketCount - MatchCount) / 2;
        _lowerBound = IndexValue.FromSignedInteger(first);
        _upperBound = IndexValue.FromSignedInteger(checked(first + MatchCount - 1));

        if (BoundedRangeQuery() != MatchCount)
        {
            throw new InvalidOperationException("The deterministic range fixture is inconsistent.");
        }
    }

    [Benchmark]
    public int BoundedRangeQuery()
    {
        var destination = new HashSet<string>(StringComparer.Ordinal);
        _indexes.UnionRange(
            BenchmarkData.SalaryScope,
            _lowerBound,
            _upperBound,
            includeLowerBound: true,
            includeUpperBound: true,
            destination);
        return destination.Count;
    }
}
