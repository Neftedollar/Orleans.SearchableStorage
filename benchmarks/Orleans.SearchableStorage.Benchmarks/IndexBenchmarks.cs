using BenchmarkDotNet.Attributes;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

[BenchmarkCategory("Indexing", "Mutation")]
public class IndexMutationBenchmarks
{
    private StoragePartitionView _view = null!;
    private string _targetKey = string.Empty;
    private StoredRecord _firstReplacement = null!;
    private StoredRecord _secondReplacement = null!;
    private bool _useFirstReplacement;
    private StoredRecord _expectedRecord = null!;

    [Params(1_024, 65_536)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _view = new StoragePartitionView(BenchmarkData.CreateRecords(RecordCount));
        _targetKey = BenchmarkData.CreateRecordKey(RecordCount / 2);
        _firstReplacement = BenchmarkData.CreateRecord(
            RecordCount / 2,
            salary: RecordCount + 10,
            city: 201,
            etag: "replacement-a");
        _secondReplacement = BenchmarkData.CreateRecord(
            RecordCount / 2,
            salary: RecordCount + 20,
            city: 202,
            etag: "replacement-b");

        _view.ApplyUpsert(_targetKey, _firstReplacement);
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
        _view.ApplyUpsert(_targetKey, replacement);
        _expectedRecord = replacement;
        return _view.Records.Count;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public int DeleteAndRestoreIndexedRecord()
    {
        _view.ApplyDelete(_targetKey);
        _view.ApplyUpsert(_targetKey, _firstReplacement);
        _expectedRecord = _firstReplacement;
        return _view.Records.Count;
    }

    internal void ValidateFixture()
    {
        if (!_view.Records.TryGetValue(_targetKey, out var record)
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
        if (!_view.Indexes.FindHashEntries(BenchmarkData.CityScope, expectedCity).Contains(_targetKey)
            || !_view.Indexes.FindRangeEntries(BenchmarkData.SalaryScope, expectedSalary).Contains(_targetKey)
            || _view.Indexes.FindHashEntries(BenchmarkData.CityScope, alternateCity).Contains(_targetKey)
            || _view.Indexes.FindRangeEntries(BenchmarkData.SalaryScope, alternateSalary).Contains(_targetKey)
            || _view.Indexes.FindRangeEntries(
                BenchmarkData.SalaryScope,
                IndexValue.FromSignedInteger(RecordCount / 2)).Contains(_targetKey))
        {
            throw new InvalidOperationException("The indexed mutation fixture contains stale or missing index entries.");
        }
    }
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
