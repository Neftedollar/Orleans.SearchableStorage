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
        if (Representation == DerivedIndexRepresentation.MaterializingHashSets
            && (!_materializingIndexes.FindHashEntries(BenchmarkData.CityScope, expectedCity)
                    .Contains(_targetKey)
                || !_materializingIndexes.FindRangeEntries(BenchmarkData.SalaryScope, expectedSalary)
                    .Contains(_targetKey)
                || _materializingIndexes.FindHashEntries(BenchmarkData.CityScope, alternateCity)
                    .Contains(_targetKey)
                || _materializingIndexes.FindRangeEntries(BenchmarkData.SalaryScope, alternateSalary)
                    .Contains(_targetKey)
                || _materializingIndexes.FindHashEntries(BenchmarkData.CityScope, originalCity)
                    .Contains(_targetKey)
                || _materializingIndexes.FindRangeEntries(BenchmarkData.SalaryScope, originalSalary)
                    .Contains(_targetKey)))
        {
            throw new InvalidOperationException(
                "The indexed mutation fixture contains stale or missing index entries.");
        }

        if (Representation == DerivedIndexRepresentation.BoundedOrderedView)
        {
            var catalog = _view.OrderedIndexes.GetStateCatalog(BenchmarkData.StateName);
            if (_view.OrderedIndexes.GetFacetRecordCount(
                    BenchmarkData.CityScope,
                    SearchableIndexKind.Hash) != records.Count
                || _view.OrderedIndexes.GetFacetRecordCount(
                    BenchmarkData.SalaryScope,
                    SearchableIndexKind.Range) != records.Count)
            {
                throw new InvalidOperationException(
                    "The indexed mutation fixture did not maintain exact facet scope totals.");
            }

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
        if (result is StoragePartitionIndexes materializing)
        {
            if (!materializing.FindHashEntries(BenchmarkData.CityScope, expectedCity).Contains(targetKey)
                || !materializing.FindRangeEntries(BenchmarkData.SalaryScope, expectedSalary)
                    .Contains(targetKey))
            {
                throw new InvalidOperationException(
                    "The derived-index build benchmark omitted an expected posting.");
            }

            return;
        }

        if (result is not StoragePartitionView view)
        {
            throw new InvalidOperationException(
                "The derived-index build benchmark returned an unexpected representation.");
        }

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
            || view.OrderedIndexes.GetFacetRecordCount(
                BenchmarkData.CityScope,
                SearchableIndexKind.Hash) != RecordCount
            || view.OrderedIndexes.GetFacetRecordCount(
                BenchmarkData.SalaryScope,
                SearchableIndexKind.Range) != RecordCount
            || !PostingContains(catalog, targetGrainId, targetKey)
            || !PostingContains(orderedCity, targetGrainId, targetKey)
            || !PostingContains(orderedSalary, targetGrainId, targetKey))
        {
            throw new InvalidOperationException(
                "The bounded derived-index build benchmark omitted ordered catalog/posting entries.");
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

internal enum RetainedMemoryDatasetProfile
{
    CompaniesHouseDeShared,
}

/// <summary>
/// Builds a company-register-shaped retained-memory fixture. Equal categorical values and scopes
/// deliberately use distinct string instances, matching records recovered independently from a
/// durable snapshot instead of benefiting from benchmark-only reference sharing.
/// </summary>
internal static class RetainedMemoryProfileData
{
    private const string StateName = "companies-house-company";

    public static Dictionary<string, StoredRecord> CreateCompaniesHouseRecords(
        int count,
        int indexCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (indexCount is not 4 and not 8)
        {
            throw new ArgumentOutOfRangeException(
                nameof(indexCount),
                indexCount,
                "The retained-memory company profile supports exactly four or eight indexes.");
        }

        var records = new Dictionary<string, StoredRecord>(count, StringComparer.Ordinal);
        for (var index = 0; index < count; index++)
        {
            var grainId = BenchmarkData.CreateGrainId(index);
            records.Add(
                BenchmarkData.CreateStoredRecordKey(StateName, grainId),
                new StoredRecord
                {
                    GrainId = grainId,
                    Payload = CreatePayload(index),
                    ETag = checked(index + 1).ToString(System.Globalization.CultureInfo.InvariantCulture),
                    IndexEntries = CreateIndexEntries(index, indexCount),
                });
        }

        return records;
    }

    internal static RetainedMemoryProfileDiagnostics CaptureDeSharingDiagnostics(int indexCount)
    {
        var records = new[]
        {
            new StoredRecord
            {
                GrainId = BenchmarkData.CreateGrainId(0),
                Payload = CreatePayload(0),
                ETag = "1",
                IndexEntries = CreateIndexEntries(0, indexCount),
            },
            new StoredRecord
            {
                GrainId = BenchmarkData.CreateGrainId(1),
                Payload = CreatePayload(1),
                ETag = "2",
                IndexEntries = CreateIndexEntries(0, indexCount),
            },
        };
        var first = records[0];
        var repeated = records[1];
        var scopesAreEqualAndDistinct = first.IndexEntries
            .Zip(repeated.IndexEntries)
            .All(static pair =>
                string.Equals(pair.First.Scope, pair.Second.Scope, StringComparison.Ordinal)
                && !ReferenceEquals(pair.First.Scope, pair.Second.Scope));
        var categoricalValuesAreEqualAndDistinct = first.IndexEntries
            .Zip(repeated.IndexEntries)
            .Where(static pair => pair.First.Kind == SearchableIndexKind.Hash)
            .All(static pair =>
                pair.First.Value.Equals(pair.Second.Value)
                && !ReferenceEquals(pair.First.Value, pair.Second.Value)
                && string.Equals(pair.First.Value.Text, pair.Second.Value.Text, StringComparison.Ordinal)
                && !ReferenceEquals(pair.First.Value.Text, pair.Second.Value.Text));

        return new RetainedMemoryProfileDiagnostics(
            RecordCount: records.Length,
            IndexCount: first.IndexEntries.Count,
            ScopesAreEqualByValueAndDistinctByReference: scopesAreEqualAndDistinct,
            CategoricalValuesAreEqualByValueAndDistinctByReference:
                categoricalValuesAreEqualAndDistinct);
    }

    private static List<IndexEntry> CreateIndexEntries(int recordOrdinal, int indexCount)
    {
        var result = new List<IndexEntry>(indexCount);
        for (var indexOrdinal = 0; indexOrdinal < indexCount; indexOrdinal++)
        {
            var kind = indexOrdinal is 2 or 3 or 7
                ? SearchableIndexKind.Range
                : SearchableIndexKind.Hash;
            result.Add(new IndexEntry
            {
                Scope = CloneText(GetScope(indexOrdinal)),
                Kind = kind,
                Value = CreateValue(recordOrdinal, indexOrdinal),
            });
        }

        return result;
    }

    private static IndexValue CreateValue(int recordOrdinal, int indexOrdinal) => indexOrdinal switch
    {
        0 => IndexValue.Create(CloneText($"status-{recordOrdinal % 4:D2}")),
        1 => IndexValue.Create(CloneText($"company-type-{recordOrdinal % 16:D2}")),
        2 => IndexValue.FromSignedInteger(recordOrdinal % 20_000),
        3 => IndexValue.FromSignedInteger(recordOrdinal % 730),
        4 => IndexValue.Create(CloneText($"jurisdiction-{recordOrdinal % 8:D2}")),
        5 => IndexValue.Create(CloneText($"sic-{recordOrdinal % 1_024:D4}")),
        6 => IndexValue.Create(CloneText($"postal-area-{recordOrdinal % 256:D3}")),
        7 => IndexValue.FromSignedInteger(recordOrdinal % 3_650),
        _ => throw new ArgumentOutOfRangeException(nameof(indexOrdinal), indexOrdinal, null),
    };

    private static string GetScope(int indexOrdinal) => indexOrdinal switch
    {
        0 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company6:Status13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        1 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company11:CompanyType13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        2 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company17:IncorporationDate13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        3 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company15:AccountsDueDate13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        4 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company12:Jurisdiction13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        5 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company7:SicCode13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        6 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company10:PostalArea13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        7 => "107:Orleans.SearchableStorage.Qualification.CompaniesHouse.CompanyState23:companies-house-company14:LastFilingDate13:oss-schema-v164:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        _ => throw new ArgumentOutOfRangeException(nameof(indexOrdinal), indexOrdinal, null),
    };

    private static byte[] CreatePayload(int recordOrdinal)
    {
        var result = new byte[BenchmarkData.DefaultPayloadSize];
        for (var offset = 0; offset < result.Length; offset++)
        {
            result[offset] = unchecked((byte)((recordOrdinal * 31) + offset));
        }

        return result;
    }

    private static string CloneText(string value) =>
        string.Create(
            value.Length,
            value,
            static (destination, source) => source.AsSpan().CopyTo(destination));
}

internal sealed record RetainedMemoryProfileDiagnostics(
    int RecordCount,
    int IndexCount,
    bool ScopesAreEqualByValueAndDistinctByReference,
    bool CategoricalValuesAreEqualByValueAndDistinctByReference);

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
