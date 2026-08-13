using BenchmarkDotNet.Attributes;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

[BenchmarkCategory("Persistence", "Movement")]
public class SlotMovementBenchmarks
{
    private const int VirtualSlotCount = 64;
    private const int TargetSlot = 0;
    private const int CursorOffset = 3;
    private const int PageRecordLimit = 16;
    private const int PageByteTarget = 64 * 1_024;
    private static readonly Guid MoveId = new("243e21a4-15fe-4cc8-9926-af4393281f54");

    private Dictionary<string, StoredRecord> _sourceRecords = null!;
    private StoragePartitionView _sourceView = null!;
    private StoragePartitionView _importView = null!;
    private StoragePartitionView _deleteView = null!;
    private List<StorageMoveRecord> _importPage = null!;
    private List<StorageMoveDeleteRecord> _deletePage = null!;
    private string[] _targetKeys = null!;
    private string _afterRecordKey = null!;
    private long _expectedExportResult;

    [Params(4_096, 65_536)]
    public int RecordCount { get; set; }

    [Params(
        SlotMovementDistribution.Uniform,
        SlotMovementDistribution.Skewed,
        SlotMovementDistribution.OversizeSingleton)]
    public SlotMovementDistribution Distribution { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RecordCount);
        _sourceRecords = CreateRecords();
        _targetKeys = _sourceRecords
            .Where(static pair => StorageLayout.GetSlot(pair.Value.GrainId, VirtualSlotCount) == TargetSlot)
            .Select(static pair => pair.Key)
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (_targetKeys.Length <= CursorOffset + PageRecordLimit)
        {
            throw new InvalidOperationException(
                "The movement fixture does not contain enough target-slot records for a bounded page.");
        }

        _afterRecordKey = _targetKeys[CursorOffset - 1];
        if (Distribution == SlotMovementDistribution.OversizeSingleton)
        {
            MakeFirstAcceptedRecordOversize(_targetKeys[CursorOffset]);
        }

        _sourceView = new StoragePartitionView(_sourceRecords, VirtualSlotCount);
        _importPage = CreateExportPage(
            _sourceView,
            out _,
            out _,
            out _);
        _deletePage = CreateDeletePage(
            _sourceView,
            out _,
            out _,
            out _);

        ValidateFixture();
        IterationSetup();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _importView = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            VirtualSlotCount);
        _deleteView = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(_sourceRecords, StringComparer.Ordinal),
            VirtualSlotCount);
    }

    [Benchmark]
    public int RebuildSlotCatalog()
    {
        var catalog = new StoragePartitionSlotCatalog(_sourceRecords, VirtualSlotCount);
        return catalog.GetRecordCount(TargetSlot);
    }

    [Benchmark]
    public long ExportBoundedSlotPage()
    {
        var records = CreateExportPage(
            _sourceView,
            out var nextRecordKey,
            out var exhausted,
            out var encodedByteCount);
        return CreatePageResult(records.Count, nextRecordKey, exhausted, encodedByteCount);
    }

    [Benchmark]
    public int ImportBoundedSlotPage()
    {
        StorageMovePageOperations.ApplyImports(_importView, _importPage);
        return _importView.Records.Count;
    }

    [Benchmark]
    public int DeleteBoundedSlotPage()
    {
        StorageMovePageOperations.ApplyDeletes(_deleteView, _deletePage);
        return _deleteView.Records.Count;
    }

    internal void ValidateFixture()
    {
        var catalog = new StoragePartitionSlotCatalog(_sourceRecords, VirtualSlotCount);
        if (Enumerable.Range(0, VirtualSlotCount).Sum(catalog.GetRecordCount) != RecordCount
            || catalog.GetRecordCount(TargetSlot) != _targetKeys.Length
            || !catalog.EnumerateAfter(TargetSlot, afterRecordKey: null).SequenceEqual(_targetKeys))
        {
            throw new InvalidOperationException(
                "The rebuilt movement slot catalog did not match the independent membership oracle.");
        }

        var firstExport = CreateExportPage(
            _sourceView,
            out var exportNext,
            out var exportExhausted,
            out var exportBytes);
        var repeatedExport = CreateExportPage(
            _sourceView,
            out var repeatedExportNext,
            out var repeatedExportExhausted,
            out var repeatedExportBytes);
        ValidateExportPage(firstExport, exportNext, exportExhausted, exportBytes);
        ValidateExportPage(
            repeatedExport,
            repeatedExportNext,
            repeatedExportExhausted,
            repeatedExportBytes);
        if (!CreatePageDigest(
                StorageJournalOperation.Import,
                firstExport,
                [],
                exportNext,
                exportExhausted,
                exportBytes)
            .AsSpan()
            .SequenceEqual(CreatePageDigest(
                StorageJournalOperation.Import,
                repeatedExport,
                [],
                repeatedExportNext,
                repeatedExportExhausted,
                repeatedExportBytes)))
        {
            throw new InvalidOperationException(
                "The movement export fixture did not reproduce its canonical page identity.");
        }

        var firstDelete = CreateDeletePage(
            _sourceView,
            out var deleteNext,
            out var deleteExhausted,
            out var deleteBytes);
        var repeatedDelete = CreateDeletePage(
            _sourceView,
            out var repeatedDeleteNext,
            out var repeatedDeleteExhausted,
            out var repeatedDeleteBytes);
        ValidateDeletePage(firstDelete, deleteNext, deleteExhausted, deleteBytes);
        ValidateDeletePage(
            repeatedDelete,
            repeatedDeleteNext,
            repeatedDeleteExhausted,
            repeatedDeleteBytes);
        if (!CreatePageDigest(
                StorageJournalOperation.MoveDelete,
                [],
                firstDelete,
                deleteNext,
                deleteExhausted,
                deleteBytes)
            .AsSpan()
            .SequenceEqual(CreatePageDigest(
                StorageJournalOperation.MoveDelete,
                [],
                repeatedDelete,
                repeatedDeleteNext,
                repeatedDeleteExhausted,
                repeatedDeleteBytes)))
        {
            throw new InvalidOperationException(
                "The movement delete fixture did not reproduce its canonical page identity.");
        }

        _expectedExportResult = CreatePageResult(
            firstExport.Count,
            exportNext,
            exportExhausted,
            exportBytes);

        var importView = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            VirtualSlotCount);
        StorageMovePageOperations.ApplyImports(importView, firstExport);
        StorageMovePageOperations.ApplyImports(importView, firstExport);
        ValidateImportedView(importView, firstExport);

        var deleteView = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(_sourceRecords, StringComparer.Ordinal),
            VirtualSlotCount);
        StorageMovePageOperations.ApplyDeletes(deleteView, firstDelete);
        StorageMovePageOperations.ApplyDeletes(deleteView, firstDelete);
        ValidateDeletedView(deleteView, firstDelete);
    }

    internal void ValidateBenchmarkResults(
        int rebuiltTargetRecordCount,
        long exportResult,
        int importedRecordCount,
        int remainingRecordCount)
    {
        if (rebuiltTargetRecordCount != _targetKeys.Length
            || exportResult != _expectedExportResult
            || importedRecordCount != _importPage.Count
            || remainingRecordCount != RecordCount - _deletePage.Count)
        {
            throw new InvalidOperationException(
                "A timed movement operation did not match its frozen result oracle.");
        }

        ValidateImportedView(_importView, _importPage);
        ValidateDeletedView(_deleteView, _deletePage);
    }

    private Dictionary<string, StoredRecord> CreateRecords()
    {
        var result = new Dictionary<string, StoredRecord>(RecordCount, StringComparer.Ordinal);
        var candidate = 0;
        for (var index = 0; index < RecordCount; index++)
        {
            GrainId grainId;
            while (true)
            {
                grainId = BenchmarkData.CreateGrainId(candidate++);
                var slot = StorageLayout.GetSlot(grainId, VirtualSlotCount);
                if (Distribution != SlotMovementDistribution.Skewed
                    || index < RecordCount / 2 && slot == TargetSlot
                    || index >= RecordCount / 2 && slot != TargetSlot)
                {
                    break;
                }
            }

            result.Add(
                BenchmarkData.CreateStoredRecordKey(BenchmarkData.StateName, grainId),
                BenchmarkData.CreateRecord(index, grainId: grainId));
        }

        return result;
    }

    private void MakeFirstAcceptedRecordOversize(string recordKey)
    {
        var current = _sourceRecords[recordKey];
        _sourceRecords[recordKey] = new StoredRecord
        {
            GrainId = current.GrainId,
            Payload = new byte[PageByteTarget + 1],
            ETag = new string('e', (PageByteTarget / 2) + 1),
            IndexEntries = current.IndexEntries,
        };
    }

    private List<StorageMoveRecord> CreateExportPage(
        StoragePartitionView view,
        out string? nextRecordKey,
        out bool exhausted,
        out long encodedByteCount)
    {
        var records = StorageMovePageOperations.CreateExportRecords(
            view,
            TargetSlot,
            StorageMoveRecordCodec.EncodeText(_afterRecordKey),
            PageRecordLimit,
            PageByteTarget,
            out var encodedNextRecordKey,
            out exhausted,
            out encodedByteCount);
        nextRecordKey = StorageMoveRecordCodec.DecodeNullableText(
            encodedNextRecordKey,
            nameof(encodedNextRecordKey));
        return records;
    }

    private List<StorageMoveDeleteRecord> CreateDeletePage(
        StoragePartitionView view,
        out string? nextRecordKey,
        out bool exhausted,
        out long encodedByteCount)
    {
        var records = StorageMovePageOperations.CreateDeleteRecords(
            view,
            TargetSlot,
            StorageMoveRecordCodec.EncodeText(_afterRecordKey),
            PageRecordLimit,
            PageByteTarget,
            out var encodedNextRecordKey,
            out exhausted,
            out encodedByteCount);
        nextRecordKey = StorageMoveRecordCodec.DecodeNullableText(
            encodedNextRecordKey,
            nameof(encodedNextRecordKey));
        return records;
    }

    private void ValidateExportPage(
        List<StorageMoveRecord> records,
        string? nextRecordKey,
        bool exhausted,
        long encodedByteCount)
    {
        var expectedCount = Distribution == SlotMovementDistribution.OversizeSingleton
            ? 1
            : PageRecordLimit;
        var expectedKeys = _targetKeys.Skip(CursorOffset).Take(expectedCount).ToArray();
        var oracleBytes = records.Sum(GetOracleEncodedByteCount);
        if (records.Count != expectedCount
            || !records.Select(StorageMoveRecordCodec.DecodeRecordKey).SequenceEqual(expectedKeys)
            || !string.Equals(nextRecordKey, expectedKeys[^1], StringComparison.Ordinal)
            || exhausted
            || encodedByteCount != oracleBytes
            || records.Any(item => !RecordEquals(
                StorageMoveRecordCodec.Decode(item.Record),
                _sourceRecords[StorageMoveRecordCodec.DecodeRecordKey(item)])))
        {
            throw new InvalidOperationException(
                "The movement export page did not match its stable-key/count/byte oracle.");
        }

        if (Distribution == SlotMovementDistribution.OversizeSingleton)
        {
            if (encodedByteCount <= PageByteTarget)
            {
                throw new InvalidOperationException(
                    "The oversize movement export fixture did not admit exactly one oversize record.");
            }
        }
        else if (encodedByteCount > PageByteTarget)
        {
            throw new InvalidOperationException(
                "A multi-record movement export page exceeded its byte target.");
        }
    }

    private void ValidateDeletePage(
        List<StorageMoveDeleteRecord> records,
        string? nextRecordKey,
        bool exhausted,
        long encodedByteCount)
    {
        var expectedCount = Distribution == SlotMovementDistribution.OversizeSingleton
            ? 1
            : PageRecordLimit;
        var expectedKeys = _targetKeys.Skip(CursorOffset).Take(expectedCount).ToArray();
        var oracleBytes = records.Sum(GetOracleEncodedByteCount);
        if (records.Count != expectedCount
            || !records.Select(StorageMoveRecordCodec.DecodeRecordKey).SequenceEqual(expectedKeys)
            || !string.Equals(nextRecordKey, expectedKeys[^1], StringComparison.Ordinal)
            || exhausted
            || encodedByteCount != oracleBytes
            || records.Any(item => !string.Equals(
                StorageMoveRecordCodec.DecodeExpectedETag(item),
                _sourceRecords[StorageMoveRecordCodec.DecodeRecordKey(item)].ETag,
                StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                "The movement delete page did not match its stable-key/count/byte oracle.");
        }

        if (Distribution == SlotMovementDistribution.OversizeSingleton)
        {
            if (encodedByteCount <= PageByteTarget)
            {
                throw new InvalidOperationException(
                    "The oversize movement delete fixture did not admit exactly one oversize item.");
            }
        }
        else if (encodedByteCount > PageByteTarget)
        {
            throw new InvalidOperationException(
                "A multi-record movement delete page exceeded its byte target.");
        }
    }

    private static void ValidateImportedView(
        StoragePartitionView view,
        List<StorageMoveRecord> records)
    {
        if (view.Records.Count != records.Count
            || view.SlotCatalog?.GetRecordCount(TargetSlot) != records.Count)
        {
            throw new InvalidOperationException(
                "The movement import page produced the wrong record or slot membership count.");
        }

        foreach (var item in records)
        {
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            var decodedRecord = StorageMoveRecordCodec.Decode(item.Record);
            if (!view.Records.TryGetValue(recordKey, out var actual)
                || !RecordEquals(actual, decodedRecord)
                || ReferenceEquals(actual.Payload, item.Record.Payload)
                || !ContainsEveryDerivedIndex(view, recordKey, actual))
            {
                throw new InvalidOperationException(
                    "The movement import page did not rebuild a detached record and every derived index.");
            }
        }
    }

    private void ValidateDeletedView(
        StoragePartitionView view,
        List<StorageMoveDeleteRecord> records)
    {
        if (view.Records.Count != RecordCount - records.Count
            || view.Records.Count != Enumerable.Range(0, VirtualSlotCount)
                .Sum(slot => view.SlotCatalog!.GetRecordCount(slot)))
        {
            throw new InvalidOperationException(
                "The movement delete page left inconsistent record and slot-catalog counts.");
        }

        foreach (var item in records)
        {
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            var original = _sourceRecords[recordKey];
            if (view.Records.ContainsKey(recordKey)
                || view.SlotCatalog!.EnumerateAfter(TargetSlot, null).Contains(
                    recordKey,
                    StringComparer.Ordinal)
                || ContainsAnyDerivedIndex(view, recordKey, original))
            {
                throw new InvalidOperationException(
                    "The movement delete page left a record in a durable or derived projection.");
            }
        }
    }

    private static bool ContainsEveryDerivedIndex(
        StoragePartitionView view,
        string recordKey,
        StoredRecord record)
    {
        if (!view.OrderedIndexes.GetStateCatalog(BenchmarkData.StateName)
            .TryGetRecordKeys(record.GrainId, out var stateKeys)
            || !stateKeys.Contains(recordKey, StringComparer.Ordinal))
        {
            return false;
        }

        foreach (var entry in record.IndexEntries)
        {
            if (!view.OrderedIndexes.GetExactPosting(entry.Scope, entry.Kind, entry.Value)
                    .TryGetRecordKeys(record.GrainId, out var orderedKeys)
                || !orderedKeys.Contains(recordKey, StringComparer.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ContainsAnyDerivedIndex(
        StoragePartitionView view,
        string recordKey,
        StoredRecord record)
    {
        if (view.OrderedIndexes.GetStateCatalog(BenchmarkData.StateName)
            .TryGetRecordKeys(record.GrainId, out var stateKeys)
            && stateKeys.Contains(recordKey, StringComparer.Ordinal))
        {
            return true;
        }

        foreach (var entry in record.IndexEntries)
        {
            if (view.OrderedIndexes.GetExactPosting(entry.Scope, entry.Kind, entry.Value)
                    .TryGetRecordKeys(record.GrainId, out var orderedKeys)
                && orderedKeys.Contains(recordKey, StringComparer.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool RecordEquals(StoredRecord left, StoredRecord right)
    {
        return left.GrainId.Equals(right.GrainId)
            && left.Payload.AsSpan().SequenceEqual(right.Payload)
            && string.Equals(left.ETag, right.ETag, StringComparison.Ordinal)
            && left.IndexEntries.Count == right.IndexEntries.Count
            && left.IndexEntries.Zip(right.IndexEntries).All(static pair =>
                string.Equals(pair.First.Scope, pair.Second.Scope, StringComparison.Ordinal)
                && pair.First.Kind == pair.Second.Kind
                && pair.First.Value.Equals(pair.Second.Value));
    }

    private static long GetOracleEncodedByteCount(StorageMoveRecord item)
    {
        var record = item.Record;
        var total = checked(
            GetOracleTextByteCount(item.RecordKey)
            + GetOracleByteArrayCount(record.GrainType)
            + GetOracleByteArrayCount(record.GrainKey)
            + GetOracleByteArrayCount(record.Payload)
            + GetOracleTextByteCount(record.ETag)
            + sizeof(int));
        foreach (var entry in record.IndexEntries)
        {
            total = checked(
                total
                + GetOracleTextByteCount(entry.Scope)
                + sizeof(int)
                + sizeof(int)
                + sizeof(byte)
                + (entry.Value.Text is null ? 0 : GetOracleTextByteCount(entry.Value.Text))
                + entry.Value.PrimitiveBits.LongLength);
        }

        return total;
    }

    private static long GetOracleEncodedByteCount(StorageMoveDeleteRecord item) => checked(
        GetOracleTextByteCount(item.RecordKey)
        + GetOracleTextByteCount(item.ExpectedETag));

    private static long GetOracleTextByteCount(byte[] value) => checked(
        sizeof(int) + value.LongLength);

    private static long GetOracleByteArrayCount(byte[] value) => checked(
        sizeof(int) + value.LongLength);

    private byte[] CreatePageDigest(
        StorageJournalOperation operation,
        List<StorageMoveRecord> imports,
        List<StorageMoveDeleteRecord> deletes,
        string? nextRecordKey,
        bool exhausted,
        long encodedByteCount)
    {
        return StorageMovePageDigest.Compute(operation, new StorageMoveJournalPayload
        {
            MoveId = MoveId,
            Slot = TargetSlot,
            VirtualSlotCount = VirtualSlotCount,
            SourceEpoch = 1,
            SourceOwner = 0,
            TargetOwner = 1,
            PageOrdinal = 0,
            AfterRecordKey = StorageMoveRecordCodec.EncodeText(_afterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.EncodeNullableText(nextRecordKey),
            Exhausted = exhausted,
            FrozenNextVersion = checked(RecordCount + 1L),
            Imports = imports,
            Deletes = deletes,
            ItemLimit = PageRecordLimit,
            ByteTarget = PageByteTarget,
            EncodedByteCount = encodedByteCount,
        });
    }

    private static long CreatePageResult(
        int recordCount,
        string? nextRecordKey,
        bool exhausted,
        long encodedByteCount) => checked(
            recordCount
            + encodedByteCount
            + (nextRecordKey?.Length ?? 0)
            + (exhausted ? 1 : 0));
}

public enum SlotMovementDistribution
{
    Uniform,
    Skewed,
    OversizeSingleton,
}
