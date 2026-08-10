using System.Globalization;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkData
{
    public const string CityScope = "benchmark/state/city";
    public const string SalaryScope = "benchmark/state/salary";
    public const int DefaultPayloadSize = 64;

    public static Dictionary<string, StoredRecord> CreateRecords(
        int count,
        int payloadSize = DefaultPayloadSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        var records = new Dictionary<string, StoredRecord>(count, StringComparer.Ordinal);
        for (var index = 0; index < count; index++)
        {
            records.Add(CreateRecordKey(index), CreateRecord(index, payloadSize));
        }

        return records;
    }

    public static StoredRecord CreateRecord(
        int index,
        int payloadSize = DefaultPayloadSize,
        int? salary = null,
        int? city = null,
        string? etag = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfNegative(payloadSize);

        var payload = new byte[payloadSize];
        for (var offset = 0; offset < payload.Length; offset++)
        {
            payload[offset] = unchecked((byte)((index * 31) + offset));
        }

        return new StoredRecord
        {
            GrainId = GrainId.Create("benchmark-record", CreateRecordKey(index)),
            Payload = payload,
            ETag = etag ?? checked(index + 1).ToString(CultureInfo.InvariantCulture),
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = CityScope,
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create($"city-{city ?? index % 128:D3}"),
                },
                new IndexEntry
                {
                    Scope = SalaryScope,
                    Kind = SearchableIndexKind.Range,
                    Value = IndexValue.FromSignedInteger(salary ?? index),
                },
            ],
        };
    }

    public static string CreateRecordKey(int index) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"record-{index:D8}");

    public static Guid CreateOperationId(long value)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
        return new Guid(
            unchecked((int)value),
            unchecked((short)(value >> 16)),
            unchecked((short)(value >> 32)),
            0x51,
            0x53,
            0x53,
            0x42,
            0x45,
            0x4E,
            0x43,
            unchecked((byte)value));
    }

    public static StorageJournalEntry CreateUpsertEntry(long sequence, int recordIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sequence);
        var previousOperationId = sequence == 1
            ? Guid.Empty
            : CreateOperationId(sequence - 1);
        return new StorageJournalEntry
        {
            Sequence = sequence,
            WriterEpoch = 1,
            OperationId = CreateOperationId(sequence),
            PreviousOperationId = previousOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = CreateRecordKey(recordIndex),
            ExpectedETag = null,
            Record = CreateRecord(
                recordIndex,
                salary: checked(recordIndex + 1_000_000),
                city: checked(recordIndex + 1_000),
                etag: sequence.ToString(CultureInfo.InvariantCulture)),
            NextVersionAfter = checked(sequence + 1),
        };
    }
}
