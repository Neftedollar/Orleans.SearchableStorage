using System.Buffers.Binary;
using System.Security.Cryptography;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

internal static class StorageMoveProtocol
{
    public const int Version = 1;
    public const int DefaultPageRecords = SearchableStorageMovementOptions.DefaultTransferPageRecordLimit;
    public const int MaximumPageRecords = SearchableStorageMovementOptions.MaximumTransferPageRecordLimit;
    public const int DefaultPageBytes = SearchableStorageMovementOptions.DefaultTransferPageByteTarget;
    public const int MaximumPageBytes = SearchableStorageMovementOptions.MaximumTransferPageByteTarget;

    public static void ValidatePageLimits(int itemLimit, int byteTarget, string parameterName)
    {
        if (itemLimit <= 0 || itemLimit > MaximumPageRecords)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                itemLimit,
                $"A move page item limit must be between 1 and {MaximumPageRecords}.");
        }

        if (byteTarget <= 0 || byteTarget > MaximumPageBytes)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                byteTarget,
                $"A move page byte target must be between 1 and {MaximumPageBytes}.");
        }
    }
}

/// <summary>
/// Computes a deterministic digest over a bounded move page. Text is encoded as UTF-16 code units
/// so the storage write domain, including unpaired surrogates, remains transferable.
/// </summary>
internal static class StorageMovePageDigest
{
    public const int DigestLength = 32;

    public static byte[] Compute(StorageJournalOperation operation, StorageMoveJournalPayload move)
    {
        ArgumentNullException.ThrowIfNull(move);
        using var writer = new MoveHashWriter();
        writer.WriteInt32(StorageMoveProtocol.Version);
        writer.WriteInt32((int)operation);
        WriteGuid(writer, move.MoveId);
        writer.WriteInt32(move.Slot);
        writer.WriteInt32(move.VirtualSlotCount);
        writer.WriteInt64(move.SourceEpoch);
        writer.WriteInt32(move.SourceOwner);
        writer.WriteInt32(move.TargetOwner);
        writer.WriteInt64(move.PageOrdinal);
        WriteNullableText(writer, move.AfterRecordKey);
        WriteNullableText(writer, move.NextRecordKey);
        writer.WriteBoolean(move.Exhausted);
        writer.WriteInt64(move.FrozenNextVersion);
        writer.WriteInt32(move.ItemLimit);
        writer.WriteInt32(move.ByteTarget);
        writer.WriteInt64(move.EncodedByteCount);
        writer.WriteInt32(move.Imports.Count);
        foreach (var item in move.Imports)
        {
            WriteImport(writer, item);
        }

        writer.WriteInt32(move.Deletes.Count);
        foreach (var item in move.Deletes)
        {
            WriteDelete(writer, item);
        }

        return writer.GetHashAndReset();
    }

    public static bool Equals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        return left.Length == DigestLength
            && right.Length == DigestLength
            && CryptographicOperations.FixedTimeEquals(left, right);
    }

    public static long GetEncodedByteCount(IReadOnlyList<StorageMoveRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);
        var total = 0L;
        foreach (var record in records)
        {
            total = CheckedAddEncodedByteCount(total, GetEncodedByteCount(record));
        }

        return total;
    }

    public static long GetEncodedByteCount(IReadOnlyList<StorageMoveDeleteRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);
        var total = 0L;
        foreach (var record in records)
        {
            total = CheckedAddEncodedByteCount(total, GetEncodedByteCount(record));
        }

        return total;
    }

    public static long GetEncodedByteCount(StorageMoveRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        StorageMoveRecordCodec.Validate(record, nameof(record));
        return checked(GetTextByteCount(record.RecordKey) + GetRecordByteCount(record.Record));
    }

    public static long GetEncodedByteCount(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        StoragePersistenceStateValidation.ValidateRecord(record, nameof(record));
        return checked(GetTextEncodedByteCount(recordKey) + GetStoredRecordEncodedByteCount(record));
    }

    public static long GetEncodedByteCount(
        string recordKey,
        GrainId grainId,
        byte[]? payload,
        string etag,
        IReadOnlyList<IndexEntry> indexEntries,
        byte[]? indexSchemaFingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(etag);
        ArgumentNullException.ThrowIfNull(indexEntries);
        return checked(
            GetTextEncodedByteCount(recordKey)
            + sizeof(int) + grainId.Type.AsSpan().Length
            + sizeof(int) + grainId.Key.AsSpan().Length
            + sizeof(int) + (payload?.LongLength ?? 0)
            + GetTextEncodedByteCount(etag)
            + sizeof(int)
            + indexEntries.Sum(GetIndexEntryEncodedByteCount)
            + (indexSchemaFingerprint is null
                ? 0
                : sizeof(byte) + indexSchemaFingerprint.LongLength));
    }

    public static long GetStoredRecordEncodedByteCount(StoredRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        StoragePersistenceStateValidation.ValidateRecord(record, nameof(record));
        return checked(
            sizeof(int) + record.GrainId.Type.AsSpan().Length
            + sizeof(int) + record.GrainId.Key.AsSpan().Length
            + sizeof(int) + (record.Payload?.LongLength ?? 0)
            + GetTextEncodedByteCount(record.ETag)
            + sizeof(int)
            + record.IndexEntries.Sum(GetIndexEntryEncodedByteCount)
            + (record.IndexSchemaFingerprint is null
                ? 0
                : sizeof(byte) + record.IndexSchemaFingerprint.LongLength));
    }

    public static long GetIndexEntryEncodedByteCount(IndexEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.Scope);
        ArgumentNullException.ThrowIfNull(entry.Value);
        return checked(
            GetTextEncodedByteCount(entry.Scope)
            + sizeof(int)
            + GetIndexValueByteCount(entry.Value));
    }

    public static long GetIndexEntryEncodedByteCount(StorageMoveIndexEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(entry.Scope);
        ArgumentNullException.ThrowIfNull(entry.Value);
        return checked(
            GetTextByteCount(entry.Scope)
            + sizeof(int)
            + GetIndexValueByteCount(entry.Value));
    }

    public static long GetTextEncodedByteCount(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return checked(sizeof(int) + (long)value.Length * sizeof(char));
    }

    public static long GetEncodedByteCount(StorageMoveDeleteRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        StorageMoveRecordCodec.Validate(record, nameof(record));
        return checked(
            GetTextByteCount(record.RecordKey)
            + GetTextByteCount(record.ExpectedETag));
    }

    internal static long CheckedAddEncodedByteCount(long current, long item)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(current);
        ArgumentOutOfRangeException.ThrowIfNegative(item);
        return checked(current + item);
    }

    private static long GetRecordByteCount(StorageMoveStoredRecord record)
    {
        StorageMoveRecordCodec.Validate(record, nameof(record));
        var total = checked(
            sizeof(int) + (long)record.GrainType.Length
            + sizeof(int) + (long)record.GrainKey.Length
            + sizeof(int) + (record.Payload?.LongLength ?? 0)
            + GetTextByteCount(record.ETag)
            + sizeof(int));
        foreach (var entry in record.IndexEntries)
        {
            total = checked(
                total
                + GetTextByteCount(entry.Scope)
                + sizeof(int)
                + GetIndexValueByteCount(entry.Value));
        }

        if (record.IndexSchemaFingerprint is { } fingerprint)
        {
            total = checked(total + sizeof(byte) + fingerprint.LongLength);
        }

        return total;
    }

    private static long GetIndexValueByteCount(StorageMoveIndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return checked(
            sizeof(int)
            + sizeof(byte)
            + (value.Text is null ? 0 : GetTextByteCount(value.Text))
            + value.PrimitiveBits.LongLength);
    }

    private static long GetIndexValueByteCount(IndexValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        const int primitiveByteCount =
            sizeof(long)
            + sizeof(ulong)
            + (4 * sizeof(int))
            + sizeof(long)
            + sizeof(long)
            + 16
            + sizeof(byte);
        return checked(
            sizeof(int)
            + sizeof(byte)
            + (value.Text is null ? 0 : GetTextEncodedByteCount(value.Text))
            + primitiveByteCount);
    }

    private static long GetTextByteCount(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if ((value.Length & 1) != 0)
        {
            throw new ArgumentException("A movement text field is not complete UTF-16.", nameof(value));
        }

        return checked(sizeof(int) + value.LongLength);
    }

    private static void WriteImport(MoveHashWriter writer, StorageMoveRecord item)
    {
        WriteText(writer, item.RecordKey);
        WriteRecord(writer, item.Record);
    }

    private static void WriteDelete(MoveHashWriter writer, StorageMoveDeleteRecord item)
    {
        WriteText(writer, item.RecordKey);
        WriteText(writer, item.ExpectedETag);
    }

    private static void WriteRecord(MoveHashWriter writer, StorageMoveStoredRecord record)
    {
        StorageMoveRecordCodec.Validate(record, nameof(record));
        writer.WriteBytes(record.GrainType);
        writer.WriteBytes(record.GrainKey);
        writer.WriteNullableBytes(record.Payload);
        WriteText(writer, record.ETag);
        writer.WriteInt32(record.IndexEntries.Count);
        foreach (var entry in record.IndexEntries)
        {
            WriteText(writer, entry.Scope);
            writer.WriteInt32((int)entry.Kind);
            WriteIndexValue(writer, entry.Value);
        }

        if (record.IndexSchemaFingerprint is { } fingerprint)
        {
            // Absent fingerprints retain the pre-schema canonical digest byte-for-byte. Managed
            // records append a domain byte and their fixed-size identity.
            writer.WriteBoolean(true);
            writer.WriteRawBytes(fingerprint);
        }
    }

    private static void WriteIndexValue(MoveHashWriter writer, StorageMoveIndexValue value)
    {
        writer.WriteInt32((int)value.Kind);
        WriteNullableText(writer, value.Text);
        writer.WriteRawBytes(value.PrimitiveBits);
    }

    private static void WriteNullableText(MoveHashWriter writer, byte[]? value)
    {
        writer.WriteBoolean(value is not null);
        if (value is not null)
        {
            WriteText(writer, value);
        }
    }

    private static void WriteText(MoveHashWriter writer, byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if ((value.Length & 1) != 0)
        {
            throw new ArgumentException("A movement text field is not complete UTF-16.", nameof(value));
        }

        writer.WriteInt32(value.Length / sizeof(char));
        writer.WriteRawBytes(value);
    }

    private static void WriteGuid(MoveHashWriter writer, Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        if (!value.TryWriteBytes(bytes, bigEndian: true, out var bytesWritten)
            || bytesWritten != bytes.Length)
        {
            throw new InvalidOperationException("A move-protocol GUID could not be encoded.");
        }

        writer.WriteRawBytes(bytes);
    }

    private sealed class MoveHashWriter : IDisposable
    {
        private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        public void WriteBoolean(bool value)
        {
            Span<byte> bytes = stackalloc byte[1];
            bytes[0] = value ? (byte)1 : (byte)0;
            WriteRawBytes(bytes);
        }

        public void WriteInt32(int value)
        {
            Span<byte> bytes = stackalloc byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            WriteRawBytes(bytes);
        }

        public void WriteInt64(long value)
        {
            Span<byte> bytes = stackalloc byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(bytes, value);
            WriteRawBytes(bytes);
        }

        public void WriteBytes(ReadOnlySpan<byte> value)
        {
            WriteInt32(value.Length);
            WriteRawBytes(value);
        }

        public void WriteNullableBytes(byte[]? value)
        {
            if (value is null)
            {
                // Preserve the existing non-null encoding byte-for-byte while giving an absent
                // payload a domain which cannot collide with a valid zero-byte payload.
                WriteInt32(-1);
                return;
            }

            WriteBytes(value);
        }

        public void WriteRawBytes(ReadOnlySpan<byte> value)
        {
            _hash.AppendData(value);
        }

        public byte[] GetHashAndReset() => _hash.GetHashAndReset();

        public void Dispose() => _hash.Dispose();
    }
}
