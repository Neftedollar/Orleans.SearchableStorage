using System.Buffers.Binary;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Lossless movement and persistence-v4 snapshot representation of a stored record. Orleans
/// encodes strings as UTF-8, which replaces unpaired UTF-16 surrogates. Movement pages and new v4
/// snapshots therefore carry every persisted text field as explicit big-endian UTF-16 code units
/// while leaving the legacy record, WAL-v3, and snapshot Id8 schemas unchanged.
/// </summary>
[GenerateSerializer]
internal sealed class StorageMoveStoredRecord
{
    [Id(0)] public required byte[] GrainType { get; init; }
    [Id(1)] public required byte[] GrainKey { get; init; }
    [Id(2)] public required byte[] Payload { get; init; }
    [Id(3)] public required byte[] ETag { get; init; }
    [Id(4)] public required List<StorageMoveIndexEntry> IndexEntries { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMoveIndexEntry
{
    [Id(0)] public required byte[] Scope { get; init; }
    [Id(1)] public SearchableIndexKind Kind { get; init; }
    [Id(2)] public required StorageMoveIndexValue Value { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMoveIndexValue
{
    [Id(0)] public IndexValueKind Kind { get; init; }
    [Id(1)] public byte[]? Text { get; init; }

    /// <summary>
    /// Exact big-endian bits for every primitive field in the persisted IndexValue, including
    /// fields which are inactive for <see cref="Kind"/>. This makes movement a true state copy.
    /// </summary>
    [Id(2)] public required byte[] PrimitiveBits { get; init; }
}

internal static class StorageMoveRecordCodec
{
    private const int DecimalByteCount = 4 * sizeof(int);
    private const int GuidByteCount = 16;
    private const int PrimitiveByteCount =
        sizeof(long)
        + sizeof(ulong)
        + DecimalByteCount
        + sizeof(long)
        + sizeof(long)
        + GuidByteCount
        + sizeof(byte);

    public static byte[] EncodeText(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var result = new byte[checked(value.Length * sizeof(char))];
        for (var index = 0; index < value.Length; index++)
        {
            BinaryPrimitives.WriteUInt16BigEndian(
                result.AsSpan(index * sizeof(char), sizeof(char)),
                value[index]);
        }

        return result;
    }

    public static byte[]? EncodeNullableText(string? value) =>
        value is null ? null : EncodeText(value);

    public static string DecodeText(byte[] value, string parameterName)
    {
        ValidateText(value, parameterName);
        return string.Create(
            value.Length / sizeof(char),
            value,
            static (characters, bytes) =>
            {
                for (var index = 0; index < characters.Length; index++)
                {
                    characters[index] = (char)BinaryPrimitives.ReadUInt16BigEndian(
                        bytes.AsSpan(index * sizeof(char), sizeof(char)));
                }
            });
    }

    public static string? DecodeNullableText(byte[]? value, string parameterName) =>
        value is null ? null : DecodeText(value, parameterName);

    public static byte[]? CopyText(byte[]? value) => value is null ? null : [.. value];

    public static bool TextEquals(byte[]? left, byte[]? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        ValidateText(left, nameof(left));
        ValidateText(right, nameof(right));
        return left.AsSpan().SequenceEqual(right);
    }

    public static int CompareText(byte[] left, byte[] right)
    {
        ValidateText(left, nameof(left));
        ValidateText(right, nameof(right));
        return left.AsSpan().SequenceCompareTo(right);
    }

    public static StorageMoveRecord Encode(string recordKey, StoredRecord record)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        StoragePersistenceStateValidation.ValidateRecord(record, nameof(record));
        return new StorageMoveRecord
        {
            RecordKey = EncodeText(recordKey),
            Record = Encode(record),
        };
    }

    public static StorageMoveDeleteRecord EncodeDelete(string recordKey, string expectedETag)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedETag);
        return new StorageMoveDeleteRecord
        {
            RecordKey = EncodeText(recordKey),
            ExpectedETag = EncodeText(expectedETag),
        };
    }

    public static StorageMoveStoredRecord Encode(StoredRecord record)
    {
        StoragePersistenceStateValidation.ValidateRecord(record, nameof(record));
        return new StorageMoveStoredRecord
        {
            GrainType = record.GrainId.Type.AsSpan().ToArray(),
            GrainKey = record.GrainId.Key.AsSpan().ToArray(),
            Payload = [.. record.Payload],
            ETag = EncodeText(record.ETag),
            IndexEntries = record.IndexEntries.Select(Encode).ToList(),
        };
    }

    public static string DecodeRecordKey(StorageMoveRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return DecodeText(record.RecordKey, nameof(record));
    }

    public static string DecodeRecordKey(StorageMoveDeleteRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return DecodeText(record.RecordKey, nameof(record));
    }

    public static string DecodeExpectedETag(StorageMoveDeleteRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return DecodeText(record.ExpectedETag, nameof(record));
    }

    public static StoredRecord Decode(StorageMoveStoredRecord record)
    {
        Validate(record, nameof(record));
        var result = new StoredRecord
        {
            GrainId = GrainId.Create(
                new GrainType([.. record.GrainType]),
                new IdSpan([.. record.GrainKey])),
            Payload = [.. record.Payload],
            ETag = DecodeText(record.ETag, nameof(record)),
            IndexEntries = record.IndexEntries.Select(Decode).ToList(),
        };
        StoragePersistenceStateValidation.ValidateRecord(result, nameof(record));
        return result;
    }

    public static StorageMoveRecord Copy(StorageMoveRecord record)
    {
        Validate(record, nameof(record));
        return new StorageMoveRecord
        {
            RecordKey = [.. record.RecordKey],
            Record = Copy(record.Record),
        };
    }

    public static StorageMoveDeleteRecord Copy(StorageMoveDeleteRecord record)
    {
        Validate(record, nameof(record));
        return new StorageMoveDeleteRecord
        {
            RecordKey = [.. record.RecordKey],
            ExpectedETag = [.. record.ExpectedETag],
        };
    }

    public static StorageMoveStoredRecord Copy(StorageMoveStoredRecord record)
    {
        Validate(record, nameof(record));
        return new StorageMoveStoredRecord
        {
            GrainType = [.. record.GrainType],
            GrainKey = [.. record.GrainKey],
            Payload = [.. record.Payload],
            ETag = [.. record.ETag],
            IndexEntries = record.IndexEntries.Select(Copy).ToList(),
        };
    }

    public static bool BinaryEquals(StorageMoveRecord left, StorageMoveRecord right)
    {
        Validate(left, nameof(left));
        Validate(right, nameof(right));
        if (!left.RecordKey.AsSpan().SequenceEqual(right.RecordKey))
        {
            return false;
        }

        var leftRecord = left.Record;
        var rightRecord = right.Record;
        if (!leftRecord.GrainType.AsSpan().SequenceEqual(rightRecord.GrainType)
            || !leftRecord.GrainKey.AsSpan().SequenceEqual(rightRecord.GrainKey)
            || !leftRecord.Payload.AsSpan().SequenceEqual(rightRecord.Payload)
            || !leftRecord.ETag.AsSpan().SequenceEqual(rightRecord.ETag)
            || leftRecord.IndexEntries.Count != rightRecord.IndexEntries.Count)
        {
            return false;
        }

        for (var index = 0; index < leftRecord.IndexEntries.Count; index++)
        {
            var leftEntry = leftRecord.IndexEntries[index];
            var rightEntry = rightRecord.IndexEntries[index];
            if (!leftEntry.Scope.AsSpan().SequenceEqual(rightEntry.Scope)
                || leftEntry.Kind != rightEntry.Kind
                || leftEntry.Value.Kind != rightEntry.Value.Kind
                || !TextEquals(leftEntry.Value.Text, rightEntry.Value.Text)
                || !leftEntry.Value.PrimitiveBits.AsSpan()
                    .SequenceEqual(rightEntry.Value.PrimitiveBits))
            {
                return false;
            }
        }

        return true;
    }

    public static void Validate(StorageMoveRecord record, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(record, parameterName);
        var recordKey = DecodeText(record.RecordKey, parameterName);
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey, parameterName);
        Validate(record.Record, parameterName);
    }

    public static void Validate(StorageMoveDeleteRecord record, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(record, parameterName);
        var recordKey = DecodeText(record.RecordKey, parameterName);
        var expectedETag = DecodeText(record.ExpectedETag, parameterName);
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey, parameterName);
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedETag, parameterName);
    }

    public static void Validate(StorageMoveStoredRecord record, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(record, parameterName);
        ArgumentNullException.ThrowIfNull(record.GrainType, parameterName);
        ArgumentNullException.ThrowIfNull(record.GrainKey, parameterName);
        ArgumentNullException.ThrowIfNull(record.Payload, parameterName);
        ArgumentNullException.ThrowIfNull(record.ETag, parameterName);
        ArgumentNullException.ThrowIfNull(record.IndexEntries, parameterName);
        if (record.GrainType.Length is <= 0 or > GrainIdCanonicalOrder.MaximumTypeBytes
            || record.GrainKey.Length is <= 0 or > GrainIdCanonicalOrder.MaximumKeyBytes)
        {
            throw new ArgumentException("A moved GrainId has invalid type or key bounds.", parameterName);
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(DecodeText(record.ETag, parameterName), parameterName);
        foreach (var entry in record.IndexEntries)
        {
            Validate(entry, parameterName);
        }
    }

    private static StorageMoveIndexEntry Encode(IndexEntry entry)
    {
        return new StorageMoveIndexEntry
        {
            Scope = EncodeText(entry.Scope),
            Kind = entry.Kind,
            Value = Encode(entry.Value),
        };
    }

    private static StorageMoveIndexValue Encode(IndexValue value)
    {
        var bits = new byte[PrimitiveByteCount];
        var offset = 0;
        BinaryPrimitives.WriteInt64BigEndian(bits.AsSpan(offset, sizeof(long)), value.SignedInteger);
        offset += sizeof(long);
        BinaryPrimitives.WriteUInt64BigEndian(bits.AsSpan(offset, sizeof(ulong)), value.UnsignedInteger);
        offset += sizeof(ulong);
        foreach (var part in decimal.GetBits(value.Decimal))
        {
            BinaryPrimitives.WriteInt32BigEndian(bits.AsSpan(offset, sizeof(int)), part);
            offset += sizeof(int);
        }

        BinaryPrimitives.WriteInt64BigEndian(
            bits.AsSpan(offset, sizeof(long)),
            BitConverter.DoubleToInt64Bits(value.FloatingPoint));
        offset += sizeof(long);
        BinaryPrimitives.WriteInt64BigEndian(bits.AsSpan(offset, sizeof(long)), value.UtcTicks);
        offset += sizeof(long);
        if (!value.Guid.TryWriteBytes(
                bits.AsSpan(offset, GuidByteCount),
                bigEndian: true,
                out var bytesWritten)
            || bytesWritten != GuidByteCount)
        {
            throw new InvalidOperationException("An indexed GUID could not be encoded for movement.");
        }

        offset += GuidByteCount;
        bits[offset] = value.Boolean ? (byte)1 : (byte)0;
        return new StorageMoveIndexValue
        {
            Kind = value.Kind,
            Text = EncodeNullableText(value.Text),
            PrimitiveBits = bits,
        };
    }

    private static IndexEntry Decode(StorageMoveIndexEntry entry)
    {
        Validate(entry, nameof(entry));
        return new IndexEntry
        {
            Scope = DecodeText(entry.Scope, nameof(entry)),
            Kind = entry.Kind,
            Value = Decode(entry.Value),
        };
    }

    private static IndexValue Decode(StorageMoveIndexValue value)
    {
        Validate(value, nameof(value));
        var bits = value.PrimitiveBits;
        var offset = 0;
        var signed = BinaryPrimitives.ReadInt64BigEndian(bits.AsSpan(offset, sizeof(long)));
        offset += sizeof(long);
        var unsigned = BinaryPrimitives.ReadUInt64BigEndian(bits.AsSpan(offset, sizeof(ulong)));
        offset += sizeof(ulong);
        var decimalBits = new int[4];
        for (var index = 0; index < decimalBits.Length; index++)
        {
            decimalBits[index] = BinaryPrimitives.ReadInt32BigEndian(
                bits.AsSpan(offset, sizeof(int)));
            offset += sizeof(int);
        }

        var floatingBits = BinaryPrimitives.ReadInt64BigEndian(bits.AsSpan(offset, sizeof(long)));
        offset += sizeof(long);
        var utcTicks = BinaryPrimitives.ReadInt64BigEndian(bits.AsSpan(offset, sizeof(long)));
        offset += sizeof(long);
        var guid = new Guid(bits.AsSpan(offset, GuidByteCount), bigEndian: true);
        offset += GuidByteCount;
        return new IndexValue
        {
            Kind = value.Kind,
            Text = DecodeNullableText(value.Text, nameof(value)),
            SignedInteger = signed,
            UnsignedInteger = unsigned,
            Decimal = new decimal(decimalBits),
            FloatingPoint = BitConverter.Int64BitsToDouble(floatingBits),
            UtcTicks = utcTicks,
            Guid = guid,
            Boolean = bits[offset] == 1,
        };
    }

    private static StorageMoveIndexEntry Copy(StorageMoveIndexEntry entry)
    {
        return new StorageMoveIndexEntry
        {
            Scope = [.. entry.Scope],
            Kind = entry.Kind,
            Value = Copy(entry.Value),
        };
    }

    private static StorageMoveIndexValue Copy(StorageMoveIndexValue value)
    {
        return new StorageMoveIndexValue
        {
            Kind = value.Kind,
            Text = CopyText(value.Text),
            PrimitiveBits = [.. value.PrimitiveBits],
        };
    }

    private static void Validate(StorageMoveIndexEntry entry, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(entry, parameterName);
        ArgumentNullException.ThrowIfNull(entry.Scope, parameterName);
        ArgumentNullException.ThrowIfNull(entry.Value, parameterName);
        ArgumentException.ThrowIfNullOrWhiteSpace(DecodeText(entry.Scope, parameterName), parameterName);
        if (!Enum.IsDefined(entry.Kind))
        {
            throw new ArgumentException($"Unknown moved index kind '{entry.Kind}'.", parameterName);
        }

        Validate(entry.Value, parameterName);
    }

    private static void Validate(StorageMoveIndexValue value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        ArgumentNullException.ThrowIfNull(value.PrimitiveBits, parameterName);
        if (!Enum.IsDefined(value.Kind)
            || value.PrimitiveBits.Length != PrimitiveByteCount
            || value.PrimitiveBits[^1] > 1
            || (value.Kind == IndexValueKind.String && value.Text is null))
        {
            throw new ArgumentException("A moved index value has invalid bounds or bits.", parameterName);
        }

        if (value.Text is not null)
        {
            ValidateText(value.Text, parameterName);
        }

        // Constructing validates the decimal flags in addition to the fixed byte count.
        _ = DecodePrimitiveDecimal(value.PrimitiveBits);
    }

    private static decimal DecodePrimitiveDecimal(byte[] bits)
    {
        const int decimalOffset = sizeof(long) + sizeof(ulong);
        var parts = new int[4];
        for (var index = 0; index < parts.Length; index++)
        {
            parts[index] = BinaryPrimitives.ReadInt32BigEndian(
                bits.AsSpan(decimalOffset + (index * sizeof(int)), sizeof(int)));
        }

        return new decimal(parts);
    }

    private static void ValidateText(byte[] value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if ((value.Length & 1) != 0)
        {
            throw new ArgumentException(
                "A movement text field must contain complete UTF-16 code units.",
                parameterName);
        }
    }
}
