using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Querying;

internal static class QueryProtocol
{
    public const int PagingVersion = 1;
    public const int OrderingVersion = 1;
    public const int WorkPolicyVersion = 1;
    public const int ContinuationPayloadVersion = 1;
}

internal enum PartitionQueryResponseFamily
{
    GrainIdPage = 1,
}

internal static class GrainIdCanonicalOrder
{
    internal const int MaximumTypeBytes = 1_024;
    internal const int MaximumKeyBytes = 4_096;

    public static IComparer<GrainId> Comparer { get; } = new CanonicalComparer();

    public static IEqualityComparer<GrainId> EqualityComparer { get; } = new CanonicalEqualityComparer();

    public static int Compare(GrainId left, GrainId right)
    {
        var typeComparison = left.Type.AsSpan().SequenceCompareTo(right.Type.AsSpan());
        return typeComparison != 0
            ? typeComparison
            : left.Key.AsSpan().SequenceCompareTo(right.Key.AsSpan());
    }

    public static int GetEncodedLength(GrainId grainId)
    {
        Validate(grainId, nameof(grainId));
        return checked(
            (2 * sizeof(int))
            + grainId.Type.AsSpan().Length
            + grainId.Key.AsSpan().Length);
    }

    internal static void Write(CanonicalBinaryWriter writer, GrainId grainId)
    {
        ArgumentNullException.ThrowIfNull(writer);
        Validate(grainId, nameof(grainId));
        writer.WriteBytes(grainId.Type.AsSpan());
        writer.WriteBytes(grainId.Key.AsSpan());
    }

    internal static GrainId Read(ref CanonicalBinaryReader reader)
    {
        var type = reader.ReadBytes(maximumLength: MaximumTypeBytes, requireNonEmpty: true);
        var key = reader.ReadBytes(maximumLength: MaximumKeyBytes, requireNonEmpty: true);
        var result = GrainId.Create(new GrainType(type), new IdSpan(key));
        Validate(result, nameof(reader));
        return result;
    }

    internal static void Validate(GrainId grainId, string parameterName)
    {
        var typeLength = grainId.Type.AsSpan().Length;
        var keyLength = grainId.Key.AsSpan().Length;
        if (typeLength is <= 0 or > MaximumTypeBytes
            || keyLength is <= 0 or > MaximumKeyBytes)
        {
            throw new ArgumentException(
                $"A canonical GrainId must contain 1 to {MaximumTypeBytes} type bytes and "
                + $"1 to {MaximumKeyBytes} key bytes.",
                parameterName);
        }
    }

    private sealed class CanonicalComparer : IComparer<GrainId>
    {
        public int Compare(GrainId x, GrainId y) => GrainIdCanonicalOrder.Compare(x, y);
    }

    private sealed class CanonicalEqualityComparer : IEqualityComparer<GrainId>
    {
        public bool Equals(GrainId x, GrainId y) => GrainIdCanonicalOrder.Compare(x, y) == 0;

        public int GetHashCode(GrainId obj)
        {
            var hash = new HashCode();
            foreach (var value in obj.Type.AsSpan())
            {
                hash.Add(value);
            }

            foreach (var value in obj.Key.AsSpan())
            {
                hash.Add(value);
            }

            return hash.ToHashCode();
        }
    }
}

internal static class QueryPlanFingerprint
{
    private const int FingerprintBytes = 32;
    internal const int MaximumCanonicalPlanBytes = 64 * 1_024;
    internal const int MaximumStateNameBytes = 1_024;
    internal const int MaximumPlanTextBytes = 16 * 1_024;

    public static byte[] Compute(string stateName, PartitionQueryPlan plan)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentNullException.ThrowIfNull(plan);
        QueryPlanValidator.Validate(plan);

        try
        {
            using var writer = new CanonicalBinaryWriter(MaximumCanonicalPlanBytes);
            writer.WriteInt32(QueryProtocol.PagingVersion);
            writer.WriteString(stateName, MaximumStateNameBytes);
            WritePlan(writer, plan);
            return SHA256.HashData(writer.WrittenSpan);
        }
        catch (CanonicalEncodingLimitExceededException exception)
        {
            throw new ArgumentException(
                $"The canonical partition query must not exceed {MaximumCanonicalPlanBytes} bytes.",
                nameof(plan),
                exception);
        }
    }

    public static bool Equals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        return left.Length == FingerprintBytes
            && right.Length == FingerprintBytes
            && CryptographicOperations.FixedTimeEquals(left, right);
    }

    private static void WritePlan(CanonicalBinaryWriter writer, PartitionQueryPlan plan)
    {
        writer.WriteInt32((int)plan.Operation);
        switch (plan.Operation)
        {
            case PartitionQueryOperation.Empty:
                return;
            case PartitionQueryOperation.Exact:
                writer.WriteString(plan.Scope!, MaximumPlanTextBytes);
                writer.WriteInt32((int)plan.IndexKind);
                WriteIndexValue(writer, plan.Value!);
                return;
            case PartitionQueryOperation.Range:
                writer.WriteString(plan.Scope!, MaximumPlanTextBytes);
                WriteOptionalIndexValue(writer, plan.LowerBound);
                WriteOptionalIndexValue(writer, plan.UpperBound);
                writer.WriteBoolean(plan.IncludeLowerBound);
                writer.WriteBoolean(plan.IncludeUpperBound);
                return;
            case PartitionQueryOperation.And:
            case PartitionQueryOperation.Or:
                WritePlan(writer, plan.Left!);
                WritePlan(writer, plan.Right!);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(plan), plan.Operation, "Unknown query operation.");
        }
    }

    private static void WriteOptionalIndexValue(CanonicalBinaryWriter writer, IndexValue? value)
    {
        writer.WriteBoolean(value is not null);
        if (value is not null)
        {
            WriteIndexValue(writer, value);
        }
    }

    private static void WriteIndexValue(CanonicalBinaryWriter writer, IndexValue value)
    {
        writer.WriteInt32((int)value.Kind);
        switch (value.Kind)
        {
            case IndexValueKind.String:
                writer.WriteString(value.Text!, MaximumPlanTextBytes);
                break;
            case IndexValueKind.SignedInteger:
                writer.WriteInt64(value.SignedInteger);
                break;
            case IndexValueKind.UnsignedInteger:
                writer.WriteUInt64(value.UnsignedInteger);
                break;
            case IndexValueKind.Decimal:
                foreach (var part in decimal.GetBits(value.Decimal))
                {
                    writer.WriteInt32(part);
                }

                break;
            case IndexValueKind.FloatingPoint:
                writer.WriteInt64(BitConverter.DoubleToInt64Bits(value.FloatingPoint));
                break;
            case IndexValueKind.Timestamp:
                writer.WriteInt64(value.UtcTicks);
                break;
            case IndexValueKind.Guid:
                Span<byte> guid = stackalloc byte[16];
                if (!value.Guid.TryWriteBytes(guid, bigEndian: true, out var bytesWritten)
                    || bytesWritten != guid.Length)
                {
                    throw new InvalidOperationException("A query GUID could not be encoded.");
                }

                writer.WriteRawBytes(guid);
                break;
            case IndexValueKind.Boolean:
                writer.WriteBoolean(value.Boolean);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(value), value.Kind, "Unknown index value kind.");
        }
    }
}

internal static class StorageLayoutFingerprint
{
    private const int FingerprintBytes = 32;
    private static readonly ConditionalWeakTable<StorageLayoutSnapshot, FingerprintBox> Cache = new();

    public static byte[] Compute(StorageLayoutSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return [.. Cache.GetValue(snapshot, static value => new FingerprintBox(ComputeCore(value))).Value];
    }

    public static bool Equals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        return left.Length == FingerprintBytes
            && right.Length == FingerprintBytes
            && CryptographicOperations.FixedTimeEquals(left, right);
    }

    private static byte[] ComputeCore(StorageLayoutSnapshot snapshot)
    {
        using var writer = new CanonicalBinaryWriter();
        writer.WriteInt32(snapshot.FormatVersion);
        writer.WriteString(snapshot.ProviderName);
        writer.WriteInt32(snapshot.InitialPartitionCount);
        writer.WriteInt32(snapshot.VirtualSlotCount);
        writer.WriteInt64(snapshot.Epoch);
        var assignments = snapshot.CopySlotAssignments();
        writer.WriteInt32(assignments.Length);
        foreach (var owner in assignments)
        {
            writer.WriteInt32(owner);
        }

        return SHA256.HashData(writer.WrittenSpan);
    }

    private sealed record FingerprintBox(byte[] Value);
}

internal sealed class CanonicalBinaryWriter : IDisposable
{
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);
    private readonly int _maximumLength;
    private readonly MemoryStream _stream;

    public CanonicalBinaryWriter(int maximumLength = int.MaxValue)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumLength);
        _maximumLength = maximumLength;
        _stream = new MemoryStream(Math.Min(maximumLength, 256));
    }

    public ReadOnlySpan<byte> WrittenSpan => _stream.GetBuffer().AsSpan(0, checked((int)_stream.Length));

    public void WriteBoolean(bool value)
    {
        EnsureCanWrite(1);
        _stream.WriteByte(value ? (byte)1 : (byte)0);
    }

    public void WriteInt32(int value)
    {
        EnsureCanWrite(sizeof(int));
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        _stream.Write(bytes);
    }

    public void WriteInt64(long value)
    {
        EnsureCanWrite(sizeof(long));
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        _stream.Write(bytes);
    }

    public void WriteUInt64(ulong value)
    {
        EnsureCanWrite(sizeof(ulong));
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
        _stream.Write(bytes);
    }

    public void WriteString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var bytes = StrictUtf8.GetBytes(value);
        WriteBytes(bytes);
    }

    public void WriteString(string value, int maximumByteLength)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumByteLength);
        var byteCount = StrictUtf8.GetByteCount(value);
        if (byteCount > maximumByteLength)
        {
            throw new CanonicalEncodingLimitExceededException(
                $"A canonical text field exceeds {maximumByteLength} UTF-8 bytes.");
        }

        EnsureCanWrite(checked(sizeof(int) + byteCount));
        var bytes = StrictUtf8.GetBytes(value);
        WriteBytes(bytes);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteInt32(value.Length);
        WriteRawBytes(value);
    }

    public void WriteRawBytes(ReadOnlySpan<byte> value)
    {
        EnsureCanWrite(value.Length);
        _stream.Write(value);
    }

    public byte[] ToArray() => _stream.ToArray();

    public void Dispose() => _stream.Dispose();

    private void EnsureCanWrite(int byteCount)
    {
        if (byteCount < 0 || _stream.Length + byteCount > _maximumLength)
        {
            throw new CanonicalEncodingLimitExceededException(
                $"A canonical payload exceeds {_maximumLength} bytes.");
        }
    }
}

internal sealed class CanonicalEncodingLimitExceededException(string message) : Exception(message);

internal ref struct CanonicalBinaryReader
{
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);
    private readonly ReadOnlySpan<byte> _source;
    private int _offset;

    public CanonicalBinaryReader(ReadOnlySpan<byte> source)
    {
        _source = source;
        _offset = 0;
    }

    public int Remaining => _source.Length - _offset;

    public bool ReadBoolean()
    {
        var value = ReadRawBytes(1)[0];
        return value switch
        {
            0 => false,
            1 => true,
            _ => throw new FormatException("A canonical Boolean field is invalid."),
        };
    }

    public int ReadInt32()
    {
        return BinaryPrimitives.ReadInt32BigEndian(ReadRawBytes(sizeof(int)));
    }

    public long ReadInt64()
    {
        return BinaryPrimitives.ReadInt64BigEndian(ReadRawBytes(sizeof(long)));
    }

    public string ReadString(int maximumByteLength, bool requireNonEmpty = false)
    {
        var value = ReadBytes(maximumByteLength, requireNonEmpty);
        return StrictUtf8.GetString(value);
    }

    public byte[] ReadBytes(int maximumLength, bool requireNonEmpty = false)
    {
        var length = ReadInt32();
        if (length < 0
            || length > maximumLength
            || (requireNonEmpty && length == 0))
        {
            throw new FormatException("A canonical byte field has an invalid length.");
        }

        return ReadRawBytes(length).ToArray();
    }

    public ReadOnlySpan<byte> ReadRawBytes(int length)
    {
        if (length < 0 || length > Remaining)
        {
            throw new FormatException("A canonical field extends beyond the available input.");
        }

        var result = _source.Slice(_offset, length);
        _offset += length;
        return result;
    }

    public void EnsureFullyConsumed()
    {
        if (Remaining != 0)
        {
            throw new FormatException("A canonical payload contains trailing data.");
        }
    }
}
