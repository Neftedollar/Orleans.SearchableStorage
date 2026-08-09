using System.Globalization;

namespace Orleans.SearchableStorage.Indexing;

[GenerateSerializer]
internal sealed class IndexValue : IComparable<IndexValue>, IEquatable<IndexValue>
{
    [Id(0)]
    public IndexValueKind Kind { get; set; }

    [Id(1)]
    public string? Text { get; set; }

    [Id(2)]
    public long SignedInteger { get; set; }

    [Id(3)]
    public ulong UnsignedInteger { get; set; }

    [Id(4)]
    public decimal Decimal { get; set; }

    [Id(5)]
    public double FloatingPoint { get; set; }

    [Id(6)]
    public long UtcTicks { get; set; }

    [Id(7)]
    public Guid Guid { get; set; }

    [Id(8)]
    public bool Boolean { get; set; }

    public static bool IsSupported(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type.IsEnum
            || type == typeof(string)
            || type == typeof(char)
            || type == typeof(sbyte)
            || type == typeof(short)
            || type == typeof(int)
            || type == typeof(long)
            || type == typeof(byte)
            || type == typeof(ushort)
            || type == typeof(uint)
            || type == typeof(ulong)
            || type == typeof(decimal)
            || type == typeof(float)
            || type == typeof(double)
            || type == typeof(DateTime)
            || type == typeof(DateTimeOffset)
            || type == typeof(Guid)
            || type == typeof(bool);
    }

    public static bool IsRangeSupported(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type.IsEnum
            || type == typeof(string)
            || type == typeof(char)
            || type == typeof(sbyte)
            || type == typeof(short)
            || type == typeof(int)
            || type == typeof(long)
            || type == typeof(byte)
            || type == typeof(ushort)
            || type == typeof(uint)
            || type == typeof(ulong)
            || type == typeof(decimal)
            || type == typeof(float)
            || type == typeof(double)
            || type == typeof(DateTime)
            || type == typeof(DateTimeOffset);
    }

    public static IndexValue Create(object value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var type = value.GetType();
        if (type.IsEnum)
        {
            var underlyingType = Enum.GetUnderlyingType(type);
            return IsUnsignedInteger(underlyingType)
                ? FromUnsignedInteger(Convert.ToUInt64(value, CultureInfo.InvariantCulture))
                : FromSignedInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture));
        }

        return value switch
        {
            string text => new IndexValue { Kind = IndexValueKind.String, Text = text },
            char character => new IndexValue { Kind = IndexValueKind.String, Text = character.ToString() },
            sbyte number => FromSignedInteger(number),
            short number => FromSignedInteger(number),
            int number => FromSignedInteger(number),
            long number => FromSignedInteger(number),
            byte number => FromUnsignedInteger(number),
            ushort number => FromUnsignedInteger(number),
            uint number => FromUnsignedInteger(number),
            ulong number => FromUnsignedInteger(number),
            decimal number => new IndexValue { Kind = IndexValueKind.Decimal, Decimal = number },
            float number when !float.IsNaN(number) => new IndexValue { Kind = IndexValueKind.FloatingPoint, FloatingPoint = number },
            double number when !double.IsNaN(number) => new IndexValue { Kind = IndexValueKind.FloatingPoint, FloatingPoint = number },
            DateTime timestamp when timestamp.Kind == DateTimeKind.Utc => new IndexValue { Kind = IndexValueKind.Timestamp, UtcTicks = timestamp.Ticks },
            DateTime timestamp => throw new ArgumentException("Indexed DateTime values must use DateTimeKind.Utc.", nameof(value)),
            DateTimeOffset timestamp => new IndexValue { Kind = IndexValueKind.Timestamp, UtcTicks = timestamp.UtcTicks },
            Guid guid => new IndexValue { Kind = IndexValueKind.Guid, Guid = guid },
            bool boolean => new IndexValue { Kind = IndexValueKind.Boolean, Boolean = boolean },
            _ => throw new NotSupportedException($"Values of type '{type}' cannot be indexed."),
        };
    }

    public int CompareTo(IndexValue? other)
    {
        if (other is null)
        {
            return 1;
        }

        var kindComparison = Kind.CompareTo(other.Kind);
        if (kindComparison != 0)
        {
            return kindComparison;
        }

        return Kind switch
        {
            IndexValueKind.String => string.Compare(Text, other.Text, StringComparison.Ordinal),
            IndexValueKind.SignedInteger => SignedInteger.CompareTo(other.SignedInteger),
            IndexValueKind.UnsignedInteger => UnsignedInteger.CompareTo(other.UnsignedInteger),
            IndexValueKind.Decimal => Decimal.CompareTo(other.Decimal),
            IndexValueKind.FloatingPoint => FloatingPoint.CompareTo(other.FloatingPoint),
            IndexValueKind.Timestamp => UtcTicks.CompareTo(other.UtcTicks),
            IndexValueKind.Guid => Guid.CompareTo(other.Guid),
            IndexValueKind.Boolean => Boolean.CompareTo(other.Boolean),
            _ => throw new InvalidOperationException($"Unknown index value kind '{Kind}'."),
        };
    }

    public bool Equals(IndexValue? other)
    {
        return other is not null && CompareTo(other) == 0;
    }

    public override bool Equals(object? obj)
    {
        return obj is IndexValue other && Equals(other);
    }

    public override int GetHashCode()
    {
        return Kind switch
        {
            IndexValueKind.String => HashCode.Combine(Kind, Text),
            IndexValueKind.SignedInteger => HashCode.Combine(Kind, SignedInteger),
            IndexValueKind.UnsignedInteger => HashCode.Combine(Kind, UnsignedInteger),
            IndexValueKind.Decimal => HashCode.Combine(Kind, Decimal),
            IndexValueKind.FloatingPoint => HashCode.Combine(Kind, FloatingPoint),
            IndexValueKind.Timestamp => HashCode.Combine(Kind, UtcTicks),
            IndexValueKind.Guid => HashCode.Combine(Kind, Guid),
            IndexValueKind.Boolean => HashCode.Combine(Kind, Boolean),
            _ => throw new InvalidOperationException($"Unknown index value kind '{Kind}'."),
        };
    }

    private static IndexValue FromSignedInteger(long value)
    {
        return new IndexValue { Kind = IndexValueKind.SignedInteger, SignedInteger = value };
    }

    private static IndexValue FromUnsignedInteger(ulong value)
    {
        return new IndexValue { Kind = IndexValueKind.UnsignedInteger, UnsignedInteger = value };
    }

    private static bool IsUnsignedInteger(Type type)
    {
        return type == typeof(byte)
            || type == typeof(ushort)
            || type == typeof(uint)
            || type == typeof(ulong);
    }
}

internal enum IndexValueKind
{
    String = 0,
    SignedInteger = 1,
    UnsignedInteger = 2,
    Decimal = 3,
    FloatingPoint = 4,
    Timestamp = 5,
    Guid = 6,
    Boolean = 7,
}
