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
        return IndexValueConverterProvider.TryGetConverter(type, out _);
    }

    public static bool IsRangeSupported(Type type)
    {
        return IndexValueConverterProvider.TryGetConverter(type, out var converter)
            && converter.SupportsRange;
    }

    public static IndexValue Create(object value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var type = value.GetType();
        if (!IndexValueConverterProvider.TryGetConverter(type, out var converter))
        {
            throw new NotSupportedException($"Values of type '{type}' cannot be indexed.");
        }

        return converter.ConvertObject(value)
            ?? throw new InvalidOperationException("A non-null index value unexpectedly converted to null.");
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

    internal static IndexValue FromSignedInteger(long value)
    {
        return new IndexValue { Kind = IndexValueKind.SignedInteger, SignedInteger = value };
    }

    internal static IndexValue FromUnsignedInteger(ulong value)
    {
        return new IndexValue { Kind = IndexValueKind.UnsignedInteger, UnsignedInteger = value };
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
