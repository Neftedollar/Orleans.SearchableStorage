using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexValueTests
{
    private static readonly Guid SampleGuid = Guid.Parse("8af52a20-90df-4ea6-80c5-68b75f4c39c5");

    [Theory]
    [InlineData(-10, 0)]
    [InlineData(0, 10)]
    [InlineData(10, 20)]
    public void SignedIntegerValuesPreserveNumericOrder(long lower, long upper)
    {
        IndexValue.Create(lower).CompareTo(IndexValue.Create(upper)).Should().BeNegative();
    }

    [Theory]
    [InlineData(DateTimeKind.Local)]
    [InlineData(DateTimeKind.Unspecified)]
    public void NonUtcDateTimeValuesAreRejected(DateTimeKind kind)
    {
        var action = () => IndexValue.Create(new DateTime(2026, 1, 1, 0, 0, 0, kind));

        action.Should().Throw<ArgumentException>()
            .WithMessage("*DateTimeKind.Utc*");
    }

    [Theory]
    [InlineData("string")]
    [InlineData("char")]
    [InlineData("sbyte")]
    [InlineData("short")]
    [InlineData("int")]
    [InlineData("long")]
    [InlineData("byte")]
    [InlineData("ushort")]
    [InlineData("uint")]
    [InlineData("ulong")]
    [InlineData("decimal")]
    [InlineData("float")]
    [InlineData("double")]
    [InlineData("utc-date-time")]
    [InlineData("date-time-offset")]
    [InlineData("guid")]
    [InlineData("boolean")]
    [InlineData("signed-enum")]
    [InlineData("unsigned-enum")]
    public void SupportedValuesProduceStableEqualityAndHashCodes(string valueKind)
    {
        var value = CreateValue(valueKind);

        var first = IndexValue.Create(value);
        var second = IndexValue.Create(value);

        first.Should().Be(second);
        first.Equals((object)second).Should().BeTrue();
        first.CompareTo(second).Should().Be(0);
        first.GetHashCode().Should().Be(second.GetHashCode());
    }

    [Fact]
    public void ComparisonEqualRepresentationsShareHashCodes()
    {
        var utc = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        (object Left, object Right)[] pairs =
        [
            ('H', "H"),
            ((sbyte)-1, -1L),
            ((byte)1, 1UL),
            (1.0M, 1.00M),
            (-0.0F, 0.0D),
            (utc, new DateTimeOffset(utc)),
            (SampleGuid, SampleGuid),
            (true, true),
        ];

        foreach (var pair in pairs)
        {
            var left = IndexValue.Create(pair.Left);
            var right = IndexValue.Create(pair.Right);

            left.CompareTo(right).Should().Be(0);
            left.GetHashCode().Should().Be(right.GetHashCode());
        }
    }

    [Theory]
    [InlineData("string")]
    [InlineData("unsigned")]
    [InlineData("decimal")]
    [InlineData("floating-point")]
    [InlineData("utc-date-time")]
    [InlineData("date-time-offset")]
    [InlineData("signed-enum")]
    [InlineData("unsigned-enum")]
    public void OrderedValueFamiliesPreserveOrder(string valueKind)
    {
        var (lower, upper) = CreateOrderedValues(valueKind);

        IndexValue.Create(lower).CompareTo(IndexValue.Create(upper)).Should().BeNegative();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void NaNValuesAreRejected(bool singlePrecision)
    {
        var value = singlePrecision ? (object)float.NaN : double.NaN;

        var action = () => IndexValue.Create(value);

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*NaN*");
    }

    [Fact]
    public void NullableAndEnumShapesReportTheirCapabilities()
    {
        IndexValue.IsSupported(typeof(int?)).Should().BeTrue();
        IndexValue.IsRangeSupported(typeof(int?)).Should().BeTrue();
        IndexValue.IsSupported(typeof(SignedSample)).Should().BeTrue();
        IndexValue.IsRangeSupported(typeof(SignedSample)).Should().BeTrue();
        IndexValue.IsRangeSupported(typeof(Guid?)).Should().BeFalse();
    }

    [Fact]
    public void UnsupportedValuesAreRejected()
    {
        var action = () => IndexValue.Create(TimeSpan.FromMinutes(1));

        action.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void NonNullValueSortsAfterNull()
    {
        IndexValue.Create(1).CompareTo(null).Should().BePositive();
    }

    [Fact]
    public void DifferentValueKindsRemainDistinct()
    {
        var text = IndexValue.Create("1");
        var number = IndexValue.Create(1);

        text.CompareTo(number).Should().NotBe(0);
        text.Should().NotBe(number);
    }

    [Fact]
    public void UnknownSerializedKindsAreRejected()
    {
        var value = new IndexValue { Kind = (IndexValueKind)int.MaxValue };

        var compare = () => value.CompareTo(new IndexValue { Kind = value.Kind });
        var hash = value.GetHashCode;

        compare.Should().Throw<InvalidOperationException>()
            .WithMessage("*Unknown index value kind*");
        hash.Should().Throw<InvalidOperationException>()
            .WithMessage("*Unknown index value kind*");
    }

    private static object CreateValue(string valueKind)
    {
        return valueKind switch
        {
            "string" => "Helsinki",
            "char" => 'H',
            "sbyte" => (sbyte)-8,
            "short" => (short)-16,
            "int" => -32,
            "long" => -64L,
            "byte" => (byte)8,
            "ushort" => (ushort)16,
            "uint" => 32U,
            "ulong" => 64UL,
            "decimal" => 12.5M,
            "float" => 12.5F,
            "double" => 12.5D,
            "utc-date-time" => new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc),
            "date-time-offset" => new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero),
            "guid" => SampleGuid,
            "boolean" => true,
            "signed-enum" => SignedSample.Negative,
            "unsigned-enum" => UnsignedSample.Value,
            _ => throw new ArgumentOutOfRangeException(nameof(valueKind), valueKind, "Unknown test value kind."),
        };
    }

    private static (object Lower, object Upper) CreateOrderedValues(string valueKind)
    {
        return valueKind switch
        {
            "string" => ("A", "B"),
            "unsigned" => (1U, 2U),
            "decimal" => (1M, 2M),
            "floating-point" => (1D, 2D),
            "utc-date-time" => (
                new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc)),
            "date-time-offset" => (
                new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero)),
            "signed-enum" => (SignedSample.Negative, SignedSample.Positive),
            "unsigned-enum" => (UnsignedSample.None, UnsignedSample.Value),
            _ => throw new ArgumentOutOfRangeException(nameof(valueKind), valueKind, "Unknown ordered test value kind."),
        };
    }

    private enum SignedSample : short
    {
        Negative = -1,
        Positive = 1,
    }

    private enum UnsignedSample : uint
    {
        None = 0,
        Value = 1,
    }
}
