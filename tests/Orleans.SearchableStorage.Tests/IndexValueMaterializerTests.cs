using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexValueMaterializerTests
{
    [Fact]
    public void EverySupportedClrDomainRoundTripsFromItsCanonicalIndexValue()
    {
        RoundTrip("alpha").Should().Be("alpha");
        RoundTrip('λ').Should().Be('λ');
        RoundTrip(sbyte.MinValue).Should().Be(sbyte.MinValue);
        RoundTrip(short.MinValue).Should().Be(short.MinValue);
        RoundTrip(int.MinValue).Should().Be(int.MinValue);
        RoundTrip(long.MinValue).Should().Be(long.MinValue);
        RoundTrip(byte.MaxValue).Should().Be(byte.MaxValue);
        RoundTrip(ushort.MaxValue).Should().Be(ushort.MaxValue);
        RoundTrip(uint.MaxValue).Should().Be(uint.MaxValue);
        RoundTrip(ulong.MaxValue).Should().Be(ulong.MaxValue);
        RoundTrip(123.4500m).Should().Be(123.4500m);
        RoundTrip(123.25f).Should().Be(123.25f);
        RoundTrip(double.PositiveInfinity).Should().Be(double.PositiveInfinity);
        RoundTrip(new DateTime(638712864000000000, DateTimeKind.Utc))
            .Should().Be(new DateTime(638712864000000000, DateTimeKind.Utc));
        RoundTrip(new DateTimeOffset(2025, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)))
            .Should().Be(new DateTimeOffset(2025, 1, 2, 1, 4, 5, TimeSpan.Zero));
        var guid = Guid.Parse("6a6bb99d-895b-412d-9636-7802af958a9f");
        RoundTrip(guid).Should().Be(guid);
        RoundTrip(true).Should().BeTrue();
        RoundTrip(FacetEnum.High).Should().Be(FacetEnum.High);
        RoundTrip<FacetUnsignedEnum>(FacetUnsignedEnum.Maximum).Should().Be(FacetUnsignedEnum.Maximum);
        RoundTrip<int?>(42).Should().Be(42);
        RoundTrip<FacetEnum?>(FacetEnum.Low).Should().Be(FacetEnum.Low);
    }

    [Fact]
    public void NullableNullIsNotAnIndexedValue()
    {
        IndexValueConverterProvider.TryGetConverter(typeof(int?), out var converter).Should().BeTrue();

        converter!.ConvertObject(null).Should().BeNull();
    }

    [Fact]
    public void MalformedWireDomainsAreRejectedBeforeMaterialization()
    {
        var stringConverter = GetConverter<string>();
        var floatConverter = GetConverter<float>();
        var timestampConverter = GetConverter<DateTime>();
        var enumConverter = GetConverter<FacetEnum>();

        Action nullText = () => _ = IndexValueMaterializer.Materialize<string>(
            new IndexValue { Kind = IndexValueKind.String, Text = null },
            stringConverter);
        Action nan = () => _ = IndexValueMaterializer.Materialize<float>(
            new IndexValue { Kind = IndexValueKind.FloatingPoint, FloatingPoint = double.NaN },
            floatConverter);
        Action nonSingle = () => _ = IndexValueMaterializer.Materialize<float>(
            new IndexValue { Kind = IndexValueKind.FloatingPoint, FloatingPoint = 0.1d },
            floatConverter);
        Action badTicks = () => _ = IndexValueMaterializer.Materialize<DateTime>(
            new IndexValue { Kind = IndexValueKind.Timestamp, UtcTicks = DateTime.MaxValue.Ticks + 1 },
            timestampConverter);
        Action enumOverflow = () => _ = IndexValueMaterializer.Materialize<FacetEnum>(
            new IndexValue { Kind = IndexValueKind.SignedInteger, SignedInteger = short.MaxValue },
            enumConverter);

        nullText.Should().Throw<ArgumentException>();
        nan.Should().Throw<ArgumentException>();
        nonSingle.Should().Throw<InvalidOperationException>().WithMessage("*not representable*");
        badTicks.Should().Throw<ArgumentException>();
        enumOverflow.Should().Throw<OverflowException>();
    }

    [Fact]
    public void ExistingVersionOneQueryFingerprintKeepsRawDecimalScaleAndNegativeZeroBits()
    {
        var one = Fingerprint(IndexValue.Create(1m));
        var oneWithScale = Fingerprint(IndexValue.Create(1.00m));
        var positiveZero = Fingerprint(IndexValue.Create(0d));
        var negativeZero = Fingerprint(IndexValue.Create(-0d));

        one.Should().NotEqual(oneWithScale);
        positiveZero.Should().NotEqual(negativeZero);
    }

    private static T RoundTrip<T>(T value)
    {
        var converter = GetConverter<T>();
        var canonical = converter.ConvertObject(value)
            ?? throw new InvalidOperationException("The test value unexpectedly converted to null.");
        return IndexValueMaterializer.Materialize<T>(canonical, converter);
    }

    private static IndexValueConverter GetConverter<T>()
    {
        IndexValueConverterProvider.TryGetConverter(typeof(T), out var converter).Should().BeTrue();
        return converter!;
    }

    private static byte[] Fingerprint(IndexValue value)
    {
        return QueryPlanFingerprint.Compute("state", new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = "scope",
            IndexKind = SearchableIndexKind.Hash,
            Value = value,
        });
    }

    private enum FacetEnum : sbyte
    {
        Low = -1,
        High = 1,
    }

    private enum FacetUnsignedEnum : ulong
    {
        Maximum = ulong.MaxValue,
    }
}
