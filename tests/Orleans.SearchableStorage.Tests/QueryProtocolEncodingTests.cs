using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryProtocolEncodingTests
{
    [Fact]
    public void QueryFingerprintIsDeterministicAndPreservesBooleanChildOrder()
    {
        var first = Exact("state/type/value", 1);
        var second = Exact("state/type/other", 2);
        var forward = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = first,
            Right = second,
        };
        var reverse = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = second,
            Right = first,
        };

        var fingerprint = QueryPlanFingerprint.Compute("state", forward);

        fingerprint.Should().HaveCount(32);
        QueryPlanFingerprint.Compute("state", forward).Should().Equal(fingerprint);
        QueryPlanFingerprint.Compute("state", reverse).Should().NotEqual(fingerprint);
        QueryPlanFingerprint.Compute("another-state", forward).Should().NotEqual(fingerprint);
    }

    [Fact]
    public void QueryFingerprintIncludesRangeBoundsAndInclusivity()
    {
        var inclusive = Range(includeUpper: true);
        var exclusive = Range(includeUpper: false);

        QueryPlanFingerprint.Compute("state", inclusive)
            .Should().NotEqual(QueryPlanFingerprint.Compute("state", exclusive));
    }

    [Fact]
    public void QueryFingerprintRejectsOversizedStateScopeAndTextWithoutEncodingThem()
    {
        var plan = Exact("scope", 1);
        var oversizedScope = Exact(
            new string('s', QueryPlanFingerprint.MaximumPlanTextBytes + 1),
            1);
        var oversizedText = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = "scope",
            IndexKind = SearchableIndexKind.Hash,
            Value = new IndexValue
            {
                Kind = IndexValueKind.String,
                Text = new string('v', QueryPlanFingerprint.MaximumPlanTextBytes + 1),
            },
        };

        Action state = () => _ = QueryPlanFingerprint.Compute(
            new string('n', QueryPlanFingerprint.MaximumStateNameBytes + 1),
            plan);
        Action scope = () => _ = QueryPlanFingerprint.Compute("state", oversizedScope);
        Action text = () => _ = QueryPlanFingerprint.Compute("state", oversizedText);

        state.Should().Throw<ArgumentException>().WithMessage("*canonical partition query*");
        scope.Should().Throw<ArgumentException>().WithMessage("*canonical partition query*");
        text.Should().Throw<ArgumentException>().WithMessage("*canonical partition query*");
    }

    [Fact]
    public void QueryFingerprintRejectsAnOversizedAggregateCanonicalPlan()
    {
        var largeScope = new string('s', 16_380);
        var plan = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Or,
            Left = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Or,
                Left = Exact(largeScope + "1", 1),
                Right = Exact(largeScope + "2", 2),
            },
            Right = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Or,
                Left = Exact(largeScope + "3", 3),
                Right = Exact(largeScope + "4", 4),
            },
        };

        Action compute = () => _ = QueryPlanFingerprint.Compute("state", plan);

        compute.Should().Throw<ArgumentException>()
            .WithMessage("*must not exceed 65536 bytes*");
    }

    [Fact]
    public void LayoutFingerprintIsCachedBySnapshotIdentityButReturnedDefensively()
    {
        var snapshot = CreateLayout(epoch: 7, owners: [0, 1, 0, 1]);
        var expected = StorageLayoutFingerprint.Compute(snapshot);
        var callerCopy = StorageLayoutFingerprint.Compute(snapshot);

        callerCopy[0] ^= 0xff;

        StorageLayoutFingerprint.Compute(snapshot).Should().Equal(expected);
        StorageLayoutFingerprint.Compute(CreateLayout(epoch: 8, owners: [0, 1, 0, 1]))
            .Should().NotEqual(expected);
        StorageLayoutFingerprint.Compute(CreateLayout(epoch: 7, owners: [1, 0, 1, 0]))
            .Should().NotEqual(expected);
    }

    [Fact]
    public void CanonicalGrainIdOrderComparesTypeBeforeKeyAndRoundTripsBytes()
    {
        var typeAKeyA = GrainId.Create("a", "a");
        var typeAKeyZ = GrainId.Create("a", "z");
        var typeBKeyA = GrainId.Create("b", "a");

        GrainIdCanonicalOrder.Compare(typeAKeyA, typeAKeyZ).Should().BeNegative();
        GrainIdCanonicalOrder.Compare(typeAKeyZ, typeBKeyA).Should().BeNegative();
        GrainIdCanonicalOrder.Comparer.Compare(typeBKeyA, typeAKeyA).Should().BePositive();
        GrainIdCanonicalOrder.GetEncodedLength(typeAKeyA).Should().Be(
            (2 * sizeof(int)) + typeAKeyA.Type.AsSpan().Length + typeAKeyA.Key.AsSpan().Length);

        using var writer = new CanonicalBinaryWriter();
        GrainIdCanonicalOrder.Write(writer, typeAKeyZ);
        var reader = new CanonicalBinaryReader(writer.WrittenSpan);
        GrainIdCanonicalOrder.Read(ref reader).Should().Be(typeAKeyZ);
        reader.EnsureFullyConsumed();
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(GrainIdCanonicalOrder.MaximumTypeBytes, GrainIdCanonicalOrder.MaximumKeyBytes)]
    public void CanonicalGrainIdBoundsRoundTripExactly(int typeLength, int keyLength)
    {
        var grainId = CreateRawGrainId(typeLength, keyLength);

        using var writer = new CanonicalBinaryWriter();
        GrainIdCanonicalOrder.Write(writer, grainId);
        var reader = new CanonicalBinaryReader(writer.WrittenSpan);

        GrainIdCanonicalOrder.Read(ref reader).Should().Be(grainId);
        GrainIdCanonicalOrder.GetEncodedLength(grainId).Should().Be(
            (2 * sizeof(int)) + typeLength + keyLength);
        reader.EnsureFullyConsumed();
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(GrainIdCanonicalOrder.MaximumTypeBytes + 1, 1)]
    [InlineData(1, 0)]
    [InlineData(1, GrainIdCanonicalOrder.MaximumKeyBytes + 1)]
    public void CanonicalGrainIdBoundsRejectEmptyAndOversizedComponents(
        int typeLength,
        int keyLength)
    {
        var grainId = CreateRawGrainId(typeLength, keyLength);

        Action measure = () => _ = GrainIdCanonicalOrder.GetEncodedLength(grainId);
        Action write = () =>
        {
            using var writer = new CanonicalBinaryWriter();
            GrainIdCanonicalOrder.Write(writer, grainId);
        };

        measure.Should().Throw<ArgumentException>();
        write.Should().Throw<ArgumentException>();
    }

    private static GrainId CreateRawGrainId(int typeLength, int keyLength)
    {
        var type = Enumerable.Repeat((byte)'t', typeLength).ToArray();
        var key = Enumerable.Repeat((byte)'k', keyLength).ToArray();
        return GrainId.Create(new GrainType(type), new IdSpan(key));
    }

    private static PartitionQueryPlan Exact(string scope, long value)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = scope,
            IndexKind = SearchableIndexKind.Hash,
            Value = new IndexValue
            {
                Kind = IndexValueKind.SignedInteger,
                SignedInteger = value,
            },
        };
    }

    private static PartitionQueryPlan Range(bool includeUpper)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = "state/type/range",
            LowerBound = new IndexValue
            {
                Kind = IndexValueKind.SignedInteger,
                SignedInteger = 1,
            },
            UpperBound = new IndexValue
            {
                Kind = IndexValueKind.SignedInteger,
                SignedInteger = 2,
            },
            IncludeLowerBound = true,
            IncludeUpperBound = includeUpper,
        };
    }

    private static StorageLayoutSnapshot CreateLayout(long epoch, int[] owners)
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = "fingerprint-provider",
            PartitionCount = owners.Distinct().Count(),
            VirtualSlotCount = owners.Length,
            Epoch = epoch,
            SlotAssignments = owners,
        });
    }
}
