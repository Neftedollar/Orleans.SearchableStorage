using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class RangeIndexTests
{
    [Fact]
    public void HighEndRangeUsesLogarithmicLowerBoundSeek()
    {
        const int bucketCount = 4_096;
        var comparer = new CountingComparer();
        var index = new RangeIndex(CreateBuckets(bucketCount), comparer);
        var matches = new HashSet<string>(StringComparer.Ordinal);
        comparer.Reset();

        index.UnionRange(
            IndexValue.FromSignedInteger(4_000),
            IndexValue.FromSignedInteger(4_002),
            includeLowerBound: true,
            includeUpperBound: true,
            destination: matches);

        matches.Should().BeEquivalentTo(["record-4000", "record-4001", "record-4002"]);
        comparer.ComparisonCount.Should().BeLessThan(64);
    }

    [Fact]
    public void ExclusiveBoundsSkipMatchingBuckets()
    {
        var index = new RangeIndex(CreateBuckets(4));
        var matches = new HashSet<string>(StringComparer.Ordinal);

        index.UnionRange(
            IndexValue.FromSignedInteger(1),
            IndexValue.FromSignedInteger(3),
            includeLowerBound: false,
            includeUpperBound: false,
            destination: matches);

        matches.Should().ContainSingle().Which.Should().Be("record-2");
    }

    [Fact]
    public void MissingLowerBoundStartsAtFirstBucket()
    {
        var index = new RangeIndex(CreateBuckets(4));
        var matches = new HashSet<string>(StringComparer.Ordinal);

        index.UnionRange(
            lowerBound: null,
            IndexValue.FromSignedInteger(1),
            includeLowerBound: false,
            includeUpperBound: true,
            destination: matches);

        matches.Should().BeEquivalentTo(["record-0", "record-1"]);
    }

    [Fact]
    public void MissingUpperBoundContinuesThroughLastBucket()
    {
        var index = new RangeIndex(CreateBuckets(4));
        var matches = new HashSet<string>(StringComparer.Ordinal);

        index.UnionRange(
            IndexValue.FromSignedInteger(2),
            upperBound: null,
            includeLowerBound: false,
            includeUpperBound: false,
            destination: matches);

        matches.Should().ContainSingle().Which.Should().Be("record-3");
    }

    [Fact]
    public void ReversedBoundsAreRejected()
    {
        var index = new RangeIndex(CreateBuckets(4));
        var matches = new HashSet<string>(StringComparer.Ordinal);

        var action = () => index.UnionRange(
            IndexValue.FromSignedInteger(3),
            IndexValue.FromSignedInteger(1),
            includeLowerBound: true,
            includeUpperBound: true,
            destination: matches);

        action.Should().Throw<ArgumentException>()
            .WithParameterName("lowerBound");
    }

    [Fact]
    public void ComparerEqualBucketsAreMergedDuringConstruction()
    {
        var first = IndexValue.FromSignedInteger(1);
        var second = IndexValue.FromSignedInteger(2);
        var buckets = new Dictionary<IndexValue, HashSet<string>>
        {
            [first] = new HashSet<string>(["first"], StringComparer.Ordinal),
            [second] = new HashSet<string>(["second"], StringComparer.Ordinal),
        };

        var index = new RangeIndex(buckets, new DecadeComparer());

        index.TryGetValue(first, out var records).Should().BeTrue();
        records!.Should().BeEquivalentTo(["first", "second"]);
    }

    [Fact]
    public void AddCreatesBucketsAndIsIdempotentWithinABucket()
    {
        var index = new RangeIndex(CreateBuckets(0));
        var value = IndexValue.FromSignedInteger(7);

        index.Add(value, "first").Should().BeTrue();
        index.Add(value, "second").Should().BeTrue();
        index.Add(value, "first").Should().BeFalse();

        index.TryGetValue(value, out var records).Should().BeTrue();
        records!.Should().BeEquivalentTo(["first", "second"]);
    }

    [Fact]
    public void AddUsesOrderingEqualityToFindAnExistingBucket()
    {
        var index = new RangeIndex(CreateBuckets(0), new DecadeComparer());

        index.Add(IndexValue.FromSignedInteger(11), "first").Should().BeTrue();
        index.Add(IndexValue.FromSignedInteger(19), "second").Should().BeTrue();
        index.Add(IndexValue.FromSignedInteger(15), "first").Should().BeFalse();

        index.TryGetValue(IndexValue.FromSignedInteger(12), out var records).Should().BeTrue();
        records!.Should().BeEquivalentTo(["first", "second"]);
    }

    [Fact]
    public void RemoveDeletesOnlyTheRequestedRecordAndDropsEmptyBuckets()
    {
        var value = IndexValue.FromSignedInteger(7);
        var index = new RangeIndex(new Dictionary<IndexValue, HashSet<string>>
        {
            [value] = new HashSet<string>(["first", "second"], StringComparer.Ordinal),
        });

        index.Remove(value, "first").Should().BeTrue();
        index.Remove(value, "missing").Should().BeFalse();
        index.TryGetValue(value, out var remaining).Should().BeTrue();
        remaining!.Should().ContainSingle().Which.Should().Be("second");

        index.Remove(value, "second").Should().BeTrue();
        index.TryGetValue(value, out _).Should().BeFalse();
        index.Remove(value, "second").Should().BeFalse();
    }

    [Fact]
    public void RemoveUsesOrderingEqualityAndRetainsOtherRecords()
    {
        var index = new RangeIndex(CreateBuckets(0), new DecadeComparer());
        index.Add(IndexValue.FromSignedInteger(11), "first");
        index.Add(IndexValue.FromSignedInteger(19), "second");

        index.Remove(IndexValue.FromSignedInteger(15), "first").Should().BeTrue();

        index.TryGetValue(IndexValue.FromSignedInteger(12), out var remaining).Should().BeTrue();
        remaining!.Should().ContainSingle().Which.Should().Be("second");
    }

    [Theory]
    [InlineData(true, true, true)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, false)]
    public void EqualBoundsRequireBothEndpointsToBeInclusive(
        bool includeLowerBound,
        bool includeUpperBound,
        bool shouldMatch)
    {
        var index = new RangeIndex(CreateBuckets(3));
        var matches = new HashSet<string>(StringComparer.Ordinal);
        var bound = IndexValue.FromSignedInteger(1);

        index.UnionRange(
            bound,
            bound,
            includeLowerBound,
            includeUpperBound,
            matches);

        matches.Contains("record-1").Should().Be(shouldMatch);
        matches.Should().HaveCount(shouldMatch ? 1 : 0);
    }

    [Theory]
    [InlineData(-3, -1)]
    [InlineData(4, 8)]
    public void RangeOutsideIndexedValuesReturnsNoRecords(long lower, long upper)
    {
        var index = new RangeIndex(CreateBuckets(4));
        var matches = new HashSet<string>(StringComparer.Ordinal);

        index.UnionRange(
            IndexValue.FromSignedInteger(lower),
            IndexValue.FromSignedInteger(upper),
            includeLowerBound: true,
            includeUpperBound: true,
            matches);

        matches.Should().BeEmpty();
    }

    [Fact]
    public void EmptyIndexAcceptsUnboundedRange()
    {
        var index = new RangeIndex(CreateBuckets(0));
        var matches = new HashSet<string>(["existing"], StringComparer.Ordinal);

        index.UnionRange(
            lowerBound: null,
            upperBound: null,
            includeLowerBound: false,
            includeUpperBound: false,
            matches);

        matches.Should().ContainSingle().Which.Should().Be("existing");
    }

    [Fact]
    public void ExclusiveBoundsUseComparerEquality()
    {
        var buckets = new Dictionary<IndexValue, HashSet<string>>
        {
            [IndexValue.FromSignedInteger(1)] = new HashSet<string>(["first-decade"], StringComparer.Ordinal),
            [IndexValue.FromSignedInteger(11)] = new HashSet<string>(["second-decade"], StringComparer.Ordinal),
            [IndexValue.FromSignedInteger(21)] = new HashSet<string>(["third-decade"], StringComparer.Ordinal),
        };
        var index = new RangeIndex(buckets, new DecadeComparer());
        var matches = new HashSet<string>(StringComparer.Ordinal);

        index.UnionRange(
            IndexValue.FromSignedInteger(8),
            IndexValue.FromSignedInteger(19),
            includeLowerBound: false,
            includeUpperBound: true,
            matches);

        matches.Should().ContainSingle().Which.Should().Be("second-decade");
    }

    [Fact]
    public void AddedAndRemovedBucketsRemainOrderedForRangeQueries()
    {
        var index = new RangeIndex(CreateBuckets(0));
        index.Add(IndexValue.FromSignedInteger(30), "record-30");
        index.Add(IndexValue.FromSignedInteger(10), "record-10");
        index.Add(IndexValue.FromSignedInteger(20), "record-20");
        index.Remove(IndexValue.FromSignedInteger(20), "record-20");
        index.Add(IndexValue.FromSignedInteger(25), "record-25");
        var matches = new HashSet<string>(StringComparer.Ordinal);

        index.UnionRange(
            IndexValue.FromSignedInteger(15),
            IndexValue.FromSignedInteger(30),
            includeLowerBound: true,
            includeUpperBound: false,
            matches);

        matches.Should().ContainSingle().Which.Should().Be("record-25");
    }

    [Fact]
    public void MutationsUseLogarithmicBucketLookup()
    {
        const int bucketCount = 4_096;
        var comparer = new CountingComparer();
        var index = new RangeIndex(CreateBuckets(bucketCount), comparer);
        var addedValue = IndexValue.FromSignedInteger(bucketCount + 1);
        comparer.Reset();

        index.Add(addedValue, "added").Should().BeTrue();

        comparer.ComparisonCount.Should().BeLessThan(64);
        comparer.Reset();

        index.Remove(addedValue, "added").Should().BeTrue();

        comparer.ComparisonCount.Should().BeLessThan(64);
    }

    [Fact]
    public void RecordKeysAlwaysUseOrdinalEquality()
    {
        var value = IndexValue.FromSignedInteger(1);
        var index = new RangeIndex(new Dictionary<IndexValue, HashSet<string>>
        {
            [value] = new HashSet<string>(["record"], StringComparer.OrdinalIgnoreCase),
        });

        index.Add(value, "RECORD").Should().BeTrue();

        index.TryGetValue(value, out var records).Should().BeTrue();
        records!.Should().BeEquivalentTo(["record", "RECORD"]);
    }

    private static Dictionary<IndexValue, HashSet<string>> CreateBuckets(int count)
    {
        return Enumerable.Range(0, count)
            .ToDictionary(
                static value => IndexValue.FromSignedInteger(value),
                static value => new HashSet<string>([$"record-{value}"], StringComparer.Ordinal));
    }

    private sealed class CountingComparer : IComparer<IndexValue>
    {
        public int ComparisonCount { get; private set; }

        public int Compare(IndexValue? x, IndexValue? y)
        {
            ComparisonCount++;
            return Comparer<IndexValue>.Default.Compare(x, y);
        }

        public void Reset()
        {
            ComparisonCount = 0;
        }
    }

    private sealed class DecadeComparer : IComparer<IndexValue>
    {
        public int Compare(IndexValue? x, IndexValue? y)
        {
            if (x is null || y is null)
            {
                return Comparer<IndexValue>.Default.Compare(x, y);
            }

            return (x.SignedInteger / 10).CompareTo(y.SignedInteger / 10);
        }
    }
}
