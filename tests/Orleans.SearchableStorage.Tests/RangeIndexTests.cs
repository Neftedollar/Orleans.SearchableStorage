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
        comparer.ComparisonCount.Should().BeLessThan(32);
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
