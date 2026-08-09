using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexValueTests
{
    [Theory]
    [InlineData(-10, 0)]
    [InlineData(0, 10)]
    [InlineData(10, 20)]
    public void SignedIntegerValuesPreserveNumericOrder(long lower, long upper)
    {
        IndexValue.Create(lower).CompareTo(IndexValue.Create(upper)).Should().BeNegative();
    }

    [Fact]
    public void NonUtcDateTimeValuesAreRejected()
    {
        var action = () => IndexValue.Create(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified));

        action.Should().Throw<ArgumentException>()
            .WithMessage("*DateTimeKind.Utc*");
    }
}
