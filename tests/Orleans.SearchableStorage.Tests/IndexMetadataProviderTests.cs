using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexMetadataProviderTests
{
    [Fact]
    public void IndexScopeUsesUnambiguousLengthPrefixedComponents()
    {
        var state = new DelimiterState
        {
            First = "first",
            Second = "second",
        };

        var firstScope = IndexMetadataProvider.Extract("a", state)
            .Single(entry => entry.Value.Text == state.First)
            .Scope;
        var secondScope = IndexMetadataProvider.Extract("a\u001fb", state)
            .Single(entry => entry.Value.Text == state.Second)
            .Scope;

        firstScope.Should().NotBe(secondScope);
    }

    private sealed class DelimiterState
    {
        [SearchableIndex(SearchableIndexKind.Hash, Name = "b\u001fc")]
        public string First { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Hash, Name = "c")]
        public string Second { get; init; } = string.Empty;
    }
}
