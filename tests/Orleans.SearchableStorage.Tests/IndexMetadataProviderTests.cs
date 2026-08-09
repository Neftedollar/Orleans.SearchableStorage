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

    [Fact]
    public void IndexScopesAreCachedPerStateName()
    {
        var state = new NullableState { Optional = "value" };

        var firstScope = IndexMetadataProvider.Extract("state", state).Single().Scope;
        var secondScope = IndexMetadataProvider.Extract("state", state).Single().Scope;

        ReferenceEquals(firstScope, secondScope).Should().BeTrue();
    }

    [Fact]
    public void ConstructedGenericScopeUsesVersionIndependentRecursiveTypeIdentity()
    {
        var integerScope = IndexMetadataProvider.Extract(
            "state",
            new GenericState<List<int>> { Value = "value" })
            .Single()
            .Scope;
        var longScope = IndexMetadataProvider.Extract(
            "state",
            new GenericState<List<long>> { Value = "value" })
            .Single()
            .Scope;

        integerScope.Should().NotContain("Version=");
        integerScope.Should().NotBe(longScope);
    }

    [Fact]
    public void OpenGenericTypesDoNotHavePersistedIdentities()
    {
        var action = () => IndexMetadataProvider.CreateTypeIdentity(typeof(GenericState<>));

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*closed persisted identity*");
    }

    [Fact]
    public void ArrayTypeIdentitiesEncodeShapeAndElementType()
    {
        var integerVector = IndexMetadataProvider.CreateTypeIdentity(typeof(int[]));
        var integerMatrix = IndexMetadataProvider.CreateTypeIdentity(typeof(int[,]));
        var longVector = IndexMetadataProvider.CreateTypeIdentity(typeof(long[]));

        integerVector.Should().NotContain("Version=");
        integerVector.Should().NotBe(integerMatrix);
        integerVector.Should().NotBe(longVector);
    }

    [Fact]
    public void NullIndexedValuesAreOmitted()
    {
        var entries = IndexMetadataProvider.Extract("state", new NullableState());

        entries.Should().BeEmpty();
    }

    [Fact]
    public void NullableValueTypesUseTheirUnderlyingIndexConverter()
    {
        var entries = IndexMetadataProvider.Extract(
            "state",
            new NullableNumberState { Optional = 42 });
        var selected = IndexMetadataProvider.GetSelectedIndex<NullableNumberState, int?>(
            "state",
            state => state.Optional);

        entries.Should().ContainSingle();
        entries[0].Value.Kind.Should().Be(IndexValueKind.SignedInteger);
        entries[0].Value.SignedInteger.Should().Be(42);
        selected.Converter.RuntimeValueType.Should().Be<int>();
        selected.Converter.ConvertObject(42)!.SignedInteger.Should().Be(42);
    }

    [Fact]
    public void NullNullableValueTypesAreOmitted()
    {
        var entries = IndexMetadataProvider.Extract(
            "state",
            new NullableNumberState { Optional = null });

        entries.Should().BeEmpty();
    }

    [Fact]
    public void NullableEnumPropertiesComposeOptionalAndEnumConverters()
    {
        var entries = IndexMetadataProvider.Extract(
            "state",
            new NullableEnumState { Optional = SignedSample.Negative });

        entries.Should().ContainSingle();
        entries[0].Value.Kind.Should().Be(IndexValueKind.SignedInteger);
        entries[0].Value.SignedInteger.Should().Be(-1);
    }

    [Fact]
    public void NullStateProducesNoIndexEntries()
    {
        var entries = IndexMetadataProvider.Extract<NullableState>("state", null!);

        entries.Should().BeEmpty();
    }

    [Fact]
    public void UnmarkedPropertiesCannotBeSelected()
    {
        var action = () => IndexMetadataProvider.GetSelectedIndex<SelectorState, string>(
            "state",
            state => state.Unindexed);

        action.Should().Throw<ArgumentException>()
            .WithParameterName("expression");
    }

    [Fact]
    public void NestedPropertySelectorsAreRejected()
    {
        var action = () => IndexMetadataProvider.GetSelectedIndex<SelectorState, int>(
            "state",
            state => state.Indexed.Length);

        action.Should().Throw<ArgumentException>()
            .WithParameterName("expression");
    }

    [Fact]
    public void ConvertedSelectorsResolveTheIndexedProperty()
    {
        var selected = IndexMetadataProvider.GetSelectedIndex<SelectorState, object>(
            "state",
            state => state.Indexed);

        selected.Converter.ValueType.Should().Be<string>();
    }

    [Fact]
    public void InheritedIndexedPropertiesAreResolvedThroughTheTypeShape()
    {
        var state = new DerivedState { Score = 17 };

        var entries = IndexMetadataProvider.Extract("state", state);
        entries.Should().ContainSingle();
        entries[0].Value.SignedInteger.Should().Be(17);

        var selected = IndexMetadataProvider.GetSelectedIndex<DerivedState, int>(
            "state",
            candidate => candidate.Score);

        selected.Kind.Should().Be(SearchableIndexKind.Range);
    }

    [Fact]
    public async Task TypeModelsAreSharedAcrossConcurrentCallers()
    {
        var models = await Task.WhenAll(
            Enumerable.Range(0, 32)
                .Select(_ => Task.Run(IndexMetadataProvider.GetTypeModel<ConcurrentModelState>)));

        models.All(model => ReferenceEquals(model, models[0])).Should().BeTrue();
    }

    [Fact]
    public void DuplicateIndexNamesAreRejected()
    {
        var action = () => IndexMetadataProvider.Extract("state", new DuplicateNameState());

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*duplicate index name*");
    }

    [Fact]
    public void UnsupportedIndexTypesAreRejected()
    {
        var action = () => IndexMetadataProvider.Extract("state", new UnsupportedState());

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*unsupported type*");
    }

    [Fact]
    public void UnorderedTypesCannotUseRangeIndexes()
    {
        var action = () => IndexMetadataProvider.Extract("state", new UnorderedRangeState());

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*unordered type*");
    }

    [Fact]
    public void IndexedPropertiesMustBeReadable()
    {
        var action = () => IndexMetadataProvider.Extract("state", new WriteOnlyState());

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*readable instance property*");
    }

    private sealed class DelimiterState
    {
        [SearchableIndex(SearchableIndexKind.Hash, Name = "b\u001fc")]
        public string First { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Hash, Name = "c")]
        public string Second { get; init; } = string.Empty;
    }

    private sealed class NullableState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string? Optional { get; init; }
    }

    private sealed class GenericState<T>
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Value { get; init; } = string.Empty;
    }

    private sealed class NullableNumberState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public int? Optional { get; init; }
    }

    private sealed class NullableEnumState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public SignedSample? Optional { get; init; }
    }

    private sealed class SelectorState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Indexed { get; init; } = string.Empty;

        public string Unindexed { get; init; } = string.Empty;
    }

    private sealed class DuplicateNameState
    {
        [SearchableIndex(SearchableIndexKind.Hash, Name = "duplicate")]
        public string First { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Hash, Name = "duplicate")]
        public string Second { get; init; } = string.Empty;
    }

    private sealed class UnsupportedState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public TimeSpan Duration { get; init; }
    }

    private sealed class UnorderedRangeState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public Guid Identifier { get; init; }
    }

    private sealed class WriteOnlyState
    {
        private readonly List<string> _values = [];

        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Value
        {
            set
            {
                _values.Add(value);
            }
        }
    }

    private class BaseState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public int Score { get; init; }
    }

    private sealed class DerivedState : BaseState
    {
    }

    private sealed class ConcurrentModelState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Value { get; init; } = string.Empty;
    }

    private enum SignedSample : short
    {
        Negative = -1,
    }
}
