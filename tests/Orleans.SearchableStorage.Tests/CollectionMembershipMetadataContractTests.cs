using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class CollectionMembershipMetadataContractTests
{
    [Fact]
    public void ExactArrayAndListShapesOmitNullCollectionsAndEmptyCollections()
    {
        IndexMetadataProvider.Extract(
                "state",
                new SupportedState { Tags = null, AudienceIds = null })
            .Should().BeEmpty();
        IndexMetadataProvider.Extract(
                "state",
                new SupportedState { Tags = [], AudienceIds = [] })
            .Should().BeEmpty();
    }

    [Fact]
    public void ExactArrayAndListShapesCanonicalizeDeduplicateAndSortElements()
    {
        var entries = IndexMetadataProvider.Extract(
            "state",
            new SupportedState
            {
                Tags = ["z", null, "", "a", "z", "a"],
                AudienceIds = [3, null, 1, 2, 3, null],
            });

        entries.Where(static entry => entry.Scope.EndsWith("4:Tags", StringComparison.Ordinal))
            .Select(static entry => entry.Value.Text)
            .Should().Equal("", "a", "z");
        entries.Where(static entry => entry.Scope.EndsWith("11:AudienceIds", StringComparison.Ordinal))
            .Select(static entry => entry.Value.SignedInteger)
            .Should().Equal(1, 2, 3);
    }

    [Fact]
    public void EachCollectionShapeAccepts64UniqueValuesAndRejectsThe65th()
    {
        var maximum = SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope;
        var accepted = IndexMetadataProvider.Extract(
            "state",
            new SupportedState
            {
                Tags = Enumerable.Range(0, maximum).Select(static value => $"v-{value:D2}").ToArray(),
                AudienceIds = Enumerable.Range(0, maximum).Select(static value => (int?)value).ToList(),
            });

        accepted.Should().HaveCount(maximum * 2);

        Action array = () => IndexMetadataProvider.Extract(
            "state",
            new SupportedState
            {
                Tags = Enumerable.Range(0, maximum + 1).Select(static value => $"v-{value:D2}").ToArray(),
                AudienceIds = [],
            });
        Action list = () => IndexMetadataProvider.Extract(
            "state",
            new SupportedState
            {
                Tags = [],
                AudienceIds = Enumerable.Range(0, maximum + 1).Select(static value => (int?)value).ToList(),
            });

        foreach (var action in new[] { array, list })
        {
            var exception = action.Should()
                .ThrowExactly<SearchableStorageCapacityExceededException>()
                .Which;
            exception.Boundary.Should().Be(StorageCapacityGuardrails.RecordScopeIndexEntries);
            exception.Actual.Should().Be(maximum + 1L);
            exception.Limit.Should().Be(maximum);
        }
    }

    [Fact]
    public void OnlyExactSzArraysAndExactListsOfSupportedScalarElementsAreAccepted()
    {
        AssertUnsupported(new ReadOnlyListState());
        AssertUnsupported(new EnumerableState());
        AssertUnsupported(new ListInterfaceState());
        AssertUnsupported(new SetState());
        AssertUnsupported(new SetInterfaceState());
        AssertUnsupported(new CustomEnumerableState());
        AssertUnsupported(new ListSubclassState());
        AssertUnsupported(new DictionaryState());
        AssertUnsupported(new ErasedArrayState());
        AssertUnsupported(new MatrixState());
        AssertUnsupported(new JaggedArrayState());
        AssertUnsupported(new NestedListState());
        AssertUnsupported(new UnsupportedElementState());

        Action range = () => IndexMetadataProvider.Extract("state", new RangeCollectionState());
        range.Should().ThrowExactly<NotSupportedException>()
            .WithMessage("*supports only Hash indexes*");
    }

    [Fact]
    public void MembershipSchemasUseV2SemanticsWithoutChangingScalarV1Goldens()
    {
        var scalar = IndexMetadataProvider.GetSchemaDefinition<ScalarGoldenState>("golden", 1);
        var membership = IndexMetadataProvider.GetSchemaDefinition<SupportedState>("golden", 1);
        var sameElementContainers = IndexMetadataProvider.GetSchemaDefinition<SameElementContainerState>(
            "golden",
            1);
        var mixed = IndexMetadataProvider.GetSchemaDefinition<MixedState>("golden", 1);
        var nextApplicationVersion = IndexMetadataProvider.GetSchemaDefinition<SupportedState>("golden", 2);
        var indexes = membership.Indexes.ToDictionary(static index => index.Name);

        IndexSchemaDefinition.DefinitionVersion.Should().Be(1);
        IndexSchemaDefinition.MembershipFingerprintFormatVersion.Should().Be(2);
        IndexSchemaDefinition.MembershipExtractorVersion.Should().Be(1);
        Convert.ToHexString(scalar.SchemaKey).Should().Be(
            "C1588F71DE04B0864E7D6107EFB6EB105796EF06D9A69FF05C544DA59FF057CB");
        Convert.ToHexString(scalar.Fingerprint).Should().Be(
            "BB8DA5199A3F440547BE5BCBC3E4303C7A7D53B9DA63452B0A77C29D774F9162");
        Convert.ToHexString(membership.SchemaKey).Should().Be(
            "B38B58BC85BA47E7203197BC1083AB196081B5FB1E1DB8B0CE0C6D8DC37C80C3");
        Convert.ToHexString(membership.Fingerprint).Should().Be(
            "729B8BAF6B90944161C1ED084A275B368E237F67D20E8BF9C26F9EDD4E134DA6");
        membership.SchemaKey.Should().Equal(nextApplicationVersion.SchemaKey);
        membership.Fingerprint.Should().NotEqual(nextApplicationVersion.Fingerprint);

        indexes[nameof(SupportedState.Tags)].Multiplicity.Should()
            .Be(IndexValueMultiplicity.CollectionMembership);
        indexes[nameof(SupportedState.Tags)].ExtractorVersion.Should().Be(1);
        indexes[nameof(SupportedState.Tags)].SupportsRange.Should().BeFalse();
        indexes[nameof(SupportedState.AudienceIds)].Multiplicity.Should()
            .Be(IndexValueMultiplicity.CollectionMembership);
        indexes[nameof(SupportedState.AudienceIds)].SupportsRange.Should().BeFalse();
        indexes[nameof(SupportedState.Tags)].ValueTypeIdentity.Should()
            .Be(IndexMetadataProvider.CreateTypeIdentity(typeof(string[])));
        indexes[nameof(SupportedState.AudienceIds)].ValueTypeIdentity.Should()
            .Be(IndexMetadataProvider.CreateTypeIdentity(typeof(List<int?>)));
        indexes[nameof(SupportedState.Tags)].ValueTypeIdentity.Should()
            .NotBe(indexes[nameof(SupportedState.AudienceIds)].ValueTypeIdentity);
        var sameElementIndexes = sameElementContainers.Indexes.ToDictionary(static index => index.Name);
        sameElementIndexes["array"].ValueTypeIdentity.Should()
            .Be(IndexMetadataProvider.CreateTypeIdentity(typeof(string[])));
        sameElementIndexes["list"].ValueTypeIdentity.Should()
            .Be(IndexMetadataProvider.CreateTypeIdentity(typeof(List<string>)));
        sameElementIndexes["array"].ValueTypeIdentity.Should()
            .NotBe(sameElementIndexes["list"].ValueTypeIdentity);
        var mixedIndexes = mixed.Indexes.ToDictionary(static index => index.Name);
        mixedIndexes[nameof(MixedState.City)].Multiplicity.Should().Be(IndexValueMultiplicity.Scalar);
        mixedIndexes[nameof(MixedState.City)].ExtractorVersion.Should().Be(0);
        mixedIndexes[nameof(MixedState.City)].SupportsRange.Should().BeTrue();
        mixedIndexes[nameof(MixedState.Tags)].Multiplicity.Should()
            .Be(IndexValueMultiplicity.CollectionMembership);
    }

    [Fact]
    public void CanonicalAliasesDeduplicateBeforeThePerScopeLimit()
    {
        var instant = new DateTimeOffset(2026, 8, 12, 10, 0, 0, TimeSpan.Zero);
        var alias = instant.ToOffset(TimeSpan.FromHours(3));

        var entries = IndexMetadataProvider.Extract(
            "state",
            new CanonicalAliasState { Instants = [instant, alias] });

        entries.Should().ContainSingle();
        entries[0].Value.UtcTicks.Should().Be(instant.UtcTicks);
    }

    [Fact]
    public void AggregateCollectionEntriesFlowIntoTheExistingAtomicRecordGuardrail()
    {
        var values = Enumerable.Range(
                0,
                SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope)
            .ToArray();
        var entries = IndexMetadataProvider.Extract(
            "state",
            new AggregateState
            {
                First = values,
                Second = values,
                Third = values,
                Fourth = values,
                Fifth = values,
            });
        var request = new StorageWriteRequest
        {
            RecordKey = "state/record",
            GrainId = GrainId.Create("aggregate", "record"),
            Payload = [1],
            IndexEntries = [.. entries],
            Persistence = new StoragePersistenceSettings(),
        };

        Action validate = () => StorageCapacityGuardrails.ValidateWriteRequest(request);

        entries.Should().HaveCount(5 * SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope);
        var exception = validate.Should()
            .ThrowExactly<SearchableStorageCapacityExceededException>()
            .Which;
        exception.Boundary.Should().Be(StorageCapacityGuardrails.RecordIndexEntries);
        exception.Actual.Should().Be(entries.Count);
        exception.Limit.Should().Be(SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord);
    }

    private static void AssertUnsupported<TState>(TState state)
    {
        Action action = () => IndexMetadataProvider.Extract("state", state);

        action.Should().ThrowExactly<NotSupportedException>()
            .WithMessage("*unsupported type*");
    }

    private sealed class SupportedState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string?[]? Tags { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public List<int?>? AudienceIds { get; init; }
    }

    private sealed class ScalarGoldenState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Salary { get; init; }
    }

    private sealed class SameElementContainerState
    {
        [SearchableIndex(SearchableIndexKind.Hash, Name = "array")]
        public string[] Array { get; init; } = [];

        [SearchableIndex(SearchableIndexKind.Hash, Name = "list")]
        public List<string> List { get; init; } = [];
    }

    private sealed class MixedState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public string City { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Hash)]
        public string[] Tags { get; init; } = [];
    }

    private sealed class CanonicalAliasState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public List<DateTimeOffset> Instants { get; init; } = [];
    }

    private sealed class AggregateState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public int[] First { get; init; } = [];

        [SearchableIndex(SearchableIndexKind.Hash)]
        public int[] Second { get; init; } = [];

        [SearchableIndex(SearchableIndexKind.Hash)]
        public int[] Third { get; init; } = [];

        [SearchableIndex(SearchableIndexKind.Hash)]
        public int[] Fourth { get; init; } = [];

        [SearchableIndex(SearchableIndexKind.Hash)]
        public int[] Fifth { get; init; } = [];
    }

    private sealed class ReadOnlyListState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public IReadOnlyList<int> Values { get; init; } = [];
    }

    private sealed class EnumerableState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public IEnumerable<int> Values { get; init; } = [];
    }

    private sealed class ListInterfaceState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public IList<int> Values { get; init; } = [];
    }

    private sealed class SetState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public HashSet<int> Values { get; init; } = [];
    }

    private sealed class SetInterfaceState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public ISet<int> Values { get; init; } = new HashSet<int>();
    }

    private sealed class CustomEnumerableState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public CustomEnumerable Values { get; init; } = new();
    }

    private sealed class ListSubclassState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public CustomList Values { get; init; } = [];
    }

    private sealed class DictionaryState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public Dictionary<int, int> Values { get; init; } = [];
    }

    private sealed class ErasedArrayState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public Array Values { get; init; } = Array.Empty<int>();
    }

    private sealed class MatrixState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public int[,] Values { get; init; } = new int[0, 0];
    }

    private sealed class JaggedArrayState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public int[][] Values { get; init; } = [];
    }

    private sealed class NestedListState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public List<List<int>> Values { get; init; } = [];
    }

    private sealed class UnsupportedElementState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public List<TimeSpan> Values { get; init; } = [];
    }

    private sealed class RangeCollectionState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public int[] Values { get; init; } = [];
    }

    private sealed class CustomEnumerable : IEnumerable<int>
    {
        public IEnumerator<int> GetEnumerator() => Enumerable.Empty<int>().GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    private sealed class CustomList : List<int>;
}
