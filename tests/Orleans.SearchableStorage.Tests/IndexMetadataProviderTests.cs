using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexMetadataProviderTests
{
    public static IEnumerable<object[]> BuiltInLeafCodecCases()
    {
        yield return [typeof(string), (int)IndexKeyCodecId.String];
        yield return [typeof(char), (int)IndexKeyCodecId.String];
        yield return [typeof(sbyte), (int)IndexKeyCodecId.SignedInteger];
        yield return [typeof(short), (int)IndexKeyCodecId.SignedInteger];
        yield return [typeof(int), (int)IndexKeyCodecId.SignedInteger];
        yield return [typeof(long), (int)IndexKeyCodecId.SignedInteger];
        yield return [typeof(byte), (int)IndexKeyCodecId.UnsignedInteger];
        yield return [typeof(ushort), (int)IndexKeyCodecId.UnsignedInteger];
        yield return [typeof(uint), (int)IndexKeyCodecId.UnsignedInteger];
        yield return [typeof(ulong), (int)IndexKeyCodecId.UnsignedInteger];
        yield return [typeof(decimal), (int)IndexKeyCodecId.Decimal];
        yield return [typeof(float), (int)IndexKeyCodecId.FloatingPoint];
        yield return [typeof(double), (int)IndexKeyCodecId.FloatingPoint];
        yield return [typeof(DateTime), (int)IndexKeyCodecId.Timestamp];
        yield return [typeof(DateTimeOffset), (int)IndexKeyCodecId.Timestamp];
        yield return [typeof(Guid), (int)IndexKeyCodecId.Guid];
        yield return [typeof(bool), (int)IndexKeyCodecId.Boolean];
    }

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

    [Fact]
    public void ManagedSchemaIdentityIsDeterministicAndScopesAreGenerationBound()
    {
        var first = IndexMetadataProvider.GetSchemaDefinition<NullableNumberState>("state");
        var second = IndexMetadataProvider.GetSchemaDefinition<NullableNumberState>("state");
        var legacy = IndexMetadataProvider.Extract(
            "state",
            new NullableNumberState { Optional = 42 }).Single();
        var managed = IndexMetadataProvider.Extract(
            "state",
            new NullableNumberState { Optional = 42 },
            first.Fingerprint).Single();

        first.SchemaKey.Should().Equal(second.SchemaKey);
        first.Fingerprint.Should().Equal(second.Fingerprint);
        first.Fingerprint.Should().HaveCount(IndexSchemaDefinition.FingerprintLength);
        first.Indexes.Should().ContainSingle();
        first.Indexes[0].CodecId.Should().Be(IndexKeyCodecId.SignedInteger);
        managed.Scope.Should().StartWith(legacy.Scope);
        managed.Scope.Should().NotBe(legacy.Scope);
    }

    [Fact]
    public void ManagedSchemaIdentityCanonicalizesIndexModelOrder()
    {
        var model = IndexMetadataProvider.GetTypeModel<ReverseDeclaredSchemaState>();
        var reversedModel = model with
        {
            Indexes = model.Indexes.Reverse().ToArray(),
        };

        var first = IndexSchemaIdentity.Create("state", applicationSchemaVersion: 1, model);
        var reversed = IndexSchemaIdentity.Create(
            "state",
            applicationSchemaVersion: 1,
            reversedModel);

        first.Fingerprint.Should().Equal(reversed.Fingerprint);
        first.Indexes.Select(static index => index.Name).Should().Equal("alpha", "zeta");
        reversed.Indexes.Select(static index => index.Name).Should().Equal("alpha", "zeta");
    }

    [Fact]
    public void ManagedIndexCodecIdentifiersRemainFrozen()
    {
        Enum.GetValues<IndexKeyCodecId>().Should().Equal(
            IndexKeyCodecId.String,
            IndexKeyCodecId.SignedInteger,
            IndexKeyCodecId.UnsignedInteger,
            IndexKeyCodecId.Decimal,
            IndexKeyCodecId.FloatingPoint,
            IndexKeyCodecId.Timestamp,
            IndexKeyCodecId.Guid,
            IndexKeyCodecId.Boolean);
        ((int)IndexKeyCodecId.String).Should().Be(1);
        ((int)IndexKeyCodecId.SignedInteger).Should().Be(2);
        ((int)IndexKeyCodecId.UnsignedInteger).Should().Be(3);
        ((int)IndexKeyCodecId.Decimal).Should().Be(4);
        ((int)IndexKeyCodecId.FloatingPoint).Should().Be(5);
        ((int)IndexKeyCodecId.Timestamp).Should().Be(6);
        ((int)IndexKeyCodecId.Guid).Should().Be(7);
        ((int)IndexKeyCodecId.Boolean).Should().Be(8);
    }

    [Theory]
    [MemberData(nameof(BuiltInLeafCodecCases))]
    public void BuiltInLeafConvertersDeclareTheirFrozenCodecVersion(
        Type valueType,
        int expectedCodecId)
    {
        var found = IndexValueConverterProvider.TryGetConverter(valueType, out var converter);

        found.Should().BeTrue();
        converter.Should().NotBeNull();
        ((int)converter!.CodecId).Should().Be(expectedCodecId);
        converter.CodecVersion.Should().Be(CompatibilityManifest.GetInt(
            "wireContracts",
            "indexKeyCodec",
            "version"));
    }

    [Fact]
    public void EnumAndOptionalConvertersPropagateTheElementCodecIdentity()
    {
        var signedInteger = GetRequiredConverter(typeof(short));
        var enumConverter = GetRequiredConverter(typeof(SignedSample));
        var optionalConverter = GetRequiredConverter(typeof(short?));
        var optionalEnumConverter = GetRequiredConverter(typeof(SignedSample?));

        enumConverter.CodecId.Should().Be(signedInteger.CodecId);
        enumConverter.CodecVersion.Should().Be(signedInteger.CodecVersion);
        optionalConverter.CodecId.Should().Be(signedInteger.CodecId);
        optionalConverter.CodecVersion.Should().Be(signedInteger.CodecVersion);
        optionalEnumConverter.CodecId.Should().Be(enumConverter.CodecId);
        optionalEnumConverter.CodecVersion.Should().Be(enumConverter.CodecVersion);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ConverterRejectsNonPositiveCodecVersions(int codecVersion)
    {
        var action = () => new IndexValueConverter<int>(
            static value => IndexValue.FromSignedInteger(value),
            supportsRange: true,
            IndexKeyCodecId.SignedInteger,
            codecVersion);

        action.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(codecVersion));
    }

    [Fact]
    public void ManagedSchemaIdentityUsesThePropertyConverterVersion()
    {
        var baselineModel = IndexMetadataProvider.GetTypeModel<CodecVersionState>();
        var baselineIndex = baselineModel.Indexes.Single();

        SearchableTypeModel<CodecVersionState> CreateModel(int codecVersion)
        {
            var converter = new IndexValueConverter<int>(
                static value => IndexValue.FromSignedInteger(value),
                baselineIndex.Converter.SupportsRange,
                baselineIndex.Converter.CodecId,
                codecVersion,
                queryValueDomain: baselineIndex.Converter.QueryValueDomain);
            var index = new PropertyIndexMetadata<CodecVersionState, int>(
                baselineModel.TypeIdentity,
                baselineIndex.MemberInfo,
                baselineIndex.Name,
                baselineIndex.Kind,
                baselineIndex.ValueTypeIdentity,
                static (ref CodecVersionState state) => state.Value,
                converter);

            return baselineModel with { Indexes = [index] };
        }

        var baseline = IndexSchemaIdentity.Create("state", 1, baselineModel);
        var explicitVersionOne = IndexSchemaIdentity.Create("state", 1, CreateModel(1));
        var versionTwo = IndexSchemaIdentity.Create("state", 1, CreateModel(2));

        explicitVersionOne.Fingerprint.Should().Equal(baseline.Fingerprint);
        versionTwo.SchemaKey.Should().Equal(baseline.SchemaKey);
        versionTwo.Indexes.Should().ContainSingle();
        versionTwo.Indexes[0].CodecVersion.Should().Be(2);
        versionTwo.Fingerprint.Should().NotEqual(baseline.Fingerprint);
    }

    [Fact]
    public void SchemaFingerprintChangesWithStateNameTypeAndIndexDeclaration()
    {
        var baseline = IndexMetadataProvider.GetSchemaDefinition<NullableNumberState>("state");
        var renamedState = IndexMetadataProvider.GetSchemaDefinition<NullableNumberState>("other");
        var differentType = IndexMetadataProvider.GetSchemaDefinition<AlternateNumberState>("state");
        var differentKind = IndexMetadataProvider.GetSchemaDefinition<HashNumberState>("state");

        baseline.Fingerprint.Should().NotEqual(renamedState.Fingerprint);
        baseline.Fingerprint.Should().NotEqual(differentType.Fingerprint);
        baseline.Fingerprint.Should().NotEqual(differentKind.Fingerprint);
    }

    [Fact]
    public void RegistryRejectsTwoClrTypesForOneProviderStatePair()
    {
        var registrations = new ISearchableStateRegistration[]
        {
            new SearchableStateRegistration<NullableNumberState>("provider", "state"),
            new SearchableStateRegistration<HashNumberState>("provider", "state"),
        };

        var action = () => new SearchableStateRegistry(registrations, options: null);

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*registered*more than once*");
    }

    private static IndexValueConverter GetRequiredConverter(Type valueType)
    {
        IndexValueConverterProvider.TryGetConverter(valueType, out var converter).Should().BeTrue();
        return converter!;
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

    private sealed class ReverseDeclaredSchemaState
    {
        [SearchableIndex(SearchableIndexKind.Hash, Name = "zeta")]
        public string First { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range, Name = "alpha")]
        public int Second { get; init; }
    }

    private sealed class AlternateNumberState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public int? Optional { get; init; }
    }

    private sealed class HashNumberState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public int? Optional { get; init; }
    }

    private sealed class NullableEnumState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public SignedSample? Optional { get; init; }
    }

    private sealed class CodecVersionState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public int Value { get; init; }
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
