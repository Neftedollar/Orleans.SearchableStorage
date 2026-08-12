using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Tests;

public sealed class OneZeroContractMatrixTests
{
    public static TheoryData<Type, bool> SupportedIndexTypes => new()
    {
        { typeof(string), true },
        { typeof(char), true },
        { typeof(sbyte), true },
        { typeof(short), true },
        { typeof(int), true },
        { typeof(long), true },
        { typeof(byte), true },
        { typeof(ushort), true },
        { typeof(uint), true },
        { typeof(ulong), true },
        { typeof(decimal), true },
        { typeof(float), true },
        { typeof(double), true },
        { typeof(DateTime), true },
        { typeof(DateTimeOffset), true },
        { typeof(Guid), false },
        { typeof(bool), false },
        { typeof(SignedContractEnum), true },
        { typeof(UnsignedContractEnum), true },
        { typeof(int?), true },
        { typeof(Guid?), false },
        { typeof(bool?), false },
        { typeof(SignedContractEnum?), true },
    };

    public static TheoryData<Type> UnsupportedIndexTypes => new()
    {
        typeof(TimeSpan),
        typeof(DateOnly),
        typeof(TimeOnly),
        typeof(Half),
        typeof(Int128),
        typeof(UInt128),
        typeof(IntPtr),
        typeof(UIntPtr),
        typeof(object),
        typeof(byte[]),
        typeof(List<string>),
    };

    [Theory]
    [MemberData(nameof(SupportedIndexTypes))]
    public void CurrentScalarIndexTypeMatrixReportsRangeCapability(
        Type type,
        bool supportsRange)
    {
        IndexValue.IsSupported(type).Should().BeTrue();
        IndexValue.IsRangeSupported(type).Should().Be(supportsRange);
    }

    [Theory]
    [MemberData(nameof(UnsupportedIndexTypes))]
    public void RepresentativeTypesOutsideTheCurrentImplementationRemainUnsupported(Type type)
    {
        IndexValue.IsSupported(type).Should().BeFalse();
        IndexValue.IsRangeSupported(type).Should().BeFalse();
    }

    [Fact]
    public void FocusedPredicateOperatorsTranslateToTheDocumentedPlans()
    {
        var exact = Translate(state => state.City == "Helsinki")
            .Should().BeOfType<ExactQueryPlan>().Subject;
        var less = Translate(state => state.Score < 10)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var lessOrEqual = Translate(state => state.Score <= 10)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var greater = Translate(state => state.Score > 10)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var reversedGreaterOrEqual = Translate(state => 10 <= state.Score)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var conjunction = Translate(state => state.City == "Helsinki" && state.Score >= 10);
        var disjunction = Translate(state => state.City == "Helsinki" || state.Score >= 10);

        exact.Index.Kind.Should().Be(SearchableIndexKind.Hash);
        less.IncludeUpperBound.Should().BeFalse();
        lessOrEqual.IncludeUpperBound.Should().BeTrue();
        greater.IncludeLowerBound.Should().BeFalse();
        reversedGreaterOrEqual.IncludeLowerBound.Should().BeTrue();
        conjunction.Should().BeOfType<AndQueryPlan>();
        disjunction.Should().BeOfType<OrQueryPlan>();
    }

    [Fact]
    public void RepeatedWhereClausesComposeAsAnd()
    {
        var query = Array.Empty<ContractState>()
            .AsQueryable()
            .Where(state => state.City == "Helsinki")
            .Where(state => state.Score >= 10);

        var plan = QueryTranslator.Translate<ContractState>("state", query.Expression);

        plan.Should().BeOfType<AndQueryPlan>();
    }

    [Fact]
    public void IdentifierQueriesRequireAFilterButFacetsMayUseTheUnfilteredRoot()
    {
        var expression = Array.Empty<ContractState>().AsQueryable().Expression;

        Action translateIdentifiers = () => QueryTranslator.Translate<ContractState>(
            "state",
            expression);
        var facetPlan = QueryTranslator.TranslateFacet<ContractState>("state", expression);

        translateIdentifiers.Should().Throw<NotSupportedException>()
            .WithMessage("*at least one Where or WhereIn filter*");
        facetPlan.Should().BeSameAs(AllQueryPlan.Instance);
    }

    [Fact]
    public void ShapesNotImplementedByTheCurrentTranslatorFailInsteadOfFallingBack()
    {
        var acceptedValues = new[] { 7, 11 };
        Expression<Func<ContractState, bool>>[] unsupportedPredicates =
        [
            state => state.Score != 7,
            state => !state.Enabled,
            state => state.Enabled,
            state => state.City.StartsWith("Hel"),
            state => acceptedValues.Contains(state.Score),
            state => state.Score + 1 > 7,
            state => state.Nested.Value == 7,
            state => state.Unindexed == 7,
            state => state.Score == state.Unindexed,
            state => state.City == null,
        ];

        foreach (var predicate in unsupportedPredicates)
        {
            var translate = () => Translate(predicate);

            translate.Should().Throw<NotSupportedException>();
        }
    }

    [Fact]
    public void GeneralQueryableOperatorsFailInsteadOfFallingBack()
    {
        var filtered = Array.Empty<ContractState>()
            .AsQueryable()
            .Where(state => state.Score >= 10);
        Expression[] unsupportedQueries =
        [
            filtered.OrderBy(state => state.Score).Expression,
            filtered.Skip(1).Expression,
            filtered.Take(1).Expression,
            filtered.Select(state => state.Score).Expression,
        ];

        foreach (var expression in unsupportedQueries)
        {
            var translate = () => QueryTranslator.Translate<ContractState>("state", expression);

            translate.Should().Throw<NotSupportedException>();
        }
    }

    private static QueryPlan Translate(Expression<Func<ContractState, bool>> predicate)
    {
        var expression = Array.Empty<ContractState>()
            .AsQueryable()
            .Where(predicate)
            .Expression;
        return QueryTranslator.Translate<ContractState>("state", expression);
    }

    private sealed class ContractState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Score { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public bool Enabled { get; init; }

        public int Unindexed { get; init; }

        public NestedState Nested { get; init; } = new();
    }

    private sealed class NestedState
    {
        public int Value { get; init; }
    }

    private enum SignedContractEnum : short
    {
        Value = 1,
    }

    private enum UnsignedContractEnum : ulong
    {
        Value = 1,
    }
}
