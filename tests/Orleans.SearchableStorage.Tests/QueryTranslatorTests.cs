using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryTranslatorTests
{
    [Fact]
    public void CapturedValueTranslatesToExactPlan()
    {
        var city = "Helsinki";
        var plan = Translate(state => state.City == city);

        var exact = plan.Should().BeOfType<ExactQueryPlan>().Subject;
        exact.Index.PropertyName.Should().Be(nameof(QueryState.City));
        exact.Index.Kind.Should().Be(SearchableIndexKind.Hash);
        exact.Value.Text.Should().Be(city);
    }

    [Fact]
    public void SamePropertyBoundsCombineIntoOneRangePlan()
    {
        var lower = 5;
        var upper = 8;
        var plan = Translate(state => state.Salary > lower && state.Salary <= upper);

        var range = plan.Should().BeOfType<RangeQueryPlan>().Subject;
        range.LowerBound!.SignedInteger.Should().Be(lower);
        range.IncludeLowerBound.Should().BeFalse();
        range.UpperBound!.SignedInteger.Should().Be(upper);
        range.IncludeUpperBound.Should().BeTrue();
    }

    [Fact]
    public void ReversedOperandsAreNormalized()
    {
        var lower = 5;
        var upper = 8;
        var plan = Translate(state => lower <= state.Salary && upper > state.Salary);

        var range = plan.Should().BeOfType<RangeQueryPlan>().Subject;
        range.LowerBound!.SignedInteger.Should().Be(lower);
        range.IncludeLowerBound.Should().BeTrue();
        range.UpperBound!.SignedInteger.Should().Be(upper);
        range.IncludeUpperBound.Should().BeFalse();
    }

    [Theory]
    [InlineData(ExpressionType.GreaterThan, false)]
    [InlineData(ExpressionType.GreaterThanOrEqual, true)]
    public void LowerBoundInclusivityIsPreserved(ExpressionType comparison, bool inclusive)
    {
        var parameter = Expression.Parameter(typeof(QueryState), "state");
        var salary = Expression.Property(parameter, nameof(QueryState.Salary));
        var predicate = Expression.Lambda<Func<QueryState, bool>>(
            Expression.MakeBinary(comparison, salary, Expression.Constant(5)),
            parameter);

        var range = Translate(predicate).Should().BeOfType<RangeQueryPlan>().Subject;

        range.LowerBound!.SignedInteger.Should().Be(5);
        range.IncludeLowerBound.Should().Be(inclusive);
        range.UpperBound.Should().BeNull();
    }

    [Theory]
    [InlineData(ExpressionType.LessThan, false)]
    [InlineData(ExpressionType.LessThanOrEqual, true)]
    public void UpperBoundInclusivityIsPreserved(ExpressionType comparison, bool inclusive)
    {
        var parameter = Expression.Parameter(typeof(QueryState), "state");
        var salary = Expression.Property(parameter, nameof(QueryState.Salary));
        var predicate = Expression.Lambda<Func<QueryState, bool>>(
            Expression.MakeBinary(comparison, salary, Expression.Constant(8)),
            parameter);

        var range = Translate(predicate).Should().BeOfType<RangeQueryPlan>().Subject;

        range.LowerBound.Should().BeNull();
        range.UpperBound!.SignedInteger.Should().Be(8);
        range.IncludeUpperBound.Should().Be(inclusive);
    }

    [Fact]
    public void DifferentIndexedPropertiesRemainAnIntersection()
    {
        var plan = Translate(
            state => state.City == "Helsinki" && state.Salary >= 5 && state.Salary < 8);

        var and = plan.Should().BeOfType<AndQueryPlan>().Subject;
        FlattenAnd(and).Should().ContainSingle(candidate => candidate is RangeQueryPlan);
    }

    [Fact]
    public void BooleanOrRemainsAUnion()
    {
        var plan = Translate(state => state.City == "Helsinki" || state.City == "Tampere");

        plan.Should().BeOfType<OrQueryPlan>();
    }

    [Fact]
    public void MixedBooleanPlanProducesOnePartitionWireTree()
    {
        var plan = Translate(
            state => (state.City == "Helsinki" || state.Code == 7)
                && state.Salary > 5
                && state.Salary < 8);

        var wirePlan = PartitionQueryPlanFactory.Create(plan);

        wirePlan.Operation.Should().Be(PartitionQueryOperation.And);
        wirePlan.Left!.Operation.Should().Be(PartitionQueryOperation.Or);
        wirePlan.Right!.Operation.Should().Be(PartitionQueryOperation.Range);
        wirePlan.Right.LowerBound!.SignedInteger.Should().Be(5);
        wirePlan.Right.UpperBound!.SignedInteger.Should().Be(8);
    }

    [Fact]
    public void ContradictoryPredicateProducesAnEmptyPartitionWirePlan()
    {
        var plan = Translate(state => state.Salary > 8 && state.Salary < 5);

        var wirePlan = PartitionQueryPlanFactory.Create(plan);

        wirePlan.Operation.Should().Be(PartitionQueryOperation.Empty);
    }

    [Fact]
    public void MultipleWhereCallsAreCombined()
    {
        var query = Array.Empty<QueryState>()
            .AsQueryable()
            .Where(state => state.Salary >= 5)
            .Where(state => state.Salary < 8);

        var range = QueryTranslator.Translate<QueryState>("state", query.Expression)
            .Should().BeOfType<RangeQueryPlan>().Subject;

        range.LowerBound!.SignedInteger.Should().Be(5);
        range.UpperBound!.SignedInteger.Should().Be(8);
    }

    [Fact]
    public void ContradictoryBoundsProduceAnEmptyPlan()
    {
        var plan = Translate(state => state.Salary > 8 && state.Salary <= 5);

        plan.Should().BeOfType<EmptyQueryPlan>();
    }

    [Fact]
    public void ExactValueOutsideRangeProducesAnEmptyPlan()
    {
        var plan = Translate(state => state.Salary == 5 && state.Salary > 5);

        plan.Should().BeOfType<EmptyQueryPlan>();
    }

    [Fact]
    public void UnindexedPropertiesAreRejected()
    {
        var action = () => Translate(state => state.Description == "value");

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*Description*does not declare SearchableIndexAttribute*");
    }

    [Fact]
    public void RelationalComparisonOnHashIndexIsRejected()
    {
        var action = () => Translate(state => state.Code < 5);

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*requires a range index*Code*hash index*");
    }

    [Fact]
    public void UnsupportedPredicateMethodIsRejected()
    {
        var action = () => Translate(state => state.City.StartsWith("Hel", StringComparison.Ordinal));

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*Call*not supported*");
    }

    [Fact]
    public void CapturedMethodCallsAreRejected()
    {
        var action = () => Translate(state => state.City == GetCity());

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*value expression 'Call' is not supported*");
    }

    [Fact]
    public void NullComparisonsAreRejected()
    {
        var action = () => Translate(state => state.City == null);

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*Null comparisons are not supported*");
    }

    [Fact]
    public void UnsupportedLinqOperatorsAreRejected()
    {
        var expression = Array.Empty<QueryState>()
            .AsQueryable()
            .Where(state => state.Salary >= 5)
            .OrderBy(state => state.Salary)
            .Expression;

        var action = () => QueryTranslator.Translate<QueryState>("state", expression);

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*operator 'OrderBy' is not supported*");
    }

    [Fact]
    public void QueryWithoutPredicateIsRejected()
    {
        var expression = Array.Empty<QueryState>().AsQueryable().Expression;

        var action = () => QueryTranslator.Translate<QueryState>("state", expression);

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*at least one Where predicate*");
    }

    [Fact]
    public void ForeignQueryProvidersCannotUseTerminalOperation()
    {
        var query = Array.Empty<QueryState>().AsQueryable().Where(state => state.Salary >= 5);

        Action action = () => _ = query.ToGrainIdsAsync();

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*ISearchableStorageQueryClient.Query*");
    }

    private static QueryPlan Translate(Expression<Func<QueryState, bool>> predicate)
    {
        var expression = Array.Empty<QueryState>()
            .AsQueryable()
            .Where(predicate)
            .Expression;
        return QueryTranslator.Translate<QueryState>("state", expression);
    }

    private static IEnumerable<QueryPlan> FlattenAnd(QueryPlan plan)
    {
        if (plan is AndQueryPlan and)
        {
            return FlattenAnd(and.Left).Concat(FlattenAnd(and.Right));
        }

        return [plan];
    }

    private static string GetCity()
    {
        return "Helsinki";
    }

    private sealed class QueryState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Salary { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public int Code { get; init; }

        public string Description { get; init; } = string.Empty;
    }
}
