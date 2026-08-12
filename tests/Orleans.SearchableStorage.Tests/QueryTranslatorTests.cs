using System.Linq.Expressions;
using System.Reflection;
using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
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
    public void CompilerPromotedIntegralAndEnumValuesTranslateToIndexDomains()
    {
        short minimumLevel = 4;
        long minimumNumber = 5;

        var age = Translate<PromotionState>(state => state.Age == 5)
            .Should().BeOfType<ExactQueryPlan>().Subject;
        var level = Translate<PromotionState>(state => state.Level >= minimumLevel)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var number = Translate<PromotionState>(state => minimumNumber <= state.Number)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var code = Translate<PromotionState>(state => state.Code == 'A')
            .Should().BeOfType<ExactQueryPlan>().Subject;
        var signedStatus = Translate<PromotionState>(state => state.Status == SignedStatus.Active)
            .Should().BeOfType<ExactQueryPlan>().Subject;
        var unsignedStatus = Translate<PromotionState>(state => state.UnsignedStatus == UnsignedStatus.Active)
            .Should().BeOfType<ExactQueryPlan>().Subject;
        var optionalStatus = Translate<PromotionState>(state => state.OptionalStatus == SignedStatus.Active)
            .Should().BeOfType<ExactQueryPlan>().Subject;
        var ratio = Translate<PromotionState>(state => state.Ratio == 5d)
            .Should().BeOfType<ExactQueryPlan>().Subject;

        age.Value.Kind.Should().Be(IndexValueKind.UnsignedInteger);
        age.Value.UnsignedInteger.Should().Be(5);
        level.LowerBound!.SignedInteger.Should().Be(minimumLevel);
        level.IncludeLowerBound.Should().BeTrue();
        number.LowerBound!.SignedInteger.Should().Be(minimumNumber);
        number.IncludeLowerBound.Should().BeTrue();
        code.Value.Kind.Should().Be(IndexValueKind.String);
        code.Value.Text.Should().Be("A");
        signedStatus.Value.SignedInteger.Should().Be((sbyte)SignedStatus.Active);
        unsignedStatus.Value.UnsignedInteger.Should().Be((ushort)UnsignedStatus.Active);
        optionalStatus.Value.SignedInteger.Should().Be((sbyte)SignedStatus.Active);
        ratio.Value.FloatingPoint.Should().Be(5d);
    }

    [Fact]
    public void IntegralBoundsOutsideTheIndexedDomainSaturateWithoutWrapping()
    {
        var aboveAgeDomain = 300;
        var belowAgeDomain = -1;
        var aboveNumberDomain = long.MaxValue;
        var floatingValueAtDecimalLimit = (double)decimal.MaxValue;
        var allBelowUpper = Translate<PromotionState>(state => state.Age < aboveAgeDomain)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var allAboveLower = Translate<PromotionState>(state => state.Age > belowAgeDomain)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var reversedAll = Translate<PromotionState>(state => aboveAgeDomain > state.Age)
            .Should().BeOfType<RangeQueryPlan>().Subject;

        allBelowUpper.UpperBound!.UnsignedInteger.Should().Be(byte.MaxValue);
        allBelowUpper.IncludeUpperBound.Should().BeTrue();
        allAboveLower.LowerBound!.UnsignedInteger.Should().Be(byte.MinValue);
        allAboveLower.IncludeLowerBound.Should().BeTrue();
        reversedAll.UpperBound!.UnsignedInteger.Should().Be(byte.MaxValue);
        Translate<PromotionState>(state => state.Age == aboveAgeDomain).Should().BeOfType<EmptyQueryPlan>();
        Translate<PromotionState>(state => state.Age > aboveAgeDomain).Should().BeOfType<EmptyQueryPlan>();
        Translate<PromotionState>(state => state.Age < belowAgeDomain).Should().BeOfType<EmptyQueryPlan>();
        Translate<PromotionState>(state => state.Number == aboveNumberDomain).Should().BeOfType<EmptyQueryPlan>();
        var floatingUpperDomain = Translate<PromotionState>(state => state.Number < floatingValueAtDecimalLimit)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        floatingUpperDomain.UpperBound!.SignedInteger.Should().Be(int.MaxValue);
        floatingUpperDomain.IncludeUpperBound.Should().BeTrue();
    }

    [Fact]
    public void FractionalIntegralBoundsAreRoundedAccordingToClrComparisonSemantics()
    {
        var bound = 5.2m;

        var greater = Translate<PromotionState>(state => state.Number > bound)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var greaterOrEqual = Translate<PromotionState>(state => state.Number >= bound)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var less = Translate<PromotionState>(state => state.Number < bound)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var lessOrEqual = Translate<PromotionState>(state => state.Number <= bound)
            .Should().BeOfType<RangeQueryPlan>().Subject;

        greater.LowerBound!.SignedInteger.Should().Be(6);
        greater.IncludeLowerBound.Should().BeTrue();
        greaterOrEqual.LowerBound!.SignedInteger.Should().Be(6);
        greaterOrEqual.IncludeLowerBound.Should().BeTrue();
        less.UpperBound!.SignedInteger.Should().Be(5);
        less.IncludeUpperBound.Should().BeTrue();
        lessOrEqual.UpperBound!.SignedInteger.Should().Be(5);
        lessOrEqual.IncludeUpperBound.Should().BeTrue();
        Translate<PromotionState>(state => state.Number == bound).Should().BeOfType<EmptyQueryPlan>();
    }

    [Fact]
    public void UnorderedAndInfiniteFloatingBoundsProduceCorrectIntegralDomains()
    {
        var notANumber = double.NaN;
        var positiveInfinity = double.PositiveInfinity;
        var negativeInfinity = double.NegativeInfinity;

        Translate<PromotionState>(state => state.Number == notANumber).Should().BeOfType<EmptyQueryPlan>();
        Translate<PromotionState>(state => state.Number < notANumber).Should().BeOfType<EmptyQueryPlan>();
        Translate<PromotionState>(state => notANumber > state.Number).Should().BeOfType<EmptyQueryPlan>();
        Translate<PromotionState>(state => state.Number > positiveInfinity).Should().BeOfType<EmptyQueryPlan>();
        Translate<PromotionState>(state => state.Number < negativeInfinity).Should().BeOfType<EmptyQueryPlan>();
        var belowInfinity = Translate<PromotionState>(state => state.Number < positiveInfinity)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var aboveInfinity = Translate<PromotionState>(state => state.Number > negativeInfinity)
            .Should().BeOfType<RangeQueryPlan>().Subject;

        belowInfinity.UpperBound!.SignedInteger.Should().Be(int.MaxValue);
        belowInfinity.IncludeUpperBound.Should().BeTrue();
        aboveInfinity.LowerBound!.SignedInteger.Should().Be(int.MinValue);
        aboveInfinity.IncludeLowerBound.Should().BeTrue();
    }

    [Fact]
    public void SubDecimalFloatingBoundsPreserveTheirOrderingAgainstIntegers()
    {
        var positiveEpsilon = double.Epsilon;
        var justAboveFive = double.BitIncrement(5d);
        var justBelowFive = double.BitDecrement(5d);

        var atLeastEpsilon = Translate<PromotionState>(state => state.Number >= positiveEpsilon)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var belowEpsilon = Translate<PromotionState>(state => state.Number < positiveEpsilon)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var atLeastAboveFive = Translate<PromotionState>(state => state.Number >= justAboveFive)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var belowAboveFive = Translate<PromotionState>(state => state.Number < justAboveFive)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var aboveBelowFive = Translate<PromotionState>(state => state.Number > justBelowFive)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var atMostBelowFive = Translate<PromotionState>(state => state.Number <= justBelowFive)
            .Should().BeOfType<RangeQueryPlan>().Subject;

        atLeastEpsilon.LowerBound!.SignedInteger.Should().Be(1);
        belowEpsilon.UpperBound!.SignedInteger.Should().Be(0);
        atLeastAboveFive.LowerBound!.SignedInteger.Should().Be(6);
        belowAboveFive.UpperBound!.SignedInteger.Should().Be(5);
        aboveBelowFive.LowerBound!.SignedInteger.Should().Be(5);
        atMostBelowFive.UpperBound!.SignedInteger.Should().Be(4);
    }

    [Fact]
    public void FloatingEqualityAgainstAnIntegralIndexRequiresAnExactInteger()
    {
        var exactValue = 5d;
        var adjacentValue = double.BitIncrement(exactValue);

        var exact = Translate<PromotionState>(state => state.Number == exactValue)
            .Should().BeOfType<ExactQueryPlan>().Subject;

        exact.Value.SignedInteger.Should().Be(5);
        Translate<PromotionState>(state => state.Number == adjacentValue)
            .Should().BeOfType<EmptyQueryPlan>();
    }

    [Fact]
    public void EmptyNumericLeavesAreSimplifiedThroughBooleanComposition()
    {
        var aboveAgeDomain = 300;
        var belowAgeDomain = -1;
        var impossibleAndExact = Translate<PromotionState>(
            state => state.Age == aboveAgeDomain && state.City == "Helsinki");
        var impossibleOrExact = Translate<PromotionState>(
            state => state.Age == aboveAgeDomain || state.City == "Helsinki");
        var impossibleOrImpossible = Translate<PromotionState>(
            state => state.Age == aboveAgeDomain || state.Age == belowAgeDomain);

        impossibleAndExact.Should().BeOfType<EmptyQueryPlan>();
        impossibleOrExact.Should().BeOfType<ExactQueryPlan>()
            .Which.Index.PropertyName.Should().Be(nameof(PromotionState.City));
        impossibleOrImpossible.Should().BeOfType<EmptyQueryPlan>();
    }

    [Fact]
    public void FloatingAndDecimalPropertyDomainsRetainTheirCanonicalIndexKinds()
    {
        var decimalBound = 5;
        var floatingBound = 8f;
        var notANumber = double.NaN;

        var decimalRange = Translate<PromotionState>(state => state.Amount >= decimalBound)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var floatingRange = Translate<PromotionState>(state => state.DoubleRatio <= floatingBound)
            .Should().BeOfType<RangeQueryPlan>().Subject;

        decimalRange.LowerBound!.Kind.Should().Be(IndexValueKind.Decimal);
        decimalRange.LowerBound.Decimal.Should().Be(decimalBound);
        floatingRange.UpperBound!.Kind.Should().Be(IndexValueKind.FloatingPoint);
        floatingRange.UpperBound.FloatingPoint.Should().Be(floatingBound);
        Translate<PromotionState>(state => state.DoubleRatio == notANumber)
            .Should().BeOfType<EmptyQueryPlan>();
    }

    [Fact]
    public void SupportedBclComparisonOperatorsRetainIndexSemantics()
    {
        var timestamp = new DateTime(638_700_000_000_000_000, DateTimeKind.Utc);
        var offset = new DateTimeOffset(timestamp);
        var identifier = Guid.NewGuid();

        var timestampRange = Translate<BclOperatorState>(state => state.Timestamp >= timestamp)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var offsetRange = Translate<BclOperatorState>(state => state.Offset < offset)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var identifierExact = Translate<BclOperatorState>(state => state.Identifier == identifier)
            .Should().BeOfType<ExactQueryPlan>().Subject;

        timestampRange.LowerBound!.UtcTicks.Should().Be(timestamp.Ticks);
        timestampRange.IncludeLowerBound.Should().BeTrue();
        offsetRange.UpperBound!.UtcTicks.Should().Be(offset.UtcTicks);
        offsetRange.IncludeUpperBound.Should().BeFalse();
        identifierExact.Value.Guid.Should().Be(identifier);
    }

    [Fact]
    public void PropertyConversionsWhichCanChangeComparisonSemanticsAreRejected()
    {
        object sameTextDifferentReference = new string("Helsinki".ToCharArray());
        var customValue = new IntConvertible(5);
        var wrappedValue = new IntWrapper(5);

        var boxing = () => Translate<PromotionState>(
            state => (object)state.City == sameTextDifferentReference);
        var narrowing = () => Translate<PromotionState>(state => (byte)state.Number == 5);
        var lossyFloating = () => Translate<PromotionState>(state => (float)state.Number == 5f);
        var lossyLong = () => Translate<PromotionState>(state => (double)state.LongNumber == 5d);
        var userDefinedProperty = () => Translate<PromotionState>(
            state => (IntWrapper)state.Number == wrappedValue);
        var userDefinedValue = () => Translate<PromotionState>(state => state.Number == customValue);

        boxing.Should().Throw<NotSupportedException>().WithMessage("*change equality or ordering semantics*");
        narrowing.Should().Throw<NotSupportedException>().WithMessage("*change equality or ordering semantics*");
        lossyFloating.Should().Throw<NotSupportedException>().WithMessage("*change equality or ordering semantics*");
        lossyLong.Should().Throw<NotSupportedException>().WithMessage("*change equality or ordering semantics*");
        userDefinedProperty.Should().Throw<NotSupportedException>()
            .WithMessage("*change equality or ordering semantics*");
        userDefinedValue.Should().Throw<NotSupportedException>().WithMessage("*value expression 'Convert' is not supported*");
    }

    [Fact]
    public void CustomBinaryComparisonMethodsAreRejectedBeforeValueTranslation()
    {
        var parameter = Expression.Parameter(typeof(PromotionState), "state");
        var property = Expression.Property(parameter, nameof(PromotionState.Number));
        var method = typeof(QueryTranslatorTests).GetMethod(
            nameof(AlwaysFalse),
            BindingFlags.NonPublic | BindingFlags.Static)!;
        var comparison = Expression.MakeBinary(
            ExpressionType.Equal,
            property,
            Expression.Constant(5),
            liftToNull: false,
            method);
        var predicate = Expression.Lambda<Func<PromotionState, bool>>(comparison, parameter);

        var action = () => Translate(predicate);

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*Custom comparison method*can differ from index equality or ordering*");
    }

    [Fact]
    public void StateParameterConversionChainHonorsTheDepthBoundary()
    {
        var atLimit = CreateStateReceiverConversionPredicate(QueryPlanLimits.MaximumDepth);
        var beyondLimit = CreateStateReceiverConversionPredicate(QueryPlanLimits.MaximumDepth + 1);

        var accepted = () => Translate(atLimit);
        var rejected = () => Translate(beyondLimit);

        accepted.Should().NotThrow();
        rejected.Should().Throw<NotSupportedException>()
            .WithMessage($"*state-parameter conversion chain*{QueryPlanLimits.MaximumDepth}*");
    }

    [Fact]
    public void IndexedPropertyConversionChainHonorsTheDepthBoundary()
    {
        var atLimit = CreatePropertyConversionPredicate(QueryPlanLimits.MaximumDepth);
        var beyondLimit = CreatePropertyConversionPredicate(QueryPlanLimits.MaximumDepth + 1);

        var accepted = () => Translate(atLimit);
        var rejected = () => Translate(beyondLimit);

        accepted.Should().NotThrow();
        rejected.Should().Throw<NotSupportedException>()
            .WithMessage($"*indexed-property conversion chain*{QueryPlanLimits.MaximumDepth}*");
    }

    [Fact]
    public void ClosedValueTraversalHonorsTheDepthBoundaryIncludingTheLeaf()
    {
        var atLimit = CreateClosedValueConversionPredicate(QueryPlanLimits.MaximumDepth - 1);
        var beyondLimit = CreateClosedValueConversionPredicate(QueryPlanLimits.MaximumDepth);

        var accepted = () => Translate(atLimit);
        var rejected = () => Translate(beyondLimit);

        accepted.Should().NotThrow();
        rejected.Should().Throw<NotSupportedException>()
            .WithMessage($"*query expression exceeds the maximum supported depth of {QueryPlanLimits.MaximumDepth}*");
    }

    [Fact]
    public void CapturedPropertyChainsAreInterpretedWithoutDynamicCodeGeneration()
    {
        var holder = new BoundHolder { Bounds = new Bounds { Minimum = 7 } };

        var captured = Translate(state => state.Salary >= holder.Bounds.Minimum)
            .Should().BeOfType<RangeQueryPlan>().Subject;
        var staticValue = Translate(state => state.Salary >= Bounds.StaticMinimum)
            .Should().BeOfType<RangeQueryPlan>().Subject;

        captured.LowerBound!.SignedInteger.Should().Be(7);
        staticValue.LowerBound!.SignedInteger.Should().Be(Bounds.StaticMinimum);
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
    public void NegationExplainsThePartitionComplementConstraint()
    {
        var action = () => Translate(state => state.City != "Helsinki");

        action.Should().Throw<NotSupportedException>()
            .WithMessage("*partition-wide set complement*");
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
            .WithMessage("*at least one Where or WhereIn filter*");
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
        return Translate<QueryState>(predicate);
    }

    private static QueryPlan Translate<TState>(Expression<Func<TState, bool>> predicate)
    {
        var expression = Array.Empty<TState>()
            .AsQueryable()
            .Where(predicate)
            .Expression;
        return QueryTranslator.Translate<TState>("state", expression);
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

    private static bool AlwaysFalse(int left, int right)
    {
        return false;
    }

    private static Expression<Func<PromotionState, bool>> CreateStateReceiverConversionPredicate(
        int conversionCount)
    {
        var parameter = Expression.Parameter(typeof(PromotionState), "state");
        Expression receiver = parameter;
        for (var index = 0; index < conversionCount; index++)
        {
            receiver = Expression.Convert(receiver, typeof(PromotionState));
        }

        var property = Expression.Property(receiver, nameof(PromotionState.Number));
        return Expression.Lambda<Func<PromotionState, bool>>(
            Expression.Equal(property, Expression.Constant(5)),
            parameter);
    }

    private static Expression<Func<PromotionState, bool>> CreatePropertyConversionPredicate(
        int conversionCount)
    {
        var parameter = Expression.Parameter(typeof(PromotionState), "state");
        Expression property = Expression.Property(parameter, nameof(PromotionState.Number));
        for (var index = 0; index < conversionCount; index++)
        {
            property = Expression.Convert(property, typeof(int));
        }

        return Expression.Lambda<Func<PromotionState, bool>>(
            Expression.Equal(property, Expression.Constant(5)),
            parameter);
    }

    private static Expression<Func<PromotionState, bool>> CreateClosedValueConversionPredicate(
        int conversionCount)
    {
        var parameter = Expression.Parameter(typeof(PromotionState), "state");
        var property = Expression.Property(parameter, nameof(PromotionState.Number));
        Expression value = Expression.Constant(5);
        for (var index = 0; index < conversionCount; index++)
        {
            value = Expression.Convert(value, typeof(int));
        }

        return Expression.Lambda<Func<PromotionState, bool>>(
            Expression.Equal(property, value),
            parameter);
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

    private sealed class PromotionState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public byte Age { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public short Level { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Number { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public long LongNumber { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public char Code { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public SignedStatus Status { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public UnsignedStatus UnsignedStatus { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public SignedStatus? OptionalStatus { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public float Ratio { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public double DoubleRatio { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public decimal Amount { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;
    }

    private sealed class BoundHolder
    {
        public required Bounds Bounds { get; init; }
    }

    private sealed class BclOperatorState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public DateTime Timestamp { get; init; }

        [SearchableIndex(SearchableIndexKind.Range)]
        public DateTimeOffset Offset { get; init; }

        [SearchableIndex(SearchableIndexKind.Hash)]
        public Guid Identifier { get; init; }
    }

    private sealed class Bounds
    {
        public static int StaticMinimum => 11;

        public int Minimum { get; init; }
    }

    private readonly record struct IntConvertible(int Value)
    {
        public static implicit operator int(IntConvertible value)
        {
            return value.Value;
        }
    }

    private readonly record struct IntWrapper(int Value)
    {
        public static implicit operator IntWrapper(int value)
        {
            return new IntWrapper(value);
        }
    }

    private enum SignedStatus : sbyte
    {
        Active = 1,
    }

    private enum UnsignedStatus : ushort
    {
        Active = 1,
    }
}
