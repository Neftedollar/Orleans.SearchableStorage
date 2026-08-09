using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryComparisonPlanFactoryTests
{
    [Theory]
    [InlineData(typeof(byte), typeof(char), true)]
    [InlineData(typeof(byte), typeof(sbyte), false)]
    [InlineData(typeof(short), typeof(int), true)]
    [InlineData(typeof(ushort), typeof(uint), true)]
    [InlineData(typeof(int), typeof(double), true)]
    [InlineData(typeof(int), typeof(uint), false)]
    [InlineData(typeof(uint), typeof(ulong), true)]
    [InlineData(typeof(long), typeof(double), false)]
    [InlineData(typeof(ulong), typeof(decimal), true)]
    [InlineData(typeof(ulong), typeof(double), false)]
    [InlineData(typeof(float), typeof(double), true)]
    [InlineData(typeof(double), typeof(float), false)]
    [InlineData(typeof(decimal), typeof(decimal), true)]
    [InlineData(typeof(decimal), typeof(double), false)]
    [InlineData(typeof(string), typeof(string), true)]
    [InlineData(typeof(string), typeof(object), false)]
    public void PropertyConversionPolicyRequiresAnExactRepresentationOfTheIndexedDomain(
        Type indexedType,
        Type targetType,
        bool expectedToBeSupported)
    {
        var index = CreateIndex(indexedType);

        var validate = () => QueryComparisonPlanFactory.ValidatePropertyConversions(
            index,
            index.PropertyName,
            [new QueryPropertyConversion(targetType, Method: null)]);

        if (expectedToBeSupported)
        {
            validate.Should().NotThrow();
        }
        else
        {
            validate.Should().Throw<NotSupportedException>()
                .WithMessage("*change equality or ordering semantics*");
        }
    }

    [Fact]
    public void FloatingAndDecimalPlansRejectArbitraryConvertibleValues()
    {
        var floatingIndex = CreateIndex(typeof(double));
        var decimalIndex = CreateIndex(typeof(decimal));

        var createFloating = () => QueryComparisonPlanFactory.Create(
            floatingIndex,
            ExpressionType.Equal,
            "5");
        var createDecimal = () => QueryComparisonPlanFactory.Create(
            decimalIndex,
            ExpressionType.Equal,
            "5");

        createFloating.Should().Throw<NotSupportedException>()
            .WithMessage("*does not match indexed property*");
        createDecimal.Should().Throw<NotSupportedException>()
            .WithMessage("*does not match indexed property*");
    }

    private static SelectedIndex CreateIndex(Type indexedType)
    {
        IndexValueConverterProvider.TryGetConverter(indexedType, out var converter).Should().BeTrue();
        return new SelectedIndex(
            "scope",
            SearchableIndexKind.Range,
            converter!,
            "Value");
    }
}
