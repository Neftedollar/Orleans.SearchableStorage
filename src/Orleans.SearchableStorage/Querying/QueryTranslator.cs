using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Querying;

internal static class QueryTranslator
{
    public static QueryPlan Translate<TState>(string stateName, Expression expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentNullException.ThrowIfNull(expression);

        var plan = TranslateQueryExpression<TState>(stateName, expression);
        return plan
            ?? throw new NotSupportedException(
                "A searchable storage query must contain at least one Where predicate.");
    }

    private static QueryPlan? TranslateQueryExpression<TState>(
        string stateName,
        Expression expression)
    {
        if (expression is ConstantExpression { Value: IQueryable<TState> })
        {
            return null;
        }

        if (expression is not MethodCallExpression methodCall
            || methodCall.Method.DeclaringType != typeof(Queryable)
            || methodCall.Method.Name != nameof(Queryable.Where))
        {
            throw UnsupportedQueryOperator(expression);
        }

        if (methodCall.Arguments.Count != 2
            || StripQuote(methodCall.Arguments[1]) is not LambdaExpression { Parameters.Count: 1 } predicate)
        {
            throw new NotSupportedException(
                "Only Queryable.Where predicates with one state parameter are supported.");
        }

        var sourcePlan = TranslateQueryExpression<TState>(stateName, methodCall.Arguments[0]);
        var predicatePlan = TranslatePredicate<TState>(stateName, predicate.Body, predicate.Parameters[0]);
        return sourcePlan is null
            ? predicatePlan
            : QueryPlanBuilder.And(sourcePlan, predicatePlan);
    }

    private static QueryPlan TranslatePredicate<TState>(
        string stateName,
        Expression expression,
        ParameterExpression parameter)
    {
        return expression.NodeType switch
        {
            ExpressionType.AndAlso when expression is BinaryExpression binary =>
                QueryPlanBuilder.And(
                    TranslatePredicate<TState>(stateName, binary.Left, parameter),
                    TranslatePredicate<TState>(stateName, binary.Right, parameter)),
            ExpressionType.OrElse when expression is BinaryExpression binary =>
                QueryPlanBuilder.Or(
                    TranslatePredicate<TState>(stateName, binary.Left, parameter),
                    TranslatePredicate<TState>(stateName, binary.Right, parameter)),
            ExpressionType.Equal or
            ExpressionType.LessThan or
            ExpressionType.LessThanOrEqual or
            ExpressionType.GreaterThan or
            ExpressionType.GreaterThanOrEqual when expression is BinaryExpression comparison =>
                TranslateComparison<TState>(stateName, comparison, parameter),
            _ => throw new NotSupportedException(
                $"Predicate expression '{expression.NodeType}' is not supported. " +
                "Use indexed comparisons combined with && or ||."),
        };
    }

    private static QueryPlan TranslateComparison<TState>(
        string stateName,
        BinaryExpression comparison,
        ParameterExpression parameter)
    {
        var leftIsProperty = TryGetDirectProperty(comparison.Left, parameter, out var leftProperty);
        var rightIsProperty = TryGetDirectProperty(comparison.Right, parameter, out var rightProperty);
        if (leftIsProperty == rightIsProperty)
        {
            throw new NotSupportedException(
                "Each comparison must contain exactly one direct state property and one captured or constant value.");
        }

        var property = leftIsProperty ? leftProperty! : rightProperty!;
        var valueExpression = leftIsProperty ? comparison.Right : comparison.Left;
        var comparisonType = leftIsProperty
            ? comparison.NodeType
            : Reverse(comparison.NodeType);

        SelectedIndex index;
        try
        {
            index = IndexMetadataProvider.GetSelectedIndex<TState>(
                stateName,
                property,
                nameof(comparison));
        }
        catch (ArgumentException exception)
        {
            throw new NotSupportedException(
                $"Property '{property.Name}' is not searchable because it does not declare SearchableIndexAttribute.",
                exception);
        }

        var value = EvaluateClosedValue(valueExpression, parameter);
        var indexValue = ConvertValue(index, value);
        if (comparisonType == ExpressionType.Equal)
        {
            return new ExactQueryPlan(index, indexValue);
        }

        if (index.Kind != SearchableIndexKind.Range)
        {
            throw new NotSupportedException(
                $"Comparison '{comparisonType}' requires a range index, but property '{property.Name}' uses a hash index.");
        }

        return comparisonType switch
        {
            ExpressionType.GreaterThan => new RangeQueryPlan(
                index,
                indexValue,
                IncludeLowerBound: false,
                UpperBound: null,
                IncludeUpperBound: false),
            ExpressionType.GreaterThanOrEqual => new RangeQueryPlan(
                index,
                indexValue,
                IncludeLowerBound: true,
                UpperBound: null,
                IncludeUpperBound: false),
            ExpressionType.LessThan => new RangeQueryPlan(
                index,
                LowerBound: null,
                IncludeLowerBound: false,
                indexValue,
                IncludeUpperBound: false),
            ExpressionType.LessThanOrEqual => new RangeQueryPlan(
                index,
                LowerBound: null,
                IncludeLowerBound: false,
                indexValue,
                IncludeUpperBound: true),
            _ => throw new UnreachableException(),
        };
    }

    private static bool TryGetDirectProperty(
        Expression expression,
        ParameterExpression parameter,
        out PropertyInfo? property)
    {
        expression = StripConvert(expression);
        if (expression is MemberExpression
            {
                Member: PropertyInfo selectedProperty,
                Expression: not null,
            } member
            && StripConvert(member.Expression) == parameter)
        {
            property = selectedProperty;
            return true;
        }

        property = null;
        return false;
    }

    private static object? EvaluateClosedValue(
        Expression expression,
        ParameterExpression parameter)
    {
        ValidateClosedValueExpression(expression, parameter);

        var boxed = Expression.Convert(expression, typeof(object));
        return Expression.Lambda<Func<object?>>(boxed).Compile()();
    }

    private static void ValidateClosedValueExpression(
        Expression expression,
        ParameterExpression parameter)
    {
        if (expression == parameter)
        {
            throw new NotSupportedException("A query value cannot depend on the state parameter.");
        }

        switch (expression)
        {
            case ConstantExpression:
                return;
            case MemberExpression { Member: FieldInfo or PropertyInfo } member:
                if (member.Expression is not null)
                {
                    ValidateClosedValueExpression(member.Expression, parameter);
                }

                return;
            case UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary:
                ValidateClosedValueExpression(unary.Operand, parameter);
                return;
            default:
                throw new NotSupportedException(
                    $"Query value expression '{expression.NodeType}' is not supported. " +
                    "Use a constant or captured value without method calls or calculations.");
        }
    }

    private static IndexValue ConvertValue(SelectedIndex index, object? value)
    {
        if (value is null)
        {
            throw new NotSupportedException(
                $"Null comparisons are not supported because property '{index.PropertyName}' does not index null values.");
        }

        var runtimeType = value.GetType();
        if (runtimeType != index.Converter.RuntimeValueType)
        {
            throw new NotSupportedException(
                $"Comparison value type '{runtimeType}' does not match indexed property " +
                $"'{index.PropertyName}' type '{index.Converter.RuntimeValueType}'.");
        }

        return index.Converter.ConvertObject(value)
            ?? throw new InvalidOperationException("A non-null query value unexpectedly converted to null.");
    }

    private static Expression StripQuote(Expression expression)
    {
        return expression is UnaryExpression { NodeType: ExpressionType.Quote } quote
            ? quote.Operand
            : expression;
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression
               {
                   NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked,
               } conversion)
        {
            expression = conversion.Operand;
        }

        return expression;
    }

    private static ExpressionType Reverse(ExpressionType comparisonType)
    {
        return comparisonType switch
        {
            ExpressionType.Equal => ExpressionType.Equal,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            _ => throw new UnreachableException(),
        };
    }

    private static NotSupportedException UnsupportedQueryOperator(Expression expression)
    {
        var operatorName = expression is MethodCallExpression methodCall
            ? methodCall.Method.Name
            : expression.NodeType.ToString();
        return new NotSupportedException(
            $"LINQ operator '{operatorName}' is not supported. " +
            "Use Where followed by ToGrainIdsAsync.");
    }
}
