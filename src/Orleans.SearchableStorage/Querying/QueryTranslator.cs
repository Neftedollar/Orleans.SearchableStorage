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

        var budget = new TranslationBudget();
        var plan = TranslateQueryExpression<TState>(stateName, expression, budget, depth: 1)
            ?? throw new NotSupportedException(
                "A searchable storage query must contain at least one Where predicate.");
        QueryPlanValidator.Validate(plan);
        return plan;
    }

    private static QueryPlan? TranslateQueryExpression<TState>(
        string stateName,
        Expression expression,
        TranslationBudget budget,
        int depth)
    {
        budget.Visit(depth);
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

        var sourcePlan = TranslateQueryExpression<TState>(
            stateName,
            methodCall.Arguments[0],
            budget,
            depth + 1);
        var predicatePlan = TranslatePredicate<TState>(
            stateName,
            predicate.Body,
            predicate.Parameters[0],
            budget,
            depth: 1);
        return sourcePlan is null
            ? predicatePlan
            : QueryPlanBuilder.And(sourcePlan, predicatePlan);
    }

    private static QueryPlan TranslatePredicate<TState>(
        string stateName,
        Expression expression,
        ParameterExpression parameter,
        TranslationBudget budget,
        int depth)
    {
        budget.Visit(depth);
        return expression.NodeType switch
        {
            ExpressionType.AndAlso when expression is BinaryExpression binary =>
                QueryPlanBuilder.And(
                    TranslatePredicate<TState>(stateName, binary.Left, parameter, budget, depth + 1),
                    TranslatePredicate<TState>(stateName, binary.Right, parameter, budget, depth + 1)),
            ExpressionType.OrElse when expression is BinaryExpression binary =>
                QueryPlanBuilder.Or(
                    TranslatePredicate<TState>(stateName, binary.Left, parameter, budget, depth + 1),
                    TranslatePredicate<TState>(stateName, binary.Right, parameter, budget, depth + 1)),
            ExpressionType.Equal or
            ExpressionType.LessThan or
            ExpressionType.LessThanOrEqual or
            ExpressionType.GreaterThan or
            ExpressionType.GreaterThanOrEqual when expression is BinaryExpression comparison =>
                TranslateComparison<TState>(stateName, comparison, parameter, budget),
            ExpressionType.NotEqual or ExpressionType.Not =>
                throw new NotSupportedException(
                    "Predicate negation is not supported because it requires a partition-wide set complement. " +
                    "Use positive indexed comparisons combined with && or ||."),
            _ => throw new NotSupportedException(
                $"Predicate expression '{expression.NodeType}' is not supported. " +
                "Use indexed comparisons combined with && or ||."),
        };
    }

    private static QueryPlan TranslateComparison<TState>(
        string stateName,
        BinaryExpression comparison,
        ParameterExpression parameter,
        TranslationBudget budget)
    {
        var leftIsProperty = TryGetDirectProperty(comparison.Left, parameter, out var leftProperty);
        var rightIsProperty = TryGetDirectProperty(comparison.Right, parameter, out var rightProperty);
        if (leftIsProperty == rightIsProperty)
        {
            throw new NotSupportedException(
                "Each comparison must contain exactly one direct state property and one captured or constant value.");
        }

        var propertyAccess = leftIsProperty ? leftProperty : rightProperty;
        var property = propertyAccess.Property;
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

        QueryComparisonPlanFactory.ValidatePropertyConversions(
            index,
            property.Name,
            propertyAccess.Conversions);
        QueryComparisonPlanFactory.ValidateComparisonMethod(index, comparison.NodeType, comparison.Method);
        var value = EvaluateClosedValue(valueExpression, parameter, budget);

        if (comparisonType != ExpressionType.Equal
            && index.Kind != SearchableIndexKind.Range)
        {
            throw new NotSupportedException(
                $"Comparison '{comparisonType}' requires a range index, but property '{property.Name}' uses a hash index.");
        }

        return QueryComparisonPlanFactory.Create(index, comparisonType, value);
    }

    private static bool TryGetDirectProperty(
        Expression expression,
        ParameterExpression parameter,
        out PropertyAccess propertyAccess)
    {
        var conversions = new List<QueryPropertyConversion>();
        while (expression is UnaryExpression
               {
                   NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked,
               } conversion)
        {
            if (conversions.Count == QueryPlanLimits.MaximumDepth)
            {
                throw new NotSupportedException(
                    $"An indexed-property conversion chain exceeds the maximum supported depth of " +
                    $"{QueryPlanLimits.MaximumDepth}.");
            }

            conversions.Add(new QueryPropertyConversion(conversion.Type, conversion.Method));
            expression = conversion.Operand;
        }

        conversions.Reverse();
        if (expression is MemberExpression
            {
                Member: PropertyInfo selectedProperty,
                Expression: not null,
            } member
            && IsStateParameter(member.Expression, parameter))
        {
            propertyAccess = new PropertyAccess(selectedProperty, conversions);
            return true;
        }

        propertyAccess = default;
        return false;
    }

    private static bool IsStateParameter(Expression expression, ParameterExpression parameter)
    {
        var conversionCount = 0;
        while (expression is UnaryExpression
               {
                   NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked,
                   Method: null,
               } conversion
               && conversion.Type.IsAssignableFrom(parameter.Type))
        {
            if (conversionCount == QueryPlanLimits.MaximumDepth)
            {
                throw new NotSupportedException(
                    $"A state-parameter conversion chain exceeds the maximum supported depth of " +
                    $"{QueryPlanLimits.MaximumDepth}.");
            }

            conversionCount++;
            expression = conversion.Operand;
        }

        return expression == parameter;
    }

    private static object? EvaluateClosedValue(
        Expression expression,
        ParameterExpression parameter,
        TranslationBudget budget)
    {
        ValidateClosedValueExpression(expression, parameter, budget, depth: 1);
        var boxed = Expression.Convert(expression, typeof(object));
        return Expression.Lambda<Func<object?>>(boxed).Compile(preferInterpretation: true)();
    }

    private static void ValidateClosedValueExpression(
        Expression expression,
        ParameterExpression parameter,
        TranslationBudget budget,
        int depth)
    {
        budget.Visit(depth);
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
                    ValidateClosedValueExpression(member.Expression, parameter, budget, depth + 1);
                }

                return;
            case UnaryExpression
            {
                NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked,
            } unary:
                if (!QueryComparisonPlanFactory.IsBuiltInConversion(unary.Method))
                {
                    throw new NotSupportedException(
                        $"Query value expression '{expression.NodeType}' is not supported. " +
                        "Use a constant or captured value without user-defined conversions.");
                }

                ValidateClosedValueExpression(unary.Operand, parameter, budget, depth + 1);
                return;
            default:
                throw new NotSupportedException(
                    $"Query value expression '{expression.NodeType}' is not supported. " +
                    "Use a constant or captured value without method calls or calculations.");
        }
    }

    private static Expression StripQuote(Expression expression)
    {
        return expression is UnaryExpression { NodeType: ExpressionType.Quote } quote
            ? quote.Operand
            : expression;
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
            $"LINQ operator '{operatorName}' is not supported by the current query stage. " +
            "Use Where followed by ToGrainIdsAsync.");
    }

    private readonly record struct PropertyAccess(
        PropertyInfo Property,
        IReadOnlyList<QueryPropertyConversion> Conversions);

    private sealed class TranslationBudget
    {
        private int _nodeCount;

        public void Visit(int depth)
        {
            if (depth > QueryPlanLimits.MaximumDepth)
            {
                throw new NotSupportedException(
                    $"The query expression exceeds the maximum supported depth of " +
                    $"{QueryPlanLimits.MaximumDepth}.");
            }

            _nodeCount++;
            if (_nodeCount > QueryPlanLimits.MaximumNodeCount)
            {
                throw new NotSupportedException(
                    $"The query expression exceeds the maximum supported node count of " +
                    $"{QueryPlanLimits.MaximumNodeCount}.");
            }
        }
    }
}
