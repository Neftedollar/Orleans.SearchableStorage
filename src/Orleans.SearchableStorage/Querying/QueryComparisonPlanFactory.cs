using System.Diagnostics;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Querying;

internal static class QueryComparisonPlanFactory
{
    public static bool IsBuiltInConversion(MethodInfo? method)
    {
        // CLR conversion nodes have no method. Predefined C# decimal conversions are the
        // exception: expression trees represent them with Decimal.op_Implicit.
        return method is null
            || (method.DeclaringType == typeof(decimal)
                && method.Name == "op_Implicit"
                && method.ReturnType == typeof(decimal));
    }

    public static void ValidateComparisonMethod(
        SelectedIndex index,
        ExpressionType comparisonType,
        MethodInfo? method)
    {
        if (method is null)
        {
            return;
        }

        var expectedName = comparisonType switch
        {
            ExpressionType.Equal => "op_Equality",
            ExpressionType.LessThan => "op_LessThan",
            ExpressionType.LessThanOrEqual => "op_LessThanOrEqual",
            ExpressionType.GreaterThan => "op_GreaterThan",
            ExpressionType.GreaterThanOrEqual => "op_GreaterThanOrEqual",
            _ => null,
        };
        if (expectedName is null
            || method.DeclaringType is not { } declaringType
            || (declaringType != typeof(string)
                && declaringType != typeof(decimal)
                && declaringType != typeof(DateTime)
                && declaringType != typeof(DateTimeOffset)
                && declaringType != typeof(Guid))
            || method.Name != expectedName)
        {
            throw new NotSupportedException(
                $"Custom comparison method '{method}' is not supported for indexed property " +
                $"'{index.PropertyName}' because it can differ from index equality or ordering.");
        }
    }

    public static void ValidatePropertyConversions(
        SelectedIndex index,
        string propertyName,
        IReadOnlyList<QueryPropertyConversion> conversions)
    {
        foreach (var conversion in conversions)
        {
            if (conversion.IsUserDefined
                || !CanRepresentIndexedDomainExactly(index.Converter, conversion.TargetType))
            {
                throw new NotSupportedException(
                    $"Conversion of indexed property '{propertyName}' to " +
                    $"'{conversion.TargetType}' is not supported because it can change equality or ordering semantics.");
            }
        }
    }

    public static QueryPlan Create(
        SelectedIndex index,
        ExpressionType comparisonType,
        object? value)
    {
        if (value is null)
        {
            throw new NotSupportedException(
                $"Null comparisons are not supported because property '{index.PropertyName}' does not index null values.");
        }

        return index.Converter.QueryValueDomain switch
        {
            IntegralIndexQueryValueDomain integral =>
                CreateIntegralComparisonPlan(index, comparisonType, value, integral),
            FloatingPointIndexQueryValueDomain =>
                CreateFloatingPointComparisonPlan(index, comparisonType, value),
            DecimalIndexQueryValueDomain =>
                CreateDecimalComparisonPlan(index, comparisonType, value),
            _ => CreateStrictComparisonPlan(index, comparisonType, value),
        };
    }

    private static bool CanRepresentIndexedDomainExactly(
        IndexValueConverter converter,
        Type targetType)
    {
        targetType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (targetType == converter.RuntimeValueType)
        {
            return true;
        }

        return converter.QueryValueDomain switch
        {
            IntegralIndexQueryValueDomain integral => CanRepresentIntegralDomainExactly(integral, targetType),
            FloatingPointIndexQueryValueDomain =>
                converter.RuntimeValueType == typeof(float) && targetType == typeof(double),
            DecimalIndexQueryValueDomain => targetType == typeof(decimal),
            _ => false,
        };
    }

    private static bool CanRepresentIntegralDomainExactly(
        IntegralIndexQueryValueDomain domain,
        Type targetType)
    {
        if (TryGetIntegralTypeRange(targetType, out var minimum, out var maximum))
        {
            return minimum <= domain.Minimum && maximum >= domain.Maximum;
        }

        if (targetType == typeof(decimal))
        {
            return true;
        }

        if (targetType == typeof(double))
        {
            const decimal largestExactInteger = 9_007_199_254_740_992m;
            return domain.Minimum >= -largestExactInteger && domain.Maximum <= largestExactInteger;
        }

        if (targetType == typeof(float))
        {
            const decimal largestExactInteger = 16_777_216m;
            return domain.Minimum >= -largestExactInteger && domain.Maximum <= largestExactInteger;
        }

        return false;
    }

    private static bool TryGetIntegralTypeRange(
        Type type,
        out decimal minimum,
        out decimal maximum)
    {
        if (type == typeof(sbyte))
        {
            (minimum, maximum) = (sbyte.MinValue, sbyte.MaxValue);
        }
        else if (type == typeof(byte))
        {
            (minimum, maximum) = (byte.MinValue, byte.MaxValue);
        }
        else if (type == typeof(short))
        {
            (minimum, maximum) = (short.MinValue, short.MaxValue);
        }
        else if (type == typeof(ushort))
        {
            (minimum, maximum) = (ushort.MinValue, ushort.MaxValue);
        }
        else if (type == typeof(int))
        {
            (minimum, maximum) = (int.MinValue, int.MaxValue);
        }
        else if (type == typeof(uint))
        {
            (minimum, maximum) = (uint.MinValue, uint.MaxValue);
        }
        else if (type == typeof(long))
        {
            (minimum, maximum) = (long.MinValue, long.MaxValue);
        }
        else if (type == typeof(ulong))
        {
            (minimum, maximum) = (ulong.MinValue, ulong.MaxValue);
        }
        else if (type == typeof(char))
        {
            (minimum, maximum) = (char.MinValue, char.MaxValue);
        }
        else
        {
            minimum = default;
            maximum = default;
            return false;
        }

        return true;
    }

    private static QueryPlan CreateIntegralComparisonPlan(
        SelectedIndex index,
        ExpressionType comparisonType,
        object value,
        IntegralIndexQueryValueDomain domain)
    {
        var number = ReadIntegralQueryNumber(index, value, domain);
        if (comparisonType == ExpressionType.Equal)
        {
            return number.Kind == IntegralQueryNumberKind.Finite
                && decimal.Truncate(number.Value) == number.Value
                && number.Value >= domain.Minimum
                && number.Value <= domain.Maximum
                    ? new ExactQueryPlan(index, domain.Convert(number.Value))
                    : EmptyQueryPlan.Instance;
        }

        if (number.Kind == IntegralQueryNumberKind.Unordered)
        {
            return EmptyQueryPlan.Instance;
        }

        if (number.Kind == IntegralQueryNumberKind.AboveDomain)
        {
            return comparisonType is ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                ? CreateUpperBoundPlan(index, domain, domain.Maximum)
                : EmptyQueryPlan.Instance;
        }

        if (number.Kind == IntegralQueryNumberKind.BelowDomain)
        {
            return comparisonType is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                ? CreateLowerBoundPlan(index, domain, domain.Minimum)
                : EmptyQueryPlan.Instance;
        }

        if (decimal.Truncate(number.Value) == number.Value
            && number.Value >= domain.Minimum
            && number.Value <= domain.Maximum)
        {
            return CreateStandardComparisonPlan(index, comparisonType, domain.Convert(number.Value));
        }

        return comparisonType switch
        {
            ExpressionType.GreaterThan => number.Value < domain.Minimum
                ? CreateLowerBoundPlan(index, domain, domain.Minimum)
                : number.Value >= domain.Maximum
                    ? EmptyQueryPlan.Instance
                    : CreateLowerBoundPlan(index, domain, decimal.Floor(number.Value) + 1),
            ExpressionType.GreaterThanOrEqual => number.Value <= domain.Minimum
                ? CreateLowerBoundPlan(index, domain, domain.Minimum)
                : number.Value > domain.Maximum
                    ? EmptyQueryPlan.Instance
                    : CreateLowerBoundPlan(index, domain, decimal.Ceiling(number.Value)),
            ExpressionType.LessThan => number.Value > domain.Maximum
                ? CreateUpperBoundPlan(index, domain, domain.Maximum)
                : number.Value <= domain.Minimum
                    ? EmptyQueryPlan.Instance
                    : CreateUpperBoundPlan(index, domain, decimal.Ceiling(number.Value) - 1),
            ExpressionType.LessThanOrEqual => number.Value >= domain.Maximum
                ? CreateUpperBoundPlan(index, domain, domain.Maximum)
                : number.Value < domain.Minimum
                    ? EmptyQueryPlan.Instance
                    : CreateUpperBoundPlan(index, domain, decimal.Floor(number.Value)),
            _ => throw new UnreachableException(),
        };
    }

    private static IntegralQueryNumber ReadIntegralQueryNumber(
        SelectedIndex index,
        object value,
        IntegralIndexQueryValueDomain domain)
    {
        return value switch
        {
            sbyte current => IntegralQueryNumber.Finite(current),
            byte current => IntegralQueryNumber.Finite(current),
            short current => IntegralQueryNumber.Finite(current),
            ushort current => IntegralQueryNumber.Finite(current),
            int current => IntegralQueryNumber.Finite(current),
            uint current => IntegralQueryNumber.Finite(current),
            long current => IntegralQueryNumber.Finite(current),
            ulong current => IntegralQueryNumber.Finite(current),
            char current => IntegralQueryNumber.Finite(current),
            decimal current => IntegralQueryNumber.Finite(current),
            float current => ReadFloatingIntegralQueryNumber(index, current, domain, 16_777_216m),
            double current => ReadFloatingIntegralQueryNumber(index, current, domain, 9_007_199_254_740_992m),
            Enum current => IntegralQueryNumber.Finite(Convert.ToDecimal(current, CultureInfo.InvariantCulture)),
            _ => throw ComparisonTypeMismatch(index, value),
        };
    }

    private static IntegralQueryNumber ReadFloatingIntegralQueryNumber(
        SelectedIndex index,
        double value,
        IntegralIndexQueryValueDomain domain,
        decimal largestExactInteger)
    {
        if (double.IsNaN(value))
        {
            return new IntegralQueryNumber(0, IntegralQueryNumberKind.Unordered);
        }

        if (domain.Minimum < -largestExactInteger || domain.Maximum > largestExactInteger)
        {
            throw new NotSupportedException(
                $"Floating-point promotion of indexed property '{index.PropertyName}' is not supported " +
                "because it cannot represent every indexed integer exactly.");
        }

        if (value > (double)domain.Maximum)
        {
            return new IntegralQueryNumber(0, IntegralQueryNumberKind.AboveDomain);
        }

        if (value < (double)domain.Minimum)
        {
            return new IntegralQueryNumber(0, IntegralQueryNumberKind.BelowDomain);
        }

        var truncated = Math.Truncate(value);
        // Converting a fractional double directly to decimal can collapse epsilon or an adjacent
        // representable value onto an integer. Any midpoint in the same open integer interval has
        // identical ordering against indexed integers, so .5m preserves exactly what the plan needs.
        return truncated == value
            ? IntegralQueryNumber.Finite((decimal)value)
            : IntegralQueryNumber.Finite((decimal)Math.Floor(value) + 0.5m);
    }

    private static QueryPlan CreateFloatingPointComparisonPlan(
        SelectedIndex index,
        ExpressionType comparisonType,
        object value)
    {
        var numericValue = value switch
        {
            float current => current,
            double current => current,
            _ => throw ComparisonTypeMismatch(index, value),
        };

        if (double.IsNaN(numericValue))
        {
            return EmptyQueryPlan.Instance;
        }

        var indexValue = new IndexValue
        {
            Kind = IndexValueKind.FloatingPoint,
            FloatingPoint = numericValue,
        };
        return CreateStandardComparisonPlan(index, comparisonType, indexValue);
    }

    private static QueryPlan CreateDecimalComparisonPlan(
        SelectedIndex index,
        ExpressionType comparisonType,
        object value)
    {
        if (value is not decimal numericValue)
        {
            throw ComparisonTypeMismatch(index, value);
        }

        return CreateStandardComparisonPlan(
            index,
            comparisonType,
            new IndexValue { Kind = IndexValueKind.Decimal, Decimal = numericValue });
    }

    private static QueryPlan CreateStrictComparisonPlan(
        SelectedIndex index,
        ExpressionType comparisonType,
        object value)
    {
        if (value.GetType() != index.Converter.RuntimeValueType)
        {
            throw ComparisonTypeMismatch(index, value);
        }

        var indexValue = index.Converter.ConvertObject(value)
            ?? throw new InvalidOperationException("A non-null query value unexpectedly converted to null.");
        return CreateStandardComparisonPlan(index, comparisonType, indexValue);
    }

    private static QueryPlan CreateStandardComparisonPlan(
        SelectedIndex index,
        ExpressionType comparisonType,
        IndexValue indexValue)
    {
        return comparisonType switch
        {
            ExpressionType.Equal => new ExactQueryPlan(index, indexValue),
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

    private static RangeQueryPlan CreateLowerBoundPlan(
        SelectedIndex index,
        IntegralIndexQueryValueDomain domain,
        decimal value)
    {
        return new RangeQueryPlan(
            index,
            domain.Convert(value),
            IncludeLowerBound: true,
            UpperBound: null,
            IncludeUpperBound: false);
    }

    private static RangeQueryPlan CreateUpperBoundPlan(
        SelectedIndex index,
        IntegralIndexQueryValueDomain domain,
        decimal value)
    {
        return new RangeQueryPlan(
            index,
            LowerBound: null,
            IncludeLowerBound: false,
            domain.Convert(value),
            IncludeUpperBound: true);
    }

    private static NotSupportedException ComparisonTypeMismatch(
        SelectedIndex index,
        object value)
    {
        return new NotSupportedException(
            $"Comparison value type '{value.GetType()}' does not match indexed property " +
            $"'{index.PropertyName}' type '{index.Converter.RuntimeValueType}'.");
    }

    private readonly record struct IntegralQueryNumber(
        decimal Value,
        IntegralQueryNumberKind Kind)
    {
        public static IntegralQueryNumber Finite(decimal value)
        {
            return new IntegralQueryNumber(value, IntegralQueryNumberKind.Finite);
        }
    }

    private enum IntegralQueryNumberKind
    {
        Finite,
        BelowDomain,
        AboveDomain,
        Unordered,
    }
}

internal readonly record struct QueryPropertyConversion(Type TargetType, MethodInfo? Method)
{
    public bool IsUserDefined => !QueryComparisonPlanFactory.IsBuiltInConversion(Method);
}
