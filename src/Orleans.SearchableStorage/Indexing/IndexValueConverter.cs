using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using PolyType;
using PolyType.Abstractions;
using PolyType.ReflectionProvider;

namespace Orleans.SearchableStorage.Indexing;

internal abstract class IndexValueConverter
{
    public abstract Type ValueType { get; }

    public abstract Type RuntimeValueType { get; }

    public abstract bool SupportsRange { get; }

    public abstract IndexQueryValueDomain? QueryValueDomain { get; }

    public abstract IndexValue? ConvertObject(object? value);
}

internal sealed class IndexValueConverter<T>(
    Func<T, IndexValue?> converter,
    bool supportsRange,
    Type? runtimeValueType = null,
    Func<object, IndexValue?>? objectConverter = null,
    IndexQueryValueDomain? queryValueDomain = null) : IndexValueConverter
{
    public override Type ValueType => typeof(T);

    public override Type RuntimeValueType { get; } = runtimeValueType ?? typeof(T);

    public override bool SupportsRange { get; } = supportsRange;

    public override IndexQueryValueDomain? QueryValueDomain { get; } = queryValueDomain;

    public IndexValue? Convert(T value)
    {
        return converter(value);
    }

    public override IndexValue? ConvertObject(object? value)
    {
        if (value is null)
        {
            return null;
        }

        if (value.GetType() != RuntimeValueType)
        {
            throw new ArgumentException(
                $"Value type '{value.GetType()}' does not match converter type '{RuntimeValueType}'.",
                nameof(value));
        }

        if (objectConverter is not null)
        {
            return objectConverter(value);
        }

        if (value is not T typedValue)
        {
            throw new InvalidOperationException(
                $"Value type '{value.GetType()}' could not be read as converter type '{typeof(T)}'.");
        }

        return converter(typedValue);
    }
}

internal abstract record IndexQueryValueDomain;

internal sealed record IntegralIndexQueryValueDomain(
    decimal Minimum,
    decimal Maximum,
    Func<decimal, IndexValue> Convert) : IndexQueryValueDomain;

internal sealed record FloatingPointIndexQueryValueDomain : IndexQueryValueDomain
{
    public static FloatingPointIndexQueryValueDomain Instance { get; } = new();
}

internal sealed record DecimalIndexQueryValueDomain : IndexQueryValueDomain
{
    public static DecimalIndexQueryValueDomain Instance { get; } = new();
}

internal static class IndexValueConverterProvider
{
    private static readonly ConcurrentDictionary<Type, ConverterResolution> Cache = new();

    public static bool TryGetConverter(
        Type type,
        [NotNullWhen(true)] out IndexValueConverter? converter)
    {
        ArgumentNullException.ThrowIfNull(type);

        var resolution = Cache.GetOrAdd(type, static requestedType =>
        {
            // The supported deployment model uses PolyType's runtime provider, so every
            // converter is derived from the same shape source and can be cached by CLR type.
            var shape = ReflectionTypeShapeProvider.Default.GetTypeShape(requestedType);
            return new ConverterResolution((IndexValueConverter?)shape.Invoke(ConverterBuilder.Instance));
        });

        converter = resolution.Converter;
        return converter is not null;
    }

    public static IndexValueConverter<T>? GetConverter<T>(ITypeShape<T> shape)
    {
        ArgumentNullException.ThrowIfNull(shape);

        var resolution = Cache.GetOrAdd(
            typeof(T),
            static (_, suppliedShape) => new ConverterResolution(
                (IndexValueConverter?)suppliedShape.Invoke(ConverterBuilder.Instance)),
            shape);

        return (IndexValueConverter<T>?)resolution.Converter;
    }

    private sealed record ConverterResolution(IndexValueConverter? Converter);

    private sealed class ConverterBuilder : TypeShapeVisitor, ITypeShapeFunc
    {
        public static ConverterBuilder Instance { get; } = new();

        object? ITypeShapeFunc.Invoke<T>(ITypeShape<T> typeShape, object? state)
        {
            if (typeof(T) == typeof(string))
            {
                return new IndexValueConverter<string>(
                    static value => value is null
                        ? null
                        : new IndexValue { Kind = IndexValueKind.String, Text = value },
                    supportsRange: true);
            }

            if (typeof(T) == typeof(char))
            {
                return new IndexValueConverter<char>(
                    static value => new IndexValue { Kind = IndexValueKind.String, Text = value.ToString() },
                    supportsRange: true,
                    queryValueDomain: new IntegralIndexQueryValueDomain(
                        char.MinValue,
                        char.MaxValue,
                        static value => new IndexValue
                        {
                            Kind = IndexValueKind.String,
                            Text = ((char)decimal.ToUInt16(value)).ToString(),
                        }));
            }

            if (typeof(T) == typeof(sbyte))
            {
                return new IndexValueConverter<sbyte>(
                    static value => IndexValue.FromSignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateSignedIntegralDomain(sbyte.MinValue, sbyte.MaxValue));
            }

            if (typeof(T) == typeof(short))
            {
                return new IndexValueConverter<short>(
                    static value => IndexValue.FromSignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateSignedIntegralDomain(short.MinValue, short.MaxValue));
            }

            if (typeof(T) == typeof(int))
            {
                return new IndexValueConverter<int>(
                    static value => IndexValue.FromSignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateSignedIntegralDomain(int.MinValue, int.MaxValue));
            }

            if (typeof(T) == typeof(long))
            {
                return new IndexValueConverter<long>(
                    static value => IndexValue.FromSignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateSignedIntegralDomain(long.MinValue, long.MaxValue));
            }

            if (typeof(T) == typeof(byte))
            {
                return new IndexValueConverter<byte>(
                    static value => IndexValue.FromUnsignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateUnsignedIntegralDomain(byte.MinValue, byte.MaxValue));
            }

            if (typeof(T) == typeof(ushort))
            {
                return new IndexValueConverter<ushort>(
                    static value => IndexValue.FromUnsignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateUnsignedIntegralDomain(ushort.MinValue, ushort.MaxValue));
            }

            if (typeof(T) == typeof(uint))
            {
                return new IndexValueConverter<uint>(
                    static value => IndexValue.FromUnsignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateUnsignedIntegralDomain(uint.MinValue, uint.MaxValue));
            }

            if (typeof(T) == typeof(ulong))
            {
                return new IndexValueConverter<ulong>(
                    static value => IndexValue.FromUnsignedInteger(value),
                    supportsRange: true,
                    queryValueDomain: CreateUnsignedIntegralDomain(ulong.MinValue, ulong.MaxValue));
            }

            if (typeof(T) == typeof(decimal))
            {
                return new IndexValueConverter<decimal>(
                    static value => new IndexValue { Kind = IndexValueKind.Decimal, Decimal = value },
                    supportsRange: true,
                    queryValueDomain: DecimalIndexQueryValueDomain.Instance);
            }

            if (typeof(T) == typeof(float))
            {
                return new IndexValueConverter<float>(
                    static value => !float.IsNaN(value)
                        ? new IndexValue { Kind = IndexValueKind.FloatingPoint, FloatingPoint = value }
                        : throw new NotSupportedException("NaN values cannot be indexed."),
                    supportsRange: true,
                    queryValueDomain: FloatingPointIndexQueryValueDomain.Instance);
            }

            if (typeof(T) == typeof(double))
            {
                return new IndexValueConverter<double>(
                    static value => !double.IsNaN(value)
                        ? new IndexValue { Kind = IndexValueKind.FloatingPoint, FloatingPoint = value }
                        : throw new NotSupportedException("NaN values cannot be indexed."),
                    supportsRange: true,
                    queryValueDomain: FloatingPointIndexQueryValueDomain.Instance);
            }

            if (typeof(T) == typeof(DateTime))
            {
                return new IndexValueConverter<DateTime>(
                    static value => value.Kind == DateTimeKind.Utc
                        ? new IndexValue { Kind = IndexValueKind.Timestamp, UtcTicks = value.Ticks }
                        : throw new ArgumentException("Indexed DateTime values must use DateTimeKind.Utc.", nameof(value)),
                    supportsRange: true);
            }

            if (typeof(T) == typeof(DateTimeOffset))
            {
                return new IndexValueConverter<DateTimeOffset>(
                    static value => new IndexValue { Kind = IndexValueKind.Timestamp, UtcTicks = value.UtcTicks },
                    supportsRange: true);
            }

            if (typeof(T) == typeof(Guid))
            {
                return new IndexValueConverter<Guid>(
                    static value => new IndexValue { Kind = IndexValueKind.Guid, Guid = value },
                    supportsRange: false);
            }

            if (typeof(T) == typeof(bool))
            {
                return new IndexValueConverter<bool>(
                    static value => new IndexValue { Kind = IndexValueKind.Boolean, Boolean = value },
                    supportsRange: false);
            }

            return typeShape.Kind is TypeShapeKind.Enum or TypeShapeKind.Optional
                ? typeShape.Accept(this, state)
                : null;
        }

        private static IntegralIndexQueryValueDomain CreateSignedIntegralDomain(
            decimal minimum,
            decimal maximum)
        {
            return new IntegralIndexQueryValueDomain(
                minimum,
                maximum,
                static value => IndexValue.FromSignedInteger(decimal.ToInt64(value)));
        }

        private static IntegralIndexQueryValueDomain CreateUnsignedIntegralDomain(
            decimal minimum,
            decimal maximum)
        {
            return new IntegralIndexQueryValueDomain(
                minimum,
                maximum,
                static value => IndexValue.FromUnsignedInteger(decimal.ToUInt64(value)));
        }

        public override object? VisitEnum<TEnum, TUnderlying>(
            IEnumTypeShape<TEnum, TUnderlying> enumShape,
            object? state = null)
        {
            var underlyingConverter = GetConverter(enumShape.UnderlyingType)
                ?? throw new InvalidOperationException($"Enum '{typeof(TEnum)}' has an unsupported underlying type '{typeof(TUnderlying)}'.");

            // The CLR guarantees that an enum has the same representation as its underlying type.
            // BitCast keeps this path strongly typed without boxing or reflection-based conversion.
            return new IndexValueConverter<TEnum>(
                value => underlyingConverter.Convert(Unsafe.BitCast<TEnum, TUnderlying>(value)),
                supportsRange: true,
                queryValueDomain: underlyingConverter.QueryValueDomain);
        }

        public override object? VisitOptional<TOptional, TElement>(
            IOptionalTypeShape<TOptional, TElement> optionalShape,
            object? state = null)
        {
            var elementConverter = GetConverter(optionalShape.ElementType);
            if (elementConverter is null)
            {
                return null;
            }

            var deconstructor = optionalShape.GetDeconstructor();
            // A non-null Nullable<T> boxes as T. Keep typed extraction on TOptional while
            // accepting the element's runtime type at the object-based query boundary.
            return new IndexValueConverter<TOptional>(
                value => deconstructor(value, out TElement? element)
                    ? elementConverter.Convert(element!)
                    : null,
                elementConverter.SupportsRange,
                runtimeValueType: elementConverter.RuntimeValueType,
                objectConverter: elementConverter.ConvertObject,
                queryValueDomain: elementConverter.QueryValueDomain);
        }
    }
}
