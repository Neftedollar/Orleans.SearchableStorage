namespace Orleans.SearchableStorage.Indexing;

/// <summary>
/// Restores a canonical indexed value to the CLR property domain selected by a public facet.
/// </summary>
internal static class IndexValueMaterializer
{
    public static TValue Materialize<TValue>(IndexValue value, IndexValueConverter converter)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(converter);
        Querying.IndexValueCanonicalEncoding.Validate(value, nameof(value));
        if (converter.ValueType != typeof(TValue))
        {
            throw new InvalidOperationException(
                $"Facet result type '{typeof(TValue)}' does not match indexed property type '{converter.ValueType}'.");
        }

        return (TValue)MaterializeObject(value, converter);
    }

    public static void Validate(IndexValue value, IndexValueConverter converter)
    {
        _ = MaterializeObject(value, converter);
    }

    private static object MaterializeObject(IndexValue value, IndexValueConverter converter)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(converter);
        Querying.IndexValueCanonicalEncoding.Validate(value, nameof(value));
        var declaredType = converter.ValueType;
        var targetType = Nullable.GetUnderlyingType(declaredType) ?? declaredType;
        object materialized;
        if (targetType.IsEnum)
        {
            var underlying = Enum.GetUnderlyingType(targetType);
            materialized = Enum.ToObject(targetType, MaterializePrimitive(value, underlying));
        }
        else
        {
            materialized = MaterializePrimitive(value, targetType);
        }

        return materialized;
    }

    private static object MaterializePrimitive(IndexValue value, Type targetType)
    {
        if (targetType == typeof(string) && value.Kind == IndexValueKind.String)
        {
            return value.Text!;
        }

        if (targetType == typeof(char) && value.Kind == IndexValueKind.String
            && value.Text is { Length: 1 })
        {
            return value.Text[0];
        }

        if (targetType == typeof(sbyte) && value.Kind == IndexValueKind.SignedInteger)
        {
            return checked((sbyte)value.SignedInteger);
        }

        if (targetType == typeof(short) && value.Kind == IndexValueKind.SignedInteger)
        {
            return checked((short)value.SignedInteger);
        }

        if (targetType == typeof(int) && value.Kind == IndexValueKind.SignedInteger)
        {
            return checked((int)value.SignedInteger);
        }

        if (targetType == typeof(long) && value.Kind == IndexValueKind.SignedInteger)
        {
            return value.SignedInteger;
        }

        if (targetType == typeof(byte) && value.Kind == IndexValueKind.UnsignedInteger)
        {
            return checked((byte)value.UnsignedInteger);
        }

        if (targetType == typeof(ushort) && value.Kind == IndexValueKind.UnsignedInteger)
        {
            return checked((ushort)value.UnsignedInteger);
        }

        if (targetType == typeof(uint) && value.Kind == IndexValueKind.UnsignedInteger)
        {
            return checked((uint)value.UnsignedInteger);
        }

        if (targetType == typeof(ulong) && value.Kind == IndexValueKind.UnsignedInteger)
        {
            return value.UnsignedInteger;
        }

        if (targetType == typeof(decimal) && value.Kind == IndexValueKind.Decimal)
        {
            return value.Decimal;
        }

        if (targetType == typeof(float) && value.Kind == IndexValueKind.FloatingPoint)
        {
            var result = (float)value.FloatingPoint;
            if ((double)result != value.FloatingPoint)
            {
                throw new InvalidOperationException(
                    "The canonical floating-point value is not representable as the indexed Single value.");
            }

            return result;
        }

        if (targetType == typeof(double) && value.Kind == IndexValueKind.FloatingPoint)
        {
            return value.FloatingPoint;
        }

        if (targetType == typeof(DateTime) && value.Kind == IndexValueKind.Timestamp)
        {
            return new DateTime(value.UtcTicks, DateTimeKind.Utc);
        }

        if (targetType == typeof(DateTimeOffset) && value.Kind == IndexValueKind.Timestamp)
        {
            return new DateTimeOffset(value.UtcTicks, TimeSpan.Zero);
        }

        if (targetType == typeof(Guid) && value.Kind == IndexValueKind.Guid)
        {
            return value.Guid;
        }

        if (targetType == typeof(bool) && value.Kind == IndexValueKind.Boolean)
        {
            return value.Boolean;
        }

        throw new InvalidOperationException(
            $"Canonical index kind '{value.Kind}' cannot be materialized as '{targetType}'.");
    }
}
