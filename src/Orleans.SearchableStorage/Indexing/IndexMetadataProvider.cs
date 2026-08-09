using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Indexing;

internal static class IndexMetadataProvider
{
    private static readonly ConcurrentDictionary<Type, StateIndexMetadata> Cache = new();

    public static IReadOnlyList<IndexEntry> Extract<TState>(string stateName, TState state)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);

        if (state is null)
        {
            return [];
        }

        var metadata = GetMetadata(typeof(TState));
        var entries = new List<IndexEntry>(metadata.Indexes.Count);
        foreach (var index in metadata.Indexes)
        {
            var value = index.Property.GetValue(state);
            if (value is null)
            {
                continue;
            }

            entries.Add(new IndexEntry
            {
                Scope = CreateScope(typeof(TState), stateName, index.Name),
                Kind = index.Kind,
                Value = IndexValue.Create(value),
            });
        }

        return entries;
    }

    public static SelectedIndex GetSelectedIndex<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentNullException.ThrowIfNull(expression);

        Expression body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        if (body is not MemberExpression { Member: PropertyInfo property, Expression: ParameterExpression })
        {
            throw new ArgumentException("The index selector must select one state property.", nameof(expression));
        }

        var metadata = GetMetadata(typeof(TState));
        var index = metadata.Indexes.SingleOrDefault(candidate => candidate.Property == property)
            ?? throw new ArgumentException($"Property '{property.Name}' is not marked with SearchableIndexAttribute.", nameof(expression));

        return new SelectedIndex(CreateScope(typeof(TState), stateName, index.Name), index.Kind, property.PropertyType);
    }

    private static StateIndexMetadata GetMetadata(Type stateType)
    {
        return Cache.GetOrAdd(stateType, CreateMetadata);
    }

    private static StateIndexMetadata CreateMetadata(Type stateType)
    {
        var indexes = new List<PropertyIndexMetadata>();
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var property in stateType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            var attribute = property.GetCustomAttribute<SearchableIndexAttribute>(inherit: true);
            if (attribute is null)
            {
                continue;
            }

            if (property.GetMethod is null || property.GetMethod.IsStatic)
            {
                throw new InvalidOperationException($"Indexed property '{stateType.FullName}.{property.Name}' must be a readable instance property.");
            }

            if (!IndexValue.IsSupported(property.PropertyType))
            {
                throw new NotSupportedException($"Indexed property '{stateType.FullName}.{property.Name}' has unsupported type '{property.PropertyType}'.");
            }

            if (attribute.Kind == SearchableIndexKind.Range && !IndexValue.IsRangeSupported(property.PropertyType))
            {
                throw new NotSupportedException($"Range-indexed property '{stateType.FullName}.{property.Name}' has unordered type '{property.PropertyType}'.");
            }

            var name = string.IsNullOrWhiteSpace(attribute.Name) ? property.Name : attribute.Name;
            if (!names.Add(name!))
            {
                throw new InvalidOperationException($"State type '{stateType.FullName}' contains duplicate index name '{name}'.");
            }

            indexes.Add(new PropertyIndexMetadata(property, name!, attribute.Kind));
        }

        return new StateIndexMetadata(indexes);
    }

    private static string CreateScope(Type stateType, string stateName, string indexName)
    {
        var assemblyName = stateType.Assembly.GetName().Name
            ?? throw new InvalidOperationException($"State type '{stateType}' has no assembly name.");
        var typeName = stateType.FullName
            ?? throw new InvalidOperationException($"State type '{stateType}' has no stable full name.");

        return string.Concat(
            FormatComponent(assemblyName),
            FormatComponent(typeName),
            FormatComponent(stateName),
            FormatComponent(indexName));
    }

    private static string FormatComponent(string value)
    {
        return string.Concat(value.Length.ToString(CultureInfo.InvariantCulture), ":", value);
    }

    private sealed record StateIndexMetadata(IReadOnlyList<PropertyIndexMetadata> Indexes);

    private sealed record PropertyIndexMetadata(PropertyInfo Property, string Name, SearchableIndexKind Kind);
}

internal sealed record SelectedIndex(string Scope, SearchableIndexKind Kind, Type ValueType);
