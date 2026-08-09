using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Orleans.SearchableStorage.Storage;
using PolyType.Abstractions;
using PolyType.ReflectionProvider;

namespace Orleans.SearchableStorage.Indexing;

internal static class IndexMetadataProvider
{
    public static IReadOnlyList<IndexEntry> Extract<TState>(string stateName, TState state)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);

        if (state is null)
        {
            return [];
        }

        var model = GetTypeModel<TState>();
        var entries = new List<IndexEntry>(model.Indexes.Count);
        foreach (var index in model.Indexes)
        {
            var value = index.Read(ref state);
            if (value is null)
            {
                continue;
            }

            entries.Add(new IndexEntry
            {
                Scope = CreateScope(typeof(TState), stateName, index.Name),
                Kind = index.Kind,
                Value = value,
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

        var model = GetTypeModel<TState>();
        var index = model.Indexes.SingleOrDefault(candidate => IsSameProperty(candidate.MemberInfo, property))
            ?? throw new ArgumentException($"Property '{property.Name}' is not marked with SearchableIndexAttribute.", nameof(expression));

        return new SelectedIndex(
            CreateScope(typeof(TState), stateName, index.Name),
            index.Kind,
            index.Converter);
    }

    private static bool IsSameProperty(MemberInfo indexedMember, PropertyInfo selectedProperty)
    {
        if (indexedMember.Equals(selectedProperty))
        {
            return true;
        }

        if (indexedMember is not PropertyInfo indexedProperty)
        {
            return false;
        }

        // PolyType and expression trees can expose different PropertyInfo views of one inherited
        // property. Accessor base definitions provide a stable identity without reading values.
        var indexedGetter = indexedProperty.GetMethod;
        var selectedGetter = selectedProperty.GetMethod;
        if (indexedGetter is null || selectedGetter is null)
        {
            return false;
        }

        var indexedDefinition = indexedGetter.GetBaseDefinition();
        var selectedDefinition = selectedGetter.GetBaseDefinition();
        return indexedDefinition.Module.Equals(selectedDefinition.Module)
            && indexedDefinition.MetadataToken == selectedDefinition.MetadataToken
            && indexedDefinition.DeclaringType == selectedDefinition.DeclaringType;
    }

    internal static SearchableTypeModel<TState> GetTypeModel<TState>()
    {
        var model = Volatile.Read(ref TypeModelCache<TState>.Value);
        if (model is not null)
        {
            return model;
        }

        lock (TypeModelCache<TState>.SyncRoot)
        {
            // A failed model build is deliberately not cached. Invalid state declarations should
            // continue to report their original validation exception instead of a type initializer error.
            return TypeModelCache<TState>.Value ??= CreateTypeModel<TState>();
        }
    }

    private static SearchableTypeModel<TState> CreateTypeModel<TState>()
    {
        var shape = ReflectionTypeShapeProvider.Default.GetTypeShape<TState>();
        if (shape is not IObjectTypeShape<TState> objectShape)
        {
            throw new NotSupportedException($"State type '{typeof(TState)}' does not expose an object shape through PolyType.");
        }

        var indexes = new List<PropertyIndexMetadata<TState>>();
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var property in objectShape.Properties)
        {
            var attribute = property.AttributeProvider.GetCustomAttribute<SearchableIndexAttribute>(inherit: true);
            if (attribute is null)
            {
                continue;
            }

            if (property.IsField || !property.HasGetter || !property.IsGetterPublic)
            {
                throw new InvalidOperationException(
                    $"Indexed property '{typeof(TState).FullName}.{property.Name}' must be a readable instance property.");
            }

            var name = string.IsNullOrWhiteSpace(attribute.Name) ? property.Name : attribute.Name;
            if (!names.Add(name!))
            {
                throw new InvalidOperationException($"State type '{typeof(TState).FullName}' contains duplicate index name '{name}'.");
            }

            var context = new PropertyBuildContext(name!, attribute.Kind);
            var index = (PropertyIndexMetadata<TState>?)property.Accept(PropertyIndexBuilder.Instance, context);
            if (index is null)
            {
                throw new NotSupportedException(
                    $"Indexed property '{typeof(TState).FullName}.{property.Name}' has unsupported type '{property.PropertyType.Type}'.");
            }

            if (attribute.Kind == SearchableIndexKind.Range && !index.Converter.SupportsRange)
            {
                throw new NotSupportedException(
                    $"Range-indexed property '{typeof(TState).FullName}.{property.Name}' has unordered type '{property.PropertyType.Type}'.");
            }

            indexes.Add(index);
        }

        return new SearchableTypeModel<TState>(indexes);
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

    private static class TypeModelCache<TState>
    {
        public static readonly object SyncRoot = new();

        public static SearchableTypeModel<TState>? Value;
    }

    private sealed record PropertyBuildContext(string Name, SearchableIndexKind Kind);

    private sealed class PropertyIndexBuilder : TypeShapeVisitor
    {
        public static PropertyIndexBuilder Instance { get; } = new();

        public override object? VisitProperty<TState, TValue>(
            IPropertyShape<TState, TValue> propertyShape,
            object? state = null)
        {
            var context = (PropertyBuildContext?)state
                ?? throw new ArgumentNullException(nameof(state));
            var converter = IndexValueConverterProvider.GetConverter(propertyShape.PropertyType);
            if (converter is null)
            {
                return null;
            }

            var memberInfo = propertyShape.MemberInfo
                ?? throw new InvalidOperationException(
                    $"PolyType did not expose member identity for '{typeof(TState).FullName}.{propertyShape.Name}'.");

            return new PropertyIndexMetadata<TState, TValue>(
                memberInfo,
                context.Name,
                context.Kind,
                propertyShape.GetGetter(),
                converter);
        }
    }
}

internal sealed record SearchableTypeModel<TState>(IReadOnlyList<PropertyIndexMetadata<TState>> Indexes);

internal abstract class PropertyIndexMetadata<TState>(
    MemberInfo memberInfo,
    string name,
    SearchableIndexKind kind,
    IndexValueConverter converter)
{
    public MemberInfo MemberInfo { get; } = memberInfo;

    public string Name { get; } = name;

    public SearchableIndexKind Kind { get; } = kind;

    public IndexValueConverter Converter { get; } = converter;

    public abstract IndexValue? Read(ref TState state);
}

internal sealed class PropertyIndexMetadata<TState, TValue>(
    MemberInfo memberInfo,
    string name,
    SearchableIndexKind kind,
    Getter<TState, TValue> getter,
    IndexValueConverter<TValue> converter)
    : PropertyIndexMetadata<TState>(memberInfo, name, kind, converter)
{
    public override IndexValue? Read(ref TState state)
    {
        return converter.Convert(getter(ref state));
    }
}

internal sealed record SelectedIndex(
    string Scope,
    SearchableIndexKind Kind,
    IndexValueConverter Converter);
