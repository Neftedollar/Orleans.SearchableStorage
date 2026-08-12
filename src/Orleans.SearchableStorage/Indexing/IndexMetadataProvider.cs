using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Orleans.SearchableStorage.Storage;
using PolyType;
using PolyType.Abstractions;
using PolyType.ReflectionProvider;

namespace Orleans.SearchableStorage.Indexing;

internal static class IndexMetadataProvider
{
    public static IReadOnlyList<IndexEntry> Extract<TState>(string stateName, TState state)
    {
        return Extract(stateName, state, schemaFingerprint: null);
    }

    public static IReadOnlyList<IndexEntry> Extract<TState>(
        string stateName,
        TState state,
        byte[]? schemaFingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);

        if (schemaFingerprint is not null)
        {
            IndexSchemaIdentity.ValidateIdentity(schemaFingerprint, nameof(schemaFingerprint));
        }

        if (state is null)
        {
            return [];
        }

        var model = GetTypeModel<TState>();
        var entries = new List<IndexEntry>(model.Indexes.Count);
        var values = new List<IndexValue>();
        foreach (var index in model.Indexes)
        {
            values.Clear();
            index.AppendValues(ref state, values);
            if (values.Count == 0)
            {
                continue;
            }

            var scope = schemaFingerprint is null
                ? index.GetScope(stateName)
                : IndexSchemaIdentity.BindScope(index.GetScope(stateName), schemaFingerprint);
            foreach (var value in values)
            {
                entries.Add(new IndexEntry
                {
                    Scope = scope,
                    Kind = index.Kind,
                    Value = value,
                });
            }
        }

        return entries;
    }

    public static IndexSchemaDefinition GetSchemaDefinition<TState>(
        string stateName,
        int applicationSchemaVersion = 1)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applicationSchemaVersion);
        return IndexSchemaIdentity.Create(
            stateName,
            applicationSchemaVersion,
            GetTypeModel<TState>());
    }

    public static SelectedIndex GetSelectedIndex<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> expression,
        byte[]? schemaFingerprint = null)
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

        return GetSelectedIndex<TState>(
            stateName,
            property,
            nameof(expression),
            schemaFingerprint);
    }

    public static SelectedIndex GetSelectedIndex<TState>(
        string stateName,
        PropertyInfo property,
        string parameterName,
        byte[]? schemaFingerprint = null)
    {
        var index = GetDeclaredIndex<TState>(stateName, property, parameterName, schemaFingerprint);
        if (index.Multiplicity != IndexValueMultiplicity.Scalar)
        {
            throw new ArgumentException(
                $"Indexed property '{property.Name}' is a collection membership index; this operation requires a scalar index.",
                parameterName);
        }

        return index;
    }

    public static SelectedIndex GetCollectionMembershipIndex<TState>(
        string stateName,
        PropertyInfo property,
        string parameterName,
        byte[]? schemaFingerprint = null)
    {
        var index = GetDeclaredIndex<TState>(stateName, property, parameterName, schemaFingerprint);
        if (index.Multiplicity != IndexValueMultiplicity.CollectionMembership)
        {
            throw new ArgumentException(
                $"Indexed property '{property.Name}' is a scalar index; collection Contains requires a collection membership index.",
                parameterName);
        }

        return index;
    }

    public static SelectedIndex GetDeclaredIndex<TState>(
        string stateName,
        PropertyInfo property,
        string parameterName,
        byte[]? schemaFingerprint = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentNullException.ThrowIfNull(property);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameterName);

        var model = GetTypeModel<TState>();
        var index = model.Indexes.SingleOrDefault(candidate => IsSameProperty(candidate.MemberInfo, property))
            ?? throw new ArgumentException(
                $"Property '{property.Name}' is not marked with SearchableIndexAttribute.",
                parameterName);

        var scope = index.GetScope(stateName);
        if (schemaFingerprint is not null)
        {
            IndexSchemaIdentity.ValidateIdentity(schemaFingerprint, nameof(schemaFingerprint));
            scope = IndexSchemaIdentity.BindScope(scope, schemaFingerprint);
        }

        return new SelectedIndex(
            scope,
            index.Kind,
            index.Converter,
            property.Name,
            index.Multiplicity);
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
        var typeIdentity = CreateTypeIdentity(typeof(TState));
        var shape = ReflectionTypeShapeProvider.Default.GetTypeShape<TState>();
        if (shape is not IObjectTypeShape<TState> objectShape)
        {
            // Collection and scalar state types remain valid Orleans state. They cannot declare
            // searchable properties, so their correct type model is an empty one.
            return new SearchableTypeModel<TState>(typeIdentity, []);
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

            var context = new PropertyBuildContext(typeIdentity, name!, attribute.Kind);
            var index = (PropertyIndexMetadata<TState>?)property.Accept(PropertyIndexBuilder.Instance, context);
            if (index is null)
            {
                throw new NotSupportedException(
                    $"Indexed property '{typeof(TState).FullName}.{property.Name}' has unsupported type '{property.PropertyType.Type}'.");
            }

            if (attribute.Kind == SearchableIndexKind.Range
                && index.Multiplicity == IndexValueMultiplicity.CollectionMembership)
            {
                throw new NotSupportedException(
                    $"Collection property '{typeof(TState).FullName}.{property.Name}' supports only "
                    + $"{nameof(SearchableIndexKind.Hash)} indexes.");
            }

            if (attribute.Kind == SearchableIndexKind.Range && !index.SupportsRange)
            {
                throw new NotSupportedException(
                    $"Range-indexed property '{typeof(TState).FullName}.{property.Name}' has unordered type '{property.PropertyType.Type}'.");
            }

            indexes.Add(index);
        }

        return new SearchableTypeModel<TState>(typeIdentity, indexes);
    }

    internal static string CreateScope(string typeIdentity, string stateName, string indexName)
    {
        return string.Concat(
            FormatComponent(typeIdentity),
            FormatComponent(stateName),
            FormatComponent(indexName));
    }

    internal static string CreateTypeIdentity(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (type.ContainsGenericParameters)
        {
            throw new InvalidOperationException($"Type '{type}' does not have a closed persisted identity.");
        }

        var builder = new StringBuilder();
        AppendTypeIdentity(builder, type);
        return builder.ToString();
    }

    private static void AppendTypeIdentity(StringBuilder builder, Type type)
    {
        if (type.IsArray)
        {
            AppendComponent(builder, type.IsSZArray ? "szarray" : "array");
            AppendComponent(builder, type.GetArrayRank().ToString(CultureInfo.InvariantCulture));
            AppendTypeIdentity(
                builder,
                type.GetElementType()
                    ?? throw new InvalidOperationException($"Array type '{type}' has no element type."));
            return;
        }

        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            var arguments = type.GetGenericArguments();
            AppendComponent(builder, "generic");
            AppendNamedTypeIdentity(builder, definition);
            AppendComponent(builder, arguments.Length.ToString(CultureInfo.InvariantCulture));
            foreach (var argument in arguments)
            {
                AppendTypeIdentity(builder, argument);
            }

            return;
        }

        AppendComponent(builder, "named");
        AppendNamedTypeIdentity(builder, type);
    }

    private static void AppendNamedTypeIdentity(StringBuilder builder, Type type)
    {
        var assemblyName = type.Assembly.GetName();
        var simpleName = assemblyName.Name
            ?? throw new InvalidOperationException($"Type '{type}' has no assembly name.");
        var typeName = type.FullName
            ?? throw new InvalidOperationException($"Type '{type}' has no stable full name.");
        var publicKeyToken = assemblyName.GetPublicKeyToken();

        AppendComponent(builder, simpleName);
        AppendComponent(builder, assemblyName.CultureName ?? string.Empty);
        AppendComponent(
            builder,
            publicKeyToken is { Length: > 0 }
                ? Convert.ToHexString(publicKeyToken)
                : string.Empty);
        AppendComponent(builder, typeName);
    }

    private static void AppendComponent(StringBuilder builder, string value)
    {
        builder.Append(FormatComponent(value));
    }

    internal static string FormatComponent(string value)
    {
        return string.Concat(value.Length.ToString(CultureInfo.InvariantCulture), ":", value);
    }

    private static class TypeModelCache<TState>
    {
        public static readonly object SyncRoot = new();

        public static SearchableTypeModel<TState>? Value;
    }

    private sealed record PropertyBuildContext(
        string TypeIdentity,
        string Name,
        SearchableIndexKind Kind);

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
            var memberInfo = propertyShape.MemberInfo
                ?? throw new InvalidOperationException(
                    $"PolyType did not expose member identity for '{typeof(TState).FullName}.{propertyShape.Name}'.");

            if (converter is null)
            {
                if (propertyShape.PropertyType.Kind != TypeShapeKind.Enumerable)
                {
                    return null;
                }

                return propertyShape.PropertyType.Accept(
                    CollectionPropertyIndexBuilder<TState>.Instance,
                    new CollectionPropertyBuildContext<TState, TValue>(
                        context,
                        memberInfo,
                        propertyShape.GetGetter()));
            }

            return new PropertyIndexMetadata<TState, TValue>(
                context.TypeIdentity,
                memberInfo,
                context.Name,
                context.Kind,
                CreateTypeIdentity(propertyShape.PropertyType.Type),
                propertyShape.GetGetter(),
                converter);
        }
    }

    private sealed record CollectionPropertyBuildContext<TState, TCollection>(
        PropertyBuildContext Property,
        MemberInfo MemberInfo,
        Getter<TState, TCollection> Getter);

    private sealed class CollectionPropertyIndexBuilder<TState> : TypeShapeVisitor
    {
        public static CollectionPropertyIndexBuilder<TState> Instance { get; } = new();

        public override object? VisitEnumerable<TCollection, TElement>(
            IEnumerableTypeShape<TCollection, TElement> enumerableShape,
            object? state = null)
        {
            var context = (CollectionPropertyBuildContext<TState, TCollection>?)state
                ?? throw new ArgumentNullException(nameof(state));
            var collectionType = typeof(TCollection);
            var isExactArray = collectionType.IsSZArray
                && collectionType.GetElementType() == typeof(TElement);
            var isExactList = collectionType.IsGenericType
                && collectionType.GetGenericTypeDefinition() == typeof(List<>)
                && collectionType.GetGenericArguments()[0] == typeof(TElement);
            if ((!isExactArray && !isExactList)
                || enumerableShape.Rank != 1
                || enumerableShape.IsAsyncEnumerable
                || enumerableShape.IsSetType)
            {
                return null;
            }

            var converter = IndexValueConverterProvider.GetConverter(enumerableShape.ElementType);
            if (converter is null)
            {
                return null;
            }

            return new CollectionPropertyIndexMetadata<TState, TCollection, TElement>(
                context.Property.TypeIdentity,
                context.MemberInfo,
                context.Property.Name,
                context.Property.Kind,
                CreateTypeIdentity(typeof(TCollection)),
                context.Getter,
                enumerableShape.GetGetEnumerable(),
                converter);
        }
    }
}

internal sealed record SearchableTypeModel<TState>(
    string TypeIdentity,
    IReadOnlyList<PropertyIndexMetadata<TState>> Indexes);

internal abstract class PropertyIndexMetadata<TState>(
    string typeIdentity,
    MemberInfo memberInfo,
    string name,
    SearchableIndexKind kind,
    string valueTypeIdentity,
    IndexValueConverter converter,
    IndexValueMultiplicity multiplicity = IndexValueMultiplicity.Scalar,
    int extractorVersion = 0)
{
    private readonly ConcurrentDictionary<string, string> _scopes = new(StringComparer.Ordinal);
    private readonly string _typeIdentity = typeIdentity;

    public MemberInfo MemberInfo { get; } = memberInfo;

    public string Name { get; } = name;

    public SearchableIndexKind Kind { get; } = kind;

    public string ValueTypeIdentity { get; } = valueTypeIdentity;

    public IndexValueConverter Converter { get; } = converter;

    public bool SupportsRange { get; } = multiplicity == IndexValueMultiplicity.Scalar
        && converter.SupportsRange;

    public IndexValueMultiplicity Multiplicity { get; } = multiplicity;

    public int ExtractorVersion { get; } = extractorVersion;

    public string GetScope(string stateName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);

        // State names are provider configuration, so this cache is bounded by the finite set of
        // persistent-state registrations which use this state type in the process.
        return _scopes.GetOrAdd(
            stateName,
            static (currentStateName, identity) => IndexMetadataProvider.CreateScope(
                identity.TypeIdentity,
                currentStateName,
                identity.IndexName),
            (TypeIdentity: _typeIdentity, IndexName: Name));
    }

    public abstract void AppendValues(ref TState state, List<IndexValue> destination);
}

internal sealed class PropertyIndexMetadata<TState, TValue>(
    string typeIdentity,
    MemberInfo memberInfo,
    string name,
    SearchableIndexKind kind,
    string valueTypeIdentity,
    Getter<TState, TValue> getter,
    IndexValueConverter<TValue> converter)
    : PropertyIndexMetadata<TState>(
        typeIdentity,
        memberInfo,
        name,
        kind,
        valueTypeIdentity,
        converter)
{
    public override void AppendValues(ref TState state, List<IndexValue> destination)
    {
        var value = converter.Convert(getter(ref state));
        if (value is not null)
        {
            destination.Add(value);
        }
    }
}

internal sealed class CollectionPropertyIndexMetadata<TState, TCollection, TElement>(
    string typeIdentity,
    MemberInfo memberInfo,
    string name,
    SearchableIndexKind kind,
    string valueTypeIdentity,
    Getter<TState, TCollection> getter,
    Func<TCollection, IEnumerable<TElement>> getEnumerable,
    IndexValueConverter<TElement> converter)
    : PropertyIndexMetadata<TState>(
        typeIdentity,
        memberInfo,
        name,
        kind,
        valueTypeIdentity,
        converter,
        IndexValueMultiplicity.CollectionMembership,
        IndexSchemaDefinition.MembershipExtractorVersion)
{
    public override void AppendValues(ref TState state, List<IndexValue> destination)
    {
        var collection = getter(ref state);
        if (collection is null)
        {
            return;
        }

        var unique = new HashSet<IndexValue>();
        foreach (var element in getEnumerable(collection))
        {
            var value = converter.Convert(element);
            if (value is null || !unique.Add(value))
            {
                continue;
            }

            if (unique.Count > SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope)
            {
                throw new SearchableStorageCapacityExceededException(
                    StorageCapacityGuardrails.RecordScopeIndexEntries,
                    unique.Count,
                    SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope);
            }
        }

        destination.AddRange(unique);
        destination.Sort();
    }
}

internal enum IndexValueMultiplicity
{
    Scalar = 0,
    CollectionMembership = 1,
}

internal sealed record SelectedIndex(
    string Scope,
    SearchableIndexKind Kind,
    IndexValueConverter Converter,
    string PropertyName,
    IndexValueMultiplicity Multiplicity = IndexValueMultiplicity.Scalar);
