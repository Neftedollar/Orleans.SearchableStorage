using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Orleans.SearchableStorage.SourceCompatibility;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SourceGenericConstraintAnalyzer : DiagnosticAnalyzer
{
    public const string MissingBaselineEntryId = "OSSAPI001";
    public const string StaleBaselineEntryId = "OSSAPI002";
    public const string InvalidBaselineId = "OSSAPI003";

    private const string ShippedFileName = "SourceConstraints.Shipped.txt";
    private const string UnshippedFileName = "SourceConstraints.Unshipped.txt";

    private static readonly DiagnosticDescriptor MissingBaselineEntry = new(
        MissingBaselineEntryId,
        "Declare the public C# generic constraint",
        "Source constraint signature '{0}' is not part of the reviewed baseline",
        "ApiDesign",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Every public or protected generic parameter, including an unconstrained one, must be reviewed.",
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    private static readonly DiagnosticDescriptor StaleBaselineEntry = new(
        StaleBaselineEntryId,
        "Remove or mark the obsolete C# generic constraint",
        "Reviewed source constraint signature '{0}' is not present in the compilation",
        "ApiDesign",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A reviewed public or protected generic constraint changed or was removed.",
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    private static readonly DiagnosticDescriptor InvalidBaseline = new(
        InvalidBaselineId,
        "Provide one valid source constraint baseline pair",
        "Source constraint baseline is invalid: {0}",
        "ApiDesign",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    private static readonly SymbolDisplayFormat ConstraintTypeFormat = new(
        globalNamespaceStyle: SymbolDisplayGlobalNamespaceStyle.Included,
        typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces,
        genericsOptions: SymbolDisplayGenericsOptions.IncludeTypeParameters,
        miscellaneousOptions:
            SymbolDisplayMiscellaneousOptions.EscapeKeywordIdentifiers
            | SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier
            | SymbolDisplayMiscellaneousOptions.UseSpecialTypes);

    private static readonly SymbolDisplayFormat MethodOwnerFormat = new(
        globalNamespaceStyle: SymbolDisplayGlobalNamespaceStyle.Included,
        typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces,
        genericsOptions: SymbolDisplayGenericsOptions.IncludeTypeParameters,
        memberOptions:
            SymbolDisplayMemberOptions.IncludeContainingType
            | SymbolDisplayMemberOptions.IncludeParameters
            | SymbolDisplayMemberOptions.IncludeExplicitInterface,
        parameterOptions:
            SymbolDisplayParameterOptions.IncludeType
            | SymbolDisplayParameterOptions.IncludeParamsRefOut,
        miscellaneousOptions:
            SymbolDisplayMiscellaneousOptions.EscapeKeywordIdentifiers
            | SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier
            | SymbolDisplayMiscellaneousOptions.UseSpecialTypes);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [MissingBaselineEntry, StaleBaselineEntry, InvalidBaseline];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationAction(AnalyzeCompilation);
    }

    private static void AnalyzeCompilation(CompilationAnalysisContext context)
    {
        var shippedFiles = FindFiles(context, ShippedFileName);
        var unshippedFiles = FindFiles(context, UnshippedFileName);
        if (shippedFiles.Length != 1 || unshippedFiles.Length != 1)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                InvalidBaseline,
                Location.None,
                $"expected exactly one {ShippedFileName} and one {UnshippedFileName} AdditionalFile"));
            return;
        }

        if (!TryReadBaseline(shippedFiles[0], context.CancellationToken, out var shipped)
            || !TryReadBaseline(unshippedFiles[0], context.CancellationToken, out var unshipped))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                InvalidBaseline,
                Location.None,
                "a baseline file could not be read"));
            return;
        }

        var duplicate = shipped.Intersect(unshipped, StringComparer.Ordinal).FirstOrDefault();
        if (duplicate is not null)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                InvalidBaseline,
                Location.None,
                $"duplicate entry '{duplicate}'"));
            return;
        }

        var expected = shipped.Union(unshipped, StringComparer.Ordinal).ToImmutableHashSet(StringComparer.Ordinal);
        var actual = CollectCurrentSignatures(context.Compilation.Assembly.GlobalNamespace);

        foreach (var pair in actual.OrderBy(static pair => pair.Key, StringComparer.Ordinal))
        {
            if (!expected.Contains(pair.Key))
            {
                context.ReportDiagnostic(Diagnostic.Create(MissingBaselineEntry, pair.Value, pair.Key));
            }
        }

        foreach (var entry in expected.Except(actual.Keys, StringComparer.Ordinal).OrderBy(static entry => entry, StringComparer.Ordinal))
        {
            context.ReportDiagnostic(Diagnostic.Create(StaleBaselineEntry, Location.None, entry));
        }
    }

    private static ImmutableArray<AdditionalText> FindFiles(CompilationAnalysisContext context, string fileName) =>
        context.Options.AdditionalFiles
            .Where(file => string.Equals(Path.GetFileName(file.Path), fileName, StringComparison.Ordinal))
            .GroupBy(static file => Path.GetFullPath(file.Path), StringComparer.Ordinal)
            .Select(static group => group.First())
            .ToImmutableArray();

    private static bool TryReadBaseline(
        AdditionalText file,
        CancellationToken cancellationToken,
        out ImmutableHashSet<string> entries)
    {
        var text = file.GetText(cancellationToken);
        if (text is null)
        {
            entries = ImmutableHashSet<string>.Empty;
            return false;
        }

        entries = text.Lines
            .Select(line => line.ToString().Trim())
            .Where(static line => line.Length > 0 && !line.StartsWith("#", StringComparison.Ordinal))
            .ToImmutableHashSet(StringComparer.Ordinal);
        return true;
    }

    private static ImmutableDictionary<string, Location> CollectCurrentSignatures(INamespaceSymbol root)
    {
        var entries = ImmutableDictionary.CreateBuilder<string, Location>(StringComparer.Ordinal);
        VisitNamespace(root, entries);
        return entries.ToImmutable();
    }

    private static void VisitNamespace(
        INamespaceSymbol namespaceSymbol,
        ImmutableDictionary<string, Location>.Builder entries)
    {
        foreach (var childNamespace in namespaceSymbol.GetNamespaceMembers())
        {
            VisitNamespace(childNamespace, entries);
        }

        foreach (var type in namespaceSymbol.GetTypeMembers())
        {
            VisitType(type, containingTypeIsVisible: true, entries);
        }
    }

    private static void VisitType(
        INamedTypeSymbol type,
        bool containingTypeIsVisible,
        ImmutableDictionary<string, Location>.Builder entries)
    {
        var isVisible = containingTypeIsVisible && IsExternallyVisible(type.DeclaredAccessibility, type.ContainingType is null);
        if (!isVisible || type.IsImplicitlyDeclared)
        {
            return;
        }

        AddTypeParameters($"type {type.ToDisplayString(ConstraintTypeFormat)}", type.TypeParameters, entries);

        foreach (var method in type.GetMembers().OfType<IMethodSymbol>())
        {
            if (method.MethodKind == MethodKind.Ordinary
                && !method.IsImplicitlyDeclared
                && IsExternallyVisible(method.DeclaredAccessibility, isTopLevelType: false))
            {
                AddTypeParameters($"method {method.ToDisplayString(MethodOwnerFormat)}", method.TypeParameters, entries);
            }
        }

        foreach (var nestedType in type.GetTypeMembers())
        {
            VisitType(nestedType, isVisible, entries);
        }
    }

    private static bool IsExternallyVisible(Accessibility accessibility, bool isTopLevelType) =>
        accessibility == Accessibility.Public
        || (!isTopLevelType
            && (accessibility == Accessibility.Protected
                || accessibility == Accessibility.ProtectedOrInternal));

    private static void AddTypeParameters(
        string owner,
        ImmutableArray<ITypeParameterSymbol> parameters,
        ImmutableDictionary<string, Location>.Builder entries)
    {
        foreach (var parameter in parameters)
        {
            var signature = $"{owner} :: {parameter.Name} = {FormatConstraints(parameter)}";
            entries.Add(signature, parameter.Locations.FirstOrDefault(static location => location.IsInSource) ?? Location.None);
        }
    }

    private static string FormatConstraints(ITypeParameterSymbol parameter)
    {
        var constraints = new List<string>();
        if (parameter.HasUnmanagedTypeConstraint)
        {
            constraints.Add("unmanaged");
        }
        else if (parameter.HasValueTypeConstraint)
        {
            constraints.Add("struct");
        }
        else if (parameter.HasReferenceTypeConstraint)
        {
            constraints.Add(parameter.ReferenceTypeConstraintNullableAnnotation == NullableAnnotation.Annotated
                ? "class?"
                : "class");
        }
        else if (parameter.HasNotNullConstraint)
        {
            constraints.Add("notnull");
        }

        constraints.AddRange(parameter.ConstraintTypes
            .Select(type => type.ToDisplayString(ConstraintTypeFormat))
            .OrderBy(static type => type, StringComparer.Ordinal));

        if (parameter.HasConstructorConstraint)
        {
            constraints.Add("new()");
        }

        if (parameter.AllowsRefLikeType)
        {
            constraints.Add("allows ref struct");
        }

        return constraints.Count == 0 ? "<none>" : string.Join(", ", constraints);
    }
}
