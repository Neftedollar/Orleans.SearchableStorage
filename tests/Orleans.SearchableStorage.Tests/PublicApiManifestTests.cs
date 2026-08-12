using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Orleans.SearchableStorage.ApiContract;

namespace Orleans.SearchableStorage.Tests;

public sealed class PublicApiManifestTests
{
    [Fact]
    public void ShippingPublicApiMatchesReviewedManifest()
    {
        var expectedPath = Path.Combine(AppContext.BaseDirectory, "public-api.txt");
        var expected = File.ReadAllText(expectedPath).Replace("\r\n", "\n", StringComparison.Ordinal);
        var actual = PublicApiManifest.Generate(typeof(SearchableStorageOptions).Assembly);
        if (actual == expected)
        {
            return;
        }

        var commonLength = Math.Min(actual.Length, expected.Length);
        var firstDifference = 0;
        while (firstDifference < commonLength
               && actual[firstDifference] == expected[firstDifference])
        {
            firstDifference++;
        }
        var actualPath = Path.Combine(Path.GetTempPath(), "Orleans.SearchableStorage.public-api.actual.txt");
        File.WriteAllText(actualPath, actual);
        Assert.Fail(
            $"Shipping public API differs from eng/public-api.txt near character {firstDifference}. "
            + $"Review the compatibility impact, then run the generator intentionally. Actual: {actualPath}");
    }

    [Fact]
    public void FormatterCapturesSignaturesNullabilityConstraintsDefaultsAndAccessors()
    {
        var fixture = typeof(PublicApiManifestTests).GetNestedType(
            "FormatterFixture`1",
            System.Reflection.BindingFlags.Public)
            ?? throw new InvalidOperationException("Formatter fixture type is missing.");
        var manifest = PublicApiManifest.GenerateType(fixture);

        Assert.Contains(
            "type [System.ObsoleteAttribute(message=\"fixture type\", error=true, "
            + "diagnosticId=\"OSSFIX001\", urlFormat=\"https://example.test/{0}\")] "
            + "public abstract class Orleans.SearchableStorage.Tests.PublicApiManifestTests.FormatterFixture<T>",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains("  generic T : class, System.IDisposable, new()", manifest, StringComparison.Ordinal);
        Assert.Contains("  field public const System.Int32 DefaultCount = 7", manifest, StringComparison.Ordinal);
        Assert.Contains(
            "  property public System.String? Name { get; protected init; }",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains(
            "  property public abstract T? Current { get; protected set; }",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains(
            "  event public abstract System.EventHandler? Changed",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains("  method public System.Void Overload()", manifest, StringComparison.Ordinal);
        Assert.Contains(
            "  method public [System.ObsoleteAttribute(message=\"fixture member\", error=false, "
            + "diagnosticId=null, urlFormat=null)] System.Void Overload(System.String? value)",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains(
            "  method public abstract T? Transform<TValue>(in System.Int32 count, "
            + "ref System.String? text, out TValue? value, System.Int32 limit = 7, "
            + "System.Threading.CancellationToken cancellationToken = "
            + "default(System.Threading.CancellationToken)) "
            + "where TValue : class, System.IDisposable, new()",
            manifest,
            StringComparison.Ordinal);
    }

    [Fact]
    public void FormatterCapturesStableTypeAttributes()
    {
        var flags = PublicApiManifest.GenerateType(typeof(FormatterOptions));
        var usage = PublicApiManifest.GenerateType(typeof(FormatterMarkerAttribute));

        Assert.Contains(
            "type [System.FlagsAttribute] public enum "
            + "Orleans.SearchableStorage.Tests.PublicApiManifestTests.FormatterOptions",
            flags,
            StringComparison.Ordinal);
        Assert.Contains("  underlying System.Byte", flags, StringComparison.Ordinal);
        Assert.Contains(
            "  field public const Orleans.SearchableStorage.Tests.PublicApiManifestTests.FormatterOptions "
            + "First = Orleans.SearchableStorage.Tests.PublicApiManifestTests.FormatterOptions.First",
            flags,
            StringComparison.Ordinal);
        Assert.Contains(
            "type [System.AttributeUsageAttribute(validOn=System.AttributeTargets.Class | "
            + "System.AttributeTargets.Method, "
            + "allowMultiple=true, inherited=false)] public sealed class "
            + "Orleans.SearchableStorage.Tests.PublicApiManifestTests.FormatterMarkerAttribute",
            usage,
            StringComparison.Ordinal);
    }

    [Fact]
    public void FormatterCapturesCompilerSignificantApiMetadata()
    {
        var fixture = typeof(PublicApiManifestTests).GetNestedType(
            "AdvancedFormatterFixture`4",
            System.Reflection.BindingFlags.Public)
            ?? throw new InvalidOperationException("Advanced formatter fixture type is missing.");
        var manifest = PublicApiManifest.GenerateType(fixture);
        Assert.Contains("  generic TUnmanaged : unmanaged", manifest, StringComparison.Ordinal);
        Assert.Contains("  generic TNullable : class?", manifest, StringComparison.Ordinal);
        Assert.Contains("  generic TAllows : allows ref struct", manifest, StringComparison.Ordinal);
        Assert.Contains(
            "[System.Diagnostics.CodeAnalysis.SetsRequiredMembersAttribute]",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains(
            "[System.Runtime.CompilerServices.DynamicAttribute",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains(
            "[System.Runtime.CompilerServices.TupleElementNamesAttribute",
            manifest,
            StringComparison.Ordinal);
        Assert.Contains("write-nullability=nullable", manifest, StringComparison.Ordinal);
        Assert.Contains(
            "System.Diagnostics.CodeAnalysis.AllowNullAttribute",
            manifest,
            StringComparison.Ordinal);
    }

    [Obsolete(
        "fixture type",
        error: true,
        DiagnosticId = "OSSFIX001",
        UrlFormat = "https://example.test/{0}")]
    public abstract class FormatterFixture<T>
        where T : class, IDisposable, new()
    {
        public const int DefaultCount = 7;

        public string? Name { get; protected init; }

        public abstract T? Current { get; protected set; }

        public abstract event EventHandler? Changed;

        public void Overload()
        {
        }

        [Obsolete("fixture member")]
        public void Overload(string? value)
        {
        }

        public abstract T? Transform<TValue>(
            in int count,
            ref string? text,
            out TValue value,
            int limit = DefaultCount,
            CancellationToken cancellationToken = default)
            where TValue : class, IDisposable, new();
    }

    public sealed class AdvancedFormatterFixture<TUnmanaged, TNotNull, TNullable, TAllows>
        where TUnmanaged : unmanaged
        where TNotNull : notnull
        where TNullable : class?
        where TAllows : allows ref struct
    {
        [SetsRequiredMembers]
        public AdvancedFormatterFixture()
        {
            RequiredName = string.Empty;
        }

        public required string RequiredName { get; init; }

        [AllowNull]
        public string Name { get; set; } = string.Empty;

        public dynamic Transform((int Count, string? Label) value, dynamic input) => input;
    }

    [Flags]
    public enum FormatterOptions : byte
    {
        None = 0,
        First = 1,
        Second = 2,
    }

    [AttributeUsage(
        AttributeTargets.Class | AttributeTargets.Method,
        AllowMultiple = true,
        Inherited = false)]
    public sealed class FormatterMarkerAttribute : Attribute
    {
    }
}
