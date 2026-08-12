using System.Security.Cryptography;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Indexing;

/// <summary>
/// Describes the complete index declaration derived from one registered state type and state name.
/// The descriptor is immutable and contains no application data or index values.
/// </summary>
internal sealed record IndexSchemaDefinition(
    string StateName,
    string StateTypeIdentity,
    int ApplicationSchemaVersion,
    byte[] SchemaKey,
    byte[] Fingerprint,
    IReadOnlyList<IndexPropertyDefinition> Indexes)
{
    public const int DefinitionVersion = 1;
    public const int MembershipFingerprintFormatVersion = 2;
    public const int MembershipExtractorVersion = 1;
    public const int FingerprintLength = 32;
}

/// <summary>
/// Describes one property-level access path without retaining a CLR member or converter instance.
/// </summary>
internal sealed record IndexPropertyDefinition(
    string Name,
    SearchableIndexKind Kind,
    string ValueTypeIdentity,
    IndexKeyCodecId CodecId,
    int CodecVersion,
    bool SupportsRange,
    IndexValueMultiplicity Multiplicity = IndexValueMultiplicity.Scalar,
    int ExtractorVersion = 0);

/// <summary>
/// Produces deterministic identities for the managed index-schema protocol.
/// </summary>
internal static class IndexSchemaIdentity
{
    public const string ControlKeyDomain = "oss:index-schema-control";
    public const int ControlKeyVersion = 1;

    private const int MaximumCanonicalBytes = 64 * 1_024;
    private const int MaximumComponentBytes = 16 * 1_024;
    private const string ManagedScopeMarker = "oss-schema-v1";

    public static IndexSchemaDefinition Create<TState>(
        string stateName,
        int applicationSchemaVersion,
        SearchableTypeModel<TState> model)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applicationSchemaVersion);
        ArgumentNullException.ThrowIfNull(model);

        var indexes = model.Indexes
            .Select(static index => new IndexPropertyDefinition(
                index.Name,
                index.Kind,
                index.ValueTypeIdentity,
                index.Converter.CodecId,
                index.Converter.CodecVersion,
                index.Converter.SupportsRange,
                index.Multiplicity,
                index.ExtractorVersion))
            .OrderBy(static definition => definition.Name, StringComparer.Ordinal)
            .ToArray();
        var hasMembershipIndexes = indexes.Any(
            static index => index.Multiplicity == IndexValueMultiplicity.CollectionMembership);

        var schemaKey = Compute(
            writer =>
            {
                writer.WriteInt32(IndexSchemaDefinition.DefinitionVersion);
                writer.WriteString(model.TypeIdentity, MaximumComponentBytes);
                writer.WriteString(stateName, MaximumComponentBytes);
            });
        var fingerprint = Compute(
            writer =>
            {
                writer.WriteInt32(hasMembershipIndexes
                    ? IndexSchemaDefinition.MembershipFingerprintFormatVersion
                    : IndexSchemaDefinition.DefinitionVersion);
                writer.WriteInt32(applicationSchemaVersion);
                writer.WriteString(model.TypeIdentity, MaximumComponentBytes);
                writer.WriteString(stateName, MaximumComponentBytes);
                writer.WriteInt32(indexes.Length);
                foreach (var index in indexes)
                {
                    writer.WriteString(index.Name, MaximumComponentBytes);
                    writer.WriteInt32((int)index.Kind);
                    writer.WriteString(index.ValueTypeIdentity, MaximumComponentBytes);
                    writer.WriteInt32((int)index.CodecId);
                    writer.WriteInt32(index.CodecVersion);
                    writer.WriteBoolean(index.SupportsRange);
                    if (hasMembershipIndexes)
                    {
                        writer.WriteInt32((int)index.Multiplicity);
                        writer.WriteInt32(index.ExtractorVersion);
                    }
                }
            });

        return new IndexSchemaDefinition(
            stateName,
            model.TypeIdentity,
            applicationSchemaVersion,
            schemaKey,
            fingerprint,
            indexes);
    }

    public static string BindScope(string baseScope, byte[] fingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(baseScope);
        ValidateIdentity(fingerprint, nameof(fingerprint));
        return string.Concat(
            baseScope,
            IndexMetadataProvider.FormatComponent(ManagedScopeMarker),
            IndexMetadataProvider.FormatComponent(Convert.ToHexString(fingerprint)));
    }

    public static bool IsBoundScope(string scope, byte[] fingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ValidateIdentity(fingerprint, nameof(fingerprint));
        var suffix = string.Concat(
            IndexMetadataProvider.FormatComponent(ManagedScopeMarker),
            IndexMetadataProvider.FormatComponent(Convert.ToHexString(fingerprint)));
        return scope.EndsWith(suffix, StringComparison.Ordinal);
    }

    public static byte[] CreateControlKey(string providerName, string stateName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        return Compute(
            writer =>
            {
                writer.WriteString(ControlKeyDomain, MaximumComponentBytes);
                writer.WriteInt32(ControlKeyVersion);
                writer.WriteString(providerName, MaximumComponentBytes);
                writer.WriteString(stateName, MaximumComponentBytes);
            });
    }

    public static void ValidateIdentity(byte[] identity, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(identity, parameterName);
        if (identity.Length != IndexSchemaDefinition.FingerprintLength)
        {
            throw new ArgumentException(
                $"An index-schema identity must contain exactly {IndexSchemaDefinition.FingerprintLength} bytes.",
                parameterName);
        }
    }

    public static bool FixedTimeEquals(byte[] left, byte[] right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return CryptographicOperations.FixedTimeEquals(left, right);
    }

    private static byte[] Compute(Action<CanonicalBinaryWriter> write)
    {
        ArgumentNullException.ThrowIfNull(write);
        using var writer = new CanonicalBinaryWriter(MaximumCanonicalBytes);
        write(writer);
        return SHA256.HashData(writer.WrittenSpan);
    }
}
