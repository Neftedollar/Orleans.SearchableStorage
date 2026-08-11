using System.Security.Cryptography;
using System.Text;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Querying;

internal sealed class ContinuationTokenBinding
{
    private readonly byte[] _layoutFingerprint;
    private readonly byte[] _queryFingerprint;

    public ContinuationTokenBinding(
        string providerName,
        PartitionQueryResponseFamily responseFamily,
        byte[] queryFingerprint,
        int orderingVersion,
        int layoutFormatVersion,
        long routingEpoch,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ValidateFingerprint(queryFingerprint, nameof(queryFingerprint));
        ValidateFingerprint(layoutFingerprint, nameof(layoutFingerprint));
        ArgumentNullException.ThrowIfNull(policy);

        ProviderName = providerName;
        ResponseFamily = responseFamily;
        _queryFingerprint = [.. queryFingerprint];
        OrderingVersion = orderingVersion;
        LayoutFormatVersion = layoutFormatVersion;
        RoutingEpoch = routingEpoch;
        _layoutFingerprint = [.. layoutFingerprint];
        Policy = policy;
    }

    public string ProviderName { get; }

    public PartitionQueryResponseFamily ResponseFamily { get; }

    public byte[] QueryFingerprint => [.. _queryFingerprint];

    public int OrderingVersion { get; }

    public int LayoutFormatVersion { get; }

    public long RoutingEpoch { get; }

    public byte[] LayoutFingerprint => [.. _layoutFingerprint];

    public QueryExecutionPolicy Policy { get; }

    internal ReadOnlySpan<byte> QueryFingerprintSpan => _queryFingerprint;

    internal ReadOnlySpan<byte> LayoutFingerprintSpan => _layoutFingerprint;

    internal static void ValidateFingerprint(byte[] value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if (value.Length != ContinuationTokenCodec.FingerprintBytes)
        {
            throw new ArgumentException(
                $"A query-protocol fingerprint must contain exactly {ContinuationTokenCodec.FingerprintBytes} bytes.",
                parameterName);
        }
    }
}

internal sealed class ContinuationTokenPayload
{
    private readonly byte[] _layoutFingerprint;
    private readonly byte[] _queryFingerprint;

    public ContinuationTokenPayload(ContinuationTokenBinding binding, GrainId after)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ProviderName = binding.ProviderName;
        ResponseFamily = binding.ResponseFamily;
        _queryFingerprint = binding.QueryFingerprint;
        OrderingVersion = binding.OrderingVersion;
        LayoutFormatVersion = binding.LayoutFormatVersion;
        RoutingEpoch = binding.RoutingEpoch;
        _layoutFingerprint = binding.LayoutFingerprint;
        Policy = binding.Policy;
        After = after;
    }

    public static ContinuationTokenPayload CreateFacet(
        ContinuationTokenBinding binding,
        IndexValue afterFacetValue)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(afterFacetValue);
        return new ContinuationTokenPayload(
            binding.ProviderName,
            binding.ResponseFamily,
            binding.QueryFingerprint,
            binding.OrderingVersion,
            binding.LayoutFormatVersion,
            binding.RoutingEpoch,
            binding.LayoutFingerprint,
            binding.Policy,
            after: default,
            afterFacetValue);
    }

    private ContinuationTokenPayload(
        string providerName,
        PartitionQueryResponseFamily responseFamily,
        byte[] queryFingerprint,
        int orderingVersion,
        int layoutFormatVersion,
        long routingEpoch,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy,
        GrainId after,
        IndexValue? afterFacetValue)
    {
        ProviderName = providerName;
        ResponseFamily = responseFamily;
        _queryFingerprint = queryFingerprint;
        OrderingVersion = orderingVersion;
        LayoutFormatVersion = layoutFormatVersion;
        RoutingEpoch = routingEpoch;
        _layoutFingerprint = layoutFingerprint;
        Policy = policy;
        After = after;
        AfterFacetValue = afterFacetValue;
    }

    public string ProviderName { get; }

    public PartitionQueryResponseFamily ResponseFamily { get; }

    public byte[] QueryFingerprint => [.. _queryFingerprint];

    public int OrderingVersion { get; }

    public int LayoutFormatVersion { get; }

    public long RoutingEpoch { get; }

    public byte[] LayoutFingerprint => [.. _layoutFingerprint];

    public QueryExecutionPolicy Policy { get; }

    public GrainId After { get; }

    public IndexValue? AfterFacetValue { get; }

    internal ReadOnlySpan<byte> QueryFingerprintSpan => _queryFingerprint;

    internal ReadOnlySpan<byte> LayoutFingerprintSpan => _layoutFingerprint;

    internal static ContinuationTokenPayload CreateDecoded(
        string providerName,
        PartitionQueryResponseFamily responseFamily,
        byte[] queryFingerprint,
        int orderingVersion,
        int layoutFormatVersion,
        long routingEpoch,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy,
        GrainId after,
        IndexValue? afterFacetValue = null)
    {
        return new ContinuationTokenPayload(
            providerName,
            responseFamily,
            queryFingerprint,
            orderingVersion,
            layoutFormatVersion,
            routingEpoch,
            layoutFingerprint,
            policy,
            after,
            afterFacetValue);
    }
}

internal sealed class ContinuationTokenCodec
{
    internal const int FingerprintBytes = 32;
    internal const int NonceBytes = 12;
    internal const int AuthenticationTagBytes = 16;
    internal const int EnvelopeVersion = 1;
    internal const int Aes256GcmAlgorithm = 1;
    private const int MaximumProviderNameBytes = 1_024;
    private const int MaximumPlaintextBytes = SearchableStorageQueryOptions.MaximumContinuationTokenBytes;
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);
    private readonly SearchableStorageQueryConfiguration _configuration;
    private readonly Dictionary<string, ContinuationProtectionKey> _decryptionKeys;
    private readonly string _providerName;

    public ContinuationTokenCodec(
        string providerName,
        SearchableStorageQueryConfiguration configuration)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(configuration);
        if (StrictUtf8.GetByteCount(providerName) > MaximumProviderNameBytes)
        {
            throw new ArgumentException(
                $"The provider name must not exceed {MaximumProviderNameBytes} UTF-8 bytes.",
                nameof(providerName));
        }

        _providerName = providerName;
        _configuration = configuration;
        _decryptionKeys = new Dictionary<string, ContinuationProtectionKey>(StringComparer.Ordinal);
        if (configuration.CurrentKey is not null)
        {
            _decryptionKeys.Add(configuration.CurrentKey.KeyId, configuration.CurrentKey);
        }

        foreach (var key in configuration.DecryptionKeys)
        {
            _decryptionKeys.Add(key.KeyId, key);
        }
    }

    public string Protect(ContinuationTokenPayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var currentKey = GetRequiredCurrentKey();
        ValidatePayloadForProtection(payload);

        var plaintext = EncodePayload(payload);
        var nonce = new byte[NonceBytes];
        RandomNumberGenerator.Fill(nonce);
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[AuthenticationTagBytes];
        var associatedData = CreateAssociatedData(currentKey.KeyId);
        var keyMaterial = currentKey.CopyKeyMaterial();
        try
        {
            using var aes = new AesGcm(keyMaterial, AuthenticationTagBytes);
            aes.Encrypt(nonce, plaintext, ciphertext, tag, associatedData);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(keyMaterial);
            CryptographicOperations.ZeroMemory(plaintext);
        }

        using var envelope = new CanonicalBinaryWriter();
        envelope.WriteInt32(EnvelopeVersion);
        envelope.WriteInt32(Aes256GcmAlgorithm);
        envelope.WriteString(currentKey.KeyId);
        envelope.WriteRawBytes(nonce);
        envelope.WriteBytes(ciphertext);
        envelope.WriteRawBytes(tag);
        var token = Base64UrlEncode(envelope.WrittenSpan);
        if (token.Length > _configuration.ContinuationTokenByteLimit)
        {
            throw new SearchableStorageQueryConfigurationException(
                "The configured continuation-token byte limit is too small for the protected query cursor.");
        }

        return token;
    }

    public ContinuationTokenPayload Unprotect(
        string token,
        ContinuationTokenBinding expectedBinding)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(token);
        ArgumentNullException.ThrowIfNull(expectedBinding);
        _ = GetRequiredCurrentKey();

        try
        {
            var envelopeBytes = Base64UrlDecode(token, _configuration.ContinuationTokenByteLimit);
            var envelope = new CanonicalBinaryReader(envelopeBytes);
            var envelopeVersion = envelope.ReadInt32();
            var algorithm = envelope.ReadInt32();
            var keyId = envelope.ReadString(
                SearchableStorageContinuationKey.MaximumKeyIdBytes,
                requireNonEmpty: true);
            var nonce = envelope.ReadRawBytes(NonceBytes).ToArray();
            var ciphertext = envelope.ReadBytes(MaximumPlaintextBytes, requireNonEmpty: true);
            var tag = envelope.ReadRawBytes(AuthenticationTagBytes).ToArray();
            envelope.EnsureFullyConsumed();

            if (envelopeVersion != EnvelopeVersion
                || algorithm != Aes256GcmAlgorithm
                || !_decryptionKeys.TryGetValue(keyId, out var key))
            {
                throw InvalidToken();
            }

            var plaintext = new byte[ciphertext.Length];
            var keyMaterial = key.CopyKeyMaterial();
            try
            {
                using var aes = new AesGcm(keyMaterial, AuthenticationTagBytes);
                aes.Decrypt(
                    nonce,
                    ciphertext,
                    tag,
                    plaintext,
                    CreateAssociatedData(keyId));
                var payload = DecodePayload(plaintext);
                ValidateBinding(payload, expectedBinding);
                return payload;
            }
            finally
            {
                CryptographicOperations.ZeroMemory(keyMaterial);
                CryptographicOperations.ZeroMemory(plaintext);
            }
        }
        catch (SearchableStorageStaleContinuationTokenException)
        {
            throw;
        }
        catch (SearchableStorageInvalidContinuationTokenException)
        {
            throw;
        }
        catch (Exception exception) when (IsInvalidTokenFailure(exception))
        {
            throw InvalidToken();
        }
    }

    private ContinuationProtectionKey GetRequiredCurrentKey()
    {
        return _configuration.CurrentKey
            ?? throw new SearchableStorageQueryConfigurationException(
                "A current 32-byte continuation-protection key must be configured before "
                + "public query paging can be used.");
    }

    private void ValidatePayloadForProtection(ContinuationTokenPayload payload)
    {
        if (!string.Equals(payload.ProviderName, _providerName, StringComparison.Ordinal)
            || payload.ResponseFamily is not PartitionQueryResponseFamily.GrainIdPage
                and not PartitionQueryResponseFamily.DistinctFacetValuePage
            || payload.OrderingVersion != GetOrderingVersion(payload.ResponseFamily)
            || payload.LayoutFormatVersion <= 0
            || payload.RoutingEpoch <= 0
            || payload.QueryFingerprintSpan.Length != FingerprintBytes
            || payload.LayoutFingerprintSpan.Length != FingerprintBytes)
        {
            throw new ArgumentException(
                "The continuation payload is not valid for this provider and protocol.",
                nameof(payload));
        }

        ValidatePolicy(payload.Policy);
        if (payload.ResponseFamily == PartitionQueryResponseFamily.GrainIdPage)
        {
            if (payload.After.IsDefault || payload.AfterFacetValue is not null)
            {
                throw new ArgumentException("A grain-id cursor payload is invalid.", nameof(payload));
            }

            GrainIdCanonicalOrder.Validate(payload.After, nameof(payload));
        }
        else if (!payload.After.IsDefault || payload.AfterFacetValue is null)
        {
            throw new ArgumentException("A facet-value cursor payload is invalid.", nameof(payload));
        }
    }

    private static byte[] EncodePayload(ContinuationTokenPayload payload)
    {
        using var writer = new CanonicalBinaryWriter();
        writer.WriteInt32(QueryProtocol.ContinuationPayloadVersion);
        writer.WriteInt32(QueryProtocol.PagingVersion);
        writer.WriteString(payload.ProviderName);
        writer.WriteInt32((int)payload.ResponseFamily);
        writer.WriteRawBytes(payload.QueryFingerprintSpan);
        writer.WriteInt32(payload.OrderingVersion);
        writer.WriteInt32(payload.LayoutFormatVersion);
        writer.WriteInt64(payload.RoutingEpoch);
        writer.WriteRawBytes(payload.LayoutFingerprintSpan);
        if (payload.ResponseFamily == PartitionQueryResponseFamily.GrainIdPage)
        {
            GrainIdCanonicalOrder.Write(writer, payload.After);
        }
        else
        {
            IndexValueCanonicalEncoding.Write(writer, payload.AfterFacetValue!);
        }
        writer.WriteInt32(GetWorkPolicyVersion(payload.ResponseFamily));
        WritePolicy(writer, payload.Policy);
        return writer.ToArray();
    }

    private static ContinuationTokenPayload DecodePayload(ReadOnlySpan<byte> plaintext)
    {
        var reader = new CanonicalBinaryReader(plaintext);
        if (reader.ReadInt32() != QueryProtocol.ContinuationPayloadVersion
            || reader.ReadInt32() != QueryProtocol.PagingVersion)
        {
            throw InvalidToken();
        }

        var providerName = reader.ReadString(MaximumProviderNameBytes, requireNonEmpty: true);
        var responseFamily = (PartitionQueryResponseFamily)reader.ReadInt32();
        var queryFingerprint = reader.ReadRawBytes(FingerprintBytes).ToArray();
        var orderingVersion = reader.ReadInt32();
        var layoutFormatVersion = reader.ReadInt32();
        var routingEpoch = reader.ReadInt64();
        var layoutFingerprint = reader.ReadRawBytes(FingerprintBytes).ToArray();
        var after = responseFamily == PartitionQueryResponseFamily.GrainIdPage
            ? GrainIdCanonicalOrder.Read(ref reader)
            : default;
        var afterFacetValue = responseFamily == PartitionQueryResponseFamily.DistinctFacetValuePage
            ? IndexValueCanonicalEncoding.Read(ref reader)
            : null;
        var workPolicyVersion = reader.ReadInt32();
        var policy = ReadPolicy(ref reader);
        reader.EnsureFullyConsumed();

        if (responseFamily is not PartitionQueryResponseFamily.GrainIdPage
            and not PartitionQueryResponseFamily.DistinctFacetValuePage
            || orderingVersion != GetOrderingVersion(responseFamily)
            || workPolicyVersion != GetWorkPolicyVersion(responseFamily)
            || layoutFormatVersion <= 0
            || routingEpoch <= 0)
        {
            throw InvalidToken();
        }

        ValidatePolicy(policy);
        return ContinuationTokenPayload.CreateDecoded(
            providerName,
            responseFamily,
            queryFingerprint,
            orderingVersion,
            layoutFormatVersion,
            routingEpoch,
            layoutFingerprint,
            policy,
            after,
            afterFacetValue);
    }

    private void ValidateBinding(
        ContinuationTokenPayload payload,
        ContinuationTokenBinding expected)
    {
        if (!string.Equals(payload.ProviderName, _providerName, StringComparison.Ordinal)
            || !string.Equals(payload.ProviderName, expected.ProviderName, StringComparison.Ordinal)
            || payload.ResponseFamily != expected.ResponseFamily
            || payload.OrderingVersion != expected.OrderingVersion
            || !QueryPlanFingerprint.Equals(
                payload.QueryFingerprintSpan,
                expected.QueryFingerprintSpan)
            || payload.Policy != expected.Policy)
        {
            throw InvalidToken();
        }

        if (payload.LayoutFormatVersion != expected.LayoutFormatVersion
            || payload.RoutingEpoch != expected.RoutingEpoch
            || !StorageLayoutFingerprint.Equals(
                payload.LayoutFingerprintSpan,
                expected.LayoutFingerprintSpan))
        {
            throw new SearchableStorageStaleContinuationTokenException();
        }
    }

    private static int GetOrderingVersion(PartitionQueryResponseFamily family)
    {
        return family switch
        {
            PartitionQueryResponseFamily.GrainIdPage => QueryProtocol.OrderingVersion,
            PartitionQueryResponseFamily.DistinctFacetValuePage => QueryProtocol.FacetValueOrderingVersion,
            _ => throw InvalidToken(),
        };
    }

    private static int GetWorkPolicyVersion(PartitionQueryResponseFamily family)
    {
        return family switch
        {
            PartitionQueryResponseFamily.GrainIdPage => QueryProtocol.WorkPolicyVersion,
            PartitionQueryResponseFamily.DistinctFacetValuePage => QueryProtocol.FacetWorkPolicyVersion,
            _ => throw InvalidToken(),
        };
    }

    private byte[] CreateAssociatedData(string keyId)
    {
        using var writer = new CanonicalBinaryWriter();
        writer.WriteString(_providerName);
        writer.WriteInt32(EnvelopeVersion);
        writer.WriteInt32(Aes256GcmAlgorithm);
        writer.WriteString(keyId);
        return writer.ToArray();
    }

    private static void WritePolicy(CanonicalBinaryWriter writer, QueryExecutionPolicy policy)
    {
        writer.WriteInt32(policy.PageSize);
        writer.WriteInt64(policy.PartitionWorkBudget);
        writer.WriteInt32(policy.PartitionResponseItemLimit);
        writer.WriteInt32(policy.PartitionResponseByteLimit);
        writer.WriteInt32(policy.CoordinatorBufferedItemLimit);
        writer.WriteInt32(policy.CoordinatorBufferedByteLimit);
        writer.WriteInt32(policy.PageByteLimit);
    }

    private static QueryExecutionPolicy ReadPolicy(ref CanonicalBinaryReader reader)
    {
        return new QueryExecutionPolicy(
            reader.ReadInt32(),
            reader.ReadInt64(),
            reader.ReadInt32(),
            reader.ReadInt32(),
            reader.ReadInt32(),
            reader.ReadInt32(),
            reader.ReadInt32());
    }

    private static void ValidatePolicy(QueryExecutionPolicy policy)
    {
        if (policy.PageSize <= 0
            || policy.PageSize > SearchableStorageQueryOptions.MaximumPageSize
            || policy.PartitionWorkBudget <= 0
            || policy.PartitionWorkBudget > SearchableStorageQueryOptions.MaximumPartitionWorkBudget
            || policy.PartitionResponseItemLimit <= 0
            || policy.PartitionResponseItemLimit > SearchableStorageQueryOptions.MaximumPartitionResponseItems
            || policy.PartitionResponseByteLimit <= 0
            || policy.PartitionResponseByteLimit > SearchableStorageQueryOptions.MaximumPartitionResponseBytes
            || policy.CoordinatorBufferedItemLimit <= 0
            || policy.CoordinatorBufferedItemLimit > SearchableStorageQueryOptions.MaximumCoordinatorBufferedItems
            || policy.CoordinatorBufferedByteLimit <= 0
            || policy.CoordinatorBufferedByteLimit > SearchableStorageQueryOptions.MaximumCoordinatorBufferedBytes
            || policy.PageByteLimit <= 0
            || policy.PageByteLimit > SearchableStorageQueryOptions.MaximumPageBytes
            || policy.PageByteLimit > policy.CoordinatorBufferedByteLimit
            || policy.PartitionResponseItemLimit > policy.CoordinatorBufferedItemLimit
            || policy.PartitionResponseByteLimit > policy.CoordinatorBufferedByteLimit)
        {
            throw InvalidToken();
        }
    }

    private static string Base64UrlEncode(ReadOnlySpan<byte> value)
    {
        return Convert.ToBase64String(value)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    private static byte[] Base64UrlDecode(string token, int maximumEncodedBytes)
    {
        if (token.Length > maximumEncodedBytes
            || token.Length % 4 == 1
            || token.Any(static character => !IsBase64UrlCharacter(character)))
        {
            throw InvalidToken();
        }

        var paddingLength = (4 - (token.Length % 4)) % 4;
        var normalized = token.Replace('-', '+').Replace('_', '/') + new string('=', paddingLength);
        var decoded = Convert.FromBase64String(normalized);
        var maximumDecodedBytes = checked(((maximumEncodedBytes + 3) / 4) * 3);
        if (decoded.Length > maximumDecodedBytes
            || !string.Equals(Base64UrlEncode(decoded), token, StringComparison.Ordinal))
        {
            throw InvalidToken();
        }

        return decoded;
    }

    private static bool IsBase64UrlCharacter(char value)
    {
        return value is >= 'A' and <= 'Z'
            or >= 'a' and <= 'z'
            or >= '0' and <= '9'
            or '-'
            or '_';
    }

    private static bool IsInvalidTokenFailure(Exception exception)
    {
        return exception is FormatException
            or CryptographicException
            or DecoderFallbackException
            or ArgumentException
            or OverflowException;
    }

    private static SearchableStorageInvalidContinuationTokenException InvalidToken()
    {
        return new SearchableStorageInvalidContinuationTokenException();
    }
}
