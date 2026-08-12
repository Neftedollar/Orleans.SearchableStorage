using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class ContinuationTokenCodecTests
{
    private const string ProviderName = "token-provider-with-a-unique-identity";
    private static readonly byte[] CurrentMaterial = SHA256.HashData("current secret"u8);
    private static readonly byte[] OldMaterial = SHA256.HashData("old secret"u8);
    private static readonly byte[] NewMaterial = SHA256.HashData("new secret"u8);

    [Fact]
    public void CapturedVersionOneWorkPolicyTokenFailsClosedAfterPolicyBump()
    {
        const string capturedToken =
            "AAAAAQAAAAEAAAAHY3VycmVudMfuhzQ6VBL6HqXT3wAAAMl28Tm577txN9R0FMJ76Cjof9pStuYR280I3XkxqXU8Ag_6AouNhd4jKrzeJ8l6O83_d0ChJ-dvrfe4hh8ipli4FXrsTi4eJ2nzPsxY1PuBmIdSioEbLEGWxekSlo2xBIc3eARbUx3Z4jrpmRSNvy244qrAt1aTocZC-J8SFh7X-yYlTegblznGxeUvQWuX45xGk3-chCB77Y4rUKz8I3p-46R9xDKfr1wY9Is3A0LWMbkOLc4XYMK40hNrlrxvzoz4f9VPLPpgY0B56qnnTkANBtkoV7g5utqt";
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();

        Action decode = () => _ = codec.Unprotect(capturedToken, binding);

        AssertInvalid(decode);
    }

    [Fact]
    public void RoundTripPreservesEveryBoundFieldAndExclusiveFrontier()
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var after = GrainId.Create(
            "frontier-type-with-a-unique-value",
            "frontier-key-with-a-unique-value");

        var token = codec.Protect(new ContinuationTokenPayload(binding, after));
        var decoded = codec.Unprotect(token, binding);

        decoded.ProviderName.Should().Be(ProviderName);
        decoded.ResponseFamily.Should().Be(PartitionQueryResponseFamily.GrainIdPage);
        decoded.QueryFingerprint.Should().Equal(binding.QueryFingerprint);
        decoded.OrderingVersion.Should().Be(QueryProtocol.OrderingVersion);
        decoded.LayoutFormatVersion.Should().Be(4);
        decoded.RoutingEpoch.Should().Be(17);
        decoded.LayoutFingerprint.Should().Equal(binding.LayoutFingerprint);
        decoded.Policy.Should().Be(binding.Policy);
        decoded.After.Should().Be(after);
    }

    [Fact]
    public void IdenticalPayloadsUseFreshNinetySixBitNonces()
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var payload = new ContinuationTokenPayload(
            binding,
            GrainId.Create("paging", "after"));

        var first = codec.Protect(payload);
        var second = codec.Protect(payload);

        first.Should().NotBe(second);
        ReadEnvelope(first).Nonce.Should().HaveCount(ContinuationTokenCodec.NonceBytes);
        ReadEnvelope(first).Nonce.Should().NotEqual(ReadEnvelope(second).Nonce);
        codec.Unprotect(first, binding).After.Should().Be(payload.After);
        codec.Unprotect(second, binding).After.Should().Be(payload.After);
    }

    [Fact]
    public void RotationAcceptsAnExplicitOldDecryptOnlyKey()
    {
        var oldCodec = CreateCodec("old", OldMaterial);
        var binding = CreateBinding();
        var token = oldCodec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var rotatedCodec = CreateCodec(
            "new",
            NewMaterial,
            new SearchableStorageContinuationKey("old", OldMaterial));

        rotatedCodec.Unprotect(token, binding).After.Should().Be(GrainId.Create("paging", "after"));
        ReadEnvelope(rotatedCodec.Protect(
                new ContinuationTokenPayload(binding, GrainId.Create("paging", "next"))))
            .KeyId.Should().Be("new");
    }

    [Fact]
    public void RemovingARotationKeyInvalidatesItsOutstandingTokens()
    {
        var binding = CreateBinding();
        var oldToken = CreateCodec("old", OldMaterial).Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var codecWithoutOldKey = CreateCodec("new", NewMaterial);

        Action decode = () => _ = codecWithoutOldKey.Unprotect(oldToken, binding);

        AssertInvalid(decode);
    }

    [Theory]
    [InlineData(TokenMutation.EnvelopeVersion)]
    [InlineData(TokenMutation.Algorithm)]
    [InlineData(TokenMutation.KeyId)]
    [InlineData(TokenMutation.Nonce)]
    [InlineData(TokenMutation.Ciphertext)]
    [InlineData(TokenMutation.Tag)]
    public void AlteredEnvelopeFieldsFailClosedWithoutAnAuthenticationOracle(TokenMutation mutation)
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var token = codec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var bytes = Base64UrlDecode(token);
        var keyIdLength = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(8, 4));
        var nonceOffset = 12 + keyIdLength;
        var ciphertextOffset = nonceOffset + ContinuationTokenCodec.NonceBytes + sizeof(int);
        var offset = mutation switch
        {
            TokenMutation.EnvelopeVersion => 3,
            TokenMutation.Algorithm => 7,
            TokenMutation.KeyId => 12,
            TokenMutation.Nonce => nonceOffset,
            TokenMutation.Ciphertext => ciphertextOffset,
            TokenMutation.Tag => bytes.Length - 1,
            _ => throw new ArgumentOutOfRangeException(nameof(mutation)),
        };
        bytes[offset] ^= 0x01;

        Action decode = () => _ = codec.Unprotect(Base64UrlEncode(bytes), binding);

        AssertInvalid(decode);
    }

    [Fact]
    public void ProviderIdentityIsAuthenticatedAssociatedData()
    {
        var originalCodec = CreateCodec("current", CurrentMaterial);
        var originalBinding = CreateBinding();
        var token = originalCodec.Protect(
            new ContinuationTokenPayload(originalBinding, GrainId.Create("paging", "after")));
        var otherCodec = CreateCodec("current", CurrentMaterial, providerName: "another-provider");
        var otherBinding = CreateBinding(providerName: "another-provider");

        Action decode = () => _ = otherCodec.Unprotect(token, otherBinding);

        AssertInvalid(decode);
    }

    [Fact]
    public void QueryFamilyAndPolicyBindingsCannotBeSubstituted()
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var token = codec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var wrongQuery = CreateBinding(queryFingerprint: SHA256.HashData("other query"u8));
        var wrongFamily = CreateBinding(responseFamily: (PartitionQueryResponseFamily)2);
        var wrongPolicy = CreateBinding(policy: binding.Policy with { PageSize = binding.Policy.PageSize + 1 });

        AssertInvalid(() => _ = codec.Unprotect(token, wrongQuery));
        AssertInvalid(() => _ = codec.Unprotect(token, wrongFamily));
        AssertInvalid(() => _ = codec.Unprotect(token, wrongPolicy));
    }

    [Fact]
    public void AuthenticatedLayoutMismatchIsReportedAsStale()
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var token = codec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var movedLayout = CreateBinding(routingEpoch: binding.RoutingEpoch + 1);

        Action decode = () => _ = codec.Unprotect(token, movedLayout);

        decode.Should().Throw<SearchableStorageStaleContinuationTokenException>();
    }

    [Theory]
    [InlineData(
        StorageLayout.MovementFormatVersion,
        StorageLayout.IndexSchemaFormatVersion)]
    [InlineData(
        StorageLayout.IndexSchemaFormatVersion,
        StorageLayout.MovementFormatVersion)]
    public void RoutingCompatibleSchemaFenceKeepsTokensBoundToTheirQuery(
        int tokenLayoutFormatVersion,
        int currentLayoutFormatVersion)
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var tokenBinding = CreateBinding(layoutFormatVersion: tokenLayoutFormatVersion);
        var token = codec.Protect(new ContinuationTokenPayload(
            tokenBinding,
            GrainId.Create("paging", "after")));
        var compatibleBinding = CreateBinding(layoutFormatVersion: currentLayoutFormatVersion);
        var anotherSchemaQuery = CreateBinding(
            layoutFormatVersion: currentLayoutFormatVersion,
            queryFingerprint: SHA256.HashData("another schema-bound query"u8));

        codec.Unprotect(token, compatibleBinding).After
            .Should().Be(GrainId.Create("paging", "after"));
        AssertInvalid(() => _ = codec.Unprotect(token, anotherSchemaQuery));
    }

    [Fact]
    public void OversizedAndNonCanonicalBase64UrlTokensAreRejectedBeforeCryptography()
    {
        var options = CreateOptions("current", CurrentMaterial);
        options.ContinuationTokenByteLimit = 256;
        var codec = new ContinuationTokenCodec(
            ProviderName,
            SearchableStorageQueryConfiguration.Create(options));
        var binding = CreateBinding();

        AssertInvalid(() => _ = codec.Unprotect(new string('A', 257), binding));
        AssertInvalid(() => _ = codec.Unprotect("AA=", binding));
        AssertInvalid(() => _ = codec.Unprotect("A", binding));
    }

    [Fact]
    public void AuthenticatedMalformedPlaintextIsRejectedAfterSuccessfulAeadValidation()
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var token = codec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var malformed = AppendAuthenticatedPlaintextByte(
            token,
            ProviderName,
            CurrentMaterial);

        Action decode = () => _ = codec.Unprotect(malformed, binding);

        AssertInvalid(decode);
    }

    [Theory]
    [InlineData(AuthenticatedField.TokenVersion)]
    [InlineData(AuthenticatedField.PagingVersion)]
    [InlineData(AuthenticatedField.ResponseFamily)]
    [InlineData(AuthenticatedField.OrderingVersion)]
    [InlineData(AuthenticatedField.WorkPolicyVersion)]
    public void AuthenticatedUnknownProtocolFieldsFailClosed(AuthenticatedField field)
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var token = codec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var altered = ReprotectAuthenticatedPlaintext(
            token,
            ProviderName,
            CurrentMaterial,
            plaintext => MutateProtocolField(plaintext, field));

        Action decode = () => _ = codec.Unprotect(altered, binding);

        AssertInvalid(decode);
    }

    [Fact]
    public void AuthenticatedVersionOneGrainPageWorkPolicyFailsClosed()
    {
        QueryProtocol.WorkPolicyVersion.Should().Be(2);
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var token = codec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));
        var altered = ReprotectAuthenticatedPlaintext(
            token,
            ProviderName,
            CurrentMaterial,
            plaintext => MutateProtocolField(
                plaintext,
                AuthenticatedField.WorkPolicyVersion,
                replacement: 1));

        Action decode = () => _ = codec.Unprotect(altered, binding);

        AssertInvalid(decode);
    }

    [Fact]
    public void ClearEnvelopeDoesNotRevealProviderFingerprintsOrFrontier()
    {
        var codec = CreateCodec("current-key-identifier", CurrentMaterial);
        var binding = CreateBinding(
            queryFingerprint: SHA256.HashData("query-fingerprint-secret"u8),
            layoutFingerprint: SHA256.HashData("layout-fingerprint-secret"u8));
        var after = GrainId.Create(
            "frontier-type-with-a-unique-value",
            "frontier-key-with-a-unique-value");

        var decodedEnvelope = Base64UrlDecode(
            codec.Protect(new ContinuationTokenPayload(binding, after)));

        Contains(decodedEnvelope, Encoding.UTF8.GetBytes("current-key-identifier")).Should().BeTrue();
        Contains(decodedEnvelope, Encoding.UTF8.GetBytes(ProviderName)).Should().BeFalse();
        Contains(decodedEnvelope, binding.QueryFingerprint).Should().BeFalse();
        Contains(decodedEnvelope, binding.LayoutFingerprint).Should().BeFalse();
        Contains(decodedEnvelope, after.Type.AsSpan()).Should().BeFalse();
        Contains(decodedEnvelope, after.Key.AsSpan()).Should().BeFalse();
    }

    [Fact]
    public void DefaultFrontierCannotBeProtected()
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();

        Action protect = () => _ = codec.Protect(
            new ContinuationTokenPayload(binding, default));

        protect.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(GrainIdCanonicalOrder.MaximumTypeBytes, GrainIdCanonicalOrder.MaximumKeyBytes)]
    public void CanonicalFrontierBoundsProduceResumableTokens(int typeLength, int keyLength)
    {
        var options = CreateOptions("current", CurrentMaterial);
        options.ContinuationTokenByteLimit = SearchableStorageQueryOptions.MaximumContinuationTokenBytes;
        var codec = new ContinuationTokenCodec(
            ProviderName,
            SearchableStorageQueryConfiguration.Create(options));
        var binding = CreateBinding();
        var frontier = CreateRawGrainId(typeLength, keyLength);

        var token = codec.Protect(new ContinuationTokenPayload(binding, frontier));

        token.Length.Should().BeLessThanOrEqualTo(options.ContinuationTokenByteLimit);
        codec.Unprotect(token, binding).After.Should().Be(frontier);
    }

    [Fact]
    public void MaximumFacetTextCursorFitsTheHardProtectedTokenCeiling()
    {
        var providerName = new string('p', 1_024);
        var keyId = new string('k', SearchableStorageContinuationKey.MaximumKeyIdBytes);
        var options = CreateOptions(keyId, CurrentMaterial);
        options.ContinuationTokenByteLimit = SearchableStorageQueryOptions.MaximumContinuationTokenBytes;
        var codec = new ContinuationTokenCodec(
            providerName,
            SearchableStorageQueryConfiguration.Create(options));
        var binding = CreateBinding(
            providerName: providerName,
            responseFamily: PartitionQueryResponseFamily.DistinctFacetValuePage);
        var frontier = IndexValue.Create(new string('x', IndexValueCanonicalEncoding.MaximumTextBytes));

        var token = codec.Protect(ContinuationTokenPayload.CreateFacet(binding, frontier));
        var decoded = codec.Unprotect(token, binding);

        token.Length.Should().BeLessThanOrEqualTo(SearchableStorageQueryOptions.MaximumContinuationTokenBytes);
        decoded.AfterFacetValue.Should().Be(frontier);
        decoded.After.Should().Be(default);
    }

    [Fact]
    public void OversizedFacetTextCursorIsRejectedBeforeTokenEmission()
    {
        var options = CreateOptions("current", CurrentMaterial);
        options.ContinuationTokenByteLimit = SearchableStorageQueryOptions.MaximumContinuationTokenBytes;
        var codec = new ContinuationTokenCodec(
            ProviderName,
            SearchableStorageQueryConfiguration.Create(options));
        var binding = CreateBinding(
            responseFamily: PartitionQueryResponseFamily.DistinctFacetValuePage);
        var frontier = IndexValue.Create(new string(
            'x',
            IndexValueCanonicalEncoding.MaximumTextBytes + 1));

        Action protect = () => _ = codec.Protect(
            ContinuationTokenPayload.CreateFacet(binding, frontier));

        protect.Should().Throw<CanonicalEncodingLimitExceededException>();
    }

    [Fact]
    public void FacetCursorIsBoundToItsResponseFamilyAndFacetPolicy()
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var facet = CreateBinding(
            responseFamily: PartitionQueryResponseFamily.DistinctFacetValuePage);
        var token = codec.Protect(ContinuationTokenPayload.CreateFacet(
            facet,
            IndexValue.Create("frontier")));
        var regular = CreateBinding();
        var changedPolicy = CreateBinding(
            responseFamily: PartitionQueryResponseFamily.DistinctFacetValuePage,
            policy: facet.Policy with { PartitionWorkBudget = facet.Policy.PartitionWorkBudget + 1 });

        AssertInvalid(() => _ = codec.Unprotect(token, regular));
        AssertInvalid(() => _ = codec.Unprotect(token, changedPolicy));
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(GrainIdCanonicalOrder.MaximumTypeBytes + 1, 1)]
    [InlineData(1, 0)]
    [InlineData(1, GrainIdCanonicalOrder.MaximumKeyBytes + 1)]
    public void EmptyAndOversizedFrontiersCannotBeProtected(int typeLength, int keyLength)
    {
        var codec = CreateCodec("current", CurrentMaterial);
        var binding = CreateBinding();
        var frontier = CreateRawGrainId(typeLength, keyLength);

        Action protect = () => _ = codec.Protect(
            new ContinuationTokenPayload(binding, frontier));

        protect.Should().Throw<ArgumentException>();
    }

    private static ContinuationTokenCodec CreateCodec(
        string currentKeyId,
        byte[] currentMaterial,
        SearchableStorageContinuationKey? decryptOnly = null,
        string providerName = ProviderName)
    {
        var options = CreateOptions(currentKeyId, currentMaterial);
        if (decryptOnly is not null)
        {
            options.ContinuationProtection.DecryptionKeys.Add(decryptOnly);
        }

        return new ContinuationTokenCodec(
            providerName,
            SearchableStorageQueryConfiguration.Create(options));
    }

    private static SearchableStorageQueryOptions CreateOptions(string keyId, byte[] material)
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            keyId,
            material);
        return options;
    }

    private static GrainId CreateRawGrainId(int typeLength, int keyLength)
    {
        var type = Enumerable.Repeat((byte)'t', typeLength).ToArray();
        var key = Enumerable.Repeat((byte)'k', keyLength).ToArray();
        return GrainId.Create(new GrainType(type), new IdSpan(key));
    }

    private static ContinuationTokenBinding CreateBinding(
        string providerName = ProviderName,
        PartitionQueryResponseFamily responseFamily = PartitionQueryResponseFamily.GrainIdPage,
        byte[]? queryFingerprint = null,
        int layoutFormatVersion = StorageLayout.MovementFormatVersion,
        long routingEpoch = 17,
        byte[]? layoutFingerprint = null,
        QueryExecutionPolicy? policy = null)
    {
        return new ContinuationTokenBinding(
            providerName,
            responseFamily,
            queryFingerprint ?? SHA256.HashData("query fingerprint"u8),
            responseFamily == PartitionQueryResponseFamily.DistinctFacetValuePage
                ? QueryProtocol.FacetValueOrderingVersion
                : QueryProtocol.OrderingVersion,
            layoutFormatVersion,
            routingEpoch,
            layoutFingerprint ?? SHA256.HashData("layout fingerprint"u8),
            policy ?? new QueryExecutionPolicy(
                PageSize: 23,
                PartitionWorkBudget: 1000,
                PartitionResponseItemLimit: 100,
                PartitionResponseByteLimit: 10_000,
                CoordinatorBufferedItemLimit: 1000,
                CoordinatorBufferedByteLimit: 100_000,
                PageByteLimit: 50_000));
    }

    private static EncodedEnvelope ReadEnvelope(string token)
    {
        var reader = new CanonicalBinaryReader(Base64UrlDecode(token));
        reader.ReadInt32().Should().Be(ContinuationTokenCodec.EnvelopeVersion);
        reader.ReadInt32().Should().Be(ContinuationTokenCodec.Aes256GcmAlgorithm);
        var keyId = reader.ReadString(SearchableStorageContinuationKey.MaximumKeyIdBytes, true);
        var nonce = reader.ReadRawBytes(ContinuationTokenCodec.NonceBytes).ToArray();
        _ = reader.ReadBytes(SearchableStorageQueryOptions.MaximumContinuationTokenBytes, true);
        _ = reader.ReadRawBytes(ContinuationTokenCodec.AuthenticationTagBytes);
        reader.EnsureFullyConsumed();
        return new EncodedEnvelope(keyId, nonce);
    }

    private static string AppendAuthenticatedPlaintextByte(
        string token,
        string providerName,
        byte[] keyMaterial)
    {
        return ReprotectAuthenticatedPlaintext(
            token,
            providerName,
            keyMaterial,
            static plaintext => [.. plaintext, 0x7f]);
    }

    private static string ReprotectAuthenticatedPlaintext(
        string token,
        string providerName,
        byte[] keyMaterial,
        Func<byte[], byte[]> transform)
    {
        var reader = new CanonicalBinaryReader(Base64UrlDecode(token));
        var envelopeVersion = reader.ReadInt32();
        var algorithm = reader.ReadInt32();
        var keyId = reader.ReadString(SearchableStorageContinuationKey.MaximumKeyIdBytes, true);
        var oldNonce = reader.ReadRawBytes(ContinuationTokenCodec.NonceBytes).ToArray();
        var ciphertext = reader.ReadBytes(SearchableStorageQueryOptions.MaximumContinuationTokenBytes, true);
        var oldTag = reader.ReadRawBytes(ContinuationTokenCodec.AuthenticationTagBytes).ToArray();
        reader.EnsureFullyConsumed();
        var associatedData = CreateAssociatedData(providerName, keyId);
        var plaintext = new byte[ciphertext.Length];
        using (var aes = new AesGcm(keyMaterial, ContinuationTokenCodec.AuthenticationTagBytes))
        {
            aes.Decrypt(oldNonce, ciphertext, oldTag, plaintext, associatedData);
        }

        var transformed = transform(plaintext);
        var nonce = RandomNumberGenerator.GetBytes(ContinuationTokenCodec.NonceBytes);
        var transformedCiphertext = new byte[transformed.Length];
        var tag = new byte[ContinuationTokenCodec.AuthenticationTagBytes];
        using (var aes = new AesGcm(keyMaterial, ContinuationTokenCodec.AuthenticationTagBytes))
        {
            aes.Encrypt(nonce, transformed, transformedCiphertext, tag, associatedData);
        }

        using var writer = new CanonicalBinaryWriter();
        writer.WriteInt32(envelopeVersion);
        writer.WriteInt32(algorithm);
        writer.WriteString(keyId);
        writer.WriteRawBytes(nonce);
        writer.WriteBytes(transformedCiphertext);
        writer.WriteRawBytes(tag);
        return Base64UrlEncode(writer.WrittenSpan);
    }

    private static byte[] MutateProtocolField(
        byte[] plaintext,
        AuthenticatedField field,
        int? replacement = null)
    {
        var providerLength = BinaryPrimitives.ReadInt32BigEndian(plaintext.AsSpan(8, sizeof(int)));
        var familyOffset = 12 + providerLength;
        var orderingOffset = familyOffset + sizeof(int) + ContinuationTokenCodec.FingerprintBytes;
        var grainOffset = orderingOffset
            + sizeof(int)
            + sizeof(int)
            + sizeof(long)
            + ContinuationTokenCodec.FingerprintBytes;
        var typeLength = BinaryPrimitives.ReadInt32BigEndian(
            plaintext.AsSpan(grainOffset, sizeof(int)));
        var keyLengthOffset = grainOffset + sizeof(int) + typeLength;
        var keyLength = BinaryPrimitives.ReadInt32BigEndian(
            plaintext.AsSpan(keyLengthOffset, sizeof(int)));
        var workPolicyOffset = keyLengthOffset + sizeof(int) + keyLength;
        var offset = field switch
        {
            AuthenticatedField.TokenVersion => 0,
            AuthenticatedField.PagingVersion => sizeof(int),
            AuthenticatedField.ResponseFamily => familyOffset,
            AuthenticatedField.OrderingVersion => orderingOffset,
            AuthenticatedField.WorkPolicyVersion => workPolicyOffset,
            _ => throw new ArgumentOutOfRangeException(nameof(field)),
        };
        var value = replacement
            ?? (field == AuthenticatedField.WorkPolicyVersion
                ? checked(QueryProtocol.WorkPolicyVersion + 1)
                : 2);
        BinaryPrimitives.WriteInt32BigEndian(plaintext.AsSpan(offset, sizeof(int)), value);
        return plaintext;
    }

    private static byte[] CreateAssociatedData(string providerName, string keyId)
    {
        using var writer = new CanonicalBinaryWriter();
        writer.WriteString(providerName);
        writer.WriteInt32(ContinuationTokenCodec.EnvelopeVersion);
        writer.WriteInt32(ContinuationTokenCodec.Aes256GcmAlgorithm);
        writer.WriteString(keyId);
        return writer.ToArray();
    }

    private static bool Contains(ReadOnlySpan<byte> source, ReadOnlySpan<byte> value)
    {
        return source.IndexOf(value) >= 0;
    }

    private static string Base64UrlEncode(ReadOnlySpan<byte> value)
    {
        return Convert.ToBase64String(value)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    private static byte[] Base64UrlDecode(string value)
    {
        var padding = new string('=', (4 - value.Length % 4) % 4);
        return Convert.FromBase64String(value.Replace('-', '+').Replace('_', '/') + padding);
    }

    private static void AssertInvalid(Action action)
    {
        action.Should().Throw<SearchableStorageInvalidContinuationTokenException>()
            .WithMessage("The searchable-storage continuation token is invalid.");
    }

    public enum TokenMutation
    {
        EnvelopeVersion,
        Algorithm,
        KeyId,
        Nonce,
        Ciphertext,
        Tag,
    }

    public enum AuthenticatedField
    {
        TokenVersion,
        PagingVersion,
        ResponseFamily,
        OrderingVersion,
        WorkPolicyVersion,
    }

    private sealed record EncodedEnvelope(string KeyId, byte[] Nonce);
}
