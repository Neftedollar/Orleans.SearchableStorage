using System.Security.Cryptography;
using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.TestGrains;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexSchemaFacetContinuationBindingTests
{
    [Fact]
    public void DistinctFacetTokenWithV1SchemaFingerprintIsRejectedByV2Binding()
    {
        const string providerName = "facet-schema-binding";
        const string stateName = "facet-schema-state";
        var versionOne = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(
            stateName,
            applicationSchemaVersion: 1);
        var versionTwo = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(
            stateName,
            applicationSchemaVersion: 2);
        var versionOneIndex = IndexMetadataProvider.GetSelectedIndex<VacancyState, string>(
            stateName,
            state => state.City,
            versionOne.Fingerprint);
        var versionTwoIndex = IndexMetadataProvider.GetSelectedIndex<VacancyState, string>(
            stateName,
            state => state.City,
            versionTwo.Fingerprint);
        var query = new PartitionQueryPlan { Operation = PartitionQueryOperation.All };
        var versionOneFacetFingerprint = FacetQueryFingerprint.Compute(
            stateName,
            query,
            versionOneIndex.Scope,
            versionOneIndex.Kind);
        var versionTwoFacetFingerprint = FacetQueryFingerprint.Compute(
            stateName,
            query,
            versionTwoIndex.Scope,
            versionTwoIndex.Kind);
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "facet-schema-binding",
            Enumerable.Range(1, 32).Select(static value => checked((byte)value)).ToArray());
        var codec = new ContinuationTokenCodec(
            providerName,
            SearchableStorageQueryConfiguration.Create(options));
        var layoutFingerprint = SHA256.HashData("stable layout"u8);
        var policy = new QueryExecutionPolicy(
            PageSize: 1,
            PartitionWorkBudget: 1_000,
            PartitionResponseItemLimit: 100,
            PartitionResponseByteLimit: 10_000,
            CoordinatorBufferedItemLimit: 100,
            CoordinatorBufferedByteLimit: 100_000,
            PageByteLimit: 50_000);
        var versionOneBinding = CreateBinding(
            providerName,
            versionOneFacetFingerprint,
            layoutFingerprint,
            policy);
        var versionTwoBinding = CreateBinding(
            providerName,
            versionTwoFacetFingerprint,
            layoutFingerprint,
            policy);

        versionOne.Fingerprint.Should().NotEqual(versionTwo.Fingerprint);
        IndexSchemaIdentity.IsBoundScope(
            versionOneIndex.Scope,
            versionOne.Fingerprint).Should().BeTrue();
        IndexSchemaIdentity.IsBoundScope(
            versionTwoIndex.Scope,
            versionTwo.Fingerprint).Should().BeTrue();
        versionOneFacetFingerprint.Should().NotEqual(versionTwoFacetFingerprint);
        var token = codec.Protect(ContinuationTokenPayload.CreateFacet(
            versionOneBinding,
            IndexValue.Create("Amsterdam")));

        Action resumeWithVersionTwo = () => _ = codec.Unprotect(token, versionTwoBinding);

        resumeWithVersionTwo.Should()
            .Throw<SearchableStorageInvalidContinuationTokenException>();
    }

    private static ContinuationTokenBinding CreateBinding(
        string providerName,
        byte[] queryFingerprint,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy)
    {
        return new ContinuationTokenBinding(
            providerName,
            PartitionQueryResponseFamily.DistinctFacetValuePage,
            queryFingerprint,
            QueryProtocol.FacetValueOrderingVersion,
            StorageLayout.IndexSchemaFormatVersion,
            routingEpoch: 7,
            layoutFingerprint,
            policy);
    }
}
