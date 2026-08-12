using AwesomeAssertions;
using System.Globalization;
using System.Reflection;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Xunit;

namespace Orleans.SearchableStorage.Tests;

public sealed class CompatibilityManifestTests
{
    [Fact]
    public void ReviewedProtocolManifestMatchesProductionConstants()
    {
        CompatibilityManifest.GetInt("manifestVersion").Should().Be(1);

        CompatibilityManifest.GetInt("protocols", "layout", "legacyFormat")
            .Should().Be(StorageLayout.LegacyFormatVersion);
        CompatibilityManifest.GetInt("protocols", "layout", "movementFormat")
            .Should().Be(StorageLayout.MovementFormatVersion);
        CompatibilityManifest.GetInt("protocols", "layout", "managedSchemaFormat")
            .Should().Be(StorageLayout.IndexSchemaFormatVersion);
        CompatibilityManifest.GetInt("protocols", "layout", "movementProtocol")
            .Should().Be(StorageLayout.CurrentMovementProtocolVersion);
        CompatibilityManifest.GetInt("protocols", "layout", "managedSchemaProtocol")
            .Should().Be(StorageLayout.CurrentIndexSchemaProtocolVersion);

        CompatibilityManifest.GetInt("protocols", "partitionPersistence", "legacyFormat")
            .Should().Be(StoragePersistence.LegacyPersistenceFormatVersion);
        CompatibilityManifest.GetInt("protocols", "partitionPersistence", "movementFormat")
            .Should().Be(StoragePersistence.MovementPersistenceFormatVersion);
        CompatibilityManifest.GetInt("protocols", "partitionPersistence", "currentFormat")
            .Should().Be(StoragePersistence.CurrentPersistenceFormatVersion);

        CompatibilityManifest.GetInt("protocols", "query", "paging")
            .Should().Be(QueryProtocol.PagingVersion);
        CompatibilityManifest.GetInt("protocols", "query", "ordering")
            .Should().Be(QueryProtocol.OrderingVersion);
        CompatibilityManifest.GetInt("protocols", "query", "workPolicy")
            .Should().Be(QueryProtocol.WorkPolicyVersion);
        CompatibilityManifest.GetInt("protocols", "query", "continuationPayload")
            .Should().Be(QueryProtocol.ContinuationPayloadVersion);
        CompatibilityManifest.GetInt("protocols", "query", "facetValueOrdering")
            .Should().Be(QueryProtocol.FacetValueOrderingVersion);
        CompatibilityManifest.GetInt("protocols", "query", "facetWorkPolicy")
            .Should().Be(QueryProtocol.FacetWorkPolicyVersion);
        CompatibilityManifest.GetInt("protocols", "continuationToken", "envelope")
            .Should().Be(ContinuationTokenCodec.EnvelopeVersion);
        CompatibilityManifest.GetInt("protocols", "continuationToken", "aes256GcmAlgorithm")
            .Should().Be(ContinuationTokenCodec.Aes256GcmAlgorithm);
        CompatibilityManifest.GetInt("protocols", "continuationToken", "nonceBytes")
            .Should().Be(ContinuationTokenCodec.NonceBytes);
        CompatibilityManifest.GetInt("protocols", "continuationToken", "authenticationTagBytes")
            .Should().Be(ContinuationTokenCodec.AuthenticationTagBytes);
        CompatibilityManifest.GetInt("protocols", "continuationToken", "fingerprintBytes")
            .Should().Be(ContinuationTokenCodec.FingerprintBytes);

        CompatibilityManifest.GetInt("protocols", "managedIndexSchema", "protocol")
            .Should().Be(StorageIndexSchema.ProtocolVersion);
        CompatibilityManifest.GetInt("protocols", "managedIndexSchema", "definition")
            .Should().Be(IndexSchemaDefinition.DefinitionVersion);
        CompatibilityManifest.GetInt(
                "protocols",
                "managedIndexSchema",
                "membershipFingerprintFormat")
            .Should().Be(IndexSchemaDefinition.MembershipFingerprintFormatVersion);
        CompatibilityManifest.GetInt("protocols", "managedIndexSchema", "membershipExtractor")
            .Should().Be(IndexSchemaDefinition.MembershipExtractorVersion);
        CompatibilityManifest.GetInt("protocols", "managedIndexSchema", "rebuildPageSize")
            .Should().Be(StorageIndexSchema.RebuildPageSize);

        CompatibilityManifest.GetInt("protocols", "movement", "protocol")
            .Should().Be(StorageMoveProtocol.Version);
        CompatibilityManifest.GetInt("protocols", "snapshotRecordEncoding", "legacy")
            .Should().Be(StorageSnapshotFactory.LegacyRecordEncodingVersion);
        CompatibilityManifest.GetInt("protocols", "snapshotRecordEncoding", "lossless")
            .Should().Be(StorageSnapshotFactory.LosslessRecordEncodingVersion);
    }

    [Fact]
    public void ReviewedWireEnumMapsMatchProductionValues()
    {
        AssertEnumMap<IndexKeyCodecId>("indexKeyCodec", "ids");
        AssertEnumMap<PartitionQueryOperation>("partitionQueryOperation");
        AssertEnumMap<PartitionQueryAccessPath>("partitionQueryAccessPath");
        AssertEnumMap<PartitionQueryResponseFamily>("partitionQueryResponseFamily");
        AssertEnumMap<PartitionQueryPageStopReason>("partitionQueryPageStopReason");
        AssertEnumMap<SearchableIndexKind>("searchableIndexKind");
        AssertEnumMap<IndexValueKind>("indexValueKind");
        AssertEnumMap<StorageJournalOperation>("storageJournalOperation");
        AssertEnumMap<StoragePartitionMoveRole>("storagePartitionMoveRole");
        AssertEnumMap<StoragePartitionMovePhase>("storagePartitionMovePhase");
        AssertEnumMap<StorageMoveDeleteMode>("storageMoveDeleteMode");
        AssertEnumMap<StorageMoveRetirementKind>("storageMoveRetirementKind");
        AssertEnumMap<SearchableStorageMovementState>("movementState");
        AssertEnumMap<SearchableStorageSlotMovePhase>("movementPhase");
    }

    [Fact]
    public void ReviewedWireContractsNameExecutableTests()
    {
        var assembly = typeof(CompatibilityManifestTests).Assembly;
        foreach (var testName in CompatibilityManifest.GetStrings("executableWireContractTests"))
        {
            var separator = testName.LastIndexOf('.');
            separator.Should().BeGreaterThan(0);
            var declaringType = assembly.GetType(testName[..separator]);
            declaringType.Should().NotBeNull($"{testName} must name a test type in this assembly");
            var method = declaringType!.GetMethod(
                testName[(separator + 1)..],
                BindingFlags.Public | BindingFlags.Instance);
            method.Should().NotBeNull($"{testName} must name a public instance test method");
            method!.GetParameters().Should().BeEmpty();
            method!.GetCustomAttributes(inherit: false)
                .Should().Contain(attribute => attribute is FactAttribute);
        }
    }

    private static void AssertEnumMap<TEnum>(params string[] path)
        where TEnum : struct, Enum
    {
        var expected = CompatibilityManifest.GetIntMap(["wireContracts", .. path]);
        var actual = Enum.GetValues<TEnum>().ToDictionary(
            static value => value.ToString(),
            static value => Convert.ToInt32(value, CultureInfo.InvariantCulture),
            StringComparer.Ordinal);
        actual.Should().Equal(expected);
    }
}
