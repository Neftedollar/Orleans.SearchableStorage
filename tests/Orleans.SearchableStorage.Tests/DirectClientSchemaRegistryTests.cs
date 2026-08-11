using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class DirectClientSchemaRegistryTests
{
    private readonly MemoryStorageFixture _fixture;

    public DirectClientSchemaRegistryTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData(
        MemoryStorageFixture.DirectClientSchemaV1ProviderName,
        MemoryStorageFixture.DirectClientSchemaV1StateName,
        1)]
    [InlineData(
        MemoryStorageFixture.DirectClientSchemaV2ProviderName,
        MemoryStorageFixture.DirectClientSchemaV2StateName,
        2)]
    public async Task DirectClientRequiresARegistryAndBindsItsDeclaredVersion(
        string providerName,
        string stateName,
        int applicationSchemaVersion)
    {
        var grainFactory = _fixture.Cluster.GrainFactory;
        var admin = new SearchableStorageAdminClient(
            grainFactory,
            providerName,
            partitionCount: 1);
        var status = applicationSchemaVersion == 1
            ? await admin.RebuildIndexSchemaAsync<VacancyState>(stateName)
            : await admin.RebuildIndexSchemaAsync<VacancyState>(
                stateName,
                applicationSchemaVersion,
                CancellationToken.None);
        var expected = IndexMetadataProvider.GetSchemaDefinition<VacancyState>(
            stateName,
            applicationSchemaVersion);
        status.Fingerprint.Should().Be(Convert.ToHexString(expected.Fingerprint));

        var undeclared = new SearchableStorageClient(
            grainFactory,
            providerName,
            partitionCount: 1);
        Func<Task> queryWithoutRegistry = async () => await undeclared.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            $"undeclared-{Guid.NewGuid():N}");

        await queryWithoutRegistry.Should().ThrowAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*managed index schemas enabled*");

        var registry = new SearchableStorageSchemaRegistry()
            .AddState<VacancyState>(stateName, applicationSchemaVersion);
        var declared = new SearchableStorageClient(
            grainFactory,
            providerName,
            partitionCount: 1,
            new SearchableStorageQueryOptions(),
            registry);

        var result = await declared.FindAsync<VacancyState, string>(
            stateName,
            state => state.City,
            $"declared-{Guid.NewGuid():N}");

        result.Should().BeEmpty();
    }
}
