using AwesomeAssertions;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Persistence;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.Storage;
using Orleans.TestingHost;
using StackExchange.Redis;

namespace Orleans.SearchableStorage.Tests;

[Trait("Category", "BackendIntegration")]
[Trait("Backend", "PostgreSQL")]
public sealed class PostgreSqlSearchableStorageContractTests
    : FaultInjectingSearchableStorageContractTests<PostgreSqlStorageFixture>
{
    public PostgreSqlSearchableStorageContractTests(PostgreSqlStorageFixture fixture)
        : base(fixture)
    {
        fixture.EnsurePreconditionsMet();
    }

    [SkippableFact]
    public void OfficialAdoNetProviderIsThePhysicalBackend()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var physical = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
        var inner = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);
        var options = silo.ServiceProvider.GetRequiredService<IOptionsMonitor<AdoNetGrainStorageOptions>>()
            .Get(ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);

        physical.Should().BeOfType<WriteFaultInjectingGrainStorage>();
        inner.Should().BeOfType<AdoNetGrainStorage>();
        options.Invariant.Should().Be("Npgsql");
        options.GrainStorageSerializer.Should().BeOfType<JsonGrainStorageSerializer>();
    }

    [SkippableFact]
    public async Task CleanupRemovesOnlyTheOwnedPostgreSqlSchema()
    {
        var ownedSchema = $"oss_cleanup_{Guid.NewGuid():N}";
        var unrelatedSchema = $"oss_unrelated_{Guid.NewGuid():N}";
        var manager = Fixture.SchemaManager;

        try
        {
            await manager.CreateSchemaWithSentinelAsync(ownedSchema);
            await manager.CreateSchemaWithSentinelAsync(unrelatedSchema);
            (await manager.SentinelExistsAsync(ownedSchema)).Should().BeTrue();
            (await manager.SentinelExistsAsync(unrelatedSchema)).Should().BeTrue();

            await manager.DropSchemaAsync(ownedSchema);

            (await manager.SchemaExistsAsync(ownedSchema)).Should().BeFalse();
            (await manager.SchemaExistsAsync(unrelatedSchema)).Should().BeTrue();
            (await manager.SentinelExistsAsync(unrelatedSchema)).Should().BeTrue();
        }
        finally
        {
            await Task.WhenAll(
                manager.DropSchemaAsync(ownedSchema),
                manager.DropSchemaAsync(unrelatedSchema));
        }
    }
}

[Trait("Category", "BackendIntegration")]
[Trait("Backend", "Redis")]
public sealed class RedisSearchableStorageContractTests
    : FaultInjectingSearchableStorageContractTests<RedisStorageFixture>
{
    public RedisSearchableStorageContractTests(RedisStorageFixture fixture)
        : base(fixture)
    {
        fixture.EnsurePreconditionsMet();
    }

    [SkippableFact]
    public void OfficialRedisProviderIsThePhysicalBackend()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var physical = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
        var inner = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);
        var options = silo.ServiceProvider.GetRequiredService<IOptionsMonitor<RedisStorageOptions>>()
            .Get(ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);

        physical.Should().BeOfType<WriteFaultInjectingGrainStorage>();
        inner.Should().BeOfType<RedisGrainStorage>();
        options.ConfigurationOptions.Should().NotBeNull();
        options.GrainStorageSerializer.Should().BeOfType<JsonGrainStorageSerializer>();
    }

    [SkippableFact]
    public async Task CleanupRemovesOnlyTheOwnedRedisStateKeys()
    {
        var cleanupServiceId = $"{Fixture.ServiceId}-cleanup-{Guid.NewGuid():N}";
        var unrelatedServiceId = $"oss-unrelated-{Guid.NewGuid():N}";
        RedisKey ownedKey = $"{cleanupServiceId}/state/sentinel";
        RedisKey unrelatedKey = $"{unrelatedServiceId}/state/sentinel";
        var configuration = ConfigurationOptions.Parse(Fixture.ConnectionString);
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
        var database = multiplexer.GetDatabase();

        try
        {
            await database.StringSetAsync(ownedKey, "owned");
            await database.StringSetAsync(unrelatedKey, "unrelated");

            await Fixture.StateKeyManager.DeleteStateKeysAsync(cleanupServiceId);

            (await database.KeyExistsAsync(ownedKey)).Should().BeFalse();
            (await database.KeyExistsAsync(unrelatedKey)).Should().BeTrue();
        }
        finally
        {
            await database.KeyDeleteAsync([ownedKey, unrelatedKey]);
        }
    }
}

[Trait("Category", "BackendIntegration")]
[Trait("Backend", "AzureBlob")]
public sealed class AzureBlobSearchableStorageContractTests
    : FaultInjectingSearchableStorageContractTests<AzureBlobStorageFixture>
{
    public AzureBlobSearchableStorageContractTests(AzureBlobStorageFixture fixture)
        : base(fixture)
    {
        fixture.EnsurePreconditionsMet();
    }

    [SkippableFact]
    public void OfficialAzureBlobProviderIsThePhysicalBackend()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var physical = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
        var inner = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);
        var options = silo.ServiceProvider.GetRequiredService<IOptionsMonitor<AzureBlobStorageOptions>>()
            .Get(ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);

        physical.Should().BeOfType<WriteFaultInjectingGrainStorage>();
        inner.Should().BeOfType<AzureBlobGrainStorage>();
        options.ContainerName.Should().Be(Fixture.ContainerName);
        options.GrainStorageSerializer.Should().BeOfType<JsonGrainStorageSerializer>();
    }

    [SkippableFact]
    public async Task CleanupRemovesOnlyTheOwnedAzureBlobContainer()
    {
        var ownedContainerName = $"oss-cleanup-{Guid.NewGuid():N}";
        var unrelatedContainerName = $"oss-unrelated-{Guid.NewGuid():N}";
        var serviceClient = new BlobServiceClient(Fixture.ConnectionString);
        var ownedContainer = serviceClient.GetBlobContainerClient(ownedContainerName);
        var unrelatedContainer = serviceClient.GetBlobContainerClient(unrelatedContainerName);

        try
        {
            await ownedContainer.CreateIfNotExistsAsync();
            await unrelatedContainer.CreateIfNotExistsAsync();
            await ownedContainer.GetBlobClient("sentinel").UploadAsync(BinaryData.FromString("owned"));
            await unrelatedContainer.GetBlobClient("sentinel").UploadAsync(BinaryData.FromString("unrelated"));

            await Fixture.ContainerManager.DeleteContainerAsync(ownedContainerName);

            (await ownedContainer.ExistsAsync()).Value.Should().BeFalse();
            (await unrelatedContainer.ExistsAsync()).Value.Should().BeTrue();
            (await unrelatedContainer.GetBlobClient("sentinel").ExistsAsync()).Value.Should().BeTrue();
        }
        finally
        {
            await Task.WhenAll(
                ownedContainer.DeleteIfExistsAsync(),
                unrelatedContainer.DeleteIfExistsAsync());
        }
    }
}
