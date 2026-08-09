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
        var stateName = $"cleanup-probe-{Guid.NewGuid():N}";
        var grainId = GrainId.Create("redis-cleanup-probe", Guid.NewGuid().ToString("N"));
        var state = new GrainState<List<string>> { State = ["provider-created"] };
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);
        var configuration = ConfigurationOptions.Parse(Fixture.ConnectionString);
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
        var database = multiplexer.GetDatabase();
        var keysBeforeWrite = GetRedisKeys(multiplexer, $"{Fixture.ServiceId}/*");
        var cleanupKeys = new List<RedisKey>();
        var providerStateWritten = false;

        try
        {
            await storage.WriteStateAsync(stateName, grainId, state);
            providerStateWritten = true;

            var providerKey = GetRedisKeys(multiplexer, $"{Fixture.ServiceId}/*")
                .Except(keysBeforeWrite, StringComparer.Ordinal)
                .Should()
                .ContainSingle()
                .Which;
            providerKey.Should().StartWith($"{Fixture.ServiceId}/state/");
            var providerKeySuffix = providerKey[Fixture.ServiceId.Length..];
            RedisKey ownedKey = $"{cleanupServiceId}{providerKeySuffix}";
            RedisKey unrelatedKey = $"{unrelatedServiceId}{providerKeySuffix}";

            await database.StringSetAsync(ownedKey, "owned");
            cleanupKeys.Add(ownedKey);
            await database.StringSetAsync(unrelatedKey, "unrelated");
            cleanupKeys.Add(unrelatedKey);

            await Fixture.StateKeyManager.DeleteStateKeysAsync(cleanupServiceId);

            (await database.KeyExistsAsync(ownedKey)).Should().BeFalse();
            (await database.KeyExistsAsync(unrelatedKey)).Should().BeTrue();
        }
        finally
        {
            var cleanupTasks = new List<Task>();
            if (providerStateWritten)
            {
                cleanupTasks.Add(storage.ClearStateAsync(stateName, grainId, state));
            }

            if (cleanupKeys.Count > 0)
            {
                cleanupTasks.Add(database.KeyDeleteAsync([.. cleanupKeys]));
            }

            await Task.WhenAll(cleanupTasks);
        }
    }

    private static HashSet<string> GetRedisKeys(ConnectionMultiplexer multiplexer, string pattern)
    {
        return multiplexer.GetEndPoints()
            .SelectMany(endpoint => multiplexer.GetServer(endpoint).Keys(pattern: pattern))
            .Select(static key => key.ToString())
            .ToHashSet(StringComparer.Ordinal);
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
