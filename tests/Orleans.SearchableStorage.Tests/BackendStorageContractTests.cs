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
    : JournaledPersistenceContractTests<PostgreSqlStorageFixture>
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
    : JournaledPersistenceContractTests<RedisStorageFixture>
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
        var probes = Enumerable.Range(0, 2)
            .Select(_ => new RedisCleanupProbe(
                $"cleanup-probe-{Guid.NewGuid():N}",
                GrainId.Create("redis-cleanup-probe", Guid.NewGuid().ToString("N")),
                new GrainState<List<string>> { State = ["provider-created"] }))
            .ToArray();
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName);
        var configuration = ConfigurationOptions.Parse(Fixture.ConnectionString);
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
        var database = multiplexer.GetDatabase();
        var keysBeforeWrite = GetRedisKeys(multiplexer, $"{Fixture.ServiceId}/*");
        var cleanupKeys = new List<RedisKey>();
        var writtenProbes = new List<RedisCleanupProbe>();

        try
        {
            foreach (var probe in probes)
            {
                await storage.WriteStateAsync(probe.StateName, probe.GrainId, probe.State);
                writtenProbes.Add(probe);
            }

            var providerKeys = GetRedisKeys(multiplexer, $"{Fixture.ServiceId}/*")
                .Except(keysBeforeWrite, StringComparer.Ordinal)
                .ToArray();
            providerKeys.Should().HaveCount(2);
            providerKeys.Should().OnlyContain(key => key.StartsWith($"{Fixture.ServiceId}/state/"));
            var ownedKeys = new List<RedisKey>();
            var unrelatedKeys = new List<RedisKey>();
            foreach (var providerKey in providerKeys)
            {
                var providerKeySuffix = providerKey[Fixture.ServiceId.Length..];
                RedisKey ownedKey = $"{cleanupServiceId}{providerKeySuffix}";
                RedisKey unrelatedKey = $"{unrelatedServiceId}{providerKeySuffix}";
                ownedKeys.Add(ownedKey);
                unrelatedKeys.Add(unrelatedKey);
                cleanupKeys.Add(ownedKey);
                cleanupKeys.Add(unrelatedKey);
                await database.StringSetAsync(ownedKey, "owned");
                await database.StringSetAsync(unrelatedKey, "unrelated");
            }

            await Fixture.StateKeyManager.DeleteStateKeysAsync(cleanupServiceId);

            foreach (var ownedKey in ownedKeys)
            {
                (await database.KeyExistsAsync(ownedKey)).Should().BeFalse();
            }

            foreach (var unrelatedKey in unrelatedKeys)
            {
                (await database.KeyExistsAsync(unrelatedKey)).Should().BeTrue();
            }
        }
        finally
        {
            var cleanupTasks = writtenProbes
                .Select(probe => storage.ClearStateAsync(probe.StateName, probe.GrainId, probe.State))
                .Concat(cleanupKeys.Select(key => database.KeyDeleteAsync(key)))
                .ToArray();

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

    private sealed record RedisCleanupProbe(
        string StateName,
        GrainId GrainId,
        GrainState<List<string>> State);
}

public sealed class RedisStateKeyManagerTests
{
    [Fact]
    public async Task DeleteKeysIndividuallyDeduplicatesKeysBeforeDispatch()
    {
        RedisKey first = "state:{first}:1";
        RedisKey second = "state:{second}:2";
        var deletedKeys = new List<string>();

        await RedisStateKeyManager.DeleteKeysIndividuallyAsync(
            [first, second, first],
            key =>
            {
                deletedKeys.Add(key.ToString());
                return Task.FromResult(true);
            });

        deletedKeys.Should().BeEquivalentTo([first.ToString(), second.ToString()]);
        deletedKeys.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task DeleteKeysIndividuallyWaitsForEachBoundedBatch()
    {
        var keys = Enumerable.Range(0, RedisStateKeyManager.DeleteBatchSize + 1)
            .Select(index => (RedisKey)$"state:{{slot-{index}}}:{index}")
            .ToArray();
        var firstBatchRelease = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var startedKeys = new List<RedisKey>();

        var deleteTask = RedisStateKeyManager.DeleteKeysIndividuallyAsync(
            keys,
            key =>
            {
                startedKeys.Add(key);
                return firstBatchRelease.Task;
            });

        startedKeys.Should().HaveCount(RedisStateKeyManager.DeleteBatchSize);
        startedKeys.Should().NotContain(keys[^1]);

        firstBatchRelease.SetResult(true);
        await deleteTask;

        startedKeys.Should().HaveCount(keys.Length);
        startedKeys.Should().OnlyHaveUniqueItems();
    }
}

[Trait("Category", "BackendIntegration")]
[Trait("Backend", "AzureBlob")]
public sealed class AzureBlobSearchableStorageContractTests
    : JournaledPersistenceContractTests<AzureBlobStorageFixture>
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
