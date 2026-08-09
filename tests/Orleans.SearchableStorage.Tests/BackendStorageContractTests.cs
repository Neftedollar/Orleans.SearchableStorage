using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Persistence;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

[Trait("Category", "BackendIntegration")]
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
}

[Trait("Category", "BackendIntegration")]
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
}

[Trait("Category", "BackendIntegration")]
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
}
