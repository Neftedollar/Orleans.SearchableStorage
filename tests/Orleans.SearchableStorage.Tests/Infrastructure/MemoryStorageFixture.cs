using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

public sealed class MemoryStorageFixture : ISearchableStorageFixture, IAsyncLifetime
{
    public const int StoragePartitionCount = 8;
    internal const string InnerPhysicalStorageProviderName = "Orleans.SearchableStorage.Tests.InnerPhysical";

    public MemoryStorageFixture()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        Cluster = builder.Build();
    }

    public TestCluster Cluster { get; }

    public int PartitionCount => StoragePartitionCount;

    public Task InitializeAsync()
    {
        return Cluster.DeployAsync();
    }

    public Task DisposeAsync()
    {
        return Cluster.StopAllSilosAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddMemoryGrainStorage(
                InnerPhysicalStorageProviderName,
                options => options.Configure<OrleansJsonSerializer>(
                    (storageOptions, serializer) =>
                    {
                        storageOptions.NumStorageGrains = 4;
                        storageOptions.GrainStorageSerializer = new JsonGrainStorageSerializer(serializer);
                    }));
            siloBuilder.Services.AddKeyedSingleton<IGrainStorage>(
                SearchableStorageConstants.PhysicalStorageProviderName,
                (services, _) => new WriteFaultInjectingGrainStorage(
                    services.GetRequiredKeyedService<IGrainStorage>(InnerPhysicalStorageProviderName),
                    services.GetRequiredService<IGrainFactory>()));
            siloBuilder.AddSearchableGrainStorage(
                TestGrains.VacancyGrain.StorageProviderName,
                options => options.PartitionCount = StoragePartitionCount);
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.Configure<ClientMessagingOptions>(options => options.LocalAddress = IPAddress.Loopback);
        }
    }
}
