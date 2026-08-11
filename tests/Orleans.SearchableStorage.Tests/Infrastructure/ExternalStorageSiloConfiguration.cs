using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

internal static class ExternalStorageSiloConfiguration
{
    public const string InnerPhysicalStorageProviderName = "Orleans.SearchableStorage.Tests.ExternalPhysical";

    public static void AddSearchableStorage(ISiloBuilder siloBuilder)
    {
        // Keep one failure-injection boundary around every official provider so all backends run
        // the same pre-commit and lost-acknowledgement contract scenarios.
        siloBuilder.Services.AddKeyedSingleton<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName,
            (services, _) => new WriteFaultInjectingGrainStorage(
                services.GetRequiredKeyedService<IGrainStorage>(InnerPhysicalStorageProviderName),
                services.GetRequiredService<IGrainFactory>()));
        siloBuilder.AddSearchableGrainStorage(
            TestGrains.VacancyGrain.StorageProviderName,
            options => options.PartitionCount = BackendStorageTestConstants.PartitionCount);
        siloBuilder.AddSearchableGrainStorage(
            StorageIndexSchemaTestConstants.BackendContractProviderName,
            options => options.PartitionCount = 1);
        siloBuilder.AddSearchableStorageState<TestGrains.VacancyState>(
            StorageIndexSchemaTestConstants.BackendContractProviderName,
            StorageIndexSchemaTestConstants.BackendContractStateName);
    }

    public static void UseJsonSerializer<TOptions>(
        TOptions options,
        OrleansJsonSerializer serializer)
        where TOptions : IStorageProviderSerializerOptions
    {
        options.GrainStorageSerializer = new JsonGrainStorageSerializer(serializer);
    }

    public static string GetRequiredSetting(IConfiguration configuration, string key)
    {
        return configuration[key]
            ?? throw new InvalidOperationException($"Backend test setting '{key}' is missing.");
    }
}

public sealed class ExternalStorageClientConfigurator : IClientBuilderConfigurator
{
    public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
    {
        clientBuilder.Configure<ClientMessagingOptions>(options => options.LocalAddress = IPAddress.Loopback);
    }
}
