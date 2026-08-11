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
    public const string ManagedSchemaProviderName = "ManagedSchemaSearchable";
    public const string PagedManagedSchemaProviderName = "PagedManagedSchemaSearchable";
    public const string ManagedSchemaStateName = "managed-vacancy";
    public const string PagedManagedSchemaStateName = "managed-vacancy-paged";
    public const string FreshSchemaProviderName = "FreshSchemaSearchable";
    public const string FreshSchemaStateName = "fresh-schema-vacancy";
    public const string LegacyLayoutSchemaProviderName = "LegacyLayoutSchemaSearchable";
    public const string LegacyLayoutSchemaStateName = "legacy-layout-schema-vacancy";
    public const string VersionedSchemaProviderName = "VersionedSchemaSearchable";
    public const string VersionedSchemaStateName = "versioned-schema-vacancy";
    public const string DirectClientSchemaV1ProviderName = "DirectClientSchemaV1Searchable";
    public const string DirectClientSchemaV1StateName = "direct-client-schema-v1-vacancy";
    public const string DirectClientSchemaV2ProviderName = "DirectClientSchemaV2Searchable";
    public const string DirectClientSchemaV2StateName = "direct-client-schema-v2-vacancy";
    public const string ContinuationSchemaProviderName = "ContinuationSchemaSearchable";
    public const string ContinuationSchemaStateName = "continuation-schema-vacancy";
    public const string SchemaMaterializationFailureProviderName =
        "SchemaMaterializationFailureSearchable";
    public const string SchemaMaterializationFailureStateName =
        "schema-materialization-failure";
    public const string MultiStateSchemaProviderName = "MultiStateSchemaSearchable";
    public const string NoIndexSchemaStateName = "no-index-schema-state";
    public const string NullableSchemaStateName = "nullable-schema-state";
    public const string CorruptPayloadSchemaProviderName = "CorruptPayloadSchemaSearchable";
    public const string CorruptPayloadSchemaStateName = "corrupt-payload-schema-state";
    public const string CancelableSchemaProviderName = "CancelableSchemaSearchable";
    public const string CancelableSchemaStateName = "cancelable-schema-state";
    public const string FacetGenerationSchemaProviderName = "FacetGenerationSchemaSearchable";
    public const string FacetGenerationSchemaStateName = "facet-generation-schema-state";
    public const string SchemaBeginBeforeProviderName = "SchemaBeginBeforeSearchable";
    public const string SchemaBeginAfterProviderName = "SchemaBeginAfterSearchable";
    public const string SchemaProgressBeforeProviderName = "SchemaProgressBeforeSearchable";
    public const string SchemaProgressAfterProviderName = "SchemaProgressAfterSearchable";
    public const string SchemaPublicationBeforeProviderName = "SchemaPublicationBeforeSearchable";
    public const string SchemaPublicationAfterProviderName = "SchemaPublicationAfterSearchable";
    public const string SchemaFinalBeforeProviderName = "SchemaFinalBeforeSearchable";
    public const string SchemaFinalAfterProviderName = "SchemaFinalAfterSearchable";
    public const string SchemaFaultStateName = "schema-fault-vacancy";
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

    public async Task DisposeAsync()
    {
        try
        {
            await Cluster.StopAllSilosAsync();
        }
        finally
        {
            // Full disposal also releases the port allocator and handles left by a partial stop.
            await Cluster.DisposeAsync();
        }
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
            siloBuilder.AddSearchableGrainStorage(
                ManagedSchemaProviderName,
                options => options.PartitionCount = StoragePartitionCount);
            siloBuilder.AddSearchableGrainStorage(
                PagedManagedSchemaProviderName,
                options => options.PartitionCount = 1);
            siloBuilder.AddSearchableStorageState<TestGrains.VacancyState>(
                ManagedSchemaProviderName,
                ManagedSchemaStateName);
            siloBuilder.AddSearchableStorageState<TestGrains.VacancyState>(
                PagedManagedSchemaProviderName,
                PagedManagedSchemaStateName);

            AddManagedSchemaProvider(
                siloBuilder,
                FreshSchemaProviderName,
                FreshSchemaStateName,
                configurePaging: true);
            AddManagedSchemaProvider(
                siloBuilder,
                LegacyLayoutSchemaProviderName,
                LegacyLayoutSchemaStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                VersionedSchemaProviderName,
                VersionedSchemaStateName,
                applicationSchemaVersion: 2);
            AddManagedSchemaProvider(
                siloBuilder,
                DirectClientSchemaV1ProviderName,
                DirectClientSchemaV1StateName);
            AddManagedSchemaProvider(
                siloBuilder,
                DirectClientSchemaV2ProviderName,
                DirectClientSchemaV2StateName,
                applicationSchemaVersion: 2);
            AddManagedSchemaProvider(
                siloBuilder,
                ContinuationSchemaProviderName,
                ContinuationSchemaStateName,
                configurePaging: true);
            siloBuilder.AddSearchableGrainStorage(
                SchemaMaterializationFailureProviderName,
                options => options.PartitionCount = 1);
            siloBuilder.AddSearchableStorageState<TestGrains.SchemaMaterializationFailureState>(
                SchemaMaterializationFailureProviderName,
                SchemaMaterializationFailureStateName);
            siloBuilder.AddSearchableGrainStorage(
                MultiStateSchemaProviderName,
                options => options.PartitionCount = 1);
            siloBuilder.AddSearchableStorageState<TestGrains.NoIndexSchemaState>(
                MultiStateSchemaProviderName,
                NoIndexSchemaStateName);
            siloBuilder.AddSearchableStorageState<TestGrains.NullableQueryState>(
                MultiStateSchemaProviderName,
                NullableSchemaStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                CorruptPayloadSchemaProviderName,
                CorruptPayloadSchemaStateName);
            siloBuilder.AddSearchableGrainStorage(
                CancelableSchemaProviderName,
                options => options.PartitionCount = 1);
            siloBuilder.AddSearchableStorageState<TestGrains.BlockingSchemaState>(
                CancelableSchemaProviderName,
                CancelableSchemaStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                FacetGenerationSchemaProviderName,
                FacetGenerationSchemaStateName,
                applicationSchemaVersion: 2,
                configurePaging: true);
            AddManagedSchemaProvider(
                siloBuilder,
                StorageIndexSchemaTestConstants.BackendContractProviderName,
                StorageIndexSchemaTestConstants.BackendContractStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaBeginBeforeProviderName,
                SchemaFaultStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaBeginAfterProviderName,
                SchemaFaultStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaProgressBeforeProviderName,
                SchemaFaultStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaProgressAfterProviderName,
                SchemaFaultStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaPublicationBeforeProviderName,
                SchemaFaultStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaPublicationAfterProviderName,
                SchemaFaultStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaFinalBeforeProviderName,
                SchemaFaultStateName);
            AddManagedSchemaProvider(
                siloBuilder,
                SchemaFinalAfterProviderName,
                SchemaFaultStateName);
        }

        private static void AddManagedSchemaProvider(
            ISiloBuilder siloBuilder,
            string providerName,
            string stateName,
            int applicationSchemaVersion = 1,
            bool configurePaging = false)
        {
            siloBuilder.AddSearchableGrainStorage(
                providerName,
                options =>
                {
                    options.PartitionCount = 1;
                    if (configurePaging)
                    {
                        options.Query.ContinuationProtection.CurrentKey =
                            CreateSchemaTestContinuationKey();
                    }
                });
            siloBuilder.AddSearchableStorageState<TestGrains.VacancyState>(
                providerName,
                stateName,
                applicationSchemaVersion);
        }

        private static SearchableStorageContinuationKey CreateSchemaTestContinuationKey()
        {
            return new SearchableStorageContinuationKey(
                "schema-lifecycle-tests",
                Enumerable.Range(1, 32).Select(static value => checked((byte)value)).ToArray());
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
