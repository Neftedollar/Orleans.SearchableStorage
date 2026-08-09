using Azure.Storage.Blobs;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

public sealed class AzureBlobStorageFixture : ExternalStorageFixture<AzureBlobSiloConfigurator>
{
    private string? _connectionString;

    public AzureBlobStorageFixture()
        : base("azure-blob")
    {
        ContainerName = $"oss-{Guid.NewGuid():N}";
    }

    public string ContainerName { get; }

    protected override Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
    {
        _connectionString = BackendTestEnvironment.GetConnectionString(
            BackendTestEnvironment.AzureBlobConnectionStringVariable,
            BackendTestEnvironment.DefaultAzureBlobConnectionString);
        IReadOnlyDictionary<string, string?> settings = new Dictionary<string, string?>
        {
            [AzureBlobSiloConfigurator.ConnectionStringKey] = _connectionString,
            [AzureBlobSiloConfigurator.ContainerNameKey] = ContainerName,
        };
        return Task.FromResult(settings);
    }

    protected override async Task CleanupBackendAsync()
    {
        if (_connectionString is null)
        {
            return;
        }

        var client = new BlobServiceClient(_connectionString);
        await client.GetBlobContainerClient(ContainerName).DeleteIfExistsAsync();
    }
}

public sealed class AzureBlobSiloConfigurator : IHostConfigurator
{
    public const string ConnectionStringKey = "BackendTests:AzureBlob:ConnectionString";
    public const string ContainerNameKey = "BackendTests:AzureBlob:ContainerName";

    public void Configure(IHostBuilder hostBuilder)
    {
        hostBuilder.UseOrleans((context, siloBuilder) =>
        {
            var connectionString = ExternalStorageSiloConfiguration.GetRequiredSetting(
                context.Configuration,
                ConnectionStringKey);
            var containerName = ExternalStorageSiloConfiguration.GetRequiredSetting(
                context.Configuration,
                ContainerNameKey);
            siloBuilder.AddAzureBlobGrainStorage(
                ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName,
                (OptionsBuilder<AzureBlobStorageOptions> optionsBuilder) =>
                    optionsBuilder.Configure<OrleansJsonSerializer>((options, serializer) =>
                    {
                        options.BlobServiceClient = new BlobServiceClient(connectionString);
                        options.ContainerName = containerName;
                        options.DeleteStateOnClear = true;
                        ExternalStorageSiloConfiguration.UseJsonSerializer(options, serializer);
                    }));
            ExternalStorageSiloConfiguration.AddSearchableStorage(siloBuilder);
        });
    }
}
