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
    private AzureBlobContainerManager? _containerManager;
    private string? _connectionString;

    public AzureBlobStorageFixture()
        : base("azure-blob")
    {
        ContainerName = $"oss-{Guid.NewGuid():N}";
    }

    public string ContainerName { get; }

    internal AzureBlobContainerManager ContainerManager => _containerManager
        ?? throw new InvalidOperationException("The Azure Blob test resource has not been prepared.");

    internal string ConnectionString => _connectionString
        ?? throw new InvalidOperationException("The Azure Blob test resource has not been prepared.");

    protected override Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
    {
        _connectionString = BackendTestEnvironment.GetConnectionString(
            BackendTestEnvironment.AzureBlobConnectionStringVariable,
            BackendTestEnvironment.DefaultAzureBlobConnectionString);
        _containerManager = new AzureBlobContainerManager(_connectionString);
        IReadOnlyDictionary<string, string?> settings = new Dictionary<string, string?>
        {
            [AzureBlobSiloConfigurator.ConnectionStringKey] = _connectionString,
            [AzureBlobSiloConfigurator.ContainerNameKey] = ContainerName,
        };
        return Task.FromResult(settings);
    }

    protected override async Task CleanupBackendAsync()
    {
        if (_containerManager is null)
        {
            return;
        }

        await _containerManager.DeleteContainerAsync(ContainerName);
    }
}

internal sealed class AzureBlobContainerManager(string connectionString)
{
    public async Task DeleteContainerAsync(string containerName)
    {
        var client = new BlobServiceClient(connectionString);
        await client.GetBlobContainerClient(containerName).DeleteIfExistsAsync();
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
