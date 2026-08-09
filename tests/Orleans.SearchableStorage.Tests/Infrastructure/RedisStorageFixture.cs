using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Persistence;
using Orleans.Serialization;
using Orleans.TestingHost;
using StackExchange.Redis;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

public sealed class RedisStorageFixture : ExternalStorageFixture<RedisSiloConfigurator>
{
    private string? _connectionString;

    public RedisStorageFixture()
        : base("redis")
    {
    }

    protected override Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
    {
        _connectionString = BackendTestEnvironment.GetConnectionString(
            BackendTestEnvironment.RedisConnectionStringVariable,
            BackendTestEnvironment.DefaultRedisConnectionString);
        IReadOnlyDictionary<string, string?> settings = new Dictionary<string, string?>
        {
            [RedisSiloConfigurator.ConnectionStringKey] = _connectionString,
        };
        return Task.FromResult(settings);
    }

    protected override async Task CleanupBackendAsync()
    {
        if (_connectionString is null)
        {
            return;
        }

        var configuration = ConfigurationOptions.Parse(_connectionString);
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
        var database = multiplexer.GetDatabase();
        foreach (var endpoint in multiplexer.GetEndPoints())
        {
            var keys = multiplexer.GetServer(endpoint)
                .Keys(pattern: $"{ServiceId}/state/*")
                .ToArray();
            if (keys.Length > 0)
            {
                await database.KeyDeleteAsync(keys);
            }
        }
    }
}

public sealed class RedisSiloConfigurator : IHostConfigurator
{
    public const string ConnectionStringKey = "BackendTests:Redis:ConnectionString";

    public void Configure(IHostBuilder hostBuilder)
    {
        hostBuilder.UseOrleans((context, siloBuilder) =>
        {
            var connectionString = ExternalStorageSiloConfiguration.GetRequiredSetting(
                context.Configuration,
                ConnectionStringKey);
            siloBuilder.AddRedisGrainStorage(
                ExternalStorageSiloConfiguration.InnerPhysicalStorageProviderName,
                (OptionsBuilder<RedisStorageOptions> optionsBuilder) =>
                    optionsBuilder.Configure<OrleansJsonSerializer>((options, serializer) =>
                    {
                        options.ConfigurationOptions = ConfigurationOptions.Parse(connectionString);
                        options.DeleteStateOnClear = true;
                        ExternalStorageSiloConfiguration.UseJsonSerializer(options, serializer);
                    }));
            ExternalStorageSiloConfiguration.AddSearchableStorage(siloBuilder);
        });
    }
}
