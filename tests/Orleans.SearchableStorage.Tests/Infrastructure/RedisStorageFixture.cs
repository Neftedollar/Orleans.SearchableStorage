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
    private RedisStateKeyManager? _stateKeyManager;

    public RedisStorageFixture()
        : base("redis")
    {
    }

    internal string ConnectionString => _connectionString
        ?? throw new InvalidOperationException("The Redis test resource has not been prepared.");

    internal RedisStateKeyManager StateKeyManager => _stateKeyManager
        ?? throw new InvalidOperationException("The Redis test resource has not been prepared.");

    protected override Task<IReadOnlyDictionary<string, string?>> PrepareBackendAsync()
    {
        _connectionString = BackendTestEnvironment.GetConnectionString(
            BackendTestEnvironment.RedisConnectionStringVariable,
            BackendTestEnvironment.DefaultRedisConnectionString);
        _stateKeyManager = new RedisStateKeyManager(_connectionString);
        IReadOnlyDictionary<string, string?> settings = new Dictionary<string, string?>
        {
            [RedisSiloConfigurator.ConnectionStringKey] = _connectionString,
        };
        return Task.FromResult(settings);
    }

    protected override async Task CleanupBackendAsync()
    {
        if (_stateKeyManager is null)
        {
            return;
        }

        await _stateKeyManager.DeleteStateKeysAsync(ServiceId);
    }
}

internal sealed class RedisStateKeyManager(string connectionString)
{
    internal const int DeleteBatchSize = 128;

    public async Task DeleteStateKeysAsync(string serviceId)
    {
        var configuration = ConfigurationOptions.Parse(connectionString);
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
        var database = multiplexer.GetDatabase();
        var keys = multiplexer.GetEndPoints()
            .SelectMany(endpoint => multiplexer.GetServer(endpoint)
                .Keys(pattern: $"{serviceId}/state/*")
                .ToArray())
            .Distinct()
            .ToArray();

        await DeleteKeysIndividuallyAsync(keys, key => database.KeyDeleteAsync(key));
    }

    internal static async Task DeleteKeysIndividuallyAsync(
        IEnumerable<RedisKey> keys,
        Func<RedisKey, Task<bool>> deleteAsync)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(deleteAsync);

        // A Redis Cluster accepts a multi-key DEL only when every key maps to the same hash slot.
        foreach (var batch in keys.Distinct().Chunk(DeleteBatchSize))
        {
            await Task.WhenAll(batch.Select(deleteAsync));
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
