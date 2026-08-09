using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.SearchableStorage;

/// <summary>
/// Registers searchable grain storage in an Orleans silo.
/// </summary>
public static class SearchableStorageSiloBuilderExtensions
{
    /// <summary>
    /// Adds a named searchable grain-storage provider.
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The name used by <see cref="PersistentStateAttribute"/>.</param>
    /// <param name="configure">An optional provider configuration delegate.</param>
    /// <returns>The supplied silo builder.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    public static ISiloBuilder AddSearchableGrainStorage(
        this ISiloBuilder builder,
        string providerName,
        Action<SearchableStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddSearchableGrainStorage(providerName, configure);
        return builder;
    }

    /// <summary>
    /// Adds a named searchable grain-storage provider to a service collection.
    /// </summary>
    /// <param name="services">The silo service collection.</param>
    /// <param name="providerName">The name used by <see cref="PersistentStateAttribute"/>.</param>
    /// <param name="configure">An optional provider configuration delegate.</param>
    /// <returns>The supplied service collection.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    public static IServiceCollection AddSearchableGrainStorage(
        this IServiceCollection services,
        string providerName,
        Action<SearchableStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        var options = services.AddOptions<SearchableStorageOptions>(providerName);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options
            .Validate(static value => value.PartitionCount > 0, "PartitionCount must be greater than zero.")
            .ValidateOnStart();

        services.AddTransient<
            IPostConfigureOptions<SearchableStorageOptions>,
            DefaultStorageProviderSerializerOptionsConfigurator<SearchableStorageOptions>>();

        services.AddKeyedSingleton<IGrainStorage>(
            providerName,
            (serviceProvider, _) => SearchableGrainStorageFactory.Create(serviceProvider, providerName));

        services.AddKeyedSingleton<ISearchableStorageClient>(
            providerName,
            (serviceProvider, _) =>
            {
                var configuredOptions = serviceProvider
                    .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
                    .Get(providerName);
                return new SearchableStorageClient(
                    serviceProvider.GetRequiredService<IGrainFactory>(),
                    providerName,
                    configuredOptions.PartitionCount);
            });

        return services;
    }
}

internal static class SearchableGrainStorageFactory
{
    public static IGrainStorage Create(IServiceProvider services, string name)
    {
        var options = services
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(name);

        return ActivatorUtilities.CreateInstance<SearchableGrainStorage>(services, name, options);
    }
}
