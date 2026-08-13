using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.Storage;

namespace Orleans.SearchableStorage;

/// <summary>
/// Registers searchable grain storage in an Orleans silo.
/// </summary>
public static class SearchableStorageSiloBuilderExtensions
{
    /// <summary>
    /// Registers the single CLR state type used by one searchable provider/state-name pair.
    /// Explicit registration lets the storage validate index-schema drift and rebuild derived
    /// indexes without guessing serialized payload types.
    /// </summary>
    /// <typeparam name="TState">The Orleans persistent-state type.</typeparam>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <returns>The supplied silo builder.</returns>
    public static ISiloBuilder AddSearchableStorageState<TState>(
        this ISiloBuilder builder,
        string providerName,
        string stateName)
    {
        return builder.AddSearchableStorageState<TState>(
            providerName,
            stateName,
            applicationSchemaVersion: 1);
    }

    /// <summary>
    /// Registers one CLR state type with an explicit application-controlled schema version.
    /// </summary>
    /// <typeparam name="TState">The Orleans persistent-state type.</typeparam>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="applicationSchemaVersion">
    /// A positive application-owned version included in the durable schema fingerprint.
    /// </param>
    /// <returns>The supplied silo builder.</returns>
    public static ISiloBuilder AddSearchableStorageState<TState>(
        this ISiloBuilder builder,
        string providerName,
        string stateName,
        int applicationSchemaVersion)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddSearchableStorageState<TState>(
            providerName,
            stateName,
            applicationSchemaVersion);
        return builder;
    }

    /// <summary>
    /// Registers the single CLR state type used by one searchable provider/state-name pair.
    /// </summary>
    /// <typeparam name="TState">The Orleans persistent-state type.</typeparam>
    /// <param name="services">The silo service collection.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <returns>The supplied service collection.</returns>
    public static IServiceCollection AddSearchableStorageState<TState>(
        this IServiceCollection services,
        string providerName,
        string stateName)
    {
        return services.AddSearchableStorageState<TState>(
            providerName,
            stateName,
            applicationSchemaVersion: 1);
    }

    /// <summary>
    /// Registers one CLR state type with an explicit application-controlled schema version.
    /// </summary>
    /// <typeparam name="TState">The Orleans persistent-state type.</typeparam>
    /// <param name="services">The silo service collection.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="applicationSchemaVersion">
    /// A positive application-owned version included in the durable schema fingerprint.
    /// </param>
    /// <returns>The supplied service collection.</returns>
    public static IServiceCollection AddSearchableStorageState<TState>(
        this IServiceCollection services,
        string providerName,
        string stateName,
        int applicationSchemaVersion)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applicationSchemaVersion);
        services.AddSingleton<ISearchableStateRegistration>(
            new SearchableStateRegistration<TState>(
                providerName,
                stateName,
                applicationSchemaVersion));
        services.TryAddSingleton<SearchableStateRegistry>();
        return services;
    }

    /// <summary>
    /// Adds a named searchable grain-storage provider.
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The name used by <see cref="PersistentStateAttribute"/>.</param>
    /// <param name="configure">An optional provider configuration delegate.</param>
    /// <returns>The supplied silo builder.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    /// <exception cref="InvalidOperationException">
    /// The provider name is already registered in index-only mode.
    /// </exception>
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
    /// Adds a named payload-free searchable index to an Orleans silo. Application state remains
    /// in caller-owned storage and index mutations are issued through
    /// <see cref="ISearchableStorageIndexWriter"/>.
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The name used to resolve the keyed index services.</param>
    /// <param name="configure">An optional provider configuration delegate.</param>
    /// <returns>The supplied silo builder.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    /// <exception cref="InvalidOperationException">
    /// The provider name is already registered in the payload-owning storage mode.
    /// </exception>
    public static ISiloBuilder AddSearchableIndex(
        this ISiloBuilder builder,
        string providerName,
        Action<SearchableStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddSearchableIndex(providerName, configure);
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
    /// <exception cref="InvalidOperationException">
    /// The provider name is already registered in index-only mode.
    /// </exception>
    public static IServiceCollection AddSearchableGrainStorage(
        this IServiceCollection services,
        string providerName,
        Action<SearchableStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        AddSearchableProvider(
            services,
            providerName,
            StorageNamespaceMode.Integrated,
            configure);

        services.AddTransient<
            IPostConfigureOptions<SearchableStorageOptions>,
            DefaultStorageProviderSerializerOptionsConfigurator<SearchableStorageOptions>>();

        services.AddKeyedSingleton<IGrainStorage>(
            providerName,
            (serviceProvider, _) => SearchableGrainStorageFactory.Create(serviceProvider, providerName));

        return services;
    }

    /// <summary>
    /// Adds a named payload-free searchable index to an Orleans service collection. The collection
    /// can belong to a silo or an external Orleans client. This registration exposes keyed writer,
    /// query, and administration services, but deliberately does not expose an
    /// <see cref="IGrainStorage"/> provider.
    /// </summary>
    /// <param name="services">The silo or external Orleans-client service collection.</param>
    /// <param name="providerName">The name used to resolve the keyed index services.</param>
    /// <param name="configure">An optional provider configuration delegate.</param>
    /// <returns>The supplied service collection.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    /// <exception cref="InvalidOperationException">
    /// The provider name is already registered in the payload-owning storage mode.
    /// </exception>
    public static IServiceCollection AddSearchableIndex(
        this IServiceCollection services,
        string providerName,
        Action<SearchableStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        AddSearchableProvider(
            services,
            providerName,
            StorageNamespaceMode.IndexOnly,
            configure);

        services.AddKeyedSingleton<ISearchableStorageIndexWriter>(
            providerName,
            (serviceProvider, _) =>
            {
                var configuredOptions = serviceProvider
                    .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
                    .Get(providerName);
                return new SearchableStorageIndexWriter(
                    providerName,
                    configuredOptions,
                    serviceProvider.GetRequiredService<IGrainFactory>(),
                    serviceProvider.GetRequiredService<SearchableStateRegistry>(),
                    serviceProvider.GetService<ILogger<SearchableStorageIndexWriter>>());
            });

        return services;
    }

    private static void AddSearchableProvider(
        IServiceCollection services,
        string providerName,
        StorageNamespaceMode namespaceMode,
        Action<SearchableStorageOptions>? configure)
    {
        EnsureCompatibleProviderRegistration(services, providerName, namespaceMode);

        var options = services.AddOptions<SearchableStorageOptions>(providerName);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        // Namespace mode is selected by the registration API rather than by a stringly-typed or
        // user-configurable option. Keep it last so every resolution sees the authoritative mode.
        options.Configure(value => value.NamespaceMode = namespaceMode);

        options
            .Validate(static value => value.PartitionCount > 0, "PartitionCount must be greater than zero.")
            .Validate(
                static value => value.VirtualSlotTargetCount > 0,
                "VirtualSlotTargetCount must be greater than zero.")
            .Validate(
                static value => IsVirtualSlotLayoutAddressable(value),
                $"PartitionCount and VirtualSlotTargetCount must produce no more than {StorageLayout.MaximumVirtualSlotCount} virtual slots.")
            .Validate(
                static value => value.JournalSegmentCapacity > 0,
                "JournalSegmentCapacity must be greater than zero.")
            .Validate(
                static value => value.MaximumJournalReplayEntries > 0,
                "MaximumJournalReplayEntries must be greater than zero.")
            .Validate(
                static value => IsJournalLayoutSupported(value),
                "JournalSegmentCapacity and MaximumJournalReplayEntries must produce an addressable "
                + "journal ring within the fixed searchable-storage capacity limits.")
            .Validate(
                static value => value.CompactionThreshold > 0,
                "CompactionThreshold must be greater than zero.")
            .Validate(
                static value => value.CompactionThreshold <= value.MaximumJournalReplayEntries,
                "CompactionThreshold must not exceed MaximumJournalReplayEntries.")
            .Validate(
                static value => value.Movement.TransferPageRecordLimit > 0
                    && value.Movement.TransferPageRecordLimit
                        <= SearchableStorageMovementOptions.MaximumTransferPageRecordLimit,
                $"Movement.TransferPageRecordLimit must be between 1 and {SearchableStorageMovementOptions.MaximumTransferPageRecordLimit}.")
            .Validate(
                static value => value.Movement.TransferPageByteTarget > 0
                    && value.Movement.TransferPageByteTarget
                        <= SearchableStorageMovementOptions.MaximumTransferPageByteTarget,
                $"Movement.TransferPageByteTarget must be between 1 and {SearchableStorageMovementOptions.MaximumTransferPageByteTarget}.")
            .ValidateOnStart();

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<
                IValidateOptions<SearchableStorageOptions>,
                Querying.SearchableStorageQueryOptionsValidator>());

        services.TryAddSingleton<StorageLayoutCacheRegistry>();
        services.TryAddSingleton<SearchableStateRegistry>();

        services.AddKeyedSingleton<ISearchableStorageQueryClient>(
            providerName,
            (serviceProvider, _) =>
            {
                var configuredOptions = serviceProvider
                    .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
                    .Get(providerName);
                return new SearchableStorageClient(
                    serviceProvider.GetRequiredService<IGrainFactory>(),
                    providerName,
                    configuredOptions.PartitionCount,
                    configuredOptions.Query,
                    serviceProvider.GetRequiredService<SearchableStateRegistry>(),
                    configuredOptions.NamespaceMode,
                    serviceProvider.GetService<ILogger<SearchableStorageClient>>());
            });

        services.AddKeyedSingleton<ISearchableStorageClient>(
            providerName,
            (serviceProvider, _) => serviceProvider.GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName));

        services.AddKeyedSingleton<ISearchableStorageAdminClient>(
            providerName,
            (serviceProvider, _) =>
            {
                var configuredOptions = serviceProvider
                    .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
                    .Get(providerName);
                return new SearchableStorageAdminClient(
                    serviceProvider.GetRequiredService<IGrainFactory>(),
                    providerName,
                    configuredOptions.PartitionCount,
                    configuredOptions.Movement,
                    configuredOptions.NamespaceMode,
                    serviceProvider.GetService<ILogger<SearchableStorageAdminClient>>());
            });
    }

    private static void EnsureCompatibleProviderRegistration(
        IServiceCollection services,
        string providerName,
        StorageNamespaceMode namespaceMode)
    {
        var existing = services
            .Where(static descriptor =>
                descriptor.ServiceType == typeof(SearchableProviderRegistration))
            .Select(static descriptor =>
                descriptor.ImplementationInstance as SearchableProviderRegistration)
            .FirstOrDefault(registration => registration is not null
                && string.Equals(
                    registration.ProviderName,
                    providerName,
                    StringComparison.Ordinal));
        if (existing is not null)
        {
            if (existing.NamespaceMode != namespaceMode)
            {
                throw new InvalidOperationException(
                    $"Searchable provider '{providerName}' is already registered in a different "
                    + $"namespace mode ('{existing.NamespaceMode}'). A provider name cannot mix "
                    + "payload-owning storage and an index-only namespace.");
            }

            return;
        }

        services.AddSingleton(
            new SearchableProviderRegistration(providerName, namespaceMode));
    }

    private static bool IsJournalLayoutSupported(SearchableStorageOptions options)
    {
        if (options.JournalSegmentCapacity <= 0 || options.MaximumJournalReplayEntries <= 0)
        {
            // The dedicated validators produce the more specific messages for non-positive values.
            return true;
        }

        try
        {
            StoragePersistence.ValidateOptions(
                options.JournalSegmentCapacity,
                options.MaximumJournalReplayEntries);
            StorageCapacityGuardrails.ValidatePersistenceConfiguration(
                options.JournalSegmentCapacity,
                options.MaximumJournalReplayEntries,
                nameof(options.JournalSegmentCapacity),
                nameof(options.MaximumJournalReplayEntries));
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
    }

    private static bool IsVirtualSlotLayoutAddressable(SearchableStorageOptions options)
    {
        if (options.PartitionCount <= 0 || options.VirtualSlotTargetCount <= 0)
        {
            // Dedicated validation produces the more specific message for non-positive values.
            return true;
        }

        try
        {
            _ = StorageLayout.DeriveVirtualSlotCount(
                options.PartitionCount,
                options.VirtualSlotTargetCount);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    private sealed record SearchableProviderRegistration(
        string ProviderName,
        StorageNamespaceMode NamespaceMode);
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
