using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.SearchableStorage.Diagnostics;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage;

internal sealed class SearchableStorageIndexWriter : ISearchableStorageIndexWriter
{
    private readonly string _providerName;
    private readonly Func<int, IStoragePartitionGrain> _getPartition;
    private readonly StorageLayoutCache _layoutCache;
    private readonly StoragePersistenceSettings _persistenceSettings;
    private readonly SearchableStateRegistry _stateRegistry;
    private readonly Func<string, IStorageIndexSchemaGrain>? _getIndexSchema;
    private readonly ILogger<SearchableStorageIndexWriter>? _logger;
    private readonly ActiveSchemaValidationCache _activeSchemas = new();

    public SearchableStorageIndexWriter(
        string name,
        SearchableStorageOptions options,
        IGrainFactory grainFactory,
        SearchableStateRegistry stateRegistry,
        ILogger<SearchableStorageIndexWriter>? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(stateRegistry);

        var configuration = CreateConfiguration(name, options);
        _providerName = name;
        _persistenceSettings = configuration.PersistenceSettings;
        _stateRegistry = stateRegistry;
        _logger = logger;
        _getIndexSchema = stateName => grainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(name, stateName));
        var layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(name);
        _layoutCache = new StorageLayoutCache(
            async () => await layoutGrain.InitializeRoutingAsync(configuration.Layout));
        var partitions = new ConcurrentDictionary<int, IStoragePartitionGrain>();
        _getPartition = index => partitions.GetOrAdd(
            index,
            partitionIndex => grainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(name, partitionIndex)));
    }

    internal SearchableStorageIndexWriter(
        string name,
        SearchableStorageOptions options,
        StorageLayoutCache layoutCache,
        Func<int, IStoragePartitionGrain> getPartition,
        SearchableStateRegistry? stateRegistry = null,
        Func<string, IStorageIndexSchemaGrain>? getIndexSchema = null,
        ILogger<SearchableStorageIndexWriter>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(layoutCache);
        ArgumentNullException.ThrowIfNull(getPartition);

        var configuration = CreateConfiguration(name, options);
        _providerName = name;
        _persistenceSettings = configuration.PersistenceSettings;
        _layoutCache = layoutCache;
        _getPartition = getPartition;
        _stateRegistry = stateRegistry ?? SearchableStateRegistry.Empty;
        _getIndexSchema = getIndexSchema;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task UpsertAsync<TState>(
        string stateName,
        GrainId grainId,
        TState state,
        CancellationToken cancellationToken = default)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "index.upsert",
            "execute",
            _logger,
            lifecycle: false,
            () => UpsertCoreAsync(stateName, grainId, state, cancellationToken));
    }

    /// <inheritdoc />
    public Task RemoveAsync<TState>(
        string stateName,
        GrainId grainId,
        CancellationToken cancellationToken = default)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "index.remove",
            "execute",
            _logger,
            lifecycle: false,
            () => RemoveCoreAsync<TState>(stateName, grainId, cancellationToken));
    }

    private async Task UpsertCoreAsync<TState>(
        string stateName,
        GrainId grainId,
        TState state,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        StorageCapacityGuardrails.ValidateGrainId(grainId);

        var recordKey = CreateRecordKey(stateName, grainId);
        StorageCapacityGuardrails.ValidateRecordKey(recordKey);
        var registration = GetRequiredRegistration<TState>(stateName);
        // A managed index-only namespace is always gated before application getters run.
        await EnsureSchemaActiveAsync(registration, cancellationToken);

        var indexes = IndexMetadataProvider.Extract(
            stateName,
            state,
            registration.Schema.Fingerprint);
        var request = new StorageWriteRequest
        {
            RecordKey = recordKey,
            GrainId = grainId,
            Payload = null,
            ExpectedETag = null,
            IndexEntries = [.. indexes],
            Persistence = _persistenceSettings,
            IndexSchemaFingerprint = registration.Schema.Fingerprint,
            StateName = stateName,
            IndexSchemaProtocolVersion = StorageIndexSchema.ProtocolVersion,
            Unconditional = true,
        };
        StorageCapacityGuardrails.ValidateWriteRequest(request);

        _ = await ExecuteRoutedAsync(
            grainId,
            (partition, slot, epoch) => partition.WriteRoutedAsync(
                new RoutedStorageWriteRequest
                {
                    Request = request,
                    Slot = slot,
                    Epoch = epoch,
                }),
            cancellationToken);
    }

    private async Task RemoveCoreAsync<TState>(
        string stateName,
        GrainId grainId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        StorageCapacityGuardrails.ValidateGrainId(grainId);

        var recordKey = CreateRecordKey(stateName, grainId);
        StorageCapacityGuardrails.ValidateRecordKey(recordKey);
        var registration = GetRequiredRegistration<TState>(stateName);
        await EnsureSchemaActiveAsync(registration, cancellationToken);

        await ExecuteRoutedAsync(
            grainId,
            (partition, slot, epoch) => partition.ClearRoutedAsync(
                new RoutedStorageClearRequest
                {
                    Request = new StorageClearRequest
                    {
                        RecordKey = recordKey,
                        ExpectedETag = null,
                        Persistence = _persistenceSettings,
                        StateName = stateName,
                        IndexSchemaFingerprint = registration.Schema.Fingerprint,
                        IndexSchemaProtocolVersion = StorageIndexSchema.ProtocolVersion,
                        Unconditional = true,
                    },
                    GrainId = grainId,
                    Slot = slot,
                    Epoch = epoch,
                }),
            cancellationToken);
    }

    private async Task<T> ExecuteRoutedAsync<T>(
        GrainId grainId,
        Func<IStoragePartitionGrain, int, long, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var layout = await GetRequiredLayoutAsync(cancellationToken);
        for (var attempt = 0; ; attempt++)
        {
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            var owner = layout.GetOwner(slot);
            try
            {
                return await WaitForCallAsync(
                    operation(_getPartition(owner), slot, layout.Epoch),
                    cancellationToken);
            }
            catch (StorageRouteMismatchException) when (attempt == 0)
            {
                _layoutCache.Invalidate(layout);
                layout = await GetRequiredLayoutAsync(cancellationToken);
            }
        }
    }

    private async Task ExecuteRoutedAsync(
        GrainId grainId,
        Func<IStoragePartitionGrain, int, long, Task> operation,
        CancellationToken cancellationToken)
    {
        await ExecuteRoutedAsync(
            grainId,
            async (partition, slot, epoch) =>
            {
                await operation(partition, slot, epoch);
                return true;
            },
            cancellationToken);
    }

    private async Task<StorageLayoutSnapshot> GetRequiredLayoutAsync(
        CancellationToken cancellationToken)
    {
        return await _layoutCache.GetAsync(cancellationToken)
            ?? throw new InvalidOperationException(
                "The index layout was not initialized by its index provider.");
    }

    private async Task EnsureSchemaActiveAsync(
        ISearchableStateRegistration registration,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(registration);
        cancellationToken.ThrowIfCancellationRequested();
        if (_activeSchemas.IsActive(registration))
        {
            return;
        }

        var control = _getIndexSchema
            ?? throw new InvalidOperationException(
                "The managed index-schema control is unavailable.");
        var snapshot = await WaitForCallAsync(
            control(registration.StateName).GetAsync(
                StorageIndexSchema.CreateRequest(registration)),
            cancellationToken);
        if (snapshot.Rebuild is not null)
        {
            throw new SearchableStorageIndexSchemaException(
                $"Index schema rebuild '{snapshot.Rebuild.RebuildId}' is still running for state "
                + $"'{registration.StateName}'. Keep searchable traffic quiesced and resume it "
                + "through ISearchableStorageAdminClient.");
        }

        if (snapshot.ActiveFingerprint is null
            || !IndexSchemaIdentity.FixedTimeEquals(
                snapshot.ActiveFingerprint,
                registration.Schema.Fingerprint))
        {
            throw new SearchableStorageIndexSchemaException(
                $"The registered index schema for state '{registration.StateName}' is not active. "
                + "Quiesce searchable traffic and run RebuildIndexSchemaAsync<TState> before "
                + "using it.");
        }

        _activeSchemas.MarkActive(registration);
    }

    private ISearchableStateRegistration GetRequiredRegistration<TState>(string stateName)
    {
        var registration = _stateRegistry.Find<TState>(_providerName, stateName);
        if (registration is null)
        {
            throw new SearchableStorageIndexSchemaException(
                $"Index-only provider '{_providerName}' requires a managed schema declaration for "
                + $"state '{stateName}'. Register it with "
                + "AddSearchableStorageState<TState> before using the index writer.");
        }

        return registration;
    }

    private static string CreateRecordKey(string stateName, GrainId grainId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        return string.Concat(
            stateName,
            "/",
            Convert.ToHexString(grainId.Type.AsSpan()),
            "/",
            Convert.ToHexString(grainId.Key.AsSpan()));
    }

    private static StorageConfiguration CreateConfiguration(
        string name,
        SearchableStorageOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        if (options.NamespaceMode != StorageNamespaceMode.IndexOnly)
        {
            throw new ArgumentException(
                "SearchableStorageIndexWriter requires an index-only namespace.",
                nameof(options));
        }

        if (options.PartitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.PartitionCount,
                "PartitionCount must be greater than zero.");
        }

        _ = StorageLayout.DeriveVirtualSlotCount(
            options.PartitionCount,
            options.VirtualSlotTargetCount);
        ValidatePersistenceOptions(
            options.JournalSegmentCapacity,
            options.MaximumJournalReplayEntries,
            options.CompactionThreshold);

        var persistenceSettings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = options.JournalSegmentCapacity,
            MaximumJournalReplayEntries = options.MaximumJournalReplayEntries,
            CompactionThreshold = options.CompactionThreshold,
            NamespaceMode = StorageNamespaceMode.IndexOnly,
        };
        var layout = StorageLayout.CreateDescriptor(
            name,
            options.PartitionCount,
            persistenceSettings.JournalSegmentCapacity,
            persistenceSettings.MaximumJournalReplayEntries,
            options.VirtualSlotTargetCount,
            StorageNamespaceMode.IndexOnly);
        return new StorageConfiguration(persistenceSettings, layout);
    }

    private static void ValidatePersistenceOptions(
        int journalSegmentCapacity,
        int maximumJournalReplayEntries,
        int compactionThreshold)
    {
        if (journalSegmentCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(journalSegmentCapacity),
                journalSegmentCapacity,
                "JournalSegmentCapacity must be greater than zero.");
        }

        if (maximumJournalReplayEntries <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumJournalReplayEntries),
                maximumJournalReplayEntries,
                "MaximumJournalReplayEntries must be greater than zero.");
        }

        StoragePersistence.ValidateOptions(
            journalSegmentCapacity,
            maximumJournalReplayEntries);
        StorageCapacityGuardrails.ValidatePersistenceConfiguration(
            journalSegmentCapacity,
            maximumJournalReplayEntries,
            nameof(journalSegmentCapacity),
            nameof(maximumJournalReplayEntries));

        if (compactionThreshold <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(compactionThreshold),
                compactionThreshold,
                "CompactionThreshold must be greater than zero.");
        }

        if (compactionThreshold > maximumJournalReplayEntries)
        {
            throw new ArgumentOutOfRangeException(
                nameof(compactionThreshold),
                compactionThreshold,
                "CompactionThreshold must not exceed MaximumJournalReplayEntries.");
        }
    }

    private static async Task<T> WaitForCallAsync<T>(
        Task<T> call,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        try
        {
            return await call.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ = ObserveCompletionAsync(call);
            throw;
        }
    }

    private static async Task ObserveCompletionAsync(Task call)
    {
        await call.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private readonly record struct StorageConfiguration(
        StoragePersistenceSettings PersistenceSettings,
        StorageLayoutDescriptor Layout);
}
