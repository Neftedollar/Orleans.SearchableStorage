using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.SearchableStorage.Diagnostics;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Orleans.SearchableStorage;

internal sealed class SearchableGrainStorage : IGrainStorage
{
    private readonly string _providerName;
    private readonly IActivatorProvider _activatorProvider;
    private readonly Func<int, IStoragePartitionGrain> _getPartition;
    private readonly StorageLayoutCache _layoutCache;
    private readonly IGrainStorageSerializer _serializer;
    private readonly StoragePersistenceSettings _persistenceSettings;
    private readonly SearchableStateRegistry _stateRegistry;
    private readonly Func<string, IStorageIndexSchemaGrain>? _getIndexSchema;
    private readonly ILogger<SearchableGrainStorage>? _logger;
    private readonly ActiveSchemaValidationCache _activeSchemas = new();

    public SearchableGrainStorage(
        string name,
        SearchableStorageOptions options,
        IGrainFactory grainFactory,
        IActivatorProvider activatorProvider,
        SearchableStateRegistry stateRegistry,
        ILogger<SearchableGrainStorage>? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(activatorProvider);
        ArgumentNullException.ThrowIfNull(stateRegistry);

        var configuration = CreateConfiguration(name, options);
        _providerName = name;
        _persistenceSettings = configuration.PersistenceSettings;
        _serializer = configuration.Serializer;
        _activatorProvider = activatorProvider;
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

    internal SearchableGrainStorage(
        string name,
        SearchableStorageOptions options,
        IActivatorProvider activatorProvider,
        StorageLayoutCache layoutCache,
        Func<int, IStoragePartitionGrain> getPartition,
        SearchableStateRegistry? stateRegistry = null,
        Func<string, IStorageIndexSchemaGrain>? getIndexSchema = null,
        ILogger<SearchableGrainStorage>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(activatorProvider);
        ArgumentNullException.ThrowIfNull(layoutCache);
        ArgumentNullException.ThrowIfNull(getPartition);

        var configuration = CreateConfiguration(name, options);
        _providerName = name;
        _persistenceSettings = configuration.PersistenceSettings;
        _serializer = configuration.Serializer;
        _activatorProvider = activatorProvider;
        _stateRegistry = stateRegistry ?? SearchableStateRegistry.Empty;
        _getIndexSchema = getIndexSchema;
        _logger = logger;
        _layoutCache = layoutCache;
        _getPartition = getPartition;
    }

    private static StorageConfiguration CreateConfiguration(
        string name,
        SearchableStorageOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);

        var partitionCount = options.PartitionCount;
        var virtualSlotTargetCount = options.VirtualSlotTargetCount;
        var journalSegmentCapacity = options.JournalSegmentCapacity;
        var maximumJournalReplayEntries = options.MaximumJournalReplayEntries;
        var compactionThreshold = options.CompactionThreshold;
        var serializer = options.GrainStorageSerializer;

        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), partitionCount, "PartitionCount must be greater than zero.");
        }

        _ = StorageLayout.DeriveVirtualSlotCount(partitionCount, virtualSlotTargetCount);

        ValidatePersistenceOptions(journalSegmentCapacity, maximumJournalReplayEntries, compactionThreshold);

        var persistenceSettings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = journalSegmentCapacity,
            MaximumJournalReplayEntries = maximumJournalReplayEntries,
            CompactionThreshold = compactionThreshold,
        };
        var configuredSerializer = serializer
            ?? throw new ArgumentException("A grain storage serializer has not been configured.", nameof(options));
        var layout = StorageLayout.CreateDescriptor(
            name,
            partitionCount,
            persistenceSettings.JournalSegmentCapacity,
            persistenceSettings.MaximumJournalReplayEntries,
            virtualSlotTargetCount);
        return new StorageConfiguration(configuredSerializer, persistenceSettings, layout);
    }

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "storage.read",
            "execute",
            _logger,
            lifecycle: false,
            () => ReadStateCoreAsync(stateName, grainId, grainState));
    }

    private async Task ReadStateCoreAsync<T>(
        string stateName,
        GrainId grainId,
        IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);
        StorageCapacityGuardrails.ValidateGrainId(grainId);

        var recordKey = CreateRecordKey(stateName, grainId);
        StorageCapacityGuardrails.ValidateRecordKey(recordKey);
        var result = await ExecuteRoutedAsync(
            grainId,
            (partition, slot, epoch) => partition.ReadRoutedAsync(new RoutedStorageReadRequest
            {
                RecordKey = recordKey,
                GrainId = grainId,
                Slot = slot,
                Epoch = epoch,
            }));
        if (!result.Found)
        {
            grainState.State = CreateInstance<T>();
            grainState.ETag = null;
            grainState.RecordExists = false;
            return;
        }

        if (result.Payload is null || result.ETag is null)
        {
            throw new InvalidOperationException($"Stored record '{recordKey}' is incomplete.");
        }

        grainState.State = _serializer.Deserialize<T>(result.Payload) ?? CreateInstance<T>();
        grainState.ETag = result.ETag;
        grainState.RecordExists = true;
    }

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "storage.write",
            "execute",
            _logger,
            lifecycle: false,
            () => WriteStateCoreAsync(stateName, grainId, grainState));
    }

    private async Task WriteStateCoreAsync<T>(
        string stateName,
        GrainId grainId,
        IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);
        StorageCapacityGuardrails.ValidateGrainId(grainId);

        var recordKey = CreateRecordKey(stateName, grainId);
        StorageCapacityGuardrails.ValidateRecordKey(recordKey);
        var payload = _serializer.Serialize(grainState.State).ToArray();
        StorageCapacityGuardrails.ValidateRecordKeyAndPayload(recordKey, payload);
        var registration = _stateRegistry.Find<T>(_providerName, stateName);
        var requiresPreExtractionSchemaGate = registration is not null
            || _stateRegistry.ContainsProvider(_providerName);
        if (requiresPreExtractionSchemaGate)
        {
            // Registered schemas fail closed while a rebuild is active, before invoking application
            // getters. A partial managed registration is also rejected locally at this point.
            await EnsureSchemaActiveAsync(stateName, registration);
        }

        var indexes = registration is null
            ? IndexMetadataProvider.Extract(stateName, grainState.State)
            : IndexMetadataProvider.Extract(
                stateName,
                grainState.State,
                registration.Schema.Fingerprint);
        var request = new StorageWriteRequest
        {
            RecordKey = recordKey,
            GrainId = grainId,
            Payload = payload,
            ExpectedETag = grainState.ETag,
            IndexEntries = [.. indexes],
            Persistence = _persistenceSettings,
            IndexSchemaFingerprint = registration?.Schema.Fingerprint,
            StateName = stateName,
            IndexSchemaProtocolVersion = registration is null
                ? 0
                : StorageIndexSchema.ProtocolVersion,
        };
        StorageCapacityGuardrails.ValidateWriteRequest(request);
        if (!requiresPreExtractionSchemaGate)
        {
            // A legacy/unmanaged first write completes all local materialization and capacity
            // admission before the layout probe is allowed to initialize durable authority.
            await EnsureSchemaActiveAsync(stateName, registration);
        }

        grainState.ETag = await ExecuteRoutedAsync(
            grainId,
            (partition, slot, epoch) => partition.WriteRoutedAsync(new RoutedStorageWriteRequest
            {
                Request = request,
                Slot = slot,
                Epoch = epoch,
            }));
        grainState.RecordExists = true;
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "storage.clear",
            "execute",
            _logger,
            lifecycle: false,
            () => ClearStateCoreAsync(stateName, grainId, grainState));
    }

    private async Task ClearStateCoreAsync<T>(
        string stateName,
        GrainId grainId,
        IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);
        StorageCapacityGuardrails.ValidateGrainId(grainId);

        var recordKey = CreateRecordKey(stateName, grainId);
        StorageCapacityGuardrails.ValidateRecordKey(recordKey);
        var registration = _stateRegistry.Find<T>(_providerName, stateName);
        await EnsureSchemaActiveAsync(stateName, registration);

        await ExecuteRoutedAsync(
            grainId,
            (partition, slot, epoch) => partition.ClearRoutedAsync(new RoutedStorageClearRequest
            {
                Request = new StorageClearRequest
                {
                    RecordKey = recordKey,
                    ExpectedETag = grainState.ETag,
                    Persistence = _persistenceSettings,
                    StateName = stateName,
                    IndexSchemaFingerprint = registration?.Schema.Fingerprint,
                    IndexSchemaProtocolVersion = registration is null
                        ? 0
                        : StorageIndexSchema.ProtocolVersion,
                },
                GrainId = grainId,
                Slot = slot,
                Epoch = epoch,
            }));
        grainState.State = CreateInstance<T>();
        grainState.ETag = null;
        grainState.RecordExists = false;
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

    private async Task<T> ExecuteRoutedAsync<T>(
        GrainId grainId,
        Func<IStoragePartitionGrain, int, long, Task<T>> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var layout = await GetRequiredLayoutAsync();
        for (var attempt = 0; ; attempt++)
        {
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            var owner = layout.GetOwner(slot);
            try
            {
                return await operation(_getPartition(owner), slot, layout.Epoch);
            }
            catch (StorageRouteMismatchException) when (attempt == 0)
            {
                _layoutCache.Invalidate(layout);
                layout = await GetRequiredLayoutAsync();
            }
        }
    }

    private async Task ExecuteRoutedAsync(
        GrainId grainId,
        Func<IStoragePartitionGrain, int, long, Task> operation)
    {
        await ExecuteRoutedAsync(
            grainId,
            async (partition, slot, epoch) =>
            {
                await operation(partition, slot, epoch);
                return true;
            });
    }

    private async Task<StorageLayoutSnapshot> GetRequiredLayoutAsync()
    {
        return await _layoutCache.GetAsync()
            ?? throw new InvalidOperationException("The storage layout was not initialized by its storage provider.");
    }

    private T CreateInstance<T>()
    {
        return _activatorProvider.GetActivator<T>().Create();
    }

    private async Task EnsureSchemaActiveAsync(
        string stateName,
        ISearchableStateRegistration? registration)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        if (registration is null)
        {
            if (_stateRegistry.ContainsProvider(_providerName))
            {
                throw new SearchableStorageIndexSchemaException(
                    $"Provider '{_providerName}' has managed schema declarations, but state "
                    + $"'{stateName}' is not registered on this silo. Register every state used by "
                    + "the provider with AddSearchableStorageState<TState>.");
            }

            var layout = await GetRequiredLayoutAsync();
            var schemaProtocolPublished = layout.IndexSchemaProtocolVersion
                == StorageIndexSchema.ProtocolVersion;
            var schemaEnablementActive = layout.CopyIndexSchemaEnablement() is not null;
            if (schemaProtocolPublished || schemaEnablementActive)
            {
                var capabilityState = schemaProtocolPublished
                    ? "has managed index schemas enabled"
                    : "is durably enabling managed index schemas";
                throw new SearchableStorageIndexSchemaException(
                    $"Provider '{_providerName}' {capabilityState}, but state "
                    + $"'{stateName}' "
                    + "is not registered on the silo. Register every state used by the provider "
                    + "with AddSearchableStorageState<TState>.");
            }

            return;
        }

        if (_activeSchemas.IsActive(registration))
        {
            return;
        }

        var control = _getIndexSchema
            ?? throw new InvalidOperationException("The managed index-schema control is unavailable.");
        var snapshot = await control(registration.StateName).GetAsync(
            StorageIndexSchema.CreateRequest(registration));
        if (snapshot.Rebuild is not null)
        {
            throw new SearchableStorageIndexSchemaException(
                $"Index schema rebuild '{snapshot.Rebuild.RebuildId}' is still running for state "
                + $"'{registration.StateName}'. Keep searchable traffic quiesced and resume it through "
                + "ISearchableStorageAdminClient.");
        }

        if (snapshot.ActiveFingerprint is null
            || !IndexSchemaIdentity.FixedTimeEquals(
                snapshot.ActiveFingerprint,
                registration.Schema.Fingerprint))
        {
            throw new SearchableStorageIndexSchemaException(
                $"The registered index schema for state '{registration.StateName}' is not active. "
                + "Quiesce searchable traffic and run RebuildIndexSchemaAsync<TState> before using it.");
        }

        _activeSchemas.MarkActive(registration);
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

        StoragePersistence.ValidateOptions(journalSegmentCapacity, maximumJournalReplayEntries);
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

    private readonly record struct StorageConfiguration(
        IGrainStorageSerializer Serializer,
        StoragePersistenceSettings PersistenceSettings,
        StorageLayoutDescriptor Layout);
}
