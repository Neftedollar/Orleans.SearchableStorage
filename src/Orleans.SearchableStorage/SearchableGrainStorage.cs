using System.Collections.Concurrent;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Orleans.SearchableStorage;

internal sealed class SearchableGrainStorage : IGrainStorage
{
    private readonly IActivatorProvider _activatorProvider;
    private readonly Func<int, IStoragePartitionGrain> _getPartition;
    private readonly StorageLayoutCache _layoutCache;
    private readonly IGrainStorageSerializer _serializer;
    private readonly StoragePersistenceSettings _persistenceSettings;

    public SearchableGrainStorage(
        string name,
        SearchableStorageOptions options,
        IGrainFactory grainFactory,
        IActivatorProvider activatorProvider)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(activatorProvider);

        var configuration = CreateConfiguration(name, options);
        _persistenceSettings = configuration.PersistenceSettings;
        _serializer = configuration.Serializer;
        _activatorProvider = activatorProvider;
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
        Func<int, IStoragePartitionGrain> getPartition)
    {
        ArgumentNullException.ThrowIfNull(activatorProvider);
        ArgumentNullException.ThrowIfNull(layoutCache);
        ArgumentNullException.ThrowIfNull(getPartition);

        var configuration = CreateConfiguration(name, options);
        _persistenceSettings = configuration.PersistenceSettings;
        _serializer = configuration.Serializer;
        _activatorProvider = activatorProvider;
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

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);

        var recordKey = CreateRecordKey(stateName, grainId);
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

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);

        var recordKey = CreateRecordKey(stateName, grainId);
        var payload = _serializer.Serialize(grainState.State).ToArray();
        var indexes = IndexMetadataProvider.Extract(stateName, grainState.State);
        var request = new StorageWriteRequest
        {
            RecordKey = recordKey,
            GrainId = grainId,
            Payload = payload,
            ExpectedETag = grainState.ETag,
            IndexEntries = [.. indexes],
            Persistence = _persistenceSettings,
        };

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

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);

        var recordKey = CreateRecordKey(stateName, grainId);
        await ExecuteRoutedAsync(
            grainId,
            (partition, slot, epoch) => partition.ClearRoutedAsync(new RoutedStorageClearRequest
            {
                Request = new StorageClearRequest
                {
                    RecordKey = recordKey,
                    ExpectedETag = grainState.ETag,
                    Persistence = _persistenceSettings,
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
