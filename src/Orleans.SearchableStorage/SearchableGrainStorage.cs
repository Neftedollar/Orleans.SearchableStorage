using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Orleans.SearchableStorage;

internal sealed class SearchableGrainStorage : IGrainStorage
{
    private readonly IActivatorProvider _activatorProvider;
    private readonly StorageLayoutDescriptor _layout;
    private readonly IStorageLayoutGrain _layoutGrain;
    private readonly object _layoutLock = new();
    private readonly IGrainStorageSerializer _serializer;
    private readonly StoragePersistenceSettings _persistenceSettings;
    private readonly Lazy<IStoragePartitionGrain>[] _partitions;
    private Task? _layoutInitializationTask;

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

        var partitionCount = options.PartitionCount;
        var journalSegmentCapacity = options.JournalSegmentCapacity;
        var maximumJournalReplayEntries = options.MaximumJournalReplayEntries;
        var compactionThreshold = options.CompactionThreshold;
        var serializer = options.GrainStorageSerializer;

        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), partitionCount, "PartitionCount must be greater than zero.");
        }

        ValidatePersistenceOptions(journalSegmentCapacity, maximumJournalReplayEntries, compactionThreshold);

        _persistenceSettings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = journalSegmentCapacity,
            MaximumJournalReplayEntries = maximumJournalReplayEntries,
            CompactionThreshold = compactionThreshold,
        };
        _serializer = serializer
            ?? throw new ArgumentException("A grain storage serializer has not been configured.", nameof(options));
        _activatorProvider = activatorProvider;
        _layout = StorageLayout.CreateDescriptor(
            name,
            partitionCount,
            _persistenceSettings.JournalSegmentCapacity,
            _persistenceSettings.MaximumJournalReplayEntries);
        _layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(name);
        _partitions = new Lazy<IStoragePartitionGrain>[partitionCount];
        for (var index = 0; index < _partitions.Length; index++)
        {
            var partitionIndex = index;
            _partitions[index] = new Lazy<IStoragePartitionGrain>(
                () => grainFactory.GetGrain<IStoragePartitionGrain>(StorageLayout.CreatePartitionKey(name, partitionIndex)));
        }
    }

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);
        await EnsureLayoutAsync();

        var recordKey = CreateRecordKey(stateName, grainId);
        var result = await GetPartition(grainId).ReadAsync(recordKey);
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
        await EnsureLayoutAsync();

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

        grainState.ETag = await GetPartition(grainId).WriteAsync(request);
        grainState.RecordExists = true;
    }

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);
        await EnsureLayoutAsync();

        var recordKey = CreateRecordKey(stateName, grainId);
        await GetPartition(grainId).ClearAsync(new StorageClearRequest
        {
            RecordKey = recordKey,
            ExpectedETag = grainState.ETag,
            Persistence = _persistenceSettings,
        });
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

    private IStoragePartitionGrain GetPartition(GrainId grainId)
    {
        // Orleans defines this hash as uniform and stable, so the physical partition remains
        // unchanged across processes and runtime upgrades which preserve the GrainId contract.
        var index = (int)((uint)grainId.GetUniformHashCode() % (uint)_partitions.Length);
        return _partitions[index].Value;
    }

    private async Task EnsureLayoutAsync()
    {
        Task initializationTask;
        lock (_layoutLock)
        {
            initializationTask = _layoutInitializationTask ??= _layoutGrain.InitializeAsync(_layout);
        }

        try
        {
            await initializationTask;
        }
        catch
        {
            lock (_layoutLock)
            {
                if (ReferenceEquals(_layoutInitializationTask, initializationTask))
                {
                    _layoutInitializationTask = null;
                }
            }

            throw;
        }
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
}
