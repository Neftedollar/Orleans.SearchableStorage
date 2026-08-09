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
    private readonly SearchableStorageOptions _options;
    private readonly IGrainStorageSerializer _serializer;
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

        if (options.PartitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.PartitionCount, "PartitionCount must be greater than zero.");
        }

        _options = options;
        _serializer = options.GrainStorageSerializer
            ?? throw new ArgumentException("A grain storage serializer has not been configured.", nameof(options));
        _activatorProvider = activatorProvider;
        _layout = StorageLayout.CreateDescriptor(name, options.PartitionCount);
        _layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(name);
        _partitions = new Lazy<IStoragePartitionGrain>[options.PartitionCount];
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
        };

        grainState.ETag = await GetPartition(grainId).WriteAsync(request);
        grainState.RecordExists = true;
    }

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(grainState);
        await EnsureLayoutAsync();

        var recordKey = CreateRecordKey(stateName, grainId);
        await GetPartition(grainId).ClearAsync(recordKey, grainState.ETag);
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
        var index = (int)((uint)grainId.GetUniformHashCode() % (uint)_options.PartitionCount);
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
}
