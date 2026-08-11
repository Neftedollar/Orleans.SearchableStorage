using System.Collections.Concurrent;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Shares ordinary routing-layout reads between concurrent callers without coupling the shared
/// operation to any caller's cancellation token. Capability checks can request an explicit
/// caller-owned fresh read without replacing that routing cache.
/// </summary>
internal sealed class StorageLayoutCache
{
    private readonly Func<Task<StorageLayoutSnapshot?>> _loadLayout;
    private readonly object _lock = new();
    private Task<StorageLayoutSnapshot?>? _layoutTask;

    public StorageLayoutCache(Func<Task<StorageLayoutSnapshot?>> loadLayout)
    {
        ArgumentNullException.ThrowIfNull(loadLayout);
        _loadLayout = loadLayout;
    }

    public async Task<StorageLayoutSnapshot?> GetAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Task<StorageLayoutSnapshot?> layoutTask;
        lock (_lock)
        {
            layoutTask = _layoutTask ??= _loadLayout();
        }

        try
        {
            var layout = await layoutTask.WaitAsync(cancellationToken);
            if (layout is null)
            {
                Reset(layoutTask);
            }

            return layout;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ = ObserveCompletionAsync(layoutTask);
            throw;
        }
        catch
        {
            Reset(layoutTask);
            throw;
        }
    }

    /// <summary>
    /// Reads a snapshot through a load started by this caller, without consulting or replacing the
    /// shared routing cache. This is reserved for capability checks which must not inherit a read
    /// initiated before the current operation.
    /// </summary>
    public async Task<StorageLayoutSnapshot?> ReadFreshAsync(
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var layoutTask = _loadLayout();

        try
        {
            return await layoutTask.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ = ObserveDetachedCompletionAsync(layoutTask);
            throw;
        }
    }

    public void Invalidate(StorageLayoutSnapshot observedLayout)
    {
        ArgumentNullException.ThrowIfNull(observedLayout);
        lock (_lock)
        {
            if (_layoutTask is { IsCompletedSuccessfully: true } layoutTask
                && ReferenceEquals(layoutTask.Result, observedLayout))
            {
                _layoutTask = null;
            }
        }
    }

    private void Reset(Task<StorageLayoutSnapshot?> layoutTask)
    {
        lock (_lock)
        {
            if (ReferenceEquals(_layoutTask, layoutTask))
            {
                _layoutTask = null;
            }
        }
    }

    private async Task ObserveCompletionAsync(Task<StorageLayoutSnapshot?> layoutTask)
    {
        try
        {
            if (await layoutTask is null)
            {
                Reset(layoutTask);
            }
        }
        catch
        {
            Reset(layoutTask);
        }
    }

    private static async Task ObserveDetachedCompletionAsync(
        Task<StorageLayoutSnapshot?> layoutTask)
    {
        try
        {
            _ = await layoutTask;
        }
        catch
        {
            // The caller stopped waiting, but the underlying Orleans RPC still needs observation.
        }
    }
}

/// <summary>
/// Bounds partition routing maps to one shared cache per provider and silo instead of one cache per
/// partition activation.
/// </summary>
internal sealed class StorageLayoutCacheRegistry
{
    private readonly ConcurrentDictionary<string, StorageLayoutCache> _caches =
        new(StringComparer.Ordinal);
    private readonly Func<string, StorageLayoutCache> _createCache;

    public StorageLayoutCacheRegistry(IGrainFactory grainFactory)
        : this(providerName => CreateCache(grainFactory, providerName))
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
    }

    internal StorageLayoutCacheRegistry(Func<string, StorageLayoutCache> createCache)
    {
        ArgumentNullException.ThrowIfNull(createCache);
        _createCache = createCache;
    }

    public StorageLayoutCache Get(string providerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        return _caches.GetOrAdd(providerName, _createCache);
    }

    private static StorageLayoutCache CreateCache(
        IGrainFactory grainFactory,
        string providerName)
    {
        var layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        return new StorageLayoutCache(layoutGrain.GetCurrentLayoutAsync);
    }
}
