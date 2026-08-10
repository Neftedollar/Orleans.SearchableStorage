using System.Collections.Concurrent;
using AwesomeAssertions;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageLayoutCacheTests
{
    [Fact]
    public async Task RegistrySharesOneSnapshotPerProviderAndIsolatesProviders()
    {
        var loadCounts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var registry = new StorageLayoutCacheRegistry(
            providerName => new StorageLayoutCache(
                () =>
                {
                    loadCounts.AddOrUpdate(providerName, 1, static (_, count) => count + 1);
                    return Task.FromResult<StorageLayoutSnapshot?>(
                        CreateLayout(epoch: 1, providerName: providerName));
                }));

        var providerCaches = Enumerable.Range(0, 32)
            .Select(_ => registry.Get("shared-provider"))
            .ToArray();
        var layouts = await Task.WhenAll(providerCaches.Select(static cache => cache.GetAsync()));

        providerCaches.Should().OnlyContain(cache => ReferenceEquals(cache, providerCaches[0]));
        layouts.Should().OnlyContain(layout => ReferenceEquals(layout, layouts[0]));
        loadCounts["shared-provider"].Should().Be(1);

        var isolatedCache = registry.Get("isolated-provider");
        isolatedCache.Should().NotBeSameAs(providerCaches[0]);
        var isolatedLayout = await isolatedCache.GetAsync();
        isolatedLayout.Should().NotBeSameAs(layouts[0]);
        isolatedLayout!.ProviderName.Should().Be("isolated-provider");
        loadCounts["isolated-provider"].Should().Be(1);
    }

    [Fact]
    public async Task PreCanceledCallerDoesNotStartLayoutLoad()
    {
        var loadCount = 0;
        var cache = new StorageLayoutCache(
            () =>
            {
                loadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout(epoch: 1));
            });
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task> get = () => cache.GetAsync(cancellation.Token);

        await get.Should().ThrowAsync<OperationCanceledException>();
        loadCount.Should().Be(0);
    }

    [Fact]
    public async Task ConcurrentCallersShareOneLayoutLoad()
    {
        var loadCount = 0;
        var load = CreateCompletionSource();
        var cache = new StorageLayoutCache(
            () =>
            {
                loadCount++;
                return load.Task;
            });

        var first = cache.GetAsync();
        var second = cache.GetAsync();

        loadCount.Should().Be(1);
        first.IsCompleted.Should().BeFalse();
        second.IsCompleted.Should().BeFalse();

        var layout = CreateLayout(epoch: 1);
        load.SetResult(layout);

        (await first).Should().BeSameAs(layout);
        (await second).Should().BeSameAs(layout);
        (await cache.GetAsync()).Should().BeSameAs(layout);
        loadCount.Should().Be(1);
    }

    [Fact]
    public async Task CancelingOneWaiterDoesNotCancelOrResetSuccessfulSharedLoad()
    {
        var loadCount = 0;
        var load = CreateCompletionSource();
        var cache = new StorageLayoutCache(
            () =>
            {
                loadCount++;
                return load.Task;
            });
        using var cancellation = new CancellationTokenSource();

        var successfulWaiter = cache.GetAsync();
        var canceledWaiter = cache.GetAsync(cancellation.Token);
        await cancellation.CancelAsync();

        Func<Task> waitForCanceledCaller = async () => await canceledWaiter;
        await waitForCanceledCaller.Should().ThrowAsync<OperationCanceledException>();

        var layout = CreateLayout(epoch: 1);
        load.SetResult(layout);

        (await successfulWaiter).Should().BeSameAs(layout);
        (await cache.GetAsync()).Should().BeSameAs(layout);
        loadCount.Should().Be(1);
    }

    [Fact]
    public async Task FaultedLoadIsResetAndRetried()
    {
        var failure = new InvalidOperationException("Layout load failed.");
        var recovered = CreateLayout(epoch: 2);
        var loadCount = 0;
        var cache = new StorageLayoutCache(
            () => ++loadCount == 1
                ? Task.FromException<StorageLayoutSnapshot?>(failure)
                : Task.FromResult<StorageLayoutSnapshot?>(recovered));

        Func<Task> firstGet = () => cache.GetAsync();

        (await firstGet.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(failure);
        (await cache.GetAsync()).Should().BeSameAs(recovered);
        (await cache.GetAsync()).Should().BeSameAs(recovered);
        loadCount.Should().Be(2);
    }

    [Fact]
    public async Task NullLayoutIsNotCached()
    {
        var initialized = CreateLayout(epoch: 1);
        var loadCount = 0;
        var cache = new StorageLayoutCache(
            () => ++loadCount == 1
                ? Task.FromResult<StorageLayoutSnapshot?>(null)
                : Task.FromResult<StorageLayoutSnapshot?>(initialized));

        (await cache.GetAsync()).Should().BeNull();
        (await cache.GetAsync()).Should().BeSameAs(initialized);
        (await cache.GetAsync()).Should().BeSameAs(initialized);
        loadCount.Should().Be(2);
    }

    [Fact]
    public async Task InvalidatingTheObservedLayoutForcesTheNextCallerToRefresh()
    {
        var firstLayout = CreateLayout(epoch: 1);
        var secondLayout = CreateLayout(epoch: 2);
        var loadCount = 0;
        var cache = new StorageLayoutCache(
            () => Task.FromResult<StorageLayoutSnapshot?>(
                ++loadCount == 1 ? firstLayout : secondLayout));

        (await cache.GetAsync()).Should().BeSameAs(firstLayout);
        (await cache.GetAsync()).Should().BeSameAs(firstLayout);

        cache.Invalidate(firstLayout);

        (await cache.GetAsync()).Should().BeSameAs(secondLayout);
        (await cache.GetAsync()).Should().BeSameAs(secondLayout);
        loadCount.Should().Be(2);
    }

    [Fact]
    public async Task CanceledWaiterObservesAndResetsALateLoadFailure()
    {
        var failure = new InvalidOperationException("Late layout load failure.");
        var recovered = CreateLayout(epoch: 2);
        var firstLoad = new TaskCompletionSource<StorageLayoutSnapshot?>();
        var loadCount = 0;
        var cache = new StorageLayoutCache(
            () => ++loadCount == 1
                ? firstLoad.Task
                : Task.FromResult<StorageLayoutSnapshot?>(recovered));
        using var cancellation = new CancellationTokenSource();

        await Task.Run(async () =>
        {
            var canceledWaiter = cache.GetAsync(cancellation.Token);
            await cancellation.CancelAsync();
            Func<Task> waitForCanceledCaller = async () => await canceledWaiter;
            await waitForCanceledCaller.Should().ThrowAsync<OperationCanceledException>();

            // This completion source deliberately runs continuations inline. The canceled caller
            // has already installed the cache's late-failure observer, so SetException does not
            // return until that observer has reset the failed shared task.
            firstLoad.SetException(failure);

            (await cache.GetAsync()).Should().BeSameAs(recovered);
            loadCount.Should().Be(2);
        });
    }

    [Fact]
    public async Task CanceledWaiterObservesAndResetsALateNullResult()
    {
        var initialized = CreateLayout(epoch: 2);
        var firstLoad = new TaskCompletionSource<StorageLayoutSnapshot?>();
        var loadCount = 0;
        var cache = new StorageLayoutCache(
            () => ++loadCount == 1
                ? firstLoad.Task
                : Task.FromResult<StorageLayoutSnapshot?>(initialized));
        using var cancellation = new CancellationTokenSource();

        await Task.Run(async () =>
        {
            var canceledWaiter = cache.GetAsync(cancellation.Token);
            await cancellation.CancelAsync();
            Func<Task> waitForCanceledCaller = async () => await canceledWaiter;
            await waitForCanceledCaller.Should().ThrowAsync<OperationCanceledException>();

            firstLoad.SetResult(null);

            (await cache.GetAsync()).Should().BeSameAs(initialized);
            loadCount.Should().Be(2);
        });
    }

    private static TaskCompletionSource<StorageLayoutSnapshot?> CreateCompletionSource()
    {
        return new TaskCompletionSource<StorageLayoutSnapshot?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static StorageLayoutSnapshot CreateLayout(
        long epoch,
        string providerName = "cache-tests")
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = providerName,
            PartitionCount = 1,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = 1,
            SlotAssignments = [0],
            Epoch = epoch,
        });
    }
}
