using System.Collections.Concurrent;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageRoutingExecutionTests
{
    private const string ProviderName = "routing-execution";

    [Fact]
    public async Task DuplicateAssignmentsFanOutToEachDistinctOwnerOnce()
    {
        var layout = CreateLayout(epoch: 1, 0, 0, 1, 1, 0, 1);
        var firstId = CreateGrainId("first");
        var secondId = CreateGrainId("second");
        var first = new ControlledPartition(_ => Task.FromResult(new[] { firstId }));
        var second = new ControlledPartition(_ => Task.FromResult(new[] { secondId }));
        var client = CreateClient(
            new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(layout)),
            new Dictionary<int, ControlledPartition>
            {
                [0] = first,
                [1] = second,
            });

        var result = await ExecuteFindAsync(client);

        first.FindCallCount.Should().Be(1);
        second.FindCallCount.Should().Be(1);
        first.UnboundedFindCallCount.Should().Be(0);
        second.UnboundedFindCallCount.Should().Be(0);
        first.ObservedEpochs.Should().Equal(1);
        second.ObservedEpochs.Should().Equal(1);
        result.Should().BeEquivalentTo([firstId, secondId]);
    }

    [Fact]
    public async Task RouteMismatchDiscardsPartialResultsAndRetriesTheWholeQuery()
    {
        var firstLayout = CreateLayout(epoch: 1, 0, 1);
        var refreshedLayout = CreateLayout(epoch: 2, 0, 2);
        var staleId = CreateGrainId("stale");
        var refreshedFirstId = CreateGrainId("refreshed-first");
        var refreshedSecondId = CreateGrainId("refreshed-second");
        var loadCount = 0;
        var cache = new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(
            Interlocked.Increment(ref loadCount) == 1 ? firstLayout : refreshedLayout));
        var first = new ControlledPartition(query => Task.FromResult(
            query.Epoch == 1 ? new[] { staleId } : new[] { refreshedFirstId }));
        var staleOwner = new ControlledPartition(query => Task.FromException<GrainId[]>(
            CreateMismatch(query.Epoch, currentEpoch: 2, requestedPartition: 1, currentOwner: 2)));
        var newOwner = new ControlledPartition(_ => Task.FromResult(new[] { refreshedSecondId }));
        var client = CreateClient(
            cache,
            new Dictionary<int, ControlledPartition>
            {
                [0] = first,
                [1] = staleOwner,
                [2] = newOwner,
            });

        var result = await ExecuteFindAsync(client);

        loadCount.Should().Be(2);
        first.ObservedEpochs.Should().Equal(1, 2);
        staleOwner.ObservedEpochs.Should().Equal(1);
        newOwner.ObservedEpochs.Should().Equal(2);
        result.Should().BeEquivalentTo([refreshedFirstId, refreshedSecondId]);
        result.Should().NotContain(staleId);
    }

    [Fact]
    public async Task RouteMismatchAlongsideARealFailureDoesNotRetryOrHideTheFailure()
    {
        var layout = CreateLayout(epoch: 1, 0, 1);
        var loadCount = 0;
        var cache = new StorageLayoutCache(() =>
        {
            Interlocked.Increment(ref loadCount);
            return Task.FromResult<StorageLayoutSnapshot?>(layout);
        });
        var mismatch = new ControlledPartition(query => Task.FromException<GrainId[]>(
            CreateMismatch(query.Epoch, currentEpoch: 2, requestedPartition: 0, currentOwner: 2)));
        var failure = new ControlledPartition(_ => Task.FromException<GrainId[]>(
            new InvalidOperationException("authoritative partition failure")));
        var client = CreateClient(
            cache,
            new Dictionary<int, ControlledPartition>
            {
                [0] = mismatch,
                [1] = failure,
            });

        Func<Task> execute = async () => await ExecuteFindAsync(client);

        await execute.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("authoritative partition failure");
        loadCount.Should().Be(1);
        mismatch.FindCallCount.Should().Be(1);
        failure.FindCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RouteMismatchAlongsidePartitionCancellationDoesNotRetryOrHideCancellation()
    {
        var layout = CreateLayout(epoch: 1, 0, 1);
        var loadCount = 0;
        var cache = new StorageLayoutCache(() =>
        {
            Interlocked.Increment(ref loadCount);
            return Task.FromResult<StorageLayoutSnapshot?>(layout);
        });
        var mismatch = new ControlledPartition(query => Task.FromException<GrainId[]>(
            CreateMismatch(query.Epoch, currentEpoch: 2, requestedPartition: 0, currentOwner: 2)));
        var cancellation = new CancellationToken(canceled: true);
        var canceled = new ControlledPartition(
            _ => Task.FromCanceled<GrainId[]>(cancellation));
        var client = CreateClient(
            cache,
            new Dictionary<int, ControlledPartition>
            {
                [0] = mismatch,
                [1] = canceled,
            });

        Func<Task> execute = async () => await ExecuteFindAsync(client);

        await execute.Should().ThrowAsync<OperationCanceledException>();
        loadCount.Should().Be(1);
        mismatch.FindCallCount.Should().Be(1);
        canceled.FindCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RealPartitionFailureRemainsAuthoritativeAlongsidePartitionCancellation()
    {
        var layout = CreateLayout(epoch: 1, 0, 1);
        var failure = new InvalidOperationException("authoritative partition failure");
        var failed = new ControlledPartition(_ => Task.FromException<GrainId[]>(failure));
        var cancellation = new CancellationToken(canceled: true);
        var canceled = new ControlledPartition(
            _ => Task.FromCanceled<GrainId[]>(cancellation));
        var client = CreateClient(
            new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(layout)),
            new Dictionary<int, ControlledPartition>
            {
                [0] = failed,
                [1] = canceled,
            });

        Func<Task> execute = async () => await ExecuteFindAsync(client);

        (await execute.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(failure);
        failed.FindCallCount.Should().Be(1);
        canceled.FindCallCount.Should().Be(1);
    }

    [Fact]
    public async Task SynchronousPartitionFailureStillObservesEveryStartedPartitionCall()
    {
        var layout = CreateLayout(epoch: 1, 0, 1);
        var blocked = new TaskCompletionSource<GrainId[]>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var first = new ControlledPartition(_ => blocked.Task);
        var failure = new InvalidOperationException("synchronous partition failure");
        var second = new ControlledPartition(_ => throw failure);
        var client = CreateClient(
            new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(layout)),
            new Dictionary<int, ControlledPartition>
            {
                [0] = first,
                [1] = second,
            });

        var execution = ExecuteFindAsync(client);

        first.FindCallCount.Should().Be(1);
        second.FindCallCount.Should().Be(1);
        execution.IsCompleted.Should().BeFalse();
        blocked.SetResult([]);
        Func<Task> waitForExecution = async () => await execution;
        (await waitForExecution.Should().ThrowAsync<InvalidOperationException>())
            .Which.Should().BeSameAs(failure);
    }

    [Fact]
    public async Task ASecondRouteMismatchAfterTheSingleRetryIsSurfaced()
    {
        var firstLayout = CreateLayout(epoch: 1, 0);
        var secondLayout = CreateLayout(epoch: 2, 0);
        var loadCount = 0;
        var cache = new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(
            Interlocked.Increment(ref loadCount) == 1 ? firstLayout : secondLayout));
        var partition = new ControlledPartition(query => Task.FromException<GrainId[]>(
            CreateMismatch(
                query.Epoch,
                currentEpoch: query.Epoch + 1,
                requestedPartition: 0,
                currentOwner: 0)));
        var client = CreateClient(
            cache,
            new Dictionary<int, ControlledPartition> { [0] = partition });

        Func<Task> execute = async () => await ExecuteFindAsync(client);

        var mismatch = (await execute.Should().ThrowAsync<StorageRouteMismatchException>()).Which;
        mismatch.ExpectedEpoch.Should().Be(2);
        mismatch.CurrentEpoch.Should().Be(3);
        loadCount.Should().Be(2);
        partition.ObservedEpochs.Should().Equal(1, 2);
    }

    [Fact]
    public async Task ConcurrentMismatchesShareOneLayoutRefresh()
    {
        var firstLayout = CreateLayout(epoch: 1, 0);
        var secondLayout = CreateLayout(epoch: 2, 0);
        var staleCallsArrived = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var staleFailure = new TaskCompletionSource<GrainId[]>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var refreshStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var refresh = new TaskCompletionSource<StorageLayoutSnapshot?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var loadCount = 0;
        var staleCallCount = 0;
        var cache = new StorageLayoutCache(() =>
        {
            var currentLoad = Interlocked.Increment(ref loadCount);
            if (currentLoad == 1)
            {
                return Task.FromResult<StorageLayoutSnapshot?>(firstLayout);
            }

            refreshStarted.TrySetResult();
            return refresh.Task;
        });
        var partition = new ControlledPartition(query =>
        {
            if (query.Epoch == 1)
            {
                if (Interlocked.Increment(ref staleCallCount) == 2)
                {
                    staleCallsArrived.TrySetResult();
                }

                return staleFailure.Task;
            }

            return Task.FromResult(Array.Empty<GrainId>());
        });
        var client = CreateClient(
            cache,
            new Dictionary<int, ControlledPartition> { [0] = partition });

        var first = ExecuteFindAsync(client);
        var second = ExecuteFindAsync(client);
        await staleCallsArrived.Task;
        staleFailure.SetException(CreateMismatch(1, 2, requestedPartition: 0, currentOwner: 0));
        await refreshStarted.Task;
        refresh.SetResult(secondLayout);
        await Task.WhenAll(first, second);

        loadCount.Should().Be(2);
        partition.ObservedEpochs.Count(epoch => epoch == 1).Should().Be(2);
        partition.ObservedEpochs.Count(epoch => epoch == 2).Should().Be(2);
    }

    [Fact]
    public async Task CancellationDuringRefreshLeavesTheSharedLayoutUsable()
    {
        var firstLayout = CreateLayout(epoch: 1, 0);
        var secondLayout = CreateLayout(epoch: 2, 0);
        var refreshStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var refresh = new TaskCompletionSource<StorageLayoutSnapshot?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var loadCount = 0;
        var cache = new StorageLayoutCache(() =>
        {
            if (Interlocked.Increment(ref loadCount) == 1)
            {
                return Task.FromResult<StorageLayoutSnapshot?>(firstLayout);
            }

            refreshStarted.TrySetResult();
            return refresh.Task;
        });
        var successfulId = CreateGrainId("after-cancellation");
        var partition = new ControlledPartition(query => query.Epoch == 1
            ? Task.FromException<GrainId[]>(
                CreateMismatch(1, 2, requestedPartition: 0, currentOwner: 0))
            : Task.FromResult(new[] { successfulId }));
        var client = CreateClient(
            cache,
            new Dictionary<int, ControlledPartition> { [0] = partition });
        using var cancellation = new CancellationTokenSource();

        var canceled = ExecuteFindAsync(client, cancellation.Token);
        await refreshStarted.Task;
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await canceled);
        refresh.SetResult(secondLayout);

        var result = await ExecuteFindAsync(client);

        loadCount.Should().Be(2);
        result.Should().ContainSingle().Which.Should().Be(successfulId);
        partition.ObservedEpochs.Should().Equal(1, 2);
    }

    private static SearchableStorageClient CreateClient(
        StorageLayoutCache cache,
        IReadOnlyDictionary<int, ControlledPartition> partitions)
    {
        return new SearchableStorageClient(
            ProviderName,
            cache,
            owner => partitions[owner]);
    }

    private static Task<IReadOnlyList<GrainId>> ExecuteFindAsync(
        SearchableStorageClient client,
        CancellationToken cancellationToken = default)
    {
        return client.FindAsync<RoutingState, string>(
            "state",
            state => state.Value,
            "match",
            cancellationToken);
    }

    private static StorageLayoutSnapshot CreateLayout(long epoch, params int[] assignments)
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = ProviderName,
            PartitionCount = assignments.Distinct().Count(),
            VirtualSlotCount = assignments.Length,
            SlotAssignments = assignments,
            Epoch = epoch,
        });
    }

    private static StorageRouteMismatchException CreateMismatch(
        long expectedEpoch,
        long currentEpoch,
        int requestedPartition,
        int currentOwner)
    {
        return new StorageRouteMismatchException(
            expectedEpoch,
            currentEpoch,
            requestedPartition,
            slot: 0,
            currentOwner);
    }

    private static GrainId CreateGrainId(string key)
    {
        return GrainId.Create("routing-execution", key);
    }

    private sealed class RoutingState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Value { get; init; } = string.Empty;
    }

    private sealed class ControlledPartition : StoragePartitionGrainMovementTestDouble, IStoragePartitionGrain
    {
        private readonly Func<RoutedPartitionQueryPageRequest, Task<GrainId[]>> _find;
        private readonly ConcurrentQueue<long> _observedEpochs = new();
        private int _findCallCount;
        private int _unboundedFindCallCount;

        public ControlledPartition(Func<RoutedPartitionQueryPageRequest, Task<GrainId[]>> find)
        {
            _find = find;
        }

        public int FindCallCount => Volatile.Read(ref _findCallCount);

        public int UnboundedFindCallCount => Volatile.Read(ref _unboundedFindCallCount);

        public long[] ObservedEpochs => _observedEpochs.ToArray();

        public Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query)
        {
            Interlocked.Increment(ref _unboundedFindCallCount);
            throw new NotSupportedException("The unbounded exact RPC must not be used.");
        }

        public Task<StorageReadResult> ReadAsync(string recordKey) => throw new NotSupportedException();

        public Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request) =>
            throw new NotSupportedException();

        public Task<string> WriteAsync(StorageWriteRequest request) => throw new NotSupportedException();

        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request) =>
            throw new NotSupportedException();

        public Task ClearAsync(StorageClearRequest request) => throw new NotSupportedException();

        public Task ClearRoutedAsync(RoutedStorageClearRequest request) => throw new NotSupportedException();

        public Task<GrainId[]> FindAsync(ExactIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> RangeAsync(RangeIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query) =>
            throw new NotSupportedException();

        public Task<GrainId[]> QueryAsync(PartitionQueryPlan query) => throw new NotSupportedException();

        public Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query) =>
            throw new NotSupportedException();

        public Task<PartitionQueryPageResult> QueryPageRoutedAsync(
            RoutedPartitionQueryPageRequest request)
        {
            Interlocked.Increment(ref _findCallCount);
            _observedEpochs.Enqueue(request.Epoch);
            return CreatePageResultAsync(_find(request), request);
        }

        public Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
            RoutedPartitionDistinctFacetPageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
            RoutedPartitionFacetCandidatePageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
            RoutedPartitionFacetCountSliceRequest request) => throw new NotSupportedException();

        private static async Task<PartitionQueryPageResult> CreatePageResultAsync(
            Task<GrainId[]> task,
            RoutedPartitionQueryPageRequest request)
        {
            var items = (await task)
                .Where(item => !request.HasAfter
                    || GrainIdCanonicalOrder.Compare(item, request.After) > 0)
                .Distinct(GrainIdCanonicalOrder.EqualityComparer)
                .Order(GrainIdCanonicalOrder.Comparer)
                .ToArray();
            return new PartitionQueryPageResult
            {
                Items = items,
                Exhausted = true,
                StopReason = PartitionQueryPageStopReason.Exhausted,
                Work = new PartitionQueryPageWork(),
                ItemByteCount = items.Sum(GrainIdCanonicalOrder.GetEncodedLength),
                ProtocolVersion = request.ProtocolVersion,
                OrderingVersion = request.OrderingVersion,
                WorkPolicyVersion = request.WorkPolicyVersion,
                ResponseFamily = request.ResponseFamily,
                Epoch = request.Epoch,
                QueryFingerprint = [.. request.QueryFingerprint],
                LayoutFormatVersion = request.LayoutFormatVersion,
                LayoutFingerprint = [.. request.LayoutFingerprint],
            };
        }

        public Task CompactAsync() => throw new NotSupportedException();

        public Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync() =>
            throw new NotSupportedException();
    }
}
