using System.Collections.Concurrent;
using System.Text.Json;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;
using Orleans.Serialization.Activators;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableGrainStorageRoutingExecutionTests
{
    private const string ProviderName = "point-routing-execution";

    [Fact]
    public async Task ReadMismatchRefreshesTheLayoutAndRetriesTheAuthoritativeOwnerOnce()
    {
        var context = CreateContext();
        var expected = new RoutedState { Value = "authoritative" };
        context.OldOwner.Read = request => Task.FromException<StorageReadResult>(
            CreateMismatch(request, currentEpoch: 2, currentOwner: context.NewOwnerIndex));
        context.NewOwner.Read = _ => Task.FromResult(new StorageReadResult
        {
            Found = true,
            Payload = TestSerializer.Instance.Serialize(expected).ToArray(),
            ETag = "etag-2",
        });
        var state = new GrainState<RoutedState>();

        await context.Storage.ReadStateAsync("state", context.GrainId, state);

        AssertSingleRefresh(context);
        context.OldOwner.ReadRequests.Should().ContainSingle()
            .Which.Should().Match<RoutedStorageReadRequest>(request =>
                request.GrainId.Equals(context.GrainId)
                && request.Slot == context.Slot
                && request.Epoch == 1);
        context.NewOwner.ReadRequests.Should().ContainSingle()
            .Which.Epoch.Should().Be(2);
        state.State.Value.Should().Be("authoritative");
        state.ETag.Should().Be("etag-2");
        state.RecordExists.Should().BeTrue();
    }

    [Fact]
    public async Task WriteMismatchRefreshesTheLayoutAndPreservesTheConditionalWrite()
    {
        var context = CreateContext();
        context.OldOwner.Write = request => Task.FromException<string>(
            CreateMismatch(request, currentEpoch: 2, currentOwner: context.NewOwnerIndex));
        context.NewOwner.Write = _ => Task.FromResult("etag-2");
        var state = new GrainState<RoutedState>
        {
            State = new RoutedState { Value = "updated" },
            ETag = "etag-1",
            RecordExists = true,
        };

        await context.Storage.WriteStateAsync("state", context.GrainId, state);

        AssertSingleRefresh(context);
        var stale = context.OldOwner.WriteRequests.Should().ContainSingle().Which;
        var authoritative = context.NewOwner.WriteRequests.Should().ContainSingle().Which;
        stale.Slot.Should().Be(context.Slot);
        stale.Epoch.Should().Be(1);
        authoritative.Slot.Should().Be(context.Slot);
        authoritative.Epoch.Should().Be(2);
        authoritative.Request.ExpectedETag.Should().Be("etag-1");
        authoritative.Request.GrainId.Should().Be(context.GrainId);
        state.ETag.Should().Be("etag-2");
        state.RecordExists.Should().BeTrue();
    }

    [Fact]
    public async Task ClearMismatchRefreshesTheLayoutAndPreservesTheConditionalClear()
    {
        var context = CreateContext();
        context.OldOwner.Clear = request => Task.FromException(
            CreateMismatch(request, currentEpoch: 2, currentOwner: context.NewOwnerIndex));
        context.NewOwner.Clear = _ => Task.CompletedTask;
        var state = new GrainState<RoutedState>
        {
            State = new RoutedState { Value = "removed" },
            ETag = "etag-1",
            RecordExists = true,
        };

        await context.Storage.ClearStateAsync("state", context.GrainId, state);

        AssertSingleRefresh(context);
        var authoritative = context.NewOwner.ClearRequests.Should().ContainSingle().Which;
        authoritative.Slot.Should().Be(context.Slot);
        authoritative.Epoch.Should().Be(2);
        authoritative.GrainId.Should().Be(context.GrainId);
        authoritative.Request.ExpectedETag.Should().Be("etag-1");
        state.State.Should().NotBeNull();
        state.State.Value.Should().BeEmpty();
        state.ETag.Should().BeNull();
        state.RecordExists.Should().BeFalse();
    }

    [Fact]
    public async Task ASecondWriteMismatchIsSurfacedWithoutChangingCallerMetadata()
    {
        var context = CreateContext();
        context.OldOwner.Write = request => Task.FromException<string>(
            CreateMismatch(request, currentEpoch: 2, currentOwner: context.NewOwnerIndex));
        context.NewOwner.Write = request => Task.FromException<string>(
            CreateMismatch(request, currentEpoch: 3, currentOwner: context.NewOwnerIndex));
        var original = new RoutedState { Value = "unchanged" };
        var state = new GrainState<RoutedState>
        {
            State = original,
            ETag = "etag-1",
            RecordExists = true,
        };

        Func<Task> write = () => context.Storage.WriteStateAsync("state", context.GrainId, state);

        var mismatch = (await write.Should().ThrowAsync<StorageRouteMismatchException>()).Which;
        mismatch.ExpectedEpoch.Should().Be(2);
        mismatch.CurrentEpoch.Should().Be(3);
        AssertSingleRefresh(context);
        context.OldOwner.WriteRequests.Should().ContainSingle();
        context.NewOwner.WriteRequests.Should().ContainSingle();
        state.State.Should().BeSameAs(original);
        state.ETag.Should().Be("etag-1");
        state.RecordExists.Should().BeTrue();
    }

    [Fact]
    public async Task ASecondClearMismatchIsSurfacedWithoutChangingCallerMetadata()
    {
        var context = CreateContext();
        context.OldOwner.Clear = request => Task.FromException(
            CreateMismatch(request, currentEpoch: 2, currentOwner: context.NewOwnerIndex));
        context.NewOwner.Clear = request => Task.FromException(
            CreateMismatch(request, currentEpoch: 3, currentOwner: context.NewOwnerIndex));
        var original = new RoutedState { Value = "unchanged" };
        var state = new GrainState<RoutedState>
        {
            State = original,
            ETag = "etag-1",
            RecordExists = true,
        };

        Func<Task> clear = () => context.Storage.ClearStateAsync("state", context.GrainId, state);

        var mismatch = (await clear.Should().ThrowAsync<StorageRouteMismatchException>()).Which;
        mismatch.ExpectedEpoch.Should().Be(2);
        mismatch.CurrentEpoch.Should().Be(3);
        AssertSingleRefresh(context);
        context.OldOwner.ClearRequests.Should().ContainSingle();
        context.NewOwner.ClearRequests.Should().ContainSingle();
        state.State.Should().BeSameAs(original);
        state.ETag.Should().Be("etag-1");
        state.RecordExists.Should().BeTrue();
    }

    private static RoutingContext CreateContext()
    {
        var grainId = GrainId.Create("point-routing", Guid.NewGuid().ToString("N"));
        var initial = CreateLayout(epoch: 1, 0, 1);
        var slot = StorageLayout.GetSlot(grainId, initial.VirtualSlotCount);
        var refreshed = CreateLayout(epoch: 2, 1, 0);
        var oldOwnerIndex = initial.GetOwner(slot);
        var newOwnerIndex = refreshed.GetOwner(slot);
        var oldOwner = new ControlledPartition();
        var newOwner = new ControlledPartition();
        var loadCount = 0;
        var cache = new StorageLayoutCache(() => Task.FromResult<StorageLayoutSnapshot?>(
            Interlocked.Increment(ref loadCount) == 1 ? initial : refreshed));
        var partitions = new Dictionary<int, ControlledPartition>
        {
            [oldOwnerIndex] = oldOwner,
            [newOwnerIndex] = newOwner,
        };
        var storage = new SearchableGrainStorage(
            ProviderName,
            new SearchableStorageOptions
            {
                PartitionCount = 2,
                VirtualSlotTargetCount = 2,
                GrainStorageSerializer = TestSerializer.Instance,
            },
            TestActivatorProvider.Instance,
            cache,
            owner => partitions[owner]);
        return new RoutingContext(
            storage,
            grainId,
            slot,
            oldOwnerIndex,
            newOwnerIndex,
            oldOwner,
            newOwner,
            () => Volatile.Read(ref loadCount));
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
        RoutedStorageReadRequest request,
        long currentEpoch,
        int currentOwner)
    {
        return new StorageRouteMismatchException(
            request.Epoch,
            currentEpoch,
            requestedPartition: currentOwner == 0 ? 1 : 0,
            request.Slot,
            currentOwner);
    }

    private static StorageRouteMismatchException CreateMismatch(
        RoutedStorageWriteRequest request,
        long currentEpoch,
        int currentOwner)
    {
        return new StorageRouteMismatchException(
            request.Epoch,
            currentEpoch,
            requestedPartition: currentOwner == 0 ? 1 : 0,
            request.Slot,
            currentOwner);
    }

    private static StorageRouteMismatchException CreateMismatch(
        RoutedStorageClearRequest request,
        long currentEpoch,
        int currentOwner)
    {
        return new StorageRouteMismatchException(
            request.Epoch,
            currentEpoch,
            requestedPartition: currentOwner == 0 ? 1 : 0,
            request.Slot,
            currentOwner);
    }

    private static void AssertSingleRefresh(RoutingContext context)
    {
        context.LayoutLoadCount().Should().Be(2);
    }

    private sealed record RoutingContext(
        SearchableGrainStorage Storage,
        GrainId GrainId,
        int Slot,
        int OldOwnerIndex,
        int NewOwnerIndex,
        ControlledPartition OldOwner,
        ControlledPartition NewOwner,
        Func<int> LayoutLoadCount);

    private sealed class RoutedState
    {
        public string Value { get; set; } = string.Empty;
    }

    private sealed class TestSerializer : IGrainStorageSerializer
    {
        public static TestSerializer Instance { get; } = new();

        public BinaryData Serialize<T>(T input)
        {
            return BinaryData.FromBytes(JsonSerializer.SerializeToUtf8Bytes(input));
        }

        public T Deserialize<T>(BinaryData input)
        {
            return JsonSerializer.Deserialize<T>(input.ToMemory().Span)!;
        }
    }

    private sealed class TestActivatorProvider : IActivatorProvider
    {
        public static TestActivatorProvider Instance { get; } = new();

        public IActivator<T> GetActivator<T>() => TestActivator<T>.Instance;
    }

    private sealed class TestActivator<T> : IActivator<T>
    {
        public static TestActivator<T> Instance { get; } = new();

        public T Create() => Activator.CreateInstance<T>();
    }

    private sealed class ControlledPartition : IStoragePartitionGrain
    {
        public Func<RoutedStorageReadRequest, Task<StorageReadResult>> Read { get; set; } =
            _ => throw new NotSupportedException();

        public Func<RoutedStorageWriteRequest, Task<string>> Write { get; set; } =
            _ => throw new NotSupportedException();

        public Func<RoutedStorageClearRequest, Task> Clear { get; set; } =
            _ => throw new NotSupportedException();

        public ConcurrentQueue<RoutedStorageReadRequest> ReadRequests { get; } = new();

        public ConcurrentQueue<RoutedStorageWriteRequest> WriteRequests { get; } = new();

        public ConcurrentQueue<RoutedStorageClearRequest> ClearRequests { get; } = new();

        public Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request)
        {
            ReadRequests.Enqueue(request);
            return Read(request);
        }

        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request)
        {
            WriteRequests.Enqueue(request);
            return Write(request);
        }

        public Task ClearRoutedAsync(RoutedStorageClearRequest request)
        {
            ClearRequests.Enqueue(request);
            return Clear(request);
        }

        public Task<StorageReadResult> ReadAsync(string recordKey) => throw new NotSupportedException();

        public Task<string> WriteAsync(StorageWriteRequest request) => throw new NotSupportedException();

        public Task ClearAsync(StorageClearRequest request) => throw new NotSupportedException();

        public Task<GrainId[]> FindAsync(ExactIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query) =>
            throw new NotSupportedException();

        public Task<GrainId[]> RangeAsync(RangeIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query) =>
            throw new NotSupportedException();

        public Task<GrainId[]> QueryAsync(PartitionQueryPlan query) => throw new NotSupportedException();

        public Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query) =>
            throw new NotSupportedException();

        public Task<PartitionQueryPageResult> QueryPageRoutedAsync(
            RoutedPartitionQueryPageRequest request) => throw new NotSupportedException();

        public Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
            RoutedPartitionDistinctFacetPageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
            RoutedPartitionFacetCandidatePageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
            RoutedPartitionFacetCountSliceRequest request) => throw new NotSupportedException();

        public Task CompactAsync() => throw new NotSupportedException();

        public Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync() =>
            throw new NotSupportedException();
    }
}
