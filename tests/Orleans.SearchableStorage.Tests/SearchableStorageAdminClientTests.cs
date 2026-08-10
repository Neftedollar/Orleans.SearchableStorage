using AwesomeAssertions;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageAdminClientTests
{
    [Fact]
    public void ConstructorRejectsNullLayoutCache()
    {
        Action create = () => _ = new SearchableStorageAdminClient(layoutCache: null!);

        create.Should().Throw<ArgumentNullException>()
            .WithParameterName("layoutCache");
    }

    [Fact]
    public async Task AbsentLayoutIsReturnedAsNullAndReloaded()
    {
        var initialized = CreateLayout([0, 1], epoch: 1, initialPartitionCount: 2);
        var loadCount = 0;
        var client = new SearchableStorageAdminClient(new StorageLayoutCache(
            () => ++loadCount == 1
                ? Task.FromResult<StorageLayoutSnapshot?>(null)
                : Task.FromResult<StorageLayoutSnapshot?>(initialized)));

        (await client.GetLayoutAsync()).Should().BeNull();
        var result = await client.GetLayoutAsync();

        result.Should().NotBeNull();
        result!.InitialPartitionCount.Should().Be(2);
        loadCount.Should().Be(2);
    }

    [Fact]
    public async Task LayoutSummaryCountsSlotsAndSortsPhysicalOwners()
    {
        var snapshot = CreateLayout(
            [2, 0, 2, 1, 2, 0],
            epoch: 7,
            initialPartitionCount: 3);
        var client = new SearchableStorageAdminClient(new StorageLayoutCache(
            () => Task.FromResult<StorageLayoutSnapshot?>(snapshot)));

        var result = await client.GetLayoutAsync();

        result.Should().NotBeNull();
        result!.Epoch.Should().Be(7);
        result.InitialPartitionCount.Should().Be(3);
        result.VirtualSlotCount.Should().Be(6);
        result.Partitions.Select(static partition => partition.PartitionIndex)
            .Should().Equal(0, 1, 2);
        result.Partitions.Select(static partition => partition.SlotCount)
            .Should().Equal(2, 1, 3);
    }

    [Fact]
    public async Task PreCanceledCallerDoesNotStartLayoutRead()
    {
        var loadCount = 0;
        var client = new SearchableStorageAdminClient(new StorageLayoutCache(
            () =>
            {
                loadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(
                    CreateLayout([0], epoch: 1, initialPartitionCount: 1));
            }));
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task> get = () => client.GetLayoutAsync(cancellation.Token);

        await get.Should().ThrowAsync<OperationCanceledException>();
        loadCount.Should().Be(0);
    }

    [Fact]
    public async Task CancelingOneCallerDoesNotCancelOrEvictSuccessfulSharedRead()
    {
        var load = new TaskCompletionSource<StorageLayoutSnapshot?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var loadCount = 0;
        var client = new SearchableStorageAdminClient(new StorageLayoutCache(
            () =>
            {
                loadCount++;
                return load.Task;
            }));
        using var cancellation = new CancellationTokenSource();

        var successfulCaller = client.GetLayoutAsync();
        var canceledCaller = client.GetLayoutAsync(cancellation.Token);
        await cancellation.CancelAsync();
        Func<Task> waitForCanceledCaller = async () => await canceledCaller;
        await waitForCanceledCaller.Should().ThrowAsync<OperationCanceledException>();

        load.SetResult(CreateLayout([0, 1], epoch: 1, initialPartitionCount: 2));

        (await successfulCaller).Should().NotBeNull();
        (await client.GetLayoutAsync()).Should().NotBeNull();
        loadCount.Should().Be(1);
    }

    [Fact]
    public async Task PublicSummaryCannotMutateCachedSlotAssignments()
    {
        var snapshot = CreateLayout([1, 0, 1, 0], epoch: 3, initialPartitionCount: 2);
        var client = new SearchableStorageAdminClient(new StorageLayoutCache(
            () => Task.FromResult<StorageLayoutSnapshot?>(snapshot)));

        var first = await client.GetLayoutAsync();
        first!.Partitions.Should().NotBeOfType<SearchableStoragePartitionLayout[]>();

        var exposedSummary = first.Partitions.Should()
            .BeAssignableTo<IList<SearchableStoragePartitionLayout>>()
            .Subject;
        exposedSummary.IsReadOnly.Should().BeTrue();

        var replace = () => exposedSummary[0] = new SearchableStoragePartitionLayout
        {
            PartitionIndex = 99,
            SlotCount = 99,
        };
        replace.Should().Throw<NotSupportedException>();

        var second = await client.GetLayoutAsync();

        second!.Partitions.Select(static partition => partition.PartitionIndex)
            .Should().Equal(0, 1);
        second.Partitions.Select(static partition => partition.SlotCount)
            .Should().Equal(2, 2);
        snapshot.CopySlotAssignments().Should().Equal(1, 0, 1, 0);
        typeof(SearchableStorageLayout).GetProperties()
            .Should().NotContain(property => property.Name.Contains("Assignment", StringComparison.Ordinal));
    }

    private static StorageLayoutSnapshot CreateLayout(
        int[] assignments,
        long epoch,
        int initialPartitionCount)
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = "admin-tests",
            PartitionCount = initialPartitionCount,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = assignments.Length,
            SlotAssignments = assignments,
            Epoch = epoch,
        });
    }
}
