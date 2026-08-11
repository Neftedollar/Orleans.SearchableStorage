using AwesomeAssertions;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageAdminClientTests
{
    [Fact]
    public async Task ExistingReadOnlyAdminImplementationsGetAnExplicitMovementFailure()
    {
        ISearchableStorageAdminClient client = new ReadOnlyAdminClient();

        var faulted = client.EnableMovementAsync();
        Func<Task> call = async () => await faulted;

        faulted.IsFaulted.Should().BeTrue();
        await call.Should().ThrowAsync<NotSupportedException>()
            .WithMessage("*AddSearchableGrainStorage*");
    }

    [Fact]
    public void AdditiveLayoutMovementPropertiesDoNotBecomeRequiredMembers()
    {
        var required = typeof(System.Runtime.CompilerServices.RequiredMemberAttribute);

        typeof(SearchableStorageLayout)
            .GetProperty(nameof(SearchableStorageLayout.MovementProtocolVersion))!
            .GetCustomAttributes(required, inherit: false).Should().BeEmpty();
        typeof(SearchableStorageLayout)
            .GetProperty(nameof(SearchableStorageLayout.MovementState))!
            .GetCustomAttributes(required, inherit: false).Should().BeEmpty();
        typeof(SearchableStorageLayout)
            .GetProperty(nameof(SearchableStorageLayout.ActiveMove))!
            .GetCustomAttributes(required, inherit: false).Should().BeEmpty();
    }

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

    [Fact]
    public async Task RebalancePlanUsesDeterministicMinimumMoveQuotas()
    {
        var snapshot = CreateMovementLayout([0, 0, 0, 0, 1, 1, 2, 3]);
        var client = CreateCachedClient(snapshot);

        var plan = await client.PlanRebalanceAsync(targetPartitionCount: 3);

        plan.RequiredMoveCount.Should().Be(2);
        plan.ActiveMove.Should().BeNull();
        plan.NextMove.Should().NotBeNull();
        plan.NextMove!.Slot.Should().Be(3);
        plan.NextMove.SourcePartitionIndex.Should().Be(0);
        plan.NextMove.TargetPartitionIndex.Should().Be(1);
    }

    [Fact]
    public async Task RebalanceKeepsAValidRemainderOnTheOwnerWhichAlreadyHasIt()
    {
        var snapshot = CreateMovementLayout([0, 0, 1, 1, 1]);
        var client = CreateCachedClient(snapshot);

        var plan = await client.PlanRebalanceAsync(targetPartitionCount: 2);

        plan.RequiredMoveCount.Should().Be(0,
            "the existing 2/3 split is already balanced and requires no ownership churn");
        plan.NextMove.Should().BeNull();
        plan.ActiveMove.Should().BeNull();
    }

    [Fact]
    public async Task RebalanceTargetIsBoundedByThePersistedVirtualSlotCount()
    {
        var client = CreateCachedClient(CreateMovementLayout([0, 0, 1, 1]));

        var maximum = await client.PlanRebalanceAsync(targetPartitionCount: 4);
        Func<Task> aboveMaximum = async () => await client.PlanRebalanceAsync(
            targetPartitionCount: 5);

        maximum.TargetPartitionCount.Should().Be(4);
        await aboveMaximum.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithParameterName("targetPartitionCount");
    }

    [Fact]
    public async Task LayoutSummaryProjectsItsActiveMoveFromTheSameImmutableSnapshot()
    {
        var snapshot = CreateMovementLayout(
            [0, 0, 1, 1],
            new StorageSlotMoveIntent
            {
                MoveId = Guid.NewGuid(),
                Slot = 0,
                SourceOwner = 0,
                TargetOwner = 1,
                SourceEpoch = 2,
                Phase = SearchableStorageSlotMovePhase.Copying,
                TransferPageRecordLimit = 16,
                TransferPageByteTarget = 4_096,
                ExportedRecordCount = 7,
                ExportedByteCount = 700,
            });
        var client = CreateCachedClient(snapshot);

        var layout = await client.GetLayoutAsync();

        layout.Should().NotBeNull();
        layout!.Epoch.Should().Be(2);
        layout.ActiveMove.Should().NotBeNull();
        layout.ActiveMove!.CurrentEpoch.Should().Be(layout.Epoch);
        layout.ActiveMove.ExportedRecordCount.Should().Be(7);
    }

    [Fact]
    public async Task RebalancePlanSimulatesAPlannedOwnershipCommitWithoutOfferingASecondPlan()
    {
        var snapshot = CreateMovementLayout(
            [0, 0, 1, 1],
            new StorageSlotMoveIntent
            {
                MoveId = Guid.NewGuid(),
                Slot = 0,
                SourceOwner = 0,
                TargetOwner = 2,
                SourceEpoch = 2,
                Phase = SearchableStorageSlotMovePhase.Planned,
                TransferPageRecordLimit = 16,
                TransferPageByteTarget = 4_096,
            });
        var client = CreateCachedClient(snapshot);

        var plan = await client.PlanRebalanceAsync(targetPartitionCount: 2);

        plan.ActiveMove.Should().NotBeNull();
        plan.NextMove.Should().BeNull();
        plan.RequiredMoveCount.Should().Be(2,
            "the active commit moves outside the target owner range and one corrective commit remains");
    }

    [Fact]
    public async Task RebalancePlanDoesNotCountAnAlreadyCommittedOrAbortingOwnershipAgain()
    {
        var moveId = Guid.NewGuid();
        var committed = CreateMovementLayout(
            [1, 0, 1, 1],
            new StorageSlotMoveIntent
            {
                MoveId = moveId,
                Slot = 0,
                SourceOwner = 0,
                TargetOwner = 1,
                SourceEpoch = 2,
                Phase = SearchableStorageSlotMovePhase.OwnershipCommitted,
                TransferPageRecordLimit = 16,
                TransferPageByteTarget = 4_096,
            },
            epoch: 3);
        var aborting = CreateMovementLayout(
            [0, 0, 1, 1],
            new StorageSlotMoveIntent
            {
                MoveId = Guid.NewGuid(),
                Slot = 0,
                SourceOwner = 0,
                TargetOwner = 1,
                SourceEpoch = 2,
                Phase = SearchableStorageSlotMovePhase.Aborting,
                TransferPageRecordLimit = 16,
                TransferPageByteTarget = 4_096,
            });

        var committedPlan = await CreateCachedClient(committed).PlanRebalanceAsync(2);
        var abortingPlan = await CreateCachedClient(aborting).PlanRebalanceAsync(2);

        committedPlan.RequiredMoveCount.Should().Be(1);
        abortingPlan.RequiredMoveCount.Should().Be(0);
        committedPlan.NextMove.Should().BeNull();
        abortingPlan.NextMove.Should().BeNull();
    }

    [Theory]
    [InlineData(SearchableStorageMovementState.Disabled)]
    [InlineData(SearchableStorageMovementState.Enabling)]
    public async Task RebalancePlanRejectsAProtocolWhichIsNotEnabled(
        SearchableStorageMovementState movementState)
    {
        var state = new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = "admin-tests",
            PartitionCount = 2,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = 4,
            SlotAssignments = [0, 1, 0, 1],
            Epoch = 1,
        };
        if (movementState == SearchableStorageMovementState.Enabling)
        {
            state.MovementEnablement = new StorageMovementEnableIntent
            {
                EnablementId = Guid.NewGuid(),
                SourceEpoch = 1,
                PlannedEpoch = 2,
                Owners = [0, 1],
            };
        }

        var client = CreateCachedClient(StorageLayoutSnapshot.FromState(state));

        var plan = () => client.PlanRebalanceAsync(2);

        await plan.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*EnableMovementAsync*");
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

    private static StorageLayoutSnapshot CreateMovementLayout(
        int[] assignments,
        StorageSlotMoveIntent? move = null,
        long epoch = 2)
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = "admin-tests",
            PartitionCount = 2,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = assignments.Length,
            SlotAssignments = assignments,
            Epoch = epoch,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
            MoveIntent = move,
        });
    }

    private static SearchableStorageAdminClient CreateCachedClient(StorageLayoutSnapshot snapshot)
    {
        return new SearchableStorageAdminClient(new StorageLayoutCache(
            () => Task.FromResult<StorageLayoutSnapshot?>(snapshot)));
    }

    private sealed class ReadOnlyAdminClient : ISearchableStorageAdminClient
    {
        public Task<SearchableStorageLayout?> GetLayoutAsync(
            CancellationToken cancellationToken = default) => Task.FromResult<SearchableStorageLayout?>(null);
    }
}
