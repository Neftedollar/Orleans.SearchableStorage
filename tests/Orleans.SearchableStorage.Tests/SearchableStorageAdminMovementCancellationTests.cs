using AwesomeAssertions;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageAdminMovementCancellationTests
{
    [Fact]
    public async Task PreCanceledMovementCallsStartNoLayoutRpc()
    {
        var layout = new ControlledLayoutGrain();
        var client = CreateClient(layout);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var moveId = Guid.NewGuid();
        Func<Task>[] calls =
        [
            async () => await client.EnableMovementAsync(cancellation.Token),
            async () => await client.PlanMoveAsync(0, 1, cancellation.Token),
            async () => await client.GetMoveAsync(cancellation.Token),
            async () => await client.AdvanceMoveAsync(moveId, cancellation.Token),
            async () => await client.ExecuteMoveAsync(moveId, cancellation.Token),
            async () => await client.AbortMoveAsync(moveId, cancellation.Token),
            async () => await client.PlanRebalanceAsync(1, cancellation.Token),
            async () => await client.ExecuteRebalanceAsync(1, cancellation.Token),
        ];

        foreach (var call in calls)
        {
            await call.Should().ThrowAsync<OperationCanceledException>();
        }

        layout.TotalCallCount.Should().Be(0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancelingAnInflightAdvanceObservesItsLateOutcomeAndCanResume(
        bool lateFault)
    {
        var moveId = Guid.NewGuid();
        var pending = new TaskCompletionSource<StorageSlotMoveProgressSnapshot>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var layout = new ControlledLayoutGrain();
        layout.AdvanceMove = _ => layout.AdvanceMoveCallCount == 1
            ? pending.Task
            : Task.FromResult(CreateProgress(moveId, SearchableStorageSlotMovePhase.Completed, epoch: 3));
        var client = CreateClient(layout);
        using var cancellation = new CancellationTokenSource();

        var advance = client.AdvanceMoveAsync(moveId, cancellation.Token);
        await cancellation.CancelAsync();
        Func<Task> wait = async () => await advance;
        await wait.Should().ThrowAsync<OperationCanceledException>();

        if (lateFault)
        {
            pending.SetException(new InvalidOperationException("late participant fault"));
        }
        else
        {
            pending.SetResult(CreateProgress(moveId, SearchableStorageSlotMovePhase.Copying, epoch: 2));
        }

        await Task.Yield();
        var resumed = await client.AdvanceMoveAsync(moveId);

        resumed.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
        layout.AdvanceMoveCallCount.Should().Be(2);
    }

    [Fact]
    public async Task CancelingInflightEnableDoesNotCancelItsDurableIntentAndLaterCallResumes()
    {
        var pending = new TaskCompletionSource<StorageLayoutSnapshot>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var enabled = CreateLayout();
        var layout = new ControlledLayoutGrain
        {
            BeginMovement = () => Task.FromResult(enabled),
        };
        layout.BeginMovement = () => layout.BeginMovementCallCount == 1
            ? pending.Task
            : Task.FromResult(enabled);
        var client = CreateClient(layout);
        using var cancellation = new CancellationTokenSource();

        var enable = client.EnableMovementAsync(cancellation.Token);
        await cancellation.CancelAsync();
        Func<Task> wait = async () => await enable;
        await wait.Should().ThrowAsync<OperationCanceledException>();
        pending.SetResult(enabled);
        await Task.Yield();

        var resumed = await client.EnableMovementAsync();

        resumed.MovementState.Should().Be(SearchableStorageMovementState.Enabled);
        layout.BeginMovementCallCount.Should().Be(2);
    }

    [Fact]
    public async Task CancelingInflightAbortLeavesRollbackResumable()
    {
        var moveId = Guid.NewGuid();
        var pending = new TaskCompletionSource<StorageSlotMoveProgressSnapshot>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var layout = new ControlledLayoutGrain();
        layout.RequestAbort = _ => layout.RequestAbortCallCount == 1
            ? pending.Task
            : Task.FromResult(CreateProgress(moveId, SearchableStorageSlotMovePhase.Aborting, epoch: 2));
        layout.AdvanceMove = _ => Task.FromResult(
            CreateProgress(moveId, SearchableStorageSlotMovePhase.Aborted, epoch: 2));
        var client = CreateClient(layout);
        using var cancellation = new CancellationTokenSource();

        var abort = client.AbortMoveAsync(moveId, cancellation.Token);
        await cancellation.CancelAsync();
        Func<Task> wait = async () => await abort;
        await wait.Should().ThrowAsync<OperationCanceledException>();
        pending.SetResult(CreateProgress(moveId, SearchableStorageSlotMovePhase.Aborting, epoch: 2));
        await Task.Yield();

        var resumed = await client.AbortMoveAsync(moveId);

        resumed.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        resumed.IsComplete.Should().BeTrue();
        layout.RequestAbortCallCount.Should().Be(2);
        layout.AdvanceMoveCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteMoveChecksCancellationBeforeStartingItsNextProtocolTurn()
    {
        var moveId = Guid.NewGuid();
        using var cancellation = new CancellationTokenSource();
        var layout = new ControlledLayoutGrain();
        layout.AdvanceMove = _ =>
        {
            cancellation.Cancel();
            return Task.FromResult(
                CreateProgress(moveId, SearchableStorageSlotMovePhase.Copying, epoch: 2));
        };
        var client = CreateClient(layout);

        Func<Task> execute = async () => await client.ExecuteMoveAsync(moveId, cancellation.Token);

        await execute.Should().ThrowAsync<OperationCanceledException>();
        layout.AdvanceMoveCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ExecuteRebalanceChecksCancellationBeforeExecutingANewlyPlannedMove()
    {
        var moveId = Guid.NewGuid();
        using var cancellation = new CancellationTokenSource();
        var layout = new ControlledLayoutGrain
        {
            GetLayout = _ => Task.FromResult<StorageLayoutSnapshot?>(CreateLayout([0, 1])),
        };
        layout.PlanMove = _ =>
        {
            cancellation.Cancel();
            return Task.FromResult(
                CreateProgress(moveId, SearchableStorageSlotMovePhase.Planned, epoch: 2));
        };
        var client = CreateClient(layout);

        Func<Task> execute = async () => await client.ExecuteRebalanceAsync(1, cancellation.Token);

        await execute.Should().ThrowAsync<OperationCanceledException>();
        layout.GetLayoutCallCount.Should().Be(1);
        layout.PlanMoveCallCount.Should().Be(1);
        layout.AdvanceMoveCallCount.Should().Be(0);
    }

    private static SearchableStorageAdminClient CreateClient(ControlledLayoutGrain layout)
    {
        return new SearchableStorageAdminClient(
            layout,
            StorageLayout.CreateIdentity(ControlledLayoutGrain.ProviderName, partitionCount: 2),
            new SearchableStorageMovementOptions());
    }

    private static StorageLayoutSnapshot CreateLayout(int[]? assignments = null)
    {
        assignments ??= [0, 1];
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = ControlledLayoutGrain.ProviderName,
            PartitionCount = 2,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = assignments.Length,
            SlotAssignments = assignments,
            Epoch = 2,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
        });
    }

    private static StorageSlotMoveProgressSnapshot CreateProgress(
        Guid moveId,
        SearchableStorageSlotMovePhase phase,
        long epoch)
    {
        return new StorageSlotMoveProgressSnapshot
        {
            Intent = new StorageSlotMoveIntent
            {
                MoveId = moveId,
                Slot = 0,
                SourceOwner = 0,
                TargetOwner = 1,
                SourceEpoch = 2,
                Phase = phase,
                TransferPageRecordLimit = 16,
                TransferPageByteTarget = 4_096,
            },
            CurrentEpoch = epoch,
        };
    }

    private sealed class ControlledLayoutGrain : StorageLayoutGrainMovementTestDouble, IStorageLayoutGrain
    {
        public const string ProviderName = "movement-cancellation";

        public Func<StorageLayoutIdentity, Task<StorageLayoutSnapshot?>> GetLayout { get; init; } =
            _ => Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());

        public Func<Task<StorageLayoutSnapshot>> BeginMovement { get; set; } =
            () => Task.FromException<StorageLayoutSnapshot>(new NotSupportedException());

        public Func<StorageSlotMovePlanRequest, Task<StorageSlotMoveProgressSnapshot>> PlanMove { get; set; } =
            _ => Task.FromException<StorageSlotMoveProgressSnapshot>(new NotSupportedException());

        public Func<StorageSlotMoveCommand, Task<StorageSlotMoveProgressSnapshot>> AdvanceMove { get; set; } =
            _ => Task.FromException<StorageSlotMoveProgressSnapshot>(new NotSupportedException());

        public Func<StorageSlotMoveCommand, Task<StorageSlotMoveProgressSnapshot>> RequestAbort { get; set; } =
            _ => Task.FromException<StorageSlotMoveProgressSnapshot>(new NotSupportedException());

        public int GetLayoutCallCount { get; private set; }

        public int BeginMovementCallCount { get; private set; }

        public int PlanMoveCallCount { get; private set; }

        public int AdvanceMoveCallCount { get; private set; }

        public int RequestAbortCallCount { get; private set; }

        public int TotalCallCount => GetLayoutCallCount
            + BeginMovementCallCount
            + PlanMoveCallCount
            + AdvanceMoveCallCount
            + RequestAbortCallCount;

        public Task<StorageLayoutSnapshot?> GetLayoutAsync(StorageLayoutIdentity identity)
        {
            GetLayoutCallCount++;
            return GetLayout(identity);
        }

        public new Task<StorageLayoutSnapshot> BeginMovementEnablementAsync()
        {
            BeginMovementCallCount++;
            return BeginMovement();
        }

        public new Task<StorageLayoutSnapshot> AdvanceMovementEnablementAsync(Guid enablementId) =>
            Task.FromException<StorageLayoutSnapshot>(new NotSupportedException());

        public new Task<StorageSlotMoveProgressSnapshot> PlanMoveAsync(StorageSlotMovePlanRequest request)
        {
            PlanMoveCallCount++;
            return PlanMove(request);
        }

        public new Task<StorageSlotMoveProgressSnapshot?> GetMoveProgressAsync() =>
            Task.FromException<StorageSlotMoveProgressSnapshot?>(new NotSupportedException());

        public new Task<StorageSlotMoveProgressSnapshot> AdvanceMoveAsync(StorageSlotMoveCommand command)
        {
            AdvanceMoveCallCount++;
            return AdvanceMove(command);
        }

        public new Task<StorageSlotMoveProgressSnapshot> RequestMoveAbortAsync(StorageSlotMoveCommand command)
        {
            RequestAbortCallCount++;
            return RequestAbort(command);
        }
    }
}
