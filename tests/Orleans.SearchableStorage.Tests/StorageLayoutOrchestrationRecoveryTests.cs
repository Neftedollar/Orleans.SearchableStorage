using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageLayoutOrchestrationRecoveryTests
{
    public static IEnumerable<object[]> LayoutFaultCases()
    {
        var stages = new[]
        {
            LayoutWriteStage.EnableIntent,
            LayoutWriteStage.EnableOwnerProgress,
            LayoutWriteStage.EnableFinalPublish,
            LayoutWriteStage.MoveIntent,
            LayoutWriteStage.SourceFrozen,
            LayoutWriteStage.TargetVersionFenced,
            LayoutWriteStage.Copying,
            LayoutWriteStage.CopyComplete,
            LayoutWriteStage.OwnershipCommitted,
            LayoutWriteStage.SourceVisibilityFenced,
            LayoutWriteStage.TargetEnabled,
            LayoutWriteStage.DeletingSource,
            LayoutWriteStage.Retiring,
            LayoutWriteStage.TerminalReceipt,
        };
        foreach (var stage in stages)
        {
            yield return [stage, InjectedFaultMode.BeforeCommit];
            yield return [stage, InjectedFaultMode.CommittedLostAcknowledgement];
        }
    }

    public static IEnumerable<object[]> ParticipantFaultCases()
    {
        var cases = new[]
        {
            (ParticipantOperation.EnableProtocol, 0),
            (ParticipantOperation.FreezeSource, 0),
            (ParticipantOperation.PrepareTarget, 1),
            (ParticipantOperation.ImportPage, 1),
            (ParticipantOperation.HideSource, 0),
            (ParticipantOperation.EnableTarget, 1),
            (ParticipantOperation.DeleteSourcePage, 0),
            (ParticipantOperation.RetireCompleted, 0),
            (ParticipantOperation.RetireCompleted, 1),
        };
        foreach (var (operation, participant) in cases)
        {
            yield return [operation, participant, InjectedFaultMode.BeforeCommit];
            yield return [operation, participant, InjectedFaultMode.CommittedLostAcknowledgement];
        }
    }

    public static IEnumerable<object[]> AbortParticipantFaultCases()
    {
        var cases = new[]
        {
            (ParticipantOperation.DeleteTargetAbortPage, 1),
            (ParticipantOperation.RetireAborted, 1),
            (ParticipantOperation.RetireAborted, 0),
        };
        foreach (var (operation, participant) in cases)
        {
            yield return [operation, participant, InjectedFaultMode.BeforeCommit];
            yield return [operation, participant, InjectedFaultMode.CommittedLostAcknowledgement];
        }
    }

    [Theory]
    [MemberData(nameof(LayoutFaultCases))]
    public async Task EveryLayoutCasCanResumeBeforeOrAfterALostAcknowledgement(
        LayoutWriteStage stage,
        InjectedFaultMode faultMode)
    {
        var harness = new MovementHarness();
        harness.Store.Inject(stage, faultMode);

        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        var completed = await harness.ExecuteAsync(planned.Intent.MoveId);

        harness.Store.FaultWasInjected.Should().BeTrue();
        completed.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
        harness.Store.DurableState.Epoch.Should().Be(3);
        harness.Store.DurableState.SlotAssignments[0].Should().Be(1);
        harness.Store.DurableState.MoveIntent.Should().BeNull();
        harness.Store.DurableState.LastMoveReceipt!.MoveId.Should().Be(planned.Intent.MoveId);
        harness.Partitions.Values.Should().OnlyContain(static partition => !partition.Control.IsPresent);

        var repeated = await harness.Grain.AdvanceMoveAsync(MovementHarness.Command(planned.Intent.MoveId));
        repeated.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
        (await harness.Grain.GetMoveProgressAsync()).Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(ParticipantFaultCases))]
    public async Task EveryParticipantMutationCanResumeBeforeOrAfterALostAcknowledgement(
        ParticipantOperation operation,
        int participant,
        InjectedFaultMode faultMode)
    {
        var harness = new MovementHarness();
        harness.Partitions[participant].Inject(operation, faultMode);

        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        var completed = await harness.ExecuteAsync(planned.Intent.MoveId);

        harness.Partitions[participant].FaultWasInjected.Should().BeTrue();
        completed.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
        harness.Store.DurableState.SlotAssignments[0].Should().Be(1);
        harness.Partitions.Values.Should().OnlyContain(static partition => !partition.Control.IsPresent);
    }

    [Theory]
    [InlineData(InjectedFaultMode.BeforeCommit)]
    [InlineData(InjectedFaultMode.CommittedLostAcknowledgement)]
    public async Task AbortingIntentAndTargetCleanupResumeAcrossLostAcknowledgements(
        InjectedFaultMode faultMode)
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        await harness.AdvanceUntilAsync(
            planned.Intent.MoveId,
            SearchableStorageSlotMovePhase.Copying);
        harness.Store.Inject(LayoutWriteStage.Aborting, faultMode);

        await harness.RequestAbortAsync(planned.Intent.MoveId);
        harness.Partitions[1].Inject(ParticipantOperation.DeleteTargetAbortPage, faultMode);
        var aborted = await harness.ExecuteAsync(planned.Intent.MoveId);

        harness.Store.FaultWasInjected.Should().BeTrue();
        harness.Partitions[1].FaultWasInjected.Should().BeTrue();
        aborted.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        harness.Store.DurableState.Epoch.Should().Be(2);
        harness.Store.DurableState.SlotAssignments[0].Should().Be(0);
        harness.Partitions.Values.Should().OnlyContain(static partition => !partition.Control.IsPresent);

        var repeatedAbort = await harness.Grain.RequestMoveAbortAsync(
            MovementHarness.Command(planned.Intent.MoveId));
        repeatedAbort.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
    }

    [Theory]
    [MemberData(nameof(AbortParticipantFaultCases))]
    public async Task EveryAbortParticipantMutationResumesAcrossLostAcknowledgements(
        ParticipantOperation operation,
        int participant,
        InjectedFaultMode faultMode)
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        await harness.AdvanceUntilAsync(
            planned.Intent.MoveId,
            SearchableStorageSlotMovePhase.Copying);
        await harness.RequestAbortAsync(planned.Intent.MoveId);
        harness.Partitions[participant].Inject(operation, faultMode);

        var aborted = await harness.ExecuteAsync(planned.Intent.MoveId);

        harness.Partitions[participant].FaultWasInjected.Should().BeTrue();
        aborted.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        harness.Store.DurableState.Epoch.Should().Be(2);
        harness.Store.DurableState.SlotAssignments[0].Should().Be(0);
        harness.Partitions.Values.Should().OnlyContain(static partition => !partition.Control.IsPresent);
    }

    [Theory]
    [InlineData(InjectedFaultMode.BeforeCommit)]
    [InlineData(InjectedFaultMode.CommittedLostAcknowledgement)]
    public async Task AbortedTerminalReceiptCasResumesOnTheSourceAssignmentBranch(
        InjectedFaultMode faultMode)
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        await harness.RequestAbortAsync(planned.Intent.MoveId);
        harness.Store.Inject(LayoutWriteStage.TerminalReceipt, faultMode);

        var aborted = await harness.ExecuteAsync(planned.Intent.MoveId);

        harness.Store.FaultWasInjected.Should().BeTrue();
        aborted.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        harness.Store.DurableState.Epoch.Should().Be(2);
        harness.Store.DurableState.SlotAssignments[0].Should().Be(0);
        harness.Store.DurableState.LastMoveReceipt!.TerminalPhase
            .Should().Be(SearchableStorageSlotMovePhase.Aborted);
        harness.Store.DurableState.LastMoveReceipt.CompletionEpoch.Should().Be(2);
    }

    [Fact]
    public async Task DurableTargetPreparedStateReconcilesForwardThroughTheVersionFence()
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        await harness.AdvanceUntilAsync(
            planned.Intent.MoveId,
            SearchableStorageSlotMovePhase.SourceFrozen);
        var move = CreateMoveIdentity(planned.Intent);
        var frozenNextVersion = harness.Partitions[0].Control.FrozenNextVersion;
        harness.Partitions[1].SeedTargetPrepared(move, frozenNextVersion);

        var reconciled = await harness.Grain.AdvanceMoveAsync(
            MovementHarness.Command(planned.Intent.MoveId));

        reconciled.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.TargetVersionFenced);
        var target = await harness.Partitions[1].GetMovementStateAsync();
        target.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetImporting);
        target.NextVersion.Should().BeGreaterThanOrEqualTo(frozenNextVersion);
    }

    [Fact]
    public async Task DurableTargetPreparedStateCanAbortWithoutLosingItsVersionFence()
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        await harness.AdvanceUntilAsync(
            planned.Intent.MoveId,
            SearchableStorageSlotMovePhase.SourceFrozen);
        var move = CreateMoveIdentity(planned.Intent);
        var frozenNextVersion = harness.Partitions[0].Control.FrozenNextVersion;
        harness.Partitions[1].SeedTargetPrepared(move, frozenNextVersion);

        await harness.RequestAbortAsync(planned.Intent.MoveId);
        var aborted = await harness.ExecuteAsync(planned.Intent.MoveId);

        aborted.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        var target = await harness.Partitions[1].GetMovementStateAsync();
        target.NextVersion.Should().BeGreaterThanOrEqualTo(frozenNextVersion);
        harness.Partitions.Values.Should().OnlyContain(static participant => !participant.Control.IsPresent);
    }

    [Fact]
    public async Task FailedReadOnlyExportRetriesTheExactDurableCursor()
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var planned = await harness.PlanAsync();
        await harness.AdvanceUntilAsync(
            planned.Intent.MoveId,
            SearchableStorageSlotMovePhase.TargetVersionFenced);
        harness.Partitions[0].Inject(
            ParticipantOperation.ExportPage,
            InjectedFaultMode.BeforeCommit);

        Func<Task> failed = async () => await harness.Grain.AdvanceMoveAsync(
            MovementHarness.Command(planned.Intent.MoveId));
        await failed.Should().ThrowAsync<InjectedMovementFaultException>();
        harness.Store.DurableState.MoveIntent!.Phase.Should().Be(
            SearchableStorageSlotMovePhase.TargetVersionFenced);

        var retried = await harness.Grain.AdvanceMoveAsync(
            MovementHarness.Command(planned.Intent.MoveId));

        retried.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Copying);
        harness.Partitions[0].ExportRequests.Should().HaveCount(2);
        harness.Partitions[0].ExportRequests[0].Should().Be(
            harness.Partitions[0].ExportRequests[1]);
    }

    [Fact]
    public async Task AbortIsRejectedAfterTheOwnershipCasAndOnlyOneMoveCanBeAuthoritative()
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var planned = await harness.PlanAsync();

        Func<Task> competingPlan = async () => await harness.Grain.PlanMoveAsync(
            MovementHarness.Plan(slot: 1, targetOwner: 0));
        await competingPlan.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*already active*");

        await harness.AdvanceUntilAsync(
            planned.Intent.MoveId,
            SearchableStorageSlotMovePhase.OwnershipCommitted);
        Func<Task> abort = async () => await harness.Grain.RequestMoveAbortAsync(
            MovementHarness.Command(planned.Intent.MoveId));

        await abort.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be aborted*");
    }

    [Fact]
    public async Task CompletedReceiptRemainsIdempotentWhileTheNextMoveIsActive()
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var first = await harness.PlanAsync();
        var completed = await harness.ExecuteAsync(first.Intent.MoveId);
        completed.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);

        var second = await harness.Grain.PlanMoveAsync(
            MovementHarness.Plan(slot: 1, targetOwner: 0));
        await harness.AdvanceUntilAsync(
            second.Intent.MoveId,
            SearchableStorageSlotMovePhase.OwnershipCommitted);

        var repeated = await harness.Grain.AdvanceMoveAsync(
            MovementHarness.Command(first.Intent.MoveId));
        Func<Task> repeatedAbort = async () => await harness.Grain.RequestMoveAbortAsync(
            MovementHarness.Command(first.Intent.MoveId));

        repeated.Intent.MoveId.Should().Be(first.Intent.MoveId);
        repeated.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
        repeated.CurrentEpoch.Should().Be(harness.Store.DurableState.Epoch);
        repeated.CurrentEpoch.Should().BeGreaterThan(first.CurrentEpoch);
        second.Intent.MoveId.Should().NotBe(first.Intent.MoveId);
        harness.Store.DurableState.MoveIntent!.MoveId.Should().Be(second.Intent.MoveId);
        await repeatedAbort.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*completed its ownership commit*");
    }

    [Fact]
    public async Task AbortedReceiptRemainsIdempotentWhileTheNextMoveIsActive()
    {
        var harness = new MovementHarness();
        await harness.EnableAsync();
        var first = await harness.PlanAsync();
        await harness.RequestAbortAsync(first.Intent.MoveId);
        var aborted = await harness.ExecuteAsync(first.Intent.MoveId);
        aborted.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);

        var second = await harness.Grain.PlanMoveAsync(
            MovementHarness.Plan(slot: 1, targetOwner: 0));

        var repeatedAdvance = await harness.Grain.AdvanceMoveAsync(
            MovementHarness.Command(first.Intent.MoveId));
        var repeatedAbort = await harness.Grain.RequestMoveAbortAsync(
            MovementHarness.Command(first.Intent.MoveId));

        repeatedAdvance.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        repeatedAbort.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        second.Intent.MoveId.Should().NotBe(first.Intent.MoveId);
        harness.Store.DurableState.MoveIntent!.MoveId.Should().Be(second.Intent.MoveId);
    }

    [Fact]
    public async Task MaximumRoutingEpochIsRejectedBeforeAMoveIntentIsPersisted()
    {
        var harness = new MovementHarness(CreateEnabledState(epoch: long.MaxValue));

        Func<Task> plan = async () => await harness.Grain.PlanMoveAsync(
            MovementHarness.Plan(slot: 0, targetOwner: 1));

        await plan.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot advance further*");
        harness.Store.DurableState.MoveIntent.Should().BeNull();
        harness.Store.TotalWriteAttempts.Should().Be(0);
    }

    [Fact]
    public async Task ManualTargetOwnerUsesTheFullAddressableBoundaryAndRejectsTheFirstInvalidIndex()
    {
        var state = CreateEnabledState(epoch: 2);
        var store = new DurableLayoutStore(state);
        var source = new MovementParticipant(owner: 0);
        source.SeedProtocol(minimumRoutingEpoch: 2);
        var target = new MovementParticipant(owner: StorageLayout.MaximumVirtualSlotCount - 1);
        target.SeedProtocol(minimumRoutingEpoch: 2);
        var grain = new StorageLayoutGrain(
            store.CreateActivation(),
            MovementHarness.ProviderName,
            requestDeactivation: static () => { },
            getPartition: owner => owner == 0 ? source : target);

        Func<Task> invalid = async () => await grain.PlanMoveAsync(
            MovementHarness.Plan(0, StorageLayout.MaximumVirtualSlotCount));
        await invalid.Should().ThrowAsync<ArgumentOutOfRangeException>();
        store.TotalWriteAttempts.Should().Be(0);

        var accepted = await grain.PlanMoveAsync(
            MovementHarness.Plan(0, StorageLayout.MaximumVirtualSlotCount - 1));

        accepted.Intent.TargetOwner.Should().Be(StorageLayout.MaximumVirtualSlotCount - 1);
        store.TotalWriteAttempts.Should().Be(1);
    }

    [Fact]
    public async Task PlannedMoveRaisesALaggingSourceFloorAsItsOwnBoundedStepBeforeFreeze()
    {
        var state = CreateEnabledState(epoch: 5);
        state.PartitionCount = 3;
        state.VirtualSlotCount = 3;
        state.SlotAssignments = [0, 1, 2];
        var store = new DurableLayoutStore(state);
        var source = new MovementParticipant(owner: 2);
        source.SeedProtocol(minimumRoutingEpoch: 2);
        var target = new MovementParticipant(owner: 0);
        target.SeedProtocol(minimumRoutingEpoch: 5);
        var participants = new Dictionary<int, MovementParticipant>
        {
            [0] = target,
            [1] = new MovementParticipant(owner: 1),
            [2] = source,
        };
        var grain = new StorageLayoutGrain(
            store.CreateActivation(),
            MovementHarness.ProviderName,
            requestDeactivation: static () => { },
            getPartition: owner => participants[owner]);
        var planned = await grain.PlanMoveAsync(MovementHarness.Plan(slot: 2, targetOwner: 0));

        var floorAdvanced = await grain.AdvanceMoveAsync(
            MovementHarness.Command(planned.Intent.MoveId));

        floorAdvanced.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Planned);
        source.MinimumRoutingEpoch.Should().Be(5);
        source.Control.IsPresent.Should().BeFalse();

        var frozen = await grain.AdvanceMoveAsync(MovementHarness.Command(planned.Intent.MoveId));
        frozen.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.SourceFrozen);
    }

    [Fact]
    public async Task PlannedMoveEnablesANewTargetBeforeTouchingTheSource()
    {
        var state = CreateEnabledState(epoch: 2);
        var store = new DurableLayoutStore(state);
        var source = new MovementParticipant(owner: 0);
        source.SeedProtocol(minimumRoutingEpoch: 2);
        var target = new MovementParticipant(owner: 2);
        var participants = new Dictionary<int, MovementParticipant>
        {
            [0] = source,
            [1] = new MovementParticipant(owner: 1),
            [2] = target,
        };
        var grain = new StorageLayoutGrain(
            store.CreateActivation(),
            MovementHarness.ProviderName,
            requestDeactivation: static () => { },
            getPartition: owner => participants[owner]);
        var planned = await grain.PlanMoveAsync(MovementHarness.Plan(slot: 0, targetOwner: 2));

        var targetEnabled = await grain.AdvanceMoveAsync(
            MovementHarness.Command(planned.Intent.MoveId));

        targetEnabled.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.Planned);
        target.MinimumRoutingEpoch.Should().Be(2);
        source.Control.IsPresent.Should().BeFalse();

        var frozen = await grain.AdvanceMoveAsync(MovementHarness.Command(planned.Intent.MoveId));
        frozen.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.SourceFrozen);
    }

    [Theory]
    [InlineData(true, InjectedFaultMode.BeforeCommit)]
    [InlineData(true, InjectedFaultMode.CommittedLostAcknowledgement)]
    [InlineData(false, InjectedFaultMode.BeforeCommit)]
    [InlineData(false, InjectedFaultMode.CommittedLostAcknowledgement)]
    public async Task LazyParticipantFloorReconciliationResumesAcrossLostAcknowledgements(
        bool laggingSource,
        InjectedFaultMode faultMode)
    {
        var state = CreateEnabledState(epoch: 5);
        var store = new DurableLayoutStore(state);
        var source = new MovementParticipant(owner: 0);
        source.SeedProtocol(minimumRoutingEpoch: laggingSource ? 2 : 5);
        var target = new MovementParticipant(owner: 2);
        if (laggingSource)
        {
            target.SeedProtocol(minimumRoutingEpoch: 5);
            source.Inject(ParticipantOperation.EnableProtocol, faultMode);
        }
        else
        {
            target.Inject(ParticipantOperation.EnableProtocol, faultMode);
        }

        var participants = new Dictionary<int, MovementParticipant>
        {
            [0] = source,
            [1] = new MovementParticipant(owner: 1),
            [2] = target,
        };
        var grain = new StorageLayoutGrain(
            store.CreateActivation(),
            MovementHarness.ProviderName,
            requestDeactivation: static () => { },
            getPartition: owner => participants[owner]);
        var planned = await grain.PlanMoveAsync(MovementHarness.Plan(slot: 0, targetOwner: 2));

        Func<Task> firstAdvance = async () => await grain.AdvanceMoveAsync(
            MovementHarness.Command(planned.Intent.MoveId));
        await firstAdvance.Should().ThrowAsync<InjectedMovementFaultException>();
        StorageSlotMoveProgressSnapshot progress;
        do
        {
            progress = await grain.AdvanceMoveAsync(MovementHarness.Command(planned.Intent.MoveId));
        }
        while (progress.Intent.Phase == SearchableStorageSlotMovePhase.Planned);

        progress.Intent.Phase.Should().Be(SearchableStorageSlotMovePhase.SourceFrozen);
        source.MinimumRoutingEpoch.Should().Be(5);
        target.MinimumRoutingEpoch.Should().Be(5);
        (laggingSource ? source : target).FaultWasInjected.Should().BeTrue();
    }

    private static StorageLayoutState CreateEnabledState(long epoch)
    {
        return new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = MovementHarness.ProviderName,
            PartitionCount = 2,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = 2,
            SlotAssignments = [0, 1],
            Epoch = epoch,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
        };
    }

    private static StorageMoveIdentity CreateMoveIdentity(StorageSlotMoveIntent move) => new()
    {
        ProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
        MoveId = move.MoveId,
        Slot = move.Slot,
        VirtualSlotCount = 2,
        SourceEpoch = move.SourceEpoch,
        SourceOwner = move.SourceOwner,
        TargetOwner = move.TargetOwner,
    };

    private sealed class MovementHarness
    {
        public const string ProviderName = "layout-orchestration-recovery";

        public MovementHarness(StorageLayoutState? initialState = null)
        {
            Store = new DurableLayoutStore(initialState ?? CreateDisabledState());
            Partitions = new Dictionary<int, MovementParticipant>
            {
                [0] = new MovementParticipant(0),
                [1] = new MovementParticipant(1),
            };
            RecreateGrain();
        }

        public DurableLayoutStore Store { get; }

        public Dictionary<int, MovementParticipant> Partitions { get; }

        public StorageLayoutGrain Grain { get; private set; } = null!;

        public async Task EnableAsync()
        {
            while (true)
            {
                try
                {
                    var layout = await Grain.BeginMovementEnablementAsync();
                    while (layout.MovementState == SearchableStorageMovementState.Enabling)
                    {
                        var intent = layout.CopyMovementEnablement()!;
                        layout = await Grain.AdvanceMovementEnablementAsync(intent.EnablementId);
                    }

                    layout.MovementState.Should().Be(SearchableStorageMovementState.Enabled);
                    return;
                }
                catch (InjectedMovementFaultException)
                {
                    RecreateGrain();
                }
            }
        }

        public async Task<StorageSlotMoveProgressSnapshot> PlanAsync()
        {
            while (true)
            {
                try
                {
                    return await Grain.PlanMoveAsync(Plan(slot: 0, targetOwner: 1));
                }
                catch (InjectedMovementFaultException)
                {
                    RecreateGrain();
                }
            }
        }

        public async Task<StorageSlotMoveProgressSnapshot> ExecuteAsync(Guid moveId)
        {
            while (true)
            {
                try
                {
                    var progress = await Grain.AdvanceMoveAsync(Command(moveId));
                    if (progress.Intent.Phase is SearchableStorageSlotMovePhase.Completed
                        or SearchableStorageSlotMovePhase.Aborted)
                    {
                        return progress;
                    }
                }
                catch (InjectedMovementFaultException)
                {
                    RecreateGrain();
                }
            }
        }

        public async Task AdvanceUntilAsync(Guid moveId, SearchableStorageSlotMovePhase phase)
        {
            while (true)
            {
                try
                {
                    var progress = await Grain.AdvanceMoveAsync(Command(moveId));
                    if (progress.Intent.Phase == phase)
                    {
                        return;
                    }
                }
                catch (InjectedMovementFaultException)
                {
                    RecreateGrain();
                }
            }
        }

        public async Task RequestAbortAsync(Guid moveId)
        {
            while (true)
            {
                try
                {
                    _ = await Grain.RequestMoveAbortAsync(Command(moveId));
                    return;
                }
                catch (InjectedMovementFaultException)
                {
                    RecreateGrain();
                }
            }
        }

        public static StorageSlotMovePlanRequest Plan(int slot, int targetOwner) => new()
        {
            Slot = slot,
            TargetOwner = targetOwner,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
            TransferPageRecordLimit = 16,
            TransferPageByteTarget = 4_096,
        };

        public static StorageSlotMoveCommand Command(Guid moveId) => new()
        {
            MoveId = moveId,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
        };

        private static StorageLayoutState CreateDisabledState()
        {
            return new StorageLayoutState
            {
                Initialized = true,
                FormatVersion = StorageLayout.MovementFormatVersion,
                ProviderName = ProviderName,
                PartitionCount = 2,
                JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
                MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
                VirtualSlotCount = 2,
                SlotAssignments = [0, 1],
                Epoch = 1,
            };
        }

        private void RecreateGrain()
        {
            Grain = new StorageLayoutGrain(
                Store.CreateActivation(),
                ProviderName,
                requestDeactivation: static () => { },
                getPartition: owner => Partitions[owner]);
        }
    }

    private sealed class DurableLayoutStore
    {
        private LayoutWriteStage? _faultStage;
        private InjectedFaultMode _faultMode;

        public DurableLayoutStore(StorageLayoutState initialState)
        {
            DurableState = initialState.Copy();
        }

        public StorageLayoutState DurableState { get; private set; }

        public int TotalWriteAttempts { get; private set; }

        public bool FaultWasInjected { get; private set; }

        public void Inject(LayoutWriteStage stage, InjectedFaultMode mode)
        {
            _faultStage = stage;
            _faultMode = mode;
            FaultWasInjected = false;
        }

        public IPersistentState<StorageLayoutState> CreateActivation() =>
            new ActivationState(this, DurableState.Copy());

        private Task WriteAsync(StorageLayoutState candidate)
        {
            TotalWriteAttempts++;
            var stage = Classify(DurableState, candidate);
            var inject = !FaultWasInjected && _faultStage == stage;
            if (inject && _faultMode == InjectedFaultMode.BeforeCommit)
            {
                FaultWasInjected = true;
                return Task.FromException(new InjectedMovementFaultException(stage.ToString()));
            }

            DurableState = candidate.Copy();
            if (inject)
            {
                FaultWasInjected = true;
                return Task.FromException(new InjectedMovementFaultException(stage.ToString()));
            }

            return Task.CompletedTask;
        }

        private static LayoutWriteStage Classify(
            StorageLayoutState previous,
            StorageLayoutState candidate)
        {
            if (previous.MovementEnablement is null && candidate.MovementEnablement is not null)
            {
                return LayoutWriteStage.EnableIntent;
            }

            if (previous.MovementEnablement is not null
                && candidate.MovementEnablement is not null
                && candidate.MovementEnablement.NextOwnerIndex
                    > previous.MovementEnablement.NextOwnerIndex)
            {
                return LayoutWriteStage.EnableOwnerProgress;
            }

            if (previous.MovementProtocolVersion == 0
                && candidate.MovementProtocolVersion == StorageLayout.CurrentMovementProtocolVersion)
            {
                return LayoutWriteStage.EnableFinalPublish;
            }

            if (previous.MoveIntent is null && candidate.MoveIntent is not null)
            {
                return LayoutWriteStage.MoveIntent;
            }

            if (previous.MoveIntent is not null && candidate.MoveIntent is null)
            {
                return LayoutWriteStage.TerminalReceipt;
            }

            if (previous.MoveIntent is not null
                && candidate.MoveIntent is not null
                && candidate.MoveIntent.Phase != previous.MoveIntent.Phase)
            {
                return candidate.MoveIntent.Phase switch
                {
                    SearchableStorageSlotMovePhase.SourceFrozen => LayoutWriteStage.SourceFrozen,
                    SearchableStorageSlotMovePhase.TargetVersionFenced => LayoutWriteStage.TargetVersionFenced,
                    SearchableStorageSlotMovePhase.Copying => LayoutWriteStage.Copying,
                    SearchableStorageSlotMovePhase.CopyComplete => LayoutWriteStage.CopyComplete,
                    SearchableStorageSlotMovePhase.OwnershipCommitted => LayoutWriteStage.OwnershipCommitted,
                    SearchableStorageSlotMovePhase.SourceVisibilityFenced => LayoutWriteStage.SourceVisibilityFenced,
                    SearchableStorageSlotMovePhase.TargetEnabled => LayoutWriteStage.TargetEnabled,
                    SearchableStorageSlotMovePhase.DeletingSource => LayoutWriteStage.DeletingSource,
                    SearchableStorageSlotMovePhase.Retiring => LayoutWriteStage.Retiring,
                    SearchableStorageSlotMovePhase.Aborting => LayoutWriteStage.Aborting,
                    _ => throw new InvalidOperationException(
                        $"Unexpected layout move phase {candidate.MoveIntent.Phase}."),
                };
            }

            throw new InvalidOperationException("A layout test write did not match a durable orchestration stage.");
        }

        private sealed class ActivationState : IPersistentState<StorageLayoutState>
        {
            private readonly DurableLayoutStore _store;

            public ActivationState(DurableLayoutStore store, StorageLayoutState state)
            {
                _store = store;
                State = state;
            }

            public StorageLayoutState State { get; set; }

            public string? Etag { get; private set; }

            public bool RecordExists { get; private set; } = true;

            public Task ClearStateAsync() => throw new NotSupportedException();

            public Task ReadStateAsync() => Task.CompletedTask;

            public async Task WriteStateAsync()
            {
                await _store.WriteAsync(State);
                RecordExists = true;
                Etag = _store.TotalWriteAttempts.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
        }
    }

    private sealed class MovementParticipant : StoragePartitionGrainMovementTestDouble, IStoragePartitionGrain
    {
        private readonly int _owner;
        private int _persistenceFormatVersion;
        private int _movementProtocolVersion;
        private int _indexSchemaProtocolVersion;
        private bool _routedOperationsRequired;
        private long _minimumRoutingEpoch = 1;
        private long _nextVersion = 10;
        private ParticipantOperation? _faultOperation;
        private InjectedFaultMode _faultMode;

        public MovementParticipant(int owner)
        {
            _owner = owner;
        }

        public StoragePartitionMoveControl Control { get; private set; } = new();

        public long MinimumRoutingEpoch => _minimumRoutingEpoch;

        public bool FaultWasInjected { get; private set; }

        public List<(long PageOrdinal, string? AfterRecordKey)> ExportRequests { get; } = [];

        public void Inject(ParticipantOperation operation, InjectedFaultMode mode)
        {
            _faultOperation = operation;
            _faultMode = mode;
            FaultWasInjected = false;
        }

        public void SeedProtocol(long minimumRoutingEpoch)
        {
            _persistenceFormatVersion = StoragePersistence.MovementPersistenceFormatVersion;
            _movementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion;
            _routedOperationsRequired = true;
            _minimumRoutingEpoch = minimumRoutingEpoch;
        }

        public void SeedTargetPrepared(StorageMoveIdentity move, long frozenNextVersion)
        {
            Control = CreateControl(
                move,
                StoragePartitionMoveRole.Target,
                StoragePartitionMovePhase.TargetPrepared,
                frozenNextVersion);
        }

        public new Task<StoragePartitionProtocolState> EnableMovementProtocolAsync(
            StoragePartitionProtocolRequest request) => MutateAsync(
                ParticipantOperation.EnableProtocol,
                () =>
                {
                    _indexSchemaProtocolVersion = request.IndexSchemaProtocolVersion;
                    _persistenceFormatVersion = _indexSchemaProtocolVersion
                        == StorageIndexSchema.ProtocolVersion
                            ? StoragePersistence.CurrentPersistenceFormatVersion
                            : StoragePersistence.MovementPersistenceFormatVersion;
                    _movementProtocolVersion = request.ProtocolVersion;
                    _routedOperationsRequired = true;
                    _minimumRoutingEpoch = request.MinimumRoutingEpoch;
                },
                CreateState);

        public new Task<StoragePartitionProtocolState> GetMovementStateAsync() =>
            Task.FromResult(CreateState());

        public new Task<StoragePartitionProtocolState> FreezeMoveSourceAsync(StorageMoveIdentity move) =>
            MutateAsync(
                ParticipantOperation.FreezeSource,
                () =>
                {
                    if (!Control.IsPresent)
                    {
                        Control = CreateControl(
                            move,
                            StoragePartitionMoveRole.Source,
                            StoragePartitionMovePhase.SourceFrozen,
                            _nextVersion);
                    }
                },
                CreateState);

        public new Task<StoragePartitionProtocolState> PrepareMoveTargetAsync(
            StorageMoveTargetPrepareRequest request) => MutateAsync(
                ParticipantOperation.PrepareTarget,
                () =>
                {
                    if (!Control.IsPresent)
                    {
                        Control = CreateControl(
                            request.Move,
                            StoragePartitionMoveRole.Target,
                            StoragePartitionMovePhase.TargetImporting,
                            request.FrozenNextVersion);
                    }

                    Control.Phase = StoragePartitionMovePhase.TargetImporting;
                    _nextVersion = Math.Max(_nextVersion, request.FrozenNextVersion);
                },
                CreateState);

        public new Task<StorageMoveExportPage> ExportMovePageAsync(StorageMovePageRequest request)
        {
            ExportRequests.Add((
                request.PageOrdinal,
                StorageMoveRecordCodec.DecodeNullableText(request.AfterRecordKey, nameof(request))));
            if (!FaultWasInjected && _faultOperation == ParticipantOperation.ExportPage)
            {
                FaultWasInjected = true;
                return Task.FromException<StorageMoveExportPage>(
                    new InjectedMovementFaultException(ParticipantOperation.ExportPage.ToString()));
            }

            var first = request.PageOrdinal == 0;
            var records = first
                ? new List<StorageMoveRecord>
                {
                    StorageMoveRecordCodec.Encode(
                        "record",
                        new StoredRecord
                        {
                            GrainId = GrainId.Create("move-recovery", "record"),
                            Payload = [1],
                            ETag = "1",
                            IndexEntries = [],
                        }),
                }
                : [];
            return Task.FromResult(new StorageMoveExportPage
            {
                Move = request.Move.Copy(),
                PageOrdinal = request.PageOrdinal,
                AfterRecordKey = request.AfterRecordKey,
                NextRecordKey = first
                    ? StorageMoveRecordCodec.EncodeText("record")
                    : StorageMoveRecordCodec.CopyText(request.AfterRecordKey),
                Exhausted = !first,
                EncodedByteCount = first ? 17 : 0,
                Records = records,
                PageDigest = CreateDigest(request.PageOrdinal),
                FrozenNextVersion = Control.FrozenNextVersion,
                ItemLimit = request.ItemLimit,
                ByteTarget = request.ByteTarget,
            });
        }

        public new Task<StorageMovePageCommitResult> ImportMovePageAsync(
            StorageMoveImportPageRequest request) => MutateAsync(
                ParticipantOperation.ImportPage,
                () =>
                {
                    Control.ProgressAfterRecordKey = request.Page.NextRecordKey;
                    Control.NextPageOrdinal = checked(Control.NextPageOrdinal + 1);
                    Control.LastPageDigest = [.. request.Page.PageDigest];
                    Control.ImportedRecordCount = checked(
                        Control.ImportedRecordCount + request.Page.Records.Count);
                    Control.ImportedByteCount = checked(
                        Control.ImportedByteCount + request.Page.EncodedByteCount);
                    Control.Phase = request.Page.Exhausted
                        ? StoragePartitionMovePhase.TargetImportComplete
                        : StoragePartitionMovePhase.TargetImporting;
                },
                () => CreateCommitResult(request.Page.Exhausted));

        public new Task<StoragePartitionProtocolState> HideMoveSourceAsync(
            StorageMoveVisibilityFenceRequest request) => MutateAsync(
                ParticipantOperation.HideSource,
                () =>
                {
                    Control.Phase = StoragePartitionMovePhase.SourceHidden;
                    _minimumRoutingEpoch = request.CommittedEpoch;
                },
                CreateState);

        public new Task<StoragePartitionProtocolState> EnableMoveTargetAsync(StorageMoveIdentity move) =>
            MutateAsync(
                ParticipantOperation.EnableTarget,
                () => Control.Phase = StoragePartitionMovePhase.TargetEnabled,
                CreateState);

        public new Task<StorageMovePageCommitResult> DeleteMovePageAsync(
            StorageMoveDeletePageRequest request)
        {
            var operation = request.Mode == StorageMoveDeleteMode.SourceCleanup
                ? ParticipantOperation.DeleteSourcePage
                : ParticipantOperation.DeleteTargetAbortPage;
            return MutateAsync(
                operation,
                () =>
                {
                    var sourceFirstPage = request.Mode == StorageMoveDeleteMode.SourceCleanup
                        && request.PageOrdinal == 0;
                    Control.ProgressAfterRecordKey = sourceFirstPage
                        ? StorageMoveRecordCodec.EncodeText("record")
                        : StorageMoveRecordCodec.CopyText(request.AfterRecordKey);
                    Control.NextPageOrdinal = checked(request.PageOrdinal + 1);
                    Control.LastPageDigest = CreateDigest(request.PageOrdinal);
                    if (sourceFirstPage)
                    {
                        Control.DeletedRecordCount++;
                        Control.DeletedByteCount += 11;
                    }

                    Control.Phase = request.Mode == StorageMoveDeleteMode.TargetAbort
                        ? StoragePartitionMovePhase.TargetAbortComplete
                        : sourceFirstPage
                            ? StoragePartitionMovePhase.SourceDeleting
                            : StoragePartitionMovePhase.SourceDeleteComplete;
                },
                () => CreateCommitResult(
                    request.Mode == StorageMoveDeleteMode.TargetAbort || request.PageOrdinal > 0));
        }

        public new Task<StoragePartitionProtocolState> RetireMoveParticipantAsync(
            StorageMoveRetireRequest request)
        {
            var operation = request.Kind == StorageMoveRetirementKind.Completed
                ? ParticipantOperation.RetireCompleted
                : ParticipantOperation.RetireAborted;
            return MutateAsync(
                operation,
                () => Control = new StoragePartitionMoveControl(),
                CreateState);
        }

        private Task<T> MutateAsync<T>(
            ParticipantOperation operation,
            Action mutation,
            Func<T> result)
        {
            var inject = !FaultWasInjected && _faultOperation == operation;
            if (inject && _faultMode == InjectedFaultMode.BeforeCommit)
            {
                FaultWasInjected = true;
                return Task.FromException<T>(new InjectedMovementFaultException(operation.ToString()));
            }

            mutation();
            if (inject)
            {
                FaultWasInjected = true;
                return Task.FromException<T>(new InjectedMovementFaultException(operation.ToString()));
            }

            return Task.FromResult(result());
        }

        private StoragePartitionProtocolState CreateState()
        {
            return new StoragePartitionProtocolState
            {
                PersistenceFormatVersion = _persistenceFormatVersion,
                MovementProtocolVersion = _movementProtocolVersion,
                RoutedOperationsRequired = _routedOperationsRequired,
                MinimumRoutingEpoch = _minimumRoutingEpoch,
                NextVersion = _nextVersion,
                MoveControl = Control.Copy(),
                IndexSchemaProtocolVersion = _indexSchemaProtocolVersion,
            };
        }

        private StorageMovePageCommitResult CreateCommitResult(bool exhausted)
        {
            return new StorageMovePageCommitResult
            {
                State = CreateState(),
                PageOrdinal = checked(Control.NextPageOrdinal - 1),
                AfterRecordKey = Control.ProgressAfterRecordKey,
                Exhausted = exhausted,
                PageDigest = [.. Control.LastPageDigest],
                EncodedByteCount = Control.LastPageEncodedByteCount,
            };
        }

        private static StoragePartitionMoveControl CreateControl(
            StorageMoveIdentity move,
            StoragePartitionMoveRole role,
            StoragePartitionMovePhase phase,
            long frozenNextVersion)
        {
            return new StoragePartitionMoveControl
            {
                IsPresent = true,
                MoveId = move.MoveId,
                Slot = move.Slot,
                VirtualSlotCount = move.VirtualSlotCount,
                SourceEpoch = move.SourceEpoch,
                SourceOwner = move.SourceOwner,
                TargetOwner = move.TargetOwner,
                Role = role,
                Phase = phase,
                FrozenNextVersion = frozenNextVersion,
            };
        }

        private static byte[] CreateDigest(long ordinal)
        {
            var digest = new byte[StorageMovePageDigest.DigestLength];
            digest[0] = checked((byte)(ordinal + 1));
            return digest;
        }
    }

    public enum InjectedFaultMode
    {
        BeforeCommit = 0,
        CommittedLostAcknowledgement = 1,
    }

    public enum LayoutWriteStage
    {
        EnableIntent = 0,
        EnableOwnerProgress = 1,
        EnableFinalPublish = 2,
        MoveIntent = 3,
        SourceFrozen = 4,
        TargetVersionFenced = 5,
        Copying = 6,
        CopyComplete = 7,
        OwnershipCommitted = 8,
        SourceVisibilityFenced = 9,
        TargetEnabled = 10,
        DeletingSource = 11,
        Retiring = 12,
        Aborting = 13,
        TerminalReceipt = 14,
    }

    public enum ParticipantOperation
    {
        EnableProtocol = 0,
        FreezeSource = 1,
        PrepareTarget = 2,
        ImportPage = 3,
        HideSource = 4,
        EnableTarget = 5,
        DeleteSourcePage = 6,
        DeleteTargetAbortPage = 7,
        RetireCompleted = 8,
        RetireAborted = 9,
        ExportPage = 10,
    }

    private sealed class InjectedMovementFaultException : Exception
    {
        public InjectedMovementFaultException(string stage)
            : base($"Injected movement fault at {stage}.")
        {
        }
    }
}
