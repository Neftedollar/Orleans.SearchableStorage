using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageLayoutIndexSchemaFenceTests
{
    private const string ProviderName = "layout-index-schema-fence";

    public static IEnumerable<object[]> FenceFaultCases()
    {
        foreach (var protocolAlreadyEnabled in new[] { false, true })
        {
            foreach (var stage in Enum.GetValues<FenceWriteStage>())
            {
                yield return [
                    protocolAlreadyEnabled,
                    stage,
                    FenceFaultMode.BeforeCommit,
                ];
                yield return [
                    protocolAlreadyEnabled,
                    stage,
                    FenceFaultMode.CommittedLostAcknowledgement,
                ];
            }
        }
    }

    [Theory]
    [MemberData(nameof(FenceFaultCases))]
    public async Task EveryFenceCasResumesBeforeOrAfterALostAcknowledgement(
        bool protocolAlreadyEnabled,
        FenceWriteStage stage,
        FenceFaultMode mode)
    {
        var initial = CreateMovementDisabledState();
        if (protocolAlreadyEnabled)
        {
            initial.FormatVersion = StorageLayout.IndexSchemaFormatVersion;
            initial.IndexSchemaProtocolVersion = StorageLayout.CurrentIndexSchemaProtocolVersion;
        }

        var store = new DurableLayoutStore(initial);
        var request = CreateRequest(store.DurableState, Guid.NewGuid());
        store.Inject(stage, mode);

        var grain = CreateGrain(store);
        grain = await RetryAfterInjectedFaultAsync(
            grain,
            store,
            current => current.BeginIndexSchemaProtocolEnablementAsync(request));

        store.DurableState.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
        store.DurableState.IndexSchemaProtocolVersion.Should().Be(
            protocolAlreadyEnabled ? StorageLayout.CurrentIndexSchemaProtocolVersion : 0);
        store.DurableState.IndexSchemaEnablement!.EnablementId.Should().Be(request.EnablementId);

        grain = await RetryAfterInjectedFaultAsync(
            grain,
            store,
            current => current.EnableIndexSchemaProtocolAsync(request));

        store.FaultWasInjected.Should().BeTrue();
        store.DurableState.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
        store.DurableState.IndexSchemaProtocolVersion
            .Should().Be(StorageLayout.CurrentIndexSchemaProtocolVersion);
        store.DurableState.IndexSchemaEnablement.Should().BeNull();

        var repeatedBegin = await grain.BeginIndexSchemaProtocolEnablementAsync(request);
        var repeatedPublish = await grain.EnableIndexSchemaProtocolAsync(request);
        repeatedBegin.IndexSchemaProtocolVersion
            .Should().Be(StorageLayout.CurrentIndexSchemaProtocolVersion);
        repeatedPublish.IndexSchemaProtocolVersion
            .Should().Be(StorageLayout.CurrentIndexSchemaProtocolVersion);
    }

    [Fact]
    public async Task ActiveFenceRejectsMovementAndAnotherSchemaClaimant()
    {
        var disabledStore = new DurableLayoutStore(CreateMovementDisabledState());
        var disabledGrain = CreateGrain(disabledStore);
        var request = CreateRequest(disabledStore.DurableState, Guid.NewGuid());
        _ = await disabledGrain.BeginIndexSchemaProtocolEnablementAsync(request);

        Func<Task> beginMovement = async () => await disabledGrain.BeginMovementEnablementAsync();
        Func<Task> advanceMovement = async () => await disabledGrain.AdvanceMovementEnablementAsync(
            Guid.NewGuid());
        Func<Task> competingSchema = async () =>
            await disabledGrain.BeginIndexSchemaProtocolEnablementAsync(
                CreateRequest(disabledStore.DurableState, Guid.NewGuid()));

        await beginMovement.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*index-schema enablement*active*");
        await advanceMovement.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*index-schema enablement*active*");
        await competingSchema.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot address it*");

        var enabledStore = new DurableLayoutStore(CreateMovementEnabledState());
        var enabledGrain = CreateGrain(enabledStore);
        _ = await enabledGrain.BeginIndexSchemaProtocolEnablementAsync(
            CreateRequest(enabledStore.DurableState, Guid.NewGuid()));
        Func<Task> planMove = async () => await enabledGrain.PlanMoveAsync(new StorageSlotMovePlanRequest
        {
            Slot = 0,
            TargetOwner = 1,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
            TransferPageRecordLimit = 16,
            TransferPageByteTarget = 4_096,
        });
        await planMove.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*index-schema enablement*active*");

        disabledStore.DurableState.MovementEnablement.Should().BeNull();
        enabledStore.DurableState.MoveIntent.Should().BeNull();
    }

    [Fact]
    public async Task MovementIntentRejectsSchemaFenceBeforeTheFormatUpgrade()
    {
        var store = new DurableLayoutStore(CreateMovementDisabledState());
        var grain = CreateGrain(store);
        _ = await grain.BeginMovementEnablementAsync();
        var request = CreateRequest(store.DurableState, Guid.NewGuid());

        Func<Task> beginSchema = async () =>
            await grain.BeginIndexSchemaProtocolEnablementAsync(request);

        await beginSchema.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot run at the same time*");
        store.DurableState.FormatVersion.Should().Be(StorageLayout.MovementFormatVersion);
        store.DurableState.IndexSchemaEnablement.Should().BeNull();
    }

    [Fact]
    public async Task PublishRequiresTheDurableClaimAndItsExactRoutingBoundary()
    {
        var store = new DurableLayoutStore(CreateMovementDisabledState());
        var grain = CreateGrain(store);
        var request = CreateRequest(store.DurableState, Guid.NewGuid());

        Func<Task> publishWithoutBegin = async () =>
            await grain.EnableIndexSchemaProtocolAsync(request);
        await publishWithoutBegin.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*durably begun*");

        _ = await grain.BeginIndexSchemaProtocolEnablementAsync(request);
        Func<Task> publishForAnotherClaim = async () =>
            await grain.EnableIndexSchemaProtocolAsync(new StorageIndexSchemaLayoutProtocolRequest
            {
                ProtocolVersion = request.ProtocolVersion,
                LayoutEpoch = request.LayoutEpoch,
                LayoutFingerprint = [.. request.LayoutFingerprint],
                EnablementId = Guid.NewGuid(),
            });
        await publishForAnotherClaim.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot address it*");

        store.DurableState.IndexSchemaProtocolVersion.Should().Be(0);
        store.DurableState.IndexSchemaEnablement.Should().NotBeNull();
    }

    [Fact]
    public async Task VersionFivePreservesRoutingIdentityAndNeverDowngradesDuringMovement()
    {
        var state = CreateMovementDisabledState();
        var baseline = StorageLayoutSnapshot.FromState(state);
        var store = new DurableLayoutStore(state);
        var grain = CreateGrain(store);
        var request = CreateRequest(state, Guid.NewGuid());

        _ = await grain.BeginIndexSchemaProtocolEnablementAsync(request);
        var enabled = await grain.EnableIndexSchemaProtocolAsync(request);

        enabled.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
        StorageLayoutFingerprint.Compute(enabled)
            .Should().Equal(StorageLayoutFingerprint.Compute(baseline));
        (await grain.InitializeRoutingAsync(StorageLayout.CreateDescriptor(ProviderName, 2)))
            .FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);

        var enablingMovement = await grain.BeginMovementEnablementAsync();
        enablingMovement.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
        store.DurableState.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
    }

    [Fact]
    public async Task LaterGenerationRebuildsClaimTheSameVersionFiveMaintenanceFence()
    {
        var state = CreateMovementEnabledState();
        state.FormatVersion = StorageLayout.IndexSchemaFormatVersion;
        state.IndexSchemaProtocolVersion = StorageLayout.CurrentIndexSchemaProtocolVersion;
        var store = new DurableLayoutStore(state);
        var grain = CreateGrain(store);
        var request = CreateRequest(state, Guid.NewGuid());

        var claimed = await grain.BeginIndexSchemaProtocolEnablementAsync(request);

        claimed.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
        claimed.IndexSchemaProtocolVersion
            .Should().Be(StorageLayout.CurrentIndexSchemaProtocolVersion);
        claimed.CopyIndexSchemaEnablement()!.EnablementId.Should().Be(request.EnablementId);
        Func<Task> planMove = async () => await grain.PlanMoveAsync(new StorageSlotMovePlanRequest
        {
            Slot = 0,
            TargetOwner = 1,
            MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion,
            TransferPageRecordLimit = 16,
            TransferPageByteTarget = 4_096,
        });
        await planMove.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*index-schema enablement*active*");

        var released = await grain.EnableIndexSchemaProtocolAsync(request);
        released.CopyIndexSchemaEnablement().Should().BeNull();
        store.DurableState.FormatVersion.Should().Be(StorageLayout.IndexSchemaFormatVersion);
    }

    [Theory]
    [InlineData(StorageLayout.MovementFormatVersion, 1, false)]
    [InlineData(StorageLayout.IndexSchemaFormatVersion, 0, false)]
    [InlineData(StorageLayout.MovementFormatVersion, 0, true)]
    public async Task InvalidFormatAndCapabilityCombinationsFailClosed(
        int formatVersion,
        int schemaProtocolVersion,
        bool includeIntent)
    {
        var state = CreateMovementDisabledState();
        state.FormatVersion = formatVersion;
        state.IndexSchemaProtocolVersion = schemaProtocolVersion;
        if (includeIntent)
        {
            state.IndexSchemaEnablement = new StorageIndexSchemaEnableIntent
            {
                EnablementId = Guid.NewGuid(),
                ProtocolVersion = StorageLayout.CurrentIndexSchemaProtocolVersion,
                LayoutEpoch = state.Epoch,
                LayoutFingerprint = StorageLayoutFingerprint.Compute(
                    StorageLayoutSnapshot.FromState(state)),
            };
        }

        var grain = CreateGrain(new DurableLayoutStore(state));
        Func<Task> read = async () => await grain.GetCurrentLayoutAsync();

        await read.Should().ThrowAsync<InvalidOperationException>();
    }

    private static async Task<StorageLayoutGrain> RetryAfterInjectedFaultAsync(
        StorageLayoutGrain grain,
        DurableLayoutStore store,
        Func<StorageLayoutGrain, Task<StorageLayoutSnapshot>> operation)
    {
        try
        {
            _ = await operation(grain);
            return grain;
        }
        catch (InjectedFenceFaultException)
        {
            Func<Task> retiredActivation = async () => await operation(grain);
            await retiredActivation.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*retiring after an ambiguous persistence outcome*");
            var replacement = CreateGrain(store);
            _ = await operation(replacement);
            return replacement;
        }
    }

    private static StorageLayoutGrain CreateGrain(DurableLayoutStore store)
    {
        return new StorageLayoutGrain(
            store.CreateActivation(),
            ProviderName,
            requestDeactivation: static () => { },
            getPartition: static _ => throw new InvalidOperationException(
                "A schema-fence unit test must not contact a movement participant."));
    }

    private static StorageIndexSchemaLayoutProtocolRequest CreateRequest(
        StorageLayoutState state,
        Guid enablementId)
    {
        var snapshot = StorageLayoutSnapshot.FromState(state);
        return new StorageIndexSchemaLayoutProtocolRequest
        {
            ProtocolVersion = StorageLayout.CurrentIndexSchemaProtocolVersion,
            LayoutEpoch = snapshot.Epoch,
            LayoutFingerprint = StorageLayoutFingerprint.Compute(snapshot),
            EnablementId = enablementId,
        };
    }

    private static StorageLayoutState CreateMovementDisabledState()
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

    private static StorageLayoutState CreateMovementEnabledState()
    {
        var state = CreateMovementDisabledState();
        state.Epoch = 2;
        state.MovementProtocolVersion = StorageLayout.CurrentMovementProtocolVersion;
        return state;
    }

    public enum FenceWriteStage
    {
        Begin,
        Publish,
    }

    public enum FenceFaultMode
    {
        BeforeCommit,
        CommittedLostAcknowledgement,
    }

    private sealed class InjectedFenceFaultException(string message) : Exception(message);

    private sealed class DurableLayoutStore
    {
        private FenceWriteStage? _stage;
        private FenceFaultMode _mode;

        public DurableLayoutStore(StorageLayoutState state)
        {
            DurableState = state.Copy();
        }

        public StorageLayoutState DurableState { get; private set; }

        public bool FaultWasInjected { get; private set; }

        public int WriteAttempts { get; private set; }

        public void Inject(FenceWriteStage stage, FenceFaultMode mode)
        {
            _stage = stage;
            _mode = mode;
        }

        public IPersistentState<StorageLayoutState> CreateActivation()
        {
            return new ActivationState(this, DurableState.Copy());
        }

        private Task WriteAsync(StorageLayoutState candidate)
        {
            WriteAttempts++;
            var stage = candidate.IndexSchemaEnablement is null
                ? FenceWriteStage.Publish
                : FenceWriteStage.Begin;
            var inject = !FaultWasInjected && _stage == stage;
            if (inject && _mode == FenceFaultMode.BeforeCommit)
            {
                FaultWasInjected = true;
                return Task.FromException(new InjectedFenceFaultException(stage.ToString()));
            }

            DurableState = candidate.Copy();
            if (inject)
            {
                FaultWasInjected = true;
                return Task.FromException(new InjectedFenceFaultException(stage.ToString()));
            }

            return Task.CompletedTask;
        }

        private sealed class ActivationState(
            DurableLayoutStore store,
            StorageLayoutState state) : IPersistentState<StorageLayoutState>
        {
            public StorageLayoutState State { get; set; } = state;

            public string? Etag { get; private set; }

            public bool RecordExists { get; private set; } = true;

            public Task ClearStateAsync() => throw new NotSupportedException();

            public Task ReadStateAsync() => Task.CompletedTask;

            public async Task WriteStateAsync()
            {
                await store.WriteAsync(State);
                RecordExists = true;
                Etag = store.WriteAttempts.ToString(
                    System.Globalization.CultureInfo.InvariantCulture);
            }
        }
    }
}
