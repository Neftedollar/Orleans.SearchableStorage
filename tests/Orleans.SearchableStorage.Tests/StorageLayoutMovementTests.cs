using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageLayoutMovementTests
{
    [Fact]
    public async Task InterleavedRoutingReadPublishesOnlyTheLastDurableLayoutSnapshot()
    {
        const string providerName = "durable-interleaved-layout";
        var enablementId = Guid.NewGuid();
        var state = new BlockingLayoutPersistentState
        {
            State = new StorageLayoutState
            {
                Initialized = true,
                FormatVersion = StorageLayout.MovementFormatVersion,
                ProviderName = providerName,
                PartitionCount = 1,
                JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
                MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
                VirtualSlotCount = 1,
                SlotAssignments = [0],
                Epoch = 1,
                MovementEnablement = new StorageMovementEnableIntent
                {
                    EnablementId = enablementId,
                    SourceEpoch = 1,
                    PlannedEpoch = 2,
                    Owners = [0],
                    NextOwnerIndex = 1,
                },
            },
        };
        var grain = new StorageLayoutGrain(
            state,
            providerName,
            requestDeactivation: static () => { });
        var before = await grain.GetCurrentLayoutAsync();

        var advance = grain.AdvanceMovementEnablementAsync(enablementId);
        await state.WriteStarted;
        var whileWriteIsBlocked = await grain.GetCurrentLayoutAsync();

        before.Should().NotBeNull();
        whileWriteIsBlocked.Should().BeSameAs(before);
        whileWriteIsBlocked!.Epoch.Should().Be(1);
        whileWriteIsBlocked.MovementState.Should().Be(SearchableStorageMovementState.Enabling);

        state.ReleaseWrite();
        var committed = await advance;
        var after = await grain.GetCurrentLayoutAsync();

        committed.Epoch.Should().Be(2);
        committed.MovementState.Should().Be(SearchableStorageMovementState.Enabled);
        after.Should().BeSameAs(committed);
        after.Should().NotBeSameAs(before);
    }

    private sealed class BlockingLayoutPersistentState : IPersistentState<StorageLayoutState>
    {
        private readonly TaskCompletionSource _writeStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _allowWrite = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public StorageLayoutState State { get; set; } = new();

        public string? Etag { get; private set; }

        public bool RecordExists { get; private set; }

        public Task WriteStarted => _writeStarted.Task;

        public Task ClearStateAsync() => throw new NotSupportedException();

        public Task ReadStateAsync() => Task.CompletedTask;

        public async Task WriteStateAsync()
        {
            _writeStarted.TrySetResult();
            await _allowWrite.Task;
            RecordExists = true;
            Etag = "1";
        }

        public void ReleaseWrite() => _allowWrite.TrySetResult();
    }
}
