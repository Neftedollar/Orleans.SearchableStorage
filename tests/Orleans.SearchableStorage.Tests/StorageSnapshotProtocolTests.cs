using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageSnapshotProtocolTests
{
    [Fact]
    public async Task ExactRetryIsIdempotentAndDoesNotWriteAgain()
    {
        var state = new TestPersistentState<StorageSnapshotState>();
        var grain = new StorageSnapshotGrain(state);
        var snapshot = CreateSnapshot(generation: 2, sequence: 2);

        await grain.StoreAsync(snapshot);
        await grain.StoreAsync(snapshot.Copy());

        state.WriteCount.Should().Be(1);
        StoragePersistenceStateEquality.SnapshotEquals(state.State, snapshot).Should().BeTrue();
    }

    [Fact]
    public async Task AmbiguousWritePoisonsEverySubsequentOperationOnTheSameActivation()
    {
        var injected = new InvalidOperationException("Ambiguous snapshot write.");
        var state = new TestPersistentState<StorageSnapshotState>
        {
            WriteException = injected,
        };
        var grain = new StorageSnapshotGrain(state, requestDeactivation: static () => { });
        var snapshot = CreateSnapshot(generation: 2, sequence: 2);

        Func<Task> firstStore = () => grain.StoreAsync(snapshot);
        await firstStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);

        Func<Task> read = () => grain.ReadAsync();
        Func<Task> retryStore = () => grain.StoreAsync(snapshot);
        Func<Task> retire = () => grain.RetireAsync(StorageSnapshotDescriptor.FromSnapshot(snapshot));
        await read.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous persistence write*");
        await retryStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous persistence write*");
        await retire.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous persistence write*");
        state.WriteCount.Should().Be(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SameGenerationWithDifferentIdentityOrContentIsRejected(bool changeIdentity)
    {
        var state = new TestPersistentState<StorageSnapshotState>();
        var grain = new StorageSnapshotGrain(state);
        var snapshot = CreateSnapshot(generation: 2, sequence: 2);
        await grain.StoreAsync(snapshot);
        var conflict = snapshot.Copy();
        if (changeIdentity)
        {
            conflict.SnapshotId = Guid.NewGuid();
        }
        else
        {
            conflict.Records["record"].Payload[0]++;
        }

        Func<Task> store = () => grain.StoreAsync(conflict);

        await store.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*different metadata or payload*");
        state.WriteCount.Should().Be(1);
        StoragePersistenceStateEquality.SnapshotEquals(state.State, snapshot).Should().BeTrue();
    }

    [Fact]
    public async Task LowerGenerationIsRejectedWithoutChangingTheLiveSnapshot()
    {
        var state = new TestPersistentState<StorageSnapshotState>();
        var grain = new StorageSnapshotGrain(state);
        var current = CreateSnapshot(generation: 2, sequence: 2);
        await grain.StoreAsync(current);

        Func<Task> store = () => grain.StoreAsync(CreateSnapshot(generation: 1, sequence: 1));

        await store.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*stale*");
        state.WriteCount.Should().Be(1);
        StoragePersistenceStateEquality.SnapshotEquals(state.State, current).Should().BeTrue();
    }

    [Fact]
    public async Task HigherGenerationRequiresRetirementAndDelayedRetirementCannotEraseReuse()
    {
        var state = new TestPersistentState<StorageSnapshotState>();
        var grain = new StorageSnapshotGrain(state);
        var previous = CreateSnapshot(generation: 2, sequence: 2);
        var next = CreateSnapshot(generation: 3, sequence: 3);
        await grain.StoreAsync(previous);

        Func<Task> overwriteLive = () => grain.StoreAsync(next);
        await overwriteLive.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*must be retired*");

        var previousDescriptor = StorageSnapshotDescriptor.FromSnapshot(previous);
        await grain.RetireAsync(previousDescriptor);
        await grain.RetireAsync(previousDescriptor.Copy());
        await grain.StoreAsync(next);
        await grain.RetireAsync(previousDescriptor);

        var current = await grain.ReadAsync();
        current.Generation.Should().Be(3);
        current.Tombstoned.Should().BeFalse();
        current.Records.Should().ContainKey("record");
        state.WriteCount.Should().Be(3);

        var futureDescriptor = StorageSnapshotDescriptor.FromSnapshot(next);
        futureDescriptor.Generation++;
        Func<Task> retireFuture = () => grain.RetireAsync(futureDescriptor);
        await retireFuture.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot skip*");
        state.State.Generation.Should().Be(3);
        state.State.Tombstoned.Should().BeFalse();
    }

    [Fact]
    public async Task SameGenerationRetirementRequiresExactIdentityMetadata()
    {
        var state = new TestPersistentState<StorageSnapshotState>();
        var grain = new StorageSnapshotGrain(state);
        var snapshot = CreateSnapshot(generation: 2, sequence: 2);
        await grain.StoreAsync(snapshot);
        var mismatched = StorageSnapshotDescriptor.FromSnapshot(snapshot);
        mismatched.SnapshotId = Guid.NewGuid();

        Func<Task> retire = () => grain.RetireAsync(mismatched);

        await retire.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*mismatched identity metadata*");
        state.WriteCount.Should().Be(1);
        state.State.Tombstoned.Should().BeFalse();
    }

    private static StorageSnapshotState CreateSnapshot(long generation, long sequence)
    {
        return new StorageSnapshotState
        {
            Initialized = true,
            Slot = 0,
            Generation = generation,
            SnapshotId = Guid.NewGuid(),
            Sequence = sequence,
            OperationId = Guid.NewGuid(),
            NextVersion = checked(sequence + 1),
            Records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["record"] = new StoredRecord
                {
                    GrainId = GrainId.Create("snapshot-protocol", "record"),
                    Payload = [1, 2, 3],
                    ETag = "1",
                    IndexEntries = [],
                },
            },
        };
    }
}
