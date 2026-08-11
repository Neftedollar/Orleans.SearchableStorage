using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
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

    [Fact]
    public async Task LosslessSnapshotLostAcknowledgementRecoversAsAnExactBinaryRetry()
    {
        var injected = new InvalidOperationException("Lost lossless snapshot acknowledgement.");
        var snapshot = CreateLosslessSnapshot();
        var state = new TestPersistentState<StorageSnapshotState>
        {
            WriteException = injected,
        };
        var firstActivation = new StorageSnapshotGrain(
            state,
            requestDeactivation: static () => { });
        var reverseInsertion = StorageSnapshotFactory.DecodeRecords(
                snapshot,
                StoragePersistence.CurrentPersistenceFormatVersion)
            .Reverse()
            .ToDictionary(
                static pair => pair.Key,
                static pair => pair.Value,
                StringComparer.Ordinal);
        var regenerated = StorageSnapshotFactory.Create(
            StorageSnapshotDescriptor.FromSnapshot(snapshot),
            reverseInsertion,
            StoragePersistence.CurrentPersistenceFormatVersion);
        StoragePersistenceStateEquality.SnapshotEquals(snapshot, regenerated).Should().BeTrue();

        Func<Task> firstStore = () => firstActivation.StoreAsync(snapshot);
        await firstStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);
        state.LastWriteState.Should().NotBeNull();

        state.State = state.LastWriteState!;
        state.WriteException = null;
        var recoveredActivation = new StorageSnapshotGrain(state);
        await recoveredActivation.StoreAsync(snapshot.Copy());
        state.WriteCount.Should().Be(1);
        StoragePersistenceStateEquality.SnapshotEquals(state.State, snapshot).Should().BeTrue();

        var conflict = snapshot.Copy();
        conflict.LosslessRecords[0].Record.Payload[0]++;
        Func<Task> conflictingStore = () => recoveredActivation.StoreAsync(conflict);
        await conflictingStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*different metadata or payload*");
        state.WriteCount.Should().Be(1);
    }

    [Fact]
    public void SnapshotRecordEncodingRejectsUnknownMixedUnorderedAndV3BinaryPayloads()
    {
        var lossless = CreateLosslessSnapshot();
        var unknown = lossless.Copy();
        unknown.RecordEncodingVersion = 2;
        var mixed = lossless.Copy();
        mixed.Records["legacy"] = StoragePersistenceStateCopy.CopyRecord(
            CreateSnapshot(generation: 2, sequence: 2).Records["record"])!;
        var unordered = lossless.Copy();
        unordered.LosslessRecords.Reverse();
        var impossibleLegacy = CreateSnapshot(generation: 2, sequence: 2);
        impossibleLegacy.NextVersion = 10;

        Action validateUnknown = () => StorageSnapshotFactory.ValidatePayload(unknown);
        Action validateMixed = () => StorageSnapshotFactory.ValidatePayload(mixed);
        Action validateUnordered = () => StorageSnapshotFactory.ValidatePayload(unordered);
        Action validateImpossibleLegacy = () =>
            StorageSnapshotFactory.ValidatePayload(impossibleLegacy);
        Action recoverBinaryFromV3 = () => StorageSnapshotFactory.DecodeRecords(
            lossless,
            StoragePersistence.PreviousPersistenceFormatVersion);

        validateUnknown.Should().Throw<InvalidOperationException>();
        validateMixed.Should().Throw<InvalidOperationException>();
        validateUnordered.Should().Throw<InvalidOperationException>();
        validateImpossibleLegacy.Should().Throw<InvalidOperationException>();
        recoverBinaryFromV3.Should().Throw<InvalidOperationException>();
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

    [Fact]
    public async Task LosslessRetirementResetsTheTombstoneAndAllowsHigherGenerationReuse()
    {
        var state = new TestPersistentState<StorageSnapshotState>();
        var grain = new StorageSnapshotGrain(state);
        var current = CreateLosslessSnapshot();
        await grain.StoreAsync(current);

        await grain.RetireAsync(StorageSnapshotDescriptor.FromSnapshot(current));
        state.State.Tombstoned.Should().BeTrue();
        state.State.RecordEncodingVersion.Should()
            .Be(StorageSnapshotFactory.LegacyRecordEncodingVersion);
        state.State.Records.Should().BeEmpty();
        state.State.LosslessRecords.Should().BeEmpty();

        var nextDescriptor = new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = current.Slot,
            Generation = checked(current.Generation + 1),
            SnapshotId = Guid.NewGuid(),
            Sequence = checked(current.Sequence + 1),
            OperationId = Guid.NewGuid(),
            NextVersion = current.NextVersion,
        };
        var next = StorageSnapshotFactory.Create(
            nextDescriptor,
            StorageSnapshotFactory.DecodeRecords(
                current,
                StoragePersistence.CurrentPersistenceFormatVersion),
            StoragePersistence.CurrentPersistenceFormatVersion);
        await grain.StoreAsync(next);

        state.State.Tombstoned.Should().BeFalse();
        state.State.Generation.Should().Be(nextDescriptor.Generation);
        state.State.RecordEncodingVersion.Should()
            .Be(StorageSnapshotFactory.LosslessRecordEncodingVersion);
        state.State.LosslessRecords.Should().HaveCount(2);
        state.WriteCount.Should().Be(3);
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

    private static StorageSnapshotState CreateLosslessSnapshot()
    {
        var identity = CreateSnapshot(generation: 2, sequence: 2);
        var descriptor = StorageSnapshotDescriptor.FromSnapshot(identity);
        var seed = identity.Records["record"];
        var first = new StoredRecord
        {
            GrainId = seed.GrainId,
            Payload = [.. seed.Payload],
            ETag = "1",
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = "scope/\ud800",
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create("value/\udc00"),
                },
            ],
        };
        var second = new StoredRecord
        {
            GrainId = seed.GrainId,
            Payload = [.. seed.Payload],
            ETag = "2",
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = "scope/\ud800",
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create("value/\udc00"),
                },
            ],
        };
        return StorageSnapshotFactory.Create(
            descriptor,
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["a-\ud800"] = first,
                ["b-\udc00"] = second,
            },
            StoragePersistence.CurrentPersistenceFormatVersion);
    }
}
