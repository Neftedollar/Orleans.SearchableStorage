using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageJournalSegmentProtocolTests
{
    [Fact]
    public async Task ExactRetryIsIdempotentAndDoesNotWriteAgain()
    {
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state);
        var entry = CreateEntry(sequence: 1, writerEpoch: 1, Guid.NewGuid(), Guid.Empty);

        await grain.StoreAsync(entry, 0, Guid.Empty, 0, segmentCapacity: 2);
        await grain.StoreAsync(entry.Copy(), 0, Guid.Empty, 0, segmentCapacity: 2);

        state.WriteCount.Should().Be(1);
        state.State.Entries.Should().ContainSingle();
        StoragePersistenceStateEquality.JournalEntryEquals(state.State.Entries[0], entry)
            .Should().BeTrue();
    }

    [Fact]
    public async Task ExactPayloadCannotBypassASuppliedAdvancedCommitPoint()
    {
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state);
        var entry = CreateEntry(sequence: 1, writerEpoch: 1, Guid.NewGuid(), Guid.Empty);
        await grain.StoreAsync(entry, 0, Guid.Empty, 0, segmentCapacity: 2);

        Func<Task> retryAgainstAdvancedManifest = () => grain.StoreAsync(
            entry.Copy(),
            committedSequence: 1,
            committedOperationId: entry.OperationId,
            absoluteSegmentIndex: 0,
            segmentCapacity: 2);

        await retryAgainstAdvancedManifest.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*extend the durable manifest commit point*");
        state.WriteCount.Should().Be(1);
    }

    [Fact]
    public async Task AmbiguousWritePoisonsEverySubsequentOperationOnTheSameActivation()
    {
        var injected = new InvalidOperationException("Ambiguous journal write.");
        var state = new TestPersistentState<StorageJournalSegmentState>
        {
            WriteException = injected,
        };
        var grain = new StorageJournalSegmentGrain(state, requestDeactivation: static () => { });
        var entry = CreateEntry(sequence: 1, writerEpoch: 1, Guid.NewGuid(), Guid.Empty);

        Func<Task> firstStore = () => grain.StoreAsync(entry, 0, Guid.Empty, 0, segmentCapacity: 2);
        await firstStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);

        Func<Task> read = () => grain.ReadAsync();
        Func<Task> retryStore = () => grain.StoreAsync(entry, 0, Guid.Empty, 0, segmentCapacity: 2);
        Func<Task> retire = () => grain.RetireAsync(absoluteSegmentIndex: 0);
        await read.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous persistence write*");
        await retryStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous persistence write*");
        await retire.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous persistence write*");
        state.WriteCount.Should().Be(1);
    }

    [Fact]
    public async Task LowerWriterEpochCannotReplaceAnUncommittedEntry()
    {
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state);
        var original = CreateEntry(sequence: 1, writerEpoch: 2, Guid.NewGuid(), Guid.Empty);
        var stale = CreateEntry(sequence: 1, writerEpoch: 1, Guid.NewGuid(), Guid.Empty);
        await grain.StoreAsync(original, 0, Guid.Empty, 0, segmentCapacity: 1);

        Func<Task> replace = () => grain.StoreAsync(stale, 0, Guid.Empty, 0, segmentCapacity: 1);

        await replace.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*stale*");
        state.WriteCount.Should().Be(1);
        state.State.HighestWriterEpoch.Should().Be(2);
        state.State.Entries.Should().ContainSingle()
            .Which.OperationId.Should().Be(original.OperationId);
    }

    [Fact]
    public async Task SameWriterEpochWithDifferentOperationIdCannotReplaceAnUncommittedEntry()
    {
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state);
        var original = CreateEntry(sequence: 1, writerEpoch: 2, Guid.NewGuid(), Guid.Empty);
        var conflict = CreateEntry(sequence: 1, writerEpoch: 2, Guid.NewGuid(), Guid.Empty);
        await grain.StoreAsync(original, 0, Guid.Empty, 0, segmentCapacity: 1);

        Func<Task> replace = () => grain.StoreAsync(conflict, 0, Guid.Empty, 0, segmentCapacity: 1);

        await replace.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*higher writer epoch*");
        state.WriteCount.Should().Be(1);
        state.State.Entries.Should().ContainSingle()
            .Which.OperationId.Should().Be(original.OperationId);
    }

    [Fact]
    public async Task HigherWriterEpochReplacesOnlyTheUncommittedTailEntry()
    {
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state);
        var original = CreateEntry(sequence: 1, writerEpoch: 2, Guid.NewGuid(), Guid.Empty);
        var replacement = CreateEntry(sequence: 1, writerEpoch: 3, Guid.NewGuid(), Guid.Empty);
        await grain.StoreAsync(original, 0, Guid.Empty, 0, segmentCapacity: 1);

        await grain.StoreAsync(replacement, 0, Guid.Empty, 0, segmentCapacity: 1);

        state.WriteCount.Should().Be(2);
        state.State.HighestWriterEpoch.Should().Be(3);
        state.State.Entries.Should().ContainSingle()
            .Which.OperationId.Should().Be(replacement.OperationId);
    }

    [Fact]
    public async Task RingReuseFencesDelayedStoreAndRetirementForTheOldAbsoluteSegment()
    {
        var state = new TestPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state);
        var oldEntry = CreateEntry(sequence: 1, writerEpoch: 1, Guid.NewGuid(), Guid.Empty);
        await grain.StoreAsync(oldEntry, 0, Guid.Empty, 0, segmentCapacity: 1);
        await grain.RetireAsync(absoluteSegmentIndex: 0);

        var currentCommitId = Guid.NewGuid();
        var reusedEntry = CreateEntry(sequence: 5, writerEpoch: 2, Guid.NewGuid(), currentCommitId);
        await grain.StoreAsync(
            reusedEntry,
            committedSequence: 4,
            currentCommitId,
            absoluteSegmentIndex: 4,
            segmentCapacity: 1);

        await grain.RetireAsync(absoluteSegmentIndex: 0);
        var afterDelayedRetirement = await grain.ReadAsync();
        afterDelayedRetirement.AbsoluteSegmentIndex.Should().Be(4);
        afterDelayedRetirement.Tombstoned.Should().BeFalse();
        afterDelayedRetirement.Entries.Should().ContainSingle()
            .Which.OperationId.Should().Be(reusedEntry.OperationId);

        Func<Task> delayedStore = () => grain.StoreAsync(
            oldEntry,
            committedSequence: 0,
            Guid.Empty,
            absoluteSegmentIndex: 0,
            segmentCapacity: 1);
        await delayedStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*stale*");

        state.WriteCount.Should().Be(3);
        state.State.AbsoluteSegmentIndex.Should().Be(4);
        state.State.Tombstoned.Should().BeFalse();
    }

    private static StorageJournalEntry CreateEntry(
        long sequence,
        long writerEpoch,
        Guid operationId,
        Guid previousOperationId)
    {
        return new StorageJournalEntry
        {
            Sequence = sequence,
            WriterEpoch = writerEpoch,
            OperationId = operationId,
            PreviousOperationId = previousOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = $"record-{sequence}",
            Record = new StoredRecord
            {
                GrainId = GrainId.Create("journal-protocol", $"record-{sequence}"),
                Payload = [(byte)sequence],
                ETag = sequence.ToString(System.Globalization.CultureInfo.InvariantCulture),
                IndexEntries = [],
            },
            NextVersionAfter = checked(sequence + 1),
        };
    }
}
