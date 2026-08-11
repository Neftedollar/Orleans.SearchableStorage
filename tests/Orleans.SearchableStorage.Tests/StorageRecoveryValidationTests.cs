using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageRecoveryValidationTests
{
    private readonly MemoryStorageFixture _fixture;

    public StorageRecoveryValidationTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData(MalformedManifestCase.UnsupportedFormat, "is not supported")]
    [InlineData(MalformedManifestCase.ReplayLimitExceeded, "invalid persistence boundaries")]
    [InlineData(MalformedManifestCase.EmptyWithAdvancedVersion, "non-initial persistence state")]
    [InlineData(MalformedManifestCase.UnalignedPruneBoundary, "unaligned prune boundary")]
    [InlineData(MalformedManifestCase.PendingUsesWrongGenerationSlot, "PendingSnapshot' is invalid")]
    [InlineData(MalformedManifestCase.RetiringWithoutActive, "requires an active snapshot descriptor")]
    [InlineData(MalformedManifestCase.AbsentDescriptorContainsData, "Absent snapshot descriptor")]
    [InlineData(MalformedManifestCase.ActiveUsesWrongGenerationSlot, "ActiveSnapshot' is invalid")]
    [InlineData(MalformedManifestCase.ActiveVersionExceedsManifest, "does not match the manifest boundary")]
    [InlineData(MalformedManifestCase.ActiveBoundaryOperationMismatch, "does not match the manifest boundary")]
    [InlineData(MalformedManifestCase.PendingSkipsGeneration, "pending snapshot descriptor is inconsistent")]
    [InlineData(MalformedManifestCase.RetiringIsNotThePreviousGeneration, "retiring snapshot descriptor is inconsistent")]
    public async Task MalformedManifestIsRejectedBeforeRecovery(
        MalformedManifestCase malformedCase,
        string expectedMessage)
    {
        var partition = CreatePartition();
        var manifest = CreateEmptyManifest();
        MakeManifestMalformed(manifest, malformedCase);
        await WritePhysicalStateAsync("manifest", partition.GetGrainId(), manifest);

        await AssertRecoveryRejectedAsync(partition, expectedMessage);
    }

    [Theory]
    [InlineData(MalformedSnapshotCase.Missing, "missing, retired, or has mismatched identity")]
    [InlineData(MalformedSnapshotCase.Tombstoned, "missing, retired, or has mismatched identity")]
    [InlineData(MalformedSnapshotCase.DescriptorMismatch, "missing, retired, or has mismatched identity")]
    [InlineData(MalformedSnapshotCase.InvalidRecordVersion, "contains an invalid record version")]
    public async Task MalformedCommittedSnapshotIsRejected(
        MalformedSnapshotCase malformedCase,
        string expectedMessage)
    {
        var partition = CreatePartition();
        var operationId = Guid.NewGuid();
        var descriptor = CreateDescriptor(
            slot: 0,
            generation: 1,
            sequence: 1,
            operationId,
            nextVersion: 2);
        var manifest = CreateCommittedManifest(
            committedSequence: 1,
            operationId,
            nextVersion: 2,
            writerEpoch: 1);
        manifest.ActiveSnapshot = descriptor;
        manifest.SnapshotGenerationHighWatermark = 1;
        manifest.SnapshotSequence = 1;
        await WritePhysicalStateAsync("manifest", partition.GetGrainId(), manifest);

        if (malformedCase != MalformedSnapshotCase.Missing)
        {
            var snapshot = CreateSnapshot(descriptor);
            switch (malformedCase)
            {
                case MalformedSnapshotCase.Tombstoned:
                    snapshot.Tombstoned = true;
                    snapshot.Records.Clear();
                    break;
                case MalformedSnapshotCase.DescriptorMismatch:
                    snapshot.SnapshotId = Guid.NewGuid();
                    break;
                case MalformedSnapshotCase.InvalidRecordVersion:
                    snapshot.Records["record"] = CreateRecord("record", "2");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(malformedCase), malformedCase, null);
            }

            var snapshotGrain = GetSnapshot(partition, slot: 0);
            await WritePhysicalStateAsync("snapshot", snapshotGrain.GetGrainId(), snapshot);
        }

        await AssertRecoveryRejectedAsync(partition, expectedMessage);
    }

    [Theory]
    [InlineData(MalformedJournalCase.MissingSegment, "segment 0 is missing, retired, or invalid")]
    [InlineData(MalformedJournalCase.MissingEntry, "journal entry 1 is missing")]
    [InlineData(MalformedJournalCase.DuplicateSequence, "contains invalid entry boundaries")]
    [InlineData(MalformedJournalCase.BrokenPredecessor, "Journal entry 2 is invalid")]
    [InlineData(MalformedJournalCase.RepeatedOperationId, "Journal entry 3 is invalid")]
    [InlineData(MalformedJournalCase.InvalidNextVersion, "does not contain the next record version")]
    [InlineData(MalformedJournalCase.WriterEpochBeyondManifest, "segment 0 is missing, retired, or invalid")]
    [InlineData(MalformedJournalCase.EntryOutsideSegment, "contains invalid entry boundaries")]
    [InlineData(MalformedJournalCase.UnorderedSequences, "contains invalid entry boundaries")]
    [InlineData(MalformedJournalCase.MultipleUncommittedEntries, "contains an invalid uncommitted tail")]
    [InlineData(MalformedJournalCase.InconsistentHighestWriterEpoch, "contains inconsistent writer-epoch metadata")]
    public async Task MalformedCommittedJournalChainIsRejected(
        MalformedJournalCase malformedCase,
        string expectedMessage)
    {
        var partition = CreatePartition();
        var firstOperationId = Guid.NewGuid();
        var secondOperationId = Guid.NewGuid();
        var thirdOperationId = Guid.NewGuid();
        var entries = new List<StorageJournalEntry>();
        List<StorageJournalEntry>? secondSegmentEntries = null;
        var segmentCapacity = 2;
        var maximumReplayEntries = 4;
        long? highestWriterEpochOverride = null;
        StoragePartitionManifestState manifest;

        switch (malformedCase)
        {
            case MalformedJournalCase.MissingSegment:
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 1);
                break;
            case MalformedJournalCase.MissingEntry:
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 2);
                entries.Add(CreateJournalEntry(
                    2,
                    2,
                    secondOperationId,
                    firstOperationId,
                    "uncommitted-tail",
                    "2",
                    3));
                break;
            case MalformedJournalCase.DuplicateSequence:
            {
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 1);
                var entry = CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 2);
                entries.Add(entry);
                entries.Add(entry.Copy());
                break;
            }
            case MalformedJournalCase.BrokenPredecessor:
                manifest = CreateCommittedManifest(2, secondOperationId, nextVersion: 3, writerEpoch: 1);
                entries.Add(CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 2));
                entries.Add(CreateJournalEntry(2, 1, secondOperationId, Guid.NewGuid(), "second", "2", 3));
                break;
            case MalformedJournalCase.RepeatedOperationId:
                manifest = CreateCommittedManifest(3, firstOperationId, nextVersion: 4, writerEpoch: 1);
                entries.Add(CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 2));
                entries.Add(CreateJournalEntry(2, 1, secondOperationId, firstOperationId, "second", "2", 3));
                secondSegmentEntries =
                [
                    CreateJournalEntry(3, 1, firstOperationId, secondOperationId, "third", "3", 4),
                ];
                break;
            case MalformedJournalCase.InvalidNextVersion:
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 1);
                entries.Add(CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 3));
                break;
            case MalformedJournalCase.WriterEpochBeyondManifest:
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 1);
                entries.Add(CreateJournalEntry(1, 2, firstOperationId, Guid.Empty, "first", "1", 2));
                break;
            case MalformedJournalCase.EntryOutsideSegment:
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 1);
                entries.Add(CreateJournalEntry(3, 1, firstOperationId, secondOperationId, "outside", "1", 2));
                break;
            case MalformedJournalCase.UnorderedSequences:
                manifest = CreateCommittedManifest(2, secondOperationId, nextVersion: 3, writerEpoch: 1);
                entries.Add(CreateJournalEntry(2, 1, secondOperationId, firstOperationId, "second", "2", 3));
                entries.Add(CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 2));
                break;
            case MalformedJournalCase.MultipleUncommittedEntries:
                segmentCapacity = 4;
                maximumReplayEntries = 8;
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 2);
                manifest.JournalSegmentCapacity = segmentCapacity;
                manifest.MaximumJournalReplayEntries = maximumReplayEntries;
                entries.Add(CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 2));
                entries.Add(CreateJournalEntry(2, 2, secondOperationId, firstOperationId, "second", "2", 3));
                entries.Add(CreateJournalEntry(3, 2, thirdOperationId, secondOperationId, "third", "3", 4));
                break;
            case MalformedJournalCase.InconsistentHighestWriterEpoch:
                manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 2);
                entries.Add(CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 2));
                highestWriterEpochOverride = 2;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(malformedCase), malformedCase, null);
        }

        await WritePhysicalStateAsync("manifest", partition.GetGrainId(), manifest);
        if (malformedCase != MalformedJournalCase.MissingSegment)
        {
            var segment = CreateJournalSegment(0, segmentCapacity, entries);
            if (highestWriterEpochOverride is not null)
            {
                segment.HighestWriterEpoch = highestWriterEpochOverride.Value;
            }

            var journal = GetJournal(partition, 0, segmentCapacity, maximumReplayEntries);
            await WritePhysicalStateAsync("journal", journal.GetGrainId(), segment);
            if (secondSegmentEntries is not null)
            {
                var secondSegment = CreateJournalSegment(1, segmentCapacity, secondSegmentEntries);
                var secondJournal = GetJournal(partition, 1, segmentCapacity, maximumReplayEntries);
                await WritePhysicalStateAsync("journal", secondJournal.GetGrainId(), secondSegment);
            }
        }

        await AssertRecoveryRejectedAsync(partition, expectedMessage);
    }

    [Fact]
    public async Task RecoveryAcceptsOneUncommittedTailEntryWithoutMakingItVisible()
    {
        var partition = CreatePartition();
        var firstOperationId = Guid.NewGuid();
        var orphanOperationId = Guid.NewGuid();
        var manifest = CreateCommittedManifest(1, firstOperationId, nextVersion: 2, writerEpoch: 2);
        var segment = CreateJournalSegment(
            absoluteSegmentIndex: 0,
            capacity: 2,
            [
                CreateJournalEntry(1, 1, firstOperationId, Guid.Empty, "first", "1", 2),
                CreateJournalEntry(2, 2, orphanOperationId, firstOperationId, "orphan", "2", 3),
            ]);
        var journal = GetJournal(partition, absoluteSegmentIndex: 0, segmentCapacity: 2, maximumReplayEntries: 4);
        await WritePhysicalStateAsync("manifest", partition.GetGrainId(), manifest);
        await WritePhysicalStateAsync("journal", journal.GetGrainId(), segment);

        var info = await partition.GetPersistenceInfoAsync();
        var committed = await partition.ReadAsync("first");
        var orphan = await partition.ReadAsync("orphan");

        info.CommittedSequence.Should().Be(1);
        info.WriterEpoch.Should().Be(2);
        committed.Found.Should().BeTrue();
        committed.ETag.Should().Be("1");
        orphan.Found.Should().BeFalse();
    }

    private IStoragePartitionGrain CreatePartition()
    {
        return _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey($"malformed-{Guid.NewGuid():N}", partitionIndex: 0));
    }

    private IStorageSnapshotGrain GetSnapshot(IStoragePartitionGrain partition, int slot)
    {
        return _fixture.Cluster.GrainFactory.GetGrain<IStorageSnapshotGrain>(
            StoragePersistence.CreateSnapshotSlotKey(partition.GetPrimaryKeyString(), slot));
    }

    private IStorageJournalSegmentGrain GetJournal(
        IStoragePartitionGrain partition,
        long absoluteSegmentIndex,
        int segmentCapacity,
        int maximumReplayEntries)
    {
        var slotCount = StoragePersistence.GetJournalSlotCount(maximumReplayEntries, segmentCapacity);
        var slot = StoragePersistence.GetJournalSlotIndex(
            absoluteSegmentIndex,
            maximumReplayEntries,
            segmentCapacity);
        return _fixture.Cluster.GrainFactory.GetGrain<IStorageJournalSegmentGrain>(
            StoragePersistence.CreateJournalSlotKey(partition.GetPrimaryKeyString(), slot, slotCount));
    }

    private async Task WritePhysicalStateAsync<T>(string stateName, GrainId grainId, T value)
    {
        var silo = Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            MemoryStorageFixture.InnerPhysicalStorageProviderName);
        await storage.WriteStateAsync(stateName, grainId, new GrainState<T> { State = value });
    }

    private static async Task AssertRecoveryRejectedAsync(
        IStoragePartitionGrain partition,
        string expectedMessage)
    {
        Func<Task> activate = async () => await partition.GetPersistenceInfoAsync();

        var exception = await activate.Should().ThrowAsync<Exception>();
        exception.Which.ToString().Should().Contain(expectedMessage);
    }

    private static StoragePartitionManifestState CreateEmptyManifest()
    {
        return new StoragePartitionManifestState
        {
            Initialized = true,
            PersistenceFormatVersion = StoragePersistence.MovementPersistenceFormatVersion,
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            NextVersion = 1,
        };
    }

    private static StoragePartitionManifestState CreateCommittedManifest(
        long committedSequence,
        Guid operationId,
        long nextVersion,
        long writerEpoch)
    {
        var manifest = CreateEmptyManifest();
        manifest.WriterEpoch = writerEpoch;
        manifest.CommittedSequence = committedSequence;
        manifest.CommittedOperationId = operationId;
        manifest.NextVersion = nextVersion;
        return manifest;
    }

    private static void MakeManifestMalformed(
        StoragePartitionManifestState manifest,
        MalformedManifestCase malformedCase)
    {
        switch (malformedCase)
        {
            case MalformedManifestCase.UnsupportedFormat:
                manifest.PersistenceFormatVersion = checked(
                    StoragePersistence.CurrentPersistenceFormatVersion + 1);
                break;
            case MalformedManifestCase.ReplayLimitExceeded:
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 5;
                manifest.CommittedOperationId = Guid.NewGuid();
                manifest.NextVersion = 2;
                break;
            case MalformedManifestCase.EmptyWithAdvancedVersion:
                manifest.NextVersion = 2;
                break;
            case MalformedManifestCase.UnalignedPruneBoundary:
            {
                var operationId = Guid.NewGuid();
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 2;
                manifest.CommittedOperationId = operationId;
                manifest.NextVersion = 2;
                manifest.ActiveSnapshot = CreateDescriptor(0, 1, 2, operationId, 2);
                manifest.SnapshotGenerationHighWatermark = 1;
                manifest.SnapshotSequence = 2;
                manifest.PrunedSequence = 1;
                break;
            }
            case MalformedManifestCase.PendingUsesWrongGenerationSlot:
            {
                var activeOperationId = Guid.NewGuid();
                var pendingOperationId = Guid.NewGuid();
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 2;
                manifest.CommittedOperationId = pendingOperationId;
                manifest.NextVersion = 3;
                manifest.ActiveSnapshot = CreateDescriptor(0, 1, 1, activeOperationId, 2);
                manifest.PendingSnapshot = CreateDescriptor(0, 2, 2, pendingOperationId, 3);
                manifest.SnapshotGenerationHighWatermark = 2;
                manifest.SnapshotSequence = 1;
                break;
            }
            case MalformedManifestCase.RetiringWithoutActive:
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 1;
                manifest.CommittedOperationId = Guid.NewGuid();
                manifest.NextVersion = 2;
                manifest.RetiringSnapshot = CreateDescriptor(
                    0,
                    1,
                    1,
                    manifest.CommittedOperationId,
                    2);
                break;
            case MalformedManifestCase.AbsentDescriptorContainsData:
                manifest.ActiveSnapshot.Generation = 1;
                break;
            case MalformedManifestCase.ActiveUsesWrongGenerationSlot:
            {
                var operationId = Guid.NewGuid();
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 1;
                manifest.CommittedOperationId = operationId;
                manifest.NextVersion = 2;
                manifest.ActiveSnapshot = CreateDescriptor(1, 1, 1, operationId, 2);
                manifest.SnapshotGenerationHighWatermark = 1;
                manifest.SnapshotSequence = 1;
                break;
            }
            case MalformedManifestCase.ActiveVersionExceedsManifest:
            {
                var operationId = Guid.NewGuid();
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 2;
                manifest.CommittedOperationId = operationId;
                manifest.NextVersion = 2;
                manifest.ActiveSnapshot = CreateDescriptor(0, 1, 2, operationId, 3);
                manifest.SnapshotGenerationHighWatermark = 1;
                manifest.SnapshotSequence = 2;
                break;
            }
            case MalformedManifestCase.ActiveBoundaryOperationMismatch:
            {
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 1;
                manifest.CommittedOperationId = Guid.NewGuid();
                manifest.NextVersion = 2;
                manifest.ActiveSnapshot = CreateDescriptor(0, 1, 1, Guid.NewGuid(), 2);
                manifest.SnapshotGenerationHighWatermark = 1;
                manifest.SnapshotSequence = 1;
                break;
            }
            case MalformedManifestCase.PendingSkipsGeneration:
            {
                var activeOperationId = Guid.NewGuid();
                var pendingOperationId = Guid.NewGuid();
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 4;
                manifest.CommittedOperationId = pendingOperationId;
                manifest.NextVersion = 5;
                manifest.ActiveSnapshot = CreateDescriptor(0, 1, 1, activeOperationId, 2);
                manifest.PendingSnapshot = CreateDescriptor(1, 4, 4, pendingOperationId, 5);
                manifest.SnapshotGenerationHighWatermark = 4;
                manifest.SnapshotSequence = 1;
                break;
            }
            case MalformedManifestCase.RetiringIsNotThePreviousGeneration:
            {
                var activeOperationId = Guid.NewGuid();
                manifest.WriterEpoch = 1;
                manifest.CommittedSequence = 4;
                manifest.CommittedOperationId = activeOperationId;
                manifest.NextVersion = 5;
                manifest.ActiveSnapshot = CreateDescriptor(1, 4, 4, activeOperationId, 5);
                manifest.RetiringSnapshot = CreateDescriptor(0, 1, 1, Guid.NewGuid(), 2);
                manifest.SnapshotGenerationHighWatermark = 4;
                manifest.SnapshotSequence = 4;
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(malformedCase), malformedCase, null);
        }
    }

    private static StorageSnapshotDescriptor CreateDescriptor(
        int slot,
        long generation,
        long sequence,
        Guid operationId,
        long nextVersion)
    {
        return new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = slot,
            Generation = generation,
            SnapshotId = Guid.NewGuid(),
            Sequence = sequence,
            OperationId = operationId,
            NextVersion = nextVersion,
        };
    }

    private static StorageSnapshotState CreateSnapshot(StorageSnapshotDescriptor descriptor)
    {
        return new StorageSnapshotState
        {
            Initialized = true,
            Slot = descriptor.Slot,
            Generation = descriptor.Generation,
            SnapshotId = descriptor.SnapshotId,
            Sequence = descriptor.Sequence,
            OperationId = descriptor.OperationId,
            NextVersion = descriptor.NextVersion,
            Records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["record"] = CreateRecord("record", "1"),
            },
        };
    }

    private static StorageJournalEntry CreateJournalEntry(
        long sequence,
        long writerEpoch,
        Guid operationId,
        Guid previousOperationId,
        string recordKey,
        string etag,
        long nextVersionAfter)
    {
        return new StorageJournalEntry
        {
            Sequence = sequence,
            WriterEpoch = writerEpoch,
            OperationId = operationId,
            PreviousOperationId = previousOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = recordKey,
            Record = CreateRecord(recordKey, etag),
            NextVersionAfter = nextVersionAfter,
        };
    }

    private static StorageJournalSegmentState CreateJournalSegment(
        long absoluteSegmentIndex,
        int capacity,
        List<StorageJournalEntry> entries)
    {
        return new StorageJournalSegmentState
        {
            Initialized = true,
            Capacity = capacity,
            AbsoluteSegmentIndex = absoluteSegmentIndex,
            HighestWriterEpoch = Math.Max(
                1,
                entries.Select(static entry => entry.WriterEpoch).DefaultIfEmpty().Max()),
            Entries = entries,
        };
    }

    private static StoredRecord CreateRecord(string recordKey, string etag)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("recovery-validation", recordKey),
            Payload = [1, 2, 3],
            ETag = etag,
            IndexEntries = [],
        };
    }
}

public enum MalformedManifestCase
{
    UnsupportedFormat = 0,
    ReplayLimitExceeded = 1,
    EmptyWithAdvancedVersion = 2,
    UnalignedPruneBoundary = 3,
    PendingUsesWrongGenerationSlot = 4,
    RetiringWithoutActive = 5,
    AbsentDescriptorContainsData = 6,
    ActiveUsesWrongGenerationSlot = 7,
    ActiveVersionExceedsManifest = 8,
    ActiveBoundaryOperationMismatch = 9,
    PendingSkipsGeneration = 10,
    RetiringIsNotThePreviousGeneration = 11,
}

public enum MalformedSnapshotCase
{
    Missing = 0,
    Tombstoned = 1,
    DescriptorMismatch = 2,
    InvalidRecordVersion = 3,
}

public enum MalformedJournalCase
{
    MissingSegment = 0,
    MissingEntry = 1,
    DuplicateSequence = 2,
    BrokenPredecessor = 3,
    InvalidNextVersion = 4,
    WriterEpochBeyondManifest = 5,
    RepeatedOperationId = 6,
    EntryOutsideSegment = 7,
    UnorderedSequences = 8,
    MultipleUncommittedEntries = 9,
    InconsistentHighestWriterEpoch = 10,
}
