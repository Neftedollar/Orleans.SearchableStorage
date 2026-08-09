using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePersistenceTests
{
    [Theory]
    [InlineData(1, 64, 3)]
    [InlineData(64, 64, 3)]
    [InlineData(65, 64, 4)]
    [InlineData(4_096, 64, 66)]
    [InlineData(7, 3, 5)]
    public void JournalSlotCountRoundsUpReplaySegmentsAndAddsSafetySlots(
        int maximumReplayEntries,
        int segmentCapacity,
        int expectedSlotCount)
    {
        StoragePersistence.GetJournalSlotCount(maximumReplayEntries, segmentCapacity)
            .Should().Be(expectedSlotCount);
    }

    [Fact]
    public void JournalSlotCountRejectsUnaddressableConfigurations()
    {
        StoragePersistence.GetJournalSlotCount(int.MaxValue - 2, segmentCapacity: 1)
            .Should().Be(int.MaxValue);

        Action createTooManySlots = () => StoragePersistence.GetJournalSlotCount(
            int.MaxValue,
            segmentCapacity: 1);
        Action validateTooManySlots = () => StoragePersistence.ValidateOptions(
            journalSegmentCapacity: 1,
            maxReplayEntries: int.MaxValue);

        createTooManySlots.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("maxReplayEntries");
        validateTooManySlots.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("maxReplayEntries");
    }

    [Theory]
    [InlineData(4_096, 64, 123)]
    [InlineData(7, 3, 17)]
    [InlineData(1, 10, 100)]
    public void JournalSlotsFirstAliasAtTheExactRingDistance(
        int maximumReplayEntries,
        int segmentCapacity,
        long absoluteSegmentIndex)
    {
        var slotCount = StoragePersistence.GetJournalSlotCount(maximumReplayEntries, segmentCapacity);
        var slot = StoragePersistence.GetJournalSlotIndex(
            absoluteSegmentIndex,
            maximumReplayEntries,
            segmentCapacity);

        for (var distance = 1; distance < slotCount; distance++)
        {
            StoragePersistence.GetJournalSlotIndex(
                    absoluteSegmentIndex + distance,
                    maximumReplayEntries,
                    segmentCapacity)
                .Should().NotBe(slot);
        }

        StoragePersistence.GetJournalSlotIndex(
                absoluteSegmentIndex + slotCount,
                maximumReplayEntries,
                segmentCapacity)
            .Should().Be(slot);
    }

    [Theory]
    [InlineData(4_096, 64, 0)]
    [InlineData(7, 3, 29)]
    [InlineData(1, 10, 101)]
    public void JournalRingDoesNotAliasWithinTheMaximumLiveWindow(
        int maximumReplayEntries,
        int segmentCapacity,
        long firstAbsoluteSegmentIndex)
    {
        var maximumLiveSegmentCount = StoragePersistence.GetJournalSlotCount(
            maximumReplayEntries,
            segmentCapacity);
        var slots = Enumerable.Range(0, maximumLiveSegmentCount)
            .Select(offset => StoragePersistence.GetJournalSlotIndex(
                firstAbsoluteSegmentIndex + offset,
                maximumReplayEntries,
                segmentCapacity))
            .ToArray();

        slots.Should().OnlyHaveUniqueItems();
        StoragePersistence.GetJournalSlotIndex(
                firstAbsoluteSegmentIndex + maximumLiveSegmentCount,
                maximumReplayEntries,
                segmentCapacity)
            .Should().Be(slots[0]);
    }

    [Fact]
    public void SegmentArithmeticUsesOneBasedSequencesAndInclusiveBoundaries()
    {
        const int capacity = 4;

        StoragePersistence.GetAbsoluteSegmentIndex(1, capacity).Should().Be(0);
        StoragePersistence.GetAbsoluteSegmentIndex(4, capacity).Should().Be(0);
        StoragePersistence.GetAbsoluteSegmentIndex(5, capacity).Should().Be(1);
        StoragePersistence.GetAbsoluteSegmentIndex(8, capacity).Should().Be(1);
        StoragePersistence.GetAbsoluteSegmentIndex(9, capacity).Should().Be(2);

        StoragePersistence.GetSegmentStartSequence(0, capacity).Should().Be(1);
        StoragePersistence.GetSegmentEndSequence(0, capacity).Should().Be(4);
        StoragePersistence.GetSegmentStartSequence(2, capacity).Should().Be(9);
        StoragePersistence.GetSegmentEndSequence(2, capacity).Should().Be(12);
    }

    [Fact]
    public void SegmentArithmeticRejectsInvalidAndOverflowingCoordinates()
    {
        Action zeroSequence = () => StoragePersistence.GetAbsoluteSegmentIndex(0, 4);
        Action zeroCapacity = () => StoragePersistence.GetAbsoluteSegmentIndex(1, 0);
        Action negativeSegment = () => StoragePersistence.GetSegmentStartSequence(-1, 4);
        Action overflowingStart = () => StoragePersistence.GetSegmentStartSequence(long.MaxValue, 2);
        Action overflowingEnd = () => StoragePersistence.GetSegmentEndSequence((long.MaxValue - 1) / 2, 2);

        zeroSequence.Should().Throw<ArgumentOutOfRangeException>();
        zeroCapacity.Should().Throw<ArgumentOutOfRangeException>();
        negativeSegment.Should().Throw<ArgumentOutOfRangeException>();
        overflowingStart.Should().Throw<OverflowException>();
        overflowingEnd.Should().Throw<OverflowException>();
    }

    [Theory]
    [InlineData(0, 4, 0)]
    [InlineData(1, 4, 0)]
    [InlineData(3, 4, 0)]
    [InlineData(4, 4, 4)]
    [InlineData(5, 4, 4)]
    [InlineData(8, 4, 8)]
    [InlineData(9, 4, 8)]
    public void PrunableSequenceRoundsDownToACompleteSegment(
        long snapshotSequence,
        int segmentCapacity,
        long expectedPrunableSequence)
    {
        StoragePersistence.GetPrunableSequence(snapshotSequence, segmentCapacity)
            .Should().Be(expectedPrunableSequence);
    }

    [Fact]
    public void PhysicalKeysUseStableSlotFormatting()
    {
        StoragePersistence.CreateJournalSlotKey("provider:00000003", 5, 10)
            .Should().Be("provider:00000003:journal-slot:00000005");
        StoragePersistence.CreateSnapshotSlotKey("provider:00000003", 0)
            .Should().Be("provider:00000003:snapshot-slot:0");
        StoragePersistence.CreateSnapshotSlotKey("provider:00000003", 1)
            .Should().Be("provider:00000003:snapshot-slot:1");
    }

    [Fact]
    public void PhysicalKeyCreationRejectsInvalidPartitionsAndSlots()
    {
        Action blankJournalPartition = () => StoragePersistence.CreateJournalSlotKey(" ", 0, 1);
        Action negativeJournalSlot = () => StoragePersistence.CreateJournalSlotKey("partition", -1, 1);
        Action zeroJournalSlotCount = () => StoragePersistence.CreateJournalSlotKey("partition", 0, 0);
        Action journalSlotOutsideRing = () => StoragePersistence.CreateJournalSlotKey("partition", 2, 2);
        Action blankSnapshotPartition = () => StoragePersistence.CreateSnapshotSlotKey("", 0);
        Action negativeSnapshotSlot = () => StoragePersistence.CreateSnapshotSlotKey("partition", -1);
        Action snapshotSlotOutsideRing = () => StoragePersistence.CreateSnapshotSlotKey("partition", 2);

        blankJournalPartition.Should().Throw<ArgumentException>();
        negativeJournalSlot.Should().Throw<ArgumentOutOfRangeException>();
        zeroJournalSlotCount.Should().Throw<ArgumentOutOfRangeException>();
        journalSlotOutsideRing.Should().Throw<ArgumentOutOfRangeException>();
        blankSnapshotPartition.Should().Throw<ArgumentException>();
        negativeSnapshotSlot.Should().Throw<ArgumentOutOfRangeException>();
        snapshotSlotOutsideRing.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void RecordCopyDoesNotSharePayloadIndexesOrValues()
    {
        var original = CreateRecord("record", payloadSeed: 1);

        var copy = StoragePersistenceStateCopy.CopyRecord(original)!;

        copy.Should().NotBeSameAs(original);
        copy.GrainId.Should().Be(original.GrainId);
        copy.Payload.Should().Equal(original.Payload);
        copy.Payload.Should().NotBeSameAs(original.Payload);
        copy.IndexEntries.Should().NotBeSameAs(original.IndexEntries);
        copy.IndexEntries.Should().HaveSameCount(original.IndexEntries);
        for (var index = 0; index < original.IndexEntries.Count; index++)
        {
            copy.IndexEntries[index].Should().NotBeSameAs(original.IndexEntries[index]);
            copy.IndexEntries[index].Value.Should().NotBeSameAs(original.IndexEntries[index].Value);
            copy.IndexEntries[index].Value.Should().Be(original.IndexEntries[index].Value);
        }

        copy.Payload[0] = 99;
        copy.IndexEntries[0].Value.Text = "changed";
        copy.IndexEntries.RemoveAt(1);

        original.Payload.Should().Equal(1, 2, 3);
        original.IndexEntries.Should().HaveCount(2);
        original.IndexEntries[0].Value.Text.Should().Be("city-record");
    }

    [Fact]
    public void DurableRecordValidationRejectsDefaultGrainIdentity()
    {
        var valid = CreateRecord("record", payloadSeed: 1);
        var record = new StoredRecord
        {
            GrainId = default,
            Payload = valid.Payload,
            ETag = valid.ETag,
            IndexEntries = valid.IndexEntries,
        };

        Action validate = () => StoragePersistenceStateValidation.ValidateRecord(record, "record");

        validate.Should().Throw<ArgumentException>()
            .WithParameterName("record")
            .WithMessage("*must identify a grain*");
    }

    [Fact]
    public void DurableRecordValidationRejectsNullStringIndexValue()
    {
        var record = CreateRecord("record", payloadSeed: 1);
        record.IndexEntries.Add(new IndexEntry
        {
            Scope = "invalid-string",
            Kind = SearchableIndexKind.Hash,
            Value = new IndexValue { Kind = IndexValueKind.String },
        });

        Action validate = () => StoragePersistenceStateValidation.ValidateRecord(record, "record");

        validate.Should().Throw<ArgumentException>()
            .WithParameterName("record")
            .WithMessage("*must not be null*");
    }

    [Fact]
    public void SnapshotCopyDoesNotShareRecordState()
    {
        var original = CreateSnapshot();

        var copy = original.Copy();

        copy.Should().NotBeSameAs(original);
        copy.Records.Should().NotBeSameAs(original.Records);
        copy.Records.Comparer.Should().BeSameAs(StringComparer.Ordinal);
        copy.Records["first"].Should().NotBeSameAs(original.Records["first"]);
        copy.Records["first"].Payload.Should().NotBeSameAs(original.Records["first"].Payload);
        copy.Records["first"].IndexEntries[0].Value
            .Should().NotBeSameAs(original.Records["first"].IndexEntries[0].Value);

        copy.Records["first"].Payload[0] = 99;
        copy.Records["first"].IndexEntries[0].Value.Text = "changed";
        copy.Records.Remove("second");

        original.Records["first"].Payload.Should().Equal(1, 2, 3);
        original.Records["first"].IndexEntries[0].Value.Text.Should().Be("city-first");
        original.Records.Should().ContainKey("second");
    }

    [Fact]
    public void SnapshotEqualityIncludesPayloadIndexValuesAndIndexOrderButNotRecordOrder()
    {
        var original = CreateSnapshot();
        var equal = original.Copy();
        var reorderedRecords = original.Copy();
        reorderedRecords.Records = reorderedRecords.Records
            .Reverse()
            .ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);
        var changedPayload = original.Copy();
        changedPayload.Records["first"].Payload[0]++;
        var changedIndexValue = original.Copy();
        changedIndexValue.Records["first"].IndexEntries[0].Value.Text = "different";
        var changedIndexOrder = original.Copy();
        changedIndexOrder.Records["first"].IndexEntries.Reverse();

        StoragePersistenceStateEquality.SnapshotEquals(original, equal).Should().BeTrue();
        StoragePersistenceStateEquality.SnapshotEquals(original, reorderedRecords).Should().BeTrue();
        StoragePersistenceStateEquality.SnapshotEquals(original, changedPayload).Should().BeFalse();
        StoragePersistenceStateEquality.SnapshotEquals(original, changedIndexValue).Should().BeFalse();
        StoragePersistenceStateEquality.SnapshotEquals(original, changedIndexOrder).Should().BeFalse();
    }

    [Fact]
    public void JournalAndDescriptorEqualityIncludeDurableIdentityAndRecordContent()
    {
        var snapshot = CreateSnapshot();
        var descriptor = StorageSnapshotDescriptor.FromSnapshot(snapshot);
        var changedDescriptor = descriptor.Copy();
        changedDescriptor.NextVersion++;
        var journal = CreateJournalEntry(CreateRecord("journal", payloadSeed: 3));
        var equalJournal = journal.Copy();
        var changedJournal = journal.Copy();
        changedJournal.Record!.Payload[0]++;

        StoragePersistenceStateEquality.DescriptorEquals(descriptor, snapshot).Should().BeTrue();
        StoragePersistenceStateEquality.DescriptorEquals(changedDescriptor, snapshot).Should().BeFalse();
        StoragePersistenceStateEquality.JournalEntryEquals(journal, equalJournal).Should().BeTrue();
        StoragePersistenceStateEquality.JournalEntryEquals(journal, changedJournal).Should().BeFalse();
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(64, 4_096, 1)]
    [InlineData(64, 4_096, 4_096)]
    [InlineData(64, 1, 1)]
    public void PersistenceSettingsAllowPositiveThresholdAtOrBelowReplayLimit(
        int segmentCapacity,
        int maximumReplayEntries,
        int compactionThreshold)
    {
        var settings = CreateSettings(segmentCapacity, maximumReplayEntries, compactionThreshold);

        StoragePartitionPersistence.ValidateSettings(settings);
    }

    [Theory]
    [InlineData(0, 4_096, 1)]
    [InlineData(64, 0, 1)]
    [InlineData(64, 4_096, 0)]
    [InlineData(64, 128, 129)]
    [InlineData(1, int.MaxValue, 1)]
    public void PersistenceSettingsRejectInvalidLimitsAndThresholds(
        int segmentCapacity,
        int maximumReplayEntries,
        int compactionThreshold)
    {
        var settings = CreateSettings(segmentCapacity, maximumReplayEntries, compactionThreshold);
        Action validate = () => StoragePartitionPersistence.ValidateSettings(settings);

        validate.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void PersistenceSettingsRejectNull()
    {
        Action validate = () => StoragePartitionPersistence.ValidateSettings(null!);

        validate.Should().Throw<ArgumentNullException>();
    }

    private static StoragePersistenceSettings CreateSettings(
        int segmentCapacity,
        int maximumReplayEntries,
        int compactionThreshold)
    {
        return new StoragePersistenceSettings
        {
            JournalSegmentCapacity = segmentCapacity,
            MaximumJournalReplayEntries = maximumReplayEntries,
            CompactionThreshold = compactionThreshold,
        };
    }

    private static StorageSnapshotState CreateSnapshot()
    {
        return new StorageSnapshotState
        {
            Initialized = true,
            Slot = 1,
            Generation = 7,
            SnapshotId = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Sequence = 12,
            OperationId = Guid.Parse("22222222-2222-2222-2222-222222222222"),
            NextVersion = 5,
            Records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["first"] = CreateRecord("first", payloadSeed: 1),
                ["second"] = CreateRecord("second", payloadSeed: 2),
            },
        };
    }

    private static StorageJournalEntry CreateJournalEntry(StoredRecord record)
    {
        return new StorageJournalEntry
        {
            Sequence = 3,
            WriterEpoch = 2,
            OperationId = Guid.Parse("33333333-3333-3333-3333-333333333333"),
            PreviousOperationId = Guid.Parse("44444444-4444-4444-4444-444444444444"),
            Operation = StorageJournalOperation.Upsert,
            RecordKey = "journal-record",
            ExpectedETag = "2",
            Record = record,
            NextVersionAfter = 4,
        };
    }

    private static StoredRecord CreateRecord(string recordKey, byte payloadSeed)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("storage-persistence-test", recordKey),
            Payload = [payloadSeed, 2, 3],
            ETag = payloadSeed.ToString(System.Globalization.CultureInfo.InvariantCulture),
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = "city",
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create($"city-{recordKey}"),
                },
                new IndexEntry
                {
                    Scope = "salary",
                    Kind = SearchableIndexKind.Range,
                    Value = IndexValue.FromSignedInteger(payloadSeed),
                },
            ],
        };
    }
}
