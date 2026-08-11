using AwesomeAssertions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageIndexSchemaProtocolTests
{
    [Fact]
    public void RebuildPageServerAccepts64AndRejects65()
    {
        var maximum = () => StoragePartitionGrain.ValidateRebuildPageSize(
            StorageIndexSchema.RebuildPageSize);
        var overMaximum = () => StoragePartitionGrain.ValidateRebuildPageSize(
            checked(StorageIndexSchema.RebuildPageSize + 1));

        maximum.Should().NotThrow();
        overMaximum.Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("*between 1 and 64*");
    }

    [Fact]
    public void ReindexReplayPreservesRecordAndPartitionVersions()
    {
        var grainId = GrainId.Create("schema", "record");
        var original = new StoredRecord
        {
            GrainId = grainId,
            Payload = [1, 2, 3],
            ETag = "7",
            IndexEntries = [],
        };
        var fingerprint = Enumerable.Range(0, IndexSchemaDefinition.FingerprintLength)
            .Select(static value => checked((byte)value))
            .ToArray();
        var replacement = new StoredRecord
        {
            GrainId = grainId,
            Payload = [1, 2, 3],
            ETag = "7",
            IndexEntries = [],
            IndexSchemaFingerprint = fingerprint,
        };
        var operationId = Guid.Empty;
        var nextVersion = 8L;
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["state/record"] = original,
        };
        var entry = new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.Reindex,
            RecordKey = "state/record",
            ExpectedETag = "7",
            Record = replacement,
            NextVersionAfter = nextVersion,
        };

        StorageJournalReplay.ApplyEntry(
            records,
            entry,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            [],
            ref nextVersion,
            ref operationId,
            new StorageCapacityTracker(records));

        nextVersion.Should().Be(8);
        records["state/record"].ETag.Should().Be("7");
        records["state/record"].IndexSchemaFingerprint.Should().Equal(fingerprint);
    }

    [Fact]
    public void ReindexReplayRejectsAnObjectVersionChange()
    {
        var grainId = GrainId.Create("schema", "record");
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["state/record"] = new StoredRecord
            {
                GrainId = grainId,
                Payload = [],
                ETag = "7",
                IndexEntries = [],
            },
        };
        var entry = new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.Reindex,
            RecordKey = "state/record",
            ExpectedETag = "7",
            Record = new StoredRecord
            {
                GrainId = grainId,
                Payload = [],
                ETag = "8",
                IndexEntries = [],
                IndexSchemaFingerprint = CreateFingerprint(),
            },
            NextVersionAfter = 8,
        };
        var nextVersion = 8L;
        var operationId = Guid.Empty;

        var action = () => StorageJournalReplay.ApplyEntry(
            records,
            entry,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            [],
            ref nextVersion,
            ref operationId,
            new StorageCapacityTracker(records));

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*identity or version*");
        records["state/record"].ETag.Should().Be("7");
    }

    [Fact]
    public void ReindexReplayRejectsAPayloadChange()
    {
        var grainId = GrainId.Create("schema", "record");
        var original = new StoredRecord
        {
            GrainId = grainId,
            Payload = [1, 2, 3],
            ETag = "7",
            IndexEntries = [],
        };
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["state/record"] = original,
        };
        var entry = CreateReindexEntry(
            grainId,
            payload: [1, 2, 4]);
        var nextVersion = 8L;
        var operationId = Guid.Empty;

        var action = () => StorageJournalReplay.ApplyEntry(
            records,
            entry,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            [],
            ref nextVersion,
            ref operationId,
            new StorageCapacityTracker(records));

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*identity or version*");
        records["state/record"].Payload.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void ReindexReplayRejectsAGrainIdChange()
    {
        var originalGrainId = GrainId.Create("schema", "record");
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["state/record"] = new StoredRecord
            {
                GrainId = originalGrainId,
                Payload = [1, 2, 3],
                ETag = "7",
                IndexEntries = [],
            },
        };
        var entry = CreateReindexEntry(
            GrainId.Create("schema", "other-record"),
            payload: [1, 2, 3]);
        var nextVersion = 8L;
        var operationId = Guid.Empty;

        var action = () => StorageJournalReplay.ApplyEntry(
            records,
            entry,
            expectedSequence: 1,
            maximumWriterEpoch: 1,
            [],
            ref nextVersion,
            ref operationId,
            new StorageCapacityTracker(records));

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*identity or version*");
        records["state/record"].GrainId.Should().Be(originalGrainId);
    }

    [Fact]
    public void ReindexWalRequiresAManagedSchemaFingerprint()
    {
        var entry = CreateReindexEntry(
            GrainId.Create("schema", "record"),
            payload: [1, 2, 3]);
        entry = new StorageJournalEntry
        {
            Sequence = entry.Sequence,
            WriterEpoch = entry.WriterEpoch,
            OperationId = entry.OperationId,
            PreviousOperationId = entry.PreviousOperationId,
            Operation = entry.Operation,
            RecordKey = entry.RecordKey,
            ExpectedETag = entry.ExpectedETag,
            Record = new StoredRecord
            {
                GrainId = entry.Record!.GrainId,
                Payload = entry.Record.Payload,
                ETag = entry.Record.ETag,
                IndexEntries = entry.Record.IndexEntries,
            },
            NextVersionAfter = entry.NextVersionAfter,
        };

        var validate = () => StoragePersistenceStateValidation.ValidateJournalEntry(
            entry,
            nameof(entry));

        validate.Should().Throw<ArgumentException>()
            .WithMessage("*reindex*fingerprint*");
    }

    private static StorageJournalEntry CreateReindexEntry(GrainId grainId, byte[] payload)
    {
        return new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.Reindex,
            RecordKey = "state/record",
            ExpectedETag = "7",
            Record = new StoredRecord
            {
                GrainId = grainId,
                Payload = payload,
                ETag = "7",
                IndexEntries = [],
                IndexSchemaFingerprint = CreateFingerprint(),
            },
            NextVersionAfter = 8,
        };
    }

    private static byte[] CreateFingerprint() =>
        Enumerable.Range(0, IndexSchemaDefinition.FingerprintLength)
            .Select(static value => checked((byte)value))
            .ToArray();

}
