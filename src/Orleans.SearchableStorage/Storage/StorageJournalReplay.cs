using System.Globalization;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Applies one committed journal entry while enforcing the persisted replay chain.
/// </summary>
internal static class StorageJournalReplay
{
    public static void ApplyEntry(
        Dictionary<string, StoredRecord> records,
        StorageJournalEntry entry,
        long expectedSequence,
        long maximumWriterEpoch,
        HashSet<Guid> recoveredOperationIds,
        ref long nextVersion,
        ref Guid operationId,
        StorageCapacityTracker capacity)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(recoveredOperationIds);
        ArgumentNullException.ThrowIfNull(capacity);
        if (entry.Sequence != expectedSequence
            || entry.WriterEpoch <= 0
            || entry.WriterEpoch > maximumWriterEpoch
            || entry.OperationId == Guid.Empty
            || entry.PreviousOperationId != operationId
            || !recoveredOperationIds.Add(entry.OperationId)
            || entry.NextVersionAfter <= 0)
        {
            throw new InvalidOperationException($"Journal entry {expectedSequence} is invalid.");
        }

        switch (entry.Operation)
        {
            case StorageJournalOperation.Upsert when entry.Record is not null:
                records.TryGetValue(entry.RecordKey, out var currentRecord);
                EnsureExpectedETag(entry, currentRecord);
                StoragePersistenceStateValidation.ValidateRecord(entry.Record, nameof(entry));
                if (!string.Equals(
                        entry.Record.ETag,
                        nextVersion.ToString(CultureInfo.InvariantCulture),
                        StringComparison.Ordinal)
                    || entry.NextVersionAfter != checked(nextVersion + 1))
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} does not contain the next record version.");
                }

                capacity.ValidateProjectedUpsert(records, entry.RecordKey, entry.Record);
                capacity.ApplyUpsert(records, entry.RecordKey, entry.Record);
                records[entry.RecordKey] = StoragePersistenceStateCopy.CopyRecord(entry.Record)!;
                break;
            case StorageJournalOperation.Delete when entry.Record is null:
                records.TryGetValue(entry.RecordKey, out currentRecord);
                EnsureExpectedETag(entry, currentRecord);
                if (currentRecord is null)
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} deletes a missing record.");
                }

                if (entry.NextVersionAfter != nextVersion)
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} changes the version during a delete.");
                }

                capacity.ApplyDelete(records, entry.RecordKey);
                records.Remove(entry.RecordKey);
                break;
            case StorageJournalOperation.Reindex when entry.Record is not null:
                records.TryGetValue(entry.RecordKey, out currentRecord);
                EnsureExpectedETag(entry, currentRecord);
                StoragePersistenceStateValidation.ValidateRecord(entry.Record, nameof(entry));
                if (currentRecord is null
                    || entry.Record.IndexSchemaFingerprint is null
                    || !string.Equals(entry.Record.ETag, currentRecord.ETag, StringComparison.Ordinal)
                    || !entry.Record.GrainId.Equals(currentRecord.GrainId)
                    || !NullablePayloadEquals(entry.Record.Payload, currentRecord.Payload)
                    || entry.NextVersionAfter != nextVersion)
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} changes record identity or version during reindexing.");
                }

                capacity.ValidateProjectedUpsert(records, entry.RecordKey, entry.Record);
                capacity.ApplyUpsert(records, entry.RecordKey, entry.Record);
                records[entry.RecordKey] = StoragePersistenceStateCopy.CopyRecord(entry.Record)!;
                break;
            case StorageJournalOperation.AdvanceVersion when entry.Move is not null:
                if (entry.NextVersionAfter != Math.Max(nextVersion, entry.Move.FrozenNextVersion))
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} contains an invalid version high-water mark.");
                }

                break;
            case StorageJournalOperation.Import when entry.Move is not null:
                ApplyImport(records, entry, nextVersion, capacity);
                break;
            case StorageJournalOperation.MoveDelete when entry.Move is not null:
                ApplyMoveDelete(records, entry, nextVersion, capacity);
                break;
            default:
                throw new InvalidOperationException(
                    $"Journal entry {entry.Sequence} has an invalid operation payload.");
        }

        nextVersion = entry.NextVersionAfter;
        operationId = entry.OperationId;
    }

    private static bool NullablePayloadEquals(byte[]? left, byte[]? right)
    {
        return left is null || right is null
            ? left is null && right is null
            : left.AsSpan().SequenceEqual(right);
    }

    private static void ApplyImport(
        Dictionary<string, StoredRecord> records,
        StorageJournalEntry entry,
        long nextVersion,
        StorageCapacityTracker capacity)
    {
        var move = entry.Move!;
        if (entry.NextVersionAfter != nextVersion
            || nextVersion < move.FrozenNextVersion)
        {
            throw new InvalidOperationException(
                $"Journal entry {entry.Sequence} imports records before its version fence.");
        }

        capacity.ValidateProjectedImports(records, move.Imports);

        foreach (var item in move.Imports)
        {
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            var importedRecord = StorageMoveRecordCodec.Decode(item.Record);
            if (records.ContainsKey(recordKey)
                || !long.TryParse(
                    importedRecord.ETag,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var recordVersion)
                || recordVersion <= 0
                || recordVersion >= move.FrozenNextVersion)
            {
                throw new InvalidOperationException(
                    $"Journal entry {entry.Sequence} contains an invalid imported record.");
            }

            capacity.ApplyUpsert(records, recordKey, importedRecord);
            records.Add(recordKey, importedRecord);
        }
    }

    private static void ApplyMoveDelete(
        Dictionary<string, StoredRecord> records,
        StorageJournalEntry entry,
        long nextVersion,
        StorageCapacityTracker capacity)
    {
        if (entry.NextVersionAfter != nextVersion)
        {
            throw new InvalidOperationException(
                $"Journal entry {entry.Sequence} changes the version during move cleanup.");
        }

        var move = entry.Move!;
        foreach (var item in move.Deletes)
        {
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            var expectedETag = StorageMoveRecordCodec.DecodeExpectedETag(item);
            if (!records.TryGetValue(recordKey, out var record)
                || !string.Equals(record.ETag, expectedETag, StringComparison.Ordinal)
                || StorageLayout.GetSlot(record.GrainId, move.VirtualSlotCount) != move.Slot)
            {
                throw new InvalidOperationException(
                    $"Journal entry {entry.Sequence} does not match the frozen move record set.");
            }

            capacity.ApplyDelete(records, recordKey);
            records.Remove(recordKey);
        }
    }

    private static void EnsureExpectedETag(StorageJournalEntry entry, StoredRecord? currentRecord)
    {
        if (!string.Equals(currentRecord?.ETag, entry.ExpectedETag, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Journal entry {entry.Sequence} does not follow the committed record version.");
        }
    }
}
