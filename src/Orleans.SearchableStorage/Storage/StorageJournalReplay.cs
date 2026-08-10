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
        ref Guid operationId)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(recoveredOperationIds);
        if (entry.Sequence != expectedSequence
            || entry.WriterEpoch <= 0
            || entry.WriterEpoch > maximumWriterEpoch
            || entry.OperationId == Guid.Empty
            || entry.PreviousOperationId != operationId
            || !recoveredOperationIds.Add(entry.OperationId)
            || entry.NextVersionAfter <= 0
            || string.IsNullOrWhiteSpace(entry.RecordKey))
        {
            throw new InvalidOperationException($"Journal entry {expectedSequence} is invalid.");
        }

        records.TryGetValue(entry.RecordKey, out var currentRecord);
        if (!string.Equals(currentRecord?.ETag, entry.ExpectedETag, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Journal entry {entry.Sequence} does not follow the committed record version.");
        }

        switch (entry.Operation)
        {
            case StorageJournalOperation.Upsert when entry.Record is not null:
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

                records[entry.RecordKey] = StoragePersistenceStateCopy.CopyRecord(entry.Record)!;
                break;
            case StorageJournalOperation.Delete when entry.Record is null && currentRecord is not null:
                if (entry.NextVersionAfter != nextVersion)
                {
                    throw new InvalidOperationException(
                        $"Journal entry {entry.Sequence} changes the version during a delete.");
                }

                records.Remove(entry.RecordKey);
                break;
            default:
                throw new InvalidOperationException(
                    $"Journal entry {entry.Sequence} has an invalid operation payload.");
        }

        nextVersion = entry.NextVersionAfter;
        operationId = entry.OperationId;
    }
}
