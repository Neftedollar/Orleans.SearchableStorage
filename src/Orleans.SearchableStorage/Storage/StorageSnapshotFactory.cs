namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Builds the detached snapshot payload published by partition compaction.
/// </summary>
internal static class StorageSnapshotFactory
{
    public const int LegacyRecordEncodingVersion = 0;
    public const int LosslessRecordEncodingVersion = 1;

    public static StorageSnapshotState Create(
        StorageSnapshotDescriptor descriptor,
        IReadOnlyDictionary<string, StoredRecord> records,
        int persistenceFormatVersion = StoragePersistence.PreviousPersistenceFormatVersion)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(records);
        if (!StoragePersistence.IsSupportedFormat(persistenceFormatVersion))
        {
            throw new ArgumentOutOfRangeException(
                nameof(persistenceFormatVersion),
                persistenceFormatVersion,
                "A snapshot can only be created for a supported persistence format.");
        }

        if (!StoragePersistence.SupportsIndexSchemas(persistenceFormatVersion)
            && records.Values.Any(static record => record.IndexSchemaFingerprint is not null))
        {
            throw new InvalidOperationException(
                "A persistence-v3/v4 snapshot cannot contain a managed index-schema record.");
        }

        var snapshot = new StorageSnapshotState
        {
            Initialized = true,
            Slot = descriptor.Slot,
            Generation = descriptor.Generation,
            SnapshotId = descriptor.SnapshotId,
            Sequence = descriptor.Sequence,
            OperationId = descriptor.OperationId,
            NextVersion = descriptor.NextVersion,
        };

        if (StoragePersistence.UsesLosslessSnapshots(persistenceFormatVersion))
        {
            snapshot.RecordEncodingVersion = LosslessRecordEncodingVersion;
            snapshot.LosslessRecords = records
                .OrderBy(static pair => pair.Key, StringComparer.Ordinal)
                .Select(static pair => StorageMoveRecordCodec.Encode(pair.Key, pair.Value))
                .ToList();
        }
        else
        {
            snapshot.Records = records.ToDictionary(
                static pair => pair.Key,
                static pair => StoragePersistenceStateCopy.CopyRecord(pair.Value)!,
                StringComparer.Ordinal);
        }

        return snapshot;
    }

    public static Dictionary<string, StoredRecord> DecodeRecords(
        StorageSnapshotState snapshot,
        int persistenceFormatVersion)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (!StoragePersistence.IsSupportedFormat(persistenceFormatVersion))
        {
            throw new ArgumentOutOfRangeException(
                nameof(persistenceFormatVersion),
                persistenceFormatVersion,
                "A snapshot can only be recovered for a supported persistence format.");
        }

        ValidatePayload(snapshot);
        if (persistenceFormatVersion == StoragePersistence.PreviousPersistenceFormatVersion
            && snapshot.RecordEncodingVersion != LegacyRecordEncodingVersion)
        {
            throw new InvalidOperationException(
                "A persistence-v3 manifest cannot reference a newer lossless snapshot.");
        }

        if (snapshot.RecordEncodingVersion == LegacyRecordEncodingVersion)
        {
            return snapshot.Records.ToDictionary(
                static pair => pair.Key,
                static pair => StoragePersistenceStateCopy.CopyRecord(pair.Value)!,
                StringComparer.Ordinal);
        }

        var records = new Dictionary<string, StoredRecord>(
            snapshot.LosslessRecords.Count,
            StringComparer.Ordinal);
        foreach (var item in snapshot.LosslessRecords)
        {
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            if (!records.TryAdd(recordKey, StorageMoveRecordCodec.Decode(item.Record)))
            {
                throw new InvalidOperationException(
                    $"A lossless snapshot contains duplicate record key '{recordKey}'.");
            }
        }

        return records;
    }

    public static void ValidatePayload(StorageSnapshotState snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(snapshot.Records, nameof(snapshot));
        ArgumentNullException.ThrowIfNull(snapshot.LosslessRecords, nameof(snapshot));
        if (snapshot.RecordEncodingVersion == LegacyRecordEncodingVersion)
        {
            if (snapshot.LosslessRecords.Count != 0
                || snapshot.NextVersion < 2
                || snapshot.NextVersion - 1 > snapshot.Sequence)
            {
                throw new InvalidOperationException(
                    "A legacy snapshot has mixed payloads or an invalid v3 version boundary.");
            }

            foreach (var (recordKey, record) in snapshot.Records)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(recordKey, nameof(snapshot));
                StoragePersistenceStateValidation.ValidateRecord(record, nameof(snapshot));
            }

            return;
        }

        if (snapshot.RecordEncodingVersion != LosslessRecordEncodingVersion)
        {
            throw new InvalidOperationException(
                $"Unknown snapshot record encoding version '{snapshot.RecordEncodingVersion}'.");
        }

        if (snapshot.Records.Count != 0)
        {
            throw new InvalidOperationException(
                "A lossless snapshot cannot also contain legacy records.");
        }

        byte[]? previousRecordKey = null;
        foreach (var item in snapshot.LosslessRecords)
        {
            StorageMoveRecordCodec.Validate(item, nameof(snapshot));
            if (previousRecordKey is not null
                && StorageMoveRecordCodec.CompareText(previousRecordKey, item.RecordKey) >= 0)
            {
                throw new InvalidOperationException(
                    "Lossless snapshot record keys must be strictly increasing in ordinal order.");
            }

            _ = StorageMoveRecordCodec.Decode(item.Record);
            previousRecordKey = item.RecordKey;
        }
    }
}
