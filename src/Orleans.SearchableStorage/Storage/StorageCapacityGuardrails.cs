using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Enforces one versioned logical capacity envelope across live writes, maintenance, child
/// persistence grains, and recovery. Counts use the lossless movement encoding as their canonical
/// basis and never inspect provider-specific or Orleans transport bytes.
/// </summary>
internal static class StorageCapacityGuardrails
{
    internal const string RecordPayloadBytes = "record-payload-bytes";
    internal const string RecordKeyBytes = "record-key-canonical-bytes";
    internal const string GrainTypeBytes = "grain-id-type-bytes";
    internal const string GrainKeyBytes = "grain-id-key-bytes";
    internal const string RecordIndexEntries = "record-index-entries";
    internal const string RecordScopeIndexEntries = "record-scope-index-entries";
    internal const string IndexEntryBytes = "index-entry-canonical-bytes";
    internal const string RecordIndexBytes = "record-index-canonical-bytes";
    internal const string RecordBytes = "record-canonical-bytes";
    internal const string SnapshotRecords = "snapshot-records";
    internal const string SnapshotBytes = "snapshot-canonical-bytes";
    internal const string JournalEntryBytes = "journal-entry-canonical-bytes";
    internal const string JournalSegmentEntries = "journal-segment-entries";
    internal const string JournalSegmentBytes = "journal-segment-canonical-bytes";

    private const string MaximumVersionText = "9223372036854775807";

    public static void ValidatePersistenceConfiguration(
        int journalSegmentCapacity,
        int maximumJournalReplayEntries,
        string segmentParameterName,
        string replayParameterName)
    {
        ValidateJournalSegmentCapacity(journalSegmentCapacity, segmentParameterName);
        ValidateJournalReplayCapacity(maximumJournalReplayEntries, replayParameterName);
    }

    public static void ValidateJournalSegmentCapacity(int capacity, string parameterName)
    {
        if (capacity > SearchableStorageCapacityLimits.MaximumJournalSegmentEntries)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                capacity,
                $"A journal segment cannot contain more than "
                + $"{SearchableStorageCapacityLimits.MaximumJournalSegmentEntries} entries.");
        }
    }

    public static void ValidateJournalReplayCapacity(int capacity, string parameterName)
    {
        if (capacity > SearchableStorageCapacityLimits.MaximumJournalReplayEntries)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                capacity,
                $"A journal replay tail cannot contain more than "
                + $"{SearchableStorageCapacityLimits.MaximumJournalReplayEntries} entries.");
        }
    }

    public static void ValidateRecordKeyAndPayload(string recordKey, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(recordKey);
        ArgumentNullException.ThrowIfNull(payload);
        ValidateRecordKey(recordKey);
        ThrowIfExceeded(
            RecordPayloadBytes,
            payload.LongLength,
            SearchableStorageCapacityLimits.MaximumRecordPayloadBytes);
    }

    public static void ValidateRecordKey(string recordKey)
    {
        ArgumentNullException.ThrowIfNull(recordKey);
        if (recordKey.Length == 0)
        {
            throw new ArgumentException("A record key must not be empty.", nameof(recordKey));
        }

        ThrowIfExceeded(
            RecordKeyBytes,
            StorageMovePageDigest.GetTextEncodedByteCount(recordKey),
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes);
        ArgumentException.ThrowIfNullOrWhiteSpace(recordKey);
    }

    public static void ValidateGrainId(Orleans.Runtime.GrainId grainId)
    {
        StorageGrainIdCapacity.Validate(grainId);
    }

    public static void ValidateGrainIdParts(long typeBytes, long keyBytes)
    {
        StorageGrainIdCapacity.ValidateParts(typeBytes, keyBytes);
    }

    public static void ValidateWriteRequest(StorageWriteRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateRecordCore(
            request.RecordKey,
            request.GrainId,
            request.Payload,
            MaximumVersionText,
            request.IndexEntries,
            request.IndexSchemaFingerprint);
    }

    public static long ValidateRecord(string recordKey, StoredRecord record)
    {
        ArgumentNullException.ThrowIfNull(recordKey);
        ArgumentNullException.ThrowIfNull(record);
        var byteCount = ValidateRecordCore(
            recordKey,
            record.GrainId,
            record.Payload,
            record.ETag,
            record.IndexEntries,
            record.IndexSchemaFingerprint);
        StoragePersistenceStateValidation.ValidateRecord(record, nameof(record));
        return byteCount;
    }

    public static long ValidateMoveRecord(StorageMoveRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(record.Record);
        ArgumentNullException.ThrowIfNull(record.Record.GrainType);
        ArgumentNullException.ThrowIfNull(record.Record.GrainKey);
        ArgumentNullException.ThrowIfNull(record.RecordKey);
        ArgumentNullException.ThrowIfNull(record.Record.Payload);
        ArgumentNullException.ThrowIfNull(record.Record.ETag);
        ArgumentNullException.ThrowIfNull(record.Record.IndexEntries);
        ValidateGrainIdParts(
            record.Record.GrainType.LongLength,
            record.Record.GrainKey.LongLength);
        ValidateRecordKeyBytes(record.RecordKey);
        ThrowIfExceeded(
            RecordPayloadBytes,
            record.Record.Payload.LongLength,
            SearchableStorageCapacityLimits.MaximumRecordPayloadBytes);
        ThrowIfExceeded(
            RecordIndexEntries,
            record.Record.IndexEntries.Count,
            SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord);

        var indexByteCount = 0L;
        foreach (var entry in record.Record.IndexEntries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentNullException.ThrowIfNull(entry.Scope);
            ArgumentNullException.ThrowIfNull(entry.Value);
            ArgumentNullException.ThrowIfNull(entry.Value.PrimitiveBits);
            var entryByteCount = StorageMovePageDigest.GetIndexEntryEncodedByteCount(entry);
            ThrowIfExceeded(
                IndexEntryBytes,
                entryByteCount,
                SearchableStorageCapacityLimits.MaximumIndexEntryCanonicalBytes);
            indexByteCount = checked(indexByteCount + entryByteCount);
            ThrowIfExceeded(
                RecordIndexBytes,
                indexByteCount,
                SearchableStorageCapacityLimits.MaximumIndexBytesPerRecord);
        }

        var recordByteCount = GetMoveRecordCanonicalByteCount(record, indexByteCount);
        ThrowIfExceeded(
            RecordBytes,
            recordByteCount,
            SearchableStorageCapacityLimits.MaximumRecordCanonicalBytes);

        // Full semantic validation and UTF-16 decoding happen only after cheap shape, element,
        // and canonical-byte ceilings have bounded the work.
        StorageMoveRecordCodec.Validate(record, nameof(record));

        var perScopeCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var entry in record.Record.IndexEntries)
        {
            var scope = StorageMoveRecordCodec.DecodeText(entry.Scope, nameof(record));
            perScopeCounts.TryGetValue(scope, out var scopeCount);
            scopeCount = checked(scopeCount + 1);
            ThrowIfExceeded(
                RecordScopeIndexEntries,
                scopeCount,
                SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope);
            perScopeCounts[scope] = scopeCount;
        }
        return recordByteCount;
    }

    public static long ValidateJournalEntry(StorageJournalEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(entry.RecordKey);
        if (entry.RecordKey.Length != 0)
        {
            ValidateRecordKey(entry.RecordKey);
        }

        if (entry.Record is not null)
        {
            _ = ValidateRecord(entry.RecordKey, entry.Record);
        }

        if (entry.Move is { } move)
        {
            ArgumentNullException.ThrowIfNull(move.Imports);
            ArgumentNullException.ThrowIfNull(move.Deletes);
            ArgumentNullException.ThrowIfNull(move.PageDigest);
            if (move.AfterRecordKey is not null)
            {
                ValidateRecordKeyBytes(move.AfterRecordKey);
            }

            if (move.NextRecordKey is not null)
            {
                ValidateRecordKeyBytes(move.NextRecordKey);
            }

            ValidateMovementItemCount(move.Imports.Count, move.Deletes.Count);

            foreach (var item in move.Imports)
            {
                _ = ValidateMoveRecord(item);
            }

            foreach (var item in move.Deletes)
            {
                ArgumentNullException.ThrowIfNull(item);
                ArgumentNullException.ThrowIfNull(item.RecordKey);
                ArgumentNullException.ThrowIfNull(item.ExpectedETag);
                ValidateRecordKeyBytes(item.RecordKey);
                ValidateEncodedTextShape(item.ExpectedETag, nameof(item.ExpectedETag));
            }
        }

        var byteCount = GetJournalEntryCanonicalByteCount(entry);
        ThrowIfExceeded(
            JournalEntryBytes,
            byteCount,
            SearchableStorageCapacityLimits.MaximumJournalEntryCanonicalBytes);
        // Digest comparison and semantic decoding are bounded by the prechecks above.
        StoragePersistenceStateValidation.ValidateJournalEntry(entry, nameof(entry));
        return byteCount;
    }

    public static long ValidateJournalSegment(StorageJournalSegmentState segment)
    {
        ArgumentNullException.ThrowIfNull(segment);
        ArgumentNullException.ThrowIfNull(segment.Entries);
        ValidateJournalSegmentCapacity(segment.Capacity, nameof(segment.Capacity));
        ThrowIfExceeded(
            JournalSegmentEntries,
            segment.Entries.Count,
            SearchableStorageCapacityLimits.MaximumJournalSegmentEntries);

        var byteCount = 0L;
        foreach (var entry in segment.Entries)
        {
            byteCount = checked(byteCount + ValidateJournalEntry(entry));
            ThrowIfExceeded(
                JournalSegmentBytes,
                byteCount,
                SearchableStorageCapacityLimits.MaximumJournalSegmentCanonicalBytes);
        }

        return byteCount;
    }

    public static long ValidateSnapshotRecords(IReadOnlyDictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);
        ThrowIfExceeded(
            SnapshotRecords,
            records.Count,
            SearchableStorageCapacityLimits.MaximumSnapshotRecords);

        var byteCount = 0L;
        foreach (var (recordKey, record) in records)
        {
            byteCount = checked(byteCount + ValidateRecord(recordKey, record));
            ThrowIfExceeded(
                SnapshotBytes,
                byteCount,
                SearchableStorageCapacityLimits.MaximumSnapshotCanonicalBytes);
        }

        return byteCount;
    }

    public static long ValidateSnapshotPayload(StorageSnapshotState snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var recordCount = checked(snapshot.Records.Count + snapshot.LosslessRecords.Count);
        ThrowIfExceeded(
            SnapshotRecords,
            recordCount,
            SearchableStorageCapacityLimits.MaximumSnapshotRecords);

        var byteCount = 0L;
        foreach (var (recordKey, record) in snapshot.Records)
        {
            byteCount = checked(byteCount + ValidateRecord(recordKey, record));
            ThrowIfExceeded(
                SnapshotBytes,
                byteCount,
                SearchableStorageCapacityLimits.MaximumSnapshotCanonicalBytes);
        }

        foreach (var item in snapshot.LosslessRecords)
        {
            byteCount = checked(byteCount + ValidateMoveRecord(item));
            ThrowIfExceeded(
                SnapshotBytes,
                byteCount,
                SearchableStorageCapacityLimits.MaximumSnapshotCanonicalBytes);
        }

        return byteCount;
    }

    public static void ValidateRecordKeyBytes(byte[] encodedRecordKey)
    {
        ArgumentNullException.ThrowIfNull(encodedRecordKey);
        if ((encodedRecordKey.Length & 1) != 0)
        {
            throw new ArgumentException(
                "A lossless record key must contain complete UTF-16 code units.",
                nameof(encodedRecordKey));
        }

        ThrowIfExceeded(
            RecordKeyBytes,
            checked(sizeof(int) + encodedRecordKey.LongLength),
            SearchableStorageCapacityLimits.MaximumRecordKeyCanonicalBytes);
    }

    private static long ValidateRecordCore(
        string recordKey,
        Orleans.Runtime.GrainId grainId,
        byte[] payload,
        string etag,
        List<IndexEntry> indexEntries,
        byte[]? indexSchemaFingerprint)
    {
        ArgumentNullException.ThrowIfNull(recordKey);
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(etag);
        ArgumentNullException.ThrowIfNull(indexEntries);
        if (etag.Length == 0)
        {
            throw new ArgumentException("A stored ETag must not be empty.", nameof(etag));
        }

        ValidateGrainId(grainId);
        ValidateRecordKeyAndPayload(recordKey, payload);
        ThrowIfExceeded(
            RecordIndexEntries,
            indexEntries.Count,
            SearchableStorageCapacityLimits.MaximumIndexEntriesPerRecord);

        var perScopeCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        var indexByteCount = 0L;
        foreach (var entry in indexEntries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentNullException.ThrowIfNull(entry.Scope);
            ArgumentNullException.ThrowIfNull(entry.Value);
            var entryByteCount = GetIndexEntryCanonicalByteCount(entry);
            ThrowIfExceeded(
                IndexEntryBytes,
                entryByteCount,
                SearchableStorageCapacityLimits.MaximumIndexEntryCanonicalBytes);
            indexByteCount = checked(indexByteCount + entryByteCount);
            ThrowIfExceeded(
                RecordIndexBytes,
                indexByteCount,
                SearchableStorageCapacityLimits.MaximumIndexBytesPerRecord);

            perScopeCounts.TryGetValue(entry.Scope, out var scopeCount);
            scopeCount = checked(scopeCount + 1);
            ThrowIfExceeded(
                RecordScopeIndexEntries,
                scopeCount,
                SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope);
            perScopeCounts[entry.Scope] = scopeCount;
        }

        var recordByteCount = StorageMovePageDigest.GetEncodedByteCount(
            recordKey,
            grainId,
            payload,
            etag,
            indexEntries,
            indexSchemaFingerprint);
        ThrowIfExceeded(
            RecordBytes,
            recordByteCount,
            SearchableStorageCapacityLimits.MaximumRecordCanonicalBytes);
        ArgumentException.ThrowIfNullOrWhiteSpace(etag);
        return recordByteCount;
    }

    private static long GetIndexEntryCanonicalByteCount(IndexEntry entry)
    {
        const int persistedPrimitiveBytes =
            sizeof(long)
            + sizeof(ulong)
            + (4 * sizeof(int))
            + sizeof(long)
            + sizeof(long)
            + 16
            + sizeof(byte);
        return checked(
            StorageMovePageDigest.GetTextEncodedByteCount(entry.Scope)
            + sizeof(int)
            + sizeof(int)
            + sizeof(byte)
            + (entry.Value.Text is null
                ? 0
                : StorageMovePageDigest.GetTextEncodedByteCount(entry.Value.Text))
            + persistedPrimitiveBytes);
    }

    private static long GetJournalEntryCanonicalByteCount(StorageJournalEntry entry)
    {
        var total = checked(
            sizeof(long)
            + sizeof(long)
            + 16L
            + 16L
            + sizeof(int)
            + StorageMovePageDigest.GetTextEncodedByteCount(entry.RecordKey)
            + GetNullableTextByteCount(entry.ExpectedETag)
            + sizeof(byte)
            + sizeof(long)
            + sizeof(byte));
        if (entry.Record is not null)
        {
            total = checked(total + GetStoredRecordCanonicalByteCount(entry.Record));
        }

        if (entry.Move is { } move)
        {
            total = checked(total + GetMovePayloadCanonicalByteCount(move));
        }

        return total;
    }

    private static long GetMovePayloadCanonicalByteCount(StorageMoveJournalPayload move)
    {
        var total = checked(
            16L
            + sizeof(int)
            + sizeof(int)
            + sizeof(long)
            + sizeof(int)
            + sizeof(int)
            + sizeof(long)
            + GetNullableBytesByteCount(move.AfterRecordKey)
            + GetNullableBytesByteCount(move.NextRecordKey)
            + sizeof(byte)
            + sizeof(int)
            + move.PageDigest.LongLength
            + sizeof(long)
            + sizeof(int)
            + sizeof(int)
            + sizeof(int)
            + sizeof(int)
            + sizeof(long));
        foreach (var item in move.Imports)
        {
            total = checked(total + GetMoveRecordCanonicalByteCount(item));
        }

        foreach (var item in move.Deletes)
        {
            total = checked(
                total
                + sizeof(int) + item.RecordKey.LongLength
                + sizeof(int) + item.ExpectedETag.LongLength);
        }

        return total;
    }

    private static long GetNullableTextByteCount(string? value) =>
        checked(sizeof(byte) + (value is null ? 0 : StorageMovePageDigest.GetTextEncodedByteCount(value)));

    private static long GetNullableBytesByteCount(byte[]? value) =>
        checked(sizeof(byte) + (value is null ? 0 : sizeof(int) + value.LongLength));

    private static long GetMoveRecordCanonicalByteCount(StorageMoveRecord record)
    {
        var indexByteCount = 0L;
        foreach (var entry in record.Record.IndexEntries)
        {
            indexByteCount = checked(
                indexByteCount
                + StorageMovePageDigest.GetIndexEntryEncodedByteCount(entry));
        }

        return GetMoveRecordCanonicalByteCount(record, indexByteCount);
    }

    private static long GetMoveRecordCanonicalByteCount(
        StorageMoveRecord record,
        long indexByteCount)
    {
        ValidateEncodedTextShape(record.RecordKey, nameof(record.RecordKey));
        ValidateEncodedTextShape(record.Record.ETag, nameof(record.Record.ETag));
        return checked(
            sizeof(int) + record.RecordKey.LongLength
            + sizeof(int) + record.Record.GrainType.LongLength
            + sizeof(int) + record.Record.GrainKey.LongLength
            + sizeof(int) + record.Record.Payload.LongLength
            + sizeof(int) + record.Record.ETag.LongLength
            + sizeof(int) + indexByteCount
            + (record.Record.IndexSchemaFingerprint is null
                ? 0
                : sizeof(byte) + record.Record.IndexSchemaFingerprint.LongLength));
    }

    private static long GetStoredRecordCanonicalByteCount(StoredRecord record)
    {
        var indexByteCount = 0L;
        foreach (var entry in record.IndexEntries)
        {
            indexByteCount = checked(
                indexByteCount
                + StorageMovePageDigest.GetIndexEntryEncodedByteCount(entry));
        }

        return checked(
            sizeof(int) + record.GrainId.Type.AsSpan().Length
            + sizeof(int) + record.GrainId.Key.AsSpan().Length
            + sizeof(int) + record.Payload.LongLength
            + StorageMovePageDigest.GetTextEncodedByteCount(record.ETag)
            + sizeof(int) + indexByteCount
            + (record.IndexSchemaFingerprint is null
                ? 0
                : sizeof(byte) + record.IndexSchemaFingerprint.LongLength));
    }

    private static void ValidateEncodedTextShape(byte[] value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if ((value.Length & 1) != 0)
        {
            throw new ArgumentException(
                "A lossless text value must contain complete UTF-16 code units.",
                parameterName);
        }
    }

    private static void ValidateMovementItemCount(int importCount, int deleteCount)
    {
        if (checked(importCount + deleteCount) > StorageMoveProtocol.MaximumPageRecords)
        {
            throw new ArgumentException(
                $"A movement journal page cannot contain more than "
                + $"{StorageMoveProtocol.MaximumPageRecords} items.");
        }
    }

    private static void ThrowIfExceeded(string boundary, long actual, long limit)
    {
        if (actual > limit)
        {
            throw new SearchableStorageCapacityExceededException(boundary, actual, limit);
        }
    }
}

/// <summary>Dependency-free GrainId admission shared by structural and aggregate validation.</summary>
internal static class StorageGrainIdCapacity
{
    public static void Validate(
        Orleans.Runtime.GrainId grainId,
        string? parameterName = null)
    {
        ValidateParts(
            grainId.Type.AsSpan().Length,
            grainId.Key.AsSpan().Length,
            parameterName);
    }

    public static void ValidateParts(
        long typeBytes,
        long keyBytes,
        string? parameterName = null)
    {
        if (typeBytes <= 0 || keyBytes <= 0)
        {
            throw new ArgumentException(
                "A stored GrainId must contain non-empty type and key components.",
                parameterName);
        }

        if (typeBytes > SearchableStorageCapacityLimits.MaximumGrainTypeBytes)
        {
            throw new SearchableStorageCapacityExceededException(
                StorageCapacityGuardrails.GrainTypeBytes,
                typeBytes,
                SearchableStorageCapacityLimits.MaximumGrainTypeBytes);
        }

        if (keyBytes > SearchableStorageCapacityLimits.MaximumGrainKeyBytes)
        {
            throw new SearchableStorageCapacityExceededException(
                StorageCapacityGuardrails.GrainKeyBytes,
                keyBytes,
                SearchableStorageCapacityLimits.MaximumGrainKeyBytes);
        }
    }
}

/// <summary>Tracks the aggregate snapshot envelope without rescanning a partition per mutation.</summary>
internal sealed class StorageCapacityTracker
{
    private long _canonicalByteCount;

    public StorageCapacityTracker(IReadOnlyDictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(records);
        RecordCount = records.Count;
        _canonicalByteCount = StorageCapacityGuardrails.ValidateSnapshotRecords(records);
    }

    public int RecordCount { get; private set; }

    public long CanonicalByteCount => _canonicalByteCount;

    public void ValidateProjectedUpsert(
        IReadOnlyDictionary<string, StoredRecord> records,
        string recordKey,
        StoredRecord record)
    {
        ArgumentNullException.ThrowIfNull(records);
        var newByteCount = StorageCapacityGuardrails.ValidateRecord(recordKey, record);
        var oldByteCount = records.TryGetValue(recordKey, out var current)
            ? StorageCapacityGuardrails.ValidateRecord(recordKey, current)
            : 0;
        var projectedCount = records.ContainsKey(recordKey) ? RecordCount : checked(RecordCount + 1);
        ValidateProjection(
            projectedCount,
            checked(_canonicalByteCount - oldByteCount + newByteCount));
    }

    public void ValidateProjectedImports(
        IReadOnlyDictionary<string, StoredRecord> records,
        IReadOnlyList<StorageMoveRecord> imports)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(imports);
        var projectedCount = RecordCount;
        var projectedBytes = _canonicalByteCount;
        var pageKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in imports)
        {
            var newByteCount = StorageCapacityGuardrails.ValidateMoveRecord(item);
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            if (!pageKeys.Add(recordKey))
            {
                throw new InvalidOperationException("An import page contains duplicate record keys.");
            }

            if (records.TryGetValue(recordKey, out var current))
            {
                projectedBytes = checked(
                    projectedBytes
                    - StorageCapacityGuardrails.ValidateRecord(recordKey, current)
                    + newByteCount);
            }
            else
            {
                projectedCount = checked(projectedCount + 1);
                projectedBytes = checked(projectedBytes + newByteCount);
            }

            ValidateProjection(projectedCount, projectedBytes);
        }
    }

    public void ApplyUpsert(
        IReadOnlyDictionary<string, StoredRecord> records,
        string recordKey,
        StoredRecord record)
    {
        var newByteCount = StorageCapacityGuardrails.ValidateRecord(recordKey, record);
        if (records.TryGetValue(recordKey, out var current))
        {
            _canonicalByteCount = checked(
                _canonicalByteCount
                - StorageCapacityGuardrails.ValidateRecord(recordKey, current)
                + newByteCount);
        }
        else
        {
            RecordCount = checked(RecordCount + 1);
            _canonicalByteCount = checked(_canonicalByteCount + newByteCount);
        }

        ValidateProjection(RecordCount, _canonicalByteCount);
    }

    public void ApplyDelete(
        IReadOnlyDictionary<string, StoredRecord> records,
        string recordKey)
    {
        if (!records.TryGetValue(recordKey, out var current))
        {
            return;
        }

        RecordCount--;
        _canonicalByteCount = checked(
            _canonicalByteCount - StorageCapacityGuardrails.ValidateRecord(recordKey, current));
    }

    private static void ValidateProjection(int recordCount, long canonicalByteCount)
    {
        if (recordCount > SearchableStorageCapacityLimits.MaximumSnapshotRecords)
        {
            throw new SearchableStorageCapacityExceededException(
                StorageCapacityGuardrails.SnapshotRecords,
                recordCount,
                SearchableStorageCapacityLimits.MaximumSnapshotRecords);
        }

        if (canonicalByteCount > SearchableStorageCapacityLimits.MaximumSnapshotCanonicalBytes)
        {
            throw new SearchableStorageCapacityExceededException(
                StorageCapacityGuardrails.SnapshotBytes,
                canonicalByteCount,
                SearchableStorageCapacityLimits.MaximumSnapshotCanonicalBytes);
        }
    }
}
