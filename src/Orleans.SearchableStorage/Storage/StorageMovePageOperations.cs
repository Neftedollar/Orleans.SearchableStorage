using System.Globalization;

namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Hosts the production page and apply paths so protocol benchmarks can measure the same code
/// used by the grain without constructing an Orleans activation.
/// </summary>
internal static class StorageMovePageOperations
{
    public static List<StorageMoveRecord> CreateExportRecords(
        StoragePartitionView view,
        int slot,
        byte[]? afterRecordKey,
        int itemLimit,
        int byteTarget,
        out byte[]? nextRecordKey,
        out bool exhausted,
        out long encodedByteCount)
    {
        ArgumentNullException.ThrowIfNull(view);
        StorageMoveProtocol.ValidatePageLimits(itemLimit, byteTarget, nameof(itemLimit));
        var records = new List<StorageMoveRecord>(itemLimit);
        encodedByteCount = 0;
        var hasMore = false;
        var decodedAfterRecordKey = StorageMoveRecordCodec.DecodeNullableText(
            afterRecordKey,
            nameof(afterRecordKey));
        using var enumerator = GetSlotCatalog(view)
            .EnumerateAfter(slot, decodedAfterRecordKey)
            .GetEnumerator();
        while (enumerator.MoveNext())
        {
            if (records.Count == itemLimit)
            {
                hasMore = true;
                break;
            }

            var recordKey = enumerator.Current;
            if (!view.Records.TryGetValue(recordKey, out var record))
            {
                throw new InvalidOperationException(
                    $"Virtual-slot catalog references missing record '{recordKey}'.");
            }

            var item = StorageMoveRecordCodec.Encode(recordKey, record);
            var itemBytes = StorageMovePageDigest.GetEncodedByteCount(item);
            var candidateBytes = StorageMovePageDigest.CheckedAddEncodedByteCount(
                encodedByteCount,
                itemBytes);
            if (records.Count > 0 && candidateBytes > byteTarget)
            {
                hasMore = true;
                break;
            }

            records.Add(item);
            encodedByteCount = candidateBytes;
            if (encodedByteCount > byteTarget)
            {
                hasMore = enumerator.MoveNext();
                break;
            }
        }

        exhausted = !hasMore;
        nextRecordKey = records.Count == 0
            ? StorageMoveRecordCodec.CopyText(afterRecordKey)
            : [.. records[^1].RecordKey];
        return records;
    }

    public static List<StorageMoveDeleteRecord> CreateDeleteRecords(
        StoragePartitionView view,
        int slot,
        byte[]? afterRecordKey,
        int itemLimit,
        int byteTarget,
        out byte[]? nextRecordKey,
        out bool exhausted,
        out long encodedByteCount)
    {
        ArgumentNullException.ThrowIfNull(view);
        StorageMoveProtocol.ValidatePageLimits(itemLimit, byteTarget, nameof(itemLimit));
        var records = new List<StorageMoveDeleteRecord>(itemLimit);
        encodedByteCount = 0;
        var hasMore = false;
        var decodedAfterRecordKey = StorageMoveRecordCodec.DecodeNullableText(
            afterRecordKey,
            nameof(afterRecordKey));
        using var enumerator = GetSlotCatalog(view)
            .EnumerateAfter(slot, decodedAfterRecordKey)
            .GetEnumerator();
        while (enumerator.MoveNext())
        {
            if (records.Count == itemLimit)
            {
                hasMore = true;
                break;
            }

            var recordKey = enumerator.Current;
            if (!view.Records.TryGetValue(recordKey, out var record))
            {
                throw new InvalidOperationException(
                    $"Virtual-slot catalog references missing record '{recordKey}'.");
            }

            var item = StorageMoveRecordCodec.EncodeDelete(recordKey, record.ETag);
            var itemBytes = StorageMovePageDigest.GetEncodedByteCount(item);
            var candidateBytes = StorageMovePageDigest.CheckedAddEncodedByteCount(
                encodedByteCount,
                itemBytes);
            if (records.Count > 0 && candidateBytes > byteTarget)
            {
                hasMore = true;
                break;
            }

            records.Add(item);
            encodedByteCount = candidateBytes;
            if (encodedByteCount > byteTarget)
            {
                hasMore = enumerator.MoveNext();
                break;
            }
        }

        exhausted = !hasMore;
        nextRecordKey = records.Count == 0
            ? StorageMoveRecordCodec.CopyText(afterRecordKey)
            : [.. records[^1].RecordKey];
        return records;
    }

    public static void ApplyImports(
        StoragePartitionView view,
        IReadOnlyList<StorageMoveRecord> imports)
    {
        ArgumentNullException.ThrowIfNull(view);
        ArgumentNullException.ThrowIfNull(imports);
        foreach (var item in imports)
        {
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            view.ApplyUpsert(
                recordKey,
                StorageMoveRecordCodec.Decode(item.Record));
        }
    }

    public static void ApplyDeletes(
        StoragePartitionView view,
        IReadOnlyList<StorageMoveDeleteRecord> deletes)
    {
        ArgumentNullException.ThrowIfNull(view);
        ArgumentNullException.ThrowIfNull(deletes);
        foreach (var item in deletes)
        {
            view.ApplyDelete(StorageMoveRecordCodec.DecodeRecordKey(item));
        }
    }

    public static void ValidateImportAgainstCurrentView(
        StoragePartitionView view,
        StorageMoveExportPage page,
        StoragePartitionMoveControl control,
        long nextVersion)
    {
        ArgumentNullException.ThrowIfNull(view);
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(page.Move);
        ArgumentNullException.ThrowIfNull(page.Records);
        ArgumentNullException.ThrowIfNull(control);
        if (nextVersion < control.FrozenNextVersion)
        {
            throw new InvalidOperationException("The target version fence is not durable.");
        }

        foreach (var item in page.Records)
        {
            StorageMoveRecordCodec.Validate(item, nameof(page));
            var recordKey = StorageMoveRecordCodec.DecodeRecordKey(item);
            var record = StorageMoveRecordCodec.Decode(item.Record);
            if (view.Records.ContainsKey(recordKey)
                || StorageLayout.GetSlot(record.GrainId, page.Move.VirtualSlotCount)
                    != page.Move.Slot
                || !long.TryParse(
                    record.ETag,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var version)
                || version <= 0
                || version >= control.FrozenNextVersion)
            {
                throw new InvalidOperationException(
                    $"Import page {page.PageOrdinal} contains a collision or invalid source record.");
            }
        }
    }

    private static StoragePartitionSlotCatalog GetSlotCatalog(StoragePartitionView view)
    {
        return view.SlotCatalog
            ?? throw new InvalidOperationException(
                "The partition virtual-slot catalog has not been initialized.");
    }
}
