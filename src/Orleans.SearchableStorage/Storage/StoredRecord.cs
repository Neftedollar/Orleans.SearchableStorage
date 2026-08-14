using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

[GenerateSerializer]
internal sealed class StoredRecord
{
    [Id(0)]
    public required GrainId GrainId { get; init; }

    [Id(1)]
    public byte[]? Payload { get; init; }

    [Id(2)]
    public required string ETag { get; init; }

    [Id(3)]
    public required List<IndexEntry> IndexEntries { get; init; }

    /// <summary>
    /// Identifies the managed schema used to derive <see cref="IndexEntries"/>. Legacy records
    /// leave this appended field absent until an explicit rebuild adopts them.
    /// </summary>
    [Id(4)]
    public byte[]? IndexSchemaFingerprint { get; init; }
}

internal static class StoredRecordNamespaceValidation
{
    public static void Validate(StoredRecord record, StorageNamespaceMode namespaceMode)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (!Enum.IsDefined(namespaceMode))
        {
            throw new ArgumentOutOfRangeException(nameof(namespaceMode));
        }

        if ((namespaceMode == StorageNamespaceMode.IndexOnly) != (record.Payload is null))
        {
            throw new InvalidOperationException(
                namespaceMode == StorageNamespaceMode.IndexOnly
                    ? "An index-only namespace record must not contain an application payload."
                    : "An integrated storage record must contain an application payload.");
        }
    }

    public static void ValidateAll(
        IEnumerable<StoredRecord> records,
        StorageNamespaceMode namespaceMode)
    {
        ArgumentNullException.ThrowIfNull(records);
        foreach (var record in records)
        {
            Validate(record, namespaceMode);
        }
    }
}
