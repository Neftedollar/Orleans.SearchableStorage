namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Validates the representation-independent index projection retained with one stored record.
/// </summary>
internal static class StoragePartitionIndexValidation
{
    public static void ValidateRecord(StoredRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(record.IndexEntries);

        foreach (var entry in record.IndexEntries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentException.ThrowIfNullOrWhiteSpace(entry.Scope);
            ArgumentNullException.ThrowIfNull(entry.Value);
            if (entry.Kind is not SearchableIndexKind.Hash and not SearchableIndexKind.Range)
            {
                throw new InvalidOperationException($"Unknown index kind '{entry.Kind}'.");
            }
        }
    }
}
