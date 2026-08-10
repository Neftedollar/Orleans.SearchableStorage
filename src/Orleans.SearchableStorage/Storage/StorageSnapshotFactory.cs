namespace Orleans.SearchableStorage.Storage;

/// <summary>
/// Builds the detached snapshot payload published by partition compaction.
/// </summary>
internal static class StorageSnapshotFactory
{
    public static StorageSnapshotState Create(
        StorageSnapshotDescriptor descriptor,
        IReadOnlyDictionary<string, StoredRecord> records)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(records);

        return new StorageSnapshotState
        {
            Initialized = true,
            Slot = descriptor.Slot,
            Generation = descriptor.Generation,
            SnapshotId = descriptor.SnapshotId,
            Sequence = descriptor.Sequence,
            OperationId = descriptor.OperationId,
            NextVersion = descriptor.NextVersion,
            Records = records.ToDictionary(
                static pair => pair.Key,
                static pair => StoragePersistenceStateCopy.CopyRecord(pair.Value)!,
                StringComparer.Ordinal),
        };
    }
}
