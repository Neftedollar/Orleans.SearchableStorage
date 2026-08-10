using Orleans.Storage;

namespace Orleans.SearchableStorage;

/// <summary>
/// Configures one searchable grain-storage provider.
/// </summary>
public sealed class SearchableStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    /// Gets or sets the number of stable storage partitions.
    /// </summary>
    /// <remarks>
    /// This value is part of the persisted data layout and must not be changed after data is written.
    /// </remarks>
    public int PartitionCount { get; set; } = 32;

    /// <summary>
    /// Gets or sets the target used to seed the immutable virtual-slot count for a new or
    /// version-3 layout.
    /// </summary>
    /// <remarks>
    /// The persisted slot count is rounded up to the smallest multiple of
    /// <see cref="PartitionCount"/> which reaches this target. The target must remain a valid,
    /// addressable configured value, but it is not compared with an existing version-4 layout, so
    /// changing a future library default cannot invalidate that namespace.
    /// </remarks>
    public int VirtualSlotTargetCount { get; set; } = Storage.StorageLayout.DefaultVirtualSlotTargetCount;

    /// <summary>
    /// Gets or sets the maximum number of operations stored in one journal segment.
    /// </summary>
    /// <remarks>
    /// This value is part of the persisted data layout and must not be changed after data is written.
    /// </remarks>
    public int JournalSegmentCapacity { get; set; } = Storage.StoragePersistence.DefaultJournalSegmentCapacity;

    /// <summary>
    /// Gets or sets the maximum number of journal entries replayed while activating a partition.
    /// </summary>
    /// <remarks>
    /// This value is part of the persisted data layout and must not be changed after data is written.
    /// </remarks>
    public int MaximumJournalReplayEntries { get; set; } = Storage.StoragePersistence.DefaultMaximumJournalReplayEntries;

    /// <summary>
    /// Gets or sets the number of committed journal entries after which compaction is requested.
    /// </summary>
    /// <remarks>
    /// This is an operational setting and can be changed without migrating persisted data. It must
    /// not exceed <see cref="MaximumJournalReplayEntries"/>.
    /// </remarks>
    public int CompactionThreshold { get; set; } = Storage.StoragePersistence.DefaultCompactionThreshold;

    /// <inheritdoc />
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = default!;
}
