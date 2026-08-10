namespace Orleans.SearchableStorage;

/// <summary>
/// Reads the persisted routing layout for one searchable-storage provider.
/// </summary>
public interface ISearchableStorageAdminClient
{
    /// <summary>
    /// Gets the persisted layout, or <see langword="null"/> when the provider has not initialized
    /// its layout yet.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait without canceling a shared layout read.</param>
    /// <returns>The persisted routing layout, or <see langword="null"/>.</returns>
    Task<SearchableStorageLayout?> GetLayoutAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Describes one immutable snapshot of a searchable-storage routing layout.
/// </summary>
public sealed class SearchableStorageLayout
{
    /// <summary>
    /// Gets the routing epoch represented by this snapshot.
    /// </summary>
    public required long Epoch { get; init; }

    /// <summary>
    /// Gets the physical partition count used to seed the zero-movement identity layout.
    /// </summary>
    public required int InitialPartitionCount { get; init; }

    /// <summary>
    /// Gets the immutable number of virtual slots in this provider namespace.
    /// </summary>
    public required int VirtualSlotCount { get; init; }

    /// <summary>
    /// Gets a per-owner summary without exposing the mutable serialized assignment array.
    /// </summary>
    public required IReadOnlyList<SearchableStoragePartitionLayout> Partitions { get; init; }
}

/// <summary>
/// Describes the virtual slots assigned to one physical partition.
/// </summary>
public sealed class SearchableStoragePartitionLayout
{
    /// <summary>
    /// Gets the physical partition index.
    /// </summary>
    public required int PartitionIndex { get; init; }

    /// <summary>
    /// Gets the number of virtual slots assigned to this partition.
    /// </summary>
    public required int SlotCount { get; init; }
}
