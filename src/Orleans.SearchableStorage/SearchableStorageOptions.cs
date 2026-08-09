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

    /// <inheritdoc />
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = default!;
}
