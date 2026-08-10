using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage;

/// <summary>
/// Reads searchable-storage routing metadata through an Orleans grain factory.
/// </summary>
public sealed class SearchableStorageAdminClient : ISearchableStorageAdminClient
{
    private readonly StorageLayoutCache _layoutCache;

    /// <summary>
    /// Initializes a client for one searchable-storage provider.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to read the layout grain.</param>
    /// <param name="providerName">The searchable-storage provider name.</param>
    /// <param name="partitionCount">The provider's initial physical partition count.</param>
    public SearchableStorageAdminClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);

        var layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var identity = StorageLayout.CreateIdentity(providerName, partitionCount);
        _layoutCache = new StorageLayoutCache(() => layoutGrain.GetLayoutAsync(identity));
    }

    internal SearchableStorageAdminClient(StorageLayoutCache layoutCache)
    {
        ArgumentNullException.ThrowIfNull(layoutCache);
        _layoutCache = layoutCache;
    }

    /// <inheritdoc />
    public async Task<SearchableStorageLayout?> GetLayoutAsync(
        CancellationToken cancellationToken = default)
    {
        var layout = await _layoutCache.GetAsync(cancellationToken);
        if (layout is null)
        {
            return null;
        }

        var assignments = layout.CopySlotAssignments();
        var partitions = Array.AsReadOnly(assignments
            .GroupBy(static owner => owner)
            .OrderBy(static group => group.Key)
            .Select(static group => new SearchableStoragePartitionLayout
            {
                PartitionIndex = group.Key,
                SlotCount = group.Count(),
            })
            .ToArray());

        return new SearchableStorageLayout
        {
            Epoch = layout.Epoch,
            InitialPartitionCount = layout.InitialPartitionCount,
            VirtualSlotCount = layout.VirtualSlotCount,
            Partitions = partitions,
        };
    }
}
