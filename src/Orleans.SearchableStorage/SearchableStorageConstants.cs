namespace Orleans.SearchableStorage;

/// <summary>
/// Contains names shared by the searchable storage provider and its physical persistence provider.
/// </summary>
public static class SearchableStorageConstants
{
    /// <summary>
    /// Gets the Orleans storage-provider name used to persist storage-partition grains.
    /// </summary>
    /// <remarks>
    /// Register exactly one physical grain storage provider under this name. The provider can be
    /// backed by PostgreSQL, Redis, memory, or another Orleans persistence implementation.
    /// </remarks>
    public const string PhysicalStorageProviderName = "Orleans.SearchableStorage.Physical";
}
