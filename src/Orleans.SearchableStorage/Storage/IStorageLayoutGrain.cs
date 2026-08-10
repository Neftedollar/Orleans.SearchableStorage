namespace Orleans.SearchableStorage.Storage;

internal interface IStorageLayoutGrain : IGrainWithStringKey
{
    Task InitializeAsync(StorageLayoutDescriptor descriptor);

    Task<StorageLayoutSnapshot> InitializeRoutingAsync(StorageLayoutDescriptor descriptor);

    Task<bool> ValidateAsync(StorageLayoutDescriptor descriptor);

    Task<bool> ValidateIdentityAsync(StorageLayoutIdentity identity);

    Task<StorageLayoutSnapshot?> GetLayoutAsync(StorageLayoutIdentity identity);

    Task<StorageLayoutSnapshot?> GetCurrentLayoutAsync();
}
