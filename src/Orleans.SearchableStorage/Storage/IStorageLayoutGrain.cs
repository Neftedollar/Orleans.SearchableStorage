namespace Orleans.SearchableStorage.Storage;

internal interface IStorageLayoutGrain : IGrainWithStringKey
{
    Task InitializeAsync(StorageLayoutDescriptor descriptor);

    Task<bool> ValidateAsync(StorageLayoutDescriptor descriptor);

    Task<bool> ValidateIdentityAsync(StorageLayoutIdentity identity);
}
