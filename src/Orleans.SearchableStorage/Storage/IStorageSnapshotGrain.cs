namespace Orleans.SearchableStorage.Storage;

internal interface IStorageSnapshotGrain : IGrainWithStringKey
{
    Task StoreAsync(StorageSnapshotState snapshot);

    Task<StorageSnapshotState> ReadAsync();

    Task RetireAsync(StorageSnapshotDescriptor descriptor);
}
