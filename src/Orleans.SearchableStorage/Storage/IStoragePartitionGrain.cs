using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal interface IStoragePartitionGrain : IGrainWithStringKey
{
    Task<StorageReadResult> ReadAsync(string recordKey);

    Task<string> WriteAsync(StorageWriteRequest request);

    Task ClearAsync(StorageClearRequest request);

    Task<GrainId[]> FindAsync(ExactIndexQuery query);

    Task<GrainId[]> RangeAsync(RangeIndexQuery query);

    Task<GrainId[]> QueryAsync(PartitionQueryPlan query);

    Task CompactAsync();

    Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync();
}
