using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal interface IStoragePartitionGrain : IGrainWithStringKey
{
    Task<StorageReadResult> ReadAsync(string recordKey);

    Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request);

    Task<string> WriteAsync(StorageWriteRequest request);

    Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request);

    Task ClearAsync(StorageClearRequest request);

    Task ClearRoutedAsync(RoutedStorageClearRequest request);

    Task<GrainId[]> FindAsync(ExactIndexQuery query);

    Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query);

    Task<GrainId[]> RangeAsync(RangeIndexQuery query);

    Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query);

    Task<GrainId[]> QueryAsync(PartitionQueryPlan query);

    Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query);

    Task<PartitionQueryPageResult> QueryPageRoutedAsync(RoutedPartitionQueryPageRequest request);

    Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
        RoutedPartitionDistinctFacetPageRequest request);

    Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
        RoutedPartitionFacetCandidatePageRequest request);

    Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
        RoutedPartitionFacetCountSliceRequest request);

    Task CompactAsync();

    Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync();
}
