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

    Task<StoragePartitionProtocolState> EnableMovementProtocolAsync(
        StoragePartitionProtocolRequest request);

    Task<StoragePartitionProtocolState> GetMovementStateAsync();

    Task<StoragePartitionProtocolState> FreezeMoveSourceAsync(StorageMoveIdentity move);

    Task<StoragePartitionProtocolState> PrepareMoveTargetAsync(
        StorageMoveTargetPrepareRequest request);

    Task<StorageMoveExportPage> ExportMovePageAsync(StorageMovePageRequest request);

    Task<StorageMovePageCommitResult> ImportMovePageAsync(
        StorageMoveImportPageRequest request);

    Task<StoragePartitionProtocolState> HideMoveSourceAsync(
        StorageMoveVisibilityFenceRequest request);

    Task<StoragePartitionProtocolState> EnableMoveTargetAsync(StorageMoveIdentity move);

    Task<StorageMovePageCommitResult> DeleteMovePageAsync(
        StorageMoveDeletePageRequest request);

    Task<StoragePartitionProtocolState> RetireMoveParticipantAsync(
        StorageMoveRetireRequest request);

    Task CompactAsync();

    Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync();
}
