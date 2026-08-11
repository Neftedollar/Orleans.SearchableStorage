using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

/// <summary>
/// Keeps pre-movement direct partition doubles source-compatible while making every unexpected
/// movement RPC fail loudly. Concrete doubles still provide the query or storage methods under test.
/// </summary>
internal abstract class StoragePartitionGrainMovementTestDouble : IStoragePartitionGrain
{
    Task<StorageReadResult> IStoragePartitionGrain.ReadAsync(string recordKey) =>
        throw UnexpectedNonMovementCall();

    Task<StorageReadResult> IStoragePartitionGrain.ReadRoutedAsync(RoutedStorageReadRequest request) =>
        throw UnexpectedNonMovementCall();

    Task<string> IStoragePartitionGrain.WriteAsync(StorageWriteRequest request) =>
        throw UnexpectedNonMovementCall();

    Task<string> IStoragePartitionGrain.WriteRoutedAsync(RoutedStorageWriteRequest request) =>
        throw UnexpectedNonMovementCall();

    Task IStoragePartitionGrain.ClearAsync(StorageClearRequest request) =>
        throw UnexpectedNonMovementCall();

    Task IStoragePartitionGrain.ClearRoutedAsync(RoutedStorageClearRequest request) =>
        throw UnexpectedNonMovementCall();

    Task<GrainId[]> IStoragePartitionGrain.FindAsync(ExactIndexQuery query) =>
        throw UnexpectedNonMovementCall();

    Task<GrainId[]> IStoragePartitionGrain.FindRoutedAsync(RoutedExactIndexQuery query) =>
        throw UnexpectedNonMovementCall();

    Task<GrainId[]> IStoragePartitionGrain.RangeAsync(RangeIndexQuery query) =>
        throw UnexpectedNonMovementCall();

    Task<GrainId[]> IStoragePartitionGrain.RangeRoutedAsync(RoutedRangeIndexQuery query) =>
        throw UnexpectedNonMovementCall();

    Task<GrainId[]> IStoragePartitionGrain.QueryAsync(PartitionQueryPlan query) =>
        throw UnexpectedNonMovementCall();

    Task<GrainId[]> IStoragePartitionGrain.QueryRoutedAsync(RoutedPartitionQuery query) =>
        throw UnexpectedNonMovementCall();

    Task<PartitionQueryPageResult> IStoragePartitionGrain.QueryPageRoutedAsync(
        RoutedPartitionQueryPageRequest request) => throw UnexpectedNonMovementCall();

    Task<PartitionDistinctFacetPageResult> IStoragePartitionGrain.QueryDistinctFacetPageRoutedAsync(
        RoutedPartitionDistinctFacetPageRequest request) => throw UnexpectedNonMovementCall();

    Task<PartitionFacetCandidatePageResult> IStoragePartitionGrain.QueryFacetCandidatesRoutedAsync(
        RoutedPartitionFacetCandidatePageRequest request) => throw UnexpectedNonMovementCall();

    Task<PartitionFacetCountSliceResult> IStoragePartitionGrain.QueryFacetCountSliceRoutedAsync(
        RoutedPartitionFacetCountSliceRequest request) => throw UnexpectedNonMovementCall();

    Task IStoragePartitionGrain.CompactAsync() => throw UnexpectedNonMovementCall();

    Task<StoragePartitionPersistenceInfo> IStoragePartitionGrain.GetPersistenceInfoAsync() =>
        throw UnexpectedNonMovementCall();

    public Task<StoragePartitionProtocolState> EnableMovementProtocolAsync(
        StoragePartitionProtocolRequest request) => throw UnexpectedMovementCall();

    public Task<StoragePartitionProtocolState> GetMovementStateAsync() =>
        throw UnexpectedMovementCall();

    public Task<StoragePartitionProtocolState> FreezeMoveSourceAsync(StorageMoveIdentity move) =>
        throw UnexpectedMovementCall();

    public Task<StoragePartitionProtocolState> PrepareMoveTargetAsync(
        StorageMoveTargetPrepareRequest request) => throw UnexpectedMovementCall();

    public Task<StorageMoveExportPage> ExportMovePageAsync(StorageMovePageRequest request) =>
        throw UnexpectedMovementCall();

    public Task<StorageMovePageCommitResult> ImportMovePageAsync(
        StorageMoveImportPageRequest request) => throw UnexpectedMovementCall();

    public Task<StoragePartitionProtocolState> HideMoveSourceAsync(
        StorageMoveVisibilityFenceRequest request) => throw UnexpectedMovementCall();

    public Task<StoragePartitionProtocolState> EnableMoveTargetAsync(StorageMoveIdentity move) =>
        throw UnexpectedMovementCall();

    public Task<StorageMovePageCommitResult> DeleteMovePageAsync(
        StorageMoveDeletePageRequest request) => throw UnexpectedMovementCall();

    public Task<StoragePartitionProtocolState> RetireMoveParticipantAsync(
        StorageMoveRetireRequest request) => throw UnexpectedMovementCall();

    private static NotSupportedException UnexpectedMovementCall() =>
        new("This direct partition test double does not participate in live movement.");

    private static NotSupportedException UnexpectedNonMovementCall() =>
        new("The concrete direct partition test double must implement this operation.");
}

/// <summary>
/// Supplies explicit failures for movement calls on pre-movement layout doubles.
/// </summary>
internal abstract class StorageLayoutGrainMovementTestDouble : IStorageLayoutGrain
{
    Task IStorageLayoutGrain.InitializeAsync(StorageLayoutDescriptor descriptor) =>
        throw UnexpectedNonMovementCall();

    Task<StorageLayoutSnapshot> IStorageLayoutGrain.InitializeRoutingAsync(
        StorageLayoutDescriptor descriptor) => throw UnexpectedNonMovementCall();

    Task<bool> IStorageLayoutGrain.ValidateAsync(StorageLayoutDescriptor descriptor) =>
        throw UnexpectedNonMovementCall();

    Task<bool> IStorageLayoutGrain.ValidateIdentityAsync(StorageLayoutIdentity identity) =>
        throw UnexpectedNonMovementCall();

    Task<StorageLayoutSnapshot?> IStorageLayoutGrain.GetLayoutAsync(StorageLayoutIdentity identity) =>
        throw UnexpectedNonMovementCall();

    Task<StorageLayoutSnapshot?> IStorageLayoutGrain.GetCurrentLayoutAsync() =>
        throw UnexpectedNonMovementCall();

    public Task<StorageLayoutSnapshot> BeginMovementEnablementAsync() =>
        throw UnexpectedMovementCall();

    public Task<StorageLayoutSnapshot> AdvanceMovementEnablementAsync(Guid enablementId) =>
        throw UnexpectedMovementCall();

    public Task<StorageSlotMoveProgressSnapshot> PlanMoveAsync(StorageSlotMovePlanRequest request) =>
        throw UnexpectedMovementCall();

    public Task<StorageSlotMoveProgressSnapshot?> GetMoveProgressAsync() =>
        throw UnexpectedMovementCall();

    public Task<StorageSlotMoveProgressSnapshot> AdvanceMoveAsync(StorageSlotMoveCommand command) =>
        throw UnexpectedMovementCall();

    public Task<StorageSlotMoveProgressSnapshot> RequestMoveAbortAsync(StorageSlotMoveCommand command) =>
        throw UnexpectedMovementCall();

    private static NotSupportedException UnexpectedMovementCall() =>
        new("This direct layout test double does not participate in live movement.");

    private static NotSupportedException UnexpectedNonMovementCall() =>
        new("The concrete direct layout test double must implement this operation.");
}
