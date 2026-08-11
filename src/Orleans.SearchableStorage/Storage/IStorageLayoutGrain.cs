using Orleans.Concurrency;

namespace Orleans.SearchableStorage.Storage;

internal interface IStorageLayoutGrain : IGrainWithStringKey
{
    Task InitializeAsync(StorageLayoutDescriptor descriptor);

    Task<StorageLayoutSnapshot> InitializeRoutingAsync(StorageLayoutDescriptor descriptor);

    Task<bool> ValidateAsync(StorageLayoutDescriptor descriptor);

    Task<bool> ValidateIdentityAsync(StorageLayoutIdentity identity);

    Task<StorageLayoutSnapshot?> GetLayoutAsync(StorageLayoutIdentity identity);

    [AlwaysInterleave]
    Task<StorageLayoutSnapshot?> GetCurrentLayoutAsync();

    Task<StorageLayoutSnapshot> BeginMovementEnablementAsync();

    Task<StorageLayoutSnapshot> AdvanceMovementEnablementAsync(Guid enablementId);

    Task<StorageSlotMoveProgressSnapshot> PlanMoveAsync(StorageSlotMovePlanRequest request);

    Task<StorageSlotMoveProgressSnapshot?> GetMoveProgressAsync();

    Task<StorageSlotMoveProgressSnapshot> AdvanceMoveAsync(StorageSlotMoveCommand command);

    Task<StorageSlotMoveProgressSnapshot> RequestMoveAbortAsync(StorageSlotMoveCommand command);
}
