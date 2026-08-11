namespace Orleans.SearchableStorage;

/// <summary>
/// Reads and administers one searchable-storage provider, including managed index-schema status
/// and rebuilds, persisted routing, movement enablement, virtual-slot moves, rollback, and
/// rebalancing.
/// </summary>
public interface ISearchableStorageAdminClient
{
    /// <summary>
    /// Gets the persisted layout, or <see langword="null"/> when the provider has not initialized
    /// its layout yet.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait without canceling a shared layout read.</param>
    /// <returns>The persisted routing layout, or <see langword="null"/>.</returns>
    Task<SearchableStorageLayout?> GetLayoutAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the durable lifecycle state for one explicitly registered index schema.
    /// </summary>
    /// <typeparam name="TState">The registered Orleans persistent-state type.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>The durable schema status.</returns>
    Task<SearchableStorageIndexSchemaStatus> GetIndexSchemaAsync<TState>(
        string stateName,
        CancellationToken cancellationToken = default)
    {
        return GetIndexSchemaAsync<TState>(
            stateName,
            applicationSchemaVersion: 1,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Gets the durable lifecycle state for an explicitly versioned index schema.
    /// </summary>
    /// <typeparam name="TState">The registered Orleans persistent-state type.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="applicationSchemaVersion">
    /// The positive application-owned version used during state registration.
    /// </param>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>The durable schema status.</returns>
    Task<SearchableStorageIndexSchemaStatus> GetIndexSchemaAsync<TState>(
        string stateName,
        int applicationSchemaVersion,
        CancellationToken cancellationToken = default)
    {
        return SchemaNotSupported<SearchableStorageIndexSchemaStatus>();
    }

    /// <summary>
    /// Starts or resumes a quiesced, page-limited index rebuild and returns after it is active.
    /// </summary>
    /// <remarks>
    /// The state type must be registered with <c>AddSearchableStorageState&lt;TState&gt;</c> on
    /// every participating silo. Cancellation stops only this client loop; an in-flight Orleans
    /// turn may still commit, and a later call resumes the durable cursor. Each scan request covers
    /// at most 64 catalog records, but the complete helper loop and retained partition compaction
    /// are not hard work, memory, or wall-clock bounds. Searchable writes and queries for the state
    /// fail closed until completion.
    /// </remarks>
    /// <typeparam name="TState">The registered Orleans persistent-state type.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="cancellationToken">Cancels the client loop between rebuild/protocol turns.</param>
    /// <returns>The active schema status.</returns>
    Task<SearchableStorageIndexSchemaStatus> RebuildIndexSchemaAsync<TState>(
        string stateName,
        CancellationToken cancellationToken = default)
    {
        return RebuildIndexSchemaAsync<TState>(
            stateName,
            applicationSchemaVersion: 1,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Starts or resumes a rebuild for an explicitly versioned index schema.
    /// </summary>
    /// <remarks>
    /// Follow the managed-schema runbook before calling: quiesce the required traffic and movement,
    /// deploy the same state type and version registration to every participant, and run only one
    /// schema rebuild at a time for the provider. Cancellation stops only this client loop; an
    /// in-flight Orleans turn may still commit, and a later call resumes the same durable rebuild.
    /// Each scan request covers at most 64 catalog records, but the complete helper loop and retained
    /// partition compaction are not hard work, memory, or wall-clock bounds.
    /// </remarks>
    /// <typeparam name="TState">The registered Orleans persistent-state type.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="applicationSchemaVersion">
    /// The positive application-owned version used during state registration.
    /// </param>
    /// <param name="cancellationToken">Cancels the client loop between rebuild/protocol turns.</param>
    /// <returns>The active schema status.</returns>
    Task<SearchableStorageIndexSchemaStatus> RebuildIndexSchemaAsync<TState>(
        string stateName,
        int applicationSchemaVersion,
        CancellationToken cancellationToken = default)
    {
        return SchemaNotSupported<SearchableStorageIndexSchemaStatus>();
    }

    /// <summary>
    /// Enables the live-movement protocol after searchable-storage traffic has been quiesced and
    /// every participating process has been restarted on a movement-capable binary.
    /// </summary>
    /// <remarks>
    /// The operation durably upgrades and fences one current owner at a time before publishing the
    /// movement-capable routing epoch. Cancellation stops this client's loop; a later call resumes
    /// the persisted enablement intent.
    /// </remarks>
    /// <param name="cancellationToken">Cancels this caller's wait between bounded steps.</param>
    /// <returns>The enabled routing layout.</returns>
    Task<SearchableStorageLayout> EnableMovementAsync(
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageLayout>();
    }

    /// <summary>
    /// Persists the provider's sole active move intent for one virtual slot.
    /// </summary>
    /// <param name="slot">The virtual slot to move.</param>
    /// <param name="targetPartitionIndex">The physical partition which will own the slot.</param>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>The planned move.</returns>
    Task<SearchableStorageSlotMoveProgress> PlanMoveAsync(
        int slot,
        int targetPartitionIndex,
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageSlotMoveProgress>();
    }

    /// <summary>
    /// Gets the provider's active move, or <see langword="null"/> when no move is in progress.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>The active move, or <see langword="null"/>.</returns>
    Task<SearchableStorageSlotMoveProgress?> GetMoveAsync(
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageSlotMoveProgress?>();
    }

    /// <summary>
    /// Advances one move by one protocol transition or one bounded transfer-page payload.
    /// </summary>
    /// <remarks>
    /// A participant can perform retained whole-partition compaction before committing a protocol
    /// mutation, so one call is not a strict work or wall-clock-time bound.
    /// </remarks>
    /// <param name="moveId">The stable identifier returned by <see cref="PlanMoveAsync"/>.</param>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>The progress after the protocol turn.</returns>
    Task<SearchableStorageSlotMoveProgress> AdvanceMoveAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageSlotMoveProgress>();
    }

    /// <summary>
    /// Executes or resumes one planned move until it completes.
    /// </summary>
    /// <remarks>
    /// This method is a client-side loop over <see cref="AdvanceMoveAsync"/>. Cancellation never
    /// rolls ownership back and leaves the durable move available for a later resume.
    /// </remarks>
    /// <param name="moveId">The stable identifier returned by <see cref="PlanMoveAsync"/>.</param>
    /// <param name="cancellationToken">Cancels the client-side loop between protocol turns.</param>
    /// <returns>The completed move.</returns>
    Task<SearchableStorageSlotMoveProgress> ExecuteMoveAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageSlotMoveProgress>();
    }

    /// <summary>
    /// Requests and executes a bounded rollback for a move which has not committed ownership.
    /// </summary>
    /// <remarks>
    /// A move cannot be aborted after its ownership commit. Cancellation leaves an aborting move
    /// resumable by calling this method again with the same identifier.
    /// </remarks>
    /// <param name="moveId">The stable identifier returned by <see cref="PlanMoveAsync"/>.</param>
    /// <param name="cancellationToken">Cancels the client-side rollback loop between protocol turns.</param>
    /// <returns>The aborted move.</returns>
    Task<SearchableStorageSlotMoveProgress> AbortMoveAsync(
        Guid moveId,
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageSlotMoveProgress>();
    }

    /// <summary>
    /// Computes a deterministic, minimum-movement rebalance summary without persisting a bulk plan.
    /// </summary>
    /// <param name="targetPartitionCount">The desired contiguous owner count.</param>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>The current summary and at most one next slot move.</returns>
    Task<SearchableStorageRebalancePlan> PlanRebalanceAsync(
        int targetPartitionCount,
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageRebalancePlan>();
    }

    /// <summary>
    /// Executes or resumes a deterministic rebalance one explicit slot move at a time.
    /// </summary>
    /// <remarks>
    /// No unbounded plan is persisted. After each ownership commit, the client reloads the layout,
    /// recomputes the minimum-movement quotas, and plans only the next required slot. Cancellation
    /// leaves any active move resumable.
    /// </remarks>
    /// <param name="targetPartitionCount">The desired contiguous owner count.</param>
    /// <param name="cancellationToken">Cancels the client-side loop between protocol turns.</param>
    /// <returns>A converged summary with no remaining move.</returns>
    Task<SearchableStorageRebalancePlan> ExecuteRebalanceAsync(
        int targetPartitionCount,
        CancellationToken cancellationToken = default)
    {
        return MovementNotSupported<SearchableStorageRebalancePlan>();
    }

    private static Task<T> MovementNotSupported<T>()
    {
        return Task.FromException<T>(new NotSupportedException(
            "This ISearchableStorageAdminClient implementation does not support live virtual-slot movement. "
            + "Use the keyed SearchableStorageAdminClient registered by AddSearchableGrainStorage."));
    }

    private static Task<T> SchemaNotSupported<T>()
    {
        return Task.FromException<T>(new NotSupportedException(
            "This ISearchableStorageAdminClient implementation does not support managed index "
            + "schemas. Use the keyed SearchableStorageAdminClient registered by "
            + "AddSearchableGrainStorage."));
    }
}

/// <summary>Identifies the durable lifecycle phase of one managed index schema.</summary>
public enum SearchableStorageIndexSchemaState
{
    /// <summary>No managed schema has been activated.</summary>
    Uninitialized = 0,

    /// <summary>A quiesced resumable rebuild is in progress.</summary>
    Rebuilding = 1,

    /// <summary>The registered schema fingerprint is active.</summary>
    Active = 2,
}

/// <summary>Identifies the durable operation within an active index-schema rebuild.</summary>
public enum SearchableStorageIndexSchemaRebuildPhase
{
    /// <summary>Current partition owners are durably enabling the managed-schema protocol.</summary>
    EnablingOwners = 0,

    /// <summary>Records are being scanned and reindexed in pages of at most 64 records.</summary>
    ScanningRecords = 1,

    /// <summary>The completed target fingerprint is being activated.</summary>
    ActivatingGeneration = 2,
}

/// <summary>Reports durable progress without exposing partition record cursors.</summary>
public sealed class SearchableStorageIndexSchemaStatus
{
    /// <summary>Gets the Orleans persistent-state name.</summary>
    public required string StateName { get; init; }

    /// <summary>Gets the durable lifecycle phase.</summary>
    public required SearchableStorageIndexSchemaState State { get; init; }

    /// <summary>Gets the stable rebuild identifier while rebuilding.</summary>
    public Guid? RebuildId { get; init; }

    /// <summary>
    /// Gets the current durable rebuild operation, or <see langword="null"/> outside a rebuild.
    /// </summary>
    public SearchableStorageIndexSchemaRebuildPhase? RebuildPhase { get; init; }

    /// <summary>
    /// Gets the number of owners in the rebuild's durable layout checkpoint, or
    /// <see langword="null"/> outside a rebuild.
    /// </summary>
    public int? TotalOwnerCount { get; init; }

    /// <summary>
    /// Gets the number of owners that durably enabled the schema protocol, or
    /// <see langword="null"/> outside a rebuild.
    /// </summary>
    public int? SchemaEnabledOwnerCount { get; init; }

    /// <summary>
    /// Gets the number of owners whose record scan is durably complete, or
    /// <see langword="null"/> outside a rebuild.
    /// </summary>
    public int? ScannedOwnerCount { get; init; }

    /// <summary>Gets the number of records covered by committed rebuild pages.</summary>
    public required long ProcessedRecordCount { get; init; }

    /// <summary>Gets the active or target schema fingerprint as uppercase SHA-256 hex.</summary>
    public string? Fingerprint { get; init; }
}

/// <summary>
/// Describes whether one provider namespace can execute live virtual-slot moves.
/// </summary>
public enum SearchableStorageMovementState
{
    /// <summary>Live movement has not been enabled.</summary>
    Disabled = 0,

    /// <summary>The quiesced participant upgrade and epoch fence are still in progress.</summary>
    Enabling = 1,

    /// <summary>The movement protocol and routed-operation fence are enabled.</summary>
    Enabled = 2,
}

/// <summary>
/// Identifies the durable phase of one virtual-slot move.
/// </summary>
public enum SearchableStorageSlotMovePhase
{
    /// <summary>The sole layout move intent has been persisted.</summary>
    Planned = 0,

    /// <summary>Source mutations are durably frozen at a version high-water mark.</summary>
    SourceFrozen = 1,

    /// <summary>The target version sequence is durably advanced to at least the source high-water mark.</summary>
    TargetVersionFenced = 2,

    /// <summary>Bounded record pages are being copied to the mutation-inactive target.</summary>
    Copying = 3,

    /// <summary>Every frozen source record has been imported.</summary>
    CopyComplete = 4,

    /// <summary>The layout assignment and routing epoch have committed atomically.</summary>
    OwnershipCommitted = 5,

    /// <summary>The old owner has durably rejected the previous routing epoch.</summary>
    SourceVisibilityFenced = 6,

    /// <summary>The new owner accepts mutations for the moved slot.</summary>
    TargetEnabled = 7,

    /// <summary>The obsolete source copy is being deleted in bounded pages.</summary>
    DeletingSource = 8,

    /// <summary>Source and target movement controls are being retired.</summary>
    Retiring = 9,

    /// <summary>A pre-commit rollback is deleting imported target records and unfreezing the source.</summary>
    Aborting = 10,

    /// <summary>The move completed and its durable intent was cleared.</summary>
    Completed = 11,

    /// <summary>The move was rolled back before ownership committed.</summary>
    Aborted = 12,
}

/// <summary>
/// Describes one immutable snapshot of a searchable-storage routing layout.
/// </summary>
public sealed class SearchableStorageLayout
{
    /// <summary>
    /// Gets the routing epoch represented by this snapshot.
    /// </summary>
    public required long Epoch { get; init; }

    /// <summary>
    /// Gets the physical partition count used to seed the zero-movement identity layout.
    /// </summary>
    public required int InitialPartitionCount { get; init; }

    /// <summary>
    /// Gets the immutable number of virtual slots in this provider namespace.
    /// </summary>
    public required int VirtualSlotCount { get; init; }

    /// <summary>
    /// Gets a per-owner summary without exposing the mutable serialized assignment array.
    /// </summary>
    public required IReadOnlyList<SearchableStoragePartitionLayout> Partitions { get; init; }

    /// <summary>
    /// Gets the persisted live-movement protocol version, or zero before movement is enabled.
    /// </summary>
    public int MovementProtocolVersion { get; init; }

    /// <summary>
    /// Gets the provider-wide managed index-schema protocol version, or zero before first
    /// adoption. Once enabled, every state using this provider must be explicitly registered.
    /// </summary>
    public int IndexSchemaProtocolVersion { get; init; }

    /// <summary>
    /// Gets the namespace's movement-enablement state.
    /// </summary>
    public SearchableStorageMovementState MovementState { get; init; }

    /// <summary>
    /// Gets the active slot move, or <see langword="null"/> when no move is in progress.
    /// </summary>
    public SearchableStorageSlotMoveProgress? ActiveMove { get; init; }
}

/// <summary>
/// Describes the virtual slots assigned to one physical partition.
/// </summary>
public sealed class SearchableStoragePartitionLayout
{
    /// <summary>
    /// Gets the physical partition index.
    /// </summary>
    public required int PartitionIndex { get; init; }

    /// <summary>
    /// Gets the number of virtual slots assigned to this partition.
    /// </summary>
    public required int SlotCount { get; init; }
}

/// <summary>
/// Reports durable progress for one virtual-slot move without exposing record-key cursors.
/// </summary>
public sealed class SearchableStorageSlotMoveProgress
{
    /// <summary>Gets the stable move identifier.</summary>
    public required Guid MoveId { get; init; }

    /// <summary>Gets the virtual slot being moved.</summary>
    public required int Slot { get; init; }

    /// <summary>Gets the owner recorded when the move was planned.</summary>
    public required int SourcePartitionIndex { get; init; }

    /// <summary>Gets the planned target owner.</summary>
    public required int TargetPartitionIndex { get; init; }

    /// <summary>Gets the routing epoch at which the move was planned.</summary>
    public required long SourceEpoch { get; init; }

    /// <summary>Gets the currently persisted routing epoch.</summary>
    public required long CurrentEpoch { get; init; }

    /// <summary>Gets the durable move phase.</summary>
    public required SearchableStorageSlotMovePhase Phase { get; init; }

    /// <summary>Gets the number of source records durably imported by the target.</summary>
    public required long ExportedRecordCount { get; init; }

    /// <summary>Gets the canonical encoded source bytes durably imported by the target.</summary>
    public required long ExportedByteCount { get; init; }

    /// <summary>Gets the number of obsolete source records durably deleted.</summary>
    public required long DeletedRecordCount { get; init; }

    /// <summary>Gets the canonical encoded delete-record bytes durably processed during source cleanup.</summary>
    public required long DeletedByteCount { get; init; }

    /// <summary>Gets whether the move can still be rolled back before ownership commits.</summary>
    public required bool CanAbort { get; init; }

    /// <summary>Gets whether the move has completed or was fully aborted.</summary>
    public required bool IsComplete { get; init; }
}

/// <summary>
/// Describes one deterministic next step from a rebalance summary.
/// </summary>
public sealed class SearchableStorageSlotMovePlan
{
    /// <summary>Gets the virtual slot to move.</summary>
    public required int Slot { get; init; }

    /// <summary>Gets the slot's current owner.</summary>
    public required int SourcePartitionIndex { get; init; }

    /// <summary>Gets the owner required by the balanced target quotas.</summary>
    public required int TargetPartitionIndex { get; init; }
}

/// <summary>
/// Summarizes a bounded deterministic rebalance without materializing or persisting every move.
/// </summary>
public sealed class SearchableStorageRebalancePlan
{
    /// <summary>Gets the routing epoch used to compute this summary.</summary>
    public required long Epoch { get; init; }

    /// <summary>Gets the desired contiguous physical partition count.</summary>
    public required int TargetPartitionCount { get; init; }

    /// <summary>Gets the minimum number of additional ownership commits required.</summary>
    public required int RequiredMoveCount { get; init; }

    /// <summary>Gets the next deterministic move, or <see langword="null"/> when balanced.</summary>
    public SearchableStorageSlotMovePlan? NextMove { get; init; }

    /// <summary>Gets the sole active move, if one is still being executed or cleaned up.</summary>
    public SearchableStorageSlotMoveProgress? ActiveMove { get; init; }
}
