using System.Collections.ObjectModel;
using System.Globalization;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Identifies the sanitized event category retained by durable ingestion state.
/// </summary>
public enum DurableEventKind : short
{
    RecordMutation = 1,
    AccountLifecycle = 2,
    RepositorySync = 3,
}

/// <summary>
/// Identifies a supported, metadata-only AT Protocol collection.
/// </summary>
public enum DurableRecordKind : short
{
    FeedPost = 1,
    FeedLike = 2,
    FeedRepost = 3,
    GraphFollow = 4,
    ActorProfile = 5,
}

/// <summary>
/// Identifies a current-record mutation.
/// </summary>
public enum DurableRecordAction : short
{
    Create = 1,
    Update = 2,
    Delete = 3,
}

/// <summary>
/// Identifies the persisted lifecycle state of an AT Protocol repository.
/// </summary>
public enum DurableAccountLifecycle : short
{
    Active = 1,
    Deactivated = 2,
    TakenDown = 3,
    Suspended = 4,
    Deleted = 5,
}

/// <summary>
/// Identifies the durable outcome of a sanitized TAP delivery.
/// </summary>
public enum DurableDeliveryOutcome : short
{
    Pending = 0,
    Applied = 1,
    SemanticDuplicate = 2,
    Quarantined = 3,
}

/// <summary>
/// Identifies whether the outbox publishes or removes an account projection.
/// </summary>
public enum ProjectionOperation : short
{
    Upsert = 1,
    Remove = 2,
}

/// <summary>
/// Contains the closed metadata envelope persisted for one accepted TAP delivery.
/// </summary>
/// <remarks>
/// The two digests are SHA-256 values represented as canonical lowercase hexadecimal. The
/// delivery digest covers the sanitized envelope, never a raw AT Protocol record or frame.
/// <see cref="ObservedAtMinuteUtc"/> is the first local observation minute preserved by the
/// independently committed delivery reservation; a redelivery cannot replace it.
/// </remarks>
public sealed record DurableEventEnvelope
{
    public DurableEventEnvelope(
        Guid sourceInstanceId,
        ulong tapDeliveryId,
        string deliveryDigest,
        string semanticDigest,
        AccountKey accountKey,
        long repositoryGeneration,
        DurableEventKind eventKind,
        long observedAtMinuteUtc,
        string? repositoryRevision = null,
        DurableRecordKind? collection = null,
        DurableRecordAction? action = null,
        string? recordKey = null,
        string? cid = null,
        AccountKey? targetAccountKey = null,
        bool isDirectReply = false,
        bool isLive = true,
        DurableAccountLifecycle? lifecycle = null)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A durable source-instance identifier is required.", nameof(sourceInstanceId));
        }

        Guard.Positive(tapDeliveryId, nameof(tapDeliveryId));
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.NonNegative(observedAtMinuteUtc, nameof(observedAtMinuteUtc));
        Guard.DefinedEnum(eventKind, nameof(eventKind));
        if (collection is { } recordKind)
        {
            Guard.DefinedEnum(recordKind, nameof(collection));
        }

        if (action is { } recordAction)
        {
            Guard.DefinedEnum(recordAction, nameof(action));
        }

        if (lifecycle is { } lifecycleValue)
        {
            Guard.DefinedEnum(lifecycleValue, nameof(lifecycle));
        }

        Guard.HexDigest(deliveryDigest, nameof(deliveryDigest));
        Guard.HexDigest(semanticDigest, nameof(semanticDigest));
        Guard.OptionalBounded(repositoryRevision, 256, nameof(repositoryRevision));
        if (repositoryRevision is not null)
        {
            Guard.RepositoryRevision(repositoryRevision, nameof(repositoryRevision));
        }

        Guard.OptionalBounded(recordKey, 512, nameof(recordKey));
        Guard.OptionalBounded(cid, 256, nameof(cid));
        if (targetAccountKey is { } target)
        {
            Guard.ValidAccountKey(target, nameof(targetAccountKey));
        }

        if (eventKind == DurableEventKind.RecordMutation)
        {
            if (collection is null || action is null || string.IsNullOrEmpty(recordKey) || string.IsNullOrEmpty(repositoryRevision))
            {
                throw new ArgumentException("A record mutation requires collection, action, record key, and repository revision.");
            }

            if (lifecycle is not null)
            {
                throw new ArgumentException("A record mutation cannot contain a lifecycle value.", nameof(lifecycle));
            }

            RecordShapeGuard.ValidateEnvelope(collection.Value, action.Value, cid, targetAccountKey, isDirectReply);
        }
        else if (collection is not null || action is not null || recordKey is not null || cid is not null || targetAccountKey is not null || isDirectReply)
        {
            throw new ArgumentException("Only record mutations may contain record metadata.");
        }

        if (eventKind == DurableEventKind.RepositorySync && string.IsNullOrEmpty(repositoryRevision))
        {
            throw new ArgumentException("A repository synchronization requires a repository revision.");
        }

        if (eventKind == DurableEventKind.AccountLifecycle && lifecycle is null)
        {
            throw new ArgumentException("An account lifecycle event requires its lifecycle value.", nameof(lifecycle));
        }

        if (eventKind != DurableEventKind.AccountLifecycle && lifecycle is not null)
        {
            throw new ArgumentException("Only an account lifecycle event may contain a lifecycle value.", nameof(lifecycle));
        }

        SourceInstanceId = sourceInstanceId;
        TapDeliveryId = tapDeliveryId;
        DeliveryDigest = deliveryDigest;
        SemanticDigest = semanticDigest;
        AccountKey = accountKey;
        RepositoryGeneration = repositoryGeneration;
        EventKind = eventKind;
        ObservedAtMinuteUtc = observedAtMinuteUtc;
        RepositoryRevision = repositoryRevision;
        Collection = collection;
        Action = action;
        RecordKey = recordKey;
        Cid = cid;
        TargetAccountKey = targetAccountKey;
        IsDirectReply = isDirectReply;
        IsLive = isLive;
        Lifecycle = lifecycle;
    }

    public Guid SourceInstanceId { get; }

    public ulong TapDeliveryId { get; }

    public string DeliveryDigest { get; }

    public string SemanticDigest { get; }

    public AccountKey AccountKey { get; }

    public long RepositoryGeneration { get; }

    public DurableEventKind EventKind { get; }

    public long ObservedAtMinuteUtc { get; }

    public string? RepositoryRevision { get; }

    public DurableRecordKind? Collection { get; }

    public DurableRecordAction? Action { get; }

    public string? RecordKey { get; }

    public string? Cid { get; }

    public AccountKey? TargetAccountKey { get; }

    public bool IsDirectReply { get; }

    public bool IsLive { get; }

    public DurableAccountLifecycle? Lifecycle { get; }
}

/// <summary>
/// Defines an optimistic account-state replacement within one durable transition.
/// </summary>
public sealed record AccountStateMutation
{
    public AccountStateMutation(
        AccountKey accountKey,
        long expectedVersion,
        long nextVersion,
        DurableAccountLifecycle lifecycle,
        long repositoryGeneration,
        string? completedSyncRevision,
        bool synchronizationComplete,
        long lastActivityMinuteUtc,
        long currentPostCount,
        long currentFollowingCount,
        long currentFollowerCount,
        string? lastAppliedRevision = null)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.NonNegative(expectedVersion, nameof(expectedVersion));
        Guard.DefinedEnum(lifecycle, nameof(lifecycle));
        if (nextVersion != checked(expectedVersion + 1))
        {
            throw new ArgumentException("The next account version must be exactly one greater than the expected version.", nameof(nextVersion));
        }

        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.OptionalBounded(completedSyncRevision, 256, nameof(completedSyncRevision));
        if (completedSyncRevision is not null)
        {
            Guard.RepositoryRevision(completedSyncRevision, nameof(completedSyncRevision));
        }

        if (lastAppliedRevision is not null)
        {
            Guard.RepositoryRevision(lastAppliedRevision, nameof(lastAppliedRevision));
        }

        Guard.NonNegative(lastActivityMinuteUtc, nameof(lastActivityMinuteUtc));
        Guard.NonNegative(currentPostCount, nameof(currentPostCount));
        Guard.NonNegative(currentFollowingCount, nameof(currentFollowingCount));
        Guard.NonNegative(currentFollowerCount, nameof(currentFollowerCount));
        if (synchronizationComplete && string.IsNullOrEmpty(completedSyncRevision))
        {
            throw new ArgumentException("A complete synchronization requires its repository revision.", nameof(completedSyncRevision));
        }

        if (synchronizationComplete && string.IsNullOrEmpty(lastAppliedRevision))
        {
            lastAppliedRevision = completedSyncRevision;
        }

        if (completedSyncRevision is not null
            && lastAppliedRevision is not null
            && string.CompareOrdinal(lastAppliedRevision, completedSyncRevision) < 0)
        {
            throw new ArgumentException(
                "The last applied repository revision cannot precede the completed synchronization revision.",
                nameof(lastAppliedRevision));
        }

        AccountKey = accountKey;
        ExpectedVersion = expectedVersion;
        NextVersion = nextVersion;
        Lifecycle = lifecycle;
        RepositoryGeneration = repositoryGeneration;
        CompletedSyncRevision = completedSyncRevision;
        SynchronizationComplete = synchronizationComplete;
        LastActivityMinuteUtc = lastActivityMinuteUtc;
        CurrentPostCount = currentPostCount;
        CurrentFollowingCount = currentFollowingCount;
        CurrentFollowerCount = currentFollowerCount;
        LastAppliedRevision = lastAppliedRevision;
    }

    public AccountKey AccountKey { get; }

    public long ExpectedVersion { get; }

    public long NextVersion { get; }

    public DurableAccountLifecycle Lifecycle { get; }

    public long RepositoryGeneration { get; }

    public string? CompletedSyncRevision { get; }

    public bool SynchronizationComplete { get; }

    public long LastActivityMinuteUtc { get; }

    public long CurrentPostCount { get; }

    public long CurrentFollowingCount { get; }

    public long CurrentFollowerCount { get; }

    /// <summary>
    /// Gets the repository-wide high-water revision applied to this generation.
    /// Historical replay records do not advance this value; repository synchronization establishes
    /// the initial barrier and later live record transitions advance it monotonically. Multiple
    /// records from one repository commit may legitimately share the same revision.
    /// </summary>
    public string? LastAppliedRevision { get; }
}

/// <summary>
/// Replaces the durable state or delete tombstone of one AT Protocol record.
/// </summary>
public sealed record RecordStateMutation
{
    public RecordStateMutation(
        AccountKey accountKey,
        long repositoryGeneration,
        DurableRecordKind collection,
        string recordKey,
        string latestRevision,
        bool isDeleted,
        string? cid = null,
        AccountKey? targetAccountKey = null,
        bool isDirectReply = false)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.DefinedEnum(collection, nameof(collection));
        Guard.RequiredBounded(recordKey, 512, nameof(recordKey));
        Guard.RepositoryRevision(latestRevision, nameof(latestRevision));
        Guard.OptionalBounded(cid, 256, nameof(cid));
        if (targetAccountKey is { } target)
        {
            Guard.ValidAccountKey(target, nameof(targetAccountKey));
        }

        RecordShapeGuard.ValidateStored(collection, isDeleted, cid, targetAccountKey, isDirectReply);
        AccountKey = accountKey;
        RepositoryGeneration = repositoryGeneration;
        Collection = collection;
        RecordKey = recordKey;
        LatestRevision = latestRevision;
        IsDeleted = isDeleted;
        Cid = cid;
        TargetAccountKey = targetAccountKey;
        IsDirectReply = isDirectReply;
    }

    public AccountKey AccountKey { get; }

    public long RepositoryGeneration { get; }

    public DurableRecordKind Collection { get; }

    public string RecordKey { get; }

    public string LatestRevision { get; }

    public bool IsDeleted { get; }

    public string? Cid { get; }

    public AccountKey? TargetAccountKey { get; }

    public bool IsDirectReply { get; }
}

/// <summary>
/// Replaces the number of current follow records for one source-to-target pair.
/// </summary>
public sealed record FollowPairMutation
{
    public FollowPairMutation(AccountKey sourceAccountKey, AccountKey targetAccountKey, int multiplicity)
    {
        Guard.ValidAccountKey(sourceAccountKey, nameof(sourceAccountKey));
        Guard.ValidAccountKey(targetAccountKey, nameof(targetAccountKey));
        Guard.NonNegative(multiplicity, nameof(multiplicity));
        SourceAccountKey = sourceAccountKey;
        TargetAccountKey = targetAccountKey;
        Multiplicity = multiplicity;
    }

    public AccountKey SourceAccountKey { get; }

    public AccountKey TargetAccountKey { get; }

    public int Multiplicity { get; }
}

/// <summary>
/// Adds exact live-event counts to one UTC-minute bucket.
/// </summary>
public sealed record ActivityMinuteDelta
{
    public ActivityMinuteDelta(
        AccountKey accountKey,
        long minuteUtc,
        long repositoryGeneration = 0,
        long recordCreates = 0,
        long recordUpdates = 0,
        long recordDeletes = 0,
        long postCreates = 0,
        long receivedEngagementCreates = 0)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.NonNegative(minuteUtc, nameof(minuteUtc));
        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.NonNegative(recordCreates, nameof(recordCreates));
        Guard.NonNegative(recordUpdates, nameof(recordUpdates));
        Guard.NonNegative(recordDeletes, nameof(recordDeletes));
        Guard.NonNegative(postCreates, nameof(postCreates));
        Guard.NonNegative(receivedEngagementCreates, nameof(receivedEngagementCreates));
        if ((recordCreates | recordUpdates | recordDeletes | postCreates | receivedEngagementCreates) == 0)
        {
            throw new ArgumentException("An activity bucket delta must increment at least one counter.");
        }

        if (postCreates > recordCreates)
        {
            throw new ArgumentException("Post creates must be a subset of record creates.", nameof(postCreates));
        }

        AccountKey = accountKey;
        MinuteUtc = minuteUtc;
        RepositoryGeneration = repositoryGeneration;
        RecordCreates = recordCreates;
        RecordUpdates = recordUpdates;
        RecordDeletes = recordDeletes;
        PostCreates = postCreates;
        ReceivedEngagementCreates = receivedEngagementCreates;
    }

    public AccountKey AccountKey { get; }

    public long MinuteUtc { get; }

    public long RepositoryGeneration { get; }

    public long RecordCreates { get; }

    public long RecordUpdates { get; }

    public long RecordDeletes { get; }

    public long PostCreates { get; }

    public long ReceivedEngagementCreates { get; }
}

/// <summary>
/// Contains the 17-field desired projection snapshot. Complete snapshots are carried by the ordered outbox.
/// </summary>
public sealed record ProjectionSnapshot
{
    public ProjectionSnapshot(
        AccountKey accountKey,
        long version,
        ProjectionOperation operation,
        bool isComplete,
        long projectionCutMinuteUtc,
        long? nextRecalculationMinuteUtc,
        long lastActivityMinuteUtc,
        long createdRecordCount1Day,
        long createdRecordCount7Days,
        long createdRecordCount30Days,
        long updatedRecordCount1Day,
        long updatedRecordCount7Days,
        long updatedRecordCount30Days,
        long deletedRecordCount1Day,
        long deletedRecordCount7Days,
        long deletedRecordCount30Days,
        long currentPostCount,
        long currentFollowingCount,
        long currentFollowerCount,
        long postCreates1Day,
        long postCreates7Days,
        long postCreates30Days,
        long receivedEngagementCreates30Days)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.DefinedEnum(operation, nameof(operation));
        if (version <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(version), version, "A projection version must be positive.");
        }

        Guard.NonNegative(projectionCutMinuteUtc, nameof(projectionCutMinuteUtc));
        if (nextRecalculationMinuteUtc is { } nextDue)
        {
            Guard.NonNegative(nextDue, nameof(nextRecalculationMinuteUtc));
            if (nextDue <= projectionCutMinuteUtc)
            {
                throw new ArgumentException("The next recalculation minute must follow the projection cut.", nameof(nextRecalculationMinuteUtc));
            }
        }

        var values = new[]
        {
            lastActivityMinuteUtc,
            createdRecordCount1Day,
            createdRecordCount7Days,
            createdRecordCount30Days,
            updatedRecordCount1Day,
            updatedRecordCount7Days,
            updatedRecordCount30Days,
            deletedRecordCount1Day,
            deletedRecordCount7Days,
            deletedRecordCount30Days,
            currentPostCount,
            currentFollowingCount,
            currentFollowerCount,
            postCreates1Day,
            postCreates7Days,
            postCreates30Days,
            receivedEngagementCreates30Days,
        };
        if (values.Any(static value => value < 0))
        {
            throw new ArgumentOutOfRangeException(nameof(lastActivityMinuteUtc), "Projection values cannot be negative.");
        }

        if (postCreates1Day > createdRecordCount1Day
            || postCreates7Days > createdRecordCount7Days
            || postCreates30Days > createdRecordCount30Days)
        {
            throw new ArgumentException("Post creates must be a subset of record creates in every window.");
        }

        ValidateRollingWindow(createdRecordCount1Day, createdRecordCount7Days, createdRecordCount30Days, nameof(createdRecordCount1Day));
        ValidateRollingWindow(updatedRecordCount1Day, updatedRecordCount7Days, updatedRecordCount30Days, nameof(updatedRecordCount1Day));
        ValidateRollingWindow(deletedRecordCount1Day, deletedRecordCount7Days, deletedRecordCount30Days, nameof(deletedRecordCount1Day));
        ValidateRollingWindow(postCreates1Day, postCreates7Days, postCreates30Days, nameof(postCreates1Day));
        if (operation == ProjectionOperation.Remove && !isComplete)
        {
            throw new ArgumentException("A projection removal must be complete.", nameof(isComplete));
        }

        AccountKey = accountKey;
        Version = version;
        Operation = operation;
        IsComplete = isComplete;
        ProjectionCutMinuteUtc = projectionCutMinuteUtc;
        NextRecalculationMinuteUtc = nextRecalculationMinuteUtc;
        LastActivityMinuteUtc = lastActivityMinuteUtc;
        CreatedRecordCount1Day = createdRecordCount1Day;
        CreatedRecordCount7Days = createdRecordCount7Days;
        CreatedRecordCount30Days = createdRecordCount30Days;
        UpdatedRecordCount1Day = updatedRecordCount1Day;
        UpdatedRecordCount7Days = updatedRecordCount7Days;
        UpdatedRecordCount30Days = updatedRecordCount30Days;
        DeletedRecordCount1Day = deletedRecordCount1Day;
        DeletedRecordCount7Days = deletedRecordCount7Days;
        DeletedRecordCount30Days = deletedRecordCount30Days;
        CurrentPostCount = currentPostCount;
        CurrentFollowingCount = currentFollowingCount;
        CurrentFollowerCount = currentFollowerCount;
        PostCreates1Day = postCreates1Day;
        PostCreates7Days = postCreates7Days;
        PostCreates30Days = postCreates30Days;
        ReceivedEngagementCreates30Days = receivedEngagementCreates30Days;
    }

    public AccountKey AccountKey { get; }

    public long Version { get; }

    public ProjectionOperation Operation { get; }

    public bool IsDeleted => Operation == ProjectionOperation.Remove;

    public bool IsComplete { get; }

    public long ProjectionCutMinuteUtc { get; }

    public long? NextRecalculationMinuteUtc { get; }

    public long LastActivityMinuteUtc { get; }

    public long CreatedRecordCount1Day { get; }

    public long CreatedRecordCount7Days { get; }

    public long CreatedRecordCount30Days { get; }

    public long UpdatedRecordCount1Day { get; }

    public long UpdatedRecordCount7Days { get; }

    public long UpdatedRecordCount30Days { get; }

    public long DeletedRecordCount1Day { get; }

    public long DeletedRecordCount7Days { get; }

    public long DeletedRecordCount30Days { get; }

    public long CurrentPostCount { get; }

    public long CurrentFollowingCount { get; }

    public long CurrentFollowerCount { get; }

    public long PostCreates1Day { get; }

    public long PostCreates7Days { get; }

    public long PostCreates30Days { get; }

    public long ReceivedEngagementCreates30Days { get; }

    private static void ValidateRollingWindow(long oneDay, long sevenDays, long thirtyDays, string parameterName)
    {
        if (oneDay > sevenDays || sevenDays > thirtyDays)
        {
            throw new ArgumentException("Trailing counts must be monotonic from one to seven to thirty days.", parameterName);
        }
    }
}

/// <summary>
/// Identifies the externally visible steps in the required publication order for one leased projection.
/// </summary>
public enum ProjectionDispatchAction
{
    PrepareHydration,
    UpsertSearchableIndex,
    RemoveSearchableIndex,
    Finalize,
}

/// <summary>
/// Defines the required operation order around the non-transactional index-only writer boundary.
/// This sequence is not a fencing protocol and does not make arbitrary crash retries safe.
/// </summary>
public static class ProjectionDispatchProtocol
{
    private static readonly ReadOnlyCollection<ProjectionDispatchAction> UpsertActions = new(
    [
        ProjectionDispatchAction.PrepareHydration,
        ProjectionDispatchAction.UpsertSearchableIndex,
        ProjectionDispatchAction.Finalize,
    ]);

    private static readonly ReadOnlyCollection<ProjectionDispatchAction> RemoveActions = new(
    [
        ProjectionDispatchAction.RemoveSearchableIndex,
        ProjectionDispatchAction.Finalize,
    ]);

    /// <summary>
    /// Gets the required operation sequence for the specified projection operation.
    /// </summary>
    public static IReadOnlyList<ProjectionDispatchAction> GetActions(ProjectionOperation operation)
    {
        Guard.DefinedEnum(operation, nameof(operation));
        return operation == ProjectionOperation.Upsert ? UpsertActions : RemoveActions;
    }
}

/// <summary>
/// Contains a fully planned, typed reducer transition committed as one PostgreSQL transaction.
/// </summary>
/// <remarks>
/// The caller must derive this transition from durable state protected by the same optimistic
/// account versions. A successful result is the only result that permits acknowledgement of the
/// source delivery. This nucleus deliberately does not claim to be the transition planner.
/// </remarks>
public sealed record DurableIngestionCommit
{
    public DurableIngestionCommit(
        DurableEventEnvelope envelope,
        IEnumerable<AccountStateMutation> accountStates,
        IEnumerable<RecordStateMutation>? records = null,
        IEnumerable<FollowPairMutation>? followPairs = null,
        IEnumerable<ActivityMinuteDelta>? activity = null,
        IEnumerable<ProjectionSnapshot>? projections = null,
        IEnumerable<ReconciliationDependencyMutation>? reconciliationDependencies = null)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(accountStates);
        Envelope = envelope;
        AccountStates = FreezeDistinct(accountStates, static value => value.AccountKey, nameof(accountStates));
        Records = FreezeDistinct(records ?? [], static value => (value.AccountKey, value.RepositoryGeneration, value.Collection, value.RecordKey), nameof(records));
        FollowPairs = FreezeDistinct(followPairs ?? [], static value => (value.SourceAccountKey, value.TargetAccountKey), nameof(followPairs));
        Activity = FreezeDistinct(activity ?? [], static value => (value.AccountKey, value.RepositoryGeneration, value.MinuteUtc), nameof(activity));
        Projections = FreezeDistinct(projections ?? [], static value => (value.AccountKey, value.Version), nameof(projections));
        ReconciliationDependencies = FreezeDistinct(
            reconciliationDependencies ?? [],
            static value => (value.OwnerAccountKey, value.OwnerRepositoryGeneration, value.AffectedAccountKey),
            nameof(reconciliationDependencies));

        if (AccountStates.Count == 0)
        {
            throw new ArgumentException("A reducer transition must replace at least one account state.", nameof(accountStates));
        }

        var states = AccountStates.ToDictionary(static state => state.AccountKey);
        var versions = states.ToDictionary(static pair => pair.Key, static pair => pair.Value.NextVersion);
        if (!states.TryGetValue(Envelope.AccountKey, out var eventAccountState))
        {
            throw new ArgumentException("The event account must have an optimistic state replacement in the same transition.", nameof(accountStates));
        }

        if (eventAccountState.RepositoryGeneration != Envelope.RepositoryGeneration)
        {
            throw new ArgumentException("The event repository generation must match its account-state replacement.", nameof(envelope));
        }

        var generations = AccountStates.ToDictionary(static state => state.AccountKey, static state => state.RepositoryGeneration);
        foreach (var record in Records)
        {
            if (!generations.TryGetValue(record.AccountKey, out var generation) || generation != record.RepositoryGeneration)
            {
                throw new ArgumentException("Every record replacement must match an account generation in the same transition.", nameof(records));
            }
        }

        foreach (var pair in FollowPairs)
        {
            if (!versions.ContainsKey(pair.SourceAccountKey))
            {
                throw new ArgumentException("Every follow-pair replacement must include its source account state.", nameof(followPairs));
            }
        }

        foreach (var bucket in Activity)
        {
            if (!generations.TryGetValue(bucket.AccountKey, out var generation) || generation != bucket.RepositoryGeneration)
            {
                throw new ArgumentException("Every activity increment must match an account generation in the same transition.", nameof(activity));
            }
        }

        foreach (var projection in Projections)
        {
            if (!versions.TryGetValue(projection.AccountKey, out var version) || version != projection.Version)
            {
                throw new ArgumentException("Every projection must match an account-state version in the same transition.", nameof(projections));
            }
        }

        foreach (var dependency in ReconciliationDependencies)
        {
            if (!generations.TryGetValue(dependency.OwnerAccountKey, out var generation)
                || generation != dependency.OwnerRepositoryGeneration)
            {
                throw new ArgumentException(
                    "Every reconciliation dependency must match its owner account generation in the same transition.",
                    nameof(reconciliationDependencies));
            }
        }

        ValidateEventTransition(eventAccountState);
    }

    public DurableEventEnvelope Envelope { get; }

    public IReadOnlyList<AccountStateMutation> AccountStates { get; }

    public IReadOnlyList<RecordStateMutation> Records { get; }

    public IReadOnlyList<FollowPairMutation> FollowPairs { get; }

    public IReadOnlyList<ActivityMinuteDelta> Activity { get; }

    public IReadOnlyList<ProjectionSnapshot> Projections { get; }

    public IReadOnlyList<ReconciliationDependencyMutation> ReconciliationDependencies { get; }

    private void ValidateEventTransition(AccountStateMutation eventAccountState)
    {
        switch (Envelope.EventKind)
        {
            case DurableEventKind.RecordMutation:
                ValidateRecordEventTransition();
                break;
            case DurableEventKind.AccountLifecycle:
                if (Envelope.Lifecycle != eventAccountState.Lifecycle)
                {
                    throw new ArgumentException("A lifecycle event must match the persisted lifecycle state.");
                }

                break;
            case DurableEventKind.RepositorySync:
                if (!eventAccountState.SynchronizationComplete
                    || !string.Equals(eventAccountState.CompletedSyncRevision, Envelope.RepositoryRevision, StringComparison.Ordinal))
                {
                    throw new ArgumentException("A repository-sync event must complete the same durable repository revision.");
                }

                break;
            default:
                throw new InvalidOperationException("The event kind was validated by the durable envelope.");
        }
    }

    private void ValidateRecordEventTransition()
    {
        if (Records.Count != 1)
        {
            throw new ArgumentException("A record event transition must replace exactly its one canonical record state.");
        }

        var record = Records[0];
        var expectedDeleted = Envelope.Action == DurableRecordAction.Delete;
        if (record.AccountKey != Envelope.AccountKey
            || record.RepositoryGeneration != Envelope.RepositoryGeneration
            || record.Collection != Envelope.Collection
            || !string.Equals(record.RecordKey, Envelope.RecordKey, StringComparison.Ordinal)
            || !string.Equals(record.LatestRevision, Envelope.RepositoryRevision, StringComparison.Ordinal)
            || record.IsDeleted != expectedDeleted)
        {
            throw new ArgumentException("The canonical record replacement must exactly match its source event identity and revision.");
        }

        if (!expectedDeleted
            && (!string.Equals(record.Cid, Envelope.Cid, StringComparison.Ordinal)
                || record.TargetAccountKey != Envelope.TargetAccountKey
                || record.IsDirectReply != Envelope.IsDirectReply))
        {
            throw new ArgumentException("The current record metadata must exactly match its sanitized source event.");
        }

        if (record.Collection != DurableRecordKind.GraphFollow)
        {
            return;
        }

        if (!expectedDeleted)
        {
            var target = record.TargetAccountKey
                ?? throw new InvalidOperationException("A current follow record must have a target.");
            if (!FollowPairs.Any(pair => pair.SourceAccountKey == record.AccountKey
                && pair.TargetAccountKey == target
                && pair.Multiplicity > 0))
            {
                throw new ArgumentException("A current follow record requires its positive follow-pair replacement.");
            }
        }
        else if (!FollowPairs.Any(pair => pair.SourceAccountKey == record.AccountKey))
        {
            throw new ArgumentException("A deleted follow record requires an explicit source follow-pair replacement derived by the planner.");
        }
    }

    private static ReadOnlyCollection<T> FreezeDistinct<T, TKey>(
        IEnumerable<T> source,
        Func<T, TKey> keySelector,
        string parameterName)
        where TKey : notnull
    {
        var values = source.ToArray();
        if (values.Select(keySelector).Distinct().Count() != values.Length)
        {
            throw new ArgumentException("A transition cannot contain duplicate state targets.", parameterName);
        }

        return new ReadOnlyCollection<T>(values);
    }
}

/// <summary>
/// Identifies one closed, source-independent diagnostic retained for a rejected delivery.
/// </summary>
public enum DurableQuarantineReason
{
    EventTooLarge,
    MalformedJson,
    InvalidRoot,
    MissingProperty,
    UnexpectedProperty,
    InvalidValue,
    UnsupportedEventType,
    UnsupportedCollection,
    MissingPriorRecord,
    ConflictingRecordRevision,
    InactiveAccountMutation,
    ReconciliationIncomplete,
    ReconciliationRevisionConflict,
    ReservationMismatch,
    NotRecordMutation,
    AccountNotActive,
    LiveBeforeRepositorySync,
    HistoricalAfterRepositorySync,
    CounterOverflow,
    UnsupportedTransitionKind,
    InactiveRepositorySync,
    AccountNotAdmitted,
}

/// <summary>
/// Contains bounded metadata about a rejected TAP delivery. No raw frame is retained, and callers
/// cannot supply diagnostic text derived from the source event.
/// </summary>
public sealed record DurableQuarantine
{
    public DurableQuarantine(
        Guid sourceInstanceId,
        ulong tapDeliveryId,
        string deliveryDigest,
        DurableQuarantineReason reason,
        long observedAtMinuteUtc,
        string? semanticDigest = null,
        AccountKey? accountKey = null)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A durable source-instance identifier is required.", nameof(sourceInstanceId));
        }

        Guard.Positive(tapDeliveryId, nameof(tapDeliveryId));
        Guard.HexDigest(deliveryDigest, nameof(deliveryDigest));
        if (semanticDigest is not null)
        {
            Guard.HexDigest(semanticDigest, nameof(semanticDigest));
        }

        if (accountKey is { } account)
        {
            Guard.ValidAccountKey(account, nameof(accountKey));
        }

        Guard.DefinedEnum(reason, nameof(reason));
        var diagnostic = DurableQuarantineCatalog.Get(reason);
        Guard.NonNegative(observedAtMinuteUtc, nameof(observedAtMinuteUtc));
        SourceInstanceId = sourceInstanceId;
        TapDeliveryId = tapDeliveryId;
        DeliveryDigest = deliveryDigest;
        SemanticDigest = semanticDigest;
        AccountKey = accountKey;
        Reason = reason;
        Code = diagnostic.Code;
        Message = diagnostic.Message;
        ObservedAtMinuteUtc = observedAtMinuteUtc;
    }

    public Guid SourceInstanceId { get; }

    public ulong TapDeliveryId { get; }

    public string DeliveryDigest { get; }

    public string? SemanticDigest { get; }

    public AccountKey? AccountKey { get; }

    public DurableQuarantineReason Reason { get; }

    public string Code { get; }

    public string Message { get; }

    public long ObservedAtMinuteUtc { get; }
}

internal static class DurableQuarantineCatalog
{
    internal static (string Code, string Message) Get(DurableQuarantineReason reason)
        => reason switch
        {
            DurableQuarantineReason.EventTooLarge
                => ("event-too-large", "The sanitized TAP event exceeds the bounded durable contract."),
            DurableQuarantineReason.MalformedJson
                => ("malformed-json", "The TAP event is not valid JSON."),
            DurableQuarantineReason.InvalidRoot
                => ("invalid-root", "The TAP event does not have the required root shape."),
            DurableQuarantineReason.MissingProperty
                => ("missing-property", "The TAP event is missing a required property."),
            DurableQuarantineReason.UnexpectedProperty
                => ("unexpected-property", "The TAP event contains a property outside the closed contract."),
            DurableQuarantineReason.InvalidValue
                => ("invalid-value", "The TAP event contains a value outside the closed contract."),
            DurableQuarantineReason.UnsupportedEventType
                => ("unsupported-event-type", "The TAP event type is not supported by this profile."),
            DurableQuarantineReason.UnsupportedCollection
                => ("unsupported-collection", "The AT Protocol collection is not supported by this profile."),
            DurableQuarantineReason.MissingPriorRecord
                => ("missing-prior-record", "The transition requires a current durable record which is absent."),
            DurableQuarantineReason.ConflictingRecordRevision
                => ("conflicting-record-revision", "Different record metadata cannot share one repository revision and record key."),
            DurableQuarantineReason.InactiveAccountMutation
                => ("inactive-account-mutation", "A record mutation cannot be applied to an inactive account."),
            DurableQuarantineReason.ReconciliationIncomplete
                => ("reconciliation-incomplete", "The repository reconciliation barrier has not completed."),
            DurableQuarantineReason.ReconciliationRevisionConflict
                => ("reconciliation-revision-conflict", "The reconciliation revision conflicts with durable repository state."),
            DurableQuarantineReason.ReservationMismatch
                => ("reservation-mismatch", "The event does not bind the exact durable delivery reservation."),
            DurableQuarantineReason.NotRecordMutation
                => ("not-record-mutation", "The ordinary record planner accepts only record mutation events."),
            DurableQuarantineReason.AccountNotActive
                => ("account-not-active", "A record mutation cannot be applied to a non-active repository generation."),
            DurableQuarantineReason.LiveBeforeRepositorySync
                => ("live-before-repo-sync", "A live mutation arrived before the repository synchronization barrier."),
            DurableQuarantineReason.HistoricalAfterRepositorySync
                => ("historical-after-repo-sync", "A historical mutation arrived after the repository synchronization barrier closed."),
            DurableQuarantineReason.CounterOverflow
                => ("counter-overflow", "Applying the event would overflow a durable counter or revisioned version."),
            DurableQuarantineReason.UnsupportedTransitionKind
                => ("unsupported-transition-kind", "The lifecycle planner cannot process this event kind."),
            DurableQuarantineReason.InactiveRepositorySync
                => ("inactive-repository-sync", "Repository synchronization cannot complete for an inactive account."),
            DurableQuarantineReason.AccountNotAdmitted
                => ("account-not-admitted", "The event account is outside the frozen qualification corpus."),
            _ => throw new ArgumentOutOfRangeException(nameof(reason), reason, "Unknown durable quarantine reason."),
        };
}

/// <summary>
/// Identifies the result of a durable ingestion attempt.
/// </summary>
public enum DurableCommitOutcome
{
    Applied,
    DeliveryDuplicate,
    SemanticDuplicate,
    Quarantined,
    OptimisticConflict,
    RevisionConflict,
    ValidatedNoOp,
}

/// <summary>
/// Carries the durable commit outcome and whether the source delivery may be acknowledged.
/// </summary>
public sealed record DurableCommitResult
{
    internal DurableCommitResult(DurableCommitOutcome outcome, bool acknowledgementAllowed)
    {
        var expectedAcknowledgement = outcome is not DurableCommitOutcome.OptimisticConflict
            and not DurableCommitOutcome.RevisionConflict;
        if (acknowledgementAllowed != expectedAcknowledgement)
        {
            throw new ArgumentException("The acknowledgement decision does not match the durable outcome.", nameof(acknowledgementAllowed));
        }

        Outcome = outcome;
        AcknowledgementAllowed = acknowledgementAllowed;
    }

    public DurableCommitOutcome Outcome { get; }

    public bool AcknowledgementAllowed { get; }
}

/// <summary>
/// Carries an exact leased projection outbox item.
/// </summary>
public sealed record ProjectionOutboxLease(
    Guid LeaseId,
    ProjectionSnapshot Projection,
    int AttemptCount);

/// <summary>
/// Carries an exact leased rolling-window recalculation. The evaluation minute comes from the
/// PostgreSQL lease statement, so a separate application-machine clock cannot expire data early.
/// </summary>
public sealed record ProjectionRecalculationLease(
    Guid LeaseId,
    AccountKey AccountKey,
    long SourceProjectionVersion,
    long DueMinuteUtc,
    long EvaluationMinuteUtc,
    int AttemptCount);

internal static class Guard
{
    internal static void DefinedEnum<TEnum>(TEnum value, string parameterName)
        where TEnum : struct, Enum
    {
        if (!Enum.IsDefined(value))
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "The enum value is not defined by this durable contract.");
        }
    }

    internal static void ValidAccountKey(AccountKey value, string parameterName)
    {
        if (!value.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", parameterName);
        }
    }

    internal static void NonNegative(long value, string parameterName)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "The value cannot be negative.");
        }
    }

    internal static void Positive(ulong value, string parameterName)
    {
        if (value == 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "The value must be positive.");
        }
    }

    internal static void NonNegative(int value, string parameterName)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "The value cannot be negative.");
        }
    }

    internal static void HexDigest(string value, string parameterName)
    {
        RequiredBounded(value, 64, parameterName);
        if (value.Length != 64 || value.Any(static character => character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f')))
        {
            throw new ArgumentException("A digest must contain 64 lowercase hexadecimal characters.", parameterName);
        }
    }

    internal static void RepositoryRevision(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        const string firstCharacters = "234567abcdefghij";
        const string remainingCharacters = "234567abcdefghijklmnopqrstuvwxyz";
        if (value.Length != 13
            || firstCharacters.IndexOf(value[0]) < 0
            || value[1..].Any(character => remainingCharacters.IndexOf(character) < 0))
        {
            throw new ArgumentException("A repository revision must be a canonical 13-character AT Protocol TID.", parameterName);
        }
    }

    internal static void RequiredBounded(string value, int maximumLength, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if (value.Length == 0 || value.Length > maximumLength)
        {
            throw new ArgumentOutOfRangeException(parameterName, value.Length, string.Create(CultureInfo.InvariantCulture, $"The value length must be between 1 and {maximumLength}."));
        }
    }

    internal static void OptionalBounded(string? value, int maximumLength, string parameterName)
    {
        if (value is not null && (value.Length == 0 || value.Length > maximumLength))
        {
            throw new ArgumentOutOfRangeException(parameterName, value.Length, string.Create(CultureInfo.InvariantCulture, $"The value length must be between 1 and {maximumLength} when supplied."));
        }
    }
}

internal static class RecordShapeGuard
{
    internal static void ValidateEnvelope(
        DurableRecordKind collection,
        DurableRecordAction action,
        string? cid,
        AccountKey? targetAccountKey,
        bool isDirectReply)
    {
        if (action == DurableRecordAction.Delete)
        {
            if (cid is not null || targetAccountKey is not null || isDirectReply)
            {
                throw new ArgumentException("A delete envelope cannot claim record-body metadata.");
            }

            return;
        }

        if (string.IsNullOrEmpty(cid))
        {
            throw new ArgumentException("A create or update envelope requires its record CID.", nameof(cid));
        }

        ValidateTarget(collection, targetAccountKey, isDirectReply);
    }

    internal static void ValidateStored(
        DurableRecordKind collection,
        bool isDeleted,
        string? cid,
        AccountKey? targetAccountKey,
        bool isDirectReply)
    {
        if (isDeleted)
        {
            if (cid is not null || targetAccountKey is not null || isDirectReply)
            {
                throw new ArgumentException("A canonical delete tombstone contains only its identity and latest revision.");
            }

            return;
        }

        if (string.IsNullOrEmpty(cid))
        {
            throw new ArgumentException("A current record requires its CID.", nameof(cid));
        }

        ValidateTarget(collection, targetAccountKey, isDirectReply);
    }

    private static void ValidateTarget(
        DurableRecordKind collection,
        AccountKey? targetAccountKey,
        bool isDirectReply)
    {
        var requiresTarget = collection is DurableRecordKind.FeedLike
            or DurableRecordKind.FeedRepost
            or DurableRecordKind.GraphFollow
            || (collection == DurableRecordKind.FeedPost && isDirectReply);
        if (requiresTarget != targetAccountKey.HasValue)
        {
            throw new ArgumentException(
                "The target account must be present exactly for targeted metadata records.",
                nameof(targetAccountKey));
        }
    }
}
