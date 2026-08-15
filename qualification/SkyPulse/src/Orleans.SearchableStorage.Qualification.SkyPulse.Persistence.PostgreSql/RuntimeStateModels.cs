using System.Collections.ObjectModel;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Requests an independently durable delivery reservation before transition planning starts.
/// </summary>
public sealed record DurableDeliveryReservationRequest
{
    public DurableDeliveryReservationRequest(
        Guid sourceInstanceId,
        ulong tapDeliveryId,
        string deliveryDigest,
        long firstObservedAtMinuteUtc)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A durable source-instance identifier is required.", nameof(sourceInstanceId));
        }

        Guard.Positive(tapDeliveryId, nameof(tapDeliveryId));
        Guard.HexDigest(deliveryDigest, nameof(deliveryDigest));
        Guard.NonNegative(firstObservedAtMinuteUtc, nameof(firstObservedAtMinuteUtc));
        SourceInstanceId = sourceInstanceId;
        TapDeliveryId = tapDeliveryId;
        DeliveryDigest = deliveryDigest;
        FirstObservedAtMinuteUtc = firstObservedAtMinuteUtc;
    }

    public Guid SourceInstanceId { get; }

    public ulong TapDeliveryId { get; }

    public string DeliveryDigest { get; }

    public long FirstObservedAtMinuteUtc { get; }
}

/// <summary>
/// Carries the exact durable identity which a later commit or quarantine must bind.
/// </summary>
public sealed record DurableDeliveryReservation
{
    public DurableDeliveryReservation(
        Guid sourceInstanceId,
        ulong tapDeliveryId,
        string deliveryDigest,
        long firstObservedAtMinuteUtc,
        DurableDeliveryOutcome outcome)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A durable source-instance identifier is required.", nameof(sourceInstanceId));
        }

        Guard.Positive(tapDeliveryId, nameof(tapDeliveryId));
        Guard.HexDigest(deliveryDigest, nameof(deliveryDigest));
        Guard.NonNegative(firstObservedAtMinuteUtc, nameof(firstObservedAtMinuteUtc));
        Guard.DefinedEnum(outcome, nameof(outcome));
        SourceInstanceId = sourceInstanceId;
        TapDeliveryId = tapDeliveryId;
        DeliveryDigest = deliveryDigest;
        FirstObservedAtMinuteUtc = firstObservedAtMinuteUtc;
        Outcome = outcome;
    }

    public Guid SourceInstanceId { get; }

    public ulong TapDeliveryId { get; }

    public string DeliveryDigest { get; }

    public long FirstObservedAtMinuteUtc { get; }

    public DurableDeliveryOutcome Outcome { get; }

    public bool IsPending => Outcome == DurableDeliveryOutcome.Pending;

    public bool AcknowledgementAllowed => !IsPending;
}

/// <summary>
/// Identifies an atomic reconciliation-dependency mutation.
/// </summary>
public enum ReconciliationDependencyAction
{
    Add = 1,
    Remove = 2,
}

/// <summary>
/// Adds or removes one account affected by a repository-generation reconciliation.
/// </summary>
public sealed record ReconciliationDependencyMutation
{
    public ReconciliationDependencyMutation(
        AccountKey ownerAccountKey,
        long ownerRepositoryGeneration,
        AccountKey affectedAccountKey,
        ReconciliationDependencyAction action)
    {
        Guard.ValidAccountKey(ownerAccountKey, nameof(ownerAccountKey));
        Guard.NonNegative(ownerRepositoryGeneration, nameof(ownerRepositoryGeneration));
        Guard.ValidAccountKey(affectedAccountKey, nameof(affectedAccountKey));
        Guard.DefinedEnum(action, nameof(action));
        OwnerAccountKey = ownerAccountKey;
        OwnerRepositoryGeneration = ownerRepositoryGeneration;
        AffectedAccountKey = affectedAccountKey;
        Action = action;
    }

    public AccountKey OwnerAccountKey { get; }

    public long OwnerRepositoryGeneration { get; }

    public AccountKey AffectedAccountKey { get; }

    public ReconciliationDependencyAction Action { get; }
}

/// <summary>
/// Identifies the current-database proof required to acknowledge a stale event as a no-op.
/// </summary>
public enum ValidatedNoOpReason
{
    RecordRevisionAlreadyObserved = 1,
    RepositoryGenerationSuperseded = 2,
    RepositorySyncRevisionAlreadyCompleted = 3,
    RepositoryRevisionAlreadyApplied = 4,
}

/// <summary>
/// Requests an acknowledgement-safe stale no-op whose proof is rechecked in the commit transaction.
/// </summary>
public sealed record DurableValidatedNoOp
{
    public DurableValidatedNoOp(DurableEventEnvelope envelope, ValidatedNoOpReason reason)
    {
        Envelope = envelope ?? throw new ArgumentNullException(nameof(envelope));
        Guard.DefinedEnum(reason, nameof(reason));

        if (reason == ValidatedNoOpReason.RecordRevisionAlreadyObserved &&
            envelope.EventKind != DurableEventKind.RecordMutation)
        {
            throw new ArgumentException("The record-revision proof requires a record mutation.", nameof(reason));
        }

        if (reason == ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted &&
            envelope.EventKind != DurableEventKind.RepositorySync)
        {
            throw new ArgumentException("The completed-sync proof requires a repository-sync event.", nameof(reason));
        }

        if (reason == ValidatedNoOpReason.RepositoryRevisionAlreadyApplied &&
            envelope.EventKind != DurableEventKind.RecordMutation)
        {
            throw new ArgumentException("The applied-repository-revision proof requires a record mutation.", nameof(reason));
        }

        Reason = reason;
    }

    public DurableEventEnvelope Envelope { get; }

    public ValidatedNoOpReason Reason { get; }
}

/// <summary>
/// Contains the exact durable account state observed by a transition planner.
/// </summary>
public sealed record AccountStateSnapshot
{
    public AccountStateSnapshot(
        AccountKey accountKey,
        long stateVersion,
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
        if (stateVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(stateVersion), stateVersion, "A persisted state version must be positive.");
        }

        Guard.DefinedEnum(lifecycle, nameof(lifecycle));
        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.OptionalBounded(completedSyncRevision, 256, nameof(completedSyncRevision));
        if (completedSyncRevision is not null)
        {
            Guard.RepositoryRevision(completedSyncRevision, nameof(completedSyncRevision));
        }

        if (synchronizationComplete && completedSyncRevision is null)
        {
            throw new ArgumentException("A complete synchronization requires its repository revision.", nameof(completedSyncRevision));
        }

        if (lastAppliedRevision is not null)
        {
            Guard.RepositoryRevision(lastAppliedRevision, nameof(lastAppliedRevision));
        }

        if (synchronizationComplete && lastAppliedRevision is null)
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

        Guard.NonNegative(lastActivityMinuteUtc, nameof(lastActivityMinuteUtc));
        Guard.NonNegative(currentPostCount, nameof(currentPostCount));
        Guard.NonNegative(currentFollowingCount, nameof(currentFollowingCount));
        Guard.NonNegative(currentFollowerCount, nameof(currentFollowerCount));
        AccountKey = accountKey;
        StateVersion = stateVersion;
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

    public long StateVersion { get; }

    public DurableAccountLifecycle Lifecycle { get; }

    public long RepositoryGeneration { get; }

    public string? CompletedSyncRevision { get; }

    public bool SynchronizationComplete { get; }

    public long LastActivityMinuteUtc { get; }

    public long CurrentPostCount { get; }

    public long CurrentFollowingCount { get; }

    public long CurrentFollowerCount { get; }

    public string? LastAppliedRevision { get; }
}

/// <summary>
/// Contains one exact current record state or canonical deletion tombstone.
/// </summary>
public sealed record RecordStateSnapshot
{
    public RecordStateSnapshot(
        AccountKey accountKey,
        long repositoryGeneration,
        DurableRecordKind collection,
        string recordKey,
        string latestRevision,
        bool isDeleted,
        string? cid,
        AccountKey? targetAccountKey,
        bool isDirectReply)
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
/// Contains the exact positive multiplicity of one current follow pair.
/// </summary>
public sealed record FollowPairSnapshot
{
    public FollowPairSnapshot(AccountKey sourceAccountKey, AccountKey targetAccountKey, int multiplicity)
    {
        Guard.ValidAccountKey(sourceAccountKey, nameof(sourceAccountKey));
        Guard.ValidAccountKey(targetAccountKey, nameof(targetAccountKey));
        if (multiplicity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(multiplicity), multiplicity, "A persisted follow multiplicity must be positive.");
        }

        SourceAccountKey = sourceAccountKey;
        TargetAccountKey = targetAccountKey;
        Multiplicity = multiplicity;
    }

    public AccountKey SourceAccountKey { get; }

    public AccountKey TargetAccountKey { get; }

    public int Multiplicity { get; }
}

/// <summary>
/// Contains exact counters from one current-generation UTC-minute activity bucket.
/// </summary>
public sealed record ActivityMinuteBucketSnapshot
{
    public ActivityMinuteBucketSnapshot(
        AccountKey accountKey,
        long repositoryGeneration,
        long minuteUtc,
        long recordCreates,
        long recordUpdates,
        long recordDeletes,
        long postCreates,
        long receivedEngagementCreates)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.NonNegative(minuteUtc, nameof(minuteUtc));
        Guard.NonNegative(recordCreates, nameof(recordCreates));
        Guard.NonNegative(recordUpdates, nameof(recordUpdates));
        Guard.NonNegative(recordDeletes, nameof(recordDeletes));
        Guard.NonNegative(postCreates, nameof(postCreates));
        Guard.NonNegative(receivedEngagementCreates, nameof(receivedEngagementCreates));
        if ((recordCreates | recordUpdates | recordDeletes | postCreates | receivedEngagementCreates) == 0)
        {
            throw new ArgumentException("A persisted activity bucket must contain at least one event.");
        }

        if (postCreates > recordCreates)
        {
            throw new ArgumentException("Post creates must be a subset of record creates.", nameof(postCreates));
        }

        AccountKey = accountKey;
        RepositoryGeneration = repositoryGeneration;
        MinuteUtc = minuteUtc;
        RecordCreates = recordCreates;
        RecordUpdates = recordUpdates;
        RecordDeletes = recordDeletes;
        PostCreates = postCreates;
        ReceivedEngagementCreates = receivedEngagementCreates;
    }

    public AccountKey AccountKey { get; }

    public long RepositoryGeneration { get; }

    public long MinuteUtc { get; }

    public long RecordCreates { get; }

    public long RecordUpdates { get; }

    public long RecordDeletes { get; }

    public long PostCreates { get; }

    public long ReceivedEngagementCreates { get; }
}

/// <summary>
/// Contains exact counts for one metric in the trailing one-, seven-, and thirty-day windows.
/// </summary>
public readonly record struct ActivityRollingCounts
{
    public ActivityRollingCounts(long oneDay, long sevenDays, long thirtyDays)
    {
        Guard.NonNegative(oneDay, nameof(oneDay));
        Guard.NonNegative(sevenDays, nameof(sevenDays));
        Guard.NonNegative(thirtyDays, nameof(thirtyDays));
        if (oneDay > sevenDays || sevenDays > thirtyDays)
        {
            throw new ArgumentException("Rolling activity counts must be monotonic as the window grows.");
        }

        OneDay = oneDay;
        SevenDays = sevenDays;
        ThirtyDays = thirtyDays;
    }

    public long OneDay { get; }

    public long SevenDays { get; }

    public long ThirtyDays { get; }
}

/// <summary>
/// Contains one fixed-size, exact activity aggregate for a version-fenced projection cut.
/// </summary>
/// <remarks>
/// PostgreSQL produces this snapshot with one aggregate statement. The result is independent of
/// the number of populated minute buckets and deliberately replaces materializing up to 43,200
/// individual rows in an ordinary record planning attempt.
/// </remarks>
public sealed record ActivityWindowAggregateSnapshot
{
    public ActivityWindowAggregateSnapshot(
        AccountKey accountKey,
        long accountStateVersion,
        long repositoryGeneration,
        long cutMinuteUtc,
        ActivityRollingCounts recordCreates,
        ActivityRollingCounts recordUpdates,
        ActivityRollingCounts recordDeletes,
        ActivityRollingCounts postCreates,
        long receivedEngagementCreatesThirtyDays,
        long? nextExpiryMinuteUtc)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        if (accountStateVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(accountStateVersion),
                accountStateVersion,
                "The account state version must be positive.");
        }

        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        Guard.NonNegative(cutMinuteUtc, nameof(cutMinuteUtc));
        Guard.NonNegative(receivedEngagementCreatesThirtyDays, nameof(receivedEngagementCreatesThirtyDays));
        if (postCreates.OneDay > recordCreates.OneDay
            || postCreates.SevenDays > recordCreates.SevenDays
            || postCreates.ThirtyDays > recordCreates.ThirtyDays)
        {
            throw new ArgumentException("Post creates must be a subset of record creates.", nameof(postCreates));
        }

        if (nextExpiryMinuteUtc is { } nextExpiry && nextExpiry <= cutMinuteUtc)
        {
            throw new ArgumentOutOfRangeException(
                nameof(nextExpiryMinuteUtc),
                nextExpiry,
                "The next expiry must be strictly later than the aggregate cut.");
        }

        var hasRollingActivity = (recordCreates.ThirtyDays
            | recordUpdates.ThirtyDays
            | recordDeletes.ThirtyDays
            | postCreates.ThirtyDays
            | receivedEngagementCreatesThirtyDays) != 0;
        if (hasRollingActivity != nextExpiryMinuteUtc.HasValue)
        {
            throw new ArgumentException(
                "An exact non-empty activity aggregate must carry its next expiry, and an empty aggregate cannot carry one.",
                nameof(nextExpiryMinuteUtc));
        }

        AccountKey = accountKey;
        AccountStateVersion = accountStateVersion;
        RepositoryGeneration = repositoryGeneration;
        CutMinuteUtc = cutMinuteUtc;
        RecordCreates = recordCreates;
        RecordUpdates = recordUpdates;
        RecordDeletes = recordDeletes;
        PostCreates = postCreates;
        ReceivedEngagementCreatesThirtyDays = receivedEngagementCreatesThirtyDays;
        NextExpiryMinuteUtc = nextExpiryMinuteUtc;
    }

    public AccountKey AccountKey { get; }

    public long AccountStateVersion { get; }

    public long RepositoryGeneration { get; }

    public long CutMinuteUtc { get; }

    public ActivityRollingCounts RecordCreates { get; }

    public ActivityRollingCounts RecordUpdates { get; }

    public ActivityRollingCounts RecordDeletes { get; }

    public ActivityRollingCounts PostCreates { get; }

    public long ReceivedEngagementCreatesThirtyDays { get; }

    public long? NextExpiryMinuteUtc { get; }
}

/// <summary>
/// Contains one bounded canonical page of current-generation activity buckets.
/// </summary>
public sealed record ActivityMinuteBucketPage
{
    internal ActivityMinuteBucketPage(
        AccountKey accountKey,
        long accountStateVersion,
        long repositoryGeneration,
        long firstMinuteUtcInclusive,
        long lastMinuteUtcInclusive,
        int pageSize,
        IEnumerable<ActivityMinuteBucketSnapshot> items,
        bool hasMore)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        if (accountStateVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(accountStateVersion), accountStateVersion, "The account state version must be positive.");
        }

        Guard.NonNegative(repositoryGeneration, nameof(repositoryGeneration));
        ValidateWindow(firstMinuteUtcInclusive, lastMinuteUtcInclusive);
        ValidatePageSize(pageSize);
        ArgumentNullException.ThrowIfNull(items);
        var values = items.ToArray();
        if (values.Length > pageSize || (hasMore && values.Length != pageSize))
        {
            throw new ArgumentException("The activity page does not match its bounded page size.", nameof(items));
        }

        long? previous = null;
        foreach (var item in values)
        {
            if (item.AccountKey != accountKey || item.RepositoryGeneration != repositoryGeneration ||
                item.MinuteUtc < firstMinuteUtcInclusive || item.MinuteUtc > lastMinuteUtcInclusive ||
                previous is { } prior && item.MinuteUtc <= prior)
            {
                throw new ArgumentException("Activity items must be uniquely ordered and bound to the requested account, generation, and window.", nameof(items));
            }

            previous = item.MinuteUtc;
        }

        AccountKey = accountKey;
        AccountStateVersion = accountStateVersion;
        RepositoryGeneration = repositoryGeneration;
        FirstMinuteUtcInclusive = firstMinuteUtcInclusive;
        LastMinuteUtcInclusive = lastMinuteUtcInclusive;
        PageSize = pageSize;
        Items = new ReadOnlyCollection<ActivityMinuteBucketSnapshot>(values);
        HasMore = hasMore;
    }

    public AccountKey AccountKey { get; }

    public long AccountStateVersion { get; }

    public long RepositoryGeneration { get; }

    public long FirstMinuteUtcInclusive { get; }

    public long LastMinuteUtcInclusive { get; }

    public int PageSize { get; }

    public IReadOnlyList<ActivityMinuteBucketSnapshot> Items { get; }

    public bool HasMore { get; }

    public long? NextAfterMinuteUtc => HasMore ? Items[^1].MinuteUtc : null;

    internal static void ValidateWindow(long firstMinuteUtcInclusive, long lastMinuteUtcInclusive)
    {
        Guard.NonNegative(firstMinuteUtcInclusive, nameof(firstMinuteUtcInclusive));
        Guard.NonNegative(lastMinuteUtcInclusive, nameof(lastMinuteUtcInclusive));
        if (firstMinuteUtcInclusive > lastMinuteUtcInclusive ||
            lastMinuteUtcInclusive - firstMinuteUtcInclusive >= PostgreSqlPlanningStore.MaximumActivityWindowMinutes)
        {
            throw new ArgumentException("An activity read must cover at most 30 complete UTC days.");
        }
    }

    internal static void ValidatePageSize(int pageSize)
    {
        if (pageSize is < 1 or > PostgreSqlPlanningStore.MaximumReadPageSize)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), pageSize, "The page size must be between 1 and 1,000.");
        }
    }
}

/// <summary>
/// Contains one bounded canonical page of reconciliation dependencies.
/// </summary>
public sealed record ReconciliationDependencyPage
{
    internal ReconciliationDependencyPage(
        AccountKey ownerAccountKey,
        long ownerRepositoryGeneration,
        int pageSize,
        IEnumerable<AccountKey> affectedAccountKeys,
        bool hasMore)
    {
        Guard.ValidAccountKey(ownerAccountKey, nameof(ownerAccountKey));
        Guard.NonNegative(ownerRepositoryGeneration, nameof(ownerRepositoryGeneration));
        ActivityMinuteBucketPage.ValidatePageSize(pageSize);
        ArgumentNullException.ThrowIfNull(affectedAccountKeys);
        var values = affectedAccountKeys.ToArray();
        if (values.Length > pageSize || (hasMore && values.Length != pageSize))
        {
            throw new ArgumentException("The reconciliation page does not match its bounded page size.", nameof(affectedAccountKeys));
        }

        AccountKey? previous = null;
        foreach (var value in values)
        {
            Guard.ValidAccountKey(value, nameof(affectedAccountKeys));
            if (previous is { } prior && value <= prior)
            {
                throw new ArgumentException("Affected account keys must be unique and canonically ordered.", nameof(affectedAccountKeys));
            }

            previous = value;
        }

        OwnerAccountKey = ownerAccountKey;
        OwnerRepositoryGeneration = ownerRepositoryGeneration;
        PageSize = pageSize;
        AffectedAccountKeys = new ReadOnlyCollection<AccountKey>(values);
        HasMore = hasMore;
    }

    public AccountKey OwnerAccountKey { get; }

    public long OwnerRepositoryGeneration { get; }

    public int PageSize { get; }

    public IReadOnlyList<AccountKey> AffectedAccountKeys { get; }

    public bool HasMore { get; }

    public AccountKey? NextAfterAffectedAccountKey => HasMore ? AffectedAccountKeys[^1] : null;
}
