using System.Collections.ObjectModel;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

/// <summary>
/// Groups the exact durable state needed to replace one account and, when visible, its projection.
/// </summary>
public sealed record AccountPlanningSnapshot
{
    public AccountPlanningSnapshot(
        AccountStateSnapshot state,
        ProjectionSnapshot? desiredProjection = null,
        ActivityWindowAggregateSnapshot? activityAggregate = null)
    {
        State = state ?? throw new ArgumentNullException(nameof(state));
        if (desiredProjection is not null && desiredProjection.AccountKey != state.AccountKey)
        {
            throw new ArgumentException("The desired projection must belong to the account state.", nameof(desiredProjection));
        }

        if (activityAggregate is not null
            && (activityAggregate.AccountKey != state.AccountKey
                || activityAggregate.AccountStateVersion != state.StateVersion
                || activityAggregate.RepositoryGeneration != state.RepositoryGeneration))
        {
            throw new ArgumentException("The activity aggregate must be fenced by the exact account state.", nameof(activityAggregate));
        }

        DesiredProjection = desiredProjection;
        ActivityAggregate = activityAggregate;
    }

    public AccountStateSnapshot State { get; }

    public ProjectionSnapshot? DesiredProjection { get; }

    public ActivityWindowAggregateSnapshot? ActivityAggregate { get; }
}

/// <summary>
/// Proves whether an affected target belongs to the frozen corpus and, when admitted, carries its state.
/// </summary>
public sealed record AffectedAccountPlanningSnapshot
{
    private AffectedAccountPlanningSnapshot(AccountKey accountKey, bool isAdmitted, AccountPlanningSnapshot? account)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid target account key is required.", nameof(accountKey));
        }

        if (isAdmitted != (account is not null) || account is not null && account.State.AccountKey != accountKey)
        {
            throw new ArgumentException("An admitted target must carry its exact matching account state.", nameof(account));
        }

        AccountKey = accountKey;
        IsAdmitted = isAdmitted;
        Account = account;
    }

    public AccountKey AccountKey { get; }

    public bool IsAdmitted { get; }

    public AccountPlanningSnapshot? Account { get; }

    public static AffectedAccountPlanningSnapshot Admitted(AccountPlanningSnapshot account)
    {
        ArgumentNullException.ThrowIfNull(account);
        return new AffectedAccountPlanningSnapshot(account.State.AccountKey, isAdmitted: true, account);
    }

    public static AffectedAccountPlanningSnapshot NotAdmitted(AccountKey accountKey)
        => new(accountKey, isAdmitted: false, account: null);
}

/// <summary>
/// Carries an exact follow-pair read, including proven absence as multiplicity zero.
/// </summary>
public sealed record FollowPairPlanningSnapshot
{
    public FollowPairPlanningSnapshot(AccountKey sourceAccountKey, AccountKey targetAccountKey, int multiplicity)
    {
        if (!sourceAccountKey.IsValid || !targetAccountKey.IsValid)
        {
            throw new ArgumentException("Valid source and target account keys are required.");
        }

        ArgumentOutOfRangeException.ThrowIfNegative(multiplicity);

        SourceAccountKey = sourceAccountKey;
        TargetAccountKey = targetAccountKey;
        Multiplicity = multiplicity;
    }

    public AccountKey SourceAccountKey { get; }

    public AccountKey TargetAccountKey { get; }

    public int Multiplicity { get; }
}

/// <summary>
/// Contains every exact read used by the pure ordinary-record transition planner.
/// </summary>
public sealed record RecordMutationPlanningInput
{
    public RecordMutationPlanningInput(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        AccountPlanningSnapshot owner,
        RecordStateSnapshot? currentRecord = null,
        IEnumerable<AffectedAccountPlanningSnapshot>? affectedAccounts = null,
        IEnumerable<FollowPairPlanningSnapshot>? followPairs = null)
    {
        Reservation = reservation ?? throw new ArgumentNullException(nameof(reservation));
        Envelope = envelope ?? throw new ArgumentNullException(nameof(envelope));
        Owner = owner ?? throw new ArgumentNullException(nameof(owner));
        CurrentRecord = currentRecord;
        AffectedAccounts = FreezeDistinct(
            affectedAccounts ?? [],
            static value => value.AccountKey,
            nameof(affectedAccounts));
        FollowPairs = FreezeDistinct(
            followPairs ?? [],
            static value => (value.SourceAccountKey, value.TargetAccountKey),
            nameof(followPairs));
    }

    public DurableDeliveryReservation Reservation { get; }

    public DurableEventEnvelope Envelope { get; }

    public AccountPlanningSnapshot Owner { get; }

    public RecordStateSnapshot? CurrentRecord { get; }

    public IReadOnlyList<AffectedAccountPlanningSnapshot> AffectedAccounts { get; }

    public IReadOnlyList<FollowPairPlanningSnapshot> FollowPairs { get; }

    private static ReadOnlyCollection<T> FreezeDistinct<T, TKey>(
        IEnumerable<T> source,
        Func<T, TKey> keySelector,
        string parameterName)
        where TKey : notnull
    {
        var values = source.ToArray();
        if (values.Select(keySelector).Distinct().Count() != values.Length)
        {
            throw new ArgumentException("Planning evidence cannot contain duplicate identities.", parameterName);
        }

        return new ReadOnlyCollection<T>(values);
    }
}

public enum RecordMutationPlanningDecisionKind
{
    Commit,
    ValidatedNoOp,
    Quarantine,
    Retry,
    DeliveryAlreadyCompleted,
}

public enum RecordMutationRetryReason
{
    RepositoryGenerationNotYetObserved,
    AccountEvidenceRequired,
    FollowPairEvidenceRequired,
    DurableStateInconsistent,
    DesiredProjectionRequired,
    DesiredProjectionAheadOfState,
    ActivityAggregateRequired,
    ActivityAggregateDoesNotMatchProjectionCut,
}

/// <summary>
/// Represents one closed planner decision. Only <see cref="Commit"/> mutates ordinary state directly.
/// </summary>
public sealed record RecordMutationPlanningDecision
{
    private RecordMutationPlanningDecision(
        RecordMutationPlanningDecisionKind kind,
        DurableIngestionCommit? commit = null,
        DurableValidatedNoOp? validatedNoOp = null,
        DurableQuarantine? quarantine = null,
        RecordMutationRetryReason? retryReason = null,
        string? retryMessage = null)
    {
        Kind = kind;
        Commit = commit;
        ValidatedNoOp = validatedNoOp;
        Quarantine = quarantine;
        RetryReason = retryReason;
        RetryMessage = retryMessage;
    }

    public RecordMutationPlanningDecisionKind Kind { get; }

    public DurableIngestionCommit? Commit { get; }

    public DurableValidatedNoOp? ValidatedNoOp { get; }

    public DurableQuarantine? Quarantine { get; }

    public RecordMutationRetryReason? RetryReason { get; }

    public string? RetryMessage { get; }

    internal static RecordMutationPlanningDecision Applied(DurableIngestionCommit commit)
        => new(RecordMutationPlanningDecisionKind.Commit, commit: commit);

    internal static RecordMutationPlanningDecision Stale(DurableValidatedNoOp noOp)
        => new(RecordMutationPlanningDecisionKind.ValidatedNoOp, validatedNoOp: noOp);

    internal static RecordMutationPlanningDecision Rejected(DurableQuarantine quarantine)
        => new(RecordMutationPlanningDecisionKind.Quarantine, quarantine: quarantine);

    internal static RecordMutationPlanningDecision Retry(RecordMutationRetryReason reason, string message)
        => new(RecordMutationPlanningDecisionKind.Retry, retryReason: reason, retryMessage: message);

    internal static RecordMutationPlanningDecision Completed()
        => new(RecordMutationPlanningDecisionKind.DeliveryAlreadyCompleted);
}
