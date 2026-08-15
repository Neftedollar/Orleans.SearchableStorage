using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

/// <summary>
/// Identifies durable, restartable work which must finish before its TAP delivery can be acknowledged.
/// </summary>
public enum LifecyclePagedWorkKind
{
    InactiveAccountPurge = 1,
    RepositorySynchronization = 2,
}

public enum LifecycleStartDecisionKind
{
    ImmediateCommit,
    StartPagedWork,
    ValidatedNoOp,
    Quarantine,
    Retry,
    DeliveryAlreadyCompleted,
}

public enum LifecycleStartRetryReason
{
    AccountEvidenceRequired,
    RepositoryGenerationMismatch,
    DesiredProjectionAheadOfState,
    ReconciliationNotOpen,
}

/// <summary>
/// Contains the exact durable reads needed to start one lifecycle or repository-sync transition.
/// </summary>
public sealed record LifecycleStartPlanningInput
{
    public LifecycleStartPlanningInput(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        AccountStateSnapshot? account,
        ProjectionSnapshot? desiredProjection)
    {
        Reservation = reservation ?? throw new ArgumentNullException(nameof(reservation));
        Envelope = envelope ?? throw new ArgumentNullException(nameof(envelope));
        if (account is not null && account.AccountKey != envelope.AccountKey)
        {
            throw new ArgumentException("The account snapshot must belong to the event account.", nameof(account));
        }

        if (desiredProjection is not null && desiredProjection.AccountKey != envelope.AccountKey)
        {
            throw new ArgumentException("The desired projection must belong to the event account.", nameof(desiredProjection));
        }

        Account = account;
        DesiredProjection = desiredProjection;
    }

    public DurableDeliveryReservation Reservation { get; }

    public DurableEventEnvelope Envelope { get; }

    public AccountStateSnapshot? Account { get; }

    public ProjectionSnapshot? DesiredProjection { get; }
}

/// <summary>
/// Represents a closed start decision. Paged work deliberately does not carry an acknowledgement.
/// </summary>
public sealed record LifecycleStartDecision
{
    private LifecycleStartDecision(
        LifecycleStartDecisionKind kind,
        DurableIngestionCommit? immediateCommit = null,
        LifecyclePagedWorkKind? pagedWorkKind = null,
        AccountStateMutation? initialAccountState = null,
        ProjectionSnapshot? initialRemoval = null,
        DurableValidatedNoOp? validatedNoOp = null,
        DurableQuarantine? quarantine = null,
        LifecycleStartRetryReason? retryReason = null,
        string? retryMessage = null)
    {
        Kind = kind;
        ImmediateCommit = immediateCommit;
        PagedWorkKind = pagedWorkKind;
        InitialAccountState = initialAccountState;
        InitialRemoval = initialRemoval;
        ValidatedNoOp = validatedNoOp;
        Quarantine = quarantine;
        RetryReason = retryReason;
        RetryMessage = retryMessage;
    }

    public LifecycleStartDecisionKind Kind { get; }

    public DurableIngestionCommit? ImmediateCommit { get; }

    public LifecyclePagedWorkKind? PagedWorkKind { get; }

    public AccountStateMutation? InitialAccountState { get; }

    public ProjectionSnapshot? InitialRemoval { get; }

    public DurableValidatedNoOp? ValidatedNoOp { get; }

    public DurableQuarantine? Quarantine { get; }

    public LifecycleStartRetryReason? RetryReason { get; }

    public string? RetryMessage { get; }

    internal static LifecycleStartDecision Committed(DurableIngestionCommit commit)
        => new(LifecycleStartDecisionKind.ImmediateCommit, immediateCommit: commit);

    internal static LifecycleStartDecision Paged(
        LifecyclePagedWorkKind kind,
        AccountStateMutation? initialAccountState = null,
        ProjectionSnapshot? initialRemoval = null)
        => new(
            LifecycleStartDecisionKind.StartPagedWork,
            pagedWorkKind: kind,
            initialAccountState: initialAccountState,
            initialRemoval: initialRemoval);

    internal static LifecycleStartDecision NoOp(DurableValidatedNoOp noOp)
        => new(LifecycleStartDecisionKind.ValidatedNoOp, validatedNoOp: noOp);

    internal static LifecycleStartDecision Rejected(DurableQuarantine quarantine)
        => new(LifecycleStartDecisionKind.Quarantine, quarantine: quarantine);

    internal static LifecycleStartDecision Retry(LifecycleStartRetryReason reason, string message)
        => new(LifecycleStartDecisionKind.Retry, retryReason: reason, retryMessage: message);

    internal static LifecycleStartDecision Completed()
        => new(LifecycleStartDecisionKind.DeliveryAlreadyCompleted);
}

/// <summary>
/// Pure start planner for active reconciliation, inactive cleanup, and repository-sync barriers.
/// </summary>
public static class LifecycleTransitionPlanner
{
    public static LifecycleStartDecision Plan(LifecycleStartPlanningInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (!input.Reservation.IsPending)
        {
            return LifecycleStartDecision.Completed();
        }

        if (!ReservationMatches(input.Reservation, input.Envelope))
        {
            return LifecycleStartDecision.Rejected(Quarantine(
                input,
                DurableQuarantineReason.ReservationMismatch));
        }

        return input.Envelope.EventKind switch
        {
            DurableEventKind.AccountLifecycle => PlanLifecycle(input),
            DurableEventKind.RepositorySync => PlanRepositorySync(input),
            _ => LifecycleStartDecision.Rejected(Quarantine(
                input,
                DurableQuarantineReason.UnsupportedTransitionKind)),
        };
    }

    private static LifecycleStartDecision PlanLifecycle(LifecycleStartPlanningInput input)
    {
        var envelope = input.Envelope;
        var lifecycle = envelope.Lifecycle
            ?? throw new InvalidOperationException("The durable envelope validates lifecycle shape.");
        var stateDecision = BuildLifecycleState(input, lifecycle, out var nextState);
        if (stateDecision is not null)
        {
            return stateDecision;
        }

        var removalDecision = BuildRemoval(input, nextState!, out var removal);
        if (removalDecision is not null)
        {
            return removalDecision;
        }

        if (lifecycle != DurableAccountLifecycle.Active)
        {
            return LifecycleStartDecision.Paged(
                LifecyclePagedWorkKind.InactiveAccountPurge,
                nextState,
                removal);
        }

        var dependency = new ReconciliationDependencyMutation(
            envelope.AccountKey,
            envelope.RepositoryGeneration,
            envelope.AccountKey,
            ReconciliationDependencyAction.Add);
        return LifecycleStartDecision.Committed(new DurableIngestionCommit(
            envelope,
            [nextState!],
            projections: removal is null ? [] : [removal],
            reconciliationDependencies: [dependency]));
    }

    private static LifecycleStartDecision PlanRepositorySync(LifecycleStartPlanningInput input)
    {
        var envelope = input.Envelope;
        var state = input.Account;
        if (state is null)
        {
            return LifecycleStartDecision.Retry(
                LifecycleStartRetryReason.AccountEvidenceRequired,
                "Repository synchronization requires an existing durable account state.");
        }

        if (state.RepositoryGeneration > envelope.RepositoryGeneration)
        {
            return LifecycleStartDecision.NoOp(
                new DurableValidatedNoOp(envelope, ValidatedNoOpReason.RepositoryGenerationSuperseded));
        }

        if (state.RepositoryGeneration < envelope.RepositoryGeneration)
        {
            return LifecycleStartDecision.Retry(
                LifecycleStartRetryReason.RepositoryGenerationMismatch,
                "The durable account has not reached the repository-sync generation.");
        }

        if (state.SynchronizationComplete)
        {
            if (state.CompletedSyncRevision is not null
                && string.CompareOrdinal(state.CompletedSyncRevision, envelope.RepositoryRevision) >= 0)
            {
                return LifecycleStartDecision.NoOp(
                    new DurableValidatedNoOp(envelope, ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted));
            }

            return LifecycleStartDecision.Retry(
                LifecycleStartRetryReason.ReconciliationNotOpen,
                "A newer repository-sync event cannot complete without an open reconciliation barrier.");
        }

        if (state.Lifecycle != DurableAccountLifecycle.Active)
        {
            return LifecycleStartDecision.Rejected(Quarantine(
                input,
                DurableQuarantineReason.InactiveRepositorySync));
        }

        return LifecycleStartDecision.Paged(LifecyclePagedWorkKind.RepositorySynchronization);
    }

    private static LifecycleStartDecision? BuildLifecycleState(
        LifecycleStartPlanningInput input,
        DurableAccountLifecycle lifecycle,
        out AccountStateMutation? nextState)
    {
        var envelope = input.Envelope;
        var current = input.Account;
        nextState = null;
        if (current is not null)
        {
            if (current.RepositoryGeneration > envelope.RepositoryGeneration
                || envelope.RepositoryGeneration - current.RepositoryGeneration > 1)
            {
                return LifecycleStartDecision.Retry(
                    LifecycleStartRetryReason.RepositoryGenerationMismatch,
                    "A lifecycle transition may retain the current generation or advance it by exactly one.");
            }

            if (input.DesiredProjection is { } desired && desired.Version > current.StateVersion)
            {
                return LifecycleStartDecision.Retry(
                    LifecycleStartRetryReason.DesiredProjectionAheadOfState,
                    "The desired projection is ahead of the lifecycle account state.");
            }
        }
        else if (input.DesiredProjection is not null)
        {
            return LifecycleStartDecision.Retry(
                LifecycleStartRetryReason.AccountEvidenceRequired,
                "A desired projection cannot exist without its durable account state.");
        }

        var expectedVersion = current?.StateVersion ?? 0;
        nextState = new AccountStateMutation(
            envelope.AccountKey,
            expectedVersion,
            checked(expectedVersion + 1),
            lifecycle,
            envelope.RepositoryGeneration,
            completedSyncRevision: null,
            synchronizationComplete: false,
            current?.LastActivityMinuteUtc ?? 0,
            current?.CurrentPostCount ?? 0,
            current?.CurrentFollowingCount ?? 0,
            current?.CurrentFollowerCount ?? 0,
            lastAppliedRevision: null);
        return null;
    }

    private static LifecycleStartDecision? BuildRemoval(
        LifecycleStartPlanningInput input,
        AccountStateMutation state,
        out ProjectionSnapshot? removal)
    {
        var desired = input.DesiredProjection;
        removal = desired is { Operation: ProjectionOperation.Upsert }
            ? new ProjectionSnapshot(
                state.AccountKey,
                state.NextVersion,
                ProjectionOperation.Remove,
                isComplete: true,
                Math.Max(desired.ProjectionCutMinuteUtc, input.Envelope.ObservedAtMinuteUtc),
                nextRecalculationMinuteUtc: null,
                desired.LastActivityMinuteUtc,
                desired.CreatedRecordCount1Day,
                desired.CreatedRecordCount7Days,
                desired.CreatedRecordCount30Days,
                desired.UpdatedRecordCount1Day,
                desired.UpdatedRecordCount7Days,
                desired.UpdatedRecordCount30Days,
                desired.DeletedRecordCount1Day,
                desired.DeletedRecordCount7Days,
                desired.DeletedRecordCount30Days,
                desired.CurrentPostCount,
                desired.CurrentFollowingCount,
                desired.CurrentFollowerCount,
                desired.PostCreates1Day,
                desired.PostCreates7Days,
                desired.PostCreates30Days,
                desired.ReceivedEngagementCreates30Days)
            : null;
        return null;
    }

    private static bool ReservationMatches(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope)
        => reservation.SourceInstanceId == envelope.SourceInstanceId
            && reservation.TapDeliveryId == envelope.TapDeliveryId
            && string.Equals(reservation.DeliveryDigest, envelope.DeliveryDigest, StringComparison.Ordinal)
            && reservation.FirstObservedAtMinuteUtc == envelope.ObservedAtMinuteUtc;

    private static DurableQuarantine Quarantine(
        LifecycleStartPlanningInput input,
        DurableQuarantineReason reason)
        => new(
            input.Reservation.SourceInstanceId,
            input.Reservation.TapDeliveryId,
            input.Reservation.DeliveryDigest,
            reason,
            input.Reservation.FirstObservedAtMinuteUtc,
            input.Envelope.SemanticDigest,
            input.Envelope.AccountKey);
}
