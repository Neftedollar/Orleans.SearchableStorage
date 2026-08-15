using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

/// <summary>
/// Purely derives one ordinary record transition from exact durable planning snapshots.
/// </summary>
public static class RecordMutationTransitionPlanner
{
    private const long OneDayMinutes = 24 * 60;
    private const long SevenDayMinutes = 7 * OneDayMinutes;
    private const long ThirtyDayMinutes = 30 * OneDayMinutes;

    public static RecordMutationPlanningDecision Plan(RecordMutationPlanningInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        var reservation = input.Reservation;
        var envelope = input.Envelope;

        if (!reservation.IsPending)
        {
            return RecordMutationPlanningDecision.Completed();
        }

        if (!ReservationMatches(reservation, envelope))
        {
            return Quarantine(input, DurableQuarantineReason.ReservationMismatch);
        }

        if (envelope.EventKind != DurableEventKind.RecordMutation)
        {
            return Quarantine(input, DurableQuarantineReason.NotRecordMutation);
        }

        var owner = input.Owner.State;
        if (owner.AccountKey != envelope.AccountKey)
        {
            return Retry(
                RecordMutationRetryReason.AccountEvidenceRequired,
                "The owner planning snapshot does not match the event account.");
        }

        if (owner.RepositoryGeneration > envelope.RepositoryGeneration)
        {
            return RecordMutationPlanningDecision.Stale(
                new DurableValidatedNoOp(envelope, ValidatedNoOpReason.RepositoryGenerationSuperseded));
        }

        if (owner.RepositoryGeneration < envelope.RepositoryGeneration)
        {
            return Retry(
                RecordMutationRetryReason.RepositoryGenerationNotYetObserved,
                "The durable account generation has not reached the event generation.");
        }

        if (!CurrentRecordMatchesEnvelopeIdentity(input.CurrentRecord, envelope))
        {
            return Retry(
                RecordMutationRetryReason.DurableStateInconsistent,
                "The current-record read is not bound to the event identity and generation.");
        }

        if (input.CurrentRecord is { } current)
        {
            var recordRevisionComparison = CompareRevisions(current.LatestRevision, envelope.RepositoryRevision!);
            if (recordRevisionComparison > 0)
            {
                return RecordMutationPlanningDecision.Stale(
                    new DurableValidatedNoOp(envelope, ValidatedNoOpReason.RecordRevisionAlreadyObserved));
            }

            if (recordRevisionComparison == 0)
            {
                return CurrentRecordMatchesEnvelopeValue(current, envelope)
                    ? RecordMutationPlanningDecision.Stale(
                        new DurableValidatedNoOp(envelope, ValidatedNoOpReason.RecordRevisionAlreadyObserved))
                    : Quarantine(input, DurableQuarantineReason.ConflictingRecordRevision);
            }
        }

        if (envelope.Action == DurableRecordAction.Delete
            && input.CurrentRecord is not { IsDeleted: false })
        {
            return Quarantine(input, DurableQuarantineReason.MissingPriorRecord);
        }

        if (owner.Lifecycle != DurableAccountLifecycle.Active)
        {
            return Quarantine(input, DurableQuarantineReason.AccountNotActive);
        }

        if (envelope.IsLive)
        {
            if (!owner.SynchronizationComplete)
            {
                return Quarantine(input, DurableQuarantineReason.LiveBeforeRepositorySync);
            }

            if (owner.LastAppliedRevision is null)
            {
                return Retry(
                    RecordMutationRetryReason.DurableStateInconsistent,
                    "A synchronized account is missing its repository-wide applied revision high-water.");
            }

            if (CompareRevisions(owner.LastAppliedRevision, envelope.RepositoryRevision!) > 0)
            {
                return RecordMutationPlanningDecision.Stale(
                    new DurableValidatedNoOp(envelope, ValidatedNoOpReason.RepositoryRevisionAlreadyApplied));
            }
        }
        else if (owner.SynchronizationComplete)
        {
            return Quarantine(input, DurableQuarantineReason.HistoricalAfterRepositorySync);
        }

        try
        {
            return PlanFreshMutation(input);
        }
        catch (OverflowException)
        {
            return Quarantine(input, DurableQuarantineReason.CounterOverflow);
        }
        catch (CounterUnderflowException exception)
        {
            return Retry(RecordMutationRetryReason.DurableStateInconsistent, exception.Message);
        }
    }

    private static RecordMutationPlanningDecision PlanFreshMutation(RecordMutationPlanningInput input)
    {
        var envelope = input.Envelope;
        var ownerKey = envelope.AccountKey;
        var current = input.CurrentRecord;
        var affectedEvidence = input.AffectedAccounts.ToDictionary(static value => value.AccountKey);
        var pairEvidence = input.FollowPairs.ToDictionary(static value => (value.SourceAccountKey, value.TargetAccountKey));
        var accounts = new Dictionary<AccountKey, MutableAccount>
        {
            [ownerKey] = new MutableAccount(input.Owner, isOwner: true),
        };
        var pairMutations = new List<FollowPairMutation>();
        var dependencies = new HashSet<AccountKey>();

        var oldPostIsCurrent = envelope.Collection == DurableRecordKind.FeedPost
            && current is { IsDeleted: false };
        var newPostIsCurrent = envelope.Collection == DurableRecordKind.FeedPost
            && envelope.Action != DurableRecordAction.Delete;
        accounts[ownerKey].CurrentPostDelta = BoolDelta(oldPostIsCurrent, newPostIsCurrent);

        if (envelope.Collection == DurableRecordKind.GraphFollow)
        {
            var followDecision = PlanFollowMutation(
                input,
                accounts,
                affectedEvidence,
                pairEvidence,
                pairMutations,
                dependencies);
            if (followDecision is not null)
            {
                return followDecision;
            }
        }

        var receivesEngagement = envelope.IsLive
            && envelope.Action == DurableRecordAction.Create
            && envelope.TargetAccountKey is { }
            && (envelope.Collection is DurableRecordKind.FeedLike or DurableRecordKind.FeedRepost
                || envelope.Collection == DurableRecordKind.FeedPost && envelope.IsDirectReply);
        if (receivesEngagement)
        {
            var targetKey = envelope.TargetAccountKey!.Value;
            var evidenceDecision = ResolveAffectedAccount(
                ownerKey,
                targetKey,
                input.Owner,
                affectedEvidence,
                out var target);
            if (evidenceDecision is not null)
            {
                return evidenceDecision;
            }

            if (target is not null)
            {
                var mutableTarget = GetOrAdd(accounts, target, isOwner: targetKey == ownerKey);
                mutableTarget.ReceivedEngagementCreates = checked(mutableTarget.ReceivedEngagementCreates + 1);
            }
        }

        if (envelope.IsLive)
        {
            var mutableOwner = accounts[ownerKey];
            switch (envelope.Action)
            {
                case DurableRecordAction.Create:
                    mutableOwner.RecordCreates = 1;
                    mutableOwner.PostCreates = envelope.Collection == DurableRecordKind.FeedPost ? 1 : 0;
                    break;
                case DurableRecordAction.Update:
                    mutableOwner.RecordUpdates = 1;
                    break;
                case DurableRecordAction.Delete:
                    mutableOwner.RecordDeletes = 1;
                    break;
                default:
                    throw new InvalidOperationException("The durable envelope constructor validates record actions.");
            }
        }

        try
        {
            var stateMutations = BuildAccountMutations(accounts, envelope);
            var activity = BuildActivity(accounts, envelope.IsLive, envelope.ObservedAtMinuteUtc);
            var projectionsDecision = BuildProjections(accounts, envelope, activity, out var projections);
            if (projectionsDecision is not null)
            {
                return projectionsDecision;
            }

            var record = new RecordStateMutation(
                envelope.AccountKey,
                envelope.RepositoryGeneration,
                envelope.Collection!.Value,
                envelope.RecordKey!,
                envelope.RepositoryRevision!,
                envelope.Action == DurableRecordAction.Delete,
                envelope.Action == DurableRecordAction.Delete ? null : envelope.Cid,
                envelope.Action == DurableRecordAction.Delete ? null : envelope.TargetAccountKey,
                envelope.Action != DurableRecordAction.Delete && envelope.IsDirectReply);
            var reconciliation = envelope.IsLive
                ? Array.Empty<ReconciliationDependencyMutation>()
                : dependencies
                    .Order()
                    .Select(key => new ReconciliationDependencyMutation(
                        ownerKey,
                        envelope.RepositoryGeneration,
                        key,
                        ReconciliationDependencyAction.Add))
                    .ToArray();
            var commit = new DurableIngestionCommit(
                envelope,
                stateMutations,
                records: [record],
                followPairs: pairMutations
                    .OrderBy(static value => value.SourceAccountKey)
                    .ThenBy(static value => value.TargetAccountKey),
                activity: activity,
                projections: projections,
                reconciliationDependencies: reconciliation);
            return RecordMutationPlanningDecision.Applied(commit);
        }
        catch (OverflowException)
        {
            return Quarantine(input, DurableQuarantineReason.CounterOverflow);
        }
        catch (CounterUnderflowException exception)
        {
            return Retry(RecordMutationRetryReason.DurableStateInconsistent, exception.Message);
        }
    }

    private static RecordMutationPlanningDecision? PlanFollowMutation(
        RecordMutationPlanningInput input,
        IDictionary<AccountKey, MutableAccount> accounts,
        IReadOnlyDictionary<AccountKey, AffectedAccountPlanningSnapshot> affectedEvidence,
        Dictionary<(AccountKey SourceAccountKey, AccountKey TargetAccountKey), FollowPairPlanningSnapshot> pairEvidence,
        List<FollowPairMutation> pairMutations,
        HashSet<AccountKey> dependencies)
    {
        var envelope = input.Envelope;
        var ownerKey = envelope.AccountKey;
        AccountKey? oldTarget = input.CurrentRecord is { IsDeleted: false } current
            ? current.TargetAccountKey
            : null;
        AccountKey? newTarget = envelope.Action == DurableRecordAction.Delete
            ? null
            : envelope.TargetAccountKey;
        var targets = new[] { oldTarget, newTarget }
            .Where(static value => value.HasValue)
            .Select(static value => value!.Value)
            .Distinct()
            .Order()
            .ToArray();

        foreach (var targetKey in targets)
        {
            if (!pairEvidence.TryGetValue((ownerKey, targetKey), out var pair))
            {
                return Retry(
                    RecordMutationRetryReason.FollowPairEvidenceRequired,
                    $"An exact follow-pair read is required for target {targetKey}.");
            }

            if (oldTarget == targetKey && pair.Multiplicity == 0)
            {
                return Retry(
                    RecordMutationRetryReason.DurableStateInconsistent,
                    "A current follow record has no positive durable pair multiplicity.");
            }

            var delta = (newTarget == targetKey ? 1 : 0) - (oldTarget == targetKey ? 1 : 0);
            var nextMultiplicity = checked(pair.Multiplicity + delta);
            if (nextMultiplicity < 0)
            {
                return Retry(
                    RecordMutationRetryReason.DurableStateInconsistent,
                    "A follow-pair transition would produce a negative multiplicity.");
            }

            pairMutations.Add(new FollowPairMutation(ownerKey, targetKey, nextMultiplicity));
            var distinctDelta = BoolDelta(pair.Multiplicity > 0, nextMultiplicity > 0);
            if (distinctDelta == 0)
            {
                continue;
            }

            accounts[ownerKey].CurrentFollowingDelta = checked(accounts[ownerKey].CurrentFollowingDelta + distinctDelta);
            var evidenceDecision = ResolveAffectedAccount(
                ownerKey,
                targetKey,
                input.Owner,
                affectedEvidence,
                out var target);
            if (evidenceDecision is not null)
            {
                return evidenceDecision;
            }

            if (target is null)
            {
                continue;
            }

            var mutableTarget = GetOrAdd(accounts, target, isOwner: targetKey == ownerKey);
            mutableTarget.CurrentFollowerDelta = checked(mutableTarget.CurrentFollowerDelta + distinctDelta);
            if (!envelope.IsLive && targetKey != ownerKey)
            {
                dependencies.Add(targetKey);
                mutableTarget.RequiresReconciliationRemoval = true;
            }
        }

        return null;
    }

    private static RecordMutationPlanningDecision? ResolveAffectedAccount(
        AccountKey ownerKey,
        AccountKey targetKey,
        AccountPlanningSnapshot owner,
        IReadOnlyDictionary<AccountKey, AffectedAccountPlanningSnapshot> affectedEvidence,
        out AccountPlanningSnapshot? target)
    {
        if (targetKey == ownerKey)
        {
            target = owner;
            return null;
        }

        if (!affectedEvidence.TryGetValue(targetKey, out var evidence))
        {
            target = null;
            return Retry(
                RecordMutationRetryReason.AccountEvidenceRequired,
                $"Frozen-corpus admission evidence is required for target {targetKey}.");
        }

        target = evidence.Account;
        return null;
    }

    private static List<AccountStateMutation> BuildAccountMutations(
        IReadOnlyDictionary<AccountKey, MutableAccount> accounts,
        DurableEventEnvelope envelope)
    {
        var result = new List<AccountStateMutation>(accounts.Count);
        foreach (var pair in accounts.OrderBy(static value => value.Key))
        {
            var mutable = pair.Value;
            var state = mutable.Snapshot.State;
            var currentPostCount = AddNonNegative(state.CurrentPostCount, mutable.CurrentPostDelta, "Current post count would become negative.");
            var currentFollowingCount = AddNonNegative(state.CurrentFollowingCount, mutable.CurrentFollowingDelta, "Current following count would become negative.");
            var currentFollowerCount = AddNonNegative(state.CurrentFollowerCount, mutable.CurrentFollowerDelta, "Current follower count would become negative.");
            var lastActivityMinute = mutable.IsOwner && envelope.IsLive
                ? Math.Max(state.LastActivityMinuteUtc, envelope.ObservedAtMinuteUtc)
                : state.LastActivityMinuteUtc;
            var lastAppliedRevision = mutable.IsOwner && envelope.IsLive
                ? envelope.RepositoryRevision
                : state.LastAppliedRevision;
            mutable.NextState = new AccountStateMutation(
                state.AccountKey,
                state.StateVersion,
                checked(state.StateVersion + 1),
                state.Lifecycle,
                state.RepositoryGeneration,
                state.CompletedSyncRevision,
                state.SynchronizationComplete,
                lastActivityMinute,
                currentPostCount,
                currentFollowingCount,
                currentFollowerCount,
                lastAppliedRevision);
            result.Add(mutable.NextState);
        }

        return result;
    }

    private static ActivityMinuteDelta[] BuildActivity(
        IReadOnlyDictionary<AccountKey, MutableAccount> accounts,
        bool isLive,
        long observedMinuteUtc)
    {
        if (!isLive)
        {
            return [];
        }

        return accounts
            .OrderBy(static value => value.Key)
            .Select(static pair => pair.Value)
            .Where(static value => value.HasActivity)
            .Select(value => new ActivityMinuteDelta(
                value.Snapshot.State.AccountKey,
                observedMinuteUtc,
                value.Snapshot.State.RepositoryGeneration,
                value.RecordCreates,
                value.RecordUpdates,
                value.RecordDeletes,
                value.PostCreates,
                value.ReceivedEngagementCreates))
            .ToArray();
    }

    private static RecordMutationPlanningDecision? BuildProjections(
        IReadOnlyDictionary<AccountKey, MutableAccount> accounts,
        DurableEventEnvelope envelope,
        IReadOnlyList<ActivityMinuteDelta> activity,
        out IReadOnlyList<ProjectionSnapshot> projections)
    {
        var activityByAccount = activity.ToDictionary(static value => value.AccountKey);
        var result = new List<ProjectionSnapshot>();
        foreach (var pair in accounts.OrderBy(static value => value.Key))
        {
            var mutable = pair.Value;
            var state = mutable.Snapshot.State;
            if (!envelope.IsLive)
            {
                var historicalDesired = mutable.Snapshot.DesiredProjection;
                if (historicalDesired is not null && historicalDesired.Version > state.StateVersion)
                {
                    projections = [];
                    return Retry(
                        RecordMutationRetryReason.DesiredProjectionAheadOfState,
                        $"The desired projection for {state.AccountKey} is ahead of its account state.");
                }

                if (!mutable.RequiresReconciliationRemoval
                    || historicalDesired is not { Operation: ProjectionOperation.Upsert } desiredUpsert)
                {
                    continue;
                }

                result.Add(BuildRemovalProjection(
                    desiredUpsert,
                    mutable.NextState
                        ?? throw new InvalidOperationException("Account mutations must be built before projections."),
                    envelope.ObservedAtMinuteUtc));
                continue;
            }

            if (state.Lifecycle != DurableAccountLifecycle.Active || !state.SynchronizationComplete)
            {
                continue;
            }

            var desired = mutable.Snapshot.DesiredProjection;
            if (desired is null)
            {
                projections = [];
                return Retry(
                    RecordMutationRetryReason.DesiredProjectionRequired,
                    $"A visible account {state.AccountKey} is missing its current desired projection.");
            }

            if (desired.Version > state.StateVersion)
            {
                projections = [];
                return Retry(
                    RecordMutationRetryReason.DesiredProjectionAheadOfState,
                    $"The desired projection for {state.AccountKey} is ahead of its account state.");
            }

            var projectionCutMinuteUtc = Math.Max(
                desired.ProjectionCutMinuteUtc,
                envelope.ObservedAtMinuteUtc);
            var aggregate = mutable.Snapshot.ActivityAggregate;
            if (aggregate is null)
            {
                projections = [];
                return Retry(
                    RecordMutationRetryReason.ActivityAggregateRequired,
                    $"An exact activity aggregate is required for visible account {state.AccountKey}.");
            }

            if (aggregate.CutMinuteUtc != projectionCutMinuteUtc)
            {
                projections = [];
                return Retry(
                    RecordMutationRetryReason.ActivityAggregateDoesNotMatchProjectionCut,
                    $"The activity aggregate for {state.AccountKey} is not frozen at the monotonic projection cut.");
            }

            activityByAccount.TryGetValue(state.AccountKey, out var delta);
            result.Add(BuildProjection(mutable, projectionCutMinuteUtc, aggregate, delta));
        }

        projections = result;
        return null;
    }

    private static ProjectionSnapshot BuildRemovalProjection(
        ProjectionSnapshot desired,
        AccountStateMutation state,
        long observedMinuteUtc)
        => new(
            state.AccountKey,
            state.NextVersion,
            ProjectionOperation.Remove,
            isComplete: true,
            projectionCutMinuteUtc: Math.Max(desired.ProjectionCutMinuteUtc, observedMinuteUtc),
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
            desired.ReceivedEngagementCreates30Days);

    private static ProjectionSnapshot BuildProjection(
        MutableAccount account,
        long cutMinuteUtc,
        ActivityWindowAggregateSnapshot aggregate,
        ActivityMinuteDelta? delta)
    {
        var created = AddToWindows(
            aggregate.RecordCreates,
            delta?.RecordCreates ?? 0,
            delta?.MinuteUtc,
            cutMinuteUtc);
        var updated = AddToWindows(
            aggregate.RecordUpdates,
            delta?.RecordUpdates ?? 0,
            delta?.MinuteUtc,
            cutMinuteUtc);
        var deleted = AddToWindows(
            aggregate.RecordDeletes,
            delta?.RecordDeletes ?? 0,
            delta?.MinuteUtc,
            cutMinuteUtc);
        var posts = AddToWindows(
            aggregate.PostCreates,
            delta?.PostCreates ?? 0,
            delta?.MinuteUtc,
            cutMinuteUtc);
        var engagementThirtyDays = aggregate.ReceivedEngagementCreatesThirtyDays;
        if (delta is { ReceivedEngagementCreates: > 0 }
            && IsInsideWindow(delta.MinuteUtc, cutMinuteUtc, ThirtyDayMinutes))
        {
            engagementThirtyDays = checked(
                engagementThirtyDays + delta.ReceivedEngagementCreates);
        }

        var nextRecalculation = aggregate.NextExpiryMinuteUtc;
        if (delta is not null)
        {
            if ((delta.RecordCreates | delta.RecordUpdates | delta.RecordDeletes | delta.PostCreates) != 0)
            {
                ConsiderExpiry(delta.MinuteUtc, OneDayMinutes, cutMinuteUtc, ref nextRecalculation);
                ConsiderExpiry(delta.MinuteUtc, SevenDayMinutes, cutMinuteUtc, ref nextRecalculation);
                ConsiderExpiry(delta.MinuteUtc, ThirtyDayMinutes, cutMinuteUtc, ref nextRecalculation);
            }

            if (delta.ReceivedEngagementCreates > 0)
            {
                ConsiderExpiry(delta.MinuteUtc, ThirtyDayMinutes, cutMinuteUtc, ref nextRecalculation);
            }
        }

        var state = account.NextState
            ?? throw new InvalidOperationException("Account mutations must be built before projections.");
        return new ProjectionSnapshot(
            state.AccountKey,
            state.NextVersion,
            ProjectionOperation.Upsert,
            isComplete: true,
            cutMinuteUtc,
            nextRecalculation,
            state.LastActivityMinuteUtc,
            created.OneDay,
            created.SevenDays,
            created.ThirtyDays,
            updated.OneDay,
            updated.SevenDays,
            updated.ThirtyDays,
            deleted.OneDay,
            deleted.SevenDays,
            deleted.ThirtyDays,
            state.CurrentPostCount,
            state.CurrentFollowingCount,
            state.CurrentFollowerCount,
            posts.OneDay,
            posts.SevenDays,
            posts.ThirtyDays,
            engagementThirtyDays);
    }

    private static ActivityRollingCounts AddToWindows(
        ActivityRollingCounts aggregate,
        long delta,
        long? deltaMinuteUtc,
        long cutMinuteUtc)
    {
        if (delta == 0 || deltaMinuteUtc is null)
        {
            return aggregate;
        }

        return new ActivityRollingCounts(
            checked(aggregate.OneDay + (IsInsideWindow(deltaMinuteUtc.Value, cutMinuteUtc, OneDayMinutes) ? delta : 0)),
            checked(aggregate.SevenDays + (IsInsideWindow(deltaMinuteUtc.Value, cutMinuteUtc, SevenDayMinutes) ? delta : 0)),
            checked(aggregate.ThirtyDays + (IsInsideWindow(deltaMinuteUtc.Value, cutMinuteUtc, ThirtyDayMinutes) ? delta : 0)));
    }

    private static bool IsInsideWindow(
        long activityMinuteUtc,
        long cutMinuteUtc,
        long windowMinutes)
        => activityMinuteUtc <= cutMinuteUtc
            && activityMinuteUtc > cutMinuteUtc - windowMinutes;

    private static void ConsiderExpiry(long bucketMinute, long windowMinutes, long cutMinuteUtc, ref long? next)
    {
        var expiry = checked(bucketMinute + windowMinutes);
        if (expiry > cutMinuteUtc && (next is null || expiry < next.Value))
        {
            next = expiry;
        }
    }

    private static MutableAccount GetOrAdd(
        IDictionary<AccountKey, MutableAccount> accounts,
        AccountPlanningSnapshot snapshot,
        bool isOwner)
    {
        if (!accounts.TryGetValue(snapshot.State.AccountKey, out var mutable))
        {
            mutable = new MutableAccount(snapshot, isOwner);
            accounts.Add(snapshot.State.AccountKey, mutable);
        }

        return mutable;
    }

    private static bool ReservationMatches(DurableDeliveryReservation reservation, DurableEventEnvelope envelope)
        => reservation.SourceInstanceId == envelope.SourceInstanceId
            && reservation.TapDeliveryId == envelope.TapDeliveryId
            && string.Equals(reservation.DeliveryDigest, envelope.DeliveryDigest, StringComparison.Ordinal)
            && reservation.FirstObservedAtMinuteUtc == envelope.ObservedAtMinuteUtc;

    private static bool CurrentRecordMatchesEnvelopeIdentity(RecordStateSnapshot? current, DurableEventEnvelope envelope)
        => current is null
            || current.AccountKey == envelope.AccountKey
                && current.RepositoryGeneration == envelope.RepositoryGeneration
                && current.Collection == envelope.Collection
                && string.Equals(current.RecordKey, envelope.RecordKey, StringComparison.Ordinal);

    private static bool CurrentRecordMatchesEnvelopeValue(RecordStateSnapshot current, DurableEventEnvelope envelope)
    {
        var expectedDeleted = envelope.Action == DurableRecordAction.Delete;
        return current.IsDeleted == expectedDeleted
            && (expectedDeleted
                || string.Equals(current.Cid, envelope.Cid, StringComparison.Ordinal)
                    && current.TargetAccountKey == envelope.TargetAccountKey
                    && current.IsDirectReply == envelope.IsDirectReply);
    }

    private static int CompareRevisions(string left, string right) => string.CompareOrdinal(left, right);

    private static int BoolDelta(bool oldValue, bool newValue) => (newValue ? 1 : 0) - (oldValue ? 1 : 0);

    private static long AddNonNegative(long value, int delta, string message)
    {
        var result = checked(value + delta);
        if (result < 0)
        {
            throw new CounterUnderflowException(message);
        }

        return result;
    }

    private static RecordMutationPlanningDecision Retry(RecordMutationRetryReason reason, string message)
        => RecordMutationPlanningDecision.Retry(reason, message);

    private static RecordMutationPlanningDecision Quarantine(
        RecordMutationPlanningInput input,
        DurableQuarantineReason reason)
        => RecordMutationPlanningDecision.Rejected(
            new DurableQuarantine(
                input.Reservation.SourceInstanceId,
                input.Reservation.TapDeliveryId,
                input.Reservation.DeliveryDigest,
                reason,
                input.Reservation.FirstObservedAtMinuteUtc,
                input.Envelope.SemanticDigest,
                input.Envelope.AccountKey));

    private sealed class MutableAccount
    {
        internal MutableAccount(AccountPlanningSnapshot snapshot, bool isOwner)
        {
            Snapshot = snapshot;
            IsOwner = isOwner;
        }

        internal AccountPlanningSnapshot Snapshot { get; }

        internal bool IsOwner { get; }

        internal int CurrentPostDelta { get; set; }

        internal int CurrentFollowingDelta { get; set; }

        internal int CurrentFollowerDelta { get; set; }

        internal long RecordCreates { get; set; }

        internal long RecordUpdates { get; set; }

        internal long RecordDeletes { get; set; }

        internal long PostCreates { get; set; }

        internal long ReceivedEngagementCreates { get; set; }

        internal AccountStateMutation? NextState { get; set; }

        internal bool RequiresReconciliationRemoval { get; set; }

        internal bool HasActivity => (RecordCreates | RecordUpdates | RecordDeletes | PostCreates | ReceivedEngagementCreates) != 0;
    }

    private sealed class CounterUnderflowException : InvalidOperationException
    {
        internal CounterUnderflowException(string message)
            : base(message)
        {
        }
    }
}
