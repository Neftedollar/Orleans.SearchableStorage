using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning.Tests;

public sealed class RecordMutationTransitionPlannerTests
{
    private const string SyncRevision = "3jzfcijpj2z2a";
    private const string EventRevision = "3jzfcijpj2z2b";
    private const string NewerRevision = "3jzfcijpj2z2c";
    private const long Minute = 100_000;
    private static readonly Guid Source = Guid.Parse("b327b8fe-c754-45cc-9664-5a5168b6a339");

    [Fact]
    public void LivePostCreateBuildsStockActivityFullProjectionAndRepositoryHighWater()
    {
        var owner = Account("owner-post-create");
        var envelope = Envelope(owner, DurableRecordKind.FeedPost, DurableRecordAction.Create, cid: "post-cid");

        var decision = RecordMutationTransitionPlanner.Plan(Input(envelope, Visible(owner)));

        var commit = AssertCommit(decision);
        var state = Assert.Single(commit.AccountStates);
        Assert.Equal(1, state.CurrentPostCount);
        Assert.Equal(Minute, state.LastActivityMinuteUtc);
        Assert.Equal(EventRevision, state.LastAppliedRevision);
        var activity = Assert.Single(commit.Activity);
        Assert.Equal(1, activity.RecordCreates);
        Assert.Equal(1, activity.PostCreates);
        var projection = Assert.Single(commit.Projections);
        Assert.Equal(1, projection.CreatedRecordCount1Day);
        Assert.Equal(1, projection.CreatedRecordCount7Days);
        Assert.Equal(1, projection.CreatedRecordCount30Days);
        Assert.Equal(1, projection.PostCreates1Day);
        Assert.Equal(1, projection.CurrentPostCount);
        Assert.Equal(Minute, projection.ProjectionCutMinuteUtc);
        Assert.Equal(Minute + (24 * 60), projection.NextRecalculationMinuteUtc);
    }

    [Fact]
    public void StrictlyOlderCrossRecordRevisionIsACommitTimeValidatedNoOp()
    {
        var owner = Account("owner-stale-high-water");
        var envelope = Envelope(
            owner,
            DurableRecordKind.ActorProfile,
            DurableRecordAction.Update,
            cid: "profile-cid",
            revision: EventRevision);
        var ownerSnapshot = Visible(owner, lastAppliedRevision: NewerRevision);

        var decision = RecordMutationTransitionPlanner.Plan(Input(envelope, ownerSnapshot));

        Assert.Equal(RecordMutationPlanningDecisionKind.ValidatedNoOp, decision.Kind);
        Assert.Equal(ValidatedNoOpReason.RepositoryRevisionAlreadyApplied, decision.ValidatedNoOp!.Reason);
    }

    [Fact]
    public void EqualRevisionAcrossDifferentRecordKeysIsAllowed()
    {
        var owner = Account("owner-equal-revision");
        var envelope = Envelope(owner, DurableRecordKind.ActorProfile, DurableRecordAction.Create, cid: "profile-cid");
        var ownerSnapshot = Visible(owner, lastAppliedRevision: EventRevision);

        var commit = AssertCommit(RecordMutationTransitionPlanner.Plan(Input(envelope, ownerSnapshot)));

        Assert.Equal(EventRevision, Assert.Single(commit.AccountStates).LastAppliedRevision);
    }

    [Fact]
    public void SameRecordRevisionIsAValidatedNoOpEvenDuringHistoricalReplay()
    {
        var owner = Account("owner-record-stale");
        var current = Record(owner, DurableRecordKind.FeedPost, "rkey", NewerRevision, cid: "old-cid");
        var envelope = Envelope(
            owner,
            DurableRecordKind.FeedPost,
            DurableRecordAction.Update,
            cid: "new-cid",
            isLive: false,
            revision: EventRevision);

        var decision = RecordMutationTransitionPlanner.Plan(Input(envelope, Reconciling(owner), current));

        Assert.Equal(RecordMutationPlanningDecisionKind.ValidatedNoOp, decision.Kind);
        Assert.Equal(ValidatedNoOpReason.RecordRevisionAlreadyObserved, decision.ValidatedNoOp!.Reason);
    }

    [Fact]
    public void SameRecordRevisionWithDifferentMetadataIsQuarantined()
    {
        var owner = Account("owner-record-conflict");
        var current = Record(owner, DurableRecordKind.FeedPost, "rkey", EventRevision, cid: "old-cid");
        var envelope = Envelope(
            owner,
            DurableRecordKind.FeedPost,
            DurableRecordAction.Update,
            cid: "different-cid",
            isLive: false,
            revision: EventRevision);

        var decision = RecordMutationTransitionPlanner.Plan(Input(envelope, Reconciling(owner), current));

        Assert.Equal(RecordMutationPlanningDecisionKind.Quarantine, decision.Kind);
        Assert.Equal("conflicting-record-revision", decision.Quarantine!.Code);
    }

    [Fact]
    public void LiveMutationBeforeRepositorySyncIsQuarantined()
    {
        var owner = Account("owner-before-sync");
        var envelope = Envelope(owner, DurableRecordKind.FeedPost, DurableRecordAction.Create, cid: "cid");

        var decision = RecordMutationTransitionPlanner.Plan(Input(envelope, Reconciling(owner)));

        Assert.Equal(RecordMutationPlanningDecisionKind.Quarantine, decision.Kind);
        Assert.Equal("live-before-repo-sync", decision.Quarantine!.Code);
    }

    [Fact]
    public void HistoricalFollowRetargetUpdatesPairMultiplicityAndDistinctStocksWithoutPublishing()
    {
        var owner = Account("owner-historical-follow");
        var oldTarget = Account("old-target");
        var newTarget = Account("new-target");
        var current = Record(
            owner,
            DurableRecordKind.GraphFollow,
            "rkey",
            SyncRevision,
            cid: "old-follow-cid",
            target: oldTarget);
        var envelope = Envelope(
            owner,
            DurableRecordKind.GraphFollow,
            DurableRecordAction.Update,
            cid: "new-follow-cid",
            target: newTarget,
            isLive: false);
        var input = Input(
            envelope,
            Reconciling(owner, following: 1),
            current,
            affected:
            [
                AffectedAccountPlanningSnapshot.Admitted(Reconciling(oldTarget, followers: 1)),
                AffectedAccountPlanningSnapshot.Admitted(Reconciling(newTarget)),
            ],
            pairs:
            [
                new FollowPairPlanningSnapshot(owner, oldTarget, 1),
                new FollowPairPlanningSnapshot(owner, newTarget, 0),
            ]);

        var commit = AssertCommit(RecordMutationTransitionPlanner.Plan(input));

        Assert.Equal(3, commit.AccountStates.Count);
        Assert.True(commit.AccountStates.Select(static value => value.AccountKey).SequenceEqual(
            commit.AccountStates.Select(static value => value.AccountKey).Order()));
        Assert.Equal(1, commit.AccountStates.Single(value => value.AccountKey == owner).CurrentFollowingCount);
        Assert.Equal(0, commit.AccountStates.Single(value => value.AccountKey == oldTarget).CurrentFollowerCount);
        Assert.Equal(1, commit.AccountStates.Single(value => value.AccountKey == newTarget).CurrentFollowerCount);
        Assert.Null(commit.AccountStates.Single(value => value.AccountKey == owner).LastAppliedRevision);
        Assert.Equal(0, commit.FollowPairs.Single(value => value.TargetAccountKey == oldTarget).Multiplicity);
        Assert.Equal(1, commit.FollowPairs.Single(value => value.TargetAccountKey == newTarget).Multiplicity);
        Assert.Empty(commit.Activity);
        Assert.Empty(commit.Projections);
        Assert.Equal(
            new[] { oldTarget, newTarget }.Order(),
            commit.ReconciliationDependencies.Select(static value => value.AffectedAccountKey));
    }

    [Fact]
    public void HistoricalFollowChangeRemovesPreviouslyVisibleAffectedTargets()
    {
        var owner = Account("historical-removal-owner");
        var oldTarget = Account("historical-removal-old");
        var newTarget = Account("historical-removal-new");
        var current = Record(
            owner,
            DurableRecordKind.GraphFollow,
            "rkey",
            SyncRevision,
            cid: "old-follow-cid",
            target: oldTarget);
        var envelope = Envelope(
            owner,
            DurableRecordKind.GraphFollow,
            DurableRecordAction.Update,
            cid: "new-follow-cid",
            target: newTarget,
            isLive: false);
        var input = Input(
            envelope,
            Reconciling(owner, following: 1),
            current,
            affected:
            [
                AffectedAccountPlanningSnapshot.Admitted(Visible(oldTarget, followers: 1)),
                AffectedAccountPlanningSnapshot.Admitted(Visible(newTarget)),
            ],
            pairs:
            [
                new FollowPairPlanningSnapshot(owner, oldTarget, 1),
                new FollowPairPlanningSnapshot(owner, newTarget, 0),
            ]);

        var commit = AssertCommit(RecordMutationTransitionPlanner.Plan(input));

        Assert.Equal(2, commit.Projections.Count);
        Assert.All(commit.Projections, projection =>
        {
            Assert.Equal(ProjectionOperation.Remove, projection.Operation);
            Assert.True(projection.IsComplete);
            Assert.Equal(Minute, projection.ProjectionCutMinuteUtc);
        });
        Assert.Equal(
            new[] { oldTarget, newTarget }.Order(),
            commit.Projections.Select(static value => value.AccountKey));
    }

    [Fact]
    public void MultipleFollowRecordsPreserveDistinctFollowingAndFollowerCounts()
    {
        var owner = Account("owner-follow-multiplicity");
        var target = Account("target-follow-multiplicity");
        var current = Record(
            owner,
            DurableRecordKind.GraphFollow,
            "rkey",
            SyncRevision,
            cid: "follow-cid",
            target: target);
        var envelope = Envelope(owner, DurableRecordKind.GraphFollow, DurableRecordAction.Delete);
        var input = Input(
            envelope,
            Visible(owner, following: 1),
            current,
            pairs: [new FollowPairPlanningSnapshot(owner, target, 2)]);

        var commit = AssertCommit(RecordMutationTransitionPlanner.Plan(input));

        Assert.Equal(1, Assert.Single(commit.FollowPairs).Multiplicity);
        Assert.Equal(1, Assert.Single(commit.AccountStates).CurrentFollowingCount);
        Assert.DoesNotContain(commit.AccountStates, value => value.AccountKey == target);
    }

    [Fact]
    public void UnadmittedFollowTargetChangesOnlyTheSourceDistinctCount()
    {
        var owner = Account("owner-unadmitted-target");
        var target = Account("unadmitted-target");
        var envelope = Envelope(
            owner,
            DurableRecordKind.GraphFollow,
            DurableRecordAction.Create,
            cid: "follow-cid",
            target: target);
        var input = Input(
            envelope,
            Visible(owner),
            affected: [AffectedAccountPlanningSnapshot.NotAdmitted(target)],
            pairs: [new FollowPairPlanningSnapshot(owner, target, 0)]);

        var commit = AssertCommit(RecordMutationTransitionPlanner.Plan(input));

        Assert.Equal(1, Assert.Single(commit.AccountStates).CurrentFollowingCount);
        Assert.Equal(1, Assert.Single(commit.FollowPairs).Multiplicity);
        Assert.Single(commit.Projections);
    }

    [Fact]
    public void LiveDirectReplyAddsReceivedEngagementToAdmittedTarget()
    {
        var owner = Account("reply-owner");
        var target = Account("reply-target");
        var envelope = Envelope(
            owner,
            DurableRecordKind.FeedPost,
            DurableRecordAction.Create,
            cid: "reply-cid",
            target: target,
            directReply: true);
        var input = Input(
            envelope,
            Visible(owner),
            affected: [AffectedAccountPlanningSnapshot.Admitted(Visible(target))]);

        var commit = AssertCommit(RecordMutationTransitionPlanner.Plan(input));

        Assert.Equal(2, commit.AccountStates.Count);
        Assert.Equal(2, commit.Activity.Count);
        var ownerActivity = commit.Activity.Single(value => value.AccountKey == owner);
        var targetActivity = commit.Activity.Single(value => value.AccountKey == target);
        Assert.Equal(1, ownerActivity.PostCreates);
        Assert.Equal(1, targetActivity.ReceivedEngagementCreates);
        Assert.Equal(0, targetActivity.RecordCreates);
        Assert.Equal(1, commit.Projections.Single(value => value.AccountKey == target).ReceivedEngagementCreates30Days);
        Assert.Equal(0, commit.Projections.Single(value => value.AccountKey == target).CurrentPostCount);
    }

    [Fact]
    public void ExactAggregateIsCombinedWithTheCurrentMinuteAndKeepsItsFirstExpiry()
    {
        var owner = Account("owner-window-boundaries");
        var aggregate = Aggregate(
            owner,
            Minute,
            creates: new ActivityRollingCounts(1, 6, 10),
            engagementThirtyDays: 7,
            nextExpiry: Minute + 1);
        var envelope = Envelope(owner, DurableRecordKind.ActorProfile, DurableRecordAction.Update, cid: "profile-cid");

        var projection = Assert.Single(AssertCommit(
            RecordMutationTransitionPlanner.Plan(Input(envelope, Visible(owner, aggregate: aggregate)))).Projections);

        Assert.Equal(1, projection.CreatedRecordCount1Day);
        Assert.Equal(6, projection.CreatedRecordCount7Days);
        Assert.Equal(10, projection.CreatedRecordCount30Days);
        Assert.Equal(1, projection.UpdatedRecordCount1Day);
        Assert.Equal(7, projection.ReceivedEngagementCreates30Days);
        Assert.Equal(Minute + 1, projection.NextRecalculationMinuteUtc);
    }

    [Theory]
    [InlineData(10L, 1L, 1L, 1L, 1_440L)]
    [InlineData(2_000L, 0L, 1L, 1L, 10_080L)]
    [InlineData(12_000L, 0L, 0L, 1L, 43_200L)]
    [InlineData(43_200L, 0L, 0L, 0L, null)]
    public void LateRedeliveryAdvancesAtMonotonicCutAndOnlyChangesWindowsThatStillContainIt(
        long cutLagMinutes,
        long expectedOneDay,
        long expectedSevenDays,
        long expectedThirtyDays,
        long? expectedExpiryOffset)
    {
        var owner = Account($"owner-late-{cutLagMinutes}");
        var envelope = Envelope(owner, DurableRecordKind.FeedPost, DurableRecordAction.Create, cid: "post-cid");
        var cut = Minute + cutLagMinutes;
        var snapshot = Visible(owner, desiredCut: cut, aggregateCut: cut);

        var commit = AssertCommit(RecordMutationTransitionPlanner.Plan(Input(envelope, snapshot)));
        var projection = Assert.Single(commit.Projections);

        Assert.Equal(cut, projection.ProjectionCutMinuteUtc);
        Assert.Equal(expectedOneDay, projection.CreatedRecordCount1Day);
        Assert.Equal(expectedSevenDays, projection.CreatedRecordCount7Days);
        Assert.Equal(expectedThirtyDays, projection.CreatedRecordCount30Days);
        Assert.Equal(expectedOneDay, projection.PostCreates1Day);
        Assert.Equal(expectedSevenDays, projection.PostCreates7Days);
        Assert.Equal(expectedThirtyDays, projection.PostCreates30Days);
        Assert.Equal(1, projection.CurrentPostCount);
        Assert.Equal(
            expectedExpiryOffset is { } offset ? Minute + offset : null,
            projection.NextRecalculationMinuteUtc);
        Assert.Equal(Minute, Assert.Single(commit.Activity).MinuteUtc);
        var state = Assert.Single(commit.AccountStates);
        Assert.Equal(1, state.CurrentPostCount);
        Assert.Equal(EventRevision, state.LastAppliedRevision);
    }

    [Fact]
    public void StaleAggregateCutIsAnExplicitRetryInsteadOfRollingProjectionBack()
    {
        var owner = Account("owner-aggregate-cut-race");
        var envelope = Envelope(owner, DurableRecordKind.ActorProfile, DurableRecordAction.Update, cid: "profile-cid");
        var snapshot = Visible(owner, desiredCut: Minute + 2, aggregateCut: Minute + 1);

        var decision = RecordMutationTransitionPlanner.Plan(Input(envelope, snapshot));

        Assert.Equal(RecordMutationPlanningDecisionKind.Retry, decision.Kind);
        Assert.Equal(RecordMutationRetryReason.ActivityAggregateDoesNotMatchProjectionCut, decision.RetryReason);
    }

    [Fact]
    public void VisibleAccountRequiresCurrentDesiredProjectionAndExactActivityAggregate()
    {
        var owner = Account("owner-missing-projection-evidence");
        var envelope = Envelope(owner, DurableRecordKind.ActorProfile, DurableRecordAction.Update, cid: "profile-cid");
        var state = State(owner, synchronized: true);

        var missingDesired = RecordMutationTransitionPlanner.Plan(Input(envelope, new AccountPlanningSnapshot(state)));
        Assert.Equal(RecordMutationRetryReason.DesiredProjectionRequired, missingDesired.RetryReason);

        var missingAggregate = RecordMutationTransitionPlanner.Plan(Input(
            envelope,
            new AccountPlanningSnapshot(state, Desired(owner, state.StateVersion, Minute - 1))));
        Assert.Equal(RecordMutationRetryReason.ActivityAggregateRequired, missingAggregate.RetryReason);
    }

    [Fact]
    public void FollowTransitionRequiresExactPairAndCorpusAdmissionEvidence()
    {
        var owner = Account("owner-missing-follow-evidence");
        var target = Account("target-missing-follow-evidence");
        var envelope = Envelope(
            owner,
            DurableRecordKind.GraphFollow,
            DurableRecordAction.Create,
            cid: "follow-cid",
            target: target);

        var missingPair = RecordMutationTransitionPlanner.Plan(Input(envelope, Visible(owner)));
        Assert.Equal(RecordMutationRetryReason.FollowPairEvidenceRequired, missingPair.RetryReason);

        var missingAdmission = RecordMutationTransitionPlanner.Plan(Input(
            envelope,
            Visible(owner),
            pairs: [new FollowPairPlanningSnapshot(owner, target, 0)]));
        Assert.Equal(RecordMutationRetryReason.AccountEvidenceRequired, missingAdmission.RetryReason);
    }

    [Fact]
    public void NewerDeleteAfterFollowTombstoneIsQuarantinedWithoutInventingTargetPair()
    {
        var owner = Account("owner-follow-tombstone");
        var current = Record(
            owner,
            DurableRecordKind.GraphFollow,
            "rkey",
            SyncRevision,
            isDeleted: true);
        var envelope = Envelope(
            owner,
            DurableRecordKind.GraphFollow,
            DurableRecordAction.Delete,
            isLive: false);

        var decision = RecordMutationTransitionPlanner.Plan(Input(envelope, Reconciling(owner), current));

        Assert.Equal(RecordMutationPlanningDecisionKind.Quarantine, decision.Kind);
        Assert.Equal("missing-prior-record", decision.Quarantine!.Code);
    }

    [Fact]
    public void CompletedReservationDoesNotPlanASecondTransition()
    {
        var owner = Account("owner-completed-delivery");
        var envelope = Envelope(owner, DurableRecordKind.FeedPost, DurableRecordAction.Create, cid: "cid");
        var reservation = Reservation(envelope, DurableDeliveryOutcome.Applied);
        var input = new RecordMutationPlanningInput(reservation, envelope, Visible(owner));

        var decision = RecordMutationTransitionPlanner.Plan(input);

        Assert.Equal(RecordMutationPlanningDecisionKind.DeliveryAlreadyCompleted, decision.Kind);
        Assert.Null(decision.Commit);
    }

    [Fact]
    public void ActivityAggregateEvidenceIsVersionGenerationAndCutFenced()
    {
        var owner = Account("owner-activity-fence");
        var state = State(owner, synchronized: true);
        var wrongVersion = new ActivityWindowAggregateSnapshot(
            owner,
            state.StateVersion + 1,
            state.RepositoryGeneration,
            Minute,
            default,
            default,
            default,
            default,
            receivedEngagementCreatesThirtyDays: 0,
            nextExpiryMinuteUtc: null);

        Assert.Throws<ArgumentException>(() => new AccountPlanningSnapshot(state, activityAggregate: wrongVersion));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ActivityWindowAggregateSnapshot(
            owner,
            state.StateVersion,
            state.RepositoryGeneration,
            Minute,
            default,
            default,
            default,
            default,
            receivedEngagementCreatesThirtyDays: 0,
            nextExpiryMinuteUtc: Minute));
    }

    private static DurableIngestionCommit AssertCommit(RecordMutationPlanningDecision decision)
    {
        Assert.Equal(RecordMutationPlanningDecisionKind.Commit, decision.Kind);
        return Assert.IsType<DurableIngestionCommit>(decision.Commit);
    }

    private static RecordMutationPlanningInput Input(
        DurableEventEnvelope envelope,
        AccountPlanningSnapshot owner,
        RecordStateSnapshot? current = null,
        IEnumerable<AffectedAccountPlanningSnapshot>? affected = null,
        IEnumerable<FollowPairPlanningSnapshot>? pairs = null)
        => new(Reservation(envelope), envelope, owner, current, affected, pairs);

    private static DurableDeliveryReservation Reservation(
        DurableEventEnvelope envelope,
        DurableDeliveryOutcome outcome = DurableDeliveryOutcome.Pending)
        => new(
            envelope.SourceInstanceId,
            envelope.TapDeliveryId,
            envelope.DeliveryDigest,
            envelope.ObservedAtMinuteUtc,
            outcome);

    private static DurableEventEnvelope Envelope(
        AccountKey owner,
        DurableRecordKind collection,
        DurableRecordAction action,
        string? cid = null,
        AccountKey? target = null,
        bool directReply = false,
        bool isLive = true,
        string revision = EventRevision)
        => new(
            Source,
            17,
            Digest('a'),
            Digest('b'),
            owner,
            repositoryGeneration: 3,
            DurableEventKind.RecordMutation,
            Minute,
            revision,
            collection,
            action,
            recordKey: "rkey",
            cid,
            target,
            directReply,
            isLive);

    private static AccountPlanningSnapshot Visible(
        AccountKey account,
        long posts = 0,
        long following = 0,
        long followers = 0,
        string lastAppliedRevision = SyncRevision,
        long desiredCut = Minute - 1,
        long? aggregateCut = null,
        ActivityWindowAggregateSnapshot? aggregate = null)
    {
        var state = State(
            account,
            synchronized: true,
            posts,
            following,
            followers,
            lastAppliedRevision: lastAppliedRevision);
        return new AccountPlanningSnapshot(
            state,
            Desired(account, state.StateVersion, desiredCut),
            aggregate ?? Aggregate(
                account,
                aggregateCut ?? Math.Max(Minute, desiredCut),
                accountStateVersion: state.StateVersion,
                repositoryGeneration: state.RepositoryGeneration));
    }

    private static AccountPlanningSnapshot Reconciling(
        AccountKey account,
        long posts = 0,
        long following = 0,
        long followers = 0)
        => new(State(account, synchronized: false, posts, following, followers));

    private static AccountStateSnapshot State(
        AccountKey account,
        bool synchronized,
        long posts = 0,
        long following = 0,
        long followers = 0,
        string? lastAppliedRevision = null)
        => new(
            account,
            stateVersion: 5,
            DurableAccountLifecycle.Active,
            repositoryGeneration: 3,
            synchronized ? SyncRevision : null,
            synchronized,
            lastActivityMinuteUtc: Minute - 10,
            posts,
            following,
            followers,
            lastAppliedRevision);

    private static ProjectionSnapshot Desired(AccountKey account, long version, long cut)
        => new(
            account,
            version,
            ProjectionOperation.Upsert,
            isComplete: true,
            cut,
            nextRecalculationMinuteUtc: null,
            lastActivityMinuteUtc: Minute - 10,
            createdRecordCount1Day: 0,
            createdRecordCount7Days: 0,
            createdRecordCount30Days: 0,
            updatedRecordCount1Day: 0,
            updatedRecordCount7Days: 0,
            updatedRecordCount30Days: 0,
            deletedRecordCount1Day: 0,
            deletedRecordCount7Days: 0,
            deletedRecordCount30Days: 0,
            currentPostCount: 0,
            currentFollowingCount: 0,
            currentFollowerCount: 0,
            postCreates1Day: 0,
            postCreates7Days: 0,
            postCreates30Days: 0,
            receivedEngagementCreates30Days: 0);

    private static RecordStateSnapshot Record(
        AccountKey owner,
        DurableRecordKind collection,
        string recordKey,
        string revision,
        bool isDeleted = false,
        string? cid = null,
        AccountKey? target = null,
        bool directReply = false)
        => new(owner, 3, collection, recordKey, revision, isDeleted, cid, target, directReply);

    private static ActivityWindowAggregateSnapshot Aggregate(
        AccountKey account,
        long cut,
        ActivityRollingCounts creates = default,
        ActivityRollingCounts updates = default,
        ActivityRollingCounts deletes = default,
        ActivityRollingCounts posts = default,
        long engagementThirtyDays = 0,
        long? nextExpiry = null,
        long accountStateVersion = 5,
        long repositoryGeneration = 3)
        => new(
            account,
            accountStateVersion,
            repositoryGeneration,
            cut,
            creates,
            updates,
            deletes,
            posts,
            engagementThirtyDays,
            nextExpiry);

    private static AccountKey Account(string suffix) => AccountKey.FromDid($"did:plc:{suffix}");

    private static string Digest(char value) => new(value, 64);
}
