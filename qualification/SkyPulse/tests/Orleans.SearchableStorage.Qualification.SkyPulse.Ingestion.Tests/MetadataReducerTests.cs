using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion.Tests;

public sealed class MetadataReducerTests
{
    private static readonly DateTimeOffset Cut = new(
        2026,
        8,
        14,
        12,
        0,
        0,
        TimeSpan.Zero);

    private static readonly AccountKey Actor = AccountKey.FromDid(TestEvents.ActorDid);
    private static readonly AccountKey Target = AccountKey.FromDid(TestEvents.TargetDid);
    private static readonly AccountKey OtherTarget = AccountKey.FromDid(TestEvents.OtherTargetDid);

    [Fact]
    public void BackfillChangesCurrentStockButNotLiveActivity()
    {
        var reducer = Reducer(Actor);
        var mutation = TestEvents.ParseRecord(TestEvents.Post(1, live: false), Cut.AddDays(-10));

        var result = reducer.Apply(mutation);
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevA);
        var snapshot = Snapshot(reducer, Actor, Cut);

        Assert.Equal(ReductionDisposition.Applied, result.Disposition);
        Assert.Equal(1, snapshot.CurrentPostCount);
        Assert.Equal(0, snapshot.LastActivityMinuteUtc);
        Assert.Equal(new RollingWindowCounts(0, 0, 0), snapshot.CreatedRecordCounts);
        Assert.Equal(new RollingWindowCounts(0, 0, 0), snapshot.PostCreateCounts);
    }

    [Fact]
    public void RedeliveryIsAppliedExactlyOnceEvenWhenTapIdAndObservationTimeChange()
    {
        var reducer = Reducer(Actor);
        var first = TestEvents.ParseRecord(TestEvents.Post(1, live: true), Cut.AddMinutes(-10));
        var redelivery = TestEvents.ParseRecord(TestEvents.Post(500, live: true), Cut.AddMinutes(-5));

        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(first).Disposition);
        Assert.Equal(ReductionDisposition.Duplicate, reducer.Apply(redelivery).Disposition);

        var snapshot = Snapshot(reducer, Actor, Cut);
        Assert.Equal(1, snapshot.CurrentPostCount);
        Assert.Equal(new RollingWindowCounts(1, 1, 1), snapshot.CreatedRecordCounts);
        Assert.Equal(Cut.AddMinutes(-10).ToUnixTimeSeconds() / 60, snapshot.LastActivityMinuteUtc);
    }

    [Fact]
    public void OlderLiveRevisionForAnotherRecordCannotRegressRepositoryState()
    {
        var reducer = Reducer(Actor);
        var newer = TestEvents.ParseRecord(
            TestEvents.Post(1, live: true, revision: TestEvents.RevC, recordKey: "newer"),
            Cut.AddMinutes(-2));
        var delayedOlder = TestEvents.ParseRecord(
            TestEvents.Post(2, live: true, revision: TestEvents.RevB, recordKey: "older"),
            Cut.AddMinutes(-1));

        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(newer).Disposition);
        Assert.Equal(ReductionDisposition.IgnoredStale, reducer.Apply(delayedOlder).Disposition);

        var snapshot = Snapshot(reducer, Actor, Cut);
        Assert.Equal(1, snapshot.CurrentPostCount);
        Assert.Equal(new RollingWindowCounts(1, 1, 1), snapshot.CreatedRecordCounts);
    }

    [Fact]
    public void BackfillFinalStateIsIndependentOfRevisionDeliveryOrder()
    {
        var low = TestEvents.ParseRecord(
            TestEvents.Follow(1, live: false, targetDid: TestEvents.TargetDid, revision: TestEvents.RevA),
            Cut);
        var high = TestEvents.ParseRecord(
            TestEvents.Follow(2, live: false, targetDid: TestEvents.OtherTargetDid, revision: TestEvents.RevB, action: "update"),
            Cut);

        var forward = Reducer(Actor, Target, OtherTarget);
        forward.Apply(low);
        forward.Apply(high);
        CompleteRepositorySync(forward, TestEvents.ActorDid, TestEvents.RevB);

        var reverse = Reducer(Actor, Target, OtherTarget);
        reverse.Apply(high);
        reverse.Apply(low);
        CompleteRepositorySync(reverse, TestEvents.ActorDid, TestEvents.RevB);

        Assert.Equal(Snapshot(forward, Actor, Cut), Snapshot(reverse, Actor, Cut));
        Assert.Equal(Snapshot(forward, Target, Cut), Snapshot(reverse, Target, Cut));
        Assert.Equal(Snapshot(forward, OtherTarget, Cut), Snapshot(reverse, OtherTarget, Cut));
        Assert.Equal(1, Snapshot(forward, Actor, Cut).CurrentFollowingCount);
        Assert.Equal(0, Snapshot(forward, Target, Cut).CurrentFollowerCount);
        Assert.Equal(1, Snapshot(forward, OtherTarget, Cut).CurrentFollowerCount);
    }

    [Fact]
    public void FollowUpdateAndDeleteUseTheStoredOldTarget()
    {
        var reducer = Reducer(Actor, Target, OtherTarget);
        Apply(
            reducer,
            TestEvents.Follow(1, live: false, targetDid: TestEvents.TargetDid, revision: TestEvents.RevA),
            Cut);
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevA);
        Apply(
            reducer,
            TestEvents.Follow(
                2,
                live: true,
                targetDid: TestEvents.OtherTargetDid,
                revision: TestEvents.RevB,
                action: "update"),
            Cut.AddMinutes(-2));

        Assert.Equal(1, Snapshot(reducer, Actor, Cut).CurrentFollowingCount);
        Assert.Equal(0, Snapshot(reducer, Target, Cut).CurrentFollowerCount);
        Assert.Equal(1, Snapshot(reducer, OtherTarget, Cut).CurrentFollowerCount);

        Apply(
            reducer,
            TestEvents.Delete(
                3,
                live: true,
                "app.bsky.graph.follow",
                "follow-1",
                TestEvents.RevC),
            Cut.AddMinutes(-1));

        Assert.Equal(0, Snapshot(reducer, Actor, Cut).CurrentFollowingCount);
        Assert.Equal(0, Snapshot(reducer, OtherTarget, Cut).CurrentFollowerCount);
    }

    [Fact]
    public void MissingOldMappingIsQuarantinedInsteadOfSilentlyCountingDelete()
    {
        var reducer = Reducer(Actor);
        var deletion = TestEvents.ParseRecord(
            TestEvents.Delete(
                1,
                live: true,
                "app.bsky.graph.follow",
                "missing",
                TestEvents.RevB),
            Cut);

        var decision = reducer.Apply(deletion);

        Assert.Equal(ReductionDisposition.Quarantined, decision.Disposition);
        Assert.Equal(QuarantineCode.MissingPriorRecord, decision.QuarantineCode);
        Assert.Equal(new RollingWindowCounts(0, 0, 0), Snapshot(reducer, Actor, Cut).DeletedRecordCounts);
    }

    [Fact]
    public void DuplicateSourceTargetFollowRecordsCountOneDistinctRelationship()
    {
        var reducer = Reducer(Actor, Target);
        Apply(
            reducer,
            TestEvents.Follow(1, live: false, recordKey: "follow-a", revision: TestEvents.RevA),
            Cut);
        Apply(
            reducer,
            TestEvents.Follow(2, live: false, recordKey: "follow-b", revision: TestEvents.RevB),
            Cut);
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevB);

        Assert.Equal(1, Snapshot(reducer, Actor, Cut).CurrentFollowingCount);
        Assert.Equal(1, Snapshot(reducer, Target, Cut).CurrentFollowerCount);

        Apply(
            reducer,
            TestEvents.Delete(
                3,
                live: true,
                "app.bsky.graph.follow",
                "follow-a",
                TestEvents.RevC),
            Cut);

        Assert.Equal(1, Snapshot(reducer, Actor, Cut).CurrentFollowingCount);
        Assert.Equal(1, Snapshot(reducer, Target, Cut).CurrentFollowerCount);

        Apply(
            reducer,
            TestEvents.Delete(
                4,
                live: true,
                "app.bsky.graph.follow",
                "follow-b",
                TestEvents.RevD),
            Cut);

        Assert.Equal(0, Snapshot(reducer, Actor, Cut).CurrentFollowingCount);
        Assert.Equal(0, Snapshot(reducer, Target, Cut).CurrentFollowerCount);
    }

    [Fact]
    public void OutsideCorpusActorCanAffectAnAdmittedTarget()
    {
        var reducer = Reducer(Target);
        Apply(reducer, TestEvents.Follow(1, live: true), Cut.AddMinutes(-4));
        Apply(reducer, TestEvents.Like(2, live: true), Cut.AddMinutes(-3));
        Apply(reducer, TestEvents.Repost(3, live: true), Cut.AddMinutes(-2));
        Apply(
            reducer,
            TestEvents.Post(
                4,
                live: true,
                recordKey: "reply-1",
                revision: TestEvents.RevD,
                replyTargetDid: TestEvents.TargetDid),
            Cut.AddMinutes(-1));

        Assert.False(reducer.TryGetSnapshot(Actor, Cut, out _));
        var target = Snapshot(reducer, Target, Cut);
        Assert.Equal(1, target.CurrentFollowerCount);
        Assert.Equal(3, target.ReceivedEngagementCreates30Days);
        Assert.Equal(0, target.LastActivityMinuteUtc);
    }

    [Fact]
    public void RollingWindowsAreEvaluatedAtAnExactFixedCut()
    {
        var reducer = Reducer(Actor, Target);

        Apply(reducer, TestEvents.Post(1, live: true, revision: TestEvents.RevA, recordKey: "recent-post"), Cut.AddHours(-1));
        Apply(reducer, TestEvents.Like(2, live: true, revision: TestEvents.RevB, recordKey: "week-like"), Cut.AddDays(-2));
        Apply(reducer, TestEvents.Profile(3, live: false, revision: TestEvents.RevC), Cut.AddDays(-20));
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevC);
        Apply(
            reducer,
            TestEvents.Profile(4, live: true, revision: TestEvents.RevD, action: "update"),
            Cut.AddDays(-8));
        Apply(
            reducer,
            TestEvents.Delete(
                5,
                live: true,
                "app.bsky.actor.profile",
                "self",
                TestEvents.RevE),
            Cut.AddDays(-29));
        Apply(
            reducer,
            TestEvents.Post(6, live: true, revision: TestEvents.RevF, recordKey: "boundary-post"),
            Cut.AddDays(-30));

        var actor = Snapshot(reducer, Actor, Cut);
        Assert.Equal(new RollingWindowCounts(1, 2, 2), actor.CreatedRecordCounts);
        Assert.Equal(new RollingWindowCounts(0, 0, 1), actor.UpdatedRecordCounts);
        Assert.Equal(new RollingWindowCounts(0, 0, 1), actor.DeletedRecordCounts);
        Assert.Equal(new RollingWindowCounts(1, 1, 1), actor.PostCreateCounts);
        Assert.Equal(Cut.AddHours(-1).ToUnixTimeSeconds() / 60, actor.LastActivityMinuteUtc);
        Assert.Equal(1, Snapshot(reducer, Target, Cut).ReceivedEngagementCreates30Days);
    }

    [Fact]
    public void InactiveAccountProducesPurgeAndRepairsItsOutgoingFollowStock()
    {
        var reducer = Reducer(Actor, Target);
        Apply(reducer, TestEvents.Post(1, live: false), Cut);
        Apply(reducer, TestEvents.Follow(2, live: false, revision: TestEvents.RevB), Cut);

        var inactive = TestEvents.ParseIdentity(
            TestEvents.Identity(3, "deactivated", isActive: false),
            Cut);
        var first = reducer.Apply(inactive);
        var repeated = reducer.Apply(inactive);

        Assert.Equal(ReductionDisposition.Applied, first.Disposition);
        Assert.Contains(first.Instructions, instruction => instruction == new PurgeProjectionInstruction(Actor));
        Assert.Contains(first.Instructions, instruction => instruction == new RefreshProjectionInstruction(Target));
        Assert.Equal(ReductionDisposition.Duplicate, repeated.Disposition);
        Assert.False(reducer.TryGetSnapshot(Actor, Cut, out _));
        Assert.Equal(0, Snapshot(reducer, Target, Cut).CurrentFollowerCount);
        Assert.Equal(0, reducer.CurrentRecordCount);
    }

    [Fact]
    public void ReactivationCanBackfillTheSameCurrentRecordInANewLifecycleEpoch()
    {
        var reducer = Reducer(Actor);
        var original = TestEvents.ParseRecord(TestEvents.Post(1, live: false), Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(original).Disposition);
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevA);

        var inactive = TestEvents.ParseIdentity(
            TestEvents.Identity(2, "deactivated", isActive: false),
            Cut);
        var active = TestEvents.ParseIdentity(
            TestEvents.Identity(3, "active", isActive: true),
            Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(inactive).Disposition);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(active).Disposition);

        var sameRepositoryRecord = TestEvents.ParseRecord(TestEvents.Post(4, live: false), Cut);
        Assert.Equal(original.SemanticKey, sameRepositoryRecord.SemanticKey);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(sameRepositoryRecord).Disposition);
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevA);
        Assert.Equal(1, Snapshot(reducer, Actor, Cut).CurrentPostCount);
    }

    [Fact]
    public void ActiveLifecycleStartsAClosedReconciliationBarrier()
    {
        var reducer = new MetadataReducer(accountKey => accountKey == Actor);
        var active = TestEvents.ParseIdentity(
            TestEvents.Identity(1, "active", isActive: true),
            Cut);

        var activeDecision = reducer.Apply(active);
        var liveDecision = reducer.Apply(
            TestEvents.ParseRecord(TestEvents.Post(2, live: true), Cut));

        Assert.Equal(ReductionDisposition.Applied, activeDecision.Disposition);
        Assert.Contains(
            new BeginRepositoryReconciliationInstruction(Actor, RepositoryGeneration: 0),
            activeDecision.Instructions);
        Assert.Contains(
            new TrackReconciliationDependencyInstruction(Actor, 0, Actor),
            activeDecision.Instructions);
        Assert.Contains(new PurgeProjectionInstruction(Actor), activeDecision.Instructions);
        Assert.DoesNotContain(activeDecision.Instructions, static item => item is RefreshProjectionInstruction);
        Assert.False(reducer.TryGetSnapshot(Actor, Cut, out _));
        Assert.Equal(ReductionDisposition.Quarantined, liveDecision.Disposition);
        Assert.Equal(QuarantineCode.ReconciliationIncomplete, liveDecision.QuarantineCode);
        Assert.Equal(0, reducer.CurrentRecordCount);
    }

    [Fact]
    public void BackfillTracksOwnerAndTargetUntilRepositorySyncPublishesTheCycle()
    {
        var reducer = new MetadataReducer(accountKey => accountKey == Actor || accountKey == Target);
        ActivateAndCompleteRepository(reducer, TestEvents.TargetDid);
        var active = TestEvents.ParseIdentity(
            TestEvents.Identity(1, "active", isActive: true),
            Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(active).Disposition);

        var backfill = reducer.Apply(
            TestEvents.ParseRecord(TestEvents.Follow(2, live: false), Cut));

        Assert.Contains(
            new TrackReconciliationDependencyInstruction(Actor, 0, Target),
            backfill.Instructions);
        Assert.Contains(new PurgeProjectionInstruction(Target), backfill.Instructions);
        Assert.DoesNotContain(backfill.Instructions, static item => item is RefreshProjectionInstruction);
        Assert.False(reducer.TryGetSnapshot(Actor, Cut, out _));
        Assert.False(reducer.TryGetSnapshot(Target, Cut, out _));

        var completed = reducer.Apply(
            TestEvents.ParseRepositorySync(
                TestEvents.RepositorySync(3, TestEvents.RevA),
                Cut));

        Assert.Equal(ReductionDisposition.Applied, completed.Disposition);
        Assert.Contains(new ReconcileAccountInstruction(Actor, 0, TestEvents.RevA), completed.Instructions);
        Assert.Contains(new RefreshProjectionInstruction(Target), completed.Instructions);
        Assert.DoesNotContain(
            completed.Instructions,
            instruction => instruction == new RefreshProjectionInstruction(Actor));
        Assert.Equal(1, Snapshot(reducer, Actor, Cut).CurrentFollowingCount);
        Assert.Equal(1, Snapshot(reducer, Target, Cut).CurrentFollowerCount);
    }

    [Fact]
    public void RepositorySyncIdempotenceIsScopedToTheLifecycleGeneration()
    {
        var reducer = new MetadataReducer(accountKey => accountKey == Actor);
        var active = TestEvents.ParseIdentity(
            TestEvents.Identity(1, "active", isActive: true),
            Cut);
        var sync = TestEvents.ParseRepositorySync(
            TestEvents.RepositorySync(2, TestEvents.RevA),
            Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(active).Disposition);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(sync).Disposition);
        Assert.Equal(ReductionDisposition.Duplicate, reducer.Apply(sync).Disposition);

        var inactive = TestEvents.ParseIdentity(
            TestEvents.Identity(3, "deactivated", isActive: false),
            Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(inactive).Disposition);
        var late = reducer.Apply(sync);
        Assert.Equal(ReductionDisposition.Quarantined, late.Disposition);
        Assert.Equal(QuarantineCode.InactiveAccountMutation, late.QuarantineCode);

        var reactivated = TestEvents.ParseIdentity(
            TestEvents.Identity(4, "active", isActive: true),
            Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(reactivated).Disposition);
        var nextGeneration = reducer.Apply(sync);
        Assert.Equal(ReductionDisposition.Applied, nextGeneration.Disposition);
        Assert.Contains(
            new ReconcileAccountInstruction(Actor, 1, TestEvents.RevA),
            nextGeneration.Instructions);
        Assert.Equal(ReductionDisposition.Duplicate, reducer.Apply(sync).Disposition);
    }

    [Fact]
    public void InactiveLifecycleClearsPendingDependenciesAndPurgesTheOwner()
    {
        var reducer = new MetadataReducer(accountKey => accountKey == Actor || accountKey == Target);
        ActivateAndCompleteRepository(reducer, TestEvents.TargetDid);
        var active = TestEvents.ParseIdentity(
            TestEvents.Identity(1, "active", isActive: true),
            Cut);
        reducer.Apply(active);
        reducer.Apply(TestEvents.ParseRecord(TestEvents.Follow(2, live: false), Cut));

        var inactive = reducer.Apply(
            TestEvents.ParseIdentity(
                TestEvents.Identity(3, "deactivated", isActive: false),
                Cut));

        Assert.Contains(new PurgeProjectionInstruction(Actor), inactive.Instructions);
        Assert.Contains(
            new CancelRepositoryReconciliationInstruction(Actor, 0),
            inactive.Instructions);
        Assert.Contains(new RefreshProjectionInstruction(Target), inactive.Instructions);
        Assert.False(reducer.TryGetSnapshot(Actor, Cut, out _));
        Assert.Equal(0, Snapshot(reducer, Target, Cut).CurrentFollowerCount);
        Assert.Equal(0, reducer.CurrentRecordCount);
        Assert.Equal(
            ReductionDisposition.Quarantined,
            reducer.Apply(
                TestEvents.ParseRepositorySync(
                    TestEvents.RepositorySync(4, TestEvents.RevA),
                    Cut)).Disposition);
    }

    [Fact]
    public void RepositorySyncCannotCloseBeforeTheHighestBackfillRevision()
    {
        var reducer = new MetadataReducer(accountKey => accountKey == Actor);
        reducer.Apply(
            TestEvents.ParseIdentity(
                TestEvents.Identity(1, "active", isActive: true),
                Cut));
        reducer.Apply(
            TestEvents.ParseRecord(
                TestEvents.Post(2, live: false, revision: TestEvents.RevB),
                Cut));

        var early = reducer.Apply(
            TestEvents.ParseRepositorySync(
                TestEvents.RepositorySync(3, TestEvents.RevA),
                Cut));

        Assert.Equal(ReductionDisposition.Quarantined, early.Disposition);
        Assert.Equal(QuarantineCode.ReconciliationRevisionConflict, early.QuarantineCode);
        Assert.False(reducer.TryGetSnapshot(Actor, Cut, out _));
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevB);
        Assert.Equal(1, Snapshot(reducer, Actor, Cut).CurrentPostCount);
    }

    [Fact]
    public void HistoricalMutationAfterCompletionStartsANewClosedBarrier()
    {
        var reducer = Reducer(Actor);

        var backfill = reducer.Apply(
            TestEvents.ParseRecord(
                TestEvents.Post(1, live: false, revision: TestEvents.RevA),
                Cut));

        Assert.Contains(
            new BeginRepositoryReconciliationInstruction(Actor, 0),
            backfill.Instructions);
        Assert.Contains(new PurgeProjectionInstruction(Actor), backfill.Instructions);
        Assert.False(reducer.TryGetSnapshot(Actor, Cut, out _));
        CompleteRepositorySync(reducer, TestEvents.ActorDid, TestEvents.RevA);
        Assert.Equal(1, Snapshot(reducer, Actor, Cut).CurrentPostCount);
    }

    [Fact]
    public void SharedTargetRemainsBlockedUntilEveryAffectingRepositoryCompletes()
    {
        var reducer = new MetadataReducer(accountKey => accountKey == Target);
        ActivateAndCompleteRepository(reducer, TestEvents.TargetDid);
        reducer.Apply(
            TestEvents.ParseIdentity(
                TestEvents.Identity(1, "active", isActive: true, did: TestEvents.ActorDid),
                Cut));
        reducer.Apply(
            TestEvents.ParseIdentity(
                TestEvents.Identity(2, "active", isActive: true, did: TestEvents.OtherTargetDid),
                Cut));
        reducer.Apply(
            TestEvents.ParseRecord(
                TestEvents.Follow(
                    3,
                    live: false,
                    did: TestEvents.ActorDid,
                    recordKey: "follow-a"),
                Cut));
        reducer.Apply(
            TestEvents.ParseRecord(
                TestEvents.Follow(
                    4,
                    live: false,
                    did: TestEvents.OtherTargetDid,
                    recordKey: "follow-b"),
                Cut));

        var first = reducer.Apply(
            TestEvents.ParseRepositorySync(
                TestEvents.RepositorySync(5, TestEvents.RevA, TestEvents.ActorDid),
                Cut));

        Assert.DoesNotContain(
            first.Instructions,
            instruction => instruction == new RefreshProjectionInstruction(Target));
        Assert.False(reducer.TryGetSnapshot(Target, Cut, out _));

        var second = reducer.Apply(
            TestEvents.ParseRepositorySync(
                TestEvents.RepositorySync(6, TestEvents.RevA, TestEvents.OtherTargetDid),
                Cut));

        Assert.Contains(new RefreshProjectionInstruction(Target), second.Instructions);
        Assert.Equal(2, Snapshot(reducer, Target, Cut).CurrentFollowerCount);
    }

    private static MetadataReducer Reducer(params AccountKey[] admitted)
    {
        var admittedSet = admitted.ToHashSet();
        var reducer = new MetadataReducer(admittedSet.Contains);
        ActivateAndCompleteRepository(reducer, TestEvents.ActorDid);
        ActivateAndCompleteRepository(reducer, TestEvents.TargetDid);
        ActivateAndCompleteRepository(reducer, TestEvents.OtherTargetDid);
        return reducer;
    }

    private static void ActivateAndCompleteRepository(MetadataReducer reducer, string did)
    {
        var active = TestEvents.ParseIdentity(
            TestEvents.Identity(900, "active", isActive: true, did: did),
            Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(active).Disposition);
        CompleteRepositorySync(reducer, did, TestEvents.Rev0);
    }

    private static void CompleteRepositorySync(
        MetadataReducer reducer,
        string did,
        string revision)
    {
        var repositorySync = TestEvents.ParseRepositorySync(
            TestEvents.RepositorySync(901, revision, did),
            Cut);
        Assert.Equal(ReductionDisposition.Applied, reducer.Apply(repositorySync).Disposition);
    }

    private static void Apply(MetadataReducer reducer, string json, DateTimeOffset observedAtUtc)
    {
        var mutation = TestEvents.ParseRecord(json, observedAtUtc);
        var decision = reducer.Apply(mutation);
        Assert.Equal(ReductionDisposition.Applied, decision.Disposition);
    }

    private static AccountMetricsSnapshot Snapshot(
        MetadataReducer reducer,
        AccountKey accountKey,
        DateTimeOffset cut)
    {
        Assert.True(reducer.TryGetSnapshot(accountKey, cut, out var snapshot));
        return Assert.IsType<AccountMetricsSnapshot>(snapshot);
    }
}
