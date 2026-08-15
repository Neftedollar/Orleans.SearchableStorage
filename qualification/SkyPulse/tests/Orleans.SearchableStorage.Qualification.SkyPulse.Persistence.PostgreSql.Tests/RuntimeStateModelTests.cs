using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class RuntimeStateModelTests
{
    [Fact]
    public void ReservationRequestRequiresExactSourceDigestAndMinute()
    {
        Assert.Throws<ArgumentException>(() => new DurableDeliveryReservationRequest(Guid.Empty, 1, Digest('a'), 1));
        Assert.Throws<ArgumentException>(() => new DurableDeliveryReservationRequest(Guid.NewGuid(), 1, "bad", 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DurableDeliveryReservationRequest(Guid.NewGuid(), 1, Digest('a'), -1));
    }

    [Fact]
    public void PendingReservationIsNotAcknowledgementSafeButCompletedDuplicateIs()
    {
        var pending = new DurableDeliveryReservation(Source, 1, Digest('a'), 10, DurableDeliveryOutcome.Pending);
        var applied = new DurableDeliveryReservation(Source, 1, Digest('a'), 10, DurableDeliveryOutcome.Applied);

        Assert.True(pending.IsPending);
        Assert.False(pending.AcknowledgementAllowed);
        Assert.False(applied.IsPending);
        Assert.True(applied.AcknowledgementAllowed);
    }

    [Fact]
    public void ValidatedNoOpResultIsAcknowledgementSafe()
    {
        var result = new DurableCommitResult(DurableCommitOutcome.ValidatedNoOp, acknowledgementAllowed: true);

        Assert.True(result.AcknowledgementAllowed);
        Assert.Throws<ArgumentException>(() =>
            new DurableCommitResult(DurableCommitOutcome.ValidatedNoOp, acknowledgementAllowed: false));
    }

    [Fact]
    public void ValidatedNoOpReasonMustMatchEnvelopeShape()
    {
        var record = DurableModelTests.RecordEnvelope(Account("actor"));
        var sync = new DurableEventEnvelope(
            Source,
            2,
            Digest('a'),
            Digest('b'),
            Account("actor"),
            0,
            DurableEventKind.RepositorySync,
            10,
            repositoryRevision: DurableModelTests.Revision);

        Assert.Equal(
            ValidatedNoOpReason.RecordRevisionAlreadyObserved,
            new DurableValidatedNoOp(record, ValidatedNoOpReason.RecordRevisionAlreadyObserved).Reason);
        Assert.Equal(
            ValidatedNoOpReason.RepositoryRevisionAlreadyApplied,
            new DurableValidatedNoOp(record, ValidatedNoOpReason.RepositoryRevisionAlreadyApplied).Reason);
        Assert.Equal(
            ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted,
            new DurableValidatedNoOp(sync, ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted).Reason);
        Assert.Throws<ArgumentException>(() => new DurableValidatedNoOp(sync, ValidatedNoOpReason.RecordRevisionAlreadyObserved));
        Assert.Throws<ArgumentException>(() => new DurableValidatedNoOp(sync, ValidatedNoOpReason.RepositoryRevisionAlreadyApplied));
        Assert.Throws<ArgumentException>(() => new DurableValidatedNoOp(record, ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted));
    }

    [Fact]
    public void StoreProducedAccountSnapshotRejectsImpossibleState()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new AccountStateSnapshot(
            Account("actor"),
            0,
            DurableAccountLifecycle.Active,
            0,
            null,
            false,
            0,
            0,
            0,
            0));
        Assert.Throws<ArgumentException>(() => new AccountStateSnapshot(
            Account("actor"),
            1,
            DurableAccountLifecycle.Active,
            0,
            null,
            true,
            0,
            0,
            0,
            0));
    }

    [Fact]
    public void SynchronizedAccountCarriesMonotonicRepositoryRevisionHighWater()
    {
        var snapshot = new AccountStateSnapshot(
            Account("actor-high-water"),
            1,
            DurableAccountLifecycle.Active,
            0,
            DurableModelTests.Revision,
            true,
            0,
            0,
            0,
            0);

        Assert.Equal(DurableModelTests.Revision, snapshot.LastAppliedRevision);
        Assert.Throws<ArgumentException>(() => new AccountStateSnapshot(
            Account("actor-invalid-high-water"),
            1,
            DurableAccountLifecycle.Active,
            0,
            "3jzfcijpj2z2b",
            true,
            0,
            0,
            0,
            0,
            "3jzfcijpj2z2a"));
    }

    [Fact]
    public void RecordSnapshotEnforcesCanonicalTombstoneShape()
    {
        Assert.Throws<ArgumentException>(() => new RecordStateSnapshot(
            Account("actor"),
            0,
            DurableRecordKind.FeedPost,
            "rkey",
            DurableModelTests.Revision,
            isDeleted: true,
            cid: "must-not-survive",
            targetAccountKey: null,
            isDirectReply: false));
    }

    [Fact]
    public void FollowSnapshotRequiresPositiveMultiplicity()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FollowPairSnapshot(Account("a"), Account("b"), 0));
    }

    [Fact]
    public void ActivityPageIsBoundedCanonicalAndGenerationScoped()
    {
        var account = Account("actor");
        var first = Bucket(account, 3, 10);
        var second = Bucket(account, 3, 11);
        var page = new ActivityMinuteBucketPage(account, 5, 3, 10, 20, 2, [first, second], hasMore: true);

        Assert.True(page.HasMore);
        Assert.Equal(11, page.NextAfterMinuteUtc);
        Assert.Throws<ArgumentException>(() => new ActivityMinuteBucketPage(
            account,
            5,
            3,
            10,
            20,
            2,
            [second, first],
            hasMore: false));
        Assert.Throws<ArgumentException>(() => new ActivityMinuteBucketPage(
            account,
            5,
            3,
            10,
            20,
            3,
            [first, second],
            hasMore: true));
        Assert.Throws<ArgumentException>(() => ActivityMinuteBucketPage.ValidateWindow(0, 43_200));
        Assert.Throws<ArgumentOutOfRangeException>(() => ActivityMinuteBucketPage.ValidatePageSize(1_001));
    }

    [Fact]
    public void ActivityAggregateIsFixedSizeMonotonicAndCutFenced()
    {
        var account = Account("aggregate-actor");
        var aggregate = new ActivityWindowAggregateSnapshot(
            account,
            accountStateVersion: 5,
            repositoryGeneration: 3,
            cutMinuteUtc: 20,
            recordCreates: new ActivityRollingCounts(1, 2, 3),
            recordUpdates: default,
            recordDeletes: default,
            postCreates: new ActivityRollingCounts(1, 1, 2),
            receivedEngagementCreatesThirtyDays: 4,
            nextExpiryMinuteUtc: 21);

        Assert.Equal(3, aggregate.RecordCreates.ThirtyDays);
        Assert.Equal(21, aggregate.NextExpiryMinuteUtc);
        Assert.Throws<ArgumentException>(() => new ActivityRollingCounts(2, 1, 3));
        Assert.Throws<ArgumentException>(() => new ActivityWindowAggregateSnapshot(
            account,
            5,
            3,
            20,
            new ActivityRollingCounts(1, 1, 1),
            default,
            default,
            new ActivityRollingCounts(1, 1, 2),
            0,
            21));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ActivityWindowAggregateSnapshot(
            account,
            5,
            3,
            20,
            default,
            default,
            default,
            default,
            0,
            20));
    }

    [Fact]
    public void ReconciliationPageIsBoundedUniqueAndCanonical()
    {
        var owner = Account("owner");
        var values = new[] { Account("a"), Account("b"), Account("c") }.Order().ToArray();
        var page = new ReconciliationDependencyPage(owner, 2, 2, values[..2], hasMore: true);

        Assert.True(page.HasMore);
        Assert.Equal(values[1], page.NextAfterAffectedAccountKey);
        Assert.Throws<ArgumentException>(() => new ReconciliationDependencyPage(owner, 2, 2, [values[0]], hasMore: true));
        Assert.Throws<ArgumentException>(() => new ReconciliationDependencyPage(owner, 2, 2, [values[1], values[0]], hasMore: false));
    }

    [Fact]
    public void CommitRequiresDependencyOwnerAtExactGeneration()
    {
        var actor = Account("actor");
        var affected = Account("affected");
        var envelope = DurableModelTests.RecordEnvelope(actor, generation: 3);
        var state = DurableModelTests.State(actor, expectedVersion: 0, generation: 3);
        var record = new RecordStateMutation(
            actor,
            3,
            DurableRecordKind.FeedPost,
            "rkey",
            DurableModelTests.Revision,
            isDeleted: false,
            cid: "cid");

        var commit = new DurableIngestionCommit(
            envelope,
            [state],
            records: [record],
            reconciliationDependencies:
            [new ReconciliationDependencyMutation(actor, 3, affected, ReconciliationDependencyAction.Add)]);
        Assert.Single(commit.ReconciliationDependencies);

        Assert.Throws<ArgumentException>(() => new DurableIngestionCommit(
            envelope,
            [state],
            records: [record],
            reconciliationDependencies:
            [new ReconciliationDependencyMutation(actor, 2, affected, ReconciliationDependencyAction.Add)]));
    }

    private static ActivityMinuteBucketSnapshot Bucket(AccountKey accountKey, long generation, long minute)
        => new(accountKey, generation, minute, 1, 0, 0, 1, 0);

    private static AccountKey Account(string suffix) => DurableModelTests.Account(suffix);

    private static string Digest(char value) => DurableModelTests.Digest(value);

    private static readonly Guid Source = DurableModelTests.SourceInstanceId;
}
