using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning.Tests;

public sealed class LifecycleTransitionPlannerTests
{
    private static readonly Guid Source = Guid.Parse("b6ae164d-ad5a-4504-a3fa-14de6800c793");
    private const long Minute = 30_000_000;
    private const string Revision = "3jzfcijpj2z2a";

    [Fact]
    public void ActiveLifecycleOpensBarrierAddsSelfDependencyAndRemovesVisibleProjection()
    {
        var account = Account("active-begin");
        var envelope = LifecycleEnvelope(account, DurableAccountLifecycle.Active, generation: 4);
        var state = State(account, generation: 4, synchronized: true);

        var decision = LifecycleTransitionPlanner.Plan(Input(
            envelope,
            state,
            Projection(account, state.StateVersion, ProjectionOperation.Upsert)));

        Assert.Equal(LifecycleStartDecisionKind.ImmediateCommit, decision.Kind);
        var commit = Assert.IsType<DurableIngestionCommit>(decision.ImmediateCommit);
        var replacement = Assert.Single(commit.AccountStates);
        Assert.False(replacement.SynchronizationComplete);
        Assert.Null(replacement.CompletedSyncRevision);
        Assert.Null(replacement.LastAppliedRevision);
        Assert.Equal(ProjectionOperation.Remove, Assert.Single(commit.Projections).Operation);
        Assert.True(Assert.Single(commit.Projections).IsComplete);
        var dependency = Assert.Single(commit.ReconciliationDependencies);
        Assert.Equal(account, dependency.OwnerAccountKey);
        Assert.Equal(account, dependency.AffectedAccountKey);
        Assert.Equal(ReconciliationDependencyAction.Add, dependency.Action);
    }

    [Fact]
    public void FirstActiveLifecycleCreatesClosedBarrierWithoutInventingRemoval()
    {
        var account = Account("first-active");
        var envelope = LifecycleEnvelope(account, DurableAccountLifecycle.Active, generation: 0);

        var decision = LifecycleTransitionPlanner.Plan(Input(envelope, account: null, desired: null));

        var commit = Assert.IsType<DurableIngestionCommit>(decision.ImmediateCommit);
        Assert.Equal(1, Assert.Single(commit.AccountStates).NextVersion);
        Assert.Empty(commit.Projections);
        Assert.Single(commit.ReconciliationDependencies);
    }

    [Theory]
    [InlineData(DurableAccountLifecycle.Deactivated)]
    [InlineData(DurableAccountLifecycle.TakenDown)]
    [InlineData(DurableAccountLifecycle.Suspended)]
    [InlineData(DurableAccountLifecycle.Deleted)]
    public void InactiveLifecycleStartsPagedPurgeAndCannotAcknowledgeAtStart(
        DurableAccountLifecycle lifecycle)
    {
        var account = Account($"inactive-{lifecycle}");
        var envelope = LifecycleEnvelope(account, lifecycle, generation: 5);
        var state = State(account, generation: 4, synchronized: true);

        var decision = LifecycleTransitionPlanner.Plan(Input(
            envelope,
            state,
            Projection(account, state.StateVersion, ProjectionOperation.Upsert)));

        Assert.Equal(LifecycleStartDecisionKind.StartPagedWork, decision.Kind);
        Assert.Equal(LifecyclePagedWorkKind.InactiveAccountPurge, decision.PagedWorkKind);
        Assert.Equal(lifecycle, decision.InitialAccountState!.Lifecycle);
        Assert.False(decision.InitialAccountState.SynchronizationComplete);
        Assert.Equal(ProjectionOperation.Remove, decision.InitialRemoval!.Operation);
        Assert.Null(decision.ImmediateCommit);
    }

    [Fact]
    public void RepositorySyncRequiresPagedDependencyDrainBeforeCompletion()
    {
        var account = Account("repo-sync");
        var envelope = RepositorySyncEnvelope(account, generation: 8);

        var decision = LifecycleTransitionPlanner.Plan(Input(
            envelope,
            State(account, generation: 8, synchronized: false),
            desired: null));

        Assert.Equal(LifecycleStartDecisionKind.StartPagedWork, decision.Kind);
        Assert.Equal(LifecyclePagedWorkKind.RepositorySynchronization, decision.PagedWorkKind);
        Assert.Null(decision.InitialAccountState);
        Assert.Null(decision.ImmediateCommit);
    }

    [Fact]
    public void CompletedRepositorySyncUsesCommitTimeValidatedNoOp()
    {
        var account = Account("completed-sync");
        var envelope = RepositorySyncEnvelope(account, generation: 2);

        var decision = LifecycleTransitionPlanner.Plan(Input(
            envelope,
            State(account, generation: 2, synchronized: true),
            desired: null));

        Assert.Equal(LifecycleStartDecisionKind.ValidatedNoOp, decision.Kind);
        Assert.Equal(
            ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted,
            decision.ValidatedNoOp!.Reason);
    }

    [Fact]
    public void LifecycleCannotSkipMoreThanOneRepositoryGeneration()
    {
        var account = Account("generation-skip");
        var envelope = LifecycleEnvelope(account, DurableAccountLifecycle.Deleted, generation: 4);

        var decision = LifecycleTransitionPlanner.Plan(Input(
            envelope,
            State(account, generation: 2, synchronized: true),
            desired: null));

        Assert.Equal(LifecycleStartDecisionKind.Retry, decision.Kind);
        Assert.Equal(LifecycleStartRetryReason.RepositoryGenerationMismatch, decision.RetryReason);
    }

    [Fact]
    public void CompletedDeliveryNeverStartsDurableWorkAgain()
    {
        var account = Account("completed-delivery");
        var envelope = RepositorySyncEnvelope(account, generation: 0);
        var reservation = Reservation(envelope, DurableDeliveryOutcome.Applied);

        var decision = LifecycleTransitionPlanner.Plan(new LifecycleStartPlanningInput(
            reservation,
            envelope,
            State(account, generation: 0, synchronized: false),
            desiredProjection: null));

        Assert.Equal(LifecycleStartDecisionKind.DeliveryAlreadyCompleted, decision.Kind);
    }

    [Fact]
    public void ReservationMismatchUsesOnlyTheFixedPrivacySafeReason()
    {
        var account = Account("reservation-mismatch");
        var envelope = RepositorySyncEnvelope(account, generation: 0);
        var mismatched = new DurableDeliveryReservation(
            envelope.SourceInstanceId,
            envelope.TapDeliveryId,
            Digest("different-delivery"),
            envelope.ObservedAtMinuteUtc,
            DurableDeliveryOutcome.Pending);

        var decision = LifecycleTransitionPlanner.Plan(new LifecycleStartPlanningInput(
            mismatched,
            envelope,
            State(account, generation: 0, synchronized: false),
            desiredProjection: null));

        Assert.Equal(LifecycleStartDecisionKind.Quarantine, decision.Kind);
        Assert.Equal(DurableQuarantineReason.ReservationMismatch, decision.Quarantine!.Reason);
    }

    [Fact]
    public void RepositorySyncForInactiveAccountUsesOnlyTheFixedPrivacySafeReason()
    {
        var account = Account("inactive-sync");
        var envelope = RepositorySyncEnvelope(account, generation: 3);
        var state = new AccountStateSnapshot(
            account,
            stateVersion: 4,
            DurableAccountLifecycle.Deactivated,
            repositoryGeneration: 3,
            completedSyncRevision: null,
            synchronizationComplete: false,
            lastActivityMinuteUtc: 0,
            currentPostCount: 0,
            currentFollowingCount: 0,
            currentFollowerCount: 0);

        var decision = LifecycleTransitionPlanner.Plan(Input(envelope, state, desired: null));

        Assert.Equal(LifecycleStartDecisionKind.Quarantine, decision.Kind);
        Assert.Equal(DurableQuarantineReason.InactiveRepositorySync, decision.Quarantine!.Reason);
    }

    private static LifecycleStartPlanningInput Input(
        DurableEventEnvelope envelope,
        AccountStateSnapshot? account,
        ProjectionSnapshot? desired)
        => new(Reservation(envelope), envelope, account, desired);

    private static DurableDeliveryReservation Reservation(
        DurableEventEnvelope envelope,
        DurableDeliveryOutcome outcome = DurableDeliveryOutcome.Pending)
        => new(
            envelope.SourceInstanceId,
            envelope.TapDeliveryId,
            envelope.DeliveryDigest,
            envelope.ObservedAtMinuteUtc,
            outcome);

    private static DurableEventEnvelope LifecycleEnvelope(
        AccountKey account,
        DurableAccountLifecycle lifecycle,
        long generation)
        => new(
            Source,
            tapDeliveryId: 41,
            Digest("delivery-lifecycle"),
            Digest($"semantic-{lifecycle}-{generation}"),
            account,
            generation,
            DurableEventKind.AccountLifecycle,
            Minute,
            lifecycle: lifecycle);

    private static DurableEventEnvelope RepositorySyncEnvelope(AccountKey account, long generation)
        => new(
            Source,
            tapDeliveryId: 42,
            Digest("delivery-sync"),
            Digest($"semantic-sync-{generation}"),
            account,
            generation,
            DurableEventKind.RepositorySync,
            Minute,
            repositoryRevision: Revision);

    private static AccountStateSnapshot State(
        AccountKey account,
        long generation,
        bool synchronized)
        => new(
            account,
            stateVersion: 7,
            DurableAccountLifecycle.Active,
            generation,
            synchronized ? Revision : null,
            synchronized,
            lastActivityMinuteUtc: Minute - 1,
            currentPostCount: 3,
            currentFollowingCount: 2,
            currentFollowerCount: 5,
            lastAppliedRevision: synchronized ? Revision : null);

    private static ProjectionSnapshot Projection(
        AccountKey account,
        long version,
        ProjectionOperation operation)
        => new(
            account,
            version,
            operation,
            isComplete: true,
            projectionCutMinuteUtc: Minute - 1,
            nextRecalculationMinuteUtc: null,
            lastActivityMinuteUtc: Minute - 1,
            createdRecordCount1Day: 1,
            createdRecordCount7Days: 2,
            createdRecordCount30Days: 3,
            updatedRecordCount1Day: 1,
            updatedRecordCount7Days: 2,
            updatedRecordCount30Days: 3,
            deletedRecordCount1Day: 0,
            deletedRecordCount7Days: 1,
            deletedRecordCount30Days: 2,
            currentPostCount: 3,
            currentFollowingCount: 2,
            currentFollowerCount: 5,
            postCreates1Day: 1,
            postCreates7Days: 1,
            postCreates30Days: 2,
            receivedEngagementCreates30Days: 4);

    private static AccountKey Account(string seed) => AccountKey.FromDid($"did:plc:{seed}");

    private static string Digest(string seed)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(seed))).ToLowerInvariant();
}
