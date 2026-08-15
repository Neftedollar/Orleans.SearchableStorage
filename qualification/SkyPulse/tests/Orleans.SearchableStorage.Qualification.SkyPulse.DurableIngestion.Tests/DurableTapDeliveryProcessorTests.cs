using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;
using Orleans.SearchableStorage.Qualification.SkyPulse.Tap;
using Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion.Tests;

public sealed class DurableTapDeliveryProcessorTests
{
    private static readonly Guid Source = Guid.Parse("745213bb-8f50-43b6-9465-79d6130d1476");
    private static readonly DateTimeOffset ObservedAt = DateTimeOffset.FromUnixTimeSeconds(6_000 * 60);

    [Fact]
    public async Task RecoverableParserQuarantineReservesExactDigestAndPersistsOnlyClosedReason()
    {
        var backend = new FakeBackend(Source);
        var processor = Processor(backend);
        const string json = """
            {"id":41,"type":"identity","identity":{"did":"did:plc:parser-quarantine","is_active":true,"status":"deleted"}}
            """;
        var delivery = Delivery(json);

        var result = await processor.ProcessAsync(delivery, ObservedAt);

        Assert.True(result.AcknowledgementAllowed);
        Assert.Equal((ulong)41, result.DeliveryId);
        Assert.Equal(delivery.Sha256, Assert.Single(backend.Reservations).DeliveryDigest);
        var quarantine = Assert.Single(backend.Quarantines);
        Assert.Equal(DurableQuarantineReason.InvalidValue, quarantine.Reason);
        Assert.Equal("invalid-value", quarantine.Code);
        Assert.DoesNotContain("deleted", quarantine.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(json, quarantine.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MissingRecoverableDeliveryIdIsFatalAndNeverTouchesPostgreSql()
    {
        var backend = new FakeBackend(Source);
        var processor = Processor(backend);

        var exception = await Assert.ThrowsAsync<DurableTapProtocolException>(
            () => processor.ProcessAsync(Delivery("{not-json"), ObservedAt));

        Assert.Equal("delivery-id-unrecoverable", exception.Code);
        Assert.Empty(backend.Reservations);
        Assert.Empty(backend.Quarantines);
        Assert.Empty(backend.Commits);
    }

    [Fact]
    public async Task DeliveryDigestMismatchIsFatalAndNeverReservesTheClaimedFrame()
    {
        var backend = new FakeBackend(Source);
        var processor = Processor(backend);
        var delivery = new TapDelivery(
            """
            {"id":40,"type":"identity","identity":{"did":"did:plc:digest-mismatch","is_active":true,"status":"deleted"}}
            """,
            new string('0', 64));

        var exception = await Assert.ThrowsAsync<DurableTapProtocolException>(
            () => processor.ProcessAsync(delivery, ObservedAt));

        Assert.Equal("delivery-digest-mismatch", exception.Code);
        Assert.Empty(backend.Reservations);
    }

    [Fact]
    public async Task CompletedRedeliveryIsAcknowledgementSafeWithoutReplanning()
    {
        var backend = new FakeBackend(Source)
        {
            ReservationOutcome = DurableDeliveryOutcome.Quarantined,
        };
        var processor = Processor(backend);
        var delivery = Delivery("""
            {"id":42,"type":"identity","identity":{"did":"did:plc:duplicate","is_active":true,"status":"deleted"}}
            """);

        var first = await processor.ProcessAsync(delivery, ObservedAt);
        var second = await processor.ProcessAsync(delivery, ObservedAt.AddMinutes(10));

        Assert.True(first.AcknowledgementAllowed);
        Assert.True(second.AcknowledgementAllowed);
        Assert.Equal(2, backend.Reservations.Count);
        Assert.Empty(backend.Quarantines);
        Assert.Empty(backend.Commits);
    }

    [Fact]
    public async Task OptimisticConflictsReReadBoundedEvidenceAndNeverAllowAcknowledgement()
    {
        var account = AccountKey.FromDid("did:plc:ordinary-owner");
        var backend = FakeBackend.Visible(Source, account, ObservedAt.ToUnixTimeSeconds() / 60);
        backend.CommitOutcome = DurableCommitOutcome.OptimisticConflict;
        var processor = Processor(
            backend,
            new DurableTapProcessingOptions
            {
                MaximumPlanningAttempts = 2,
                LifecyclePageSize = 10,
            });
        var delivery = Delivery("""
            {"id":43,"type":"record","record":{"live":true,"did":"did:plc:ordinary-owner","rev":"3jzfcijpj2z2b","collection":"app.bsky.actor.profile","rkey":"self","action":"create","cid":"bafy-profile","metadata_status":"valid"}}
            """);

        var result = await processor.ProcessAsync(delivery, ObservedAt);

        Assert.Equal(DurableTapProcessingDisposition.RetryWithoutAcknowledgement, result.Disposition);
        Assert.Equal(2, backend.Commits.Count);
        Assert.Equal(2, backend.AggregateReadCount);
        Assert.All(backend.Commits, commit => Assert.Equal(delivery.Sha256, commit.Envelope.DeliveryDigest));
    }

    [Fact]
    public async Task MissingBootstrappedAdmittedTargetIsFatalInsteadOfHeadOfLineRetry()
    {
        var account = AccountKey.FromDid("did:plc:reply-owner");
        var backend = FakeBackend.Visible(Source, account, ObservedAt.ToUnixTimeSeconds() / 60);
        var processor = Processor(backend);
        var delivery = Delivery("""
            {"id":47,"type":"record","record":{"live":true,"did":"did:plc:reply-owner","rev":"3jzfcijpj2z2b","collection":"app.bsky.feed.post","rkey":"reply","action":"create","cid":"bafy-reply","metadata":{"reply_parent_uri":"at://did:plc:missing-target/app.bsky.feed.post/parent"},"metadata_status":"valid"}}
            """);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => processor.ProcessAsync(delivery, ObservedAt));

        Assert.Equal(
            "An admitted affected account lost its bootstrapped durable account state.",
            exception.Message);
        Assert.Single(backend.Reservations);
        Assert.Empty(backend.Commits);
    }

    [Fact]
    public async Task MissingBootstrappedAdmittedOwnerIsFatalAndNeverAcknowledged()
    {
        var backend = new FakeBackend(Source);
        var processor = Processor(backend);
        var delivery = Delivery("""
            {"id":48,"type":"identity","identity":{"did":"did:plc:missing-owner","is_active":false,"status":"deactivated"}}
            """);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => processor.ProcessAsync(delivery, ObservedAt));

        Assert.Equal(
            "An admitted event owner has no bootstrapped durable account state.",
            exception.Message);
        Assert.Single(backend.Reservations);
        Assert.Equal(0, backend.LifecycleStartCount);
    }

    [Fact]
    public async Task LifecyclePagesMustReachExplicitCompletedResultBeforeAcknowledgement()
    {
        var account = AccountKey.FromDid("did:plc:lifecycle-owner");
        var backend = FakeBackend.Visible(Source, account, ObservedAt.ToUnixTimeSeconds() / 60);
        backend.LifecycleStartResults.Enqueue(LifecycleAdvanceResult.Pending(
            LifecyclePagedWorkKind.InactiveAccountPurge,
            LifecycleWorkPhase.OutgoingFollows));
        backend.LifecycleAdvanceResults.Enqueue(LifecycleAdvanceResult.Pending(
            LifecyclePagedWorkKind.InactiveAccountPurge,
            LifecycleWorkPhase.OwnedRecords,
            processedRows: 10));
        backend.LifecycleAdvanceResults.Enqueue(LifecycleAdvanceResult.Completed(processedRows: 2));
        var processor = Processor(backend);
        var delivery = Delivery("""
            {"id":44,"type":"identity","identity":{"did":"did:plc:lifecycle-owner","is_active":false,"status":"deactivated"}}
            """);

        var result = await processor.ProcessAsync(delivery, ObservedAt);

        Assert.True(result.AcknowledgementAllowed);
        Assert.Equal(1, backend.LifecycleStartCount);
        Assert.Equal(2, backend.LifecycleAdvanceCount);
    }

    [Fact]
    public async Task SessionSendsAckOnlyAfterDurableDecision()
    {
        var sequence = new List<string>();
        var backend = new FakeBackend(Source, sequence);
        var processor = Processor(backend);
        var runner = new DurableTapSessionRunner(processor, new FixedTimeProvider(ObservedAt));
        using var cancellation = new CancellationTokenSource();
        var session = new FakeSession(
            Delivery("""
                {"id":45,"type":"identity","identity":{"did":"did:plc:ordered-quarantine","is_active":true,"status":"deleted"}}
                """),
            sequence,
            cancellation);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => runner.RunAsync(session, cancellation.Token));

        Assert.Equal(["reserve", "quarantine", "ack"], sequence);
        Assert.Equal([(ulong)45], session.Acknowledged);
    }

    [Fact]
    public async Task SessionDisconnectsForRetryWithoutSendingAck()
    {
        var account = AccountKey.FromDid("did:plc:retry-owner");
        var backend = FakeBackend.Visible(Source, account, ObservedAt.ToUnixTimeSeconds() / 60);
        backend.CommitOutcome = DurableCommitOutcome.OptimisticConflict;
        var processor = Processor(
            backend,
            new DurableTapProcessingOptions
            {
                MaximumPlanningAttempts = 1,
                LifecyclePageSize = 10,
            });
        var runner = new DurableTapSessionRunner(processor, new FixedTimeProvider(ObservedAt));
        var session = new FakeSession(
            Delivery("""
                {"id":46,"type":"record","record":{"live":true,"did":"did:plc:retry-owner","rev":"3jzfcijpj2z2b","collection":"app.bsky.actor.profile","rkey":"self","action":"create","cid":"bafy-profile","metadata_status":"valid"}}
                """));

        var result = await runner.RunAsync(session);

        Assert.Equal(DurableTapSessionDisposition.RetryConnectionWithoutAcknowledgement, result);
        Assert.Empty(session.Acknowledged);
    }

    private static DurableTapDeliveryProcessor Processor(
        FakeBackend backend,
        DurableTapProcessingOptions? options = null)
        => new(Source, backend, new AdmitAll(), options);

    private static TapDelivery Delivery(string json)
    {
        var digest = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json))).ToLowerInvariant();
        return new TapDelivery(json, digest);
    }

    private sealed class AdmitAll : IAccountAdmission
    {
        public bool IsAdmitted(AccountKey accountKey) => accountKey.IsValid;
    }

    private sealed class FixedTimeProvider(DateTimeOffset value) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => value;
    }

    private sealed class FakeSession : IDurableTapSession
    {
        private readonly TapDelivery _delivery;
        private readonly List<string>? _sequence;
        private readonly CancellationTokenSource? _cancellation;
        private int _received;

        internal FakeSession(
            TapDelivery delivery,
            List<string>? sequence = null,
            CancellationTokenSource? cancellation = null)
        {
            _delivery = delivery;
            _sequence = sequence;
            _cancellation = cancellation;
        }

        internal List<ulong> Acknowledged { get; } = [];

        public ValueTask<TapDelivery> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Interlocked.Increment(ref _received) != 1)
            {
                throw new InvalidOperationException("The test session has only one delivery.");
            }

            return ValueTask.FromResult(_delivery);
        }

        public ValueTask AcknowledgeAsync(
            ulong deliveryId,
            CancellationToken cancellationToken = default)
        {
            Acknowledged.Add(deliveryId);
            _sequence?.Add("ack");
            _cancellation?.Cancel();
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeBackend : IDurableTapBackend
    {
        private readonly Guid _source;
        private readonly List<string>? _sequence;
        private AccountStateSnapshot? _account;
        private ProjectionSnapshot? _projection;
        private ActivityWindowAggregateSnapshot? _aggregate;

        internal FakeBackend(Guid source, List<string>? sequence = null)
        {
            _source = source;
            _sequence = sequence;
        }

        internal DurableDeliveryOutcome ReservationOutcome { get; set; } = DurableDeliveryOutcome.Pending;

        internal DurableCommitOutcome CommitOutcome { get; set; } = DurableCommitOutcome.Applied;

        internal List<DurableDeliveryReservationRequest> Reservations { get; } = [];

        internal List<DurableQuarantine> Quarantines { get; } = [];

        internal List<DurableIngestionCommit> Commits { get; } = [];

        internal Queue<LifecycleAdvanceResult> LifecycleStartResults { get; } = [];

        internal Queue<LifecycleAdvanceResult> LifecycleAdvanceResults { get; } = [];

        internal int LifecycleStartCount { get; private set; }

        internal int LifecycleAdvanceCount { get; private set; }

        internal int AggregateReadCount { get; private set; }

        internal static FakeBackend Visible(Guid source, AccountKey account, long minute)
        {
            var backend = new FakeBackend(source);
            backend._account = new AccountStateSnapshot(
                account,
                stateVersion: 1,
                DurableAccountLifecycle.Active,
                repositoryGeneration: 0,
                completedSyncRevision: "3jzfcijpj2z2a",
                synchronizationComplete: true,
                lastActivityMinuteUtc: minute - 1,
                currentPostCount: 0,
                currentFollowingCount: 0,
                currentFollowerCount: 0);
            backend._projection = new ProjectionSnapshot(
                account,
                version: 1,
                ProjectionOperation.Upsert,
                isComplete: true,
                projectionCutMinuteUtc: minute - 1,
                nextRecalculationMinuteUtc: null,
                lastActivityMinuteUtc: minute - 1,
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
            backend._aggregate = new ActivityWindowAggregateSnapshot(
                account,
                accountStateVersion: 1,
                repositoryGeneration: 0,
                cutMinuteUtc: minute,
                new ActivityRollingCounts(0, 0, 0),
                new ActivityRollingCounts(0, 0, 0),
                new ActivityRollingCounts(0, 0, 0),
                new ActivityRollingCounts(0, 0, 0),
                receivedEngagementCreatesThirtyDays: 0,
                nextExpiryMinuteUtc: null);
            return backend;
        }

        public Task<DurableDeliveryReservation> ReserveDeliveryAsync(
            DurableDeliveryReservationRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Reservations.Add(request);
            _sequence?.Add("reserve");
            return Task.FromResult(new DurableDeliveryReservation(
                _source,
                request.TapDeliveryId,
                request.DeliveryDigest,
                Reservations[0].FirstObservedAtMinuteUtc,
                ReservationOutcome));
        }

        public Task<DurableCommitResult> CommitAsync(
            DurableDeliveryReservation reservation,
            DurableIngestionCommit commit,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Commits.Add(commit);
            return Task.FromResult(Result(CommitOutcome));
        }

        public Task<DurableCommitResult> CommitValidatedNoOpAsync(
            DurableDeliveryReservation reservation,
            DurableValidatedNoOp noOp,
            CancellationToken cancellationToken = default)
            => Task.FromResult(Result(DurableCommitOutcome.ValidatedNoOp));

        public Task<DurableCommitResult> CommitQuarantineAsync(
            DurableDeliveryReservation reservation,
            DurableQuarantine quarantine,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Quarantines.Add(quarantine);
            _sequence?.Add("quarantine");
            return Task.FromResult(Result(DurableCommitOutcome.Quarantined));
        }

        public Task<AccountStateSnapshot?> ReadAccountAsync(
            AccountKey accountKey,
            CancellationToken cancellationToken = default)
            => Task.FromResult(_account is { } value && value.AccountKey == accountKey ? value : null);

        public Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
            AccountKey accountKey,
            CancellationToken cancellationToken = default)
            => Task.FromResult(_projection is { } value && value.AccountKey == accountKey ? value : null);

        public Task<RecordStateSnapshot?> ReadRecordAsync(
            AccountKey accountKey,
            long repositoryGeneration,
            DurableRecordKind collection,
            string recordKey,
            CancellationToken cancellationToken = default)
            => Task.FromResult<RecordStateSnapshot?>(null);

        public Task<FollowPairSnapshot?> ReadFollowPairAsync(
            AccountKey sourceAccountKey,
            AccountKey targetAccountKey,
            CancellationToken cancellationToken = default)
            => Task.FromResult<FollowPairSnapshot?>(null);

        public Task<ActivityWindowAggregateSnapshot> ReadActivityWindowAggregateAsync(
            AccountKey accountKey,
            long expectedAccountStateVersion,
            long repositoryGeneration,
            long cutMinuteUtc,
            CancellationToken cancellationToken = default)
        {
            AggregateReadCount++;
            return Task.FromResult(_aggregate
                ?? throw new InvalidOperationException("The fake has no aggregate evidence."));
        }

        public Task<LifecycleAdvanceResult> StartLifecycleAsync(
            DurableDeliveryReservation reservation,
            DurableEventEnvelope envelope,
            CancellationToken cancellationToken = default)
        {
            LifecycleStartCount++;
            return Task.FromResult(LifecycleStartResults.Dequeue());
        }

        public Task<LifecycleAdvanceResult> AdvanceLifecycleAsync(
            DurableDeliveryReservation reservation,
            int pageSize,
            CancellationToken cancellationToken = default)
        {
            LifecycleAdvanceCount++;
            return Task.FromResult(LifecycleAdvanceResults.Dequeue());
        }

        private static DurableCommitResult Result(DurableCommitOutcome outcome)
            => new(
                outcome,
                outcome is not DurableCommitOutcome.OptimisticConflict
                    and not DurableCommitOutcome.RevisionConflict);
    }
}
