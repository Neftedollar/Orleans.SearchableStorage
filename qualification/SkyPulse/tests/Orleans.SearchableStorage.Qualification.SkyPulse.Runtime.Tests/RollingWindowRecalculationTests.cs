namespace Orleans.SearchableStorage.Qualification.SkyPulse.Runtime.Tests;

public sealed class RollingWindowRecalculationTests
{
    private const long DueMinute = 1_000;
    private static readonly AccountKey Account = AccountKey.FromDid("did:plc:rolling-recalculation");

    [Fact]
    public void PlannerMapsExactAggregateAndPreservesNonRollingState()
    {
        var lease = Lease();
        var account = AccountState();
        var aggregate = Aggregate(cutMinute: 1_005, nextExpiryMinute: 1_010);

        var transition = RollingWindowRecalculationPlanner.Plan(
            lease,
            account,
            DesiredProjection(),
            aggregate,
            cutMinuteUtc: 1_005);

        Assert.Equal(7, transition.AccountState.ExpectedVersion);
        Assert.Equal(8, transition.AccountState.NextVersion);
        Assert.Equal(account.RepositoryGeneration, transition.AccountState.RepositoryGeneration);
        Assert.Equal(account.CompletedSyncRevision, transition.AccountState.CompletedSyncRevision);
        Assert.Equal(account.LastAppliedRevision, transition.AccountState.LastAppliedRevision);
        Assert.Equal(account.LastActivityMinuteUtc, transition.AccountState.LastActivityMinuteUtc);
        Assert.Equal(account.CurrentPostCount, transition.AccountState.CurrentPostCount);
        Assert.Equal(account.CurrentFollowingCount, transition.AccountState.CurrentFollowingCount);
        Assert.Equal(account.CurrentFollowerCount, transition.AccountState.CurrentFollowerCount);
        Assert.Equal(8, transition.Projection.Version);
        Assert.Equal(1_005, transition.Projection.ProjectionCutMinuteUtc);
        Assert.Equal(1_010, transition.Projection.NextRecalculationMinuteUtc);
        Assert.Equal(2, transition.Projection.CreatedRecordCount1Day);
        Assert.Equal(5, transition.Projection.CreatedRecordCount7Days);
        Assert.Equal(9, transition.Projection.CreatedRecordCount30Days);
        Assert.Equal(1, transition.Projection.PostCreates1Day);
        Assert.Equal(2, transition.Projection.PostCreates7Days);
        Assert.Equal(3, transition.Projection.PostCreates30Days);
        Assert.Equal(4, transition.Projection.ReceivedEngagementCreates30Days);
    }

    [Fact]
    public void PlannerRejectsProjectionThatIsNotTheExactDueSource()
    {
        var desired = Projection(version: 8, dueMinute: DueMinute);

        var exception = Assert.Throws<RollingWindowRecalculationEvidenceException>(() =>
            RollingWindowRecalculationPlanner.Plan(
                Lease(),
                AccountState(),
                desired,
                Aggregate(cutMinute: DueMinute, nextExpiryMinute: 2_000),
                DueMinute));

        Assert.Contains("source version", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WorkerUsesLeasedDatabaseMinuteAndCommitsOneAtomicTransition()
    {
        var store = new FakeStore();
        store.Leases.Enqueue([Lease()]);
        var worker = Worker(store, processMinute: 9_000);

        var result = await worker.ProcessOnceAsync();

        Assert.Equal(1, result.LeasedCount);
        Assert.Equal(1, result.CommittedCount);
        Assert.Equal(0, result.SupersededCount);
        Assert.Equal(0, result.FailedCount);
        Assert.Equal(1_005, store.AggregateCutMinute);
        Assert.Equal(8, Assert.IsType<AccountStateMutation>(store.CommittedState).NextVersion);
        Assert.Equal(1_005, Assert.IsType<ProjectionSnapshot>(store.CommittedProjection).ProjectionCutMinuteUtc);
        Assert.Equal(0, store.FailCount);
    }

    [Fact]
    public async Task DelayedWorkerJumpsDirectlyToCurrentMinute()
    {
        var store = new FakeStore
        {
            AggregateFactory = cut => Aggregate(cut, nextExpiryMinute: null, empty: true),
        };
        store.Leases.Enqueue([Lease(evaluationMinute: 50_000)]);
        var worker = Worker(store, processMinute: 1_005);

        var result = await worker.ProcessOnceAsync();

        Assert.Equal(1, result.CommittedCount);
        Assert.Equal(50_000, store.AggregateCutMinute);
        var projection = Assert.IsType<ProjectionSnapshot>(store.CommittedProjection);
        Assert.Null(projection.NextRecalculationMinuteUtc);
        Assert.Equal(0, projection.CreatedRecordCount30Days);
        Assert.Equal(0, projection.ReceivedEngagementCreates30Days);
    }

    [Fact]
    public async Task OptimisticConflictIsAValidatedSupersession()
    {
        var store = new FakeStore { CommitResult = false };
        store.Leases.Enqueue([Lease()]);

        var result = await Worker(store, processMinute: 1_005).ProcessOnceAsync();

        Assert.Equal(0, result.CommittedCount);
        Assert.Equal(1, result.SupersededCount);
        Assert.Equal(0, result.FailedCount);
        Assert.Equal(0, store.FailCount);
    }

    [Fact]
    public async Task EvidenceFailureIsSanitizedReleasedAndReportedToHost()
    {
        var store = new FakeStore
        {
            ReadException = new InvalidOperationException("source-controlled-secret"),
        };
        store.Leases.Enqueue([Lease()]);

        var result = await Worker(store, processMinute: 1_005).ProcessOnceAsync();

        Assert.Equal(1, result.FailedCount);
        Assert.Equal(1, store.FailCount);
        Assert.Equal("rolling-recalculation-failed", store.FailureCode);
        Assert.DoesNotContain("secret", store.FailureMessage, StringComparison.Ordinal);
        Assert.Equal(
            DateTimeOffset.FromUnixTimeSeconds((1_005 * 60) + 30),
            store.RetryAtUtc);
    }

    [Fact]
    public async Task ChangedLeaseDuringFailureIsReportedAsSuperseded()
    {
        var store = new FakeStore
        {
            ReadException = new RollingWindowRecalculationEvidenceException("changed concurrently"),
            FailResult = false,
        };
        store.Leases.Enqueue([Lease()]);

        var result = await Worker(store, processMinute: 1_005).ProcessOnceAsync();

        Assert.Equal(0, result.FailedCount);
        Assert.Equal(1, result.SupersededCount);
    }

    [Fact]
    public async Task UnexpectedFailureCannotHideBehindAnExpiredLease()
    {
        var store = new FakeStore
        {
            ReadException = new InvalidOperationException("programmer or infrastructure failure"),
            FailResult = false,
        };
        store.Leases.Enqueue([Lease()]);

        var result = await Worker(store, processMinute: 1_005).ProcessOnceAsync();

        Assert.Equal(1, result.FailedCount);
        Assert.Equal(0, result.SupersededCount);
    }

    [Fact]
    public async Task StoreCannotExceedRequestedBatchBound()
    {
        var store = new FakeStore();
        store.Leases.Enqueue(Enumerable.Range(0, 11)
            .Select(index => new ProjectionRecalculationLease(
                Guid.NewGuid(),
                AccountKey.FromDid($"did:plc:rolling-batch-{index}"),
                7,
                DueMinute,
                1_005,
                0))
            .ToArray());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => Worker(store, processMinute: 1_005).ProcessOnceAsync());
    }

    private static RollingWindowRecalculationWorker Worker(FakeStore store, long processMinute)
        => new(
            store,
            new RollingWindowRecalculationOptions
            {
                BatchSize = 10,
                LeaseDuration = TimeSpan.FromMinutes(1),
                FailureDelay = TimeSpan.FromSeconds(30),
            },
            new FixedTimeProvider(DateTimeOffset.FromUnixTimeSeconds(processMinute * 60)));

    private static ProjectionRecalculationLease Lease(long evaluationMinute = 1_005)
        => new(
            Guid.Parse("1e979b45-b904-4e72-81f9-3a001326be72"),
            Account,
            7,
            DueMinute,
            evaluationMinute,
            0);

    private static AccountStateSnapshot AccountState()
        => new(
            Account,
            stateVersion: 7,
            DurableAccountLifecycle.Active,
            repositoryGeneration: 3,
            completedSyncRevision: "3222222222222",
            synchronizationComplete: true,
            lastActivityMinuteUtc: 999,
            currentPostCount: 11,
            currentFollowingCount: 12,
            currentFollowerCount: 13,
            lastAppliedRevision: "3222222222222");

    private static ProjectionSnapshot DesiredProjection()
        => Projection(version: 7, dueMinute: DueMinute);

    private static ProjectionSnapshot Projection(long version, long dueMinute)
        => new(
            Account,
            version,
            ProjectionOperation.Upsert,
            isComplete: true,
            projectionCutMinuteUtc: 999,
            nextRecalculationMinuteUtc: dueMinute,
            lastActivityMinuteUtc: 999,
            createdRecordCount1Day: 8,
            createdRecordCount7Days: 9,
            createdRecordCount30Days: 10,
            updatedRecordCount1Day: 5,
            updatedRecordCount7Days: 6,
            updatedRecordCount30Days: 7,
            deletedRecordCount1Day: 1,
            deletedRecordCount7Days: 2,
            deletedRecordCount30Days: 3,
            currentPostCount: 11,
            currentFollowingCount: 12,
            currentFollowerCount: 13,
            postCreates1Day: 3,
            postCreates7Days: 4,
            postCreates30Days: 5,
            receivedEngagementCreates30Days: 4);

    private static ActivityWindowAggregateSnapshot Aggregate(
        long cutMinute,
        long? nextExpiryMinute,
        bool empty = false)
        => new(
            Account,
            accountStateVersion: 7,
            repositoryGeneration: 3,
            cutMinute,
            empty ? new ActivityRollingCounts(0, 0, 0) : new ActivityRollingCounts(2, 5, 9),
            empty ? new ActivityRollingCounts(0, 0, 0) : new ActivityRollingCounts(0, 1, 3),
            empty ? new ActivityRollingCounts(0, 0, 0) : new ActivityRollingCounts(0, 0, 1),
            empty ? new ActivityRollingCounts(0, 0, 0) : new ActivityRollingCounts(1, 2, 3),
            receivedEngagementCreatesThirtyDays: empty ? 0 : 4,
            nextExpiryMinute);

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private sealed class FakeStore : IRollingWindowRecalculationStore
    {
        internal Queue<IReadOnlyList<ProjectionRecalculationLease>> Leases { get; } = new();

        internal Exception? ReadException { get; init; }

        internal bool CommitResult { get; init; } = true;

        internal bool FailResult { get; init; } = true;

        internal Func<long, ActivityWindowAggregateSnapshot> AggregateFactory { get; init; }
            = cut => Aggregate(cut, nextExpiryMinute: checked(cut + 5));

        internal long? AggregateCutMinute { get; private set; }

        internal AccountStateMutation? CommittedState { get; private set; }

        internal ProjectionSnapshot? CommittedProjection { get; private set; }

        internal int FailCount { get; private set; }

        internal string? FailureCode { get; private set; }

        internal string? FailureMessage { get; private set; }

        internal DateTimeOffset? RetryAtUtc { get; private set; }

        public Task<IReadOnlyList<ProjectionRecalculationLease>> LeaseRecalculationsAsync(
            int batchSize,
            TimeSpan leaseDuration,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(
                Leases.Count == 0
                    ? (IReadOnlyList<ProjectionRecalculationLease>)[]
                    : Leases.Dequeue());
        }

        public Task<AccountStateSnapshot?> ReadAccountAsync(
            AccountKey accountKey,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (ReadException is not null)
            {
                throw ReadException;
            }

            return Task.FromResult<AccountStateSnapshot?>(AccountState());
        }

        public Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
            AccountKey accountKey,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<ProjectionSnapshot?>(DesiredProjection());
        }

        public Task<ActivityWindowAggregateSnapshot> ReadActivityWindowAggregateAsync(
            AccountKey accountKey,
            long expectedAccountStateVersion,
            long repositoryGeneration,
            long cutMinuteUtc,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AggregateCutMinute = cutMinuteUtc;
            return Task.FromResult(AggregateFactory(cutMinuteUtc));
        }

        public Task<bool> CommitRecalculationAsync(
            ProjectionRecalculationLease lease,
            AccountStateMutation accountState,
            ProjectionSnapshot projection,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CommittedState = accountState;
            CommittedProjection = projection;
            return Task.FromResult(CommitResult);
        }

        public Task<bool> FailRecalculationAsync(
            ProjectionRecalculationLease lease,
            DateTimeOffset availableAtUtc,
            string errorCode,
            string errorMessage,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            FailCount++;
            RetryAtUtc = availableAtUtc;
            FailureCode = errorCode;
            FailureMessage = errorMessage;
            return Task.FromResult(FailResult);
        }
    }
}
