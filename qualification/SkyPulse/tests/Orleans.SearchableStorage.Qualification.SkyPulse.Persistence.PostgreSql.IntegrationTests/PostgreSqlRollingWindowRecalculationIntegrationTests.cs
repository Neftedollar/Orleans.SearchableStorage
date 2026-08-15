using Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlRollingWindowRecalculationIntegrationTests
{
    [PostgreSqlIntegrationFact]
    public async Task DueWindowDecayAtomicallyAdvancesDesiredProjectionAndOutbox()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("rolling-window-decay");
        var nowMinute = PostgreSqlTestDatabase.CurrentMinuteUtc();
        var activityMinute = nowMinute - (24 * 60);
        var initialCut = nowMinute - 1;
        var revision = PostgreSqlTestDatabase.Revision(1);
        var envelope = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            deliveryId: 1,
            account,
            repositoryGeneration: 0,
            semanticSeed: "rolling-window-seed",
            revisionOrdinal: 1);
        var initialState = new AccountStateMutation(
            account,
            expectedVersion: 0,
            nextVersion: 1,
            DurableAccountLifecycle.Active,
            repositoryGeneration: 0,
            completedSyncRevision: revision,
            synchronizationComplete: true,
            lastActivityMinuteUtc: initialCut,
            currentPostCount: 1,
            currentFollowingCount: 0,
            currentFollowerCount: 0,
            lastAppliedRevision: revision);
        var initialProjection = new ProjectionSnapshot(
            account,
            version: 1,
            ProjectionOperation.Upsert,
            isComplete: true,
            projectionCutMinuteUtc: initialCut,
            nextRecalculationMinuteUtc: nowMinute,
            lastActivityMinuteUtc: initialCut,
            createdRecordCount1Day: 1,
            createdRecordCount7Days: 1,
            createdRecordCount30Days: 1,
            updatedRecordCount1Day: 0,
            updatedRecordCount7Days: 0,
            updatedRecordCount30Days: 0,
            deletedRecordCount1Day: 0,
            deletedRecordCount7Days: 0,
            deletedRecordCount30Days: 0,
            currentPostCount: 1,
            currentFollowingCount: 0,
            currentFollowerCount: 0,
            postCreates1Day: 1,
            postCreates7Days: 1,
            postCreates30Days: 1,
            receivedEngagementCreates30Days: 0);
        var activity = new ActivityMinuteDelta(
            account,
            activityMinute,
            repositoryGeneration: 0,
            recordCreates: 1,
            postCreates: 1);
        Assert.Equal(
            DurableCommitOutcome.Applied,
            (await database.Ingestion.CommitAsync(
                PostgreSqlTestDatabase.Commit(
                    envelope,
                    initialState,
                    initialProjection,
                    activity: activity))).Outcome);

        var worker = new RollingWindowRecalculationWorker(
            new PostgreSqlRollingWindowRecalculationStore(database.Planning, database.Dispatch),
            new RollingWindowRecalculationOptions
            {
                BatchSize = 10,
                LeaseDuration = TimeSpan.FromMinutes(1),
                FailureDelay = TimeSpan.Zero,
            },
            new FixedTimeProvider(DateTimeOffset.FromUnixTimeSeconds(nowMinute * 60)));

        var result = await worker.ProcessOnceAsync();

        Assert.Equal(1, result.LeasedCount);
        Assert.Equal(1, result.CommittedCount);
        Assert.Equal(0, result.SupersededCount);
        Assert.Equal(0, result.FailedCount);
        var state = Assert.IsType<AccountStateSnapshot>(await database.Planning.ReadAccountAsync(account));
        var desired = Assert.IsType<ProjectionSnapshot>(
            await database.Planning.ReadDesiredProjectionAsync(account));
        Assert.Equal(2, state.StateVersion);
        Assert.Equal(initialCut, state.LastActivityMinuteUtc);
        Assert.Equal(1, state.CurrentPostCount);
        Assert.Equal(2, desired.Version);
        Assert.InRange(
            desired.ProjectionCutMinuteUtc,
            nowMinute,
            PostgreSqlTestDatabase.CurrentMinuteUtc());
        Assert.Equal(0, desired.CreatedRecordCount1Day);
        Assert.Equal(1, desired.CreatedRecordCount7Days);
        Assert.Equal(1, desired.CreatedRecordCount30Days);
        Assert.Equal(0, desired.PostCreates1Day);
        Assert.Equal(1, desired.PostCreates7Days);
        Assert.Equal(1, desired.PostCreates30Days);
        Assert.Equal(activityMinute + (7 * 24 * 60), desired.NextRecalculationMinuteUtc);
        Assert.Equal(
            1,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.projection_outbox WHERE account_key = @account_key AND projection_version = 2 AND completed_at_utc IS NULL;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Equal(
            2L,
            await database.ScalarAsync<long>(
                "SELECT source_projection_version FROM skypulse.projection_recalculation_due WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Equal(
            1,
            await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.tap_delivery;"));
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
