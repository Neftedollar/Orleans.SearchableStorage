using System.Data;
using Npgsql;
using NpgsqlTypes;
using Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlLifecycleBarrierIntegrationTests
{
    [PostgreSqlIntegrationFact]
    public async Task PendingPurgeBlocksCompetingWorkReactivationAndOwnerOrTargetMutationsAtomically()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("barrier-owner");
        var actor = PostgreSqlTestDatabase.Account("barrier-actor");
        await SeedVisibleAccountAsync(database, owner, 1_000, "barrier-owner-seed");
        await SeedVisibleAccountAsync(database, actor, 1_001, "barrier-actor-seed");

        var inactive = LifecycleEnvelope(
            database.SourceInstanceId,
            1_002,
            owner,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "barrier-owner-delete");
        var inactiveReservation = await ReserveAsync(database, inactive);
        var started = await database.Lifecycle.StartAsync(inactiveReservation, inactive);
        Assert.Equal(LifecycleAdvanceDisposition.Pending, started.Disposition);
        Assert.False(started.AcknowledgementAllowed);

        var fencedOwner = await RequiredAccountAsync(database, owner);
        var fencedActor = await RequiredAccountAsync(database, actor);
        var desiredVersion = await ScalarForAccountAsync<long>(
            database,
            "SELECT projection_version FROM skypulse.desired_projection WHERE account_key = @account_key;",
            owner);
        var outboxCount = await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.projection_outbox;");

        var competingInactive = LifecycleEnvelope(
            database.SourceInstanceId,
            1_003,
            owner,
            generation: 1,
            DurableAccountLifecycle.Suspended,
            "barrier-owner-competing-purge");
        var competingReservation = await ReserveAsync(database, competingInactive);
        var competingResult = await database.Lifecycle.StartAsync(competingReservation, competingInactive);
        Assert.Equal(LifecycleAdvanceDisposition.Retry, competingResult.Disposition);
        Assert.False(competingResult.AcknowledgementAllowed);

        var active = LifecycleEnvelope(
            database.SourceInstanceId,
            1_004,
            owner,
            generation: 2,
            DurableAccountLifecycle.Active,
            "barrier-owner-reactivation");
        var activeReservation = await ReserveAsync(database, active);
        var activeResult = await database.Lifecycle.StartAsync(activeReservation, active);
        Assert.Equal(LifecycleAdvanceDisposition.Retry, activeResult.Disposition);
        Assert.False(activeResult.AcknowledgementAllowed);

        var ownerEnvelope = PostEnvelope(database.SourceInstanceId, 1_005, owner, generation: 1, "blocked-owner-record");
        var ownerCommit = new DurableIngestionCommit(
            ownerEnvelope,
            [CopyState(fencedOwner)],
            records: [PostRecord(ownerEnvelope)]);
        var ownerReservation = await ReserveAsync(database, ownerEnvelope);
        var ownerResult = await database.Ingestion.CommitAsync(ownerReservation, ownerCommit);
        Assert.Equal(DurableCommitOutcome.OptimisticConflict, ownerResult.Outcome);
        Assert.False(ownerResult.AcknowledgementAllowed);

        var targetEnvelope = FollowEnvelope(
            database.SourceInstanceId,
            1_006,
            actor,
            generation: 0,
            owner,
            "blocked-target-follow");
        var targetCommit = new DurableIngestionCommit(
            targetEnvelope,
            [CopyState(fencedActor), CopyState(fencedOwner, currentFollowerCount: fencedOwner.CurrentFollowerCount + 1)],
            records:
            [
                new RecordStateMutation(
                    actor,
                    0,
                    DurableRecordKind.GraphFollow,
                    targetEnvelope.RecordKey!,
                    targetEnvelope.RepositoryRevision!,
                    isDeleted: false,
                    targetEnvelope.Cid,
                    owner),
            ],
            followPairs: [new FollowPairMutation(actor, owner, 1)]);
        var targetReservation = await ReserveAsync(database, targetEnvelope);
        var targetResult = await database.Ingestion.CommitAsync(targetReservation, targetCommit);
        Assert.Equal(DurableCommitOutcome.OptimisticConflict, targetResult.Outcome);
        Assert.False(targetResult.AcknowledgementAllowed);

        Assert.Equal(1, await CountForAccountAsync(database, "skypulse.lifecycle_transition_work", owner));
        Assert.Equal(fencedOwner, await RequiredAccountAsync(database, owner));
        Assert.Equal(fencedActor, await RequiredAccountAsync(database, actor));
        Assert.Equal(
            desiredVersion,
            await ScalarForAccountAsync<long>(
                database,
                "SELECT projection_version FROM skypulse.desired_projection WHERE account_key = @account_key;",
                owner));
        Assert.Equal(outboxCount, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.projection_outbox;"));
        Assert.Equal(0, await CountForAccountAsync(database, "skypulse.semantic_event", owner, "record_key = 'blocked-owner-record'"));
        Assert.Equal(0, await CountForAccountAsync(database, "skypulse.record_state", owner, "record_key = 'blocked-owner-record'"));
        Assert.Equal(
            0,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.follow_pair WHERE source_account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", actor)));
        foreach (var deliveryId in new ulong[] { 1_003, 1_004, 1_005, 1_006 })
        {
            Assert.Equal(DurableDeliveryOutcome.Pending, await ReadDeliveryOutcomeAsync(database, deliveryId));
        }
    }

    [PostgreSqlIntegrationFact]
    public async Task InactivePurgePreservesEveryHigherGenerationRow()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("higher-generation-owner");
        await SeedVisibleAccountAsync(database, owner, 1_100, "higher-generation-seed");
        var inactive = LifecycleEnvelope(
            database.SourceInstanceId,
            1_101,
            owner,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "higher-generation-delete");
        var reservation = await ReserveAsync(database, inactive);
        Assert.Equal(
            LifecycleAdvanceDisposition.Pending,
            (await database.Lifecycle.StartAsync(reservation, inactive)).Disposition);

        await database.ExecuteAsync(
            """
            INSERT INTO skypulse.record_state (
                account_key, repository_generation, collection, record_key, latest_revision,
                is_deleted, cid, target_account_key, is_direct_reply)
            VALUES (@account_key, 2, 5, 'future-profile', @revision, false, 'future-cid', NULL, false);
            INSERT INTO skypulse.activity_minute_bucket (
                account_key, repository_generation, minute_utc, record_creates, record_updates,
                record_deletes, post_creates, received_engagement_creates)
            VALUES (@account_key, 2, @minute, 1, 0, 0, 0, 0);
            INSERT INTO skypulse.reconciliation_dependency (
                owner_account_key, owner_repository_generation, affected_account_key)
            VALUES (@account_key, 2, @account_key);
            """,
            PostgreSqlTestDatabase.AccountParameter("account_key", owner),
            new NpgsqlParameter("revision", NpgsqlDbType.Text) { Value = PostgreSqlTestDatabase.Revision(2_000) },
            new NpgsqlParameter("minute", NpgsqlDbType.Bigint) { Value = PostgreSqlTestDatabase.CurrentMinuteUtc() });

        var final = await AdvanceUntilCompleteAsync(database, reservation, pageSize: 1);

        Assert.True(final.AcknowledgementAllowed);
        Assert.Equal(0, await CountGenerationAsync(database, "skypulse.record_state", owner, "repository_generation <= 1"));
        Assert.Equal(1, await CountGenerationAsync(database, "skypulse.record_state", owner, "repository_generation = 2"));
        Assert.Equal(0, await CountGenerationAsync(database, "skypulse.activity_minute_bucket", owner, "repository_generation <= 1"));
        Assert.Equal(1, await CountGenerationAsync(database, "skypulse.activity_minute_bucket", owner, "repository_generation = 2"));
        Assert.Equal(0, await CountGenerationAsync(database, "skypulse.reconciliation_dependency", owner, "owner_repository_generation <= 1"));
        Assert.Equal(1, await CountGenerationAsync(database, "skypulse.reconciliation_dependency", owner, "owner_repository_generation = 2"));
    }

    [PostgreSqlIntegrationFact]
    public async Task AdvanceValidatesTheOwnerFenceBeforeDeletingItsNextPage()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("page-fence-owner");
        await SeedVisibleAccountAsync(database, owner, 1_200, "page-fence-seed");
        var inactive = LifecycleEnvelope(
            database.SourceInstanceId,
            1_201,
            owner,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "page-fence-delete");
        var reservation = await ReserveAsync(database, inactive);
        await database.Lifecycle.StartAsync(reservation, inactive);

        var movedToRecords = await database.Lifecycle.AdvanceAsync(reservation, pageSize: 1);
        Assert.Equal(LifecycleWorkPhase.OwnedRecords, movedToRecords.Phase);
        Assert.True(await CountForAccountAsync(database, "skypulse.record_state", owner) > 0);
        await database.ExecuteAsync(
            """
            UPDATE skypulse.account_state
            SET state_version = state_version + 1,
                lifecycle = 1,
                repository_generation = 2,
                synchronization_complete = false,
                completed_sync_revision = NULL,
                last_applied_revision = NULL
            WHERE account_key = @account_key;
            """,
            PostgreSqlTestDatabase.AccountParameter("account_key", owner));

        var retry = await database.Lifecycle.AdvanceAsync(reservation, pageSize: 1);

        Assert.Equal(LifecycleAdvanceDisposition.Retry, retry.Disposition);
        Assert.False(retry.AcknowledgementAllowed);
        Assert.True(await CountForAccountAsync(database, "skypulse.record_state", owner) > 0);
        Assert.Equal(DurableDeliveryOutcome.Pending, await ReadDeliveryOutcomeAsync(database, 1_201));
    }

    [PostgreSqlIntegrationFact]
    public async Task PageTargetDriftReturnsRetryBeforeAnyPageMutation()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("page-target-drift-owner");
        var targets = new[]
        {
            PostgreSqlTestDatabase.Account("page-target-drift-a"),
            PostgreSqlTestDatabase.Account("page-target-drift-b"),
        }.Order().ToArray();
        var lowerTarget = targets[0];
        var initialTarget = targets[1];
        await SeedVisibleAccountAsync(database, owner, 1_220, "page-target-drift-owner-seed");
        await SeedVisibleAccountAsync(database, lowerTarget, 1_221, "page-target-drift-lower-seed");
        await SeedVisibleAccountAsync(database, initialTarget, 1_222, "page-target-drift-initial-seed");
        await database.ExecuteAsync(
            """
            UPDATE skypulse.account_state
            SET current_following_count = 1
            WHERE account_key = @owner;
            UPDATE skypulse.account_state
            SET current_follower_count = 1
            WHERE account_key = @target;
            INSERT INTO skypulse.follow_pair (
                source_account_key, target_account_key, multiplicity)
            VALUES (@owner, @target, 1);
            """,
            PostgreSqlTestDatabase.AccountParameter("owner", owner),
            PostgreSqlTestDatabase.AccountParameter("target", initialTarget));
        var inactive = LifecycleEnvelope(
            database.SourceInstanceId,
            1_223,
            owner,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "page-target-drift-delete");
        var reservation = await ReserveAsync(database, inactive);
        Assert.Equal(
            LifecycleAdvanceDisposition.Pending,
            (await database.Lifecycle.StartAsync(reservation, inactive)).Disposition);

        await using var blockerConnection = await database.DataSource.OpenConnectionAsync();
        await using var blockerTransaction = await blockerConnection.BeginTransactionAsync(IsolationLevel.ReadCommitted);
        await PostgreSqlAccountTransactionBarrier.AcquireAsync(
            blockerConnection,
            blockerTransaction,
            [initialTarget],
            CancellationToken.None);
        var advance = database.Lifecycle.AdvanceAsync(reservation, pageSize: 1);
        try
        {
            var early = await Task.WhenAny(advance, Task.Delay(TimeSpan.FromMilliseconds(250)));
            Assert.NotSame(advance, early);
            await database.ExecuteAsync(
                """
                INSERT INTO skypulse.follow_pair (
                    source_account_key, target_account_key, multiplicity)
                VALUES (@owner, @target, 1);
                """,
                PostgreSqlTestDatabase.AccountParameter("owner", owner),
                PostgreSqlTestDatabase.AccountParameter("target", lowerTarget));
        }
        finally
        {
            await blockerTransaction.CommitAsync();
        }

        var retry = await advance.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(LifecycleAdvanceDisposition.Retry, retry.Disposition);
        Assert.False(retry.AcknowledgementAllowed);
        Assert.Equal(
            2,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.follow_pair WHERE source_account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            (short)LifecycleWorkPhase.OutgoingFollows,
            await database.ScalarAsync<short>("SELECT phase FROM skypulse.lifecycle_transition_work;"));
        Assert.Equal(DurableDeliveryOutcome.Pending, await ReadDeliveryOutcomeAsync(database, 1_223));
    }

    [PostgreSqlIntegrationFact]
    public async Task ConcurrentMutualPurgesSerializeTheCompleteOwnerAndTargetPage()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var first = PostgreSqlTestDatabase.Account("mutual-purge-first");
        var second = PostgreSqlTestDatabase.Account("mutual-purge-second");
        await SeedVisibleAccountAsync(database, first, 1_250, "mutual-purge-first-seed");
        await SeedVisibleAccountAsync(database, second, 1_251, "mutual-purge-second-seed");
        await database.ExecuteAsync(
            """
            UPDATE skypulse.account_state
            SET current_following_count = 1,
                current_follower_count = 1
            WHERE account_key IN (@first, @second);
            INSERT INTO skypulse.follow_pair (
                source_account_key, target_account_key, multiplicity)
            VALUES
                (@first, @second, 1),
                (@second, @first, 1);
            """,
            PostgreSqlTestDatabase.AccountParameter("first", first),
            PostgreSqlTestDatabase.AccountParameter("second", second));

        var firstEnvelope = LifecycleEnvelope(
            database.SourceInstanceId,
            1_252,
            first,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "mutual-purge-first-delete");
        var secondEnvelope = LifecycleEnvelope(
            database.SourceInstanceId,
            1_253,
            second,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "mutual-purge-second-delete");
        var firstReservation = await ReserveAsync(database, firstEnvelope);
        var secondReservation = await ReserveAsync(database, secondEnvelope);
        Assert.Equal(
            LifecycleAdvanceDisposition.Pending,
            (await database.Lifecycle.StartAsync(firstReservation, firstEnvelope)).Disposition);
        Assert.Equal(
            LifecycleAdvanceDisposition.Pending,
            (await database.Lifecycle.StartAsync(secondReservation, secondEnvelope)).Disposition);

        var advances = await Task.WhenAll(
            database.Lifecycle.AdvanceAsync(firstReservation, pageSize: 1),
            database.Lifecycle.AdvanceAsync(secondReservation, pageSize: 1)).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.All(advances, result =>
        {
            Assert.Equal(LifecycleAdvanceDisposition.Pending, result.Disposition);
            Assert.False(result.AcknowledgementAllowed);
            Assert.Equal(1, result.ProcessedRows);
        });
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.follow_pair;"));
        Assert.Equal(
            0L,
            await ScalarForAccountAsync<long>(
                database,
                "SELECT current_follower_count FROM skypulse.account_state WHERE account_key = @account_key;",
                first));
        Assert.Equal(
            0L,
            await ScalarForAccountAsync<long>(
                database,
                "SELECT current_follower_count FROM skypulse.account_state WHERE account_key = @account_key;",
                second));
        Assert.Equal(2, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.lifecycle_transition_work;"));
    }

    [PostgreSqlIntegrationFact]
    public async Task TwoConnectionsCannotInsertWorkBetweenBarrierCheckAndTransactionEnd()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("two-connection-owner");
        await SeedVisibleAccountAsync(database, owner, 1_300, "two-connection-seed");
        var inactive = LifecycleEnvelope(
            database.SourceInstanceId,
            1_301,
            owner,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "two-connection-delete");
        var reservation = await ReserveAsync(database, inactive);

        await using var firstConnection = await database.DataSource.OpenConnectionAsync();
        await using var firstTransaction = await firstConnection.BeginTransactionAsync(IsolationLevel.Serializable);
        var accounts = await PostgreSqlAccountTransactionBarrier.AcquireAsync(
            firstConnection,
            firstTransaction,
            [owner],
            CancellationToken.None);
        Assert.False(await PostgreSqlAccountTransactionBarrier.HasPendingWorkAsync(
            firstConnection,
            firstTransaction,
            accounts,
            CancellationToken.None));

        var startTask = database.Lifecycle.StartAsync(reservation, inactive);
        try
        {
            var early = await Task.WhenAny(startTask, Task.Delay(TimeSpan.FromMilliseconds(250)));
            Assert.NotSame(startTask, early);
        }
        finally
        {
            await firstTransaction.CommitAsync();
        }

        var result = await startTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(LifecycleAdvanceDisposition.Pending, result.Disposition);
        Assert.Equal(1, await CountForAccountAsync(database, "skypulse.lifecycle_transition_work", owner));
    }

    private static async Task SeedVisibleAccountAsync(
        PostgreSqlTestDatabase database,
        AccountKey account,
        ulong deliveryId,
        string semanticSeed)
    {
        var envelope = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            deliveryId,
            account,
            repositoryGeneration: 0,
            semanticSeed);
        var result = await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                envelope,
                PostgreSqlTestDatabase.State(account, expectedVersion: 0),
                PostgreSqlTestDatabase.Projection(account, version: 1)));
        Assert.True(result.AcknowledgementAllowed);
    }

    private static DurableEventEnvelope LifecycleEnvelope(
        Guid source,
        ulong deliveryId,
        AccountKey account,
        long generation,
        DurableAccountLifecycle lifecycle,
        string semanticSeed)
        => new(
            source,
            deliveryId,
            PostgreSqlTestDatabase.Digest($"delivery:{deliveryId}"),
            PostgreSqlTestDatabase.Digest(semanticSeed),
            account,
            generation,
            DurableEventKind.AccountLifecycle,
            PostgreSqlTestDatabase.CurrentMinuteUtc(),
            lifecycle: lifecycle);

    private static DurableEventEnvelope PostEnvelope(
        Guid source,
        ulong deliveryId,
        AccountKey account,
        long generation,
        string recordKey)
        => new(
            source,
            deliveryId,
            PostgreSqlTestDatabase.Digest($"delivery:{deliveryId}"),
            PostgreSqlTestDatabase.Digest($"semantic:{deliveryId}"),
            account,
            generation,
            DurableEventKind.RecordMutation,
            PostgreSqlTestDatabase.CurrentMinuteUtc(),
            PostgreSqlTestDatabase.Revision(deliveryId),
            DurableRecordKind.FeedPost,
            DurableRecordAction.Create,
            recordKey,
            $"cid-{deliveryId}");

    private static DurableEventEnvelope FollowEnvelope(
        Guid source,
        ulong deliveryId,
        AccountKey account,
        long generation,
        AccountKey target,
        string recordKey)
        => new(
            source,
            deliveryId,
            PostgreSqlTestDatabase.Digest($"delivery:{deliveryId}"),
            PostgreSqlTestDatabase.Digest($"semantic:{deliveryId}"),
            account,
            generation,
            DurableEventKind.RecordMutation,
            PostgreSqlTestDatabase.CurrentMinuteUtc(),
            PostgreSqlTestDatabase.Revision(deliveryId),
            DurableRecordKind.GraphFollow,
            DurableRecordAction.Create,
            recordKey,
            $"cid-{deliveryId}",
            target);

    private static RecordStateMutation PostRecord(DurableEventEnvelope envelope)
        => new(
            envelope.AccountKey,
            envelope.RepositoryGeneration,
            DurableRecordKind.FeedPost,
            envelope.RecordKey!,
            envelope.RepositoryRevision!,
            isDeleted: false,
            envelope.Cid);

    private static AccountStateMutation CopyState(
        AccountStateSnapshot state,
        long? currentFollowerCount = null)
        => new(
            state.AccountKey,
            state.StateVersion,
            checked(state.StateVersion + 1),
            state.Lifecycle,
            state.RepositoryGeneration,
            state.CompletedSyncRevision,
            state.SynchronizationComplete,
            state.LastActivityMinuteUtc,
            state.CurrentPostCount,
            state.CurrentFollowingCount,
            currentFollowerCount ?? state.CurrentFollowerCount,
            state.LastAppliedRevision);

    private static async Task<AccountStateSnapshot> RequiredAccountAsync(
        PostgreSqlTestDatabase database,
        AccountKey account)
        => await database.Planning.ReadAccountAsync(account)
            ?? throw new Xunit.Sdk.XunitException("The expected account state is missing.");

    private static async Task<DurableDeliveryReservation> ReserveAsync(
        PostgreSqlTestDatabase database,
        DurableEventEnvelope envelope)
        => await database.Ingestion.ReserveDeliveryAsync(
            new DurableDeliveryReservationRequest(
                envelope.SourceInstanceId,
                envelope.TapDeliveryId,
                envelope.DeliveryDigest,
                envelope.ObservedAtMinuteUtc));

    private static async Task<LifecycleAdvanceResult> AdvanceUntilCompleteAsync(
        PostgreSqlTestDatabase database,
        DurableDeliveryReservation reservation,
        int pageSize)
    {
        for (var attempt = 0; attempt < 64; attempt++)
        {
            var result = await database.Lifecycle.AdvanceAsync(reservation, pageSize);
            if (result.AcknowledgementAllowed)
            {
                return result;
            }

            Assert.Equal(LifecycleAdvanceDisposition.Pending, result.Disposition);
        }

        throw new Xunit.Sdk.XunitException("The bounded lifecycle transition did not complete within 64 pages.");
    }

    private static async Task<DurableDeliveryOutcome> ReadDeliveryOutcomeAsync(
        PostgreSqlTestDatabase database,
        ulong deliveryId)
        => (DurableDeliveryOutcome)await database.ScalarAsync<short>(
            """
            SELECT outcome
            FROM skypulse.tap_delivery
            WHERE source_instance_id = @source_instance_id
              AND delivery_id = @delivery_id;
            """,
            new NpgsqlParameter("source_instance_id", NpgsqlDbType.Uuid) { Value = database.SourceInstanceId },
            new NpgsqlParameter("delivery_id", NpgsqlDbType.Numeric) { Value = (decimal)deliveryId });

    private static Task<int> CountForAccountAsync(
        PostgreSqlTestDatabase database,
        string table,
        AccountKey account,
        string? additionalPredicate = null)
        => database.ScalarAsync<int>(
            $"SELECT count(*) FROM {table} WHERE account_key = @account_key"
                + (additionalPredicate is null ? ";" : $" AND {additionalPredicate};"),
            PostgreSqlTestDatabase.AccountParameter("account_key", account));

    private static Task<int> CountGenerationAsync(
        PostgreSqlTestDatabase database,
        string table,
        AccountKey account,
        string generationPredicate)
    {
        var accountColumn = table.EndsWith("reconciliation_dependency", StringComparison.Ordinal)
            ? "owner_account_key"
            : "account_key";
        return database.ScalarAsync<int>(
            $"SELECT count(*) FROM {table} WHERE {accountColumn} = @account_key AND {generationPredicate};",
            PostgreSqlTestDatabase.AccountParameter("account_key", account));
    }

    private static Task<T> ScalarForAccountAsync<T>(
        PostgreSqlTestDatabase database,
        string sql,
        AccountKey account)
        => database.ScalarAsync<T>(sql, PostgreSqlTestDatabase.AccountParameter("account_key", account));
}
