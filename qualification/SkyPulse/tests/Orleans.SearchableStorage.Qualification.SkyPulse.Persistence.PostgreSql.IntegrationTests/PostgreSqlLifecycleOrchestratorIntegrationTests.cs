using Npgsql;
using NpgsqlTypes;
using Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlLifecycleOrchestratorIntegrationTests
{
    [PostgreSqlIntegrationFact]
    public async Task RepositorySyncDrainsBoundedDependenciesBeforeItCommitsAndAllowsAcknowledgement()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("paged-sync-owner");
        var target = PostgreSqlTestDatabase.Account("paged-sync-target");
        var active = LifecycleEnvelope(
            database.SourceInstanceId,
            deliveryId: 100,
            owner,
            generation: 0,
            DurableAccountLifecycle.Active,
            "active-owner");
        var activeReservation = await ReserveAsync(database, active);

        var activeResult = await database.Lifecycle.StartAsync(activeReservation, active);

        Assert.True(activeResult.AcknowledgementAllowed);
        Assert.False(await ReadSynchronizationCompleteAsync(database, owner));
        Assert.Equal(
            1,
            await CountAsync(
                database,
                "skypulse.reconciliation_dependency",
                "owner_account_key = @account_key",
                owner));

        var targetEnvelope = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            deliveryId: 101,
            target,
            repositoryGeneration: 0,
            semanticSeed: "seed-sync-target");
        var targetCommit = await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                targetEnvelope,
                PostgreSqlTestDatabase.State(target, expectedVersion: 0),
                PostgreSqlTestDatabase.Projection(target, version: 1)));
        Assert.True(targetCommit.AcknowledgementAllowed);
        await database.ExecuteAsync(
            """
            INSERT INTO skypulse.reconciliation_dependency (
                owner_account_key, owner_repository_generation, affected_account_key)
            VALUES (@owner, 0, @target);
            """,
            PostgreSqlTestDatabase.AccountParameter("owner", owner),
            PostgreSqlTestDatabase.AccountParameter("target", target));

        var sync = RepositorySyncEnvelope(
            database.SourceInstanceId,
            deliveryId: 102,
            owner,
            generation: 0,
            PostgreSqlTestDatabase.Revision(50),
            "sync-owner");
        var syncReservation = await ReserveAsync(database, sync);
        var started = await database.Lifecycle.StartAsync(syncReservation, sync);

        Assert.False(started.AcknowledgementAllowed);
        Assert.Equal(LifecycleAdvanceDisposition.Pending, started.Disposition);
        Assert.Equal((short)DurableDeliveryOutcome.Pending, await ReadDeliveryOutcomeAsync(database, 102));
        Assert.False(await ReadSynchronizationCompleteAsync(database, owner));

        var final = await AdvanceUntilCompleteAsync(database, syncReservation, deliveryId: 102, pageSize: 1);

        Assert.True(final.AcknowledgementAllowed);
        Assert.True(await ReadSynchronizationCompleteAsync(database, owner));
        Assert.Equal(
            PostgreSqlTestDatabase.Revision(50),
            await database.ScalarAsync<string>(
                "SELECT completed_sync_revision FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            PostgreSqlTestDatabase.Revision(50),
            await database.ScalarAsync<string>(
                "SELECT last_applied_revision FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.reconciliation_dependency;"));
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.lifecycle_transition_work;"));
        Assert.Equal((short)DurableDeliveryOutcome.Applied, await ReadDeliveryOutcomeAsync(database, 102));
        Assert.Equal(
            (short)ProjectionOperation.Upsert,
            await database.ScalarAsync<short>(
                "SELECT operation FROM skypulse.desired_projection WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.True(
            await database.ScalarAsync<long>(
                "SELECT projection_version FROM skypulse.desired_projection WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", target)) > 1);
    }

    [PostgreSqlIntegrationFact]
    public async Task InactiveLifecyclePurgesOwnedStateByPagesAndRepairsDistinctFollowerStocks()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("paged-delete-owner");
        var target = PostgreSqlTestDatabase.Account("paged-delete-target");
        await SeedVisibleAccountAsync(database, owner, deliveryId: 200, "seed-delete-owner");
        await SeedVisibleAccountAsync(database, target, deliveryId: 201, "seed-delete-target");

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
            VALUES (@owner, @target, 2);
            INSERT INTO skypulse.record_state (
                account_key, repository_generation, collection, record_key, latest_revision,
                is_deleted, cid, target_account_key, is_direct_reply)
            VALUES
                (@owner, 0, 4, 'follow-a', @revision, false, 'cid-follow-a', @target, false),
                (@owner, 0, 4, 'follow-b', @revision, false, 'cid-follow-b', @target, false);
            INSERT INTO skypulse.activity_minute_bucket (
                account_key, repository_generation, minute_utc, record_creates, record_updates,
                record_deletes, post_creates, received_engagement_creates)
            VALUES
                (@owner, 0, @minute, 1, 0, 0, 1, 0),
                (@owner, 0, @minute + 1, 0, 1, 0, 0, 0),
                (@owner, 0, @minute + 2, 0, 0, 1, 0, 0);
            INSERT INTO skypulse.reconciliation_dependency (
                owner_account_key, owner_repository_generation, affected_account_key)
            VALUES (@owner, 0, @target);
            """,
            PostgreSqlTestDatabase.AccountParameter("owner", owner),
            PostgreSqlTestDatabase.AccountParameter("target", target),
            new NpgsqlParameter("revision", NpgsqlDbType.Text) { Value = PostgreSqlTestDatabase.Revision(20) },
            new NpgsqlParameter("minute", NpgsqlDbType.Bigint) { Value = PostgreSqlTestDatabase.CurrentMinuteUtc() - 10 });

        var deleted = LifecycleEnvelope(
            database.SourceInstanceId,
            deliveryId: 202,
            owner,
            generation: 1,
            DurableAccountLifecycle.Deleted,
            "delete-owner");
        var reservation = await ReserveAsync(database, deleted);
        var started = await database.Lifecycle.StartAsync(reservation, deleted);

        Assert.Equal(LifecycleAdvanceDisposition.Pending, started.Disposition);
        Assert.False(started.AcknowledgementAllowed);
        Assert.Equal((short)DurableDeliveryOutcome.Pending, await ReadDeliveryOutcomeAsync(database, 202));
        Assert.Equal(
            (short)ProjectionOperation.Remove,
            await database.ScalarAsync<short>(
                "SELECT operation FROM skypulse.desired_projection WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));

        var final = await AdvanceUntilCompleteAsync(database, reservation, deliveryId: 202, pageSize: 1);

        Assert.True(final.AcknowledgementAllowed);
        Assert.Equal((short)DurableDeliveryOutcome.Applied, await ReadDeliveryOutcomeAsync(database, 202));
        Assert.Equal(0, await CountAsync(database, "skypulse.record_state", "account_key = @account_key", owner));
        Assert.Equal(0, await CountAsync(database, "skypulse.follow_pair", "source_account_key = @account_key", owner));
        Assert.Equal(0, await CountAsync(database, "skypulse.activity_minute_bucket", "account_key = @account_key", owner));
        Assert.Equal(0, await CountAsync(database, "skypulse.reconciliation_dependency", "owner_account_key = @account_key", owner));
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.lifecycle_transition_work;"));
        Assert.Equal(
            0L,
            await database.ScalarAsync<long>(
                "SELECT current_post_count FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            0L,
            await database.ScalarAsync<long>(
                "SELECT current_following_count FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            0L,
            await database.ScalarAsync<long>(
                "SELECT current_follower_count FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", target)));
        Assert.Equal(
            (short)DurableAccountLifecycle.Deleted,
            await database.ScalarAsync<short>(
                "SELECT lifecycle FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            1L,
            await database.ScalarAsync<long>(
                "SELECT repository_generation FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
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

    private static async Task<LifecycleAdvanceResult> AdvanceUntilCompleteAsync(
        PostgreSqlTestDatabase database,
        DurableDeliveryReservation reservation,
        ulong deliveryId,
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
            Assert.Equal((short)DurableDeliveryOutcome.Pending, await ReadDeliveryOutcomeAsync(database, deliveryId));
        }

        throw new Xunit.Sdk.XunitException("The bounded lifecycle transition did not complete within 64 pages.");
    }

    private static async Task<DurableDeliveryReservation> ReserveAsync(
        PostgreSqlTestDatabase database,
        DurableEventEnvelope envelope)
        => await database.Ingestion.ReserveDeliveryAsync(
            new DurableDeliveryReservationRequest(
                envelope.SourceInstanceId,
                envelope.TapDeliveryId,
                envelope.DeliveryDigest,
                envelope.ObservedAtMinuteUtc));

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

    private static DurableEventEnvelope RepositorySyncEnvelope(
        Guid source,
        ulong deliveryId,
        AccountKey account,
        long generation,
        string revision,
        string semanticSeed)
        => new(
            source,
            deliveryId,
            PostgreSqlTestDatabase.Digest($"delivery:{deliveryId}"),
            PostgreSqlTestDatabase.Digest(semanticSeed),
            account,
            generation,
            DurableEventKind.RepositorySync,
            PostgreSqlTestDatabase.CurrentMinuteUtc(),
            repositoryRevision: revision);

    private static async Task<short> ReadDeliveryOutcomeAsync(
        PostgreSqlTestDatabase database,
        ulong deliveryId)
        => await database.ScalarAsync<short>(
            """
            SELECT outcome
            FROM skypulse.tap_delivery
            WHERE source_instance_id = @source_instance_id
              AND delivery_id = @delivery_id;
            """,
            new NpgsqlParameter("source_instance_id", NpgsqlDbType.Uuid) { Value = database.SourceInstanceId },
            new NpgsqlParameter("delivery_id", NpgsqlDbType.Numeric) { Value = (decimal)deliveryId });

    private static async Task<bool> ReadSynchronizationCompleteAsync(
        PostgreSqlTestDatabase database,
        AccountKey account)
        => await database.ScalarAsync<bool>(
            "SELECT synchronization_complete FROM skypulse.account_state WHERE account_key = @account_key;",
            PostgreSqlTestDatabase.AccountParameter("account_key", account));

    private static async Task<int> CountAsync(
        PostgreSqlTestDatabase database,
        string table,
        string predicate,
        AccountKey account)
        => await database.ScalarAsync<int>(
            $"SELECT count(*) FROM {table} WHERE {predicate};",
            PostgreSqlTestDatabase.AccountParameter("account_key", account));
}
