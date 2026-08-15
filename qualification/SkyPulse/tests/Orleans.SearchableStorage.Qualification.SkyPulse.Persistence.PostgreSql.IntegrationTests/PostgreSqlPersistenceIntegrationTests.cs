namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlPersistenceIntegrationTests
{
    [PostgreSqlIntegrationFact]
    public async Task DispatcherAdvisoryLockAllowsOneSessionAndReleasesWithItsIncarnation()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var competingStore = new PostgreSqlProjectionRuntimeStore(database.DataSource);

        var first = await database.ProjectionRuntime.TryAcquireDispatcherIncarnationAsync();
        Assert.NotNull(first);
        await using (first)
        {
            Assert.True(await first.IsHeldAsync());
            Assert.Null(await competingStore.TryAcquireDispatcherIncarnationAsync());
        }

        var successor = await competingStore.TryAcquireDispatcherIncarnationAsync();
        Assert.NotNull(successor);
        await using (successor)
        {
            Assert.True(await successor.IsHeldAsync());
        }
    }

    [PostgreSqlIntegrationFact]
    public async Task RebuildCheckpointSupersedesHistoricalOutboxThroughCurrentDesiredVersion()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("rebuild-checkpoint");
        await SeedProjectionVersionsAsync(database, account, versionCount: 2);
        var desired = Assert.Single(
            await database.ProjectionRuntime.ReadDesiredProjectionPageAsync(null, 10));

        Assert.Equal(2L, desired.Version);
        Assert.True(await database.ProjectionRuntime.MaterializeDesiredProjectionAsync(desired));
        // This point models the successful blind Memory-index upsert.
        Assert.True(await database.ProjectionRuntime.FinalizeRebuildProjectionAsync(desired));

        Assert.Equal(
            2,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.projection_outbox WHERE account_key = @account_key AND completed_at_utc IS NOT NULL;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        var hydration = Assert.Single(
            await database.ProjectionRuntime.ReadPublishedUpsertsAsync([account]));
        Assert.Equal(2L, hydration.Value.Version);
    }

    [PostgreSqlIntegrationFact]
    public async Task MigrationsCanBeReappliedAndValidateTheInstalledSchema()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync(applyMigrations: false);

        await database.Schema.ApplyMigrationsAsync();
        await database.Schema.ApplyMigrationsAsync();
        var validation = await database.Schema.ValidateAsync();

        Assert.True(validation.IsValid, string.Join(Environment.NewLine, validation.Errors));
        Assert.Equal(
            PostgreSqlSchema.CurrentVersion,
            await database.ScalarAsync<int>("SELECT max(version) FROM skypulse.schema_migration;"));
        Assert.Equal(
            PostgreSqlSchema.RequiredTableColumns.Count,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'skypulse' AND table_type = 'BASE TABLE';"));

        await database.ExecuteAsync("ALTER TABLE skypulse.account_state ADD COLUMN unexpected_drift text NULL;");
        var driftedValidation = await database.Schema.ValidateAsync();

        Assert.False(driftedValidation.IsValid);
        Assert.Contains(
            driftedValidation.Errors,
            static error => error.Contains("Unexpected catalog object column:account_state.", StringComparison.Ordinal));
    }

    [PostgreSqlIntegrationFact]
    public async Task ReusedDeliveryIdentityWithDifferentDigestFailsClosed()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var account = PostgreSqlTestDatabase.Account("delivery-conflict");
        var first = PostgreSqlTestDatabase.Commit(
            PostgreSqlTestDatabase.Envelope(source, 1, account, 0, "semantic", "delivery-a"),
            PostgreSqlTestDatabase.State(account, 0));
        var conflicting = PostgreSqlTestDatabase.Commit(
            PostgreSqlTestDatabase.Envelope(source, 1, account, 0, "semantic", "delivery-b"),
            PostgreSqlTestDatabase.State(account, 1));

        var applied = await database.Ingestion.CommitAsync(first);
        var exception = await Assert.ThrowsAsync<TapDeliveryIdentityConflictException>(
            () => database.Ingestion.CommitAsync(conflicting));

        Assert.Equal(DurableCommitOutcome.Applied, applied.Outcome);
        Assert.Equal(source, exception.SourceInstanceId);
        Assert.Equal(1UL, exception.DeliveryId);
        Assert.Equal(1, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.tap_delivery;"));
        Assert.Equal((short)DurableDeliveryOutcome.Applied, await database.ScalarAsync<short>("SELECT outcome FROM skypulse.tap_delivery;"));
        Assert.Equal(1L, await ReadAccountVersionAsync(database, account));
    }

    [PostgreSqlIntegrationFact]
    public async Task SemanticDuplicateIsScopedToRepositoryGeneration()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var account = PostgreSqlTestDatabase.Account("semantic-generation");
        const string semanticSeed = "same-semantic-transition";

        var generationZero = await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                PostgreSqlTestDatabase.Envelope(source, 1, account, 0, semanticSeed),
                PostgreSqlTestDatabase.State(account, 0, repositoryGeneration: 0)));
        var duplicateInGenerationZero = await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                PostgreSqlTestDatabase.Envelope(source, 2, account, 0, semanticSeed),
                PostgreSqlTestDatabase.State(account, 1, repositoryGeneration: 0)));
        var sameSemanticInGenerationOne = await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                PostgreSqlTestDatabase.Envelope(source, 3, account, 1, semanticSeed),
                PostgreSqlTestDatabase.State(account, 1, repositoryGeneration: 1)));

        Assert.Equal(DurableCommitOutcome.Applied, generationZero.Outcome);
        Assert.Equal(DurableCommitOutcome.SemanticDuplicate, duplicateInGenerationZero.Outcome);
        Assert.Equal(DurableCommitOutcome.Applied, sameSemanticInGenerationOne.Outcome);
        Assert.All(
            [generationZero, duplicateInGenerationZero, sameSemanticInGenerationOne],
            static result => Assert.True(result.AcknowledgementAllowed));
        Assert.Equal(2, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.semantic_event;"));
        Assert.Equal(2L, await ReadAccountVersionAsync(database, account));
        Assert.Equal(1L, await ReadAccountGenerationAsync(database, account));
    }

    [PostgreSqlIntegrationFact]
    public async Task OptimisticConflictRollsBackTheEntireTransition()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var accounts = new[]
        {
            PostgreSqlTestDatabase.Account("rollback-a"),
            PostgreSqlTestDatabase.Account("rollback-b"),
        }.Order().ToArray();
        var first = accounts[0];
        var conflicting = accounts[1];

        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                PostgreSqlTestDatabase.Envelope(source, 1, first, 0, "seed-first"),
                PostgreSqlTestDatabase.State(first, 0)));
        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                PostgreSqlTestDatabase.Envelope(source, 2, conflicting, 0, "seed-conflicting"),
                PostgreSqlTestDatabase.State(conflicting, 0)));

        var envelope = PostgreSqlTestDatabase.Envelope(
            source,
            3,
            first,
            0,
            "must-fully-roll-back",
            recordKey: "rolled-back-record");
        var transition = new DurableIngestionCommit(
            envelope,
            [
                PostgreSqlTestDatabase.State(first, 1, currentPostCount: 2),
                PostgreSqlTestDatabase.State(conflicting, 0),
            ],
            records:
            [
                PostgreSqlTestDatabase.Record(envelope),
            ],
            activity:
            [
                new ActivityMinuteDelta(first, PostgreSqlTestDatabase.CurrentMinuteUtc(), recordCreates: 1, postCreates: 1),
            ],
            projections:
            [
                PostgreSqlTestDatabase.Projection(first, 2, currentPostCount: 2),
            ]);

        var result = await database.Ingestion.CommitAsync(transition);

        Assert.Equal(DurableCommitOutcome.OptimisticConflict, result.Outcome);
        Assert.False(result.AcknowledgementAllowed);
        Assert.Equal(1L, await ReadAccountVersionAsync(database, first));
        Assert.Equal(1L, await ReadAccountVersionAsync(database, conflicting));
        Assert.Equal(
            1,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.tap_delivery WHERE source_instance_id = @source AND delivery_id = 3;",
                new NpgsqlParameter("source", NpgsqlDbType.Uuid) { Value = source }));
        Assert.Equal(
            0,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.semantic_event WHERE semantic_digest = @digest;",
                new NpgsqlParameter("digest", NpgsqlDbType.Bytea) { Value = Convert.FromHexString(envelope.SemanticDigest) }));
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.record_state;"));
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.activity_minute_bucket;"));
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.desired_projection;"));
        Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.projection_outbox;"));
    }

    [PostgreSqlIntegrationFact]
    public async Task StaleAndEqualRecordRevisionsRollBackAndDisallowAcknowledgement()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var account = PostgreSqlTestDatabase.Account("revision-conflict");
        var initialEnvelope = PostgreSqlTestDatabase.Envelope(
            source,
            1,
            account,
            0,
            "initial-revision",
            revisionOrdinal: 3);
        var initial = await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                initialEnvelope,
                PostgreSqlTestDatabase.State(account, 0)));
        Assert.Equal(DurableCommitOutcome.Applied, initial.Outcome);

        foreach (var candidate in new[]
                 {
                     (DeliveryId: 2UL, RevisionOrdinal: 1UL, Semantic: "stale-revision"),
                     (DeliveryId: 3UL, RevisionOrdinal: 3UL, Semantic: "equal-revision"),
                 })
        {
            var envelope = PostgreSqlTestDatabase.Envelope(
                source,
                candidate.DeliveryId,
                account,
                0,
                candidate.Semantic,
                revisionOrdinal: candidate.RevisionOrdinal);
            var result = await database.Ingestion.CommitAsync(
                PostgreSqlTestDatabase.Commit(
                    envelope,
                    PostgreSqlTestDatabase.State(account, 1, currentPostCount: 2),
                    PostgreSqlTestDatabase.Projection(account, 2, currentPostCount: 2)));

            Assert.Equal(DurableCommitOutcome.RevisionConflict, result.Outcome);
            Assert.False(result.AcknowledgementAllowed);
            Assert.Equal(1L, await ReadAccountVersionAsync(database, account));
            Assert.Equal(
                    1,
                    await database.ScalarAsync<int>(
                        "SELECT count(*) FROM skypulse.tap_delivery WHERE source_instance_id = @source AND delivery_id = @delivery_id;",
                    new NpgsqlParameter("source", NpgsqlDbType.Uuid) { Value = source },
                    new NpgsqlParameter("delivery_id", NpgsqlDbType.Numeric) { Value = (decimal)candidate.DeliveryId }));
            Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.desired_projection;"));
            Assert.Equal(0, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.projection_outbox;"));
        }

        Assert.Equal(
            PostgreSqlTestDatabase.Revision(3),
            await database.ScalarAsync<string>(
                "SELECT latest_revision FROM skypulse.record_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
    }

    [PostgreSqlIntegrationFact]
    public async Task ConcurrentProjectionLeasesAreExclusiveOrderedAndRecoverAfterExpiry()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("exclusive-lease");
        await SeedProjectionVersionsAsync(database, account, versionCount: 2);
        var dispatcherA = new PostgreSqlDispatchStore(database.DataSource);
        var dispatcherB = new PostgreSqlDispatchStore(database.DataSource);

        var concurrent = await Task.WhenAll(
            dispatcherA.LeaseProjectionsAsync(10, TimeSpan.FromSeconds(1)),
            dispatcherB.LeaseProjectionsAsync(10, TimeSpan.FromSeconds(1)));
        var firstLease = Assert.Single(concurrent.SelectMany(static leases => leases));

        Assert.Equal(1L, firstLease.Projection.Version);
        Assert.Empty(await database.Dispatch.LeaseProjectionsAsync(10, TimeSpan.FromSeconds(1)));

        await Task.Delay(TimeSpan.FromMilliseconds(1_200));
        var recoveredLease = Assert.Single(
            await database.Dispatch.LeaseProjectionsAsync(10, TimeSpan.FromSeconds(10)));

        Assert.Equal(1L, recoveredLease.Projection.Version);
        Assert.NotEqual(firstLease.LeaseId, recoveredLease.LeaseId);
        Assert.False(
            await database.Dispatch.FailProjectionAsync(
                firstLease,
                DateTimeOffset.UtcNow,
                "stale_worker",
                "An expired worker cannot release a newer lease."));
        Assert.True(
            await database.Dispatch.FailProjectionAsync(
                recoveredLease,
                DateTimeOffset.UtcNow,
                "retry",
                "Release the live lease for the next test boundary."));
    }

    [PostgreSqlIntegrationFact]
    public async Task UpsertPreparationRemainsDurableAcrossLeaseExpiryBeforeExactFinalize()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("prepare-crash-retry");
        await SeedProjectionVersionsAsync(database, account, versionCount: 1);
        var firstLease = Assert.Single(
            await database.Dispatch.LeaseProjectionsAsync(1, TimeSpan.FromSeconds(1)));

        Assert.False(await database.Dispatch.FinalizeProjectionAsync(firstLease));
        Assert.True(await database.Dispatch.PrepareProjectionHydrationAsync(firstLease));
        Assert.Equal(
            1L,
            await database.ScalarAsync<long>(
                "SELECT projection_version FROM skypulse.published_projection WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Equal(
            0,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.projection_outbox WHERE account_key = @account_key AND completed_at_utc IS NOT NULL;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));

        await Task.Delay(TimeSpan.FromMilliseconds(1_200));
        Assert.False(await database.Dispatch.FinalizeProjectionAsync(firstLease));
        var retryLease = Assert.Single(
            await database.Dispatch.LeaseProjectionsAsync(1, TimeSpan.FromSeconds(10)));

        Assert.Equal(firstLease.Projection, retryLease.Projection);
        Assert.NotEqual(firstLease.LeaseId, retryLease.LeaseId);
        Assert.True(await database.Dispatch.PrepareProjectionHydrationAsync(retryLease));
        Assert.True(await database.Dispatch.FinalizeProjectionAsync(retryLease));
        Assert.Equal(
            1,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.projection_outbox WHERE account_key = @account_key AND completed_at_utc IS NOT NULL;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Empty(await database.Dispatch.LeaseProjectionsAsync(1, TimeSpan.FromSeconds(1)));
    }

    [PostgreSqlIntegrationFact]
    public async Task DeleteWaitsForEarlierUpsertAndFinalizeRetainsRemovalTombstone()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var account = PostgreSqlTestDatabase.Account("ordered-delete");
        var upsertEnvelope = PostgreSqlTestDatabase.Envelope(source, 1, account, 0, "upsert-before-delete");
        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                upsertEnvelope,
                PostgreSqlTestDatabase.State(account, 0),
                PostgreSqlTestDatabase.Projection(account, 1)));
        var deleteEnvelope = PostgreSqlTestDatabase.Envelope(
            source,
            2,
            account,
            0,
            "ordered-delete",
            action: DurableRecordAction.Delete);
        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                deleteEnvelope,
                PostgreSqlTestDatabase.State(account, 1, currentPostCount: 0),
                PostgreSqlTestDatabase.Projection(
                    account,
                    2,
                    ProjectionOperation.Remove,
                    currentPostCount: 0)));

        var upsertLease = Assert.Single(
            await database.Dispatch.LeaseProjectionsAsync(10, TimeSpan.FromSeconds(10)));
        Assert.Equal(ProjectionOperation.Upsert, upsertLease.Projection.Operation);
        Assert.Equal(1L, upsertLease.Projection.Version);
        Assert.True(await database.Dispatch.PrepareProjectionHydrationAsync(upsertLease));
        Assert.True(await database.Dispatch.FinalizeProjectionAsync(upsertLease));
        var publishedUpsert = Assert.Single(
            await database.ProjectionRuntime.ReadPublishedUpsertsAsync([account]));
        Assert.Equal(1L, publishedUpsert.Value.Version);

        var deleteLease = Assert.Single(
            await database.Dispatch.LeaseProjectionsAsync(10, TimeSpan.FromSeconds(10)));
        Assert.Equal(ProjectionOperation.Remove, deleteLease.Projection.Operation);
        Assert.Equal(2L, deleteLease.Projection.Version);
        await Assert.ThrowsAsync<ArgumentException>(
            () => database.Dispatch.PrepareProjectionHydrationAsync(deleteLease));
        Assert.Equal(
            1,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.published_projection WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));

        // This models a successful external index removal before PostgreSQL finalization.
        Assert.True(await database.Dispatch.FinalizeProjectionAsync(deleteLease));
        Assert.Equal(
            1,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.published_projection WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Equal(
            2L,
            await database.ScalarAsync<long>(
                "SELECT projection_version FROM skypulse.published_projection WHERE account_key = @account_key AND operation = 2;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Equal(
            2,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.projection_outbox WHERE account_key = @account_key AND completed_at_utc IS NOT NULL;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.True(
            await database.ScalarAsync<bool>(
                "SELECT is_deleted FROM skypulse.desired_projection WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Empty(await database.ProjectionRuntime.ReadPublishedUpsertsAsync([account]));
        var desiredPage = Assert.Single(
            await database.ProjectionRuntime.ReadDesiredProjectionPageAsync(null, 10));
        Assert.Equal(ProjectionOperation.Remove, desiredPage.Operation);
        Assert.Equal(2L, desiredPage.Version);
    }

    [PostgreSqlIntegrationFact]
    public async Task StaleRecalculationCannotOverwriteAConcurrentProjectionAdvance()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var account = PostgreSqlTestDatabase.Account("recalculation-race");
        var nowMinute = PostgreSqlTestDatabase.CurrentMinuteUtc();
        var initialProjection = PostgreSqlTestDatabase.Projection(
            account,
            1,
            projectionCutMinuteUtc: nowMinute - 2,
            nextRecalculationMinuteUtc: nowMinute - 1);
        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                PostgreSqlTestDatabase.Envelope(source, 1, account, 0, "recalculation-seed"),
                PostgreSqlTestDatabase.State(account, 0),
                initialProjection));
        var staleLease = Assert.Single(
            await database.Dispatch.LeaseRecalculationsAsync(10, TimeSpan.FromMinutes(1)));

        var currentProjection = PostgreSqlTestDatabase.Projection(
            account,
            2,
            projectionCutMinuteUtc: nowMinute,
            nextRecalculationMinuteUtc: nowMinute + 10,
            currentPostCount: 2);
        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                PostgreSqlTestDatabase.Envelope(source, 2, account, 0, "concurrent-live-event"),
                PostgreSqlTestDatabase.State(account, 1, currentPostCount: 2),
                currentProjection));

        var staleProjection = PostgreSqlTestDatabase.Projection(
            account,
            2,
            projectionCutMinuteUtc: nowMinute,
            nextRecalculationMinuteUtc: nowMinute + 1);
        var staleCommitSucceeded = await database.Dispatch.CommitRecalculationAsync(
            staleLease,
            PostgreSqlTestDatabase.State(account, 1),
            staleProjection);

        Assert.False(staleCommitSucceeded);
        Assert.Equal(2L, await ReadAccountVersionAsync(database, account));
        Assert.Equal(
            2L,
            await database.ScalarAsync<long>(
                "SELECT source_projection_version FROM skypulse.projection_recalculation_due WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
        Assert.Equal(
            nowMinute + 10,
            await database.ScalarAsync<long>(
                "SELECT due_minute_utc FROM skypulse.projection_recalculation_due WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", account)));
    }

    private static async Task SeedProjectionVersionsAsync(
        PostgreSqlTestDatabase database,
        AccountKey account,
        int versionCount)
    {
        var source = database.SourceInstanceId;
        for (var version = 1; version <= versionCount; version++)
        {
            var expectedVersion = version - 1L;
            var result = await database.Ingestion.CommitAsync(
                PostgreSqlTestDatabase.Commit(
                    PostgreSqlTestDatabase.Envelope(
                        source,
                        (ulong)version,
                        account,
                        0,
                        $"projection-semantic-{version}"),
                    PostgreSqlTestDatabase.State(account, expectedVersion, currentPostCount: version),
                    PostgreSqlTestDatabase.Projection(account, version, currentPostCount: version)));
            Assert.Equal(DurableCommitOutcome.Applied, result.Outcome);
        }
    }

    private static Task<long> ReadAccountVersionAsync(PostgreSqlTestDatabase database, AccountKey account)
        => database.ScalarAsync<long>(
            "SELECT state_version FROM skypulse.account_state WHERE account_key = @account_key;",
            PostgreSqlTestDatabase.AccountParameter("account_key", account));

    private static Task<long> ReadAccountGenerationAsync(PostgreSqlTestDatabase database, AccountKey account)
        => database.ScalarAsync<long>(
            "SELECT repository_generation FROM skypulse.account_state WHERE account_key = @account_key;",
            PostgreSqlTestDatabase.AccountParameter("account_key", account));
}
