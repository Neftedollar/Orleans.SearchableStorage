using Npgsql;
using NpgsqlTypes;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class PostgreSqlCommandTests
{
    [Fact]
    public void NpgsqlRuntimeIsPinnedToReviewedVersion()
    {
        Assert.Equal(new Version(8, 0, 8, 0), typeof(NpgsqlConnection).Assembly.GetName().Version);
    }

    [Fact]
    public void ReservationCommandBindsUnsignedIdentifierDigestAndFirstObservedMinute()
    {
        using var connection = new NpgsqlConnection("Host=not-opened");
        var request = new DurableDeliveryReservationRequest(
            DurableModelTests.SourceInstanceId,
            ulong.MaxValue,
            DurableModelTests.Digest('a'),
            10);

        using var command = PostgreSqlCommands.CreateReserveDeliveryCommand(connection, transaction: null, request);

        Assert.Equal(NpgsqlDbType.Numeric, command.Parameters["delivery_id"].NpgsqlDbType);
        Assert.Equal(NpgsqlDbType.Uuid, command.Parameters["source_instance_id"].NpgsqlDbType);
        Assert.Equal((decimal)ulong.MaxValue, command.Parameters["delivery_id"].Value);
        Assert.Equal(NpgsqlDbType.Bytea, command.Parameters["delivery_digest"].NpgsqlDbType);
        Assert.Equal(32, Assert.IsType<byte[]>(command.Parameters["delivery_digest"].Value).Length);
        Assert.Equal(10L, command.Parameters["first_observed_at_minute_utc"].Value);
        Assert.DoesNotContain(request.DeliveryDigest, command.CommandText, StringComparison.Ordinal);
        Assert.Contains("NULL, NULL", command.CommandText, StringComparison.Ordinal);
    }

    [Fact]
    public void CompletionRequiresTheExactPendingReservation()
    {
        Assert.Contains("delivery_digest = @delivery_digest", PostgreSqlCommands.CompleteDeliverySql, StringComparison.Ordinal);
        Assert.Contains("observed_at_minute_utc = @first_observed_at_minute_utc", PostgreSqlCommands.CompleteDeliverySql, StringComparison.Ordinal);
        Assert.Contains("outcome = 0", PostgreSqlCommands.CompleteDeliverySql, StringComparison.Ordinal);
        Assert.Contains("FOR UPDATE", PostgreSqlCommands.ReadDeliverySql, StringComparison.Ordinal);
    }

    [Fact]
    public void AccountBarrierUsesSortedTransactionScopedLocksAndChecksDurableWork()
    {
        var first = DurableModelTests.Account("barrier-a");
        var second = DurableModelTests.Account("barrier-b");
        var canonical = PostgreSqlAccountTransactionBarrier.Canonicalize([second, first, second]);

        Assert.Equal(new[] { first, second }.Order().ToArray(), canonical);
        Assert.Contains("pg_advisory_xact_lock", PostgreSqlAccountTransactionBarrier.AcquireSql, StringComparison.Ordinal);
        Assert.Contains("lifecycle_transition_work", PostgreSqlAccountTransactionBarrier.HasPendingWorkSql, StringComparison.Ordinal);
        Assert.Contains("account_key = @account_key", PostgreSqlAccountTransactionBarrier.HasPendingWorkSql, StringComparison.Ordinal);
        Assert.NotEqual(
            PostgreSqlAccountTransactionBarrier.GetLockIdentity(first),
            PostgreSqlAccountTransactionBarrier.GetLockIdentity(second));
    }

    [Fact]
    public void OrdinaryCommitBarrierIncludesOwnerAndEveryIndirectMutationTarget()
    {
        var owner = DurableModelTests.Account("barrier-owner");
        var target = DurableModelTests.Account("barrier-target");
        var envelope = new DurableEventEnvelope(
            DurableModelTests.SourceInstanceId,
            19,
            DurableModelTests.Digest('a'),
            DurableModelTests.Digest('b'),
            owner,
            0,
            DurableEventKind.RecordMutation,
            10,
            repositoryRevision: DurableModelTests.Revision,
            collection: DurableRecordKind.GraphFollow,
            action: DurableRecordAction.Create,
            recordKey: "follow",
            cid: "cid-follow",
            targetAccountKey: target);
        var ownerState = DurableModelTests.State(owner, expectedVersion: 0);
        var targetState = DurableModelTests.State(target, expectedVersion: 0);
        var commit = new DurableIngestionCommit(
            envelope,
            [ownerState, targetState],
            records:
            [
                new RecordStateMutation(
                    owner,
                    0,
                    DurableRecordKind.GraphFollow,
                    "follow",
                    DurableModelTests.Revision,
                    isDeleted: false,
                    cid: "cid-follow",
                    targetAccountKey: target),
            ],
            followPairs: [new FollowPairMutation(owner, target, 1)],
            reconciliationDependencies:
            [
                new ReconciliationDependencyMutation(
                    owner,
                    0,
                    target,
                    ReconciliationDependencyAction.Add),
            ]);

        var accounts = PostgreSqlIngestionStore.GetMutationAccountKeys(commit);

        Assert.Equal(new[] { owner, target }.Order().ToArray(), accounts);
    }

    [Fact]
    public void PlanningAndDependencyReadsAreCanonicalAndUseLimitPlusOneParameter()
    {
        Assert.Contains("ORDER BY minute_utc", PostgreSqlPlanningStore.ReadActivitySql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @read_limit", PostgreSqlPlanningStore.ReadActivitySql, StringComparison.Ordinal);
        Assert.Contains("repository_generation = @repository_generation", PostgreSqlPlanningStore.ReadActivitySql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY affected_account_key", PostgreSqlPlanningStore.ReadDependenciesSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @read_limit", PostgreSqlPlanningStore.ReadDependenciesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ActivityPlanningAggregateIsFixedSizeExactAndVersionGenerationFenced()
    {
        var sql = PostgreSqlPlanningStore.ReadActivityWindowAggregateSql;

        Assert.Contains("state_version = @expected_state_version", sql, StringComparison.Ordinal);
        Assert.Contains("repository_generation = @repository_generation", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(record_creates) FILTER", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(record_updates) FILTER", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(record_deletes) FILTER", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(post_creates) FILTER", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(received_engagement_creates)", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(candidate.expiry_minute_utc)", sql, StringComparison.Ordinal);
        Assert.Contains("minute_utc > (@cut_minute_utc - 43200)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RecalculationLeaseCarriesTheAuthoritativePostgreSqlMinute()
    {
        var sql = PostgreSqlCommands.LeaseRecalculationsSql;

        Assert.Contains("clock_timestamp()", sql, StringComparison.Ordinal);
        Assert.Contains(
            "floor(extract(epoch from clock_timestamp()) / 60)::bigint AS evaluation_minute_utc",
            sql,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ReconciliationDependencyMutationsAreExactAndGenerationScoped()
    {
        Assert.Contains("ON CONFLICT (owner_account_key, owner_repository_generation, affected_account_key)", PostgreSqlCommands.AddReconciliationDependencySql, StringComparison.Ordinal);
        Assert.Contains("owner_repository_generation = @owner_repository_generation", PostgreSqlCommands.RemoveReconciliationDependencySql, StringComparison.Ordinal);
        Assert.Contains("affected_account_key = @affected_account_key", PostgreSqlCommands.RemoveReconciliationDependencySql, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidatedNoOpProofsReadCurrentDatabaseState()
    {
        Assert.Contains("convert_to(latest_revision, 'UTF8') >=", PostgreSqlCommands.ProveRecordRevisionAlreadyObservedSql, StringComparison.Ordinal);
        Assert.Contains("repository_generation > @repository_generation", PostgreSqlCommands.ProveRepositoryGenerationSupersededSql, StringComparison.Ordinal);
        Assert.Contains("synchronization_complete", PostgreSqlCommands.ProveRepositorySyncRevisionAlreadyCompletedSql, StringComparison.Ordinal);
        Assert.Contains("convert_to(completed_sync_revision, 'UTF8') >=", PostgreSqlCommands.ProveRepositorySyncRevisionAlreadyCompletedSql, StringComparison.Ordinal);
        Assert.Contains("convert_to(last_applied_revision, 'UTF8') >", PostgreSqlCommands.ProveRepositoryRevisionAlreadyAppliedSql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanningReadsDesiredProjectionAndRepositoryWideRevisionHighWater()
    {
        Assert.Contains("last_applied_revision", PostgreSqlPlanningStore.ReadAccountSql, StringComparison.Ordinal);
        Assert.Contains("FROM skypulse.desired_projection", PostgreSqlPlanningStore.ReadDesiredProjectionSql, StringComparison.Ordinal);
        Assert.Contains("received_engagement_creates_30_days", PostgreSqlPlanningStore.ReadDesiredProjectionSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ManifestBindingIsInsertOnceAndCompleteIdentity()
    {
        Assert.Contains("ON CONFLICT (manifest_id) DO NOTHING", PostgreSqlRuntimeManifestStore.InsertSql, StringComparison.Ordinal);
        Assert.Contains("package_canonical_manifest_sha256", PostgreSqlRuntimeManifestStore.InsertSql, StringComparison.Ordinal);
        Assert.Contains("package_repository_url", PostgreSqlRuntimeManifestStore.InsertSql, StringComparison.Ordinal);
        Assert.Contains("package_build_sdk_version", PostgreSqlRuntimeManifestStore.InsertSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProjectionCommandBindsAllSeventeenValuesWithoutJson()
    {
        using var command = new NpgsqlCommand();
        var projection = DurableModelTests.Projection(DurableModelTests.Account("actor"), 3);

        PostgreSqlCommands.AddProjectionParameters(command, projection);

        Assert.Equal(23, command.Parameters.Count);
        Assert.Equal(NpgsqlDbType.Bigint, command.Parameters["current_follower_count"].NpgsqlDbType);
        Assert.Equal(3L, command.Parameters["projection_version"].Value);
        Assert.DoesNotContain(command.Parameters.Cast<NpgsqlParameter>(), static parameter => parameter.NpgsqlDbType is NpgsqlDbType.Json or NpgsqlDbType.Jsonb);
    }

    [Fact]
    public void UpsertPreparationWritesHydrationButLeavesOutboxPending()
    {
        Assert.Contains("operation = 1", PostgreSqlCommands.PrepareHydrationSql, StringComparison.Ordinal);
        Assert.Contains("lease_id = @lease_id", PostgreSqlCommands.PrepareHydrationSql, StringComparison.Ordinal);
        Assert.Contains("leased_until_utc > clock_timestamp()", PostgreSqlCommands.PrepareHydrationSql, StringComparison.Ordinal);
        Assert.Contains("published_projection", PostgreSqlCommands.PrepareHydrationSql, StringComparison.Ordinal);
        Assert.DoesNotContain("completed_at_utc =", PostgreSqlCommands.PrepareHydrationSql, StringComparison.Ordinal);
    }

    [Fact]
    public void RemovalFinalizeRetainsExactTombstoneUnderSameLiveLease()
    {
        Assert.Contains("operation = 2", PostgreSqlCommands.MaterializeRemovalSql, StringComparison.Ordinal);
        Assert.Contains("lease_id = @lease_id", PostgreSqlCommands.MaterializeRemovalSql, StringComparison.Ordinal);
        Assert.Contains("leased_until_utc > clock_timestamp()", PostgreSqlCommands.MaterializeRemovalSql, StringComparison.Ordinal);
        Assert.Contains("INSERT INTO skypulse.published_projection", PostgreSqlCommands.MaterializeRemovalSql, StringComparison.Ordinal);
        Assert.Contains("hydration.projection_version = leased.projection_version", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
        Assert.Contains("hydration.operation = 2", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
    }

    [Fact]
    public void RebuildReadsBoundedCanonicalDesiredPagesAndMaterializesExactVersions()
    {
        Assert.Contains("WHERE is_complete", PostgreSqlCommands.ReadDesiredProjectionFirstPageSql, StringComparison.Ordinal);
        Assert.Contains("account_key > @after_account_key", PostgreSqlCommands.ReadDesiredProjectionNextPageSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY account_key", PostgreSqlCommands.ReadDesiredProjectionNextPageSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @batch_size", PostgreSqlCommands.ReadDesiredProjectionNextPageSql, StringComparison.Ordinal);
        Assert.Contains("projection_version = @projection_version", PostgreSqlCommands.MaterializeDesiredProjectionSql, StringComparison.Ordinal);
        Assert.Contains("operation = @operation", PostgreSqlCommands.MaterializeDesiredProjectionSql, StringComparison.Ordinal);
        Assert.Contains("projection_version <= @projection_version", PostgreSqlCommands.FinalizeRebuildProjectionSql, StringComparison.Ordinal);
        Assert.Contains("exact_desired", PostgreSqlCommands.FinalizeRebuildProjectionSql, StringComparison.Ordinal);
        Assert.Contains("exact_published", PostgreSqlCommands.FinalizeRebuildProjectionSql, StringComparison.Ordinal);
    }

    [Fact]
    public void BatchHydrationExcludesRemovalTombstones()
    {
        Assert.Contains("account_key = ANY(@account_keys)", PostgreSqlCommands.ReadPublishedUpsertsSql, StringComparison.Ordinal);
        Assert.Contains("operation = 1", PostgreSqlCommands.ReadPublishedUpsertsSql, StringComparison.Ordinal);
        Assert.Contains("is_complete", PostgreSqlCommands.ReadPublishedUpsertsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DispatcherLockIsSessionScopedAndVerifiableOnItsOwningBackend()
    {
        Assert.Contains("pg_try_advisory_lock", PostgreSqlProjectionRuntimeStore.TryAcquireDispatcherSql, StringComparison.Ordinal);
        Assert.Contains("pid = pg_backend_pid()", PostgreSqlProjectionRuntimeStore.IsDispatcherLockHeldSql, StringComparison.Ordinal);
        Assert.Contains("objsubid = 1", PostgreSqlProjectionRuntimeStore.IsDispatcherLockHeldSql, StringComparison.Ordinal);
    }

    [Fact]
    public void FinalizeRequiresPreparedSameVersionHydrationForUpsert()
    {
        Assert.Contains("hydration.projection_version = leased.projection_version", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
        Assert.Contains("hydration.operation = 1", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
        Assert.Contains("leased.completed_at_utc IS NULL", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
    }

    [Fact]
    public void RecordReplacementAdvancesRevisionByOrdinalUtf8Bytes()
    {
        Assert.Contains("convert_to(current.latest_revision, 'UTF8')", PostgreSqlCommands.UpsertRecordStateSql, StringComparison.Ordinal);
        Assert.Contains("< convert_to(EXCLUDED.latest_revision, 'UTF8')", PostgreSqlCommands.UpsertRecordStateSql, StringComparison.Ordinal);
    }

    [Fact]
    public void AccountReplacementCannotRegressGenerationOrSameGenerationRevisionHighWater()
    {
        Assert.Contains("@repository_generation >= repository_generation", PostgreSqlCommands.UpdateAccountStateSql, StringComparison.Ordinal);
        Assert.Contains("convert_to(@last_applied_revision, 'UTF8') >=", PostgreSqlCommands.UpdateAccountStateSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, 60)]
    [InlineData(1001, 60)]
    [InlineData(1, 0)]
    [InlineData(1, 901)]
    public async Task ProjectionLeaseRejectsUnboundedWork(int batchSize, int leaseSeconds)
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=not-opened");
        var store = new PostgreSqlDispatchStore(dataSource);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => store.LeaseProjectionsAsync(batchSize, TimeSpan.FromSeconds(leaseSeconds)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1001)]
    public async Task DesiredProjectionScanRejectsUnboundedWork(int batchSize)
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=not-opened");
        var store = new PostgreSqlProjectionRuntimeStore(dataSource);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => store.ReadDesiredProjectionPageAsync(null, batchSize));
    }

    [Fact]
    public void EveryMutatingSqlStatementUsesParametersForVariableMetadata()
    {
        var sql = new[]
        {
            PostgreSqlCommands.ReserveDeliverySql,
            PostgreSqlCommands.InsertSemanticEventSql,
            PostgreSqlCommands.UpsertRecordStateSql,
            PostgreSqlCommands.UpsertFollowPairSql,
            PostgreSqlCommands.AddActivitySql,
            PostgreSqlCommands.UpsertDesiredProjectionSql,
            PostgreSqlCommands.InsertOutboxSql,
            PostgreSqlCommands.InsertQuarantineSql,
        };

        Assert.All(sql, static statement => Assert.Contains('@', statement));
        Assert.All(sql, static statement => Assert.DoesNotContain("raw", statement, StringComparison.OrdinalIgnoreCase));
    }
}
