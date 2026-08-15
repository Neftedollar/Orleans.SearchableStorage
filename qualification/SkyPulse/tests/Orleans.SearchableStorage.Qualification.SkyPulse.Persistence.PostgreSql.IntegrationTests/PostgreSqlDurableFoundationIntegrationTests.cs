namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlDurableFoundationIntegrationTests
{
    [PostgreSqlIntegrationFact]
    public async Task RuntimeManifestIsInsertOnceAndEveryMaterialMismatchFailsClosed()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var expected = PostgreSqlTestDatabase.CreateRuntimeManifest(database.SourceInstanceId);

        await database.Manifest.BindAsync(expected);
        var actual = Assert.IsType<RuntimeManifest>(await database.Manifest.ReadAsync());
        Assert.Equal(expected.Fingerprint, actual.Fingerprint);

        var mismatch = new RuntimeManifest(
            expected.Profile,
            expected.SourceInstanceId,
            expected.Index,
            new RuntimePackageIdentity(
                expected.Package.PackageId,
                expected.Package.PackageVersion,
                PostgreSqlTestDatabase.Digest("different-nupkg"),
                expected.Package.CanonicalManifestSha256,
                expected.Package.RepositoryUrl,
                expected.Package.RepositoryCommit,
                expected.Package.BuildSdkVersion));
        await Assert.ThrowsAsync<RuntimeManifestMismatchException>(() => database.Manifest.BindAsync(mismatch));

        Assert.Equal(
            expected.Fingerprint,
            Assert.IsType<RuntimeManifest>(await database.Manifest.ReadAsync()).Fingerprint);
    }

    [PostgreSqlIntegrationFact]
    public async Task ReservationPrecedesPlanningPreservesFirstMinuteAndCompletedDuplicateIsAckable()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("reservation-redelivery");
        var envelope = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            1,
            account,
            0,
            "reservation-semantic");
        var request = new DurableDeliveryReservationRequest(
            envelope.SourceInstanceId,
            envelope.TapDeliveryId,
            envelope.DeliveryDigest,
            envelope.ObservedAtMinuteUtc);

        var first = await database.Ingestion.ReserveDeliveryAsync(request);
        var redelivery = await database.Ingestion.ReserveDeliveryAsync(
            new DurableDeliveryReservationRequest(
                request.SourceInstanceId,
                request.TapDeliveryId,
                request.DeliveryDigest,
                checked(request.FirstObservedAtMinuteUtc + 10)));

        Assert.True(first.IsPending);
        Assert.Equal(first.FirstObservedAtMinuteUtc, redelivery.FirstObservedAtMinuteUtc);
        Assert.Equal(
            (short)DurableDeliveryOutcome.Pending,
            await database.ScalarAsync<short>("SELECT outcome FROM skypulse.tap_delivery WHERE delivery_id = 1;"));

        var applied = await database.Ingestion.CommitAsync(
            first,
            PostgreSqlTestDatabase.Commit(envelope, PostgreSqlTestDatabase.State(account, 0)));
        var completedDuplicate = await database.Ingestion.ReserveDeliveryAsync(request);

        Assert.Equal(DurableCommitOutcome.Applied, applied.Outcome);
        Assert.True(completedDuplicate.AcknowledgementAllowed);
        Assert.Equal(DurableDeliveryOutcome.Applied, completedDuplicate.Outcome);
    }

    [PostgreSqlIntegrationFact]
    public async Task ReservationDigestConflictAndQuarantineTimeMismatchFailClosed()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var firstMinute = PostgreSqlTestDatabase.CurrentMinuteUtc();
        var request = new DurableDeliveryReservationRequest(
            database.SourceInstanceId,
            1,
            PostgreSqlTestDatabase.Digest("delivery-a"),
            firstMinute);
        var reservation = await database.Ingestion.ReserveDeliveryAsync(request);

        await Assert.ThrowsAsync<TapDeliveryIdentityConflictException>(() =>
            database.Ingestion.ReserveDeliveryAsync(
                new DurableDeliveryReservationRequest(
                    request.SourceInstanceId,
                    request.TapDeliveryId,
                    PostgreSqlTestDatabase.Digest("delivery-b"),
                    firstMinute)));
        await Assert.ThrowsAsync<TapDeliveryReservationMismatchException>(() =>
            database.Ingestion.CommitQuarantineAsync(
                reservation,
                new DurableQuarantine(
                    request.SourceInstanceId,
                    request.TapDeliveryId,
                    request.DeliveryDigest,
                    DurableQuarantineReason.InvalidValue,
                    checked(firstMinute + 1))));

        var result = await database.Ingestion.CommitQuarantineAsync(
            reservation,
            new DurableQuarantine(
                request.SourceInstanceId,
                request.TapDeliveryId,
                request.DeliveryDigest,
                DurableQuarantineReason.InvalidValue,
                firstMinute));
        Assert.Equal(DurableCommitOutcome.Quarantined, result.Outcome);
        Assert.True(result.AcknowledgementAllowed);
    }

    [PostgreSqlIntegrationFact]
    public async Task RecordStaleNoOpRechecksProofAndFailedProofLeavesReservationPending()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("validated-record-no-op");
        var current = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            1,
            account,
            0,
            "current-record",
            revisionOrdinal: 3);
        Assert.Equal(
            DurableCommitOutcome.Applied,
            (await database.Ingestion.CommitAsync(
                PostgreSqlTestDatabase.Commit(current, PostgreSqlTestDatabase.State(account, 0)))).Outcome);

        var stale = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            2,
            account,
            0,
            "stale-record",
            revisionOrdinal: 2);
        var staleReservation = await ReserveAsync(database, stale);
        var noOp = await database.Ingestion.CommitValidatedNoOpAsync(
            staleReservation,
            new DurableValidatedNoOp(stale, ValidatedNoOpReason.RecordRevisionAlreadyObserved));
        Assert.Equal(DurableCommitOutcome.ValidatedNoOp, noOp.Outcome);

        var future = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            3,
            account,
            0,
            "future-record",
            revisionOrdinal: 4);
        var futureReservation = await ReserveAsync(database, future);
        await Assert.ThrowsAsync<ValidatedNoOpProofFailedException>(() =>
            database.Ingestion.CommitValidatedNoOpAsync(
                futureReservation,
                new DurableValidatedNoOp(future, ValidatedNoOpReason.RecordRevisionAlreadyObserved)));

        Assert.Equal(
            (short)DurableDeliveryOutcome.Pending,
            await database.ScalarAsync<short>(
                "SELECT outcome FROM skypulse.tap_delivery WHERE delivery_id = 3;"));
        Assert.Equal(
            0,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.semantic_event WHERE semantic_digest = @digest;",
                new NpgsqlParameter("digest", NpgsqlDbType.Bytea)
                {
                    Value = Convert.FromHexString(future.SemanticDigest),
                }));
    }

    [PostgreSqlIntegrationFact]
    public async Task RepositoryWideRevisionProofAndDesiredProjectionReadAreExact()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("repository-high-water");
        var current = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            1,
            account,
            0,
            "repository-high-water-current",
            recordKey: "record-a",
            revisionOrdinal: 3);
        var minute = current.ObservedAtMinuteUtc;
        var state = new AccountStateMutation(
            account,
            expectedVersion: 0,
            nextVersion: 1,
            DurableAccountLifecycle.Active,
            repositoryGeneration: 0,
            completedSyncRevision: PostgreSqlTestDatabase.Revision(1),
            synchronizationComplete: true,
            lastActivityMinuteUtc: minute,
            currentPostCount: 1,
            currentFollowingCount: 0,
            currentFollowerCount: 0,
            lastAppliedRevision: PostgreSqlTestDatabase.Revision(3));
        var desired = PostgreSqlTestDatabase.Projection(account, 1, projectionCutMinuteUtc: minute);
        await database.Ingestion.CommitAsync(PostgreSqlTestDatabase.Commit(current, state, desired));

        var planningProjection = Assert.IsType<ProjectionSnapshot>(
            await database.Planning.ReadDesiredProjectionAsync(account));
        Assert.Equal(desired, planningProjection);
        Assert.Equal(
            PostgreSqlTestDatabase.Revision(3),
            Assert.IsType<AccountStateSnapshot>(await database.Planning.ReadAccountAsync(account)).LastAppliedRevision);

        var staleOtherRecord = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            2,
            account,
            0,
            "repository-high-water-stale",
            recordKey: "record-b",
            revisionOrdinal: 2);
        var result = await database.Ingestion.CommitValidatedNoOpAsync(
            await ReserveAsync(database, staleOtherRecord),
            new DurableValidatedNoOp(staleOtherRecord, ValidatedNoOpReason.RepositoryRevisionAlreadyApplied));

        Assert.Equal(DurableCommitOutcome.ValidatedNoOp, result.Outcome);
    }

    [PostgreSqlIntegrationFact]
    public async Task RepositoryGenerationAndCompletedSyncNoOpsRequireCurrentProof()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var account = PostgreSqlTestDatabase.Account("validated-repository-no-op");
        var current = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            1,
            account,
            1,
            "generation-one",
            revisionOrdinal: 3);
        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                current,
                PostgreSqlTestDatabase.State(account, 0, repositoryGeneration: 1)));

        var oldGeneration = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            2,
            account,
            0,
            "old-generation",
            revisionOrdinal: 4);
        var generationResult = await database.Ingestion.CommitValidatedNoOpAsync(
            await ReserveAsync(database, oldGeneration),
            new DurableValidatedNoOp(oldGeneration, ValidatedNoOpReason.RepositoryGenerationSuperseded));
        Assert.Equal(DurableCommitOutcome.ValidatedNoOp, generationResult.Outcome);

        var completedRevision = PostgreSqlTestDatabase.Revision(1);
        var sync = new DurableEventEnvelope(
            database.SourceInstanceId,
            3,
            PostgreSqlTestDatabase.Digest("sync-delivery"),
            PostgreSqlTestDatabase.Digest("sync-semantic"),
            account,
            1,
            DurableEventKind.RepositorySync,
            PostgreSqlTestDatabase.CurrentMinuteUtc(),
            repositoryRevision: completedRevision);
        var syncResult = await database.Ingestion.CommitValidatedNoOpAsync(
            await ReserveAsync(database, sync),
            new DurableValidatedNoOp(sync, ValidatedNoOpReason.RepositorySyncRevisionAlreadyCompleted));
        Assert.Equal(DurableCommitOutcome.ValidatedNoOp, syncResult.Outcome);
    }

    [PostgreSqlIntegrationFact]
    public async Task PlanningReadsAndReconciliationDependenciesAreExactBoundedAndGenerationScoped()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var owner = PostgreSqlTestDatabase.Account("planning-owner");
        var affected = new[]
        {
            PostgreSqlTestDatabase.Account("affected-a"),
            PostgreSqlTestDatabase.Account("affected-b"),
        }.Order().ToArray();
        var envelope = PostgreSqlTestDatabase.Envelope(
            database.SourceInstanceId,
            1,
            owner,
            2,
            "planning-state");
        var minute = envelope.ObservedAtMinuteUtc;
        var commit = new DurableIngestionCommit(
            envelope,
            [PostgreSqlTestDatabase.State(owner, 0, repositoryGeneration: 2)],
            records: [PostgreSqlTestDatabase.Record(envelope)],
            activity: [new ActivityMinuteDelta(owner, minute, 2, recordCreates: 1, postCreates: 1)],
            reconciliationDependencies:
            [
                new ReconciliationDependencyMutation(owner, 2, affected[0], ReconciliationDependencyAction.Add),
                new ReconciliationDependencyMutation(owner, 2, affected[1], ReconciliationDependencyAction.Add),
            ]);
        await database.Ingestion.CommitAsync(commit);

        var followEnvelope = new DurableEventEnvelope(
            database.SourceInstanceId,
            2,
            PostgreSqlTestDatabase.Digest("follow-delivery"),
            PostgreSqlTestDatabase.Digest("follow-semantic"),
            owner,
            2,
            DurableEventKind.RecordMutation,
            minute,
            repositoryRevision: PostgreSqlTestDatabase.Revision(2),
            collection: DurableRecordKind.GraphFollow,
            action: DurableRecordAction.Create,
            recordKey: "follow-record",
            cid: "follow-cid",
            targetAccountKey: affected[0]);
        await database.Ingestion.CommitAsync(
            new DurableIngestionCommit(
                followEnvelope,
                [PostgreSqlTestDatabase.State(owner, 1, repositoryGeneration: 2)],
                records: [PostgreSqlTestDatabase.Record(followEnvelope)],
                followPairs: [new FollowPairMutation(owner, affected[0], 1)]));

        var account = Assert.IsType<AccountStateSnapshot>(await database.Planning.ReadAccountAsync(owner));
        var record = Assert.IsType<RecordStateSnapshot>(await database.Planning.ReadRecordAsync(
            owner,
            2,
            DurableRecordKind.FeedPost,
            envelope.RecordKey!));
        var follow = Assert.IsType<FollowPairSnapshot>(await database.Planning.ReadFollowPairAsync(owner, affected[0]));
        var activity = await database.Planning.ReadActivityMinuteBucketsAsync(
            owner,
            2,
            minute,
            minute,
            afterMinuteUtc: null,
            pageSize: 1);
        var activityAggregate = await database.Planning.ReadActivityWindowAggregateAsync(
            owner,
            expectedAccountStateVersion: account.StateVersion,
            repositoryGeneration: account.RepositoryGeneration,
            cutMinuteUtc: minute);
        var dependencies = await database.Planning.ReadReconciliationDependenciesAsync(
            owner,
            2,
            afterAffectedAccountKey: null,
            pageSize: 1);

        Assert.Equal(2, account.StateVersion);
        Assert.Equal(envelope.RepositoryRevision, record.LatestRevision);
        Assert.Equal(1, follow.Multiplicity);
        Assert.Single(activity.Items);
        Assert.False(activity.HasMore);
        Assert.Equal(new ActivityRollingCounts(1, 1, 1), activityAggregate.RecordCreates);
        Assert.Equal(new ActivityRollingCounts(1, 1, 1), activityAggregate.PostCreates);
        Assert.Equal(minute + (24 * 60), activityAggregate.NextExpiryMinuteUtc);
        Assert.Single(dependencies.AffectedAccountKeys);
        Assert.True(dependencies.HasMore);
        Assert.Equal(affected[0], dependencies.AffectedAccountKeys[0]);
        await Assert.ThrowsAsync<PlanningStateChangedException>(() =>
            database.Planning.ReadActivityMinuteBucketsAsync(
                owner,
                1,
                minute,
                minute,
                afterMinuteUtc: null,
                pageSize: 1));
        var changed = await Assert.ThrowsAsync<PlanningStateChangedException>(() =>
            database.Planning.ReadActivityWindowAggregateAsync(
                owner,
                expectedAccountStateVersion: account.StateVersion - 1,
                repositoryGeneration: account.RepositoryGeneration,
                cutMinuteUtc: minute));
        Assert.Equal(account.StateVersion - 1, changed.ExpectedAccountStateVersion);
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
}
