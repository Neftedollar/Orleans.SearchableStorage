namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlRetentionIntegrationTests
{
    private static readonly PostgreSqlRetentionPolicy Policy = new(
        completedDeliveryAge: TimeSpan.FromHours(1),
        semanticEventAge: TimeSpan.FromHours(1),
        completedOutboxAge: TimeSpan.FromHours(1),
        quarantineAge: TimeSpan.FromHours(1),
        activityBucketAge: TimeSpan.FromDays(30));

    [PostgreSqlIntegrationFact]
    public async Task CompletedDeliveryRetentionRequiresEvidenceAndHonorsTheBatchBound()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var account = PostgreSqlTestDatabase.Account("delivery-retention");
        for (var version = 1; version <= 3; version++)
        {
            var result = await database.Ingestion.CommitAsync(
                PostgreSqlTestDatabase.Commit(
                    PostgreSqlTestDatabase.Envelope(
                        source,
                        (ulong)version,
                        account,
                        0,
                        $"retention-semantic-{version}",
                        recordKey: $"record-{version}"),
                    PostgreSqlTestDatabase.State(account, version - 1L, currentPostCount: version)));
            Assert.Equal(DurableCommitOutcome.Applied, result.Outcome);
        }

        await database.ExecuteAsync(
            """
            INSERT INTO skypulse.tap_delivery (
                source_instance_id, delivery_id, delivery_digest, semantic_digest, account_key,
                observed_at_minute_utc, outcome, committed_at_utc)
            VALUES (@source, 4, @digest, NULL, NULL, @minute, 0, NULL);
            """,
            new NpgsqlParameter("source", NpgsqlDbType.Uuid) { Value = source },
            new NpgsqlParameter("digest", NpgsqlDbType.Bytea)
            {
                Value = Convert.FromHexString(PostgreSqlTestDatabase.Digest("pending-delivery")),
            },
            new NpgsqlParameter("minute", NpgsqlDbType.Bigint)
            {
                Value = PostgreSqlTestDatabase.CurrentMinuteUtc(),
            });
        var retention = new PostgreSqlRetentionStore(database.DataSource, Policy);
        var asOfUtc = DateTimeOffset.UtcNow.AddHours(2);

        await Assert.ThrowsAsync<PostgreSqlRetentionWatermarkException>(
            () => retention.DeleteCompletedDeliveriesAsync(source, 4, asOfUtc, batchSize: 2));
        Assert.Equal(4, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.tap_delivery;"));

        await retention.AdvanceSourceDeliveryWatermarkAsync(source, 4, "integration-test:source-archive-v1");
        var firstBatch = await retention.DeleteCompletedDeliveriesAsync(source, 4, asOfUtc, batchSize: 2);
        var secondBatch = await retention.DeleteCompletedDeliveriesAsync(source, 4, asOfUtc, batchSize: 2);

        Assert.Equal(2, firstBatch.DeletedRows);
        Assert.True(firstBatch.MayHaveMore);
        Assert.Equal(1, secondBatch.DeletedRows);
        Assert.False(secondBatch.MayHaveMore);
        Assert.Equal(1, await database.ScalarAsync<int>("SELECT count(*) FROM skypulse.tap_delivery;"));
        Assert.Equal(
            (short)DurableDeliveryOutcome.Pending,
            await database.ScalarAsync<short>("SELECT outcome FROM skypulse.tap_delivery;"));
    }

    [PostgreSqlIntegrationFact]
    public async Task RecordRetentionDeletesOnlyObsoleteGenerationsInBoundedBatches()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var source = database.SourceInstanceId;
        var account = PostgreSqlTestDatabase.Account("record-retention");
        for (var version = 1; version <= 2; version++)
        {
            await database.Ingestion.CommitAsync(
                PostgreSqlTestDatabase.Commit(
                    PostgreSqlTestDatabase.Envelope(
                        source,
                        (ulong)version,
                        account,
                        0,
                        $"obsolete-record-{version}",
                        recordKey: $"obsolete-{version}"),
                    PostgreSqlTestDatabase.State(account, version - 1L, repositoryGeneration: 0, currentPostCount: version)));
        }

        var tombstoneEnvelope = PostgreSqlTestDatabase.Envelope(
            source,
            3,
            account,
            1,
            "current-tombstone",
            action: DurableRecordAction.Delete,
            recordKey: "current-tombstone");
        await database.Ingestion.CommitAsync(
            PostgreSqlTestDatabase.Commit(
                tombstoneEnvelope,
                PostgreSqlTestDatabase.State(account, 2, repositoryGeneration: 1, currentPostCount: 0)));
        var retention = new PostgreSqlRetentionStore(database.DataSource, Policy);

        var firstBatch = await retention.DeleteObsoleteRecordStateAsync(batchSize: 1);
        var secondBatch = await retention.DeleteObsoleteRecordStateAsync(batchSize: 1);
        var exhausted = await retention.DeleteObsoleteRecordStateAsync(batchSize: 1);

        Assert.Equal(1, firstBatch.DeletedRows);
        Assert.True(firstBatch.MayHaveMore);
        Assert.Equal(1, secondBatch.DeletedRows);
        Assert.True(secondBatch.MayHaveMore);
        Assert.Equal(0, exhausted.DeletedRows);
        Assert.False(exhausted.MayHaveMore);
        Assert.Equal(
            0,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.record_state WHERE repository_generation = 0;"));
        Assert.Equal(
            1,
            await database.ScalarAsync<int>(
                "SELECT count(*) FROM skypulse.record_state WHERE repository_generation = 1 AND is_deleted;"));
    }
}
