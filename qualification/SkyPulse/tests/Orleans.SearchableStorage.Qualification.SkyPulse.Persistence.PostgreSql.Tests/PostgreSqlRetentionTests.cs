using Npgsql;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class PostgreSqlRetentionTests
{
    private static readonly PostgreSqlRetentionPolicy Policy = new(
        TimeSpan.FromDays(14),
        TimeSpan.FromDays(30),
        TimeSpan.FromDays(14),
        TimeSpan.FromDays(30),
        TimeSpan.FromDays(31));

    [Fact]
    public void PolicyRequiresExplicitPositiveAgesAndFullRollingWindow()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new PostgreSqlRetentionPolicy(
                TimeSpan.Zero,
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(30)));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new PostgreSqlRetentionPolicy(
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(1),
                TimeSpan.FromDays(30) - TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void EveryCleanupStatementIsExplicitlyBounded()
    {
        var statements = new[]
        {
            PostgreSqlRetentionStore.DeleteCompletedDeliveriesSql,
            PostgreSqlRetentionStore.DeleteQuarantineSql,
            PostgreSqlRetentionStore.DeleteSemanticEventsSql,
            PostgreSqlRetentionStore.DeleteCompletedOutboxSql,
            PostgreSqlRetentionStore.DeleteExpiredActivitySql,
            PostgreSqlRetentionStore.DeleteObsoleteRecordStateSql,
            PostgreSqlRetentionStore.DeleteObsoleteActivitySql,
        };

        Assert.All(statements, static sql => Assert.Contains("LIMIT @batch_size", sql, StringComparison.Ordinal));
        Assert.All(statements, static sql => Assert.Contains("FOR UPDATE OF", sql, StringComparison.Ordinal));
        Assert.All(statements, static sql => Assert.Contains("SKIP LOCKED", sql, StringComparison.Ordinal));
    }

    [Fact]
    public void ReplaySensitiveCleanupRequiresPersistedWatermarks()
    {
        Assert.Contains("source_delivery_retention_watermark", PostgreSqlRetentionStore.DeleteCompletedDeliveriesSql, StringComparison.Ordinal);
        Assert.Contains("safe_delivery_id_inclusive >= @safe_delivery_id_inclusive", PostgreSqlRetentionStore.DeleteCompletedDeliveriesSql, StringComparison.Ordinal);
        Assert.Contains("source_delivery_retention_watermark", PostgreSqlRetentionStore.DeleteQuarantineSql, StringComparison.Ordinal);
        Assert.Contains("semantic_event_retention_watermark", PostgreSqlRetentionStore.DeleteSemanticEventsSql, StringComparison.Ordinal);
        Assert.Contains("safe_observed_minute_utc >= @safe_observed_minute_utc", PostgreSqlRetentionStore.DeleteSemanticEventsSql, StringComparison.Ordinal);
        Assert.Contains("activity_retention_watermark", PostgreSqlRetentionStore.DeleteExpiredActivitySql, StringComparison.Ordinal);
        Assert.Contains("safe_minute_utc >= @safe_minute_utc", PostgreSqlRetentionStore.DeleteExpiredActivitySql, StringComparison.Ordinal);
    }

    [Fact]
    public void WatermarksCanOnlyAdvanceMonotonicallyAndRetainEvidenceReference()
    {
        var statements = new[]
        {
            PostgreSqlRetentionStore.AdvanceSourceDeliveryWatermarkSql,
            PostgreSqlRetentionStore.AdvanceSemanticEventWatermarkSql,
            PostgreSqlRetentionStore.AdvanceActivityWatermarkSql,
        };

        Assert.All(statements, static sql => Assert.Contains("evidence_reference", sql, StringComparison.Ordinal));
        Assert.Contains("safe_delivery_id_inclusive <= EXCLUDED.safe_delivery_id_inclusive", statements[0], StringComparison.Ordinal);
        Assert.Contains("safe_observed_minute_utc <= EXCLUDED.safe_observed_minute_utc", statements[1], StringComparison.Ordinal);
        Assert.Contains("safe_minute_utc <= EXCLUDED.safe_minute_utc", statements[2], StringComparison.Ordinal);
    }

    [Fact]
    public void PendingWorkAndCurrentRecordTombstonesAreNotCleanupCandidates()
    {
        Assert.Contains("delivery.outcome <> 0", PostgreSqlRetentionStore.DeleteCompletedDeliveriesSql, StringComparison.Ordinal);
        Assert.Contains("NOT EXISTS", PostgreSqlRetentionStore.DeleteCompletedDeliveriesSql, StringComparison.Ordinal);
        Assert.Contains("FROM skypulse.quarantine", PostgreSqlRetentionStore.DeleteCompletedDeliveriesSql, StringComparison.Ordinal);
        Assert.Contains("outbox.completed_at_utc IS NOT NULL", PostgreSqlRetentionStore.DeleteCompletedOutboxSql, StringComparison.Ordinal);
        Assert.Contains("outbox.operation = 1 AND published.projection_version >= outbox.projection_version", PostgreSqlRetentionStore.DeleteCompletedOutboxSql, StringComparison.Ordinal);
        Assert.Contains("outbox.operation = 2", PostgreSqlRetentionStore.DeleteCompletedOutboxSql, StringComparison.Ordinal);
        Assert.Contains("published.projection_version >= outbox.projection_version", PostgreSqlRetentionStore.DeleteCompletedOutboxSql, StringComparison.Ordinal);
        Assert.DoesNotContain("published.account_key IS NULL", PostgreSqlRetentionStore.DeleteCompletedOutboxSql, StringComparison.Ordinal);
        Assert.Contains("record.repository_generation < account.repository_generation", PostgreSqlRetentionStore.DeleteObsoleteRecordStateSql, StringComparison.Ordinal);
        Assert.DoesNotContain("is_deleted", PostgreSqlRetentionStore.DeleteObsoleteRecordStateSql, StringComparison.Ordinal);
    }

    [Fact]
    public void CurrentActivityRequiresBothProjectionAndWindowProof()
    {
        var sql = PostgreSqlRetentionStore.DeleteExpiredActivitySql;

        Assert.Contains("account.repository_generation = bucket.repository_generation", sql, StringComparison.Ordinal);
        Assert.Contains("projection.projection_version = account.state_version", sql, StringComparison.Ordinal);
        Assert.Contains("bucket.minute_utc <= projection.projection_cut_minute_utc - @longest_window_minutes", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QuarantineLifecycleIsLinkedToItsCompletedDelivery()
    {
        var migration = PostgreSqlSchema.Migrations[0].Sql;

        Assert.Contains("REFERENCES skypulse.tap_delivery (source_instance_id, delivery_id)", migration, StringComparison.Ordinal);
        Assert.Contains("ON DELETE CASCADE", migration, StringComparison.Ordinal);
        Assert.Contains("delivery.outcome = 3", PostgreSqlRetentionStore.DeleteQuarantineSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1001)]
    public async Task CleanupRejectsUnboundedBatchBeforeOpeningConnection(int batchSize)
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=not-opened");
        var store = new PostgreSqlRetentionStore(dataSource, Policy);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => store.DeleteCompletedOutboxAsync(DateTimeOffset.UnixEpoch.AddDays(100), batchSize));
    }

    [Fact]
    public async Task CleanupRequiresUtcBeforeOpeningConnection()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=not-opened");
        var store = new PostgreSqlRetentionStore(dataSource, Policy);
        var nonUtc = new DateTimeOffset(2026, 8, 14, 12, 0, 0, TimeSpan.FromHours(3));

        await Assert.ThrowsAsync<ArgumentException>(
            () => store.DeleteCompletedOutboxAsync(nonUtc, 1));
    }

    [Fact]
    public void BatchResultDoesNotOverstateRemainingWork()
    {
        Assert.True(new PostgreSqlRetentionBatchResult(10, 10).MayHaveMore);
        Assert.False(new PostgreSqlRetentionBatchResult(9, 10).MayHaveMore);
    }
}
