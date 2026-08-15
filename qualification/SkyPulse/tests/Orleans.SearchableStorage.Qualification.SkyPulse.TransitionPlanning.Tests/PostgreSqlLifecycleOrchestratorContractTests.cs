namespace Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning.Tests;

public sealed class PostgreSqlLifecycleOrchestratorContractTests
{
    [Fact]
    public void DurableWorkIsGenerationScopedAndStartsWithoutCompletingDelivery()
    {
        var sql = PostgreSqlLifecycleOrchestrator.InsertWorkSql;

        Assert.Contains("lifecycle_transition_work", sql, StringComparison.Ordinal);
        Assert.Contains("repository_generation", sql, StringComparison.Ordinal);
        Assert.Contains("semantic_digest", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT DO NOTHING", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("tap_delivery", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("committed_at_utc", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryPotentiallyLargeCleanupReadIsExplicitlyBounded()
    {
        Assert.Contains("LIMIT @page_size", PostgreSqlLifecycleOrchestrator.ReadOutgoingFollowPageSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @page_size", PostgreSqlLifecycleOrchestrator.DeleteOwnedRecordPageSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @page_size", PostgreSqlLifecycleOrchestrator.DeleteOwnedActivityPageSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @page_size", PostgreSqlLifecycleOrchestrator.ReadDependencyPageSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY target_account_key", PostgreSqlLifecycleOrchestrator.ReadOutgoingFollowPageSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY repository_generation, collection, record_key", PostgreSqlLifecycleOrchestrator.DeleteOwnedRecordPageSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY owner_repository_generation, affected_account_key", PostgreSqlLifecycleOrchestrator.ReadDependencyPageSql, StringComparison.Ordinal);
        Assert.Contains("repository_generation <= @repository_generation", PostgreSqlLifecycleOrchestrator.DeleteOwnedRecordPageSql, StringComparison.Ordinal);
        Assert.Contains("repository_generation <= @repository_generation", PostgreSqlLifecycleOrchestrator.DeleteOwnedActivityPageSql, StringComparison.Ordinal);
        Assert.Contains("owner_repository_generation <= @repository_generation", PostgreSqlLifecycleOrchestrator.ReadDependencyPageSql, StringComparison.Ordinal);
    }

    [Fact]
    public void AdvancePreflightsTheCompletePageBarrierBeforeTakingTheExactWorkRowLock()
    {
        Assert.Contains("account_key", PostgreSqlLifecycleOrchestrator.ReadWorkSql, StringComparison.Ordinal);
        Assert.DoesNotContain("FOR UPDATE", PostgreSqlLifecycleOrchestrator.ReadWorkSql, StringComparison.Ordinal);
        Assert.Contains("FOR UPDATE", PostgreSqlLifecycleOrchestrator.ReadWorkForUpdateSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @page_size", PostgreSqlLifecycleOrchestrator.ReadOutgoingFollowBarrierPageSql, StringComparison.Ordinal);
        Assert.DoesNotContain("FOR UPDATE", PostgreSqlLifecycleOrchestrator.ReadOutgoingFollowBarrierPageSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @page_size", PostgreSqlLifecycleOrchestrator.ReadDependencyBarrierPageSql, StringComparison.Ordinal);
        Assert.DoesNotContain("FOR UPDATE", PostgreSqlLifecycleOrchestrator.ReadDependencyBarrierPageSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProjectionAggregateUsesTheExactInclusiveOneSevenAndThirtyDayWindows()
    {
        var sql = PostgreSqlLifecycleOrchestrator.ReadProjectionAggregateSql;

        Assert.Contains("@first_one_day_minute", sql, StringComparison.Ordinal);
        Assert.Contains("@first_seven_day_minute", sql, StringComparison.Ordinal);
        Assert.Contains("@first_thirty_day_minute", sql, StringComparison.Ordinal);
        Assert.Contains("minute_utc + 1440", sql, StringComparison.Ordinal);
        Assert.Contains("minute_utc + 10080", sql, StringComparison.Ordinal);
        Assert.Contains("minute_utc + 43200", sql, StringComparison.Ordinal);
        Assert.Contains("received_engagement_creates", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1001)]
    public async Task AdvanceRejectsAnUnboundedPageBeforeOpeningPostgreSql(int pageSize)
    {
        await using var dataSource = Npgsql.NpgsqlDataSource.Create("Host=not-opened");
        var orchestrator = new PostgreSqlLifecycleOrchestrator(dataSource);
        var reservation = new Persistence.PostgreSql.DurableDeliveryReservation(
            Guid.Parse("f9fabd2f-6b7d-4187-b188-5f242ad13b4e"),
            tapDeliveryId: 1,
            new string('a', 64),
            firstObservedAtMinuteUtc: 1,
            Persistence.PostgreSql.DurableDeliveryOutcome.Pending);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => orchestrator.AdvanceAsync(reservation, pageSize));
    }
}
