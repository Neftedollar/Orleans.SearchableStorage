namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlCorpusCapacityIntegrationTests
{
    [PostgreSqlIntegrationFact]
    public async Task RuntimeCapacityGrowsMonotonicallyAndResumesFromDurableState()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var store = new PostgreSqlCorpusCapacityStore(database.DataSource);
        var baseProfile = Profile("accounts-1m", 1_000_000, 'a');
        var twoMillion = Profile("accounts-2m", 2_000_000, 'b');
        var threeMillion = Profile("accounts-3m", 3_000_000, 'c');

        var initial = await store.BindBaseAsync(baseProfile);
        var requested = await store.RequestGrowthAsync(twoMillion);
        var duplicate = await store.RequestGrowthAsync(twoMillion);
        var competing = await store.RequestGrowthAsync(threeMillion);

        Assert.Equal(baseProfile, initial.Active);
        Assert.Equal(CorpusGrowthRequestOutcome.Accepted, requested.Outcome);
        Assert.Equal(twoMillion, requested.State.Target);
        Assert.Equal(CorpusGrowthRequestOutcome.AlreadyRequested, duplicate.Outcome);
        Assert.Equal(CorpusGrowthRequestOutcome.GrowthInProgress, competing.Outcome);

        var completed = await store.CompleteGrowthAsync(
            twoMillion,
            requested.State.OperationVersion);
        var rebound = await store.BindBaseAsync(baseProfile);
        var lower = await store.RequestGrowthAsync(baseProfile);

        Assert.Equal(twoMillion, completed.Active);
        Assert.Null(completed.Target);
        Assert.Equal(twoMillion, rebound.Active);
        Assert.Equal(CorpusGrowthRequestOutcome.NonMonotonic, lower.Outcome);
    }

    [PostgreSqlIntegrationFact]
    public async Task RuntimeCapacityRejectsADifferentImmutableBase()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var store = new PostgreSqlCorpusCapacityStore(database.DataSource);
        await store.BindBaseAsync(Profile("accounts-1m", 1_000_000, 'a'));

        await Assert.ThrowsAsync<CorpusCapacityIdentityMismatchException>(() =>
            store.BindBaseAsync(Profile("different-base", 1_000_000, 'b')));
    }

    private static CorpusCapacityProfile Profile(string id, long cap, char digest)
        => new(id, 1, cap, new string(digest, 64));
}
