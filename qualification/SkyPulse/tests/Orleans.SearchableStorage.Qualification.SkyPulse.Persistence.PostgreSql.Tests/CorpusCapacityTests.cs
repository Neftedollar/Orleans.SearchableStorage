using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class CorpusCapacityTests
{
    [Fact]
    public void CapacityProfileRequiresCanonicalIdentity()
    {
        var profile = new CorpusCapacityProfile("accounts-1m", 1, 1_000_000, new string('a', 64));

        Assert.Equal("accounts-1m", profile.ProfileId);
        Assert.Equal(1_000_000, profile.CorpusCap);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CorpusCapacityProfile("accounts", 1, 0, new string('a', 64)));
        Assert.Throws<ArgumentException>(() =>
            new CorpusCapacityProfile("accounts", 1, 1, new string('A', 64)));
    }

    [Fact]
    public void StoreSqlSerializesOneMonotonicTargetAndUsesExactCompletionCas()
    {
        Assert.Contains("pg_advisory_xact_lock", PostgreSqlCorpusCapacityStore.AcquireLockSql, StringComparison.Ordinal);
        Assert.Contains("target_corpus_cap", PostgreSqlCorpusCapacityStore.RequestGrowthSql, StringComparison.Ordinal);
        Assert.Contains("active_corpus_cap < @corpus_cap", PostgreSqlCorpusCapacityStore.RequestGrowthSql, StringComparison.Ordinal);
        Assert.Contains("operation_version = @expected_version", PostgreSqlCorpusCapacityStore.RequestGrowthSql, StringComparison.Ordinal);
        Assert.Contains("target_profile_id = @profile_id", PostgreSqlCorpusCapacityStore.CompleteGrowthSql, StringComparison.Ordinal);
        Assert.Contains("target_prefix_sha256 = @prefix_sha256", PostgreSqlCorpusCapacityStore.CompleteGrowthSql, StringComparison.Ordinal);
        Assert.Contains("target_profile_id = NULL", PostgreSqlCorpusCapacityStore.CompleteGrowthSql, StringComparison.Ordinal);
        Assert.Contains("synchronization_complete", PostgreSqlCorpusCapacityStore.StatisticsSql, StringComparison.Ordinal);
    }
}
