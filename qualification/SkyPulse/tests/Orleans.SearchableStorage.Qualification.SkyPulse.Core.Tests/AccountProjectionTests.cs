using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tests;

public sealed class AccountProjectionTests
{
    [Fact]
    public void ProjectionContainsOnlyTheAgreedKeyAndIndexedMetadata()
    {
        var propertyNames = typeof(AccountProjection)
            .GetProperties()
            .Select(static property => property.Name)
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            [
                "AccountKey",
                "CreatedRecordCount1Day",
                "CreatedRecordCount30Days",
                "CreatedRecordCount7Days",
                "CurrentFollowerCount",
                "CurrentFollowingCount",
                "CurrentPostCount",
                "DeletedRecordCount1Day",
                "DeletedRecordCount30Days",
                "DeletedRecordCount7Days",
                "LastActivityMinuteUtc",
                "PostCreates1Day",
                "PostCreates30Days",
                "PostCreates7Days",
                "ReceivedEngagementCreates30Days",
                "UpdatedRecordCount1Day",
                "UpdatedRecordCount30Days",
                "UpdatedRecordCount7Days",
            ],
            propertyNames);
    }

    [Fact]
    public void ProjectionPreservesAllAgreedCounters()
    {
        var projection = Projection();

        Assert.Equal(29_700_001, projection.LastActivityMinuteUtc);
        Assert.Equal(10, projection.CreatedRecordCount1Day);
        Assert.Equal(20, projection.CreatedRecordCount7Days);
        Assert.Equal(30, projection.CreatedRecordCount30Days);
        Assert.Equal(4, projection.UpdatedRecordCount1Day);
        Assert.Equal(8, projection.UpdatedRecordCount7Days);
        Assert.Equal(12, projection.UpdatedRecordCount30Days);
        Assert.Equal(1, projection.DeletedRecordCount1Day);
        Assert.Equal(2, projection.DeletedRecordCount7Days);
        Assert.Equal(3, projection.DeletedRecordCount30Days);
        Assert.Equal(100, projection.CurrentPostCount);
        Assert.Equal(200, projection.CurrentFollowingCount);
        Assert.Equal(300, projection.CurrentFollowerCount);
        Assert.Equal(5, projection.PostCreates1Day);
        Assert.Equal(10, projection.PostCreates7Days);
        Assert.Equal(15, projection.PostCreates30Days);
        Assert.Equal(40, projection.ReceivedEngagementCreates30Days);
    }

    [Theory]
    [InlineData(-1, 0, 0, 0, 0)]
    [InlineData(0, -1, 0, 0, 0)]
    [InlineData(0, 0, -1, 0, 0)]
    [InlineData(0, 0, 0, -1, 0)]
    [InlineData(0, 0, 0, 0, -1)]
    public void NegativeScalarValuesFailClosed(
        long lastActivity,
        long posts,
        long following,
        long followers,
        long engagements)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => Projection(lastActivity, posts, following, followers, engagements));
    }

    [Fact]
    public void MissingAccountKeyFailsClosed()
    {
        var admission = Admission(AccountKey.FromDid("did:plc:account"));

        Assert.Throws<ArgumentException>(
            () => admission.CreateProjection(
                default,
                0,
                new RollingWindowCounts(0, 0, 0),
                new RollingWindowCounts(0, 0, 0),
                new RollingWindowCounts(0, 0, 0),
                0,
                0,
                0,
                new RollingWindowCounts(0, 0, 0),
                0));
    }

    [Theory]
    [InlineData(-1, 0, 0)]
    [InlineData(0, -1, 0)]
    [InlineData(0, 0, -1)]
    [InlineData(2, 1, 2)]
    [InlineData(1, 3, 2)]
    public void InvalidRollingWindowCountsFailClosed(long oneDay, long sevenDays, long thirtyDays)
    {
        Assert.ThrowsAny<ArgumentException>(
            () => new RollingWindowCounts(oneDay, sevenDays, thirtyDays));
    }

    [Theory]
    [InlineData(11, 20, 30)]
    [InlineData(10, 21, 30)]
    [InlineData(10, 20, 31)]
    public void PostCreatesMustBeASubsetOfAllRecordCreates(
        long oneDay,
        long sevenDays,
        long thirtyDays)
    {
        var accountKey = AccountKey.FromDid("did:plc:account");
        var admission = Admission(accountKey);

        Assert.Throws<ArgumentException>(
            () => admission.CreateProjection(
                accountKey,
                0,
                new RollingWindowCounts(10, 20, 30),
                new RollingWindowCounts(0, 0, 0),
                new RollingWindowCounts(0, 0, 0),
                0,
                0,
                0,
                new RollingWindowCounts(oneDay, sevenDays, thirtyDays),
                0));
    }

    private static AccountProjection Projection(
        long lastActivity = 29_700_001,
        long posts = 100,
        long following = 200,
        long followers = 300,
        long engagements = 40)
    {
        var accountKey = AccountKey.FromDid("did:plc:account");
        return Admission(accountKey).CreateProjection(
            accountKey,
            lastActivity,
            new RollingWindowCounts(10, 20, 30),
            new RollingWindowCounts(4, 8, 12),
            new RollingWindowCounts(1, 2, 3),
            posts,
            following,
            followers,
            new RollingWindowCounts(5, 10, 15),
            engagements);
    }

    private static CappedCorpusAdmission Admission(AccountKey accountKey)
    {
        return FrozenCorpusAllowlist.FromCanonicalOrder([accountKey])
            .CreateAdmission(new CappedCorpusProfile("test", 1));
    }
}
