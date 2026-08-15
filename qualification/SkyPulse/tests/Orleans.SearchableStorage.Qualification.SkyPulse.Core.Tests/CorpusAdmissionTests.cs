using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tests;

public sealed class CorpusAdmissionTests
{
    [Fact]
    public void AdmissionUsesAnExactFrozenPrefixInsteadOfArrivalOrder()
    {
        var keys = OrderedKeys(5);
        var allowlist = FrozenCorpusAllowlist.FromCanonicalOrder(keys);
        var admission = allowlist.CreateAdmission(new CappedCorpusProfile("test-3", 3));

        Assert.Equal(3, admission.Count);
        Assert.Equal(keys.Take(3), admission);
        Assert.All(keys.Take(3), key => Assert.True(admission.IsAdmitted(key)));
        Assert.All(keys.Skip(3), key => Assert.False(admission.IsAdmitted(key)));
    }

    [Fact]
    public void SmallerAndLargerProfilesArePrefixesOfTheSameAllowlist()
    {
        var allowlist = FrozenCorpusAllowlist.FromCanonicalOrder(OrderedKeys(6));
        var small = allowlist.CreateAdmission(new CappedCorpusProfile("test-2", 2));
        var large = allowlist.CreateAdmission(new CappedCorpusProfile("test-5", 5));

        Assert.True(small.IsPrefixOf(large));
        Assert.False(large.IsPrefixOf(small));
        Assert.Equal(small, large.Take(small.Count));
    }

    [Fact]
    public void IdenticalKeysFromADifferentFrozenAllowlistAreNotTreatedAsTheSameCorpus()
    {
        var keys = OrderedKeys(6);
        var first = FrozenCorpusAllowlist.FromCanonicalOrder(keys)
            .CreateAdmission(new CappedCorpusProfile("first", 2));
        var second = FrozenCorpusAllowlist.FromCanonicalOrder(keys.Take(5))
            .CreateAdmission(new CappedCorpusProfile("second", 5));

        Assert.False(first.IsPrefixOf(second));
    }

    [Fact]
    public void StandardProfilesDefineExactOneAndTenMillionCaps()
    {
        Assert.Equal(1_000_000, SkyPulseProfiles.OneMillion.MaximumAccounts);
        Assert.Equal(10_000_000, SkyPulseProfiles.TenMillion.MaximumAccounts);
        Assert.True(
            SkyPulseProfiles.OneMillion.MaximumAccounts
            < SkyPulseProfiles.TenMillion.MaximumAccounts);
    }

    [Fact]
    public void FrozenAllowlistCopiesTheCallerCollection()
    {
        var keys = OrderedKeys(3).ToArray();
        var expectedFirst = keys[0];
        var allowlist = FrozenCorpusAllowlist.FromCanonicalOrder(keys);
        keys[0] = AccountKey.FromDid("did:plc:replacement");

        var admission = allowlist.CreateAdmission(new CappedCorpusProfile("test", 1));

        Assert.Equal(expectedFirst, admission[0]);
    }

    [Fact]
    public void AdmissionBeyondFrozenPopulationFailsClosed()
    {
        var allowlist = FrozenCorpusAllowlist.FromCanonicalOrder(OrderedKeys(2));

        Assert.Throws<InvalidOperationException>(
            () => allowlist.CreateAdmission(new CappedCorpusProfile("test-3", 3)));
    }

    [Fact]
    public void ProjectionCreationOutsideThePrefixCanBeRejectedExplicitly()
    {
        var keys = OrderedKeys(3);
        var admission = FrozenCorpusAllowlist.FromCanonicalOrder(keys)
            .CreateAdmission(new CappedCorpusProfile("test-2", 2));

        admission.EnsureAdmitted(keys[0]);
        Assert.Throws<InvalidOperationException>(() => admission.EnsureAdmitted(keys[2]));
        Assert.Throws<ArgumentException>(() => admission.EnsureAdmitted(default));
    }

    [Fact]
    public void ProjectionFactoryMakesAdmissionMandatory()
    {
        var keys = OrderedKeys(2);
        var admission = FrozenCorpusAllowlist.FromCanonicalOrder(keys)
            .CreateAdmission(new CappedCorpusProfile("test-1", 1));

        var admitted = admission.CreateProjection(
            keys[0],
            0,
            new RollingWindowCounts(0, 0, 0),
            new RollingWindowCounts(0, 0, 0),
            new RollingWindowCounts(0, 0, 0),
            0,
            0,
            0,
            new RollingWindowCounts(0, 0, 0),
            0);

        Assert.Equal(keys[0], admitted.AccountKey);
        Assert.Throws<InvalidOperationException>(
            () => admission.CreateProjection(
                keys[1],
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

    [Fact]
    public void CanonicalAllowlistFingerprintIsStable()
    {
        var keys = OrderedKeys(4);

        var first = FrozenCorpusAllowlist.FromCanonicalOrder(keys);
        var second = FrozenCorpusAllowlist.FromCanonicalOrder(keys.ToArray());

        Assert.Equal(first.Fingerprint, second.Fingerprint);
        Assert.Equal(AccountKey.TextLength, first.Fingerprint.Length);
    }

    [Fact]
    public void EmptyUnsortedDuplicateOrInvalidAllowlistsFailClosed()
    {
        var keys = OrderedKeys(3);

        Assert.Throws<ArgumentException>(
            () => FrozenCorpusAllowlist.FromCanonicalOrder(Array.Empty<AccountKey>()));
        Assert.Throws<ArgumentException>(
            () => FrozenCorpusAllowlist.FromCanonicalOrder([keys[1], keys[0]]));
        Assert.Throws<ArgumentException>(
            () => FrozenCorpusAllowlist.FromCanonicalOrder([keys[0], keys[0]]));
        Assert.Throws<ArgumentException>(
            () => FrozenCorpusAllowlist.FromCanonicalOrder([default]));
    }

    [Theory]
    [InlineData(null, 1)]
    [InlineData("", 1)]
    [InlineData(" ", 1)]
    [InlineData(" padded", 1)]
    [InlineData("padded ", 1)]
    [InlineData("profile", 0)]
    [InlineData("profile", -1)]
    public void InvalidProfileFailsClosed(string? name, int maximumAccounts)
    {
        Assert.ThrowsAny<ArgumentException>(
            () => new CappedCorpusProfile(name!, maximumAccounts));
    }

    private static AccountKey[] OrderedKeys(int count)
    {
        return Enumerable.Range(0, count)
            .Select(index => AccountKey.FromDid($"did:plc:account-{index}"))
            .Order()
            .ToArray();
    }
}
