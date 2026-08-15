using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tests;

public sealed class AccountKeyTests
{
    [Fact]
    public void DidHashIsStableAndDoesNotRetainTheDid()
    {
        var key = AccountKey.FromDid("did:plc:ewvi7nxzyoun6zhxrhs64oiz");

        Assert.Equal(
            "099e4ea96cd62c05a232331859d20c97425f25b21f193068b1abf7b763e40ed1",
            key.ToString());
        Assert.DoesNotContain("plc", key.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("plc:account")]
    [InlineData("did::account")]
    [InlineData("did:PLC:account")]
    [InlineData("did:plc:")]
    [InlineData("did:plc:account name")]
    [InlineData("did:plc:account/path")]
    [InlineData("did:plc:account%2")]
    public void InvalidDidFailsClosed(string? did)
    {
        Assert.ThrowsAny<ArgumentException>(() => AccountKey.FromDid(did!));
    }

    [Fact]
    public void CanonicalTextRoundTrips()
    {
        var expected = AccountKey.FromDid("did:web:example.com");

        Assert.True(AccountKey.TryParse(expected.ToString(), out var parsed));
        Assert.Equal(expected, parsed);
        Assert.Equal(expected, AccountKey.Parse(expected.ToString()));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("00")]
    [InlineData("0000000000000000000000000000000000000000000000000000000000000000")]
    [InlineData("GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")]
    [InlineData("2E5517062791F2EA3125679EA55F8634A15A270187CB45045B75111CC7A1A8DC")]
    public void NonCanonicalOrInvalidKeyTextFailsClosed(string? value)
    {
        Assert.False(AccountKey.TryParse(value, out _));
        Assert.Throws<FormatException>(() => AccountKey.Parse(value!));
    }
}
