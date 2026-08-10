namespace Orleans.SearchableStorage.Benchmarks;

public sealed class DeterministicDataTests
{
    private static readonly DatasetSpec GoldenDataset = new()
    {
        Id = "golden-v1",
        Seed = 0x0123456789ABCDEF,
        RecordCount = 1_000_000_000,
        ExactValueCardinality = 97,
        RangeValueCardinality = 1_000_000,
        PayloadBytes = 16,
    };

    [Fact]
    public void GrainKeyHasStableFixedWidthEncoding()
    {
        Assert.Equal("record-000000000042", DeterministicData.GetGrainKey(42));
    }

    [Fact]
    public void StateGenerationMatchesV1GoldenVector()
    {
        var state = DeterministicData.CreateState(GoldenDataset, ordinal: 42, revision: 3);

        Assert.Equal("exact-0000000062", state.ExactValue);
        Assert.Equal(62_184, state.RangeValue);
        Assert.Equal(3, state.Revision);
        Assert.Equal("a8dec8ccc9ce6965abf504ac81967192", Convert.ToHexStringLower(state.Payload));
    }

    [Theory]
    [InlineData(0, 318_897_140)]
    [InlineData(1, 543_341_395)]
    [InlineData(2, 90_062_529)]
    [InlineData(999_999_999_999, 474_092_996)]
    public void UniformSelectionMatchesV1GoldenVectors(long sequence, long expectedOrdinal)
    {
        var workload = new WorkloadSpec { Id = "golden", KeyDistribution = new KeyDistributionSpec() };

        var ordinal = DeterministicData.SelectOrdinal(
            GoldenDataset,
            workload,
            sequence,
            salt: 0x243F6A8885A308D3);

        Assert.Equal(expectedOrdinal, ordinal);
    }

    [Fact]
    public void GenerationIsRandomAccessAndIndependentOfPriorOrdinals()
    {
        var direct = DeterministicData.CreateState(GoldenDataset, ordinal: 999_999_999, revision: 7);
        _ = DeterministicData.CreateState(GoldenDataset, ordinal: 1, revision: 1);
        var repeated = DeterministicData.CreateState(GoldenDataset, ordinal: 999_999_999, revision: 7);

        Assert.Equal(direct.ExactValue, repeated.ExactValue);
        Assert.Equal(direct.RangeValue, repeated.RangeValue);
        Assert.Equal(direct.Payload, repeated.Payload);
    }

    [Theory]
    [InlineData(0, "Upsert")]
    [InlineData(1, "Read")]
    [InlineData(5, "ExactQuery")]
    [InlineData(13, "RangeQuery")]
    public void OperationSelectionMatchesV1GoldenVectors(long sequence, string expected)
    {
        var mix = new OperationMixSpec
        {
            Upsert = 20,
            Read = 50,
            ExactQuery = 20,
            RangeQuery = 10,
            Clear = 0,
        };

        Assert.Equal(expected, OperationSelector.Select(mix, GoldenDataset.Seed, sequence).ToString());
    }
}
