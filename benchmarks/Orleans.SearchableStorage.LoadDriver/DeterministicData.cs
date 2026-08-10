namespace Orleans.SearchableStorage.Benchmarks;

internal static class DeterministicData
{
    private const ulong GoldenRatio = 0x9E3779B97F4A7C15UL;
    private const ulong ClientSalt = 0xD1B54A32D192ED03UL;

    public static ulong DeriveClientSeed(ulong datasetSeed, int clientOrdinal)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(clientOrdinal);
        return Mix(datasetSeed ^ unchecked(((ulong)clientOrdinal + 1) * ClientSalt));
    }

    public static string GetGrainKey(long ordinal)
    {
        return string.Create(
            19,
            ordinal,
            static (span, value) =>
            {
                "record-".AsSpan().CopyTo(span);
                _ = value.TryFormat(span[7..], out _, "D12", provider: null);
            });
    }

    public static BenchmarkRecordState CreateState(DatasetSpec dataset, long ordinal, long revision)
    {
        var identity = Mix(dataset.Seed + unchecked((ulong)ordinal * GoldenRatio));
        var mutation = Mix(identity ^ unchecked((ulong)revision * 0xD1342543DE82EF95UL));
        return new BenchmarkRecordState
        {
            ExactValue = GetExactValue(dataset, ordinal),
            RangeValue = GetRangeValue(dataset, ordinal),
            Revision = revision,
            Payload = CreatePayload(dataset.PayloadBytes, mutation),
        };
    }

    public static string GetExactValue(DatasetSpec dataset, long ordinal)
    {
        var value = Mix(dataset.Seed + unchecked((ulong)ordinal * GoldenRatio)) % (uint)dataset.ExactValueCardinality;
        return $"exact-{value:D10}";
    }

    public static int GetRangeValue(DatasetSpec dataset, long ordinal)
    {
        var identity = Mix(dataset.Seed + unchecked((ulong)ordinal * GoldenRatio));
        return (int)(identity % (uint)dataset.RangeValueCardinality);
    }

    public static long SelectOrdinal(DatasetSpec dataset, WorkloadSpec workload, long sequence, ulong salt)
    {
        var value = Mix(dataset.Seed ^ unchecked((ulong)sequence * GoldenRatio) ^ salt);
        if (workload.KeyDistribution.Kind is KeyDistributionKind.Uniform)
        {
            return (long)(value % (ulong)dataset.RecordCount);
        }

        var threshold = (ulong)(workload.KeyDistribution.HotsetProbability * ulong.MaxValue);
        var hotCount = Math.Clamp(
            (long)Math.Ceiling(dataset.RecordCount * workload.KeyDistribution.HotsetFraction),
            1,
            dataset.RecordCount);
        if (value <= threshold)
        {
            return (long)(Mix(value ^ 0xA0761D6478BD642FUL) % (ulong)hotCount);
        }

        var coldCount = dataset.RecordCount - hotCount;
        return coldCount == 0
            ? (long)(Mix(value) % (ulong)hotCount)
            : hotCount + (long)(Mix(value ^ 0xE7037ED1A0B428DBUL) % (ulong)coldCount);
    }

    public static int SelectRangeStart(
        DatasetSpec dataset,
        WorkloadSpec workload,
        long sequence,
        int clientOrdinal = 0)
    {
        var rangeWindow = workload.GetRangeWindow(dataset);
        var maximumStart = Math.Max(0, dataset.RangeValueCardinality - rangeWindow);
        if (maximumStart == 0)
        {
            return 0;
        }

        var clientSeed = DeriveClientSeed(dataset.Seed, clientOrdinal);
        return (int)(Mix(clientSeed ^ unchecked((ulong)sequence * 0xA24BAED4963EE407UL)) % (uint)(maximumStart + 1));
    }

    public static ulong Mix(ulong value)
    {
        value ^= value >> 30;
        value *= 0xBF58476D1CE4E5B9UL;
        value ^= value >> 27;
        value *= 0x94D049BB133111EBUL;
        return value ^ (value >> 31);
    }

    private static byte[] CreatePayload(int length, ulong seed)
    {
        var result = new byte[length];
        for (var index = 0; index < result.Length; index++)
        {
            if ((index & 7) == 0)
            {
                seed = Mix(seed + GoldenRatio);
            }

            result[index] = (byte)(seed >> ((index & 7) * 8));
        }

        return result;
    }
}
