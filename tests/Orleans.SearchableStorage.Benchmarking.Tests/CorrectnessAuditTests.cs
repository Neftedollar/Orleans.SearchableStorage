namespace Orleans.SearchableStorage.Benchmarks;

public sealed class CorrectnessAuditTests
{
    [Fact]
    public void PointAuditPlanIsDeterministicAndSelectsDistinctOrdinals()
    {
        var dataset = new DatasetSpec
        {
            Id = "audit",
            Seed = 42,
            RecordCount = 10_000,
        };

        var first = Enumerable.Range(0, 1_000)
            .Select(index => CorrectnessAuditPlan.SelectPointOrdinal(dataset, index, 1_000))
            .ToArray();
        var second = Enumerable.Range(0, 1_000)
            .Select(index => CorrectnessAuditPlan.SelectPointOrdinal(dataset, index, 1_000))
            .ToArray();

        Assert.Equal(first, second);
        Assert.Equal(first.Length, first.Distinct().Count());
        Assert.All(first, ordinal => Assert.InRange(ordinal, 0, dataset.RecordCount - 1));
    }

    [Fact]
    public void StateAuditChecksEveryPersistedField()
    {
        var expected = new BenchmarkRecordState
        {
            ExactValue = "exact-1",
            RangeValue = 42,
            Revision = 7,
            Payload = [1, 2, 3],
        };

        Assert.True(CorrectnessAuditPlan.StateEquals(expected, Clone(expected)));
        Assert.False(CorrectnessAuditPlan.StateEquals(expected, null));
        Assert.False(CorrectnessAuditPlan.StateEquals(expected, Clone(expected, exactValue: "exact-2")));
        Assert.False(CorrectnessAuditPlan.StateEquals(expected, Clone(expected, rangeValue: 43)));
        Assert.False(CorrectnessAuditPlan.StateEquals(expected, Clone(expected, revision: 8)));
        Assert.False(CorrectnessAuditPlan.StateEquals(expected, Clone(expected, payload: [1, 2, 4])));
    }

    [Fact]
    public async Task FinalAuditAcceptsDeterministicCurrentRevisionsAndClearedRecords()
    {
        var spec = CreateFinalAuditSpec(pointSampleCount: 8, querySampleCount: 2, allowClears: true);
        var states = CreateCurrentStates(spec.Dataset);
        states[1] = null;
        states[5] = DeterministicData.CreateState(spec.Dataset, 5, revision: 47);
        var operations = new CurrentStateOperationExecutor(states);
        var engine = new BenchmarkRunEngine(spec, operations);

        var audit = await engine.RunFinalCorrectnessAuditAsync(CancellationToken.None);

        Assert.Equal(8, audit.PointChecks);
        Assert.Equal(2, audit.ExactQueryChecks);
        Assert.Equal(2, audit.RangeQueryChecks);
        Assert.Equal("all-points", audit.PointCoverage);
    }

    [Fact]
    public async Task FinalAuditRejectsStaleIndexMembership()
    {
        var spec = CreateFinalAuditSpec(pointSampleCount: 1, querySampleCount: 1, allowClears: true);
        var states = CreateCurrentStates(spec.Dataset);
        var sampledOrdinal = CorrectnessAuditPlan.SelectPointOrdinal(spec.Dataset, 0, 1);
        var staleKey = DeterministicData.GetGrainKey(sampledOrdinal);
        states[sampledOrdinal] = null;
        var operations = new CurrentStateOperationExecutor(states);
        operations.ExtraExactKeys.Add(staleKey);
        var engine = new BenchmarkRunEngine(spec, operations);

        var failure = await Assert.ThrowsAsync<InvalidDataException>(
            () => engine.RunFinalCorrectnessAuditAsync(CancellationToken.None));

        Assert.Contains("Final exact-query membership audit", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FinalAuditRejectsMissingIndexMembership()
    {
        var spec = CreateFinalAuditSpec(pointSampleCount: 1, querySampleCount: 1);
        var states = CreateCurrentStates(spec.Dataset);
        var sampledOrdinal = CorrectnessAuditPlan.SelectPointOrdinal(spec.Dataset, 0, 1);
        var missingKey = DeterministicData.GetGrainKey(sampledOrdinal);
        var operations = new CurrentStateOperationExecutor(states);
        operations.OmittedExactKeys.Add(missingKey);
        var engine = new BenchmarkRunEngine(spec, operations);

        var failure = await Assert.ThrowsAsync<InvalidDataException>(
            () => engine.RunFinalCorrectnessAuditAsync(CancellationToken.None));

        Assert.Contains("Final exact-query membership audit", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FinalAuditRejectsMalformedCurrentState()
    {
        var spec = CreateFinalAuditSpec(pointSampleCount: 8, querySampleCount: 0);
        var states = CreateCurrentStates(spec.Dataset);
        var malformed = states[3] ?? throw new InvalidOperationException();
        malformed.Payload[0] ^= 0xFF;
        var operations = new CurrentStateOperationExecutor(states);
        var engine = new BenchmarkRunEngine(spec, operations);

        var failure = await Assert.ThrowsAsync<InvalidDataException>(
            () => engine.RunFinalCorrectnessAuditAsync(CancellationToken.None));

        Assert.Contains("malformed state", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FinalAuditRejectsMissingPointWhenWorkloadCannotClear()
    {
        var spec = CreateFinalAuditSpec(pointSampleCount: 8, querySampleCount: 0);
        var states = CreateCurrentStates(spec.Dataset);
        states[3] = null;
        var engine = new BenchmarkRunEngine(spec, new CurrentStateOperationExecutor(states));

        var failure = await Assert.ThrowsAsync<InvalidDataException>(
            () => engine.RunFinalCorrectnessAuditAsync(CancellationToken.None));

        Assert.Contains("missing state", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnabledAuditRequiresAtLeastOneCheck()
    {
        var audit = new CorrectnessAuditSpec
        {
            Enabled = true,
            PointSampleCount = 0,
            QuerySampleCount = 0,
        };

        Assert.Throws<InvalidDataException>(audit.Validate);
    }

    private static BenchmarkRecordState Clone(
        BenchmarkRecordState state,
        string? exactValue = null,
        int? rangeValue = null,
        long? revision = null,
        byte[]? payload = null)
    {
        return new BenchmarkRecordState
        {
            ExactValue = exactValue ?? state.ExactValue,
            RangeValue = rangeValue ?? state.RangeValue,
            Revision = revision ?? state.Revision,
            Payload = payload ?? state.Payload.ToArray(),
        };
    }

    private static BenchmarkSpec CreateFinalAuditSpec(
        int pointSampleCount,
        int querySampleCount,
        bool allowClears = false)
    {
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "final-audit-test",
            Dataset = new SpecReference { Path = "dataset.json", Sha256 = new string('0', 64) },
            Workload = new SpecReference { Path = "workload.json", Sha256 = new string('0', 64) },
            Audit = new CorrectnessAuditSpec
            {
                PointSampleCount = pointSampleCount,
                QuerySampleCount = querySampleCount,
                MaximumOfflineQueryScanRecords = 8,
                OperationTimeoutSeconds = 1,
                LateCallDrainTimeoutSeconds = 1,
            },
        };
        var dataset = new DatasetSpec
        {
            Id = "final-audit-test",
            Seed = 42,
            RecordCount = 8,
            ExactValueCardinality = 2,
            RangeValueCardinality = 8,
            PayloadBytes = 16,
        };
        var workload = new WorkloadSpec
        {
            Id = "final-audit-test",
            WarmupSeconds = 0,
            DurationSeconds = 1,
            Concurrency = 1,
            Operations = new OperationMixSpec
            {
                Upsert = 1,
                Read = 1,
                ExactQuery = querySampleCount > 0 ? 1 : 0,
                RangeQuery = querySampleCount > 0 ? 1 : 0,
                Clear = allowClears ? 1 : 0,
            },
            QuerySelectivity = new QuerySelectivitySpec
            {
                ExactFraction = 0.5,
                RangeFraction = 0.25,
                MaximumExpectedResultCount = 8,
            },
        };
        return new BenchmarkSpec(scenario, dataset, workload);
    }

    private static Dictionary<long, BenchmarkRecordState?> CreateCurrentStates(DatasetSpec dataset)
    {
        return Enumerable.Range(0, checked((int)dataset.RecordCount))
            .ToDictionary(
                static ordinal => (long)ordinal,
                ordinal => (BenchmarkRecordState?)DeterministicData.CreateState(dataset, ordinal, ordinal + 1L));
    }

    private sealed class CurrentStateOperationExecutor(
        IReadOnlyDictionary<long, BenchmarkRecordState?> states)
        : IBenchmarkOperationExecutor
    {
        public HashSet<string> ExtraExactKeys { get; } = new(StringComparer.Ordinal);

        public HashSet<string> OmittedExactKeys { get; } = new(StringComparer.Ordinal);

        public Task<BenchmarkRecordState?> ReadStateAsync(long ordinal)
        {
            return Task.FromResult(states[ordinal]);
        }

        public Task<IReadOnlyList<string>> FindKeysAsync(string exactValue)
        {
            var result = states
                .Where(pair => pair.Value is not null &&
                    string.Equals(pair.Value.ExactValue, exactValue, StringComparison.Ordinal))
                .Select(static pair => DeterministicData.GetGrainKey(pair.Key))
                .Where(key => !OmittedExactKeys.Contains(key))
                .Concat(ExtraExactKeys)
                .Order(StringComparer.Ordinal)
                .ToArray();
            return Task.FromResult<IReadOnlyList<string>>(result);
        }

        public Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper)
        {
            var result = states
                .Where(pair => pair.Value is not null &&
                    pair.Value.RangeValue >= lower && pair.Value.RangeValue <= upper)
                .Select(static pair => DeterministicData.GetGrainKey(pair.Key))
                .Order(StringComparer.Ordinal)
                .ToArray();
            return Task.FromResult<IReadOnlyList<string>>(result);
        }

        public Task UpsertAsync(long ordinal, long revision) => throw UnexpectedCall();

        public Task<long> ExecuteAsync(OperationInvocation invocation) => throw UnexpectedCall();

        private static InvalidOperationException UnexpectedCall()
        {
            return new InvalidOperationException("The final-audit test executor only supports audit operations.");
        }
    }
}
