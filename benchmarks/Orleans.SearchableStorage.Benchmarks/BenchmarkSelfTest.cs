using System.Globalization;
using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkSelfTest
{
    public static void Run()
    {
        ValidateBenchmarkContract();
        new SearchableStorageBenchmarkConfig().ValidateContract();
        ValidateIndexMutation();
        ValidateRangeQuery();
        ValidateQueryPlanning();
        ValidateQuerySerialization();
        ValidateJournalSerialization();
        ValidateJournalAppend();
        ValidateJournalReplay();
        ValidateSnapshotConstruction();
    }

    private static void ValidateBenchmarkContract()
    {
        const string prefix = "Orleans.SearchableStorage.Benchmarks.";
        string[] expectedBenchmarks =
        [
            $"{prefix}ExactRangeLookupBenchmarks.ExactRangeValueLookup",
            $"{prefix}IndexMutationBenchmarks.DeleteAndRestoreIndexedRecord",
            $"{prefix}IndexMutationBenchmarks.ReplaceIndexedRecord",
            $"{prefix}JournalAppendBenchmarks.AppendBoundedJournalSegment",
            $"{prefix}JournalReplayBenchmarks.MaterializeSnapshotAndReplay",
            $"{prefix}JournalReplayBenchmarks.ReplayValidatedJournal",
            $"{prefix}JournalSerializationBenchmarks.DeserializeJournalSegment",
            $"{prefix}JournalSerializationBenchmarks.SerializeJournalSegment",
            $"{prefix}QueryPlanConstructionBenchmarks.CreatePartitionWirePlan",
            $"{prefix}QueryPlanConstructionBenchmarks.TranslateExpression",
            $"{prefix}QueryPlanEvaluationBenchmarks.EvaluatePartitionPlan",
            $"{prefix}QueryPlanSerializationBenchmarks.DeserializePartitionQueryPlan",
            $"{prefix}QueryPlanSerializationBenchmarks.SerializePartitionQueryPlan",
            $"{prefix}RangeQueryBenchmarks.BoundedRangeQuery",
            $"{prefix}SnapshotConstructionBenchmarks.ConstructCompactionSnapshot",
        ];
        Array.Sort(expectedBenchmarks, StringComparer.Ordinal);

        var assembly = typeof(BenchmarkSelfTest).Assembly;
        var actualBenchmarks = assembly.GetTypes()
            .SelectMany(type => type.GetMethods(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
                | BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Where(method => method.GetCustomAttribute<BenchmarkAttribute>() is not null)
            .Select(method => $"{method.DeclaringType!.FullName}.{method.Name}")
            .Order(StringComparer.Ordinal)
            .ToArray();
        EnsureSequenceEqual(expectedBenchmarks, actualBenchmarks, "exact [Benchmark] method manifest");

        var expectedParameters = new Dictionary<string, int[]>(StringComparer.Ordinal)
        {
            [$"{prefix}ExactRangeLookupBenchmarks.BucketCount"] = [4_096, 65_536],
            [$"{prefix}IndexMutationBenchmarks.RecordCount"] = [1_024, 65_536],
            [$"{prefix}JournalReplayBenchmarks.EntryCount"] = [64, 4_096],
            [$"{prefix}JournalSerializationBenchmarks.EntryCount"] = [1, 64],
            [$"{prefix}QueryPlanConstructionBenchmarks.LeafCount"] = [2, 16, 64],
            [$"{prefix}QueryPlanEvaluationBenchmarks.Distribution"] = [0, 1],
            [$"{prefix}QueryPlanEvaluationBenchmarks.RecordCount"] = [4_096, 65_536],
            [$"{prefix}QueryPlanEvaluationBenchmarks.Scenario"] = [0, 1, 2, 3, 4, 5],
            [$"{prefix}QueryPlanSerializationBenchmarks.LeafCount"] = [4, 64],
            [$"{prefix}RangeQueryBenchmarks.BucketCount"] = [4_096, 65_536],
            [$"{prefix}RangeQueryBenchmarks.MatchCount"] = [1, 256],
            [$"{prefix}SnapshotConstructionBenchmarks.PayloadSize"] = [64, 1_024],
            [$"{prefix}SnapshotConstructionBenchmarks.RecordCount"] = [1_024, 16_384],
        };
        var actualParameters = assembly.GetTypes()
            .SelectMany(type => type.GetMembers(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
                | BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Select(member => (Member: member, Attribute: member.GetCustomAttribute<ParamsAttribute>()))
            .Where(candidate => candidate.Attribute is not null)
            .ToDictionary(
                candidate => $"{candidate.Member.DeclaringType!.FullName}.{candidate.Member.Name}",
                candidate => candidate.Attribute!.Values
                    .Select(value => Convert.ToInt32(value, CultureInfo.InvariantCulture))
                    .ToArray(),
                StringComparer.Ordinal);
        EnsureSequenceEqual(
            expectedParameters.Keys.Order(StringComparer.Ordinal),
            actualParameters.Keys.Order(StringComparer.Ordinal),
            "exact [Params] member manifest");
        foreach (var (identity, expectedValues) in expectedParameters)
        {
            EnsureSequenceEqual(expectedValues, actualParameters[identity], $"exact [Params] vector for {identity}");
        }
    }

    private static void ValidateIndexMutation()
    {
        var benchmark = new IndexMutationBenchmarks { RecordCount = 1_024 };
        benchmark.GlobalSetup();
        Ensure(benchmark.ReplaceIndexedRecord() == benchmark.RecordCount, "index replacement");
        benchmark.ValidateFixture();
        Ensure(benchmark.DeleteAndRestoreIndexedRecord() == benchmark.RecordCount, "index delete/restore");
        benchmark.ValidateFixture();
    }

    private static void ValidateRangeQuery()
    {
        var exact = new ExactRangeLookupBenchmarks { BucketCount = 4_096 };
        exact.GlobalSetup();
        Ensure(exact.ExactRangeValueLookup() == 1, "exact range lookup");

        var benchmark = new RangeQueryBenchmarks
        {
            BucketCount = 4_096,
            MatchCount = 256,
        };
        benchmark.GlobalSetup();
        Ensure(benchmark.BoundedRangeQuery() == benchmark.MatchCount, "bounded range lookup");
    }

    private static void ValidateQueryPlanning()
    {
        var construction = new QueryPlanConstructionBenchmarks { LeafCount = 16 };
        construction.GlobalSetup();
        var translated = construction.TranslateExpression();
        var wire = construction.CreatePartitionWirePlan();
        Ensure(translated is not null, "query translation");
        Ensure(wire is not null, "wire-plan construction");
        construction.ValidateFixture(translated!, wire!);

        foreach (var distribution in Enum.GetValues<QueryEvaluationDistribution>())
        {
            foreach (var scenario in Enum.GetValues<QueryEvaluationScenario>())
            {
                var evaluation = new QueryPlanEvaluationBenchmarks
                {
                    RecordCount = 4_096,
                    Distribution = distribution,
                    Scenario = scenario,
                };
                evaluation.GlobalSetup();
                Ensure(
                    evaluation.EvaluatePartitionPlan() == evaluation.ExpectedResultCount,
                    $"partition query evaluation ({distribution}/{scenario})");
                evaluation.ValidateFixture();
            }
        }
    }

    private static void ValidateQuerySerialization()
    {
        var benchmark = new QueryPlanSerializationBenchmarks { LeafCount = 4 };
        benchmark.GlobalSetup();
        try
        {
            var serializedLength = benchmark.SerializePartitionQueryPlan();
            Ensure(serializedLength > 0, "query-plan serialization");
            benchmark.ValidateSerializedFixture(serializedLength);
            Ensure(benchmark.DeserializePartitionQueryPlan() == 7, "query-plan deserialization");
            benchmark.ValidateFixture();
        }
        finally
        {
            benchmark.GlobalCleanup();
        }
    }

    private static void ValidateJournalSerialization()
    {
        var benchmark = new JournalSerializationBenchmarks { EntryCount = 64 };
        benchmark.GlobalSetup();
        try
        {
            var serializedLength = benchmark.SerializeJournalSegment();
            Ensure(serializedLength > 0, "journal serialization");
            benchmark.ValidateSerializedFixture(serializedLength);
            Ensure(benchmark.DeserializeJournalSegment() == benchmark.EntryCount, "journal deserialization");
            benchmark.ValidateFixture();
        }
        finally
        {
            benchmark.GlobalCleanup();
        }
    }

    private static void ValidateJournalAppend()
    {
        var benchmark = new JournalAppendBenchmarks();
        benchmark.GlobalSetup();
        Ensure(
            benchmark.AppendBoundedJournalSegment().GetAwaiter().GetResult() == 64,
            "bounded journal append");
        benchmark.ValidateFixtureAsync().GetAwaiter().GetResult();
    }

    private static void ValidateJournalReplay()
    {
        var benchmark = new JournalReplayBenchmarks { EntryCount = 64 };
        benchmark.GlobalSetup();
        Ensure(benchmark.ReplayValidatedJournal() == 4_161, "validated journal replay");
        Ensure(benchmark.MaterializeSnapshotAndReplay() == 4_161, "snapshot recovery replay");
        benchmark.ValidateFixture();
    }

    private static void ValidateSnapshotConstruction()
    {
        var benchmark = new SnapshotConstructionBenchmarks
        {
            RecordCount = 1_024,
            PayloadSize = 64,
        };
        benchmark.GlobalSetup();
        Ensure(
            benchmark.ConstructCompactionSnapshot()
                is Orleans.SearchableStorage.Storage.StorageSnapshotState snapshot
                && snapshot.Records.Count == benchmark.RecordCount,
            "snapshot construction");
        benchmark.ValidateFixture();
    }

    private static void Ensure(bool condition, string invariant)
    {
        if (!condition)
        {
            throw new InvalidOperationException($"Benchmark invariant failed: {invariant}.");
        }
    }

    private static void EnsureSequenceEqual<T>(
        IEnumerable<T> expected,
        IEnumerable<T> actual,
        string invariant)
    {
        if (!expected.SequenceEqual(actual))
        {
            throw new InvalidOperationException($"Benchmark invariant failed: {invariant}.");
        }
    }
}
