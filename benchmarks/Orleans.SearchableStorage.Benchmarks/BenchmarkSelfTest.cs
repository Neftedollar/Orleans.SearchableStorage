using System.Globalization;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkSelfTest
{
    public static void Run()
    {
        ValidateBenchmarkContract();
        new SearchableStorageBenchmarkConfig().ValidateContract();
        var smokeConfig = new SearchableStorageBenchmarkConfig(smoke: true);
        smokeConfig.ValidateContract();
        Ensure(
            BenchmarkProvenance.ExecutionModes.Count == 3
            && BenchmarkProvenance.ExecutionModes[0] == "BenchmarkDotNet"
            && BenchmarkProvenance.ExecutionModes[1] == "BenchmarkDotNetInProcessDryRun"
            && BenchmarkProvenance.ExecutionModes[2] == "DeterministicEvidence",
            "exact benchmark provenance execution-mode contract");
        Ensure(
            smokeConfig.GetValidatedJobIdentity()
                == "net10-server-smoke;serverGC=true;concurrentGC=true;nonComparableInProcessDryRun=true",
            "non-comparable BenchmarkDotNet smoke identity");
        ValidateIndexMutation();
        ValidateRetainedMemoryV2Contract();
        ValidateRangeQuery();
        ValidateQueryPlanning();
        ValidateFacetEvaluation();
        ValidateQuerySerialization();
        ValidateJournalSerialization();
        ValidateJournalAppend();
        ValidateJournalReplay();
        ValidateSnapshotConstruction();
        ValidateSlotMovement();
    }

    private static void ValidateBenchmarkContract()
    {
        const string prefix = "Orleans.SearchableStorage.Benchmarks.";
        string[] expectedBenchmarks =
        [
            $"{prefix}ExactRangeLookupBenchmarks.ExactRangeValueLookup",
            $"{prefix}FacetPartitionBenchmarks.EvaluateCandidateMetadataPage",
            $"{prefix}FacetPartitionBenchmarks.EvaluateResumableFilteredCount",
            $"{prefix}DerivedIndexBuildBenchmarks.BuildDerivedIndexes",
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
            $"{prefix}SlotMovementBenchmarks.DeleteBoundedSlotPage",
            $"{prefix}SlotMovementBenchmarks.ExportBoundedSlotPage",
            $"{prefix}SlotMovementBenchmarks.ImportBoundedSlotPage",
            $"{prefix}SlotMovementBenchmarks.RebuildSlotCatalog",
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
            [$"{prefix}FacetPartitionBenchmarks.RecordCount"] = [4_096, 65_536],
            [$"{prefix}FacetPartitionBenchmarks.Cardinality"] = [0, 1],
            [$"{prefix}FacetPartitionBenchmarks.Distribution"] = [0, 1],
            [$"{prefix}FacetPartitionBenchmarks.Predicate"] = [0, 1],
            [$"{prefix}DerivedIndexBuildBenchmarks.RecordCount"] = [4_096, 65_536],
            [$"{prefix}DerivedIndexBuildBenchmarks.Representation"] = [0, 1],
            [$"{prefix}DerivedIndexBuildBenchmarks.Distribution"] = [0, 1],
            [$"{prefix}IndexMutationBenchmarks.RecordCount"] = [1_024, 65_536],
            [$"{prefix}IndexMutationBenchmarks.Representation"] = [0, 1],
            [$"{prefix}IndexMutationBenchmarks.Distribution"] = [0, 1],
            [$"{prefix}JournalReplayBenchmarks.EntryCount"] = [64, 4_096],
            [$"{prefix}JournalSerializationBenchmarks.EntryCount"] = [1, 64],
            [$"{prefix}QueryPlanConstructionBenchmarks.LeafCount"] = [2, 16, 64],
            [$"{prefix}QueryPlanEvaluationBenchmarks.Dataset"] = [0, 1, 2],
            [$"{prefix}QueryPlanEvaluationBenchmarks.Distribution"] = [0, 1],
            [$"{prefix}QueryPlanEvaluationBenchmarks.Scenario"] = [0, 1, 2, 3, 4, 5],
            [$"{prefix}QueryPlanEvaluationBenchmarks.Variant"] = [0, 1, 2, 3, 4, 5],
            [$"{prefix}QueryPlanSerializationBenchmarks.LeafCount"] = [4, 64],
            [$"{prefix}RangeQueryBenchmarks.BucketCount"] = [4_096, 65_536],
            [$"{prefix}RangeQueryBenchmarks.MatchCount"] = [1, 256],
            [$"{prefix}SnapshotConstructionBenchmarks.PayloadSize"] = [64, 1_024],
            [$"{prefix}SnapshotConstructionBenchmarks.RecordCount"] = [1_024, 16_384],
            [$"{prefix}SlotMovementBenchmarks.Distribution"] = [0, 1, 2],
            [$"{prefix}SlotMovementBenchmarks.RecordCount"] = [4_096, 65_536],
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
        foreach (var distribution in Enum.GetValues<BenchmarkIndexDistribution>())
        {
            foreach (var representation in Enum.GetValues<DerivedIndexRepresentation>())
            {
                var benchmark = new IndexMutationBenchmarks
                {
                    RecordCount = 1_024,
                    Representation = representation,
                    Distribution = distribution,
                };
                benchmark.GlobalSetup();
                Ensure(
                    benchmark.ReplaceIndexedRecord() == benchmark.RecordCount,
                    $"index replacement ({distribution}/{representation})");
                benchmark.ValidateFixture();
                Ensure(
                    benchmark.DeleteAndRestoreIndexedRecord() == benchmark.RecordCount,
                    $"index delete/restore ({distribution}/{representation})");
                benchmark.ValidateFixture();
            }
        }

        foreach (var distribution in Enum.GetValues<BenchmarkIndexDistribution>())
        {
            foreach (var representation in Enum.GetValues<DerivedIndexRepresentation>())
            {
                var build = new DerivedIndexBuildBenchmarks
                {
                    RecordCount = 4_096,
                    Representation = representation,
                    Distribution = distribution,
                };
                build.GlobalSetup();
                build.ValidateFixture(build.BuildDerivedIndexes());
            }
        }
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

    private static void ValidateRetainedMemoryV2Contract()
    {
        foreach (var indexCount in new[] { 4, 8 })
        {
            var diagnostics = RetainedMemoryProfileData.CaptureDeSharingDiagnostics(indexCount);
            Ensure(
                diagnostics.RecordCount == 2
                && diagnostics.IndexCount == indexCount
                && diagnostics.ScopesAreEqualByValueAndDistinctByReference
                && diagnostics.CategoricalValuesAreEqualByValueAndDistinctByReference,
                $"retained-memory de-shared profile ({indexCount} indexes)");
        }

        var options = RetainedMemoryV2CommandOptions.Parse(
        [
            "--retained-memory-v2",
            "artifacts",
            "--maximum-production-to-materializing-ratio-bps",
            "6000",
            "--quick",
        ]);
        Ensure(
            options.ArtifactsDirectory == "artifacts"
            && options.Quick
            && options.MaximumProductionToMaterializingRatioBasisPoints == 6000,
            "retained-memory v2 command options");

        long[] materializing = [140, 100, 130, 110, 120];
        long[] production = [90, 50, 80, 60, 70];
        var measurement = BenchmarkEvidence.CreateRetainedMemoryV2Measurement(
            4_096,
            RetainedMemoryDatasetProfile.CompaniesHouseDeShared,
            4,
            materializing,
            production);
        Ensure(
            measurement.MaterializingRawRetainedManagedBytes.SequenceEqual(materializing)
            && measurement.ProductionRawRetainedManagedBytes.SequenceEqual(production)
            && measurement.MaterializingMedianRetainedManagedBytes == 120
            && measurement.ProductionMedianRetainedManagedBytes == 70
            && measurement.ProductionToMaterializingRatioBasisPoints == 5_834,
            "retained-memory v2 median and ceiling ratio");

        var document = new RetainedMemoryV2Document(
            SchemaVersion: "oss-retained-managed-memory/v2",
            CapturedAtUtc: DateTimeOffset.UtcNow,
            Quick: true,
            MeasurementSemantics: "Self-test fixture.",
            RatioBasisPointScale: BenchmarkEvidence.RetainedMemoryRatioBasisPointScale,
            Gate: new RetainedMemoryV2Gate(6_000, Passed: true),
            Measurements: [measurement]);
        BenchmarkEvidence.ValidateRetainedMemoryV2Document(document);

        var rejected = false;
        try
        {
            BenchmarkEvidence.ValidateRetainedMemoryV2Document(
                document with { Gate = document.Gate with { Passed = false } });
        }
        catch (InvalidDataException)
        {
            rejected = true;
        }

        Ensure(rejected, "retained-memory v2 tamper rejection");
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
                foreach (var variant in Enum.GetValues<QueryEvaluationVariant>())
                {
                    var evaluation = new QueryPlanEvaluationBenchmarks
                    {
                        Dataset = QueryEvaluationDataset.ShortIds4K,
                        Distribution = distribution,
                        Scenario = scenario,
                        Variant = variant,
                    };
                    evaluation.GlobalSetup();
                    Ensure(
                        evaluation.EvaluatePartitionPlan() == evaluation.ExpectedTimedResultCount,
                        $"partition query evaluation ({distribution}/{scenario}/{variant})");
                    evaluation.ValidateFixture();
                    if (variant != QueryEvaluationVariant.MaterializingWholePlan)
                    {
                        var diagnostics = evaluation.OrderedDiagnostics
                            ?? throw new InvalidOperationException("Ordered benchmark diagnostics were omitted.");
                        Ensure(
                            diagnostics.FirstPage.Work.TotalOperationCount > 0,
                            $"ordered work vector ({distribution}/{scenario}/{variant})");
                        if (scenario == QueryEvaluationScenario.SelectiveExactAndBroadRange)
                        {
                            Ensure(
                                diagnostics.SelectiveExactDriverCount > 0
                                && diagnostics.FirstPage.Work.OrderedCandidateVisitCount
                                    <= diagnostics.SelectiveExactDriverCount,
                                $"selective exact driver ({distribution}/{variant})");
                        }
                    }
                }
            }
        }

        Ensure(
            SearchableStorageQueryOptions.DefaultLegacyResultItems
                == checked(
                    SearchableStorageQueryOptions.DefaultPageSize
                    * SearchableStorageQueryOptions.DefaultLegacyRounds),
            "default legacy item ceiling equals the default page-by-round window");

        var longIds = new QueryPlanEvaluationBenchmarks
        {
            Dataset = QueryEvaluationDataset.LongIds4K,
            Distribution = QueryEvaluationDistribution.Uniform,
            Scenario = QueryEvaluationScenario.Range,
            Variant = QueryEvaluationVariant.OrderedDefaultPartitionPage,
        };
        longIds.GlobalSetup();
        var longDiagnostics = longIds.OrderedDiagnostics
            ?? throw new InvalidOperationException("Long-GrainId diagnostics were omitted.");
        Ensure(longDiagnostics.MinimumGrainKeyLength == 1_024, "long-GrainId fixture length");
        Ensure(longDiagnostics.FirstPage.ItemByteCount > 0, "long-GrainId page-byte accounting");

        var selectedWindowRange = new QueryPlanEvaluationBenchmarks
        {
            Dataset = QueryEvaluationDataset.ShortIds64K,
            Distribution = QueryEvaluationDistribution.Uniform,
            Scenario = QueryEvaluationScenario.Range,
            Variant = QueryEvaluationVariant.OrderedMaximumPolicyPartitionPage,
        };
        selectedWindowRange.GlobalSetup();
        var selectedWindowDiagnostics = selectedWindowRange.OrderedDiagnostics
            ?? throw new InvalidOperationException("Selected-window range diagnostics were omitted.");
        Ensure(
            selectedWindowDiagnostics.RangeExecutionStrategy
                == QueryRangeExecutionStrategy.OrderedRangeMerge
            && selectedWindowDiagnostics.FirstPage.AccessPath
                == PartitionQueryAccessPath.RangeMerge
            && selectedWindowDiagnostics.FirstPage.Work.RangeBucketVisitCount > 0
            && selectedWindowDiagnostics.FirstPage.Work.RangeBucketVisitCount
                < selectedWindowDiagnostics.RecordCount
            && selectedWindowDiagnostics.FirstPage.Work.RangeMergeOperationCount > 0
            && selectedWindowDiagnostics.FirstPage.Work.CatalogCandidateVisitCount == 0,
            "65K narrow range admits its charged selected-window merge");
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

    private static void ValidateFacetEvaluation()
    {
        foreach (var cardinality in Enum.GetValues<FacetValueCardinality>())
        {
            foreach (var distribution in Enum.GetValues<FacetValueDistribution>())
            {
                foreach (var predicate in Enum.GetValues<FacetPredicate>())
                {
                    var benchmark = new FacetPartitionBenchmarks
                    {
                        RecordCount = 4_096,
                        Cardinality = cardinality,
                        Distribution = distribution,
                        Predicate = predicate,
                    };
                    benchmark.GlobalSetup();
                    benchmark.ValidateFixture();
                    Ensure(
                        benchmark.EvaluateCandidateMetadataPage() > 0,
                        $"facet candidate page ({cardinality}/{distribution}/{predicate})");
                    Ensure(
                        benchmark.EvaluateResumableFilteredCount() > 0,
                        $"resumable facet count ({cardinality}/{distribution}/{predicate})");
                }
            }
        }

        var exactVector = new FacetPartitionBenchmarks
        {
            RecordCount = 4_096,
            Cardinality = FacetValueCardinality.Low8,
            Distribution = FacetValueDistribution.Uniform,
            Predicate = FacetPredicate.SelectiveRange,
        };
        exactVector.GlobalSetup();
        Ensure(
            exactVector.Diagnostics.CandidateWork
                == new FacetBenchmarkWorkVector(1, 8, 0, 0, 0, 0, 0, 0, 8)
            && exactVector.Diagnostics.CountWork
                == new FacetBenchmarkWorkVector(32, 0, 512, 512, 512, 512, 1_024, 256, 0)
            && exactVector.Diagnostics.ExactCount == 256
            && exactVector.Diagnostics.CountRounds == 32,
            "exact facet candidate/count work-vector oracle");
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

    private static void ValidateSlotMovement()
    {
        foreach (var distribution in Enum.GetValues<SlotMovementDistribution>())
        {
            var benchmark = new SlotMovementBenchmarks
            {
                RecordCount = 4_096,
                Distribution = distribution,
            };
            benchmark.GlobalSetup();
            var rebuiltTargetRecordCount = benchmark.RebuildSlotCatalog();
            var exportResult = benchmark.ExportBoundedSlotPage();
            benchmark.IterationSetup();
            var importedRecordCount = benchmark.ImportBoundedSlotPage();
            var remainingRecordCount = benchmark.DeleteBoundedSlotPage();
            benchmark.ValidateBenchmarkResults(
                rebuiltTargetRecordCount,
                exportResult,
                importedRecordCount,
                remainingRecordCount);
        }
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
