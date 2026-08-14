using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkEvidence
{
    internal const int RetainedMemoryV2SampleCount = 5;
    internal const int RetainedMemoryRatioBasisPointScale = 10_000;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    public static void WriteQueryWorkMatrix(string artifactsDirectory, bool quick)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(artifactsDirectory);
        var entries = new List<QueryBenchmarkDiagnostics>();
        var datasets = quick
            ? new[] { QueryEvaluationDataset.ShortIds4K }
            : Enum.GetValues<QueryEvaluationDataset>();
        var variants = Enum.GetValues<QueryEvaluationVariant>()
            .Where(static variant => variant != QueryEvaluationVariant.MaterializingWholePlan)
            .ToArray();

        foreach (var dataset in datasets)
        {
            foreach (var distribution in Enum.GetValues<QueryEvaluationDistribution>())
            {
                foreach (var scenario in Enum.GetValues<QueryEvaluationScenario>())
                {
                    foreach (var variant in variants)
                    {
                        entries.Add(CaptureQueryDiagnostics(dataset, distribution, scenario, variant));
                    }
                }
            }
        }

        if (quick)
        {
            entries.Add(CaptureQueryDiagnostics(
                QueryEvaluationDataset.LongIds4K,
                QueryEvaluationDistribution.Uniform,
                QueryEvaluationScenario.Range,
                QueryEvaluationVariant.OrderedDefaultPartitionPage));
            entries.Add(CaptureQueryDiagnostics(
                QueryEvaluationDataset.LongIds4K,
                QueryEvaluationDistribution.Uniform,
                QueryEvaluationScenario.Range,
                QueryEvaluationVariant.OrderedMaximumPolicyPartitionPage));
        }

        var expectedCount = quick
            ? checked(2 * 6 * variants.Length + 2)
            : checked(
                Enum.GetValues<QueryEvaluationDataset>().Length
                * Enum.GetValues<QueryEvaluationDistribution>().Length
                * Enum.GetValues<QueryEvaluationScenario>().Length
                * variants.Length);
        if (entries.Count != expectedCount)
        {
            throw new InvalidOperationException(
                $"The query work evidence matrix produced {entries.Count} entries instead of {expectedCount}.");
        }

        if (entries
            .Select(static entry => (
                entry.Dataset,
                entry.Distribution,
                entry.Scenario,
                entry.Variant))
            .Distinct()
            .Count() != entries.Count)
        {
            throw new InvalidOperationException(
                "The query work evidence matrix contains a duplicate coordinate tuple.");
        }

        if (!quick)
        {
            var selectedWindowBoundary = entries.SingleOrDefault(static entry =>
                entry.Dataset == QueryEvaluationDataset.ShortIds64K
                && entry.Distribution == QueryEvaluationDistribution.Uniform
                && entry.Scenario == QueryEvaluationScenario.Range
                && entry.Variant == QueryEvaluationVariant.OrderedMaximumPolicyPartitionPage);
            if (selectedWindowBoundary is null
                || selectedWindowBoundary.RangeExecutionStrategy
                    != QueryRangeExecutionStrategy.OrderedRangeMerge
                || selectedWindowBoundary.FirstPage.AccessPath
                    != PartitionQueryAccessPath.RangeMerge
                || selectedWindowBoundary.FirstPage.Work.RangeBucketVisitCount <= 0
                || selectedWindowBoundary.FirstPage.Work.RangeBucketVisitCount
                    >= selectedWindowBoundary.RecordCount
                || selectedWindowBoundary.FirstPage.Work.RangeMergeOperationCount <= 0
                || selectedWindowBoundary.FirstPage.Work.CatalogCandidateVisitCount != 0)
            {
                throw new InvalidOperationException(
                    "The query work evidence omitted the 65K selected-window range-merge boundary.");
            }
        }

        var document = new QueryWorkMatrixDocument(
            SchemaVersion: "oss-query-work-matrix/v2",
            CapturedAtUtc: DateTimeOffset.UtcNow,
            Quick: quick,
            DefaultLegacyItemWindow: checked(
                SearchableStorageQueryOptions.DefaultPageSize
                * SearchableStorageQueryOptions.DefaultLegacyRounds),
            Entries: entries);
        WriteJson(artifactsDirectory, "query-work-matrix.json", document);
    }

    public static async Task WriteRetainedMemoryAsync(string artifactsDirectory, bool quick)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(artifactsDirectory);
        var measurements = new List<RetainedMemoryMeasurement>();
        var recordCounts = quick ? new[] { 4_096 } : new[] { 4_096, 65_536 };
        foreach (var recordCount in recordCounts)
        {
            foreach (var distribution in Enum.GetValues<BenchmarkIndexDistribution>())
            {
                foreach (var representation in Enum.GetValues<DerivedIndexRepresentation>())
                {
                    var samples = new long[3];
                    for (var sample = 0; sample < samples.Length; sample++)
                    {
                        var worker = await RunRetainedMemoryWorkerAsync(
                            recordCount,
                            distribution,
                            representation);
                        if (worker.RecordCount != recordCount
                            || worker.Distribution != distribution
                            || worker.Representation != representation)
                        {
                            throw new InvalidOperationException(
                                "The retained-memory worker returned a result for different coordinates.");
                        }

                        samples[sample] = worker.RetainedManagedBytes;
                    }

                    Array.Sort(samples);
                    if (samples[1] <= 0)
                    {
                        throw new InvalidOperationException(
                            "The isolated retained-memory probe did not observe a positive median delta.");
                    }

                    measurements.Add(new RetainedMemoryMeasurement(
                        recordCount,
                        distribution,
                        representation,
                        SampleCount: samples.Length,
                        MinimumRetainedManagedBytes: samples[0],
                        MedianRetainedManagedBytes: samples[1],
                        MaximumRetainedManagedBytes: samples[^1],
                        MedianRetainedManagedBytesPerRecord: (double)samples[1] / recordCount));
                }
            }
        }

        var document = new RetainedMemoryDocument(
            SchemaVersion: "oss-retained-managed-memory/v1",
            CapturedAtUtc: DateTimeOffset.UtcNow,
            Quick: quick,
            MeasurementSemantics:
                "Median of three isolated worker processes. Input records are retained before the baseline; "
                + "the reported net delta is managed memory still live after a forced full compacting "
                + "collection. A representation may replace equal object references inside those records, "
                + "so this is not an independently allocated-container size. "
                + "It excludes native memory, allocator fragmentation, and process working set.",
            Measurements: measurements);
        WriteJson(artifactsDirectory, "retained-memory.json", document);
    }

    public static async Task WriteRetainedMemoryV2Async(
        string artifactsDirectory,
        bool quick,
        int? maximumProductionToMaterializingRatioBasisPoints)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(artifactsDirectory);
        if (maximumProductionToMaterializingRatioBasisPoints <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumProductionToMaterializingRatioBasisPoints),
                maximumProductionToMaterializingRatioBasisPoints,
                "A retained-memory comparison threshold must be positive when specified.");
        }

        var recordCounts = quick ? new[] { 4_096 } : new[] { 4_096, 65_536 };
        var indexCount = quick ? 4 : 8;
        var measurements = new List<RetainedMemoryV2Measurement>(recordCounts.Length);
        foreach (var recordCount in recordCounts)
        {
            var materializingSamples = await CaptureRetainedMemoryV2SamplesAsync(
                recordCount,
                RetainedMemoryDatasetProfile.CompaniesHouseDeShared,
                indexCount,
                DerivedIndexRepresentation.MaterializingHashSets);
            var productionSamples = await CaptureRetainedMemoryV2SamplesAsync(
                recordCount,
                RetainedMemoryDatasetProfile.CompaniesHouseDeShared,
                indexCount,
                DerivedIndexRepresentation.BoundedOrderedView);
            measurements.Add(CreateRetainedMemoryV2Measurement(
                recordCount,
                RetainedMemoryDatasetProfile.CompaniesHouseDeShared,
                indexCount,
                materializingSamples,
                productionSamples));
        }

        bool? gatePassed = maximumProductionToMaterializingRatioBasisPoints is null
            ? null
            : measurements.All(
                measurement => measurement.ProductionToMaterializingRatioBasisPoints
                    <= maximumProductionToMaterializingRatioBasisPoints.Value);
        var document = new RetainedMemoryV2Document(
            SchemaVersion: "oss-retained-managed-memory/v2",
            CapturedAtUtc: DateTimeOffset.UtcNow,
            Quick: quick,
            MeasurementSemantics:
                "Five raw samples per representation, each captured in a fresh worker process after a forced "
                + "full compacting collection. Each retained delta includes the input StoredRecord graph and "
                + "one derived representation; the production measurement therefore includes the effect of "
                + "in-place Scope and IndexValue canonicalization. MaterializingHashSets is the structurally "
                + "lower, materializing-only comparison and is not the former complete production view. "
                + "The optional virtual-slot catalog, native memory, allocator fragmentation, child-grain "
                + "state, and process working set are excluded.",
            RatioBasisPointScale: RetainedMemoryRatioBasisPointScale,
            Gate: new RetainedMemoryV2Gate(
                maximumProductionToMaterializingRatioBasisPoints,
                gatePassed),
            Measurements: measurements);
        ValidateRetainedMemoryV2Document(document);
        WriteJson(artifactsDirectory, "retained-memory-v2.json", document);

        if (gatePassed == false)
        {
            var worstRatio = measurements.Max(
                static measurement => measurement.ProductionToMaterializingRatioBasisPoints);
            throw new InvalidOperationException(
                $"The production retained-memory ratio reached {worstRatio} basis points and exceeded the "
                + $"configured maximum of {maximumProductionToMaterializingRatioBasisPoints} basis points.");
        }
    }

    internal static RetainedMemoryV2Measurement CreateRetainedMemoryV2Measurement(
        int recordCount,
        RetainedMemoryDatasetProfile profile,
        int indexCount,
        IReadOnlyList<long> materializingSamples,
        IReadOnlyList<long> productionSamples)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(indexCount);
        ArgumentNullException.ThrowIfNull(materializingSamples);
        ArgumentNullException.ThrowIfNull(productionSamples);
        var materializing = materializingSamples.ToArray();
        var production = productionSamples.ToArray();
        if (materializing.Length != RetainedMemoryV2SampleCount
            || production.Length != RetainedMemoryV2SampleCount)
        {
            throw new ArgumentException(
                $"Retained-memory v2 measurements require exactly {RetainedMemoryV2SampleCount} raw samples "
                + "per representation.");
        }

        var materializingMedian = GetPositiveMedian(materializing, nameof(materializingSamples));
        var productionMedian = GetPositiveMedian(production, nameof(productionSamples));
        return new RetainedMemoryV2Measurement(
            RecordCount: recordCount,
            Profile: profile,
            IndexCount: indexCount,
            MaterializingRepresentation: DerivedIndexRepresentation.MaterializingHashSets,
            ProductionRepresentation: DerivedIndexRepresentation.BoundedOrderedView,
            MaterializingRawRetainedManagedBytes: materializing,
            ProductionRawRetainedManagedBytes: production,
            MaterializingMedianRetainedManagedBytes: materializingMedian,
            ProductionMedianRetainedManagedBytes: productionMedian,
            ProductionToMaterializingRatioBasisPoints: GetRatioBasisPoints(
                productionMedian,
                materializingMedian));
    }

    internal static void ValidateRetainedMemoryV2Document(RetainedMemoryV2Document document)
    {
        ArgumentNullException.ThrowIfNull(document);
        if (!string.Equals(
                document.SchemaVersion,
                "oss-retained-managed-memory/v2",
                StringComparison.Ordinal)
            || document.CapturedAtUtc == default
            || string.IsNullOrWhiteSpace(document.MeasurementSemantics)
            || document.RatioBasisPointScale != RetainedMemoryRatioBasisPointScale
            || document.Measurements is null
            || document.Gate is null)
        {
            throw new InvalidDataException("The retained-memory v2 document has invalid root metadata.");
        }

        var expectedRecordCounts = document.Quick ? new[] { 4_096 } : new[] { 4_096, 65_536 };
        var expectedIndexCount = document.Quick ? 4 : 8;
        if (document.Measurements.Count != expectedRecordCounts.Length
            || document.Measurements.Select(static measurement => measurement.RecordCount)
                .Order()
                .SequenceEqual(expectedRecordCounts) == false
            || document.Measurements.Select(static measurement => (
                    measurement.RecordCount,
                    measurement.Profile,
                    measurement.IndexCount))
                .Distinct()
                .Count() != document.Measurements.Count)
        {
            throw new InvalidDataException("The retained-memory v2 document has an invalid coordinate matrix.");
        }

        foreach (var measurement in document.Measurements)
        {
            if (measurement.Profile != RetainedMemoryDatasetProfile.CompaniesHouseDeShared
                || measurement.IndexCount != expectedIndexCount
                || measurement.MaterializingRepresentation
                    != DerivedIndexRepresentation.MaterializingHashSets
                || measurement.ProductionRepresentation
                    != DerivedIndexRepresentation.BoundedOrderedView
                || measurement.MaterializingRawRetainedManagedBytes is null
                || measurement.ProductionRawRetainedManagedBytes is null
                || measurement.MaterializingRawRetainedManagedBytes.Count
                    != RetainedMemoryV2SampleCount
                || measurement.ProductionRawRetainedManagedBytes.Count
                    != RetainedMemoryV2SampleCount
                || measurement.MaterializingRawRetainedManagedBytes.Any(static sample => sample <= 0)
                || measurement.ProductionRawRetainedManagedBytes.Any(static sample => sample <= 0))
            {
                throw new InvalidDataException(
                    "A retained-memory v2 measurement has invalid profile or sample metadata.");
            }

            var expectedMaterializingMedian = GetPositiveMedian(
                measurement.MaterializingRawRetainedManagedBytes,
                nameof(measurement.MaterializingRawRetainedManagedBytes));
            var expectedProductionMedian = GetPositiveMedian(
                measurement.ProductionRawRetainedManagedBytes,
                nameof(measurement.ProductionRawRetainedManagedBytes));
            var expectedRatio = GetRatioBasisPoints(
                expectedProductionMedian,
                expectedMaterializingMedian);
            if (measurement.MaterializingMedianRetainedManagedBytes != expectedMaterializingMedian
                || measurement.ProductionMedianRetainedManagedBytes != expectedProductionMedian
                || measurement.ProductionToMaterializingRatioBasisPoints != expectedRatio)
            {
                throw new InvalidDataException(
                    "A retained-memory v2 measurement does not match its raw samples.");
            }
        }

        if (document.Gate.MaximumProductionToMaterializingRatioBasisPoints <= 0)
        {
            throw new InvalidDataException("The retained-memory v2 gate threshold must be positive.");
        }

        bool? expectedGatePassed =
            document.Gate.MaximumProductionToMaterializingRatioBasisPoints is { } maximum
                ? document.Measurements.All(
                    measurement => measurement.ProductionToMaterializingRatioBasisPoints <= maximum)
                : null;
        if (document.Gate.Passed != expectedGatePassed)
        {
            throw new InvalidDataException(
                "The retained-memory v2 gate result does not match the measurements and threshold.");
        }
    }

    public static RetainedMemoryV2WorkerResult MeasureRetainedMemoryV2Worker(
        int recordCount,
        RetainedMemoryDatasetProfile profile,
        int indexCount,
        DerivedIndexRepresentation representation)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordCount);
        if (!Enum.IsDefined(profile))
        {
            throw new ArgumentOutOfRangeException(nameof(profile), profile, null);
        }

        if (indexCount is not 4 and not 8)
        {
            throw new ArgumentOutOfRangeException(
                nameof(indexCount),
                indexCount,
                "The retained-memory company profile supports exactly four or eight indexes.");
        }

        if (!Enum.IsDefined(representation))
        {
            throw new ArgumentOutOfRangeException(nameof(representation), representation, null);
        }

        Dictionary<string, StoredRecord>? warmupRecords =
            CreateRetainedMemoryV2Records(8, profile, indexCount);
        object? warmup = BuildDerivedIndexes(warmupRecords, representation);
        GC.KeepAlive(warmup);
        GC.KeepAlive(warmupRecords);
        warmup = null;
        warmupRecords = null;
        ForceFullCompactingCollection();

        var before = GC.GetTotalMemory(forceFullCollection: false);
        var records = CreateRetainedMemoryV2Records(recordCount, profile, indexCount);
        var result = BuildDerivedIndexes(records, representation);
        ForceFullCompactingCollection();
        var after = GC.GetTotalMemory(forceFullCollection: false);
        GC.KeepAlive(result);
        GC.KeepAlive(records);

        return new RetainedMemoryV2WorkerResult(
            recordCount,
            profile,
            indexCount,
            representation,
            RetainedManagedBytes: checked(after - before));
    }

    public static string SerializeRetainedMemoryV2WorkerResult(
        RetainedMemoryV2WorkerResult result) =>
        JsonSerializer.Serialize(result, JsonOptions);

    public static RetainedMemoryWorkerResult MeasureRetainedMemoryWorker(
        int recordCount,
        BenchmarkIndexDistribution distribution,
        DerivedIndexRepresentation representation)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordCount);

        object? warmup = BuildDerivedIndexes(
            BenchmarkData.CreateProductionRecords(8, distribution),
            representation);
        GC.KeepAlive(warmup);
        warmup = null;
        ForceFullCompactingCollection();

        var records = BenchmarkData.CreateProductionRecords(recordCount, distribution);
        ForceFullCompactingCollection();
        var before = GC.GetTotalMemory(forceFullCollection: false);
        var result = BuildDerivedIndexes(records, representation);
        ForceFullCompactingCollection();
        var after = GC.GetTotalMemory(forceFullCollection: false);
        GC.KeepAlive(result);
        GC.KeepAlive(records);

        return new RetainedMemoryWorkerResult(
            recordCount,
            distribution,
            representation,
            RetainedManagedBytes: checked(after - before));
    }

    public static string SerializeWorkerResult(RetainedMemoryWorkerResult result) =>
        JsonSerializer.Serialize(result, JsonOptions);

    private static QueryBenchmarkDiagnostics CaptureQueryDiagnostics(
        QueryEvaluationDataset dataset,
        QueryEvaluationDistribution distribution,
        QueryEvaluationScenario scenario,
        QueryEvaluationVariant variant)
    {
        var benchmark = new QueryPlanEvaluationBenchmarks
        {
            Dataset = dataset,
            Distribution = distribution,
            Scenario = scenario,
            Variant = variant,
        };
        benchmark.GlobalSetup();
        var diagnostics = benchmark.OrderedDiagnostics
            ?? throw new InvalidOperationException("The ordered query benchmark omitted diagnostics.");
        if (benchmark.EvaluatePartitionPlan() != diagnostics.TimedItemCount)
        {
            throw new InvalidOperationException("The ordered evidence did not match the timed benchmark result.");
        }

        return diagnostics;
    }

    private static object BuildDerivedIndexes(
        Dictionary<string, StoredRecord> records,
        DerivedIndexRepresentation representation) => representation switch
    {
        DerivedIndexRepresentation.MaterializingHashSets => StoragePartitionIndexes.Build(records),
        DerivedIndexRepresentation.BoundedOrderedView => new StoragePartitionView(records),
        _ => throw new ArgumentOutOfRangeException(nameof(representation), representation, null),
    };

    private static Dictionary<string, StoredRecord> CreateRetainedMemoryV2Records(
        int recordCount,
        RetainedMemoryDatasetProfile profile,
        int indexCount) => profile switch
    {
        RetainedMemoryDatasetProfile.CompaniesHouseDeShared =>
            RetainedMemoryProfileData.CreateCompaniesHouseRecords(recordCount, indexCount),
        _ => throw new ArgumentOutOfRangeException(nameof(profile), profile, null),
    };

    private static async Task<long[]> CaptureRetainedMemoryV2SamplesAsync(
        int recordCount,
        RetainedMemoryDatasetProfile profile,
        int indexCount,
        DerivedIndexRepresentation representation)
    {
        var samples = new long[RetainedMemoryV2SampleCount];
        for (var sample = 0; sample < samples.Length; sample++)
        {
            var worker = await RunRetainedMemoryV2WorkerAsync(
                recordCount,
                profile,
                indexCount,
                representation);
            if (worker.RecordCount != recordCount
                || worker.Profile != profile
                || worker.IndexCount != indexCount
                || worker.Representation != representation)
            {
                throw new InvalidOperationException(
                    "The retained-memory v2 worker returned a result for different coordinates.");
            }

            if (worker.RetainedManagedBytes <= 0)
            {
                throw new InvalidOperationException(
                    "The isolated retained-memory v2 probe did not observe a positive retained delta.");
            }

            samples[sample] = worker.RetainedManagedBytes;
        }

        return samples;
    }

    private static long GetPositiveMedian(IReadOnlyList<long> samples, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(samples);
        if (samples.Count == 0 || samples.Count % 2 == 0)
        {
            throw new ArgumentException(
                "A retained-memory median requires a non-empty odd number of samples.",
                parameterName);
        }

        var ordered = samples.ToArray();
        if (ordered.Any(static sample => sample <= 0))
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                "Every retained-memory sample must be positive.");
        }

        Array.Sort(ordered);
        return ordered[ordered.Length / 2];
    }

    private static int GetRatioBasisPoints(long numerator, long denominator)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(numerator);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(denominator);
        return checked((int)decimal.Ceiling(
            (decimal)numerator * RetainedMemoryRatioBasisPointScale / denominator));
    }

    private static async Task<RetainedMemoryV2WorkerResult> RunRetainedMemoryV2WorkerAsync(
        int recordCount,
        RetainedMemoryDatasetProfile profile,
        int indexCount,
        DerivedIndexRepresentation representation)
    {
        var executable = Environment.ProcessPath
            ?? throw new InvalidOperationException("The current benchmark process path is unavailable.");
        var assemblyPath = Assembly.GetExecutingAssembly().Location;
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = executable,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };
        if (string.Equals(Path.GetFileNameWithoutExtension(executable), "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            process.StartInfo.ArgumentList.Add(assemblyPath);
        }

        process.StartInfo.ArgumentList.Add("--retained-memory-v2-worker");
        process.StartInfo.ArgumentList.Add(recordCount.ToString(CultureInfo.InvariantCulture));
        process.StartInfo.ArgumentList.Add(((int)profile).ToString(CultureInfo.InvariantCulture));
        process.StartInfo.ArgumentList.Add(indexCount.ToString(CultureInfo.InvariantCulture));
        process.StartInfo.ArgumentList.Add(((int)representation).ToString(CultureInfo.InvariantCulture));
        if (!process.Start())
        {
            throw new InvalidOperationException("The retained-memory v2 worker did not start.");
        }

        var standardOutputTask = process.StandardOutput.ReadToEndAsync();
        var standardErrorTask = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            KillWorker(process);
            var timedOutOutput = await DrainWorkerOutputAsync(
                process,
                standardOutputTask,
                standardErrorTask,
                TimeSpan.FromSeconds(5));
            throw new TimeoutException(
                "The retained-memory v2 worker exceeded its 120 second timeout. "
                + $"Standard error: {timedOutOutput.StandardError}");
        }

        var (standardOutput, standardError) = await DrainWorkerOutputAsync(
            process,
            standardOutputTask,
            standardErrorTask,
            TimeSpan.FromSeconds(5));
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"The retained-memory v2 worker failed. Standard error: {standardError}");
        }

        return JsonSerializer.Deserialize<RetainedMemoryV2WorkerResult>(standardOutput, JsonOptions)
            ?? throw new InvalidOperationException("The retained-memory v2 worker returned no JSON result.");
    }

    private static async Task<RetainedMemoryWorkerResult> RunRetainedMemoryWorkerAsync(
        int recordCount,
        BenchmarkIndexDistribution distribution,
        DerivedIndexRepresentation representation)
    {
        var executable = Environment.ProcessPath
            ?? throw new InvalidOperationException("The current benchmark process path is unavailable.");
        var assemblyPath = Assembly.GetExecutingAssembly().Location;
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = executable,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };
        if (string.Equals(Path.GetFileNameWithoutExtension(executable), "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            process.StartInfo.ArgumentList.Add(assemblyPath);
        }

        process.StartInfo.ArgumentList.Add("--retained-memory-worker");
        process.StartInfo.ArgumentList.Add(recordCount.ToString(CultureInfo.InvariantCulture));
        process.StartInfo.ArgumentList.Add(((int)distribution).ToString(CultureInfo.InvariantCulture));
        process.StartInfo.ArgumentList.Add(((int)representation).ToString(CultureInfo.InvariantCulture));
        if (!process.Start())
        {
            throw new InvalidOperationException("The retained-memory worker did not start.");
        }

        var standardOutputTask = process.StandardOutput.ReadToEndAsync();
        var standardErrorTask = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            KillWorker(process);
            var timedOutOutput = await DrainWorkerOutputAsync(
                process,
                standardOutputTask,
                standardErrorTask,
                TimeSpan.FromSeconds(5));
            throw new TimeoutException(
                "The retained-memory worker exceeded its 120 second timeout. "
                + $"Standard error: {timedOutOutput.StandardError}");
        }

        var (standardOutput, standardError) = await DrainWorkerOutputAsync(
            process,
            standardOutputTask,
            standardErrorTask,
            TimeSpan.FromSeconds(5));
        if (process.ExitCode != 0)
        {

            throw new InvalidOperationException(
                $"The retained-memory worker failed. Standard error: {standardError}");
        }

        return JsonSerializer.Deserialize<RetainedMemoryWorkerResult>(standardOutput, JsonOptions)
            ?? throw new InvalidOperationException("The retained-memory worker returned no JSON result.");
    }

    private static async Task<(string StandardOutput, string StandardError)> DrainWorkerOutputAsync(
        Process process,
        Task<string> standardOutputTask,
        Task<string> standardErrorTask,
        TimeSpan timeout)
    {
        using var drainTimeout = new CancellationTokenSource(timeout);
        try
        {
            if (!process.HasExited)
            {
                await process.WaitForExitAsync(drainTimeout.Token);
            }

            await Task.WhenAll(standardOutputTask, standardErrorTask).WaitAsync(drainTimeout.Token);
        }
        catch (OperationCanceledException) when (drainTimeout.IsCancellationRequested)
        {
            return (
                standardOutputTask.IsCompletedSuccessfully
                    ? standardOutputTask.Result
                    : "<stdout drain timed out>",
                standardErrorTask.IsCompletedSuccessfully
                    ? standardErrorTask.Result
                    : "<stderr drain timed out>");
        }

        return (await standardOutputTask, await standardErrorTask);
    }

    private static void KillWorker(Process process)
    {
        try
        {
            process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // The worker exited between the timeout observation and the best-effort kill.
        }
    }

    private static void ForceFullCompactingCollection()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
    }

    private static void WriteJson<T>(string artifactsDirectory, string fileName, T document)
    {
        var directory = Path.GetFullPath(artifactsDirectory);
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, fileName);
        File.WriteAllText(path, JsonSerializer.Serialize(document, JsonOptions));
    }
}

internal sealed record QueryWorkMatrixDocument(
    string SchemaVersion,
    DateTimeOffset CapturedAtUtc,
    bool Quick,
    int DefaultLegacyItemWindow,
    IReadOnlyList<QueryBenchmarkDiagnostics> Entries);

internal sealed record RetainedMemoryDocument(
    string SchemaVersion,
    DateTimeOffset CapturedAtUtc,
    bool Quick,
    string MeasurementSemantics,
    IReadOnlyList<RetainedMemoryMeasurement> Measurements);

internal sealed record RetainedMemoryMeasurement(
    int RecordCount,
    BenchmarkIndexDistribution Distribution,
    DerivedIndexRepresentation Representation,
    int SampleCount,
    long MinimumRetainedManagedBytes,
    long MedianRetainedManagedBytes,
    long MaximumRetainedManagedBytes,
    double MedianRetainedManagedBytesPerRecord);

internal sealed record RetainedMemoryV2Document(
    string SchemaVersion,
    DateTimeOffset CapturedAtUtc,
    bool Quick,
    string MeasurementSemantics,
    int RatioBasisPointScale,
    RetainedMemoryV2Gate Gate,
    IReadOnlyList<RetainedMemoryV2Measurement> Measurements);

internal sealed record RetainedMemoryV2Gate(
    int? MaximumProductionToMaterializingRatioBasisPoints,
    bool? Passed);

internal sealed record RetainedMemoryV2Measurement(
    int RecordCount,
    RetainedMemoryDatasetProfile Profile,
    int IndexCount,
    DerivedIndexRepresentation MaterializingRepresentation,
    DerivedIndexRepresentation ProductionRepresentation,
    IReadOnlyList<long> MaterializingRawRetainedManagedBytes,
    IReadOnlyList<long> ProductionRawRetainedManagedBytes,
    long MaterializingMedianRetainedManagedBytes,
    long ProductionMedianRetainedManagedBytes,
    int ProductionToMaterializingRatioBasisPoints);

internal sealed record RetainedMemoryV2WorkerResult(
    int RecordCount,
    RetainedMemoryDatasetProfile Profile,
    int IndexCount,
    DerivedIndexRepresentation Representation,
    long RetainedManagedBytes);

internal sealed record RetainedMemoryWorkerResult(
    int RecordCount,
    BenchmarkIndexDistribution Distribution,
    DerivedIndexRepresentation Representation,
    long RetainedManagedBytes);
