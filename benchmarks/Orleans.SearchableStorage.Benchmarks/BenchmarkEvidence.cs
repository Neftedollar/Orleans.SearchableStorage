using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkEvidence
{
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
                + "the reported delta is managed memory still live after a forced full compacting collection. "
                + "It excludes native memory, allocator fragmentation, and process working set.",
            Measurements: measurements);
        WriteJson(artifactsDirectory, "retained-memory.json", document);
    }

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

internal sealed record RetainedMemoryWorkerResult(
    int RecordCount,
    BenchmarkIndexDistribution Distribution,
    DerivedIndexRepresentation Representation,
    long RetainedManagedBytes);
