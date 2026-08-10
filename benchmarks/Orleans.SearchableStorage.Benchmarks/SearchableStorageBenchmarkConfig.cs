using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;

namespace Orleans.SearchableStorage.Benchmarks;

internal sealed class SearchableStorageBenchmarkConfig : ManualConfig
{
    public SearchableStorageBenchmarkConfig()
    {
        ArtifactsPath = Path.Combine("BenchmarkDotNet.Artifacts");
        AddJob(
            Job.Default
                .WithId("net10-server")
                .WithRuntime(CoreRuntime.Core10_0)
                .WithGcServer(true)
                .WithGcConcurrent(true)
                .WithMsBuildArguments("-m:1"));
        AddDiagnoser(MemoryDiagnoser.Default);
        AddLogger(ConsoleLogger.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(JsonExporter.Full);
        AddColumnProvider(SearchableStorageStatisticColumnProvider.Instance);
        WithOrderer(new DefaultOrderer(SummaryOrderPolicy.Declared, MethodOrderPolicy.Declared));
        WithOptions(ConfigOptions.KeepBenchmarkFiles);
    }

    internal string GetValidatedJobIdentity()
    {
        ValidateContract();
        var job = GetJobs().Single();
        return $"{job.ResolvedId};serverGC={FormatBoolean(job.Environment.Gc.Server)};" +
            $"concurrentGC={FormatBoolean(job.Environment.Gc.Concurrent)}";
    }

    internal void ValidateContract()
    {
        var jobs = GetJobs().ToArray();
        if (jobs.Length != 1)
        {
            throw new InvalidOperationException("The benchmark contract requires exactly one job.");
        }

        var job = jobs[0];
        if (!string.Equals(job.ResolvedId, "net10-server", StringComparison.Ordinal)
            || job.Environment.Runtime?.RuntimeMoniker != RuntimeMoniker.Net10_0
            || !job.Environment.Gc.Server
            || !job.Environment.Gc.Concurrent)
        {
            throw new InvalidOperationException(
                "The benchmark contract requires the net10-server job with server and concurrent GC enabled.");
        }

        var diagnosers = GetDiagnosers().ToArray();
        if (diagnosers.Length != 1 || !ReferenceEquals(diagnosers[0], MemoryDiagnoser.Default))
        {
            throw new InvalidOperationException("The benchmark contract requires exactly MemoryDiagnoser.Default.");
        }

        var exporters = GetExporters().ToArray();
        if (exporters.Length != 2
            || !exporters.Any(exporter => ReferenceEquals(exporter, MarkdownExporter.GitHub))
            || !exporters.Any(exporter => ReferenceEquals(exporter, JsonExporter.Full)))
        {
            throw new InvalidOperationException(
                "The benchmark contract requires the GitHub Markdown and full JSON exporters.");
        }

        var columnProviders = GetColumnProviders().ToArray();
        var expectedColumnProviders = DefaultColumnProviders.Instance
            .Append(SearchableStorageStatisticColumnProvider.Instance)
            .ToArray();
        if (columnProviders.Length != expectedColumnProviders.Length
            || columnProviders.Where((provider, index) =>
                !ReferenceEquals(provider, expectedColumnProviders[index])).Any())
        {
            throw new InvalidOperationException(
                "The benchmark contract requires the default columns and the exact P95 column provider. "
                + $"Observed: {string.Join(", ", columnProviders.Select(static provider => provider.GetType().FullName))}.");
        }

        var statisticColumns = SearchableStorageStatisticColumnProvider.ContractColumns;
        if (statisticColumns.Count != 1
            || !ReferenceEquals(statisticColumns[0], StatisticColumn.P95))
        {
            throw new InvalidOperationException("The benchmark contract requires exactly the p95 statistic column.");
        }

        if (!Options.HasFlag(ConfigOptions.KeepBenchmarkFiles))
        {
            throw new InvalidOperationException("The benchmark contract must retain generated benchmark files.");
        }
    }

    private static string FormatBoolean(bool value) => value ? "true" : "false";
}

internal sealed class SearchableStorageStatisticColumnProvider : IColumnProvider
{
    private static readonly IColumn[] RequiredColumns = [StatisticColumn.P95];

    public static SearchableStorageStatisticColumnProvider Instance { get; } = new();

    private SearchableStorageStatisticColumnProvider()
    {
    }

    internal static IReadOnlyList<IColumn> ContractColumns => RequiredColumns;

    public IEnumerable<IColumn> GetColumns(Summary summary) =>
        RequiredColumns.Where(column => column.IsAvailable(summary));
}
