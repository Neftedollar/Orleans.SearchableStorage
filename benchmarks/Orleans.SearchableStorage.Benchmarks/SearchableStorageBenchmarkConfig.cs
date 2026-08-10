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
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;

namespace Orleans.SearchableStorage.Benchmarks;

internal sealed class SearchableStorageBenchmarkConfig : ManualConfig
{
    private readonly bool _smoke;

    public SearchableStorageBenchmarkConfig(bool smoke = false)
    {
        _smoke = smoke;
        ArtifactsPath = Path.Combine("BenchmarkDotNet.Artifacts");
        var job = smoke
            ? Job.Dry.WithToolchain(InProcessNoEmitToolchain.Instance)
            : Job.Default;
        AddJob(
            job
                .WithId(smoke ? "net10-server-smoke" : "net10-server")
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
            $"concurrentGC={FormatBoolean(job.Environment.Gc.Concurrent)}" +
            (_smoke ? ";nonComparableInProcessDryRun=true" : string.Empty);
    }

    internal void ValidateContract()
    {
        var jobs = GetJobs().ToArray();
        if (jobs.Length != 1)
        {
            throw new InvalidOperationException("The benchmark contract requires exactly one job.");
        }

        var job = jobs[0];
        var expectedId = _smoke ? "net10-server-smoke" : "net10-server";
        if (!string.Equals(job.ResolvedId, expectedId, StringComparison.Ordinal)
            || job.Environment.Runtime?.RuntimeMoniker != RuntimeMoniker.Net10_0
            || !job.Environment.Gc.Server
            || !job.Environment.Gc.Concurrent
            || (_smoke
                && !ReferenceEquals(
                    job.Infrastructure.Toolchain,
                    InProcessNoEmitToolchain.Instance)))
        {
            throw new InvalidOperationException(
                $"The benchmark contract requires the '{expectedId}' job with .NET 10, server/concurrent GC"
                + (_smoke ? ", and the non-comparable in-process dry-run toolchain." : "."));
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
            var observed = string.Join(
                ", ",
                columnProviders.Select(static provider => provider.GetType().FullName));
            throw new InvalidOperationException(
                "The benchmark contract requires the default columns and the exact P95 column provider. "
                + $"Observed: {observed}.");
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
