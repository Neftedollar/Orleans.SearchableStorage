using System.Reflection;
using System.Diagnostics;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;
using HdrHistogram;
using Microsoft.Crank.EventSources;
using Npgsql;
using StackExchange.Redis;
using Azure.Storage.Blobs;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

internal sealed record BenchmarkRunResult(
    string SchemaVersion,
    string Status,
    string RunId,
    string InstanceId,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset EndedAtUtc,
    RunProvenance Provenance,
    IReadOnlyList<SpecArtifactProvenance> SourceSpecs,
    EffectiveBenchmarkConfiguration EffectiveConfiguration,
    string EffectiveConfigurationSha256,
    string EffectiveConfigurationContentBase64,
    BackendCleanupReport Cleanup,
    PopulationResult? Population,
    CorrectnessAuditResult? InitialAudit,
    PopulationResult? Restoration,
    PhaseResult? Warmup,
    PhaseResult Measurement,
    CorrectnessAuditResult? FinalAudit,
    IReadOnlyList<HistogramArtifact> HistogramArtifacts);

internal sealed record BenchmarkFailureResult(
    string SchemaVersion,
    string Status,
    string RunId,
    string InstanceId,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset EndedAtUtc,
    RunProvenance Provenance,
    IReadOnlyList<SpecArtifactProvenance> SourceSpecs,
    EffectiveBenchmarkConfiguration EffectiveConfiguration,
    string EffectiveConfigurationSha256,
    string EffectiveConfigurationContentBase64,
    BackendCleanupReport Cleanup,
    PopulationResult? Population,
    CorrectnessAuditResult? InitialAudit,
    PopulationResult? Restoration,
    PhaseResult? Warmup,
    PhaseResult? Measurement,
    CorrectnessAuditResult? FinalAudit,
    PhaseResult? FailedPhase,
    FailureInfo Failure,
    IReadOnlyList<HistogramArtifact> HistogramArtifacts);

internal sealed record FailureInfo(
    string Type,
    string Message,
    string? StackTrace,
    LateCallDrainFailureEvidence? LateCallDrainEvidence);

internal sealed record LateCallDrainFailureEvidence(
    string Trigger,
    double? OperationTimeoutSeconds,
    double LateCallDrainTimeoutSeconds,
    double LateCallDrainDurationSeconds,
    bool LateCallDrainIncomplete);

internal sealed record BackendCleanupEvidence(
    string SchemaVersion,
    string RunId,
    string InstanceId,
    DateTimeOffset RecordedAtUtc,
    StorageBackend Backend,
    StoragePath ImplementationPath,
    string ServiceId,
    string IsolationNamespace,
    BackendCleanupReport Cleanup);

internal sealed record RunProvenance(
    string GitCommit,
    bool? GitDirty,
    string DriverVersion,
    string FrameworkDescription,
    string OsDescription,
    string OsArchitecture,
    string ProcessArchitecture,
    int ProcessorCount,
    string CpuModel,
    long? PhysicalMemoryBytes,
    long? CgroupMemoryLimitBytes,
    string? CgroupCpuMax,
    string MachineName,
    bool ServerGc,
    long StopwatchFrequency,
    string Serializer,
    IReadOnlyDictionary<string, string> Components);

internal sealed record SpecArtifactProvenance(
    string Kind,
    string Path,
    string Sha256,
    string ContentBase64);

internal sealed record EffectiveBenchmarkConfiguration(
    string SchemaVersion,
    string RunId,
    string InstanceId,
    string ScenarioName,
    string ExecutionClass,
    int? ClientOrdinal,
    int? ClientCount,
    EffectiveDriverOverrides AppliedOverrides,
    EffectiveDataset Dataset,
    PopulationSpec Population,
    CorrectnessAuditSpec Audit,
    EffectiveStorage Storage,
    EffectiveTopology Topology,
    WorkloadSpec Workload);

internal sealed record EffectiveDriverOverrides(
    StorageBackend? Backend,
    StoragePath? ImplementationPath,
    string? ConnectionStringEnvironment,
    string? AzureBlobContainer,
    TopologyMode? Topology,
    string? AdvertisedAddress,
    string? PrimarySiloEndpoint,
    IReadOnlyList<string>? GatewayEndpoints,
    int? SiloPort,
    int? GatewayPort)
{
    public static EffectiveDriverOverrides None { get; } = new(
        Backend: null,
        ImplementationPath: null,
        ConnectionStringEnvironment: null,
        AzureBlobContainer: null,
        Topology: null,
        AdvertisedAddress: null,
        PrimarySiloEndpoint: null,
        GatewayEndpoints: null,
        SiloPort: null,
        GatewayPort: null);

    public void ApplyTo(BenchmarkSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (ConnectionStringEnvironment is not null && ConnectionStringEnvironment.Length == 0)
        {
            throw new InvalidDataException("An applied connection-string-environment override must not be empty.");
        }

        if (AzureBlobContainer is not null && AzureBlobContainer.Length == 0)
        {
            throw new InvalidDataException("An applied Azure Blob container override must not be empty.");
        }

        if (AdvertisedAddress is not null && AdvertisedAddress.Length == 0)
        {
            throw new InvalidDataException("An applied advertised-address override must not be empty.");
        }

        if (PrimarySiloEndpoint is not null && PrimarySiloEndpoint.Length == 0)
        {
            throw new InvalidDataException("An applied primary-silo override must not be empty.");
        }

        if (GatewayEndpoints is { Count: 0 })
        {
            throw new InvalidDataException("An applied gateways override must contain at least one endpoint.");
        }

        if (Backend is { } backend)
        {
            spec.Storage.Backend = backend;
        }

        if (ImplementationPath is { } implementationPath)
        {
            spec.Storage.Path = implementationPath;
        }

        if (ConnectionStringEnvironment is { Length: > 0 } connectionStringEnvironment)
        {
            spec.Storage.ConnectionStringEnvironment = connectionStringEnvironment;
        }

        if (AzureBlobContainer is { Length: > 0 } azureBlobContainer)
        {
            spec.Storage.AzureBlobContainer = azureBlobContainer;
        }

        if (Topology is { } topology)
        {
            spec.Topology.Mode = topology;
        }

        if (AdvertisedAddress is { Length: > 0 } advertisedAddress)
        {
            spec.Topology.AdvertisedAddress = advertisedAddress;
        }

        if (PrimarySiloEndpoint is not null)
        {
            spec.Topology.PrimarySiloEndpoint = PrimarySiloEndpoint;
        }

        if (GatewayEndpoints is not null)
        {
            spec.Topology.GatewayEndpoints = GatewayEndpoints.ToArray();
        }

        if (SiloPort is { } siloPort)
        {
            spec.Topology.SiloPort = siloPort;
        }

        if (GatewayPort is { } gatewayPort)
        {
            spec.Topology.GatewayPort = gatewayPort;
        }

        spec.Validate();
    }
}

internal sealed record EffectiveDataset(
    string SchemaVersion,
    string Id,
    int Revision,
    ulong Seed,
    long RecordCount,
    int ExactValueCardinality,
    int RangeValueCardinality,
    int PayloadBytes,
    int HashIndexCount,
    int RangeIndexCount);

internal sealed record EffectiveStorage(
    StorageBackend Backend,
    StoragePath ImplementationPath,
    string ConnectionStringEnvironment,
    string AzureBlobContainer,
    string Serializer,
    int? PartitionCount,
    int? VirtualSlotTargetCount,
    int? VirtualSlotCount,
    int? JournalSegmentCapacity,
    int? MaximumJournalReplayEntries,
    int? CompactionThreshold,
    string IsolationNamespace);

internal sealed record EffectiveTopology(
    TopologyMode Mode,
    int SiloCount,
    int EmbeddedSiloCount,
    string ClusterId,
    string ServiceId,
    string AdvertisedAddress,
    string PrimarySiloEndpoint,
    IReadOnlyList<string> GatewayEndpoints,
    int SiloPort,
    int GatewayPort,
    int BarrierTimeoutSeconds,
    int BarrierLateCallDrainTimeoutSeconds);

internal sealed record PopulationResult(
    string Phase,
    string Status,
    DateTimeOffset StartedAtUtc,
    double DurationSeconds,
    long Completed);

internal sealed record CorrectnessAuditResult(
    string Status,
    DateTimeOffset StartedAtUtc,
    double DurationSeconds,
    long PointChecks,
    long ExactQueryChecks,
    long RangeQueryChecks,
    string PointCoverage);

internal sealed record PhaseResult(
    DateTimeOffset StartedAtUtc,
    double ScheduledDurationSeconds,
    double WallDurationSeconds,
    long Offered,
    long Started,
    long Completed,
    long Succeeded,
    long Failed,
    long TimedOut,
    long LateCallDrainAttempts,
    long LateCallDrainIncomplete,
    double LateCallDrainDurationSeconds,
    long Dropped,
    double OfferedPerSecond,
    double CompletedPerSecond,
    IReadOnlyDictionary<string, OperationResult> Operations);

internal sealed record OperationResult(
    long Offered,
    long Started,
    long Completed,
    long Succeeded,
    long Failed,
    long TimedOut,
    long LateCallDrainAttempts,
    long LateCallDrainIncomplete,
    double LateCallDrainDurationSeconds,
    long Dropped,
    long ResultCount,
    long HistogramClamped,
    IReadOnlyDictionary<string, long> Errors,
    LatencySummaryMicroseconds? SucceededLatencyMicroseconds,
    LatencySummaryMicroseconds? FailedLatencyMicroseconds,
    LatencySummaryMicroseconds? QueueDelayMicroseconds);

internal sealed record LatencySummaryMicroseconds(
    long Count,
    double Mean,
    long P50,
    long P90,
    long P95,
    long P99,
    long P999,
    long Maximum);

internal sealed record HistogramArtifact(
    string Operation,
    string Outcome,
    string Metric,
    string Format,
    string Unit,
    string ClientInstance,
    long LowestDiscernibleValue,
    long HighestTrackableValue,
    int SignificantDigits,
    string SamplingSemantics,
    string Path,
    string Sha256,
    long Count);

internal static class BenchmarkResultWriter
{
    private const string ResultSchemaVersion = "oss-benchmark-result/v1";
    private const string EffectiveSchemaVersion = "oss-benchmark-effective/v1";
    private const string SerializerName = "OrleansJsonSerializer";

    public static EffectiveBenchmarkConfiguration CreateEffectiveConfiguration(
        BenchmarkSpec spec,
        string runId,
        string instanceId,
        int? clientOrdinal = null,
        int? clientCount = null,
        EffectiveDriverOverrides? appliedOverrides = null)
    {
        var storageNamespace = spec.Storage.Backend switch
        {
            StorageBackend.PostgreSql => BackendNamespace.CreatePostgreSqlIdentifier(spec.Topology.ServiceId),
            StorageBackend.AzureBlob => spec.Storage.AzureBlobContainer,
            _ => spec.Topology.ServiceId,
        };
        var searchableStorage = spec.Storage.Path is StoragePath.Searchable;
        return new EffectiveBenchmarkConfiguration(
            EffectiveSchemaVersion,
            runId,
            instanceId,
            spec.Name,
            spec.ExecutionClass,
            clientOrdinal,
            clientCount,
            appliedOverrides ?? EffectiveDriverOverrides.None,
            new EffectiveDataset(
                spec.Dataset.SchemaVersion,
                spec.Dataset.Id,
                spec.Dataset.Revision,
                spec.Dataset.Seed,
                spec.Dataset.RecordCount,
                spec.Dataset.ExactValueCardinality,
                spec.Dataset.RangeValueCardinality,
                spec.Dataset.PayloadBytes,
                spec.Dataset.IndexProfile.HashIndexCount,
                spec.Dataset.IndexProfile.RangeIndexCount),
            spec.Population,
            spec.Audit,
            new EffectiveStorage(
                spec.Storage.Backend,
                spec.Storage.Path,
                spec.Storage.ConnectionStringEnvironment,
                spec.Storage.AzureBlobContainer,
                SerializerName,
                searchableStorage ? spec.Storage.PartitionCount : null,
                searchableStorage ? spec.Storage.VirtualSlotTargetCount : null,
                searchableStorage
                    ? StorageLayout.DeriveVirtualSlotCount(
                        spec.Storage.PartitionCount,
                        spec.Storage.VirtualSlotTargetCount)
                    : null,
                searchableStorage ? spec.Storage.JournalSegmentCapacity : null,
                searchableStorage ? spec.Storage.MaximumJournalReplayEntries : null,
                searchableStorage ? spec.Storage.CompactionThreshold : null,
                storageNamespace),
            new EffectiveTopology(
                spec.Topology.Mode,
                spec.Topology.SiloCount,
                spec.Topology.EmbeddedSiloCount,
                spec.Topology.ClusterId,
                spec.Topology.ServiceId,
                spec.Topology.AdvertisedAddress,
                spec.Topology.PrimarySiloEndpoint,
                spec.Topology.GatewayEndpoints,
                spec.Topology.SiloPort,
                spec.Topology.GatewayPort,
                spec.Topology.BarrierTimeoutSeconds,
                spec.Topology.BarrierLateCallDrainTimeoutSeconds),
            spec.Workload);
    }

    public static async Task<string> WriteCleanupEvidenceAsync(
        string outputDirectory,
        string runId,
        string instanceId,
        BenchmarkSpec spec,
        BackendCleanupReport cleanup,
        CancellationToken cancellationToken)
    {
        var directory = Path.GetFullPath(Path.Combine(
            outputDirectory,
            SanitizePathSegment($"{spec.Name}-{runId}-{instanceId}")));
        Directory.CreateDirectory(directory);
        var effective = CreateEffectiveConfiguration(spec, runId, instanceId);
        var evidence = new BackendCleanupEvidence(
            "oss-benchmark-cleanup/v1",
            runId,
            instanceId,
            DateTimeOffset.UtcNow,
            spec.Storage.Backend,
            spec.Storage.Path,
            spec.Topology.ServiceId,
            effective.Storage.IsolationNamespace,
            SecretRedactor.SanitizeCleanup(cleanup, spec.Storage.ConnectionStringEnvironment));
        var path = Path.Combine(directory, "cleanup.json");
        await using var stream = new FileStream(
            path,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 4_096,
            useAsync: true);
        await JsonSerializer.SerializeAsync(
            stream,
            evidence,
            BenchmarkJsonContext.Default.BackendCleanupEvidence,
            cancellationToken);
        await stream.FlushAsync(cancellationToken);
        return path;
    }

    public static async Task<string> WriteAsync(
        string outputDirectory,
        string runId,
        string instanceId,
        DateTimeOffset startedAtUtc,
        LoadedBenchmarkSpec loadedSpec,
        EffectiveBenchmarkConfiguration effectiveConfiguration,
        BackendCleanupReport cleanup,
        BenchmarkExecution execution,
        CancellationToken cancellationToken)
    {
        var directory = Path.GetFullPath(Path.Combine(
            outputDirectory,
            SanitizePathSegment($"{loadedSpec.Spec.Name}-{runId}-{instanceId}")));
        Directory.CreateDirectory(directory);

        var artifacts = await WriteHistogramArtifactsAsync(
            directory,
            instanceId,
            loadedSpec.Spec.Workload.Mode,
            execution.Measurement,
            cancellationToken);

        var effectiveBytes = JsonSerializer.SerializeToUtf8Bytes(
            effectiveConfiguration,
            BenchmarkJsonContext.Default.EffectiveBenchmarkConfiguration);
        var effectiveSha256 = Convert.ToHexStringLower(SHA256.HashData(effectiveBytes));
        var result = new BenchmarkRunResult(
            ResultSchemaVersion,
            execution.Measurement.Failed == 0 && execution.Measurement.Dropped == 0 ? "succeeded" : "completed-with-errors",
            runId,
            instanceId,
            startedAtUtc,
            DateTimeOffset.UtcNow,
            CreateProvenance(),
            loadedSpec.Artifacts.Select(static artifact => new SpecArtifactProvenance(
                artifact.Kind,
                artifact.Path,
                artifact.Sha256,
                artifact.ContentBase64)).ToArray(),
            effectiveConfiguration,
            effectiveSha256,
            Convert.ToBase64String(effectiveBytes),
            SecretRedactor.SanitizeCleanup(cleanup, loadedSpec.Spec.Storage.ConnectionStringEnvironment),
            CreatePopulation(execution.Population, "completed"),
            CreateAudit(execution.InitialAudit),
            CreatePopulation(execution.Restoration, "completed"),
            execution.Warmup is null ? null : CreatePhase(execution.Warmup),
            CreatePhase(execution.Measurement),
            CreateAudit(execution.FinalAudit),
            artifacts);
        var resultPath = Path.Combine(directory, "result.json");
        await using var resultStream = new FileStream(
            resultPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 16_384,
            useAsync: true);
        await JsonSerializer.SerializeAsync(
            resultStream,
            result,
            BenchmarkJsonContext.Default.BenchmarkRunResult,
            cancellationToken);
        await resultStream.FlushAsync(cancellationToken);

        CrankMetrics.Publish(result.Measurement);
        return resultPath;
    }

    public static async Task<string> WriteFailureAsync(
        string outputDirectory,
        string runId,
        string instanceId,
        DateTimeOffset startedAtUtc,
        LoadedBenchmarkSpec loadedSpec,
        EffectiveBenchmarkConfiguration effectiveConfiguration,
        BackendCleanupReport cleanup,
        BenchmarkExecution? execution,
        BenchmarkRunEngine? engine,
        Exception failure,
        CancellationToken cancellationToken)
    {
        var directory = Path.GetFullPath(Path.Combine(
            outputDirectory,
            SanitizePathSegment($"{loadedSpec.Spec.Name}-{runId}-{instanceId}")));
        Directory.CreateDirectory(directory);
        var completedMeasurement = execution?.Measurement ?? engine?.CompletedMeasurement;
        var artifactPhase = completedMeasurement ?? engine?.FailedPhase;
        var artifacts = artifactPhase is null
            ? []
            : await WriteHistogramArtifactsAsync(
                directory,
                instanceId,
                loadedSpec.Spec.Workload.Mode,
                artifactPhase,
                cancellationToken);
        var effectiveBytes = JsonSerializer.SerializeToUtf8Bytes(
            effectiveConfiguration,
            BenchmarkJsonContext.Default.EffectiveBenchmarkConfiguration);
        var result = new BenchmarkFailureResult(
            ResultSchemaVersion,
            "failed",
            runId,
            instanceId,
            startedAtUtc,
            DateTimeOffset.UtcNow,
            CreateProvenance(),
            loadedSpec.Artifacts.Select(static artifact => new SpecArtifactProvenance(
                artifact.Kind,
                artifact.Path,
                artifact.Sha256,
                artifact.ContentBase64)).ToArray(),
            effectiveConfiguration,
            Convert.ToHexStringLower(SHA256.HashData(effectiveBytes)),
            Convert.ToBase64String(effectiveBytes),
            SecretRedactor.SanitizeCleanup(cleanup, loadedSpec.Spec.Storage.ConnectionStringEnvironment),
            CreatePopulation(
                engine?.CompletedPopulation ?? engine?.PartialPopulation,
                engine?.CompletedPopulation is null ? "partial" : "completed"),
            CreateAudit(engine?.CompletedInitialAudit),
            CreatePopulation(
                engine?.CompletedRestoration ?? engine?.PartialRestoration,
                engine?.CompletedRestoration is null ? "partial" : "completed"),
            engine?.CompletedWarmup is null ? null : CreatePhase(engine.CompletedWarmup),
            completedMeasurement is null ? null : CreatePhase(completedMeasurement),
            CreateAudit(execution?.FinalAudit ?? engine?.CompletedFinalAudit),
            engine?.FailedPhase is null ? null : CreatePhase(engine.FailedPhase),
            CreateFailureInfo(failure, loadedSpec.Spec.Storage.ConnectionStringEnvironment),
            artifacts);
        var resultPath = Path.Combine(directory, "failure.json");
        await using var stream = new FileStream(
            resultPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 16_384,
            useAsync: true);
        await JsonSerializer.SerializeAsync(
            stream,
            result,
            BenchmarkJsonContext.Default.BenchmarkFailureResult,
            cancellationToken);
        await stream.FlushAsync(cancellationToken);
        if (result.Measurement is not null)
        {
            CrankMetrics.Publish(result.Measurement);
        }
        else if (result.FailedPhase is not null)
        {
            CrankMetrics.Publish(result.FailedPhase);
        }

        return resultPath;
    }

    private static async Task<IReadOnlyList<HistogramArtifact>> WriteHistogramArtifactsAsync(
        string directory,
        string instanceId,
        LoadMode loadMode,
        PhaseExecution phase,
        CancellationToken cancellationToken)
    {
        var artifacts = new List<HistogramArtifact>();
        foreach (var (kind, operation) in phase.Operations)
        {
            await WriteHistogramAsync(kind, "succeeded", "latency", operation.SucceededLatency);
            await WriteHistogramAsync(kind, "failed", "latency", operation.FailedLatency);
            await WriteHistogramAsync(kind, "all", "queue-delay", operation.QueueDelay);
        }

        return artifacts;

        async Task WriteHistogramAsync(
            OperationKind kind,
            string outcome,
            string metric,
            LongHistogram? histogram)
        {
            if (histogram is null)
            {
                return;
            }

            var operationName = kind.ToString().ToLowerInvariant();
            histogram.Tag = $"{operationName}-{outcome}-{metric}";
            var fileName = $"{metric}-{operationName}-{outcome}.hlog";
            var path = Path.Combine(directory, fileName);
            await using (var stream = new FileStream(
                path,
                FileMode.Create,
                FileAccess.Write,
                FileShare.Read,
                bufferSize: 16_384,
                useAsync: true))
            {
                HistogramLogWriter.Write(stream, phase.StartedAtUtc.UtcDateTime, histogram);
                await stream.FlushAsync(cancellationToken);
            }

            var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
            artifacts.Add(new HistogramArtifact(
                operationName,
                outcome,
                metric,
                "HdrHistogram log v1.3 (compressed V2 histogram)",
                "microseconds",
                instanceId,
                histogram.LowestTrackableValue,
                histogram.HighestTrackableValue,
                histogram.NumberOfSignificantValueDigits,
                metric == "queue-delay"
                    ? "scheduled-arrival to worker-start; open-loop only"
                    : loadMode is LoadMode.OpenLoop
                        ? "scheduled-arrival to completion; includes queue delay"
                        : "operation-start to completion; closed-loop",
                fileName,
                Convert.ToHexStringLower(SHA256.HashData(bytes)),
                histogram.TotalCount));
        }
    }

    private static RunProvenance CreateProvenance()
    {
        var assembly = typeof(BenchmarkResultWriter).Assembly;
        var gitCommit = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT")
            ?? Environment.GetEnvironmentVariable("GITHUB_SHA")
            ?? Environment.GetEnvironmentVariable("BUILD_SOURCEVERSION")
            ?? TryReadGit("rev-parse", "HEAD")
            ?? "unknown";
        var gitDirty = ResolveGitDirty(
            gitCommit,
            Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY"),
            static arguments => TryReadGit(arguments));

        return new RunProvenance(
            gitCommit,
            gitDirty,
            assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                ?? assembly.GetName().Version?.ToString()
                ?? "unknown",
            RuntimeInformation.FrameworkDescription,
            RuntimeInformation.OSDescription,
            RuntimeInformation.OSArchitecture.ToString(),
            RuntimeInformation.ProcessArchitecture.ToString(),
            Environment.ProcessorCount,
            ReadCpuModel(),
            ReadPhysicalMemoryBytes(),
            ReadCgroupMemoryLimitBytes(),
            ReadOptionalTextFile("/sys/fs/cgroup/cpu.max"),
            Environment.MachineName,
            GCSettings.IsServerGC,
            System.Diagnostics.Stopwatch.Frequency,
            SerializerName,
            new SortedDictionary<string, string>(StringComparer.Ordinal)
            {
                ["Azure.Storage.Blobs"] = GetAssemblyVersion(typeof(BlobServiceClient).Assembly),
                ["HdrHistogram"] = GetAssemblyVersion(typeof(LongHistogram).Assembly),
                ["Microsoft.Crank.EventSources"] = GetAssemblyVersion(typeof(BenchmarksEventSource).Assembly),
                ["Microsoft.Orleans.Runtime"] = GetAssemblyVersion(typeof(Grain).Assembly),
                ["Npgsql"] = GetAssemblyVersion(typeof(NpgsqlConnection).Assembly),
                ["Orleans.SearchableStorage"] = GetAssemblyVersion(typeof(SearchableStorageClient).Assembly),
                ["StackExchange.Redis"] = GetAssemblyVersion(typeof(ConnectionMultiplexer).Assembly),
            });
    }

    private static string GetAssemblyVersion(Assembly assembly)
    {
        return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? assembly.GetName().Version?.ToString()
            ?? "unknown";
    }

    private static bool? ParseNullableBoolean(string? value)
    {
        return bool.TryParse(value, out var parsed) ? parsed : null;
    }

    internal static bool? ResolveGitDirty(
        string gitCommit,
        string? configuredDirty,
        Func<string[], string?> gitReader)
    {
        var configured = ParseNullableBoolean(configuredDirty);
        if (configured is not null || string.Equals(gitCommit, "unknown", StringComparison.Ordinal))
        {
            return configured;
        }

        var status = gitReader(["status", "--porcelain", "--untracked-files=normal"]);
        return status is null ? null : status.Length > 0;
    }

    private static string? TryReadGit(params string[] arguments)
    {
        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "git",
                    WorkingDirectory = Environment.CurrentDirectory,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                },
            };
            foreach (var argument in arguments)
            {
                process.StartInfo.ArgumentList.Add(argument);
            }

            if (!process.Start())
            {
                return null;
            }

            var output = process.StandardOutput.ReadToEndAsync();
            if (!process.WaitForExit(milliseconds: 2_000))
            {
                process.Kill(entireProcessTree: true);
                return null;
            }

            return process.ExitCode == 0 ? output.GetAwaiter().GetResult().Trim() : null;
        }
        catch
        {
            return null;
        }
    }

    private static string ReadCpuModel()
    {
        try
        {
            return File.ReadLines("/proc/cpuinfo")
                .FirstOrDefault(static line => line.StartsWith("model name", StringComparison.OrdinalIgnoreCase))?
                .Split(':', 2)[1]
                .Trim()
                ?? RuntimeInformation.ProcessArchitecture.ToString();
        }
        catch (IOException)
        {
            return RuntimeInformation.ProcessArchitecture.ToString();
        }
        catch (UnauthorizedAccessException)
        {
            return RuntimeInformation.ProcessArchitecture.ToString();
        }
    }

    private static long? ReadPhysicalMemoryBytes()
    {
        try
        {
            var line = File.ReadLines("/proc/meminfo")
                .FirstOrDefault(static value => value.StartsWith("MemTotal:", StringComparison.Ordinal));
            var text = line?.Split(' ', StringSplitOptions.RemoveEmptyEntries).ElementAtOrDefault(1);
            return long.TryParse(text, provider: null, out var kibibytes)
                ? checked(kibibytes * 1_024)
                : null;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static long? ReadCgroupMemoryLimitBytes()
    {
        var text = ReadOptionalTextFile("/sys/fs/cgroup/memory.max");
        return long.TryParse(text, provider: null, out var value) ? value : null;
    }

    private static string? ReadOptionalTextFile(string path)
    {
        try
        {
            return File.Exists(path) ? File.ReadAllText(path).Trim() : null;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static PopulationResult? CreatePopulation(PopulationExecution? population, string status)
    {
        return population is null
            ? null
            : new PopulationResult(
                population.Phase,
                status,
                population.StartedAtUtc,
                population.DurationSeconds,
                population.Completed);
    }

    private static CorrectnessAuditResult? CreateAudit(CorrectnessAuditExecution? audit)
    {
        return audit is null
            ? null
            : new CorrectnessAuditResult(
                "passed",
                audit.StartedAtUtc,
                audit.DurationSeconds,
                audit.PointChecks,
                audit.ExactQueryChecks,
                audit.RangeQueryChecks,
                audit.PointCoverage);
    }

    private static FailureInfo CreateFailureInfo(Exception failure, string connectionStringEnvironment)
    {
        var drain = FindLateCallDrainEvidence(failure);
        return new FailureInfo(
            failure.GetType().FullName ?? failure.GetType().Name,
            SecretRedactor.Redact(failure.Message, connectionStringEnvironment) ?? string.Empty,
            SecretRedactor.Redact(failure.StackTrace, connectionStringEnvironment),
            drain is null
                ? null
                : new LateCallDrainFailureEvidence(
                    drain is BenchmarkCallTimeoutException ? "timeout" : "cancellation",
                    drain is BenchmarkCallTimeoutException timeout ? timeout.OperationTimeout.TotalSeconds : null,
                    drain.LateCallDrainTimeout.TotalSeconds,
                    drain.LateCallDrainDuration.TotalSeconds,
                    drain.LateCallDrainIncomplete));
    }

    private static ILateCallDrainEvidence? FindLateCallDrainEvidence(Exception failure)
    {
        if (failure is ILateCallDrainEvidence evidence)
        {
            return evidence;
        }

        if (failure is AggregateException aggregate)
        {
            foreach (var inner in aggregate.Flatten().InnerExceptions)
            {
                if (FindLateCallDrainEvidence(inner) is { } nested)
                {
                    return nested;
                }
            }
        }

        return failure.InnerException is null ? null : FindLateCallDrainEvidence(failure.InnerException);
    }

    private static PhaseResult CreatePhase(PhaseExecution phase)
    {
        var operations = phase.Operations.ToDictionary(
            static pair => pair.Key.ToString().ToLowerInvariant(),
            static pair => CreateOperation(pair.Value),
            StringComparer.Ordinal);
        return new PhaseResult(
            phase.StartedAtUtc,
            phase.ScheduledDurationSeconds,
            phase.WallDurationSeconds,
            phase.Offered,
            phase.Operations.Values.Sum(static value => value.Started),
            phase.Completed,
            phase.Operations.Values.Sum(static value => value.Succeeded),
            phase.Failed,
            phase.Operations.Values.Sum(static value => value.TimedOut),
            phase.Operations.Values.Sum(static value => value.LateCallDrainAttempts),
            phase.Operations.Values.Sum(static value => value.LateCallDrainIncomplete),
            phase.Operations.Values.Sum(static value => value.LateCallDrainDurationSeconds),
            phase.Dropped,
            CalculateRate(phase.Offered, phase.ScheduledDurationSeconds),
            CalculateRate(phase.Completed, phase.WallDurationSeconds),
            operations);
    }

    internal static double CalculateRate(long count, double durationSeconds)
    {
        if (durationSeconds <= 0 || !double.IsFinite(durationSeconds))
        {
            return 0;
        }

        var rate = count / durationSeconds;
        return double.IsFinite(rate) && rate >= 0 ? rate : 0;
    }

    private static OperationResult CreateOperation(OperationExecution operation)
    {
        return new OperationResult(
            operation.Offered,
            operation.Started,
            operation.Completed,
            operation.Succeeded,
            operation.Failed,
            operation.TimedOut,
            operation.LateCallDrainAttempts,
            operation.LateCallDrainIncomplete,
            operation.LateCallDrainDurationSeconds,
            operation.Dropped,
            operation.ResultCount,
            operation.HistogramClamped,
            operation.Errors,
            CreateLatencySummary(operation.SucceededLatency),
            CreateLatencySummary(operation.FailedLatency),
            CreateLatencySummary(operation.QueueDelay));
    }

    internal static LatencySummaryMicroseconds? CreateLatencySummary(HistogramBase? histogram)
    {
        return histogram is null || histogram.TotalCount == 0
            ? null
            : new LatencySummaryMicroseconds(
                histogram.TotalCount,
                histogram.GetMean(),
                histogram.GetValueAtPercentile(50),
                histogram.GetValueAtPercentile(90),
                histogram.GetValueAtPercentile(95),
                histogram.GetValueAtPercentile(99),
                histogram.GetValueAtPercentile(99.9),
                histogram.GetMaxValue());
    }

    private static string SanitizePathSegment(string value)
    {
        return new string(value
            .Select(static character => char.IsAsciiLetterOrDigit(character) || character is '-' or '_'
                ? character
                : '-')
            .ToArray());
    }
}

internal static partial class SecretRedactor
{
    public static BackendCleanupReport SanitizeCleanup(
        BackendCleanupReport cleanup,
        string? connectionStringEnvironment)
    {
        return cleanup with { Error = Redact(cleanup.Error, connectionStringEnvironment) };
    }

    public static string? Redact(string? value, string? connectionStringEnvironment)
    {
        if (value is null)
        {
            return null;
        }

        var result = value;
        if (!string.IsNullOrWhiteSpace(connectionStringEnvironment) &&
            Environment.GetEnvironmentVariable(connectionStringEnvironment) is { Length: > 0 } secret)
        {
            result = result.Replace(secret, "[REDACTED]", StringComparison.Ordinal);
        }

        result = ConnectionStringSecretRegex().Replace(result, "$1[REDACTED]");
        result = JsonSecretRegex().Replace(result, "$1\"[REDACTED]\"");
        result = AuthorizationBearerRegex().Replace(result, "$1[REDACTED]");
        result = UriUserInfoRegex().Replace(result, "$1[REDACTED]@");
        result = UriQueryRegex().Replace(result, "$1=[REDACTED]");
        return result;
    }

    [GeneratedRegex(
        "(?i)(\\b(?:password|pwd|accountkey|sharedaccesssignature|sas|sig|token|secret)\\s*=\\s*)(?:\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'|\\{[^}\\r\\n]*\\}|[^;&\\r\\n]*)",
        RegexOptions.CultureInvariant)]
    private static partial Regex ConnectionStringSecretRegex();

    [GeneratedRegex(
        "(?i)(\\\"(?:password|pwd|accountkey|sharedaccesssignature|sas|sig|token|secret)\\\"\\s*:\\s*)\\\"(?:\\\\.|[^\\\"\\\\])*\\\"",
        RegexOptions.CultureInvariant)]
    private static partial Regex JsonSecretRegex();

    [GeneratedRegex(
        "(?i)(\\bauthorization\\b[\\\"']?\\s*[:=]\\s*[\\\"']?\\s*bearer\\s+)[^\\\"',;}\\s]+",
        RegexOptions.CultureInvariant)]
    private static partial Regex AuthorizationBearerRegex();

    [GeneratedRegex(
        @"(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@",
        RegexOptions.CultureInvariant)]
    private static partial Regex UriUserInfoRegex();

    [GeneratedRegex(@"([?&][^=\s&]+)=([^&\s]+)", RegexOptions.CultureInvariant)]
    private static partial Regex UriQueryRegex();
}

internal static class CrankMetrics
{
    public static void RegisterAndStart()
    {
        BenchmarksEventSource.Register(
            "oss/operations",
            Operations.Max,
            Operations.Sum,
            "Operations",
            "Completed benchmark operations",
            "n0");
        BenchmarksEventSource.Register(
            "oss/failures",
            Operations.Max,
            Operations.Sum,
            "Failures",
            "Failed benchmark operations",
            "n0");
        BenchmarksEventSource.Register(
            "oss/dropped",
            Operations.Max,
            Operations.Sum,
            "Dropped arrivals",
            "Open-loop arrivals rejected by the bounded queue",
            "n0");
        BenchmarksEventSource.Register(
            "oss/operations-per-second",
            Operations.Max,
            Operations.Sum,
            "Operations/sec",
            "Completed benchmark operations per wall-clock second",
            "n2");
    }

    public static void Publish(PhaseResult measurement)
    {
        BenchmarksEventSource.Measure("oss/operations", measurement.Completed);
        BenchmarksEventSource.Measure("oss/failures", measurement.Failed);
        BenchmarksEventSource.Measure("oss/dropped", measurement.Dropped);
        BenchmarksEventSource.Measure("oss/operations-per-second", measurement.CompletedPerSecond);
    }
}
