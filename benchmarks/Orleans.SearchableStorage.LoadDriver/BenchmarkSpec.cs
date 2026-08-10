using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.SearchableStorage.Benchmarks;

internal sealed class BenchmarkScenarioSpec
{
    public const string CurrentSchemaVersion = "oss-benchmark-scenario/v1";

    [JsonPropertyName("$schema")]
    public string? JsonSchema { get; init; }

    public string SchemaVersion { get; init; } = CurrentSchemaVersion;

    public string Name { get; init; } = string.Empty;

    public string ExecutionClass { get; init; } = "smoke";

    public SpecReference Dataset { get; init; } = new();

    public SpecReference Workload { get; init; } = new();

    public PopulationSpec Population { get; init; } = new();

    public CorrectnessAuditSpec Audit { get; init; } = new();

    public StorageSpec Storage { get; init; } = new();

    public TopologySpec Topology { get; init; } = new();

    public void Validate()
    {
        if (!string.Equals(SchemaVersion, CurrentSchemaVersion, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Unsupported benchmark schema '{SchemaVersion}'. Expected '{CurrentSchemaVersion}'.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(Name);
        ArgumentException.ThrowIfNullOrWhiteSpace(ExecutionClass);
        Dataset.Validate();
        Workload.Validate();
        Population.Validate();
        Audit.Validate();
        Storage.Validate();
        Topology.Validate();
    }
}

internal sealed class SpecReference
{
    public string Path { get; init; } = string.Empty;

    public string Sha256 { get; init; } = string.Empty;

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Path);
        if (Sha256.Length != 64 || Sha256.Any(static value =>
                value is not (>= '0' and <= '9') and not (>= 'a' and <= 'f')))
        {
            throw new InvalidDataException(
                $"Spec reference '{Path}' must declare a lowercase 64-character SHA-256 digest.");
        }
    }
}

internal sealed class BenchmarkSpec(
    BenchmarkScenarioSpec scenario,
    DatasetSpec dataset,
    WorkloadSpec workload)
{
    public string Name => scenario.Name;

    public string ExecutionClass => scenario.ExecutionClass;

    public DatasetSpec Dataset => dataset;

    public PopulationSpec Population => scenario.Population;

    public CorrectnessAuditSpec Audit => scenario.Audit;

    public StorageSpec Storage => scenario.Storage;

    public TopologySpec Topology => scenario.Topology;

    public WorkloadSpec Workload => workload;

    public static async Task<LoadedBenchmarkSpec> LoadAsync(string path, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var scenarioArtifact = await LoadArtifactAsync(
            "scenario",
            Path.GetFullPath(path),
            expectedSha256: null,
            cancellationToken);
        var scenario = JsonSerializer.Deserialize(
            Convert.FromBase64String(scenarioArtifact.ContentBase64),
            BenchmarkJsonContext.Default.BenchmarkScenarioSpec)
            ?? throw new InvalidDataException($"Benchmark scenario '{scenarioArtifact.Path}' is empty.");
        scenario.Validate();

        var directory = Path.GetDirectoryName(scenarioArtifact.Path)
            ?? throw new InvalidDataException("The benchmark scenario path has no parent directory.");
        var referenceRoot = Path.GetFullPath(Path.Combine(directory, ".."));
        var datasetArtifact = await LoadArtifactAsync(
            "dataset",
            ResolveArtifactReference(referenceRoot, scenario.Dataset.Path),
            scenario.Dataset.Sha256,
            cancellationToken);
        var dataset = JsonSerializer.Deserialize(
            Convert.FromBase64String(datasetArtifact.ContentBase64),
            BenchmarkJsonContext.Default.DatasetSpec)
            ?? throw new InvalidDataException($"Dataset spec '{datasetArtifact.Path}' is empty.");

        var workloadArtifact = await LoadArtifactAsync(
            "workload",
            ResolveArtifactReference(referenceRoot, scenario.Workload.Path),
            scenario.Workload.Sha256,
            cancellationToken);
        var workload = JsonSerializer.Deserialize(
            Convert.FromBase64String(workloadArtifact.ContentBase64),
            BenchmarkJsonContext.Default.WorkloadSpec)
            ?? throw new InvalidDataException($"Workload spec '{workloadArtifact.Path}' is empty.");

        var spec = new BenchmarkSpec(scenario, dataset, workload);
        spec.Validate();
        return new LoadedBenchmarkSpec(spec, [scenarioArtifact, datasetArtifact, workloadArtifact]);
    }

    public void Validate()
    {
        Dataset.Validate();
        Population.Validate();
        Audit.Validate();
        Storage.Validate();
        Topology.Validate();
        Workload.Validate();

        if (Audit.Enabled && !Population.Enabled)
        {
            throw new InvalidDataException(
                "Schema v1 correctness audits require population.enabled=true; pre-seeded datasets are not defined yet.");
        }

        var exactSelectivity = 1.0 / Dataset.ExactValueCardinality;
        ValidateSelectivity("exact", exactSelectivity, Workload.QuerySelectivity.ExactFraction);
        var rangeWindow = Workload.GetRangeWindow(Dataset);
        var rangeSelectivity = rangeWindow / (double)Dataset.RangeValueCardinality;
        ValidateSelectivity("range", rangeSelectivity, Workload.QuerySelectivity.RangeFraction);

        if (Workload.Operations.ExactQuery > 0)
        {
            ValidateExpectedResultCount("exact", Dataset.RecordCount, exactSelectivity, Workload.QuerySelectivity);
        }

        if (Workload.Operations.RangeQuery > 0)
        {
            ValidateExpectedResultCount("range", Dataset.RecordCount, rangeSelectivity, Workload.QuerySelectivity);
        }

        if (Storage.Path is StoragePath.Plain &&
            (Workload.Operations.ExactQuery != 0 || Workload.Operations.RangeQuery != 0))
        {
            throw new InvalidDataException("Plain storage workloads cannot contain searchable query operations.");
        }

        if (Audit.Enabled)
        {
            if (Audit.PointSampleCount > Dataset.RecordCount)
            {
                throw new InvalidDataException("audit.pointSampleCount must not exceed dataset.recordCount.");
            }

            if (Audit.QuerySampleCount > 0 && Storage.Path is StoragePath.Plain)
            {
                throw new InvalidDataException("Plain storage scenarios cannot request searchable correctness audits.");
            }

            if (Audit.QuerySampleCount > 0 && Dataset.RecordCount > Audit.MaximumOfflineQueryScanRecords)
            {
                throw new InvalidDataException(
                    "Searchable correctness audits require an offline generator scan; reduce querySampleCount to zero " +
                    "or use a dataset within maximumOfflineQueryScanRecords.");
            }
        }

        if (Workload.WarmupSeconds > 0 && Workload.Operations.Clear > 0 &&
            (!Population.Enabled || !Population.RestoreAfterWarmup))
        {
            throw new InvalidDataException(
                "Warmup workloads containing clear operations require population.restoreAfterWarmup=true.");
        }
    }

    private static async Task<LoadedSpecArtifact> LoadArtifactAsync(
        string kind,
        string path,
        string? expectedSha256,
        CancellationToken cancellationToken)
    {
        var content = await File.ReadAllBytesAsync(path, cancellationToken);
        var sha256 = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(content));
        if (expectedSha256 is not null && !string.Equals(sha256, expectedSha256, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                $"The {kind} spec '{path}' has SHA-256 '{sha256}', expected '{expectedSha256}'.");
        }

        return new LoadedSpecArtifact(kind, path, sha256, Convert.ToBase64String(content));
    }

    private static string ResolveArtifactReference(string referenceRoot, string relativePath)
    {
        if (Path.IsPathRooted(relativePath))
        {
            throw new InvalidDataException($"Spec reference '{relativePath}' must be relative to the versioned spec root.");
        }

        var segments = relativePath.Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0 || segments.Any(static segment => segment is "." or ".."))
        {
            throw new InvalidDataException(
                $"Spec reference '{relativePath}' must not contain current-directory or parent-directory traversal.");
        }

        var root = Path.GetFullPath(referenceRoot);
        var resolved = Path.GetFullPath(relativePath, root);
        var rootedPrefix = root.EndsWith(Path.DirectorySeparatorChar)
            ? root
            : root + Path.DirectorySeparatorChar;
        if (!resolved.StartsWith(rootedPrefix, StringComparison.Ordinal))
        {
            throw new InvalidDataException($"Spec reference '{relativePath}' escapes the versioned spec root.");
        }

        var current = root;
        foreach (var segment in segments)
        {
            current = Path.Combine(current, segment);
            if (File.Exists(current) || Directory.Exists(current))
            {
                var attributes = File.GetAttributes(current);
                if ((attributes & FileAttributes.ReparsePoint) != 0)
                {
                    throw new InvalidDataException($"Spec reference '{relativePath}' must not traverse symbolic links.");
                }
            }
        }

        return resolved;
    }

    private static void ValidateSelectivity(string name, double actual, double declared)
    {
        var relativeError = Math.Abs(actual - declared) / declared;
        if (relativeError > 0.01)
        {
            throw new InvalidDataException(
                $"Declared {name} selectivity {declared:R} differs from the dataset-derived value {actual:R} by more than 1%.");
        }
    }

    private static void ValidateExpectedResultCount(
        string name,
        long recordCount,
        double selectivity,
        QuerySelectivitySpec querySelectivity)
    {
        var expected = Math.Ceiling(recordCount * selectivity);
        if (expected > querySelectivity.MaximumExpectedResultCount)
        {
            throw new InvalidDataException(
                $"The {name} query is expected to return about {expected:N0} ids, exceeding " +
                $"maximumExpectedResultCount={querySelectivity.MaximumExpectedResultCount:N0}. " +
                "Use a narrower spec or disable that query until bounded query protocols exist.");
        }
    }
}

internal sealed record LoadedBenchmarkSpec(BenchmarkSpec Spec, IReadOnlyList<LoadedSpecArtifact> Artifacts);

internal sealed record LoadedSpecArtifact(string Kind, string Path, string Sha256, string ContentBase64);

internal sealed class DatasetSpec
{
    public const string CurrentSchemaVersion = "oss-benchmark-dataset/v1";

    [JsonPropertyName("$schema")]
    public string? JsonSchema { get; init; }

    public string SchemaVersion { get; init; } = CurrentSchemaVersion;

    public string Id { get; init; } = string.Empty;

    public int Revision { get; init; } = 1;

    public ulong Seed { get; init; } = 1;

    public long RecordCount { get; init; } = 1_000;

    public int ExactValueCardinality { get; init; } = 128;

    public int RangeValueCardinality { get; init; } = 1_000_000;

    public int PayloadBytes { get; init; } = 256;

    public IndexProfileSpec IndexProfile { get; init; } = new();

    public void Validate()
    {
        if (!string.Equals(SchemaVersion, CurrentSchemaVersion, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Unsupported dataset schema '{SchemaVersion}'. Expected '{CurrentSchemaVersion}'.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(Id);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Revision);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RecordCount);
        if (RecordCount > 999_999_999_999)
        {
            throw new InvalidDataException("Dataset recordCount must not exceed 999,999,999,999.");
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ExactValueCardinality);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RangeValueCardinality);
        ArgumentOutOfRangeException.ThrowIfNegative(PayloadBytes);
        if (PayloadBytes > 1_048_576)
        {
            throw new InvalidDataException("Dataset payloadBytes must not exceed 1 MiB.");
        }

        IndexProfile.Validate();
    }
}

internal sealed class IndexProfileSpec
{
    public int HashIndexCount { get; init; } = 1;

    public int RangeIndexCount { get; init; } = 1;

    public void Validate()
    {
        // V1 intentionally freezes one index of each kind so comparisons do not silently
        // pretend that changing a JSON number changes the compiled state shape.
        if (HashIndexCount != 1 || RangeIndexCount != 1)
        {
            throw new InvalidDataException("Dataset schema v1 supports exactly one hash index and one range index.");
        }
    }
}

internal sealed class PopulationSpec
{
    public const int MaximumConcurrency = 4_096;
    public const int MaximumTimeoutSeconds = 3_600;

    public bool Enabled { get; init; } = true;

    public int Concurrency { get; init; } = 16;

    public int OperationTimeoutSeconds { get; init; } = 30;

    public int LateCallDrainTimeoutSeconds { get; init; } = 30;

    public bool RestoreAfterWarmup { get; init; }

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Concurrency);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(OperationTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(LateCallDrainTimeoutSeconds);
        if (Concurrency > MaximumConcurrency)
        {
            throw new InvalidDataException(
                $"Schema v1 limits population concurrency to {MaximumConcurrency:N0}.");
        }

        if (OperationTimeoutSeconds > MaximumTimeoutSeconds ||
            LateCallDrainTimeoutSeconds > MaximumTimeoutSeconds)
        {
            throw new InvalidDataException(
                $"Schema v1 limits population timeouts to {MaximumTimeoutSeconds:N0} seconds.");
        }
    }
}

internal sealed class CorrectnessAuditSpec
{
    public const int MaximumPointSampleCount = 1_000_000;
    public const int MaximumQuerySampleCount = 64;
    public const long MaximumOfflineScanRecordCount = 1_000_000;
    public const int MaximumTimeoutSeconds = 3_600;
    public bool Enabled { get; init; } = true;

    public int PointSampleCount { get; init; } = 1_000;

    public int QuerySampleCount { get; init; } = 4;

    public long MaximumOfflineQueryScanRecords { get; init; } = 100_000;

    public int OperationTimeoutSeconds { get; init; } = 30;

    public int LateCallDrainTimeoutSeconds { get; init; } = 30;

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(PointSampleCount);
        ArgumentOutOfRangeException.ThrowIfNegative(QuerySampleCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumOfflineQueryScanRecords);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(OperationTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(LateCallDrainTimeoutSeconds);
        if (Enabled && PointSampleCount == 0 && QuerySampleCount == 0)
        {
            throw new InvalidDataException("An enabled correctness audit must contain at least one point or query check.");
        }


        if (PointSampleCount > MaximumPointSampleCount || QuerySampleCount > MaximumQuerySampleCount)
        {
            throw new InvalidDataException(
                $"Schema v1 limits audits to {MaximumPointSampleCount:N0} point samples and {MaximumQuerySampleCount:N0} query samples.");
        }

        if (MaximumOfflineQueryScanRecords > MaximumOfflineScanRecordCount)
        {
            throw new InvalidDataException(
                $"maximumOfflineQueryScanRecords must not exceed {MaximumOfflineScanRecordCount:N0} in schema v1.");
        }

        if (OperationTimeoutSeconds > MaximumTimeoutSeconds ||
            LateCallDrainTimeoutSeconds > MaximumTimeoutSeconds)
        {
            throw new InvalidDataException(
                $"Schema v1 limits correctness-audit timeouts to {MaximumTimeoutSeconds:N0} seconds.");
        }
    }
}

[JsonConverter(typeof(JsonStringEnumConverter<StorageBackend>))]
internal enum StorageBackend
{
    Memory,
    PostgreSql,
    Redis,
    AzureBlob,
}

[JsonConverter(typeof(JsonStringEnumConverter<StoragePath>))]
internal enum StoragePath
{
    Searchable,
    Plain,
}

internal sealed class StorageSpec
{
    private const int MaximumVirtualSlotCount = 262_144;
    public StorageBackend Backend { get; set; } = StorageBackend.Memory;

    public StoragePath Path { get; set; } = StoragePath.Searchable;

    public string ConnectionStringEnvironment { get; set; } = string.Empty;

    public string AzureBlobContainer { get; set; } = "oss-benchmarks";

    public int PartitionCount { get; init; } = 16;

    public int VirtualSlotTargetCount { get; init; } = 1_024;

    public int JournalSegmentCapacity { get; init; } = 256;

    public int MaximumJournalReplayEntries { get; init; } = 4_096;

    public int CompactionThreshold { get; init; } = 2_048;

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(PartitionCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(VirtualSlotTargetCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(JournalSegmentCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumJournalReplayEntries);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(CompactionThreshold);

        if (PartitionCount > MaximumVirtualSlotCount || VirtualSlotTargetCount > MaximumVirtualSlotCount)
        {
            throw new InvalidDataException(
                $"Storage partitionCount and virtualSlotTargetCount must not exceed {MaximumVirtualSlotCount:N0}.");
        }

        var multiplier = checked((VirtualSlotTargetCount + (long)PartitionCount - 1) / PartitionCount);
        if (multiplier * PartitionCount > MaximumVirtualSlotCount)
        {
            throw new InvalidDataException(
                "The smallest virtual-slot count divisible by partitionCount exceeds the 262,144-slot limit.");
        }

        var replaySegmentCount =
            (MaximumJournalReplayEntries + (long)JournalSegmentCapacity - 1) / JournalSegmentCapacity;
        if (replaySegmentCount > int.MaxValue - 2L)
        {
            throw new InvalidDataException(
                "journalSegmentCapacity and maximumJournalReplayEntries produce an unaddressable journal ring.");
        }

        if (CompactionThreshold > MaximumJournalReplayEntries)
        {
            throw new InvalidDataException("Storage compactionThreshold must not exceed maximumJournalReplayEntries.");
        }

        if (Backend is not StorageBackend.Memory && string.IsNullOrWhiteSpace(ConnectionStringEnvironment))
        {
            throw new InvalidDataException("External storage backends require connectionStringEnvironment.");
        }

        if (!string.IsNullOrEmpty(ConnectionStringEnvironment) &&
            !IsPortableEnvironmentVariableName(ConnectionStringEnvironment))
        {
            throw new InvalidDataException(
                "connectionStringEnvironment must be a portable environment-variable name: " +
                "an ASCII letter or underscore followed by at most 127 ASCII letters, digits, or underscores.");
        }

        if (Backend is StorageBackend.AzureBlob)
        {
            ValidateAzureBlobContainerName(AzureBlobContainer);
        }
    }

    private static bool IsPortableEnvironmentVariableName(string value)
    {
        return value.Length is > 0 and <= 128 &&
            (char.IsAsciiLetter(value[0]) || value[0] == '_') &&
            value.Skip(1).All(static character =>
                char.IsAsciiLetterOrDigit(character) || character == '_');
    }

    private static void ValidateAzureBlobContainerName(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length is < 3 or > 63 ||
            !char.IsAsciiLetterOrDigit(value[0]) ||
            !char.IsAsciiLetterOrDigit(value[^1]) ||
            value.Any(static character => !char.IsAsciiLetterOrDigit(character) && character != '-') ||
            value.Any(char.IsAsciiLetterUpper) ||
            value.Contains("--", StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "azureBlobContainer must be 3-63 lowercase letters, digits, or non-consecutive hyphens, and start/end with a letter or digit.");
        }
    }
}

[JsonConverter(typeof(JsonStringEnumConverter<TopologyMode>))]
internal enum TopologyMode
{
    Embedded,
    External,
}

internal sealed class TopologySpec
{
    public const int MaximumEmbeddedSiloCount = 64;
    public const int MaximumSiloCount = 4_096;
    public const int MaximumBarrierTimeoutSeconds = 3_600;

    public int SiloCount { get; init; } = 1;

    public TopologyMode Mode { get; set; } = TopologyMode.Embedded;

    public int EmbeddedSiloCount { get; init; } = 1;

    public string ClusterId { get; set; } = "oss-benchmark";

    public string ServiceId { get; set; } = "oss-benchmark";

    public string AdvertisedAddress { get; set; } = "127.0.0.1";

    public string PrimarySiloEndpoint { get; set; } = string.Empty;

    public IReadOnlyList<string> GatewayEndpoints { get; set; } = [];

    public int SiloPort { get; set; } = 11_111;

    public int GatewayPort { get; set; } = 30_000;

    public int BarrierTimeoutSeconds { get; init; } = 600;

    public int BarrierLateCallDrainTimeoutSeconds { get; init; } = 30;

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(SiloCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(SiloCount, MaximumSiloCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(EmbeddedSiloCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(EmbeddedSiloCount, MaximumEmbeddedSiloCount);
        if (Mode is TopologyMode.Embedded && SiloCount != EmbeddedSiloCount)
        {
            throw new InvalidDataException(
                "Embedded topology siloCount must equal embeddedSiloCount so result provenance is unambiguous.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(ClusterId);
        BackendNamespace.ValidateServiceId(ServiceId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(SiloPort);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(SiloPort, 65_535);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(GatewayPort);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(GatewayPort, 65_535);
        if ((long)SiloPort + EmbeddedSiloCount - 1 > 65_535 ||
            (long)GatewayPort + EmbeddedSiloCount - 1 > 65_535)
        {
            throw new InvalidDataException("Embedded silo and gateway port ranges must end at or before port 65535.");
        }
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(BarrierTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(BarrierTimeoutSeconds, MaximumBarrierTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(BarrierLateCallDrainTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            BarrierLateCallDrainTimeoutSeconds,
            MaximumBarrierTimeoutSeconds);
        EndpointResolver.ValidateAddressSyntax(AdvertisedAddress);
        if (!string.IsNullOrWhiteSpace(PrimarySiloEndpoint))
        {
            EndpointResolver.ValidateEndpointSyntax(PrimarySiloEndpoint, SiloPort);
        }

        foreach (var gateway in GatewayEndpoints)
        {
            EndpointResolver.ValidateEndpointSyntax(gateway, GatewayPort);
        }

        if (Mode is TopologyMode.External && GatewayEndpoints.Count == 0)
        {
            throw new InvalidDataException("External topology requires at least one gateway endpoint.");
        }
    }
}

[JsonConverter(typeof(JsonStringEnumConverter<LoadMode>))]
internal enum LoadMode
{
    ClosedLoop,
    OpenLoop,
}

internal sealed class WorkloadSpec
{
    public const double MaximumTargetRatePerSecond = 1_000_000;
    public const int MaximumConcurrency = 4_096;
    public const int MaximumPortableQueueDepth = 262_144;
    public const int MaximumDurationSeconds = 86_400;
    public const int MaximumTimeoutSeconds = 3_600;
    public const string CurrentSchemaVersion = "oss-benchmark-workload/v1";

    [JsonPropertyName("$schema")]
    public string? JsonSchema { get; init; }

    public string SchemaVersion { get; init; } = CurrentSchemaVersion;

    public string Id { get; init; } = string.Empty;

    public int Revision { get; init; } = 1;

    public LoadMode Mode { get; init; } = LoadMode.ClosedLoop;

    public int WarmupSeconds { get; init; } = 2;

    public int DurationSeconds { get; init; } = 5;

    public int Concurrency { get; init; } = 16;

    public double TargetRatePerSecond { get; init; } = 1_000;

    public int MaximumQueueDepth { get; init; } = 4_096;

    public int OperationTimeoutSeconds { get; init; } = 30;

    public int LateCallDrainTimeoutSeconds { get; init; } = 30;

    public KeyDistributionSpec KeyDistribution { get; init; } = new();

    public QuerySelectivitySpec QuerySelectivity { get; init; } = new();

    public OperationMixSpec Operations { get; init; } = new();

    public void Validate()
    {
        if (!string.Equals(SchemaVersion, CurrentSchemaVersion, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Unsupported workload schema '{SchemaVersion}'. Expected '{CurrentSchemaVersion}'.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(Id);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Revision);
        ArgumentOutOfRangeException.ThrowIfNegative(WarmupSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(DurationSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Concurrency);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumQueueDepth);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(OperationTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(LateCallDrainTimeoutSeconds);

        if (Concurrency > MaximumConcurrency)
        {
            throw new InvalidDataException($"Schema v1 limits workload concurrency to {MaximumConcurrency:N0}.");
        }

        if (MaximumQueueDepth > MaximumPortableQueueDepth)
        {
            throw new InvalidDataException(
                $"Schema v1 limits maximumQueueDepth to {MaximumPortableQueueDepth:N0}.");
        }

        if (WarmupSeconds > MaximumDurationSeconds || DurationSeconds > MaximumDurationSeconds)
        {
            throw new InvalidDataException(
                $"Schema v1 limits each workload phase to {MaximumDurationSeconds:N0} seconds.");
        }

        if (OperationTimeoutSeconds > MaximumTimeoutSeconds ||
            LateCallDrainTimeoutSeconds > MaximumTimeoutSeconds)
        {
            throw new InvalidDataException(
                $"Schema v1 limits workload timeouts to {MaximumTimeoutSeconds:N0} seconds.");
        }

        if (double.IsNaN(TargetRatePerSecond) ||
            double.IsInfinity(TargetRatePerSecond) ||
            TargetRatePerSecond <= 0)
        {
            throw new InvalidDataException("targetRatePerSecond must be a finite positive number.");
        }

        if (TargetRatePerSecond > MaximumTargetRatePerSecond)
        {
            throw new InvalidDataException(
                $"targetRatePerSecond must not exceed the portable scheduler limit of {MaximumTargetRatePerSecond:N0}.");
        }

        var maximumDuration = Math.Max(WarmupSeconds, DurationSeconds);
        if (TargetRatePerSecond * maximumDuration > long.MaxValue - 1d)
        {
            throw new InvalidDataException("The open-loop rate and duration exceed the scheduler sequence range.");
        }

        KeyDistribution.Validate();
        QuerySelectivity.Validate();
        Operations.Validate();
    }

    public int GetRangeWindow(DatasetSpec dataset)
    {
        return Math.Clamp(
            (int)Math.Round(
                dataset.RangeValueCardinality * QuerySelectivity.RangeFraction,
                MidpointRounding.AwayFromZero),
            1,
            dataset.RangeValueCardinality);
    }
}

[JsonConverter(typeof(JsonStringEnumConverter<KeyDistributionKind>))]
internal enum KeyDistributionKind
{
    Uniform,
    Hotspot,
}

internal sealed class KeyDistributionSpec
{
    public KeyDistributionKind Kind { get; init; } = KeyDistributionKind.Uniform;

    public double HotsetFraction { get; init; } = 0.01;

    public double HotsetProbability { get; init; } = 0.80;

    public void Validate()
    {
        ValidateFraction(HotsetFraction, nameof(HotsetFraction));
        ValidateFraction(HotsetProbability, nameof(HotsetProbability));
    }

    private static void ValidateFraction(double value, string name)
    {
        if (double.IsNaN(value) || double.IsInfinity(value) || value is <= 0 or > 1)
        {
            throw new InvalidDataException($"Workload {name} must be a finite number in (0, 1].");
        }
    }
}

internal sealed class QuerySelectivitySpec
{
    public const long MaximumPortableExpectedResultCount = 10_000;

    public double ExactFraction { get; init; } = 0.0078125;

    public double RangeFraction { get; init; } = 0.0001;

    public long MaximumExpectedResultCount { get; init; } = 10_000;

    public void Validate()
    {
        ValidateFraction(ExactFraction, nameof(ExactFraction));
        ValidateFraction(RangeFraction, nameof(RangeFraction));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumExpectedResultCount);
        if (MaximumExpectedResultCount > MaximumPortableExpectedResultCount)
        {
            throw new InvalidDataException(
                $"Schema v1 does not permit unpaged query results above " +
                $"{MaximumPortableExpectedResultCount:N0} records before bounded query delivery is implemented.");
        }
    }

    private static void ValidateFraction(double value, string name)
    {
        if (double.IsNaN(value) || double.IsInfinity(value) || value is <= 0 or > 1)
        {
            throw new InvalidDataException($"Query selectivity {name} must be a finite number in (0, 1].");
        }
    }
}

internal sealed class OperationMixSpec
{
    public int Upsert { get; init; } = 20;

    public int Read { get; init; } = 50;

    public int ExactQuery { get; init; } = 20;

    public int RangeQuery { get; init; } = 10;

    public int Clear { get; init; }

    [JsonIgnore]
    public int TotalWeight => checked(Upsert + Read + ExactQuery + RangeQuery + Clear);

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(Upsert);
        ArgumentOutOfRangeException.ThrowIfNegative(Read);
        ArgumentOutOfRangeException.ThrowIfNegative(ExactQuery);
        ArgumentOutOfRangeException.ThrowIfNegative(RangeQuery);
        ArgumentOutOfRangeException.ThrowIfNegative(Clear);
        if (TotalWeight == 0)
        {
            throw new InvalidDataException("At least one workload operation weight must be positive.");
        }
    }
}

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = true,
    UseStringEnumConverter = true,
    UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow)]
[JsonSerializable(typeof(BenchmarkScenarioSpec))]
[JsonSerializable(typeof(DatasetSpec))]
[JsonSerializable(typeof(WorkloadSpec))]
[JsonSerializable(typeof(EffectiveBenchmarkConfiguration))]
[JsonSerializable(typeof(BenchmarkRunResult))]
[JsonSerializable(typeof(BenchmarkFailureResult))]
[JsonSerializable(typeof(BackendCleanupEvidence))]
internal sealed partial class BenchmarkJsonContext : JsonSerializerContext;
