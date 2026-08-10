using System.Security.Cryptography;
using System.Text.Json;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkTestData
{
    public static BenchmarkSpec CreateSpec(
        StoragePath storagePath = StoragePath.Searchable,
        OperationMixSpec? operations = null,
        long recordCount = 1_000,
        LoadMode loadMode = LoadMode.ClosedLoop)
    {
        var dataset = new DatasetSpec
        {
            Id = "test-dataset",
            Seed = 0x0123456789ABCDEF,
            RecordCount = recordCount,
            ExactValueCardinality = 128,
            RangeValueCardinality = 1_000_000,
            PayloadBytes = 32,
        };
        var workload = new WorkloadSpec
        {
            Id = "test-workload",
            Mode = loadMode,
            WarmupSeconds = 0,
            DurationSeconds = 1,
            Operations = operations ?? new OperationMixSpec(),
        };
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "test-scenario",
            Dataset = new SpecReference { Path = "dataset.json", Sha256 = new string('0', 64) },
            Workload = new SpecReference { Path = "workload.json", Sha256 = new string('0', 64) },
            Audit = storagePath is StoragePath.Plain
                ? new CorrectnessAuditSpec { QuerySampleCount = 0 }
                : new CorrectnessAuditSpec(),
            Storage = new StorageSpec { Path = storagePath },
        };
        return new BenchmarkSpec(scenario, dataset, workload);
    }

    public static async Task<TemporaryBenchmarkSpecs> WriteSpecsAsync(
        string? scenarioSchemaVersion = null,
        string? datasetSchemaVersion = null,
        string? workloadSchemaVersion = null,
        string? unknownScenarioProperty = null,
        string? unknownDatasetProperty = null,
        string? unknownWorkloadProperty = null,
        LoadMode loadMode = LoadMode.ClosedLoop,
        int warmupSeconds = 0)
    {
        var directory = Path.Combine(Path.GetTempPath(), $"oss-benchmark-tests-{Guid.NewGuid():N}");
        var versionRoot = Path.Combine(directory, "v1");
        var scenarioDirectory = Path.Combine(versionRoot, "scenarios");
        var datasetDirectory = Path.Combine(versionRoot, "datasets");
        var workloadDirectory = Path.Combine(versionRoot, "workloads");
        Directory.CreateDirectory(scenarioDirectory);
        Directory.CreateDirectory(datasetDirectory);
        Directory.CreateDirectory(workloadDirectory);

        var dataset = new DatasetSpec
        {
            SchemaVersion = datasetSchemaVersion ?? DatasetSpec.CurrentSchemaVersion,
            Id = "test-dataset",
        };
        var datasetPath = Path.Combine(datasetDirectory, "dataset.json");
        var datasetBytes = AddUnknownProperty(
            JsonSerializer.SerializeToUtf8Bytes(dataset, BenchmarkJsonContext.Default.DatasetSpec),
            unknownDatasetProperty);
        await File.WriteAllBytesAsync(datasetPath, datasetBytes);

        var workload = new WorkloadSpec
        {
            SchemaVersion = workloadSchemaVersion ?? WorkloadSpec.CurrentSchemaVersion,
            Id = "test-workload",
            Mode = loadMode,
            WarmupSeconds = warmupSeconds,
            DurationSeconds = 1,
        };
        var workloadPath = Path.Combine(workloadDirectory, "workload.json");
        var workloadBytes = AddUnknownProperty(
            JsonSerializer.SerializeToUtf8Bytes(workload, BenchmarkJsonContext.Default.WorkloadSpec),
            unknownWorkloadProperty);
        await File.WriteAllBytesAsync(workloadPath, workloadBytes);

        var scenario = new BenchmarkScenarioSpec
        {
            SchemaVersion = scenarioSchemaVersion ?? BenchmarkScenarioSpec.CurrentSchemaVersion,
            Name = "test-scenario",
            Dataset = new SpecReference
            {
                Path = "datasets/dataset.json",
                Sha256 = Convert.ToHexStringLower(SHA256.HashData(datasetBytes)),
            },
            Workload = new SpecReference
            {
                Path = "workloads/workload.json",
                Sha256 = Convert.ToHexStringLower(SHA256.HashData(workloadBytes)),
            },
        };
        var scenarioBytes = AddUnknownProperty(
            JsonSerializer.SerializeToUtf8Bytes(scenario, BenchmarkJsonContext.Default.BenchmarkScenarioSpec),
            unknownScenarioProperty);
        var scenarioPath = Path.Combine(scenarioDirectory, "scenario.json");
        await File.WriteAllBytesAsync(scenarioPath, scenarioBytes);
        return new TemporaryBenchmarkSpecs(directory, scenarioPath, datasetPath, workloadPath);
    }

    private static byte[] AddUnknownProperty(byte[] json, string? propertyName)
    {
        if (propertyName is null)
        {
            return json;
        }

        using var document = JsonDocument.Parse(json);
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            foreach (var property in document.RootElement.EnumerateObject())
            {
                property.WriteTo(writer);
            }

            writer.WriteString(propertyName, "must-be-rejected");
            writer.WriteEndObject();
        }

        return stream.ToArray();
    }
}

internal sealed class TemporaryBenchmarkSpecs(
    string directory,
    string scenarioPath,
    string datasetPath,
    string workloadPath) : IDisposable
{
    public string ScenarioPath { get; } = scenarioPath;

    public string DatasetPath { get; } = datasetPath;

    public string WorkloadPath { get; } = workloadPath;

    public void Dispose()
    {
        Directory.Delete(directory, recursive: true);
    }
}
