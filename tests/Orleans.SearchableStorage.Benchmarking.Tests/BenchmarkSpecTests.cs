using System.Text.Json;
using System.Text.Json.Nodes;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class BenchmarkSpecTests
{
    [Theory]
    [InlineData("scenario")]
    [InlineData("dataset")]
    [InlineData("workload")]
    public async Task LoadAsyncRejectsUnknownProperties(string artifact)
    {
        using var specs = await BenchmarkTestData.WriteSpecsAsync(
            unknownScenarioProperty: artifact == "scenario" ? "unexpected" : null,
            unknownDatasetProperty: artifact == "dataset" ? "unexpected" : null,
            unknownWorkloadProperty: artifact == "workload" ? "unexpected" : null);

        var act = () => BenchmarkSpec.LoadAsync(specs.ScenarioPath, CancellationToken.None);

        await Assert.ThrowsAsync<JsonException>(act);
    }

    [Theory]
    [InlineData("scenario")]
    [InlineData("dataset")]
    [InlineData("workload")]
    public async Task LoadAsyncRejectsUnknownSchemaVersions(string artifact)
    {
        using var specs = await BenchmarkTestData.WriteSpecsAsync(
            scenarioSchemaVersion: artifact == "scenario" ? "oss-benchmark-scenario/v999" : null,
            datasetSchemaVersion: artifact == "dataset" ? "oss-benchmark-dataset/v999" : null,
            workloadSchemaVersion: artifact == "workload" ? "oss-benchmark-workload/v999" : null);

        var act = () => BenchmarkSpec.LoadAsync(specs.ScenarioPath, CancellationToken.None);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(act);
        Assert.Contains("Unsupported", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LoadAsyncRejectsDigestMismatch()
    {
        using var specs = await BenchmarkTestData.WriteSpecsAsync();
        await File.AppendAllTextAsync(specs.DatasetPath, " ");

        var act = () => BenchmarkSpec.LoadAsync(specs.ScenarioPath, CancellationToken.None);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(act);
        Assert.Contains("expected", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SHA-256", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SpecReferencesRequireTheCanonicalLowercaseDigestForm()
    {
        var reference = new SpecReference
        {
            Path = "dataset.json",
            Sha256 = new string('A', 64),
        };

        var exception = Assert.Throws<InvalidDataException>(reference.Validate);

        Assert.Contains("lowercase", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LoadAsyncRejectsParentTraversalBeforeReadingReferencedArtifact()
    {
        using var specs = await BenchmarkTestData.WriteSpecsAsync();
        var scenario = JsonNode.Parse(await File.ReadAllTextAsync(specs.ScenarioPath))!.AsObject();
        scenario["dataset"]!["path"] = "../outside.json";
        await File.WriteAllTextAsync(specs.ScenarioPath, scenario.ToJsonString());

        var exception = await Assert.ThrowsAsync<InvalidDataException>(
            () => BenchmarkSpec.LoadAsync(specs.ScenarioPath, CancellationToken.None));

        Assert.Contains("traversal", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task LoadAsyncRejectsSymbolicLinkReferences()
    {
        using var specs = await BenchmarkTestData.WriteSpecsAsync();
        var datasetDirectory = Path.GetDirectoryName(specs.DatasetPath)!;
        var linkPath = Path.Combine(datasetDirectory, "linked-dataset.json");
        File.CreateSymbolicLink(linkPath, specs.DatasetPath);
        var scenario = JsonNode.Parse(await File.ReadAllTextAsync(specs.ScenarioPath))!.AsObject();
        scenario["dataset"]!["path"] = "datasets/linked-dataset.json";
        await File.WriteAllTextAsync(specs.ScenarioPath, scenario.ToJsonString());

        var exception = await Assert.ThrowsAsync<InvalidDataException>(
            () => BenchmarkSpec.LoadAsync(specs.ScenarioPath, CancellationToken.None));

        Assert.Contains("symbolic", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DatasetValidationAcceptsOneBillionRecordsWithoutMaterialization()
    {
        var dataset = new DatasetSpec
        {
            Id = "capacity-1b",
            RecordCount = 1_000_000_000,
        };
        dataset.Validate();
        var before = GC.GetAllocatedBytesForCurrentThread();

        dataset.Validate();

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.InRange(allocated, 0, 4_096);
        Assert.Equal(1_000_000_000, dataset.RecordCount);
    }

    [Fact]
    public void PlainStorageRejectsSearchOperations()
    {
        var spec = BenchmarkTestData.CreateSpec(StoragePath.Plain);

        var exception = Assert.Throws<InvalidDataException>(spec.Validate);

        Assert.Contains("Plain storage", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void PlainStorageAcceptsNonSearchWorkload()
    {
        var operations = new OperationMixSpec
        {
            Upsert = 50,
            Read = 50,
            ExactQuery = 0,
            RangeQuery = 0,
        };
        var spec = BenchmarkTestData.CreateSpec(StoragePath.Plain, operations);

        spec.Validate();
    }

    [Fact]
    public void CorrectnessAuditRequiresPopulationInSchemaVersionOne()
    {
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "audit-without-population",
            Population = new PopulationSpec { Enabled = false },
            Audit = new CorrectnessAuditSpec { Enabled = true },
        };
        var spec = new BenchmarkSpec(
            scenario,
            new DatasetSpec { Id = "dataset" },
            new WorkloadSpec { Id = "workload" });

        var exception = Assert.Throws<InvalidDataException>(spec.Validate);

        Assert.Contains("population.enabled=true", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Host=db;Password=literal-secret")]
    [InlineData("1INVALID")]
    [InlineData("INVALID-NAME")]
    [InlineData("INVALID NAME")]
    public void ConnectionStringEnvironmentMustBeAPortableIdentifier(string value)
    {
        var spec = BenchmarkTestData.CreateSpec();
        spec.Storage.Backend = StorageBackend.PostgreSql;
        spec.Storage.ConnectionStringEnvironment = value;

        var exception = Assert.Throws<InvalidDataException>(spec.Validate);

        Assert.Contains("environment-variable name", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void BroadUnpagedQueryResultDeclarationIsRejected()
    {
        var workload = new WorkloadSpec
        {
            Id = "unsafe-broad-query",
            QuerySelectivity = new QuerySelectivitySpec
            {
                MaximumExpectedResultCount = QuerySelectivitySpec.MaximumPortableExpectedResultCount + 1,
            },
        };

        var exception = Assert.Throws<InvalidDataException>(workload.Validate);

        Assert.Contains("bounded query delivery", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OperationalAllocationCapsAreEnforcedBeforeExecution()
    {
        var workload = new WorkloadSpec
        {
            Id = "unsafe-concurrency",
            Concurrency = WorkloadSpec.MaximumConcurrency + 1,
        };
        var population = new PopulationSpec
        {
            Concurrency = PopulationSpec.MaximumConcurrency + 1,
        };

        Assert.Throws<InvalidDataException>(workload.Validate);
        Assert.Throws<InvalidDataException>(population.Validate);
    }

    [Fact]
    public void RuntimeRejectsSchemaInvalidExecutionClassAndClosedLoopRate()
    {
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "invalid-execution-class",
            ExecutionClass = string.Empty,
        };
        var workload = new WorkloadSpec
        {
            Id = "invalid-rate",
            Mode = LoadMode.ClosedLoop,
            TargetRatePerSecond = 0,
        };

        Assert.Throws<ArgumentException>(scenario.Validate);
        Assert.Throws<InvalidDataException>(workload.Validate);
    }

    [Fact]
    public void EmbeddedTopologyRequiresDeclaredTotalToMatchHostedSilos()
    {
        var topology = new TopologySpec
        {
            Mode = TopologyMode.Embedded,
            SiloCount = 2,
            EmbeddedSiloCount = 1,
        };

        var exception = Assert.Throws<InvalidDataException>(topology.Validate);

        Assert.Contains("siloCount", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ExternalTopologyCarriesExplicitTotalSiloCount()
    {
        var topology = new TopologySpec
        {
            Mode = TopologyMode.External,
            SiloCount = 4,
            GatewayEndpoints = ["127.0.0.1:30000"],
        };

        topology.Validate();

        Assert.Equal(4, topology.SiloCount);
    }
}
