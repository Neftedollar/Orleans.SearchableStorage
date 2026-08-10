using System.Collections.Concurrent;
using System.Text.Json;
using Json.Schema;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class SpecSchemaTests
{
    private static readonly ConcurrentDictionary<string, Lazy<Task<JsonSchema>>> Schemas =
        new(StringComparer.Ordinal);

    [Fact]
    public async Task EveryCheckedInSpecConformsToItsCheckedInJsonSchema()
    {
        var specsRoot = Path.Combine(AppContext.BaseDirectory, "specs");
        var schemaRoot = Path.Combine(specsRoot, "schema");
        var schemaByProfileDirectory = new Dictionary<string, JsonSchema>(StringComparer.Ordinal)
        {
            ["datasets"] = await LoadSchemaAsync(Path.Combine(schemaRoot, "dataset.v1.schema.json")),
            ["workloads"] = await LoadSchemaAsync(Path.Combine(schemaRoot, "workload.v1.schema.json")),
            ["scenarios"] = await LoadSchemaAsync(Path.Combine(schemaRoot, "scenario.v1.schema.json")),
        };
        var profiles = Directory
            .EnumerateFiles(Path.Combine(specsRoot, "v1"), "*.json", SearchOption.AllDirectories)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.NotEmpty(profiles);

        var failures = new List<string>();
        foreach (var profile in profiles)
        {
            var profileDirectory = new DirectoryInfo(Path.GetDirectoryName(profile)!).Name;
            if (!schemaByProfileDirectory.TryGetValue(profileDirectory, out var schema))
            {
                failures.Add($"{profile}: no schema mapping for directory '{profileDirectory}'.");
                continue;
            }

            using var instance = JsonDocument.Parse(await File.ReadAllTextAsync(profile));
            var result = schema.Evaluate(
                instance.RootElement,
                new EvaluationOptions { OutputFormat = OutputFormat.Hierarchical });
            if (!result.IsValid)
            {
                failures.Add($"{profile}:{Environment.NewLine}{FormatFailure(result)}");
            }
        }

        Assert.True(
            failures.Count == 0,
            $"Checked-in benchmark specs must conform to their schemas:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");
    }

    [Fact]
    public async Task ScenarioSchemaRejectsAuditWithoutPopulation()
    {
        var specsRoot = Path.Combine(AppContext.BaseDirectory, "specs");
        var schema = await LoadSchemaAsync(Path.Combine(specsRoot, "schema", "scenario.v1.schema.json"));
        var scenarioPath = Directory
            .EnumerateFiles(Path.Combine(specsRoot, "v1", "scenarios"), "*.json")
            .Order(StringComparer.Ordinal)
            .First();
        var node = System.Text.Json.Nodes.JsonNode.Parse(await File.ReadAllTextAsync(scenarioPath))!;
        node["population"]!["enabled"] = false;
        node["audit"]!["enabled"] = true;
        using var instance = JsonDocument.Parse(node.ToJsonString());

        var result = schema.Evaluate(
            instance.RootElement,
            new EvaluationOptions { OutputFormat = OutputFormat.Hierarchical });

        Assert.False(result.IsValid);
    }

    [Fact]
    public async Task ScenarioSchemaRejectsNonCanonicalUppercaseDigests()
    {
        var specsRoot = Path.Combine(AppContext.BaseDirectory, "specs");
        var schema = await LoadSchemaAsync(Path.Combine(specsRoot, "schema", "scenario.v1.schema.json"));
        var scenarioPath = Directory
            .EnumerateFiles(Path.Combine(specsRoot, "v1", "scenarios"), "*.json")
            .Order(StringComparer.Ordinal)
            .First();
        var node = System.Text.Json.Nodes.JsonNode.Parse(await File.ReadAllTextAsync(scenarioPath))!;
        node["dataset"]!["sha256"] = new string('A', 64);
        using var instance = JsonDocument.Parse(node.ToJsonString());

        var result = schema.Evaluate(
            instance.RootElement,
            new EvaluationOptions { OutputFormat = OutputFormat.Hierarchical });

        Assert.False(result.IsValid);
    }

    private static Task<JsonSchema> LoadSchemaAsync(string path)
    {
        Assert.True(File.Exists(path), $"Missing checked-in JSON Schema: {path}");
        var fullPath = Path.GetFullPath(path);
        return Schemas.GetOrAdd(
            fullPath,
            static schemaPath => new Lazy<Task<JsonSchema>>(
                async () => JsonSchema.FromText(
                    await File.ReadAllTextAsync(schemaPath),
                    baseUri: new Uri(schemaPath)),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;
    }

    private static string FormatFailure(EvaluationResults result)
    {
        var messages = new List<string>();
        CollectFailures(result, messages);
        return string.Join(Environment.NewLine, messages);
    }

    private static void CollectFailures(EvaluationResults result, ICollection<string> messages)
    {
        if (result.Errors is not null)
        {
            foreach (var (keyword, message) in result.Errors)
            {
                messages.Add($"  {result.InstanceLocation} [{keyword}]: {message}");
            }
        }

        foreach (var detail in result.Details ?? [])
        {
            CollectFailures(detail, messages);
        }
    }
}
