using System.Text.Json.Nodes;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

internal static class CompatibilityManifest
{
    private static readonly JsonNode Root = JsonNode.Parse(
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "compatibility-manifest.json")))
        ?? throw new InvalidOperationException("The compatibility manifest is empty.");

    public static int GetInt(params string[] path) => Resolve(path).GetValue<int>();

    public static string GetString(params string[] path) =>
        Resolve(path).GetValue<string>();

    public static IReadOnlyDictionary<string, int> GetIntMap(params string[] path)
    {
        var result = Resolve(path).AsObject();
        return result.ToDictionary(
            static pair => pair.Key,
            static pair => pair.Value?.GetValue<int>()
                ?? throw new InvalidOperationException("A manifest map value is null."),
            StringComparer.Ordinal);
    }

    public static IReadOnlyList<string> GetStrings(params string[] path)
    {
        return Resolve(path).AsArray()
            .Select(static value => value?.GetValue<string>()
                ?? throw new InvalidOperationException("A manifest array value is null."))
            .ToArray();
    }

    private static JsonNode Resolve(IReadOnlyList<string> path)
    {
        JsonNode? current = Root;
        foreach (var component in path)
        {
            current = current[component];
            if (current is null)
            {
                throw new InvalidOperationException(
                    $"Compatibility manifest path '{string.Join('.', path)}' does not exist.");
            }
        }

        return current;
    }
}
