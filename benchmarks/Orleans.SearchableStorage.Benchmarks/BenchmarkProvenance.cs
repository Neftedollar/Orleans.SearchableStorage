using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkProvenance
{
    private const string Unknown = "unknown";

    public const string BenchmarkDotNetExecutionMode = "BenchmarkDotNet";
    public const string BenchmarkDotNetInProcessDryRunExecutionMode =
        "BenchmarkDotNetInProcessDryRun";
    public const string DeterministicEvidenceExecutionMode = "DeterministicEvidence";

    internal static IReadOnlyList<string> ExecutionModes { get; } =
    [
        BenchmarkDotNetExecutionMode,
        BenchmarkDotNetInProcessDryRunExecutionMode,
        DeterministicEvidenceExecutionMode,
    ];

    public static string ResolveArtifactsPath(IReadOnlyList<string> arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        var path = "BenchmarkDotNet.Artifacts";
        for (var index = 0; index < arguments.Count; index++)
        {
            var argument = arguments[index];
            if (argument is "--artifacts" or "-a")
            {
                if (index + 1 < arguments.Count)
                {
                    path = arguments[++index];
                }

                continue;
            }

            const string longPrefix = "--artifacts=";
            const string shortPrefix = "-a=";
            if (argument.StartsWith(longPrefix, StringComparison.Ordinal))
            {
                path = argument[longPrefix.Length..];
            }
            else if (argument.StartsWith(shortPrefix, StringComparison.Ordinal))
            {
                path = argument[shortPrefix.Length..];
            }
        }

        return path;
    }

    public static void Write(
        string artifactsPath,
        SearchableStorageBenchmarkConfig config,
        string executionMode)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(artifactsPath);
        ArgumentNullException.ThrowIfNull(config);
        if (!ExecutionModes.Contains(executionMode, StringComparer.Ordinal))
        {
            throw new ArgumentOutOfRangeException(
                nameof(executionMode),
                executionMode,
                "Unknown benchmark provenance execution mode.");
        }

        var directory = Path.GetFullPath(artifactsPath);
        Directory.CreateDirectory(directory);
        var payload = new BenchmarkProvenanceDocument(
            SchemaVersion: "oss-benchmarkdotnet-provenance/v1",
            ExecutionMode: executionMode,
            CapturedAtUtc: DateTimeOffset.UtcNow,
            GitCommit: ReadEnvironment("OSS_BENCHMARK_GIT_COMMIT")
                ?? ReadEnvironment("GITHUB_SHA")
                ?? RunGit("rev-parse", "HEAD")
                ?? Unknown,
            GitDirty: ReadDirtyState(),
            BenchmarkAssemblyVersion: GetVersion(typeof(BenchmarkProvenance).Assembly),
            BenchmarkDotNetVersion: GetVersion(typeof(BenchmarkAttribute).Assembly),
            SearchableStorageVersion: GetVersion(typeof(SearchableIndexAttribute).Assembly),
            FrameworkDescription: RuntimeInformation.FrameworkDescription,
            OsDescription: RuntimeInformation.OSDescription,
            ProcessArchitecture: RuntimeInformation.ProcessArchitecture.ToString(),
            ProcessorCount: Environment.ProcessorCount,
            JobIdentity: config.GetValidatedJobIdentity());
        var path = Path.Combine(directory, "provenance.json");
        File.WriteAllText(
            path,
            JsonSerializer.Serialize(payload, BenchmarkProvenanceJsonContext.Default.BenchmarkProvenanceDocument));
    }

    private static bool? ReadDirtyState()
    {
        var configured = ReadEnvironment("OSS_BENCHMARK_GIT_DIRTY");
        if (configured is not null && bool.TryParse(configured, out var dirty))
        {
            return dirty;
        }

        var status = RunGit("status", "--porcelain");
        return status is null ? null : status.Length > 0;
    }

    private static string? RunGit(params string[] arguments)
    {
        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "git",
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

            var output = process.StandardOutput.ReadToEnd().Trim();
            if (!process.WaitForExit(milliseconds: 5_000) || process.ExitCode != 0)
            {
                try
                {
                    process.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                    // The process exited between the timeout check and the best-effort kill.
                }

                return null;
            }

            return output;
        }
        catch (Exception exception) when (
            exception is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            return null;
        }
    }

    private static string? ReadEnvironment(string name)
    {
        return Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : null;
    }

    private static string GetVersion(Assembly assembly)
    {
        return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? assembly.GetName().Version?.ToString()
            ?? Unknown;
    }
}

internal sealed record BenchmarkProvenanceDocument(
    string SchemaVersion,
    string ExecutionMode,
    DateTimeOffset CapturedAtUtc,
    string GitCommit,
    bool? GitDirty,
    string BenchmarkAssemblyVersion,
    string BenchmarkDotNetVersion,
    string SearchableStorageVersion,
    string FrameworkDescription,
    string OsDescription,
    string ProcessArchitecture,
    int ProcessorCount,
    string JobIdentity);

[System.Text.Json.Serialization.JsonSerializable(typeof(BenchmarkProvenanceDocument))]
[System.Text.Json.Serialization.JsonSourceGenerationOptions(WriteIndented = true)]
internal sealed partial class BenchmarkProvenanceJsonContext : JsonSerializerContext;
