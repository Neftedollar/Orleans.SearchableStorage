namespace Orleans.SearchableStorage.Benchmarks;

internal sealed class DriverOptions
{
    public const int MaximumClientCount = 4_096;
    public required string Command { get; init; }

    public required string SpecPath { get; init; }

    public string OutputDirectory { get; init; } = Path.Combine("artifacts", "benchmarks");

    public string InstanceId { get; init; } = Environment.MachineName;

    public string? RunId { get; init; }

    public int? ClientOrdinal { get; init; }

    public int? ClientCount { get; init; }

    public StorageBackend? Backend { get; init; }

    public StoragePath? ImplementationPath { get; init; }

    public string? ConnectionStringEnvironment { get; init; }

    public string? AzureBlobContainer { get; init; }

    public TopologyMode? Topology { get; init; }

    public string? AdvertisedAddress { get; init; }

    public string? PrimarySiloEndpoint { get; init; }

    public IReadOnlyList<string>? GatewayEndpoints { get; init; }

    public int? SiloPort { get; init; }

    public int? GatewayPort { get; init; }

    public static DriverOptions Parse(string[] args)
    {
        if (args.Length == 0 || args[0] is "--help" or "-h" or "help")
        {
            throw new CommandLineHelpException();
        }

        var command = args[0].ToLowerInvariant();
        if (command is not ("run" or "serve" or "validate"))
        {
            throw new ArgumentException($"Unknown command '{args[0]}'. Expected run, serve, or validate.");
        }

        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var index = 1; index < args.Length; index += 2)
        {
            var name = args[index];
            if (!name.StartsWith("--", StringComparison.Ordinal) || index + 1 >= args.Length)
            {
                throw new ArgumentException($"Option '{name}' must use the form --name value.");
            }

            if (!values.TryAdd(name[2..], args[index + 1]))
            {
                throw new ArgumentException($"Option '{name}' was specified more than once.");
            }
        }

        var known = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "spec",
            "output",
            "instance-id",
            "run-id",
            "client-ordinal",
            "client-count",
            "backend",
            "implementation-path",
            "connection-string-environment",
            "azure-blob-container",
            "topology",
            "advertised-address",
            "primary-silo",
            "gateways",
            "silo-port",
            "gateway-port",
        };
        var unknown = values.Keys.FirstOrDefault(key => !known.Contains(key));
        if (unknown is not null)
        {
            throw new ArgumentException($"Unknown option '--{unknown}'.");
        }

        return new DriverOptions
        {
            Command = command,
            SpecPath = GetRequired(values, "spec"),
            OutputDirectory = GetOptional(values, "output") ?? Path.Combine("artifacts", "benchmarks"),
            InstanceId = GetOptional(values, "instance-id") ?? Environment.MachineName,
            RunId = GetOptional(values, "run-id"),
            ClientOrdinal = ParseNonNegativeInteger(GetOptional(values, "client-ordinal"), "client-ordinal"),
            ClientCount = ParsePositiveInteger(GetOptional(values, "client-count"), "client-count"),
            Backend = ParseBackend(GetOptional(values, "backend")),
            ImplementationPath = ParseImplementationPath(GetOptional(values, "implementation-path")),
            ConnectionStringEnvironment = GetOptional(values, "connection-string-environment"),
            AzureBlobContainer = GetOptional(values, "azure-blob-container"),
            Topology = ParseTopology(GetOptional(values, "topology")),
            AdvertisedAddress = GetOptional(values, "advertised-address"),
            PrimarySiloEndpoint = GetOptional(values, "primary-silo"),
            GatewayEndpoints = ParseGateways(GetOptional(values, "gateways")),
            SiloPort = ParsePort(GetOptional(values, "silo-port"), "silo-port"),
            GatewayPort = ParsePort(GetOptional(values, "gateway-port"), "gateway-port"),
        };
    }

    public void ApplyTo(BenchmarkSpec spec)
    {
        CreateEffectiveOverrides().ApplyTo(spec);
    }

    internal EffectiveDriverOverrides CreateEffectiveOverrides()
    {
        return new EffectiveDriverOverrides(
            Backend,
            ImplementationPath,
            ConnectionStringEnvironment,
            AzureBlobContainer,
            Topology,
            AdvertisedAddress,
            PrimarySiloEndpoint,
            GatewayEndpoints?.ToArray(),
            SiloPort,
            GatewayPort);
    }

    public string ApplyRunIdentity(BenchmarkSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        var configured = RunId ?? Environment.GetEnvironmentVariable("OSS_BENCHMARK_RUN_ID");
        if (string.IsNullOrWhiteSpace(configured) && spec.Topology.Mode is TopologyMode.External)
        {
            throw new InvalidOperationException(
                "External topology requires --run-id or OSS_BENCHMARK_RUN_ID so silo and load processes share an isolated namespace.");
        }

        var raw = configured ?? CreateGeneratedRunId(
            DateTimeOffset.UtcNow,
            System.Security.Cryptography.RandomNumberGenerator.GetBytes(8));
        var runId = SanitizeRunId(raw);
        var path = spec.Storage.Path.ToString().ToLowerInvariant();
        spec.Topology.ClusterId = AppendIdentity(spec.Topology.ClusterId, runId, path, 150);
        spec.Topology.ServiceId = AppendIdentity(spec.Topology.ServiceId, runId, path, 150);
        if (spec.Storage.Backend is StorageBackend.AzureBlob)
        {
            spec.Storage.AzureBlobContainer = AppendIdentity(
                spec.Storage.AzureBlobContainer.ToLowerInvariant(),
                runId,
                path,
                63);
        }

        spec.Validate();
        return runId;
    }

    public static void ValidateExternalExecutionProvenance(BenchmarkSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (spec.Topology.Mode is not TopologyMode.External)
        {
            return;
        }

        var commit = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT");
        if (commit is null || commit.Length != 40 || commit.Any(static value => !Uri.IsHexDigit(value)))
        {
            throw new InvalidOperationException(
                "External benchmark processes require OSS_BENCHMARK_GIT_COMMIT to be the exact full 40-character source commit SHA.");
        }

        if (!bool.TryParse(Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY"), out _))
        {
            throw new InvalidOperationException(
                "External benchmark processes require an explicit boolean OSS_BENCHMARK_GIT_DIRTY provenance value.");
        }
    }

    public (int ClientOrdinal, int ClientCount) GetClientCoordinates(BenchmarkSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (spec.Topology.Mode is TopologyMode.External && (ClientOrdinal is null || ClientCount is null))
        {
            throw new InvalidOperationException(
                "External load clients require explicit --client-ordinal and --client-count coordinates.");
        }

        var ordinal = ClientOrdinal ?? 0;
        var count = ClientCount ?? 1;
        if (count > MaximumClientCount)
        {
            throw new InvalidOperationException($"Client count must not exceed {MaximumClientCount:N0}.");
        }
        if (ordinal >= count)
        {
            throw new InvalidOperationException(
                $"Client ordinal {ordinal} must be less than client count {count}.");
        }

        if (spec.Topology.Mode is TopologyMode.Embedded && (ordinal != 0 || count != 1))
        {
            throw new InvalidOperationException(
                "Embedded topology supports exactly one load client with ordinal 0 and count 1.");
        }

        if (spec.Workload.Mode is LoadMode.OpenLoop)
        {
            var maximumLocalSequence = checked((long)Math.Ceiling(
                spec.Workload.TargetRatePerSecond * Math.Max(
                    spec.Workload.WarmupSeconds,
                    spec.Workload.DurationSeconds)));
            if (maximumLocalSequence > (long.MaxValue - ordinal) / count)
            {
                throw new InvalidOperationException(
                    "The open-loop rate, duration, and client count exceed the global deterministic sequence range.");
            }
        }

        return (ordinal, count);
    }

    public static void PrintUsage(TextWriter writer)
    {
        writer.WriteLine("Orleans.SearchableStorage distributed load driver");
        writer.WriteLine();
        writer.WriteLine("  run      --spec <file> [--output <directory>] [topology overrides]");
        writer.WriteLine("  serve    --spec <file> [silo/backend overrides]");
        writer.WriteLine("  validate --spec <file>");
        writer.WriteLine();
        writer.WriteLine("Overrides:");
        writer.WriteLine("  --backend memory|postgresql|redis|azure-blob");
        writer.WriteLine("  --implementation-path searchable|plain  --run-id <shared-run-id>");
        writer.WriteLine("  --client-ordinal <zero-based ordinal>  --client-count <positive count>");
        writer.WriteLine("  --connection-string-environment <environment-variable-name>");
        writer.WriteLine("  --topology embedded|external");
        writer.WriteLine("  --gateways <host:port[,host:port...]>  --primary-silo <host:port>");
        writer.WriteLine("  --advertised-address <address>  --silo-port <port>  --gateway-port <port>");
    }

    private static string GetRequired(IReadOnlyDictionary<string, string> values, string name)
    {
        return GetOptional(values, name)
            ?? throw new ArgumentException($"Required option '--{name}' is missing.");
    }

    private static string? GetOptional(IReadOnlyDictionary<string, string> values, string name)
    {
        return values.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
    }

    private static StorageBackend? ParseBackend(string? value)
    {
        return value?.ToLowerInvariant() switch
        {
            null => null,
            "memory" => StorageBackend.Memory,
            "postgres" or "postgresql" => StorageBackend.PostgreSql,
            "redis" => StorageBackend.Redis,
            "azure" or "azure-blob" or "azureblob" => StorageBackend.AzureBlob,
            _ => throw new ArgumentException($"Unknown storage backend '{value}'."),
        };
    }

    private static StoragePath? ParseImplementationPath(string? value)
    {
        return value?.ToLowerInvariant() switch
        {
            null => null,
            "searchable" => StoragePath.Searchable,
            "plain" => StoragePath.Plain,
            _ => throw new ArgumentException($"Unknown implementation path '{value}'."),
        };
    }

    private static TopologyMode? ParseTopology(string? value)
    {
        return value?.ToLowerInvariant() switch
        {
            null => null,
            "embedded" => TopologyMode.Embedded,
            "external" => TopologyMode.External,
            _ => throw new ArgumentException($"Unknown topology '{value}'."),
        };
    }

    private static string[]? ParseGateways(string? value)
    {
        return value?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) switch
        {
            null => null,
            { Length: 0 } => throw new ArgumentException("At least one gateway endpoint is required."),
            { } endpoints => endpoints,
        };
    }

    private static int? ParsePort(string? value, string optionName)
    {
        if (value is null)
        {
            return null;
        }

        if (!int.TryParse(value, System.Globalization.NumberStyles.None, provider: null, out var port) || port is < 1 or > 65_535)
        {
            throw new ArgumentException($"Option '--{optionName}' must be an integer from 1 through 65535.");
        }

        return port;
    }

    private static int? ParseNonNegativeInteger(string? value, string optionName)
    {
        if (value is null)
        {
            return null;
        }

        if (!int.TryParse(value, System.Globalization.NumberStyles.None, provider: null, out var result) || result < 0)
        {
            throw new ArgumentException($"Option '--{optionName}' must be a nonnegative integer.");
        }

        return result;
    }

    private static int? ParsePositiveInteger(string? value, string optionName)
    {
        var result = ParseNonNegativeInteger(value, optionName);
        if (result == 0)
        {
            throw new ArgumentException($"Option '--{optionName}' must be a positive integer.");
        }

        return result;
    }

    internal static string CreateGeneratedRunId(DateTimeOffset timestamp, ReadOnlySpan<byte> entropy)
    {
        if (entropy.Length < 8)
        {
            throw new ArgumentException("Generated run ids require at least 64 bits of entropy.", nameof(entropy));
        }

        return $"{timestamp.UtcDateTime:yyyyMMddHHmmss}-{Convert.ToHexStringLower(entropy[..8])}";
    }

    internal static string SanitizeRunId(string value)
    {
        var replaced = new string(value
            .ToLowerInvariant()
            .Select(static character => char.IsAsciiLetterOrDigit(character) ? character : '-')
            .ToArray());
        var normalized = string.Join(
            '-',
            replaced.Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
        if (normalized.Length == 0)
        {
            throw new ArgumentException("Run id must contain at least one ASCII letter or digit.", nameof(value));
        }

        if (normalized.Length <= 32)
        {
            return normalized;
        }

        var hash = Convert.ToHexStringLower(
            System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(normalized)))[..12];
        return $"{normalized[..19].TrimEnd('-')}-{hash}";
    }

    internal static string AppendIdentity(string prefix, string runId, string path, int maximumLength)
    {
        var suffix = $"-{runId}-{path}";
        var availablePrefix = maximumLength - suffix.Length;
        if (availablePrefix < 1)
        {
            throw new InvalidOperationException("The run identity is too long for the backend namespace.");
        }

        var trimmedPrefix = string.Join(
            '-',
            prefix.Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
        if (trimmedPrefix.Length == 0)
        {
            throw new InvalidOperationException("The namespace prefix must contain an ASCII letter or digit.");
        }

        if (trimmedPrefix.Length > availablePrefix)
        {
            const int hashLength = 12;
            var hash = Convert.ToHexStringLower(
                System.Security.Cryptography.SHA256.HashData(
                    System.Text.Encoding.UTF8.GetBytes($"{trimmedPrefix}{suffix}")))[..hashLength];
            var hashedPrefixLength = availablePrefix - hashLength - 1;
            if (hashedPrefixLength < 1)
            {
                throw new InvalidOperationException("The run identity is too long for a collision-resistant backend namespace.");
            }

            var shortenedPrefix = trimmedPrefix[..hashedPrefixLength].TrimEnd('-');
            if (shortenedPrefix.Length == 0)
            {
                shortenedPrefix = trimmedPrefix[..1];
            }

            trimmedPrefix = $"{shortenedPrefix}-{hash}";
        }

        return trimmedPrefix + suffix;
    }
}

internal sealed class CommandLineHelpException : Exception;
