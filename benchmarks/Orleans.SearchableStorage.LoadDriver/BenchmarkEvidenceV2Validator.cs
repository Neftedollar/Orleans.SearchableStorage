using System.Security.Cryptography;
using System.Text.Json;
using Json.Schema;

namespace Orleans.SearchableStorage.Benchmarks;

/// <summary>
/// Validates the frozen, aggregate benchmark-evidence contract. This is intentionally
/// separate from the executable v1 load-driver contract: v2 evidence describes reviewed
/// lifecycle cases and their qualification thresholds without changing v1 interpretation.
/// </summary>
internal static class BenchmarkEvidenceV2Validator
{
    public const string ResultSchemaVersion = "oss-benchmark-result/v2";
    private const string ProfileSchemaVersion = "oss-benchmark-reference-profile/v2";
    private const string ResultSchemaFileName = "result.v2.schema.json";
    private const string ProfileSchemaFileName = "reference-profile.v2.schema.json";

    private static readonly Dictionary<string, string> MetricUnits =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["offered-operations"] = "count",
            ["completed-operations"] = "count",
            ["failed-operations"] = "count",
            ["timed-out-operations"] = "count",
            ["error-rate"] = "ratio",
            ["timeout-rate"] = "ratio",
            ["latency-p50"] = "milliseconds",
            ["latency-p95"] = "milliseconds",
            ["latency-p99"] = "milliseconds",
            ["latency-p999"] = "milliseconds",
            ["returned-grain-ids"] = "count",
            ["completed-pages"] = "count",
            ["facet-values"] = "count",
            ["hydrated-grains"] = "count",
            ["activated-grains"] = "count",
            ["gen2-collections"] = "count",
            ["replayed-entries"] = "count",
            ["compacted-entries"] = "count",
            ["rebuilt-records"] = "count",
            ["resume-checkpoints"] = "count",
            ["retained-managed-bytes"] = "bytes",
            ["peak-managed-bytes"] = "bytes",
            ["memory-headroom-bytes"] = "bytes",
            ["cold-activation-duration"] = "milliseconds",
            ["recovery-duration"] = "milliseconds",
            ["gc-pause-duration"] = "milliseconds",
            ["compaction-pause-duration"] = "milliseconds",
            ["logical-orleans-read-calls"] = "count",
            ["logical-orleans-write-calls"] = "count",
            ["logical-orleans-delete-calls"] = "count",
            ["provider-read-call-amplification"] = "ratio",
            ["provider-write-call-amplification"] = "ratio",
            ["provider-delete-call-amplification"] = "ratio",
            ["provider-byte-amplification"] = "ratio",
        };

    private static readonly Dictionary<string, string> ProviderMetricUnits =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["read-calls"] = "count",
            ["write-calls"] = "count",
            ["delete-calls"] = "count",
            ["read-bytes"] = "bytes",
            ["write-bytes"] = "bytes",
            ["delete-bytes"] = "bytes",
        };

    private static readonly string[] RequiredLifecycleConditions =
        ["hot-application-grain", "warm-storage-owner", "cold-storage-owner"];
    private static readonly string[] ProviderOperations = ["read", "write", "delete"];

    private static readonly Lazy<JsonSchema> ResultSchema = new(
        () => LoadSchema(ResultSchemaFileName),
        LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<JsonSchema> ProfileSchema = new(
        () => LoadSchema(ProfileSchemaFileName),
        LazyThreadSafetyMode.ExecutionAndPublication);

    public static async Task ValidateAsync(
        string resultPath,
        byte[] resultBytes,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resultPath);
        ArgumentNullException.ThrowIfNull(resultBytes);
        cancellationToken.ThrowIfCancellationRequested();

        ValidateAgainstSchema(resultBytes, ResultSchema.Value, ResultSchemaFileName, "benchmark evidence result");
        using var resultDocument = JsonDocument.Parse(resultBytes);
        var result = resultDocument.RootElement;
        Require(
            GetString(result, "schemaVersion") == ResultSchemaVersion,
            $"Unexpected benchmark evidence schema version; expected '{ResultSchemaVersion}'.");

        var profileReference = result.GetProperty("profile");
        var profileBytes = await LoadProfileAsync(resultPath, profileReference, cancellationToken);
        ValidateAgainstSchema(profileBytes, ProfileSchema.Value, ProfileSchemaFileName, "reference profile");
        using var profileDocument = JsonDocument.Parse(profileBytes);
        await ValidateRawEvidenceArtifactsAsync(
            resultPath,
            result.GetProperty("run"),
            cancellationToken);
        ValidateSemantics(result, profileDocument.RootElement);
    }

    private static async Task<byte[]> LoadProfileAsync(
        string resultPath,
        JsonElement profileReference,
        CancellationToken cancellationToken)
    {
        var resultDirectory = Path.GetDirectoryName(Path.GetFullPath(resultPath))
            ?? throw new InvalidDataException("Benchmark evidence result path has no parent directory.");
        var profilePath = ResolveContentAddressedPath(
            resultDirectory,
            GetString(profileReference, "path"),
            "Reference profile");
        var bytes = await File.ReadAllBytesAsync(profilePath, cancellationToken);
        Require(bytes.Length > 0, "Reference profile is empty.");
        Require(
            Convert.ToHexStringLower(SHA256.HashData(bytes)) == GetString(profileReference, "sha256"),
            "Reference profile SHA-256 does not match its bytes.");
        return bytes;
    }

    private static void ValidateSemantics(JsonElement result, JsonElement profile)
    {
        Require(
            GetString(profile, "schemaVersion") == ProfileSchemaVersion,
            $"Unexpected reference profile schema version; expected '{ProfileSchemaVersion}'.");
        var profileReference = result.GetProperty("profile");
        Require(
            GetString(profileReference, "id") == GetString(profile, "id"),
            "Result profile id differs from the embedded reference profile.");

        var classification = GetString(result, "classification");
        Require(
            classification == GetString(profile, "classification"),
            "Result classification differs from the embedded reference profile.");
        var profileStatus = GetString(profile, "status");
        Require(
            classification == "schema-fixture" && profileStatus == "contract-smoke" ||
            classification == "qualification" && profileStatus == "frozen",
            "Reference-profile status does not match its classification.");

        var qualified = result.GetProperty("qualified").GetBoolean();
        var scaleClaim = result.GetProperty("scaleClaim").GetBoolean();
        var run = result.GetProperty("run");
        var backend = GetString(run, "backend");
        var topology = GetString(run, "topology");
        Require(backend == GetString(profile, "backend"), "Run backend differs from the reference profile.");
        Require(
            GetString(run, "implementationPath") == GetString(profile, "implementationPath"),
            "Run implementation path differs from the reference profile.");
        Require(topology == GetString(profile, "topology"), "Run topology differs from the reference profile.");

        if (classification == "schema-fixture")
        {
            Require(!qualified, "Schema fixtures can exercise evaluation but cannot be qualified.");
            Require(!scaleClaim, "Schema fixtures cannot make a scale claim.");
        }

        if (scaleClaim)
        {
            Require(qualified, "A scale claim requires qualified evidence.");
            Require(classification == "qualification", "A scale claim requires qualification-class evidence.");
            Require(topology == "external", "A scale claim requires an external topology.");
            Require(
                run.GetProperty("recordCount").GetInt64() >= 10_000_000,
                "A scale claim requires a complete run of at least 10,000,000 records.");
            Require(
                run.GetProperty("siloCount").GetInt32() >= 2,
                "A scale claim requires at least two declared silos.");
        }

        var profileCases = IndexById(profile.GetProperty("cases"), "reference-profile case");
        var resultCases = IndexById(result.GetProperty("cases"), "result case");
        RequireSameKeys(profileCases, resultCases, "Result cases must exactly cover the reference profile cases.");

        var allPhasesCompleted = true;
        var allRequiredProviderTelemetryObserved = true;
        foreach (var (caseId, profileCase) in profileCases)
        {
            ValidateProfileCaseShape(profileCase, backend, caseId);
            var resultCase = resultCases[caseId];
            RequireCopiedValue(profileCase, resultCase, "workload", caseId);
            RequireCopiedValue(profileCase, resultCase, "lifecycle", caseId);
            RequireCopiedValue(profileCase, resultCase, "accessPath", caseId);
            ValidateImplementationCapability(profile, profileCase, caseId);

            ValidateFanout(profileCase, resultCase, run, caseId);
            allPhasesCompleted &= ValidatePhases(profileCase, resultCase, run, caseId);
            allRequiredProviderTelemetryObserved &= ValidateByteAndProviderEvidence(
                profileCase,
                resultCase,
                backend,
                caseId);
            ValidateDerivedCaseMetrics(resultCase, caseId);
        }

        var thresholds = IndexById(profile.GetProperty("thresholds"), "reference-profile threshold");
        ValidateThresholdDefinitions(thresholds, profileCases);
        var evaluations = IndexByProperty(
            result.GetProperty("thresholdEvaluations"),
            "thresholdId",
            "threshold evaluation");
        RequireSameKeys(thresholds, evaluations, "Threshold evaluations must exactly cover the reference profile thresholds.");

        var allThresholdsPassed = true;
        foreach (var (thresholdId, threshold) in thresholds)
        {
            var evaluation = evaluations[thresholdId];
            var caseId = GetString(threshold, "caseId");
            var phaseName = GetString(threshold, "phase");
            var metricName = GetString(threshold, "metric");
            var metric = FindMetric(resultCases[caseId], phaseName, metricName, thresholdId);
            Require(
                GetString(metric, "unit") == GetString(threshold, "unit"),
                $"Threshold '{thresholdId}' unit differs from its observed metric.");

            var observedValue = metric.GetProperty("value").GetDouble();
            Require(
                observedValue == evaluation.GetProperty("observedValue").GetDouble(),
                $"Threshold '{thresholdId}' observed value differs from its phase metric.");
            var expectedPass = GetString(threshold, "comparison") switch
            {
                "less-than-or-equal" => observedValue <= threshold.GetProperty("value").GetDouble(),
                "greater-than-or-equal" => observedValue >= threshold.GetProperty("value").GetDouble(),
                var comparison => throw new InvalidDataException(
                    $"Threshold '{thresholdId}' has unsupported comparison '{comparison}'."),
            };
            Require(
                evaluation.GetProperty("passed").GetBoolean() == expectedPass,
                $"Threshold '{thresholdId}' pass/fail value was not produced by the frozen evaluator.");
            allThresholdsPassed &= expectedPass;
        }

        if (qualified)
        {
            Require(classification == "qualification", "Qualified evidence requires a qualification profile.");
            Require(topology == "external", "Qualified evidence requires an external topology.");
            Require(!run.GetProperty("gitDirty").GetBoolean(), "Qualified evidence requires a clean source revision.");
            Require(allPhasesCompleted, "Qualified evidence requires every declared phase to complete.");
            Require(allRequiredProviderTelemetryObserved, "Qualified evidence is missing required provider-native telemetry.");
            Require(allThresholdsPassed, "Qualified evidence must pass every frozen threshold.");
        }

        if (classification == "qualification")
        {
            Require(topology == "external", "Qualification profiles require an external topology.");
            Require(
                GetString(run, "gitCommit") != new string('0', 40),
                "Qualification evidence requires a real non-zero source commit.");
            ValidateQualificationProfileCoverage(profile, profileCases, thresholds, backend);
            ValidateQualificationArtifactCoverage(run, profileCases, thresholds, backend);
            if (backend != "memory")
            {
                Require(
                    profileCases.Values.All(static item => GetString(item, "providerTelemetryPolicy") == "required"),
                    "Durable-provider qualification profiles require provider-native telemetry for every case.");
            }
        }
    }

    private static void ValidateProfileCaseShape(
        JsonElement profileCase,
        string backend,
        string caseId)
    {
        var workload = GetString(profileCase, "workload");
        var expectedAccessPath = workload switch
        {
            "point-read" => "storage-owner-point-read",
            "point-write" => "storage-owner-point-write",
            "point-delete" => "storage-owner-point-delete",
            "public-paging" => "hash-posting",
            "public-facet" => "facet-candidate-metadata",
            "public-hydration" => "hydration-batch",
            "storage-recovery" => "storage-owner-recovery",
            "schema-rebuild" => "schema-rebuild-coordinator",
            "schema-resume" => "schema-resume-checkpoint",
            _ => throw new InvalidDataException($"Case '{caseId}' has unsupported workload '{workload}'."),
        };
        Require(
            GetString(profileCase, "accessPath") == expectedAccessPath,
            $"Case '{caseId}' access path does not match workload '{workload}'.");

        var expectedPhases = workload switch
        {
            "storage-recovery" => new HashSet<string>(
                ["activation", "garbage-collection", "journal-replay", "compaction"],
                StringComparer.Ordinal),
            "schema-rebuild" => new HashSet<string>(["schema-rebuild"], StringComparer.Ordinal),
            "schema-resume" => new HashSet<string>(["schema-resume"], StringComparer.Ordinal),
            _ => new HashSet<string>(["steady-state"], StringComparer.Ordinal),
        };
        var phases = profileCase.GetProperty("requiredPhases")
            .EnumerateArray()
            .Select(static phase => phase.GetString()!)
            .ToHashSet(StringComparer.Ordinal);
        Require(
            expectedPhases.SetEquals(phases),
            $"Case '{caseId}' required phases do not match workload '{workload}'.");
        if (workload == "storage-recovery")
        {
            Require(
                GetString(profileCase, "lifecycle") == "cold-storage-owner",
                $"Recovery case '{caseId}' must explicitly exercise a cold storage owner.");
        }

        var providerPolicy = GetString(profileCase, "providerTelemetryPolicy");
        Require(
            backend == "memory"
                ? providerPolicy == "not-applicable"
                : providerPolicy != "not-applicable",
            $"Case '{caseId}' provider telemetry policy does not match backend '{backend}'.");
    }

    private static void ValidateQualificationProfileCoverage(
        JsonElement profile,
        IReadOnlyDictionary<string, JsonElement> cases,
        IReadOnlyDictionary<string, JsonElement> thresholds,
        string backend)
    {
        var implementationPath = GetString(profile, "implementationPath");
        var workloads = cases.Values
            .Select(static item => GetString(item, "workload"))
            .ToHashSet(StringComparer.Ordinal);
        var requiredWorkloads = implementationPath == "plain"
            ? new HashSet<string>(["point-read", "point-write", "point-delete"], StringComparer.Ordinal)
            : new HashSet<string>(
                [
                    "point-read",
                    "point-write",
                    "point-delete",
                    "public-paging",
                    "public-facet",
                    "public-hydration",
                    "storage-recovery",
                    "schema-rebuild",
                    "schema-resume",
                ],
                StringComparer.Ordinal);
        Require(
            requiredWorkloads.IsSubsetOf(workloads),
            "Qualification profile does not cover every required workload surface.");

        var lifecycles = cases.Values
            .Select(static item => GetString(item, "lifecycle"))
            .ToHashSet(StringComparer.Ordinal);
        Require(
            RequiredLifecycleConditions.All(lifecycles.Contains),
            "Qualification profile must distinguish hot application-grain, warm owner, and cold owner lifecycles.");

        var phases = cases.Values
            .SelectMany(static item => item.GetProperty("requiredPhases").EnumerateArray().Select(
                static phase => phase.GetString()!))
            .ToHashSet(StringComparer.Ordinal);
        var requiredPhases = implementationPath == "plain"
            ? new HashSet<string>(["steady-state", "activation", "garbage-collection"], StringComparer.Ordinal)
            : new HashSet<string>(
                [
                    "steady-state",
                    "activation",
                    "garbage-collection",
                    "journal-replay",
                    "compaction",
                    "schema-rebuild",
                    "schema-resume",
                ],
                StringComparer.Ordinal);
        Require(
            requiredPhases.IsSubsetOf(phases),
            "Qualification profile does not cover every required lifecycle phase.");

        var thresholdMetrics = thresholds.Values
            .Select(static item => GetString(item, "metric"))
            .ToHashSet(StringComparer.Ordinal);
        var requiredThresholdMetrics = new HashSet<string>(
            [
                "offered-operations",
                "completed-operations",
                "error-rate",
                "timeout-rate",
                "latency-p50",
                "latency-p95",
                "latency-p99",
                "latency-p999",
                "retained-managed-bytes",
                "peak-managed-bytes",
                "memory-headroom-bytes",
                "cold-activation-duration",
                "gc-pause-duration",
            ],
            StringComparer.Ordinal);
        if (implementationPath == "searchable")
        {
            requiredThresholdMetrics.UnionWith(
                ["recovery-duration", "compaction-pause-duration"]);
        }

        if (backend != "memory")
        {
            requiredThresholdMetrics.UnionWith(
                [
                    "provider-read-call-amplification",
                    "provider-write-call-amplification",
                    "provider-delete-call-amplification",
                    "provider-byte-amplification",
                ]);
        }

        Require(
            requiredThresholdMetrics.IsSubsetOf(thresholdMetrics),
            "Qualification profile does not freeze every required load, latency, memory, lifecycle, and provider threshold.");
    }

    private static void ValidateImplementationCapability(
        JsonElement profile,
        JsonElement profileCase,
        string caseId)
    {
        if (GetString(profile, "implementationPath") != "plain")
        {
            return;
        }

        var workload = GetString(profileCase, "workload");
        Require(
            workload is "point-read" or "point-write" or "point-delete",
            $"Plain-storage profile case '{caseId}' must be a point read, write, or delete; secondary-index surfaces are searchable-only.");
    }

    private static void ValidateFanout(
        JsonElement profileCase,
        JsonElement resultCase,
        JsonElement run,
        string caseId)
    {
        var ceiling = profileCase.GetProperty("fanoutCeiling");
        var observed = resultCase.GetProperty("observedFanout");
        var observedOwners = observed.GetProperty("physicalOwners").GetInt32();
        var observedSlots = observed.GetProperty("virtualSlots").GetInt32();
        Require(
            observedOwners <= ceiling.GetProperty("physicalOwners").GetInt32(),
            $"Case '{caseId}' physical-owner fanout exceeds its frozen ceiling.");
        Require(
            observedSlots <= ceiling.GetProperty("virtualSlots").GetInt32(),
            $"Case '{caseId}' virtual-slot fanout exceeds its frozen ceiling.");
        Require(
            observedOwners <= run.GetProperty("physicalOwnerCount").GetInt32(),
            $"Case '{caseId}' physical-owner fanout exceeds the run topology.");
        Require(
            observedSlots <= run.GetProperty("virtualSlotCount").GetInt32(),
            $"Case '{caseId}' virtual-slot fanout exceeds the run topology.");
    }

    private static bool ValidatePhases(
        JsonElement profileCase,
        JsonElement resultCase,
        JsonElement run,
        string caseId)
    {
        var required = profileCase
            .GetProperty("requiredPhases")
            .EnumerateArray()
            .Select(static phase => phase.GetString()!)
            .ToHashSet(StringComparer.Ordinal);
        var actual = IndexByProperty(resultCase.GetProperty("phases"), "phase", $"case '{caseId}' phase");
        Require(
            required.SetEquals(actual.Keys),
            $"Case '{caseId}' phases must exactly cover its reference-profile phases.");

        var allCompleted = true;
        foreach (var (phaseName, phase) in actual)
        {
            allCompleted &= GetString(phase, "status") == "completed";
            var metrics = IndexByProperty(
                phase.GetProperty("metrics"),
                "metric",
                $"case '{caseId}' phase '{phaseName}' metric");
            ValidateMetricUnits(metrics, $"case '{caseId}' phase '{phaseName}'");
            ValidateLoadMetrics(metrics, caseId, phaseName);
            ValidateLatencyMetrics(metrics, caseId, phaseName);
            ValidateMemoryMetrics(metrics, run, caseId, phaseName);
        }

        return allCompleted;
    }

    private static bool ValidateByteAndProviderEvidence(
        JsonElement profileCase,
        JsonElement resultCase,
        string backend,
        string caseId)
    {
        var bytes = resultCase.GetProperty("byteEvidence");
        Require(
            GetString(bytes.GetProperty("logicalStateBytes"), "availability") == "observed",
            $"Case '{caseId}' must report logical state bytes as observed evidence.");
        Require(
            GetString(bytes.GetProperty("canonicalSerializedBytes"), "availability") == "observed",
            $"Case '{caseId}' must report canonical serialized bytes as observed evidence.");

        var policy = GetString(profileCase, "providerTelemetryPolicy");
        var physicalAvailability = GetString(bytes.GetProperty("providerNativePhysicalBytes"), "availability");
        var providerTelemetry = resultCase.GetProperty("providerNativeTelemetry");
        var telemetryAvailability = GetString(providerTelemetry, "availability");
        if (backend == "memory")
        {
            Require(policy == "not-applicable", $"Memory case '{caseId}' provider telemetry policy must be not-applicable.");
            Require(
                physicalAvailability == "not-applicable",
                $"Memory case '{caseId}' provider-native physical bytes must be explicit not-applicable, never zero or missing.");
            Require(
                telemetryAvailability == "not-applicable",
                $"Memory case '{caseId}' provider-native telemetry must be explicit not-applicable.");
            return true;
        }

        Require(policy != "not-applicable", $"Durable-provider case '{caseId}' cannot use a not-applicable telemetry policy.");
        Require(
            physicalAvailability != "not-applicable",
            $"Durable-provider case '{caseId}' physical-byte telemetry must be observed or explicitly missing.");
        Require(
            telemetryAvailability != "not-applicable",
            $"Durable-provider case '{caseId}' provider telemetry must be observed or explicitly missing.");

        if (policy == "required")
        {
            Require(
                physicalAvailability == "observed",
                $"Case '{caseId}' requires observed provider-native physical bytes.");
            Require(
                telemetryAvailability == "observed",
                $"Case '{caseId}' requires observed provider-native telemetry.");
            ValidateProviderObservations(resultCase, providerTelemetry, bytes, caseId);
            return true;
        }

        if (physicalAvailability == "observed" && telemetryAvailability == "observed")
        {
            ValidateProviderObservations(resultCase, providerTelemetry, bytes, caseId);
            return true;
        }

        Require(
            physicalAvailability == "missing" && telemetryAvailability == "missing",
            $"Case '{caseId}' optional provider bytes and counters must both be observed or both be explicitly missing.");
        return false;
    }

    private static void ValidateThresholdDefinitions(
        IReadOnlyDictionary<string, JsonElement> thresholds,
        Dictionary<string, JsonElement> cases)
    {
        foreach (var (thresholdId, threshold) in thresholds)
        {
            var caseId = GetString(threshold, "caseId");
            Require(cases.TryGetValue(caseId, out var profileCase), $"Threshold '{thresholdId}' names unknown case '{caseId}'.");
            var phaseName = GetString(threshold, "phase");
            Require(
                profileCase.GetProperty("requiredPhases").EnumerateArray().Any(
                    phase => phase.GetString() == phaseName),
                $"Threshold '{thresholdId}' names phase '{phaseName}' outside case '{caseId}'.");
            var metricName = GetString(threshold, "metric");
            Require(
                MetricUnits.TryGetValue(metricName, out var expectedUnit) &&
                expectedUnit == GetString(threshold, "unit"),
                $"Threshold '{thresholdId}' uses the wrong unit for metric '{metricName}'.");
        }
    }

    private static async Task ValidateRawEvidenceArtifactsAsync(
        string resultPath,
        JsonElement run,
        CancellationToken cancellationToken)
    {
        var resultDirectory = Path.GetDirectoryName(Path.GetFullPath(resultPath))
            ?? throw new InvalidDataException("Benchmark evidence result path has no parent directory.");
        var artifacts = IndexByProperty(
            run.GetProperty("rawEvidenceArtifacts"),
            "path",
            "raw evidence artifact path");

        foreach (var (relativePath, artifact) in artifacts)
        {
            var artifactPath = ResolveContentAddressedPath(
                resultDirectory,
                relativePath,
                "Raw evidence artifact");
            await using var stream = new FileStream(
                artifactPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 64 * 1024,
                useAsync: true);
            Require(stream.Length > 0, $"Raw evidence artifact '{relativePath}' is empty.");
            var actualSha256 = Convert.ToHexStringLower(
                await SHA256.HashDataAsync(stream, cancellationToken));
            Require(
                actualSha256 == GetString(artifact, "sha256"),
                $"Raw evidence artifact '{relativePath}' SHA-256 does not match its bytes.");
        }
    }

    private static string ResolveContentAddressedPath(
        string rootDirectory,
        string relativePath,
        string label)
    {
        Require(!Path.IsPathRooted(relativePath), $"{label} path '{relativePath}' must be relative.");
        var segments = relativePath.Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries);
        Require(
            segments.Length > 0 && segments.All(static segment => segment is not "." and not ".."),
            $"{label} path '{relativePath}' must not contain traversal segments.");

        var root = Path.GetFullPath(rootDirectory);
        var rootedPrefix = root.EndsWith(Path.DirectorySeparatorChar)
            ? root
            : root + Path.DirectorySeparatorChar;
        var resolvedPath = Path.GetFullPath(relativePath, root);
        Require(
            resolvedPath.StartsWith(rootedPrefix, StringComparison.Ordinal),
            $"{label} path '{relativePath}' escapes the result directory.");
        var current = root;
        foreach (var segment in segments)
        {
            current = Path.Combine(current, segment);
            if (File.Exists(current) || Directory.Exists(current))
            {
                Require(
                    (File.GetAttributes(current) & FileAttributes.ReparsePoint) == 0,
                    $"{label} path '{relativePath}' must not traverse symbolic links.");
            }
        }

        Require(File.Exists(resolvedPath), $"{label} '{relativePath}' does not exist.");
        Require(
            (File.GetAttributes(resolvedPath) & FileAttributes.ReparsePoint) == 0,
            $"{label} '{relativePath}' must not be a symbolic link.");
        return resolvedPath;
    }

    private static void ValidateQualificationArtifactCoverage(
        JsonElement run,
        IReadOnlyDictionary<string, JsonElement> cases,
        IReadOnlyDictionary<string, JsonElement> thresholds,
        string backend)
    {
        var kinds = run.GetProperty("rawEvidenceArtifacts")
            .EnumerateArray()
            .Select(static artifact => GetString(artifact, "kind"))
            .ToHashSet(StringComparer.Ordinal);
        if (thresholds.Values.Any(static threshold =>
                GetString(threshold, "metric").StartsWith("latency-", StringComparison.Ordinal)))
        {
            Require(kinds.Contains("operation-histogram"), "Qualification latency evidence requires a raw operation histogram.");
        }

        if (cases.Values.Any(static item => item.GetProperty("requiredPhases").EnumerateArray().Any(
                phase => phase.GetString() == "garbage-collection")))
        {
            Require(kinds.Contains("gc-trace"), "Qualification garbage-collection evidence requires a raw GC trace.");
        }

        if (backend != "memory")
        {
            Require(kinds.Contains("provider-telemetry"), "Durable-provider qualification requires raw provider telemetry.");
        }

        if (cases.Values.Any(static item => item.GetProperty("requiredPhases").EnumerateArray().Any(
                phase => phase.GetString() is "schema-rebuild" or "schema-resume")))
        {
            Require(kinds.Contains("schema-checkpoint"), "Schema qualification evidence requires a raw schema checkpoint.");
        }
    }

    private static void ValidateMetricUnits(
        IReadOnlyDictionary<string, JsonElement> metrics,
        string label)
    {
        foreach (var (metricName, metric) in metrics)
        {
            Require(
                MetricUnits.TryGetValue(metricName, out var expectedUnit),
                $"{label} contains unsupported metric '{metricName}'.");
            Require(
                GetString(metric, "unit") == expectedUnit,
                $"{label} metric '{metricName}' must use unit '{expectedUnit}'.");
            var value = metric.GetProperty("value").GetDouble();
            Require(double.IsFinite(value) && value >= 0, $"{label} metric '{metricName}' must be finite and non-negative.");
            if (expectedUnit is "count" or "bytes")
            {
                Require(value == Math.Truncate(value), $"{label} metric '{metricName}' must be an integer {expectedUnit} value.");
            }
        }
    }

    private static void ValidateLoadMetrics(
        IReadOnlyDictionary<string, JsonElement> metrics,
        string caseId,
        string phaseName)
    {
        if (phaseName != "steady-state")
        {
            return;
        }

        string[] required =
        [
            "offered-operations",
            "completed-operations",
            "failed-operations",
            "timed-out-operations",
            "error-rate",
            "timeout-rate",
        ];
        Require(
            required.All(metrics.ContainsKey),
            $"Steady-state case '{caseId}' must report offered/completed/failed/timed-out counts and error/timeout rates.");

        var offered = MetricValue(metrics, "offered-operations");
        var completed = MetricValue(metrics, "completed-operations");
        var failed = MetricValue(metrics, "failed-operations");
        var timedOut = MetricValue(metrics, "timed-out-operations");
        Require(completed <= offered, $"Steady-state case '{caseId}' completed operations exceed offered operations.");
        Require(failed <= completed, $"Steady-state case '{caseId}' failed operations exceed completed operations.");
        Require(timedOut <= failed, $"Steady-state case '{caseId}' timed-out operations exceed failed operations.");
        var expectedErrorRate = offered == 0 ? 0 : failed / offered;
        var expectedTimeoutRate = offered == 0 ? 0 : timedOut / offered;
        Require(
            NearlyEqual(MetricValue(metrics, "error-rate"), expectedErrorRate),
            $"Steady-state case '{caseId}' error rate does not equal failed/offered.");
        Require(
            NearlyEqual(MetricValue(metrics, "timeout-rate"), expectedTimeoutRate),
            $"Steady-state case '{caseId}' timeout rate does not equal timed-out/offered.");
    }

    private static void ValidateLatencyMetrics(
        IReadOnlyDictionary<string, JsonElement> metrics,
        string caseId,
        string phaseName)
    {
        string[] latencyNames = ["latency-p50", "latency-p95", "latency-p99", "latency-p999"];
        var present = latencyNames.Count(metrics.ContainsKey);
        if (phaseName == "steady-state")
        {
            Require(present == latencyNames.Length, $"Steady-state case '{caseId}' must report p50/p95/p99/p99.9 latency.");
        }
        else if (present > 0)
        {
            Require(present == latencyNames.Length, $"Case '{caseId}' phase '{phaseName}' must report every frozen latency percentile together.");
        }

        if (present == 0)
        {
            return;
        }

        var p50 = MetricValue(metrics, "latency-p50");
        var p95 = MetricValue(metrics, "latency-p95");
        var p99 = MetricValue(metrics, "latency-p99");
        var p999 = MetricValue(metrics, "latency-p999");
        Require(
            p50 <= p95 && p95 <= p99 && p99 <= p999,
            $"Case '{caseId}' phase '{phaseName}' latency percentiles are not monotonic.");
    }

    private static void ValidateMemoryMetrics(
        IReadOnlyDictionary<string, JsonElement> metrics,
        JsonElement run,
        string caseId,
        string phaseName)
    {
        string[] memoryNames = ["retained-managed-bytes", "peak-managed-bytes", "memory-headroom-bytes"];
        var present = memoryNames.Count(metrics.ContainsKey);
        if (present == 0)
        {
            return;
        }

        Require(
            present == memoryNames.Length,
            $"Case '{caseId}' phase '{phaseName}' must report retained, peak, and headroom memory together.");
        var retained = MetricValue(metrics, "retained-managed-bytes");
        var peak = MetricValue(metrics, "peak-managed-bytes");
        var headroom = MetricValue(metrics, "memory-headroom-bytes");
        var physical = run.GetProperty("provenance").GetProperty("physicalMemoryBytes").GetInt64();
        Require(retained <= peak, $"Case '{caseId}' retained managed bytes exceed peak managed bytes.");
        Require(peak <= physical, $"Case '{caseId}' peak managed bytes exceed declared physical memory.");
        Require(
            headroom == physical - peak,
            $"Case '{caseId}' memory headroom must equal declared physical memory minus peak managed bytes.");
    }

    private static void ValidateProviderObservations(
        JsonElement resultCase,
        JsonElement providerTelemetry,
        JsonElement byteEvidence,
        string caseId)
    {
        var observations = IndexByProperty(
            providerTelemetry.GetProperty("observations"),
            "metric",
            $"case '{caseId}' provider-native observation");
        Require(
            observations.Keys.Order(StringComparer.Ordinal).SequenceEqual(
                ProviderMetricUnits.Keys.Order(StringComparer.Ordinal),
                StringComparer.Ordinal),
            $"Case '{caseId}' observed provider telemetry must contain exactly read/write/delete calls and bytes.");
        foreach (var (metricName, observation) in observations)
        {
            Require(
                GetString(observation, "unit") == ProviderMetricUnits[metricName],
                $"Case '{caseId}' provider metric '{metricName}' has the wrong unit.");
            var value = observation.GetProperty("value").GetDouble();
            Require(
                double.IsFinite(value) && value >= 0 && value == Math.Truncate(value),
                $"Case '{caseId}' provider metric '{metricName}' must be a non-negative integer observation.");
        }

        var logicalCalls = resultCase.GetProperty("logicalOrleansCalls");
        foreach (var operation in ProviderOperations)
        {
            if (logicalCalls.GetProperty(operation).GetInt64() > 0)
            {
                Require(
                    MetricValue(observations, $"{operation}-calls") > 0,
                    $"Case '{caseId}' required provider telemetry reports zero {operation} calls for observed logical Orleans {operation} calls.");
            }
        }

        var observedPhysicalBytes = byteEvidence
            .GetProperty("providerNativePhysicalBytes")
            .GetProperty("valueBytes")
            .GetInt64();
        var providerByteTotal = checked(
            (long)MetricValue(observations, "read-bytes") +
            (long)MetricValue(observations, "write-bytes") +
            (long)MetricValue(observations, "delete-bytes"));
        Require(
            observedPhysicalBytes == providerByteTotal,
            $"Case '{caseId}' provider-native physical bytes must equal the native read/write/delete byte total.");
    }

    private static void ValidateDerivedCaseMetrics(JsonElement resultCase, string caseId)
    {
        var phases = resultCase.GetProperty("phases").EnumerateArray().ToArray();
        var logicalCalls = resultCase.GetProperty("logicalOrleansCalls");
        var logicalByMetric = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            ["logical-orleans-read-calls"] = logicalCalls.GetProperty("read").GetInt64(),
            ["logical-orleans-write-calls"] = logicalCalls.GetProperty("write").GetInt64(),
            ["logical-orleans-delete-calls"] = logicalCalls.GetProperty("delete").GetInt64(),
        };
        foreach (var phase in phases)
        {
            var metrics = IndexByProperty(phase.GetProperty("metrics"), "metric", "phase metric");
            foreach (var (metricName, expectedValue) in logicalByMetric)
            {
                if (metrics.TryGetValue(metricName, out var metric))
                {
                    Require(
                        metric.GetProperty("value").GetDouble() == expectedValue,
                        $"Case '{caseId}' metric '{metricName}' differs from its logical Orleans call evidence.");
                }
            }
        }

        var providerTelemetry = resultCase.GetProperty("providerNativeTelemetry");
        if (GetString(providerTelemetry, "availability") != "observed")
        {
            Require(
                phases.All(static phase => phase.GetProperty("metrics").EnumerateArray().All(static metric =>
                    !GetString(metric, "metric").StartsWith("provider-", StringComparison.Ordinal))),
                $"Case '{caseId}' cannot report provider amplification without observed provider-native telemetry.");
            return;
        }

        var provider = IndexByProperty(providerTelemetry.GetProperty("observations"), "metric", "provider metric");
        var expectedAmplification = new Dictionary<string, double>(StringComparer.Ordinal);
        AddCallAmplification("read", "provider-read-call-amplification");
        AddCallAmplification("write", "provider-write-call-amplification");
        AddCallAmplification("delete", "provider-delete-call-amplification");
        var canonicalBytes = resultCase.GetProperty("byteEvidence")
            .GetProperty("canonicalSerializedBytes")
            .GetProperty("valueBytes")
            .GetDouble();
        var providerBytes = MetricValue(provider, "read-bytes") +
            MetricValue(provider, "write-bytes") +
            MetricValue(provider, "delete-bytes");
        expectedAmplification["provider-byte-amplification"] = providerBytes / canonicalBytes;

        foreach (var phase in phases)
        {
            var metrics = IndexByProperty(phase.GetProperty("metrics"), "metric", "phase metric");
            foreach (var (metricName, expectedValue) in expectedAmplification)
            {
                if (metrics.TryGetValue(metricName, out var metric))
                {
                    Require(
                        NearlyEqual(metric.GetProperty("value").GetDouble(), expectedValue),
                        $"Case '{caseId}' metric '{metricName}' was not derived from provider-native and logical evidence.");
                }
            }
        }

        void AddCallAmplification(string operation, string metricName)
        {
            var logical = logicalCalls.GetProperty(operation).GetDouble();
            if (logical > 0)
            {
                expectedAmplification[metricName] = MetricValue(provider, $"{operation}-calls") / logical;
            }
            else
            {
                Require(
                    phases.All(phase => phase.GetProperty("metrics").EnumerateArray().All(metric =>
                        GetString(metric, "metric") != metricName)),
                    $"Case '{caseId}' cannot report {metricName} when the logical {operation} call count is zero.");
            }
        }
    }

    private static double MetricValue(IReadOnlyDictionary<string, JsonElement> metrics, string name) =>
        metrics[name].GetProperty("value").GetDouble();

    private static bool NearlyEqual(double left, double right)
    {
        var scale = Math.Max(1d, Math.Max(Math.Abs(left), Math.Abs(right)));
        return Math.Abs(left - right) <= 1e-12 * scale;
    }

    private static JsonElement FindMetric(
        JsonElement resultCase,
        string phaseName,
        string metricName,
        string thresholdId)
    {
        var phases = IndexByProperty(resultCase.GetProperty("phases"), "phase", "result phase");
        Require(phases.TryGetValue(phaseName, out var phase), $"Threshold '{thresholdId}' phase evidence is absent.");
        var metrics = IndexByProperty(phase.GetProperty("metrics"), "metric", $"phase '{phaseName}' metric");
        Require(metrics.TryGetValue(metricName, out var metric), $"Threshold '{thresholdId}' metric evidence is absent.");
        return metric;
    }

    private static Dictionary<string, JsonElement> IndexById(JsonElement array, string label) =>
        IndexByProperty(array, "id", label);

    private static Dictionary<string, JsonElement> IndexByProperty(
        JsonElement array,
        string propertyName,
        string label)
    {
        var values = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var item in array.EnumerateArray())
        {
            var key = GetString(item, propertyName);
            if (!values.TryAdd(key, item))
            {
                throw new InvalidDataException($"Duplicate {label} '{key}'.");
            }
        }

        return values;
    }

    private static void RequireSameKeys(
        IReadOnlyDictionary<string, JsonElement> expected,
        IReadOnlyDictionary<string, JsonElement> actual,
        string message)
    {
        Require(
            expected.Keys.Order(StringComparer.Ordinal).SequenceEqual(
                actual.Keys.Order(StringComparer.Ordinal),
                StringComparer.Ordinal),
            message);
    }

    private static void RequireCopiedValue(
        JsonElement profileCase,
        JsonElement resultCase,
        string propertyName,
        string caseId)
    {
        Require(
            GetString(profileCase, propertyName) == GetString(resultCase, propertyName),
            $"Case '{caseId}' {propertyName} differs from the reference profile.");
    }

    private static string GetString(JsonElement element, string propertyName) =>
        element.GetProperty(propertyName).GetString()
        ?? throw new InvalidDataException($"Property '{propertyName}' cannot be null.");

    private static void ValidateAgainstSchema(
        byte[] bytes,
        JsonSchema schema,
        string schemaFileName,
        string label)
    {
        using var document = JsonDocument.Parse(bytes);
        var evaluation = schema.Evaluate(
            document.RootElement,
            new EvaluationOptions { OutputFormat = OutputFormat.Hierarchical });
        if (evaluation.IsValid)
        {
            return;
        }

        var errors = new List<string>();
        CollectSchemaErrors(evaluation, errors);
        var detail = errors.Count == 0
            ? $"the document does not satisfy {schemaFileName}"
            : string.Join("; ", errors.Take(8));
        throw new InvalidDataException($"{label} schema validation failed: {detail}");
    }

    private static JsonSchema LoadSchema(string fileName)
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "specs", "schema", fileName);
        if (!File.Exists(schemaPath))
        {
            throw new FileNotFoundException($"Benchmark schema '{fileName}' was not deployed with the load driver.", schemaPath);
        }

        return JsonSchema.FromText(
            File.ReadAllText(schemaPath),
            new BuildOptions { SchemaRegistry = new SchemaRegistry() },
            baseUri: new Uri(schemaPath));
    }

    private static void CollectSchemaErrors(EvaluationResults result, ICollection<string> errors)
    {
        if (result.Errors is not null)
        {
            foreach (var (keyword, message) in result.Errors)
            {
                errors.Add($"{result.InstanceLocation} [{keyword}]: {message}");
            }
        }

        foreach (var detail in result.Details ?? [])
        {
            CollectSchemaErrors(detail, errors);
        }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidDataException(message);
        }
    }
}
