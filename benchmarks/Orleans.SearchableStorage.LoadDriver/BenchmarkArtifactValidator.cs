using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using HdrHistogram;
using Json.Schema;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkArtifactValidator
{
    private const string ResultSchemaFileName = "result.v1.schema.json";
    private static readonly string[] ExpectedSourceKinds = ["dataset", "scenario", "workload"];
    private static readonly Lazy<JsonSchema> ResultSchema = new(
        LoadResultSchema,
        LazyThreadSafetyMode.ExecutionAndPublication);

    public static async Task<int> RunCommandAsync(string[] args)
    {
        if (args.Length != 2 || !string.Equals(args[0], "--result", StringComparison.Ordinal))
        {
            Console.Error.WriteLine("usage: validate-artifact --result <result.json|failure.json>");
            return 64;
        }

        try
        {
            var resultPath = Path.GetFullPath(args[1]);
            await ValidateAsync(resultPath, CancellationToken.None);
            Console.WriteLine($"Benchmark artifact is structurally valid: {resultPath}");
            return 0;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Benchmark artifact validation failed: {exception.Message}");
            return 1;
        }
    }

    internal static async Task ValidateAsync(string resultPath, CancellationToken cancellationToken)
    {
        if (!File.Exists(resultPath))
        {
            throw new FileNotFoundException("Benchmark result does not exist.", resultPath);
        }

        await using var resultStream = new FileStream(
            resultPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 64 * 1024,
            useAsync: true);
        if (resultStream.Length <= 0)
        {
            throw new InvalidDataException("Benchmark result is empty.");
        }

        var isVersionTwo = await IsVersionTwoAsync(resultStream, cancellationToken);
        ValidateVersionTwoLength(isVersionTwo, resultStream.Length);
        if (resultStream.Length > int.MaxValue)
        {
            throw new InvalidDataException(
                "Benchmark result is too large to validate in this process.");
        }

        resultStream.Position = 0;
        var resultBytes = new byte[checked((int)resultStream.Length)];
        await resultStream.ReadExactlyAsync(resultBytes, cancellationToken);
        using (var versionDocument = JsonDocument.Parse(resultBytes))
        {
            if (versionDocument.RootElement.TryGetProperty("schemaVersion", out var schemaVersion) &&
                string.Equals(
                    schemaVersion.GetString(),
                    BenchmarkEvidenceV2Validator.ResultSchemaVersion,
                    StringComparison.Ordinal))
            {
                await BenchmarkEvidenceV2Validator.ValidateAsync(resultPath, resultBytes, cancellationToken);
                return;
            }
        }

        ValidateResultSchema(resultBytes);
        using var document = JsonDocument.Parse(resultBytes);
        var status = document.RootElement.GetProperty("status").GetString();
        ArtifactView view;
        if (string.Equals(status, "failed", StringComparison.Ordinal))
        {
            var result = JsonSerializer.Deserialize(
                resultBytes,
                BenchmarkJsonContext.Default.BenchmarkFailureResult)
                ?? throw new InvalidDataException("Failure result is empty.");
            view = new ArtifactView(
                result.Status,
                result.RunId,
                result.InstanceId,
                result.StartedAtUtc,
                result.EndedAtUtc,
                result.SourceSpecs,
                result.EffectiveConfiguration,
                result.EffectiveConfigurationSha256,
                result.EffectiveConfigurationContentBase64,
                result.Population,
                result.InitialAudit,
                result.Restoration,
                result.Warmup,
                result.Measurement,
                result.FinalAudit,
                result.FailedPhase,
                result.HistogramArtifacts,
                IsFailure: true);
        }
        else
        {
            var result = JsonSerializer.Deserialize(
                resultBytes,
                BenchmarkJsonContext.Default.BenchmarkRunResult)
                ?? throw new InvalidDataException("Success result is empty.");
            view = new ArtifactView(
                result.Status,
                result.RunId,
                result.InstanceId,
                result.StartedAtUtc,
                result.EndedAtUtc,
                result.SourceSpecs,
                result.EffectiveConfiguration,
                result.EffectiveConfigurationSha256,
                result.EffectiveConfigurationContentBase64,
                result.Population,
                result.InitialAudit,
                result.Restoration,
                result.Warmup,
                result.Measurement,
                result.FinalAudit,
                FailedPhase: null,
                result.HistogramArtifacts,
                IsFailure: false);
        }

        var source = ValidateCommonEvidence(view);
        if (view.IsFailure)
        {
            ValidateFailureSemantics(view);
        }
        else
        {
            ValidateSuccessSemantics(view);
        }

        await ValidateHistogramsAsync(
            resultPath,
            view,
            source.Workload.Mode,
            cancellationToken);
    }

    internal static void ValidateVersionTwoLength(bool isVersionTwo, long resultLength)
    {
        if (isVersionTwo && resultLength > BenchmarkEvidenceV2Validator.MaximumResultBytes)
        {
            throw new InvalidDataException(
                $"Benchmark evidence result exceeds the {BenchmarkEvidenceV2Validator.MaximumResultBytes}-byte v2 input limit.");
        }
    }

    private static async Task<bool> IsVersionTwoAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        const int bufferSize = 64 * 1024;
        var buffer = new byte[bufferSize];
        var rootState = RootProbeState.Start;
        var stringRole = ProbeStringRole.None;
        var nestedDepth = 0;
        var inString = false;
        var escapePending = false;
        var unicodeDigitsRemaining = 0;
        var unicodeValue = 0;
        var skippingPrimitive = false;
        var currentPropertyIsSchemaVersion = false;
        var isVersionTwo = false;
        var matcher = default(AsciiStringMatcher);

        int bytesRead;
        while ((bytesRead = await stream.ReadAsync(buffer, cancellationToken)) > 0)
        {
            for (var index = 0; index < bytesRead; index++)
            {
                var value = buffer[index];
                if (inString)
                {
                    if (unicodeDigitsRemaining > 0)
                    {
                        unicodeValue = (unicodeValue << 4) | HexValue(value);
                        unicodeDigitsRemaining--;
                        if (unicodeDigitsRemaining == 0)
                        {
                            matcher.Feed(unicodeValue);
                        }

                        continue;
                    }

                    if (escapePending)
                    {
                        escapePending = false;
                        if (value == (byte)'u')
                        {
                            unicodeDigitsRemaining = 4;
                            unicodeValue = 0;
                        }
                        else
                        {
                            matcher.Feed(value switch
                            {
                                (byte)'"' => '"',
                                (byte)'\\' => '\\',
                                (byte)'/' => '/',
                                (byte)'b' => '\b',
                                (byte)'f' => '\f',
                                (byte)'n' => '\n',
                                (byte)'r' => '\r',
                                (byte)'t' => '\t',
                                _ => -1,
                            });
                        }

                        continue;
                    }

                    if (value == (byte)'\\')
                    {
                        escapePending = true;
                        continue;
                    }

                    if (value != (byte)'"')
                    {
                        matcher.Feed(value);
                        continue;
                    }

                    inString = false;
                    switch (stringRole)
                    {
                        case ProbeStringRole.RootProperty:
                            currentPropertyIsSchemaVersion = matcher.IsExact;
                            rootState = RootProbeState.Colon;
                            break;
                        case ProbeStringRole.RootValue:
                            if (currentPropertyIsSchemaVersion)
                            {
                                isVersionTwo = matcher.IsExact;
                            }

                            currentPropertyIsSchemaVersion = false;
                            rootState = RootProbeState.CommaOrEnd;
                            break;
                    }

                    stringRole = ProbeStringRole.None;
                    continue;
                }

                if (nestedDepth > 0)
                {
                    if (value == (byte)'"')
                    {
                        StartString(ProbeStringRole.Nested, target: null);
                    }
                    else if (value is (byte)'{' or (byte)'[')
                    {
                        nestedDepth++;
                    }
                    else if (value is (byte)'}' or (byte)']')
                    {
                        nestedDepth--;
                        if (nestedDepth == 0)
                        {
                            currentPropertyIsSchemaVersion = false;
                            rootState = RootProbeState.CommaOrEnd;
                        }
                    }

                    continue;
                }

                if (skippingPrimitive)
                {
                    if (IsJsonWhitespace(value))
                    {
                        skippingPrimitive = false;
                        currentPropertyIsSchemaVersion = false;
                        rootState = RootProbeState.CommaOrEnd;
                    }
                    else if (value == (byte)',')
                    {
                        skippingPrimitive = false;
                        currentPropertyIsSchemaVersion = false;
                        rootState = RootProbeState.PropertyOrEnd;
                    }
                    else if (value == (byte)'}')
                    {
                        skippingPrimitive = false;
                        currentPropertyIsSchemaVersion = false;
                        rootState = RootProbeState.Done;
                    }

                    continue;
                }

                switch (rootState)
                {
                    case RootProbeState.Start:
                        if (IsJsonWhitespace(value) || value is 0xef or 0xbb or 0xbf)
                        {
                            continue;
                        }

                        if (value == (byte)'{')
                        {
                            rootState = RootProbeState.PropertyOrEnd;
                        }
                        else
                        {
                            return false;
                        }

                        break;
                    case RootProbeState.PropertyOrEnd:
                        if (IsJsonWhitespace(value))
                        {
                            continue;
                        }

                        if (value == (byte)'"')
                        {
                            StartString(ProbeStringRole.RootProperty, "schemaVersion");
                        }
                        else if (value == (byte)'}')
                        {
                            rootState = RootProbeState.Done;
                        }

                        break;
                    case RootProbeState.Colon:
                        if (IsJsonWhitespace(value))
                        {
                            continue;
                        }

                        if (value == (byte)':')
                        {
                            rootState = RootProbeState.Value;
                        }

                        break;
                    case RootProbeState.Value:
                        if (IsJsonWhitespace(value))
                        {
                            continue;
                        }

                        if (value == (byte)'"')
                        {
                            StartString(
                                ProbeStringRole.RootValue,
                                currentPropertyIsSchemaVersion
                                    ? BenchmarkEvidenceV2Validator.ResultSchemaVersion
                                    : null);
                        }
                        else if (value is (byte)'{' or (byte)'[')
                        {
                            nestedDepth = 1;
                        }
                        else
                        {
                            skippingPrimitive = true;
                        }

                        break;
                    case RootProbeState.CommaOrEnd:
                        if (IsJsonWhitespace(value))
                        {
                            continue;
                        }

                        if (value == (byte)',')
                        {
                            rootState = RootProbeState.PropertyOrEnd;
                        }
                        else if (value == (byte)'}')
                        {
                            rootState = RootProbeState.Done;
                        }

                        break;
                    case RootProbeState.Done:
                        break;
                }
            }
        }

        return isVersionTwo;

        void StartString(ProbeStringRole role, string? target)
        {
            inString = true;
            stringRole = role;
            escapePending = false;
            unicodeDigitsRemaining = 0;
            unicodeValue = 0;
            matcher = new AsciiStringMatcher(target);
        }

        static int HexValue(byte value) => value switch
        {
            >= (byte)'0' and <= (byte)'9' => value - '0',
            >= (byte)'a' and <= (byte)'f' => value - 'a' + 10,
            >= (byte)'A' and <= (byte)'F' => value - 'A' + 10,
            _ => -1,
        };
    }

    internal static Task<bool> IsVersionTwoForTestsAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return IsVersionTwoAsync(stream, cancellationToken);
    }

    private static bool IsJsonWhitespace(byte value) =>
        value is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n';

    private enum RootProbeState
    {
        Start,
        PropertyOrEnd,
        Colon,
        Value,
        CommaOrEnd,
        Done,
    }

    private enum ProbeStringRole
    {
        None,
        Nested,
        RootProperty,
        RootValue,
    }

    private struct AsciiStringMatcher(string? target)
    {
        private readonly string? target = target;
        private int index;
        private bool possible = target is not null;

        public readonly bool IsExact => possible && index == target!.Length;

        public void Feed(int value)
        {
            if (!possible)
            {
                return;
            }

            if (value < 0 || index >= target!.Length || value != target[index])
            {
                possible = false;
                return;
            }

            index++;
        }
    }

    private static void ValidateResultSchema(byte[] resultBytes)
    {
        using var document = JsonDocument.Parse(resultBytes);
        var evaluation = ResultSchema.Value.Evaluate(
            document.RootElement,
            new EvaluationOptions { OutputFormat = OutputFormat.Hierarchical });
        if (evaluation.IsValid)
        {
            return;
        }

        var errors = new List<string>();
        CollectSchemaErrors(evaluation, errors);
        var detail = errors.Count == 0
            ? "the document does not satisfy result.v1.schema.json"
            : string.Join("; ", errors.Take(8));
        throw new InvalidDataException($"Benchmark result schema validation failed: {detail}");
    }

    private static JsonSchema LoadResultSchema()
    {
        var schemaPath = Path.Combine(
            AppContext.BaseDirectory,
            "specs",
            "schema",
            ResultSchemaFileName);
        if (!File.Exists(schemaPath))
        {
            throw new FileNotFoundException("The benchmark result schema was not deployed with the load driver.", schemaPath);
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

    private static SourceEvidence ValidateCommonEvidence(ArtifactView view)
    {
        Require(view.StartedAtUtc <= view.EndedAtUtc, "endedAtUtc must not precede startedAtUtc.");
        Require(
            string.Equals(view.RunId, view.EffectiveConfiguration.RunId, StringComparison.Ordinal),
            "runId differs from the effective configuration.");
        Require(
            string.Equals(view.InstanceId, view.EffectiveConfiguration.InstanceId, StringComparison.Ordinal),
            "instanceId differs from the effective configuration.");

        var effectiveBytes = DecodeBase64Json(
            view.EffectiveConfigurationContentBase64,
            view.EffectiveConfigurationSha256,
            "effective configuration");
        var decodedEffective = JsonNode.Parse(effectiveBytes)
            ?? throw new InvalidDataException("Effective configuration decoded to JSON null.");
        var topLevelEffective = JsonSerializer.SerializeToNode(
            view.EffectiveConfiguration,
            BenchmarkJsonContext.Default.EffectiveBenchmarkConfiguration);
        Require(
            JsonNode.DeepEquals(decodedEffective, topLevelEffective),
            "effectiveConfiguration differs from its content-addressed document.");

        Require(view.SourceSpecs.Count == 3, "Exactly three source specs are required.");
        var artifacts = new Dictionary<string, SpecArtifactProvenance>(StringComparer.Ordinal);
        foreach (var artifact in view.SourceSpecs)
        {
            if (!artifacts.TryAdd(artifact.Kind, artifact))
            {
                throw new InvalidDataException($"Source spec kind '{artifact.Kind}' is duplicated.");
            }
        }

        Require(
            artifacts.Keys.Order(StringComparer.Ordinal).SequenceEqual(
                ExpectedSourceKinds,
                StringComparer.Ordinal),
            "Source specs must contain exactly scenario, dataset, and workload.");

        var scenario = DecodeSource(
            artifacts["scenario"],
            BenchmarkJsonContext.Default.BenchmarkScenarioSpec);
        var dataset = DecodeSource(
            artifacts["dataset"],
            BenchmarkJsonContext.Default.DatasetSpec);
        var workload = DecodeSource(
            artifacts["workload"],
            BenchmarkJsonContext.Default.WorkloadSpec);

        Require(
            string.Equals(scenario.Dataset.Sha256, artifacts["dataset"].Sha256, StringComparison.Ordinal),
            "Scenario dataset digest does not match the embedded dataset artifact.");
        Require(
            string.Equals(scenario.Workload.Sha256, artifacts["workload"].Sha256, StringComparison.Ordinal),
            "Scenario workload digest does not match the embedded workload artifact.");

        scenario.Validate();
        new BenchmarkSpec(scenario, dataset, workload).Validate();
        ValidateEffectiveConfiguration(view.EffectiveConfiguration, scenario, dataset, workload);
        return new SourceEvidence(scenario, dataset, workload);
    }

    private static byte[] DecodeBase64Json(string encoded, string expectedSha256, string label)
    {
        byte[] bytes;
        try
        {
            bytes = Convert.FromBase64String(encoded);
        }
        catch (FormatException exception)
        {
            throw new InvalidDataException($"{label} is not valid Base64.", exception);
        }

        Require(bytes.Length > 0, $"{label} is empty.");
        using var _ = JsonDocument.Parse(bytes);
        var actualSha256 = Convert.ToHexStringLower(SHA256.HashData(bytes));
        Require(
            string.Equals(actualSha256, expectedSha256, StringComparison.Ordinal),
            $"{label} SHA-256 does not match its bytes.");
        return bytes;
    }

    private static T DecodeSource<T>(SpecArtifactProvenance artifact, JsonTypeInfo<T> typeInfo)
        where T : class
    {
        var bytes = DecodeBase64Json(artifact.ContentBase64, artifact.Sha256, $"source spec '{artifact.Kind}'");
        return JsonSerializer.Deserialize(bytes, typeInfo)
            ?? throw new InvalidDataException($"Source spec '{artifact.Kind}' decoded to JSON null.");
    }

    private static void ValidateEffectiveConfiguration(
        EffectiveBenchmarkConfiguration effective,
        BenchmarkScenarioSpec scenario,
        DatasetSpec dataset,
        WorkloadSpec workload)
    {
        Require(effective.SchemaVersion == "oss-benchmark-effective/v1", "Unexpected effective schema version.");
        Require(effective.ClientOrdinal is not null && effective.ClientCount is not null, "Result client coordinates are required.");
        Require(
            effective.ClientOrdinal >= 0 && effective.ClientOrdinal < effective.ClientCount,
            "Effective client ordinal must be within the declared client count.");

        var scenarioBytes = JsonSerializer.SerializeToUtf8Bytes(
            scenario,
            BenchmarkJsonContext.Default.BenchmarkScenarioSpec);
        var reconstructedScenario = JsonSerializer.Deserialize(
            scenarioBytes,
            BenchmarkJsonContext.Default.BenchmarkScenarioSpec)
            ?? throw new InvalidDataException("Source scenario could not be reconstructed.");
        var reconstructed = new BenchmarkSpec(reconstructedScenario, dataset, workload);
        effective.AppliedOverrides.ApplyTo(reconstructed);
        var identity = new DriverOptions
        {
            Command = "run",
            SpecPath = "content-addressed-scenario.json",
            RunId = effective.RunId,
        };
        var reconstructedRunId = identity.ApplyRunIdentity(reconstructed);
        Require(reconstructedRunId == effective.RunId, "Effective runId is not canonical.");
        var expected = BenchmarkResultWriter.CreateEffectiveConfiguration(
            reconstructed,
            effective.RunId,
            effective.InstanceId,
            effective.ClientOrdinal,
            effective.ClientCount,
            effective.AppliedOverrides);
        var actualNode = JsonSerializer.SerializeToNode(
            effective,
            BenchmarkJsonContext.Default.EffectiveBenchmarkConfiguration);
        var expectedNode = JsonSerializer.SerializeToNode(
            expected,
            BenchmarkJsonContext.Default.EffectiveBenchmarkConfiguration);
        Require(
            JsonNode.DeepEquals(actualNode, expectedNode),
            "Effective configuration is not the source scenario plus its declared overrides and run identity.");
    }

    private static void ValidateSuccessSemantics(ArtifactView view)
    {
        var effective = view.EffectiveConfiguration;
        var expectedPopulation = GetClientShardCount(
            effective.Dataset.RecordCount,
            effective.ClientOrdinal!.Value,
            effective.ClientCount!.Value);
        ValidateRequiredPopulation(
            view.Population,
            effective.Population.Enabled,
            "population",
            expectedPopulation);
        ValidateRequiredAudit(view.InitialAudit, effective.Audit.Enabled, effective);
        ValidateRequiredWarmup(view.Warmup, effective.Workload.WarmupSeconds);
        var restorationRequired = effective.Workload.WarmupSeconds > 0 &&
            effective.Population.Enabled &&
            effective.Population.RestoreAfterWarmup;
        ValidateRequiredPopulation(
            view.Restoration,
            restorationRequired,
            "post-warmup restoration",
            expectedPopulation);
        Require(view.Measurement is not null, "Successful result must contain measurement evidence.");
        ValidateScheduledDuration(
            view.Measurement!,
            effective.Workload.DurationSeconds,
            "measurement");
        ValidateRequiredAudit(view.FinalAudit, effective.Audit.Enabled, effective);
    }

    private static void ValidateFailureSemantics(ArtifactView view)
    {
        var effective = view.EffectiveConfiguration;
        var expectedPopulation = GetClientShardCount(
            effective.Dataset.RecordCount,
            effective.ClientOrdinal!.Value,
            effective.ClientCount!.Value);
        ValidateOptionalPopulation(view.Population, effective.Population.Enabled, "population", expectedPopulation);
        var restorationEnabled = effective.Workload.WarmupSeconds > 0 &&
            effective.Population.Enabled &&
            effective.Population.RestoreAfterWarmup;
        ValidateOptionalPopulation(
            view.Restoration,
            restorationEnabled,
            "post-warmup restoration",
            expectedPopulation);
        ValidateOptionalAudit(view.InitialAudit, effective.Audit.Enabled, effective);
        ValidateOptionalAudit(view.FinalAudit, effective.Audit.Enabled, effective);
        if (view.Warmup is not null)
        {
            Require(effective.Workload.WarmupSeconds > 0, "Failure result contains warmup evidence for a disabled warmup.");
            ValidateScheduledDuration(view.Warmup, effective.Workload.WarmupSeconds, "warmup");
        }

        if (view.InitialAudit is not null)
        {
            ValidateRequiredPopulation(
                view.Population,
                effective.Population.Enabled,
                "population",
                expectedPopulation);
        }

        if (view.Warmup is not null)
        {
            ValidateRequiredPopulation(
                view.Population,
                effective.Population.Enabled,
                "population",
                expectedPopulation);
            ValidateRequiredAudit(view.InitialAudit, effective.Audit.Enabled, effective);
        }

        if (view.Restoration is not null)
        {
            ValidateRequiredPopulation(
                view.Population,
                effective.Population.Enabled,
                "population",
                expectedPopulation);
            ValidateRequiredAudit(view.InitialAudit, effective.Audit.Enabled, effective);
            ValidateRequiredWarmup(view.Warmup, effective.Workload.WarmupSeconds);
        }

        if (view.FinalAudit is not null)
        {
            Require(view.Measurement is not null, "Final-audit evidence requires a completed measurement.");
        }

        if (IsWarmupFailure(view))
        {
            Require(view.Measurement is null, "A failed warmup cannot contain measurement evidence.");
            Require(view.Restoration is null, "A failed warmup cannot contain post-warmup restoration evidence.");
            Require(view.FinalAudit is null, "A failed warmup cannot contain final-audit evidence.");
            ValidateScheduledDuration(view.FailedPhase!, effective.Workload.WarmupSeconds, "failed warmup");
            ValidateRequiredPopulation(
                view.Population,
                effective.Population.Enabled,
                "population",
                expectedPopulation);
            ValidateRequiredAudit(view.InitialAudit, effective.Audit.Enabled, effective);
            return;
        }

        var measurement = view.Measurement ?? view.FailedPhase;
        if (measurement is null)
        {
            return;
        }

        ValidateScheduledDuration(measurement, effective.Workload.DurationSeconds, "measurement");
        ValidateRequiredPopulation(
            view.Population,
            effective.Population.Enabled,
            "population",
            expectedPopulation);
        ValidateRequiredAudit(view.InitialAudit, effective.Audit.Enabled, effective);
        ValidateRequiredWarmup(view.Warmup, effective.Workload.WarmupSeconds);
        ValidateRequiredPopulation(
            view.Restoration,
            restorationEnabled,
            "post-warmup restoration",
            expectedPopulation);
        if (view.FailedPhase is not null)
        {
            Require(view.FinalAudit is null, "A failed measurement cannot contain final-audit evidence.");
        }
    }

    private static void ValidateRequiredPopulation(
        PopulationResult? population,
        bool required,
        string phase,
        long expectedCompleted)
    {
        if (!required)
        {
            Require(population is null, $"Population phase '{phase}' must be null when disabled.");
            return;
        }

        Require(population is not null, $"Population phase '{phase}' is required.");
        Require(population!.Phase == phase, $"Population phase name must be '{phase}'.");
        Require(population.Status == "completed", $"Population phase '{phase}' must be completed.");
        Require(population.Completed == expectedCompleted, $"Population phase '{phase}' has an unexpected completed count.");
    }

    private static void ValidateOptionalPopulation(
        PopulationResult? population,
        bool allowed,
        string phase,
        long expectedCompleted)
    {
        if (!allowed)
        {
            Require(population is null, $"Population phase '{phase}' must be null when disabled.");
            return;
        }

        if (population is null)
        {
            return;
        }

        Require(population.Phase == phase, $"Population phase name must be '{phase}'.");
        Require(population.Completed <= expectedCompleted, $"Population phase '{phase}' exceeds its client shard.");
        Require(
            population.Status == "partial" || population.Completed == expectedCompleted,
            $"Completed population phase '{phase}' has an unexpected completed count.");
    }

    private static void ValidateRequiredAudit(
        CorrectnessAuditResult? audit,
        bool required,
        EffectiveBenchmarkConfiguration effective)
    {
        if (!required)
        {
            Require(audit is null, "Correctness audit evidence must be null when audits are disabled.");
            return;
        }

        Require(audit is not null, "Required correctness audit evidence is missing.");
        ValidateAudit(audit!, effective);
    }

    private static void ValidateOptionalAudit(
        CorrectnessAuditResult? audit,
        bool allowed,
        EffectiveBenchmarkConfiguration effective)
    {
        if (!allowed)
        {
            Require(audit is null, "Correctness audit evidence must be null when audits are disabled.");
        }
        else if (audit is not null)
        {
            ValidateAudit(audit, effective);
        }
    }

    private static void ValidateAudit(
        CorrectnessAuditResult audit,
        EffectiveBenchmarkConfiguration effective)
    {
        var ordinal = effective.ClientOrdinal!.Value;
        var count = effective.ClientCount!.Value;
        var expectedPointChecks = GetClientShardCount(effective.Audit.PointSampleCount, ordinal, count);
        var expectedQueryChecks = ordinal == 0 ? effective.Audit.QuerySampleCount : 0;
        Require(audit.Status == "passed", "Correctness audit status must be passed.");
        Require(audit.PointChecks == expectedPointChecks, "Correctness audit point-check count is inconsistent.");
        Require(audit.ExactQueryChecks == expectedQueryChecks, "Correctness audit exact-query count is inconsistent.");
        Require(audit.RangeQueryChecks == expectedQueryChecks, "Correctness audit range-query count is inconsistent.");
        Require(
            audit.PointCoverage == CorrectnessAuditPlan.DescribePointCoverage(
                effective.Audit.PointSampleCount == effective.Dataset.RecordCount,
                ordinal,
                count),
            "Correctness audit point-coverage label is inconsistent.");
    }

    private static void ValidateRequiredWarmup(PhaseResult? warmup, int warmupSeconds)
    {
        if (warmupSeconds <= 0)
        {
            Require(warmup is null, "Warmup evidence must be null when warmup is disabled.");
            return;
        }

        Require(warmup is not null, "Enabled warmup evidence is missing.");
        ValidateScheduledDuration(warmup!, warmupSeconds, "warmup");
    }

    private static void ValidateScheduledDuration(PhaseResult phase, int expectedSeconds, string phaseName)
    {
        Require(
            Math.Abs(phase.ScheduledDurationSeconds - expectedSeconds) <= 1e-6,
            $"{phaseName} scheduled duration differs from the effective workload.");
    }

    private static long GetClientShardCount(long total, int ordinal, int count)
    {
        return total / count + (ordinal < total % count ? 1 : 0);
    }

    private static async Task ValidateHistogramsAsync(
        string resultPath,
        ArtifactView view,
        LoadMode loadMode,
        CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(resultPath)
            ?? throw new InvalidDataException("Benchmark result has no parent directory.");
        var manifestPaths = view.HistogramArtifacts
            .Select(static artifact => artifact.Path)
            .Order(StringComparer.Ordinal)
            .ToArray();
        var diskPaths = Directory.EnumerateFiles(directory, "*.hlog", SearchOption.AllDirectories)
            .Select(path => Path.GetRelativePath(directory, path)
                .Replace(Path.DirectorySeparatorChar, '/'))
            .Order(StringComparer.Ordinal)
            .ToArray();
        Require(
            manifestPaths.SequenceEqual(diskPaths, StringComparer.Ordinal),
            "The result directory HLOG set differs from histogramArtifacts.");

        var phase = view.Measurement ?? view.FailedPhase;
        if (phase is null)
        {
            Require(view.HistogramArtifacts.Count == 0, "Histogram artifacts have no measurement or failed phase.");
            return;
        }

        if (IsWarmupFailure(view))
        {
            Require(view.HistogramArtifacts.Count == 0, "A failed warmup must not claim measurement histograms.");
            return;
        }

        Require(view.HistogramArtifacts.Count == 15, "A measurement or failed phase requires the exact 15-file histogram contract.");
        var expectedTuples = Enum.GetValues<OperationKind>()
            .SelectMany(static operation => new[]
            {
                $"{operation.ToString().ToLowerInvariant()}|succeeded|latency",
                $"{operation.ToString().ToLowerInvariant()}|failed|latency",
                $"{operation.ToString().ToLowerInvariant()}|all|queue-delay",
            })
            .Order(StringComparer.Ordinal)
            .ToArray();
        var actualTuples = view.HistogramArtifacts
            .Select(static artifact => $"{artifact.Operation}|{artifact.Outcome}|{artifact.Metric}")
            .Order(StringComparer.Ordinal)
            .ToArray();
        Require(actualTuples.SequenceEqual(expectedTuples, StringComparer.Ordinal), "Histogram tuple set is not the exact 15-file contract.");

        foreach (var artifact in view.HistogramArtifacts)
        {
            Require(
                phase.Operations.TryGetValue(artifact.Operation, out var operation),
                $"Histogram operation '{artifact.Operation}' has no phase metrics.");
            var expectedPath = $"{artifact.Metric}-{artifact.Operation}-{artifact.Outcome}.hlog";
            Require(artifact.Path == expectedPath, $"Histogram path must be '{expectedPath}'.");
            Require(artifact.ClientInstance == view.InstanceId, "Histogram client instance differs from result instanceId.");
            var fullPath = Path.GetFullPath(Path.Combine(directory, artifact.Path));
            Require(Path.GetDirectoryName(fullPath) == Path.GetFullPath(directory), "Histogram path escapes the result directory.");
            var fileInfo = new FileInfo(fullPath);
            Require(fileInfo.Exists && fileInfo.Length > 0, $"Histogram '{artifact.Path}' is missing or empty.");
            Require(fileInfo.LinkTarget is null, $"Histogram '{artifact.Path}' must not be a symbolic link.");

            var bytes = await File.ReadAllBytesAsync(fullPath, cancellationToken);
            var actualSha256 = Convert.ToHexStringLower(SHA256.HashData(bytes));
            Require(actualSha256 == artifact.Sha256, $"Histogram '{artifact.Path}' SHA-256 mismatch.");

            HistogramBase histogram;
            try
            {
                await using var stream = new MemoryStream(bytes, writable: false);
                using var reader = new HistogramLogReader(stream);
                var histograms = reader.ReadHistograms().ToArray();
                Require(histograms.Length == 1, $"Histogram '{artifact.Path}' must contain exactly one HDR histogram.");
                histogram = histograms[0];
            }
            catch (Exception exception) when (exception is not InvalidDataException)
            {
                throw new InvalidDataException($"Histogram '{artifact.Path}' is not a valid HDR histogram log.", exception);
            }

            Require(histogram.LowestTrackableValue == artifact.LowestDiscernibleValue, $"Histogram '{artifact.Path}' lowest value differs from its manifest.");
            Require(histogram.HighestTrackableValue == artifact.HighestTrackableValue, $"Histogram '{artifact.Path}' highest value differs from its manifest.");
            Require(histogram.NumberOfSignificantValueDigits == artifact.SignificantDigits, $"Histogram '{artifact.Path}' significant digits differ from its manifest.");
            Require(histogram.TotalCount == artifact.Count, $"Histogram '{artifact.Path}' count differs from its manifest.");

            var expectedCount = artifact.Metric == "queue-delay"
                ? loadMode is LoadMode.OpenLoop ? operation!.Completed : 0
                : artifact.Outcome == "succeeded" ? operation!.Succeeded : operation!.Failed;
            Require(histogram.TotalCount == expectedCount, $"Histogram '{artifact.Path}' count differs from phase metrics.");
            var summary = artifact.Metric == "queue-delay"
                ? operation!.QueueDelayMicroseconds
                : artifact.Outcome == "succeeded"
                    ? operation!.SucceededLatencyMicroseconds
                    : operation!.FailedLatencyMicroseconds;
            var actualSummary = BenchmarkResultWriter.CreateLatencySummary(histogram);
            Require(actualSummary == summary, $"Histogram '{artifact.Path}' summary differs from its HDR values.");
        }
    }

    private static bool IsWarmupFailure(ArtifactView view)
    {
        return view.IsFailure &&
            view.FailedPhase is not null &&
            view.Measurement is null &&
            view.Warmup is null &&
            view.EffectiveConfiguration.Workload.WarmupSeconds > 0;
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidDataException(message);
        }
    }

    private sealed record SourceEvidence(
        BenchmarkScenarioSpec Scenario,
        DatasetSpec Dataset,
        WorkloadSpec Workload);

    private sealed record ArtifactView(
        string Status,
        string RunId,
        string InstanceId,
        DateTimeOffset StartedAtUtc,
        DateTimeOffset EndedAtUtc,
        IReadOnlyList<SpecArtifactProvenance> SourceSpecs,
        EffectiveBenchmarkConfiguration EffectiveConfiguration,
        string EffectiveConfigurationSha256,
        string EffectiveConfigurationContentBase64,
        PopulationResult? Population,
        CorrectnessAuditResult? InitialAudit,
        PopulationResult? Restoration,
        PhaseResult? Warmup,
        PhaseResult? Measurement,
        CorrectnessAuditResult? FinalAudit,
        PhaseResult? FailedPhase,
        IReadOnlyList<HistogramArtifact> HistogramArtifacts,
        bool IsFailure);
}
