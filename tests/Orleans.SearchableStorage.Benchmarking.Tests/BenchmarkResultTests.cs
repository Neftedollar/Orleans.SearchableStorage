using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using HdrHistogram;
using Json.Schema;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class BenchmarkResultTests
{
    private static readonly Lazy<JsonSchema> ResultSchema = new(LoadResultSchema);

    [Fact]
    public async Task SuccessResultRoundTripsProvenanceEffectiveHashAuditCleanupAndHistogramReferences()
    {
        const string environmentName = "OSS_BENCHMARK_TEST_CONNECTION";
        const string secretCanary = "Host=secret.example;Password=do-not-serialize";
        const string commit = "0123456789abcdef0123456789abcdef01234567";
        var previousSecret = Environment.GetEnvironmentVariable(environmentName);
        var previousCommit = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT");
        var previousDirty = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY");
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-result-tests-{Guid.NewGuid():N}");
        Environment.SetEnvironmentVariable(environmentName, secretCanary);
        Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT", commit);
        Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", "true");
        try
        {
            var spec = BenchmarkTestData.CreateSpec();
            spec.Storage.ConnectionStringEnvironment = environmentName;
            var loadedSpec = new LoadedBenchmarkSpec(
                spec,
                [
                    CreateArtifact("scenario"),
                    CreateArtifact("dataset"),
                    CreateArtifact("workload"),
                ]);
            var effective = BenchmarkResultWriter.CreateEffectiveConfiguration(
                spec,
                runId: "test-run",
                instanceId: "client-0",
                clientOrdinal: 0,
                clientCount: 1);
            var execution = CreateSuccessfulExecution();
            var cleanup = new BackendCleanupReport(
                "process-memory",
                Attempted: true,
                Succeeded: true,
                Error: null);
            CrankMetrics.RegisterAndStart();

            var resultPath = await BenchmarkResultWriter.WriteAsync(
                outputDirectory,
                "test-run",
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                effective,
                cleanup,
                execution,
                CancellationToken.None);

            var resultBytes = await File.ReadAllBytesAsync(resultPath);
            AssertResultSchemaAccepts(resultBytes);
            AssertResultSchemaIsStrict(resultBytes);
            var resultJson = Encoding.UTF8.GetString(resultBytes);
            var result = JsonSerializer.Deserialize(
                resultBytes,
                BenchmarkJsonContext.Default.BenchmarkRunResult);
            Assert.NotNull(result);
            Assert.DoesNotContain(secretCanary, resultJson, StringComparison.Ordinal);
            Assert.Equal(environmentName, result.EffectiveConfiguration.Storage.ConnectionStringEnvironment);
            Assert.Equal(EffectiveDriverOverrides.None, result.EffectiveConfiguration.AppliedOverrides);
            Assert.Equal(1, result.EffectiveConfiguration.Topology.SiloCount);
            Assert.Equal(1_024, result.EffectiveConfiguration.Storage.VirtualSlotCount);
            Assert.Equal("succeeded", result.Status);
            Assert.Equal("passed", result.InitialAudit?.Status);
            Assert.Equal("all-points", result.InitialAudit?.PointCoverage);
            Assert.Equal("passed", result.FinalAudit?.Status);
            Assert.Equal(cleanup, result.Cleanup);
            Assert.Equal(3, result.SourceSpecs.Count);
            Assert.NotEmpty(result.Provenance.DriverVersion);
            Assert.Equal(commit, result.Provenance.GitCommit);
            Assert.True(result.Provenance.GitDirty);
            Assert.NotEmpty(result.Provenance.FrameworkDescription);
            Assert.Equal(
                [
                    "Azure.Storage.Blobs",
                    "HdrHistogram",
                    "Microsoft.Crank.EventSources",
                    "Microsoft.Orleans.Runtime",
                    "Npgsql",
                    "Orleans.SearchableStorage",
                    "StackExchange.Redis",
                ],
                result.Provenance.Components.Keys.Order(StringComparer.Ordinal).ToArray());

            var effectiveBytes = Convert.FromBase64String(result.EffectiveConfigurationContentBase64);
            Assert.Equal(
                result.EffectiveConfigurationSha256,
                Convert.ToHexStringLower(SHA256.HashData(effectiveBytes)));
            var effectiveRoundTrip = JsonSerializer.Deserialize(
                effectiveBytes,
                BenchmarkJsonContext.Default.EffectiveBenchmarkConfiguration);
            Assert.NotNull(effectiveRoundTrip);
            Assert.Equal(
                effectiveBytes,
                JsonSerializer.SerializeToUtf8Bytes(
                    effectiveRoundTrip,
                    BenchmarkJsonContext.Default.EffectiveBenchmarkConfiguration));

            Assert.Equal(15, result.HistogramArtifacts.Count);
            Assert.Equal(
                Enum.GetValues<OperationKind>()
                    .SelectMany(static operation => new[]
                    {
                        $"{operation.ToString().ToLowerInvariant()}|succeeded|latency",
                        $"{operation.ToString().ToLowerInvariant()}|failed|latency",
                        $"{operation.ToString().ToLowerInvariant()}|all|queue-delay",
                    })
                    .Order(StringComparer.Ordinal),
                result.HistogramArtifacts
                    .Select(static artifact => $"{artifact.Operation}|{artifact.Outcome}|{artifact.Metric}")
                    .Order(StringComparer.Ordinal));
            Assert.Equal(
                ["clear", "exactquery", "rangequery", "read", "upsert"],
                result.Measurement.Operations.Keys.Order(StringComparer.Ordinal).ToArray());
            Assert.True(double.IsFinite(result.Measurement.OfferedPerSecond));
            Assert.True(double.IsFinite(result.Measurement.CompletedPerSecond));
            Assert.Equal(1, result.Measurement.OfferedPerSecond);
            Assert.Equal(1, result.Measurement.CompletedPerSecond);
            Assert.NotNull(result.Measurement.Operations["read"].SucceededLatencyMicroseconds);
            var latency = result.Measurement.Operations["read"].SucceededLatencyMicroseconds!;
            Assert.InRange(latency.P95, latency.P90, latency.P99);
            foreach (var artifact in result.HistogramArtifacts)
            {
                var artifactPath = Path.Combine(Path.GetDirectoryName(resultPath)!, artifact.Path);
                var histogramBytes = await File.ReadAllBytesAsync(artifactPath);
                Assert.Equal(artifact.Sha256, Convert.ToHexStringLower(SHA256.HashData(histogramBytes)));
                Assert.Equal("client-0", artifact.ClientInstance);
                Assert.Equal("HdrHistogram log v1.3 (compressed V2 histogram)", artifact.Format);
                Assert.Equal(WorkerMetrics.LowestDiscernibleMicroseconds, artifact.LowestDiscernibleValue);
                Assert.Equal(WorkerMetrics.HighestTrackableMicroseconds, artifact.HighestTrackableValue);
                Assert.Equal(WorkerMetrics.SignificantDigits, artifact.SignificantDigits);
                Assert.Equal($"{artifact.Metric}-{artifact.Operation}-{artifact.Outcome}.hlog", artifact.Path);
                var operation = result.Measurement.Operations[artifact.Operation];
                var expectedCount = artifact.Metric == "queue-delay"
                    ? 0
                    : artifact.Outcome == "succeeded"
                        ? operation.Succeeded
                        : operation.Failed;
                Assert.Equal(expectedCount, artifact.Count);
                await using var histogramStream = File.OpenRead(artifactPath);
                using var histogramReader = new HistogramLogReader(histogramStream);
                var histogram = Assert.Single(histogramReader.ReadHistograms());
                Assert.Equal(artifact.Count, histogram.TotalCount);
                Assert.Equal(artifact.LowestDiscernibleValue, histogram.LowestTrackableValue);
                Assert.Equal(artifact.HighestTrackableValue, histogram.HighestTrackableValue);
                Assert.Equal(artifact.SignificantDigits, histogram.NumberOfSignificantValueDigits);
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable(environmentName, previousSecret);
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT", previousCommit);
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", previousDirty);
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ArtifactValidatorReconstructsEveryDeclaredDriverOverride()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-override-tests-{Guid.NewGuid():N}");
        try
        {
            using var temporarySpecs = await BenchmarkTestData.WriteSpecsAsync();
            var loadedSpec = await BenchmarkSpec.LoadAsync(temporarySpecs.ScenarioPath, CancellationToken.None);
            var options = new DriverOptions
            {
                Command = "run",
                SpecPath = temporarySpecs.ScenarioPath,
                RunId = "override-run",
                Backend = StorageBackend.Memory,
                ImplementationPath = StoragePath.Searchable,
                ConnectionStringEnvironment = "OSS_OVERRIDE_CONNECTION",
                AzureBlobContainer = "override-container",
                Topology = TopologyMode.External,
                AdvertisedAddress = "127.0.0.2",
                PrimarySiloEndpoint = "127.0.0.1:12001",
                GatewayEndpoints = ["127.0.0.1:31001"],
                SiloPort = 12_001,
                GatewayPort = 31_001,
            };
            options.ApplyTo(loadedSpec.Spec);
            var runId = options.ApplyRunIdentity(loadedSpec.Spec);
            var appliedOverrides = options.CreateEffectiveOverrides();
            var effective = BenchmarkResultWriter.CreateEffectiveConfiguration(
                loadedSpec.Spec,
                runId,
                "client-0",
                clientOrdinal: 0,
                clientCount: 1,
                appliedOverrides);
            var resultPath = await BenchmarkResultWriter.WriteAsync(
                outputDirectory,
                runId,
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                effective,
                new BackendCleanupReport("process-memory", Attempted: true, Succeeded: true, Error: null),
                CreateSuccessfulExecution(),
                CancellationToken.None);

            await BenchmarkArtifactValidator.ValidateAsync(resultPath, CancellationToken.None);

            Assert.Same(appliedOverrides, effective.AppliedOverrides);
            Assert.Equal(TopologyMode.External, effective.Topology.Mode);
            Assert.Equal("127.0.0.2", effective.Topology.AdvertisedAddress);
            Assert.Equal(["127.0.0.1:31001"], effective.Topology.GatewayEndpoints);
            Assert.Equal(12_001, effective.Topology.SiloPort);
            Assert.Equal(31_001, effective.Topology.GatewayPort);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task FailureResultRoundTripsCleanupFailureAndIncompleteDrainEvidence()
    {
        const string environmentName = "OSS_BENCHMARK_FAILURE_TEST_CONNECTION";
        const string environmentSecret = "Host=db.example;Username=app;Password=environment-secret;";
        string[] secretCanaries =
        [
            "environment-secret",
            "userinfo-secret",
            "query-secret",
            "connection-secret",
            "account-key-secret",
            "json-secret",
            "quoted-secret with spaces",
            "single-quoted secret",
            "braced secret",
            "unquoted secret with spaces",
            "escaped-json-secret",
            "bearer-secret-token",
        ];
        var previousSecret = Environment.GetEnvironmentVariable(environmentName);
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-failure-tests-{Guid.NewGuid():N}");
        Environment.SetEnvironmentVariable(environmentName, environmentSecret);
        try
        {
            var spec = BenchmarkTestData.CreateSpec(
                StoragePath.Plain,
                new OperationMixSpec
                {
                    Upsert = 0,
                    Read = 1,
                    ExactQuery = 0,
                    RangeQuery = 0,
                });
            spec.Storage.ConnectionStringEnvironment = environmentName;
            var loadedSpec = new LoadedBenchmarkSpec(
                spec,
                [CreateArtifact("scenario"), CreateArtifact("dataset"), CreateArtifact("workload")]);
            var effective = BenchmarkResultWriter.CreateEffectiveConfiguration(
                spec,
                "failed-run",
                "client-2",
                clientOrdinal: 2,
                clientCount: 4);
            var cleanup = new BackendCleanupReport(
                "drop-schema-on-silo-exit",
                Attempted: true,
                Succeeded: false,
                Error: $"Cleanup failed using {environmentSecret} AccountKey=account-key-secret");
            var timeout = new BenchmarkCallTimeoutException(
                operationTimeout: TimeSpan.FromSeconds(2),
                lateCallDrainTimeout: TimeSpan.FromSeconds(3),
                lateCallDrainDuration: TimeSpan.FromSeconds(3.25),
                lateCallDrainIncomplete: true,
                new TimeoutException("transport still running"));
            var leakyMessage =
                $"Database failed: Password=connection-secret; {environmentSecret} " +
                "Password=\"quoted-secret with spaces\"; " +
                "Pwd='single-quoted secret'; AccountKey={braced secret}; " +
                "SharedAccessSignature=unquoted secret with spaces; " +
                "Authorization: Bearer bearer-secret-token; " +
                "https://alice:userinfo-secret@example.test/path?token=query-secret&safe=value " +
                "{\"token\":\"json-secret\",\"secret\":\"escaped-json-secret\\\"tail\"}";
            Exception failureWithSecrets;
            try
            {
                throw new AggregateException(timeout, new InvalidOperationException(leakyMessage));
            }
            catch (AggregateException exception)
            {
                failureWithSecrets = exception;
            }

            var engine = new BenchmarkRunEngine(spec, clusterClient: null!, clientOrdinal: 2, clientCount: 4);
            var completedExecution = CreateSuccessfulExecution();

            var failurePath = await BenchmarkResultWriter.WriteFailureAsync(
                outputDirectory,
                "failed-run",
                "client-2",
                DateTimeOffset.UtcNow,
                loadedSpec,
                effective,
                cleanup,
                completedExecution,
                engine,
                failureWithSecrets,
                CancellationToken.None);

            var failureBytes = await File.ReadAllBytesAsync(failurePath);
            AssertResultSchemaAccepts(failureBytes);
            var serializedFailure = Encoding.UTF8.GetString(failureBytes);
            var consoleSafeFailure = SecretRedactor.Redact(failureWithSecrets.ToString(), environmentName)!;
            foreach (var canary in secretCanaries)
            {
                Assert.DoesNotContain(canary, serializedFailure, StringComparison.Ordinal);
                Assert.DoesNotContain(canary, consoleSafeFailure, StringComparison.Ordinal);
            }

            var failure = JsonSerializer.Deserialize(
                failureBytes,
                BenchmarkJsonContext.Default.BenchmarkFailureResult);
            Assert.NotNull(failure);
            Assert.Equal("failed", failure.Status);
            Assert.True(failure.Cleanup.Attempted);
            Assert.False(failure.Cleanup.Succeeded);
            Assert.NotNull(failure.Cleanup.Error);
            Assert.Contains("[REDACTED]", failure.Cleanup.Error, StringComparison.Ordinal);
            Assert.NotNull(failure.Failure.LateCallDrainEvidence);
            Assert.Equal("timeout", failure.Failure.LateCallDrainEvidence.Trigger);
            Assert.Equal(2, failure.Failure.LateCallDrainEvidence.OperationTimeoutSeconds);
            Assert.Equal(3, failure.Failure.LateCallDrainEvidence.LateCallDrainTimeoutSeconds);
            Assert.Equal(3.25, failure.Failure.LateCallDrainEvidence.LateCallDrainDurationSeconds);
            Assert.True(failure.Failure.LateCallDrainEvidence.LateCallDrainIncomplete);
            Assert.Equal(2, failure.EffectiveConfiguration.ClientOrdinal);
            Assert.Equal(4, failure.EffectiveConfiguration.ClientCount);
            Assert.Null(failure.EffectiveConfiguration.Storage.PartitionCount);
            Assert.Null(failure.EffectiveConfiguration.Storage.VirtualSlotTargetCount);
            Assert.Null(failure.EffectiveConfiguration.Storage.VirtualSlotCount);
            Assert.Null(failure.EffectiveConfiguration.Storage.JournalSegmentCapacity);
            Assert.Null(failure.EffectiveConfiguration.Storage.MaximumJournalReplayEntries);
            Assert.Null(failure.EffectiveConfiguration.Storage.CompactionThreshold);
            Assert.NotNull(failure.Measurement);
            Assert.Null(failure.FailedPhase);
            Assert.NotEmpty(failure.HistogramArtifacts);
        }
        finally
        {
            Environment.SetEnvironmentVariable(environmentName, previousSecret);
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task FailureResultUsesEngineMeasurementAndSerializesPartialPopulation()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-engine-failure-tests-{Guid.NewGuid():N}");
        try
        {
            var spec = BenchmarkTestData.CreateSpec();
            var loadedSpec = new LoadedBenchmarkSpec(
                spec,
                [CreateArtifact("scenario"), CreateArtifact("dataset"), CreateArtifact("workload")]);
            var execution = CreateSuccessfulExecution();
            var engine = new BenchmarkRunEngine(spec, new UnusedOperationExecutor(), clientOrdinal: 0, clientCount: 1);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedMeasurement), execution.Measurement);
            SetEngineState(
                engine,
                nameof(BenchmarkRunEngine.PartialPopulation),
                new PopulationExecution("population", DateTimeOffset.UtcNow, 0.25, Completed: 17));

            var failurePath = await BenchmarkResultWriter.WriteFailureAsync(
                outputDirectory,
                "engine-fallback",
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                BenchmarkResultWriter.CreateEffectiveConfiguration(spec, "engine-fallback", "client-0", 0, 1),
                new BackendCleanupReport(
                    "process-memory",
                    Attempted: true,
                    Succeeded: false,
                    Error: "synthetic teardown failure"),
                execution: null,
                engine,
                new InvalidOperationException("teardown failed"),
                CancellationToken.None);

            var failureBytes = await File.ReadAllBytesAsync(failurePath);
            AssertResultSchemaAccepts(failureBytes);
            var failure = JsonSerializer.Deserialize(
                failureBytes,
                BenchmarkJsonContext.Default.BenchmarkFailureResult);

            Assert.NotNull(failure);
            Assert.NotNull(failure.Measurement);
            Assert.Equal(execution.Measurement.Completed, failure.Measurement.Completed);
            Assert.Equal("partial", failure.Population?.Status);
            Assert.Equal(17, failure.Population?.Completed);
            Assert.Equal(15, failure.HistogramArtifacts.Count);
            foreach (var artifact in failure.HistogramArtifacts)
            {
                var artifactPath = Path.Combine(Path.GetDirectoryName(failurePath)!, artifact.Path);
                await using var histogramStream = File.OpenRead(artifactPath);
                using var histogramReader = new HistogramLogReader(histogramStream);
                Assert.Equal(artifact.Count, Assert.Single(histogramReader.ReadHistograms()).TotalCount);
            }
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ArtifactContractAcceptsFailureWithACompletedMeasurement()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-completed-failure-tests-{Guid.NewGuid():N}");
        try
        {
            using var temporarySpecs = await BenchmarkTestData.WriteSpecsAsync();
            var loadedSpec = await BenchmarkSpec.LoadAsync(temporarySpecs.ScenarioPath, CancellationToken.None);
            var spec = loadedSpec.Spec;
            var runId = new DriverOptions
            {
                Command = "run",
                SpecPath = temporarySpecs.ScenarioPath,
                RunId = "completed-failure",
            }.ApplyRunIdentity(spec);
            var execution = CreateSuccessfulExecution();
            var engine = new BenchmarkRunEngine(spec, new UnusedOperationExecutor(), clientOrdinal: 0, clientCount: 1);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedPopulation), execution.Population);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedInitialAudit), execution.InitialAudit);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedMeasurement), execution.Measurement);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedFinalAudit), execution.FinalAudit);

            var failurePath = await BenchmarkResultWriter.WriteFailureAsync(
                outputDirectory,
                runId,
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                BenchmarkResultWriter.CreateEffectiveConfiguration(spec, runId, "client-0", 0, 1),
                new BackendCleanupReport(
                    "process-memory",
                    Attempted: true,
                    Succeeded: false,
                    Error: "synthetic teardown failure"),
                execution: null,
                engine,
                new InvalidOperationException("teardown failed"),
                CancellationToken.None);

            await BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None);

            var original = await File.ReadAllBytesAsync(failurePath);
            var missingMeasurement = JsonNode.Parse(original)!.AsObject();
            missingMeasurement["measurement"] = null;
            await File.WriteAllTextAsync(failurePath, missingMeasurement.ToJsonString());
            var exception = await Assert.ThrowsAsync<InvalidDataException>(
                () => BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None));
            Assert.Contains("Final-audit evidence", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task FailureArtifactRequiresCompletedPriorPhases()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-phase-order-tests-{Guid.NewGuid():N}");
        try
        {
            using var temporarySpecs = await BenchmarkTestData.WriteSpecsAsync(warmupSeconds: 1);
            var loadedSpec = await BenchmarkSpec.LoadAsync(temporarySpecs.ScenarioPath, CancellationToken.None);
            var spec = loadedSpec.Spec;
            var runId = new DriverOptions
            {
                Command = "run",
                SpecPath = temporarySpecs.ScenarioPath,
                RunId = "phase-order",
            }.ApplyRunIdentity(spec);
            var execution = CreateSuccessfulExecution();
            var engine = new BenchmarkRunEngine(spec, new UnusedOperationExecutor(), clientOrdinal: 0, clientCount: 1);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedPopulation), execution.Population);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedInitialAudit), execution.InitialAudit);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedWarmup), CreateWarmup());

            var failurePath = await BenchmarkResultWriter.WriteFailureAsync(
                outputDirectory,
                runId,
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                BenchmarkResultWriter.CreateEffectiveConfiguration(spec, runId, "client-0", 0, 1),
                new BackendCleanupReport(
                    "process-memory",
                    Attempted: true,
                    Succeeded: false,
                    Error: "synthetic restoration failure"),
                execution: null,
                engine,
                new InvalidOperationException("restoration failed"),
                CancellationToken.None);

            await BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None);
            var root = JsonNode.Parse(await File.ReadAllBytesAsync(failurePath))!.AsObject();
            root["population"] = null;
            await File.WriteAllTextAsync(failurePath, root.ToJsonString());
            var exception = await Assert.ThrowsAsync<InvalidDataException>(
                () => BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None));
            Assert.Contains("Population phase 'population' is required", exception.Message, StringComparison.Ordinal);

            PhaseExecution CreateWarmup()
            {
                var start = Stopwatch.GetTimestamp();
                return PhaseExecution.Create(
                    DateTimeOffset.UtcNow,
                    start,
                    start + Stopwatch.Frequency,
                    start + Stopwatch.Frequency,
                    [new WorkerMetrics(recordHistograms: false)],
                    schedulerCounters: null,
                    recordHistograms: false);
            }
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task FailedPhaseRequiresItsHistogramEvidence()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-failed-phase-tests-{Guid.NewGuid():N}");
        try
        {
            using var temporarySpecs = await BenchmarkTestData.WriteSpecsAsync();
            var loadedSpec = await BenchmarkSpec.LoadAsync(temporarySpecs.ScenarioPath, CancellationToken.None);
            var spec = loadedSpec.Spec;
            var runId = new DriverOptions
            {
                Command = "run",
                SpecPath = temporarySpecs.ScenarioPath,
                RunId = "failed-phase",
            }.ApplyRunIdentity(spec);
            var execution = CreateSuccessfulExecution();
            var engine = new BenchmarkRunEngine(spec, new UnusedOperationExecutor(), clientOrdinal: 0, clientCount: 1);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedPopulation), execution.Population);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedInitialAudit), execution.InitialAudit);
            SetEngineState(engine, nameof(BenchmarkRunEngine.FailedPhase), execution.Measurement);

            var failurePath = await BenchmarkResultWriter.WriteFailureAsync(
                outputDirectory,
                runId,
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                BenchmarkResultWriter.CreateEffectiveConfiguration(spec, runId, "client-0", 0, 1),
                new BackendCleanupReport(
                    "process-memory",
                    Attempted: true,
                    Succeeded: false,
                    Error: "synthetic workload failure"),
                execution: null,
                engine,
                new InvalidOperationException("measurement failed"),
                CancellationToken.None);

            await BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None);
            var failure = JsonSerializer.Deserialize(
                await File.ReadAllBytesAsync(failurePath),
                BenchmarkJsonContext.Default.BenchmarkFailureResult);
            Assert.NotNull(failure);
            Assert.Null(failure.Measurement);
            Assert.NotNull(failure.FailedPhase);
            Assert.Equal(15, failure.HistogramArtifacts.Count);

            var root = JsonNode.Parse(await File.ReadAllBytesAsync(failurePath))!.AsObject();
            root["histogramArtifacts"] = new JsonArray();
            foreach (var histogramPath in Directory.EnumerateFiles(
                Path.GetDirectoryName(failurePath)!,
                "*.hlog",
                SearchOption.AllDirectories))
            {
                File.Delete(histogramPath);
            }
            await File.WriteAllTextAsync(failurePath, root.ToJsonString());
            AssertResultSchemaAccepts(await File.ReadAllBytesAsync(failurePath));

            await Assert.ThrowsAsync<InvalidDataException>(
                () => BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None));
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task FailedWarmupRequiresPriorEvidenceAndNoMeasurementHistograms()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-failed-warmup-tests-{Guid.NewGuid():N}");
        try
        {
            using var temporarySpecs = await BenchmarkTestData.WriteSpecsAsync(warmupSeconds: 1);
            var loadedSpec = await BenchmarkSpec.LoadAsync(temporarySpecs.ScenarioPath, CancellationToken.None);
            var spec = loadedSpec.Spec;
            var runId = new DriverOptions
            {
                Command = "run",
                SpecPath = temporarySpecs.ScenarioPath,
                RunId = "failed-warmup",
            }.ApplyRunIdentity(spec);
            var execution = CreateSuccessfulExecution();
            var engine = new BenchmarkRunEngine(spec, new UnusedOperationExecutor(), clientOrdinal: 0, clientCount: 1);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedPopulation), execution.Population);
            SetEngineState(engine, nameof(BenchmarkRunEngine.CompletedInitialAudit), execution.InitialAudit);
            SetEngineState(engine, nameof(BenchmarkRunEngine.FailedPhase), CreatePhase(recordHistograms: false));

            var failurePath = await BenchmarkResultWriter.WriteFailureAsync(
                outputDirectory,
                runId,
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                BenchmarkResultWriter.CreateEffectiveConfiguration(spec, runId, "client-0", 0, 1),
                new BackendCleanupReport(
                    "process-memory",
                    Attempted: true,
                    Succeeded: false,
                    Error: "synthetic warmup failure"),
                execution: null,
                engine,
                new InvalidOperationException("warmup failed"),
                CancellationToken.None);

            await BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None);
            var original = await File.ReadAllBytesAsync(failurePath);
            var failure = JsonSerializer.Deserialize(original, BenchmarkJsonContext.Default.BenchmarkFailureResult);
            Assert.NotNull(failure);
            Assert.Null(failure.Measurement);
            Assert.NotNull(failure.FailedPhase);
            Assert.Empty(failure.HistogramArtifacts);

            await AssertTamperRejectedAsync(root => root["failedPhase"]!["scheduledDurationSeconds"] = 2);
            await AssertTamperRejectedAsync(root => root["population"] = null);

            async Task AssertTamperRejectedAsync(Action<JsonObject> tamper)
            {
                var root = JsonNode.Parse(original)!.AsObject();
                tamper(root);
                await File.WriteAllTextAsync(failurePath, root.ToJsonString());
                await Assert.ThrowsAsync<InvalidDataException>(
                    () => BenchmarkArtifactValidator.ValidateAsync(failurePath, CancellationToken.None));
                await File.WriteAllBytesAsync(failurePath, original);
            }

            PhaseExecution CreatePhase(bool recordHistograms)
            {
                var worker = new WorkerMetrics(recordHistograms);
                worker.RecordOffered(OperationKind.Read);
                worker.RecordStarted(OperationKind.Read);
                worker.RecordCompleted(
                    OperationKind.Read,
                    Stopwatch.Frequency / 1_000,
                    queueDelayStopwatchTicks: null,
                    succeeded: true,
                    resultCount: 1,
                    exception: null);
                var start = Stopwatch.GetTimestamp();
                return PhaseExecution.Create(
                    DateTimeOffset.UtcNow,
                    start,
                    start + Stopwatch.Frequency,
                    start + Stopwatch.Frequency,
                    [worker],
                    schedulerCounters: null,
                    recordHistograms);
            }
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void GitDirtyRemainsUnknownWhenGitStatusCannotBeRead()
    {
        var invoked = false;

        var dirty = BenchmarkResultWriter.ResolveGitDirty(
            "0123456789abcdef0123456789abcdef01234567",
            configuredDirty: null,
            arguments =>
            {
                invoked = true;
                Assert.Equal(["status", "--porcelain", "--untracked-files=normal"], arguments);
                return null;
            });

        Assert.True(invoked);
        Assert.Null(dirty);
    }

    [Theory]
    [InlineData(10, 0)]
    [InlineData(10, -1)]
    [InlineData(10, double.NaN)]
    [InlineData(10, double.PositiveInfinity)]
    public void InvalidDurationsProduceFiniteZeroRates(long count, double durationSeconds)
    {
        var rate = BenchmarkResultWriter.CalculateRate(count, durationSeconds);

        Assert.Equal(0, rate);
        Assert.True(double.IsFinite(rate));
    }

    [Fact]
    public void PositiveDurationProducesExactPositiveRate()
    {
        var rate = BenchmarkResultWriter.CalculateRate(10, 2.5);

        Assert.Equal(4, rate);
        Assert.True(double.IsFinite(rate));
        Assert.True(rate > 0);
    }

    [Fact]
    public void P95UsesTheDistinctNinetyFifthPercentileFixture()
    {
        var histogram = new LongHistogram(
            WorkerMetrics.LowestDiscernibleMicroseconds,
            WorkerMetrics.HighestTrackableMicroseconds,
            WorkerMetrics.SignificantDigits);
        Record(10, 89);
        Record(100, 5);
        Record(200, 4);
        Record(300, 2);

        var summary = BenchmarkResultWriter.CreateLatencySummary(histogram);

        Assert.NotNull(summary);
        Assert.Equal(100, summary.P90);
        Assert.Equal(200, summary.P95);
        Assert.Equal(300, summary.P99);

        void Record(long value, int count)
        {
            for (var index = 0; index < count; index++)
            {
                histogram.RecordValue(value);
            }
        }
    }

    [Fact]
    public void EffectiveCoordinatesUseDerivedVirtualSlotsAndDeclaredTotalSilos()
    {
        var storage = new StorageSpec
        {
            PartitionCount = 3,
            VirtualSlotTargetCount = 10,
        };
        var embedded = CreateCoordinateSpec(storage, new TopologySpec
        {
            SiloCount = 1,
            EmbeddedSiloCount = 1,
        });
        var external = CreateCoordinateSpec(new StorageSpec(), new TopologySpec
        {
            Mode = TopologyMode.External,
            SiloCount = 4,
            EmbeddedSiloCount = 1,
            GatewayEndpoints = ["127.0.0.1:30000"],
        });
        embedded.Validate();
        external.Validate();

        var embeddedEffective = BenchmarkResultWriter.CreateEffectiveConfiguration(
            embedded,
            "embedded-run",
            "client-0");
        var externalEffective = BenchmarkResultWriter.CreateEffectiveConfiguration(
            external,
            "external-run",
            "client-0");

        Assert.Equal(12, embeddedEffective.Storage.VirtualSlotCount);
        Assert.Equal(1, embeddedEffective.Topology.SiloCount);
        Assert.Equal(4, externalEffective.Topology.SiloCount);
        Assert.Equal(1, externalEffective.Topology.EmbeddedSiloCount);
    }

    [Fact]
    public async Task ArtifactValidatorRejectsEncodedContentTamperingAndUnsafeHistogramPaths()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        const string commit = "fedcba9876543210fedcba9876543210fedcba98";
        var previousCommit = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT");
        var previousDirty = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY");
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-validator-tests-{Guid.NewGuid():N}");
        Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT", commit);
        Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", "false");
        try
        {
            using var temporarySpecs = await BenchmarkTestData.WriteSpecsAsync(loadMode: LoadMode.OpenLoop);
            var loadedSpec = await BenchmarkSpec.LoadAsync(temporarySpecs.ScenarioPath, CancellationToken.None);
            var spec = loadedSpec.Spec;
            var runId = new DriverOptions
            {
                Command = "run",
                SpecPath = temporarySpecs.ScenarioPath,
                RunId = "validator-run",
            }.ApplyRunIdentity(spec);
            var resultPath = await BenchmarkResultWriter.WriteAsync(
                outputDirectory,
                runId,
                "client-0",
                DateTimeOffset.UtcNow,
                loadedSpec,
                BenchmarkResultWriter.CreateEffectiveConfiguration(spec, runId, "client-0", 0, 1),
                new BackendCleanupReport("process-memory", Attempted: true, Succeeded: true, Error: null),
                CreateSuccessfulExecution(openLoop: true),
                CancellationToken.None);
            var gateRoot = JsonNode.Parse(await File.ReadAllBytesAsync(resultPath))!.AsObject();
            gateRoot["provenance"]!["serverGc"] = true;
            await File.WriteAllTextAsync(resultPath, gateRoot.ToJsonString());
            var original = await File.ReadAllBytesAsync(resultPath);
            var scenarioSha256 = loadedSpec.Artifacts.Single(static artifact => artifact.Kind == "scenario").Sha256;

            var valid = await RunArtifactValidatorAsync(
                outputDirectory,
                commit,
                expectedRunId: "validator-run",
                expectedScenarioSha256: scenarioSha256,
                expectedSiloCount: 1);
            Assert.True(valid.ExitCode == 0, valid.Output);

            await AssertTamperRejectedAsync(root =>
                root["effectiveConfigurationContentBase64"] = Convert.ToBase64String("{}"u8));
            await AssertTamperRejectedAsync(root =>
                root["unexpectedTopLevel"] = true);
            await AssertTamperRejectedAsync(root =>
                root["effectiveConfiguration"]!["scenarioName"] = "tampered-scenario");
            await AssertTamperRejectedAsync(root =>
                root["sourceSpecs"]![0]!["contentBase64"] = Convert.ToBase64String("{}"u8));
            await AssertTamperRejectedAsync(root =>
                root["histogramArtifacts"]![0]!["path"] = "../escaped.hlog");
            await AssertTamperRejectedAsync(root =>
                root["measurement"]!["completed"] = 2);
            await AssertTamperRejectedAsync(root =>
            {
                root["measurement"]!["offered"] = 2;
                root["measurement"]!["offeredPerSecond"] = 2;
                root["measurement"]!["operations"]!["read"]!["offered"] = 2;
            });
            await AssertTamperRejectedAsync(root =>
                root["measurement"]!["completedPerSecond"] = 0.5);
            await AssertTamperRejectedAsync(root =>
            {
                root["measurement"]!["timedOut"] = 1;
                root["measurement"]!["operations"]!["read"]!["timedOut"] = 1;
            });
            await AssertTamperRejectedAsync(root =>
                root["histogramArtifacts"]![0]!["count"] = 99);
            await AssertTamperRejectedAsync(root =>
                root["histogramArtifacts"]![0]!["clientInstance"] = "another-client");
            await AssertTamperRejectedAsync(root =>
                root["histogramArtifacts"]![0]!["significantDigits"] = 2);
            await AssertTamperRejectedAsync(root =>
            {
                root["measurement"]!["operations"]!["read"]!["succeededLatencyMicroseconds"]!["p95"] = 600_000_000;
                root["measurement"]!["operations"]!["read"]!["succeededLatencyMicroseconds"]!["maximum"] = 1;
            });
            await AssertTamperRejectedAsync(root =>
            {
                root["population"] = null;
                root["initialAudit"] = null;
                root["finalAudit"] = null;
            });
            await AssertTamperRejectedAsync(root =>
                root["provenance"]!["serverGc"] = false);
            await AssertTamperRejectedAsync(root =>
                root["provenance"]!["components"]!.AsObject().Remove("Npgsql"));
            await AssertTamperRejectedAsync(root =>
                root["cleanup"]!["succeeded"] = false);
            await AssertTamperRejectedAsync(root =>
                root["runId"] = "different-run");
            await AssertSourceGraphTamperRejectedAsync();
            await AssertEffectiveWorkloadTamperRejectedAsync();
            await AssertEffectiveCoordinateTamperRejectedAsync();
            await AssertHistogramPayloadTamperRejectedAsync();
            await AssertUnreferencedHistogramRejectedAsync();

            Assert.NotEqual(
                0,
                (await RunArtifactValidatorAsync(
                    outputDirectory,
                    commit,
                    expectedRunId: "different-run",
                    expectedScenarioSha256: scenarioSha256,
                    expectedSiloCount: 1)).ExitCode);
            Assert.NotEqual(
                0,
                (await RunArtifactValidatorAsync(
                    outputDirectory,
                    commit,
                    expectedRunId: "validator-run",
                    expectedScenarioSha256: new string('a', 64),
                    expectedSiloCount: 1)).ExitCode);
            Assert.NotEqual(
                0,
                (await RunArtifactValidatorAsync(
                    outputDirectory,
                    commit,
                    expectedRunId: "validator-run",
                    expectedScenarioSha256: scenarioSha256,
                    expectedSiloCount: 2)).ExitCode);

            async Task AssertTamperRejectedAsync(Action<JsonObject> tamper)
            {
                var root = JsonNode.Parse(original)!.AsObject();
                tamper(root);
                await File.WriteAllTextAsync(resultPath, root.ToJsonString());
                var rejected = await RunArtifactValidatorAsync(
                    outputDirectory,
                    commit,
                    expectedRunId: "validator-run",
                    expectedScenarioSha256: scenarioSha256,
                    expectedSiloCount: 1);
                Assert.NotEqual(0, rejected.ExitCode);
                await File.WriteAllBytesAsync(resultPath, original);
            }

            async Task AssertSourceGraphTamperRejectedAsync()
            {
                var root = JsonNode.Parse(original)!.AsObject();
                var datasetArtifact = root["sourceSpecs"]!.AsArray()
                    .Single(static node => node!["kind"]!.GetValue<string>() == "dataset")!;
                var dataset = JsonNode.Parse(Convert.FromBase64String(
                    datasetArtifact["contentBase64"]!.GetValue<string>()))!.AsObject();
                dataset["$schema"] = "alternate-dataset-schema-location.json";
                var datasetBytes = Encoding.UTF8.GetBytes(dataset.ToJsonString());
                datasetArtifact["contentBase64"] = Convert.ToBase64String(datasetBytes);
                datasetArtifact["sha256"] = Convert.ToHexStringLower(SHA256.HashData(datasetBytes));
                await AssertRootRejectedAsync(root);
            }

            async Task AssertEffectiveWorkloadTamperRejectedAsync()
            {
                var root = JsonNode.Parse(original)!.AsObject();
                root["effectiveConfiguration"]!["workload"]!["durationSeconds"] = 2;
                UpdateEffectiveContent(root);
                await AssertRootRejectedAsync(root);
            }

            async Task AssertEffectiveCoordinateTamperRejectedAsync()
            {
                foreach (var tamper in new Action<JsonObject>[]
                {
                    effective => effective["storage"]!["backend"] = "Redis",
                    effective => effective["storage"]!["implementationPath"] = "Plain",
                    effective => effective["storage"]!["connectionStringEnvironment"] = "OTHER_ENVIRONMENT",
                    effective => effective["storage"]!["azureBlobContainer"] = "other-container",
                    effective => effective["topology"]!["advertisedAddress"] = "127.0.0.9",
                    effective => effective["topology"]!["gatewayEndpoints"] = new JsonArray("127.0.0.1:39999"),
                    effective => effective["topology"]!["siloPort"] = 12_345,
                    effective => effective["topology"]!["gatewayPort"] = 32_345,
                })
                {
                    var root = JsonNode.Parse(original)!.AsObject();
                    tamper(root["effectiveConfiguration"]!.AsObject());
                    UpdateEffectiveContent(root);
                    await AssertRootRejectedAsync(root);
                }
            }

            async Task AssertHistogramPayloadTamperRejectedAsync()
            {
                const string artifactPath = "latency-read-succeeded.hlog";
                var histogramPath = Path.Combine(Path.GetDirectoryName(resultPath)!, artifactPath);
                var histogramBytes = await File.ReadAllBytesAsync(histogramPath);
                try
                {
                    var invalidBytes = "this is not an HDR histogram"u8.ToArray();
                    await File.WriteAllBytesAsync(histogramPath, invalidBytes);
                    var root = JsonNode.Parse(original)!.AsObject();
                    var artifact = root["histogramArtifacts"]!.AsArray()
                        .Single(node => node!["path"]!.GetValue<string>() == artifactPath)!;
                    artifact["sha256"] = Convert.ToHexStringLower(SHA256.HashData(invalidBytes));
                    await AssertRootRejectedAsync(root);
                }
                finally
                {
                    await File.WriteAllBytesAsync(histogramPath, histogramBytes);
                    await File.WriteAllBytesAsync(resultPath, original);
                }
            }

            async Task AssertUnreferencedHistogramRejectedAsync()
            {
                var siblingDirectory = Path.Combine(outputDirectory, "stale-sibling");
                var extraHistogram = Path.Combine(siblingDirectory, "unreferenced.hlog");
                try
                {
                    Directory.CreateDirectory(siblingDirectory);
                    await File.WriteAllTextAsync(extraHistogram, "unreferenced histogram payload");
                    await AssertRootRejectedAsync(JsonNode.Parse(original)!.AsObject());
                }
                finally
                {
                    if (Directory.Exists(siblingDirectory))
                    {
                        Directory.Delete(siblingDirectory, recursive: true);
                    }
                    await File.WriteAllBytesAsync(resultPath, original);
                }
            }

            async Task AssertRootRejectedAsync(JsonObject root)
            {
                await File.WriteAllTextAsync(resultPath, root.ToJsonString());
                var rejected = await RunArtifactValidatorAsync(
                    outputDirectory,
                    commit,
                    expectedRunId: "validator-run",
                    expectedScenarioSha256: scenarioSha256,
                    expectedSiloCount: 1);
                Assert.NotEqual(0, rejected.ExitCode);
                await File.WriteAllBytesAsync(resultPath, original);
            }

            static void UpdateEffectiveContent(JsonObject root)
            {
                var effectiveBytes = Encoding.UTF8.GetBytes(root["effectiveConfiguration"]!.ToJsonString());
                root["effectiveConfigurationContentBase64"] = Convert.ToBase64String(effectiveBytes);
                root["effectiveConfigurationSha256"] = Convert.ToHexStringLower(SHA256.HashData(effectiveBytes));
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT", previousCommit);
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", previousDirty);
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Theory]
    [InlineData("Password=literal-secret")]
    [InlineData("AccountKey={braced secret with spaces}")]
    [InlineData("https://user:password@example.test/path")]
    [InlineData("https://example.test/path?sig=signature-value")]
    [InlineData("https://example.test/path?access_token=token-value")]
    [InlineData("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature")]
    public async Task ArtifactSecretScanRejectsCredentialShapes(string leakedValue)
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-secret-validator-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(outputDirectory);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(outputDirectory, "evidence.json"), leakedValue);

            var rejected = await RunSecretValidatorAsync(outputDirectory);

            Assert.NotEqual(0, rejected.ExitCode);
        }
        finally
        {
            Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task ArtifactSecretScanAcceptsRedactedCredentialShapes()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-redacted-validator-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(outputDirectory);
        try
        {
            await File.WriteAllTextAsync(
                Path.Combine(outputDirectory, "evidence.json"),
                "Password=[REDACTED]; {\"token\":\"[REDACTED]\"}; " +
                "Authorization: Bearer [REDACTED]; " +
                "https://[REDACTED]@example.test/path?sig=[REDACTED]");

            var accepted = await RunSecretValidatorAsync(outputDirectory);

            Assert.True(accepted.ExitCode == 0, accepted.Output);
        }
        finally
        {
            Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task ArtifactSecretScanRejectsCredentialShapesInArtifactNames()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-path-secret-validator-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(outputDirectory);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(outputDirectory, "token=artifact-secret"), "redacted content");

            var rejected = await RunSecretValidatorAsync(outputDirectory);

            Assert.NotEqual(0, rejected.ExitCode);
            Assert.Contains("artifact path", rejected.Output, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task ArtifactSecretScanRejectsSymbolicLinksBeforeUpload()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        var outputDirectory = Path.Combine(Path.GetTempPath(), $"oss-symlink-validator-tests-{Guid.NewGuid():N}");
        var targetDirectory = Path.Combine(Path.GetTempPath(), $"oss-symlink-target-{Guid.NewGuid():N}");
        Directory.CreateDirectory(outputDirectory);
        Directory.CreateDirectory(targetDirectory);
        try
        {
            var target = Path.Combine(targetDirectory, "secret.txt");
            await File.WriteAllTextAsync(target, "Authorization: Bearer symlink-secret-token");
            _ = File.CreateSymbolicLink(Path.Combine(outputDirectory, "linked-evidence.json"), target);

            var rejected = await RunSecretValidatorAsync(outputDirectory);

            Assert.NotEqual(0, rejected.ExitCode);
            Assert.Contains("symbolic links", rejected.Output, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Directory.Delete(outputDirectory, recursive: true);
            Directory.Delete(targetDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task ArtifactSecretScanRejectsRegularFileRoots()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        var artifactRoot = Path.Combine(Path.GetTempPath(), $"oss-file-root-validator-tests-{Guid.NewGuid():N}");
        try
        {
            await File.WriteAllTextAsync(artifactRoot, "Authorization: Bearer top-level-secret-token");

            var rejected = await RunSecretValidatorAsync(artifactRoot);

            Assert.NotEqual(0, rejected.ExitCode);
            Assert.Contains("roots must be directories", rejected.Output, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(artifactRoot);
        }
    }

    private static LoadedSpecArtifact CreateArtifact(string kind)
    {
        var content = Encoding.UTF8.GetBytes($"{{\"kind\":\"{kind}\"}}");
        return new LoadedSpecArtifact(
            kind,
            $"{kind}.json",
            Convert.ToHexStringLower(SHA256.HashData(content)),
            Convert.ToBase64String(content));
    }

    private static BenchmarkSpec CreateCoordinateSpec(StorageSpec storage, TopologySpec topology)
    {
        var baseline = BenchmarkTestData.CreateSpec();
        return new BenchmarkSpec(
            new BenchmarkScenarioSpec
            {
                Name = "coordinate-scenario",
                Dataset = new SpecReference
                {
                    Path = "dataset.json",
                    Sha256 = new string('0', 64),
                },
                Workload = new SpecReference
                {
                    Path = "workload.json",
                    Sha256 = new string('0', 64),
                },
                Population = baseline.Population,
                Audit = baseline.Audit,
                Storage = storage,
                Topology = topology,
            },
            baseline.Dataset,
            baseline.Workload);
    }

    private static void SetEngineState<T>(BenchmarkRunEngine engine, string propertyName, T value)
    {
        var setter = typeof(BenchmarkRunEngine)
            .GetProperty(propertyName)!
            .GetSetMethod(nonPublic: true)!;
        setter.Invoke(engine, [value]);
    }

    private static async Task<(int ExitCode, string Output)> RunArtifactValidatorAsync(
        string outputDirectory,
        string commit,
        string expectedRunId,
        string expectedScenarioSha256,
        int expectedSiloCount)
    {
        var validator = Path.Combine(AppContext.BaseDirectory, "eng", "validate-benchmark-artifacts.sh");
        Assert.True(File.Exists(validator), $"Artifact validator was not copied to '{validator}'.");
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "bash",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };
        process.StartInfo.ArgumentList.Add(validator);
        process.StartInfo.ArgumentList.Add("load");
        process.StartInfo.ArgumentList.Add(outputDirectory);
        process.StartInfo.ArgumentList.Add("searchable");
        process.StartInfo.ArgumentList.Add(commit);
        process.StartInfo.ArgumentList.Add("embedded");
        process.StartInfo.ArgumentList.Add("memory");
        process.StartInfo.ArgumentList.Add(expectedRunId);
        process.StartInfo.ArgumentList.Add(expectedScenarioSha256);
        process.StartInfo.ArgumentList.Add(expectedSiloCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
        process.StartInfo.Environment["DOTNET_HOST_PATH"] = ResolveDotNetHostPath();
        Assert.True(process.Start());
        var standardOutput = process.StandardOutput.ReadToEndAsync();
        var standardError = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        return (
            process.ExitCode,
            $"stdout:{Environment.NewLine}{await standardOutput}{Environment.NewLine}" +
            $"stderr:{Environment.NewLine}{await standardError}");
    }

    private static async Task<(int ExitCode, string Output)> RunSecretValidatorAsync(string outputDirectory)
    {
        var validator = Path.Combine(AppContext.BaseDirectory, "eng", "validate-benchmark-artifacts.sh");
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "bash",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };
        process.StartInfo.ArgumentList.Add(validator);
        process.StartInfo.ArgumentList.Add("secrets");
        process.StartInfo.ArgumentList.Add(outputDirectory);
        Assert.True(process.Start());
        var standardOutput = process.StandardOutput.ReadToEndAsync();
        var standardError = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        return (
            process.ExitCode,
            $"stdout:{Environment.NewLine}{await standardOutput}{Environment.NewLine}" +
            $"stderr:{Environment.NewLine}{await standardError}");
    }

    private static string ResolveDotNetHostPath()
    {
        if (Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") is { Length: > 0 } configured)
        {
            return configured;
        }

        var fileName = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
        var hostPath = Path.GetFullPath(Path.Combine(
            RuntimeEnvironment.GetRuntimeDirectory(),
            "..",
            "..",
            "..",
            fileName));
        Assert.True(File.Exists(hostPath), $"Unable to locate the dotnet host at '{hostPath}'.");
        return hostPath;
    }

    private static void AssertResultSchemaIsStrict(byte[] validResult)
    {
        var root = JsonNode.Parse(validResult)!.AsObject();

        var unknownTopLevel = root.DeepClone().AsObject();
        unknownTopLevel["unexpected"] = true;
        AssertResultSchemaRejects(unknownTopLevel, "unknown top-level property");

        var unknownOperation = root.DeepClone().AsObject();
        unknownOperation["measurement"]!["operations"]!["read"]!["unexpected"] = true;
        AssertResultSchemaRejects(unknownOperation, "unknown operation property");

        var missingOperationKind = root.DeepClone().AsObject();
        _ = missingOperationKind["measurement"]!["operations"]!.AsObject().Remove("clear");
        AssertResultSchemaRejects(missingOperationKind, "missing operation kind");

        var unknownOperationKind = root.DeepClone().AsObject();
        unknownOperationKind["measurement"]!["operations"]!["other"] =
            unknownOperationKind["measurement"]!["operations"]!["read"]!.DeepClone();
        AssertResultSchemaRejects(unknownOperationKind, "unknown operation kind");

        var wrongCounterType = root.DeepClone().AsObject();
        wrongCounterType["measurement"]!["offered"] = "one";
        AssertResultSchemaRejects(wrongCounterType, "wrong counter type");

        var wrongPercentileType = root.DeepClone().AsObject();
        wrongPercentileType["measurement"]!["operations"]!["read"]!["succeededLatencyMicroseconds"]!["p99"] = "slow";
        AssertResultSchemaRejects(wrongPercentileType, "wrong percentile type");

        var missingProvenance = root.DeepClone().AsObject();
        _ = missingProvenance.Remove("provenance");
        AssertResultSchemaRejects(missingProvenance, "missing provenance");

        var unknownComponent = root.DeepClone().AsObject();
        unknownComponent["provenance"]!["components"]!["Unexpected.Component"] = "1.0.0";
        AssertResultSchemaRejects(unknownComponent, "unknown provenance component");

        var incompatibleHistogram = root.DeepClone().AsObject();
        incompatibleHistogram["histogramArtifacts"]![0]!["significantDigits"] = 2;
        AssertResultSchemaRejects(incompatibleHistogram, "incompatible histogram settings");
    }

    private static void AssertResultSchemaAccepts(byte[] json)
    {
        var result = EvaluateResultSchema(json);
        Assert.True(result.IsValid, FormatSchemaFailure("Expected result to be valid", result));
    }

    private static void AssertResultSchemaRejects(JsonNode json, string caseName)
    {
        var result = EvaluateResultSchema(Encoding.UTF8.GetBytes(json.ToJsonString()));
        Assert.False(result.IsValid, $"Result schema accepted {caseName}.");
    }

    private static EvaluationResults EvaluateResultSchema(byte[] json)
    {
        using var document = JsonDocument.Parse(json);
        return ResultSchema.Value.Evaluate(
            document.RootElement,
            new EvaluationOptions { OutputFormat = OutputFormat.Hierarchical });
    }

    private static JsonSchema LoadResultSchema()
    {
        var schemaPath = Path.Combine(AppContext.BaseDirectory, "specs", "schema", "result.v1.schema.json");
        return JsonSchema.FromText(File.ReadAllText(schemaPath), baseUri: new Uri(schemaPath));
    }

    private static string FormatSchemaFailure(string prefix, EvaluationResults result)
    {
        var errors = new List<string>();
        CollectSchemaErrors(result, errors);
        return $"{prefix}:{Environment.NewLine}{string.Join(Environment.NewLine, errors)}";
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

    private static BenchmarkExecution CreateSuccessfulExecution(bool openLoop = false)
    {
        var worker = new WorkerMetrics(recordHistograms: true);
        worker.RecordOffered(OperationKind.Read);
        worker.RecordStarted(OperationKind.Read);
        worker.RecordCompleted(
            OperationKind.Read,
            Stopwatch.Frequency / 1_000,
            queueDelayStopwatchTicks: openLoop ? Stopwatch.Frequency / 2_000 : null,
            succeeded: true,
            resultCount: 1,
            exception: null);
        SchedulerCounters? schedulerCounters = null;
        if (openLoop)
        {
            schedulerCounters = new SchedulerCounters();
            schedulerCounters.RecordOffered(OperationKind.Read);
        }

        var start = Stopwatch.GetTimestamp();
        var measurement = PhaseExecution.Create(
            DateTimeOffset.UtcNow,
            start,
            start + Stopwatch.Frequency,
            start + Stopwatch.Frequency,
            [worker],
            schedulerCounters,
            recordHistograms: true);
        return new BenchmarkExecution(
            Warmup: null,
            new PopulationExecution("population", DateTimeOffset.UtcNow, 0.01, 1_000),
            Restoration: null,
            new CorrectnessAuditExecution(
                DateTimeOffset.UtcNow,
                0.01,
                PointChecks: 1_000,
                ExactQueryChecks: 4,
                RangeQueryChecks: 4,
                PointCoverage: "all-points"),
            measurement,
            new CorrectnessAuditExecution(
                DateTimeOffset.UtcNow,
                0.01,
                PointChecks: 1_000,
                ExactQueryChecks: 4,
                RangeQueryChecks: 4,
                PointCoverage: "all-points"));
    }

    private sealed class UnusedOperationExecutor : IBenchmarkOperationExecutor
    {
        public Task UpsertAsync(long ordinal, long revision) => throw UnexpectedCall();

        public Task<long> ExecuteAsync(OperationInvocation invocation) => throw UnexpectedCall();

        public Task<BenchmarkRecordState?> ReadStateAsync(long ordinal) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> FindKeysAsync(string exactValue) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper) => throw UnexpectedCall();

        private static InvalidOperationException UnexpectedCall()
        {
            return new InvalidOperationException("The result fallback test must not execute workload operations.");
        }
    }
}
