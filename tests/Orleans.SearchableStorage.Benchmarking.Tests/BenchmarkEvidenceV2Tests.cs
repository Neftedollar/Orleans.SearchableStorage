using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class BenchmarkEvidenceV2Tests
{
    [Fact]
    public async Task CheckedInContractFixturesPassTheVersionTwoSemanticValidator()
    {
        var versionTwoRoot = Path.Combine(AppContext.BaseDirectory, "specs", "v2");
        var results = Directory
            .EnumerateFiles(versionTwoRoot, "*.result.v2.json")
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(2, results.Length);
        foreach (var result in results)
        {
            await BenchmarkArtifactValidator.ValidateAsync(result, CancellationToken.None);
        }
    }

    [Theory]
    [InlineData("unknown-result-member")]
    [InlineData("schema-fixture-qualified")]
    [InlineData("schema-fixture-scale-claim")]
    [InlineData("profile-digest-drift")]
    [InlineData("raw-artifact-digest-drift")]
    [InlineData("raw-artifact-traversal")]
    [InlineData("threshold-pass-tamper")]
    [InlineData("threshold-observation-tamper")]
    [InlineData("fanout-ceiling")]
    [InlineData("copied-workload")]
    [InlineData("error-rate")]
    [InlineData("latency-order")]
    [InlineData("memory-physical-zero-substitution")]
    [InlineData("logical-call-drift")]
    [InlineData("provider-metric-missing")]
    [InlineData("provider-metric-arbitrary")]
    [InlineData("provider-metric-unit")]
    [InlineData("provider-amplification")]
    [InlineData("provider-unavailable-as-zero")]
    [InlineData("profile-threshold-unit")]
    [InlineData("profile-status")]
    [InlineData("profile-access-path")]
    [InlineData("plain-secondary-index-surface")]
    [InlineData("duplicate-profile-case")]
    [InlineData("qualification-zero-commit")]
    [InlineData("qualification-rehashed-external-scale-shape")]
    [InlineData("scale-claim-small-record-count")]
    [InlineData("scale-claim-single-silo")]
    [InlineData("provider-call-zero")]
    public async Task VersionTwoValidatorRejectsSchemaAndSemanticCorruption(string corruption)
    {
        using var fixture = FixtureCopy.Create();
        var providerCase = corruption.StartsWith("provider-", StringComparison.Ordinal);
        var resultPath = Path.Combine(
            fixture.Root,
            providerCase
                ? "contract-smoke.postgresql.result.v2.json"
                : "contract-smoke.memory.result.v2.json");
        var result = JsonNode.Parse(await File.ReadAllTextAsync(resultPath))!.AsObject();

        switch (corruption)
        {
            case "unknown-result-member":
                result["unexpected"] = true;
                break;
            case "schema-fixture-qualified":
                result["qualified"] = true;
                break;
            case "schema-fixture-scale-claim":
                result["scaleClaim"] = true;
                break;
            case "profile-digest-drift":
                result["profile"]!["sha256"] = new string('0', 64);
                break;
            case "raw-artifact-digest-drift":
                result["run"]!["rawEvidenceArtifacts"]![0]!["sha256"] = new string('0', 64);
                break;
            case "raw-artifact-traversal":
                result["run"]!["rawEvidenceArtifacts"]![0]!["path"] = "../outside.json";
                break;
            case "threshold-pass-tamper":
                result["thresholdEvaluations"]![0]!["passed"] = false;
                break;
            case "threshold-observation-tamper":
                result["thresholdEvaluations"]![0]!["observedValue"] = 999;
                break;
            case "fanout-ceiling":
                result["cases"]![0]!["observedFanout"]!["physicalOwners"] = 2;
                break;
            case "copied-workload":
                result["cases"]![0]!["workload"] = "point-write";
                break;
            case "error-rate":
                FindMetric(result, "point-read-hot", "steady-state", "error-rate")["value"] = 0.5;
                break;
            case "latency-order":
                FindMetric(result, "point-read-hot", "steady-state", "latency-p50")["value"] = 5;
                break;
            case "memory-physical-zero-substitution":
                result["cases"]![0]!["byteEvidence"]!["providerNativePhysicalBytes"] = new JsonObject
                {
                    ["availability"] = "observed",
                    ["valueBytes"] = 0,
                };
                break;
            case "logical-call-drift":
                result["cases"]![0]!["logicalOrleansCalls"]!["read"] = 9;
                break;
            case "provider-metric-missing":
                result["cases"]![0]!["providerNativeTelemetry"]!["observations"]!.AsArray().RemoveAt(0);
                break;
            case "provider-metric-arbitrary":
                result["cases"]![0]!["providerNativeTelemetry"]!["observations"]![0]!["metric"] = "sql-round-trips";
                break;
            case "provider-metric-unit":
                result["cases"]![0]!["providerNativeTelemetry"]!["observations"]![0]!["unit"] = "bytes";
                break;
            case "provider-amplification":
                FindMetric(
                    result,
                    "point-write-provider-evidence",
                    "steady-state",
                    "provider-write-call-amplification")["value"] = 1;
                break;
            case "provider-unavailable-as-zero":
                result["cases"]![0]!["providerNativeTelemetry"] = new JsonObject
                {
                    ["availability"] = "observed",
                    ["observations"] = new JsonArray(),
                };
                break;
            case "profile-threshold-unit":
                MutateProfile(fixture.Root, result, profile =>
                    profile["thresholds"]![0]!["unit"] = "bytes");
                break;
            case "profile-status":
                MutateProfile(fixture.Root, result, profile => profile["status"] = "frozen");
                break;
            case "profile-access-path":
                result["cases"]![0]!["accessPath"] = "hash-posting";
                MutateProfile(fixture.Root, result, profile =>
                    profile["cases"]![0]!["accessPath"] = "hash-posting");
                break;
            case "plain-secondary-index-surface":
                result["run"]!["implementationPath"] = "plain";
                MutateProfile(fixture.Root, result, profile => profile["implementationPath"] = "plain");
                break;
            case "duplicate-profile-case":
                MutateProfile(fixture.Root, result, profile =>
                {
                    var cases = profile["cases"]!.AsArray();
                    cases.Add(cases[0]!.DeepClone());
                });
                break;
            case "qualification-zero-commit":
                PromoteToQualification(fixture.Root, result);
                break;
            case "qualification-rehashed-external-scale-shape":
                PromoteToQualification(fixture.Root, result);
                result["run"]!["gitCommit"] = "0123456789abcdef0123456789abcdef01234567";
                result["run"]!["recordCount"] = 10_000_000;
                result["run"]!["siloCount"] = 2;
                result["run"]!["clientCount"] = 2;
                result["scaleClaim"] = true;
                break;
            case "scale-claim-small-record-count":
                PromoteToQualification(fixture.Root, result);
                result["run"]!["gitCommit"] = "0123456789abcdef0123456789abcdef01234567";
                result["scaleClaim"] = true;
                result["run"]!["siloCount"] = 2;
                break;
            case "scale-claim-single-silo":
                PromoteToQualification(fixture.Root, result);
                result["run"]!["gitCommit"] = "0123456789abcdef0123456789abcdef01234567";
                result["scaleClaim"] = true;
                result["run"]!["recordCount"] = 10_000_000;
                break;
            case "provider-call-zero":
                result["cases"]![0]!["providerNativeTelemetry"]!["observations"]![1]!["value"] = 0;
                FindMetric(
                    result,
                    "point-write-provider-evidence",
                    "steady-state",
                    "provider-write-call-amplification")["value"] = 0;
                result["thresholdEvaluations"]![0]!["observedValue"] = 0;
                break;
            default:
                throw new UnreachableException(corruption);
        }

        await File.WriteAllTextAsync(resultPath, result.ToJsonString(IndentedJson));
        await Assert.ThrowsAsync<InvalidDataException>(
            () => BenchmarkArtifactValidator.ValidateAsync(resultPath, CancellationToken.None));
    }

    [Fact]
    public async Task VersionTwoValidatorRejectsRawArtifactContentDrift()
    {
        using var fixture = FixtureCopy.Create();
        var resultPath = Path.Combine(fixture.Root, "contract-smoke.memory.result.v2.json");
        var rawPath = Path.Combine(fixture.Root, "results", "evidence", "contract-smoke.memory.raw.json");
        await File.AppendAllTextAsync(rawPath, Environment.NewLine);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => BenchmarkArtifactValidator.ValidateAsync(resultPath, CancellationToken.None));
    }

    [Fact]
    public async Task VersionTwoValidatorRejectsOversizedProfileBeforeReadingIt()
    {
        using var fixture = FixtureCopy.Create();
        var resultPath = Path.Combine(fixture.Root, "contract-smoke.memory.result.v2.json");
        var result = JsonNode.Parse(await File.ReadAllTextAsync(resultPath))!.AsObject();
        var profilePath = Path.Combine(fixture.Root, result["profile"]!["path"]!.GetValue<string>());
        await using (var stream = new FileStream(profilePath, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            stream.SetLength(BenchmarkEvidenceV2Validator.MaximumProfileBytes + 1L);
        }

        await Assert.ThrowsAsync<InvalidDataException>(
            () => BenchmarkArtifactValidator.ValidateAsync(resultPath, CancellationToken.None));
    }

    [Fact]
    public async Task VersionTwoValidatorRejectsOversizedRawArtifactBeforeHashingIt()
    {
        using var fixture = FixtureCopy.Create();
        var resultPath = Path.Combine(fixture.Root, "contract-smoke.memory.result.v2.json");
        var rawPath = Path.Combine(fixture.Root, "results", "evidence", "contract-smoke.memory.raw.json");
        await using (var stream = new FileStream(rawPath, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            stream.SetLength(BenchmarkEvidenceV2Validator.MaximumRawArtifactBytes + 1L);
        }

        await Assert.ThrowsAsync<InvalidDataException>(
            () => BenchmarkArtifactValidator.ValidateAsync(resultPath, CancellationToken.None));
    }

    [Fact]
    public async Task VersionTwoValidatorRejectsNestedRawArtifactSymlink()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var fixture = FixtureCopy.Create();
        var resultPath = Path.Combine(fixture.Root, "contract-smoke.memory.result.v2.json");
        var evidenceDirectory = Path.Combine(fixture.Root, "results", "evidence");
        var targetDirectory = Path.Combine(fixture.Root, "actual-evidence");
        Directory.Move(evidenceDirectory, targetDirectory);
        try
        {
            Directory.CreateSymbolicLink(evidenceDirectory, targetDirectory);
        }
        catch (UnauthorizedAccessException)
        {
            return;
        }

        await Assert.ThrowsAsync<InvalidDataException>(
            () => BenchmarkArtifactValidator.ValidateAsync(resultPath, CancellationToken.None));
    }

    [Fact]
    public async Task VersionDispatchFindsLateVersionAfterWhitespaceAndLongRootString()
    {
        var json = new string(' ', 5_000) +
            "{\"noise\":\"" + new string('x', 70_000) +
            "\",\"schemaVersion\":\"oss-benchmark-result/v2\"}";
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        Assert.True(await BenchmarkArtifactValidator.IsVersionTwoForTestsAsync(
            stream,
            CancellationToken.None));
        Assert.Throws<InvalidDataException>(() => BenchmarkArtifactValidator.ValidateVersionTwoLength(
            isVersionTwo: true,
            BenchmarkEvidenceV2Validator.MaximumResultBytes + 1L));
    }

    [Fact]
    public async Task VersionDispatchCarriesPropertyAndValueAcrossReadBoundary()
    {
        const int bufferSize = 64 * 1024;
        const string property = "\"schemaVersion\"";
        var json = "{" + new string(' ', bufferSize - 1 - property.Length) + property +
            ":\"oss-benchmark-result/v2\"}";
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        Assert.True(await BenchmarkArtifactValidator.IsVersionTwoForTestsAsync(
            stream,
            CancellationToken.None));
    }

    [Theory]
    [InlineData(
        "{\"schemaVersion\":\"oss-benchmark-result/v1\",\"schema\\u0056ersion\":\"oss-benchmark-result/v1\"}",
        false)]
    [InlineData(
        "{\"schemaVersion\":\"oss-benchmark-result/v2\",\"schema\\u0056ersion\":\"oss-benchmark-result/v1\"}",
        false)]
    [InlineData(
        "{\"schemaVersion\":\"oss-benchmark-result/v1\",\"schema\\u0056ersion\":\"oss-benchmark-result/v2\"}",
        true)]
    public async Task VersionDispatchUsesTheLastSemanticSchemaVersionLikeJsonDocument(
        string json,
        bool expectedVersionTwo)
    {
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        Assert.Equal(
            expectedVersionTwo,
            await BenchmarkArtifactValidator.IsVersionTwoForTestsAsync(stream, CancellationToken.None));
        if (expectedVersionTwo)
        {
            Assert.Throws<InvalidDataException>(() => BenchmarkArtifactValidator.ValidateVersionTwoLength(
                isVersionTwo: true,
                BenchmarkEvidenceV2Validator.MaximumResultBytes + 1L));
        }
        else
        {
            BenchmarkArtifactValidator.ValidateVersionTwoLength(
                isVersionTwo: false,
                BenchmarkEvidenceV2Validator.MaximumResultBytes + 1L);
        }
    }

    [Fact]
    public async Task VersionDispatchDoesNotApplyVersionTwoCapToLargeVersionOneString()
    {
        var json = "{\"note\":\"oss-benchmark-result/v2" + new string('x', 70_000) +
            "\",\"schemaVersion\":\"oss-benchmark-result/v1\"}";
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

        Assert.False(await BenchmarkArtifactValidator.IsVersionTwoForTestsAsync(
            stream,
            CancellationToken.None));
        BenchmarkArtifactValidator.ValidateVersionTwoLength(
            isVersionTwo: false,
            BenchmarkEvidenceV2Validator.MaximumResultBytes + 1L);
    }

    [Fact]
    public async Task VersionDispatchRejectsOversizedVersionTwoWithLateVersionBeforeAllocation()
    {
        var resultPath = Path.Combine(
            Path.GetTempPath(),
            $"oss-oversized-v2-{Guid.NewGuid():N}.json");
        try
        {
            await using (var stream = new FileStream(
                             resultPath,
                             FileMode.CreateNew,
                             FileAccess.Write,
                             FileShare.None,
                             bufferSize: 64 * 1024,
                             useAsync: true))
            {
                await stream.WriteAsync("{\"noise\":\""u8.ToArray());
                var block = new byte[64 * 1024];
                Array.Fill(block, (byte)'x');
                var remaining = BenchmarkEvidenceV2Validator.MaximumResultBytes;
                while (remaining > 0)
                {
                    var count = Math.Min(block.Length, remaining);
                    await stream.WriteAsync(block.AsMemory(0, count));
                    remaining -= count;
                }

                await stream.WriteAsync(
                    "\",\"schemaVersion\":\"oss-benchmark-result/v2\"}"u8.ToArray());
            }

            await Assert.ThrowsAsync<InvalidDataException>(() =>
                BenchmarkArtifactValidator.ValidateAsync(resultPath, CancellationToken.None));
        }
        finally
        {
            File.Delete(resultPath);
        }
    }

    private static readonly JsonSerializerOptions IndentedJson = new() { WriteIndented = true };

    private static JsonObject FindMetric(
        JsonObject result,
        string caseId,
        string phaseName,
        string metricName)
    {
        var resultCase = result["cases"]!.AsArray()
            .Select(static node => node!.AsObject())
            .Single(node => node["id"]!.GetValue<string>() == caseId);
        var phase = resultCase["phases"]!.AsArray()
            .Select(static node => node!.AsObject())
            .Single(node => node["phase"]!.GetValue<string>() == phaseName);
        return phase["metrics"]!.AsArray()
            .Select(static node => node!.AsObject())
            .Single(node => node["metric"]!.GetValue<string>() == metricName);
    }

    private static void MutateProfile(
        string root,
        JsonObject result,
        Action<JsonObject> mutation)
    {
        var profileReference = result["profile"]!.AsObject();
        var profilePath = Path.Combine(root, profileReference["path"]!.GetValue<string>());
        var profile = JsonNode.Parse(File.ReadAllText(profilePath))!.AsObject();
        mutation(profile);
        var bytes = Encoding.UTF8.GetBytes(profile.ToJsonString(IndentedJson));
        File.WriteAllBytes(profilePath, bytes);
        profileReference["sha256"] = Convert.ToHexStringLower(SHA256.HashData(bytes));
    }

    private static void PromoteToQualification(string root, JsonObject result)
    {
        result["classification"] = "qualification";
        result["qualified"] = true;
        result["run"]!["topology"] = "external";
        MutateProfile(root, result, profile =>
        {
            profile["classification"] = "qualification";
            profile["status"] = "frozen";
            profile["topology"] = "external";
        });
    }

    private sealed class FixtureCopy(string root) : IDisposable
    {
        public string Root { get; } = root;

        public static FixtureCopy Create()
        {
            var source = Path.Combine(AppContext.BaseDirectory, "specs", "v2");
            var destination = Path.Combine(Path.GetTempPath(), $"oss-benchmark-v2-{Guid.NewGuid():N}");
            CopyDirectory(source, destination);
            return new FixtureCopy(destination);
        }

        public void Dispose()
        {
            if (Directory.Exists(Root))
            {
                Directory.Delete(Root, recursive: true);
            }
        }

        private static void CopyDirectory(string source, string destination)
        {
            Directory.CreateDirectory(destination);
            foreach (var directory in Directory.EnumerateDirectories(source, "*", SearchOption.AllDirectories))
            {
                Directory.CreateDirectory(Path.Combine(destination, Path.GetRelativePath(source, directory)));
            }

            foreach (var file in Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories))
            {
                File.Copy(file, Path.Combine(destination, Path.GetRelativePath(source, file)));
            }
        }
    }
}
