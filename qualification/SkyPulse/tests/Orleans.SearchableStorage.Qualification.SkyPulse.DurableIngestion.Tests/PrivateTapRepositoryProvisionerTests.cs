using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion.Tests;

public sealed class PrivateTapRepositoryProvisionerTests
{
    private const string AdminPassword = "synthetic-admin-secret";
    private const int ProfileVersion = 7;
    private static readonly Guid SourceInstance = Guid.Parse("1618e31b-bca5-4ccd-9fc1-e32bec22cd65");

    [Fact]
    public async Task ReplaysEveryExactDidOnEveryStartupAndProvesCardinality()
    {
        using var fixture = new RouteFixture(
            "did:plc:route-alpha",
            "did:plc:route-bravo",
            "did:plc:route-charlie");
        var requests = new List<IReadOnlyList<string>>();
        var callCount = 0;
        using var provisioner = fixture.CreateProvisioner(async (request, cancellationToken) =>
        {
            callCount++;
            RouteFixture.AssertAuthenticatedContractRequest(request);
            if (request.Method == HttpMethod.Post)
            {
                requests.Add(await ReadDidsAsync(request, cancellationToken));
                return EmptyOk();
            }

            Assert.Equal("/stats/repo-count", request.RequestUri!.AbsolutePath);
            return JsonOk($"{{\"repo_count\":{fixture.Profile.CorpusCap}}}");
        });

        Assert.Equal(
            TapRepositoryProvisionerConfigurationStatus.Configured,
            provisioner.ValidateConfigured(fixture.Profile));
        Assert.Equal(0, callCount);

        Assert.Equal(
            TapRepositoryProvisioningStatus.Provisioned,
            await provisioner.ProvisionAsync(fixture.Profile));
        Assert.Equal(
            TapRepositoryProvisioningStatus.Provisioned,
            await provisioner.ProvisionAsync(fixture.Profile));

        Assert.Equal(6, callCount);
        Assert.Equal(4, requests.Count);
        Assert.Equal(fixture.OrderedDids, requests.Take(2).SelectMany(static value => value));
        Assert.Equal(fixture.OrderedDids, requests.Skip(2).SelectMany(static value => value));
    }

    [Fact]
    public async Task RetriesOneIdenticalBoundedBatchWithoutLeakingResponseBody()
    {
        using var fixture = new RouteFixture("did:plc:retry-private-value");
        var attempts = new List<byte[]>();
        using var provisioner = fixture.CreateProvisioner(async (request, cancellationToken) =>
        {
            RouteFixture.AssertAuthenticatedContractRequest(request);
            if (request.Method == HttpMethod.Get)
            {
                return JsonOk("{\"repo_count\":1}");
            }

            attempts.Add(await request.Content!.ReadAsByteArrayAsync(cancellationToken));
            return attempts.Count == 1
                ? Response(HttpStatusCode.ServiceUnavailable, "did:plc:response-must-not-surface")
                : EmptyOk();
        }, maximumAttempts: 2);

        Assert.Equal(
            TapRepositoryProvisioningStatus.Provisioned,
            await provisioner.ProvisionAsync(fixture.Profile));
        Assert.Equal(2, attempts.Count);
        Assert.Equal(attempts[0], attempts[1]);
    }

    [Fact]
    public async Task CountMismatchFailsSetEqualityAfterIdempotentReplay()
    {
        using var fixture = new RouteFixture("did:plc:count-alpha", "did:plc:count-bravo");
        var posted = 0;
        using var provisioner = fixture.CreateProvisioner(async (request, cancellationToken) =>
        {
            RouteFixture.AssertAuthenticatedContractRequest(request);
            if (request.Method == HttpMethod.Post)
            {
                posted += (await ReadDidsAsync(request, cancellationToken)).Count;
                return EmptyOk();
            }

            return JsonOk("{\"repo_count\":3}");
        });

        Assert.Equal(
            TapRepositoryProvisioningStatus.IdentityMismatch,
            await provisioner.ProvisionAsync(fixture.Profile));
        Assert.Equal(2, posted);
    }

    [Fact]
    public void WrongProfileVersionOrPrefixFailsBeforeAnyNetworkRequest()
    {
        using var fixture = new RouteFixture("did:plc:identity-alpha");
        var calls = 0;
        using var provisioner = fixture.CreateProvisioner((_, _) =>
        {
            calls++;
            return Task.FromResult(EmptyOk());
        });
        var wrongVersion = new TapRepositoryBootstrapProfile(
            fixture.Profile.ProfileId,
            ProfileVersion + 1,
            fixture.Profile.CorpusCap,
            fixture.Profile.ProfilePrefixSha256,
            SourceInstance);
        var wrongPrefix = new TapRepositoryBootstrapProfile(
            fixture.Profile.ProfileId,
            ProfileVersion,
            fixture.Profile.CorpusCap,
            new string('0', 64),
            SourceInstance);

        Assert.Equal(
            TapRepositoryProvisionerConfigurationStatus.IdentityMismatch,
            provisioner.ValidateConfigured(wrongVersion));
        Assert.Equal(
            TapRepositoryProvisionerConfigurationStatus.IdentityMismatch,
            provisioner.ValidateConfigured(wrongPrefix));
        Assert.Equal(0, calls);
    }

    [Fact]
    public void RuntimeTargetCanSelectAnotherExactAbsoluteRoutePath()
    {
        using var fixture = new RouteFixture("did:plc:runtime-route");
        using var provisioner = fixture.CreateProvisioner((_, _) =>
            Task.FromResult(EmptyOk()));

        Assert.Equal(
            TapRepositoryProvisionerConfigurationStatus.Configured,
            provisioner.ValidateConfigured(fixture.Profile, fixture.ManifestPath));
        Assert.Throws<ArgumentException>(() =>
            provisioner.ValidateConfigured(fixture.Profile, "routing.private.manifest.json"));
    }

    [Fact]
    public void RejectsPublicFileModeBeforeTheArtifactVerifierCanRepairIt()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        using var fixture = new RouteFixture("did:plc:mode-alpha");
        File.SetUnixFileMode(
            fixture.RoutePath,
            UnixFileMode.UserRead
                | UnixFileMode.UserWrite
                | UnixFileMode.GroupRead
                | UnixFileMode.OtherRead);
        var calls = 0;
        using var provisioner = fixture.CreateProvisioner((_, _) =>
        {
            calls++;
            return Task.FromResult(EmptyOk());
        });

        Assert.Equal(
            TapRepositoryProvisionerConfigurationStatus.IdentityMismatch,
            provisioner.ValidateConfigured(fixture.Profile));
        Assert.NotEqual(
            UnixFileMode.UserRead | UnixFileMode.UserWrite,
            File.GetUnixFileMode(fixture.RoutePath));
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task OversizedFailureIsBoundedAndSanitized()
    {
        const string privateDid = "did:plc:never-echo-this";
        using var fixture = new RouteFixture(privateDid);
        using var provisioner = fixture.CreateProvisioner((request, _) =>
        {
            RouteFixture.AssertAuthenticatedContractRequest(request);
            return Task.FromResult(Response(
                HttpStatusCode.InternalServerError,
                new string('x', 200) + privateDid + AdminPassword));
        }, maximumAttempts: 1, maximumResponseBytes: 128);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(
            () => provisioner.ProvisionAsync(fixture.Profile));

        Assert.DoesNotContain(privateDid, exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(AdminPassword, exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("127.0.0.1", exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ConstructorSnapshotsSecurityRelevantOptions()
    {
        using var fixture = new RouteFixture("did:plc:snapshot-alpha");
        var options = fixture.CreateOptions();
        var expectedAuthorization = BasicAuthorization(AdminPassword);
        var handler = new DelegateHandler((request, _) =>
        {
            Assert.Equal(expectedAuthorization, request.Headers.Authorization?.ToString());
            return Task.FromResult(request.Method == HttpMethod.Post
                ? EmptyOk()
                : JsonOk("{\"repo_count\":1}"));
        });
        using var provisioner = new PrivateTapRepositoryProvisioner(options, handler);
        options.AdminPassword = "mutated-secret";
        options.RoutingManifestPath = Path.Combine(Path.GetTempPath(), "missing-route.json");
        options.BatchSize = 1_000;

        Assert.Equal(
            TapRepositoryProvisioningStatus.Provisioned,
            await provisioner.ProvisionAsync(fixture.Profile));
    }

    private static async Task<IReadOnlyList<string>> ReadDidsAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        Assert.Equal("/repos/add", request.RequestUri!.AbsolutePath);
        Assert.NotNull(request.Content);
        using var document = JsonDocument.Parse(
            await request.Content.ReadAsByteArrayAsync(cancellationToken));
        var properties = document.RootElement.EnumerateObject().ToArray();
        Assert.Single(properties);
        Assert.Equal("dids", properties[0].Name);
        return properties[0].Value
            .EnumerateArray()
            .Select(static value => value.GetString()!)
            .ToArray();
    }

    private static HttpResponseMessage EmptyOk()
        => new(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent([]),
        };

    private static HttpResponseMessage JsonOk(string body)
        => Response(HttpStatusCode.OK, body);

    private static HttpResponseMessage Response(HttpStatusCode statusCode, string body)
        => new(statusCode)
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body)),
        };

    private static string BasicAuthorization(string password)
        => "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"admin:{password}"));

    private sealed class RouteFixture : IDisposable
    {
        private static readonly JsonSerializerOptions CanonicalJson = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
            NumberHandling = JsonNumberHandling.Strict,
            RespectNullableAnnotations = true,
            WriteIndented = false,
        };

        internal RouteFixture(params string[] dids)
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"skypulse-tap-provisioner-{Guid.NewGuid():N}");
            if (OperatingSystem.IsWindows())
            {
                Directory.CreateDirectory(DirectoryPath);
            }
            else
            {
                Directory.CreateDirectory(
                    DirectoryPath,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            }
            OrderedDids = dids
                .Select(static did => (Did: did, Key: AccountKey.FromDid(did).ToString()))
                .OrderBy(static value => value.Key, StringComparer.Ordinal)
                .Select(static value => value.Did)
                .ToArray();
            var entries = OrderedDids
                .Select(static (did, index) => new RouteEntry(
                    index + 1L,
                    AccountKey.FromDid(did).ToString(),
                    did))
                .ToArray();
            var routeBytes = EncodeRoute(entries);
            var batches = BuildBatches(routeBytes, entries, batchRecordLimit: 2);
            var projectionBytes = entries
                .SelectMany(static entry => Convert.FromHexString(entry.AccountKey))
                .ToArray();
            var prefixSha = LowerHex(SHA256.HashData(projectionBytes));
            var manifest = new PrivateRoutingManifest(
                AcquisitionContract.RoutingManifestFormat,
                DummySha('1'),
                new PrivateArtifactEvidence(1, DummySha('2')),
                DummySha('3'),
                DummySha('4'),
                DummySha('5'),
                new RoutingProfileBinding("qualification", entries.LongLength, entries.LongLength * 32, prefixSha),
                2,
                batches,
                new RoutingArtifactEvidence(
                    AcquisitionContract.RoutingFileName,
                    AcquisitionContract.RoutingArtifactFormat,
                    entries.LongLength,
                    routeBytes.LongLength,
                    LowerHex(SHA256.HashData(routeBytes)),
                    prefixSha));

            RoutePath = Path.Combine(DirectoryPath, AcquisitionContract.RoutingFileName);
            ManifestPath = Path.Combine(DirectoryPath, AcquisitionContract.RoutingManifestFileName);
            WritePrivate(RoutePath, routeBytes);
            var serialized = JsonSerializer.SerializeToUtf8Bytes(manifest, CanonicalJson);
            WritePrivate(ManifestPath, [.. serialized, (byte)'\n']);
            Profile = new TapRepositoryBootstrapProfile(
                "qualification",
                ProfileVersion,
                entries.LongLength,
                prefixSha,
                SourceInstance);
        }

        internal string DirectoryPath { get; }

        internal string ManifestPath { get; }

        internal string RoutePath { get; }

        internal IReadOnlyList<string> OrderedDids { get; }

        internal TapRepositoryBootstrapProfile Profile { get; }

        internal PrivateTapRepositoryProvisioner CreateProvisioner(
            Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send,
            int maximumAttempts = 1,
            int maximumResponseBytes = 16 * 1024)
            => new(CreateOptions(maximumAttempts, maximumResponseBytes), new DelegateHandler(send));

        internal PrivateTapRepositoryProvisionerOptions CreateOptions(
            int maximumAttempts = 1,
            int maximumResponseBytes = 16 * 1024)
            => new()
            {
                RoutingManifestPath = ManifestPath,
                TapWebSocketEndpoint = new Uri("ws://127.0.0.1:2480/channel"),
                AdminPassword = AdminPassword,
                ExpectedProfileVersion = ProfileVersion,
                ExclusiveRepositoryAdministrationConfirmed = true,
                FullNetworkModeDisabledConfirmed = true,
                AutomaticRepositoryDiscoveryDisabledConfirmed = true,
                BatchSize = 2,
                MaximumAttempts = maximumAttempts,
                RetryBaseDelay = TimeSpan.Zero,
                MaximumResponseBytes = maximumResponseBytes,
            };

        internal static void AssertAuthenticatedContractRequest(HttpRequestMessage request)
        {
            Assert.Equal("http", request.RequestUri!.Scheme);
            Assert.Equal("127.0.0.1", request.RequestUri.Host);
            Assert.Equal(BasicAuthorization(AdminPassword), request.Headers.Authorization?.ToString());
            Assert.DoesNotContain(AdminPassword, request.RequestUri.ToString(), StringComparison.Ordinal);
        }

        public void Dispose()
        {
            if (Directory.Exists(DirectoryPath))
            {
                Directory.Delete(DirectoryPath, recursive: true);
            }
        }

        private static byte[] EncodeRoute(IReadOnlyList<RouteEntry> entries)
        {
            using var stream = new MemoryStream();
            foreach (var entry in entries)
            {
                var line = Encoding.UTF8.GetBytes(
                    $"{{\"ordinal\":{entry.Ordinal},\"accountKey\":\"{entry.AccountKey}\",\"did\":\"{entry.Did}\"}}\n");
                stream.Write(line);
            }

            return stream.ToArray();
        }

        private static List<RoutingBatchEvidence> BuildBatches(
            byte[] route,
            IReadOnlyList<RouteEntry> entries,
            int batchRecordLimit)
        {
            var result = new List<RoutingBatchEvidence>();
            var offset = 0;
            for (var index = 0; index < entries.Count; index += batchRecordLimit)
            {
                var recordCount = Math.Min(batchRecordLimit, entries.Count - index);
                var end = offset;
                for (var record = 0; record < recordCount; record++)
                {
                    end = Array.IndexOf(route, (byte)'\n', end) + 1;
                }

                var bytes = route.AsSpan(offset, end - offset);
                result.Add(new RoutingBatchEvidence(
                    result.Count + 1,
                    index + 1L,
                    recordCount,
                    offset,
                    bytes.Length,
                    LowerHex(SHA256.HashData(bytes))));
                offset = end;
            }

            return result;
        }

        private static void WritePrivate(string path, byte[] bytes)
        {
            File.WriteAllBytes(path, bytes);
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
        }

        private static string DummySha(char value) => new(value, 64);

        private static string LowerHex(ReadOnlySpan<byte> value)
            => Convert.ToHexString(value).ToLowerInvariant();
    }

    private sealed record RouteEntry(long Ordinal, string AccountKey, string Did);

    private sealed class DelegateHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
            => send(request, cancellationToken);
    }
}
