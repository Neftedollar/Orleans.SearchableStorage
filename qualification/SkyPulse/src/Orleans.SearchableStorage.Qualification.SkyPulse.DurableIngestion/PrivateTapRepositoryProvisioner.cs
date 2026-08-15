using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

/// <summary>
/// Defines the private, bounded proof needed to install one exact frozen repository set in TAP.
/// </summary>
public sealed class PrivateTapRepositoryProvisionerOptions
{
    public const int MaximumTapBatchSize = 1_000;
    public const int MaximumRetryAttempts = 8;
    public const int ReviewedMaximumRouteLineBytes = 16 * 1024;

    public string RoutingManifestPath { get; set; } = string.Empty;

    public Uri? TapWebSocketEndpoint { get; set; }

    public string AdminPassword { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the independently configured durable profile version expected by this route.
    /// </summary>
    public int ExpectedProfileVersion { get; set; }

    /// <summary>
    /// Gets or sets the operator assertion that no other principal can add or remove TAP repos.
    /// </summary>
    public bool ExclusiveRepositoryAdministrationConfirmed { get; set; }

    /// <summary>
    /// Gets or sets the operator assertion that TAP full-network mode is disabled.
    /// </summary>
    public bool FullNetworkModeDisabledConfirmed { get; set; }

    /// <summary>
    /// Gets or sets the operator assertion that every automatic repository-discovery path is off.
    /// </summary>
    public bool AutomaticRepositoryDiscoveryDisabledConfirmed { get; set; }

    public int BatchSize { get; set; } = 500;

    public int MaximumAttempts { get; set; } = 3;

    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    public int MaximumResponseBytes { get; set; } = 16 * 1024;

    public int MaximumRequestBodyBytes { get; set; } = 4 * 1024 * 1024;

    public long MaximumManifestBytes { get; set; } = 64L * 1024 * 1024;

    public long MaximumRoutingArtifactBytes { get; set; } = 64L * 1024 * 1024 * 1024;

    public int MaximumRouteLineBytes { get; set; } = ReviewedMaximumRouteLineBytes;

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(RoutingManifestPath)
            || RoutingManifestPath.Length > 4_096
            || RoutingManifestPath.IndexOfAny(['\r', '\n']) >= 0
            || !Path.IsPathFullyQualified(RoutingManifestPath)
            || !string.Equals(
                Path.GetFileName(RoutingManifestPath),
                AcquisitionContract.RoutingManifestFileName,
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "The private routing manifest must be an absolute bounded path to routing.private.manifest.json.");
        }

        if (TapWebSocketEndpoint is null
            || !TapWebSocketEndpoint.IsAbsoluteUri
            || TapWebSocketEndpoint.Scheme is not ("ws" or "wss")
            || !string.Equals(TapWebSocketEndpoint.AbsolutePath, "/channel", StringComparison.Ordinal)
            || !string.IsNullOrEmpty(TapWebSocketEndpoint.Query)
            || !string.IsNullOrEmpty(TapWebSocketEndpoint.Fragment)
            || !string.IsNullOrEmpty(TapWebSocketEndpoint.UserInfo))
        {
            throw new InvalidOperationException(
                "The TAP WebSocket endpoint must be an absolute ws/wss /channel URI without query, fragment, or user information.");
        }

        if (TapWebSocketEndpoint.Scheme == "ws" && !TapWebSocketEndpoint.IsLoopback)
        {
            throw new InvalidOperationException("Unencrypted TAP administration is allowed only on loopback.");
        }

        if (string.IsNullOrWhiteSpace(AdminPassword)
            || AdminPassword.Length > 4_096
            || AdminPassword.IndexOfAny(['\r', '\n']) >= 0)
        {
            throw new InvalidOperationException(
                "A non-empty bounded TAP admin password without line breaks is required.");
        }

        if (ExpectedProfileVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ExpectedProfileVersion),
                ExpectedProfileVersion,
                "The expected durable profile version must be positive.");
        }

        if (!ExclusiveRepositoryAdministrationConfirmed
            || !FullNetworkModeDisabledConfirmed
            || !AutomaticRepositoryDiscoveryDisabledConfirmed)
        {
            throw new InvalidOperationException(
                "Exact repository-set proof requires exclusive administration with full-network and automatic discovery disabled.");
        }

        if (BatchSize is < 1 or > MaximumTapBatchSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(BatchSize),
                BatchSize,
                $"A TAP repository batch must contain between 1 and {MaximumTapBatchSize} DIDs.");
        }

        if (MaximumAttempts is < 1 or > MaximumRetryAttempts)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaximumAttempts),
                MaximumAttempts,
                $"A TAP request must use between 1 and {MaximumRetryAttempts} attempts.");
        }

        if (RequestTimeout < TimeSpan.FromSeconds(1) || RequestTimeout > TimeSpan.FromMinutes(5))
        {
            throw new ArgumentOutOfRangeException(
                nameof(RequestTimeout),
                RequestTimeout,
                "The TAP request timeout must be between one second and five minutes.");
        }

        if (RetryBaseDelay < TimeSpan.Zero || RetryBaseDelay > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(
                nameof(RetryBaseDelay),
                RetryBaseDelay,
                "The TAP retry base delay must be between zero and thirty seconds.");
        }

        if (MaximumResponseBytes is < 128 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaximumResponseBytes),
                MaximumResponseBytes,
                "The TAP response bound must be between 128 bytes and one MiB.");
        }

        if (MaximumRequestBodyBytes is < 1024 or > 64 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaximumRequestBodyBytes),
                MaximumRequestBodyBytes,
                "The TAP request-body bound must be between one KiB and sixty-four MiB.");
        }

        if (MaximumManifestBytes is < 1024 or > 1024L * 1024 * 1024
            || MaximumRoutingArtifactBytes < 1024
            || MaximumRoutingArtifactBytes > 1024L * 1024 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaximumManifestBytes),
                "Private routing artifact byte bounds are invalid.");
        }

        if (MaximumRouteLineBytes is < 256 or > ReviewedMaximumRouteLineBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaximumRouteLineBytes),
                MaximumRouteLineBytes,
                $"A route line must be bounded by at most {ReviewedMaximumRouteLineBytes} bytes.");
        }
    }
}

/// <summary>
/// Replays the exact verified private DID route through TAP's idempotent admin API and proves
/// set equality by authenticated cardinality under the frozen exclusive-administration invariant.
/// </summary>
public sealed class PrivateTapRepositoryProvisioner : ITapRepositoryProvisioner, IDisposable
{
    private static readonly byte[] LineFeed = [(byte)'\n'];
    private readonly PrivateTapRepositoryProvisionerOptions _options;
    private readonly HttpClient _httpClient;
    private readonly Uri _addRepositoriesUri;
    private readonly Uri _repositoryCountUri;
    private readonly AuthenticationHeaderValue _authorization;
    private bool _disposed;

    public PrivateTapRepositoryProvisioner(PrivateTapRepositoryProvisionerOptions options)
        : this(options, CreateHandler())
    {
    }

    internal PrivateTapRepositoryProvisioner(
        PrivateTapRepositoryProvisionerOptions options,
        HttpMessageHandler handler)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler);
        _options = Snapshot(options);
        _options.Validate();
        _httpClient = new HttpClient(handler, disposeHandler: true)
        {
            Timeout = Timeout.InfiniteTimeSpan,
        };
        _addRepositoriesUri = DeriveAdminUri(_options.TapWebSocketEndpoint!, "/repos/add");
        _repositoryCountUri = DeriveAdminUri(_options.TapWebSocketEndpoint!, "/stats/repo-count");
        var passwordByteCount = Encoding.UTF8.GetByteCount(_options.AdminPassword);
        var credentialBytes = new byte["admin:"u8.Length + passwordByteCount];
        "admin:"u8.CopyTo(credentialBytes);
        _ = Encoding.UTF8.GetBytes(
            _options.AdminPassword.AsSpan(),
            credentialBytes.AsSpan("admin:"u8.Length));
        try
        {
            _authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(credentialBytes));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(credentialBytes);
        }
    }

    public TapRepositoryProvisionerConfigurationStatus ValidateConfigured(
        TapRepositoryBootstrapProfile profile)
        => ValidateConfigured(profile, _options.RoutingManifestPath);

    public TapRepositoryProvisionerConfigurationStatus ValidateConfigured(
        TapRepositoryBootstrapProfile profile,
        string routingManifestPath)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ObjectDisposedException.ThrowIf(_disposed, this);
        ValidateRoutingManifestPath(routingManifestPath);
        if (!File.Exists(routingManifestPath))
        {
            return TapRepositoryProvisionerConfigurationStatus.Missing;
        }

        try
        {
            using var route = VerifiedPrivateRoute.Open(_options, profile, routingManifestPath);
            return TapRepositoryProvisionerConfigurationStatus.Configured;
        }
        catch (Exception exception) when (IsSanitizedRouteFailure(exception))
        {
            return TapRepositoryProvisionerConfigurationStatus.IdentityMismatch;
        }
    }

    public async Task<TapRepositoryProvisioningStatus> ProvisionAsync(
        TapRepositoryBootstrapProfile profile,
        CancellationToken cancellationToken = default)
        => await ProvisionAsync(
            profile,
            _options.RoutingManifestPath,
            cancellationToken).ConfigureAwait(false);

    public async Task<TapRepositoryProvisioningStatus> ProvisionAsync(
        TapRepositoryBootstrapProfile profile,
        string routingManifestPath,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ObjectDisposedException.ThrowIf(_disposed, this);
        ValidateRoutingManifestPath(routingManifestPath);
        cancellationToken.ThrowIfCancellationRequested();

        VerifiedPrivateRoute route;
        try
        {
            route = VerifiedPrivateRoute.Open(_options, profile, routingManifestPath);
        }
        catch (Exception exception) when (IsSanitizedRouteFailure(exception))
        {
            return TapRepositoryProvisioningStatus.IdentityMismatch;
        }

        using (route)
        {
            using var verifier = new RoutePassVerifier(
                route.Manifest,
                profile,
                _options.MaximumRouteLineBytes);
            var reader = new BoundedNdjsonReader(route.Stream, _options.MaximumRouteLineBytes);
            foreach (var evidence in route.Manifest.Batches)
            {
                var verifiedDids = new List<string>(evidence.RecordCount);
                using var batchHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                var batchByteLength = 0L;
                for (var record = 0; record < evidence.RecordCount; record++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (!reader.TryRead(out var line))
                    {
                        throw new InvalidDataException(
                            "The private route ended before its verified batch boundary.");
                    }

                    verifiedDids.Add(verifier.Accept(line.Span));
                    batchHash.AppendData(line.Span);
                    batchHash.AppendData(LineFeed);
                    batchByteLength = checked(batchByteLength + line.Length + 1L);
                }

                if (batchByteLength != evidence.ByteLength
                    || !string.Equals(
                        LowerHex(batchHash.GetHashAndReset()),
                        evidence.Sha256,
                        StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        "A private route batch changed after initial verification.");
                }

                for (var offset = 0; offset < verifiedDids.Count; offset += _options.BatchSize)
                {
                    var count = Math.Min(_options.BatchSize, verifiedDids.Count - offset);
                    await AddBatchAsync(
                        verifiedDids.GetRange(offset, count),
                        cancellationToken).ConfigureAwait(false);
                }
            }

            if (reader.TryRead(out _))
            {
                throw new InvalidDataException(
                    "The private route contains data beyond its verified batch partition.");
            }

            verifier.Complete();
        }

        var repositoryCount = await ReadRepositoryCountAsync(cancellationToken).ConfigureAwait(false);
        return repositoryCount == profile.CorpusCap
            ? TapRepositoryProvisioningStatus.Provisioned
            : TapRepositoryProvisioningStatus.IdentityMismatch;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _httpClient.Dispose();
    }

    private async Task AddBatchAsync(
        IReadOnlyCollection<string> dids,
        CancellationToken cancellationToken)
    {
        var body = EncodeBatch(dids, _options.MaximumRequestBodyBytes);
        try
        {
            var response = await SendWithRetriesAsync(
                HttpMethod.Post,
                _addRepositoriesUri,
                body,
                cancellationToken).ConfigureAwait(false);
            try
            {
                if (response.Length != 0)
                {
                    throw new InvalidDataException(
                        "TAP returned a non-empty repository-add response outside the reviewed contract.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(response);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(body);
        }
    }

    private async Task<long> ReadRepositoryCountAsync(CancellationToken cancellationToken)
    {
        var body = await SendWithRetriesAsync(
            HttpMethod.Get,
            _repositoryCountUri,
            body: null,
            cancellationToken).ConfigureAwait(false);
        try
        {
            try
            {
                using var document = JsonDocument.Parse(body);
                var root = document.RootElement;
                var properties = root.ValueKind == JsonValueKind.Object
                    ? root.EnumerateObject().ToArray()
                    : [];
                if (properties.Length != 1
                    || !string.Equals(properties[0].Name, "repo_count", StringComparison.Ordinal)
                    || !properties[0].Value.TryGetInt64(out var count)
                    || count < 0)
                {
                    throw new InvalidDataException(
                        "TAP returned a repository-count response outside the reviewed contract.");
                }

                return count;
            }
            catch (JsonException)
            {
                throw new InvalidDataException(
                    "TAP returned a repository-count response outside the reviewed contract.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(body);
        }
    }

    private async Task<byte[]> SendWithRetriesAsync(
        HttpMethod method,
        Uri uri,
        byte[]? body,
        CancellationToken cancellationToken)
    {
        Exception? lastTransientFailure = null;
        for (var attempt = 1; attempt <= _options.MaximumAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                using var request = new HttpRequestMessage(method, uri);
                request.Headers.Authorization = _authorization;
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                if (body is not null)
                {
                    request.Content = new ByteArrayContent(body);
                    request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json")
                    {
                        CharSet = "utf-8",
                    };
                }

                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(_options.RequestTimeout);
                using var response = await _httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    timeout.Token).ConfigureAwait(false);
                var responseBody = await ReadBoundedBodyAsync(response, timeout.Token).ConfigureAwait(false);
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return responseBody;
                }

                try
                {
                    if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
                    {
                        throw new InvalidOperationException(
                            "TAP repository administration authentication failed.");
                    }

                    if (!IsTransient(response.StatusCode))
                    {
                        throw new InvalidOperationException(
                            "TAP repository administration rejected a bounded request.");
                    }

                    lastTransientFailure = new HttpRequestException(
                        "TAP repository administration returned a transient status.");
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(responseBody);
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                lastTransientFailure = new TimeoutException(
                    "A bounded TAP repository administration request timed out.");
            }
            catch (HttpRequestException)
            {
                lastTransientFailure = new HttpRequestException(
                    "A TAP repository administration transport request failed.");
            }
            catch (IOException)
            {
                lastTransientFailure = new IOException(
                    "A TAP repository administration response transport failed.");
            }

            if (attempt < _options.MaximumAttempts)
            {
                await Task.Delay(RetryDelay(attempt), cancellationToken).ConfigureAwait(false);
            }
        }

        if (lastTransientFailure is TimeoutException)
        {
            throw new TimeoutException(
                "TAP repository administration exhausted its bounded timeout retries.");
        }

        throw new HttpRequestException(
            "TAP repository administration exhausted its bounded transport retries.");
    }

    private async Task<byte[]> ReadBoundedBodyAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        if (response.Content.Headers.ContentLength is { } contentLength
            && contentLength > _options.MaximumResponseBytes)
        {
            throw new InvalidDataException("A TAP administration response exceeded its byte bound.");
        }

        await using var stream = await response.Content
            .ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);
        var buffer = ArrayPool<byte>.Shared.Rent(checked(_options.MaximumResponseBytes + 1));
        var written = 0;
        try
        {
            while (true)
            {
                var read = await stream
                    .ReadAsync(buffer.AsMemory(written, buffer.Length - written), cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                {
                    return buffer.AsSpan(0, written).ToArray();
                }

                written = checked(written + read);
                if (written > _options.MaximumResponseBytes)
                {
                    throw new InvalidDataException("A TAP administration response exceeded its byte bound.");
                }
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer.AsSpan(0, written));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private TimeSpan RetryDelay(int failedAttempt)
    {
        if (_options.RetryBaseDelay == TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var multiplier = 1L << Math.Min(failedAttempt - 1, 6);
        var ticks = Math.Min(
            TimeSpan.FromSeconds(30).Ticks,
            checked(_options.RetryBaseDelay.Ticks * multiplier));
        return TimeSpan.FromTicks(ticks);
    }

    private static byte[] EncodeBatch(IReadOnlyCollection<string> dids, int maximumBytes)
    {
        if (dids.Count == 0 || dids.Count > PrivateTapRepositoryProvisionerOptions.MaximumTapBatchSize)
        {
            throw new InvalidOperationException("A TAP repository batch is outside its record bound.");
        }

        var minimumBytes = 12L + dids.Sum(static did => (long)Encoding.UTF8.GetByteCount(did) + 3);
        if (minimumBytes > maximumBytes)
        {
            throw new InvalidDataException("A TAP repository batch exceeded its request byte bound.");
        }

        var output = new ArrayBufferWriter<byte>((int)minimumBytes);
        using (var writer = new Utf8JsonWriter(output))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("dids");
            writer.WriteStartArray();
            foreach (var did in dids)
            {
                writer.WriteStringValue(did);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        if (output.WrittenCount > maximumBytes)
        {
            throw new InvalidDataException("A TAP repository batch exceeded its request byte bound.");
        }

        return output.WrittenSpan.ToArray();
    }

    private static SocketsHttpHandler CreateHandler()
        => new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            AutomaticDecompression = DecompressionMethods.None,
            ConnectTimeout = TimeSpan.FromSeconds(30),
            MaxConnectionsPerServer = 1,
            MaxResponseHeadersLength = 16,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            UseCookies = false,
            UseProxy = false,
        };

    private static Uri DeriveAdminUri(Uri webSocketEndpoint, string path)
        => new UriBuilder(webSocketEndpoint)
        {
            Scheme = webSocketEndpoint.Scheme == "wss" ? "https" : "http",
            Path = path,
            Query = string.Empty,
            Fragment = string.Empty,
        }.Uri;

    private static string LowerHex(ReadOnlySpan<byte> value)
        => Convert.ToHexString(value).ToLowerInvariant();

    private static bool IsTransient(HttpStatusCode statusCode)
        => statusCode is HttpStatusCode.RequestTimeout
            or HttpStatusCode.TooManyRequests
            or HttpStatusCode.InternalServerError
            or HttpStatusCode.BadGateway
            or HttpStatusCode.ServiceUnavailable
            or HttpStatusCode.GatewayTimeout
            || (int)statusCode == 425;

    private static PrivateTapRepositoryProvisionerOptions Snapshot(
        PrivateTapRepositoryProvisionerOptions source)
        => new()
        {
            RoutingManifestPath = source.RoutingManifestPath,
            TapWebSocketEndpoint = source.TapWebSocketEndpoint,
            AdminPassword = source.AdminPassword,
            ExpectedProfileVersion = source.ExpectedProfileVersion,
            ExclusiveRepositoryAdministrationConfirmed = source.ExclusiveRepositoryAdministrationConfirmed,
            FullNetworkModeDisabledConfirmed = source.FullNetworkModeDisabledConfirmed,
            AutomaticRepositoryDiscoveryDisabledConfirmed = source.AutomaticRepositoryDiscoveryDisabledConfirmed,
            BatchSize = source.BatchSize,
            MaximumAttempts = source.MaximumAttempts,
            RequestTimeout = source.RequestTimeout,
            RetryBaseDelay = source.RetryBaseDelay,
            MaximumResponseBytes = source.MaximumResponseBytes,
            MaximumRequestBodyBytes = source.MaximumRequestBodyBytes,
            MaximumManifestBytes = source.MaximumManifestBytes,
            MaximumRoutingArtifactBytes = source.MaximumRoutingArtifactBytes,
            MaximumRouteLineBytes = source.MaximumRouteLineBytes,
        };

    private static void ValidateRoutingManifestPath(string routingManifestPath)
    {
        if (string.IsNullOrWhiteSpace(routingManifestPath)
            || routingManifestPath.Length > 4_096
            || routingManifestPath.IndexOfAny(['\r', '\n']) >= 0
            || !Path.IsPathFullyQualified(routingManifestPath)
            || !string.Equals(
                Path.GetFileName(routingManifestPath),
                AcquisitionContract.RoutingManifestFileName,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "A target private route must be an absolute bounded routing.private.manifest.json path.",
                nameof(routingManifestPath));
        }
    }

    private static bool IsSanitizedRouteFailure(Exception exception)
        => exception is InvalidDataException or JsonException;

    private sealed class VerifiedPrivateRoute : IDisposable
    {
        private VerifiedPrivateRoute(FileStream stream, PrivateRoutingManifest manifest)
        {
            Stream = stream;
            Manifest = manifest;
        }

        internal FileStream Stream { get; }

        internal PrivateRoutingManifest Manifest { get; }

        internal static VerifiedPrivateRoute Open(
            PrivateTapRepositoryProvisionerOptions options,
            TapRepositoryBootstrapProfile profile,
            string routingManifestPath)
        {
            if (profile.ProfileVersion != options.ExpectedProfileVersion)
            {
                throw new InvalidDataException(
                    "The private route profile version does not match the durable runtime profile.");
            }

            RequirePrivateRegularFile(routingManifestPath, options.MaximumManifestBytes);
            var directory = Path.GetDirectoryName(routingManifestPath)
                ?? throw new InvalidDataException("The private route manifest has no parent directory.");
            var routePath = Path.Combine(directory, AcquisitionContract.RoutingFileName);
            RequirePrivateRegularFile(routePath, options.MaximumRoutingArtifactBytes);
            var manifest = PrivateRoutingExporter.Verify(
                routingManifestPath,
                new PrivateRoutingExpectedProfile(
                    profile.ProfileId,
                    profile.CorpusCap,
                    profile.ProfilePrefixSha256));
            if (!MatchesProfile(manifest, profile))
            {
                throw new InvalidDataException(
                    "The private route does not match the exact selected corpus profile.");
            }

            var stream = new FileStream(
                routePath,
                new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    BufferSize = 64 * 1024,
                    Options = FileOptions.SequentialScan,
                });
            try
            {
                RequirePrivateRegularFile(stream, options.MaximumRoutingArtifactBytes);
                if (stream.Length != manifest.Routing.ByteLength)
                {
                    throw new InvalidDataException(
                        "The private routing artifact length does not match its manifest.");
                }

                using var verifier = new RoutePassVerifier(
                    manifest,
                    profile,
                    options.MaximumRouteLineBytes);
                var reader = new BoundedNdjsonReader(stream, options.MaximumRouteLineBytes);
                while (reader.TryRead(out var line))
                {
                    _ = verifier.Accept(line.Span);
                }

                verifier.Complete();
                stream.Position = 0;
                return new VerifiedPrivateRoute(stream, manifest);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        public void Dispose() => Stream.Dispose();

        private static bool MatchesProfile(
            PrivateRoutingManifest manifest,
            TapRepositoryBootstrapProfile profile)
            => string.Equals(manifest.Format, AcquisitionContract.RoutingManifestFormat, StringComparison.Ordinal)
                && string.Equals(manifest.Routing.Format, AcquisitionContract.RoutingArtifactFormat, StringComparison.Ordinal)
                && string.Equals(manifest.Profile.Name, profile.ProfileId, StringComparison.Ordinal)
                && manifest.Profile.AccountCount == profile.CorpusCap
                && manifest.Routing.AccountCount == profile.CorpusCap
                && manifest.Profile.ByteLength == checked(profile.CorpusCap * AccountKey.ByteLength)
                && string.Equals(
                    manifest.Profile.PrefixSha256,
                    profile.ProfilePrefixSha256,
                    StringComparison.Ordinal)
                && string.Equals(
                    manifest.Routing.AccountKeyProjectionSha256,
                    profile.ProfilePrefixSha256,
                    StringComparison.Ordinal);

        private static void RequirePrivateRegularFile(string path, long maximumBytes)
        {
            var file = new FileInfo(path);
            if (!file.Exists
                || file.LinkTarget is not null
                || (file.Attributes & FileAttributes.ReparsePoint) != 0
                || file.Length <= 0
                || file.Length > maximumBytes)
            {
                throw new InvalidDataException("A private routing artifact is missing or outside its file contract.");
            }

            if (!OperatingSystem.IsWindows())
            {
                const UnixFileMode required = UnixFileMode.UserRead | UnixFileMode.UserWrite;
                if (File.GetUnixFileMode(path) != required)
                {
                    throw new InvalidDataException(
                        "A private routing artifact must have Unix mode 0600.");
                }
            }
        }

        private static void RequirePrivateRegularFile(FileStream stream, long maximumBytes)
        {
            var attributes = File.GetAttributes(stream.SafeFileHandle);
            if ((attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint)) != 0
                || stream.Length <= 0
                || stream.Length > maximumBytes)
            {
                throw new InvalidDataException(
                    "An opened private routing artifact is outside its file contract.");
            }

            if (!OperatingSystem.IsWindows())
            {
                const UnixFileMode required = UnixFileMode.UserRead | UnixFileMode.UserWrite;
                if (File.GetUnixFileMode(stream.SafeFileHandle) != required)
                {
                    throw new InvalidDataException(
                        "An opened private routing artifact must have Unix mode 0600.");
                }
            }
        }
    }

    private sealed class RoutePassVerifier : IDisposable
    {
        private readonly PrivateRoutingManifest _manifest;
        private readonly TapRepositoryBootstrapProfile _profile;
        private readonly int _maximumLineBytes;
        private readonly IncrementalHash _artifactHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        private readonly IncrementalHash _projectionHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        private byte[]? _priorKey;
        private long _ordinal;
        private long _byteLength;
        private bool _completed;

        internal RoutePassVerifier(
            PrivateRoutingManifest manifest,
            TapRepositoryBootstrapProfile profile,
            int maximumLineBytes)
        {
            _manifest = manifest;
            _profile = profile;
            _maximumLineBytes = maximumLineBytes;
        }

        internal string Accept(ReadOnlySpan<byte> line)
        {
            if (_completed || line.IsEmpty || line.Length > _maximumLineBytes)
            {
                throw new InvalidDataException("A private route line is outside its byte contract.");
            }

            _ordinal++;
            if (_ordinal > _profile.CorpusCap)
            {
                throw new InvalidDataException("The private route contains more entries than its exact profile.");
            }

            var (lineOrdinal, key, did) = ParseRouteLine(line);
            if (lineOrdinal != _ordinal
                || (_priorKey is not null && _priorKey.AsSpan().SequenceCompareTo(key) >= 0))
            {
                throw new InvalidDataException("The private route is not a unique ordered exact prefix.");
            }

            AccountKey computed;
            try
            {
                computed = AccountKey.FromDid(did);
            }
            catch (ArgumentException)
            {
                throw new InvalidDataException("A private route DID is not canonical.");
            }

            if (!string.Equals(computed.ToString(), Convert.ToHexString(key).ToLowerInvariant(), StringComparison.Ordinal))
            {
                throw new InvalidDataException("A private route account key does not match its exact DID bytes.");
            }

            _artifactHash.AppendData(line);
            _artifactHash.AppendData(LineFeed);
            _projectionHash.AppendData(key);
            _byteLength = checked(_byteLength + line.Length + 1L);
            _priorKey = key;
            return did;
        }

        internal void Complete()
        {
            if (_completed)
            {
                throw new InvalidOperationException("A private route pass was already completed.");
            }

            _completed = true;
            var artifactSha = LowerHex(_artifactHash.GetHashAndReset());
            var projectionSha = LowerHex(_projectionHash.GetHashAndReset());
            if (_ordinal != _profile.CorpusCap
                || _ordinal != _manifest.Routing.AccountCount
                || _byteLength != _manifest.Routing.ByteLength
                || !string.Equals(artifactSha, _manifest.Routing.Sha256, StringComparison.Ordinal)
                || !string.Equals(projectionSha, _profile.ProfilePrefixSha256, StringComparison.Ordinal)
                || !string.Equals(
                    projectionSha,
                    _manifest.Routing.AccountKeyProjectionSha256,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException("The private route does not prove the exact selected prefix.");
            }
        }

        public void Dispose()
        {
            _artifactHash.Dispose();
            _projectionHash.Dispose();
        }

        private static (long Ordinal, byte[] Key, string Did) ParseRouteLine(ReadOnlySpan<byte> line)
        {
            try
            {
                using var document = JsonDocument.Parse(line.ToArray());
                var root = document.RootElement;
                var properties = root.ValueKind == JsonValueKind.Object
                    ? root.EnumerateObject().ToArray()
                    : [];
                if (properties.Length != 3
                    || !string.Equals(properties[0].Name, "ordinal", StringComparison.Ordinal)
                    || !string.Equals(properties[1].Name, "accountKey", StringComparison.Ordinal)
                    || !string.Equals(properties[2].Name, "did", StringComparison.Ordinal)
                    || !properties[0].Value.TryGetInt64(out var ordinal)
                    || ordinal <= 0
                    || properties[1].Value.ValueKind != JsonValueKind.String
                    || properties[2].Value.ValueKind != JsonValueKind.String)
                {
                    throw new InvalidDataException("A private route line is outside its closed schema.");
                }

                var keyText = properties[1].Value.GetString()!;
                if (!AccountKey.TryParse(keyText, out _))
                {
                    throw new InvalidDataException("A private route account key is not canonical.");
                }

                return (ordinal, Convert.FromHexString(keyText), properties[2].Value.GetString()!);
            }
            catch (JsonException)
            {
                throw new InvalidDataException("A private route line is not valid closed JSON.");
            }
        }

        private static string LowerHex(ReadOnlySpan<byte> value)
            => Convert.ToHexString(value).ToLowerInvariant();
    }

    private sealed class BoundedNdjsonReader
    {
        private readonly Stream _stream;
        private readonly byte[] _readBuffer = new byte[64 * 1024];
        private readonly byte[] _lineBuffer;
        private int _readOffset;
        private int _readCount;
        private int _lineLength;
        private bool _endOfStream;

        internal BoundedNdjsonReader(Stream stream, int maximumLineBytes)
        {
            _stream = stream;
            _lineBuffer = new byte[maximumLineBytes];
        }

        internal bool TryRead(out ReadOnlyMemory<byte> line)
        {
            while (true)
            {
                if (_readOffset == _readCount)
                {
                    _readCount = _stream.Read(_readBuffer);
                    _readOffset = 0;
                    if (_readCount == 0)
                    {
                        _endOfStream = true;
                    }
                }

                if (_endOfStream)
                {
                    if (_lineLength != 0)
                    {
                        throw new InvalidDataException("The private route must end every record with LF.");
                    }

                    line = default;
                    return false;
                }

                var remaining = _readBuffer.AsSpan(_readOffset, _readCount - _readOffset);
                var lineFeed = remaining.IndexOf((byte)'\n');
                var fragmentLength = lineFeed < 0 ? remaining.Length : lineFeed;
                if (_lineLength > _lineBuffer.Length - fragmentLength)
                {
                    throw new InvalidDataException("A private route line exceeded its byte bound.");
                }

                remaining[..fragmentLength].CopyTo(_lineBuffer.AsSpan(_lineLength));
                _lineLength += fragmentLength;
                _readOffset += fragmentLength;
                if (lineFeed < 0)
                {
                    continue;
                }

                _readOffset++;
                line = _lineBuffer.AsMemory(0, _lineLength);
                _lineLength = 0;
                return true;
            }
        }
    }
}
