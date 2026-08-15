using System.Net;
using System.Net.Http.Headers;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

public sealed class HttpListReposSource : IListReposSource, IDisposable
{
    private readonly HttpClient _client;
    private readonly bool _ownsClient;
    private readonly int _maximumResponseBytes;

    public HttpListReposSource(int maximumResponseBytes)
        : this(
            new HttpClient(CreateHandler(), disposeHandler: true)
            {
                Timeout = Timeout.InfiniteTimeSpan,
            },
            maximumResponseBytes,
            ownsClient: true)
    {
    }

    internal HttpListReposSource(HttpClient client, int maximumResponseBytes)
        : this(client, maximumResponseBytes, ownsClient: false)
    {
    }

    private HttpListReposSource(HttpClient client, int maximumResponseBytes, bool ownsClient)
    {
        ArgumentNullException.ThrowIfNull(client);
        if (maximumResponseBytes is < 1024 or > 64 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumResponseBytes));
        }

        _client = client;
        _ownsClient = ownsClient;
        _maximumResponseBytes = maximumResponseBytes;
    }

    public async ValueTask<ListReposPage> GetPageAsync(
        ListReposRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Limit is < 1 or > 1000)
        {
            throw new ArgumentOutOfRangeException(nameof(request));
        }

        var builder = new UriBuilder(request.Endpoint)
        {
            Path = AcquisitionContract.ListReposXrpcPath,
            Query = request.Cursor is null
                ? $"limit={request.Limit}"
                : $"limit={request.Limit}&cursor={Uri.EscapeDataString(request.Cursor)}",
        };
        using var message = new HttpRequestMessage(HttpMethod.Get, builder.Uri);
        message.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        using var response = await _client.SendAsync(
            message,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                "listRepos returned a non-success status.",
                null,
                response.StatusCode);
        }

        if (response.Content.Headers.ContentType?.MediaType is { } mediaType
            && !string.Equals(mediaType, "application/json", StringComparison.OrdinalIgnoreCase))
        {
            throw new AcquisitionContractException(
                "list-repos-content-type",
                "listRepos returned an unexpected content type.");
        }

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);
        var bytes = await ReadBoundedAsync(stream, _maximumResponseBytes, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            return Parse(bytes, request.ExpectedInstanceId, request.Limit);
        }
        finally
        {
            Array.Clear(bytes);
        }
    }

    public void Dispose()
    {
        if (_ownsClient)
        {
            _client.Dispose();
        }
    }

    internal static SocketsHttpHandler CreateHandler()
        => new()
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

    internal static ListReposPage Parse(
        ReadOnlyMemory<byte> utf8,
        string instanceId,
        int requestedLimit)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(
                utf8,
                new JsonDocumentOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = 8,
                });
        }
        catch (JsonException exception)
        {
            throw new AcquisitionContractException(
                "malformed-list-repos",
                "listRepos returned malformed JSON.",
                exception);
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw Invalid("listRepos root must be an object.");
            }

            string? cursor = null;
            List<ListReposObservation>? repositories = null;
            var seenCursor = false;
            var seenRepos = false;
            foreach (var property in document.RootElement.EnumerateObject())
            {
                switch (property.Name)
                {
                    case "cursor" when !seenCursor:
                        seenCursor = true;
                        cursor = ReadBoundedString(property.Value, 2048, "cursor");
                        break;
                    case "repos" when !seenRepos:
                        seenRepos = true;
                        repositories = ReadRepositories(property.Value, requestedLimit);
                        break;
                    case "cursor" or "repos":
                        throw Invalid("listRepos contains a duplicate property.");
                    default:
                        throw Invalid("listRepos contains a property outside the pinned contract.");
                }
            }

            if (repositories is null)
            {
                throw Invalid("listRepos omitted its required repositories array.");
            }

            return new ListReposPage(instanceId, repositories, cursor);
        }
    }

    private static List<ListReposObservation> ReadRepositories(JsonElement element, int requestedLimit)
    {
        if (element.ValueKind != JsonValueKind.Array)
        {
            throw Invalid("listRepos repositories must be an array.");
        }

        var result = new List<ListReposObservation>(Math.Min(element.GetArrayLength(), requestedLimit));
        foreach (var repository in element.EnumerateArray())
        {
            if (result.Count == requestedLimit)
            {
                throw Invalid("listRepos exceeded the requested page limit.");
            }

            result.Add(ReadRepository(repository));
        }

        return result;
    }

    private static ListReposObservation ReadRepository(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw Invalid("A listRepos repository must be an object.");
        }

        string? did = null;
        bool? active = null;
        var seenDid = false;
        var seenHead = false;
        var seenRev = false;
        var seenActive = false;
        var seenStatus = false;
        foreach (var property in element.EnumerateObject())
        {
            switch (property.Name)
            {
                case "did" when !seenDid:
                    seenDid = true;
                    did = ReadBoundedString(property.Value, 2048, "did");
                    try
                    {
                        _ = AccountKey.FromDid(did);
                    }
                    catch (ArgumentException exception)
                    {
                        throw new AcquisitionContractException(
                            "invalid-list-repos-did",
                            "listRepos returned a non-canonical repository DID.",
                            exception);
                    }

                    break;
                case "head" when !seenHead:
                    seenHead = true;
                    _ = ReadBoundedString(property.Value, 512, "head");
                    break;
                case "rev" when !seenRev:
                    seenRev = true;
                    _ = ReadBoundedString(property.Value, 128, "rev");
                    break;
                case "active" when !seenActive:
                    seenActive = true;
                    if (property.Value.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                    {
                        throw Invalid("listRepos active must be a boolean when present.");
                    }

                    active = property.Value.GetBoolean();
                    break;
                case "status" when !seenStatus:
                    seenStatus = true;
                    _ = ReadBoundedString(property.Value, 128, "status");
                    break;
                case "did" or "head" or "rev" or "active" or "status":
                    throw Invalid("A listRepos repository contains a duplicate property.");
                default:
                    throw Invalid("A listRepos repository contains a property outside the pinned contract.");
            }
        }

        if (did is null || !seenHead || !seenRev)
        {
            throw Invalid("A listRepos repository omitted a required pinned-contract property.");
        }

        return new ListReposObservation(did, active);
    }

    private static string ReadBoundedString(JsonElement element, int maximumUtf8Bytes, string name)
    {
        if (element.ValueKind != JsonValueKind.String)
        {
            throw Invalid($"listRepos {name} must be a string.");
        }

        var value = element.GetString()!;
        if (value.Length == 0
            || !string.Equals(value, value.Trim(), StringComparison.Ordinal)
            || Encoding.UTF8.GetByteCount(value) > maximumUtf8Bytes
            || value.Any(static character => char.IsControl(character)))
        {
            throw Invalid($"listRepos {name} is empty, non-canonical, or too large.");
        }

        return value;
    }

    private static async Task<byte[]> ReadBoundedAsync(
        Stream source,
        int maximumBytes,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(checked(maximumBytes + 1));
        var written = 0;
        try
        {
            while (true)
            {
                var read = await source
                    .ReadAsync(buffer.AsMemory(written, buffer.Length - written), cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                {
                    return buffer.AsSpan(0, written).ToArray();
                }

                written = checked(written + read);
                if (written > maximumBytes)
                {
                    throw new AcquisitionContractException(
                        "list-repos-response-bound",
                        "listRepos response exceeded the configured byte bound.");
                }
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer.AsSpan(0, written));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static AcquisitionContractException Invalid(string message)
        => new("malformed-list-repos", message);
}
