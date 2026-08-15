using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

public sealed class JetstreamV2LifecycleSource : IJetstreamLifecycleSource
{
    public async ValueTask<IJetstreamLifecycleSession> OpenAsync(
        JetstreamOpenRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var socket = new ClientWebSocket();
        socket.Options.AddSubProtocol(AcquisitionContract.JetstreamSubprotocol);
        try
        {
            await socket.ConnectAsync(BuildUri(request), cancellationToken).ConfigureAwait(false);
            if (socket.SubProtocol is { Length: > 0 } negotiated
                && !string.Equals(
                    negotiated,
                    AcquisitionContract.JetstreamSubprotocol,
                    StringComparison.Ordinal))
            {
                throw new AcquisitionContractException(
                    "jetstream-subprotocol",
                    "Jetstream negotiated an unexpected WebSocket subprotocol.");
            }

            return new Session(socket, request.ExpectedInstanceId, request.MaximumFrameBytes);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private static Uri BuildUri(JetstreamOpenRequest request)
    {
        var query = "kinds=account&kinds=identity&kinds=sync";
        if (request.InclusiveCursor is { } cursor)
        {
            query += $"&cursor={cursor}";
        }

        return new UriBuilder(request.Endpoint)
        {
            Path = AcquisitionContract.JetstreamXrpcPath,
            Query = query,
        }.Uri;
    }

    private sealed class Session(
        ClientWebSocket socket,
        string instanceId,
        int maximumFrameBytes) : IJetstreamLifecycleSession
    {
        private readonly TaskCompletionSource<ulong> _firstCursor = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private long _latestReceived;
        private int _readStarted;

        public string InstanceId { get; } = instanceId;

        public ulong? LatestReceivedCursor
        {
            get
            {
                var value = Interlocked.Read(ref _latestReceived);
                return value == 0 ? null : checked((ulong)value);
            }
        }

        public async IAsyncEnumerable<JetstreamLifecycleObservation> ReadAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _readStarted, 1) != 0)
            {
                throw new InvalidOperationException("A Jetstream lifecycle session can be read only once.");
            }

            var buffer = new byte[maximumFrameBytes];
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var length = 0;
                    ValueWebSocketReceiveResult result;
                    do
                    {
                        if (length == buffer.Length)
                        {
                            throw new AcquisitionContractException(
                                "jetstream-frame-bound",
                                "Jetstream lifecycle frame exceeded the configured byte bound.");
                        }

                        result = await socket.ReceiveAsync(
                            buffer.AsMemory(length),
                            cancellationToken).ConfigureAwait(false);
                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            yield break;
                        }

                        if (result.MessageType != WebSocketMessageType.Text)
                        {
                            throw new AcquisitionContractException(
                                "jetstream-frame-type",
                                "Uncompressed Jetstream v2 must deliver text frames.");
                        }

                        length = checked(length + result.Count);
                    }
                    while (!result.EndOfMessage);

                    JetstreamLifecycleObservation observation;
                    try
                    {
                        observation = JetstreamV2FrameParser.Parse(
                            buffer.AsMemory(0, length),
                            InstanceId);
                    }
                    finally
                    {
                        Array.Clear(buffer, 0, length);
                    }

                    if (observation.Cursor > long.MaxValue)
                    {
                        throw new AcquisitionContractException(
                            "jetstream-cursor-range",
                            "Jetstream cursor exceeded the supported signed 64-bit lexicon range.");
                    }

                    Interlocked.Exchange(ref _latestReceived, checked((long)observation.Cursor));
                    _firstCursor.TrySetResult(observation.Cursor);
                    yield return observation;
                }
            }
            finally
            {
                Array.Clear(buffer);
            }
        }

        public async ValueTask<ulong> WaitForCloseCursorAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (LatestReceivedCursor is { } cursor)
            {
                return cursor;
            }

            return await _firstCursor.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                try
                {
                    await socket.CloseOutputAsync(
                        WebSocketCloseStatus.NormalClosure,
                        string.Empty,
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (WebSocketException)
                {
                }
            }

            socket.Dispose();
        }
    }
}

internal static class JetstreamV2FrameParser
{
    private const string MessageType = "message";
    private const string ErrorType = "error";
    private const string Prefix = "network.bsky.jetstream.subscribeEvents#";

    public static JetstreamLifecycleObservation Parse(ReadOnlyMemory<byte> utf8, string instanceId)
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
            throw Invalid("Jetstream returned malformed JSON.", exception);
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw Invalid("Jetstream frame root must be an object.");
            }

            string? type = null;
            JsonElement payload = default;
            var seenType = false;
            var seenPayload = false;
            var seenError = false;
            var seenMessage = false;
            foreach (var property in root.EnumerateObject())
            {
                switch (property.Name)
                {
                    case "$type" when !seenType:
                        seenType = true;
                        type = ReadText(property.Value, 64, "$type");
                        break;
                    case "payload" when !seenPayload:
                        seenPayload = true;
                        payload = property.Value;
                        break;
                    case "error" when !seenError:
                        seenError = true;
                        _ = ReadText(property.Value, 128, "error");
                        break;
                    case "message" when !seenMessage:
                        seenMessage = true;
                        _ = ReadText(property.Value, 2048, "message");
                        break;
                    case "$type" or "payload" or "error" or "message":
                        throw Invalid("Jetstream frame contains a duplicate property.");
                    default:
                        throw Invalid("Jetstream frame contains a property outside the pinned contract.");
                }
            }

            if (string.Equals(type, ErrorType, StringComparison.Ordinal))
            {
                throw new AcquisitionContractException(
                    "jetstream-gap",
                    "Jetstream sent a terminal error frame; continuity is not proven.");
            }

            if (!string.Equals(type, MessageType, StringComparison.Ordinal)
                || !seenPayload
                || seenError
                || seenMessage)
            {
                throw Invalid("Jetstream frame is not a canonical message envelope.");
            }

            return ParsePayload(payload, instanceId);
        }
    }

    private static JetstreamLifecycleObservation ParsePayload(JsonElement payload, string instanceId)
    {
        if (payload.ValueKind != JsonValueKind.Object)
        {
            throw Invalid("Jetstream message payload must be an object.");
        }

        var declaredType = ReadDeclaredPayloadType(payload);
        if (string.Equals(declaredType, $"{Prefix}info", StringComparison.Ordinal))
        {
            throw new AcquisitionContractException(
                "jetstream-gap",
                "Jetstream sent an advisory cursor clamp; continuity is not proven.");
        }

        if (string.Equals(declaredType, $"{Prefix}commit", StringComparison.Ordinal))
        {
            throw new AcquisitionContractException(
                "jetstream-commit-leak",
                "The lifecycle-only Jetstream subscription delivered a commit.");
        }

        string? type = null;
        string? did = null;
        ulong? seq = null;
        JsonElement nested = default;
        var nestedName = string.Empty;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in payload.EnumerateObject())
        {
            if (!seen.Add(property.Name))
            {
                throw Invalid("Jetstream payload contains a duplicate property.");
            }

            switch (property.Name)
            {
                case "$type":
                    type = ReadText(property.Value, 128, "$type");
                    break;
                case "seq":
                    if (property.Value.ValueKind != JsonValueKind.Number
                        || !property.Value.TryGetInt64(out var parsedSeq)
                        || parsedSeq <= 0)
                    {
                        throw Invalid("Jetstream seq must be a positive 64-bit integer.");
                    }

                    seq = checked((ulong)parsedSeq);
                    break;
                case "did":
                    did = ReadDid(property.Value);
                    break;
                case "time":
                    _ = ReadText(property.Value, 128, "time");
                    break;
                case "identity" or "account" or "sync":
                    if (nestedName.Length != 0)
                    {
                        throw Invalid("Jetstream lifecycle payload contains multiple nested events.");
                    }

                    nestedName = property.Name;
                    nested = property.Value;
                    break;
                case "commit":
                    throw new AcquisitionContractException(
                        "jetstream-commit-leak",
                        "The lifecycle-only Jetstream subscription delivered a commit.");
                default:
                    throw Invalid("Jetstream lifecycle payload contains a property outside the pinned contract.");
            }
        }

        if (type is null || did is null || seq is null || nestedName.Length == 0 || !seen.Contains("time"))
        {
            throw Invalid("Jetstream lifecycle payload omitted a required property.");
        }

        var expectedName = type.StartsWith(Prefix, StringComparison.Ordinal)
            ? type[Prefix.Length..]
            : string.Empty;
        if (!string.Equals(expectedName, nestedName, StringComparison.Ordinal))
        {
            if (string.Equals(expectedName, "commit", StringComparison.Ordinal))
            {
                throw new AcquisitionContractException(
                    "jetstream-commit-leak",
                    "The lifecycle-only Jetstream subscription delivered a commit.");
            }

            if (string.Equals(expectedName, "info", StringComparison.Ordinal))
            {
                throw new AcquisitionContractException(
                    "jetstream-gap",
                    "Jetstream sent an advisory cursor clamp; continuity is not proven.");
            }

            throw Invalid("Jetstream lifecycle discriminator and nested payload disagree.");
        }

        return nestedName switch
        {
            "account" => new JetstreamLifecycleObservation(
                instanceId,
                seq.Value,
                JetstreamLifecycleKind.Account,
                did,
                ReadAccount(nested, did)),
            "identity" => ReadNonStatus(
                nested,
                did,
                instanceId,
                seq.Value,
                JetstreamLifecycleKind.Identity,
                "identity",
                new HashSet<string>(["did", "seq", "time", "handle"], StringComparer.Ordinal)),
            "sync" => ReadNonStatus(
                nested,
                did,
                instanceId,
                seq.Value,
                JetstreamLifecycleKind.Sync,
                "sync",
                new HashSet<string>(["did", "seq", "time", "rev", "blocks"], StringComparer.Ordinal)),
            _ => throw Invalid("Jetstream delivered an unsupported lifecycle kind."),
        };
    }

    private static string? ReadDeclaredPayloadType(JsonElement payload)
    {
        string? type = null;
        foreach (var property in payload.EnumerateObject())
        {
            if (!string.Equals(property.Name, "$type", StringComparison.Ordinal))
            {
                continue;
            }

            if (type is not null)
            {
                throw Invalid("Jetstream payload contains a duplicate property.");
            }

            type = ReadText(property.Value, 128, "$type");
        }

        return type;
    }

    private static bool ReadAccount(JsonElement element, string outerDid)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw Invalid("Jetstream account value must be an object.");
        }

        string? did = null;
        bool? active = null;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            if (!seen.Add(property.Name))
            {
                throw Invalid("Jetstream account value contains a duplicate property.");
            }

            switch (property.Name)
            {
                case "did":
                    did = ReadDid(property.Value);
                    break;
                case "active":
                    if (property.Value.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                    {
                        throw Invalid("Jetstream account active must be a boolean.");
                    }

                    active = property.Value.GetBoolean();
                    break;
                case "seq":
                    ReadUpstreamSeq(property.Value);
                    break;
                case "time":
                    _ = ReadText(property.Value, 128, "account.time");
                    break;
                case "status":
                    _ = ReadText(property.Value, 128, "account.status");
                    break;
                default:
                    throw Invalid("Jetstream account value contains a property outside the pinned contract.");
            }
        }

        if (did is null || active is null || !seen.Contains("seq") || !seen.Contains("time")
            || !string.Equals(did, outerDid, StringComparison.Ordinal))
        {
            throw Invalid("Jetstream account value is incomplete or names a different DID.");
        }

        return active.Value;
    }

    private static JetstreamLifecycleObservation ReadNonStatus(
        JsonElement element,
        string outerDid,
        string instanceId,
        ulong seq,
        JetstreamLifecycleKind kind,
        string name,
        HashSet<string> allowed)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw Invalid($"Jetstream {name} value must be an object.");
        }

        string? did = null;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            if (!allowed.Contains(property.Name) || !seen.Add(property.Name))
            {
                throw Invalid($"Jetstream {name} value has an unknown or duplicate property.");
            }

            switch (property.Name)
            {
                case "did":
                    did = ReadDid(property.Value);
                    break;
                case "seq":
                    ReadUpstreamSeq(property.Value);
                    break;
                case "time":
                    _ = ReadText(property.Value, 128, $"{name}.time");
                    break;
                case "handle":
                    // Deliberately validate-and-discard; never materialize outside this frame scope.
                    _ = ReadText(property.Value, 2048, "identity.handle");
                    break;
                case "rev":
                    _ = ReadText(property.Value, 128, "sync.rev");
                    break;
                case "blocks":
                    ReadLexiconBytes(property.Value, 10_000, "sync.blocks");
                    break;
            }
        }

        if (did is null || !seen.Contains("seq") || !seen.Contains("time")
            || (kind == JetstreamLifecycleKind.Sync
                && (!seen.Contains("rev") || !seen.Contains("blocks")))
            || !string.Equals(did, outerDid, StringComparison.Ordinal))
        {
            throw Invalid($"Jetstream {name} value is incomplete or names a different DID.");
        }

        return new JetstreamLifecycleObservation(instanceId, seq, kind, outerDid, null);
    }

    private static void ReadLexiconBytes(JsonElement element, int maximumDecodedBytes, string name)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw Invalid($"Jetstream {name} must use the lexicon bytes object encoding.");
        }

        string? encoded = null;
        foreach (var property in element.EnumerateObject())
        {
            if (!string.Equals(property.Name, "$bytes", StringComparison.Ordinal)
                || encoded is not null
                || property.Value.ValueKind != JsonValueKind.String)
            {
                throw Invalid($"Jetstream {name} has an unknown, duplicate, or non-string property.");
            }

            encoded = property.Value.GetString();
        }

        if (encoded is null || encoded.Length % 4 == 1)
        {
            throw Invalid($"Jetstream {name} is not canonical raw base64.");
        }

        var decodedLength = checked((encoded.Length / 4 * 3) + ((encoded.Length % 4) switch
        {
            2 => 1,
            3 => 2,
            _ => 0,
        }));
        if (decodedLength > maximumDecodedBytes)
        {
            throw Invalid($"Jetstream {name} exceeds the pinned decoded byte bound.");
        }

        for (var index = 0; index < encoded.Length; index++)
        {
            if (Base64Value(encoded[index]) < 0)
            {
                throw Invalid($"Jetstream {name} is not canonical raw base64.");
            }
        }

        var remainder = encoded.Length % 4;
        if ((remainder == 2 && (Base64Value(encoded[^1]) & 0x0f) != 0)
            || (remainder == 3 && (Base64Value(encoded[^1]) & 0x03) != 0))
        {
            throw Invalid($"Jetstream {name} contains non-zero unused base64 bits.");
        }
    }

    private static int Base64Value(char value)
        => value switch
        {
            >= 'A' and <= 'Z' => value - 'A',
            >= 'a' and <= 'z' => value - 'a' + 26,
            >= '0' and <= '9' => value - '0' + 52,
            '+' => 62,
            '/' => 63,
            _ => -1,
        };

    private static string ReadDid(JsonElement element)
    {
        var did = ReadText(element, 2048, "did");
        try
        {
            _ = AccountKey.FromDid(did);
        }
        catch (ArgumentException exception)
        {
            throw Invalid("Jetstream returned a non-canonical DID.", exception);
        }

        return did;
    }

    private static void ReadUpstreamSeq(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Number
            || !element.TryGetInt64(out var value)
            || value < 0)
        {
            throw Invalid("Jetstream upstream seq must be a non-negative 64-bit integer.");
        }
    }

    private static string ReadText(JsonElement element, int maximumUtf8Bytes, string name)
    {
        if (element.ValueKind != JsonValueKind.String)
        {
            throw Invalid($"Jetstream {name} must be a string.");
        }

        var value = element.GetString()!;
        if (value.Length == 0
            || !string.Equals(value, value.Trim(), StringComparison.Ordinal)
            || Encoding.UTF8.GetByteCount(value) > maximumUtf8Bytes
            || value.Any(static character => char.IsControl(character)))
        {
            throw Invalid($"Jetstream {name} is empty, non-canonical, or too large.");
        }

        return value;
    }

    private static AcquisitionContractException Invalid(string message, Exception? inner = null)
        => inner is null
            ? new AcquisitionContractException("malformed-jetstream", message)
            : new AcquisitionContractException("malformed-jetstream", message, inner);
}
