using System.Text;
using System.Text.Json;
using System.Security.Cryptography;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

internal enum ExplicitLifecycleStatus : byte
{
    Inactive = 0,
    Active = 1,
}

internal sealed record SanitizedLifecycleObservation(
    long Ordinal,
    string Did,
    ExplicitLifecycleStatus Status);

internal static class SanitizedObservationParser
{
    internal const int MaximumLineBytes = 16 * 1024;
    private const int MaximumDidBytes = 2 * 1024;
    private const int MaximumSourcePositionBytes = 2 * 1024;

    public static SanitizedLifecycleObservation Parse(ReadOnlyMemory<byte> utf8Line, long lineNumber)
    {
        if (utf8Line.IsEmpty)
        {
            throw Error(lineNumber, "Empty lines are not allowed.");
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(
                utf8Line,
                new JsonDocumentOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = 4,
                });
        }
        catch (JsonException exception)
        {
            throw Error(lineNumber, "The line is not a valid JSON object.", exception);
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw Error(lineNumber, "Each line must be a JSON object.");
            }

            long? ordinal = null;
            string? did = null;
            ExplicitLifecycleStatus? status = null;
            string? sourcePosition = null;
            var seenOrdinal = false;
            var seenDid = false;
            var seenStatus = false;
            var seenSourcePosition = false;

            foreach (var property in document.RootElement.EnumerateObject())
            {
                switch (property.Name)
                {
                    case "ordinal" when !seenOrdinal:
                        seenOrdinal = true;
                        if (property.Value.ValueKind != JsonValueKind.Number
                            || !property.Value.TryGetInt64(out var parsedOrdinal)
                            || parsedOrdinal <= 0)
                        {
                            throw Error(lineNumber, "'ordinal' must be a positive 64-bit integer.");
                        }

                        ordinal = parsedOrdinal;
                        break;

                    case "did" when !seenDid:
                        seenDid = true;
                        did = ReadBoundedCanonicalText(
                            property.Value,
                            MaximumDidBytes,
                            "did",
                            lineNumber);
                        try
                        {
                            _ = AccountKey.FromDid(did);
                        }
                        catch (ArgumentException exception)
                        {
                            throw Error(lineNumber, "'did' is not a canonical repository DID.", exception);
                        }

                        break;

                    case "status" when !seenStatus:
                        seenStatus = true;
                        if (property.Value.ValueKind != JsonValueKind.String)
                        {
                            throw Error(lineNumber, "'status' must be 'active' or 'inactive'.");
                        }

                        status = property.Value.GetString() switch
                        {
                            "active" => ExplicitLifecycleStatus.Active,
                            "inactive" => ExplicitLifecycleStatus.Inactive,
                            _ => throw Error(
                                lineNumber,
                                "Unknown lifecycle status; only explicit 'active' or 'inactive' is allowed."),
                        };
                        break;

                    case "sourcePosition" when !seenSourcePosition:
                        seenSourcePosition = true;
                        sourcePosition = ReadBoundedCanonicalText(
                            property.Value,
                            MaximumSourcePositionBytes,
                            "sourcePosition",
                            lineNumber);
                        break;

                    case "ordinal" or "did" or "status" or "sourcePosition":
                        throw Error(lineNumber, $"Duplicate property '{property.Name}' is inconsistent.");

                    default:
                        throw Error(
                            lineNumber,
                            $"Property '{property.Name}' is outside the sanitized observation allowlist.");
                }
            }

            if (ordinal is null || did is null || status is null || sourcePosition is null)
            {
                throw Error(
                    lineNumber,
                    "Exactly 'ordinal', 'did', 'status', and 'sourcePosition' are required.");
            }

            return new SanitizedLifecycleObservation(ordinal.Value, did, status.Value);
        }
    }

    private static string ReadBoundedCanonicalText(
        JsonElement element,
        int maximumUtf8Bytes,
        string propertyName,
        long lineNumber)
    {
        if (element.ValueKind != JsonValueKind.String)
        {
            throw Error(lineNumber, $"'{propertyName}' must be a string.");
        }

        var value = element.GetString()!;
        if (value.Length == 0
            || !string.Equals(value, value.Trim(), StringComparison.Ordinal)
            || Encoding.UTF8.GetByteCount(value) > maximumUtf8Bytes
            || value.Any(static character => char.IsControl(character)))
        {
            throw Error(lineNumber, $"'{propertyName}' is empty, non-canonical, or too large.");
        }

        return value;
    }

    private static InvalidDataException Error(long lineNumber, string message, Exception? inner = null)
        => new($"Sanitized journal line {lineNumber}: {message}", inner);
}

internal sealed class BoundedUtf8LineReader : IDisposable
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private readonly FileStream _stream;
    private readonly IncrementalHash _sourceHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly byte[] _readBuffer = new byte[64 * 1024];
    private readonly byte[] _lineBuffer;
    private int _readOffset;
    private int _readCount;
    private bool _endOfStream;
    private bool _hashFinalized;
    private string? _sha256;

    public BoundedUtf8LineReader(string path, int maximumLineBytes)
    {
        _stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
                BufferSize = 1,
            });
        _lineBuffer = new byte[maximumLineBytes];
    }

    public long ByteLength { get; private set; }

    public string GetCompletedSha256()
    {
        if (!_endOfStream || _readOffset != _readCount)
        {
            throw new InvalidOperationException("The complete journal must be consumed before its hash is read.");
        }

        if (!_hashFinalized)
        {
            _sha256 = Convert.ToHexString(_sourceHash.GetHashAndReset()).ToLowerInvariant();
            _hashFinalized = true;
        }

        return _sha256!;
    }

    public bool TryReadLine(out ReadOnlyMemory<byte> line)
    {
        var lineLength = 0;
        while (true)
        {
            if (_readOffset == _readCount)
            {
                if (_endOfStream)
                {
                    if (lineLength == 0)
                    {
                        line = default;
                        return false;
                    }

                    line = TrimCarriageReturn(lineLength);
                    ValidateUtf8(line);
                    return true;
                }

                _readCount = _stream.Read(_readBuffer);
                _readOffset = 0;
                if (_readCount == 0)
                {
                    _endOfStream = true;
                    continue;
                }

                _sourceHash.AppendData(_readBuffer.AsSpan(0, _readCount));
                ByteLength = checked(ByteLength + _readCount);
            }

            var unread = _readBuffer.AsSpan(_readOffset, _readCount - _readOffset);
            var newline = unread.IndexOf((byte)'\n');
            var copyLength = newline >= 0 ? newline : unread.Length;
            if (lineLength + copyLength > _lineBuffer.Length)
            {
                throw new InvalidDataException(
                    $"A sanitized journal line exceeds the {_lineBuffer.Length}-byte limit.");
            }

            unread[..copyLength].CopyTo(_lineBuffer.AsSpan(lineLength));
            lineLength += copyLength;
            _readOffset += copyLength;

            if (newline >= 0)
            {
                _readOffset++;
                line = TrimCarriageReturn(lineLength);
                ValidateUtf8(line);
                return true;
            }
        }
    }

    public void Dispose()
    {
        _sourceHash.Dispose();
        _stream.Dispose();
    }

    private ReadOnlyMemory<byte> TrimCarriageReturn(int lineLength)
    {
        if (lineLength > 0 && _lineBuffer[lineLength - 1] == (byte)'\r')
        {
            lineLength--;
        }

        return _lineBuffer.AsMemory(0, lineLength);
    }

    private static void ValidateUtf8(ReadOnlyMemory<byte> line)
    {
        try
        {
            _ = StrictUtf8.GetCharCount(line.Span);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException("The sanitized journal is not valid UTF-8.", exception);
        }
    }
}
