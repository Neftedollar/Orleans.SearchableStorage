using System.Buffers;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

/// <summary>
/// One exact sanitized TAP message. The body is retained only until the durable consumer commits
/// or quarantines the delivery; it must never be logged or stored as an arbitrary raw frame.
/// </summary>
public sealed record TapDelivery(string Json, string Sha256);

/// <summary>
/// Receives sanitized TAP messages and acknowledges them only when its caller has durably decided
/// the exact delivery.
/// </summary>
public sealed class TapDeliverySession : IAsyncDisposable
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private readonly ITapSocket _socket;
    private readonly int _maximumMessageBytes;
    private readonly SemaphoreSlim _receiveGate = new(1, 1);
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private int _disposed;

    internal TapDeliverySession(ITapSocket socket, int maximumMessageBytes)
    {
        _socket = socket;
        _maximumMessageBytes = maximumMessageBytes;
    }

    public async ValueTask<TapDelivery> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _receiveGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var rented = ArrayPool<byte>.Shared.Rent(Math.Min(_maximumMessageBytes, 16 * 1024));
            try
            {
                using var message = new MemoryStream(capacity: Math.Min(_maximumMessageBytes, 16 * 1024));
                while (true)
                {
                    var result = await _socket
                        .ReceiveAsync(rented.AsMemory(), cancellationToken)
                        .ConfigureAwait(false);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        throw new TapConnectionClosedException();
                    }

                    if (result.MessageType != WebSocketMessageType.Text)
                    {
                        throw new InvalidDataException("TAP sent a non-text WebSocket message.");
                    }

                    if (result.Count < 0 || message.Length + result.Count > _maximumMessageBytes)
                    {
                        throw new InvalidDataException(
                            $"TAP message exceeds the configured {_maximumMessageBytes}-byte limit.");
                    }

                    message.Write(rented, 0, result.Count);
                    if (result.EndOfMessage)
                    {
                        break;
                    }
                }

                if (message.Length == 0)
                {
                    throw new InvalidDataException("TAP sent an empty WebSocket message.");
                }

                var exactBytes = message.GetBuffer().AsSpan(0, checked((int)message.Length));
                var digest = Convert.ToHexString(SHA256.HashData(exactBytes)).ToLowerInvariant();
                return new TapDelivery(StrictUtf8.GetString(exactBytes), digest);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented, clearArray: true);
            }
        }
        finally
        {
            _receiveGate.Release();
        }
    }

    /// <summary>
    /// Acknowledges an exact TAP outbox identifier after the durable database decision commits.
    /// </summary>
    public async ValueTask AcknowledgeAsync(
        ulong deliveryId,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (deliveryId == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(deliveryId), "A TAP delivery identifier must be positive.");
        }

        var payload = JsonSerializer.SerializeToUtf8Bytes(
            new Acknowledgement("ack", deliveryId),
            JsonSerializerOptions.Web);
        await _sendGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            await _socket
                .SendAsync(payload, WebSocketMessageType.Text, endOfMessage: true, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _sendGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _socket.DisposeAsync().ConfigureAwait(false);
        await _receiveGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        await _sendGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        _receiveGate.Dispose();
        _sendGate.Dispose();
    }

    private sealed record Acknowledgement(string Type, ulong Id);
}

public sealed class TapConnectionClosedException : IOException
{
    internal TapConnectionClosedException()
        : base("The TAP WebSocket closed before another complete message was received.")
    {
    }
}
