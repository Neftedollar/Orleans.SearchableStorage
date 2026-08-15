using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

/// <summary>
/// Opens an authenticated WebSocket-ack session against the reviewed TAP overlay.
/// </summary>
public sealed class TapWebSocketClient
{
    private readonly Func<ITapSocket> _socketFactory;

    public TapWebSocketClient()
        : this(static () => new ClientTapSocket())
    {
    }

    internal TapWebSocketClient(Func<ITapSocket> socketFactory)
    {
        ArgumentNullException.ThrowIfNull(socketFactory);
        _socketFactory = socketFactory;
    }

    public Task<TapDeliverySession> ConnectAsync(
        TapWebSocketOptions options,
        CancellationToken cancellationToken = default)
        => ConnectAsync(options, _socketFactory(), cancellationToken);

    internal static async Task<TapDeliverySession> ConnectAsync(
        TapWebSocketOptions options,
        ITapSocket socket,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(socket);
        options.Validate();

        var credential = Convert.ToBase64String(Encoding.UTF8.GetBytes($"admin:{options.AdminPassword}"));
        var authorization = new AuthenticationHeaderValue("Basic", credential).ToString();
        try
        {
            await socket
                .ConnectAsync(options.Endpoint, authorization, options.KeepAliveInterval, cancellationToken)
                .ConfigureAwait(false);
            return new TapDeliverySession(socket, options.MaximumMessageBytes);
        }
        catch
        {
            await socket.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

internal readonly record struct TapSocketReceiveResult(
    int Count,
    WebSocketMessageType MessageType,
    bool EndOfMessage);

internal interface ITapSocket : IAsyncDisposable
{
    Task ConnectAsync(
        Uri endpoint,
        string authorization,
        TimeSpan keepAliveInterval,
        CancellationToken cancellationToken);

    ValueTask<TapSocketReceiveResult> ReceiveAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken);

    ValueTask SendAsync(
        ReadOnlyMemory<byte> buffer,
        WebSocketMessageType messageType,
        bool endOfMessage,
        CancellationToken cancellationToken);
}

internal sealed class ClientTapSocket : ITapSocket
{
    private readonly ClientWebSocket _socket = new();

    public Task ConnectAsync(
        Uri endpoint,
        string authorization,
        TimeSpan keepAliveInterval,
        CancellationToken cancellationToken)
    {
        _socket.Options.SetRequestHeader("Authorization", authorization);
        _socket.Options.KeepAliveInterval = keepAliveInterval;
        return _socket.ConnectAsync(endpoint, cancellationToken);
    }

    public async ValueTask<TapSocketReceiveResult> ReceiveAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken)
    {
        var result = await _socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
        return new TapSocketReceiveResult(result.Count, result.MessageType, result.EndOfMessage);
    }

    public ValueTask SendAsync(
        ReadOnlyMemory<byte> buffer,
        WebSocketMessageType messageType,
        bool endOfMessage,
        CancellationToken cancellationToken)
        => _socket.SendAsync(buffer, messageType, endOfMessage, cancellationToken);

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return ValueTask.CompletedTask;
    }
}
