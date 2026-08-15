using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tap.Tests;

public sealed class TapWebSocketClientTests
{
    [Fact]
    public async Task ConnectUsesExactEndpointAndBasicAdminCredential()
    {
        var socket = new FakeTapSocket();
        var options = ValidOptions();

        await using var session = await TapWebSocketClient.ConnectAsync(options, socket, default);

        Assert.Equal(options.Endpoint, socket.Endpoint);
        Assert.Equal("Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("admin:test-secret")), socket.Authorization);
        Assert.Equal(options.KeepAliveInterval, socket.KeepAliveInterval);
    }

    [Fact]
    public async Task ReceiveReassemblesOneFragmentedTextMessage()
    {
        var socket = new FakeTapSocket();
        socket.Enqueue("{\"id\":1,", endOfMessage: false);
        socket.Enqueue("\"type\":\"identity\"}", endOfMessage: true);
        await using var session = await TapWebSocketClient.ConnectAsync(ValidOptions(), socket, default);

        var delivery = await session.ReceiveAsync();

        Assert.Equal("{\"id\":1,\"type\":\"identity\"}", delivery.Json);
        Assert.Equal(
            Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(delivery.Json))).ToLowerInvariant(),
            delivery.Sha256);
    }

    [Fact]
    public async Task AcknowledgeWritesOnlyCanonicalIdAndType()
    {
        var socket = new FakeTapSocket();
        await using var session = await TapWebSocketClient.ConnectAsync(ValidOptions(), socket, default);

        await session.AcknowledgeAsync(42);

        var sent = Assert.Single(socket.Sent);
        Assert.Equal(WebSocketMessageType.Text, sent.MessageType);
        Assert.True(sent.EndOfMessage);
        Assert.Equal("{\"type\":\"ack\",\"id\":42}", Encoding.UTF8.GetString(sent.Payload));
    }

    [Fact]
    public async Task OversizedMessageFailsClosedWithoutAcknowledgement()
    {
        var socket = new FakeTapSocket();
        socket.Enqueue(new string('x', 1024), endOfMessage: false);
        socket.Enqueue("y", endOfMessage: true);
        var options = ValidOptions();
        options.MaximumMessageBytes = 1024;
        await using var session = await TapWebSocketClient.ConnectAsync(options, socket, default);

        await Assert.ThrowsAsync<InvalidDataException>(async () => await session.ReceiveAsync());
        Assert.Empty(socket.Sent);
    }

    [Fact]
    public async Task BinaryAndInvalidUtf8MessagesFailClosed()
    {
        var binarySocket = new FakeTapSocket();
        binarySocket.Enqueue([1, 2, 3], WebSocketMessageType.Binary, endOfMessage: true);
        await using var binarySession = await TapWebSocketClient.ConnectAsync(
            ValidOptions(),
            binarySocket,
            default);
        await Assert.ThrowsAsync<InvalidDataException>(async () => await binarySession.ReceiveAsync());

        var utf8Socket = new FakeTapSocket();
        utf8Socket.Enqueue([0xc3, 0x28], WebSocketMessageType.Text, endOfMessage: true);
        await using var utf8Session = await TapWebSocketClient.ConnectAsync(
            ValidOptions(),
            utf8Socket,
            default);
        await Assert.ThrowsAsync<DecoderFallbackException>(async () => await utf8Session.ReceiveAsync());
    }

    [Theory]
    [InlineData("ws://example.com/channel")]
    [InlineData("wss://example.com/not-channel")]
    [InlineData("wss://example.com/channel?unexpected=true")]
    public void InvalidEndpointFailsClosed(string endpoint)
    {
        var options = ValidOptions();
        options.Endpoint = new Uri(endpoint);

        Assert.Throws<InvalidOperationException>(options.Validate);
    }

    [Fact]
    public void WhitespaceOnlyPasswordMatchesTheHardenedTapStartupContract()
    {
        var options = ValidOptions();
        options.AdminPassword = "   ";

        Assert.Throws<InvalidOperationException>(options.Validate);
    }

    private static TapWebSocketOptions ValidOptions()
        => new()
        {
            Endpoint = new Uri("ws://127.0.0.1:2480/channel"),
            AdminPassword = "test-secret",
            MaximumMessageBytes = 4096,
            KeepAliveInterval = TimeSpan.FromSeconds(20),
        };

    private sealed class FakeTapSocket : ITapSocket
    {
        private readonly Queue<TapFrame> _received = new();

        public Uri? Endpoint { get; private set; }

        public string? Authorization { get; private set; }

        public TimeSpan KeepAliveInterval { get; private set; }

        public List<SentFrame> Sent { get; } = [];

        public void Enqueue(string value, bool endOfMessage)
            => _received.Enqueue(new TapFrame(Encoding.UTF8.GetBytes(value), WebSocketMessageType.Text, endOfMessage));

        public void Enqueue(byte[] value, WebSocketMessageType messageType, bool endOfMessage)
            => _received.Enqueue(new TapFrame(value, messageType, endOfMessage));

        public Task ConnectAsync(
            Uri endpoint,
            string authorization,
            TimeSpan keepAliveInterval,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Endpoint = endpoint;
            Authorization = authorization;
            KeepAliveInterval = keepAliveInterval;
            return Task.CompletedTask;
        }

        public ValueTask<TapSocketReceiveResult> ReceiveAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var frame = _received.Dequeue();
            frame.Payload.CopyTo(buffer);
            return ValueTask.FromResult(
                new TapSocketReceiveResult(frame.Payload.Length, frame.MessageType, frame.EndOfMessage));
        }

        public ValueTask SendAsync(
            ReadOnlyMemory<byte> buffer,
            WebSocketMessageType messageType,
            bool endOfMessage,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Sent.Add(new SentFrame(buffer.ToArray(), messageType, endOfMessage));
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private sealed record TapFrame(byte[] Payload, WebSocketMessageType MessageType, bool EndOfMessage);
    }

    private sealed record SentFrame(byte[] Payload, WebSocketMessageType MessageType, bool EndOfMessage);
}
