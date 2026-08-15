using Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

public interface IDurableTapSession : IAsyncDisposable
{
    ValueTask<TapDelivery> ReceiveAsync(CancellationToken cancellationToken = default);

    ValueTask AcknowledgeAsync(ulong deliveryId, CancellationToken cancellationToken = default);
}

public interface IDurableTapSessionFactory
{
    Task<IDurableTapSession> ConnectAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Opens the reviewed authenticated TAP WebSocket transport.
/// </summary>
public sealed class TapWebSocketSessionFactory : IDurableTapSessionFactory
{
    private readonly TapWebSocketClient _client;
    private readonly TapWebSocketOptions _options;

    public TapWebSocketSessionFactory(TapWebSocketOptions options, TapWebSocketClient? client = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = new TapWebSocketOptions
        {
            Endpoint = options.Endpoint,
            AdminPassword = options.AdminPassword,
            MaximumMessageBytes = options.MaximumMessageBytes,
            KeepAliveInterval = options.KeepAliveInterval,
        };
        _client = client ?? new TapWebSocketClient();
    }

    public async Task<IDurableTapSession> ConnectAsync(CancellationToken cancellationToken = default)
        => new TapSessionAdapter(
            await _client.ConnectAsync(_options, cancellationToken).ConfigureAwait(false));

    private sealed class TapSessionAdapter(TapDeliverySession session) : IDurableTapSession
    {
        public ValueTask<TapDelivery> ReceiveAsync(CancellationToken cancellationToken = default)
            => session.ReceiveAsync(cancellationToken);

        public ValueTask AcknowledgeAsync(
            ulong deliveryId,
            CancellationToken cancellationToken = default)
            => session.AcknowledgeAsync(deliveryId, cancellationToken);

        public ValueTask DisposeAsync() => session.DisposeAsync();
    }
}

public enum DurableTapSessionDisposition
{
    RetryConnectionWithoutAcknowledgement = 1,
}

/// <summary>
/// Runs one correctness-first ordered session. It never reads the next frame before the current
/// delivery has either been durably decided and acknowledged, or explicitly abandoned without an
/// acknowledgement so TAP can redeliver it.
/// </summary>
public sealed class DurableTapSessionRunner
{
    private readonly DurableTapDeliveryProcessor _processor;
    private readonly TimeProvider _timeProvider;

    public DurableTapSessionRunner(
        DurableTapDeliveryProcessor processor,
        TimeProvider? timeProvider = null)
    {
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<DurableTapSessionDisposition> RunAsync(
        IDurableTapSession session,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var delivery = await session.ReceiveAsync(cancellationToken).ConfigureAwait(false);
            var result = await _processor.ProcessAsync(
                delivery,
                _timeProvider.GetUtcNow(),
                cancellationToken).ConfigureAwait(false);
            if (!result.AcknowledgementAllowed)
            {
                return DurableTapSessionDisposition.RetryConnectionWithoutAcknowledgement;
            }

            if (result.DeliveryId == 0)
            {
                throw new InvalidOperationException("An acknowledgement-safe result has no positive delivery identifier.");
            }

            await session
                .AcknowledgeAsync(result.DeliveryId, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
