using System.Globalization;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

/// <summary>
/// Defines the authenticated, acknowledgement-based connection to the patched TAP process.
/// </summary>
public sealed class TapWebSocketOptions
{
    public const int DefaultMaximumMessageBytes = 16 * 1024;

    public Uri Endpoint { get; set; } = new("ws://127.0.0.1:2480/channel");

    public string AdminPassword { get; set; } = string.Empty;

    public int MaximumMessageBytes { get; set; } = DefaultMaximumMessageBytes;

    public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(30);

    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(Endpoint);
        if (!Endpoint.IsAbsoluteUri
            || Endpoint.Scheme is not ("ws" or "wss")
            || !string.Equals(Endpoint.AbsolutePath, "/channel", StringComparison.Ordinal)
            || !string.IsNullOrEmpty(Endpoint.Query)
            || !string.IsNullOrEmpty(Endpoint.Fragment)
            || !string.IsNullOrEmpty(Endpoint.UserInfo))
        {
            throw new InvalidOperationException(
                "The TAP endpoint must be an absolute ws/wss /channel URI without query, fragment, or user information.");
        }

        if (Endpoint.Scheme == "ws" && !Endpoint.IsLoopback)
        {
            throw new InvalidOperationException("Unencrypted TAP WebSockets are allowed only on loopback.");
        }

        if (string.IsNullOrWhiteSpace(AdminPassword)
            || AdminPassword.Length > 4096
            || AdminPassword.IndexOfAny(['\r', '\n']) >= 0)
        {
            throw new InvalidOperationException(
                "A non-empty bounded TAP admin password without line breaks is required.");
        }

        if (MaximumMessageBytes is < 1024 or > DefaultMaximumMessageBytes)
        {
            throw new InvalidOperationException(
                "The maximum TAP message size must be between 1 KiB and the reviewed 16-KiB parser limit, inclusive.");
        }

        if (KeepAliveInterval <= TimeSpan.Zero || KeepAliveInterval > TimeSpan.FromMinutes(5))
        {
            throw new InvalidOperationException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "The TAP keep-alive interval must be positive and no longer than {0} minutes.",
                    5));
        }
    }
}
