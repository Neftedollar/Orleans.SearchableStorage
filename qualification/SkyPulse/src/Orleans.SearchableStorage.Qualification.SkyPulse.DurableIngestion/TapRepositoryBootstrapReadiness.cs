namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

/// <summary>
/// Identifies the exact private routing profile which must be installed in TAP. Account DIDs
/// cannot be reconstructed from the one-way account-key corpus.
/// </summary>
public sealed record TapRepositoryBootstrapProfile
{
    public TapRepositoryBootstrapProfile(
        string profileId,
        int profileVersion,
        long corpusCap,
        string profilePrefixSha256,
        Guid sourceInstanceId)
    {
        ArgumentNullException.ThrowIfNull(profileId);
        if (profileId.Length is < 1 or > 80
            || !string.Equals(profileId, profileId.Trim(), StringComparison.Ordinal)
            || profileId.Any(static character => character is not (>= 'a' and <= 'z')
                and not (>= '0' and <= '9')
                and not '.' and not '-' and not '_'))
        {
            throw new ArgumentException(
                "The profile ID must use the canonical 1-80 character form.",
                nameof(profileId));
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(profileVersion);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(corpusCap);
        if (profilePrefixSha256 is null
            || profilePrefixSha256.Length != 64
            || profilePrefixSha256.Any(static character => character is not (>= '0' and <= '9')
                and not (>= 'a' and <= 'f')))
        {
            throw new ArgumentException(
                "The profile-prefix digest must be 64 lowercase hexadecimal characters.",
                nameof(profilePrefixSha256));
        }

        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A stable TAP source-instance identifier is required.", nameof(sourceInstanceId));
        }

        ProfileId = profileId;
        ProfileVersion = profileVersion;
        CorpusCap = corpusCap;
        ProfilePrefixSha256 = profilePrefixSha256;
        SourceInstanceId = sourceInstanceId;
    }

    public string ProfileId { get; }

    public int ProfileVersion { get; }

    public long CorpusCap { get; }

    public string ProfilePrefixSha256 { get; }

    public Guid SourceInstanceId { get; }
}

public enum TapRepositoryProvisionerConfigurationStatus
{
    Configured = 1,
    Missing = 2,
    IdentityMismatch = 3,
}

public enum TapRepositoryProvisioningStatus
{
    Provisioned = 1,
    IdentityMismatch = 2,
}

/// <summary>
/// Defines a two-phase repository-set bootstrap. Configuration validation must not contact TAP.
/// Provisioning starts only after the acknowledgement WebSocket is already receiving, so a large
/// repository backfill cannot fill the TAP outbox before the consumer exists.
/// </summary>
public interface ITapRepositoryProvisioner
{
    TapRepositoryProvisionerConfigurationStatus ValidateConfigured(
        TapRepositoryBootstrapProfile profile);

    TapRepositoryProvisionerConfigurationStatus ValidateConfigured(
        TapRepositoryBootstrapProfile profile,
        string routingManifestPath);

    Task<TapRepositoryProvisioningStatus> ProvisionAsync(
        TapRepositoryBootstrapProfile profile,
        CancellationToken cancellationToken = default);

    Task<TapRepositoryProvisioningStatus> ProvisionAsync(
        TapRepositoryBootstrapProfile profile,
        string routingManifestPath,
        CancellationToken cancellationToken = default);
}
