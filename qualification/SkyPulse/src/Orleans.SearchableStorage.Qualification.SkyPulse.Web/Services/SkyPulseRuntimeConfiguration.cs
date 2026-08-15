using Microsoft.Extensions.Configuration;
using Npgsql;
using Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;
using Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;
using Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

public enum SkyPulseRuntimeMode
{
    LocalFunctional = 1,
    Durable = 2,
}

/// <summary>
/// Contains the profile identity and bounded dispatcher settings required by durable mode.
/// The PostgreSQL connection string and TAP administrator password must come from deployment
/// secret providers; neither belongs in checked-in application settings.
/// </summary>
public sealed class SkyPulseDurableConfiguration
{
    private static readonly string PlaceholderSchemaFingerprint = new('0', 64);

    public string ProfileId { get; set; } = string.Empty;

    public int ProfileVersion { get; set; }

    public long CorpusCap { get; set; }

    /// <summary>
    /// Gets or sets the exact <c>prefixSha256</c> of the selected profile in
    /// <c>corpus.manifest.json</c>, not the hash of the larger parent artifact.
    /// </summary>
    public string ProfilePrefixSha256 { get; set; } = string.Empty;

    public Guid SourceInstanceId { get; set; }

    /// <summary>
    /// Canonical <c>corpus.manifest.json</c>. Its sibling <c>accounts.ak32</c> is selected by the
    /// manifest and both files are verified before TAP is opened.
    /// </summary>
    public string CorpusManifestPath { get; set; } = string.Empty;

    public string TapEndpoint { get; set; } = string.Empty;

    /// <summary>
    /// TAP Basic-auth secret. Supply it through a secret provider or environment variable, never
    /// through the checked-in application settings file.
    /// </summary>
    public string TapAdminPassword { get; set; } = string.Empty;

    /// <summary>
    /// Absolute path to the mode-0600 private DID routing manifest produced for the exact corpus
    /// profile. The sibling routing artifact is never logged or copied into PostgreSQL.
    /// </summary>
    public string RoutingManifestPath { get; set; } = string.Empty;

    /// <summary>
    /// Secret required by the monotonic runtime corpus-growth endpoint. It is required only when
    /// at least one reviewed growth profile is configured.
    /// </summary>
    public string CorpusGrowthAdminToken { get; set; } = string.Empty;

    public List<SkyPulseCorpusGrowthProfileConfiguration> GrowthProfiles { get; set; } = [];

    public TimeSpan CorpusGrowthPollDelay { get; set; } = TimeSpan.FromSeconds(1);

    public bool ExclusiveRepositoryAdministrationConfirmed { get; set; }

    public bool FullNetworkModeDisabledConfirmed { get; set; }

    public bool AutomaticRepositoryDiscoveryDisabledConfirmed { get; set; }

    public int CorpusBootstrapPageSize { get; set; } = 1_000;

    public int IngestionMaximumPlanningAttempts { get; set; } = 8;

    public int IngestionLifecyclePageSize { get; set; } = 1_000;

    public TimeSpan IngestionStartupPollDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    public TimeSpan IngestionReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    public int RebuildPageSize { get; set; } = 256;

    public int DispatchBatchSize { get; set; } = 64;

    public TimeSpan DispatchLeaseDuration { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan PreIndexFailureDelay { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan DispatchIdleDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    public int RecalculationBatchSize { get; set; } = 64;

    public TimeSpan RecalculationLeaseDuration { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan RecalculationFailureDelay { get; set; } = TimeSpan.FromSeconds(5);

    internal void Validate()
    {
        _ = CreateManifest(PlaceholderSchemaFingerprint);
        _ = CreateTapOptions();
        _ = CreateTapRepositoryProvisionerOptions();
        _ = CreateIngestionOptions();
        _ = CreateCorpusProfileCatalog();
        ValidateBatch(RebuildPageSize, nameof(RebuildPageSize));
        ValidateBatch(DispatchBatchSize, nameof(DispatchBatchSize));
        ValidateBatch(RecalculationBatchSize, nameof(RecalculationBatchSize));
        if (string.IsNullOrWhiteSpace(CorpusManifestPath)
            || CorpusManifestPath.Length > 4_096
            || CorpusManifestPath.IndexOfAny(['\r', '\n']) >= 0)
        {
            throw new InvalidOperationException(
                "SkyPulse:Durable:CorpusManifestPath must name a bounded local corpus manifest path.");
        }

        if (CorpusBootstrapPageSize is < 1 or > PostgreSqlCorpusBootstrapper.MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(CorpusBootstrapPageSize),
                CorpusBootstrapPageSize,
                $"The corpus bootstrap page must be between 1 and {PostgreSqlCorpusBootstrapper.MaximumPageSize}.");
        }

        ValidateDelay(
            IngestionStartupPollDelay,
            nameof(IngestionStartupPollDelay),
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMinutes(1));
        ValidateDelay(
            IngestionReconnectDelay,
            nameof(IngestionReconnectDelay),
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMinutes(5));
        ValidateDelay(
            CorpusGrowthPollDelay,
            nameof(CorpusGrowthPollDelay),
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMinutes(1));
        if (DispatchLeaseDuration < TimeSpan.FromSeconds(1)
            || DispatchLeaseDuration > TimeSpan.FromMinutes(15))
        {
            throw new ArgumentOutOfRangeException(
                nameof(DispatchLeaseDuration),
                DispatchLeaseDuration,
                "The dispatch lease must be between one second and fifteen minutes.");
        }

        if (PreIndexFailureDelay < TimeSpan.Zero
            || PreIndexFailureDelay > TimeSpan.FromHours(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(PreIndexFailureDelay),
                PreIndexFailureDelay,
                "The pre-index retry delay must be between zero and one hour.");
        }

        if (DispatchIdleDelay < TimeSpan.FromMilliseconds(10)
            || DispatchIdleDelay > TimeSpan.FromMinutes(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(DispatchIdleDelay),
                DispatchIdleDelay,
                "The empty-dispatch delay must be between ten milliseconds and one minute.");
        }

        if (RecalculationLeaseDuration < TimeSpan.FromSeconds(1)
            || RecalculationLeaseDuration > TimeSpan.FromMinutes(15))
        {
            throw new ArgumentOutOfRangeException(
                nameof(RecalculationLeaseDuration),
                RecalculationLeaseDuration,
                "The recalculation lease must be between one second and fifteen minutes.");
        }

        if (RecalculationFailureDelay < TimeSpan.Zero
            || RecalculationFailureDelay > TimeSpan.FromHours(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(RecalculationFailureDelay),
                RecalculationFailureDelay,
                "The recalculation retry delay must be between zero and one hour.");
        }
    }

    private static void ValidateBatch(int value, string parameterName)
    {
        if (value is < 1 or > DurableProjectionRuntimeOptions.MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                value,
                $"A batch size must be between 1 and {DurableProjectionRuntimeOptions.MaximumBatchSize}.");
        }
    }

    internal RuntimeManifest CreateManifest(string schemaFingerprint)
    {
        var canonicalFingerprint = CanonicalSha256(schemaFingerprint, nameof(schemaFingerprint));
        return new RuntimeManifest(
            new RuntimeProfileIdentity(ProfileId, ProfileVersion, CorpusCap, ProfilePrefixSha256),
            SourceInstanceId,
            new RuntimeIndexIdentity(
                SkyPulseIndexContract.ProviderName,
                SkyPulseIndexContract.ProviderName,
                SkyPulseIndexContract.StateName,
                SkyPulseIndexContract.ApplicationSchemaVersion,
                canonicalFingerprint),
            SkyPulsePackageContract.Identity);
    }

    internal DurableProjectionRuntimeOptions CreateRuntimeOptions()
        => new()
        {
            RebuildPageSize = RebuildPageSize,
            DispatchBatchSize = DispatchBatchSize,
            DispatchLeaseDuration = DispatchLeaseDuration,
            PreIndexFailureDelay = PreIndexFailureDelay,
        };

    internal RollingWindowRecalculationOptions CreateRecalculationOptions()
        => new()
        {
            BatchSize = RecalculationBatchSize,
            LeaseDuration = RecalculationLeaseDuration,
            FailureDelay = RecalculationFailureDelay,
        };

    internal TapWebSocketOptions CreateTapOptions()
    {
        if (!Uri.TryCreate(TapEndpoint, UriKind.Absolute, out var endpoint))
        {
            throw new InvalidOperationException(
                "SkyPulse:Durable:TapEndpoint must be an absolute reviewed TAP WebSocket URI.");
        }

        var options = new TapWebSocketOptions
        {
            Endpoint = endpoint,
            AdminPassword = TapAdminPassword,
        };
        options.Validate();
        return options;
    }

    internal DurableTapProcessingOptions CreateIngestionOptions()
    {
        var options = new DurableTapProcessingOptions
        {
            MaximumPlanningAttempts = IngestionMaximumPlanningAttempts,
            LifecyclePageSize = IngestionLifecyclePageSize,
        };
        options.Validate();
        return options;
    }

    internal PrivateTapRepositoryProvisionerOptions CreateTapRepositoryProvisionerOptions()
    {
        var tap = CreateTapOptions();
        var options = new PrivateTapRepositoryProvisionerOptions
        {
            RoutingManifestPath = RoutingManifestPath,
            TapWebSocketEndpoint = tap.Endpoint,
            AdminPassword = tap.AdminPassword,
            ExpectedProfileVersion = ProfileVersion,
            ExclusiveRepositoryAdministrationConfirmed = ExclusiveRepositoryAdministrationConfirmed,
            FullNetworkModeDisabledConfirmed = FullNetworkModeDisabledConfirmed,
            AutomaticRepositoryDiscoveryDisabledConfirmed = AutomaticRepositoryDiscoveryDisabledConfirmed,
        };
        options.Validate();
        return options;
    }

    internal IReadOnlyList<DurableCorpusProfile> CreateCorpusProfileCatalog()
    {
        var growthProfiles = GrowthProfiles
            ?? throw new InvalidOperationException("SkyPulse:Durable:GrowthProfiles cannot be null.");
        var profiles = new List<DurableCorpusProfile>(checked(growthProfiles.Count + 1))
        {
            DurableCorpusProfile.Create(
                ProfileId,
                ProfileVersion,
                CorpusCap,
                ProfilePrefixSha256,
                RoutingManifestPath),
        };
        var ids = new HashSet<string>(StringComparer.Ordinal) { ProfileId };
        var caps = new HashSet<long> { CorpusCap };
        foreach (var configured in growthProfiles)
        {
            ArgumentNullException.ThrowIfNull(configured);
            var profile = DurableCorpusProfile.Create(
                configured.ProfileId,
                ProfileVersion,
                configured.CorpusCap,
                configured.ProfilePrefixSha256,
                configured.RoutingManifestPath);
            if (profile.Capacity.CorpusCap <= CorpusCap)
            {
                throw new InvalidOperationException(
                    "Every runtime growth profile must be larger than the immutable base profile.");
            }

            if (!ids.Add(profile.Capacity.ProfileId) || !caps.Add(profile.Capacity.CorpusCap))
            {
                throw new InvalidOperationException(
                    "Runtime corpus growth profile IDs and caps must be unique.");
            }

            profiles.Add(profile);
        }

        if (profiles.Count > 1
            && (CorpusGrowthAdminToken.Length is < 32 or > 4_096
                || string.IsNullOrWhiteSpace(CorpusGrowthAdminToken)
                || CorpusGrowthAdminToken.IndexOfAny(['\r', '\n']) >= 0))
        {
            throw new InvalidOperationException(
                "A secret 32-4096 character corpus-growth admin token is required when growth profiles are configured.");
        }

        return profiles.OrderBy(static profile => profile.Capacity.CorpusCap).ToArray();
    }

    private static void ValidateDelay(
        TimeSpan value,
        string parameterName,
        TimeSpan minimum,
        TimeSpan maximum)
    {
        if (value < minimum || value > maximum)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                value,
                $"The delay must be between {minimum} and {maximum}.");
        }
    }

    private static string CanonicalSha256(string value, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        if (value.Length != 64
            || value.Any(static character => character is not (>= '0' and <= '9')
                and not (>= 'a' and <= 'f')
                and not (>= 'A' and <= 'F')))
        {
            throw new ArgumentException(
                "The schema fingerprint must be a 64-character hexadecimal SHA-256 value.",
                parameterName);
        }

        return value.ToLowerInvariant();
    }
}

public sealed class SkyPulseCorpusGrowthProfileConfiguration
{
    public string ProfileId { get; set; } = string.Empty;

    public long CorpusCap { get; set; }

    public string ProfilePrefixSha256 { get; set; } = string.Empty;

    public string RoutingManifestPath { get; set; } = string.Empty;
}

internal sealed class SkyPulseApplicationConfiguration
{
    private SkyPulseApplicationConfiguration(
        SkyPulseRuntimeMode mode,
        string? postgreSqlConnectionString,
        SkyPulseDurableConfiguration? durable)
    {
        Mode = mode;
        PostgreSqlConnectionString = postgreSqlConnectionString;
        Durable = durable;
    }

    internal SkyPulseRuntimeMode Mode { get; }

    internal string? PostgreSqlConnectionString { get; }

    internal SkyPulseDurableConfiguration? Durable { get; }

    internal static SkyPulseApplicationConfiguration Resolve(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var modeValue = configuration["SkyPulse:Mode"];
        if (!Enum.TryParse<SkyPulseRuntimeMode>(modeValue, ignoreCase: true, out var mode)
            || !Enum.IsDefined(mode))
        {
            throw new InvalidOperationException(
                "SkyPulse:Mode must explicitly be 'LocalFunctional' or 'Durable'.");
        }

        if (mode == SkyPulseRuntimeMode.LocalFunctional)
        {
            return new SkyPulseApplicationConfiguration(mode, null, null);
        }

        var connectionString = configuration.GetConnectionString("SkyPulsePostgreSql");
        ValidateConnectionString(connectionString);
        var durable = configuration
            .GetSection("SkyPulse:Durable")
            .Get<SkyPulseDurableConfiguration>()
            ?? throw new InvalidOperationException(
                "SkyPulse:Durable configuration is required in Durable mode.");
        durable.Validate();
        return new SkyPulseApplicationConfiguration(mode, connectionString, durable);
    }

    private static void ValidateConnectionString(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "ConnectionStrings:SkyPulsePostgreSql is required in Durable mode.");
        }

        NpgsqlConnectionStringBuilder parsed;
        try
        {
            parsed = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException(
                "ConnectionStrings:SkyPulsePostgreSql is not a valid PostgreSQL connection string.",
                exception);
        }

        if (string.IsNullOrWhiteSpace(parsed.Host)
            || string.IsNullOrWhiteSpace(parsed.Database)
            || string.IsNullOrWhiteSpace(parsed.Username))
        {
            throw new InvalidOperationException(
                "The durable PostgreSQL connection string must explicitly name Host, Database, and Username.");
        }
    }
}

internal static class SkyPulsePackageContract
{
    internal static RuntimePackageIdentity Identity { get; } = new(
        "Orleans.SearchableStorage",
        "1.0.0-rc.2",
        "d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c",
        "c711886b0559b2e667ffa43c8628aaa3088ee32fe64ce4363230ba4e1b52d983",
        "https://github.com/Neftedollar/Orleans.SearchableStorage",
        "6301f8b676edcc6ae0936ead38927f45adb99b00",
        "10.0.303");
}
