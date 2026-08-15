using Npgsql;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;
using Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

internal sealed record DurableCorpusProfile(
    CorpusCapacityProfile Capacity,
    string RoutingManifestPath)
{
    internal static DurableCorpusProfile Create(
        string profileId,
        int profileVersion,
        long corpusCap,
        string prefixSha256,
        string routingManifestPath)
    {
        ArgumentNullException.ThrowIfNull(profileId);
        if (profileId.Length is < 1 or > 80
            || !string.Equals(profileId, profileId.Trim(), StringComparison.Ordinal)
            || profileId.Any(static character => character is not (>= 'a' and <= 'z')
                and not (>= '0' and <= '9')
                and not '.' and not '-' and not '_'))
        {
            throw new ArgumentException(
                "A runtime corpus profile ID must use the canonical 1-80 character form.",
                nameof(profileId));
        }

        if (corpusCap > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(corpusCap),
                corpusCap,
                $"A file-backed runtime corpus profile cannot exceed {int.MaxValue} accounts.");
        }

        if (string.IsNullOrWhiteSpace(routingManifestPath)
            || routingManifestPath.Length > 4_096
            || routingManifestPath.IndexOfAny(['\r', '\n']) >= 0
            || !Path.IsPathFullyQualified(routingManifestPath)
            || !string.Equals(
                Path.GetFileName(routingManifestPath),
                AcquisitionContract.RoutingManifestFileName,
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Every runtime corpus profile needs an absolute bounded routing.private.manifest.json path.");
        }

        return new DurableCorpusProfile(
            new CorpusCapacityProfile(profileId, profileVersion, corpusCap, prefixSha256),
            Path.GetFullPath(routingManifestPath));
    }

    internal TapRepositoryBootstrapProfile ToTapProfile(Guid sourceInstanceId)
        => new(
            Capacity.ProfileId,
            Capacity.ProfileVersion,
            Capacity.CorpusCap,
            Capacity.PrefixSha256,
            sourceInstanceId);
}

internal sealed record CorpusCapacityView(
    string Phase,
    string ActiveProfileId,
    long ActiveCorpusCap,
    string? RequestedProfileId,
    long? RequestedCorpusCap,
    long AdmissionCorpusCap,
    long PostgreSqlAccountCount,
    long SynchronizedAccountCount,
    IReadOnlyList<CorpusCapacityTargetView> AvailableTargets);

internal sealed record CorpusCapacityTargetView(string ProfileId, long CorpusCap);

internal enum RuntimeCorpusGrowthRequestOutcome
{
    Accepted = 1,
    AlreadyActive = 2,
    AlreadyRequested = 3,
    GrowthInProgress = 4,
    NonMonotonic = 5,
    UnknownProfile = 6,
}

internal sealed record RuntimeCorpusGrowthRequestResult(
    RuntimeCorpusGrowthRequestOutcome Outcome,
    CorpusCapacityView Capacity);

/// <summary>
/// Coordinates restartable online expansion while the TAP acknowledgement session remains open.
/// The old searchable prefix remains available throughout the operation.
/// </summary>
internal sealed class DurableCorpusCapacityManager
{
    private const int Uninitialized = 0;
    private const int Stable = 1;
    private const int Bootstrapping = 2;
    private const int Provisioning = 3;
    private const int Faulted = 4;

    private readonly PostgreSqlCorpusCapacityStore _store;
    private readonly NpgsqlDataSource _dataSource;
    private readonly MonotonicCorpusAdmission _admission;
    private readonly ITapRepositoryProvisioner _provisioner;
    private readonly SkyPulseDurableConfiguration _configuration;
    private readonly IReadOnlyDictionary<string, DurableCorpusProfile> _profilesById;
    private readonly DurableCorpusProfile _baseProfile;
    private int _phase;
    private int _initialized;

    public DurableCorpusCapacityManager(
        PostgreSqlCorpusCapacityStore store,
        NpgsqlDataSource dataSource,
        MonotonicCorpusAdmission admission,
        ITapRepositoryProvisioner provisioner,
        SkyPulseDurableConfiguration configuration)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _admission = admission ?? throw new ArgumentNullException(nameof(admission));
        _provisioner = provisioner ?? throw new ArgumentNullException(nameof(provisioner));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        var profiles = configuration.CreateCorpusProfileCatalog();
        _baseProfile = profiles[0];
        _profilesById = profiles.ToDictionary(
            static profile => profile.Capacity.ProfileId,
            StringComparer.Ordinal);
    }

    internal IAccountAdmission Admission => _admission;

    internal string Phase => Volatile.Read(ref _phase) switch
    {
        Uninitialized => "uninitialized",
        Stable => "stable",
        Bootstrapping => "bootstrapping",
        Provisioning => "provisioning",
        Faulted => "faulted",
        _ => "invalid",
    };

    internal async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _initialized, 1, 0) != 0)
        {
            throw new InvalidOperationException("The runtime corpus-capacity manager is already initialized.");
        }

        try
        {
            var state = await _store
                .BindBaseAsync(_baseProfile.Capacity, cancellationToken)
                .ConfigureAwait(false);
            ValidateStateCatalog(state);
            var selected = Resolve(state.Target ?? state.Active);
            Volatile.Write(ref _phase, Bootstrapping);
            var verified = OpenAdmission(selected);
            try
            {
                var bootstrapper = new PostgreSqlCorpusBootstrapper(
                    _dataSource,
                    verified,
                    _configuration.CorpusBootstrapPageSize);
                await bootstrapper.BootstrapAsync(cancellationToken).ConfigureAwait(false);
                _admission.Initialize(verified);
            }
            catch
            {
                verified.Dispose();
                throw;
            }

            Volatile.Write(ref _phase, Stable);
        }
        catch
        {
            Volatile.Write(ref _phase, Faulted);
            throw;
        }
    }

    /// <summary>
    /// Replays and proves the currently admitted exact route. A pending durable target becomes
    /// active only after TAP confirms exact repository cardinality.
    /// </summary>
    internal async Task EnsureCurrentProvisionedAsync(CancellationToken cancellationToken)
    {
        try
        {
            var state = await _store.ReadAsync(cancellationToken).ConfigureAwait(false);
            ValidateStateCatalog(state);
            var selected = Resolve(state.Target ?? state.Active);
            await EnsureProvisionedAsync(selected, cancellationToken).ConfigureAwait(false);
            if (state.Target is not null)
            {
                _ = await _store
                    .CompleteGrowthAsync(selected.Capacity, state.OperationVersion, cancellationToken)
                    .ConfigureAwait(false);
            }

            Volatile.Write(ref _phase, Stable);
        }
        catch
        {
            Volatile.Write(ref _phase, Faulted);
            throw;
        }
    }

    internal async Task RunGrowthLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var state = await _store.ReadAsync(cancellationToken).ConfigureAwait(false);
            ValidateStateCatalog(state);
            if (state.Target is null)
            {
                Volatile.Write(ref _phase, Stable);
                await Task.Delay(_configuration.CorpusGrowthPollDelay, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var target = Resolve(state.Target);
            await PrepareAdmissionAsync(state.Active, target, cancellationToken).ConfigureAwait(false);
            await EnsureProvisionedAsync(target, cancellationToken).ConfigureAwait(false);
            _ = await _store
                .CompleteGrowthAsync(target.Capacity, state.OperationVersion, cancellationToken)
                .ConfigureAwait(false);
            Volatile.Write(ref _phase, Stable);
        }
    }

    internal async Task<RuntimeCorpusGrowthRequestResult> RequestGrowthAsync(
        string profileId,
        CancellationToken cancellationToken)
    {
        if (!_profilesById.TryGetValue(profileId, out var target))
        {
            return new RuntimeCorpusGrowthRequestResult(
                RuntimeCorpusGrowthRequestOutcome.UnknownProfile,
                await ReadViewAsync(cancellationToken).ConfigureAwait(false));
        }

        var result = await _store
            .RequestGrowthAsync(target.Capacity, cancellationToken)
            .ConfigureAwait(false);
        return new RuntimeCorpusGrowthRequestResult(
            result.Outcome switch
            {
                CorpusGrowthRequestOutcome.Accepted => RuntimeCorpusGrowthRequestOutcome.Accepted,
                CorpusGrowthRequestOutcome.AlreadyActive => RuntimeCorpusGrowthRequestOutcome.AlreadyActive,
                CorpusGrowthRequestOutcome.AlreadyRequested => RuntimeCorpusGrowthRequestOutcome.AlreadyRequested,
                CorpusGrowthRequestOutcome.GrowthInProgress => RuntimeCorpusGrowthRequestOutcome.GrowthInProgress,
                CorpusGrowthRequestOutcome.NonMonotonic => RuntimeCorpusGrowthRequestOutcome.NonMonotonic,
                _ => throw new InvalidOperationException("PostgreSQL returned an unknown corpus-growth outcome."),
            },
            await ReadViewAsync(cancellationToken).ConfigureAwait(false));
    }

    internal async Task<CorpusCapacityView> ReadViewAsync(CancellationToken cancellationToken)
    {
        var state = await _store.ReadAsync(cancellationToken).ConfigureAwait(false);
        ValidateStateCatalog(state);
        var statistics = await _store.ReadStatisticsAsync(cancellationToken).ConfigureAwait(false);
        var targets = state.Target is null
            ? _profilesById.Values
                .Where(profile => profile.Capacity.CorpusCap > state.Active.CorpusCap)
                .OrderBy(static profile => profile.Capacity.CorpusCap)
                .Select(static profile => new CorpusCapacityTargetView(
                    profile.Capacity.ProfileId,
                    profile.Capacity.CorpusCap))
                .ToArray()
            : [];
        return new CorpusCapacityView(
            Phase,
            state.Active.ProfileId,
            state.Active.CorpusCap,
            state.Target?.ProfileId,
            state.Target?.CorpusCap,
            _admission.IsInitialized ? _admission.Count : 0,
            statistics.AccountCount,
            statistics.SynchronizedAccountCount,
            targets);
    }

    private async Task PrepareAdmissionAsync(
        CorpusCapacityProfile active,
        DurableCorpusProfile target,
        CancellationToken cancellationToken)
    {
        if (_admission.Count == target.Capacity.CorpusCap)
        {
            if (!string.Equals(_admission.ProfileId, target.Capacity.ProfileId, StringComparison.Ordinal)
                || !string.Equals(
                    _admission.ProfilePrefixSha256,
                    target.Capacity.PrefixSha256,
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "The in-process admission identity does not match the durable target.");
            }

            return;
        }

        if (_admission.Count != active.CorpusCap || target.Capacity.CorpusCap <= _admission.Count)
        {
            throw new InvalidOperationException(
                "The in-process admission cannot be advanced from the durable active prefix.");
        }

        Volatile.Write(ref _phase, Bootstrapping);
        var verified = OpenAdmission(target);
        try
        {
            var bootstrapper = new PostgreSqlCorpusBootstrapper(
                _dataSource,
                verified,
                _configuration.CorpusBootstrapPageSize);
            await bootstrapper
                .BootstrapRangeAsync(checked((int)active.CorpusCap), cancellationToken)
                .ConfigureAwait(false);
            _admission.Advance(verified);
        }
        catch
        {
            verified.Dispose();
            throw;
        }
    }

    private async Task EnsureProvisionedAsync(
        DurableCorpusProfile target,
        CancellationToken cancellationToken)
    {
        Volatile.Write(ref _phase, Provisioning);
        var tapProfile = target.ToTapProfile(_configuration.SourceInstanceId);
        if (_provisioner.ValidateConfigured(tapProfile, target.RoutingManifestPath)
            != TapRepositoryProvisionerConfigurationStatus.Configured)
        {
            throw new InvalidOperationException(
                "The exact private TAP repository provisioner is not configured for the requested profile.");
        }

        if (await _provisioner
                .ProvisionAsync(tapProfile, target.RoutingManifestPath, cancellationToken)
                .ConfigureAwait(false)
            != TapRepositoryProvisioningStatus.Provisioned)
        {
            throw new InvalidOperationException(
                "TAP did not prove the exact requested repository-set cardinality.");
        }
    }

    private VerifiedCorpusAdmission OpenAdmission(DurableCorpusProfile profile)
        => VerifiedCorpusAdmission.Open(
            _configuration.CorpusManifestPath,
            profile.Capacity.ProfileId,
            profile.Capacity.CorpusCap,
            profile.Capacity.PrefixSha256);

    private DurableCorpusProfile Resolve(CorpusCapacityProfile persisted)
    {
        if (!_profilesById.TryGetValue(persisted.ProfileId, out var configured)
            || configured.Capacity.ProfileVersion != persisted.ProfileVersion
            || configured.Capacity.CorpusCap != persisted.CorpusCap
            || !string.Equals(
                configured.Capacity.PrefixSha256,
                persisted.PrefixSha256,
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "A durable corpus-capacity profile is missing or differs from deployment configuration.");
        }

        return configured;
    }

    private void ValidateStateCatalog(CorpusCapacityState state)
    {
        if (!ProfilesEqual(state.Base, _baseProfile.Capacity))
        {
            throw new CorpusCapacityIdentityMismatchException();
        }

        _ = Resolve(state.Active);
        if (state.Target is not null)
        {
            _ = Resolve(state.Target);
        }
    }

    private static bool ProfilesEqual(CorpusCapacityProfile left, CorpusCapacityProfile right)
        => left.ProfileVersion == right.ProfileVersion
            && left.CorpusCap == right.CorpusCap
            && string.Equals(left.ProfileId, right.ProfileId, StringComparison.Ordinal)
            && string.Equals(left.PrefixSha256, right.PrefixSha256, StringComparison.Ordinal);
}
