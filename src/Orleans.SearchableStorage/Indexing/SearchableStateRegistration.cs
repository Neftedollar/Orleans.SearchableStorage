using Microsoft.Extensions.Options;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Indexing;

internal interface ISearchableStateRegistration
{
    string ProviderName { get; }

    string StateName { get; }

    Type StateType { get; }

    IndexSchemaDefinition Schema { get; }

    IReadOnlyList<Storage.IndexEntry> Extract(
        IGrainStorageSerializer serializer,
        byte[] payload,
        byte[] schemaFingerprint);
}

internal sealed class SearchableStateRegistration<TState> : ISearchableStateRegistration
{
    public SearchableStateRegistration(
        string providerName,
        string stateName,
        int applicationSchemaVersion = 1)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applicationSchemaVersion);
        ProviderName = providerName;
        StateName = stateName;
        Schema = IndexMetadataProvider.GetSchemaDefinition<TState>(
            stateName,
            applicationSchemaVersion);
    }

    public string ProviderName { get; }

    public string StateName { get; }

    public Type StateType => typeof(TState);

    public IndexSchemaDefinition Schema { get; }

    public IReadOnlyList<Storage.IndexEntry> Extract(
        IGrainStorageSerializer serializer,
        byte[] payload,
        byte[] schemaFingerprint)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(payload);
        var state = serializer.Deserialize<TState>(payload);
        return IndexMetadataProvider.Extract(StateName, state, schemaFingerprint);
    }
}

/// <summary>
/// Avoids routing every steady-state operation through the per-state schema control. Only a
/// fingerprint already confirmed as active is retained; failures and rebuilding states never call
/// <see cref="MarkActive"/>.
/// </summary>
internal sealed class ActiveSchemaValidationCache
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<CacheKey, byte[]> _active =
        new();

    public bool IsActive(ISearchableStateRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        return _active.TryGetValue(CacheKey.Create(registration), out var fingerprint)
            && IndexSchemaIdentity.FixedTimeEquals(fingerprint, registration.Schema.Fingerprint);
    }

    public void MarkActive(ISearchableStateRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        _active[CacheKey.Create(registration)] = [.. registration.Schema.Fingerprint];
    }

    private readonly record struct CacheKey(string ProviderName, string StateName, Type StateType)
    {
        public static CacheKey Create(ISearchableStateRegistration registration)
        {
            return new CacheKey(
                registration.ProviderName,
                registration.StateName,
                registration.StateType);
        }
    }
}

/// <summary>
/// Resolves explicitly registered state schemas. The registry contains declarations only; durable
/// activation and rebuild state remains in the storage control plane.
/// </summary>
internal sealed class SearchableStateRegistry
{
    public static SearchableStateRegistry Empty { get; } = new([], options: null);

    private readonly Dictionary<(string ProviderName, string StateName), ISearchableStateRegistration> _registrations;
    private readonly Dictionary<(string ProviderName, string Fingerprint), ISearchableStateRegistration>
        _registrationsByFingerprint;
    private readonly HashSet<string> _managedProviders;
    private readonly IOptionsMonitor<SearchableStorageOptions>? _options;

    public SearchableStateRegistry(
        IEnumerable<ISearchableStateRegistration> registrations,
        IOptionsMonitor<SearchableStorageOptions>? options)
    {
        ArgumentNullException.ThrowIfNull(registrations);
        _options = options;
        _registrations = new Dictionary<(string, string), ISearchableStateRegistration>();
        _registrationsByFingerprint = new Dictionary<(string, string), ISearchableStateRegistration>();
        _managedProviders = new HashSet<string>(StringComparer.Ordinal);
        foreach (var registration in registrations)
        {
            var key = (registration.ProviderName, registration.StateName);
            if (!_registrations.TryAdd(key, registration))
            {
                var existing = _registrations[key];
                throw new InvalidOperationException(
                    $"Searchable state '{key.StateName}' for provider '{key.ProviderName}' is registered "
                    + $"more than once ('{existing.StateType}' version "
                    + $"{existing.Schema.ApplicationSchemaVersion} and '{registration.StateType}' version "
                    + $"{registration.Schema.ApplicationSchemaVersion}).");
            }

            var fingerprintKey = (
                registration.ProviderName,
                Convert.ToHexString(registration.Schema.Fingerprint));
            if (!_registrationsByFingerprint.TryAdd(fingerprintKey, registration))
            {
                throw new InvalidOperationException(
                    $"Provider '{registration.ProviderName}' contains duplicate managed schema fingerprints.");
            }

            _managedProviders.Add(registration.ProviderName);
        }
    }

    public bool ContainsProvider(string providerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        return _managedProviders.Contains(providerName);
    }

    public ISearchableStateRegistration? Find(string providerName, string stateName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        return _registrations.GetValueOrDefault((providerName, stateName));
    }

    public ISearchableStateRegistration? Find<TState>(string providerName, string stateName)
    {
        var registration = Find(providerName, stateName);
        if (registration is not null && registration.StateType != typeof(TState))
        {
            throw new InvalidOperationException(
                $"Searchable state '{stateName}' for provider '{providerName}' is registered as "
                + $"'{registration.StateType}', but the caller used '{typeof(TState)}'. One provider/state "
                + "pair must map to exactly one CLR state type.");
        }

        return registration;
    }

    public ISearchableStateRegistration? FindByFingerprint(
        string providerName,
        byte[] fingerprint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        IndexSchemaIdentity.ValidateIdentity(fingerprint, nameof(fingerprint));
        return _registrationsByFingerprint.GetValueOrDefault(
            (providerName, Convert.ToHexString(fingerprint)));
    }

    public IReadOnlyList<Storage.IndexEntry> Extract(
        string providerName,
        string stateName,
        byte[] payload,
        byte[] schemaFingerprint)
    {
        var registration = Find(providerName, stateName)
            ?? throw new InvalidOperationException(
                $"No searchable state registration exists for provider '{providerName}' and state '{stateName}'.");
        var serializer = _options?.Get(providerName).GrainStorageSerializer
            ?? throw new InvalidOperationException(
                $"No grain storage serializer is available for provider '{providerName}'.");
        return registration.Extract(serializer, payload, schemaFingerprint);
    }
}
