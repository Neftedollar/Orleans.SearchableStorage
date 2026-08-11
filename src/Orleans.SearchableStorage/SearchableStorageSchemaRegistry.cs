using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage;

/// <summary>
/// Declares the managed state schemas used by a directly constructed
/// <see cref="SearchableStorageClient"/>.
/// </summary>
/// <remarks>
/// Build one registry during application startup, add each provider state name exactly once, and
/// pass it to the client constructor. The client captures a snapshot, so later changes to this
/// object do not alter an already constructed client.
/// </remarks>
public sealed class SearchableStorageSchemaRegistry
{
    private readonly object _syncRoot = new();
    private readonly Dictionary<string, IStateDeclaration> _states =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Adds one CLR state type and its application-controlled schema version.
    /// </summary>
    /// <typeparam name="TState">The Orleans persistent-state type.</typeparam>
    /// <param name="stateName">The Orleans persistent-state name.</param>
    /// <param name="applicationSchemaVersion">
    /// A positive application-owned version. Increment it when index semantics change without a
    /// corresponding attribute, name, kind, CLR type, or built-in codec change.
    /// </param>
    /// <returns>This registry, for conventional chained configuration.</returns>
    public SearchableStorageSchemaRegistry AddState<TState>(
        string stateName,
        int applicationSchemaVersion = 1)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(applicationSchemaVersion);

        var declaration = new StateDeclaration<TState>(stateName, applicationSchemaVersion);
        lock (_syncRoot)
        {
            if (!_states.TryAdd(stateName, declaration))
            {
                var existing = _states[stateName];
                throw new InvalidOperationException(
                    $"Searchable state '{stateName}' is already declared as "
                    + $"'{existing.StateType}' with application schema version "
                    + $"{existing.ApplicationSchemaVersion}.");
            }
        }

        return this;
    }

    internal SearchableStateRegistry CreateRegistry(string providerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ISearchableStateRegistration[] registrations;
        lock (_syncRoot)
        {
            registrations = _states.Values
                .Select(declaration => declaration.CreateRegistration(providerName))
                .ToArray();
        }

        return new SearchableStateRegistry(registrations, options: null);
    }

    private interface IStateDeclaration
    {
        Type StateType { get; }

        int ApplicationSchemaVersion { get; }

        ISearchableStateRegistration CreateRegistration(string providerName);
    }

    private sealed class StateDeclaration<TState>(
        string stateName,
        int applicationSchemaVersion) : IStateDeclaration
    {
        public Type StateType => typeof(TState);

        public int ApplicationSchemaVersion => applicationSchemaVersion;

        public ISearchableStateRegistration CreateRegistration(string providerName)
        {
            return new SearchableStateRegistration<TState>(
                providerName,
                stateName,
                applicationSchemaVersion);
        }
    }
}
