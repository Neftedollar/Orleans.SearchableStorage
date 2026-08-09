using Orleans.Runtime;

namespace Orleans.SearchableStorage.ApiSample;

/// <summary>
/// Demonstrates normal Orleans persistent state backed by the searchable provider.
/// </summary>
public sealed class VacancyGrain : Grain, IVacancyGrain
{
    /// <summary>
    /// Identifies the persisted state within the grain.
    /// </summary>
    public const string StateName = "vacancy";

    /// <summary>
    /// Identifies the searchable storage provider.
    /// </summary>
    public const string StorageProviderName = "Searchable";

    private readonly IPersistentState<VacancyState> _state;

    /// <summary>
    /// Initializes a new instance of the <see cref="VacancyGrain"/> class.
    /// </summary>
    public VacancyGrain(
        [PersistentState(StateName, StorageProviderName)]
        IPersistentState<VacancyState> state)
    {
        _state = state;
    }

    /// <inheritdoc />
    public Task<VacancyState?> GetAsync()
    {
        return Task.FromResult(_state.RecordExists ? _state.State : null);
    }

    /// <inheritdoc />
    public async Task SetAsync(VacancyState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        _state.State = state;
        await _state.WriteStateAsync();
    }

    /// <inheritdoc />
    public Task ClearAsync()
    {
        return _state.ClearStateAsync();
    }
}
