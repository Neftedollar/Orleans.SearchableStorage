using Orleans.Runtime;

namespace Orleans.SearchableStorage.Tests.TestGrains;

public sealed class VacancyGrain : Grain, IVacancyGrain
{
    public const string StateName = "vacancy";
    public const string StorageProviderName = "Searchable";

    private readonly IPersistentState<VacancyState> _state;

    public VacancyGrain(
        [PersistentState(StateName, StorageProviderName)]
        IPersistentState<VacancyState> state)
    {
        _state = state;
    }

    public Task<VacancyState> GetAsync()
    {
        return Task.FromResult(_state.State);
    }

    public async Task SetAsync(string city, int salary)
    {
        _state.State.City = city;
        _state.State.Salary = salary;
        await _state.WriteStateAsync();
    }

    public Task ClearAsync()
    {
        return _state.ClearStateAsync();
    }
}
