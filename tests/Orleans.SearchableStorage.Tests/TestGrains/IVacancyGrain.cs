namespace Orleans.SearchableStorage.Tests.TestGrains;

public interface IVacancyGrain : IGrainWithStringKey
{
    Task<VacancyState> GetAsync();

    Task SetAsync(string city, int salary);

    Task ClearAsync();
}
