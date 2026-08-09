namespace Orleans.SearchableStorage.ApiSample;

/// <summary>
/// Exposes the state operations used by the sample HTTP API.
/// </summary>
public interface IVacancyGrain : IGrainWithStringKey
{
    /// <summary>
    /// Reads the vacancy or returns <see langword="null"/> when it has not been stored.
    /// </summary>
    Task<VacancyState?> GetAsync();

    /// <summary>
    /// Replaces the stored vacancy and its index entries.
    /// </summary>
    Task SetAsync(VacancyState state);

    /// <summary>
    /// Removes the stored vacancy and its index entries.
    /// </summary>
    Task ClearAsync();
}
