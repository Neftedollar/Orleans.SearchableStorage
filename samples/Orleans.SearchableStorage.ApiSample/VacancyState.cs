namespace Orleans.SearchableStorage.ApiSample;

/// <summary>
/// Represents one vacancy stored through the searchable provider.
/// </summary>
[GenerateSerializer]
public sealed class VacancyState
{
    /// <summary>
    /// Gets or sets the city used for exact lookup.
    /// </summary>
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the salary used for bounded range lookup.
    /// </summary>
    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public int Salary { get; set; }
}
