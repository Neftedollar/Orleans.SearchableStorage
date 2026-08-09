namespace Orleans.SearchableStorage.Tests.TestGrains;

[GenerateSerializer]
public sealed class VacancyState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City { get; set; } = string.Empty;

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public int Salary { get; set; }
}
