namespace Orleans.SearchableStorage.Tests.TestGrains;

[GenerateSerializer]
public sealed class CollectionMembershipState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string?[]? Tags { get; set; }

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public List<int?>? AudienceIds { get; set; }

    [Id(2)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City { get; set; } = string.Empty;

    [Id(3)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public int Salary { get; set; }
}
