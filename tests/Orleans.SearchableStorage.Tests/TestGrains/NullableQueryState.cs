namespace Orleans.SearchableStorage.Tests.TestGrains;

[GenerateSerializer]
public sealed class NullableQueryState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public int? Score { get; set; }
}
