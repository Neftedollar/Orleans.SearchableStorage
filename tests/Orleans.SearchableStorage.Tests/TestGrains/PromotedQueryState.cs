namespace Orleans.SearchableStorage.Tests.TestGrains;

[GenerateSerializer]
public sealed class PromotedQueryState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public byte Age { get; set; }

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public PromotionStatus Status { get; set; }

    [Id(2)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public PromotionStatus? OptionalStatus { get; set; }
}

public enum PromotionStatus : ushort
{
    Inactive = 0,
    Active = 1,
}
