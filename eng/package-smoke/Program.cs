using Orleans.SearchableStorage;

var options = new SearchableStorageOptions
{
    PartitionCount = 8,
    VirtualSlotTargetCount = 64,
};

IReadOnlyList<string> cities = ["Haifa", "Tel Aviv"];
var query = Array.Empty<ConsumerState>()
    .AsQueryable()
    .WhereIn(state => state.City, cities);

Console.WriteLine(
    $"{SearchableStorageConstants.PhysicalStorageProviderName}:{options.PartitionCount}:{query.Expression.NodeType}");

internal sealed class ConsumerState
{
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City { get; set; } = string.Empty;
}
