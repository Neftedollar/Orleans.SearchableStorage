namespace Orleans.SearchableStorage.Storage;

internal static class StorageLayout
{
    public const int CurrentFormatVersion = 1;

    public static StorageLayoutDescriptor CreateDescriptor(string providerName, int partitionCount)
    {
        return new StorageLayoutDescriptor
        {
            FormatVersion = CurrentFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
        };
    }

    public static string CreatePartitionKey(string providerName, int partitionIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegative(partitionIndex);
        return $"{providerName}:{partitionIndex:D8}";
    }
}

[GenerateSerializer]
internal sealed class StorageLayoutDescriptor
{
    [Id(0)]
    public int FormatVersion { get; init; }

    [Id(1)]
    public required string ProviderName { get; init; }

    [Id(2)]
    public int PartitionCount { get; init; }
}

[GenerateSerializer]
internal sealed class StorageLayoutState
{
    [Id(0)]
    public bool Initialized { get; set; }

    [Id(1)]
    public int FormatVersion { get; set; }

    [Id(2)]
    public string ProviderName { get; set; } = string.Empty;

    [Id(3)]
    public int PartitionCount { get; set; }
}
