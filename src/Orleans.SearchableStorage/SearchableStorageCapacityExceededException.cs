namespace Orleans.SearchableStorage;

/// <summary>
/// Indicates that a write, maintenance operation, or durable payload exceeds the fixed
/// searchable-storage capacity envelope.
/// </summary>
[GenerateSerializer]
public sealed class SearchableStorageCapacityExceededException : InvalidOperationException
{
    /// <summary>Initializes an exception with a stable serializer-compatible default value.</summary>
    public SearchableStorageCapacityExceededException()
        : this("unspecified", 0, 0)
    {
    }

    /// <summary>Initializes a capacity exception without including record or index values.</summary>
    /// <param name="boundary">The stable name of the exceeded boundary.</param>
    /// <param name="actual">The measured element or canonical byte count.</param>
    /// <param name="limit">The enforced maximum.</param>
    public SearchableStorageCapacityExceededException(string boundary, long actual, long limit)
        : base(CreateMessage(boundary, actual, limit))
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(boundary);
        ArgumentOutOfRangeException.ThrowIfNegative(actual);
        ArgumentOutOfRangeException.ThrowIfNegative(limit);
        Boundary = boundary;
        Actual = actual;
        Limit = limit;
    }

    /// <summary>Gets the stable machine-readable boundary name.</summary>
    [Id(0)]
    public string Boundary { get; private set; }

    /// <summary>Gets the measured element or canonical byte count.</summary>
    [Id(1)]
    public long Actual { get; private set; }

    /// <summary>Gets the enforced maximum.</summary>
    [Id(2)]
    public long Limit { get; private set; }

    private static string CreateMessage(string boundary, long actual, long limit)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(boundary);
        return $"Searchable storage capacity boundary '{boundary}' was exceeded: actual {actual}, limit {limit}.";
    }
}
