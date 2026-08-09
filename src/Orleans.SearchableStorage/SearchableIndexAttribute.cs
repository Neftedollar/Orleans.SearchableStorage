namespace Orleans.SearchableStorage;

/// <summary>
/// Identifies a grain-state property which is maintained in a secondary index.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class SearchableIndexAttribute : Attribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SearchableIndexAttribute"/> class.
    /// </summary>
    /// <param name="kind">The physical index kind.</param>
    public SearchableIndexAttribute(SearchableIndexKind kind)
    {
        Kind = kind;
    }

    /// <summary>
    /// Gets the physical index kind.
    /// </summary>
    public SearchableIndexKind Kind { get; }

    /// <summary>
    /// Gets or sets the stable index name. The property name is used when this value is not set.
    /// </summary>
    public string? Name { get; init; }
}

/// <summary>
/// Describes the secondary index maintained for a state property.
/// </summary>
public enum SearchableIndexKind
{
    /// <summary>
    /// Maps one exact value to matching grain identifiers.
    /// </summary>
    Hash = 0,

    /// <summary>
    /// Maintains ordered values for equality and range lookup.
    /// </summary>
    Range = 1,
}
