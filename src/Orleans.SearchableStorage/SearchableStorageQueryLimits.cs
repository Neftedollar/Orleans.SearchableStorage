namespace Orleans.SearchableStorage;

/// <summary>
/// Defines fixed public bounds for the searchable-storage query expression surface.
/// </summary>
public static class SearchableStorageQueryLimits
{
    /// <summary>The maximum raw input item count accepted by one <c>WhereIn</c> operator.</summary>
    public const int MaximumWhereInValues = 64;
}
