namespace Orleans.SearchableStorage.Tests.TestGrains;

[GenerateSerializer]
public sealed class SchemaMaterializationFailureState
{
    public const string FailureMessagePrefix =
        "Sensitive schema materialization diagnostic; raw indexed value=";
    private static int _throwOnIndexAccess;

    [Id(0)]
    public string StoredCity { get; set; } = string.Empty;

    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City
    {
        get
        {
            if (Volatile.Read(ref _throwOnIndexAccess) != 0)
            {
                // The remote rebuild diagnostic must not expose either this application-controlled
                // text or the indexed value embedded in it.
                throw new InvalidDataException($"{FailureMessagePrefix}{StoredCity}");
            }

            return StoredCity;
        }
    }

    public static bool ThrowOnIndexAccess
    {
        get => Volatile.Read(ref _throwOnIndexAccess) != 0;
        set => Volatile.Write(ref _throwOnIndexAccess, value ? 1 : 0);
    }
}
