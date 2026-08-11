namespace Orleans.SearchableStorage;

/// <summary>
/// Indicates that an explicitly registered index schema is not the active durable generation.
/// </summary>
[GenerateSerializer]
public sealed class SearchableStorageIndexSchemaException : InvalidOperationException
{
    /// <summary>Initializes an exception with the stable default message.</summary>
    public SearchableStorageIndexSchemaException()
        : base("The registered searchable index schema is not active.")
    {
    }

    /// <summary>Initializes a schema lifecycle exception.</summary>
    /// <param name="message">A human-readable recovery instruction.</param>
    public SearchableStorageIndexSchemaException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a schema lifecycle exception with an underlying failure.</summary>
    /// <param name="message">A human-readable recovery instruction.</param>
    /// <param name="innerException">The underlying failure.</param>
    public SearchableStorageIndexSchemaException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
