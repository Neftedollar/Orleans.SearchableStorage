namespace Orleans.SearchableStorage;

/// <summary>
/// Reports a malformed, unauthenticated, or otherwise inapplicable continuation token.
/// </summary>
public sealed class SearchableStorageInvalidContinuationTokenException : Exception
{
    /// <summary>Initializes an exception with the default message.</summary>
    public SearchableStorageInvalidContinuationTokenException()
        : base("The searchable-storage continuation token is invalid.")
    {
    }

    /// <summary>Initializes an exception with a message.</summary>
    /// <param name="message">The exception message.</param>
    public SearchableStorageInvalidContinuationTokenException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes an exception with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The underlying exception.</param>
    public SearchableStorageInvalidContinuationTokenException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Reports an authenticated continuation which names an obsolete storage layout.
/// </summary>
public sealed class SearchableStorageStaleContinuationTokenException : Exception
{
    /// <summary>Initializes an exception with the default message.</summary>
    public SearchableStorageStaleContinuationTokenException()
        : base("The searchable-storage continuation token refers to a stale routing layout.")
    {
    }

    /// <summary>Initializes an exception with a message.</summary>
    /// <param name="message">The exception message.</param>
    public SearchableStorageStaleContinuationTokenException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes an exception with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The underlying exception.</param>
    public SearchableStorageStaleContinuationTokenException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Reports that continuation protection required by public paging is not configured correctly.
/// </summary>
public sealed class SearchableStorageQueryConfigurationException : InvalidOperationException
{
    /// <summary>Initializes an exception with the default message.</summary>
    public SearchableStorageQueryConfigurationException()
        : base("Searchable-storage query paging is not configured correctly.")
    {
    }

    /// <summary>Initializes an exception with a message.</summary>
    /// <param name="message">The exception message.</param>
    public SearchableStorageQueryConfigurationException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes an exception with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The underlying exception.</param>
    public SearchableStorageQueryConfigurationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Reports that a searchable-storage query cannot complete or make progress within its configured
/// work, item, byte, or round ceilings.
/// </summary>
public sealed class SearchableStorageQueryLimitExceededException : InvalidOperationException
{
    /// <summary>Initializes an exception with the default message.</summary>
    public SearchableStorageQueryLimitExceededException()
        : base("The searchable-storage query exceeded a configured execution limit.")
    {
    }

    /// <summary>Initializes an exception with a message.</summary>
    /// <param name="message">The exception message.</param>
    public SearchableStorageQueryLimitExceededException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes an exception with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The underlying exception.</param>
    public SearchableStorageQueryLimitExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
