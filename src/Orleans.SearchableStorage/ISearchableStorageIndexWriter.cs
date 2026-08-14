using Orleans.Runtime;

namespace Orleans.SearchableStorage;

/// <summary>
/// Maintains a searchable index without storing application payloads.
/// </summary>
/// <remarks>
/// <para>
/// Mutations are unconditional replacements keyed by state name and grain identifier. The caller
/// owns persistence of the application state and must serialize calls for each key. Delayed or
/// reordered calls are applied in arrival order; this writer does not reject stale application
/// events.
/// </para>
/// <para>
/// Cancellation stops waiting for the Orleans call but cannot recall a mutation which has
/// already reached the owning partition. Repeating the same mutation after an ambiguous failure
/// converges to the same indexed values, although it may append another durable journal entry.
/// </para>
/// </remarks>
public interface ISearchableStorageIndexWriter
{
    /// <summary>
    /// Replaces all indexed values for one state record using the properties marked with
    /// searchable-index attributes. The supplied state is inspected but is not stored as a
    /// payload.
    /// </summary>
    /// <typeparam name="TState">The state type whose indexed properties are extracted.</typeparam>
    /// <param name="stateName">The logical persistent-state name.</param>
    /// <param name="grainId">The grain identifier returned by matching queries.</param>
    /// <param name="state">The current application state.</param>
    /// <param name="cancellationToken">A token which stops waiting for completion.</param>
    /// <returns>A task representing the index mutation.</returns>
    Task UpsertAsync<TState>(
        string stateName,
        GrainId grainId,
        TState state,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes one state record and all of its indexed values. Removing an absent record succeeds.
    /// </summary>
    /// <typeparam name="TState">
    /// The registered state type used to validate the managed index schema.
    /// </typeparam>
    /// <param name="stateName">The logical persistent-state name.</param>
    /// <param name="grainId">The grain identifier to remove.</param>
    /// <param name="cancellationToken">A token which stops waiting for completion.</param>
    /// <returns>A task representing the index mutation.</returns>
    Task RemoveAsync<TState>(
        string stateName,
        GrainId grainId,
        CancellationToken cancellationToken = default);
}
