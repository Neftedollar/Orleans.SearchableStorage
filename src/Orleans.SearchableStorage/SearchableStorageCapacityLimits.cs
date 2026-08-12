namespace Orleans.SearchableStorage;

/// <summary>
/// Defines the fixed capacity envelope enforced by searchable storage.
/// </summary>
/// <remarks>
/// Byte limits use the deterministic canonical measure documented in
/// <c>docs/storage-capacity-limits.md</c>. They are not Orleans transport or physical-provider
/// byte limits. These constants are deliberately not configurable: every participant in a
/// durable partition must enforce the same envelope during writes, maintenance, and recovery.
/// </remarks>
public static class SearchableStorageCapacityLimits
{
    /// <summary>The maximum byte count in the type component of a stored GrainId.</summary>
    public const int MaximumGrainTypeBytes = 1_024;

    /// <summary>The maximum byte count in the key component of a stored GrainId.</summary>
    public const int MaximumGrainKeyBytes = 4_096;

    /// <summary>The maximum serialized state payload for one record.</summary>
    public const int MaximumRecordPayloadBytes = 4 * 1_024 * 1_024;

    /// <summary>The maximum canonical UTF-16 byte count, including its length prefix, for a record key.</summary>
    public const int MaximumRecordKeyCanonicalBytes = 16 * 1_024;

    /// <summary>The maximum number of index entries produced for one record.</summary>
    public const int MaximumIndexEntriesPerRecord = 256;

    /// <summary>The maximum number of index entries produced for one scope in one record.</summary>
    public const int MaximumIndexEntriesPerScope = 64;

    /// <summary>The maximum raw input item count accepted by one <c>WhereIn</c> operator.</summary>
    public const int MaximumWhereInValues = MaximumIndexEntriesPerScope;

    /// <summary>The maximum canonical byte count for one index entry.</summary>
    public const int MaximumIndexEntryCanonicalBytes = 64 * 1_024;

    /// <summary>The maximum aggregate canonical index-entry byte count for one record.</summary>
    public const int MaximumIndexBytesPerRecord = 512 * 1_024;

    /// <summary>
    /// The maximum total canonical byte count for one stored record and its key: the payload and
    /// aggregate-index ceilings plus 256 KiB for identity, ETag, fingerprint, and framing.
    /// </summary>
    public const int MaximumRecordCanonicalBytes =
        MaximumRecordPayloadBytes + MaximumIndexBytesPerRecord + (256 * 1_024);

    /// <summary>The maximum number of records in one partition snapshot.</summary>
    public const int MaximumSnapshotRecords = 1_000_000;

    /// <summary>The maximum aggregate canonical record byte count in one partition snapshot.</summary>
    public const long MaximumSnapshotCanonicalBytes = 512L * 1_024 * 1_024;

    /// <summary>The maximum canonical byte count for one journal entry.</summary>
    public const int MaximumJournalEntryCanonicalBytes = 5 * 1_024 * 1_024;

    /// <summary>The maximum number of entries in one physical journal segment.</summary>
    public const int MaximumJournalSegmentEntries = 64;

    /// <summary>The maximum aggregate canonical entry byte count in one journal segment.</summary>
    public const long MaximumJournalSegmentCanonicalBytes =
        (long)MaximumJournalEntryCanonicalBytes * MaximumJournalSegmentEntries;

    /// <summary>The maximum configured committed journal tail recovered after a snapshot.</summary>
    public const int MaximumJournalReplayEntries = 65_536;
}
