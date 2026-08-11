using System.Text;
using Orleans.Storage;

namespace Orleans.SearchableStorage;

/// <summary>
/// Configures one searchable grain-storage provider.
/// </summary>
public sealed class SearchableStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    /// Gets the bounded-query and continuation-protection configuration for this provider.
    /// </summary>
    public SearchableStorageQueryOptions Query { get; } = new();

    /// <summary>
    /// Gets the bounded live-movement settings used by the keyed admin client.
    /// </summary>
    public SearchableStorageMovementOptions Movement { get; } = new();

    /// <summary>
    /// Gets or sets the immutable initial physical-owner count used to seed a provider namespace.
    /// </summary>
    /// <remarks>
    /// This value is part of the persisted layout identity and must not be changed after data is
    /// written. Live rebalancing can assign virtual slots to additional owner indices without
    /// changing this initial count.
    /// </remarks>
    public int PartitionCount { get; set; } = 32;

    /// <summary>
    /// Gets or sets the target used to seed the immutable virtual-slot count for a new or
    /// version-3 layout.
    /// </summary>
    /// <remarks>
    /// The persisted slot count is rounded up to the smallest multiple of
    /// <see cref="PartitionCount"/> which reaches this target. The target must remain a valid,
    /// addressable configured value, but it is not compared with an existing version-4 layout, so
    /// changing a future library default cannot invalidate that namespace.
    /// </remarks>
    public int VirtualSlotTargetCount { get; set; } = Storage.StorageLayout.DefaultVirtualSlotTargetCount;

    /// <summary>
    /// Gets or sets the maximum number of operations stored in one journal segment.
    /// </summary>
    /// <remarks>
    /// This value is part of the persisted data layout and must not be changed after data is written.
    /// It cannot exceed <see cref="SearchableStorageCapacityLimits.MaximumJournalSegmentEntries"/>.
    /// </remarks>
    public int JournalSegmentCapacity { get; set; } = Storage.StoragePersistence.DefaultJournalSegmentCapacity;

    /// <summary>
    /// Gets or sets the maximum number of journal entries replayed while activating a partition.
    /// </summary>
    /// <remarks>
    /// This value is part of the persisted data layout and must not be changed after data is written.
    /// It cannot exceed <see cref="SearchableStorageCapacityLimits.MaximumJournalReplayEntries"/>.
    /// </remarks>
    public int MaximumJournalReplayEntries { get; set; } = Storage.StoragePersistence.DefaultMaximumJournalReplayEntries;

    /// <summary>
    /// Gets or sets the number of committed journal entries after which compaction is requested.
    /// </summary>
    /// <remarks>
    /// This is an operational setting and can be changed without migrating persisted data. It must
    /// not exceed <see cref="MaximumJournalReplayEntries"/>.
    /// </remarks>
    public int CompactionThreshold { get; set; } = Storage.StoragePersistence.DefaultCompactionThreshold;

    /// <inheritdoc />
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = default!;
}

/// <summary>
/// Configures bounded virtual-slot transfer pages for one searchable-storage provider.
/// </summary>
public sealed class SearchableStorageMovementOptions
{
    /// <summary>The default record-count ceiling for one transfer page.</summary>
    public const int DefaultTransferPageRecordLimit = 128;

    /// <summary>The hard record-count ceiling for one transfer page.</summary>
    public const int MaximumTransferPageRecordLimit = 1_024;

    /// <summary>The default canonical encoded-byte target for one transfer page.</summary>
    public const int DefaultTransferPageByteTarget = 256 * 1_024;

    /// <summary>The hard canonical encoded-byte target accepted for one transfer page.</summary>
    public const int MaximumTransferPageByteTarget = 4 * 1_024 * 1_024;

    /// <summary>
    /// Gets or sets the maximum number of records returned by one source export page.
    /// </summary>
    public int TransferPageRecordLimit { get; set; } = DefaultTransferPageRecordLimit;

    /// <summary>
    /// Gets or sets the canonical encoded-byte target for one source export page.
    /// </summary>
    /// <remarks>
    /// The movement protocol counts its deterministic canonical record encoding, not physical
    /// storage or Orleans transport bytes. A single accepted record larger than this target is
    /// returned alone. The target therefore bounds every multi-record page under that measure but
    /// is not an absolute maximum record-size or network-payload policy.
    /// </remarks>
    public int TransferPageByteTarget { get; set; } = DefaultTransferPageByteTarget;
}

/// <summary>
/// Configures bounded query execution for one searchable-storage provider.
/// </summary>
public sealed class SearchableStorageQueryOptions
{
    /// <summary>The recommended public page size and built-in compatibility traversal page size.</summary>
    public const int DefaultPageSize = 128;

    /// <summary>The hard upper bound for a requested public page size.</summary>
    public const int MaximumPageSize = 1_024;

    /// <summary>The default logical-work budget for one partition turn.</summary>
    public const long DefaultPartitionWorkBudget = 65_536;

    /// <summary>The hard upper bound for one partition turn's logical-work budget.</summary>
    public const long MaximumPartitionWorkBudget = 1_048_576;

    /// <summary>The default item ceiling for one partition response.</summary>
    public const int DefaultPartitionResponseItems = 1_024;

    /// <summary>The hard item ceiling for one partition response.</summary>
    public const int MaximumPartitionResponseItems = 4_096;

    /// <summary>The default encoded-byte ceiling for one partition response.</summary>
    public const int DefaultPartitionResponseBytes = 256 * 1_024;

    /// <summary>The hard encoded-byte ceiling for one partition response.</summary>
    public const int MaximumPartitionResponseBytes = 1_024 * 1_024;

    /// <summary>The default coordinator item-buffer ceiling.</summary>
    public const int DefaultCoordinatorBufferedItems = 8_192;

    /// <summary>The hard coordinator item-buffer ceiling.</summary>
    public const int MaximumCoordinatorBufferedItems = 65_536;

    /// <summary>The default coordinator encoded-byte-buffer ceiling.</summary>
    public const int DefaultCoordinatorBufferedBytes = 2 * 1_024 * 1_024;

    /// <summary>The hard coordinator encoded-byte-buffer ceiling.</summary>
    public const int MaximumCoordinatorBufferedBytes = 16 * 1_024 * 1_024;

    /// <summary>The default encoded-byte ceiling for one public page.</summary>
    public const int DefaultPageBytes = 1 * 1_024 * 1_024;

    /// <summary>The hard encoded-byte ceiling for one public page.</summary>
    public const int MaximumPageBytes = 4 * 1_024 * 1_024;

    /// <summary>The default encoded continuation-token length ceiling.</summary>
    public const int DefaultContinuationTokenBytes = 2_048;

    /// <summary>The hard encoded continuation-token length ceiling.</summary>
    public const int MaximumContinuationTokenBytes = 32 * 1_024;

    /// <summary>The default aggregate logical-work ceiling for an all-results compatibility query.</summary>
    public const long DefaultLegacyAggregateWork = 4_194_304;

    /// <summary>The hard aggregate logical-work ceiling for an all-results compatibility query.</summary>
    public const long MaximumLegacyAggregateWork = 67_108_864;

    /// <summary>The default result-item ceiling for an all-results compatibility query.</summary>
    public const int DefaultLegacyResultItems = 8_192;

    /// <summary>The hard result-item ceiling for an all-results compatibility query.</summary>
    public const int MaximumLegacyResultItems = 100_000;

    /// <summary>The default result-byte ceiling for an all-results compatibility query.</summary>
    public const int DefaultLegacyResultBytes = 8 * 1_024 * 1_024;

    /// <summary>The hard result-byte ceiling for an all-results compatibility query.</summary>
    public const int MaximumLegacyResultBytes = 64 * 1_024 * 1_024;

    /// <summary>The default round ceiling for an all-results compatibility query.</summary>
    public const int DefaultLegacyRounds = 64;

    /// <summary>The hard round ceiling for an all-results compatibility query.</summary>
    public const int MaximumLegacyRounds = 1_024;

    /// <summary>The default maximum top-N facet size.</summary>
    public const int DefaultFacetTopN = 128;

    /// <summary>The hard maximum top-N facet size.</summary>
    public const int MaximumFacetTopN = 1_024;

    /// <summary>The default aggregate logical-work ceiling for one terminal facet.</summary>
    public const long DefaultFacetAggregateWork = 4_194_304;

    /// <summary>The hard aggregate logical-work ceiling for one terminal facet.</summary>
    public const long MaximumFacetAggregateWork = 67_108_864;

    /// <summary>The default candidate/probe turn ceiling for one terminal facet.</summary>
    public const int DefaultFacetRounds = 2_048;

    /// <summary>The hard candidate/probe turn ceiling for one terminal facet.</summary>
    public const int MaximumFacetRounds = 32_768;

    /// <summary>The default aggregate candidate-item ceiling for one terminal facet.</summary>
    public const int DefaultFacetAggregateItems = 8_192;

    /// <summary>The hard aggregate candidate-item ceiling for one terminal facet.</summary>
    public const int MaximumFacetAggregateItems = 65_536;

    /// <summary>The default aggregate encoded candidate-byte ceiling for one terminal facet.</summary>
    public const int DefaultFacetAggregateBytes = 8 * 1_024 * 1_024;

    /// <summary>The hard aggregate encoded candidate-byte ceiling for one terminal facet.</summary>
    public const int MaximumFacetAggregateBytes = 64 * 1_024 * 1_024;

    /// <summary>Gets or sets the provider's accepted public page-size ceiling.</summary>
    public int PageSizeLimit { get; set; } = MaximumPageSize;

    /// <summary>Gets or sets the logical-work budget for one partition turn.</summary>
    public long PartitionWorkBudget { get; set; } = DefaultPartitionWorkBudget;

    /// <summary>Gets or sets the item ceiling for one partition response before owner-count apportionment.</summary>
    public int PartitionResponseItemLimit { get; set; } = DefaultPartitionResponseItems;

    /// <summary>Gets or sets the encoded-byte ceiling for one partition response before owner-count apportionment.</summary>
    public int PartitionResponseByteLimit { get; set; } = DefaultPartitionResponseBytes;

    /// <summary>Gets or sets the coordinator item-buffer ceiling.</summary>
    public int CoordinatorBufferedItemLimit { get; set; } = DefaultCoordinatorBufferedItems;

    /// <summary>Gets or sets the coordinator encoded-byte-buffer ceiling.</summary>
    public int CoordinatorBufferedByteLimit { get; set; } = DefaultCoordinatorBufferedBytes;

    /// <summary>Gets or sets the encoded-byte ceiling for one public page.</summary>
    public int PageByteLimit { get; set; } = DefaultPageBytes;

    /// <summary>Gets or sets the maximum accepted encoded continuation-token length.</summary>
    public int ContinuationTokenByteLimit { get; set; } = DefaultContinuationTokenBytes;

    /// <summary>Gets or sets the aggregate logical-work ceiling for an all-results compatibility query.</summary>
    public long LegacyAggregateWorkLimit { get; set; } = DefaultLegacyAggregateWork;

    /// <summary>Gets or sets the result-item ceiling for an all-results compatibility query.</summary>
    public int LegacyResultItemLimit { get; set; } = DefaultLegacyResultItems;

    /// <summary>Gets or sets the result-byte ceiling for an all-results compatibility query.</summary>
    public int LegacyResultByteLimit { get; set; } = DefaultLegacyResultBytes;

    /// <summary>Gets or sets the round ceiling for an all-results compatibility query.</summary>
    public int LegacyRoundLimit { get; set; } = DefaultLegacyRounds;

    /// <summary>Gets or sets the maximum accepted top-N value-count request.</summary>
    public int FacetTopNLimit { get; set; } = DefaultFacetTopN;

    /// <summary>Gets or sets the aggregate logical-work ceiling for one terminal facet.</summary>
    public long FacetAggregateWorkLimit { get; set; } = DefaultFacetAggregateWork;

    /// <summary>Gets or sets the candidate/probe turn ceiling for one terminal facet.</summary>
    public int FacetRoundLimit { get; set; } = DefaultFacetRounds;

    /// <summary>Gets or sets the aggregate candidate-item ceiling for one terminal facet.</summary>
    public int FacetAggregateItemLimit { get; set; } = DefaultFacetAggregateItems;

    /// <summary>Gets or sets the aggregate encoded candidate-byte ceiling for one terminal facet.</summary>
    public int FacetAggregateByteLimit { get; set; } = DefaultFacetAggregateBytes;

    /// <summary>Gets the provider-scoped continuation-protection key ring.</summary>
    public SearchableStorageContinuationProtectionOptions ContinuationProtection { get; } = new();
}

/// <summary>
/// Configures provider-scoped authenticated encryption for continuation tokens.
/// </summary>
public sealed class SearchableStorageContinuationProtectionOptions
{
    /// <summary>
    /// Gets or sets the key used to encrypt new tokens. Public paging requires this value, while
    /// point storage operations and token-free compatibility queries do not.
    /// </summary>
    public SearchableStorageContinuationKey? CurrentKey { get; set; }

    /// <summary>
    /// Gets the explicit decrypt-only keys accepted during rotation.
    /// </summary>
    public IList<SearchableStorageContinuationKey> DecryptionKeys { get; } = [];
}

/// <summary>
/// Holds one AES-256 continuation-protection key and its stable operational identifier.
/// </summary>
public sealed class SearchableStorageContinuationKey
{
    internal const int RequiredKeyBytes = 32;
    internal const int MaximumKeyIdBytes = 64;
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);
    private readonly byte[] _keyMaterial;

    /// <summary>
    /// Initializes a continuation-protection key, defensively copying its material.
    /// </summary>
    /// <param name="keyId">The stable, non-secret operational key identifier.</param>
    /// <param name="keyMaterial">Exactly 32 bytes of application secret key material.</param>
    /// <exception cref="ArgumentException">The identifier is blank, too long, or invalid UTF-8, or the material is not 32 bytes.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="keyMaterial"/> is null.</exception>
    public SearchableStorageContinuationKey(string keyId, byte[] keyMaterial)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyId);
        ArgumentNullException.ThrowIfNull(keyMaterial);

        int keyIdBytes;
        try
        {
            keyIdBytes = StrictUtf8.GetByteCount(keyId);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException("The key identifier must contain valid Unicode text.", nameof(keyId), exception);
        }

        if (keyIdBytes > MaximumKeyIdBytes)
        {
            throw new ArgumentException(
                $"The UTF-8 key identifier must not exceed {MaximumKeyIdBytes} bytes.",
                nameof(keyId));
        }

        if (keyMaterial.Length != RequiredKeyBytes)
        {
            throw new ArgumentException(
                $"Continuation-protection key material must contain exactly {RequiredKeyBytes} bytes.",
                nameof(keyMaterial));
        }

        KeyId = keyId;
        _keyMaterial = [.. keyMaterial];
    }

    /// <summary>Gets the stable, non-secret operational key identifier.</summary>
    public string KeyId { get; }

    internal byte[] CopyKeyMaterial() => [.. _keyMaterial];
}
