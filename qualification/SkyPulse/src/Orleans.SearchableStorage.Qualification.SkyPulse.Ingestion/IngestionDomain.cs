using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;

/// <summary>
/// Identifies one supported AT Protocol record collection without retaining its textual name in reducer state.
/// </summary>
public enum AtRecordKind
{
    FeedPost,
    FeedLike,
    FeedRepost,
    GraphFollow,
    ActorProfile,
}

/// <summary>
/// Identifies a current-record mutation.
/// </summary>
public enum RecordMutationAction
{
    Create,
    Update,
    Delete,
}

/// <summary>
/// Describes the account lifecycle values emitted by TAP.
/// </summary>
public enum AccountLifecycleStatus
{
    Active,
    Deactivated,
    TakenDown,
    Suspended,
    Deleted,
}

/// <summary>
/// Identifies one record independently of a TAP delivery identifier.
/// </summary>
/// <remarks>
/// The value is a domain-separated SHA-256 digest over the exact DID, repository revision,
/// collection, record key, action, and CID. A TAP delivery identifier is intentionally excluded.
/// </remarks>
public readonly struct SemanticEventKey : IEquatable<SemanticEventKey>, IComparable<SemanticEventKey>
{
    private const string Domain = "orleans-searchable-storage-skypulse-event-v1\0";

    private readonly ulong _part0;
    private readonly ulong _part1;
    private readonly ulong _part2;
    private readonly ulong _part3;

    private SemanticEventKey(ReadOnlySpan<byte> digest)
    {
        _part0 = BinaryPrimitives.ReadUInt64BigEndian(digest);
        _part1 = BinaryPrimitives.ReadUInt64BigEndian(digest[8..]);
        _part2 = BinaryPrimitives.ReadUInt64BigEndian(digest[16..]);
        _part3 = BinaryPrimitives.ReadUInt64BigEndian(digest[24..]);
    }

    internal static SemanticEventKey Create(
        string did,
        string revision,
        string collection,
        string recordKey,
        string action,
        string? cid)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, Domain);
        Append(hash, did);
        Append(hash, revision);
        Append(hash, collection);
        Append(hash, recordKey);
        Append(hash, action);
        Append(hash, cid ?? string.Empty);

        Span<byte> digest = stackalloc byte[32];
        if (!hash.TryGetHashAndReset(digest, out var bytesWritten) || bytesWritten != digest.Length)
        {
            throw new CryptographicException("SHA-256 did not produce the expected digest length.");
        }

        return new SemanticEventKey(digest);
    }

    public int CompareTo(SemanticEventKey other)
    {
        var comparison = _part0.CompareTo(other._part0);
        if (comparison != 0)
        {
            return comparison;
        }

        comparison = _part1.CompareTo(other._part1);
        if (comparison != 0)
        {
            return comparison;
        }

        comparison = _part2.CompareTo(other._part2);
        return comparison != 0 ? comparison : _part3.CompareTo(other._part3);
    }

    public bool Equals(SemanticEventKey other)
        => _part0 == other._part0
            && _part1 == other._part1
            && _part2 == other._part2
            && _part3 == other._part3;

    public override bool Equals(object? obj) => obj is SemanticEventKey other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(_part0, _part1, _part2, _part3);

    public override string ToString()
    {
        Span<byte> digest = stackalloc byte[32];
        BinaryPrimitives.WriteUInt64BigEndian(digest, _part0);
        BinaryPrimitives.WriteUInt64BigEndian(digest[8..], _part1);
        BinaryPrimitives.WriteUInt64BigEndian(digest[16..], _part2);
        BinaryPrimitives.WriteUInt64BigEndian(digest[24..], _part3);
        return Convert.ToHexString(digest).ToLowerInvariant();
    }

    public static bool operator ==(SemanticEventKey left, SemanticEventKey right) => left.Equals(right);

    public static bool operator !=(SemanticEventKey left, SemanticEventKey right) => !left.Equals(right);

    public static bool operator <(SemanticEventKey left, SemanticEventKey right) => left.CompareTo(right) < 0;

    public static bool operator >(SemanticEventKey left, SemanticEventKey right) => left.CompareTo(right) > 0;

    public static bool operator <=(SemanticEventKey left, SemanticEventKey right) => left.CompareTo(right) <= 0;

    public static bool operator >=(SemanticEventKey left, SemanticEventKey right) => left.CompareTo(right) >= 0;

    private static void Append(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }
}

/// <summary>
/// Represents one source-neutral event accepted by the metadata reducer.
/// </summary>
public abstract record IngestionEvent
{
    private protected IngestionEvent(AccountKey accountKey)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        AccountKey = accountKey;
    }

    public AccountKey AccountKey { get; }
}

/// <summary>
/// Represents one sanitized record mutation.
/// </summary>
public sealed record RecordMutationEvent : IngestionEvent
{
    internal RecordMutationEvent(
        AccountKey accountKey,
        SemanticEventKey semanticKey,
        bool isLive,
        long observedAtMinuteUtc,
        string revision,
        AtRecordKind collection,
        string recordKey,
        RecordMutationAction action,
        string? cid,
        AccountKey? targetAccountKey,
        bool isDirectReply)
        : base(accountKey)
    {
        SemanticKey = semanticKey;
        IsLive = isLive;
        ObservedAtMinuteUtc = observedAtMinuteUtc;
        Revision = revision;
        Collection = collection;
        RecordKey = recordKey;
        Action = action;
        Cid = cid;
        TargetAccountKey = targetAccountKey;
        IsDirectReply = isDirectReply;
    }

    public SemanticEventKey SemanticKey { get; }

    public bool IsLive { get; }

    public long ObservedAtMinuteUtc { get; }

    public string Revision { get; }

    public AtRecordKind Collection { get; }

    public string RecordKey { get; }

    public RecordMutationAction Action { get; }

    public string? Cid { get; }

    public AccountKey? TargetAccountKey { get; }

    public bool IsDirectReply { get; }
}

/// <summary>
/// Represents an idempotent account lifecycle observation.
/// </summary>
public sealed record AccountLifecycleEvent : IngestionEvent
{
    internal AccountLifecycleEvent(AccountKey accountKey, AccountLifecycleStatus status)
        : base(accountKey)
    {
        Status = status;
    }

    public AccountLifecycleStatus Status { get; }
}

/// <summary>
/// Marks the acknowledged TAP barrier which completes one authoritative repository snapshot.
/// </summary>
public sealed record RepositorySyncEvent : IngestionEvent
{
    internal RepositorySyncEvent(AccountKey accountKey, string revision)
        : base(accountKey)
    {
        Revision = revision;
    }

    /// <summary>
    /// Gets the canonical AT Protocol TID of the authoritative repository snapshot.
    /// </summary>
    public string Revision { get; }
}

/// <summary>
/// Identifies why an input or reducer transition was quarantined.
/// </summary>
public enum QuarantineCode
{
    EventTooLarge,
    MalformedJson,
    InvalidRoot,
    MissingProperty,
    UnexpectedProperty,
    InvalidValue,
    UnsupportedEventType,
    UnsupportedCollection,
    MissingPriorRecord,
    ConflictingRevision,
    InactiveAccountMutation,
    ReconciliationIncomplete,
    ReconciliationRevisionConflict,
}

/// <summary>
/// Contains an explicit accepted-or-quarantined parser decision.
/// </summary>
public sealed record IngestionParseDecision
{
    private IngestionParseDecision(
        ulong? tapDeliveryId,
        IngestionEvent? acceptedEvent,
        QuarantineCode? quarantineCode,
        string? quarantineMessage)
    {
        TapDeliveryId = tapDeliveryId;
        AcceptedEvent = acceptedEvent;
        QuarantineCode = quarantineCode;
        QuarantineMessage = quarantineMessage;
    }

    public ulong? TapDeliveryId { get; }

    public IngestionEvent? AcceptedEvent { get; }

    public QuarantineCode? QuarantineCode { get; }

    public string? QuarantineMessage { get; }

    public bool IsAccepted => AcceptedEvent is not null;

    internal static IngestionParseDecision Accept(ulong tapDeliveryId, IngestionEvent acceptedEvent)
        => new(tapDeliveryId, acceptedEvent, null, null);

    internal static IngestionParseDecision Quarantine(QuarantineCode code, string message)
        => new(null, null, code, message);

    internal static IngestionParseDecision Quarantine(ulong tapDeliveryId, QuarantineCode code, string message)
        => new(tapDeliveryId, null, code, message);
}
