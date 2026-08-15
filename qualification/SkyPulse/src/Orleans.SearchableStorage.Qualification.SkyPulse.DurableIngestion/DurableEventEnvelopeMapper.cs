using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

/// <summary>
/// Maps the parser's closed metadata event into the closed PostgreSQL envelope. Repository
/// generation is derived only from durable account state; it is never trusted from the wire.
/// </summary>
public static class DurableEventEnvelopeMapper
{
    private const string LifecycleDomain = "orleans-searchable-storage-skypulse-lifecycle-v1\0";
    private const string RepositorySyncDomain = "orleans-searchable-storage-skypulse-repo-sync-v1\0";

    public static long ResolveRepositoryGeneration(
        IngestionEvent acceptedEvent,
        AccountStateSnapshot? currentAccount)
    {
        ArgumentNullException.ThrowIfNull(acceptedEvent);
        if (currentAccount is not null && currentAccount.AccountKey != acceptedEvent.AccountKey)
        {
            throw new ArgumentException("The durable account state does not belong to the accepted event.", nameof(currentAccount));
        }

        if (acceptedEvent is AccountLifecycleEvent
            {
                Status: AccountLifecycleStatus.Active,
            }
            && currentAccount is { Lifecycle: not DurableAccountLifecycle.Active })
        {
            return checked(currentAccount.RepositoryGeneration + 1);
        }

        return currentAccount?.RepositoryGeneration ?? 0;
    }

    public static DurableEventEnvelope Map(
        IngestionEvent acceptedEvent,
        DurableDeliveryReservation reservation,
        long repositoryGeneration)
    {
        ArgumentNullException.ThrowIfNull(acceptedEvent);
        ArgumentNullException.ThrowIfNull(reservation);
        ArgumentOutOfRangeException.ThrowIfNegative(repositoryGeneration);

        return acceptedEvent switch
        {
            RecordMutationEvent record => MapRecord(record, reservation, repositoryGeneration),
            AccountLifecycleEvent lifecycle => MapLifecycle(lifecycle, reservation, repositoryGeneration),
            RepositorySyncEvent sync => MapRepositorySync(sync, reservation, repositoryGeneration),
            _ => throw new InvalidOperationException("The accepted ingestion event kind is outside the closed contract."),
        };
    }

    private static DurableEventEnvelope MapRecord(
        RecordMutationEvent record,
        DurableDeliveryReservation reservation,
        long repositoryGeneration)
        => new(
            reservation.SourceInstanceId,
            reservation.TapDeliveryId,
            reservation.DeliveryDigest,
            record.SemanticKey.ToString(),
            record.AccountKey,
            repositoryGeneration,
            DurableEventKind.RecordMutation,
            reservation.FirstObservedAtMinuteUtc,
            record.Revision,
            Map(record.Collection),
            Map(record.Action),
            record.RecordKey,
            record.Cid,
            record.TargetAccountKey,
            record.IsDirectReply,
            record.IsLive);

    private static DurableEventEnvelope MapLifecycle(
        AccountLifecycleEvent lifecycle,
        DurableDeliveryReservation reservation,
        long repositoryGeneration)
    {
        var durableLifecycle = Map(lifecycle.Status);
        return new DurableEventEnvelope(
            reservation.SourceInstanceId,
            reservation.TapDeliveryId,
            reservation.DeliveryDigest,
            HashSemantic(LifecycleDomain, lifecycle.AccountKey, repositoryGeneration, (short)durableLifecycle, null),
            lifecycle.AccountKey,
            repositoryGeneration,
            DurableEventKind.AccountLifecycle,
            reservation.FirstObservedAtMinuteUtc,
            lifecycle: durableLifecycle);
    }

    private static DurableEventEnvelope MapRepositorySync(
        RepositorySyncEvent sync,
        DurableDeliveryReservation reservation,
        long repositoryGeneration)
        => new(
            reservation.SourceInstanceId,
            reservation.TapDeliveryId,
            reservation.DeliveryDigest,
            HashSemantic(RepositorySyncDomain, sync.AccountKey, repositoryGeneration, discriminator: 0, sync.Revision),
            sync.AccountKey,
            repositoryGeneration,
            DurableEventKind.RepositorySync,
            reservation.FirstObservedAtMinuteUtc,
            repositoryRevision: sync.Revision);

    private static DurableRecordKind Map(AtRecordKind value)
        => value switch
        {
            AtRecordKind.FeedPost => DurableRecordKind.FeedPost,
            AtRecordKind.FeedLike => DurableRecordKind.FeedLike,
            AtRecordKind.FeedRepost => DurableRecordKind.FeedRepost,
            AtRecordKind.GraphFollow => DurableRecordKind.GraphFollow,
            AtRecordKind.ActorProfile => DurableRecordKind.ActorProfile,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "The record kind is outside the closed contract."),
        };

    private static DurableRecordAction Map(RecordMutationAction value)
        => value switch
        {
            RecordMutationAction.Create => DurableRecordAction.Create,
            RecordMutationAction.Update => DurableRecordAction.Update,
            RecordMutationAction.Delete => DurableRecordAction.Delete,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "The record action is outside the closed contract."),
        };

    private static DurableAccountLifecycle Map(AccountLifecycleStatus value)
        => value switch
        {
            AccountLifecycleStatus.Active => DurableAccountLifecycle.Active,
            AccountLifecycleStatus.Deactivated => DurableAccountLifecycle.Deactivated,
            AccountLifecycleStatus.TakenDown => DurableAccountLifecycle.TakenDown,
            AccountLifecycleStatus.Suspended => DurableAccountLifecycle.Suspended,
            AccountLifecycleStatus.Deleted => DurableAccountLifecycle.Deleted,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "The lifecycle value is outside the closed contract."),
        };

    private static string HashSemantic(
        string domain,
        AccountKey accountKey,
        long repositoryGeneration,
        short discriminator,
        string? revision)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, Encoding.ASCII.GetBytes(domain));
        Span<byte> accountBytes = stackalloc byte[AccountKey.TextLength];
        if (!Encoding.ASCII.TryGetBytes(accountKey.ToString(), accountBytes, out var accountBytesWritten)
            || accountBytesWritten != accountBytes.Length)
        {
            throw new InvalidOperationException("The canonical account key did not encode to its fixed ASCII length.");
        }

        Append(hash, accountBytes);
        Span<byte> number = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(number, repositoryGeneration);
        Append(hash, number);
        Span<byte> kind = stackalloc byte[sizeof(short)];
        BinaryPrimitives.WriteInt16BigEndian(kind, discriminator);
        Append(hash, kind);
        Append(hash, revision is null ? [] : Encoding.UTF8.GetBytes(revision));
        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static void Append(IncrementalHash hash, ReadOnlySpan<byte> value)
    {
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(length, value.Length);
        hash.AppendData(length);
        hash.AppendData(value);
    }
}
