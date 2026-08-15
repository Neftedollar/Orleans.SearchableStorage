using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion.Tests;

internal static class TestEvents
{
    public const string ActorDid = "did:plc:actor";
    public const string TargetDid = "did:plc:target";
    public const string OtherTargetDid = "did:plc:other-target";
    public const string Rev0 = "3jzfcijpj2z22";
    public const string RevA = "3jzfcijpj2z2a";
    public const string RevB = "3jzfcijpj2z2b";
    public const string RevC = "3jzfcijpj2z2c";
    public const string RevD = "3jzfcijpj2z2d";
    public const string RevE = "3jzfcijpj2z2e";
    public const string RevF = "3jzfcijpj2z2f";

    public static string Post(
        ulong id,
        bool live,
        string did = ActorDid,
        string revision = RevA,
        string recordKey = "post-1",
        string action = "create",
        string? replyTargetDid = null,
        Action<Dictionary<string, object?>>? mutateRecord = null)
    {
        Dictionary<string, object?> metadata = new(StringComparer.Ordinal);
        if (replyTargetDid is not null)
        {
            metadata["reply_parent_uri"] = AtUri(replyTargetDid, "parent");
        }

        mutateRecord?.Invoke(metadata);
        return Record(
            id,
            live,
            did,
            revision,
            "app.bsky.feed.post",
            recordKey,
            action,
            metadata.Count == 0 ? null : metadata);
    }

    public static string Follow(
        ulong id,
        bool live,
        string targetDid = TargetDid,
        string did = ActorDid,
        string revision = RevA,
        string recordKey = "follow-1",
        string action = "create")
        => Record(
            id,
            live,
            did,
            revision,
            "app.bsky.graph.follow",
            recordKey,
            action,
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["follow_subject_did"] = targetDid,
            });

    public static string Like(
        ulong id,
        bool live,
        string targetDid = TargetDid,
        string did = ActorDid,
        string revision = RevA,
        string recordKey = "like-1",
        string action = "create")
        => SubjectRecord(
            id,
            live,
            "app.bsky.feed.like",
            targetDid,
            did,
            revision,
            recordKey,
            action);

    public static string Repost(
        ulong id,
        bool live,
        string targetDid = TargetDid,
        string did = ActorDid,
        string revision = RevA,
        string recordKey = "repost-1",
        string action = "create")
        => SubjectRecord(
            id,
            live,
            "app.bsky.feed.repost",
            targetDid,
            did,
            revision,
            recordKey,
            action);

    public static string Profile(
        ulong id,
        bool live,
        string did = ActorDid,
        string revision = RevA,
        string recordKey = "self",
        string action = "create",
        Action<Dictionary<string, object?>>? mutateRecord = null)
    {
        Dictionary<string, object?> metadata = new(StringComparer.Ordinal);
        mutateRecord?.Invoke(metadata);
        return Record(
            id,
            live,
            did,
            revision,
            "app.bsky.actor.profile",
            recordKey,
            action,
            metadata.Count == 0 ? null : metadata);
    }

    public static string Delete(
        ulong id,
        bool live,
        string collection,
        string recordKey,
        string revision,
        string did = ActorDid)
        => Record(id, live, did, revision, collection, recordKey, "delete", null);

    public static string Identity(
        ulong id,
        string status,
        bool isActive,
        string did = ActorDid,
        Action<Dictionary<string, object?>>? mutateIdentity = null)
    {
        Dictionary<string, object?> identity = new(StringComparer.Ordinal)
        {
            ["did"] = did,
            ["is_active"] = isActive,
            ["status"] = status,
        };
        mutateIdentity?.Invoke(identity);

        return JsonSerializer.Serialize(
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["id"] = id,
                ["type"] = "identity",
                ["identity"] = identity,
            });
    }

    public static string RepositorySync(
        ulong id,
        string revision = RevA,
        string did = ActorDid,
        string status = "active",
        Action<Dictionary<string, object?>>? mutateRepositorySync = null)
    {
        Dictionary<string, object?> repositorySync = new(StringComparer.Ordinal)
        {
            ["did"] = did,
            ["rev"] = revision,
            ["status"] = status,
        };
        mutateRepositorySync?.Invoke(repositorySync);

        return JsonSerializer.Serialize(
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["id"] = id,
                ["type"] = "repo_sync",
                ["repo_sync"] = repositorySync,
            });
    }

    public static RecordMutationEvent ParseRecord(string json, DateTimeOffset observedAtUtc)
    {
        var decision = SanitizedTapEventParser.Parse(json, observedAtUtc);
        Assert.True(decision.IsAccepted, decision.QuarantineMessage);
        return Assert.IsType<RecordMutationEvent>(decision.AcceptedEvent);
    }

    public static AccountLifecycleEvent ParseIdentity(string json, DateTimeOffset observedAtUtc)
    {
        var decision = SanitizedTapEventParser.Parse(json, observedAtUtc);
        Assert.True(decision.IsAccepted, decision.QuarantineMessage);
        return Assert.IsType<AccountLifecycleEvent>(decision.AcceptedEvent);
    }

    public static RepositorySyncEvent ParseRepositorySync(string json, DateTimeOffset observedAtUtc)
    {
        var decision = SanitizedTapEventParser.Parse(json, observedAtUtc);
        Assert.True(decision.IsAccepted, decision.QuarantineMessage);
        return Assert.IsType<RepositorySyncEvent>(decision.AcceptedEvent);
    }

    private static string SubjectRecord(
        ulong id,
        bool live,
        string collection,
        string targetDid,
        string did,
        string revision,
        string recordKey,
        string action)
        => Record(
            id,
            live,
            did,
            revision,
            collection,
            recordKey,
            action,
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["subject_uri"] = AtUri(targetDid, "subject"),
            });

    private static string Record(
        ulong id,
        bool live,
        string did,
        string revision,
        string collection,
        string recordKey,
        string action,
        Dictionary<string, object?>? metadata)
    {
        Dictionary<string, object?> envelope = new(StringComparer.Ordinal)
        {
            ["live"] = live,
            ["did"] = did,
            ["rev"] = revision,
            ["collection"] = collection,
            ["rkey"] = recordKey,
            ["action"] = action,
        };
        if (action != "delete")
        {
            envelope["cid"] = $"bafy-{revision}-{recordKey}";
            envelope["metadata_status"] = "valid";
            if (metadata is not null)
            {
                envelope["metadata"] = metadata;
            }
        }

        return JsonSerializer.Serialize(
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["id"] = id,
                ["type"] = "record",
                ["record"] = envelope,
            });
    }

    private static string AtUri(string did, string recordKey)
        => $"at://{did}/app.bsky.feed.post/{recordKey}";
}
