using System.Buffers;
using System.Text;
using System.Text.Json;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;

/// <summary>
/// Parses the strict metadata-only form of a TAP outbox event.
/// </summary>
/// <remarks>
/// The accepted shape deliberately excludes handles, post text, profile fields, media,
/// languages, labels, timestamps from records, CIDs from referenced records, and arbitrary
/// extension properties. Record references are reduced to account keys and are not retained as
/// AT URIs.
/// </remarks>
public static class SanitizedTapEventParser
{
    public const int MaximumEventBytes = 16 * 1024;

    private const int MaximumDidLength = 2_048;
    private const int MaximumTokenLength = 1_024;
    private const int MaximumRecordKeyLength = 512;
    private const int MaximumCidLength = 256;

    private const string FeedPost = "app.bsky.feed.post";
    private const string FeedLike = "app.bsky.feed.like";
    private const string FeedRepost = "app.bsky.feed.repost";
    private const string GraphFollow = "app.bsky.graph.follow";
    private const string ActorProfile = "app.bsky.actor.profile";

    private static readonly SearchValues<char> TidFirstCharacters =
        SearchValues.Create("234567abcdefghij");
    private static readonly SearchValues<char> TidCharacters =
        SearchValues.Create("234567abcdefghijklmnopqrstuvwxyz");

    /// <summary>
    /// Parses one TAP JSON message without throwing for source-controlled malformed input.
    /// </summary>
    /// <param name="json">The complete TAP outbox message.</param>
    /// <param name="observedAtUtc">
    /// The consumer-controlled observation time. TAP does not put a trustworthy event time in
    /// its envelope, so this value is intentionally supplied outside the source JSON.
    /// </param>
    public static IngestionParseDecision Parse(string? json, DateTimeOffset observedAtUtc)
    {
        if (json is null)
        {
            return IngestionParseDecision.Quarantine(
                QuarantineCode.MalformedJson,
                "The TAP message is null.");
        }

        if (Encoding.UTF8.GetByteCount(json) > MaximumEventBytes)
        {
            return IngestionParseDecision.Quarantine(
                QuarantineCode.EventTooLarge,
                $"The TAP message exceeds {MaximumEventBytes} UTF-8 bytes.");
        }

        var observedAtMinuteUtc = observedAtUtc.ToUnixTimeSeconds() / 60;
        if (observedAtMinuteUtc < 0)
        {
            return IngestionParseDecision.Quarantine(
                QuarantineCode.InvalidValue,
                "The consumer observation time precedes the Unix epoch.");
        }

        ulong? recognizedDeliveryId = null;
        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            RequireObject(root, "root");
            if (root.TryGetProperty("id", out var idProperty)
                && idProperty.ValueKind == JsonValueKind.Number
                && idProperty.TryGetUInt64(out var deliveryId)
                && deliveryId > 0)
            {
                recognizedDeliveryId = deliveryId;
            }

            var type = RequireString(root, "type", MaximumTokenLength);
            return type switch
            {
                "record" => ParseRecord(root, observedAtMinuteUtc),
                "identity" => ParseIdentity(root),
                "repo_sync" => ParseRepositorySync(root),
                _ => throw Contract(
                    QuarantineCode.UnsupportedEventType,
                    "The TAP event type is not supported."),
            };
        }
        catch (JsonException)
        {
            return IngestionParseDecision.Quarantine(
                QuarantineCode.MalformedJson,
                "The TAP message is not valid JSON.");
        }
        catch (ContractException exception)
        {
            var deliveryId = exception.DeliveryId ?? recognizedDeliveryId;
            return deliveryId is { } recognizedId
                ? IngestionParseDecision.Quarantine(recognizedId, exception.Code, exception.Message)
                : IngestionParseDecision.Quarantine(exception.Code, exception.Message);
        }
    }

    private static IngestionParseDecision ParseRecord(JsonElement root, long observedAtMinuteUtc)
    {
        RequireExactProperties(root, ["id", "type", "record"], []);
        var deliveryId = RequireDeliveryId(root);

        try
        {
            var envelope = RequireProperty(root, "record");
            RequireObject(envelope, "record");
            RequireExactProperties(
                envelope,
                ["live", "did", "rev", "collection", "rkey", "action"],
                ["cid", "metadata_status", "metadata"]);

            var isLive = RequireBoolean(envelope, "live");
            var did = RequireString(envelope, "did", MaximumDidLength);
            var accountKey = ParseAccountKey(did, "record.did");
            var revision = RequireString(envelope, "rev", MaximumTokenLength);
            RequireCanonicalTid(revision, "record.rev");
            var collectionText = RequireString(envelope, "collection", MaximumTokenLength);
            var collection = ParseCollection(collectionText);
            var recordKey = RequireString(envelope, "rkey", MaximumRecordKeyLength);
            var actionText = RequireString(envelope, "action", MaximumTokenLength);
            var action = ParseAction(actionText);

            string? cid = null;
            AccountKey? target = null;
            var isDirectReply = false;

            if (action == RecordMutationAction.Delete)
            {
                if (envelope.TryGetProperty("cid", out _)
                    || envelope.TryGetProperty("metadata_status", out _)
                    || envelope.TryGetProperty("metadata", out _))
                {
                    throw Contract(
                        QuarantineCode.UnexpectedProperty,
                        "A delete event must not contain cid or metadata data.");
                }
            }
            else
            {
                cid = RequireString(envelope, "cid", MaximumCidLength);
                var metadataStatus = RequireString(envelope, "metadata_status", MaximumTokenLength);
                if (metadataStatus == "invalid")
                {
                    if (envelope.TryGetProperty("metadata", out _))
                    {
                        throw Contract(
                            QuarantineCode.UnexpectedProperty,
                            "An invalid metadata result must not contain extracted metadata.");
                    }

                    throw Contract(
                        QuarantineCode.InvalidValue,
                        "The TAP sanitizer could not derive the required metadata safely.");
                }

                if (metadataStatus != "valid")
                {
                    throw Contract(
                        QuarantineCode.InvalidValue,
                        "metadata_status must be either 'valid' or 'invalid'.");
                }

                (target, isDirectReply) = ParseSanitizedMetadata(envelope, collection);
            }

            var semanticKey = SemanticEventKey.Create(
                did,
                revision,
                collectionText,
                recordKey,
                actionText,
                cid);

            return IngestionParseDecision.Accept(
                deliveryId,
                new RecordMutationEvent(
                    accountKey,
                    semanticKey,
                    isLive,
                    observedAtMinuteUtc,
                    revision,
                    collection,
                    recordKey,
                    action,
                    cid,
                    target,
                    isDirectReply));
        }
        catch (ContractException exception)
        {
            exception.DeliveryId = deliveryId;
            throw;
        }
    }

    private static IngestionParseDecision ParseIdentity(JsonElement root)
    {
        RequireExactProperties(root, ["id", "type", "identity"], []);
        var deliveryId = RequireDeliveryId(root);

        try
        {
            var envelope = RequireProperty(root, "identity");
            RequireObject(envelope, "identity");
            RequireExactProperties(envelope, ["did", "is_active", "status"], []);

            var did = RequireString(envelope, "did", MaximumDidLength);
            var accountKey = ParseAccountKey(did, "identity.did");
            var isActive = RequireBoolean(envelope, "is_active");
            var statusText = RequireString(envelope, "status", MaximumTokenLength);
            var status = statusText switch
            {
                "active" => AccountLifecycleStatus.Active,
                "deactivated" => AccountLifecycleStatus.Deactivated,
                "takendown" => AccountLifecycleStatus.TakenDown,
                "suspended" => AccountLifecycleStatus.Suspended,
                "deleted" => AccountLifecycleStatus.Deleted,
                _ => throw Contract(
                    QuarantineCode.InvalidValue,
                    "The account lifecycle status is not supported."),
            };

            if (isActive != (status == AccountLifecycleStatus.Active))
            {
                throw Contract(
                    QuarantineCode.InvalidValue,
                    "identity.is_active does not agree with identity.status.");
            }

            return IngestionParseDecision.Accept(
                deliveryId,
                new AccountLifecycleEvent(accountKey, status));
        }
        catch (ContractException exception)
        {
            exception.DeliveryId = deliveryId;
            throw;
        }
    }

    private static IngestionParseDecision ParseRepositorySync(JsonElement root)
    {
        RequireExactProperties(root, ["id", "type", "repo_sync"], []);
        var deliveryId = RequireDeliveryId(root);

        try
        {
            var envelope = RequireProperty(root, "repo_sync");
            RequireObject(envelope, "repo_sync");
            RequireExactProperties(envelope, ["did", "rev", "status"], []);

            var did = RequireString(envelope, "did", MaximumDidLength);
            var accountKey = ParseAccountKey(did, "repo_sync.did");
            var revision = RequireString(envelope, "rev", MaximumTokenLength);
            RequireCanonicalTid(revision, "repo_sync.rev");
            var status = RequireString(envelope, "status", MaximumTokenLength);
            if (!string.Equals(status, "active", StringComparison.Ordinal))
            {
                throw Contract(
                    QuarantineCode.InvalidValue,
                    "repo_sync.status must be 'active'.");
            }

            return IngestionParseDecision.Accept(
                deliveryId,
                new RepositorySyncEvent(accountKey, revision));
        }
        catch (ContractException exception)
        {
            exception.DeliveryId = deliveryId;
            throw;
        }
    }

    private static (AccountKey? Target, bool IsDirectReply) ParseSanitizedMetadata(
        JsonElement envelope,
        AtRecordKind collection)
        => collection switch
        {
            AtRecordKind.FeedPost => ParsePostMetadata(envelope),
            AtRecordKind.FeedLike or AtRecordKind.FeedRepost
                => (ParseSubjectMetadata(envelope), false),
            AtRecordKind.GraphFollow
                => (ParseFollowMetadata(envelope), false),
            AtRecordKind.ActorProfile
                => ParseProfileMetadata(envelope),
            _ => throw new InvalidOperationException("The collection enum is outside its closed set."),
        };

    private static (AccountKey? Target, bool IsDirectReply) ParsePostMetadata(JsonElement envelope)
    {
        if (!envelope.TryGetProperty("metadata", out var metadata))
        {
            return (null, false);
        }

        RequireObject(metadata, "record.metadata");
        RequireExactProperties(metadata, ["reply_parent_uri"], []);
        var parentUri = RequireString(
            metadata,
            "reply_parent_uri",
            MaximumDidLength + (MaximumTokenLength * 2));
        return (ParseAtUriAccount(parentUri), true);
    }

    private static AccountKey ParseSubjectMetadata(JsonElement envelope)
    {
        var metadata = RequireProperty(envelope, "metadata");
        RequireObject(metadata, "record.metadata");
        RequireExactProperties(metadata, ["subject_uri"], []);
        var uri = RequireString(
            metadata,
            "subject_uri",
            MaximumDidLength + (MaximumTokenLength * 2));
        return ParseAtUriAccount(uri);
    }

    private static AccountKey ParseFollowMetadata(JsonElement envelope)
    {
        var metadata = RequireProperty(envelope, "metadata");
        RequireObject(metadata, "record.metadata");
        RequireExactProperties(metadata, ["follow_subject_did"], []);
        var subjectDid = RequireString(metadata, "follow_subject_did", MaximumDidLength);
        return ParseAccountKey(subjectDid, "record.metadata.follow_subject_did");
    }

    private static (AccountKey? Target, bool IsDirectReply) ParseProfileMetadata(JsonElement envelope)
    {
        if (envelope.TryGetProperty("metadata", out _))
        {
            throw Contract(
                QuarantineCode.UnexpectedProperty,
                "A profile event must not contain extracted metadata.");
        }

        return (null, false);
    }

    private static AccountKey ParseAtUriAccount(string uri)
    {
        const string prefix = "at://";
        if (!uri.StartsWith(prefix, StringComparison.Ordinal))
        {
            throw Contract(QuarantineCode.InvalidValue, "A record subject must use an at:// URI.");
        }

        var repositoryEnd = uri.IndexOf('/', prefix.Length);
        if (repositoryEnd <= prefix.Length || repositoryEnd == uri.Length - 1)
        {
            throw Contract(
                QuarantineCode.InvalidValue,
                "A record subject URI must identify a repository and record path.");
        }

        return ParseAccountKey(uri[prefix.Length..repositoryEnd], "record subject repository");
    }

    private static AtRecordKind ParseCollection(string value)
        => value switch
        {
            FeedPost => AtRecordKind.FeedPost,
            FeedLike => AtRecordKind.FeedLike,
            FeedRepost => AtRecordKind.FeedRepost,
            GraphFollow => AtRecordKind.GraphFollow,
            ActorProfile => AtRecordKind.ActorProfile,
            _ => throw Contract(
                QuarantineCode.UnsupportedCollection,
                "The record collection is not supported."),
        };

    private static RecordMutationAction ParseAction(string value)
        => value switch
        {
            "create" => RecordMutationAction.Create,
            "update" => RecordMutationAction.Update,
            "delete" => RecordMutationAction.Delete,
            _ => throw Contract(
                QuarantineCode.InvalidValue,
                "The record action is not supported."),
        };

    private static AccountKey ParseAccountKey(string did, string path)
    {
        try
        {
            return AccountKey.FromDid(did);
        }
        catch (ArgumentException)
        {
            throw Contract(QuarantineCode.InvalidValue, $"{path} is not a valid DID.");
        }
    }

    private static void RequireCanonicalTid(string value, string path)
    {
        if (value.Length != 13
            || !TidFirstCharacters.Contains(value[0])
            || value.AsSpan(1).IndexOfAnyExcept(TidCharacters) >= 0)
        {
            throw Contract(
                QuarantineCode.InvalidValue,
                $"{path} must be a canonical AT Protocol TID.");
        }
    }

    private static ulong RequireDeliveryId(JsonElement root)
    {
        var property = RequireProperty(root, "id");
        if (property.ValueKind != JsonValueKind.Number
            || !property.TryGetUInt64(out var deliveryId)
            || deliveryId == 0)
        {
            throw Contract(QuarantineCode.InvalidValue, "id must be a positive unsigned integer.");
        }

        return deliveryId;
    }

    private static string RequireString(JsonElement parent, string name, int maximumLength)
    {
        var property = RequireProperty(parent, name);
        if (property.ValueKind != JsonValueKind.String)
        {
            throw Contract(QuarantineCode.InvalidValue, $"{name} must be a string.");
        }

        var value = property.GetString();
        if (string.IsNullOrWhiteSpace(value)
            || value.Length > maximumLength
            || !string.Equals(value, value.Trim(), StringComparison.Ordinal))
        {
            throw Contract(
                QuarantineCode.InvalidValue,
                $"{name} must be a non-empty canonical string no longer than {maximumLength} characters.");
        }

        return value;
    }

    private static bool RequireBoolean(JsonElement parent, string name)
    {
        var property = RequireProperty(parent, name);
        return property.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => throw Contract(QuarantineCode.InvalidValue, $"{name} must be a Boolean."),
        };
    }

    private static JsonElement RequireProperty(JsonElement parent, string name)
    {
        if (!parent.TryGetProperty(name, out var property))
        {
            throw Contract(QuarantineCode.MissingProperty, $"Required property '{name}' is missing.");
        }

        return property;
    }

    private static void RequireObject(JsonElement element, string path)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw Contract(QuarantineCode.InvalidRoot, $"{path} must be a JSON object.");
        }
    }

    private static void RequireExactProperties(
        JsonElement element,
        IReadOnlyCollection<string> required,
        IReadOnlyCollection<string> optional)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            if (!seen.Add(property.Name))
            {
                throw Contract(
                    QuarantineCode.UnexpectedProperty,
                    "A JSON property occurs more than once.");
            }

            if (!required.Contains(property.Name, StringComparer.Ordinal)
                && !optional.Contains(property.Name, StringComparer.Ordinal))
            {
                throw Contract(
                    QuarantineCode.UnexpectedProperty,
                    "An unexpected JSON property is not permitted by the metadata-only contract.");
            }
        }

        foreach (var name in required)
        {
            if (!seen.Contains(name))
            {
                throw Contract(QuarantineCode.MissingProperty, $"Required property '{name}' is missing.");
            }
        }
    }

    private static ContractException Contract(QuarantineCode code, string message)
        => new(code, message);

    private sealed class ContractException(QuarantineCode code, string message) : Exception(message)
    {
        public QuarantineCode Code { get; } = code;

        public ulong? DeliveryId { get; set; }
    }
}
