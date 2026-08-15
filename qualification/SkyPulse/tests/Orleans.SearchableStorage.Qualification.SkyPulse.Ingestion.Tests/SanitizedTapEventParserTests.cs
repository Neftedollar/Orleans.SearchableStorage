using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion.Tests;

public sealed class SanitizedTapEventParserTests
{
    private static readonly DateTimeOffset ObservedAt = new(
        2026,
        8,
        14,
        12,
        0,
        0,
        TimeSpan.Zero);

    [Theory]
    [InlineData("{\"id\":1,\"type\":\"private-user-text\"}", "private-user-text")]
    [InlineData("{\"id\":1,\"type\":\"identity\",\"private-user-text\":true}", "private-user-text")]
    [InlineData("{not-private-user-text}", "private-user-text")]
    public void QuarantineMessageDoesNotReflectSourceControlledText(string json, string secret)
    {
        var decision = SanitizedTapEventParser.Parse(json, ObservedAt);

        Assert.False(decision.IsAccepted);
        Assert.DoesNotContain(secret, decision.QuarantineMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void DeliveryIdIsNotPartOfTheSemanticEventIdentity()
    {
        var first = TestEvents.ParseRecord(TestEvents.Post(1, live: true), ObservedAt);
        var redelivery = TestEvents.ParseRecord(TestEvents.Post(99, live: true), ObservedAt.AddMinutes(10));

        Assert.Equal(first.SemanticKey, redelivery.SemanticKey);
        Assert.Equal(AccountKey.FromDid(TestEvents.ActorDid), first.AccountKey);
        Assert.Equal(64, first.SemanticKey.ToString().Length);
    }

    [Fact]
    public void AllAgreedMetadataRecordShapesAreAccepted()
    {
        var events = new[]
        {
            TestEvents.ParseRecord(TestEvents.Post(1, live: true), ObservedAt),
            TestEvents.ParseRecord(TestEvents.Like(2, live: true), ObservedAt),
            TestEvents.ParseRecord(TestEvents.Repost(3, live: true), ObservedAt),
            TestEvents.ParseRecord(TestEvents.Follow(4, live: true), ObservedAt),
            TestEvents.ParseRecord(TestEvents.Profile(5, live: true), ObservedAt),
        };

        Assert.Equal(
            [
                AtRecordKind.FeedPost,
                AtRecordKind.FeedLike,
                AtRecordKind.FeedRepost,
                AtRecordKind.GraphFollow,
                AtRecordKind.ActorProfile,
            ],
            events.Select(static item => item.Collection));
    }

    [Theory]
    [InlineData("active", true, AccountLifecycleStatus.Active)]
    [InlineData("deactivated", false, AccountLifecycleStatus.Deactivated)]
    [InlineData("takendown", false, AccountLifecycleStatus.TakenDown)]
    [InlineData("suspended", false, AccountLifecycleStatus.Suspended)]
    [InlineData("deleted", false, AccountLifecycleStatus.Deleted)]
    public void IdentityLifecycleShapesAreAccepted(
        string status,
        bool isActive,
        AccountLifecycleStatus expected)
    {
        var parsed = TestEvents.ParseIdentity(TestEvents.Identity(1, status, isActive), ObservedAt);

        Assert.Equal(expected, parsed.Status);
        Assert.Equal(AccountKey.FromDid(TestEvents.ActorDid), parsed.AccountKey);
    }

    [Fact]
    public void RepositorySyncAcceptsOnlyTheExactMetadataBarrierShape()
    {
        var parsed = TestEvents.ParseRepositorySync(
            TestEvents.RepositorySync(19, TestEvents.RevB),
            ObservedAt);

        Assert.Equal(AccountKey.FromDid(TestEvents.ActorDid), parsed.AccountKey);
        Assert.Equal(TestEvents.RevB, parsed.Revision);
        Assert.Equal(
            ["AccountKey", "Revision"],
            parsed.GetType()
                .GetProperties()
                .Select(static property => property.Name)
                .Order(StringComparer.Ordinal));
    }

    [Fact]
    public void RepositorySyncRejectsUnknownPropertiesAndNonActiveStatus()
    {
        var unknown = SanitizedTapEventParser.Parse(
            TestEvents.RepositorySync(
                20,
                mutateRepositorySync: value => value["handle"] = "must-not-cross.example"),
            ObservedAt);
        var inactive = SanitizedTapEventParser.Parse(
            TestEvents.RepositorySync(21, status: "deactivated"),
            ObservedAt);

        Assert.Equal(QuarantineCode.UnexpectedProperty, unknown.QuarantineCode);
        Assert.Equal((ulong)20, unknown.TapDeliveryId);
        Assert.Equal(QuarantineCode.InvalidValue, inactive.QuarantineCode);
        Assert.Equal((ulong)21, inactive.TapDeliveryId);
    }

    [Theory]
    [InlineData("3jzfcijpj2z2")]
    [InlineData("3jzfcijpj2z2aa")]
    [InlineData("3JZFCIJPJ2Z2A")]
    [InlineData("3jzf-cij-pj2z")]
    [InlineData("kjzfcijpj2z2a")]
    [InlineData("3jzfcijpj2z21")]
    public void RecordAndRepositorySyncRequireCanonicalAtProtocolTid(string revision)
    {
        var record = SanitizedTapEventParser.Parse(
            TestEvents.Post(22, live: false, revision: revision),
            ObservedAt);
        var repositorySync = SanitizedTapEventParser.Parse(
            TestEvents.RepositorySync(23, revision),
            ObservedAt);

        Assert.Equal(QuarantineCode.InvalidValue, record.QuarantineCode);
        Assert.Equal((ulong)22, record.TapDeliveryId);
        Assert.Equal(QuarantineCode.InvalidValue, repositorySync.QuarantineCode);
        Assert.Equal((ulong)23, repositorySync.TapDeliveryId);
    }

    [Fact]
    public void DirectReplyRetainsOnlyTheTargetAccountKey()
    {
        var parsed = TestEvents.ParseRecord(
            TestEvents.Post(1, live: true, replyTargetDid: TestEvents.TargetDid),
            ObservedAt);

        Assert.True(parsed.IsDirectReply);
        Assert.Equal(AccountKey.FromDid(TestEvents.TargetDid), parsed.TargetAccountKey);
        Assert.DoesNotContain(
            parsed.GetType().GetProperties(),
            static property => property.Name.Contains("Uri", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("text", "body")]
    [InlineData("langs", "en")]
    [InlineData("embed", "media")]
    [InlineData("createdAt", "2026-08-14T12:00:00Z")]
    public void PostContentAndUnneededBodyFieldsAreQuarantined(string name, string value)
    {
        var json = TestEvents.Post(
            1,
            live: true,
            mutateRecord: record => record[name] = value);

        var decision = SanitizedTapEventParser.Parse(json, ObservedAt);

        Assert.False(decision.IsAccepted);
        Assert.Equal(QuarantineCode.UnexpectedProperty, decision.QuarantineCode);
        Assert.Equal((ulong)1, decision.TapDeliveryId);
    }

    [Fact]
    public void ProfileAndIdentityContentAreQuarantined()
    {
        var profile = SanitizedTapEventParser.Parse(
            TestEvents.Profile(
                1,
                live: true,
                mutateRecord: record => record["displayName"] = "Not retained"),
            ObservedAt);
        var identity = SanitizedTapEventParser.Parse(
            TestEvents.Identity(
                2,
                "active",
                isActive: true,
                mutateIdentity: value => value["handle"] = "not-retained.example"),
            ObservedAt);

        Assert.Equal(QuarantineCode.UnexpectedProperty, profile.QuarantineCode);
        Assert.Equal(QuarantineCode.UnexpectedProperty, identity.QuarantineCode);
    }

    [Fact]
    public void RawRecordBodyIsNeverPartOfTheAcceptedWireContract()
    {
        const string json = """
            {
              "id": 7,
              "type": "record",
              "record": {
                "live": true,
                "did": "did:plc:actor",
                "rev": "3jzfcijpj2z2a",
                "collection": "app.bsky.feed.post",
                "rkey": "post-1",
                "action": "create",
                "cid": "bafy-post",
                "metadata_status": "valid",
                "record": { "text": "must never cross the sanitizer" }
              }
            }
            """;

        var decision = SanitizedTapEventParser.Parse(json, ObservedAt);

        Assert.Equal(QuarantineCode.UnexpectedProperty, decision.QuarantineCode);
        Assert.Equal((ulong)7, decision.TapDeliveryId);
    }

    [Fact]
    public void SanitizerInvalidStatusIsExplicitlyQuarantinedWithoutValues()
    {
        var json = TestEvents.Post(8, live: true)
            .Replace(
                "\"metadata_status\":\"valid\"",
                "\"metadata_status\":\"invalid\"",
                StringComparison.Ordinal);

        var decision = SanitizedTapEventParser.Parse(json, ObservedAt);

        Assert.Equal(QuarantineCode.InvalidValue, decision.QuarantineCode);
        Assert.Equal((ulong)8, decision.TapDeliveryId);
    }

    [Theory]
    [InlineData("rkey", 512, true)]
    [InlineData("rkey", 513, false)]
    [InlineData("cid", 256, true)]
    [InlineData("cid", 257, false)]
    public void RecordIdentityLimitsMatchTheDurablePostgreSqlContract(
        string property,
        int length,
        bool accepted)
    {
        var value = new string('a', length);
        var json = property == "rkey"
            ? TestEvents.Post(9, live: true, recordKey: value).Replace(
                $"bafy-{TestEvents.RevA}-{value}",
                "cid",
                StringComparison.Ordinal)
            : TestEvents.Post(9, live: true).Replace(
                $"bafy-{TestEvents.RevA}-post-1",
                value,
                StringComparison.Ordinal);

        var decision = SanitizedTapEventParser.Parse(json, ObservedAt);

        Assert.Equal(accepted, decision.IsAccepted);
        if (!accepted)
        {
            Assert.Equal(QuarantineCode.InvalidValue, decision.QuarantineCode);
            Assert.Equal((ulong)9, decision.TapDeliveryId);
        }
    }

    [Theory]
    [InlineData(null, QuarantineCode.MalformedJson)]
    [InlineData("", QuarantineCode.MalformedJson)]
    [InlineData("[]", QuarantineCode.InvalidRoot)]
    [InlineData("{}", QuarantineCode.MissingProperty)]
    [InlineData("{\"id\":1,\"type\":\"unknown\"}", QuarantineCode.UnsupportedEventType)]
    public void MalformedOrUnknownInputProducesAQuarantineDecision(
        string? json,
        QuarantineCode expected)
    {
        var exception = Record.Exception(() => SanitizedTapEventParser.Parse(json, ObservedAt));
        var decision = SanitizedTapEventParser.Parse(json, ObservedAt);

        Assert.Null(exception);
        Assert.False(decision.IsAccepted);
        Assert.Equal(expected, decision.QuarantineCode);
        Assert.False(string.IsNullOrWhiteSpace(decision.QuarantineMessage));
    }

    [Fact]
    public void UnknownEventWithAValidDeliveryIdCanStillBeAcknowledgedAfterQuarantine()
    {
        const string json = "{\"id\":42,\"type\":\"future-event\"}";

        var decision = SanitizedTapEventParser.Parse(json, ObservedAt);

        Assert.Equal(QuarantineCode.UnsupportedEventType, decision.QuarantineCode);
        Assert.Equal((ulong)42, decision.TapDeliveryId);
    }

    [Fact]
    public void DeleteContainsNoBodyAndStillHasAStableSemanticIdentity()
    {
        var first = TestEvents.ParseRecord(
            TestEvents.Delete(
                1,
                live: true,
                "app.bsky.graph.follow",
                "follow-1",
                TestEvents.RevB),
            ObservedAt);
        var redelivery = TestEvents.ParseRecord(
            TestEvents.Delete(
                2,
                live: true,
                "app.bsky.graph.follow",
                "follow-1",
                TestEvents.RevB),
            ObservedAt.AddMinutes(1));

        Assert.Equal(RecordMutationAction.Delete, first.Action);
        Assert.Null(first.Cid);
        Assert.Null(first.TargetAccountKey);
        Assert.Equal(first.SemanticKey, redelivery.SemanticKey);
    }
}
