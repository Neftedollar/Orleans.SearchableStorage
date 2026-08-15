using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class DurableModelTests
{
    internal static readonly Guid SourceInstanceId = Guid.Parse("8cc93693-2ddb-4d95-92e8-c8407669e0c1");
    internal const string Revision = "3jzfcijpj2z2a";
    internal const string NewerRevision = "3jzfcijpj2z2b";

    private static readonly string[] EventEnvelopePropertyNames =
    [
        "AccountKey",
        "Action",
        "Cid",
        "Collection",
        "DeliveryDigest",
        "EventKind",
        "IsDirectReply",
        "IsLive",
        "Lifecycle",
        "ObservedAtMinuteUtc",
        "RecordKey",
        "RepositoryGeneration",
        "RepositoryRevision",
        "SemanticDigest",
        "SourceInstanceId",
        "TapDeliveryId",
        "TargetAccountKey",
    ];

    [Fact]
    public void EventEnvelopeExposesOnlyClosedMetadataFields()
    {
        var properties = typeof(DurableEventEnvelope)
            .GetProperties()
            .Select(static property => property.Name)
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(EventEnvelopePropertyNames, properties);
    }

    [Fact]
    public void RecordEventRequiresClosedRecordIdentity()
    {
        Assert.Throws<ArgumentException>(
            () => new DurableEventEnvelope(
                SourceInstanceId,
                1,
                Digest('a'),
                Digest('b'),
                Account("actor"),
                0,
                DurableEventKind.RecordMutation,
                10));
    }

    [Fact]
    public void SourceInstanceMustBeDurableAndNonEmpty()
    {
        Assert.Throws<ArgumentException>(
            () => new DurableEventEnvelope(
                Guid.Empty,
                1,
                Digest('a'),
                Digest('b'),
                Account("actor"),
                0,
                DurableEventKind.RepositorySync,
                10,
                repositoryRevision: Revision));
    }

    [Fact]
    public void TapDeliveryIdentityMustBePositiveAcrossTheDurableBoundary()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DurableDeliveryReservationRequest(
            SourceInstanceId,
            0,
            Digest('a'),
            10));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DurableDeliveryReservation(
            SourceInstanceId,
            0,
            Digest('a'),
            10,
            DurableDeliveryOutcome.Pending));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DurableEventEnvelope(
            SourceInstanceId,
            0,
            Digest('a'),
            Digest('b'),
            Account("actor"),
            0,
            DurableEventKind.RepositorySync,
            10,
            repositoryRevision: Revision));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DurableQuarantine(
            SourceInstanceId,
            0,
            Digest('a'),
            DurableQuarantineReason.InvalidValue,
            10));
    }

    [Fact]
    public void LifecycleEventCarriesClosedLifecycleValue()
    {
        var envelope = new DurableEventEnvelope(
            SourceInstanceId,
            1,
            Digest('a'),
            Digest('b'),
            Account("actor"),
            2,
            DurableEventKind.AccountLifecycle,
            10,
            lifecycle: DurableAccountLifecycle.Deactivated);

        Assert.Equal(DurableAccountLifecycle.Deactivated, envelope.Lifecycle);
        Assert.Null(envelope.Collection);
    }

    [Fact]
    public void CreateRequiresCidAndTargetedCollectionRequiresTarget()
    {
        Assert.Throws<ArgumentException>(
            () => new DurableEventEnvelope(
                SourceInstanceId,
                1,
                Digest('a'),
                Digest('b'),
                Account("actor"),
                0,
                DurableEventKind.RecordMutation,
                10,
                repositoryRevision: Revision,
                collection: DurableRecordKind.GraphFollow,
                action: DurableRecordAction.Create,
                recordKey: "rkey",
                cid: "cid"));
    }

    [Fact]
    public void CompleteSynchronizationRequiresRevision()
    {
        Assert.Throws<ArgumentException>(
            () => new AccountStateMutation(
                Account("actor"),
                0,
                1,
                DurableAccountLifecycle.Active,
                0,
                null,
                synchronizationComplete: true,
                0,
                0,
                0,
                0));
    }

    [Theory]
    [InlineData("rev")]
    [InlineData("3JZFCIJPJ2Z2A")]
    [InlineData("zjzfcijpj2z2a")]
    public void RevisionMustBeCanonicalSortableTid(string revision)
    {
        Assert.Throws<ArgumentException>(
            () => new RecordStateMutation(
                Account("actor"),
                7,
                DurableRecordKind.FeedPost,
                "record-key",
                revision,
                isDeleted: true));
    }

    [Fact]
    public void DeleteRecordRetainsLatestRevisionTombstone()
    {
        var mutation = new RecordStateMutation(
            Account("actor"),
            7,
            DurableRecordKind.FeedPost,
            "record-key",
            Revision,
            isDeleted: true);

        Assert.True(mutation.IsDeleted);
        Assert.Equal(Revision, mutation.LatestRevision);
        Assert.Equal(7, mutation.RepositoryGeneration);
    }

    [Fact]
    public void DeleteRecordTombstoneRejectsBodyDerivedMetadata()
    {
        Assert.Throws<ArgumentException>(
            () => new RecordStateMutation(
                Account("actor"),
                7,
                DurableRecordKind.FeedPost,
                "record-key",
                Revision,
                isDeleted: true,
                cid: "stale-cid"));
        Assert.Throws<ArgumentException>(
            () => new RecordStateMutation(
                Account("actor"),
                7,
                DurableRecordKind.GraphFollow,
                "record-key",
                Revision,
                isDeleted: true,
                targetAccountKey: Account("stale-target")));
        Assert.Throws<ArgumentException>(
            () => new RecordStateMutation(
                Account("actor"),
                7,
                DurableRecordKind.FeedPost,
                "record-key",
                Revision,
                isDeleted: true,
                isDirectReply: true));
    }

    [Fact]
    public void ZeroFollowMultiplicityRepresentsPairRemoval()
    {
        var mutation = new FollowPairMutation(Account("source"), Account("target"), 0);
        Assert.Equal(0, mutation.Multiplicity);
    }

    [Fact]
    public void ActivityRequiresRealIncrementAndPostSubset()
    {
        Assert.Throws<ArgumentException>(() => new ActivityMinuteDelta(Account("actor"), 10));
        Assert.Throws<ArgumentException>(
            () => new ActivityMinuteDelta(Account("actor"), 10, recordCreates: 1, postCreates: 2));
    }

    [Fact]
    public void IncompleteUpsertCanBeDesiredButRemovalMustBeComplete()
    {
        var desired = Projection(Account("actor"), 1, isComplete: false);

        Assert.False(desired.IsComplete);
        Assert.Throws<ArgumentException>(
            () => Projection(Account("actor"), 1, isComplete: false, ProjectionOperation.Remove));
    }

    [Fact]
    public void ProjectionRejectsNonMonotonicRollingWindowsAndPostSuperset()
    {
        Assert.Throws<ArgumentException>(
            () => Projection(Account("actor"), 1, created1Day: 2, created7Days: 1, created30Days: 2));
        Assert.Throws<ArgumentException>(
            () => Projection(Account("actor"), 1, updated1Day: 2, updated7Days: 1, updated30Days: 2));
        Assert.Throws<ArgumentException>(
            () => Projection(Account("actor"), 1, deleted1Day: 2, deleted7Days: 1, deleted30Days: 2));
        Assert.Throws<ArgumentException>(
            () => Projection(Account("actor"), 1, created1Day: 0, post1Day: 1));
    }

    [Fact]
    public void ProjectionCarriesExactlySeventeenIndexValues()
    {
        var metricNames = new[]
        {
            "LastActivityMinuteUtc",
            "CreatedRecordCount1Day",
            "CreatedRecordCount7Days",
            "CreatedRecordCount30Days",
            "UpdatedRecordCount1Day",
            "UpdatedRecordCount7Days",
            "UpdatedRecordCount30Days",
            "DeletedRecordCount1Day",
            "DeletedRecordCount7Days",
            "DeletedRecordCount30Days",
            "CurrentPostCount",
            "CurrentFollowingCount",
            "CurrentFollowerCount",
            "PostCreates1Day",
            "PostCreates7Days",
            "PostCreates30Days",
            "ReceivedEngagementCreates30Days",
        };

        foreach (var metricName in metricNames)
        {
            var property = typeof(ProjectionSnapshot).GetProperty(metricName);
            Assert.NotNull(property);
            Assert.Equal(typeof(long), property.PropertyType);
        }

        Assert.Equal(17, metricNames.Length);
    }

    [Fact]
    public void TransitionRequiresOptimisticStateForEventAndProjection()
    {
        var actor = Account("actor");
        var other = Account("other");
        var envelope = RecordEnvelope(actor);
        var state = State(other, expectedVersion: 0);

        Assert.Throws<ArgumentException>(() => new DurableIngestionCommit(envelope, [state]));
    }

    [Fact]
    public void TransitionRequiresRecordGenerationToMatchAccountState()
    {
        var actor = Account("actor");
        var envelope = RecordEnvelope(actor, generation: 2);
        var state = State(actor, expectedVersion: 0, generation: 2);
        var record = new RecordStateMutation(
            actor,
            1,
            DurableRecordKind.FeedPost,
            "rkey",
            Revision,
            isDeleted: false,
            cid: "cid");

        Assert.Throws<ArgumentException>(
            () => new DurableIngestionCommit(envelope, [state], records: [record]));
    }

    [Fact]
    public void TransitionRequiresEnvelopeGenerationToMatchEventAccountState()
    {
        var actor = Account("actor");
        var envelope = RecordEnvelope(actor);
        var state = State(actor, expectedVersion: 0, generation: 2);
        var record = new RecordStateMutation(
            actor,
            2,
            DurableRecordKind.FeedPost,
            "rkey",
            Revision,
            isDeleted: false,
            cid: "cid");

        Assert.Throws<ArgumentException>(
            () => new DurableIngestionCommit(envelope, [state], records: [record]));
    }

    [Fact]
    public void RecordTransitionMustMatchExactEventRevision()
    {
        var actor = Account("actor");
        var envelope = RecordEnvelope(actor);
        var state = State(actor, expectedVersion: 0);
        var record = new RecordStateMutation(
            actor,
            0,
            DurableRecordKind.FeedPost,
            "rkey",
            NewerRevision,
            isDeleted: false,
            cid: "cid");

        Assert.Throws<ArgumentException>(
            () => new DurableIngestionCommit(envelope, [state], records: [record]));
    }

    [Fact]
    public void CurrentFollowRecordRequiresPositiveTargetPairReplacement()
    {
        var actor = Account("actor");
        var target = Account("target");
        var envelope = new DurableEventEnvelope(
            SourceInstanceId,
            1,
            Digest('a'),
            Digest('b'),
            actor,
            0,
            DurableEventKind.RecordMutation,
            10,
            repositoryRevision: Revision,
            collection: DurableRecordKind.GraphFollow,
            action: DurableRecordAction.Create,
            recordKey: "rkey",
            cid: "cid",
            targetAccountKey: target);
        var state = State(actor, expectedVersion: 0);
        var record = new RecordStateMutation(
            actor,
            0,
            DurableRecordKind.GraphFollow,
            "rkey",
            Revision,
            isDeleted: false,
            cid: "cid",
            targetAccountKey: target);

        Assert.Throws<ArgumentException>(
            () => new DurableIngestionCommit(envelope, [state], records: [record]));

        var commit = new DurableIngestionCommit(
            envelope,
            [state],
            records: [record],
            followPairs: [new FollowPairMutation(actor, target, 1)]);
        Assert.Single(commit.FollowPairs);
    }

    [Fact]
    public void DeletedFollowRecordRequiresPlannerDerivedSourcePairReplacement()
    {
        var actor = Account("actor");
        var target = Account("prior-target");
        var envelope = new DurableEventEnvelope(
            SourceInstanceId,
            1,
            Digest('a'),
            Digest('b'),
            actor,
            0,
            DurableEventKind.RecordMutation,
            10,
            repositoryRevision: Revision,
            collection: DurableRecordKind.GraphFollow,
            action: DurableRecordAction.Delete,
            recordKey: "rkey");
        var state = State(actor, expectedVersion: 0);
        var record = new RecordStateMutation(
            actor,
            0,
            DurableRecordKind.GraphFollow,
            "rkey",
            Revision,
            isDeleted: true);

        Assert.Throws<ArgumentException>(
            () => new DurableIngestionCommit(envelope, [state], records: [record]));

        var commit = new DurableIngestionCommit(
            envelope,
            [state],
            records: [record],
            followPairs: [new FollowPairMutation(actor, target, 0)]);
        Assert.Single(commit.FollowPairs);
    }

    [Fact]
    public void RevisionConflictNeverAllowsSourceAcknowledgement()
    {
        Assert.Throws<ArgumentException>(
            () => new DurableCommitResult(DurableCommitOutcome.RevisionConflict, acknowledgementAllowed: true));

        var result = new DurableCommitResult(DurableCommitOutcome.RevisionConflict, acknowledgementAllowed: false);
        Assert.False(result.AcknowledgementAllowed);
    }

    [Fact]
    public void TransitionRequiresActivityGenerationToMatchAccountState()
    {
        var actor = Account("actor");
        var envelope = RecordEnvelope(actor);
        var state = State(actor, expectedVersion: 0, generation: 2);
        var activity = new ActivityMinuteDelta(
            actor,
            minuteUtc: 10,
            repositoryGeneration: 1,
            recordCreates: 1,
            postCreates: 1);

        Assert.Throws<ArgumentException>(
            () => new DurableIngestionCommit(envelope, [state], activity: [activity]));
    }

    [Fact]
    public void ValidTransitionFreezesTypedChanges()
    {
        var actor = Account("actor");
        var envelope = RecordEnvelope(actor);
        var state = State(actor, expectedVersion: 0);
        var record = new RecordStateMutation(
            actor,
            0,
            DurableRecordKind.FeedPost,
            "rkey",
            Revision,
            isDeleted: false,
            cid: "cid");
        var projection = Projection(actor, 1);

        var commit = new DurableIngestionCommit(
            envelope,
            [state],
            records: [record],
            activity: [new ActivityMinuteDelta(actor, 10, recordCreates: 1, postCreates: 1)],
            projections: [projection]);

        Assert.Single(commit.AccountStates);
        Assert.Single(commit.Records);
        Assert.Single(commit.Activity);
        Assert.Single(commit.Projections);
    }

    [Fact]
    public void QuarantineAcceptsOnlyClosedCatalogDiagnostics()
    {
        var quarantine = new DurableQuarantine(
            SourceInstanceId,
            1,
            Digest('a'),
            DurableQuarantineReason.UnexpectedProperty,
            1);

        Assert.Equal(DurableQuarantineReason.UnexpectedProperty, quarantine.Reason);
        Assert.Equal("unexpected-property", quarantine.Code);
        Assert.Equal(
            "The TAP event contains a property outside the closed contract.",
            quarantine.Message);
        Assert.DoesNotContain(
            typeof(DurableQuarantine).GetConstructors().SelectMany(static constructor => constructor.GetParameters()),
            static parameter => parameter.Name is "code" or "message");
        Assert.Throws<ArgumentOutOfRangeException>(() => new DurableQuarantine(
            SourceInstanceId,
            1,
            Digest('a'),
            (DurableQuarantineReason)int.MaxValue,
            1));
    }

    internal static AccountKey Account(string suffix) => AccountKey.FromDid($"did:plc:{suffix}");

    internal static string Digest(char value) => new(value, 64);

    internal static DurableEventEnvelope RecordEnvelope(AccountKey actor, long generation = 0)
        => new(
            SourceInstanceId,
            1,
            Digest('a'),
            Digest('b'),
            actor,
            generation,
            DurableEventKind.RecordMutation,
            10,
            repositoryRevision: Revision,
            collection: DurableRecordKind.FeedPost,
            action: DurableRecordAction.Create,
            recordKey: "rkey",
            cid: "cid");

    internal static AccountStateMutation State(AccountKey account, long expectedVersion, long generation = 0)
        => new(
            account,
            expectedVersion,
            checked(expectedVersion + 1),
            DurableAccountLifecycle.Active,
            generation,
            Revision,
            synchronizationComplete: true,
            lastActivityMinuteUtc: 10,
            currentPostCount: 1,
            currentFollowingCount: 2,
            currentFollowerCount: 3);

    internal static ProjectionSnapshot Projection(
        AccountKey account,
        long version,
        bool isComplete = true,
        ProjectionOperation operation = ProjectionOperation.Upsert,
        long created1Day = 1,
        long created7Days = 1,
        long created30Days = 1,
        long updated1Day = 0,
        long updated7Days = 0,
        long updated30Days = 0,
        long deleted1Day = 0,
        long deleted7Days = 0,
        long deleted30Days = 0,
        long post1Day = 1,
        long post7Days = 1,
        long post30Days = 1)
        => new(
            account,
            version,
            operation,
            isComplete,
            projectionCutMinuteUtc: 10,
            nextRecalculationMinuteUtc: 11,
            lastActivityMinuteUtc: 10,
            createdRecordCount1Day: created1Day,
            createdRecordCount7Days: created7Days,
            createdRecordCount30Days: created30Days,
            updatedRecordCount1Day: updated1Day,
            updatedRecordCount7Days: updated7Days,
            updatedRecordCount30Days: updated30Days,
            deletedRecordCount1Day: deleted1Day,
            deletedRecordCount7Days: deleted7Days,
            deletedRecordCount30Days: deleted30Days,
            currentPostCount: 1,
            currentFollowingCount: 2,
            currentFollowerCount: 3,
            postCreates1Day: post1Day,
            postCreates7Days: post7Days,
            postCreates30Days: post30Days,
            receivedEngagementCreates30Days: 4);
}
