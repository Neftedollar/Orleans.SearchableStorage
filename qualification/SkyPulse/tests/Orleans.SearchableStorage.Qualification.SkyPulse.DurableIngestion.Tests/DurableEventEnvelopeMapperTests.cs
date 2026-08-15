using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion.Tests;

public sealed class DurableEventEnvelopeMapperTests
{
    private static readonly Guid Source = Guid.Parse("892eb137-ceab-4811-b4ef-1679f13ba90b");

    [Fact]
    public void ReactivationAdvancesExactlyOneGenerationWhileOtherEventsRetainIt()
    {
        var account = AccountKey.FromDid("did:plc:generation-owner");
        var inactive = new AccountStateSnapshot(
            account,
            stateVersion: 7,
            DurableAccountLifecycle.Deactivated,
            repositoryGeneration: 4,
            completedSyncRevision: null,
            synchronizationComplete: false,
            lastActivityMinuteUtc: 0,
            currentPostCount: 0,
            currentFollowingCount: 0,
            currentFollowerCount: 0);
        var activeEvent = Assert.IsType<AccountLifecycleEvent>(Parse("""
            {"id":1,"type":"identity","identity":{"did":"did:plc:generation-owner","is_active":true,"status":"active"}}
            """));
        var inactiveEvent = Assert.IsType<AccountLifecycleEvent>(Parse("""
            {"id":2,"type":"identity","identity":{"did":"did:plc:generation-owner","is_active":false,"status":"deleted"}}
            """));

        Assert.Equal(5, DurableEventEnvelopeMapper.ResolveRepositoryGeneration(activeEvent, inactive));
        Assert.Equal(4, DurableEventEnvelopeMapper.ResolveRepositoryGeneration(inactiveEvent, inactive));
        Assert.Equal(0, DurableEventEnvelopeMapper.ResolveRepositoryGeneration(activeEvent, null));
    }

    [Fact]
    public void LifecycleSemanticIdentityExcludesDeliveryAndObservationIdentity()
    {
        var accepted = Assert.IsType<AccountLifecycleEvent>(Parse("""
            {"id":3,"type":"identity","identity":{"did":"did:plc:semantic-owner","is_active":false,"status":"suspended"}}
            """));
        var first = DurableEventEnvelopeMapper.Map(accepted, Reservation(3, "first", 100), 9);
        var redelivery = DurableEventEnvelopeMapper.Map(accepted, Reservation(300, "second", 200), 9);

        Assert.Equal(first.SemanticDigest, redelivery.SemanticDigest);
        Assert.NotEqual(first.DeliveryDigest, redelivery.DeliveryDigest);
        Assert.Equal(DurableEventKind.AccountLifecycle, first.EventKind);
        Assert.Equal(DurableAccountLifecycle.Suspended, first.Lifecycle);
    }

    private static IngestionEvent Parse(string json)
    {
        var decision = SanitizedTapEventParser.Parse(json, DateTimeOffset.UnixEpoch.AddMinutes(1));
        Assert.True(decision.IsAccepted);
        return Assert.IsAssignableFrom<IngestionEvent>(decision.AcceptedEvent);
    }

    private static DurableDeliveryReservation Reservation(ulong id, string digestSeed, long minute)
        => new(
            Source,
            id,
            Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(digestSeed))).ToLowerInvariant(),
            minute,
            DurableDeliveryOutcome.Pending);
}
