using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;
using Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

/// <summary>
/// The exact durable operations used by the TAP delivery processor. The interface exists so the
/// commit-before-ack protocol can be tested without replacing PostgreSQL semantics in production.
/// </summary>
public interface IDurableTapBackend
{
    Task<DurableDeliveryReservation> ReserveDeliveryAsync(
        DurableDeliveryReservationRequest request,
        CancellationToken cancellationToken = default);

    Task<DurableCommitResult> CommitAsync(
        DurableDeliveryReservation reservation,
        DurableIngestionCommit commit,
        CancellationToken cancellationToken = default);

    Task<DurableCommitResult> CommitValidatedNoOpAsync(
        DurableDeliveryReservation reservation,
        DurableValidatedNoOp noOp,
        CancellationToken cancellationToken = default);

    Task<DurableCommitResult> CommitQuarantineAsync(
        DurableDeliveryReservation reservation,
        DurableQuarantine quarantine,
        CancellationToken cancellationToken = default);

    Task<AccountStateSnapshot?> ReadAccountAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default);

    Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default);

    Task<RecordStateSnapshot?> ReadRecordAsync(
        AccountKey accountKey,
        long repositoryGeneration,
        DurableRecordKind collection,
        string recordKey,
        CancellationToken cancellationToken = default);

    Task<FollowPairSnapshot?> ReadFollowPairAsync(
        AccountKey sourceAccountKey,
        AccountKey targetAccountKey,
        CancellationToken cancellationToken = default);

    Task<ActivityWindowAggregateSnapshot> ReadActivityWindowAggregateAsync(
        AccountKey accountKey,
        long expectedAccountStateVersion,
        long repositoryGeneration,
        long cutMinuteUtc,
        CancellationToken cancellationToken = default);

    Task<LifecycleAdvanceResult> StartLifecycleAsync(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken = default);

    Task<LifecycleAdvanceResult> AdvanceLifecycleAsync(
        DurableDeliveryReservation reservation,
        int pageSize,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Production adapter over the reviewed PostgreSQL stores and lifecycle orchestrator.
/// </summary>
public sealed class PostgreSqlDurableTapBackend : IDurableTapBackend
{
    private readonly PostgreSqlIngestionStore _ingestion;
    private readonly PostgreSqlPlanningStore _planning;
    private readonly PostgreSqlLifecycleOrchestrator _lifecycle;

    public PostgreSqlDurableTapBackend(
        PostgreSqlIngestionStore ingestion,
        PostgreSqlPlanningStore planning,
        PostgreSqlLifecycleOrchestrator lifecycle)
    {
        _ingestion = ingestion ?? throw new ArgumentNullException(nameof(ingestion));
        _planning = planning ?? throw new ArgumentNullException(nameof(planning));
        _lifecycle = lifecycle ?? throw new ArgumentNullException(nameof(lifecycle));
    }

    public Task<DurableDeliveryReservation> ReserveDeliveryAsync(
        DurableDeliveryReservationRequest request,
        CancellationToken cancellationToken = default)
        => _ingestion.ReserveDeliveryAsync(request, cancellationToken);

    public Task<DurableCommitResult> CommitAsync(
        DurableDeliveryReservation reservation,
        DurableIngestionCommit commit,
        CancellationToken cancellationToken = default)
        => _ingestion.CommitAsync(reservation, commit, cancellationToken);

    public Task<DurableCommitResult> CommitValidatedNoOpAsync(
        DurableDeliveryReservation reservation,
        DurableValidatedNoOp noOp,
        CancellationToken cancellationToken = default)
        => _ingestion.CommitValidatedNoOpAsync(reservation, noOp, cancellationToken);

    public Task<DurableCommitResult> CommitQuarantineAsync(
        DurableDeliveryReservation reservation,
        DurableQuarantine quarantine,
        CancellationToken cancellationToken = default)
        => _ingestion.CommitQuarantineAsync(reservation, quarantine, cancellationToken);

    public Task<AccountStateSnapshot?> ReadAccountAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
        => _planning.ReadAccountAsync(accountKey, cancellationToken);

    public Task<ProjectionSnapshot?> ReadDesiredProjectionAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
        => _planning.ReadDesiredProjectionAsync(accountKey, cancellationToken);

    public Task<RecordStateSnapshot?> ReadRecordAsync(
        AccountKey accountKey,
        long repositoryGeneration,
        DurableRecordKind collection,
        string recordKey,
        CancellationToken cancellationToken = default)
        => _planning.ReadRecordAsync(
            accountKey,
            repositoryGeneration,
            collection,
            recordKey,
            cancellationToken);

    public Task<FollowPairSnapshot?> ReadFollowPairAsync(
        AccountKey sourceAccountKey,
        AccountKey targetAccountKey,
        CancellationToken cancellationToken = default)
        => _planning.ReadFollowPairAsync(sourceAccountKey, targetAccountKey, cancellationToken);

    public Task<ActivityWindowAggregateSnapshot> ReadActivityWindowAggregateAsync(
        AccountKey accountKey,
        long expectedAccountStateVersion,
        long repositoryGeneration,
        long cutMinuteUtc,
        CancellationToken cancellationToken = default)
        => _planning.ReadActivityWindowAggregateAsync(
            accountKey,
            expectedAccountStateVersion,
            repositoryGeneration,
            cutMinuteUtc,
            cancellationToken);

    public Task<LifecycleAdvanceResult> StartLifecycleAsync(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken = default)
        => _lifecycle.StartAsync(reservation, envelope, cancellationToken);

    public Task<LifecycleAdvanceResult> AdvanceLifecycleAsync(
        DurableDeliveryReservation reservation,
        int pageSize,
        CancellationToken cancellationToken = default)
        => _lifecycle.AdvanceAsync(reservation, pageSize, cancellationToken);
}
