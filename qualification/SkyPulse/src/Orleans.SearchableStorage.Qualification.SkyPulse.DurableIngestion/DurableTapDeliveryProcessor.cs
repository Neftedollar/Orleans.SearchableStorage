using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;
using Orleans.SearchableStorage.Qualification.SkyPulse.Tap;
using Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;

public enum DurableTapProcessingDisposition
{
    Acknowledge = 1,
    RetryWithoutAcknowledgement = 2,
}

/// <summary>
/// Contains no source-controlled diagnostic text. The delivery identifier is the only value that
/// may cross into the acknowledgement boundary.
/// </summary>
public sealed record DurableTapProcessingResult(
    ulong DeliveryId,
    DurableTapProcessingDisposition Disposition)
{
    public bool AcknowledgementAllowed => Disposition == DurableTapProcessingDisposition.Acknowledge;
}

public sealed class DurableTapProtocolException : IOException
{
    public DurableTapProtocolException(string code)
        : base("The TAP delivery violated the reviewed metadata transport contract.")
    {
        if (string.IsNullOrWhiteSpace(code)
            || code.Length > 80
            || code.Any(static character => character is not (>= 'a' and <= 'z') and not '-'))
        {
            throw new ArgumentException("A bounded fixed protocol code is required.", nameof(code));
        }

        Code = code;
    }

    public string Code { get; }
}

public sealed class DurableTapProcessingOptions
{
    public int MaximumPlanningAttempts { get; init; } = 8;

    public int LifecyclePageSize { get; init; } = 1_000;

    public void Validate()
    {
        if (MaximumPlanningAttempts is < 1 or > 32)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaximumPlanningAttempts),
                MaximumPlanningAttempts,
                "The number of immediate planning attempts must be between 1 and 32.");
        }

        if (LifecyclePageSize is < 1 or > PostgreSqlLifecycleOrchestrator.MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(LifecyclePageSize),
                LifecyclePageSize,
                $"The lifecycle page size must be between 1 and {PostgreSqlLifecycleOrchestrator.MaximumPageSize}.");
        }
    }
}

/// <summary>
/// Converts one ephemeral sanitized TAP frame into an acknowledgement-safe PostgreSQL decision.
/// Raw JSON is parsed in memory and is never passed to a persistence API.
/// </summary>
public sealed class DurableTapDeliveryProcessor
{
    private readonly Guid _sourceInstanceId;
    private readonly IDurableTapBackend _backend;
    private readonly IAccountAdmission _admission;
    private readonly DurableTapProcessingOptions _options;

    public DurableTapDeliveryProcessor(
        Guid sourceInstanceId,
        IDurableTapBackend backend,
        IAccountAdmission admission,
        DurableTapProcessingOptions? options = null)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A stable TAP source-instance identifier is required.", nameof(sourceInstanceId));
        }

        _backend = backend ?? throw new ArgumentNullException(nameof(backend));
        _admission = admission ?? throw new ArgumentNullException(nameof(admission));
        _options = options ?? new DurableTapProcessingOptions();
        _options.Validate();
        _sourceInstanceId = sourceInstanceId;
    }

    public async Task<DurableTapProcessingResult> ProcessAsync(
        TapDelivery delivery,
        DateTimeOffset observedAtUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        var recomputedDigest = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(delivery.Json))).ToLowerInvariant();
        if (!string.Equals(recomputedDigest, delivery.Sha256, StringComparison.Ordinal))
        {
            throw new DurableTapProtocolException("delivery-digest-mismatch");
        }

        var parsed = SanitizedTapEventParser.Parse(delivery.Json, observedAtUtc);
        if (parsed.TapDeliveryId is not { } deliveryId || deliveryId == 0)
        {
            throw new DurableTapProtocolException("delivery-id-unrecoverable");
        }

        var observedMinuteUtc = observedAtUtc.ToUnixTimeSeconds() / 60;
        var reservation = await _backend.ReserveDeliveryAsync(
            new DurableDeliveryReservationRequest(
                _sourceInstanceId,
                deliveryId,
                delivery.Sha256,
                observedMinuteUtc),
            cancellationToken).ConfigureAwait(false);
        if (!reservation.IsPending)
        {
            return Acknowledge(deliveryId);
        }

        if (!parsed.IsAccepted)
        {
            var reason = MapParserQuarantine(
                parsed.QuarantineCode
                    ?? throw new DurableTapProtocolException("parser-decision-incomplete"));
            var result = await _backend.CommitQuarantineAsync(
                reservation,
                new DurableQuarantine(
                    reservation.SourceInstanceId,
                    reservation.TapDeliveryId,
                    reservation.DeliveryDigest,
                    reason,
                    reservation.FirstObservedAtMinuteUtc),
                cancellationToken).ConfigureAwait(false);
            return FromCommit(deliveryId, result);
        }

        var acceptedEvent = parsed.AcceptedEvent
            ?? throw new DurableTapProtocolException("parser-decision-incomplete");
        if (!_admission.IsAdmitted(acceptedEvent.AccountKey))
        {
            var result = await _backend.CommitQuarantineAsync(
                reservation,
                new DurableQuarantine(
                    reservation.SourceInstanceId,
                    reservation.TapDeliveryId,
                    reservation.DeliveryDigest,
                    DurableQuarantineReason.AccountNotAdmitted,
                    reservation.FirstObservedAtMinuteUtc,
                    accountKey: acceptedEvent.AccountKey),
                cancellationToken).ConfigureAwait(false);
            return FromCommit(deliveryId, result);
        }

        var initialAccount = await _backend
            .ReadAccountAsync(acceptedEvent.AccountKey, cancellationToken)
            .ConfigureAwait(false);
        if (initialAccount is null)
        {
            throw new InvalidOperationException(
                "An admitted event owner has no bootstrapped durable account state.");
        }

        DurableEventEnvelope envelope;
        try
        {
            var generation = DurableEventEnvelopeMapper.ResolveRepositoryGeneration(
                acceptedEvent,
                initialAccount);
            envelope = DurableEventEnvelopeMapper.Map(acceptedEvent, reservation, generation);
        }
        catch (OverflowException)
        {
            var result = await _backend.CommitQuarantineAsync(
                reservation,
                new DurableQuarantine(
                    reservation.SourceInstanceId,
                    reservation.TapDeliveryId,
                    reservation.DeliveryDigest,
                    DurableQuarantineReason.CounterOverflow,
                    reservation.FirstObservedAtMinuteUtc,
                    accountKey: acceptedEvent.AccountKey),
                cancellationToken).ConfigureAwait(false);
            return FromCommit(deliveryId, result);
        }

        return acceptedEvent is RecordMutationEvent
            ? await ProcessRecordAsync(reservation, envelope, cancellationToken).ConfigureAwait(false)
            : await ProcessLifecycleAsync(reservation, envelope, cancellationToken).ConfigureAwait(false);
    }

    private async Task<DurableTapProcessingResult> ProcessRecordAsync(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < _options.MaximumPlanningAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var input = await ReadRecordPlanningInputAsync(
                    reservation,
                    envelope,
                    cancellationToken).ConfigureAwait(false);
                var decision = RecordMutationTransitionPlanner.Plan(input);
                DurableCommitResult? result = decision.Kind switch
                {
                    RecordMutationPlanningDecisionKind.Commit
                        => await _backend.CommitAsync(
                            reservation,
                            decision.Commit!,
                            cancellationToken).ConfigureAwait(false),
                    RecordMutationPlanningDecisionKind.ValidatedNoOp
                        => await _backend.CommitValidatedNoOpAsync(
                            reservation,
                            decision.ValidatedNoOp!,
                            cancellationToken).ConfigureAwait(false),
                    RecordMutationPlanningDecisionKind.Quarantine
                        => await _backend.CommitQuarantineAsync(
                            reservation,
                            decision.Quarantine!,
                            cancellationToken).ConfigureAwait(false),
                    RecordMutationPlanningDecisionKind.Retry => null,
                    RecordMutationPlanningDecisionKind.DeliveryAlreadyCompleted
                        => throw new InvalidOperationException(
                            "A pending reservation produced an inconsistent completed planning decision."),
                    _ => throw new InvalidOperationException("The record planning decision is outside its closed set."),
                };

                if (result is null || !result.AcknowledgementAllowed)
                {
                    continue;
                }

                return Acknowledge(reservation.TapDeliveryId);
            }
            catch (PlanningStateChangedException)
            {
                // A fenced read changed. Re-read every planning input; never acknowledge this attempt.
            }
            catch (ValidatedNoOpProofFailedException)
            {
                // Commit-time proof changed. Re-plan from current durable state without acknowledging.
            }
        }

        return Retry(reservation.TapDeliveryId);
    }

    private async Task<RecordMutationPlanningInput> ReadRecordPlanningInputAsync(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        var ownerState = await _backend
            .ReadAccountAsync(envelope.AccountKey, cancellationToken)
            .ConfigureAwait(false);
        if (ownerState is null)
        {
            throw new InvalidOperationException(
                "An admitted record owner lost its bootstrapped durable account state.");
        }

        var current = await _backend.ReadRecordAsync(
            envelope.AccountKey,
            envelope.RepositoryGeneration,
            envelope.Collection!.Value,
            envelope.RecordKey!,
            cancellationToken).ConfigureAwait(false);
        var owner = await ReadAccountPlanningSnapshotAsync(
            ownerState,
            envelope.IsLive,
            envelope.ObservedAtMinuteUtc,
            cancellationToken).ConfigureAwait(false);

        var pairEvidence = new List<FollowPairPlanningSnapshot>();
        var affectedKeys = new HashSet<AccountKey>();
        if (envelope.Collection == DurableRecordKind.GraphFollow)
        {
            AccountKey? oldTarget = current is { IsDeleted: false } ? current.TargetAccountKey : null;
            AccountKey? newTarget = envelope.Action == DurableRecordAction.Delete
                ? null
                : envelope.TargetAccountKey;
            foreach (var target in new[] { oldTarget, newTarget }
                .Where(static value => value.HasValue)
                .Select(static value => value!.Value)
                .Distinct()
                .Order())
            {
                var persistedPair = await _backend
                    .ReadFollowPairAsync(envelope.AccountKey, target, cancellationToken)
                    .ConfigureAwait(false);
                var multiplicity = persistedPair?.Multiplicity ?? 0;
                pairEvidence.Add(new FollowPairPlanningSnapshot(
                    envelope.AccountKey,
                    target,
                    multiplicity));
                var delta = (newTarget == target ? 1 : 0) - (oldTarget == target ? 1 : 0);
                if ((multiplicity > 0) != ((long)multiplicity + delta > 0))
                {
                    affectedKeys.Add(target);
                }
            }
        }

        var receivesEngagement = envelope.IsLive
            && envelope.Action == DurableRecordAction.Create
            && envelope.TargetAccountKey is { }
            && (envelope.Collection is DurableRecordKind.FeedLike or DurableRecordKind.FeedRepost
                || envelope.Collection == DurableRecordKind.FeedPost && envelope.IsDirectReply);
        if (receivesEngagement)
        {
            affectedKeys.Add(envelope.TargetAccountKey!.Value);
        }

        var affected = new List<AffectedAccountPlanningSnapshot>();
        foreach (var accountKey in affectedKeys.Order())
        {
            if (accountKey == envelope.AccountKey)
            {
                continue;
            }

            if (!_admission.IsAdmitted(accountKey))
            {
                affected.Add(AffectedAccountPlanningSnapshot.NotAdmitted(accountKey));
                continue;
            }

            var state = await _backend.ReadAccountAsync(accountKey, cancellationToken).ConfigureAwait(false);
            if (state is null)
            {
                throw new InvalidOperationException(
                    "An admitted affected account lost its bootstrapped durable account state.");
            }

            affected.Add(AffectedAccountPlanningSnapshot.Admitted(
                await ReadAccountPlanningSnapshotAsync(
                    state,
                    envelope.IsLive,
                    envelope.ObservedAtMinuteUtc,
                    cancellationToken).ConfigureAwait(false)));
        }

        return new RecordMutationPlanningInput(
            reservation,
            envelope,
            owner,
            current,
            affected,
            pairEvidence);
    }

    private async Task<AccountPlanningSnapshot> ReadAccountPlanningSnapshotAsync(
        AccountStateSnapshot state,
        bool isLive,
        long observedAtMinuteUtc,
        CancellationToken cancellationToken)
    {
        var desired = await _backend
            .ReadDesiredProjectionAsync(state.AccountKey, cancellationToken)
            .ConfigureAwait(false);
        ActivityWindowAggregateSnapshot? aggregate = null;
        if (isLive
            && state.Lifecycle == DurableAccountLifecycle.Active
            && state.SynchronizationComplete
            && desired is not null
            && desired.Version <= state.StateVersion)
        {
            var cutMinuteUtc = Math.Max(desired.ProjectionCutMinuteUtc, observedAtMinuteUtc);
            aggregate = await _backend.ReadActivityWindowAggregateAsync(
                state.AccountKey,
                state.StateVersion,
                state.RepositoryGeneration,
                cutMinuteUtc,
                cancellationToken).ConfigureAwait(false);
        }

        return new AccountPlanningSnapshot(state, desired, aggregate);
    }

    private async Task<DurableTapProcessingResult> ProcessLifecycleAsync(
        DurableDeliveryReservation reservation,
        DurableEventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        var result = await _backend
            .StartLifecycleAsync(reservation, envelope, cancellationToken)
            .ConfigureAwait(false);
        while (result.Disposition == LifecycleAdvanceDisposition.Pending)
        {
            cancellationToken.ThrowIfCancellationRequested();
            result = await _backend.AdvanceLifecycleAsync(
                reservation,
                _options.LifecyclePageSize,
                cancellationToken).ConfigureAwait(false);
        }

        return result.AcknowledgementAllowed
            ? Acknowledge(reservation.TapDeliveryId)
            : Retry(reservation.TapDeliveryId);
    }

    private static DurableQuarantineReason MapParserQuarantine(QuarantineCode code)
        => code switch
        {
            QuarantineCode.EventTooLarge => DurableQuarantineReason.EventTooLarge,
            QuarantineCode.MalformedJson => DurableQuarantineReason.MalformedJson,
            QuarantineCode.InvalidRoot => DurableQuarantineReason.InvalidRoot,
            QuarantineCode.MissingProperty => DurableQuarantineReason.MissingProperty,
            QuarantineCode.UnexpectedProperty => DurableQuarantineReason.UnexpectedProperty,
            QuarantineCode.InvalidValue => DurableQuarantineReason.InvalidValue,
            QuarantineCode.UnsupportedEventType => DurableQuarantineReason.UnsupportedEventType,
            QuarantineCode.UnsupportedCollection => DurableQuarantineReason.UnsupportedCollection,
            QuarantineCode.MissingPriorRecord => DurableQuarantineReason.MissingPriorRecord,
            QuarantineCode.ConflictingRevision => DurableQuarantineReason.ConflictingRecordRevision,
            QuarantineCode.InactiveAccountMutation => DurableQuarantineReason.InactiveAccountMutation,
            QuarantineCode.ReconciliationIncomplete => DurableQuarantineReason.ReconciliationIncomplete,
            QuarantineCode.ReconciliationRevisionConflict => DurableQuarantineReason.ReconciliationRevisionConflict,
            _ => throw new ArgumentOutOfRangeException(nameof(code), code, "The parser quarantine code is outside its closed set."),
        };

    private static DurableTapProcessingResult FromCommit(
        ulong deliveryId,
        DurableCommitResult result)
        => result.AcknowledgementAllowed ? Acknowledge(deliveryId) : Retry(deliveryId);

    private static DurableTapProcessingResult Acknowledge(ulong deliveryId)
        => new(deliveryId, DurableTapProcessingDisposition.Acknowledge);

    private static DurableTapProcessingResult Retry(ulong deliveryId)
        => new(deliveryId, DurableTapProcessingDisposition.RetryWithoutAcknowledgement);
}
