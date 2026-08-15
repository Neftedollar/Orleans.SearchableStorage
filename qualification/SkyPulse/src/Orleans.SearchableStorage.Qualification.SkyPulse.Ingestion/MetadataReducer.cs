using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Ingestion;

/// <summary>
/// Describes the outcome of one deterministic reducer transition.
/// </summary>
public enum ReductionDisposition
{
    Applied,
    Duplicate,
    IgnoredStale,
    Quarantined,
}

/// <summary>
/// Identifies a downstream action produced by the pure reducer.
/// </summary>
public abstract record ReducerInstruction(AccountKey AccountKey);

/// <summary>
/// Requests regeneration and durable publication of an admitted account projection.
/// </summary>
public sealed record RefreshProjectionInstruction(AccountKey AccountKey)
    : ReducerInstruction(AccountKey);

/// <summary>
/// Requests durable removal of an admitted account projection.
/// </summary>
public sealed record PurgeProjectionInstruction(AccountKey AccountKey)
    : ReducerInstruction(AccountKey);

/// <summary>
/// Starts durable reconciliation state for one exact repository lifecycle generation.
/// </summary>
public sealed record BeginRepositoryReconciliationInstruction(
    AccountKey AccountKey,
    long RepositoryGeneration)
    : ReducerInstruction(AccountKey);

/// <summary>
/// Records an account whose projection is affected by an in-progress repository snapshot.
/// </summary>
public sealed record TrackReconciliationDependencyInstruction(
    AccountKey AccountKey,
    long RepositoryGeneration,
    AccountKey AffectedAccountKey)
    : ReducerInstruction(AccountKey);

/// <summary>
/// Cancels and clears an in-progress repository snapshot after an inactive lifecycle transition.
/// </summary>
public sealed record CancelRepositoryReconciliationInstruction(
    AccountKey AccountKey,
    long RepositoryGeneration)
    : ReducerInstruction(AccountKey);

/// <summary>
/// Requests exact owner reconciliation after TAP closes the repository snapshot barrier.
/// </summary>
public sealed record ReconcileAccountInstruction(
    AccountKey AccountKey,
    long RepositoryGeneration,
    string RepositoryRevision)
    : ReducerInstruction(AccountKey);

/// <summary>
/// Contains the reducer decision and deterministic downstream instructions for one event.
/// </summary>
public sealed record ReductionDecision
{
    private ReductionDecision(
        ReductionDisposition disposition,
        IReadOnlyList<ReducerInstruction> instructions,
        QuarantineCode? quarantineCode,
        string? quarantineMessage)
    {
        Disposition = disposition;
        Instructions = instructions;
        QuarantineCode = quarantineCode;
        QuarantineMessage = quarantineMessage;
    }

    public ReductionDisposition Disposition { get; }

    public IReadOnlyList<ReducerInstruction> Instructions { get; }

    public QuarantineCode? QuarantineCode { get; }

    public string? QuarantineMessage { get; }

    internal static ReductionDecision Applied(IEnumerable<ReducerInstruction> instructions)
        => new(
            ReductionDisposition.Applied,
            instructions.OrderBy(static instruction => instruction.AccountKey).ToArray(),
            null,
            null);

    internal static ReductionDecision Duplicate()
        => new(ReductionDisposition.Duplicate, [], null, null);

    internal static ReductionDecision IgnoredStale()
        => new(ReductionDisposition.IgnoredStale, [], null, null);

    internal static ReductionDecision Quarantined(QuarantineCode code, string message)
        => new(ReductionDisposition.Quarantined, [], code, message);
}

/// <summary>
/// Contains the exact metadata projection at a caller-selected UTC cut.
/// </summary>
/// <remarks>
/// Current-stock counters describe the reducer's current state. The caller must therefore use a
/// cut at or after the state watermark. Trailing windows contain live events in the half-open
/// interval <c>(cut - duration, cut]</c>; historical backfill never enters those windows.
/// Following and follower values count distinct source-to-target relationships. Multiple current
/// follow records for the same pair are retained as multiplicity but contribute one relationship.
/// </remarks>
public sealed record AccountMetricsSnapshot(
    AccountKey AccountKey,
    long LastActivityMinuteUtc,
    RollingWindowCounts CreatedRecordCounts,
    RollingWindowCounts UpdatedRecordCounts,
    RollingWindowCounts DeletedRecordCounts,
    long CurrentPostCount,
    long CurrentFollowingCount,
    long CurrentFollowerCount,
    RollingWindowCounts PostCreateCounts,
    long ReceivedEngagementCreates30Days);

/// <summary>
/// Reduces sanitized metadata events into exact current stock and trailing activity counters.
/// </summary>
/// <remarks>
/// This class is an in-memory reference reducer used to specify semantics and test adapters. It
/// deliberately has no database, transport, Orleans, or TAP dependency. Production persistence
/// can store the same record map, semantic-event set, rolling events, and account counters in a
/// transaction without changing these rules.
/// </remarks>
public sealed class MetadataReducer
{
    private const long OneDayMinutes = 24 * 60;
    private const long SevenDayMinutes = 7 * OneDayMinutes;
    private const long ThirtyDayMinutes = 30 * OneDayMinutes;

    private readonly object _gate = new();
    private readonly Func<AccountKey, bool> _isAdmitted;
    private readonly Dictionary<RecordAddress, StoredRecord> _records = [];
    private readonly Dictionary<FollowPair, int> _followPairMultiplicity = [];
    private readonly HashSet<ProcessedRecordEvent> _processedRecordEvents = [];
    private readonly Dictionary<AccountKey, MutableAccountMetrics> _metrics = [];
    private readonly Dictionary<AccountKey, MutableRepositoryState> _repositories = [];
    private readonly Dictionary<AccountKey, HashSet<RepositoryCycle>> _projectionBlockers = [];
    private readonly Dictionary<AccountKey, List<RollingRecordMutation>> _rollingMutations = [];
    private readonly Dictionary<AccountKey, List<long>> _receivedEngagementCreates = [];

    public MetadataReducer(Func<AccountKey, bool> isAdmitted)
    {
        ArgumentNullException.ThrowIfNull(isAdmitted);
        _isAdmitted = isAdmitted;
    }

    /// <summary>
    /// Gets the number of current records retained by the reference reducer.
    /// </summary>
    public int CurrentRecordCount
    {
        get
        {
            lock (_gate)
            {
                return _records.Count;
            }
        }
    }

    /// <summary>
    /// Applies one accepted domain event atomically to the reference state.
    /// </summary>
    public ReductionDecision Apply(IngestionEvent ingestionEvent)
    {
        ArgumentNullException.ThrowIfNull(ingestionEvent);

        lock (_gate)
        {
            return ingestionEvent switch
            {
                RecordMutationEvent mutation => ApplyRecordMutation(mutation),
                AccountLifecycleEvent lifecycle => ApplyLifecycle(lifecycle),
                RepositorySyncEvent repositorySync => ApplyRepositorySync(repositorySync),
                _ => throw new ArgumentException("The ingestion event type is not supported.", nameof(ingestionEvent)),
            };
        }
    }

    /// <summary>
    /// Attempts to materialize an admitted, active account at a fixed UTC projection cut.
    /// </summary>
    public bool TryGetSnapshot(
        AccountKey accountKey,
        DateTimeOffset projectionCutUtc,
        out AccountMetricsSnapshot? snapshot)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        var cutMinuteUtc = projectionCutUtc.ToUnixTimeSeconds() / 60;
        if (cutMinuteUtc < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(projectionCutUtc),
                projectionCutUtc,
                "The projection cut cannot precede the Unix epoch.");
        }

        lock (_gate)
        {
            if (!_isAdmitted(accountKey) || !IsProjectionAvailable(accountKey))
            {
                snapshot = null;
                return false;
            }

            var metrics = GetMetrics(accountKey);
            _rollingMutations.TryGetValue(accountKey, out var mutations);
            _receivedEngagementCreates.TryGetValue(accountKey, out var engagements);

            snapshot = new AccountMetricsSnapshot(
                accountKey,
                metrics.LastActivityMinuteUtc,
                CountActions(mutations, RecordMutationAction.Create, cutMinuteUtc),
                CountActions(mutations, RecordMutationAction.Update, cutMinuteUtc),
                CountActions(mutations, RecordMutationAction.Delete, cutMinuteUtc),
                metrics.CurrentPostCount,
                metrics.CurrentFollowingCount,
                metrics.CurrentFollowerCount,
                CountPostCreates(mutations, cutMinuteUtc),
                CountWindow(engagements, cutMinuteUtc, ThirtyDayMinutes));
            return true;
        }
    }

    private ReductionDecision ApplyRecordMutation(RecordMutationEvent mutation)
    {
        var processedEvent = new ProcessedRecordEvent(
            mutation.AccountKey,
            GetRepositoryGeneration(mutation.AccountKey),
            mutation.SemanticKey);
        if (_processedRecordEvents.Contains(processedEvent))
        {
            return ReductionDecision.Duplicate();
        }

        if (IsInactive(mutation.AccountKey))
        {
            return ReductionDecision.Quarantined(
                QuarantineCode.InactiveAccountMutation,
                "A record mutation cannot be applied while its repository is inactive.");
        }

        if (mutation.IsLive && !IsRepositorySynchronized(mutation.AccountKey))
        {
            return ReductionDecision.Quarantined(
                QuarantineCode.ReconciliationIncomplete,
                "A live mutation cannot be applied before the current repository snapshot is complete.");
        }

        var repositoryState = GetOrCreateActiveRepository(mutation.AccountKey);
        if (mutation.IsLive
            && repositoryState.LastAppliedRevision is { } lastAppliedRevision
            && string.CompareOrdinal(mutation.Revision, lastAppliedRevision) < 0)
        {
            _processedRecordEvents.Add(processedEvent);
            return ReductionDecision.IgnoredStale();
        }

        var address = new RecordAddress(mutation.AccountKey, mutation.Collection, mutation.RecordKey);
        _records.TryGetValue(address, out var existing);

        if (existing is not null)
        {
            var revisionComparison = string.CompareOrdinal(mutation.Revision, existing.Revision);
            if (revisionComparison < 0)
            {
                _processedRecordEvents.Add(processedEvent);
                return ReductionDecision.IgnoredStale();
            }

            if (revisionComparison == 0)
            {
                return ReductionDecision.Quarantined(
                    QuarantineCode.ConflictingRevision,
                    "Different mutations cannot occupy the same repository revision and record key.");
            }
        }

        if (mutation.Action == RecordMutationAction.Delete && existing is null)
        {
            return ReductionDecision.Quarantined(
                QuarantineCode.MissingPriorRecord,
                "A delete requires the previously stored metadata record.");
        }

        var refresh = new HashSet<AccountKey>();
        if (mutation.Action == RecordMutationAction.Delete)
        {
            RemoveCurrentRecord(address, existing!, refresh);
        }
        else
        {
            UpsertCurrentRecord(address, existing, mutation, refresh);
        }

        if (mutation.IsLive)
        {
            ApplyRollingActivity(mutation, refresh);
            repositoryState.LastAppliedRevision = MaximumRevision(
                repositoryState.LastAppliedRevision,
                mutation.Revision);
            _processedRecordEvents.Add(processedEvent);
            return ReductionDecision.Applied(
                refresh
                    .Where(IsProjectionAvailable)
                    .Select(static accountKey => new RefreshProjectionInstruction(accountKey)));
        }

        var repository = GetOrCreateActiveRepository(mutation.AccountKey);
        var instructions = new List<ReducerInstruction>();
        EnsureReconciliationStarted(mutation.AccountKey, repository, instructions);
        repository.PendingMaximumRevision = MaximumRevision(
            repository.PendingMaximumRevision,
            mutation.Revision);
        foreach (var accountKey in refresh)
        {
            TrackReconciliationDependency(
                mutation.AccountKey,
                repository,
                accountKey,
                instructions);
        }

        _processedRecordEvents.Add(processedEvent);
        return ReductionDecision.Applied(instructions);
    }

    private ReductionDecision ApplyLifecycle(AccountLifecycleEvent lifecycle)
    {
        if (_repositories.TryGetValue(lifecycle.AccountKey, out var repository)
            && repository.Status == lifecycle.Status)
        {
            return ReductionDecision.Duplicate();
        }

        repository ??= new MutableRepositoryState(AccountLifecycleStatus.Active);
        _repositories[lifecycle.AccountKey] = repository;
        if (lifecycle.Status == AccountLifecycleStatus.Active)
        {
            repository.Status = AccountLifecycleStatus.Active;
            var activeInstructions = new List<ReducerInstruction>();
            EnsureReconciliationStarted(lifecycle.AccountKey, repository, activeInstructions);
            return ReductionDecision.Applied(activeInstructions);
        }

        var cancelledGeneration = repository.Generation;
        var cancelledReconciliation = repository.IsReconciliationPending;
        var reconciliationAffected = ClearPendingReconciliation(lifecycle.AccountKey, repository);
        repository.Status = lifecycle.Status;
        repository.Generation = checked(repository.Generation + 1);
        repository.CompletedRevision = null;
        repository.LastAppliedRevision = null;

        var refresh = new HashSet<AccountKey>();
        var ownedRecords = _records
            .Where(pair => pair.Key.AccountKey == lifecycle.AccountKey)
            .ToArray();
        foreach (var pair in ownedRecords)
        {
            RemoveCurrentRecord(pair.Key, pair.Value, refresh);
        }

        _rollingMutations.Remove(lifecycle.AccountKey);
        _receivedEngagementCreates.Remove(lifecycle.AccountKey);
        if (_metrics.TryGetValue(lifecycle.AccountKey, out var metrics))
        {
            metrics.LastActivityMinuteUtc = 0;
        }

        refresh.UnionWith(reconciliationAffected);
        var instructions = refresh
            .Where(accountKey => accountKey != lifecycle.AccountKey && IsProjectionAvailable(accountKey))
            .Select(static accountKey => (ReducerInstruction)new RefreshProjectionInstruction(accountKey))
            .ToList();
        if (cancelledReconciliation)
        {
            instructions.Add(
                new CancelRepositoryReconciliationInstruction(
                    lifecycle.AccountKey,
                    cancelledGeneration));
        }

        if (_isAdmitted(lifecycle.AccountKey))
        {
            instructions.Add(new PurgeProjectionInstruction(lifecycle.AccountKey));
        }

        return ReductionDecision.Applied(instructions);
    }

    private ReductionDecision ApplyRepositorySync(RepositorySyncEvent repositorySync)
    {
        var repository = GetOrCreateActiveRepository(repositorySync.AccountKey);
        if (repository.Status != AccountLifecycleStatus.Active)
        {
            return ReductionDecision.Quarantined(
                QuarantineCode.InactiveAccountMutation,
                "A repository sync cannot complete while its repository is inactive.");
        }

        if (repository.LastAppliedRevision is { } lastAppliedRevision
            && string.CompareOrdinal(repositorySync.Revision, lastAppliedRevision) < 0)
        {
            return ReductionDecision.Quarantined(
                QuarantineCode.ReconciliationRevisionConflict,
                "The repository sync revision precedes the last applied live repository revision.");
        }

        if (!repository.IsReconciliationPending)
        {
            var comparison = repository.CompletedRevision is null
                ? 1
                : string.CompareOrdinal(repositorySync.Revision, repository.CompletedRevision);
            if (comparison == 0)
            {
                return ReductionDecision.Duplicate();
            }

            if (comparison < 0)
            {
                return ReductionDecision.IgnoredStale();
            }

            repository.CompletedRevision = repositorySync.Revision;
            repository.LastAppliedRevision = MaximumRevision(
                repository.LastAppliedRevision,
                repositorySync.Revision);
            return ReductionDecision.Applied(
                [new ReconcileAccountInstruction(
                    repositorySync.AccountKey,
                    repository.Generation,
                    repositorySync.Revision)]);
        }

        if (repository.PendingMaximumRevision is { } pendingMaximum
            && string.CompareOrdinal(repositorySync.Revision, pendingMaximum) < 0)
        {
            return ReductionDecision.Quarantined(
                QuarantineCode.ReconciliationRevisionConflict,
                "The repository sync revision precedes a mutation in the current snapshot.");
        }

        if (repository.CompletedRevision is { } completedRevision
            && string.CompareOrdinal(repositorySync.Revision, completedRevision) < 0)
        {
            return ReductionDecision.Quarantined(
                QuarantineCode.ReconciliationRevisionConflict,
                "The repository sync revision would regress the current lifecycle generation.");
        }

        var affected = ClearPendingReconciliation(repositorySync.AccountKey, repository);
        repository.CompletedRevision = repositorySync.Revision;
        repository.LastAppliedRevision = MaximumRevision(
            repository.LastAppliedRevision,
            repositorySync.Revision);

        var instructions = new List<ReducerInstruction>
        {
            new ReconcileAccountInstruction(
                repositorySync.AccountKey,
                repository.Generation,
                repositorySync.Revision),
        };
        instructions.AddRange(
            affected
                .Where(accountKey => accountKey != repositorySync.AccountKey
                    && _isAdmitted(accountKey)
                    && IsProjectionAvailable(accountKey))
                .Select(static accountKey => (ReducerInstruction)new RefreshProjectionInstruction(accountKey)));
        return ReductionDecision.Applied(instructions);
    }

    private void UpsertCurrentRecord(
        RecordAddress address,
        StoredRecord? existing,
        RecordMutationEvent mutation,
        HashSet<AccountKey> refresh)
    {
        if (existing is null)
        {
            if (mutation.Collection == AtRecordKind.FeedPost && _isAdmitted(mutation.AccountKey))
            {
                Increment(ref GetMetrics(mutation.AccountKey).CurrentPostCount);
                refresh.Add(mutation.AccountKey);
            }

            if (mutation.Collection == AtRecordKind.GraphFollow)
            {
                var target = RequireTarget(mutation);
                AddFollowPair(mutation.AccountKey, target, refresh);
            }
        }
        else if (mutation.Collection == AtRecordKind.GraphFollow)
        {
            var oldTarget = existing.TargetAccountKey
                ?? throw new InvalidOperationException("A stored follow record must contain its target.");
            var newTarget = RequireTarget(mutation);
            if (oldTarget != newTarget)
            {
                RemoveFollowPair(mutation.AccountKey, oldTarget, refresh);
                AddFollowPair(mutation.AccountKey, newTarget, refresh);
            }
        }

        _records[address] = new StoredRecord(mutation.Revision, mutation.TargetAccountKey);
    }

    private void RemoveCurrentRecord(
        RecordAddress address,
        StoredRecord existing,
        HashSet<AccountKey> refresh)
    {
        if (address.Collection == AtRecordKind.FeedPost && _isAdmitted(address.AccountKey))
        {
            Decrement(ref GetMetrics(address.AccountKey).CurrentPostCount);
            refresh.Add(address.AccountKey);
        }

        if (address.Collection == AtRecordKind.GraphFollow)
        {
            var target = existing.TargetAccountKey
                ?? throw new InvalidOperationException("A stored follow record must contain its target.");
            RemoveFollowPair(address.AccountKey, target, refresh);
        }

        _records.Remove(address);
    }

    private void ApplyRollingActivity(
        RecordMutationEvent mutation,
        HashSet<AccountKey> refresh)
    {
        if (_isAdmitted(mutation.AccountKey))
        {
            var metrics = GetMetrics(mutation.AccountKey);
            metrics.LastActivityMinuteUtc = Math.Max(
                metrics.LastActivityMinuteUtc,
                mutation.ObservedAtMinuteUtc);
            GetRollingMutations(mutation.AccountKey).Add(
                new RollingRecordMutation(
                    mutation.ObservedAtMinuteUtc,
                    mutation.Action,
                    mutation.Collection == AtRecordKind.FeedPost));
            refresh.Add(mutation.AccountKey);
        }

        if (mutation.Action != RecordMutationAction.Create)
        {
            return;
        }

        var createsReceivedEngagement = mutation.Collection is AtRecordKind.FeedLike
            or AtRecordKind.FeedRepost
            || (mutation.Collection == AtRecordKind.FeedPost && mutation.IsDirectReply);
        if (!createsReceivedEngagement || mutation.TargetAccountKey is not { } target || !_isAdmitted(target))
        {
            return;
        }

        GetReceivedEngagementCreates(target).Add(mutation.ObservedAtMinuteUtc);
        refresh.Add(target);
    }

    private bool IsInactive(AccountKey accountKey)
        => _repositories.TryGetValue(accountKey, out var repository)
            && repository.Status != AccountLifecycleStatus.Active;

    private bool IsRepositorySynchronized(AccountKey accountKey)
        => _repositories.TryGetValue(accountKey, out var repository)
            && repository.Status == AccountLifecycleStatus.Active
            && !repository.IsReconciliationPending
            && repository.CompletedRevision is not null;

    private bool IsProjectionAvailable(AccountKey accountKey)
        => IsRepositorySynchronized(accountKey)
            && !_projectionBlockers.ContainsKey(accountKey);

    private long GetRepositoryGeneration(AccountKey accountKey)
        => _repositories.TryGetValue(accountKey, out var repository)
            ? repository.Generation
            : 0;

    private MutableRepositoryState GetOrCreateActiveRepository(AccountKey accountKey)
    {
        if (!_repositories.TryGetValue(accountKey, out var repository))
        {
            repository = new MutableRepositoryState(AccountLifecycleStatus.Active);
            _repositories.Add(accountKey, repository);
        }

        return repository;
    }

    private void EnsureReconciliationStarted(
        AccountKey ownerAccountKey,
        MutableRepositoryState repository,
        List<ReducerInstruction> instructions)
    {
        if (repository.IsReconciliationPending)
        {
            return;
        }

        repository.IsReconciliationPending = true;
        repository.PendingMaximumRevision = null;
        repository.PendingAffectedAccountKeys.Clear();
        instructions.Add(
            new BeginRepositoryReconciliationInstruction(
                ownerAccountKey,
                repository.Generation));
        TrackReconciliationDependency(
            ownerAccountKey,
            repository,
            ownerAccountKey,
            instructions);
    }

    private void TrackReconciliationDependency(
        AccountKey ownerAccountKey,
        MutableRepositoryState repository,
        AccountKey affectedAccountKey,
        List<ReducerInstruction> instructions)
    {
        if (!repository.IsReconciliationPending)
        {
            throw new InvalidOperationException("A reconciliation dependency requires an active cycle.");
        }

        if (!repository.PendingAffectedAccountKeys.Add(affectedAccountKey))
        {
            return;
        }

        instructions.Add(
            new TrackReconciliationDependencyInstruction(
                ownerAccountKey,
                repository.Generation,
                affectedAccountKey));

        var cycle = new RepositoryCycle(ownerAccountKey, repository.Generation);
        if (!_projectionBlockers.TryGetValue(affectedAccountKey, out var blockers))
        {
            blockers = [];
            _projectionBlockers.Add(affectedAccountKey, blockers);
        }

        var wasBlocked = blockers.Count != 0;
        blockers.Add(cycle);
        if (!wasBlocked && _isAdmitted(affectedAccountKey))
        {
            instructions.Add(new PurgeProjectionInstruction(affectedAccountKey));
        }
    }

    private AccountKey[] ClearPendingReconciliation(
        AccountKey ownerAccountKey,
        MutableRepositoryState repository)
    {
        if (!repository.IsReconciliationPending)
        {
            return [];
        }

        var cycle = new RepositoryCycle(ownerAccountKey, repository.Generation);
        var affected = repository.PendingAffectedAccountKeys.ToArray();
        foreach (var affectedAccountKey in affected)
        {
            if (!_projectionBlockers.TryGetValue(affectedAccountKey, out var blockers))
            {
                throw new InvalidOperationException("A pending reconciliation dependency has no projection blocker.");
            }

            blockers.Remove(cycle);
            if (blockers.Count == 0)
            {
                _projectionBlockers.Remove(affectedAccountKey);
            }
        }

        repository.IsReconciliationPending = false;
        repository.PendingMaximumRevision = null;
        repository.PendingAffectedAccountKeys.Clear();
        return affected;
    }

    private static string MaximumRevision(string? current, string candidate)
        => current is null || string.CompareOrdinal(candidate, current) > 0
            ? candidate
            : current;

    private MutableAccountMetrics GetMetrics(AccountKey accountKey)
    {
        if (!_metrics.TryGetValue(accountKey, out var metrics))
        {
            metrics = new MutableAccountMetrics();
            _metrics.Add(accountKey, metrics);
        }

        return metrics;
    }

    private List<RollingRecordMutation> GetRollingMutations(AccountKey accountKey)
    {
        if (!_rollingMutations.TryGetValue(accountKey, out var mutations))
        {
            mutations = [];
            _rollingMutations.Add(accountKey, mutations);
        }

        return mutations;
    }

    private List<long> GetReceivedEngagementCreates(AccountKey accountKey)
    {
        if (!_receivedEngagementCreates.TryGetValue(accountKey, out var eventMinutes))
        {
            eventMinutes = [];
            _receivedEngagementCreates.Add(accountKey, eventMinutes);
        }

        return eventMinutes;
    }

    private void AddFollowPair(
        AccountKey source,
        AccountKey target,
        HashSet<AccountKey> refresh)
    {
        var pair = new FollowPair(source, target);
        if (_followPairMultiplicity.TryGetValue(pair, out var multiplicity))
        {
            _followPairMultiplicity[pair] = checked(multiplicity + 1);
            return;
        }

        _followPairMultiplicity.Add(pair, 1);
        if (_isAdmitted(source))
        {
            Increment(ref GetMetrics(source).CurrentFollowingCount);
            refresh.Add(source);
        }

        if (_isAdmitted(target))
        {
            Increment(ref GetMetrics(target).CurrentFollowerCount);
            refresh.Add(target);
        }
    }

    private void RemoveFollowPair(
        AccountKey source,
        AccountKey target,
        HashSet<AccountKey> refresh)
    {
        var pair = new FollowPair(source, target);
        if (!_followPairMultiplicity.TryGetValue(pair, out var multiplicity) || multiplicity <= 0)
        {
            throw new InvalidOperationException("A stored follow record must have a positive pair multiplicity.");
        }

        if (multiplicity > 1)
        {
            _followPairMultiplicity[pair] = multiplicity - 1;
            return;
        }

        _followPairMultiplicity.Remove(pair);
        if (_isAdmitted(source))
        {
            Decrement(ref GetMetrics(source).CurrentFollowingCount);
            refresh.Add(source);
        }

        if (_isAdmitted(target))
        {
            Decrement(ref GetMetrics(target).CurrentFollowerCount);
            refresh.Add(target);
        }
    }

    private static AccountKey RequireTarget(RecordMutationEvent mutation)
        => mutation.TargetAccountKey
            ?? throw new InvalidOperationException("The record collection requires a target account.");

    private static RollingWindowCounts CountActions(
        IReadOnlyList<RollingRecordMutation>? mutations,
        RecordMutationAction action,
        long cutMinuteUtc)
        => new(
            CountWindow(mutations, action, postOnly: false, cutMinuteUtc, OneDayMinutes),
            CountWindow(mutations, action, postOnly: false, cutMinuteUtc, SevenDayMinutes),
            CountWindow(mutations, action, postOnly: false, cutMinuteUtc, ThirtyDayMinutes));

    private static RollingWindowCounts CountPostCreates(
        IReadOnlyList<RollingRecordMutation>? mutations,
        long cutMinuteUtc)
        => new(
            CountWindow(mutations, RecordMutationAction.Create, postOnly: true, cutMinuteUtc, OneDayMinutes),
            CountWindow(mutations, RecordMutationAction.Create, postOnly: true, cutMinuteUtc, SevenDayMinutes),
            CountWindow(mutations, RecordMutationAction.Create, postOnly: true, cutMinuteUtc, ThirtyDayMinutes));

    private static long CountWindow(
        IReadOnlyList<RollingRecordMutation>? mutations,
        RecordMutationAction action,
        bool postOnly,
        long cutMinuteUtc,
        long durationMinutes)
    {
        if (mutations is null)
        {
            return 0;
        }

        var lowerExclusive = cutMinuteUtc - durationMinutes;
        long count = 0;
        foreach (var mutation in mutations)
        {
            if (mutation.MinuteUtc > lowerExclusive
                && mutation.MinuteUtc <= cutMinuteUtc
                && mutation.Action == action
                && (!postOnly || mutation.IsPost))
            {
                count++;
            }
        }

        return count;
    }

    private static long CountWindow(
        IReadOnlyList<long>? eventMinutes,
        long cutMinuteUtc,
        long durationMinutes)
    {
        if (eventMinutes is null)
        {
            return 0;
        }

        var lowerExclusive = cutMinuteUtc - durationMinutes;
        long count = 0;
        foreach (var eventMinute in eventMinutes)
        {
            if (eventMinute > lowerExclusive && eventMinute <= cutMinuteUtc)
            {
                count++;
            }
        }

        return count;
    }

    private static void Increment(ref long value) => value = checked(value + 1);

    private static void Decrement(ref long value)
    {
        if (value == 0)
        {
            throw new InvalidOperationException("Reducer current-stock counters cannot become negative.");
        }

        value--;
    }

    private readonly record struct RecordAddress(
        AccountKey AccountKey,
        AtRecordKind Collection,
        string RecordKey);

    private readonly record struct FollowPair(AccountKey Source, AccountKey Target);

    private readonly record struct ProcessedRecordEvent(
        AccountKey AccountKey,
        long RepositoryGeneration,
        SemanticEventKey SemanticEventKey);

    private readonly record struct RepositoryCycle(
        AccountKey AccountKey,
        long RepositoryGeneration);

    private sealed record StoredRecord(string Revision, AccountKey? TargetAccountKey);

    private readonly record struct RollingRecordMutation(
        long MinuteUtc,
        RecordMutationAction Action,
        bool IsPost);

    private sealed class MutableAccountMetrics
    {
        public long LastActivityMinuteUtc;
        public long CurrentPostCount;
        public long CurrentFollowingCount;
        public long CurrentFollowerCount;
    }

    private sealed class MutableRepositoryState(AccountLifecycleStatus status)
    {
        public AccountLifecycleStatus Status = status;
        public long Generation;
        public bool IsReconciliationPending;
        public string? PendingMaximumRevision;
        public string? CompletedRevision;
        public string? LastAppliedRevision;
        public HashSet<AccountKey> PendingAffectedAccountKeys { get; } = [];
    }
}
