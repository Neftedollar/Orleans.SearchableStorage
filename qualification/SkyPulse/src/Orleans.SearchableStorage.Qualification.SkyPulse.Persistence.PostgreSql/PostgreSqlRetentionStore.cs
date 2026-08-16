using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Defines the exact minimum ages retained by one SkyPulse operational database.
/// </summary>
/// <remarks>
/// A deployment must freeze these values before a run. There are deliberately no defaults.
/// Watermarks are an independent proof that an upstream replay boundary has advanced; age alone
/// never authorizes deletion of delivery, semantic-event, or activity deduplication state.
/// </remarks>
public sealed record PostgreSqlRetentionPolicy
{
    public PostgreSqlRetentionPolicy(
        TimeSpan completedDeliveryAge,
        TimeSpan semanticEventAge,
        TimeSpan completedOutboxAge,
        TimeSpan quarantineAge,
        TimeSpan activityBucketAge)
    {
        CompletedDeliveryAge = Positive(completedDeliveryAge, nameof(completedDeliveryAge));
        SemanticEventAge = Positive(semanticEventAge, nameof(semanticEventAge));
        CompletedOutboxAge = Positive(completedOutboxAge, nameof(completedOutboxAge));
        QuarantineAge = Positive(quarantineAge, nameof(quarantineAge));
        ActivityBucketAge = Positive(activityBucketAge, nameof(activityBucketAge));
        if (ActivityBucketAge < TimeSpan.FromDays(30))
        {
            throw new ArgumentOutOfRangeException(
                nameof(activityBucketAge),
                activityBucketAge,
                "Activity buckets must be retained for at least the longest thirty-day projection window.");
        }
    }

    public TimeSpan CompletedDeliveryAge { get; }

    public TimeSpan SemanticEventAge { get; }

    public TimeSpan CompletedOutboxAge { get; }

    public TimeSpan QuarantineAge { get; }

    public TimeSpan ActivityBucketAge { get; }

    private static TimeSpan Positive(TimeSpan value, string parameterName)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "A retention age must be positive.");
        }

        return value;
    }
}

/// <summary>
/// Reports one bounded deletion attempt.
/// </summary>
public readonly record struct PostgreSqlRetentionBatchResult(int DeletedRows, int BatchLimit)
{
    /// <summary>
    /// Gets whether another eligible batch may exist. A full batch is not proof that more rows do
    /// exist, so callers stop only after a later batch returns fewer rows than its limit.
    /// </summary>
    public bool MayHaveMore => DeletedRows == BatchLimit;
}

/// <summary>
/// Thrown when cleanup is requested beyond a persisted, evidence-backed replay watermark.
/// </summary>
public sealed class PostgreSqlRetentionWatermarkException : InvalidOperationException
{
    internal PostgreSqlRetentionWatermarkException(string scope)
        : base($"The persisted {scope} retention watermark does not authorize the requested cleanup boundary.")
    {
        Scope = scope;
    }

    public string Scope { get; }
}

/// <summary>
/// Advances evidence-backed replay watermarks and deletes operational history in bounded batches.
/// </summary>
/// <remarks>
/// Every cleanup statement selects at most the requested batch size with
/// <c>FOR UPDATE SKIP LOCKED</c>. Current-generation record rows (including deletion tombstones),
/// pending TAP deliveries, unfinished projection outbox rows, current relationship state, desired
/// projections, and published projections are never deletion candidates.
/// </remarks>
public sealed class PostgreSqlRetentionStore
{
    public const int MaximumBatchSize = 1_000;

    private const long ThirtyDaysInMinutes = 43_200;
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlRetentionPolicy _policy;

    public PostgreSqlRetentionStore(NpgsqlDataSource dataSource, PostgreSqlRetentionPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(policy);
        _dataSource = dataSource;
        _policy = policy;
    }

    /// <summary>
    /// Monotonically records that one durable TAP source will not redeliver identifiers at or below
    /// the supplied value without using the separately retained replay archive.
    /// </summary>
    public Task AdvanceSourceDeliveryWatermarkAsync(
        Guid sourceInstanceId,
        ulong safeDeliveryIdInclusive,
        string evidenceReference,
        CancellationToken cancellationToken = default)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A durable source-instance identifier is required.", nameof(sourceInstanceId));
        }

        return AdvanceAsync(
            AdvanceSourceDeliveryWatermarkSql,
            "source-delivery",
            command =>
            {
                command.Parameters.AddWithValue("source_instance_id", NpgsqlDbType.Uuid, sourceInstanceId);
                command.Parameters.AddWithValue("safe_delivery_id_inclusive", NpgsqlDbType.Numeric, (decimal)safeDeliveryIdInclusive);
                AddEvidenceReference(command, evidenceReference);
            },
            cancellationToken);
    }

    /// <summary>
    /// Monotonically records that semantic events observed through the supplied UTC minute cannot
    /// re-enter this operational database from the live source or a replay.
    /// </summary>
    public Task AdvanceSemanticEventWatermarkAsync(
        long safeObservedMinuteUtc,
        string evidenceReference,
        CancellationToken cancellationToken = default)
    {
        NonNegativeMinute(safeObservedMinuteUtc, nameof(safeObservedMinuteUtc));
        return AdvanceAsync(
            AdvanceSemanticEventWatermarkSql,
            "semantic-event",
            command =>
            {
                command.Parameters.AddWithValue("safe_observed_minute_utc", NpgsqlDbType.Bigint, safeObservedMinuteUtc);
                AddEvidenceReference(command, evidenceReference);
            },
            cancellationToken);
    }

    /// <summary>
    /// Monotonically records that no activity mutation at or before the supplied UTC minute can be
    /// accepted from the live source or a replay.
    /// </summary>
    public Task AdvanceActivityWatermarkAsync(
        long safeMinuteUtc,
        string evidenceReference,
        CancellationToken cancellationToken = default)
    {
        NonNegativeMinute(safeMinuteUtc, nameof(safeMinuteUtc));
        return AdvanceAsync(
            AdvanceActivityWatermarkSql,
            "activity",
            command =>
            {
                command.Parameters.AddWithValue("safe_minute_utc", NpgsqlDbType.Bigint, safeMinuteUtc);
                AddEvidenceReference(command, evidenceReference);
            },
            cancellationToken);
    }

    /// <summary>
    /// Deletes completed TAP deliveries old enough under the frozen policy and authorized by the
    /// source watermark. A delivery remains protected while its quarantine diagnostic is retained;
    /// the foreign key additionally prevents an orphan during concurrent cleanup.
    /// </summary>
    public Task<PostgreSqlRetentionBatchResult> DeleteCompletedDeliveriesAsync(
        Guid sourceInstanceId,
        ulong safeDeliveryIdInclusive,
        DateTimeOffset asOfUtc,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A durable source-instance identifier is required.", nameof(sourceInstanceId));
        }

        var committedBeforeUtc = Cutoff(asOfUtc, _policy.CompletedDeliveryAge);

        return DeleteAuthorizedAsync(
            DeleteCompletedDeliveriesSql,
            "source-delivery",
            batchSize,
            command =>
            {
                command.Parameters.AddWithValue("source_instance_id", NpgsqlDbType.Uuid, sourceInstanceId);
                command.Parameters.AddWithValue("safe_delivery_id_inclusive", NpgsqlDbType.Numeric, (decimal)safeDeliveryIdInclusive);
                command.Parameters.AddWithValue("committed_before_utc", NpgsqlDbType.TimestampTz, committedBeforeUtc);
            },
            cancellationToken);
    }

    /// <summary>
    /// Deletes quarantined diagnostic metadata old enough under the frozen policy while retaining
    /// its completed delivery identity until the delivery policy independently permits deletion.
    /// </summary>
    public Task<PostgreSqlRetentionBatchResult> DeleteQuarantineAsync(
        Guid sourceInstanceId,
        ulong safeDeliveryIdInclusive,
        DateTimeOffset asOfUtc,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        if (sourceInstanceId == Guid.Empty)
        {
            throw new ArgumentException("A durable source-instance identifier is required.", nameof(sourceInstanceId));
        }

        var quarantinedBeforeUtc = Cutoff(asOfUtc, _policy.QuarantineAge);

        return DeleteAuthorizedAsync(
            DeleteQuarantineSql,
            "source-delivery",
            batchSize,
            command =>
            {
                command.Parameters.AddWithValue("source_instance_id", NpgsqlDbType.Uuid, sourceInstanceId);
                command.Parameters.AddWithValue("safe_delivery_id_inclusive", NpgsqlDbType.Numeric, (decimal)safeDeliveryIdInclusive);
                command.Parameters.AddWithValue("quarantined_before_utc", NpgsqlDbType.TimestampTz, quarantinedBeforeUtc);
            },
            cancellationToken);
    }

    /// <summary>
    /// Deletes applied semantic-event deduplication rows old enough under the frozen policy and no
    /// newer than the requested, persisted replay watermark.
    /// </summary>
    public Task<PostgreSqlRetentionBatchResult> DeleteSemanticEventsAsync(
        long safeObservedMinuteUtc,
        DateTimeOffset asOfUtc,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        NonNegativeMinute(safeObservedMinuteUtc, nameof(safeObservedMinuteUtc));
        var appliedBeforeUtc = Cutoff(asOfUtc, _policy.SemanticEventAge);
        return DeleteAuthorizedAsync(
            DeleteSemanticEventsSql,
            "semantic-event",
            batchSize,
            command =>
            {
                command.Parameters.AddWithValue("safe_observed_minute_utc", NpgsqlDbType.Bigint, safeObservedMinuteUtc);
                command.Parameters.AddWithValue("applied_before_utc", NpgsqlDbType.TimestampTz, appliedBeforeUtc);
            },
            cancellationToken);
    }

    /// <summary>
    /// Deletes completed outbox history only after hydration proves the two-phase publication
    /// invariant: an upsert has the same or newer hydrated version; a removal has no same/older
    /// hydrated version. Pending and leased rows are never candidates.
    /// </summary>
    public Task<PostgreSqlRetentionBatchResult> DeleteCompletedOutboxAsync(
        DateTimeOffset asOfUtc,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        var completedBeforeUtc = Cutoff(asOfUtc, _policy.CompletedOutboxAge);
        return DeleteAsync(
            DeleteCompletedOutboxSql,
            batchSize,
            command => command.Parameters.AddWithValue(
                "completed_before_utc",
                NpgsqlDbType.TimestampTz,
                completedBeforeUtc),
            cancellationToken);
    }

    /// <summary>
    /// Deletes expired current-generation activity buckets only when the activity watermark and a
    /// durable desired projection both prove that the longest rolling window no longer needs them.
    /// </summary>
    public Task<PostgreSqlRetentionBatchResult> DeleteExpiredActivityAsync(
        long safeMinuteUtc,
        DateTimeOffset asOfUtc,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        NonNegativeMinute(safeMinuteUtc, nameof(safeMinuteUtc));
        var expiredBeforeMinuteUtc = ToUnixMinute(Cutoff(asOfUtc, _policy.ActivityBucketAge));
        return DeleteAuthorizedAsync(
            DeleteExpiredActivitySql,
            "activity",
            batchSize,
            command =>
            {
                command.Parameters.AddWithValue("safe_minute_utc", NpgsqlDbType.Bigint, safeMinuteUtc);
                command.Parameters.AddWithValue("expired_before_minute_utc", NpgsqlDbType.Bigint, expiredBeforeMinuteUtc);
                command.Parameters.AddWithValue("longest_window_minutes", NpgsqlDbType.Bigint, ThirtyDaysInMinutes);
            },
            cancellationToken);
    }

    /// <summary>
    /// Deletes record rows only from repository generations older than the durable account state.
    /// Current-generation deletion tombstones can never satisfy this statement.
    /// </summary>
    public Task<PostgreSqlRetentionBatchResult> DeleteObsoleteRecordStateAsync(
        int batchSize,
        CancellationToken cancellationToken = default)
        => DeleteAsync(DeleteObsoleteRecordStateSql, batchSize, configure: null, cancellationToken);

    /// <summary>
    /// Deletes activity buckets only from repository generations older than the durable account
    /// state. Current-generation rolling data is not a candidate.
    /// </summary>
    public Task<PostgreSqlRetentionBatchResult> DeleteObsoleteActivityAsync(
        int batchSize,
        CancellationToken cancellationToken = default)
        => DeleteAsync(DeleteObsoleteActivitySql, batchSize, configure: null, cancellationToken);

    private async Task AdvanceAsync(
        string sql,
        string scope,
        Action<NpgsqlCommand> configure,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        configure(command);
        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is null)
        {
            throw new PostgreSqlRetentionWatermarkException(scope);
        }
    }

    private async Task<PostgreSqlRetentionBatchResult> DeleteAuthorizedAsync(
        string sql,
        string scope,
        int batchSize,
        Action<NpgsqlCommand> configure,
        CancellationToken cancellationToken)
    {
        ValidateBatchSize(batchSize);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, batchSize);
        configure(command);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("A retention command did not return its authorization result.");
        }

        if (!reader.GetBoolean(0))
        {
            throw new PostgreSqlRetentionWatermarkException(scope);
        }

        return new PostgreSqlRetentionBatchResult(reader.GetInt32(1), batchSize);
    }

    private async Task<PostgreSqlRetentionBatchResult> DeleteAsync(
        string sql,
        int batchSize,
        Action<NpgsqlCommand>? configure,
        CancellationToken cancellationToken)
    {
        ValidateBatchSize(batchSize);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, batchSize);
        configure?.Invoke(command);
        var deleted = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return new PostgreSqlRetentionBatchResult(Convert.ToInt32(deleted, System.Globalization.CultureInfo.InvariantCulture), batchSize);
    }

    private static void ValidateBatchSize(int batchSize)
    {
        if (batchSize is < 1 or > MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize),
                batchSize,
                $"The batch size must be between 1 and {MaximumBatchSize}.");
        }
    }

    private static DateTimeOffset Cutoff(DateTimeOffset asOfUtc, TimeSpan age)
    {
        if (asOfUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("The retention instant must use the UTC offset.", nameof(asOfUtc));
        }

        try
        {
            return asOfUtc - age;
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new ArgumentOutOfRangeException(
                nameof(asOfUtc),
                asOfUtc,
                $"The retention cutoff is outside the supported timestamp range: {exception.Message}");
        }
    }

    private static long ToUnixMinute(DateTimeOffset value)
    {
        var seconds = value.ToUnixTimeSeconds();
        if (seconds < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "SkyPulse retention minutes cannot precede the Unix epoch.");
        }

        return seconds / 60;
    }

    private static void NonNegativeMinute(long value, string parameterName)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, "A UTC minute must be non-negative.");
        }
    }

    private static void AddEvidenceReference(NpgsqlCommand command, string evidenceReference)
    {
        ArgumentNullException.ThrowIfNull(evidenceReference);
        if (string.IsNullOrWhiteSpace(evidenceReference)
            || Encoding.UTF8.GetByteCount(evidenceReference) > 1_024)
        {
            throw new ArgumentException("An evidence reference must contain between 1 and 1,024 UTF-8 bytes.", nameof(evidenceReference));
        }

        command.Parameters.AddWithValue("evidence_reference", NpgsqlDbType.Text, evidenceReference);
    }

    internal const string AdvanceSourceDeliveryWatermarkSql = """
        INSERT INTO skypulse.source_delivery_retention_watermark (
            source_instance_id, safe_delivery_id_inclusive, evidence_reference)
        VALUES (@source_instance_id, @safe_delivery_id_inclusive, @evidence_reference)
        ON CONFLICT (source_instance_id)
        DO UPDATE SET
            safe_delivery_id_inclusive = EXCLUDED.safe_delivery_id_inclusive,
            evidence_reference = EXCLUDED.evidence_reference,
            updated_at_utc = clock_timestamp()
        WHERE skypulse.source_delivery_retention_watermark.safe_delivery_id_inclusive <= EXCLUDED.safe_delivery_id_inclusive
        RETURNING safe_delivery_id_inclusive;
        """;

    internal const string AdvanceSemanticEventWatermarkSql = """
        INSERT INTO skypulse.semantic_event_retention_watermark (
            watermark_id, safe_observed_minute_utc, evidence_reference)
        VALUES (1, @safe_observed_minute_utc, @evidence_reference)
        ON CONFLICT (watermark_id)
        DO UPDATE SET
            safe_observed_minute_utc = EXCLUDED.safe_observed_minute_utc,
            evidence_reference = EXCLUDED.evidence_reference,
            updated_at_utc = clock_timestamp()
        WHERE skypulse.semantic_event_retention_watermark.safe_observed_minute_utc <= EXCLUDED.safe_observed_minute_utc
        RETURNING safe_observed_minute_utc;
        """;

    internal const string AdvanceActivityWatermarkSql = """
        INSERT INTO skypulse.activity_retention_watermark (
            watermark_id, safe_minute_utc, evidence_reference)
        VALUES (1, @safe_minute_utc, @evidence_reference)
        ON CONFLICT (watermark_id)
        DO UPDATE SET
            safe_minute_utc = EXCLUDED.safe_minute_utc,
            evidence_reference = EXCLUDED.evidence_reference,
            updated_at_utc = clock_timestamp()
        WHERE skypulse.activity_retention_watermark.safe_minute_utc <= EXCLUDED.safe_minute_utc
        RETURNING safe_minute_utc;
        """;

    internal const string DeleteCompletedDeliveriesSql = """
        WITH authorized AS MATERIALIZED (
            SELECT 1
            FROM skypulse.source_delivery_retention_watermark
            WHERE source_instance_id = @source_instance_id
              AND safe_delivery_id_inclusive >= @safe_delivery_id_inclusive
        ), candidates AS MATERIALIZED (
            SELECT delivery.ctid
            FROM skypulse.tap_delivery AS delivery
            CROSS JOIN authorized
            WHERE delivery.source_instance_id = @source_instance_id
              AND delivery.delivery_id <= @safe_delivery_id_inclusive
              AND delivery.outcome <> 0
              AND delivery.committed_at_utc < @committed_before_utc
              AND NOT EXISTS (
                  SELECT 1
                  FROM skypulse.quarantine AS quarantine
                  WHERE quarantine.source_instance_id = delivery.source_instance_id
                    AND quarantine.delivery_id = delivery.delivery_id)
            ORDER BY delivery.committed_at_utc, delivery.delivery_id
            LIMIT @batch_size
            FOR UPDATE OF delivery SKIP LOCKED
        ), deleted AS (
            DELETE FROM skypulse.tap_delivery AS delivery
            USING candidates
            WHERE delivery.ctid = candidates.ctid
            RETURNING 1
        )
        SELECT EXISTS (SELECT 1 FROM authorized), COUNT(*)::integer
        FROM deleted;
        """;

    internal const string DeleteQuarantineSql = """
        WITH authorized AS MATERIALIZED (
            SELECT 1
            FROM skypulse.source_delivery_retention_watermark
            WHERE source_instance_id = @source_instance_id
              AND safe_delivery_id_inclusive >= @safe_delivery_id_inclusive
        ), candidates AS MATERIALIZED (
            SELECT quarantine.ctid
            FROM skypulse.quarantine AS quarantine
            JOIN skypulse.tap_delivery AS delivery
              ON delivery.source_instance_id = quarantine.source_instance_id
             AND delivery.delivery_id = quarantine.delivery_id
            CROSS JOIN authorized
            WHERE quarantine.source_instance_id = @source_instance_id
              AND quarantine.delivery_id <= @safe_delivery_id_inclusive
              AND quarantine.quarantined_at_utc < @quarantined_before_utc
              AND delivery.outcome = 3
              AND delivery.committed_at_utc IS NOT NULL
            ORDER BY quarantine.quarantined_at_utc, quarantine.delivery_id
            LIMIT @batch_size
            FOR UPDATE OF quarantine SKIP LOCKED
        ), deleted AS (
            DELETE FROM skypulse.quarantine AS quarantine
            USING candidates
            WHERE quarantine.ctid = candidates.ctid
            RETURNING 1
        )
        SELECT EXISTS (SELECT 1 FROM authorized), COUNT(*)::integer
        FROM deleted;
        """;

    internal const string DeleteSemanticEventsSql = """
        WITH authorized AS MATERIALIZED (
            SELECT 1
            FROM skypulse.semantic_event_retention_watermark
            WHERE watermark_id = 1
              AND safe_observed_minute_utc >= @safe_observed_minute_utc
        ), candidates AS MATERIALIZED (
            SELECT event.ctid
            FROM skypulse.semantic_event AS event
            CROSS JOIN authorized
            WHERE event.observed_at_minute_utc <= @safe_observed_minute_utc
              AND event.applied_at_utc < @applied_before_utc
            ORDER BY event.observed_at_minute_utc, event.applied_at_utc, event.account_key,
                     event.repository_generation, event.semantic_digest
            LIMIT @batch_size
            FOR UPDATE OF event SKIP LOCKED
        ), deleted AS (
            DELETE FROM skypulse.semantic_event AS event
            USING candidates
            WHERE event.ctid = candidates.ctid
            RETURNING 1
        )
        SELECT EXISTS (SELECT 1 FROM authorized), COUNT(*)::integer
        FROM deleted;
        """;

    internal const string DeleteCompletedOutboxSql = """
        WITH candidates AS MATERIALIZED (
            SELECT outbox.ctid
            FROM skypulse.projection_outbox AS outbox
            LEFT JOIN skypulse.published_projection AS published
              ON published.account_key = outbox.account_key
            WHERE outbox.completed_at_utc IS NOT NULL
              AND outbox.completed_at_utc < @completed_before_utc
              AND outbox.lease_id IS NULL
              AND outbox.leased_until_utc IS NULL
              AND (
                  (outbox.operation = 1 AND published.projection_version >= outbox.projection_version)
                  OR (outbox.operation = 2
                      AND published.projection_version >= outbox.projection_version))
            ORDER BY outbox.completed_at_utc, outbox.account_key, outbox.projection_version
            LIMIT @batch_size
            FOR UPDATE OF outbox SKIP LOCKED
        ), deleted AS (
            DELETE FROM skypulse.projection_outbox AS outbox
            USING candidates
            WHERE outbox.ctid = candidates.ctid
            RETURNING 1
        )
        SELECT COUNT(*)::integer FROM deleted;
        """;

    internal const string DeleteExpiredActivitySql = """
        WITH authorized AS MATERIALIZED (
            SELECT 1
            FROM skypulse.activity_retention_watermark
            WHERE watermark_id = 1
              AND safe_minute_utc >= @safe_minute_utc
        ), candidates AS MATERIALIZED (
            SELECT bucket.ctid
            FROM skypulse.activity_minute_bucket AS bucket
            JOIN skypulse.account_state AS account
              ON account.account_key = bucket.account_key
             AND account.repository_generation = bucket.repository_generation
            JOIN skypulse.desired_projection AS projection
              ON projection.account_key = bucket.account_key
             AND projection.projection_version = account.state_version
            CROSS JOIN authorized
            WHERE bucket.minute_utc <= @safe_minute_utc
              AND bucket.minute_utc < @expired_before_minute_utc
              AND projection.projection_cut_minute_utc >= @longest_window_minutes
              AND bucket.minute_utc <= projection.projection_cut_minute_utc - @longest_window_minutes
            ORDER BY bucket.minute_utc, bucket.account_key, bucket.repository_generation
            LIMIT @batch_size
            FOR UPDATE OF bucket SKIP LOCKED
        ), deleted AS (
            DELETE FROM skypulse.activity_minute_bucket AS bucket
            USING candidates
            WHERE bucket.ctid = candidates.ctid
            RETURNING 1
        )
        SELECT EXISTS (SELECT 1 FROM authorized), COUNT(*)::integer
        FROM deleted;
        """;

    internal const string DeleteObsoleteRecordStateSql = """
        WITH candidates AS MATERIALIZED (
            SELECT record.ctid
            FROM skypulse.record_state AS record
            JOIN skypulse.account_state AS account
              ON account.account_key = record.account_key
             AND record.repository_generation < account.repository_generation
            ORDER BY record.account_key, record.repository_generation, record.collection, record.record_key
            LIMIT @batch_size
            FOR UPDATE OF record SKIP LOCKED
        ), deleted AS (
            DELETE FROM skypulse.record_state AS record
            USING candidates
            WHERE record.ctid = candidates.ctid
            RETURNING 1
        )
        SELECT COUNT(*)::integer FROM deleted;
        """;

    internal const string DeleteObsoleteActivitySql = """
        WITH candidates AS MATERIALIZED (
            SELECT bucket.ctid
            FROM skypulse.activity_minute_bucket AS bucket
            JOIN skypulse.account_state AS account
              ON account.account_key = bucket.account_key
             AND bucket.repository_generation < account.repository_generation
            ORDER BY bucket.account_key, bucket.repository_generation, bucket.minute_utc
            LIMIT @batch_size
            FOR UPDATE OF bucket SKIP LOCKED
        ), deleted AS (
            DELETE FROM skypulse.activity_minute_bucket AS bucket
            USING candidates
            WHERE bucket.ctid = candidates.ctid
            RETURNING 1
        )
        SELECT COUNT(*)::integer FROM deleted;
        """;
}
