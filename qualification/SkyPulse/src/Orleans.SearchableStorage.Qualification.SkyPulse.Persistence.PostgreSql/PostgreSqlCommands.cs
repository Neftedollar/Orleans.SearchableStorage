using Npgsql;
using NpgsqlTypes;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

internal static class PostgreSqlCommands
{
    internal const string ReadManifestSourceSql = """
        SELECT source_instance_id
        FROM skypulse.runtime_manifest
        WHERE manifest_id = 1
        FOR SHARE;
        """;

    internal const string ReserveDeliverySql = """
        INSERT INTO skypulse.tap_delivery (
            source_instance_id, delivery_id, delivery_digest, semantic_digest, account_key,
            observed_at_minute_utc, outcome, committed_at_utc)
        VALUES (@source_instance_id, @delivery_id, @delivery_digest, NULL, NULL,
            @first_observed_at_minute_utc, 0, NULL)
        ON CONFLICT (source_instance_id, delivery_id) DO NOTHING;
        """;

    internal const string ReadDeliverySql = """
        SELECT delivery_digest, observed_at_minute_utc, outcome
        FROM skypulse.tap_delivery
        WHERE source_instance_id = @source_instance_id
          AND delivery_id = @delivery_id
        FOR UPDATE;
        """;

    internal const string InsertSemanticEventSql = """
        INSERT INTO skypulse.semantic_event (
            semantic_digest, account_key, repository_generation, event_kind,
            observed_at_minute_utc, repository_revision, lifecycle, collection, action,
            record_key, cid, target_account_key, is_direct_reply, is_live)
        VALUES (
            @semantic_digest, @account_key, @repository_generation, @event_kind,
            @observed_at_minute_utc, @repository_revision, @lifecycle, @collection, @action,
            @record_key, @cid, @target_account_key, @is_direct_reply, @is_live)
        ON CONFLICT (account_key, repository_generation, semantic_digest) DO NOTHING;
        """;

    internal const string CompleteDeliverySql = """
        UPDATE skypulse.tap_delivery
        SET outcome = @outcome,
            semantic_digest = @semantic_digest,
            account_key = @account_key,
            committed_at_utc = clock_timestamp()
        WHERE source_instance_id = @source_instance_id
          AND delivery_id = @delivery_id
          AND delivery_digest = @delivery_digest
          AND observed_at_minute_utc = @first_observed_at_minute_utc
          AND outcome = 0;
        """;

    internal const string AddReconciliationDependencySql = """
        INSERT INTO skypulse.reconciliation_dependency (
            owner_account_key, owner_repository_generation, affected_account_key)
        VALUES (@owner_account_key, @owner_repository_generation, @affected_account_key)
        ON CONFLICT (owner_account_key, owner_repository_generation, affected_account_key)
        DO NOTHING;
        """;

    internal const string RemoveReconciliationDependencySql = """
        DELETE FROM skypulse.reconciliation_dependency
        WHERE owner_account_key = @owner_account_key
          AND owner_repository_generation = @owner_repository_generation
          AND affected_account_key = @affected_account_key;
        """;

    internal const string ProveRecordRevisionAlreadyObservedSql = """
        SELECT EXISTS (
            SELECT 1
            FROM skypulse.record_state
            WHERE account_key = @account_key
              AND repository_generation = @repository_generation
              AND collection = @collection
              AND record_key = @record_key
              AND convert_to(latest_revision, 'UTF8') >= convert_to(@repository_revision, 'UTF8'));
        """;

    internal const string ProveRepositoryGenerationSupersededSql = """
        SELECT EXISTS (
            SELECT 1
            FROM skypulse.account_state
            WHERE account_key = @account_key
              AND repository_generation > @repository_generation);
        """;

    internal const string ProveRepositorySyncRevisionAlreadyCompletedSql = """
        SELECT EXISTS (
            SELECT 1
            FROM skypulse.account_state
            WHERE account_key = @account_key
              AND repository_generation = @repository_generation
              AND synchronization_complete
              AND convert_to(completed_sync_revision, 'UTF8') >= convert_to(@repository_revision, 'UTF8'));
        """;

    internal const string ProveRepositoryRevisionAlreadyAppliedSql = """
        SELECT EXISTS (
            SELECT 1
            FROM skypulse.account_state
            WHERE account_key = @account_key
              AND repository_generation = @repository_generation
              AND last_applied_revision IS NOT NULL
              AND convert_to(last_applied_revision, 'UTF8') > convert_to(@repository_revision, 'UTF8'));
        """;

    internal const string InsertAccountStateSql = """
        INSERT INTO skypulse.account_state (
            account_key, state_version, lifecycle, repository_generation,
            completed_sync_revision, last_applied_revision, synchronization_complete,
            last_activity_minute_utc, current_post_count,
            current_following_count, current_follower_count)
        VALUES (
            @account_key, @next_version, @lifecycle, @repository_generation,
            @completed_sync_revision, @last_applied_revision, @synchronization_complete,
            @last_activity_minute_utc, @current_post_count,
            @current_following_count, @current_follower_count)
        ON CONFLICT (account_key) DO NOTHING;
        """;

    internal const string UpdateAccountStateSql = """
        UPDATE skypulse.account_state
        SET state_version = @next_version,
            lifecycle = @lifecycle,
            repository_generation = @repository_generation,
            completed_sync_revision = @completed_sync_revision,
            last_applied_revision = @last_applied_revision,
            synchronization_complete = @synchronization_complete,
            last_activity_minute_utc = @last_activity_minute_utc,
            current_post_count = @current_post_count,
            current_following_count = @current_following_count,
            current_follower_count = @current_follower_count,
            updated_at_utc = clock_timestamp()
        WHERE account_key = @account_key
          AND state_version = @expected_version
          AND @repository_generation >= repository_generation
          AND (
              @repository_generation > repository_generation
              OR last_applied_revision IS NULL
              OR (@last_applied_revision IS NOT NULL
                  AND convert_to(@last_applied_revision, 'UTF8') >= convert_to(last_applied_revision, 'UTF8')));
        """;

    internal const string UpsertRecordStateSql = """
        INSERT INTO skypulse.record_state AS current (
            account_key, repository_generation, collection, record_key,
            latest_revision, is_deleted, cid, target_account_key, is_direct_reply)
        VALUES (
            @account_key, @repository_generation, @collection, @record_key,
            @latest_revision, @is_deleted, @cid, @target_account_key, @is_direct_reply)
        ON CONFLICT (account_key, repository_generation, collection, record_key)
        DO UPDATE SET
            latest_revision = EXCLUDED.latest_revision,
            is_deleted = EXCLUDED.is_deleted,
            cid = EXCLUDED.cid,
            target_account_key = EXCLUDED.target_account_key,
            is_direct_reply = EXCLUDED.is_direct_reply,
            updated_at_utc = clock_timestamp()
        WHERE convert_to(current.latest_revision, 'UTF8') < convert_to(EXCLUDED.latest_revision, 'UTF8');
        """;

    internal const string UpsertFollowPairSql = """
        INSERT INTO skypulse.follow_pair (
            source_account_key, target_account_key, multiplicity)
        VALUES (@source_account_key, @target_account_key, @multiplicity)
        ON CONFLICT (source_account_key, target_account_key)
        DO UPDATE SET
            multiplicity = EXCLUDED.multiplicity,
            updated_at_utc = clock_timestamp();
        """;

    internal const string DeleteFollowPairSql = """
        DELETE FROM skypulse.follow_pair
        WHERE source_account_key = @source_account_key
          AND target_account_key = @target_account_key;
        """;

    internal const string AddActivitySql = """
        INSERT INTO skypulse.activity_minute_bucket (
            account_key, repository_generation, minute_utc, record_creates, record_updates,
            record_deletes, post_creates, received_engagement_creates)
        VALUES (
            @account_key, @repository_generation, @minute_utc, @record_creates, @record_updates,
            @record_deletes, @post_creates, @received_engagement_creates)
        ON CONFLICT (account_key, repository_generation, minute_utc)
        DO UPDATE SET
            record_creates = skypulse.activity_minute_bucket.record_creates + EXCLUDED.record_creates,
            record_updates = skypulse.activity_minute_bucket.record_updates + EXCLUDED.record_updates,
            record_deletes = skypulse.activity_minute_bucket.record_deletes + EXCLUDED.record_deletes,
            post_creates = skypulse.activity_minute_bucket.post_creates + EXCLUDED.post_creates,
            received_engagement_creates = skypulse.activity_minute_bucket.received_engagement_creates + EXCLUDED.received_engagement_creates;
        """;

    internal const string UpsertDesiredProjectionSql = """
        INSERT INTO skypulse.desired_projection (
            account_key, projection_version, operation, is_complete,
            projection_cut_minute_utc, next_recalculation_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days)
        VALUES (
            @account_key, @projection_version, @operation, @is_complete,
            @projection_cut_minute_utc, @next_recalculation_minute_utc,
            @last_activity_minute_utc,
            @created_record_count_1_day, @created_record_count_7_days, @created_record_count_30_days,
            @updated_record_count_1_day, @updated_record_count_7_days, @updated_record_count_30_days,
            @deleted_record_count_1_day, @deleted_record_count_7_days, @deleted_record_count_30_days,
            @current_post_count, @current_following_count, @current_follower_count,
            @post_creates_1_day, @post_creates_7_days, @post_creates_30_days,
            @received_engagement_creates_30_days)
        ON CONFLICT (account_key)
        DO UPDATE SET
            projection_version = EXCLUDED.projection_version,
            operation = EXCLUDED.operation,
            is_complete = EXCLUDED.is_complete,
            projection_cut_minute_utc = EXCLUDED.projection_cut_minute_utc,
            next_recalculation_minute_utc = EXCLUDED.next_recalculation_minute_utc,
            last_activity_minute_utc = EXCLUDED.last_activity_minute_utc,
            created_record_count_1_day = EXCLUDED.created_record_count_1_day,
            created_record_count_7_days = EXCLUDED.created_record_count_7_days,
            created_record_count_30_days = EXCLUDED.created_record_count_30_days,
            updated_record_count_1_day = EXCLUDED.updated_record_count_1_day,
            updated_record_count_7_days = EXCLUDED.updated_record_count_7_days,
            updated_record_count_30_days = EXCLUDED.updated_record_count_30_days,
            deleted_record_count_1_day = EXCLUDED.deleted_record_count_1_day,
            deleted_record_count_7_days = EXCLUDED.deleted_record_count_7_days,
            deleted_record_count_30_days = EXCLUDED.deleted_record_count_30_days,
            current_post_count = EXCLUDED.current_post_count,
            current_following_count = EXCLUDED.current_following_count,
            current_follower_count = EXCLUDED.current_follower_count,
            post_creates_1_day = EXCLUDED.post_creates_1_day,
            post_creates_7_days = EXCLUDED.post_creates_7_days,
            post_creates_30_days = EXCLUDED.post_creates_30_days,
            received_engagement_creates_30_days = EXCLUDED.received_engagement_creates_30_days,
            updated_at_utc = clock_timestamp()
        WHERE skypulse.desired_projection.projection_version < EXCLUDED.projection_version;
        """;

    internal const string InsertOutboxSql = """
        INSERT INTO skypulse.projection_outbox (
            account_key, projection_version, operation, is_complete,
            projection_cut_minute_utc, next_recalculation_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days)
        VALUES (
            @account_key, @projection_version, @operation, @is_complete,
            @projection_cut_minute_utc, @next_recalculation_minute_utc,
            @last_activity_minute_utc,
            @created_record_count_1_day, @created_record_count_7_days, @created_record_count_30_days,
            @updated_record_count_1_day, @updated_record_count_7_days, @updated_record_count_30_days,
            @deleted_record_count_1_day, @deleted_record_count_7_days, @deleted_record_count_30_days,
            @current_post_count, @current_following_count, @current_follower_count,
            @post_creates_1_day, @post_creates_7_days, @post_creates_30_days,
            @received_engagement_creates_30_days);
        """;

    internal const string UpsertRecalculationDueSql = """
        INSERT INTO skypulse.projection_recalculation_due (
            account_key, source_projection_version, due_minute_utc, available_at_utc)
        VALUES (
            @account_key, @projection_version, @due_minute_utc,
            TIMESTAMPTZ 'epoch' + (@due_minute_utc * INTERVAL '1 minute'))
        ON CONFLICT (account_key)
        DO UPDATE SET
            source_projection_version = EXCLUDED.source_projection_version,
            due_minute_utc = EXCLUDED.due_minute_utc,
            available_at_utc = EXCLUDED.available_at_utc,
            attempt_count = 0,
            lease_id = NULL,
            leased_until_utc = NULL,
            last_error_code = NULL,
            last_error_message = NULL
        WHERE skypulse.projection_recalculation_due.source_projection_version < EXCLUDED.source_projection_version;
        """;

    internal const string DeleteRecalculationDueSql = """
        DELETE FROM skypulse.projection_recalculation_due
        WHERE account_key = @account_key
          AND source_projection_version <= @projection_version;
        """;

    internal const string InsertQuarantineSql = """
        INSERT INTO skypulse.quarantine (
            source_instance_id, delivery_id, delivery_digest, semantic_digest, account_key,
            observed_at_minute_utc, quarantine_code, quarantine_message)
        VALUES (
            @source_instance_id, @delivery_id, @delivery_digest, @semantic_digest, @account_key,
            @observed_at_minute_utc, @quarantine_code, @quarantine_message);
        """;

    internal const string LeaseOutboxSql = """
        WITH candidates AS (
            SELECT candidate.account_key, candidate.projection_version
            FROM skypulse.projection_outbox AS candidate
            WHERE candidate.completed_at_utc IS NULL
              AND candidate.available_at_utc <= clock_timestamp()
              AND (candidate.lease_id IS NULL OR candidate.leased_until_utc <= clock_timestamp())
              AND NOT EXISTS (
                  SELECT 1
                  FROM skypulse.projection_outbox AS earlier
                  WHERE earlier.account_key = candidate.account_key
                    AND earlier.projection_version < candidate.projection_version
                    AND earlier.completed_at_utc IS NULL)
            ORDER BY candidate.available_at_utc, candidate.account_key, candidate.projection_version
            LIMIT @batch_size
            FOR UPDATE OF candidate SKIP LOCKED
        )
        UPDATE skypulse.projection_outbox AS leased
        SET lease_id = @lease_id,
            leased_until_utc = clock_timestamp() + @lease_duration
        FROM candidates
        WHERE leased.account_key = candidates.account_key
          AND leased.projection_version = candidates.projection_version
        RETURNING
            leased.account_key, leased.projection_version, leased.operation, leased.is_complete,
            leased.projection_cut_minute_utc, leased.next_recalculation_minute_utc,
            leased.last_activity_minute_utc,
            leased.created_record_count_1_day, leased.created_record_count_7_days, leased.created_record_count_30_days,
            leased.updated_record_count_1_day, leased.updated_record_count_7_days, leased.updated_record_count_30_days,
            leased.deleted_record_count_1_day, leased.deleted_record_count_7_days, leased.deleted_record_count_30_days,
            leased.current_post_count, leased.current_following_count, leased.current_follower_count,
            leased.post_creates_1_day, leased.post_creates_7_days, leased.post_creates_30_days,
            leased.received_engagement_creates_30_days, leased.attempt_count;
        """;

    internal const string PrepareHydrationSql = """
        WITH leased AS (
            SELECT *
            FROM skypulse.projection_outbox
            WHERE account_key = @account_key
              AND projection_version = @projection_version
              AND operation = 1
              AND lease_id = @lease_id
              AND leased_until_utc > clock_timestamp()
              AND completed_at_utc IS NULL
            FOR UPDATE
        )
        INSERT INTO skypulse.published_projection (
            account_key, projection_version, operation, is_complete, projection_cut_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days, published_at_utc)
        SELECT
            account_key, projection_version, operation, is_complete, projection_cut_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days, clock_timestamp()
        FROM leased
        ON CONFLICT (account_key)
        DO UPDATE SET
            projection_version = EXCLUDED.projection_version,
            operation = EXCLUDED.operation,
            is_complete = EXCLUDED.is_complete,
            projection_cut_minute_utc = EXCLUDED.projection_cut_minute_utc,
            last_activity_minute_utc = EXCLUDED.last_activity_minute_utc,
            created_record_count_1_day = EXCLUDED.created_record_count_1_day,
            created_record_count_7_days = EXCLUDED.created_record_count_7_days,
            created_record_count_30_days = EXCLUDED.created_record_count_30_days,
            updated_record_count_1_day = EXCLUDED.updated_record_count_1_day,
            updated_record_count_7_days = EXCLUDED.updated_record_count_7_days,
            updated_record_count_30_days = EXCLUDED.updated_record_count_30_days,
            deleted_record_count_1_day = EXCLUDED.deleted_record_count_1_day,
            deleted_record_count_7_days = EXCLUDED.deleted_record_count_7_days,
            deleted_record_count_30_days = EXCLUDED.deleted_record_count_30_days,
            current_post_count = EXCLUDED.current_post_count,
            current_following_count = EXCLUDED.current_following_count,
            current_follower_count = EXCLUDED.current_follower_count,
            post_creates_1_day = EXCLUDED.post_creates_1_day,
            post_creates_7_days = EXCLUDED.post_creates_7_days,
            post_creates_30_days = EXCLUDED.post_creates_30_days,
            received_engagement_creates_30_days = EXCLUDED.received_engagement_creates_30_days,
            published_at_utc = EXCLUDED.published_at_utc
        WHERE skypulse.published_projection.projection_version <= EXCLUDED.projection_version
        RETURNING 1;
        """;

    internal const string MaterializeRemovalSql = """
        WITH leased AS (
            SELECT *
            FROM skypulse.projection_outbox
            WHERE account_key = @account_key
              AND projection_version = @projection_version
              AND operation = 2
              AND lease_id = @lease_id
              AND leased_until_utc > clock_timestamp()
              AND completed_at_utc IS NULL
            FOR UPDATE
        )
        INSERT INTO skypulse.published_projection (
            account_key, projection_version, operation, is_complete, projection_cut_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days, published_at_utc)
        SELECT
            account_key, projection_version, operation, is_complete, projection_cut_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days, clock_timestamp()
        FROM leased
        ON CONFLICT (account_key)
        DO UPDATE SET
            projection_version = EXCLUDED.projection_version,
            operation = EXCLUDED.operation,
            is_complete = EXCLUDED.is_complete,
            projection_cut_minute_utc = EXCLUDED.projection_cut_minute_utc,
            last_activity_minute_utc = EXCLUDED.last_activity_minute_utc,
            created_record_count_1_day = EXCLUDED.created_record_count_1_day,
            created_record_count_7_days = EXCLUDED.created_record_count_7_days,
            created_record_count_30_days = EXCLUDED.created_record_count_30_days,
            updated_record_count_1_day = EXCLUDED.updated_record_count_1_day,
            updated_record_count_7_days = EXCLUDED.updated_record_count_7_days,
            updated_record_count_30_days = EXCLUDED.updated_record_count_30_days,
            deleted_record_count_1_day = EXCLUDED.deleted_record_count_1_day,
            deleted_record_count_7_days = EXCLUDED.deleted_record_count_7_days,
            deleted_record_count_30_days = EXCLUDED.deleted_record_count_30_days,
            current_post_count = EXCLUDED.current_post_count,
            current_following_count = EXCLUDED.current_following_count,
            current_follower_count = EXCLUDED.current_follower_count,
            post_creates_1_day = EXCLUDED.post_creates_1_day,
            post_creates_7_days = EXCLUDED.post_creates_7_days,
            post_creates_30_days = EXCLUDED.post_creates_30_days,
            received_engagement_creates_30_days = EXCLUDED.received_engagement_creates_30_days,
            published_at_utc = EXCLUDED.published_at_utc
        WHERE skypulse.published_projection.projection_version <= EXCLUDED.projection_version
        RETURNING 1;
        """;

    internal const string CompleteOutboxSql = """
        WITH completed AS (
            UPDATE skypulse.projection_outbox AS leased
            SET completed_at_utc = clock_timestamp(),
                lease_id = NULL,
                leased_until_utc = NULL,
                last_error_code = NULL,
                last_error_message = NULL
            WHERE leased.account_key = @account_key
              AND leased.projection_version = @projection_version
              AND leased.lease_id = @lease_id
              AND leased.leased_until_utc > clock_timestamp()
              AND leased.completed_at_utc IS NULL
              AND (
                  (
                      leased.operation = 1
                      AND EXISTS (
                          SELECT 1
                          FROM skypulse.published_projection AS hydration
                          WHERE hydration.account_key = leased.account_key
                            AND hydration.projection_version = leased.projection_version
                            AND hydration.operation = 1
                            AND hydration.is_complete)
                  )
                  OR (
                      leased.operation = 2
                      AND EXISTS (
                          SELECT 1
                          FROM skypulse.published_projection AS hydration
                          WHERE hydration.account_key = leased.account_key
                            AND hydration.projection_version = leased.projection_version
                            AND hydration.operation = 2
                            AND hydration.is_complete)
                  )
              )
            RETURNING 1
        )
        SELECT EXISTS (SELECT 1 FROM completed);
        """;

    internal const string ReadDesiredProjectionFirstPageSql = """
        SELECT
            account_key, projection_version, operation, is_complete,
            projection_cut_minute_utc, next_recalculation_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days
        FROM skypulse.desired_projection
        WHERE is_complete
        ORDER BY account_key
        LIMIT @batch_size;
        """;

    internal const string ReadDesiredProjectionNextPageSql = """
        SELECT
            account_key, projection_version, operation, is_complete,
            projection_cut_minute_utc, next_recalculation_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days
        FROM skypulse.desired_projection
        WHERE is_complete
          AND account_key > @after_account_key
        ORDER BY account_key
        LIMIT @batch_size;
        """;

    internal const string MaterializeDesiredProjectionSql = """
        INSERT INTO skypulse.published_projection (
            account_key, projection_version, operation, is_complete, projection_cut_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days, published_at_utc)
        SELECT
            account_key, projection_version, operation, is_complete, projection_cut_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days, clock_timestamp()
        FROM skypulse.desired_projection
        WHERE account_key = @account_key
          AND projection_version = @projection_version
          AND operation = @operation
          AND is_complete
        ON CONFLICT (account_key)
        DO UPDATE SET
            projection_version = EXCLUDED.projection_version,
            operation = EXCLUDED.operation,
            is_complete = EXCLUDED.is_complete,
            projection_cut_minute_utc = EXCLUDED.projection_cut_minute_utc,
            last_activity_minute_utc = EXCLUDED.last_activity_minute_utc,
            created_record_count_1_day = EXCLUDED.created_record_count_1_day,
            created_record_count_7_days = EXCLUDED.created_record_count_7_days,
            created_record_count_30_days = EXCLUDED.created_record_count_30_days,
            updated_record_count_1_day = EXCLUDED.updated_record_count_1_day,
            updated_record_count_7_days = EXCLUDED.updated_record_count_7_days,
            updated_record_count_30_days = EXCLUDED.updated_record_count_30_days,
            deleted_record_count_1_day = EXCLUDED.deleted_record_count_1_day,
            deleted_record_count_7_days = EXCLUDED.deleted_record_count_7_days,
            deleted_record_count_30_days = EXCLUDED.deleted_record_count_30_days,
            current_post_count = EXCLUDED.current_post_count,
            current_following_count = EXCLUDED.current_following_count,
            current_follower_count = EXCLUDED.current_follower_count,
            post_creates_1_day = EXCLUDED.post_creates_1_day,
            post_creates_7_days = EXCLUDED.post_creates_7_days,
            post_creates_30_days = EXCLUDED.post_creates_30_days,
            received_engagement_creates_30_days = EXCLUDED.received_engagement_creates_30_days,
            published_at_utc = EXCLUDED.published_at_utc
        WHERE skypulse.published_projection.projection_version <= EXCLUDED.projection_version
        RETURNING 1;
        """;

    internal const string FinalizeRebuildProjectionSql = """
        WITH exact_desired AS (
            SELECT 1
            FROM skypulse.desired_projection
            WHERE account_key = @account_key
              AND projection_version = @projection_version
              AND operation = @operation
              AND is_complete
        ),
        exact_published AS (
            SELECT 1
            FROM skypulse.published_projection
            WHERE account_key = @account_key
              AND projection_version = @projection_version
              AND operation = @operation
              AND is_complete
        ),
        completed AS (
            UPDATE skypulse.projection_outbox
            SET completed_at_utc = COALESCE(completed_at_utc, clock_timestamp()),
                lease_id = NULL,
                leased_until_utc = NULL,
                last_error_code = NULL,
                last_error_message = NULL
            WHERE account_key = @account_key
              AND projection_version <= @projection_version
              AND EXISTS (SELECT 1 FROM exact_desired)
              AND EXISTS (SELECT 1 FROM exact_published)
            RETURNING 1
        )
        SELECT EXISTS (SELECT 1 FROM exact_desired)
           AND EXISTS (SELECT 1 FROM exact_published);
        """;

    internal const string ReadPublishedUpsertsSql = """
        SELECT
            account_key, projection_version, operation, is_complete,
            projection_cut_minute_utc, NULL::bigint AS next_recalculation_minute_utc,
            last_activity_minute_utc,
            created_record_count_1_day, created_record_count_7_days, created_record_count_30_days,
            updated_record_count_1_day, updated_record_count_7_days, updated_record_count_30_days,
            deleted_record_count_1_day, deleted_record_count_7_days, deleted_record_count_30_days,
            current_post_count, current_following_count, current_follower_count,
            post_creates_1_day, post_creates_7_days, post_creates_30_days,
            received_engagement_creates_30_days
        FROM skypulse.published_projection
        WHERE account_key = ANY(@account_keys)
          AND operation = 1
          AND is_complete;
        """;

    internal const string FailOutboxSql = """
        UPDATE skypulse.projection_outbox
        SET attempt_count = attempt_count + 1,
            available_at_utc = @available_at_utc,
            lease_id = NULL,
            leased_until_utc = NULL,
            last_error_code = @error_code,
            last_error_message = @error_message
        WHERE account_key = @account_key
          AND projection_version = @projection_version
          AND lease_id = @lease_id
          AND leased_until_utc > clock_timestamp()
          AND completed_at_utc IS NULL;
        """;

    internal const string LeaseRecalculationsSql = """
        WITH candidates AS (
            SELECT account_key
            FROM skypulse.projection_recalculation_due
            WHERE available_at_utc <= clock_timestamp()
              AND (lease_id IS NULL OR leased_until_utc <= clock_timestamp())
            ORDER BY available_at_utc, account_key
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE skypulse.projection_recalculation_due AS leased
        SET lease_id = @lease_id,
            leased_until_utc = clock_timestamp() + @lease_duration
        FROM candidates
        WHERE leased.account_key = candidates.account_key
        RETURNING leased.account_key, leased.source_projection_version,
            leased.due_minute_utc,
            floor(extract(epoch from clock_timestamp()) / 60)::bigint AS evaluation_minute_utc,
            leased.attempt_count;
        """;

    internal const string CompleteRecalculationSql = """
        DELETE FROM skypulse.projection_recalculation_due
        WHERE account_key = @account_key
          AND source_projection_version = @source_projection_version
          AND lease_id = @lease_id
          AND leased_until_utc > clock_timestamp();
        """;

    internal const string FailRecalculationSql = """
        UPDATE skypulse.projection_recalculation_due
        SET attempt_count = attempt_count + 1,
            available_at_utc = @available_at_utc,
            lease_id = NULL,
            leased_until_utc = NULL,
            last_error_code = @error_code,
            last_error_message = @error_message
        WHERE account_key = @account_key
          AND source_projection_version = @source_projection_version
          AND lease_id = @lease_id
          AND leased_until_utc > clock_timestamp();
        """;

    internal static NpgsqlCommand CreateReserveDeliveryCommand(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        DurableDeliveryReservationRequest request)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = ReserveDeliverySql;
        AddDeliveryIdentity(command, request.SourceInstanceId, request.TapDeliveryId);
        command.Parameters.AddWithValue("delivery_digest", NpgsqlDbType.Bytea, PostgreSqlSchema.DecodeDigest(request.DeliveryDigest));
        command.Parameters.AddWithValue("first_observed_at_minute_utc", NpgsqlDbType.Bigint, request.FirstObservedAtMinuteUtc);
        return command;
    }

    internal static void AddProjectionParameters(NpgsqlCommand command, ProjectionSnapshot projection)
    {
        command.Parameters.AddWithValue("account_key", NpgsqlDbType.Bytea, PostgreSqlSchema.EncodeAccountKey(projection.AccountKey));
        command.Parameters.AddWithValue("projection_version", NpgsqlDbType.Bigint, projection.Version);
        command.Parameters.AddWithValue("operation", NpgsqlDbType.Smallint, (short)projection.Operation);
        command.Parameters.AddWithValue("is_complete", NpgsqlDbType.Boolean, projection.IsComplete);
        command.Parameters.AddWithValue("projection_cut_minute_utc", NpgsqlDbType.Bigint, projection.ProjectionCutMinuteUtc);
        AddNullable(command, "next_recalculation_minute_utc", NpgsqlDbType.Bigint, projection.NextRecalculationMinuteUtc);
        command.Parameters.AddWithValue("last_activity_minute_utc", NpgsqlDbType.Bigint, projection.LastActivityMinuteUtc);
        command.Parameters.AddWithValue("created_record_count_1_day", NpgsqlDbType.Bigint, projection.CreatedRecordCount1Day);
        command.Parameters.AddWithValue("created_record_count_7_days", NpgsqlDbType.Bigint, projection.CreatedRecordCount7Days);
        command.Parameters.AddWithValue("created_record_count_30_days", NpgsqlDbType.Bigint, projection.CreatedRecordCount30Days);
        command.Parameters.AddWithValue("updated_record_count_1_day", NpgsqlDbType.Bigint, projection.UpdatedRecordCount1Day);
        command.Parameters.AddWithValue("updated_record_count_7_days", NpgsqlDbType.Bigint, projection.UpdatedRecordCount7Days);
        command.Parameters.AddWithValue("updated_record_count_30_days", NpgsqlDbType.Bigint, projection.UpdatedRecordCount30Days);
        command.Parameters.AddWithValue("deleted_record_count_1_day", NpgsqlDbType.Bigint, projection.DeletedRecordCount1Day);
        command.Parameters.AddWithValue("deleted_record_count_7_days", NpgsqlDbType.Bigint, projection.DeletedRecordCount7Days);
        command.Parameters.AddWithValue("deleted_record_count_30_days", NpgsqlDbType.Bigint, projection.DeletedRecordCount30Days);
        command.Parameters.AddWithValue("current_post_count", NpgsqlDbType.Bigint, projection.CurrentPostCount);
        command.Parameters.AddWithValue("current_following_count", NpgsqlDbType.Bigint, projection.CurrentFollowingCount);
        command.Parameters.AddWithValue("current_follower_count", NpgsqlDbType.Bigint, projection.CurrentFollowerCount);
        command.Parameters.AddWithValue("post_creates_1_day", NpgsqlDbType.Bigint, projection.PostCreates1Day);
        command.Parameters.AddWithValue("post_creates_7_days", NpgsqlDbType.Bigint, projection.PostCreates7Days);
        command.Parameters.AddWithValue("post_creates_30_days", NpgsqlDbType.Bigint, projection.PostCreates30Days);
        command.Parameters.AddWithValue("received_engagement_creates_30_days", NpgsqlDbType.Bigint, projection.ReceivedEngagementCreates30Days);
    }

    internal static void AddDeliveryIdentity(NpgsqlCommand command, Guid sourceInstanceId, ulong deliveryId)
    {
        command.Parameters.AddWithValue("source_instance_id", NpgsqlDbType.Uuid, sourceInstanceId);
        command.Parameters.AddWithValue("delivery_id", NpgsqlDbType.Numeric, (decimal)deliveryId);
    }

    internal static void AddNullable(NpgsqlCommand command, string name, NpgsqlDbType type, object? value)
    {
        command.Parameters.AddWithValue(name, type, value ?? DBNull.Value);
    }
}
