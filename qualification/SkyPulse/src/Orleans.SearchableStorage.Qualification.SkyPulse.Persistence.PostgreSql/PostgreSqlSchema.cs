using System.Collections.ObjectModel;
using System.Data;
using System.Security.Cryptography;
using System.Text;
using Npgsql;
using Orleans.SearchableStorage.Qualification.SkyPulse;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

/// <summary>
/// Describes one immutable PostgreSQL schema migration.
/// </summary>
public sealed record PostgreSqlSchemaMigration(int Version, string Sql, string Sha256);

/// <summary>
/// Owns the versioned, metadata-only PostgreSQL schema contract.
/// </summary>
public static class PostgreSqlSchema
{
    public const string SchemaName = "skypulse";

    public const int CurrentVersion = 2;

    private const string Migration1 = """
        CREATE TABLE skypulse.runtime_manifest (
            manifest_id smallint PRIMARY KEY DEFAULT 1 CHECK (manifest_id = 1),
            profile_id text NOT NULL CHECK (octet_length(profile_id) BETWEEN 1 AND 256),
            profile_version integer NOT NULL CHECK (profile_version > 0),
            corpus_cap bigint NOT NULL CHECK (corpus_cap > 0),
            allowlist_sha256 bytea NOT NULL CHECK (octet_length(allowlist_sha256) = 32),
            source_instance_id uuid NOT NULL UNIQUE
                CHECK (source_instance_id <> '00000000-0000-0000-0000-000000000000'),
            index_namespace text NOT NULL CHECK (octet_length(index_namespace) BETWEEN 1 AND 256),
            index_provider_name text NOT NULL CHECK (octet_length(index_provider_name) BETWEEN 1 AND 256),
            index_schema_id text NOT NULL CHECK (octet_length(index_schema_id) BETWEEN 1 AND 256),
            index_schema_version integer NOT NULL CHECK (index_schema_version > 0),
            index_schema_fingerprint bytea NOT NULL CHECK (octet_length(index_schema_fingerprint) = 32),
            package_id text NOT NULL CHECK (octet_length(package_id) BETWEEN 1 AND 256),
            package_version text NOT NULL CHECK (octet_length(package_version) BETWEEN 1 AND 256),
            package_nupkg_sha256 bytea NOT NULL CHECK (octet_length(package_nupkg_sha256) = 32),
            package_canonical_manifest_sha256 bytea NOT NULL
                CHECK (octet_length(package_canonical_manifest_sha256) = 32),
            package_repository_url text NOT NULL
                CHECK (package_repository_url ~ '^https://github[.]com/[^/]+/[^/]+$'),
            package_repository_commit text NOT NULL
                CHECK (package_repository_commit ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'),
            package_build_sdk_version text NOT NULL
                CHECK (package_build_sdk_version ~ '^(0|[1-9][0-9]*)[.](0|[1-9][0-9]*)[.](0|[1-9][0-9]*)$'),
            manifest_fingerprint bytea NOT NULL CHECK (octet_length(manifest_fingerprint) = 32),
            bound_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );

        CREATE TABLE skypulse.account_state (
            account_key bytea PRIMARY KEY CHECK (octet_length(account_key) = 32),
            state_version bigint NOT NULL CHECK (state_version > 0),
            lifecycle smallint NOT NULL CHECK (lifecycle BETWEEN 1 AND 5),
            repository_generation bigint NOT NULL CHECK (repository_generation >= 0),
            completed_sync_revision text NULL CHECK (completed_sync_revision IS NULL OR octet_length(completed_sync_revision) BETWEEN 1 AND 1024),
            last_applied_revision text NULL CHECK (last_applied_revision IS NULL OR octet_length(last_applied_revision) BETWEEN 1 AND 1024),
            synchronization_complete boolean NOT NULL,
            last_activity_minute_utc bigint NOT NULL CHECK (last_activity_minute_utc >= 0),
            current_post_count bigint NOT NULL CHECK (current_post_count >= 0),
            current_following_count bigint NOT NULL CHECK (current_following_count >= 0),
            current_follower_count bigint NOT NULL CHECK (current_follower_count >= 0),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            CHECK (NOT synchronization_complete OR (completed_sync_revision IS NOT NULL AND last_applied_revision IS NOT NULL)),
            CHECK (completed_sync_revision IS NULL OR last_applied_revision IS NULL
                OR convert_to(last_applied_revision, 'UTF8') >= convert_to(completed_sync_revision, 'UTF8'))
        );

        CREATE TABLE skypulse.tap_delivery (
            source_instance_id uuid NOT NULL,
            delivery_id numeric(20, 0) NOT NULL CHECK (delivery_id > 0),
            delivery_digest bytea NOT NULL CHECK (octet_length(delivery_digest) = 32),
            semantic_digest bytea NULL CHECK (semantic_digest IS NULL OR octet_length(semantic_digest) = 32),
            account_key bytea NULL CHECK (account_key IS NULL OR octet_length(account_key) = 32),
            observed_at_minute_utc bigint NOT NULL CHECK (observed_at_minute_utc >= 0),
            outcome smallint NOT NULL CHECK (outcome BETWEEN 0 AND 3),
            committed_at_utc timestamp with time zone NULL,
            PRIMARY KEY (source_instance_id, delivery_id),
            FOREIGN KEY (source_instance_id)
                REFERENCES skypulse.runtime_manifest (source_instance_id)
                ON DELETE RESTRICT,
            CHECK (
                (outcome = 0 AND committed_at_utc IS NULL
                    AND semantic_digest IS NULL AND account_key IS NULL)
                OR (outcome IN (1, 2) AND committed_at_utc IS NOT NULL
                    AND semantic_digest IS NOT NULL AND account_key IS NOT NULL)
                OR (outcome = 3 AND committed_at_utc IS NOT NULL))
        );

        CREATE INDEX tap_delivery_retention_idx
            ON skypulse.tap_delivery (source_instance_id, committed_at_utc, delivery_id)
            WHERE outcome <> 0;

        CREATE TABLE skypulse.source_delivery_retention_watermark (
            source_instance_id uuid PRIMARY KEY,
            safe_delivery_id_inclusive numeric(20, 0) NOT NULL CHECK (safe_delivery_id_inclusive >= 0),
            evidence_reference text NOT NULL CHECK (octet_length(evidence_reference) BETWEEN 1 AND 1024),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            FOREIGN KEY (source_instance_id)
                REFERENCES skypulse.runtime_manifest (source_instance_id)
                ON DELETE RESTRICT
        );

        CREATE TABLE skypulse.semantic_event_retention_watermark (
            watermark_id smallint PRIMARY KEY DEFAULT 1 CHECK (watermark_id = 1),
            safe_observed_minute_utc bigint NOT NULL CHECK (safe_observed_minute_utc >= 0),
            evidence_reference text NOT NULL CHECK (octet_length(evidence_reference) BETWEEN 1 AND 1024),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );

        CREATE TABLE skypulse.activity_retention_watermark (
            watermark_id smallint PRIMARY KEY DEFAULT 1 CHECK (watermark_id = 1),
            safe_minute_utc bigint NOT NULL CHECK (safe_minute_utc >= 0),
            evidence_reference text NOT NULL CHECK (octet_length(evidence_reference) BETWEEN 1 AND 1024),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );

        CREATE TABLE skypulse.semantic_event (
            semantic_digest bytea NOT NULL CHECK (octet_length(semantic_digest) = 32),
            account_key bytea NOT NULL CHECK (octet_length(account_key) = 32),
            repository_generation bigint NOT NULL CHECK (repository_generation >= 0),
            event_kind smallint NOT NULL CHECK (event_kind BETWEEN 1 AND 3),
            observed_at_minute_utc bigint NOT NULL CHECK (observed_at_minute_utc >= 0),
            repository_revision text NULL CHECK (repository_revision IS NULL OR octet_length(repository_revision) BETWEEN 1 AND 1024),
            lifecycle smallint NULL CHECK (lifecycle IS NULL OR lifecycle BETWEEN 1 AND 5),
            collection smallint NULL CHECK (collection IS NULL OR collection BETWEEN 1 AND 5),
            action smallint NULL CHECK (action IS NULL OR action BETWEEN 1 AND 3),
            record_key text NULL CHECK (record_key IS NULL OR octet_length(record_key) BETWEEN 1 AND 2048),
            cid text NULL CHECK (cid IS NULL OR octet_length(cid) BETWEEN 1 AND 1024),
            target_account_key bytea NULL CHECK (target_account_key IS NULL OR octet_length(target_account_key) = 32),
            is_direct_reply boolean NOT NULL,
            is_live boolean NOT NULL,
            applied_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (account_key, repository_generation, semantic_digest),
            CHECK (
                (event_kind = 1 AND repository_revision IS NOT NULL AND collection IS NOT NULL
                    AND action IS NOT NULL AND record_key IS NOT NULL AND lifecycle IS NULL)
                OR (event_kind = 2 AND repository_revision IS NULL AND collection IS NULL
                    AND action IS NULL AND record_key IS NULL AND cid IS NULL
                    AND target_account_key IS NULL AND NOT is_direct_reply AND lifecycle IS NOT NULL)
                OR (event_kind = 3 AND repository_revision IS NOT NULL AND collection IS NULL
                    AND action IS NULL AND record_key IS NULL AND cid IS NULL
                    AND target_account_key IS NULL AND NOT is_direct_reply AND lifecycle IS NULL)
            ),
            CHECK (
                event_kind <> 1 OR (
                    (action = 3 AND cid IS NULL AND target_account_key IS NULL AND NOT is_direct_reply)
                    OR (action IN (1, 2) AND cid IS NOT NULL AND (
                        (collection = 1 AND ((is_direct_reply AND target_account_key IS NOT NULL)
                            OR (NOT is_direct_reply AND target_account_key IS NULL)))
                        OR (collection IN (2, 3, 4) AND target_account_key IS NOT NULL AND NOT is_direct_reply)
                        OR (collection = 5 AND target_account_key IS NULL AND NOT is_direct_reply)
                    ))
                )
            )
        );

        CREATE INDEX semantic_event_account_idx
            ON skypulse.semantic_event (account_key, repository_generation, applied_at_utc);

        CREATE INDEX semantic_event_retention_idx
            ON skypulse.semantic_event (observed_at_minute_utc, applied_at_utc, account_key, repository_generation, semantic_digest);

        CREATE TABLE skypulse.record_state (
            account_key bytea NOT NULL CHECK (octet_length(account_key) = 32),
            repository_generation bigint NOT NULL CHECK (repository_generation >= 0),
            collection smallint NOT NULL CHECK (collection BETWEEN 1 AND 5),
            record_key text NOT NULL CHECK (octet_length(record_key) BETWEEN 1 AND 2048),
            latest_revision text NOT NULL CHECK (octet_length(latest_revision) BETWEEN 1 AND 1024),
            is_deleted boolean NOT NULL,
            cid text NULL CHECK (cid IS NULL OR octet_length(cid) BETWEEN 1 AND 1024),
            target_account_key bytea NULL CHECK (target_account_key IS NULL OR octet_length(target_account_key) = 32),
            is_direct_reply boolean NOT NULL,
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (account_key, repository_generation, collection, record_key),
            CHECK (
                (is_deleted AND cid IS NULL AND target_account_key IS NULL AND NOT is_direct_reply)
                OR (NOT is_deleted AND cid IS NOT NULL AND (
                    (collection = 1 AND ((is_direct_reply AND target_account_key IS NOT NULL)
                        OR (NOT is_direct_reply AND target_account_key IS NULL)))
                    OR (collection IN (2, 3, 4) AND target_account_key IS NOT NULL AND NOT is_direct_reply)
                    OR (collection = 5 AND target_account_key IS NULL AND NOT is_direct_reply)
                ))
            )
        );

        CREATE TABLE skypulse.follow_pair (
            source_account_key bytea NOT NULL CHECK (octet_length(source_account_key) = 32),
            target_account_key bytea NOT NULL CHECK (octet_length(target_account_key) = 32),
            multiplicity integer NOT NULL CHECK (multiplicity > 0),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (source_account_key, target_account_key)
        );

        CREATE TABLE skypulse.reconciliation_dependency (
            owner_account_key bytea NOT NULL CHECK (octet_length(owner_account_key) = 32),
            owner_repository_generation bigint NOT NULL CHECK (owner_repository_generation >= 0),
            affected_account_key bytea NOT NULL CHECK (octet_length(affected_account_key) = 32),
            PRIMARY KEY (owner_account_key, owner_repository_generation, affected_account_key)
        );

        CREATE TABLE skypulse.lifecycle_transition_work (
            source_instance_id uuid NOT NULL,
            delivery_id numeric(20, 0) NOT NULL CHECK (delivery_id > 0),
            delivery_digest bytea NOT NULL CHECK (octet_length(delivery_digest) = 32),
            semantic_digest bytea NOT NULL CHECK (octet_length(semantic_digest) = 32),
            account_key bytea NOT NULL CHECK (octet_length(account_key) = 32),
            repository_generation bigint NOT NULL CHECK (repository_generation >= 0),
            event_kind smallint NOT NULL CHECK (event_kind IN (2, 3)),
            observed_at_minute_utc bigint NOT NULL CHECK (observed_at_minute_utc >= 0),
            repository_revision text NULL
                CHECK (repository_revision IS NULL OR octet_length(repository_revision) BETWEEN 1 AND 1024),
            lifecycle smallint NULL CHECK (lifecycle IS NULL OR lifecycle BETWEEN 2 AND 5),
            is_live boolean NOT NULL,
            phase smallint NOT NULL CHECK (phase BETWEEN 1 AND 4),
            started_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (source_instance_id, delivery_id),
            UNIQUE (account_key),
            FOREIGN KEY (source_instance_id, delivery_id)
                REFERENCES skypulse.tap_delivery (source_instance_id, delivery_id)
                ON DELETE CASCADE,
            CHECK (
                (event_kind = 2 AND repository_revision IS NULL AND lifecycle IS NOT NULL)
                OR (event_kind = 3 AND repository_revision IS NOT NULL AND lifecycle IS NULL)
            )
        );

        CREATE INDEX lifecycle_transition_work_account_idx
            ON skypulse.lifecycle_transition_work (account_key, repository_generation);

        CREATE TABLE skypulse.activity_minute_bucket (
            account_key bytea NOT NULL CHECK (octet_length(account_key) = 32),
            repository_generation bigint NOT NULL CHECK (repository_generation >= 0),
            minute_utc bigint NOT NULL CHECK (minute_utc >= 0),
            record_creates bigint NOT NULL CHECK (record_creates >= 0),
            record_updates bigint NOT NULL CHECK (record_updates >= 0),
            record_deletes bigint NOT NULL CHECK (record_deletes >= 0),
            post_creates bigint NOT NULL CHECK (post_creates >= 0),
            received_engagement_creates bigint NOT NULL CHECK (received_engagement_creates >= 0),
            PRIMARY KEY (account_key, repository_generation, minute_utc),
            CHECK (post_creates <= record_creates)
        );

        CREATE INDEX activity_minute_bucket_retention_idx
            ON skypulse.activity_minute_bucket (minute_utc, account_key, repository_generation);

        CREATE TABLE skypulse.desired_projection (
            account_key bytea PRIMARY KEY CHECK (octet_length(account_key) = 32),
            projection_version bigint NOT NULL CHECK (projection_version > 0),
            operation smallint NOT NULL CHECK (operation BETWEEN 1 AND 2),
            is_deleted boolean GENERATED ALWAYS AS (operation = 2) STORED,
            is_complete boolean NOT NULL,
            projection_cut_minute_utc bigint NOT NULL CHECK (projection_cut_minute_utc >= 0),
            next_recalculation_minute_utc bigint NULL CHECK (next_recalculation_minute_utc IS NULL OR next_recalculation_minute_utc > projection_cut_minute_utc),
            last_activity_minute_utc bigint NOT NULL CHECK (last_activity_minute_utc >= 0),
            created_record_count_1_day bigint NOT NULL CHECK (created_record_count_1_day >= 0),
            created_record_count_7_days bigint NOT NULL CHECK (created_record_count_7_days >= 0),
            created_record_count_30_days bigint NOT NULL CHECK (created_record_count_30_days >= 0),
            updated_record_count_1_day bigint NOT NULL CHECK (updated_record_count_1_day >= 0),
            updated_record_count_7_days bigint NOT NULL CHECK (updated_record_count_7_days >= 0),
            updated_record_count_30_days bigint NOT NULL CHECK (updated_record_count_30_days >= 0),
            deleted_record_count_1_day bigint NOT NULL CHECK (deleted_record_count_1_day >= 0),
            deleted_record_count_7_days bigint NOT NULL CHECK (deleted_record_count_7_days >= 0),
            deleted_record_count_30_days bigint NOT NULL CHECK (deleted_record_count_30_days >= 0),
            current_post_count bigint NOT NULL CHECK (current_post_count >= 0),
            current_following_count bigint NOT NULL CHECK (current_following_count >= 0),
            current_follower_count bigint NOT NULL CHECK (current_follower_count >= 0),
            post_creates_1_day bigint NOT NULL CHECK (post_creates_1_day >= 0),
            post_creates_7_days bigint NOT NULL CHECK (post_creates_7_days >= 0),
            post_creates_30_days bigint NOT NULL CHECK (post_creates_30_days >= 0),
            received_engagement_creates_30_days bigint NOT NULL CHECK (received_engagement_creates_30_days >= 0),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            CHECK (operation <> 2 OR is_complete),
            CHECK (created_record_count_1_day <= created_record_count_7_days AND created_record_count_7_days <= created_record_count_30_days),
            CHECK (updated_record_count_1_day <= updated_record_count_7_days AND updated_record_count_7_days <= updated_record_count_30_days),
            CHECK (deleted_record_count_1_day <= deleted_record_count_7_days AND deleted_record_count_7_days <= deleted_record_count_30_days),
            CHECK (post_creates_1_day <= post_creates_7_days AND post_creates_7_days <= post_creates_30_days),
            CHECK (post_creates_1_day <= created_record_count_1_day),
            CHECK (post_creates_7_days <= created_record_count_7_days),
            CHECK (post_creates_30_days <= created_record_count_30_days)
        );

        CREATE TABLE skypulse.published_projection (
            account_key bytea PRIMARY KEY CHECK (octet_length(account_key) = 32),
            projection_version bigint NOT NULL CHECK (projection_version > 0),
            operation smallint NOT NULL CHECK (operation BETWEEN 1 AND 2),
            is_deleted boolean GENERATED ALWAYS AS (operation = 2) STORED,
            is_complete boolean NOT NULL CHECK (is_complete),
            projection_cut_minute_utc bigint NOT NULL CHECK (projection_cut_minute_utc >= 0),
            last_activity_minute_utc bigint NOT NULL CHECK (last_activity_minute_utc >= 0),
            created_record_count_1_day bigint NOT NULL CHECK (created_record_count_1_day >= 0),
            created_record_count_7_days bigint NOT NULL CHECK (created_record_count_7_days >= 0),
            created_record_count_30_days bigint NOT NULL CHECK (created_record_count_30_days >= 0),
            updated_record_count_1_day bigint NOT NULL CHECK (updated_record_count_1_day >= 0),
            updated_record_count_7_days bigint NOT NULL CHECK (updated_record_count_7_days >= 0),
            updated_record_count_30_days bigint NOT NULL CHECK (updated_record_count_30_days >= 0),
            deleted_record_count_1_day bigint NOT NULL CHECK (deleted_record_count_1_day >= 0),
            deleted_record_count_7_days bigint NOT NULL CHECK (deleted_record_count_7_days >= 0),
            deleted_record_count_30_days bigint NOT NULL CHECK (deleted_record_count_30_days >= 0),
            current_post_count bigint NOT NULL CHECK (current_post_count >= 0),
            current_following_count bigint NOT NULL CHECK (current_following_count >= 0),
            current_follower_count bigint NOT NULL CHECK (current_follower_count >= 0),
            post_creates_1_day bigint NOT NULL CHECK (post_creates_1_day >= 0),
            post_creates_7_days bigint NOT NULL CHECK (post_creates_7_days >= 0),
            post_creates_30_days bigint NOT NULL CHECK (post_creates_30_days >= 0),
            received_engagement_creates_30_days bigint NOT NULL CHECK (received_engagement_creates_30_days >= 0),
            published_at_utc timestamp with time zone NOT NULL,
            CHECK (created_record_count_1_day <= created_record_count_7_days AND created_record_count_7_days <= created_record_count_30_days),
            CHECK (updated_record_count_1_day <= updated_record_count_7_days AND updated_record_count_7_days <= updated_record_count_30_days),
            CHECK (deleted_record_count_1_day <= deleted_record_count_7_days AND deleted_record_count_7_days <= deleted_record_count_30_days),
            CHECK (post_creates_1_day <= post_creates_7_days AND post_creates_7_days <= post_creates_30_days),
            CHECK (post_creates_1_day <= created_record_count_1_day),
            CHECK (post_creates_7_days <= created_record_count_7_days),
            CHECK (post_creates_30_days <= created_record_count_30_days)
        );

        CREATE TABLE skypulse.projection_outbox (
            account_key bytea NOT NULL CHECK (octet_length(account_key) = 32),
            projection_version bigint NOT NULL CHECK (projection_version > 0),
            operation smallint NOT NULL CHECK (operation BETWEEN 1 AND 2),
            is_deleted boolean GENERATED ALWAYS AS (operation = 2) STORED,
            is_complete boolean NOT NULL,
            projection_cut_minute_utc bigint NOT NULL CHECK (projection_cut_minute_utc >= 0),
            next_recalculation_minute_utc bigint NULL CHECK (next_recalculation_minute_utc IS NULL OR next_recalculation_minute_utc > projection_cut_minute_utc),
            last_activity_minute_utc bigint NOT NULL CHECK (last_activity_minute_utc >= 0),
            created_record_count_1_day bigint NOT NULL CHECK (created_record_count_1_day >= 0),
            created_record_count_7_days bigint NOT NULL CHECK (created_record_count_7_days >= 0),
            created_record_count_30_days bigint NOT NULL CHECK (created_record_count_30_days >= 0),
            updated_record_count_1_day bigint NOT NULL CHECK (updated_record_count_1_day >= 0),
            updated_record_count_7_days bigint NOT NULL CHECK (updated_record_count_7_days >= 0),
            updated_record_count_30_days bigint NOT NULL CHECK (updated_record_count_30_days >= 0),
            deleted_record_count_1_day bigint NOT NULL CHECK (deleted_record_count_1_day >= 0),
            deleted_record_count_7_days bigint NOT NULL CHECK (deleted_record_count_7_days >= 0),
            deleted_record_count_30_days bigint NOT NULL CHECK (deleted_record_count_30_days >= 0),
            current_post_count bigint NOT NULL CHECK (current_post_count >= 0),
            current_following_count bigint NOT NULL CHECK (current_following_count >= 0),
            current_follower_count bigint NOT NULL CHECK (current_follower_count >= 0),
            post_creates_1_day bigint NOT NULL CHECK (post_creates_1_day >= 0),
            post_creates_7_days bigint NOT NULL CHECK (post_creates_7_days >= 0),
            post_creates_30_days bigint NOT NULL CHECK (post_creates_30_days >= 0),
            received_engagement_creates_30_days bigint NOT NULL CHECK (received_engagement_creates_30_days >= 0),
            available_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            lease_id uuid NULL,
            leased_until_utc timestamp with time zone NULL,
            completed_at_utc timestamp with time zone NULL,
            last_error_code text NULL CHECK (last_error_code IS NULL OR octet_length(last_error_code) BETWEEN 1 AND 256),
            last_error_message text NULL CHECK (last_error_message IS NULL OR octet_length(last_error_message) BETWEEN 1 AND 2048),
            PRIMARY KEY (account_key, projection_version),
            CHECK ((lease_id IS NULL) = (leased_until_utc IS NULL)),
            CHECK (operation <> 2 OR is_complete),
            CHECK (created_record_count_1_day <= created_record_count_7_days AND created_record_count_7_days <= created_record_count_30_days),
            CHECK (updated_record_count_1_day <= updated_record_count_7_days AND updated_record_count_7_days <= updated_record_count_30_days),
            CHECK (deleted_record_count_1_day <= deleted_record_count_7_days AND deleted_record_count_7_days <= deleted_record_count_30_days),
            CHECK (post_creates_1_day <= post_creates_7_days AND post_creates_7_days <= post_creates_30_days),
            CHECK (post_creates_1_day <= created_record_count_1_day),
            CHECK (post_creates_7_days <= created_record_count_7_days),
            CHECK (post_creates_30_days <= created_record_count_30_days)
        );

        CREATE INDEX projection_outbox_due_idx
            ON skypulse.projection_outbox (available_at_utc, account_key, projection_version)
            WHERE completed_at_utc IS NULL;

        CREATE INDEX projection_outbox_retention_idx
            ON skypulse.projection_outbox (completed_at_utc, account_key, projection_version)
            WHERE completed_at_utc IS NOT NULL;

        CREATE TABLE skypulse.projection_recalculation_due (
            account_key bytea PRIMARY KEY CHECK (octet_length(account_key) = 32),
            source_projection_version bigint NOT NULL CHECK (source_projection_version > 0),
            due_minute_utc bigint NOT NULL CHECK (due_minute_utc >= 0),
            available_at_utc timestamp with time zone NOT NULL,
            attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            lease_id uuid NULL,
            leased_until_utc timestamp with time zone NULL,
            last_error_code text NULL CHECK (last_error_code IS NULL OR octet_length(last_error_code) BETWEEN 1 AND 256),
            last_error_message text NULL CHECK (last_error_message IS NULL OR octet_length(last_error_message) BETWEEN 1 AND 2048),
            CHECK ((lease_id IS NULL) = (leased_until_utc IS NULL))
        );

        CREATE TABLE skypulse.quarantine (
            source_instance_id uuid NOT NULL,
            delivery_id numeric(20, 0) NOT NULL CHECK (delivery_id > 0),
            delivery_digest bytea NOT NULL CHECK (octet_length(delivery_digest) = 32),
            semantic_digest bytea NULL CHECK (semantic_digest IS NULL OR octet_length(semantic_digest) = 32),
            account_key bytea NULL CHECK (account_key IS NULL OR octet_length(account_key) = 32),
            observed_at_minute_utc bigint NOT NULL CHECK (observed_at_minute_utc >= 0),
            quarantine_code text NOT NULL CHECK (octet_length(quarantine_code) BETWEEN 1 AND 256),
            quarantine_message text NOT NULL CHECK (octet_length(quarantine_message) BETWEEN 1 AND 2048),
            quarantined_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (source_instance_id, delivery_id),
            FOREIGN KEY (source_instance_id, delivery_id)
                REFERENCES skypulse.tap_delivery (source_instance_id, delivery_id)
                ON DELETE CASCADE
        );

        CREATE INDEX quarantine_retention_idx
            ON skypulse.quarantine (source_instance_id, quarantined_at_utc, delivery_id);
        """;

    private const string Migration2 = """
        CREATE TABLE skypulse.corpus_capacity (
            capacity_id smallint PRIMARY KEY DEFAULT 1 CHECK (capacity_id = 1),
            base_profile_id text NOT NULL CHECK (octet_length(base_profile_id) BETWEEN 1 AND 256),
            base_profile_version integer NOT NULL CHECK (base_profile_version > 0),
            base_corpus_cap bigint NOT NULL CHECK (base_corpus_cap > 0),
            base_prefix_sha256 bytea NOT NULL CHECK (octet_length(base_prefix_sha256) = 32),
            active_profile_id text NOT NULL CHECK (octet_length(active_profile_id) BETWEEN 1 AND 256),
            active_corpus_cap bigint NOT NULL,
            active_prefix_sha256 bytea NOT NULL CHECK (octet_length(active_prefix_sha256) = 32),
            target_profile_id text NULL CHECK (target_profile_id IS NULL OR octet_length(target_profile_id) BETWEEN 1 AND 256),
            target_corpus_cap bigint NULL,
            target_prefix_sha256 bytea NULL CHECK (target_prefix_sha256 IS NULL OR octet_length(target_prefix_sha256) = 32),
            operation_version bigint NOT NULL CHECK (operation_version > 0),
            updated_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
            FOREIGN KEY (capacity_id)
                REFERENCES skypulse.runtime_manifest (manifest_id)
                ON DELETE RESTRICT,
            CHECK (active_corpus_cap >= base_corpus_cap),
            CHECK (target_corpus_cap IS NULL OR target_corpus_cap > active_corpus_cap),
            CHECK (
                (target_profile_id IS NULL AND target_corpus_cap IS NULL AND target_prefix_sha256 IS NULL)
                OR (target_profile_id IS NOT NULL AND target_corpus_cap IS NOT NULL
                    AND target_prefix_sha256 IS NOT NULL))
        );
        """;

    public const string BootstrapSql = """
        CREATE SCHEMA IF NOT EXISTS skypulse;
        CREATE TABLE IF NOT EXISTS skypulse.schema_migration (
            version integer PRIMARY KEY CHECK (version > 0),
            sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
            applied_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
        """;

    public const string ReadMigrationsSql = """
        SELECT version, sha256
        FROM skypulse.schema_migration
        ORDER BY version;
        """;

    internal const string ShadowBootstrapSql = """
        CREATE TABLE pg_temp.schema_migration (
            version integer PRIMARY KEY CHECK (version > 0),
            sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
            applied_at_utc timestamp with time zone NOT NULL DEFAULT clock_timestamp()
        );
        """;

    internal const string ReadCatalogContractSql = """
        WITH target_relations AS MATERIALIZED (
            SELECT relation.oid, relation.relname, relation.relkind,
                   relation.relrowsecurity, relation.relforcerowsecurity,
                   relation.relreplident, access_method.amname
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            LEFT JOIN pg_catalog.pg_am AS access_method ON access_method.oid = relation.relam
            WHERE namespace.nspname = @schema_name
              AND relation.relkind IN ('r', 'p')
        ), table_contract AS (
            SELECT
                'table'::text AS object_kind,
                relation.relname::text AS object_name,
                concat_ws('|', relation.relkind::text, relation.relrowsecurity::text,
                    relation.relforcerowsecurity::text, relation.relreplident::text,
                    COALESCE(relation.amname, '')) AS definition
            FROM target_relations AS relation
        ), column_contract AS (
            SELECT
                'column'::text AS object_kind,
                relation.relname || '.' || attribute.attnum::text AS object_name,
                concat_ws('|', attribute.attname, pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
                    attribute.attnotnull::text, attribute.attidentity::text,
                    attribute.attgenerated::text,
                    COALESCE(pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid, true), '')) AS definition
            FROM target_relations AS relation
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = relation.oid
             AND attribute.attnum > 0
             AND NOT attribute.attisdropped
            LEFT JOIN pg_catalog.pg_attrdef AS default_value
              ON default_value.adrelid = attribute.attrelid
             AND default_value.adnum = attribute.attnum
        ), constraint_contract AS (
            SELECT
                'constraint'::text AS object_kind,
                relation.relname || '.' || constraint_value.conname AS object_name,
                concat_ws('|', constraint_value.contype::text, constraint_value.condeferrable::text,
                    constraint_value.condeferred::text, constraint_value.convalidated::text,
                    pg_catalog.pg_get_constraintdef(constraint_value.oid, true)) AS definition
            FROM target_relations AS relation
            JOIN pg_catalog.pg_constraint AS constraint_value
              ON constraint_value.conrelid = relation.oid
        ), index_contract AS (
            SELECT
                'index'::text AS object_kind,
                relation.relname || '.' || index_relation.relname AS object_name,
                concat_ws('|', index_value.indisunique::text, index_value.indisprimary::text,
                    index_value.indisexclusion::text, index_value.indisvalid::text,
                    index_value.indisready::text, pg_catalog.pg_get_indexdef(index_value.indexrelid, 0, true)) AS definition
            FROM target_relations AS relation
            JOIN pg_catalog.pg_index AS index_value ON index_value.indrelid = relation.oid
            JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid = index_value.indexrelid
        ), sequence_contract AS (
            SELECT
                'sequence'::text AS object_kind,
                sequence_relation.relname::text AS object_name,
                concat_ws('|', pg_catalog.format_type(sequence_value.seqtypid, NULL),
                    sequence_value.seqstart::text, sequence_value.seqincrement::text,
                    sequence_value.seqmax::text, sequence_value.seqmin::text,
                    sequence_value.seqcache::text, sequence_value.seqcycle::text) AS definition
            FROM pg_catalog.pg_class AS sequence_relation
            JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = sequence_relation.relnamespace
            JOIN pg_catalog.pg_sequence AS sequence_value ON sequence_value.seqrelid = sequence_relation.oid
            WHERE namespace.nspname = @schema_name
        )
        SELECT object_kind, object_name, definition FROM table_contract
        UNION ALL SELECT object_kind, object_name, definition FROM column_contract
        UNION ALL SELECT object_kind, object_name, definition FROM constraint_contract
        UNION ALL SELECT object_kind, object_name, definition FROM index_contract
        UNION ALL SELECT object_kind, object_name, definition FROM sequence_contract
        ORDER BY object_kind, object_name;
        """;

    internal static string CreateShadowMigrationSql(PostgreSqlSchemaMigration migration)
        => migration.Sql.Replace("skypulse.", "pg_temp.", StringComparison.Ordinal);

    private static readonly ReadOnlyCollection<PostgreSqlSchemaMigration> MigrationList = new(
    [
        new PostgreSqlSchemaMigration(1, Migration1, ComputeSha256(Migration1)),
        new PostgreSqlSchemaMigration(2, Migration2, ComputeSha256(Migration2)),
    ]);

    private static readonly IReadOnlyDictionary<string, IReadOnlySet<string>> RequiredColumns =
        BuildRequiredTableColumns();

    public static IReadOnlyList<PostgreSqlSchemaMigration> Migrations => MigrationList;

    public static IReadOnlyDictionary<string, IReadOnlySet<string>> RequiredTableColumns => RequiredColumns;

    internal static byte[] EncodeAccountKey(AccountKey accountKey)
    {
        Guard.ValidAccountKey(accountKey, nameof(accountKey));
        return Convert.FromHexString(accountKey.ToString());
    }

    internal static AccountKey DecodeAccountKey(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length != AccountKey.ByteLength)
        {
            throw new InvalidOperationException($"A persisted account key must contain {AccountKey.ByteLength} bytes.");
        }

        return AccountKey.Parse(Convert.ToHexString(value).ToLowerInvariant());
    }

    internal static byte[] DecodeDigest(string value)
    {
        Guard.HexDigest(value, nameof(value));
        return Convert.FromHexString(value);
    }

    private static string ComputeSha256(string value)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();

    private static ReadOnlyDictionary<string, IReadOnlySet<string>> BuildRequiredTableColumns()
    {
        var result = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal)
        {
            ["schema_migration"] = Columns("version", "sha256", "applied_at_utc"),
            ["runtime_manifest"] = Columns(
                "manifest_id", "profile_id", "profile_version", "corpus_cap", "allowlist_sha256",
                "source_instance_id", "index_namespace", "index_provider_name", "index_schema_id",
                "index_schema_version", "index_schema_fingerprint", "package_id", "package_version",
                "package_nupkg_sha256", "package_canonical_manifest_sha256", "package_repository_url",
                "package_repository_commit", "package_build_sdk_version", "manifest_fingerprint", "bound_at_utc"),
            ["corpus_capacity"] = Columns(
                "capacity_id", "base_profile_id", "base_profile_version", "base_corpus_cap",
                "base_prefix_sha256", "active_profile_id", "active_corpus_cap",
                "active_prefix_sha256", "target_profile_id", "target_corpus_cap",
                "target_prefix_sha256", "operation_version", "updated_at_utc"),
            ["account_state"] = Columns("account_key", "state_version", "lifecycle", "repository_generation", "completed_sync_revision", "last_applied_revision", "synchronization_complete", "last_activity_minute_utc", "current_post_count", "current_following_count", "current_follower_count", "updated_at_utc"),
            ["tap_delivery"] = Columns("source_instance_id", "delivery_id", "delivery_digest", "semantic_digest", "account_key", "observed_at_minute_utc", "outcome", "committed_at_utc"),
            ["source_delivery_retention_watermark"] = Columns("source_instance_id", "safe_delivery_id_inclusive", "evidence_reference", "updated_at_utc"),
            ["semantic_event_retention_watermark"] = Columns("watermark_id", "safe_observed_minute_utc", "evidence_reference", "updated_at_utc"),
            ["activity_retention_watermark"] = Columns("watermark_id", "safe_minute_utc", "evidence_reference", "updated_at_utc"),
            ["semantic_event"] = Columns("semantic_digest", "account_key", "repository_generation", "event_kind", "observed_at_minute_utc", "repository_revision", "lifecycle", "collection", "action", "record_key", "cid", "target_account_key", "is_direct_reply", "is_live", "applied_at_utc"),
            ["record_state"] = Columns("account_key", "repository_generation", "collection", "record_key", "latest_revision", "is_deleted", "cid", "target_account_key", "is_direct_reply", "updated_at_utc"),
            ["follow_pair"] = Columns("source_account_key", "target_account_key", "multiplicity", "updated_at_utc"),
            ["reconciliation_dependency"] = Columns("owner_account_key", "owner_repository_generation", "affected_account_key"),
            ["lifecycle_transition_work"] = Columns(
                "source_instance_id", "delivery_id", "delivery_digest", "semantic_digest", "account_key",
                "repository_generation", "event_kind", "observed_at_minute_utc", "repository_revision",
                "lifecycle", "is_live", "phase", "started_at_utc", "updated_at_utc"),
            ["activity_minute_bucket"] = Columns("account_key", "repository_generation", "minute_utc", "record_creates", "record_updates", "record_deletes", "post_creates", "received_engagement_creates"),
            ["desired_projection"] = ProjectionColumns(includeNextRecalculation: true, includeUpdated: true),
            ["published_projection"] = ProjectionColumns(includeNextRecalculation: false, includeUpdated: false, includePublished: true),
            ["projection_outbox"] = ProjectionColumns(includeNextRecalculation: true, includeUpdated: false, includeOutbox: true),
            ["projection_recalculation_due"] = Columns("account_key", "source_projection_version", "due_minute_utc", "available_at_utc", "attempt_count", "lease_id", "leased_until_utc", "last_error_code", "last_error_message"),
            ["quarantine"] = Columns("source_instance_id", "delivery_id", "delivery_digest", "semantic_digest", "account_key", "observed_at_minute_utc", "quarantine_code", "quarantine_message", "quarantined_at_utc"),
        };

        return new ReadOnlyDictionary<string, IReadOnlySet<string>>(result);
    }

    private static HashSet<string> ProjectionColumns(
        bool includeNextRecalculation,
        bool includeUpdated,
        bool includePublished = false,
        bool includeOutbox = false)
    {
        var values = new HashSet<string>(StringComparer.Ordinal)
        {
            "account_key", "projection_version", "operation", "is_deleted", "is_complete",
            "projection_cut_minute_utc", "last_activity_minute_utc",
            "created_record_count_1_day", "created_record_count_7_days", "created_record_count_30_days",
            "updated_record_count_1_day", "updated_record_count_7_days", "updated_record_count_30_days",
            "deleted_record_count_1_day", "deleted_record_count_7_days", "deleted_record_count_30_days",
            "current_post_count", "current_following_count", "current_follower_count",
            "post_creates_1_day", "post_creates_7_days", "post_creates_30_days",
            "received_engagement_creates_30_days",
        };

        if (includeNextRecalculation)
        {
            values.Add("next_recalculation_minute_utc");
        }

        if (includeUpdated)
        {
            values.Add("updated_at_utc");
        }

        if (includePublished)
        {
            values.Add("published_at_utc");
        }

        if (includeOutbox)
        {
            values.UnionWith(["available_at_utc", "attempt_count", "lease_id", "leased_until_utc", "completed_at_utc", "last_error_code", "last_error_message"]);
        }

        return values;
    }

    private static HashSet<string> Columns(params string[] values)
        => new HashSet<string>(values, StringComparer.Ordinal);
}

/// <summary>
/// Reports exact schema-catalog validation errors without repairing an unexpected schema.
/// </summary>
public sealed record PostgreSqlSchemaValidationResult(IReadOnlyList<string> Errors)
{
    public bool IsValid => Errors.Count == 0;
}

/// <summary>
/// Applies known migrations and validates the installed table/column and migration contract.
/// </summary>
public sealed class PostgreSqlSchemaManager
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlSchemaManager(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
    }

    public async Task ApplyMigrationsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (var bootstrap = connection.CreateCommand())
        {
            bootstrap.CommandText = PostgreSqlSchema.BootstrapSql;
            await bootstrap.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        foreach (var migration in PostgreSqlSchema.Migrations)
        {
            await using var read = connection.CreateCommand();
            read.Transaction = transaction;
            read.CommandText = "SELECT sha256 FROM skypulse.schema_migration WHERE version = @version FOR UPDATE;";
            read.Parameters.AddWithValue("version", migration.Version);
            var existing = await read.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (existing is string existingSha)
            {
                if (!string.Equals(existingSha, migration.Sha256, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"PostgreSQL schema migration {migration.Version} has an unexpected SHA-256.");
                }

                continue;
            }

            await using var apply = connection.CreateCommand();
            apply.Transaction = transaction;
            apply.CommandText = migration.Sql;
            await apply.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await using var record = connection.CreateCommand();
            record.Transaction = transaction;
            record.CommandText = "INSERT INTO skypulse.schema_migration (version, sha256) VALUES (@version, @sha256);";
            record.Parameters.AddWithValue("version", migration.Version);
            record.Parameters.AddWithValue("sha256", migration.Sha256);
            await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<PostgreSqlSchemaValidationResult> ValidateAsync(CancellationToken cancellationToken = default)
    {
        var errors = new List<string>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);
        Dictionary<int, string> applied;
        try
        {
            applied = await ReadInstalledMigrationsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException exception)
        {
            errors.Add($"The installed migration catalog could not be read: PostgreSQL {exception.SqlState}.");
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new PostgreSqlSchemaValidationResult(new ReadOnlyCollection<string>(errors));
        }

        foreach (var expected in PostgreSqlSchema.Migrations)
        {
            if (!applied.TryGetValue(expected.Version, out var sha))
            {
                errors.Add($"Missing migration {expected.Version}.");
            }
            else if (!string.Equals(sha, expected.Sha256, StringComparison.Ordinal))
            {
                errors.Add($"Migration {expected.Version} has SHA-256 {sha}, expected {expected.Sha256}.");
            }
        }

        foreach (var unexpected in applied.Keys.Except(PostgreSqlSchema.Migrations.Select(static value => value.Version)))
        {
            errors.Add($"Unexpected migration {unexpected}.");
        }

        try
        {
            await ExecuteAsync(connection, transaction, PostgreSqlSchema.ShadowBootstrapSql, cancellationToken).ConfigureAwait(false);
            foreach (var migration in PostgreSqlSchema.Migrations)
            {
                await ExecuteAsync(
                    connection,
                    transaction,
                    PostgreSqlSchema.CreateShadowMigrationSql(migration),
                    cancellationToken).ConfigureAwait(false);
            }

            var temporarySchemaName = await ReadTemporarySchemaNameAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            var expectedCatalog = await ReadCatalogContractAsync(
                connection,
                transaction,
                temporarySchemaName,
                cancellationToken).ConfigureAwait(false);
            var actualCatalog = await ReadCatalogContractAsync(
                connection,
                transaction,
                PostgreSqlSchema.SchemaName,
                cancellationToken).ConfigureAwait(false);

            foreach (var expected in expectedCatalog)
            {
                if (!actualCatalog.TryGetValue(expected.Key, out var actualDefinition))
                {
                    errors.Add($"Required catalog object {expected.Key} does not exist.");
                }
                else if (!string.Equals(expected.Value, actualDefinition, StringComparison.Ordinal))
                {
                    errors.Add($"Catalog object {expected.Key} does not match the reviewed schema contract.");
                }
            }

            foreach (var extra in actualCatalog.Keys.Except(expectedCatalog.Keys, StringComparer.Ordinal))
            {
                errors.Add($"Unexpected catalog object {extra} exists; schema drift is not allowed.");
            }
        }
        catch (PostgresException exception)
        {
            errors.Add($"The executable schema contract could not be constructed: PostgreSQL {exception.SqlState}.");
        }

        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        return new PostgreSqlSchemaValidationResult(new ReadOnlyCollection<string>(errors));
    }

    private static async Task<Dictionary<int, string>> ReadInstalledMigrationsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, string>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlSchema.ReadMigrationsSql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(reader.GetInt32(0), reader.GetString(1));
        }

        return result;
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<string> ReadTemporarySchemaNameAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = pg_catalog.pg_my_temp_schema();";
        return (string?)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("PostgreSQL did not expose the validation temporary schema.");
    }

    private static async Task<Dictionary<string, string>> ReadCatalogContractAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schemaName,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PostgreSqlSchema.ReadCatalogContractSql;
        command.Parameters.AddWithValue("schema_name", NpgsqlTypes.NpgsqlDbType.Text, schemaName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var key = $"{reader.GetString(0)}:{reader.GetString(1)}";
            var definition = NormalizeCatalogDefinition(reader.GetString(2), schemaName);
            if (!result.TryAdd(key, definition))
            {
                throw new InvalidOperationException($"The catalog contract contains duplicate object {key}.");
            }
        }

        return result;
    }

    private static string NormalizeCatalogDefinition(string definition, string schemaName)
        => definition
            .Replace($"\"{schemaName.Replace("\"", "\"\"", StringComparison.Ordinal)}\".", string.Empty, StringComparison.Ordinal)
            .Replace($"{schemaName}.", string.Empty, StringComparison.Ordinal);
}
