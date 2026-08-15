using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class PostgreSqlSchemaTests
{
    [Fact]
    public void ReconstructedVersionOneMigrationChecksumIsPinned()
    {
        Assert.Equal(
            "10742f90fe43929271f02552eb7d7744163b98650c9ae2c7adca12fd25ba5ec1",
            PostgreSqlSchema.Migrations[0].Sha256);
    }

    [Fact]
    public void VersionTwoCorpusCapacityMigrationChecksumIsPinned()
    {
        Assert.Equal(
            "f982657018c290b2944c49a25238432195075b0ec950a9dd30e73ce1acd18395",
            PostgreSqlSchema.Migrations[1].Sha256);
    }

    [Fact]
    public void CorpusCapacitySchemaAllowsOnlyOneMonotonicTarget()
    {
        var sql = PostgreSqlSchema.Migrations[1].Sql;
        var columns = PostgreSqlSchema.RequiredTableColumns["corpus_capacity"];

        Assert.Contains("active_corpus_cap", columns);
        Assert.Contains("target_corpus_cap", columns);
        Assert.Contains("operation_version", columns);
        Assert.Contains("PRIMARY KEY DEFAULT 1 CHECK (capacity_id = 1)", sql, StringComparison.Ordinal);
        Assert.Contains("CHECK (active_corpus_cap >= base_corpus_cap)", sql, StringComparison.Ordinal);
        Assert.Contains(
            "CHECK (target_corpus_cap IS NULL OR target_corpus_cap > active_corpus_cap)",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            "REFERENCES skypulse.runtime_manifest (manifest_id)",
            sql,
            StringComparison.Ordinal);
    }

    [Fact]
    public void SchemaContainsRequiredDurableBoundaries()
    {
        var tables = PostgreSqlSchema.RequiredTableColumns;

        Assert.Contains("schema_migration", tables.Keys);
        Assert.Contains("runtime_manifest", tables.Keys);
        Assert.Contains("corpus_capacity", tables.Keys);
        Assert.Contains("tap_delivery", tables.Keys);
        Assert.Contains("source_delivery_retention_watermark", tables.Keys);
        Assert.Contains("semantic_event_retention_watermark", tables.Keys);
        Assert.Contains("activity_retention_watermark", tables.Keys);
        Assert.Contains("semantic_event", tables.Keys);
        Assert.Contains("account_state", tables.Keys);
        Assert.Contains("record_state", tables.Keys);
        Assert.Contains("follow_pair", tables.Keys);
        Assert.Contains("activity_minute_bucket", tables.Keys);
        Assert.Contains("reconciliation_dependency", tables.Keys);
        Assert.Contains("lifecycle_transition_work", tables.Keys);
        Assert.Contains("desired_projection", tables.Keys);
        Assert.Contains("published_projection", tables.Keys);
        Assert.Contains("projection_outbox", tables.Keys);
        Assert.Contains("projection_recalculation_due", tables.Keys);
        Assert.Contains("quarantine", tables.Keys);
        Assert.Contains("source_instance_id", tables["tap_delivery"]);
        Assert.Contains("last_applied_revision", tables["account_state"]);
    }

    [Fact]
    public void RecordSchemaRetainsDeleteRevisionTombstone()
    {
        var columns = PostgreSqlSchema.RequiredTableColumns["record_state"];
        var sql = PostgreSqlSchema.Migrations[0].Sql;

        Assert.Contains("latest_revision", columns);
        Assert.Contains("is_deleted", columns);
        Assert.Contains("is_deleted AND cid IS NULL AND target_account_key IS NULL AND NOT is_direct_reply", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SchemaHasNoRawOrContentPayloadColumn()
    {
        var forbidden = new[] { "raw", "json", "body", "text_payload", "content", "handle", "media" };
        var columns = PostgreSqlSchema.RequiredTableColumns.Values.SelectMany(static value => value);

        Assert.DoesNotContain(columns, column => forbidden.Any(token => column.Contains(token, StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void SemanticDeduplicationIsScopedToRepositoryGeneration()
    {
        var sql = PostgreSqlSchema.Migrations[0].Sql;

        Assert.Contains("PRIMARY KEY (account_key, repository_generation, semantic_digest)", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (account_key, repository_generation, semantic_digest)", PostgreSqlCommands.InsertSemanticEventSql, StringComparison.Ordinal);
    }

    [Fact]
    public void OutboxCarriesImmutableProjectionSnapshotAndLeaseState()
    {
        var columns = PostgreSqlSchema.RequiredTableColumns["projection_outbox"];

        Assert.Contains("projection_version", columns);
        Assert.Contains("current_follower_count", columns);
        Assert.Contains("lease_id", columns);
        Assert.Contains("leased_until_utc", columns);
        Assert.Contains("completed_at_utc", columns);
    }

    [Fact]
    public void LeaseSqlEnforcesPerAccountVersionOrder()
    {
        Assert.Contains("earlier.projection_version < candidate.projection_version", PostgreSqlCommands.LeaseOutboxSql, StringComparison.Ordinal);
        Assert.Contains("earlier.completed_at_utc IS NULL", PostgreSqlCommands.LeaseOutboxSql, StringComparison.Ordinal);
        Assert.Contains("FOR UPDATE OF candidate SKIP LOCKED", PostgreSqlCommands.LeaseOutboxSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeliveryIdentityIsScopedToDurableSourceInstance()
    {
        var sql = PostgreSqlSchema.Migrations[0].Sql;

        Assert.Contains("PRIMARY KEY (source_instance_id, delivery_id)", sql, StringComparison.Ordinal);
        Assert.Equal(3, sql.Split("CHECK (delivery_id > 0)", StringSplitOptions.None).Length - 1);
        Assert.Contains("WHERE source_instance_id = @source_instance_id", PostgreSqlCommands.ReadDeliverySql, StringComparison.Ordinal);
    }

    [Fact]
    public void CompletionSqlPublishesExactLeasedRowAtomically()
    {
        Assert.Contains("WITH completed AS", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
        Assert.Contains("FROM completed", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
        Assert.Contains("published_projection", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
        Assert.Contains("lease_id = @lease_id", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
        Assert.Contains("leased_until_utc > clock_timestamp()", PostgreSqlCommands.CompleteOutboxSql, StringComparison.Ordinal);
    }

    [Fact]
    public void SchemaManagerBootstrapIsIdempotentAndVersioned()
    {
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS", PostgreSqlSchema.BootstrapSql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS skypulse.schema_migration", PostgreSqlSchema.BootstrapSql, StringComparison.Ordinal);
        Assert.Contains("sha256", PostgreSqlSchema.ReadMigrationsSql, StringComparison.Ordinal);
        Assert.Equal(PostgreSqlSchema.CurrentVersion, PostgreSqlSchema.Migrations[^1].Version);
    }

    [Fact]
    public void ValidationComparesAnExecutableExactCatalogContract()
    {
        var sql = PostgreSqlSchema.ReadCatalogContractSql;

        Assert.Contains("pg_catalog.format_type", sql, StringComparison.Ordinal);
        Assert.Contains("attribute.attnotnull", sql, StringComparison.Ordinal);
        Assert.Contains("attribute.attidentity", sql, StringComparison.Ordinal);
        Assert.Contains("attribute.attgenerated", sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.pg_get_expr", sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.pg_get_constraintdef", sql, StringComparison.Ordinal);
        Assert.Contains("constraint_value.contype", sql, StringComparison.Ordinal);
        Assert.Contains("index_value.indisunique", sql, StringComparison.Ordinal);
        Assert.Contains("index_value.indisprimary", sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.pg_get_indexdef", sql, StringComparison.Ordinal);
        Assert.Contains("sequence_value.seqincrement", sql, StringComparison.Ordinal);
        Assert.Contains("pg_temp.schema_migration", PostgreSqlSchema.ShadowBootstrapSql, StringComparison.Ordinal);
        Assert.Contains("pg_temp.tap_delivery", PostgreSqlSchema.CreateShadowMigrationSql(PostgreSqlSchema.Migrations[0]), StringComparison.Ordinal);
    }

    [Fact]
    public void RuntimeManifestAndDeliveryReservationFailClosedInSchema()
    {
        var sql = PostgreSqlSchema.Migrations[0].Sql;
        var manifest = PostgreSqlSchema.RequiredTableColumns["runtime_manifest"];

        Assert.Contains("package_nupkg_sha256", manifest);
        Assert.Contains("package_canonical_manifest_sha256", manifest);
        Assert.Contains("package_repository_url", manifest);
        Assert.Contains("package_repository_commit", manifest);
        Assert.Contains("package_build_sdk_version", manifest);
        Assert.Contains("manifest_fingerprint", manifest);
        Assert.Contains("REFERENCES skypulse.runtime_manifest (source_instance_id)", sql, StringComparison.Ordinal);
        Assert.Contains("outcome = 0 AND committed_at_utc IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("semantic_digest IS NULL AND account_key IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("outcome IN (1, 2)", sql, StringComparison.Ordinal);
        Assert.Contains("semantic_digest IS NOT NULL AND account_key IS NOT NULL", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReconciliationDependencyUsesExactGenerationScopedPrimaryKey()
    {
        var sql = PostgreSqlSchema.Migrations[0].Sql;

        Assert.Contains(
            "PRIMARY KEY (owner_account_key, owner_repository_generation, affected_account_key)",
            sql,
            StringComparison.Ordinal);
    }

    [Fact]
    public void LifecycleWorkRetainsOnlyBoundedMetadataAndReferencesItsPendingDelivery()
    {
        var sql = PostgreSqlSchema.Migrations[0].Sql;
        var columns = PostgreSqlSchema.RequiredTableColumns["lifecycle_transition_work"];

        Assert.Contains("phase", columns);
        Assert.Contains("repository_generation", columns);
        Assert.Contains("semantic_digest", columns);
        Assert.Contains(
            "REFERENCES skypulse.tap_delivery (source_instance_id, delivery_id)",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            "UNIQUE (account_key)",
            sql,
            StringComparison.Ordinal);
    }
}
