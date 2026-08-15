using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;
using Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Tap;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

public sealed class PostgreSqlDurableTapIngestionIntegrationTests
{
    [PostgreSqlIntegrationFact]
    public async Task ExactFrameReservationQuarantineAndLostAckRedeliveryAreAtomic()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        var backend = new PostgreSqlDurableTapBackend(
            database.Ingestion,
            database.Planning,
            database.Lifecycle);
        var processor = new DurableTapDeliveryProcessor(
            database.SourceInstanceId,
            backend,
            new AdmitAll());
        const string json = """
            {"id":71,"type":"identity","identity":{"did":"did:plc:pg-quarantine","is_active":true,"status":"deleted"}}
            """;
        var delivery = new TapDelivery(json, Digest(json));
        var observed = DateTimeOffset.UtcNow;

        var committed = await processor.ProcessAsync(delivery, observed);
        var redelivery = await processor.ProcessAsync(delivery, observed.AddMinutes(10));

        Assert.True(committed.AcknowledgementAllowed);
        Assert.True(redelivery.AcknowledgementAllowed);
        Assert.Equal(
            (short)DurableDeliveryOutcome.Quarantined,
            await database.ScalarAsync<short>(
                "SELECT outcome FROM skypulse.tap_delivery WHERE delivery_id = 71;"));
        Assert.Equal(
            1L,
            await database.ScalarAsync<long>(
                "SELECT count(*) FROM skypulse.quarantine WHERE delivery_id = 71;"));
        Assert.Equal(
            "invalid-value",
            await database.ScalarAsync<string>(
                "SELECT quarantine_code FROM skypulse.quarantine WHERE delivery_id = 71;"));
    }

    [PostgreSqlIntegrationFact]
    public async Task BootstrapHistoricalSyncAndLiveRecordFormOneAcknowledgementSafePipeline()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        using var corpus = new CorpusFixture();
        var frozen = corpus.Freeze();
        var profile = Assert.Single(frozen.Manifest.Profiles);
        using var admission = VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            profile.Name,
            profile.AccountCount,
            profile.PrefixSha256);
        await new PostgreSqlCorpusBootstrapper(database.DataSource, admission, pageSize: 1)
            .BootstrapAsync();
        var backend = new PostgreSqlDurableTapBackend(
            database.Ingestion,
            database.Planning,
            database.Lifecycle);
        var processor = new DurableTapDeliveryProcessor(
            database.SourceInstanceId,
            backend,
            admission,
            new DurableTapProcessingOptions
            {
                MaximumPlanningAttempts = 4,
                LifecyclePageSize = 1,
            });
        var observed = DateTimeOffset.UtcNow;
        const string historicalJson = """
            {"id":72,"type":"record","record":{"live":false,"did":"did:plc:bootstrap-a","rev":"3jzfcijpj2z2a","collection":"app.bsky.actor.profile","rkey":"self","action":"create","cid":"bafy-profile","metadata_status":"valid"}}
            """;
        const string syncJson = """
            {"id":73,"type":"repo_sync","repo_sync":{"did":"did:plc:bootstrap-a","rev":"3jzfcijpj2z2b","status":"active"}}
            """;
        const string liveJson = """
            {"id":74,"type":"record","record":{"live":true,"did":"did:plc:bootstrap-a","rev":"3jzfcijpj2z2c","collection":"app.bsky.feed.post","rkey":"post","action":"create","cid":"bafy-post","metadata_status":"valid"}}
            """;

        var historical = await processor.ProcessAsync(
            new TapDelivery(historicalJson, Digest(historicalJson)),
            observed);
        var sync = await processor.ProcessAsync(
            new TapDelivery(syncJson, Digest(syncJson)),
            observed.AddMinutes(1));
        var liveDelivery = new TapDelivery(liveJson, Digest(liveJson));
        var live = await processor.ProcessAsync(liveDelivery, observed.AddMinutes(2));
        var lostAckRedelivery = await processor.ProcessAsync(liveDelivery, observed.AddMinutes(12));

        Assert.True(historical.AcknowledgementAllowed);
        Assert.True(sync.AcknowledgementAllowed);
        Assert.True(live.AcknowledgementAllowed);
        Assert.True(lostAckRedelivery.AcknowledgementAllowed);
        var owner = AccountKey.FromDid("did:plc:bootstrap-a");
        Assert.True(await database.ScalarAsync<bool>(
            "SELECT synchronization_complete FROM skypulse.account_state WHERE account_key = @account_key;",
            PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            1L,
            await database.ScalarAsync<long>(
                "SELECT current_post_count FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            2L,
            await database.ScalarAsync<long>(
                "SELECT count(*) FROM skypulse.record_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", owner)));
        Assert.Equal(
            3L,
            await database.ScalarAsync<long>(
                "SELECT count(*) FROM skypulse.tap_delivery WHERE outcome = 1;"));
        Assert.Equal(0L, await database.ScalarAsync<long>("SELECT count(*) FROM skypulse.quarantine;"));
    }

    [PostgreSqlIntegrationFact]
    public async Task FullPrefixBootstrapIsIdempotentNeverOverwritesProgressAndRejectsExtras()
    {
        await using var database = await PostgreSqlTestDatabase.CreateAsync();
        using var corpus = new CorpusFixture();
        var frozen = corpus.Freeze();
        var profile = Assert.Single(frozen.Manifest.Profiles);
        using var admission = VerifiedCorpusAdmission.Open(
            frozen.ManifestPath,
            profile.Name,
            profile.AccountCount,
            profile.PrefixSha256);
        var progressed = admission.ReadPage(0, 1)[0];
        await database.ExecuteAsync(
            """
            INSERT INTO skypulse.account_state (
                account_key, state_version, lifecycle, repository_generation,
                completed_sync_revision, last_applied_revision, synchronization_complete,
                last_activity_minute_utc, current_post_count,
                current_following_count, current_follower_count)
            VALUES (@account_key, 9, 1, 3, '3jzfcijpj2z2a', '3jzfcijpj2z2a', TRUE, 100, 7, 5, 4);
            """,
            PostgreSqlTestDatabase.AccountParameter("account_key", progressed));
        var bootstrapper = new PostgreSqlCorpusBootstrapper(database.DataSource, admission, pageSize: 2);

        await bootstrapper.BootstrapAsync();
        await bootstrapper.BootstrapAsync();

        Assert.Equal(3L, await database.ScalarAsync<long>("SELECT count(*) FROM skypulse.account_state;"));
        Assert.Equal(
            9L,
            await database.ScalarAsync<long>(
                "SELECT state_version FROM skypulse.account_state WHERE account_key = @account_key;",
                PostgreSqlTestDatabase.AccountParameter("account_key", progressed)));
        Assert.Equal(
            2L,
            await database.ScalarAsync<long>(
                "SELECT count(*) FROM skypulse.reconciliation_dependency WHERE owner_account_key = affected_account_key;"));

        var extra = PostgreSqlTestDatabase.Account("bootstrap-extra");
        await database.ExecuteAsync(
            """
            INSERT INTO skypulse.account_state (
                account_key, state_version, lifecycle, repository_generation,
                completed_sync_revision, last_applied_revision, synchronization_complete,
                last_activity_minute_utc, current_post_count,
                current_following_count, current_follower_count)
            VALUES (@account_key, 1, 1, 0, NULL, NULL, FALSE, 0, 0, 0, 0);
            """,
            PostgreSqlTestDatabase.AccountParameter("account_key", extra));
        await Assert.ThrowsAsync<InvalidOperationException>(() => bootstrapper.BootstrapAsync());
    }

    private static string Digest(string value)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();

    private sealed class AdmitAll : IAccountAdmission
    {
        public bool IsAdmitted(AccountKey accountKey) => accountKey.IsValid;
    }

    private sealed class CorpusFixture : IDisposable
    {
        private readonly string _directory = Path.Combine(
            Path.GetTempPath(),
            $"skypulse-pg-bootstrap-{Guid.NewGuid():N}");

        internal CorpusFreezeResult Freeze()
        {
            Directory.CreateDirectory(_directory);
            var journal = Path.Combine(_directory, "observations.ndjson");
            File.WriteAllText(
                journal,
                """
                {"ordinal":1,"did":"did:plc:bootstrap-a","status":"active","sourcePosition":"a"}
                {"ordinal":2,"did":"did:plc:bootstrap-b","status":"active","sourcePosition":"b"}
                {"ordinal":3,"did":"did:plc:bootstrap-c","status":"active","sourcePosition":"c"}

                """,
                new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(journal, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }

            return CorpusFreezer.Freeze(new CorpusFreezeOptions
            {
                JournalPath = journal,
                OutputDirectory = Path.Combine(_directory, "frozen"),
                MemoryBudgetBytes = 4 * 1024,
                MergeFanIn = 2,
                Profiles = [new CorpusProfileRequest("three", 3)],
            });
        }

        public void Dispose()
        {
            if (Directory.Exists(_directory))
            {
                Directory.Delete(_directory, recursive: true);
            }
        }
    }
}
