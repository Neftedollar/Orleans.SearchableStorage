using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests;

internal sealed class PostgreSqlTestDatabase : IAsyncDisposable
{
    private const string TidAlphabet = "234567abcdefghijklmnopqrstuvwxyz";

    private PostgreSqlTestDatabase(NpgsqlDataSource dataSource, Guid sourceInstanceId)
    {
        SourceInstanceId = sourceInstanceId;
        DataSource = dataSource;
        Ingestion = new PostgreSqlIngestionStore(dataSource);
        Dispatch = new PostgreSqlDispatchStore(dataSource);
        ProjectionRuntime = new PostgreSqlProjectionRuntimeStore(dataSource);
        Planning = new PostgreSqlPlanningStore(dataSource);
        Lifecycle = new PostgreSqlLifecycleOrchestrator(dataSource);
        Manifest = new PostgreSqlRuntimeManifestStore(dataSource);
        Schema = new PostgreSqlSchemaManager(dataSource);
    }

    internal Guid SourceInstanceId { get; }

    internal NpgsqlDataSource DataSource { get; }

    internal PostgreSqlIngestionStore Ingestion { get; }

    internal PostgreSqlDispatchStore Dispatch { get; }

    internal PostgreSqlProjectionRuntimeStore ProjectionRuntime { get; }

    internal PostgreSqlPlanningStore Planning { get; }

    internal PostgreSqlLifecycleOrchestrator Lifecycle { get; }

    internal PostgreSqlRuntimeManifestStore Manifest { get; }

    internal PostgreSqlSchemaManager Schema { get; }

    internal static async Task<PostgreSqlTestDatabase> CreateAsync(bool applyMigrations = true)
    {
        var connectionString = Environment.GetEnvironmentVariable(
            PostgreSqlIntegrationFactAttribute.ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                $"{PostgreSqlIntegrationFactAttribute.ConnectionStringEnvironmentVariable} is required.");
        }

        var dataSource = NpgsqlDataSource.Create(connectionString);
        var database = new PostgreSqlTestDatabase(dataSource, Guid.NewGuid());
        try
        {
            await database.ExecuteAsync("DROP SCHEMA IF EXISTS skypulse CASCADE;");
            if (applyMigrations)
            {
                await database.Schema.ApplyMigrationsAsync();
                await database.Manifest.BindAsync(CreateRuntimeManifest(database.SourceInstanceId));
            }

            return database;
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }
    }

    internal async Task ExecuteAsync(string sql, params NpgsqlParameter[] parameters)
    {
        await using var command = DataSource.CreateCommand(sql);
        command.Parameters.AddRange(parameters);
        await command.ExecuteNonQueryAsync();
    }

    internal async Task<T> ScalarAsync<T>(string sql, params NpgsqlParameter[] parameters)
    {
        await using var command = DataSource.CreateCommand(sql);
        command.Parameters.AddRange(parameters);
        var value = await command.ExecuteScalarAsync();
        if (value is null or DBNull)
        {
            throw new InvalidOperationException("The PostgreSQL scalar query returned no value.");
        }

        return (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture)!;
    }

    internal static NpgsqlParameter AccountParameter(string name, AccountKey accountKey)
        => new(name, NpgsqlDbType.Bytea) { Value = Convert.FromHexString(accountKey.ToString()) };

    internal static AccountKey Account(string seed) => AccountKey.FromDid($"did:plc:{seed}");

    internal static string Digest(string seed)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(seed))).ToLowerInvariant();

    internal static long CurrentMinuteUtc()
        => DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 60;

    internal static DurableEventEnvelope Envelope(
        Guid sourceInstanceId,
        ulong deliveryId,
        AccountKey accountKey,
        long repositoryGeneration,
        string semanticSeed,
        string? deliverySeed = null,
        DurableRecordAction action = DurableRecordAction.Create,
        string recordKey = "record",
        ulong? revisionOrdinal = null)
        => new(
            sourceInstanceId,
            deliveryId,
            Digest(deliverySeed ?? $"delivery:{deliveryId}"),
            Digest(semanticSeed),
            accountKey,
            repositoryGeneration,
            DurableEventKind.RecordMutation,
            CurrentMinuteUtc(),
            repositoryRevision: Revision(revisionOrdinal ?? deliveryId),
            collection: DurableRecordKind.FeedPost,
            action: action,
            recordKey: recordKey,
            cid: action == DurableRecordAction.Delete ? null : $"cid-{repositoryGeneration}-{deliveryId}");

    internal static AccountStateMutation State(
        AccountKey accountKey,
        long expectedVersion,
        long repositoryGeneration = 0,
        DurableAccountLifecycle lifecycle = DurableAccountLifecycle.Active,
        long currentPostCount = 1)
        => new(
            accountKey,
            expectedVersion,
            checked(expectedVersion + 1),
            lifecycle,
            repositoryGeneration,
            Revision((ulong)expectedVersion + 1),
            synchronizationComplete: true,
            lastActivityMinuteUtc: CurrentMinuteUtc(),
            currentPostCount,
            currentFollowingCount: 0,
            currentFollowerCount: 0);

    internal static string Revision(ulong ordinal)
    {
        Span<char> value = stackalloc char[13];
        value[0] = '3';
        for (var index = value.Length - 1; index > 0; index--)
        {
            value[index] = TidAlphabet[(int)(ordinal & 31)];
            ordinal >>= 5;
        }

        if (ordinal != 0)
        {
            throw new ArgumentOutOfRangeException(nameof(ordinal), "The test revision ordinal exceeds a canonical TID.");
        }

        return new string(value);
    }

    internal static ProjectionSnapshot Projection(
        AccountKey accountKey,
        long version,
        ProjectionOperation operation = ProjectionOperation.Upsert,
        long? nextRecalculationMinuteUtc = null,
        long? projectionCutMinuteUtc = null,
        long currentPostCount = 1)
    {
        var cutMinute = projectionCutMinuteUtc ?? CurrentMinuteUtc();
        return new ProjectionSnapshot(
            accountKey,
            version,
            operation,
            isComplete: true,
            cutMinute,
            nextRecalculationMinuteUtc,
            lastActivityMinuteUtc: cutMinute,
            createdRecordCount1Day: 1,
            createdRecordCount7Days: 1,
            createdRecordCount30Days: 1,
            updatedRecordCount1Day: 0,
            updatedRecordCount7Days: 0,
            updatedRecordCount30Days: 0,
            deletedRecordCount1Day: 0,
            deletedRecordCount7Days: 0,
            deletedRecordCount30Days: 0,
            currentPostCount,
            currentFollowingCount: 0,
            currentFollowerCount: 0,
            postCreates1Day: 1,
            postCreates7Days: 1,
            postCreates30Days: 1,
            receivedEngagementCreates30Days: 0);
    }

    internal static RecordStateMutation Record(DurableEventEnvelope envelope)
        => new(
            envelope.AccountKey,
            envelope.RepositoryGeneration,
            envelope.Collection ?? throw new ArgumentException("The envelope is not a record event.", nameof(envelope)),
            envelope.RecordKey ?? throw new ArgumentException("The envelope is not a record event.", nameof(envelope)),
            envelope.RepositoryRevision ?? throw new ArgumentException("The envelope has no repository revision.", nameof(envelope)),
            isDeleted: envelope.Action == DurableRecordAction.Delete,
            cid: envelope.Cid,
            targetAccountKey: envelope.TargetAccountKey,
            isDirectReply: envelope.IsDirectReply);

    internal static RuntimeManifest CreateRuntimeManifest(Guid sourceInstanceId)
        => new(
            new RuntimeProfileIdentity("skypulse-integration", 1, 1_000_000, Digest("allowlist")),
            sourceInstanceId,
            new RuntimeIndexIdentity(
                "skypulse-integration",
                "SkyPulseIndex",
                "skypulse-account-v1",
                1,
                Digest("index-schema")),
            new RuntimePackageIdentity(
                "Orleans.SearchableStorage",
                "1.0.0-rc.2",
                Digest("nupkg"),
                Digest("canonical-package-manifest"),
                "https://github.com/Neftedollar/Orleans.SearchableStorage",
                new string('a', 40),
                "10.0.303"));

    internal static DurableIngestionCommit Commit(
        DurableEventEnvelope envelope,
        AccountStateMutation state,
        ProjectionSnapshot? projection = null,
        RecordStateMutation? record = null,
        ActivityMinuteDelta? activity = null)
        => new(
            envelope,
            [state],
            records: [record ?? Record(envelope)],
            activity: activity is null ? [] : [activity],
            projections: projection is null ? [] : [projection]);

    public async ValueTask DisposeAsync()
    {
        try
        {
            await ExecuteAsync("DROP SCHEMA IF EXISTS skypulse CASCADE;");
        }
        finally
        {
            await DataSource.DisposeAsync();
        }
    }
}

internal static class PostgreSqlIngestionStoreTestExtensions
{
    internal static async Task<DurableCommitResult> CommitAsync(
        this PostgreSqlIngestionStore store,
        DurableIngestionCommit commit,
        CancellationToken cancellationToken = default)
    {
        var envelope = commit.Envelope;
        var reservation = await store.ReserveDeliveryAsync(
            new DurableDeliveryReservationRequest(
                envelope.SourceInstanceId,
                envelope.TapDeliveryId,
                envelope.DeliveryDigest,
                envelope.ObservedAtMinuteUtc),
            cancellationToken);
        return await store.CommitAsync(reservation, commit, cancellationToken);
    }
}
