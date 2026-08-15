using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Microsoft.AspNetCore.Http;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web.Tests;

public sealed class DurableProjectionAdapterTests
{
    [Fact]
    public async Task RuntimeAdapterMapsEveryFieldAndForwardsRemoval()
    {
        var indexWriter = new CapturingIndexWriter();
        var sessions = new QuerySessionRegistry(
            new EmptyPageQuery(),
            Options.Create(new QuerySessionOptions()),
            TimeProvider.System);
        var adapter = new RuntimeProjectionIndexWriterAdapter(indexWriter, sessions);
        var snapshot = Projection(ProjectionOperation.Upsert);

        await adapter.UpsertAsync(snapshot);
        await adapter.RemoveAsync(snapshot.AccountKey);

        var projection = Assert.IsType<AccountProjection>(indexWriter.Upserted);
        Assert.Equal(snapshot.AccountKey, projection.AccountKey);
        Assert.Equal(snapshot.LastActivityMinuteUtc, projection.LastActivityMinuteUtc);
        Assert.Equal(snapshot.CreatedRecordCount1Day, projection.CreatedRecordCount1Day);
        Assert.Equal(snapshot.CreatedRecordCount7Days, projection.CreatedRecordCount7Days);
        Assert.Equal(snapshot.CreatedRecordCount30Days, projection.CreatedRecordCount30Days);
        Assert.Equal(snapshot.UpdatedRecordCount1Day, projection.UpdatedRecordCount1Day);
        Assert.Equal(snapshot.UpdatedRecordCount7Days, projection.UpdatedRecordCount7Days);
        Assert.Equal(snapshot.UpdatedRecordCount30Days, projection.UpdatedRecordCount30Days);
        Assert.Equal(snapshot.DeletedRecordCount1Day, projection.DeletedRecordCount1Day);
        Assert.Equal(snapshot.DeletedRecordCount7Days, projection.DeletedRecordCount7Days);
        Assert.Equal(snapshot.DeletedRecordCount30Days, projection.DeletedRecordCount30Days);
        Assert.Equal(snapshot.CurrentPostCount, projection.CurrentPostCount);
        Assert.Equal(snapshot.CurrentFollowingCount, projection.CurrentFollowingCount);
        Assert.Equal(snapshot.CurrentFollowerCount, projection.CurrentFollowerCount);
        Assert.Equal(snapshot.PostCreates1Day, projection.PostCreates1Day);
        Assert.Equal(snapshot.PostCreates7Days, projection.PostCreates7Days);
        Assert.Equal(snapshot.PostCreates30Days, projection.PostCreates30Days);
        Assert.Equal(
            snapshot.ReceivedEngagementCreates30Days,
            projection.ReceivedEngagementCreates30Days);
        Assert.Equal(snapshot.AccountKey, indexWriter.Removed);
    }

    [Fact]
    public async Task RuntimeAdapterRejectsRemovalSnapshotAsHydration()
    {
        var indexWriter = new CapturingIndexWriter();
        var adapter = new RuntimeProjectionIndexWriterAdapter(
            indexWriter,
            new QuerySessionRegistry(
                new EmptyPageQuery(),
                Options.Create(new QuerySessionOptions()),
                TimeProvider.System));

        await Assert.ThrowsAsync<ArgumentException>(
            () => adapter.UpsertAsync(Projection(ProjectionOperation.Remove)).AsTask());

        Assert.Null(indexWriter.Upserted);
    }

    [Fact]
    public async Task RuntimeRemovalForCurrentPageTriggersImmediateResync()
    {
        var snapshot = Projection(ProjectionOperation.Upsert);
        var row = SkyPulseQueryRow.FromProjection(DurableProjectionMapper.ToAccountProjection(snapshot));
        var sessions = new QuerySessionRegistry(
            new FixedPageQuery(new SkyPulseQueryPage([row], null)),
            Options.Create(new QuerySessionOptions()),
            TimeProvider.System);
        var created = await sessions.CreateAsync(new SkyPulseQueryRequest());
        Assert.True(sessions.TryGet(created.SessionId, out var session));
        var adapter = new RuntimeProjectionIndexWriterAdapter(new CapturingIndexWriter(), sessions);

        await adapter.RemoveAsync(snapshot.AccountKey);

        await using var events = session.ReadEventsAsync().GetAsyncEnumerator();
        Assert.True(await events.MoveNextAsync());
        Assert.Equal(QuerySessionEventKind.ResyncRequired, events.Current.Kind);
    }

    [Fact]
    public void DurableConfigurationBindsExactRuntimeIdentity()
    {
        var configuration = BuildConfiguration(includeConnectionString: true);

        var resolved = SkyPulseApplicationConfiguration.Resolve(configuration);
        var manifest = resolved.Durable!.CreateManifest(new string('A', 64));

        Assert.Equal(SkyPulseRuntimeMode.Durable, resolved.Mode);
        Assert.NotNull(resolved.PostgreSqlConnectionString);
        Assert.Equal("skypulse-test-v1", manifest.Profile.ProfileId);
        Assert.Equal(1_000_000, manifest.Profile.CorpusCap);
        Assert.Equal(SkyPulseIndexContract.ProviderName, manifest.Index.IndexNamespace);
        Assert.Equal(new string('a', 64), manifest.Index.SchemaFingerprint);
        Assert.Equal("Orleans.SearchableStorage", manifest.Package.PackageId);
        Assert.Equal("1.0.0-rc.2", manifest.Package.PackageVersion);
        Assert.Equal(
            "d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c",
            manifest.Package.NupkgSha256);
        var recalculation = resolved.Durable.CreateRecalculationOptions();
        Assert.Equal(64, recalculation.BatchSize);
        Assert.Equal(TimeSpan.FromMinutes(5), recalculation.LeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(5), recalculation.FailureDelay);
        var provisioner = resolved.Durable.CreateTapRepositoryProvisionerOptions();
        Assert.Equal("/var/lib/skypulse/routing.private.manifest.json", provisioner.RoutingManifestPath);
        Assert.Equal(1, provisioner.ExpectedProfileVersion);
        Assert.True(provisioner.ExclusiveRepositoryAdministrationConfirmed);
        Assert.True(provisioner.FullNetworkModeDisabledConfirmed);
        Assert.True(provisioner.AutomaticRepositoryDiscoveryDisabledConfirmed);
    }

    [Fact]
    public void DurableModeFailsClosedWithoutPostgreSqlConfiguration()
    {
        var configuration = BuildConfiguration(includeConnectionString: false);

        var exception = Assert.Throws<InvalidOperationException>(
            () => SkyPulseApplicationConfiguration.Resolve(configuration));

        Assert.Contains("SkyPulsePostgreSql", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DurableModeRequiresAllExactTapTopologyConfirmations()
    {
        var configuration = BuildConfiguration(
            includeConnectionString: true,
            topologyConfirmed: false);

        var exception = Assert.Throws<InvalidOperationException>(
            () => SkyPulseApplicationConfiguration.Resolve(configuration));

        Assert.Contains("exclusive administration", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LocalFunctionalModeDoesNotInventDurableConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SkyPulse:Mode"] = "LocalFunctional",
            })
            .Build();

        var resolved = SkyPulseApplicationConfiguration.Resolve(configuration);

        Assert.Equal(SkyPulseRuntimeMode.LocalFunctional, resolved.Mode);
        Assert.Null(resolved.PostgreSqlConnectionString);
        Assert.Null(resolved.Durable);
    }

    [Fact]
    public void DurableModeRejectsUnboundedRecalculationBatch()
    {
        var values = new Dictionary<string, string?>
        {
            ["SkyPulse:Mode"] = "Durable",
            ["ConnectionStrings:SkyPulsePostgreSql"] =
                "Host=postgres;Database=skypulse;Username=skypulse;Password=test",
            ["SkyPulse:Durable:ProfileId"] = "skypulse-test-v1",
            ["SkyPulse:Durable:ProfileVersion"] = "1",
            ["SkyPulse:Durable:CorpusCap"] = "1000000",
            ["SkyPulse:Durable:ProfilePrefixSha256"] = new string('b', 64),
            ["SkyPulse:Durable:SourceInstanceId"] = "f90a18b7-3cf4-4f99-a603-c896928998a7",
            ["SkyPulse:Durable:CorpusManifestPath"] = "/var/lib/skypulse/corpus.manifest.json",
            ["SkyPulse:Durable:TapEndpoint"] = "ws://127.0.0.1:2480/channel",
            ["SkyPulse:Durable:TapAdminPassword"] = "test-secret",
            ["SkyPulse:Durable:RoutingManifestPath"] = "/var/lib/skypulse/routing.private.manifest.json",
            ["SkyPulse:Durable:ExclusiveRepositoryAdministrationConfirmed"] = "true",
            ["SkyPulse:Durable:FullNetworkModeDisabledConfirmed"] = "true",
            ["SkyPulse:Durable:AutomaticRepositoryDiscoveryDisabledConfirmed"] = "true",
            ["SkyPulse:Durable:RecalculationBatchSize"] = "1001",
        };
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => SkyPulseApplicationConfiguration.Resolve(configuration));
    }

    [Fact]
    public void DurableConfigurationBuildsStrictMonotonicGrowthCatalog()
    {
        var resolved = SkyPulseApplicationConfiguration.Resolve(
            BuildConfiguration(includeConnectionString: true, includeGrowthProfile: true));

        var profiles = resolved.Durable!.CreateCorpusProfileCatalog();

        Assert.Equal(2, profiles.Count);
        Assert.Equal(1_000_000, profiles[0].Capacity.CorpusCap);
        Assert.Equal("skypulse-test-2m", profiles[1].Capacity.ProfileId);
        Assert.Equal(2_000_000, profiles[1].Capacity.CorpusCap);
        Assert.Equal(
            "/var/lib/skypulse/2m/routing.private.manifest.json",
            profiles[1].RoutingManifestPath);
    }

    [Fact]
    public void DurableGrowthProfilesRequireASecretAdminToken()
    {
        var values = BuildConfigurationValues(includeConnectionString: true, includeGrowthProfile: true);
        values["SkyPulse:Durable:CorpusGrowthAdminToken"] = "too-short";
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            SkyPulseApplicationConfiguration.Resolve(configuration));

        Assert.Contains("admin token", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CorpusGrowthAuthorizationRequiresOneExactHeaderValue()
    {
        var token = new string('s', 32);
        var context = new DefaultHttpContext();

        Assert.False(CorpusGrowthAuthorization.IsAuthorized(context.Request, token));
        context.Request.Headers[CorpusGrowthAuthorization.HeaderName] = token;
        Assert.True(CorpusGrowthAuthorization.IsAuthorized(context.Request, token));
        context.Request.Headers[CorpusGrowthAuthorization.HeaderName] = "wrong";
        Assert.False(CorpusGrowthAuthorization.IsAuthorized(context.Request, token));
        context.Request.Headers[CorpusGrowthAuthorization.HeaderName] =
            new Microsoft.Extensions.Primitives.StringValues([token, token]);
        Assert.False(CorpusGrowthAuthorization.IsAuthorized(context.Request, token));
    }

    private static IConfiguration BuildConfiguration(
        bool includeConnectionString,
        bool topologyConfirmed = true,
        bool includeGrowthProfile = false)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(BuildConfigurationValues(
                includeConnectionString,
                topologyConfirmed,
                includeGrowthProfile))
            .Build();

    private static Dictionary<string, string?> BuildConfigurationValues(
        bool includeConnectionString,
        bool topologyConfirmed = true,
        bool includeGrowthProfile = false)
    {
        var values = new Dictionary<string, string?>
        {
            ["SkyPulse:Mode"] = "Durable",
            ["SkyPulse:Durable:ProfileId"] = "skypulse-test-v1",
            ["SkyPulse:Durable:ProfileVersion"] = "1",
            ["SkyPulse:Durable:CorpusCap"] = "1000000",
            ["SkyPulse:Durable:ProfilePrefixSha256"] = new string('b', 64),
            ["SkyPulse:Durable:SourceInstanceId"] = "f90a18b7-3cf4-4f99-a603-c896928998a7",
            ["SkyPulse:Durable:CorpusManifestPath"] = "/var/lib/skypulse/corpus.manifest.json",
            ["SkyPulse:Durable:TapEndpoint"] = "ws://127.0.0.1:2480/channel",
            ["SkyPulse:Durable:TapAdminPassword"] = "test-secret",
            ["SkyPulse:Durable:RoutingManifestPath"] = "/var/lib/skypulse/routing.private.manifest.json",
            ["SkyPulse:Durable:ExclusiveRepositoryAdministrationConfirmed"] = topologyConfirmed.ToString(),
            ["SkyPulse:Durable:FullNetworkModeDisabledConfirmed"] = topologyConfirmed.ToString(),
            ["SkyPulse:Durable:AutomaticRepositoryDiscoveryDisabledConfirmed"] = topologyConfirmed.ToString(),
        };
        if (includeConnectionString)
        {
            values["ConnectionStrings:SkyPulsePostgreSql"] =
                "Host=postgres;Database=skypulse;Username=skypulse;Password=test";
        }

        if (includeGrowthProfile)
        {
            values["SkyPulse:Durable:CorpusGrowthAdminToken"] = new string('s', 32);
            values["SkyPulse:Durable:GrowthProfiles:0:ProfileId"] = "skypulse-test-2m";
            values["SkyPulse:Durable:GrowthProfiles:0:CorpusCap"] = "2000000";
            values["SkyPulse:Durable:GrowthProfiles:0:ProfilePrefixSha256"] = new string('c', 64);
            values["SkyPulse:Durable:GrowthProfiles:0:RoutingManifestPath"] =
                "/var/lib/skypulse/2m/routing.private.manifest.json";
        }

        return values;
    }

    private static ProjectionSnapshot Projection(ProjectionOperation operation)
        => new(
            AccountKey.FromDid("did:plc:durable-web-adapter"),
            version: 9,
            operation,
            isComplete: true,
            projectionCutMinuteUtc: 10_000,
            nextRecalculationMinuteUtc: operation == ProjectionOperation.Upsert ? 10_001 : null,
            lastActivityMinuteUtc: 9_999,
            createdRecordCount1Day: 11,
            createdRecordCount7Days: 17,
            createdRecordCount30Days: 31,
            updatedRecordCount1Day: 12,
            updatedRecordCount7Days: 18,
            updatedRecordCount30Days: 32,
            deletedRecordCount1Day: 13,
            deletedRecordCount7Days: 19,
            deletedRecordCount30Days: 33,
            currentPostCount: 41,
            currentFollowingCount: 42,
            currentFollowerCount: 43,
            postCreates1Day: 5,
            postCreates7Days: 7,
            postCreates30Days: 9,
            receivedEngagementCreates30Days: 51);

    private sealed class CapturingIndexWriter : IProjectionIndexWriter
    {
        public AccountProjection? Upserted { get; private set; }

        public AccountKey? Removed { get; private set; }

        public ValueTask UpsertAsync(
            AccountProjection projection,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Upserted = projection;
            return ValueTask.CompletedTask;
        }

        public ValueTask RemoveAsync(
            AccountKey accountKey,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Removed = accountKey;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class EmptyPageQuery : ISkyPulsePageQuery
    {
        public Task<SkyPulseQueryPage> QueryAsync(
            SkyPulseQueryRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(new SkyPulseQueryPage([], null));
        }
    }

    private sealed class FixedPageQuery(SkyPulseQueryPage page) : ISkyPulsePageQuery
    {
        public Task<SkyPulseQueryPage> QueryAsync(
            SkyPulseQueryRequest request,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(page);
        }
    }
}
