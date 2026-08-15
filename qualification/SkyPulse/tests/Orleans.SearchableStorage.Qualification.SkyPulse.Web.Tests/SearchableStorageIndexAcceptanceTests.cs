using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.SearchableStorage;
using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.Web;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web.Tests;

public sealed class SearchableStorageIndexAcceptanceTests
{
    [Fact]
    public async Task PackageOnlyIndexUpsertQueryHydrationAndRemoveConverge()
    {
        using var host = Host.CreateDefaultBuilder()
            .UseOrleans(siloBuilder =>
            {
                siloBuilder.UseLocalhostClustering();
                siloBuilder.AddMemoryGrainStorage(
                    SearchableStorageConstants.PhysicalStorageProviderName);
                siloBuilder.AddSearchableIndex(
                    SkyPulseIndexContract.ProviderName,
                    options =>
                    {
                        options.PartitionCount = 2;
                        options.VirtualSlotTargetCount = 64;
                        options.Query.ContinuationProtection.CurrentKey =
                            new SearchableStorageContinuationKey(
                                "acceptance-test",
                                SHA256.HashData(Encoding.UTF8.GetBytes("skypulse-acceptance-test")));
                    });
                siloBuilder.AddSearchableStorageState<AccountIndexState>(
                    SkyPulseIndexContract.ProviderName,
                    SkyPulseIndexContract.StateName,
                    SkyPulseIndexContract.ApplicationSchemaVersion);
            })
            .ConfigureServices(services =>
            {
                services.AddSingleton<InMemoryProjectionStore>();
                services.AddSingleton<IProjectionStore>(static provider =>
                    provider.GetRequiredService<InMemoryProjectionStore>());
            })
            .Build();

        await host.StartAsync();
        try
        {
            var admin = host.Services.GetRequiredKeyedService<ISearchableStorageAdminClient>(
                SkyPulseIndexContract.ProviderName);
            var indexWriter = new SearchableStorageProjectionIndexWriter(
                host.Services.GetRequiredKeyedService<ISearchableStorageIndexWriter>(
                    SkyPulseIndexContract.ProviderName));
            var rawIndexWriter = host.Services.GetRequiredKeyedService<ISearchableStorageIndexWriter>(
                SkyPulseIndexContract.ProviderName);
            var projectionStore = host.Services.GetRequiredService<IProjectionStore>();
            var pageQuery = new SearchableStorageSkyPulsePageQuery(
                host.Services.GetRequiredKeyedService<ISearchableStorageQueryClient>(
                    SkyPulseIndexContract.ProviderName),
                projectionStore);

            var schema = await admin.RebuildIndexSchemaAsync<AccountIndexState>(
                SkyPulseIndexContract.StateName,
                SkyPulseIndexContract.ApplicationSchemaVersion);
            Assert.Equal(SearchableStorageIndexSchemaState.Active, schema.State);

            var accountKey = AccountKey.FromDid("did:plc:skypulse-acceptance");
            var accountKeys = new[]
            {
                accountKey,
                AccountKey.FromDid("did:plc:skypulse-page-two"),
                AccountKey.FromDid("did:plc:skypulse-page-three"),
            };
            Array.Sort(accountKeys);
            var admission = FrozenCorpusAllowlist.FromCanonicalOrder(accountKeys)
                .CreateAdmission(new CappedCorpusProfile("acceptance", accountKeys.Length));
            var projection = CreateProjection(admission, accountKey, currentPostCount: 101);
            var otherProjections = accountKeys
                .Where(key => key != accountKey)
                .Select(key => CreateProjection(admission, key, currentPostCount: 50))
                .ToArray();

            foreach (var current in otherProjections.Prepend(projection))
            {
                await projectionStore.UpsertAsync(current);
                await indexWriter.UpsertAsync(current);
            }

            var page = await pageQuery.QueryAsync(new SkyPulseQueryRequest
            {
                PageSize = 10,
                CurrentPostCount = new LongRangeFilter { Minimum = 100 },
                CreatedRecordCount30Days = new LongRangeFilter { Minimum = 300, Maximum = 310 },
                ReceivedEngagementCreates30Days = new LongRangeFilter { Minimum = 129 },
            });

            var row = Assert.Single(page.Rows);
            Assert.Equal(accountKey.ToString(), row.GrainId);
            Assert.Equal(101, row.CurrentPostCount);
            Assert.Equal(310, row.CreatedRecordCount30Days);
            Assert.Equal(129, row.ReceivedEngagementCreates30Days);
            Assert.Null(page.ContinuationToken);

            // Durable publication intentionally prepares the new hydration row before replacing
            // the blind index entry. During that bounded interval the old posting can still be
            // returned, so hydration must re-check the requested predicate and fail closed.
            await projectionStore.UpsertAsync(
                CreateProjection(admission, accountKey, currentPostCount: 99));
            var stalePosting = await pageQuery.QueryAsync(new SkyPulseQueryRequest
            {
                PageSize = 10,
                CurrentPostCount = new LongRangeFilter { Minimum = 100 },
            });
            Assert.Empty(stalePosting.Rows);
            await projectionStore.UpsertAsync(projection);

            var allFields = await pageQuery.QueryAsync(new SkyPulseQueryRequest
            {
                PageSize = 10,
                LastActivityMinuteUtc = Exact(30_000_001),
                CreatedRecordCount1Day = Exact(30),
                CreatedRecordCount7Days = Exact(70),
                CreatedRecordCount30Days = Exact(310),
                UpdatedRecordCount1Day = Exact(20),
                UpdatedRecordCount7Days = Exact(50),
                UpdatedRecordCount30Days = Exact(170),
                DeletedRecordCount1Day = Exact(10),
                DeletedRecordCount7Days = Exact(40),
                DeletedRecordCount30Days = Exact(90),
                CurrentPostCount = Exact(101),
                CurrentFollowingCount = Exact(111),
                CurrentFollowerCount = Exact(113),
                PostCreates1Day = Exact(3),
                PostCreates7Days = Exact(7),
                PostCreates30Days = Exact(23),
                ReceivedEngagementCreates30Days = Exact(129),
            });
            Assert.Equal(accountKey.ToString(), Assert.Single(allFields.Rows).GrainId);

            var traversed = new List<string>();
            string? continuationToken = null;
            do
            {
                var currentPage = await pageQuery.QueryAsync(new SkyPulseQueryRequest
                {
                    PageSize = 1,
                    ContinuationToken = continuationToken,
                });
                traversed.AddRange(currentPage.Rows.Select(static value => value.GrainId));
                continuationToken = currentPage.ContinuationToken;
            }
            while (continuationToken is not null);

            Assert.Equal(3, traversed.Count);
            Assert.Equal(3, traversed.Distinct(StringComparer.Ordinal).Count());

            var replacement = CreateProjection(admission, accountKey, currentPostCount: 202);
            await projectionStore.UpsertAsync(replacement);
            await indexWriter.UpsertAsync(replacement);
            await indexWriter.UpsertAsync(replacement);

            var afterReplacement = await pageQuery.QueryAsync(new SkyPulseQueryRequest
            {
                PageSize = 10,
                CurrentPostCount = new LongRangeFilter { Minimum = 202, Maximum = 202 },
            });
            Assert.Equal(202, Assert.Single(afterReplacement.Rows).CurrentPostCount);

            var staleValue = await pageQuery.QueryAsync(new SkyPulseQueryRequest
            {
                PageSize = 10,
                CurrentPostCount = new LongRangeFilter { Minimum = 101, Maximum = 101 },
            });
            Assert.Empty(staleValue.Rows);

            await indexWriter.RemoveAsync(accountKey);
            await indexWriter.RemoveAsync(AccountKey.FromDid("did:plc:missing-index-entry"));

            var afterRemoval = await pageQuery.QueryAsync(new SkyPulseQueryRequest
            {
                PageSize = 10,
                CurrentPostCount = new LongRangeFilter { Minimum = 100 },
            });
            Assert.Empty(afterRemoval.Rows);

            var wrongTypeKey = AccountKey.FromDid("did:plc:wrong-grain-type");
            await rawIndexWriter.UpsertAsync(
                SkyPulseIndexContract.StateName,
                GrainId.Create("unexpected-skypulse-type", wrongTypeKey.ToString()),
                new AccountIndexState { CurrentPostCount = replacement.CurrentPostCount });

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => pageQuery.QueryAsync(new SkyPulseQueryRequest
                {
                    PageSize = 10,
                    CurrentPostCount = new LongRangeFilter { Minimum = 200 },
                }));
            Assert.Contains("grain type", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static AccountProjection CreateProjection(
        CappedCorpusAdmission admission,
        AccountKey accountKey,
        long currentPostCount)
        => admission.CreateProjection(
            accountKey,
            lastActivityMinuteUtc: 30_000_001,
            createdRecordCounts: new RollingWindowCounts(30, 70, 310),
            updatedRecordCounts: new RollingWindowCounts(20, 50, 170),
            deletedRecordCounts: new RollingWindowCounts(10, 40, 90),
            currentPostCount,
            currentFollowingCount: 111,
            currentFollowerCount: 113,
            postCreateCounts: new RollingWindowCounts(3, 7, 23),
            receivedEngagementCreates30Days: 129);

    private static LongRangeFilter Exact(long value) => new() { Minimum = value, Maximum = value };
}
