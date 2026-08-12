using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class CollectionMembershipAcceptanceTests
{
    private readonly MemoryStorageFixture _fixture;

    public CollectionMembershipAcceptanceTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task LiveWritesMaintainArrayAndListMembershipAcrossUpdateAndClear()
    {
        var stateName = CreateStateName("live");
        var sharedTag = $"shared-{Guid.NewGuid():N}";
        var replacementTag = $"replacement-{Guid.NewGuid():N}";
        var firstCity = $"Tel-Aviv-{Guid.NewGuid():N}";
        var secondCity = $"Haifa-{Guid.NewGuid():N}";
        var outsideCity = $"Eilat-{Guid.NewGuid():N}";
        var first = CreateRecord(
            tags: [sharedTag, null, "", sharedTag],
            audienceIds: [7, null, 7],
            city: firstCity,
            salary: 10);
        var second = CreateRecord(
            tags: [sharedTag],
            audienceIds: [8],
            city: secondCity,
            salary: 20);
        var outside = CreateRecord(
            tags: [$"outside-{Guid.NewGuid():N}"],
            audienceIds: [7],
            city: outsideCity,
            salary: 30);
        var storage = GetLegacyStorage();
        var client = CreateLegacyClient();

        try
        {
            await WriteAsync(storage, stateName, first, second, outside);

            var shared = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(state => Enumerable.Contains(state.Tags!, sharedTag))
                .ToGrainIdsAsync();
            var audience = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(state => state.AudienceIds!.Contains(7))
                .ToGrainIdsAsync();
            var intersection = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(state => Enumerable.Contains(state.Tags!, sharedTag)
                    && state.AudienceIds!.Contains(7))
                .ToGrainIdsAsync();
            var emptyString = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(state => Enumerable.Contains(state.Tags!, string.Empty))
                .ToGrainIdsAsync();
            var cityCounts = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(state => Enumerable.Contains(state.Tags!, sharedTag))
                .ToFacetValueCountsAsync(
                    state => state.City,
                    new SearchableStorageFacetRequest(4, SearchableStorageFacetAccuracy.Exact));
            var salaryBounds = await client
                .Query<CollectionMembershipState>(stateName)
                .Where(state => Enumerable.Contains(state.Tags!, sharedTag))
                .ToFacetMinMaxAsync(state => state.Salary);

            shared.Should().BeEquivalentTo([first.GrainId, second.GrainId]);
            shared.Should().OnlyHaveUniqueItems(
                "duplicate collection values must not duplicate a posting");
            audience.Should().BeEquivalentTo([first.GrainId, outside.GrainId]);
            intersection.Should().ContainSingle().Which.Should().Be(first.GrainId);
            emptyString.Should().ContainSingle().Which.Should().Be(first.GrainId);
            cityCounts.IsExact.Should().BeTrue();
            cityCounts.Items.Select(static item => (item.Value, item.Count)).Should()
                .BeEquivalentTo([(firstCity, 1L), (secondCity, 1L)]);
            salaryBounds.Should().NotBeNull();
            salaryBounds!.Minimum.Should().Be(10);
            salaryBounds.Maximum.Should().Be(20);

            first.State.Tags = [replacementTag, null, replacementTag];
            first.State.AudienceIds = [8, null, 8];
            first.State.City = $"updated-{Guid.NewGuid():N}";
            first.State.Salary = 40;
            await storage.WriteStateAsync(stateName, first.GrainId, first.StateContainer);

            (await client
                    .Query<CollectionMembershipState>(stateName)
                    .Where(state => Enumerable.Contains(state.Tags!, sharedTag))
                    .ToGrainIdsAsync())
                .Should().ContainSingle().Which.Should().Be(second.GrainId);
            (await client
                    .Query<CollectionMembershipState>(stateName)
                    .Where(state => state.AudienceIds!.Contains(7))
                    .ToGrainIdsAsync())
                .Should().ContainSingle().Which.Should().Be(outside.GrainId);
            (await client
                    .Query<CollectionMembershipState>(stateName)
                    .Where(state => Enumerable.Contains(state.Tags!, replacementTag))
                    .ToGrainIdsAsync())
                .Should().ContainSingle().Which.Should().Be(first.GrainId);

            await storage.ClearStateAsync(stateName, first.GrainId, first.StateContainer);

            (await client
                    .Query<CollectionMembershipState>(stateName)
                    .Where(state => Enumerable.Contains(state.Tags!, replacementTag))
                    .ToGrainIdsAsync())
                .Should().BeEmpty();
            first.StateContainer.RecordExists.Should().BeFalse();
            first.StateContainer.ETag.Should().BeNull();
        }
        finally
        {
            await ClearIfPresentAsync(storage, stateName, first, second, outside);
        }
    }

    [Fact]
    public async Task MembershipQueryPagesAndCancellationUseThePublicBoundedPath()
    {
        var stateName = CreateStateName("paging");
        var tag = $"paged-{Guid.NewGuid():N}";
        var first = CreateRecord([tag], [1], "Ashdod", 1);
        var second = CreateRecord([tag], [2], "Beer-Sheva", 2);
        var storage = GetLegacyStorage();
        var client = CreateLegacyClient();

        try
        {
            await WriteAsync(storage, stateName, first, second);
            var query = client
                .Query<CollectionMembershipState>(stateName)
                .Where(state => Enumerable.Contains(state.Tags!, tag));
            var found = new List<GrainId>();
            string? continuation = null;
            var pageCount = 0;
            do
            {
                var page = await query.ToGrainIdPageAsync(
                    new SearchableStorageQueryPageRequest(1, continuation));
                page.Items.Should().HaveCountLessThanOrEqualTo(1);
                found.AddRange(page.Items);
                continuation = page.ContinuationToken;
                pageCount++;
            }
            while (continuation is not null);

            found.Should().Equal(new[] { first.GrainId, second.GrainId }.Order());
            pageCount.Should().BeGreaterThanOrEqualTo(2);
            using var canceled = new CancellationTokenSource();
            await canceled.CancelAsync();
            Func<Task> executeCanceled = async () => await query.ToGrainIdsAsync(canceled.Token);
            await executeCanceled.Should().ThrowExactlyAsync<OperationCanceledException>();
        }
        finally
        {
            await ClearIfPresentAsync(storage, stateName, first, second);
        }
    }

    [Fact]
    public async Task WhereInSnapshotsCanonicalScalarValuesAndEnforcesItsRawBound()
    {
        var stateName = CreateStateName("where-in");
        var firstCity = $"Acre-{Guid.NewGuid():N}";
        var secondCity = $"Tiberias-{Guid.NewGuid():N}";
        var outsideCity = $"Dimona-{Guid.NewGuid():N}";
        var first = CreateRecord([], [], firstCity, 1);
        var second = CreateRecord([], [], secondCity, 2);
        var outside = CreateRecord([], [], outsideCity, 3);
        var storage = GetLegacyStorage();
        var client = CreateLegacyClient();

        try
        {
            await WriteAsync(storage, stateName, first, second, outside);
            var mutableValues = new List<string> { secondCity, firstCity, firstCity };
            var snapshotted = client
                .Query<CollectionMembershipState>(stateName)
                .WhereIn(state => state.City, mutableValues);
            mutableValues.Clear();
            mutableValues.Add(outsideCity);

            var matches = await snapshotted.ToGrainIdsAsync();
            var canonicalOrder = client
                .Query<CollectionMembershipState>(stateName)
                .WhereIn(state => state.City, new[] { firstCity, secondCity });
            var canonicalMatches = await canonicalOrder.ToGrainIdsAsync();
            var firstSnapshotPage = await snapshotted.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(1));
            var firstCanonicalPage = await canonicalOrder.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(1));
            var crossResumed = firstSnapshotPage.Items.ToList();
            var crossContinuation = firstSnapshotPage.ContinuationToken;
            while (crossContinuation is not null)
            {
                var page = await canonicalOrder.ToGrainIdPageAsync(
                    new SearchableStorageQueryPageRequest(1, crossContinuation));
                crossResumed.AddRange(page.Items);
                crossContinuation = page.ContinuationToken;
            }
            var empty = await client
                .Query<CollectionMembershipState>(stateName)
                .WhereIn(state => state.City, Array.Empty<string>())
                .ToGrainIdsAsync();

            matches.Should().Equal(new[] { first.GrainId, second.GrainId }.Order());
            canonicalMatches.Should().Equal(matches);
            firstSnapshotPage.Items.Should().Equal(firstCanonicalPage.Items);
            firstSnapshotPage.ContinuationToken.Should().NotBeNull();
            firstCanonicalPage.ContinuationToken.Should().NotBeNull();
            crossResumed.Should().Equal(matches,
                "the duplicate/order variant's continuation must bind to the canonical plan");
            empty.Should().BeEmpty();

            var oversizedValues = Enumerable.Range(
                    0,
                    SearchableStorageQueryLimits.MaximumWhereInValues + 1)
                .Select(static value => $"city-{value:D3}")
                .ToArray();
            Action createOversized = () => _ = client
                .Query<CollectionMembershipState>(stateName)
                .WhereIn(state => state.City, oversizedValues);
            var oversized = createOversized.Should().ThrowExactly<ArgumentOutOfRangeException>();
            oversized.Which.ActualValue.Should()
                .Be(SearchableStorageQueryLimits.MaximumWhereInValues + 1);
        }
        finally
        {
            await ClearIfPresentAsync(storage, stateName, first, second, outside);
        }
    }

    [Fact]
    public async Task CollectionSelectorsAreRejectedByScalarFindRangeAndFacetTerminals()
    {
        var stateName = CreateStateName("selector-rejection");
        var client = CreateLegacyClient();
        var query = client.Query<CollectionMembershipState>(stateName);
        string?[] tag = ["tag"];
        string?[] lower = ["a"];
        string?[] upper = ["z"];
        Func<Task> find = async () => await client.FindAsync<CollectionMembershipState, string?[]?>(
            stateName,
            state => state.Tags,
            tag);
        Func<Task> range = async () => await client.RangeAsync<CollectionMembershipState, string?[]?>(
            stateName,
            state => state.Tags,
            lower,
            upper);
        Func<Task> distinct = async () => await query.ToDistinctFacetValuePageAsync(
            state => state.Tags,
            new SearchableStorageFacetPageRequest(1));
        Func<Task> counts = async () => await query.ToFacetValueCountsAsync(
            state => state.Tags,
            new SearchableStorageFacetRequest(1, SearchableStorageFacetAccuracy.Exact));
        Func<Task> minMax = async () => await query.ToFacetMinMaxAsync(state => state.Tags);

        foreach (var action in new[] { find, range, distinct, counts, minMax })
        {
            await action.Should().ThrowExactlyAsync<ArgumentException>()
                .WithMessage("*scalar index*");
        }
    }

    [Fact]
    public async Task BoundedSchemaRebuildResumesAndReextractsCollectionMembership()
    {
        const int recordCount = 70;
        var services = GetPrimaryServices();
        var options = services.GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(MemoryStorageFixture.CollectionMembershipSchemaProviderName);
        var admin = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(
            MemoryStorageFixture.CollectionMembershipSchemaProviderName);
        var query = services.GetRequiredKeyedService<ISearchableStorageQueryClient>(
            MemoryStorageFixture.CollectionMembershipSchemaProviderName);
        var commonTag = $"schema-common-{Guid.NewGuid():N}";
        var seeded = await SeedLegacySchemaRecordsAsync(options, commonTag, recordCount);
        seeded.Select(static record => record.Owner).Distinct().Should().ContainSingle();
        var definition = IndexMetadataProvider.GetSchemaDefinition<CollectionMembershipState>(
            MemoryStorageFixture.CollectionMembershipSchemaStateName);
        var request = StorageIndexSchema.CreateRequest(
            MemoryStorageFixture.CollectionMembershipSchemaProviderName,
            definition);
        var control = _fixture.Cluster.GrainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(
                MemoryStorageFixture.CollectionMembershipSchemaProviderName,
                MemoryStorageFixture.CollectionMembershipSchemaStateName));
        var snapshot = await control.BeginRebuildAsync(request);
        var rebuildId = snapshot.Rebuild!.RebuildId;

        while (snapshot.Rebuild is { ProcessedRecordCount: 0 })
        {
            snapshot = await control.AdvanceRebuildAsync(new StorageIndexSchemaCommand
            {
                Schema = request,
                RebuildId = rebuildId,
            });
        }

        snapshot.Rebuild.Should().NotBeNull();
        snapshot.Rebuild!.ProcessedRecordCount.Should().Be(StorageIndexSchema.RebuildPageSize);
        snapshot.Rebuild.HasAfter.Should().BeTrue();
        snapshot.Rebuild.NextOwnerIndex.Should().Be(0);
        var partition = seeded[0].Partition;
        await _fixture.Cluster.DeactivateAsync(partition);
        await _fixture.Cluster.DeactivateAsync(control);

        var resumed = await admin.GetIndexSchemaAsync<CollectionMembershipState>(
            MemoryStorageFixture.CollectionMembershipSchemaStateName);
        resumed.State.Should().Be(SearchableStorageIndexSchemaState.Rebuilding);
        resumed.RebuildId.Should().Be(rebuildId);
        resumed.ProcessedRecordCount.Should().Be(StorageIndexSchema.RebuildPageSize);
        Func<Task> queryWhileRebuilding = async () => await query
            .Query<CollectionMembershipState>(MemoryStorageFixture.CollectionMembershipSchemaStateName)
            .Where(state => Enumerable.Contains(state.Tags!, commonTag))
            .ToGrainIdsAsync();
        await queryWhileRebuilding.Should().ThrowAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*still running*");

        var completed = await admin.RebuildIndexSchemaAsync<CollectionMembershipState>(
            MemoryStorageFixture.CollectionMembershipSchemaStateName);

        completed.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        completed.ProcessedRecordCount.Should().Be(recordCount);
        var byArray = await query
            .Query<CollectionMembershipState>(MemoryStorageFixture.CollectionMembershipSchemaStateName)
            .Where(state => Enumerable.Contains(state.Tags!, commonTag))
            .ToGrainIdsAsync();
        var byList = await query
            .Query<CollectionMembershipState>(MemoryStorageFixture.CollectionMembershipSchemaStateName)
            .Where(state => state.AudienceIds!.Contains(42))
            .ToGrainIdsAsync();
        byArray.Should().BeEquivalentTo(seeded.Select(static record => record.GrainId));
        byList.Should().BeEquivalentTo(seeded.Select(static record => record.GrainId));
    }

    private async Task<IReadOnlyList<SeededSchemaRecord>> SeedLegacySchemaRecordsAsync(
        SearchableStorageOptions options,
        string commonTag,
        int recordCount)
    {
        var layout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(MemoryStorageFixture.CollectionMembershipSchemaProviderName)
            .InitializeRoutingAsync(StorageLayout.CreateDescriptor(
                MemoryStorageFixture.CollectionMembershipSchemaProviderName,
                options.PartitionCount,
                options.JournalSegmentCapacity,
                options.MaximumJournalReplayEntries,
                options.VirtualSlotTargetCount));
        var records = new List<SeededSchemaRecord>(recordCount);
        for (var index = 0; index < recordCount; index++)
        {
            var grainId = GrainId.Create(
                "collection-schema",
                $"{index:D3}-{Guid.NewGuid():N}");
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            var owner = layout.GetOwner(slot);
            var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(
                    MemoryStorageFixture.CollectionMembershipSchemaProviderName,
                    owner));
            var state = new CollectionMembershipState
            {
                Tags = [commonTag, $"schema-tag-{index:D3}", null, commonTag],
                AudienceIds = [42, index, null, 42],
                City = $"schema-city-{index:D3}",
                Salary = index,
            };
            await partition.WriteRoutedAsync(new RoutedStorageWriteRequest
            {
                Slot = slot,
                Epoch = layout.Epoch,
                Request = new StorageWriteRequest
                {
                    RecordKey = CreateRecordKey(
                        MemoryStorageFixture.CollectionMembershipSchemaStateName,
                        grainId),
                    GrainId = grainId,
                    Payload = options.GrainStorageSerializer!.Serialize(state).ToArray(),
                    IndexEntries =
                    [
                        .. IndexMetadataProvider.Extract(
                            MemoryStorageFixture.CollectionMembershipSchemaStateName,
                            state),
                    ],
                    Persistence = CreatePersistence(options),
                },
            });
            records.Add(new SeededSchemaRecord(grainId, owner, partition));
        }

        return records;
    }

    private IGrainStorage GetLegacyStorage()
    {
        return GetPrimaryServices().GetRequiredKeyedService<IGrainStorage>(
            TestGrains.VacancyGrain.StorageProviderName);
    }

    private SearchableStorageClient CreateLegacyClient()
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "collection-membership-tests",
            Enumerable.Range(1, 32).Select(static value => checked((byte)value)).ToArray());
        return new SearchableStorageClient(
            _fixture.Cluster.GrainFactory,
            TestGrains.VacancyGrain.StorageProviderName,
            _fixture.PartitionCount,
            options);
    }

    private IServiceProvider GetPrimaryServices()
    {
        return Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary).ServiceProvider;
    }

    private static CollectionRecord CreateRecord(
        string?[]? tags,
        List<int?>? audienceIds,
        string city,
        int salary)
    {
        return new CollectionRecord(
            GrainId.Create("collection-membership", Guid.NewGuid().ToString("N")),
            new GrainState<CollectionMembershipState>
            {
                State = new CollectionMembershipState
                {
                    Tags = tags,
                    AudienceIds = audienceIds,
                    City = city,
                    Salary = salary,
                },
            });
    }

    private static async Task WriteAsync(
        IGrainStorage storage,
        string stateName,
        params CollectionRecord[] records)
    {
        foreach (var record in records)
        {
            await storage.WriteStateAsync(stateName, record.GrainId, record.StateContainer);
        }
    }

    private static async Task ClearIfPresentAsync(
        IGrainStorage storage,
        string stateName,
        params CollectionRecord[] records)
    {
        foreach (var record in records.Where(static record => record.StateContainer.RecordExists))
        {
            await storage.ClearStateAsync(stateName, record.GrainId, record.StateContainer);
        }
    }

    private static StoragePersistenceSettings CreatePersistence(SearchableStorageOptions options)
    {
        return new StoragePersistenceSettings
        {
            JournalSegmentCapacity = options.JournalSegmentCapacity,
            MaximumJournalReplayEntries = options.MaximumJournalReplayEntries,
            CompactionThreshold = options.CompactionThreshold,
        };
    }

    private static string CreateStateName(string scenario)
    {
        return $"collection-{scenario}-{Guid.NewGuid():N}";
    }

    private static string CreateRecordKey(string stateName, GrainId grainId)
    {
        return string.Concat(
            stateName,
            "/",
            Convert.ToHexString(grainId.Type.AsSpan()),
            "/",
            Convert.ToHexString(grainId.Key.AsSpan()));
    }

    private sealed record CollectionRecord(
        GrainId GrainId,
        GrainState<CollectionMembershipState> StateContainer)
    {
        public CollectionMembershipState State => StateContainer.State;
    }

    private sealed record SeededSchemaRecord(
        GrainId GrainId,
        int Owner,
        IStoragePartitionGrain Partition);
}
