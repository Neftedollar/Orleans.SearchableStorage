using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

public abstract class SearchableStorageContractTests<TFixture> : IClassFixture<TFixture>
    where TFixture : class, ISearchableStorageFixture
{
    protected SearchableStorageContractTests(TFixture fixture)
    {
        Fixture = fixture;
    }

    protected TFixture Fixture { get; }

    [Fact]
    public async Task StateAndIndexesSurvivePartitionReactivation()
    {
        var city = $"reactivation-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        var grainId = grain.GetGrainId();
        var partition = GetPartition(grainId);

        await grain.SetAsync(city, 7);
        await Fixture.Cluster.DeactivateAsync(grain);
        await Fixture.Cluster.DeactivateAsync(partition);

        var state = await grain.GetAsync();
        var client = CreateClient();
        var hashResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            city);
        var rangeResults = await client.RangeAsync<VacancyState, int>(
            VacancyGrain.StateName,
            candidate => candidate.Salary,
            7,
            7);

        state.City.Should().Be(city);
        state.Salary.Should().Be(7);
        hashResults.Should().ContainSingle().Which.Should().Be(grainId);
        rangeResults.Should().ContainSingle().Which.Should().Be(grainId);

        await grain.ClearAsync();
    }

    [Fact]
    public async Task HashQueryReturnsMatchingGrainIds()
    {
        var city = $"city-{Guid.NewGuid():N}";
        var first = CreateGrain();
        var second = CreateGrain();
        var different = CreateGrain();

        await first.SetAsync(city, 10);
        await second.SetAsync(city, 20);
        await different.SetAsync($"different-{Guid.NewGuid():N}", 30);

        var results = await CreateClient().FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            city);

        results.Should().BeEquivalentTo([first.GetGrainId(), second.GetGrainId()]);

        await ClearAsync(first, second, different);
    }

    [Fact]
    public async Task HashQueryMergesDifferentPartitionsInStableOrder()
    {
        var city = $"partitioned-{Guid.NewGuid():N}";
        var first = CreateGrainInPartition(0);
        var second = CreateGrainInPartition(1);

        await first.SetAsync(city, 10);
        await second.SetAsync(city, 20);

        var results = await CreateClient().FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            city);
        var expected = new[] { first.GetGrainId(), second.GetGrainId() }
            .Order()
            .ToArray();

        GetPartitionIndex(first.GetGrainId()).Should().NotBe(GetPartitionIndex(second.GetGrainId()));
        results.Should().Equal(expected);

        await ClearAsync(first, second);
    }

    [Fact]
    public async Task RangeQueryHonorsExclusiveBounds()
    {
        var offset = Random.Shared.Next(10_000, 1_000_000);
        var below = CreateGrain();
        var firstMatch = CreateGrain();
        var secondMatch = CreateGrain();
        var above = CreateGrain();

        await below.SetAsync("below", offset + 5);
        await firstMatch.SetAsync("first", offset + 6);
        await secondMatch.SetAsync("second", offset + 7);
        await above.SetAsync("above", offset + 8);

        var results = await CreateClient().RangeAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            offset + 5,
            offset + 8,
            includeLowerBound: false,
            includeUpperBound: false);

        results.Should().BeEquivalentTo([firstMatch.GetGrainId(), secondMatch.GetGrainId()]);

        await ClearAsync(below, firstMatch, secondMatch, above);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task RangeQueryHonorsEveryBoundaryCombination(
        bool includeLowerBound,
        bool includeUpperBound)
    {
        var offset = Random.Shared.Next(1_000_000, 2_000_000);
        var lower = CreateGrain();
        var middle = CreateGrain();
        var upper = CreateGrain();

        await lower.SetAsync("lower", offset + 10);
        await middle.SetAsync("middle", offset + 15);
        await upper.SetAsync("upper", offset + 20);

        var results = await CreateClient().RangeAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            offset + 10,
            offset + 20,
            includeLowerBound,
            includeUpperBound);
        var expected = new List<GrainId> { middle.GetGrainId() };
        if (includeLowerBound)
        {
            expected.Add(lower.GetGrainId());
        }

        if (includeUpperBound)
        {
            expected.Add(upper.GetGrainId());
        }

        results.Should().BeEquivalentTo(expected);

        await ClearAsync(lower, middle, upper);
    }

    [Fact]
    public async Task ReversedRangeBoundsAreRejected()
    {
        Func<Task> query = () => CreateClient().RangeAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            20,
            10);

        await query.Should().ThrowAsync<ArgumentException>()
            .WithParameterName("lowerBound");
    }

    [Fact]
    public async Task HashIndexesCannotBeUsedForRangeQueries()
    {
        Func<Task> query = () => CreateClient().RangeAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "A",
            "Z");

        await query.Should().ThrowAsync<ArgumentException>()
            .WithParameterName("propertySelector");
    }

    [Fact]
    public async Task UpdatingStateMovesEntriesBetweenIndexBuckets()
    {
        var oldCity = $"old-{Guid.NewGuid():N}";
        var newCity = $"new-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        var client = CreateClient();

        await grain.SetAsync(oldCity, 10);
        await grain.SetAsync(newCity, 20);

        var oldResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            oldCity);
        var newResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            newCity);
        var oldSalaryResults = await client.FindAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            10);
        var newSalaryResults = await client.FindAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            20);

        oldResults.Should().BeEmpty();
        newResults.Should().ContainSingle().Which.Should().Be(grain.GetGrainId());
        oldSalaryResults.Should().BeEmpty();
        newSalaryResults.Should().ContainSingle().Which.Should().Be(grain.GetGrainId());

        await grain.ClearAsync();
    }

    [Fact]
    public async Task ClearingStateRemovesAllIndexEntries()
    {
        var city = $"clear-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        var grainId = grain.GetGrainId();
        var client = CreateClient();

        await grain.SetAsync(city, 42);
        await grain.ClearAsync();
        await Fixture.Cluster.DeactivateAsync(grain);
        await Fixture.Cluster.DeactivateAsync(GetPartition(grainId));

        var state = await grain.GetAsync();
        var hashResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            city);
        var rangeResults = await client.RangeAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            42,
            42);

        state.City.Should().BeEmpty();
        state.Salary.Should().Be(0);
        hashResults.Should().BeEmpty();
        rangeResults.Should().BeEmpty();
    }

    [Fact]
    public async Task GrainStorageBridgeMaintainsRecordMetadataAndRejectsStaleETag()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            VacancyGrain.StorageProviderName);
        var grainId = CreateGrain().GetGrainId();
        var current = new GrainState<VacancyState>
        {
            State = new VacancyState { City = "current", Salary = 10 },
        };
        var stale = new GrainState<VacancyState>
        {
            State = new VacancyState { City = "stale", Salary = 20 },
        };

        await storage.WriteStateAsync(VacancyGrain.StateName, grainId, current);

        current.RecordExists.Should().BeTrue();
        current.ETag.Should().NotBeNull();
        Func<Task> staleWrite = () => storage.WriteStateAsync(VacancyGrain.StateName, grainId, stale);
        await staleWrite.Should().ThrowAsync<InconsistentStateException>();

        var loaded = new GrainState<VacancyState>();
        await storage.ReadStateAsync(VacancyGrain.StateName, grainId, loaded);

        loaded.RecordExists.Should().BeTrue();
        loaded.ETag.Should().Be(current.ETag);
        loaded.State.City.Should().Be("current");

        await storage.ClearStateAsync(VacancyGrain.StateName, grainId, loaded);

        loaded.RecordExists.Should().BeFalse();
        loaded.ETag.Should().BeNull();
        loaded.State.City.Should().BeEmpty();
    }

    [Fact]
    public async Task GrainStorageBridgeIncrementsETagsAndRejectsStaleClear()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            VacancyGrain.StorageProviderName);
        var grainId = CreateGrain().GetGrainId();
        var current = new GrainState<VacancyState>
        {
            State = new VacancyState { City = "first", Salary = 10 },
        };

        await storage.WriteStateAsync(VacancyGrain.StateName, grainId, current);
        var firstETag = current.ETag;
        current.State = new VacancyState { City = "second", Salary = 20 };
        await storage.WriteStateAsync(VacancyGrain.StateName, grainId, current);

        current.ETag.Should().NotBe(firstETag);
        var stale = new GrainState<VacancyState>
        {
            State = current.State,
            ETag = firstETag,
            RecordExists = true,
        };
        Func<Task> staleClear = () => storage.ClearStateAsync(
            VacancyGrain.StateName,
            grainId,
            stale);
        await staleClear.Should().ThrowAsync<InconsistentStateException>();

        var loaded = new GrainState<VacancyState>();
        await storage.ReadStateAsync(VacancyGrain.StateName, grainId, loaded);
        loaded.State.City.Should().Be("second");
        loaded.ETag.Should().Be(current.ETag);

        await storage.ClearStateAsync(VacancyGrain.StateName, grainId, loaded);
    }

    [Fact]
    public async Task ClearingMissingStateIsIdempotentButStillChecksETag()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            VacancyGrain.StorageProviderName);
        var grainId = CreateGrain().GetGrainId();
        var missing = new GrainState<VacancyState>();

        await storage.ClearStateAsync(VacancyGrain.StateName, grainId, missing);

        missing.RecordExists.Should().BeFalse();
        missing.ETag.Should().BeNull();
        var stale = new GrainState<VacancyState>
        {
            ETag = "stale",
            RecordExists = true,
        };
        Func<Task> clearWithStaleETag = () => storage.ClearStateAsync(
            VacancyGrain.StateName,
            grainId,
            stale);
        await clearWithStaleETag.Should().ThrowAsync<InconsistentStateException>();
    }

    [Fact]
    public async Task MismatchedPartitionCountIsRejected()
    {
        var city = $"layout-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        await grain.SetAsync(city, 10);
        var layout = Fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(
            VacancyGrain.StorageProviderName);
        await Fixture.Cluster.DeactivateAsync(layout);

        var client = new SearchableStorageClient(
            Fixture.Cluster.GrainFactory,
            VacancyGrain.StorageProviderName,
            Fixture.PartitionCount + 1);
        Func<Task> query = () => client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            city);

        await query.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*persisted layout*");

        await grain.ClearAsync();
    }

    [Fact]
    public async Task QueriesBeforeFirstWriteReturnEmptyWithoutInitializingLayout()
    {
        var providerName = $"uninitialized-{Guid.NewGuid():N}";
        var firstClient = new SearchableStorageClient(
            Fixture.Cluster.GrainFactory,
            providerName,
            1);
        var secondClient = new SearchableStorageClient(
            Fixture.Cluster.GrainFactory,
            providerName,
            2);

        var firstResults = await firstClient.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "missing");
        var secondResults = await secondClient.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "missing");
        var firstRangeResults = await firstClient.RangeAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            1,
            10);
        var secondRangeResults = await secondClient.RangeAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            1,
            10);

        firstResults.Should().BeEmpty();
        secondResults.Should().BeEmpty();
        firstRangeResults.Should().BeEmpty();
        secondRangeResults.Should().BeEmpty();
    }

    [Fact]
    public async Task InvalidLayoutDescriptorsAreRejectedBeforePersistence()
    {
        var providerName = $"invalid-layout-{Guid.NewGuid():N}";
        var layout = Fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var wrongProvider = new StorageLayoutDescriptor
        {
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = $"wrong-{Guid.NewGuid():N}",
            PartitionCount = Fixture.PartitionCount,
        };
        var unsupportedVersion = new StorageLayoutDescriptor
        {
            FormatVersion = StorageLayout.CurrentFormatVersion + 1,
            ProviderName = providerName,
            PartitionCount = Fixture.PartitionCount,
        };
        var legacyVersion = new StorageLayoutDescriptor
        {
            FormatVersion = 1,
            ProviderName = providerName,
            PartitionCount = Fixture.PartitionCount,
        };

        Func<Task> validateWrongProvider = () => layout.ValidateAsync(wrongProvider);
        Func<Task> validateUnsupportedVersion = () => layout.ValidateAsync(unsupportedVersion);
        Func<Task> validateLegacyVersion = () => layout.ValidateAsync(legacyVersion);

        await validateWrongProvider.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*layout descriptor provider name*");
        await validateUnsupportedVersion.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithMessage("*Storage format version*");
        await validateLegacyVersion.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithMessage("*Storage format version 2 is required*");
        (await layout.ValidateAsync(StorageLayout.CreateDescriptor(providerName, Fixture.PartitionCount)))
            .Should().BeFalse();
    }

    [Fact]
    public async Task DifferentProviderNameUsesAnIsolatedNamespace()
    {
        var city = $"isolated-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        await grain.SetAsync(city, 10);
        var isolatedClient = new SearchableStorageClient(
            Fixture.Cluster.GrainFactory,
            $"other-{Guid.NewGuid():N}",
            Fixture.PartitionCount);

        var results = await isolatedClient.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            city);

        results.Should().BeEmpty();
        await grain.ClearAsync();
    }

    [Fact]
    public void KeyedQueryClientUsesProviderConfiguration()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var client = silo.ServiceProvider.GetRequiredKeyedService<ISearchableStorageClient>(
            VacancyGrain.StorageProviderName);

        client.Should().BeOfType<SearchableStorageClient>();
    }

    [Fact]
    public async Task QueryValueTypeMustMatchIndexedPropertyType()
    {
        var client = CreateClient();
        Func<Task> query = () => client.FindAsync<VacancyState, object>(
            VacancyGrain.StateName,
            state => state.Salary,
            10L);

        await query.Should().ThrowAsync<ArgumentException>()
            .WithParameterName("value");
    }

    [Fact]
    public async Task NullQueryValuesAreRejected()
    {
        Func<Task> query = () => CreateClient().FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            null!);

        await query.Should().ThrowAsync<ArgumentNullException>()
            .WithParameterName("value");
    }

    protected IVacancyGrain CreateGrain()
    {
        return Fixture.Cluster.GrainFactory.GetGrain<IVacancyGrain>(Guid.NewGuid().ToString("N"));
    }

    protected IVacancyGrain CreateGrainInPartition(int targetPartition)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(targetPartition);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(targetPartition, Fixture.PartitionCount);

        for (var attempt = 0; attempt < 10_000; attempt++)
        {
            var grain = CreateGrain();
            if (GetPartitionIndex(grain.GetGrainId()) == targetPartition)
            {
                return grain;
            }
        }

        throw new InvalidOperationException($"Could not create a grain in partition {targetPartition}.");
    }

    protected SearchableStorageClient CreateClient()
    {
        return new SearchableStorageClient(
            Fixture.Cluster.GrainFactory,
            VacancyGrain.StorageProviderName,
            Fixture.PartitionCount);
    }

    private protected IStoragePartitionGrain GetPartition(GrainId grainId)
    {
        var partitionIndex = GetPartitionIndex(grainId);
        return Fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(VacancyGrain.StorageProviderName, partitionIndex));
    }

    private protected int GetPartitionIndex(GrainId grainId)
    {
        return (int)((uint)grainId.GetUniformHashCode() % (uint)Fixture.PartitionCount);
    }

    protected static Task ClearAsync(params IVacancyGrain[] grains)
    {
        return Task.WhenAll(grains.Select(static grain => grain.ClearAsync()));
    }
}

public sealed class MemorySearchableStorageContractTests : SearchableStorageContractTests<MemoryStorageFixture>
{
    public MemorySearchableStorageContractTests(MemoryStorageFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public void PhysicalMemoryBackendUsesJsonSerializer()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var options = silo.ServiceProvider
            .GetRequiredService<IOptionsMonitor<MemoryGrainStorageOptions>>()
            .Get(MemoryStorageFixture.InnerPhysicalStorageProviderName);

        options.GrainStorageSerializer.Should().BeOfType<JsonGrainStorageSerializer>();
    }

    [Fact]
    public async Task FailedPhysicalWriteDoesNotExposeCandidateState()
    {
        var oldCity = $"before-old-{Guid.NewGuid():N}";
        var newCity = $"before-new-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        var grainId = grain.GetGrainId();
        var partition = GetPartition(grainId);

        await grain.SetAsync(oldCity, 10);
        await AddWriteFaultAsync(partition, PhysicalWriteFaultStage.BeforeCommit);

        Func<Task> write = () => grain.SetAsync(newCity, 20);
        var exception = await write.Should().ThrowAsync<OrleansException>();
        exception.Which.InnerException.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Be("Injected physical write failure.");

        await Fixture.Cluster.DeactivateAsync(grain);
        await Fixture.Cluster.DeactivateAsync(partition);

        var state = await grain.GetAsync();
        var client = CreateClient();
        var oldResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            oldCity);
        var newResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            newCity);

        state.City.Should().Be(oldCity);
        state.Salary.Should().Be(10);
        oldResults.Should().ContainSingle().Which.Should().Be(grainId);
        newResults.Should().BeEmpty();

        await grain.ClearAsync();
    }

    [Fact]
    public async Task LostAcknowledgementRehydratesCommittedCandidate()
    {
        var oldCity = $"after-old-{Guid.NewGuid():N}";
        var newCity = $"after-new-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        var grainId = grain.GetGrainId();
        var partition = GetPartition(grainId);

        await grain.SetAsync(oldCity, 10);
        await AddWriteFaultAsync(partition, PhysicalWriteFaultStage.AfterCommit);

        Func<Task> write = () => grain.SetAsync(newCity, 20);
        var exception = await write.Should().ThrowAsync<OrleansException>();
        exception.Which.InnerException.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Be("Injected physical write failure.");

        await Fixture.Cluster.DeactivateAsync(grain);
        await Fixture.Cluster.DeactivateAsync(partition);

        var state = await grain.GetAsync();
        var client = CreateClient();
        var oldResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            oldCity);
        var newResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            newCity);

        state.City.Should().Be(newCity);
        state.Salary.Should().Be(20);
        oldResults.Should().BeEmpty();
        newResults.Should().ContainSingle().Which.Should().Be(grainId);

        await grain.ClearAsync();
    }

    [Fact]
    public async Task FailedPhysicalClearPreservesCommittedState()
    {
        var city = $"clear-before-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        var grainId = grain.GetGrainId();
        var partition = GetPartition(grainId);

        await grain.SetAsync(city, 10);
        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "partition",
            PhysicalWriteFaultStage.BeforeCommit);

        Func<Task> clear = () => grain.ClearAsync();
        await AssertInjectedFailureAsync(clear);
        await Fixture.Cluster.DeactivateAsync(grain);
        await Fixture.Cluster.DeactivateAsync(partition);

        var state = await grain.GetAsync();
        var client = CreateClient();
        var hashResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            city);
        var rangeResults = await client.FindAsync<VacancyState, int>(
            VacancyGrain.StateName,
            candidate => candidate.Salary,
            10);

        state.City.Should().Be(city);
        hashResults.Should().ContainSingle().Which.Should().Be(grainId);
        rangeResults.Should().ContainSingle().Which.Should().Be(grainId);

        await grain.ClearAsync();
    }

    [Fact]
    public async Task LostClearAcknowledgementRehydratesClearedState()
    {
        var city = $"clear-after-{Guid.NewGuid():N}";
        var grain = CreateGrain();
        var grainId = grain.GetGrainId();
        var partition = GetPartition(grainId);

        await grain.SetAsync(city, 10);
        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "partition",
            PhysicalWriteFaultStage.AfterCommit);

        Func<Task> clear = () => grain.ClearAsync();
        await AssertInjectedFailureAsync(clear);
        await Fixture.Cluster.DeactivateAsync(grain);
        await Fixture.Cluster.DeactivateAsync(partition);

        var state = await grain.GetAsync();
        var client = CreateClient();
        var hashResults = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            city);
        var rangeResults = await client.FindAsync<VacancyState, int>(
            VacancyGrain.StateName,
            candidate => candidate.Salary,
            10);

        state.City.Should().BeEmpty();
        state.Salary.Should().Be(0);
        hashResults.Should().BeEmpty();
        rangeResults.Should().BeEmpty();
    }

    [Fact]
    public async Task StorageBridgeCanRetryLayoutInitializationAfterPhysicalFailure()
    {
        var providerName = $"storage-layout-retry-{Guid.NewGuid():N}";
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var configuredOptions = silo.ServiceProvider
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(VacancyGrain.StorageProviderName);
        var storage = ActivatorUtilities.CreateInstance<SearchableGrainStorage>(
            silo.ServiceProvider,
            providerName,
            new SearchableStorageOptions
            {
                PartitionCount = Fixture.PartitionCount,
                GrainStorageSerializer = configuredOptions.GrainStorageSerializer,
            });
        var layout = Fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        await AddWriteFaultAsync(
            layout.GetGrainId(),
            "layout",
            PhysicalWriteFaultStage.BeforeCommit);
        var grainId = CreateGrain().GetGrainId();
        var state = new GrainState<VacancyState>
        {
            State = new VacancyState { City = "retry", Salary = 10 },
        };

        Func<Task> firstWrite = () => storage.WriteStateAsync(VacancyGrain.StateName, grainId, state);
        await AssertInjectedFailureAsync(firstWrite);
        await storage.WriteStateAsync(VacancyGrain.StateName, grainId, state);

        var loaded = new GrainState<VacancyState>();
        await storage.ReadStateAsync(VacancyGrain.StateName, grainId, loaded);
        loaded.State.Should().BeEquivalentTo(state.State);
        loaded.RecordExists.Should().BeTrue();

        await storage.ClearStateAsync(VacancyGrain.StateName, grainId, loaded);
    }

    [Fact]
    public async Task StoragePartitionTopologyDoesNotFollowLaterOptionsMutation()
    {
        var providerName = $"stable-partitions-{Guid.NewGuid():N}";
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var configuredOptions = silo.ServiceProvider
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(VacancyGrain.StorageProviderName);
        var options = new SearchableStorageOptions
        {
            PartitionCount = Fixture.PartitionCount,
            GrainStorageSerializer = configuredOptions.GrainStorageSerializer,
        };
        var storage = ActivatorUtilities.CreateInstance<SearchableGrainStorage>(
            silo.ServiceProvider,
            providerName,
            options);
        var grainId = CreateGrainIdInUpperHalf(Fixture.PartitionCount * 2);
        var state = new GrainState<VacancyState>
        {
            State = new VacancyState { City = "stable", Salary = 10 },
        };

        options.PartitionCount *= 2;
        await storage.WriteStateAsync(VacancyGrain.StateName, grainId, state);

        var loaded = new GrainState<VacancyState>();
        await storage.ReadStateAsync(VacancyGrain.StateName, grainId, loaded);
        loaded.State.Should().BeEquivalentTo(state.State);

        await storage.ClearStateAsync(VacancyGrain.StateName, grainId, loaded);
    }

    [Fact]
    public async Task LayoutInitializationCanRetryAfterPhysicalFailure()
    {
        var providerName = $"layout-retry-{Guid.NewGuid():N}";
        var layout = Fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var descriptor = StorageLayout.CreateDescriptor(providerName, Fixture.PartitionCount);
        await AddWriteFaultAsync(
            layout.GetGrainId(),
            "layout",
            PhysicalWriteFaultStage.BeforeCommit);

        Func<Task> initialize = () => layout.InitializeAsync(descriptor);
        await AssertInjectedFailureAsync(initialize);
        await Fixture.Cluster.DeactivateAsync(layout);

        await layout.InitializeAsync(descriptor);
        await Fixture.Cluster.DeactivateAsync(layout);
        (await layout.ValidateAsync(descriptor)).Should().BeTrue();
    }

    private async Task AddWriteFaultAsync(
        IStoragePartitionGrain partition,
        PhysicalWriteFaultStage stage)
    {
        await AddWriteFaultAsync(partition.GetGrainId(), "partition", stage);
    }

    private async Task AddWriteFaultAsync(
        GrainId grainId,
        string stateName,
        PhysicalWriteFaultStage stage)
    {
        var faultGrain = Fixture.Cluster.GrainFactory.GetGrain<IStorageFaultGrain>(
            WriteFaultInjectingGrainStorage.CreateFaultGrainKey(stage, stateName));
        await faultGrain.AddFaultOnWrite(
            grainId,
            new InvalidOperationException("Injected physical write failure."));
    }

    private static async Task AssertInjectedFailureAsync(Func<Task> action)
    {
        var exception = await action.Should().ThrowAsync<OrleansException>();
        exception.Which.InnerException.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Be("Injected physical write failure.");
    }

    private GrainId CreateGrainIdInUpperHalf(int partitionCount)
    {
        for (var attempt = 0; attempt < 10_000; attempt++)
        {
            var grainId = CreateGrain().GetGrainId();
            var partitionIndex = (int)((uint)grainId.GetUniformHashCode() % (uint)partitionCount);
            if (partitionIndex >= partitionCount / 2)
            {
                return grainId;
            }
        }

        throw new InvalidOperationException("Could not create a grain id in the upper half of the requested partition range.");
    }
}
