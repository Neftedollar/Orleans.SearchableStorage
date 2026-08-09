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

        oldResults.Should().BeEmpty();
        newResults.Should().ContainSingle().Which.Should().Be(grain.GetGrainId());

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

    protected IVacancyGrain CreateGrain()
    {
        return Fixture.Cluster.GrainFactory.GetGrain<IVacancyGrain>(Guid.NewGuid().ToString("N"));
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
        var partitionIndex = (int)((uint)grainId.GetUniformHashCode() % (uint)Fixture.PartitionCount);
        return Fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(VacancyGrain.StorageProviderName, partitionIndex));
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

    private async Task AddWriteFaultAsync(
        IStoragePartitionGrain partition,
        PhysicalWriteFaultStage stage)
    {
        var faultGrain = Fixture.Cluster.GrainFactory.GetGrain<IStorageFaultGrain>(
            WriteFaultInjectingGrainStorage.CreateFaultGrainKey(stage, "partition"));
        await faultGrain.AddFaultOnWrite(
            partition.GetGrainId(),
            new InvalidOperationException("Injected physical write failure."));
    }
}
