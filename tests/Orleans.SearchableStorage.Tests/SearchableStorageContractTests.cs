using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Serialization;
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
    public async Task QueryablePredicateIntersectsExactAndRangeIndexes()
    {
        var city = $"query-and-{Guid.NewGuid():N}";
        var offset = Random.Shared.Next(2_000_000, 3_000_000);
        var match = CreateGrain();
        var wrongCity = CreateGrain();
        var wrongSalary = CreateGrain();

        await match.SetAsync(city, offset + 6);
        await wrongCity.SetAsync($"other-{Guid.NewGuid():N}", offset + 6);
        await wrongSalary.SetAsync(city, offset + 9);
        var lowerBound = offset + 5;
        var upperBound = offset + 8;

        var matches = await CreateClient()
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == city && state.Salary > lowerBound && state.Salary < upperBound)
            .ToGrainIdsAsync();

        matches.Should().ContainSingle().Which.Should().Be(match.GetGrainId());

        await ClearAsync(match, wrongCity, wrongSalary);
    }

    [Fact]
    public async Task QueryableOrUnionsAndDeduplicatesMatches()
    {
        var firstCity = $"query-or-first-{Guid.NewGuid():N}";
        var secondCity = $"query-or-second-{Guid.NewGuid():N}";
        var first = CreateGrainInPartition(0);
        var second = CreateGrainInPartition(1);
        var outside = CreateGrain();

        await first.SetAsync(firstCity, 10);
        await second.SetAsync(secondCity, 20);
        await outside.SetAsync($"outside-{Guid.NewGuid():N}", 30);

        var matches = await CreateClient()
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == firstCity || state.City == secondCity || state.City == firstCity)
            .ToGrainIdsAsync();
        var expected = new[] { first.GetGrainId(), second.GetGrainId() }
            .Order()
            .ToArray();

        matches.Should().Equal(expected);
        matches.Should().OnlyHaveUniqueItems();
        GetPartitionIndex(first.GetGrainId()).Should().NotBe(GetPartitionIndex(second.GetGrainId()));

        await ClearAsync(first, second, outside);
    }

    [Fact]
    public async Task CompoundQueriesDoNotMutateBackingIndexBuckets()
    {
        var city = $"bucket-a-{Guid.NewGuid():N}";
        var otherCity = $"bucket-b-{Guid.NewGuid():N}";
        var offset = Random.Shared.Next(6_000_000, 7_000_000);
        var insideRange = CreateGrainInPartition(0);
        var outsideRange = CreateGrainInPartition(0);
        var other = CreateGrainInPartition(0);
        try
        {
            await insideRange.SetAsync(city, offset + 1);
            await outsideRange.SetAsync(city, offset + 9);
            await other.SetAsync(otherCity, offset + 1);
            var client = CreateClient();
            var upperBound = offset + 5;

            var intersection = await client
                .Query<VacancyState>(VacancyGrain.StateName)
                .Where(state => state.City == city && state.Salary < upperBound)
                .ToGrainIdsAsync();
            var cityAfterIntersection = await client.FindAsync<VacancyState, string>(
                VacancyGrain.StateName,
                state => state.City,
                city);
            var union = await client
                .Query<VacancyState>(VacancyGrain.StateName)
                .Where(state => state.City == city || state.City == otherCity)
                .ToGrainIdsAsync();
            var cityAfterUnion = await client.FindAsync<VacancyState, string>(
                VacancyGrain.StateName,
                state => state.City,
                city);

            intersection.Should().ContainSingle().Which.Should().Be(insideRange.GetGrainId());
            cityAfterIntersection.Should().BeEquivalentTo(
                [insideRange.GetGrainId(), outsideRange.GetGrainId()]);
            union.Should().BeEquivalentTo(
                [insideRange.GetGrainId(), outsideRange.GetGrainId(), other.GetGrainId()]);
            cityAfterUnion.Should().BeEquivalentTo(
                [insideRange.GetGrainId(), outsideRange.GetGrainId()]);
        }
        finally
        {
            await ClearAsync(insideRange, outsideRange, other);
        }
    }

    [Fact]
    public async Task QueryableNestedBooleanAndEmptyPlansExecuteThroughPublicApi()
    {
        var firstCity = $"nested-first-{Guid.NewGuid():N}";
        var secondCity = $"nested-second-{Guid.NewGuid():N}";
        var offset = Random.Shared.Next(5_000_000, 6_000_000);
        var first = CreateGrain();
        var second = CreateGrain();
        var wrongSalary = CreateGrain();
        var wrongCity = CreateGrain();
        await first.SetAsync(firstCity, offset + 6);
        await second.SetAsync(secondCity, offset + 7);
        await wrongSalary.SetAsync(firstCity, offset + 9);
        await wrongCity.SetAsync($"nested-other-{Guid.NewGuid():N}", offset + 6);
        var lowerBound = offset + 5;
        var upperBound = offset + 8;
        var client = CreateClient();

        var matches = await client
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => (state.City == firstCity || state.City == secondCity)
                && state.Salary > lowerBound
                && state.Salary < upperBound)
            .ToGrainIdsAsync();
        var emptyMatches = await client
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.Salary > upperBound && state.Salary < lowerBound)
            .ToGrainIdsAsync();
        var expected = new[] { first.GetGrainId(), second.GetGrainId() }
            .Order()
            .ToArray();

        matches.Should().Equal(expected);
        emptyMatches.Should().BeEmpty();

        await ClearAsync(first, second, wrongSalary, wrongCity);
    }

    [Fact]
    public async Task QueryableReversedOperandsHonorInclusivity()
    {
        var offset = Random.Shared.Next(3_000_000, 4_000_000);
        var lower = CreateGrain();
        var middle = CreateGrain();
        var upper = CreateGrain();

        await lower.SetAsync("lower", offset + 5);
        await middle.SetAsync("middle", offset + 6);
        await upper.SetAsync("upper", offset + 8);
        var lowerBound = offset + 5;
        var upperBound = offset + 8;

        var matches = await CreateClient()
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => lowerBound <= state.Salary && upperBound > state.Salary)
            .ToGrainIdsAsync();

        matches.Should().BeEquivalentTo([lower.GetGrainId(), middle.GetGrainId()]);

        await ClearAsync(lower, middle, upper);
    }

    [Fact]
    public async Task QueryableMultipleWhereCallsCombineCapturedBounds()
    {
        var offset = Random.Shared.Next(4_000_000, 5_000_000);
        var match = CreateGrain();
        var outside = CreateGrain();
        await match.SetAsync("match", offset + 6);
        await outside.SetAsync("outside", offset + 9);
        var lowerBound = offset + 5;
        var upperBound = offset + 8;

        var query = CreateClient()
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.Salary > lowerBound)
            .Where(state => state.Salary < upperBound);
        var matches = await query.ToGrainIdsAsync();

        matches.Should().ContainSingle().Which.Should().Be(match.GetGrainId());

        await ClearAsync(match, outside);
    }

    [Theory]
    [InlineData(true, true, true)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, false)]
    public async Task QueryableEqualBoundsHonorEveryInclusivityCombination(
        bool includeLowerBound,
        bool includeUpperBound,
        bool shouldMatch)
    {
        var salary = 2_000_000_000
            + (includeLowerBound ? 2 : 0)
            + (includeUpperBound ? 1 : 0);
        var grain = CreateGrain();
        await grain.SetAsync($"equal-{Guid.NewGuid():N}", salary);
        IQueryable<VacancyState> query = CreateClient()
            .Query<VacancyState>(VacancyGrain.StateName);
        query = includeLowerBound
            ? query.Where(state => state.Salary >= salary)
            : query.Where(state => state.Salary > salary);
        query = includeUpperBound
            ? query.Where(state => state.Salary <= salary)
            : query.Where(state => state.Salary < salary);

        var matches = await query.ToGrainIdsAsync();

        if (shouldMatch)
        {
            matches.Should().ContainSingle().Which.Should().Be(grain.GetGrainId());
        }
        else
        {
            matches.Should().BeEmpty();
        }

        await grain.ClearAsync();
    }

    [Fact]
    public async Task QueryableOneSidedRangesReachOpenEnds()
    {
        var highMatch = CreateGrain();
        var highOutside = CreateGrain();
        var lowMatch = CreateGrain();
        var lowOutside = CreateGrain();
        await highMatch.SetAsync("high-match", int.MaxValue - 2);
        await highOutside.SetAsync("high-outside", int.MaxValue - 5);
        await lowMatch.SetAsync("low-match", int.MinValue + 2);
        await lowOutside.SetAsync("low-outside", int.MinValue + 5);
        var lowerBound = int.MaxValue - 4;
        var upperBound = int.MinValue + 4;
        var client = CreateClient();

        var greaterMatches = await client
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.Salary > lowerBound)
            .ToGrainIdsAsync();
        var lessMatches = await client
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.Salary < upperBound)
            .ToGrainIdsAsync();

        greaterMatches.Should().ContainSingle().Which.Should().Be(highMatch.GetGrainId());
        lessMatches.Should().ContainSingle().Which.Should().Be(lowMatch.GetGrainId());

        await ClearAsync(highMatch, highOutside, lowMatch, lowOutside);
    }

    [Fact]
    public async Task QueryableExecutionHonorsCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var query = CreateClient()
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.Salary >= 0);

        Func<Task> execute = () => query.ToGrainIdsAsync(cancellation.Token);

        await execute.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public void QueryableSynchronousEnumerationIsRejected()
    {
        var query = CreateClient()
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.Salary >= 0);

        Func<IEnumerator<VacancyState>> enumerate = query.GetEnumerator;

        enumerate.Should().Throw<NotSupportedException>()
            .WithMessage("*Synchronous query enumeration is not supported*");
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
    public async Task CollectionStateWithoutIndexesRoundTripsThroughStorage()
    {
        var stateName = $"items-{Guid.NewGuid():N}";
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            VacancyGrain.StorageProviderName);
        var grainId = CreateGrain().GetGrainId();
        var current = new GrainState<List<string>>
        {
            State = ["first", "second"],
        };

        await storage.WriteStateAsync(stateName, grainId, current);
        await Fixture.Cluster.DeactivateAsync(GetPartition(grainId));

        var loaded = new GrainState<List<string>>();
        await storage.ReadStateAsync(stateName, grainId, loaded);

        loaded.RecordExists.Should().BeTrue();
        loaded.ETag.Should().Be(current.ETag);
        loaded.State.Should().Equal(current.State);

        await storage.ClearStateAsync(stateName, grainId, loaded);
    }

    [Fact]
    public async Task QueryableEqualityFindsNullableIndexedValues()
    {
        var stateName = $"nullable-{Guid.NewGuid():N}";
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            VacancyGrain.StorageProviderName);
        var grainId = CreateGrain().GetGrainId();
        var current = new GrainState<NullableQueryState>
        {
            State = new NullableQueryState { Score = 17 },
        };
        await storage.WriteStateAsync(stateName, grainId, current);
        int? expectedScore = 17;

        var matches = await CreateClient()
            .Query<NullableQueryState>(stateName)
            .Where(state => state.Score == expectedScore)
            .ToGrainIdsAsync();

        matches.Should().ContainSingle().Which.Should().Be(grainId);

        await storage.ClearStateAsync(stateName, grainId, current);
    }

    [Fact]
    public async Task CompilerPromotedValuesRoundTripThroughStorageAndQueryableExecution()
    {
        var stateName = $"promoted-{Guid.NewGuid():N}";
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var storage = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            VacancyGrain.StorageProviderName);
        var matchId = CreateGrain().GetGrainId();
        var ageFailureId = CreateGrain().GetGrainId();
        var statusFailureId = CreateGrain().GetGrainId();
        var optionalStatusFailureId = CreateGrain().GetGrainId();
        var match = CreatePromotedState(21, PromotionStatus.Active, PromotionStatus.Active);
        var ageFailure = CreatePromotedState(17, PromotionStatus.Active, PromotionStatus.Active);
        var statusFailure = CreatePromotedState(21, PromotionStatus.Inactive, PromotionStatus.Active);
        var optionalStatusFailure = CreatePromotedState(21, PromotionStatus.Active, PromotionStatus.Inactive);

        try
        {
            await Task.WhenAll(
                storage.WriteStateAsync(stateName, matchId, match),
                storage.WriteStateAsync(stateName, ageFailureId, ageFailure),
                storage.WriteStateAsync(stateName, statusFailureId, statusFailure),
                storage.WriteStateAsync(stateName, optionalStatusFailureId, optionalStatusFailure));
            var minimumAge = (byte)18;
            var expectedStatus = PromotionStatus.Active;
            PromotionStatus? expectedOptionalStatus = PromotionStatus.Active;

            var client = CreateClient();
            var matches = await client
                .Query<PromotedQueryState>(stateName)
                .Where(state => state.Age >= minimumAge
                    && state.Status == expectedStatus
                    && state.OptionalStatus == expectedOptionalStatus)
                .ToGrainIdsAsync();
            var ageMatches = await client
                .Query<PromotedQueryState>(stateName)
                .Where(state => state.Age >= minimumAge)
                .ToGrainIdsAsync();
            var statusMatches = await client
                .Query<PromotedQueryState>(stateName)
                .Where(state => state.Status == expectedStatus)
                .ToGrainIdsAsync();
            var optionalStatusMatches = await client
                .Query<PromotedQueryState>(stateName)
                .Where(state => state.OptionalStatus == expectedOptionalStatus)
                .ToGrainIdsAsync();

            matches.Should().ContainSingle().Which.Should().Be(matchId);
            ageMatches.Should().BeEquivalentTo(
                [matchId, statusFailureId, optionalStatusFailureId]);
            statusMatches.Should().BeEquivalentTo(
                [matchId, ageFailureId, optionalStatusFailureId]);
            optionalStatusMatches.Should().BeEquivalentTo(
                [matchId, ageFailureId, statusFailureId]);
        }
        finally
        {
            await Task.WhenAll(
                storage.ClearStateAsync(stateName, matchId, match),
                storage.ClearStateAsync(stateName, ageFailureId, ageFailure),
                storage.ClearStateAsync(stateName, statusFailureId, statusFailure),
                storage.ClearStateAsync(stateName, optionalStatusFailureId, optionalStatusFailure));
        }
    }

    [Fact]
    public async Task MalformedWirePlansAreRejectedWithoutPoisoningThePartition()
    {
        var partition = GetPartition(CreateGrain().GetGrainId());
        var overDepth = CreateWirePlanAtDepth(QueryPlanLimits.MaximumDepth + 1);
        var missingChild = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = new PartitionQueryPlan { Operation = PartitionQueryOperation.Empty },
        };
        var unknownOperation = new PartitionQueryPlan
        {
            Operation = (PartitionQueryOperation)int.MaxValue,
        };

        Func<Task> sendOverDepth = async () => await partition.QueryAsync(overDepth);
        Func<Task> sendMissingChild = async () => await partition.QueryAsync(missingChild);
        Func<Task> sendUnknownOperation = async () => await partition.QueryAsync(unknownOperation);

        await sendOverDepth.Should().ThrowAsync<ArgumentException>()
            .WithMessage($"*maximum supported depth of {QueryPlanLimits.MaximumDepth}*");
        await sendMissingChild.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*requires both child plans*");
        await sendUnknownOperation.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithMessage("*Unknown partition query operation*");
        (await partition.QueryAsync(new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Empty,
        })).Should().BeEmpty();
    }

    [Fact]
    public async Task MalformedBoundedRangeQueriesAreRejectedWithoutPoisoningThePartition()
    {
        var partition = GetPartition(CreateGrain().GetGrainId());
        var missingLower = new RangeIndexQuery
        {
            Scope = "scope",
            LowerBound = null!,
            UpperBound = IndexValue.Create(2),
        };
        var missingUpper = new RangeIndexQuery
        {
            Scope = "scope",
            LowerBound = IndexValue.Create(1),
            UpperBound = null!,
        };

        Func<Task> sendMissingLower = async () => await partition.RangeAsync(missingLower);
        Func<Task> sendMissingUpper = async () => await partition.RangeAsync(missingUpper);

        await sendMissingLower.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*bounded range query requires both lower and upper bounds*");
        await sendMissingUpper.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*bounded range query requires both lower and upper bounds*");
        (await partition.RangeAsync(new RangeIndexQuery
        {
            Scope = "scope",
            LowerBound = IndexValue.Create(1),
            UpperBound = IndexValue.Create(2),
        })).Should().BeEmpty();
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

        Func<Task> queryable = () => client
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == city)
            .ToGrainIdsAsync();
        await queryable.Should().ThrowAsync<InvalidOperationException>()
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
        var firstQueryableResults = await firstClient
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == "missing")
            .ToGrainIdsAsync();
        var secondQueryableResults = await secondClient
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == "missing")
            .ToGrainIdsAsync();

        firstResults.Should().BeEmpty();
        secondResults.Should().BeEmpty();
        firstRangeResults.Should().BeEmpty();
        secondRangeResults.Should().BeEmpty();
        firstQueryableResults.Should().BeEmpty();
        secondQueryableResults.Should().BeEmpty();
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
    public void KeyedDirectAndQueryableClientsShareProviderConfiguration()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var client = silo.ServiceProvider.GetRequiredKeyedService<ISearchableStorageClient>(
            VacancyGrain.StorageProviderName);
        var queryClient = silo.ServiceProvider.GetRequiredKeyedService<ISearchableStorageQueryClient>(
            VacancyGrain.StorageProviderName);

        client.Should().BeOfType<SearchableStorageClient>();
        queryClient.Should().BeSameAs(client);
    }

    [Fact]
    public void PartitionQueryPlanRoundTripsThroughOrleansSerializer()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var serializer = silo.ServiceProvider.GetRequiredService<Serializer>();
        var original = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.And,
            Left = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Exact,
                Scope = "city",
                IndexKind = SearchableIndexKind.Hash,
                Value = IndexValue.Create("Helsinki"),
            },
            Right = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Range,
                Scope = "salary",
                LowerBound = IndexValue.Create(5),
                UpperBound = null,
                IncludeLowerBound = false,
            },
        };

        var payload = serializer.SerializeToArray(original);
        var copy = serializer.Deserialize<PartitionQueryPlan>(payload);

        copy.Operation.Should().Be(PartitionQueryOperation.And);
        copy.Left!.Operation.Should().Be(PartitionQueryOperation.Exact);
        copy.Left.Scope.Should().Be("city");
        copy.Left.Value!.Text.Should().Be("Helsinki");
        copy.Right!.Operation.Should().Be(PartitionQueryOperation.Range);
        copy.Right.Scope.Should().Be("salary");
        copy.Right.LowerBound!.SignedInteger.Should().Be(5);
        copy.Right.UpperBound.Should().BeNull();
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

    private static PartitionQueryPlan CreateWirePlanAtDepth(int depth)
    {
        var plan = new PartitionQueryPlan { Operation = PartitionQueryOperation.Empty };
        for (var currentDepth = 1; currentDepth < depth; currentDepth++)
        {
            plan = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.Or,
                Left = new PartitionQueryPlan { Operation = PartitionQueryOperation.Empty },
                Right = plan,
            };
        }

        return plan;
    }

    private static GrainState<PromotedQueryState> CreatePromotedState(
        byte age,
        PromotionStatus status,
        PromotionStatus? optionalStatus)
    {
        return new GrainState<PromotedQueryState>
        {
            State = new PromotedQueryState
            {
                Age = age,
                Status = status,
                OptionalStatus = optionalStatus,
            },
        };
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
