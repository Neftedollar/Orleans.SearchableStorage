using System.Net;
using System.Net.Http.Json;
using System.Text;
using AwesomeAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.SearchableStorage.ApiSample.Tests;

[Collection(ApiSampleTestGroup.Name)]
public sealed class ApiSampleTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly HttpClient _client;

    public ApiSampleTests(WebApplicationFactory<Program> factory)
    {
        _factory = factory;
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task RootDescribesTheExecutableApi()
    {
        var response = await _client.GetAsync("/");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var description = await response.Content.ReadFromJsonAsync<ApiDescription>();
        description.Should().NotBeNull();
        description!.Name.Should().Be("Orleans.SearchableStorage API sample");
        description.Storage.Should().Be("Journaled Orleans storage over in-memory physical persistence");
        description.Endpoints.Should().HaveCount(21);
    }

    [Fact]
    public void HostUsesThePersistenceSettingsDocumentedByTheSample()
    {
        var options = _factory.Services
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(VacancyGrain.StorageProviderName);

        options.PartitionCount.Should().Be(8);
        options.VirtualSlotTargetCount.Should().Be(64);
        options.JournalSegmentCapacity.Should().Be(16);
        options.MaximumJournalReplayEntries.Should().Be(256);
        options.CompactionThreshold.Should().Be(64);
        options.Movement.TransferPageRecordLimit.Should().Be(128);
        options.Movement.TransferPageByteTarget.Should().Be(256 * 1024);
        options.Query.ContinuationProtection.CurrentKey.Should().NotBeNull();
        options.Query.ContinuationProtection.CurrentKey!.KeyId.Should().Be("api-sample-ephemeral");
    }

    [Fact]
    public async Task HostBootstrapsAndExposesTheManagedVacancySchema()
    {
        VacancyGrain.ApplicationSchemaVersion.Should().Be(1);

        var response = await _client.GetAsync("/storage/index-schemas/vacancies");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var status = await response.Content
            .ReadFromJsonAsync<SearchableStorageIndexSchemaStatus>();
        status.Should().NotBeNull();
        status!.State.Should().Be(SearchableStorageIndexSchemaState.Active);
        status.StateName.Should().Be(VacancyGrain.StateName);
        status.Fingerprint.Should().NotBeNullOrWhiteSpace();
        status.ProcessedRecordCount.Should().Be(0);

        var layoutResponse = await _client.GetAsync("/storage/layout");
        var layout = await layoutResponse.Content.ReadFromJsonAsync<SearchableStorageLayout>();
        layout.Should().NotBeNull();
        layout!.IndexSchemaProtocolVersion.Should().Be(1);

        var rebuild = await _client.PostAsync(
            "/storage/index-schemas/vacancies/rebuild",
            content: null);
        rebuild.StatusCode.Should().Be(HttpStatusCode.OK);
        (await rebuild.Content.ReadFromJsonAsync<SearchableStorageIndexSchemaStatus>())!
            .Fingerprint.Should().Be(status.Fingerprint);
    }

    [Fact]
    public async Task FacetEndpointsExposePagingExactnessAndFilteredAggregates()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var firstCity = $"Facet-A-{suffix}";
        var secondCity = $"Facet-B-{suffix}";
        var firstId = $"facet-first-{suffix}";
        var secondId = $"facet-second-{suffix}";
        var thirdId = $"facet-third-{suffix}";
        const int isolationSalary = int.MaxValue - 1;

        await PutAsync(firstId, firstCity, isolationSalary);
        await PutAsync(secondId, firstCity, int.MaxValue);
        await PutAsync(thirdId, secondCity, int.MaxValue);

        try
        {
            var values = new List<string>();
            string? continuation = null;
            do
            {
                var path = "/vacancies/facets/cities?pageSize=1";
                if (continuation is not null)
                {
                    path += $"&continuation={Uri.EscapeDataString(continuation)}";
                }

                var response = await _client.GetAsync(path);
                response.StatusCode.Should().Be(HttpStatusCode.OK);
                var page = await response.Content.ReadFromJsonAsync<DistinctCityFacetResponse>();
                page.Should().NotBeNull();
                values.AddRange(page!.Values);
                continuation = page.ContinuationToken;
            }
            while (continuation is not null);

            values.Should().Contain([firstCity, secondCity]);
            values.Should().BeInAscendingOrder(StringComparer.Ordinal);

            var countsResponse = await _client.GetAsync(
                $"/vacancies/facets/cities/top?topN=2&accuracy=Exact&minimumSalary={isolationSalary}");
            countsResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var counts = await countsResponse.Content.ReadFromJsonAsync<CityFacetCountsResponse>();
            counts.Should().NotBeNull();
            counts!.IsExact.Should().BeTrue();
            counts.MaximumOmittedCount.Should().Be(0);
            counts.Items.Should().Equal(
                new CityFacetValueCountResponse(firstCity, 2),
                new CityFacetValueCountResponse(secondCity, 1));

            var approximateResponse = await _client.GetAsync(
                $"/vacancies/facets/cities/top?topN=1&accuracy=Approximate&minimumSalary={isolationSalary}");
            approximateResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var approximate = await approximateResponse.Content
                .ReadFromJsonAsync<CityFacetCountsResponse>();
            approximate.Should().NotBeNull();
            approximate!.Items.Should().ContainSingle();
            approximate.Items[0].Should().Be(new CityFacetValueCountResponse(firstCity, 2));
            approximate.MaximumOmittedCount.Should().BeGreaterThanOrEqualTo(1);
            if (approximate.IsExact)
            {
                approximate.MaximumOmittedCount.Should().Be(1);
            }

            var minMaxResponse = await _client.GetAsync(
                $"/vacancies/facets/salaries/min-max?city={Uri.EscapeDataString(firstCity)}");
            minMaxResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var minMax = await minMaxResponse.Content.ReadFromJsonAsync<SalaryFacetMinMaxResponse>();
            minMax.Should().Be(new SalaryFacetMinMaxResponse(isolationSalary, int.MaxValue));

            var emptyResponse = await _client.GetAsync(
                $"/vacancies/facets/salaries/min-max?city={Uri.EscapeDataString($"Missing-{suffix}")}");
            emptyResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var empty = await emptyResponse.Content.ReadFromJsonAsync<SalaryFacetMinMaxResponse>();
            empty.Should().Be(new SalaryFacetMinMaxResponse(null, null));
        }
        finally
        {
            await _client.DeleteAsync($"/vacancies/{firstId}");
            await _client.DeleteAsync($"/vacancies/{secondId}");
            await _client.DeleteAsync($"/vacancies/{thirdId}");
        }
    }

    [Fact]
    public async Task LayoutEndpointExplainsThePersistedVirtualRoutingMap()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var id = $"layout-{suffix}";
        await PutAsync(id, $"Helsinki-{suffix}", int.MaxValue);

        try
        {
            var response = await _client.GetAsync("/storage/layout");

            response.StatusCode.Should().Be(HttpStatusCode.OK);
            var layout = await response.Content.ReadFromJsonAsync<SearchableStorageLayout>();
            layout.Should().NotBeNull();
            layout!.Epoch.Should().BeGreaterThanOrEqualTo(1);
            layout.InitialPartitionCount.Should().Be(8);
            layout.VirtualSlotCount.Should().Be(64);
            layout.Partitions.Should().NotBeEmpty();
            layout.Partitions.Sum(static partition => partition.SlotCount).Should().Be(64);
            layout.Partitions.Select(static partition => partition.PartitionIndex)
                .Should().OnlyHaveUniqueItems();
        }
        finally
        {
            await _client.DeleteAsync($"/vacancies/{id}");
        }
    }

    [Fact]
    public async Task MovementEndpointsExposeManualAbortExecuteAndRebalanceWorkflow()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var initializerId = $"movement-initializer-{suffix}";
        await PutAsync(initializerId, $"Initializer-{suffix}", int.MaxValue);

        string? movedId = null;
        var restoredInitialShape = false;
        try
        {
            var enableResponse = await _client.PostAsync("/storage/movement/enable", content: null);
            enableResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var enabled = await enableResponse.Content.ReadFromJsonAsync<SearchableStorageLayout>();
            enabled.Should().NotBeNull();
            enabled!.MovementState.Should().Be(SearchableStorageMovementState.Enabled);
            enabled.MovementProtocolVersion.Should().Be(1);

            (await _client.GetAsync("/storage/moves/active"))
                .StatusCode.Should().Be(HttpStatusCode.NoContent);

            var rebalance = await GetRebalancePlanAsync(targetPartitionCount: 9);
            rebalance.RequiredMoveCount.Should().BeGreaterThan(0);
            rebalance.ActiveMove.Should().BeNull();
            rebalance.NextMove.Should().NotBeNull();
            var next = rebalance.NextMove!;

            movedId = FindVacancyIdInSlot(next.Slot, enabled.VirtualSlotCount, suffix);
            var movedCity = $"Moved-{suffix}";
            await PutAsync(movedId, movedCity, int.MaxValue - 1);

            var planned = await PostJsonAsync<SearchableStorageSlotMoveProgress>(
                "/storage/moves/plan",
                new StorageMovePlanRequest(next.Slot, next.TargetPartitionIndex));
            planned.Phase.Should().Be(SearchableStorageSlotMovePhase.Planned);
            planned.CanAbort.Should().BeTrue();

            var advanced = await PostWithoutBodyAsync<SearchableStorageSlotMoveProgress>(
                $"/storage/moves/{planned.MoveId}/advance");
            if (advanced.Phase == SearchableStorageSlotMovePhase.Planned)
            {
                // A newly introduced target owner is first upgraded/fenced in its own bounded
                // transition; the following call freezes the source and advances public progress.
                advanced = await PostWithoutBodyAsync<SearchableStorageSlotMoveProgress>(
                    $"/storage/moves/{planned.MoveId}/advance");
            }

            advanced.Phase.Should().Be(SearchableStorageSlotMovePhase.SourceFrozen);
            advanced.CanAbort.Should().BeTrue();

            var aborted = await PostWithoutBodyAsync<SearchableStorageSlotMoveProgress>(
                $"/storage/moves/{planned.MoveId}/abort");
            aborted.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
            aborted.IsComplete.Should().BeTrue();
            (await _client.GetAsync("/storage/moves/active"))
                .StatusCode.Should().Be(HttpStatusCode.NoContent);

            planned = await PostJsonAsync<SearchableStorageSlotMoveProgress>(
                "/storage/moves/plan",
                new StorageMovePlanRequest(next.Slot, next.TargetPartitionIndex));
            var completed = await PostWithoutBodyAsync<SearchableStorageSlotMoveProgress>(
                $"/storage/moves/{planned.MoveId}/execute");
            completed.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
            completed.IsComplete.Should().BeTrue();
            completed.ExportedRecordCount.Should().BeGreaterThanOrEqualTo(1);
            completed.DeletedRecordCount.Should().Be(completed.ExportedRecordCount);

            var pointResponse = await _client.GetAsync($"/vacancies/{movedId}");
            pointResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            (await pointResponse.Content.ReadFromJsonAsync<VacancyResponse>())
                .Should().Be(new VacancyResponse(movedId, movedCity, int.MaxValue - 1));
            (await GetSearchAsync(
                    $"/vacancies/search/by-city?city={Uri.EscapeDataString(movedCity)}"))
                .Ids.Should().ContainSingle().Which.Should().Be(movedId);

            var converged = await PostJsonAsync<SearchableStorageRebalancePlan>(
                "/storage/rebalance/execute",
                new StorageRebalanceRequest(9));
            converged.RequiredMoveCount.Should().Be(0);
            converged.NextMove.Should().BeNull();
            converged.ActiveMove.Should().BeNull();

            var layoutResponse = await _client.GetAsync("/storage/layout");
            var layout = await layoutResponse.Content.ReadFromJsonAsync<SearchableStorageLayout>();
            layout.Should().NotBeNull();
            layout!.Partitions.Should().HaveCount(9);
            layout.Partitions.Sum(static partition => partition.SlotCount).Should().Be(64);

            var restored = await PostJsonAsync<SearchableStorageRebalancePlan>(
                "/storage/rebalance/execute",
                new StorageRebalanceRequest(8));
            restored.RequiredMoveCount.Should().Be(0);
            restored.NextMove.Should().BeNull();
            restored.ActiveMove.Should().BeNull();
            restoredInitialShape = true;

            layoutResponse = await _client.GetAsync("/storage/layout");
            layout = await layoutResponse.Content.ReadFromJsonAsync<SearchableStorageLayout>();
            layout.Should().NotBeNull();
            layout!.Partitions.Should().HaveCount(8);
            layout.Partitions.Sum(static partition => partition.SlotCount).Should().Be(64);

            pointResponse = await _client.GetAsync($"/vacancies/{movedId}");
            pointResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            (await pointResponse.Content.ReadFromJsonAsync<VacancyResponse>())
                .Should().Be(new VacancyResponse(movedId, movedCity, int.MaxValue - 1));
            (await GetSearchAsync(
                    $"/vacancies/search/by-city?city={Uri.EscapeDataString(movedCity)}"))
                .Ids.Should().ContainSingle().Which.Should().Be(movedId);
            var extremaResponse = await _client.GetAsync(
                $"/vacancies/facets/salaries/min-max?city={Uri.EscapeDataString(movedCity)}");
            extremaResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var extrema = await extremaResponse.Content
                .ReadFromJsonAsync<SalaryFacetMinMaxResponse>();
            extrema.Should().Be(new SalaryFacetMinMaxResponse(int.MaxValue - 1, int.MaxValue - 1));
        }
        finally
        {
            // Restore the sample's balanced eight-owner shape so this stateful admin walkthrough is
            // independent from the other HTTP tests in the shared in-process host.
            if (!restoredInitialShape)
            {
                await _client.PostAsJsonAsync(
                    "/storage/rebalance/execute",
                    new StorageRebalanceRequest(8));
            }
            if (movedId is not null)
            {
                await _client.DeleteAsync($"/vacancies/{movedId}");
            }

            await _client.DeleteAsync($"/vacancies/{initializerId}");
        }
    }

    [Fact]
    public async Task VacancyLifecycleUpdatesPointStateAndBothIndexes()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var city = $"Helsinki-{suffix}";
        var firstId = $"first-{suffix}";
        var secondId = $"second-{suffix}";
        var outsideId = $"outside-{suffix}";

        await PutAsync(firstId, city, 6);
        await PutAsync(secondId, city, 7);
        await PutAsync(outsideId, $"Tampere-{suffix}", 9);

        var pointResponse = await _client.GetAsync($"/vacancies/{firstId}");
        pointResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var point = await pointResponse.Content.ReadFromJsonAsync<VacancyResponse>();
        point.Should().Be(new VacancyResponse(firstId, city, 6));

        var encodedCity = Uri.EscapeDataString($" {city} ");
        var cityMatches = await GetSearchAsync($"/vacancies/search/by-city?city={encodedCity}");
        cityMatches.Ids.Should().BeEquivalentTo([firstId, secondId]);

        var firstPage = await GetSearchPageAsync(
            $"/vacancies/search/by-city/page?city={encodedCity}&pageSize=1");
        firstPage.Ids.Should().ContainSingle();
        firstPage.ContinuationToken.Should().NotBeNullOrWhiteSpace();
        var secondPage = await GetSearchPageAsync(
            $"/vacancies/search/by-city/page?city={encodedCity}&pageSize=1"
            + $"&continuation={Uri.EscapeDataString(firstPage.ContinuationToken!)}");
        secondPage.Ids.Should().ContainSingle();
        secondPage.ContinuationToken.Should().BeNull();
        firstPage.Ids.Concat(secondPage.Ids)
            .Should().BeEquivalentTo([firstId, secondId]);

        var hydratedFirstPage = await GetHydratedSearchPageAsync(
            $"/vacancies/search/by-city/hydrated-page?city={encodedCity}&pageSize=1");
        hydratedFirstPage.Items.Should().ContainSingle();
        hydratedFirstPage.Items[0].Vacancy.Should().NotBeNull();
        var hydratedFirstVacancy = hydratedFirstPage.Items[0].Vacancy!;
        hydratedFirstVacancy.Id.Should().Be(hydratedFirstPage.Items[0].Id);
        hydratedFirstVacancy.City.Should().Be(city);
        hydratedFirstPage.ContinuationToken.Should().NotBeNullOrWhiteSpace();
        var hydratedSecondPage = await GetHydratedSearchPageAsync(
            $"/vacancies/search/by-city/hydrated-page?city={encodedCity}&pageSize=1"
            + $"&continuation={Uri.EscapeDataString(hydratedFirstPage.ContinuationToken!)}");
        hydratedSecondPage.Items.Should().ContainSingle();
        hydratedSecondPage.Items[0].Vacancy.Should().NotBeNull();
        hydratedSecondPage.Items[0].Vacancy!.City.Should().Be(city);
        hydratedSecondPage.ContinuationToken.Should().BeNull();
        hydratedFirstPage.Items.Concat(hydratedSecondPage.Items)
            .Select(static item => item.Id)
            .Should().BeEquivalentTo([firstId, secondId]);

        var salaryMatches = await GetSearchAsync(
            "/vacancies/search/by-salary?lower=5&upper=8&includeLower=false&includeUpper=false");
        salaryMatches.Ids.Should().BeEquivalentTo([firstId, secondId]);

        var deleteResponse = await _client.DeleteAsync($"/vacancies/{firstId}");
        deleteResponse.StatusCode.Should().Be(HttpStatusCode.NoContent);
        (await _client.GetAsync($"/vacancies/{firstId}"))
            .StatusCode.Should().Be(HttpStatusCode.NotFound);

        cityMatches = await GetSearchAsync($"/vacancies/search/by-city?city={encodedCity}");
        cityMatches.Ids.Should().ContainSingle().Which.Should().Be(secondId);
    }

    [Theory]
    [InlineData("PUT", "/vacancies/blank-city", "{\"city\":\" \",\"salary\":1}")]
    [InlineData("PUT", "/vacancies/negative-salary", "{\"city\":\"Helsinki\",\"salary\":-1}")]
    [InlineData("GET", "/vacancies/search/by-salary?lower=8&upper=5", null)]
    [InlineData("GET", "/vacancies/search/by-city/hydrated-page?city=Helsinki&pageSize=0", null)]
    [InlineData("GET", "/vacancies/facets/cities?pageSize=0", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?topN=0", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?topN=129", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?accuracy=999", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?minimumSalary=-1", null)]
    [InlineData("GET", "/vacancies/facets/salaries/min-max?city=%20", null)]
    [InlineData("POST", "/storage/moves/plan", "{\"slot\":-1,\"targetPartitionIndex\":0}")]
    [InlineData("POST", "/storage/moves/plan", "{\"slot\":0,\"targetPartitionIndex\":-1}")]
    [InlineData("GET", "/storage/rebalance/plan?targetPartitionCount=0", null)]
    [InlineData("POST", "/storage/rebalance/execute", "{\"targetPartitionCount\":0}")]
    public async Task InvalidRequestsReturnBadRequest(string method, string path, string? body)
    {
        using var request = new HttpRequestMessage(new HttpMethod(method), path);
        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        var response = await _client.SendAsync(request);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    private async Task PutAsync(string id, string city, int salary)
    {
        var response = await _client.PutAsJsonAsync(
            $"/vacancies/{id}",
            new VacancyWriteRequest(city, salary));
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    private async Task<SearchResponse> GetSearchAsync(string path)
    {
        var response = await _client.GetAsync(path);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await response.Content.ReadFromJsonAsync<SearchResponse>();
        result.Should().NotBeNull();
        return result!;
    }

    private async Task<SearchPageResponse> GetSearchPageAsync(string path)
    {
        var response = await _client.GetAsync(path);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await response.Content.ReadFromJsonAsync<SearchPageResponse>();
        result.Should().NotBeNull();
        return result!;
    }

    private async Task<HydratedSearchPageResponse> GetHydratedSearchPageAsync(string path)
    {
        var response = await _client.GetAsync(path);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await response.Content.ReadFromJsonAsync<HydratedSearchPageResponse>();
        result.Should().NotBeNull();
        return result!;
    }

    private async Task<SearchableStorageRebalancePlan> GetRebalancePlanAsync(
        int targetPartitionCount)
    {
        var response = await _client.GetAsync(
            $"/storage/rebalance/plan?targetPartitionCount={targetPartitionCount}");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await response.Content.ReadFromJsonAsync<SearchableStorageRebalancePlan>();
        result.Should().NotBeNull();
        return result!;
    }

    private async Task<T> PostJsonAsync<T>(string path, object body)
    {
        var response = await _client.PostAsJsonAsync(path, body);
        response.StatusCode.Should().Be(
            HttpStatusCode.OK,
            "the admin endpoint returned {0}",
            await response.Content.ReadAsStringAsync());
        var result = await response.Content.ReadFromJsonAsync<T>();
        result.Should().NotBeNull();
        return result!;
    }

    private async Task<T> PostWithoutBodyAsync<T>(string path)
    {
        var response = await _client.PostAsync(path, content: null);
        response.StatusCode.Should().Be(
            HttpStatusCode.OK,
            "the admin endpoint returned {0}",
            await response.Content.ReadAsStringAsync());
        var result = await response.Content.ReadFromJsonAsync<T>();
        result.Should().NotBeNull();
        return result!;
    }

    private string FindVacancyIdInSlot(int slot, int virtualSlotCount, string suffix)
    {
        var grainFactory = _factory.Services.GetRequiredService<IGrainFactory>();
        for (var attempt = 0; attempt < 10_000; attempt++)
        {
            var id = $"movement-{suffix}-{attempt}";
            var grainId = grainFactory.GetGrain<IVacancyGrain>(id).GetGrainId();
            var candidate = (int)((uint)grainId.GetUniformHashCode() % (uint)virtualSlotCount);
            if (candidate == slot)
            {
                return id;
            }
        }

        throw new InvalidOperationException("Could not generate a sample vacancy id for the planned slot.");
    }

    private sealed record ApiDescription(string Name, string Storage, IReadOnlyList<string> Endpoints);

    private sealed record VacancyWriteRequest(string City, int Salary);

    private sealed record VacancyResponse(string Id, string City, int Salary);

    private sealed record StorageMovePlanRequest(int Slot, int TargetPartitionIndex);

    private sealed record StorageRebalanceRequest(int TargetPartitionCount);

    private sealed record SearchResponse(IReadOnlyList<string> Ids);

    private sealed record SearchPageResponse(IReadOnlyList<string> Ids, string? ContinuationToken);

    private sealed record HydratedSearchPageItemResponse(string Id, VacancyResponse? Vacancy);

    private sealed record HydratedSearchPageResponse(
        IReadOnlyList<HydratedSearchPageItemResponse> Items,
        string? ContinuationToken);

    private sealed record DistinctCityFacetResponse(
        IReadOnlyList<string> Values,
        string? ContinuationToken);

    private sealed record CityFacetValueCountResponse(string Value, long Count);

    private sealed record CityFacetCountsResponse(
        IReadOnlyList<CityFacetValueCountResponse> Items,
        bool IsExact,
        long MaximumOmittedCount);

    private sealed record SalaryFacetMinMaxResponse(int? Minimum, int? Maximum);
}
