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
        description.Endpoints.Should().HaveCount(10);
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
        options.Query.ContinuationProtection.CurrentKey.Should().NotBeNull();
        options.Query.ContinuationProtection.CurrentKey!.KeyId.Should().Be("api-sample-ephemeral");
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
            layout!.Epoch.Should().Be(1);
            layout.InitialPartitionCount.Should().Be(8);
            layout.VirtualSlotCount.Should().Be(64);
            layout.Partitions.Should().HaveCount(8)
                .And.OnlyContain(static partition => partition.SlotCount == 8);
            layout.Partitions.Select(static partition => partition.PartitionIndex)
                .Should().Equal(Enumerable.Range(0, 8));
        }
        finally
        {
            await _client.DeleteAsync($"/vacancies/{id}");
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
    [InlineData("GET", "/vacancies/facets/cities?pageSize=0", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?topN=0", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?topN=129", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?accuracy=999", null)]
    [InlineData("GET", "/vacancies/facets/cities/top?minimumSalary=-1", null)]
    [InlineData("GET", "/vacancies/facets/salaries/min-max?city=%20", null)]
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

    private sealed record ApiDescription(string Name, string Storage, IReadOnlyList<string> Endpoints);

    private sealed record VacancyWriteRequest(string City, int Salary);

    private sealed record VacancyResponse(string Id, string City, int Salary);

    private sealed record SearchResponse(IReadOnlyList<string> Ids);

    private sealed record SearchPageResponse(IReadOnlyList<string> Ids, string? ContinuationToken);

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
