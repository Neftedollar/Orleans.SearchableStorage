using Microsoft.Extensions.DependencyInjection;

namespace Orleans.SearchableStorage.ApiSample;

internal static class VacancySearchEndpoints
{
    internal const int HydrationConcurrencyLimit = 16;

    public static async Task<IResult> FindByCityAsync(
        string city,
        [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageQueryClient search,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(city))
        {
            return ValidationError(nameof(city), "A city is required.");
        }

        var normalizedCity = city.Trim();
        var matches = await search
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == normalizedCity)
            .ToGrainIdsAsync(cancellationToken);
        return Results.Ok(ToSearchResponse(matches));
    }

    public static async Task<IResult> FindByCityPageAsync(
        string city,
        int? pageSize,
        string? continuation,
        [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageQueryClient search,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(city))
        {
            return ValidationError(nameof(city), "A city is required.");
        }

        var effectivePageSize = pageSize ?? SearchableStorageQueryOptions.DefaultPageSize;
        if (effectivePageSize <= 0
            || effectivePageSize > SearchableStorageQueryOptions.MaximumPageSize)
        {
            return ValidationError(
                nameof(pageSize),
                $"Page size must be between 1 and {SearchableStorageQueryOptions.MaximumPageSize}.");
        }

        var normalizedCity = city.Trim();
        var page = await search
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == normalizedCity)
            .ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(effectivePageSize, continuation),
                cancellationToken);
        return Results.Ok(new SearchPageResponse(
            page.Items.Select(static grainId => grainId.Key.ToString()).ToArray(),
            page.ContinuationToken));
    }

    public static async Task<IResult> FindHydratedByCityPageAsync(
        string city,
        int? pageSize,
        string? continuation,
        [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageQueryClient search,
        IGrainFactory grainFactory,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(city))
        {
            return ValidationError(nameof(city), "A city is required.");
        }

        var effectivePageSize = pageSize ?? SearchableStorageQueryOptions.DefaultPageSize;
        if (effectivePageSize <= 0
            || effectivePageSize > SearchableStorageQueryOptions.MaximumPageSize)
        {
            return ValidationError(
                nameof(pageSize),
                $"Page size must be between 1 and {SearchableStorageQueryOptions.MaximumPageSize}.");
        }

        var normalizedCity = city.Trim();
        var page = await search
            .Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == normalizedCity)
            .ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(effectivePageSize, continuation),
                cancellationToken);

        // Searchable storage deliberately returns identities, not application state. Hydrate only
        // this bounded page through the owning grains so their normal authorization and domain
        // behavior remain in the read path. A vacancy can change or disappear after index lookup;
        // a null value exposes that race instead of pretending this is a distributed snapshot.
        var items = await HydratePageAsync(
            page.Items,
            async (grainId, hydrationCancellation) =>
            {
                var id = grainId.Key.ToString();
                var state = await grainFactory
                    .GetGrain<IVacancyGrain>(id)
                    .GetAsync()
                    .WaitAsync(hydrationCancellation);
                return new HydratedSearchPageItemResponse(
                    id,
                    state is null ? null : new VacancyResponse(id, state.City, state.Salary));
            },
            cancellationToken);

        return Results.Ok(new HydratedSearchPageResponse(items, page.ContinuationToken));
    }

    internal static async Task<TResult[]> HydratePageAsync<TSource, TResult>(
        IReadOnlyList<TSource> source,
        Func<TSource, CancellationToken, ValueTask<TResult>> hydrate,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(hydrate);

        var results = new TResult[source.Count];
        await Parallel.ForEachAsync(
            Enumerable.Range(0, source.Count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = HydrationConcurrencyLimit,
                CancellationToken = cancellationToken,
            },
            async (index, itemCancellation) =>
            {
                results[index] = await hydrate(source[index], itemCancellation);
            });
        return results;
    }

    public static async Task<IResult> FindBySalaryAsync(
        int lower,
        int upper,
        bool? includeLower,
        bool? includeUpper,
        [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageQueryClient search,
        CancellationToken cancellationToken)
    {
        if (lower > upper)
        {
            return ValidationError(nameof(lower), "The lower bound must not exceed the upper bound.");
        }

        IQueryable<VacancyState> query = search.Query<VacancyState>(VacancyGrain.StateName);
        query = includeLower ?? true
            ? query.Where(state => state.Salary >= lower)
            : query.Where(state => state.Salary > lower);
        query = includeUpper ?? true
            ? query.Where(state => state.Salary <= upper)
            : query.Where(state => state.Salary < upper);

        var matches = await query.ToGrainIdsAsync(cancellationToken);
        return Results.Ok(ToSearchResponse(matches));
    }

    public static async Task<IResult> GetDistinctCitiesAsync(
        int? pageSize,
        string? continuation,
        [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageQueryClient search,
        CancellationToken cancellationToken)
    {
        var effectivePageSize = pageSize ?? SearchableStorageQueryOptions.DefaultPageSize;
        if (effectivePageSize <= 0
            || effectivePageSize > SearchableStorageQueryOptions.MaximumPageSize)
        {
            return ValidationError(
                nameof(pageSize),
                $"Page size must be between 1 and {SearchableStorageQueryOptions.MaximumPageSize}.");
        }

        var page = await search
            .Query<VacancyState>(VacancyGrain.StateName)
            .ToDistinctFacetValuePageAsync(
                state => state.City,
                new SearchableStorageFacetPageRequest(effectivePageSize, continuation),
                cancellationToken);
        return Results.Ok(new DistinctCityFacetResponse(page.Items, page.ContinuationToken));
    }

    public static async Task<IResult> GetTopCitiesAsync(
        int? topN,
        SearchableStorageFacetAccuracy? accuracy,
        int? minimumSalary,
        [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageQueryClient search,
        CancellationToken cancellationToken)
    {
        var effectiveTopN = topN ?? 10;
        if (effectiveTopN <= 0
            || effectiveTopN > SearchableStorageQueryOptions.DefaultFacetTopN)
        {
            return ValidationError(
                nameof(topN),
                $"Top N must be between 1 and {SearchableStorageQueryOptions.DefaultFacetTopN}.");
        }

        if (minimumSalary < 0)
        {
            return ValidationError(nameof(minimumSalary), "Minimum salary must not be negative.");
        }

        var effectiveAccuracy = accuracy ?? SearchableStorageFacetAccuracy.Exact;
        if (!Enum.IsDefined(effectiveAccuracy))
        {
            return ValidationError(
                nameof(accuracy),
                "Accuracy must be Exact or Approximate.");
        }

        IQueryable<VacancyState> query = search.Query<VacancyState>(VacancyGrain.StateName);
        if (minimumSalary is { } lowerBound)
        {
            query = query.Where(state => state.Salary >= lowerBound);
        }

        var facet = await query.ToFacetValueCountsAsync(
            state => state.City,
            new SearchableStorageFacetRequest(effectiveTopN, effectiveAccuracy),
            cancellationToken);
        return Results.Ok(new CityFacetCountsResponse(
            facet.Items
                .Select(static item => new CityFacetValueCountResponse(item.Value, item.Count))
                .ToArray(),
            facet.IsExact,
            facet.MaximumOmittedCount));
    }

    public static async Task<IResult> GetSalaryMinMaxAsync(
        string? city,
        [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageQueryClient search,
        CancellationToken cancellationToken)
    {
        if (city is not null && string.IsNullOrWhiteSpace(city))
        {
            return ValidationError(nameof(city), "City must not be blank when supplied.");
        }

        IQueryable<VacancyState> query = search.Query<VacancyState>(VacancyGrain.StateName);
        if (city is not null)
        {
            var normalizedCity = city.Trim();
            query = query.Where(state => state.City == normalizedCity);
        }

        var facet = await query.ToFacetMinMaxAsync(
            state => state.Salary,
            cancellationToken);
        return Results.Ok(new SalaryFacetMinMaxResponse(facet?.Minimum, facet?.Maximum));
    }

    private static SearchResponse ToSearchResponse(IEnumerable<GrainId> matches)
    {
        return new SearchResponse(matches.Select(static grainId => grainId.Key.ToString()).ToArray());
    }

    private static IResult ValidationError(string field, string message)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            [field] = [message],
        });
    }
}
