using Microsoft.Extensions.DependencyInjection;

namespace Orleans.SearchableStorage.ApiSample;

internal static class VacancySearchEndpoints
{
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
