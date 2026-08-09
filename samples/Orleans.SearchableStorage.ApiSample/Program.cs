using Orleans.SearchableStorage;
using Orleans.SearchableStorage.ApiSample;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseOrleans(siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryGrainStorage(SearchableStorageConstants.PhysicalStorageProviderName);
    siloBuilder.AddSearchableGrainStorage(
        VacancyGrain.StorageProviderName,
        options => options.PartitionCount = 8);
});

var app = builder.Build();

app.MapGet("/", () => Results.Ok(SampleMetadata.Description));

app.MapPut("/vacancies/{id}", PutVacancyAsync);
app.MapGet("/vacancies/{id}", GetVacancyAsync);
app.MapDelete("/vacancies/{id}", DeleteVacancyAsync);
app.MapGet("/vacancies/search/by-city", VacancySearchEndpoints.FindByCityAsync);
app.MapGet("/vacancies/search/by-salary", VacancySearchEndpoints.FindBySalaryAsync);

app.Run();

static async Task<IResult> PutVacancyAsync(
    string id,
    VacancyWriteRequest request,
    IGrainFactory grainFactory)
{
    if (string.IsNullOrWhiteSpace(id))
    {
        return ValidationError(nameof(id), "A vacancy id is required.");
    }

    if (string.IsNullOrWhiteSpace(request.City))
    {
        return ValidationError(nameof(request.City), "A city is required.");
    }

    if (request.Salary < 0)
    {
        return ValidationError(nameof(request.Salary), "Salary must not be negative.");
    }

    var state = new VacancyState
    {
        City = request.City.Trim(),
        Salary = request.Salary,
    };
    await grainFactory.GetGrain<IVacancyGrain>(id).SetAsync(state);
    return Results.Ok(new VacancyResponse(id, state.City, state.Salary));
}

static async Task<IResult> GetVacancyAsync(string id, IGrainFactory grainFactory)
{
    var state = await grainFactory.GetGrain<IVacancyGrain>(id).GetAsync();
    return state is null
        ? Results.NotFound()
        : Results.Ok(new VacancyResponse(id, state.City, state.Salary));
}

static async Task<IResult> DeleteVacancyAsync(string id, IGrainFactory grainFactory)
{
    await grainFactory.GetGrain<IVacancyGrain>(id).ClearAsync();
    return Results.NoContent();
}

static IResult ValidationError(string field, string message)
{
    return Results.ValidationProblem(new Dictionary<string, string[]>
    {
        [field] = [message],
    });
}

internal sealed record VacancyWriteRequest(string City, int Salary);

internal sealed record VacancyResponse(string Id, string City, int Salary);

internal sealed record SearchResponse(IReadOnlyList<string> Ids);

internal sealed record ApiDescription(string Name, string Storage, IReadOnlyList<string> Endpoints);

internal static class SampleMetadata
{
    public static ApiDescription Description { get; } = new(
        "Orleans.SearchableStorage API sample",
        "Orleans in-memory persistence",
        [
            "PUT /vacancies/{id}",
            "GET /vacancies/{id}",
            "DELETE /vacancies/{id}",
            "GET /vacancies/search/by-city?city={city}",
            "GET /vacancies/search/by-salary?lower={value}&upper={value}",
        ]);
}

/// <summary>
/// Provides the entry point used by ASP.NET Core integration tests.
/// </summary>
public partial class Program;
