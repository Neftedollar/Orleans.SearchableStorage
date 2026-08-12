using System.Security.Cryptography;
using Microsoft.Extensions.DependencyInjection;
using Orleans.SearchableStorage;
using Orleans.SearchableStorage.ApiSample;

var builder = WebApplication.CreateBuilder(args);
var ephemeralContinuationKey = RandomNumberGenerator.GetBytes(32);

builder.Host.UseOrleans(siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryGrainStorage(SearchableStorageConstants.PhysicalStorageProviderName);
    siloBuilder.AddSearchableGrainStorage(
        VacancyGrain.StorageProviderName,
        options =>
        {
            options.PartitionCount = 8;
            options.VirtualSlotTargetCount = 64;
            options.JournalSegmentCapacity = 16;
            options.MaximumJournalReplayEntries = 256;
            options.CompactionThreshold = 64;
            // Development-only process key. Restarting the sample invalidates its continuations.
            // Production deployments must load one stable, shared provider-scoped secret.
            options.Query.ContinuationProtection.CurrentKey =
                new SearchableStorageContinuationKey(
                    "api-sample-ephemeral",
                    ephemeralContinuationKey);
        });
    siloBuilder.AddSearchableStorageState<VacancyState>(
        VacancyGrain.StorageProviderName,
        VacancyGrain.StateName,
        VacancyGrain.ApplicationSchemaVersion);
});
builder.Services.AddHostedService<VacancyIndexSchemaBootstrapService>();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(SampleMetadata.Description));

app.MapPut("/vacancies/{id}", PutVacancyAsync);
app.MapGet("/vacancies/{id}", GetVacancyAsync);
app.MapDelete("/vacancies/{id}", DeleteVacancyAsync);
app.MapGet("/vacancies/search/by-city", VacancySearchEndpoints.FindByCityAsync);
app.MapGet("/vacancies/search/by-city/page", VacancySearchEndpoints.FindByCityPageAsync);
app.MapGet(
    "/vacancies/search/by-city/hydrated-page",
    VacancySearchEndpoints.FindHydratedByCityPageAsync);
app.MapGet("/vacancies/search/by-salary", VacancySearchEndpoints.FindBySalaryAsync);
app.MapGet("/vacancies/facets/cities", VacancySearchEndpoints.GetDistinctCitiesAsync);
app.MapGet("/vacancies/facets/cities/top", VacancySearchEndpoints.GetTopCitiesAsync);
app.MapGet("/vacancies/facets/salaries/min-max", VacancySearchEndpoints.GetSalaryMinMaxAsync);
app.MapGet("/storage/layout", GetStorageLayoutAsync);
app.MapGet("/storage/index-schemas/vacancies", GetVacancyIndexSchemaAsync);
app.MapPost("/storage/index-schemas/vacancies/rebuild", RebuildVacancyIndexSchemaAsync);
app.MapPost("/storage/movement/enable", EnableStorageMovementAsync);
app.MapGet("/storage/moves/active", GetActiveStorageMoveAsync);
app.MapPost("/storage/moves/plan", PlanStorageMoveAsync);
app.MapPost("/storage/moves/{moveId:guid}/advance", AdvanceStorageMoveAsync);
app.MapPost("/storage/moves/{moveId:guid}/execute", ExecuteStorageMoveAsync);
app.MapPost("/storage/moves/{moveId:guid}/abort", AbortStorageMoveAsync);
app.MapGet("/storage/rebalance/plan", PlanStorageRebalanceAsync);
app.MapPost("/storage/rebalance/execute", ExecuteStorageRebalanceAsync);

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

static async Task<IResult> GetStorageLayoutAsync(
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    var layout = await storage.GetLayoutAsync(cancellationToken);
    return layout is null ? Results.NotFound() : Results.Ok(layout);
}

static async Task<IResult> GetVacancyIndexSchemaAsync(
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    return Results.Ok(await storage.GetIndexSchemaAsync<VacancyState>(
        VacancyGrain.StateName,
        cancellationToken));
}

static Task<IResult> RebuildVacancyIndexSchemaAsync(
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    return ExecuteAdminAsync(
        () => storage.RebuildIndexSchemaAsync<VacancyState>(
            VacancyGrain.StateName,
            cancellationToken));
}

static async Task<IResult> EnableStorageMovementAsync(
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    return await ExecuteAdminAsync(
        () => storage.EnableMovementAsync(cancellationToken));
}

static async Task<IResult> GetActiveStorageMoveAsync(
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    var move = await storage.GetMoveAsync(cancellationToken);
    return move is null ? Results.NoContent() : Results.Ok(move);
}

static async Task<IResult> PlanStorageMoveAsync(
    StorageMovePlanRequest request,
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    if (request.Slot < 0)
    {
        return ValidationError(nameof(request.Slot), "A virtual slot must not be negative.");
    }

    if (request.TargetPartitionIndex < 0)
    {
        return ValidationError(
            nameof(request.TargetPartitionIndex),
            "A target partition index must not be negative.");
    }

    return await ExecuteAdminAsync(
        () => storage.PlanMoveAsync(
            request.Slot,
            request.TargetPartitionIndex,
            cancellationToken));
}

static Task<IResult> AdvanceStorageMoveAsync(
    Guid moveId,
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    return ExecuteAdminAsync(() => storage.AdvanceMoveAsync(moveId, cancellationToken));
}

static Task<IResult> ExecuteStorageMoveAsync(
    Guid moveId,
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    return ExecuteAdminAsync(() => storage.ExecuteMoveAsync(moveId, cancellationToken));
}

static Task<IResult> AbortStorageMoveAsync(
    Guid moveId,
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    return ExecuteAdminAsync(() => storage.AbortMoveAsync(moveId, cancellationToken));
}

static async Task<IResult> PlanStorageRebalanceAsync(
    int targetPartitionCount,
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    if (targetPartitionCount <= 0)
    {
        return ValidationError(
            nameof(targetPartitionCount),
            "A target partition count must be positive.");
    }

    return await ExecuteAdminAsync(
        () => storage.PlanRebalanceAsync(targetPartitionCount, cancellationToken));
}

static async Task<IResult> ExecuteStorageRebalanceAsync(
    StorageRebalanceRequest request,
    [FromKeyedServices(VacancyGrain.StorageProviderName)] ISearchableStorageAdminClient storage,
    CancellationToken cancellationToken)
{
    if (request.TargetPartitionCount <= 0)
    {
        return ValidationError(
            nameof(request.TargetPartitionCount),
            "A target partition count must be positive.");
    }

    return await ExecuteAdminAsync(
        () => storage.ExecuteRebalanceAsync(request.TargetPartitionCount, cancellationToken));
}

static async Task<IResult> ExecuteAdminAsync<T>(Func<Task<T>> operation)
{
    try
    {
        return Results.Ok(await operation());
    }
    catch (ArgumentException exception)
    {
        return Results.BadRequest(new StorageAdminErrorResponse(exception.Message));
    }
    catch (InvalidOperationException exception)
    {
        return Results.Conflict(new StorageAdminErrorResponse(exception.Message));
    }
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

internal sealed record StorageMovePlanRequest(int Slot, int TargetPartitionIndex);

internal sealed record StorageRebalanceRequest(int TargetPartitionCount);

internal sealed record StorageAdminErrorResponse(string Message);

internal sealed record SearchResponse(IReadOnlyList<string> Ids);

internal sealed record SearchPageResponse(IReadOnlyList<string> Ids, string? ContinuationToken);

internal sealed record HydratedSearchPageItemResponse(string Id, VacancyResponse? Vacancy);

internal sealed record HydratedSearchPageResponse(
    IReadOnlyList<HydratedSearchPageItemResponse> Items,
    string? ContinuationToken);

internal sealed record DistinctCityFacetResponse(
    IReadOnlyList<string> Values,
    string? ContinuationToken);

internal sealed record CityFacetValueCountResponse(string Value, long Count);

internal sealed record CityFacetCountsResponse(
    IReadOnlyList<CityFacetValueCountResponse> Items,
    bool IsExact,
    long MaximumOmittedCount);

internal sealed record SalaryFacetMinMaxResponse(int? Minimum, int? Maximum);

internal sealed record ApiDescription(string Name, string Storage, IReadOnlyList<string> Endpoints);

internal sealed class VacancyIndexSchemaBootstrapService(IServiceProvider services)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var storage = services.GetRequiredKeyedService<ISearchableStorageAdminClient>(
            VacancyGrain.StorageProviderName);
        _ = await storage.RebuildIndexSchemaAsync<VacancyState>(
            VacancyGrain.StateName,
            cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

internal static class SampleMetadata
{
    public static ApiDescription Description { get; } = new(
        "Orleans.SearchableStorage API sample",
        "Journaled Orleans storage over in-memory physical persistence",
        [
            "PUT /vacancies/{id}",
            "GET /vacancies/{id}",
            "DELETE /vacancies/{id}",
            "GET /vacancies/search/by-city?city={city}",
            "GET /vacancies/search/by-city/page?city={city}&pageSize={size}",
            "GET /vacancies/search/by-city/hydrated-page?city={city}&pageSize={size}",
            "GET /vacancies/search/by-salary?lower={value}&upper={value}",
            "GET /vacancies/facets/cities?pageSize={size}",
            "GET /vacancies/facets/cities/top?topN={count}&accuracy={Exact|Approximate}",
            "GET /vacancies/facets/salaries/min-max?city={city}",
            "GET /storage/layout",
            "GET /storage/index-schemas/vacancies",
            "POST /storage/index-schemas/vacancies/rebuild",
            "POST /storage/movement/enable",
            "GET /storage/moves/active",
            "POST /storage/moves/plan",
            "POST /storage/moves/{moveId}/advance",
            "POST /storage/moves/{moveId}/execute",
            "POST /storage/moves/{moveId}/abort",
            "GET /storage/rebalance/plan?targetPartitionCount={count}",
            "POST /storage/rebalance/execute",
        ]);
}

/// <summary>
/// Provides the entry point used by ASP.NET Core integration tests.
/// </summary>
public partial class Program;
