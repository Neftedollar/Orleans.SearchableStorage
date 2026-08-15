using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Npgsql;
using Orleans.Hosting;
using Orleans.SearchableStorage;
using Orleans.SearchableStorage.Qualification.SkyPulse.DurableIngestion;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;
using Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;
using Orleans.SearchableStorage.Qualification.SkyPulse.TransitionPlanning;
using Orleans.SearchableStorage.Qualification.SkyPulse.Web;

var builder = WebApplication.CreateBuilder(args);
var continuationKey = RandomNumberGenerator.GetBytes(32);
var runtimeConfiguration = SkyPulseApplicationConfiguration.Resolve(builder.Configuration);

builder.Host.UseOrleans(siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryGrainStorage(SearchableStorageConstants.PhysicalStorageProviderName);
    siloBuilder.AddSearchableIndex(
        SkyPulseIndexContract.ProviderName,
        options =>
        {
            options.PartitionCount = SkyPulseIndexContract.PartitionCount;
            options.VirtualSlotTargetCount = SkyPulseIndexContract.VirtualSlotTargetCount;
            options.Query.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
                "skypulse-process-ephemeral",
                continuationKey);
        });
    siloBuilder.AddSearchableStorageState<AccountIndexState>(
        SkyPulseIndexContract.ProviderName,
        SkyPulseIndexContract.StateName,
        SkyPulseIndexContract.ApplicationSchemaVersion);
});

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddOptions<QuerySessionOptions>()
    .Bind(builder.Configuration.GetSection("QuerySessions"))
    .Validate(static options =>
    {
        options.Validate();
        return true;
    })
    .ValidateOnStart();
builder.Services.AddSingleton<ISkyPulsePageQuery, SearchableStorageSkyPulsePageQuery>();
builder.Services.AddSingleton<IProjectionIndexWriter, SearchableStorageProjectionIndexWriter>();
builder.Services.AddSingleton<QuerySessionRegistry>();
builder.Services.AddHostedService<SkyPulseIndexSchemaBootstrapService>();

if (runtimeConfiguration.Mode == SkyPulseRuntimeMode.LocalFunctional)
{
    builder.Services.AddSingleton<InMemoryProjectionStore>();
    builder.Services.AddSingleton<IProjectionStore>(static services =>
        services.GetRequiredService<InMemoryProjectionStore>());
    builder.Services.AddSingleton<ProjectionUpdatePublisher>();
    builder.Services.AddSingleton<IProjectionReadiness, LocalFunctionalProjectionReadiness>();
}
else
{
    var durableConfiguration = runtimeConfiguration.Durable
        ?? throw new InvalidOperationException("Durable configuration was not resolved.");
    var connectionString = runtimeConfiguration.PostgreSqlConnectionString
        ?? throw new InvalidOperationException("The durable PostgreSQL connection string was not resolved.");
    builder.Services.AddSingleton(durableConfiguration);
    builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
    builder.Services.AddSingleton<PostgreSqlSchemaManager>();
    builder.Services.AddSingleton<PostgreSqlRuntimeManifestStore>();
    builder.Services.AddSingleton<PostgreSqlCorpusCapacityStore>();
    builder.Services.AddSingleton<PostgreSqlProjectionRuntimeStore>();
    builder.Services.AddSingleton<PostgreSqlPlanningStore>();
    builder.Services.AddSingleton<PostgreSqlIngestionStore>();
    builder.Services.AddSingleton<PostgreSqlDispatchStore>();
    builder.Services.AddSingleton<PostgreSqlLifecycleOrchestrator>();
    builder.Services.AddSingleton<IDurableTapBackend, PostgreSqlDurableTapBackend>();
    builder.Services.AddSingleton(static services => services
        .GetRequiredService<SkyPulseDurableConfiguration>()
        .CreateTapRepositoryProvisionerOptions());
    builder.Services.AddSingleton<PrivateTapRepositoryProvisioner>();
    builder.Services.AddSingleton<ITapRepositoryProvisioner>(static services =>
        services.GetRequiredService<PrivateTapRepositoryProvisioner>());
    builder.Services.AddSingleton<MonotonicCorpusAdmission>();
    builder.Services.AddSingleton<DurableCorpusCapacityManager>();
    builder.Services.AddSingleton<IDurableProjectionDispatchStore, PostgreSqlDurableProjectionDispatchStore>();
    builder.Services.AddSingleton<IRollingWindowRecalculationStore, PostgreSqlRollingWindowRecalculationStore>();
    builder.Services.AddSingleton<IProjectionStore, PostgreSqlPublishedProjectionStore>();
    builder.Services.AddSingleton<IRuntimeProjectionIndexWriter, RuntimeProjectionIndexWriterAdapter>();
    builder.Services.AddSingleton<IFatalProcessTerminator, EnvironmentFatalProcessTerminator>();
    builder.Services.AddSingleton(static services =>
    {
        var configuration = services.GetRequiredService<SkyPulseDurableConfiguration>();
        return new DurableProjectionRuntime(
            services.GetRequiredService<IDurableProjectionDispatchStore>(),
            services.GetRequiredService<IRuntimeProjectionIndexWriter>(),
            services.GetRequiredService<IFatalProcessTerminator>(),
            configuration.CreateRuntimeOptions(),
            services.GetRequiredService<TimeProvider>());
    });
    builder.Services.AddSingleton(static services =>
    {
        var configuration = services.GetRequiredService<SkyPulseDurableConfiguration>();
        return new RollingWindowRecalculationWorker(
            services.GetRequiredService<IRollingWindowRecalculationStore>(),
            configuration.CreateRecalculationOptions(),
            services.GetRequiredService<TimeProvider>());
    });
    builder.Services.AddSingleton<DurableProjectionHostedService>();
    builder.Services.AddSingleton<IHostedService>(static services =>
        services.GetRequiredService<DurableProjectionHostedService>());
    builder.Services.AddSingleton<DurableTapHostedService>();
    builder.Services.AddSingleton<IHostedService>(static services =>
        services.GetRequiredService<DurableTapHostedService>());
    builder.Services.AddSingleton<DurableCompositeReadiness>();
    builder.Services.AddSingleton<IProjectionReadiness>(static services =>
        services.GetRequiredService<DurableCompositeReadiness>());
}

builder.Services.AddHostedService<QuerySessionCleanupService>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

var projectionReadiness = app.Services.GetRequiredService<IProjectionReadiness>();
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments("/api") && !projectionReadiness.IsReady)
    {
        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        await context.Response.WriteAsJsonAsync(
                new
                {
                    error = "projection_not_ready",
                    detail = projectionReadiness.Status,
                },
                context.RequestAborted)
            .ConfigureAwait(false);
        return;
    }

    await next(context).ConfigureAwait(false);
});

app.MapGet(
    "/health",
    (IProjectionReadiness readiness) =>
    {
        var response = new
        {
            status = readiness.IsReady ? "ready" : "not_ready",
            mode = runtimeConfiguration.Mode.ToString(),
            projection = readiness.Status,
        };
        return readiness.IsReady
            ? Results.Ok(response)
            : Results.Json(response, statusCode: StatusCodes.Status503ServiceUnavailable);
    });

if (runtimeConfiguration.Mode == SkyPulseRuntimeMode.Durable)
{
    app.MapGet(
        "/api/corpus-capacity",
        async (DurableCorpusCapacityManager capacity, CancellationToken cancellationToken)
            => Results.Ok(await capacity.ReadViewAsync(cancellationToken).ConfigureAwait(false)));

    app.MapPost(
        "/api/corpus-capacity/{profileId}",
        async (
            string profileId,
            HttpContext context,
            DurableCorpusCapacityManager capacity,
            SkyPulseDurableConfiguration configuration,
            CancellationToken cancellationToken) =>
        {
            if (!CorpusGrowthAuthorization.IsAuthorized(
                    context.Request,
                    configuration.CorpusGrowthAdminToken))
            {
                return Results.Unauthorized();
            }

            var result = await capacity
                .RequestGrowthAsync(profileId, cancellationToken)
                .ConfigureAwait(false);
            return result.Outcome switch
            {
                RuntimeCorpusGrowthRequestOutcome.Accepted
                    or RuntimeCorpusGrowthRequestOutcome.AlreadyRequested
                    => Results.Json(result, statusCode: StatusCodes.Status202Accepted),
                RuntimeCorpusGrowthRequestOutcome.AlreadyActive => Results.Ok(result),
                RuntimeCorpusGrowthRequestOutcome.UnknownProfile => Results.NotFound(result),
                RuntimeCorpusGrowthRequestOutcome.GrowthInProgress
                    or RuntimeCorpusGrowthRequestOutcome.NonMonotonic
                    => Results.Conflict(result),
                _ => Results.Problem(
                    title: "The corpus-growth request returned an unknown outcome.",
                    statusCode: StatusCodes.Status500InternalServerError),
            };
        });
}

app.MapPost(
    "/api/query-sessions",
    async (SkyPulseQueryRequest request, QuerySessionRegistry sessions, CancellationToken cancellationToken) =>
    {
        try
        {
            return Results.Ok(await sessions.CreateAsync(request, cancellationToken).ConfigureAwait(false));
        }
        catch (SearchableStorageInvalidContinuationTokenException exception)
        {
            return BadRequest(exception.Message);
        }
        catch (SearchableStorageStaleContinuationTokenException exception)
        {
            return Results.Conflict(new
            {
                error = "stale_continuation",
                detail = exception.Message,
            });
        }
        catch (ArgumentException exception)
        {
            return BadRequest(exception.Message);
        }
        catch (InvalidOperationException exception)
        {
            return Results.Problem(
                title: "The query adapter rejected the bounded query.",
                detail: exception.Message,
                statusCode: StatusCodes.Status502BadGateway);
        }
    });

app.MapPost(
    "/api/query-sessions/{sessionId:guid}/refresh",
    async (Guid sessionId, QuerySessionRegistry sessions, CancellationToken cancellationToken) =>
    {
        try
        {
            var snapshot = await sessions.RefreshAsync(sessionId, cancellationToken).ConfigureAwait(false);
            return snapshot is null ? Results.NotFound() : Results.Ok(snapshot);
        }
        catch (SearchableStorageInvalidContinuationTokenException exception)
        {
            return BadRequest(exception.Message);
        }
        catch (SearchableStorageStaleContinuationTokenException exception)
        {
            return Results.Conflict(new
            {
                error = "stale_continuation",
                detail = exception.Message,
            });
        }
        catch (InvalidOperationException exception)
        {
            return Results.Conflict(new
            {
                error = "resync_required",
                detail = exception.Message,
            });
        }
    });

app.MapDelete(
    "/api/query-sessions/{sessionId:guid}",
    static (Guid sessionId, QuerySessionRegistry sessions)
        => sessions.Remove(sessionId) ? Results.NoContent() : Results.NotFound());

app.MapGet(
    "/api/query-sessions/{sessionId:guid}/events",
    async (
        Guid sessionId,
        HttpContext context,
        QuerySessionRegistry sessions,
        IOptions<QuerySessionOptions> options) =>
    {
        if (!sessions.TryGet(sessionId, out var session))
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        if (!session.TryAcquireEventReader())
        {
            context.Response.StatusCode = StatusCodes.Status409Conflict;
            await context.Response.WriteAsJsonAsync(
                    new { error = "event_reader_already_connected" },
                    context.RequestAborted)
                .ConfigureAwait(false);
            return;
        }

        var cancellationToken = context.RequestAborted;
        try
        {
            context.Response.Headers.CacheControl = "no-cache, no-store";
            context.Response.Headers.Append("X-Accel-Buffering", "no");
            context.Response.ContentType = "text/event-stream";

            await using var enumerator = session.ReadEventsAsync(cancellationToken)
                .GetAsyncEnumerator(cancellationToken);
            Task<bool>? pendingRead = null;

            while (!cancellationToken.IsCancellationRequested)
            {
                pendingRead ??= enumerator.MoveNextAsync().AsTask();
                var heartbeat = Task.Delay(options.Value.HeartbeatInterval, cancellationToken);
                var completed = await Task.WhenAny(pendingRead, heartbeat).ConfigureAwait(false);

                if (completed == heartbeat)
                {
                    sessions.Touch(session);
                    await WriteEventAsync(context.Response, "heartbeat", new { }, cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                if (!await pendingRead.ConfigureAwait(false))
                {
                    break;
                }

                var update = enumerator.Current;
                pendingRead = null;
                if (update.Kind == QuerySessionEventKind.ResyncRequired)
                {
                    await WriteEventAsync(
                            context.Response,
                            "resync",
                            new { reason = "Current-page membership changed or its bounded update buffer overflowed. Run the query again." },
                            cancellationToken)
                        .ConfigureAwait(false);
                    sessions.Remove(sessionId);
                    break;
                }

                await WriteEventAsync(context.Response, "projection", update.Projection!, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The browser closed the SSE connection.
        }
        finally
        {
            session.ReleaseEventReader();
        }
    });

app.Run();

static IResult BadRequest(string message)
    => Results.ValidationProblem(new Dictionary<string, string[]>
    {
        ["query"] = [message],
    });

static async Task WriteEventAsync<T>(
    HttpResponse response,
    string eventName,
    T value,
    CancellationToken cancellationToken)
{
    var json = JsonSerializer.Serialize(value, JsonSerializerOptions.Web);
    await response.WriteAsync($"event: {eventName}\ndata: {json}\n\n", cancellationToken)
        .ConfigureAwait(false);
    await response.Body.FlushAsync(cancellationToken).ConfigureAwait(false);
}

public partial class Program;
