namespace Orleans.SearchableStorage.Benchmarks;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && string.Equals(args[0], "validate-artifact", StringComparison.OrdinalIgnoreCase))
        {
            return await BenchmarkArtifactValidator.RunCommandAsync(args[1..]);
        }

        DriverOptions options;
        try
        {
            options = DriverOptions.Parse(args);
        }
        catch (CommandLineHelpException)
        {
            DriverOptions.PrintUsage(Console.Out);
            return 0;
        }
        catch (ArgumentException exception)
        {
            Console.Error.WriteLine(exception.Message);
            DriverOptions.PrintUsage(Console.Error);
            return 64;
        }

        using var cancellation = new CancellationTokenSource();
        ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellation.Cancel();
        };
        Console.CancelKeyPress += cancelHandler;
        string? connectionStringEnvironment = null;
        try
        {
            var loadedSpec = await BenchmarkSpec.LoadAsync(options.SpecPath, cancellation.Token);
            options.ApplyTo(loadedSpec.Spec);
            connectionStringEnvironment = loadedSpec.Spec.Storage.ConnectionStringEnvironment;
            if (options.Command == "validate")
            {
                Console.WriteLine($"Scenario '{loadedSpec.Spec.Name}' is valid.");
                foreach (var artifact in loadedSpec.Artifacts)
                {
                    Console.WriteLine($"{artifact.Kind}: {artifact.Sha256}  {artifact.Path}");
                }

                return 0;
            }

            DriverOptions.ValidateExternalExecutionProvenance(loadedSpec.Spec);
            var runId = options.ApplyRunIdentity(loadedSpec.Spec);
            if (options.Command == "serve")
            {
                var serveOutcome = await BenchmarkHosting.ServeAsync(loadedSpec.Spec, cancellation.Token);
                var cleanupPath = await BenchmarkResultWriter.WriteCleanupEvidenceAsync(
                    options.OutputDirectory,
                    runId,
                    options.InstanceId,
                    loadedSpec.Spec,
                    serveOutcome.Cleanup,
                    CancellationToken.None);
                Console.WriteLine($"Machine-readable cleanup evidence: {cleanupPath}");
                if (serveOutcome.Failure is not null)
                {
                    Console.Error.WriteLine(SecretRedactor.Redact(
                        serveOutcome.Failure.ToString(),
                        connectionStringEnvironment));
                    return ContainsCancellation(serveOutcome.Failure) && cancellation.IsCancellationRequested
                        ? 130
                        : 1;
                }

                return 0;
            }

            var (clientOrdinal, clientCount) = options.GetClientCoordinates(loadedSpec.Spec);
            CrankMetrics.RegisterAndStart();
            var effective = BenchmarkResultWriter.CreateEffectiveConfiguration(
                loadedSpec.Spec,
                runId,
                options.InstanceId,
                clientOrdinal,
                clientCount,
                options.CreateEffectiveOverrides());
            var startedAt = DateTimeOffset.UtcNow;
            BenchmarkCluster? cluster = null;
            BenchmarkRunEngine? engine = null;
            BenchmarkExecution? execution = null;
            Exception? runFailure = null;
            var cleanup = new BackendCleanupReport(
                "cluster-startup-best-effort",
                Attempted: false,
                Succeeded: false,
                Error: null);
            Exception? cleanupFailure = null;
            try
            {
                cluster = await BenchmarkHosting.StartClientClusterAsync(loadedSpec.Spec, cancellation.Token);
                engine = new BenchmarkRunEngine(
                    loadedSpec.Spec,
                    cluster.Client,
                    clientOrdinal,
                    clientCount);
                execution = await engine.RunAsync(cancellation.Token);
            }
            catch (Exception exception)
            {
                runFailure = exception;
            }
            finally
            {
                try
                {
                    if (cluster is not null)
                    {
                        await cluster.DisposeAsync();
                    }
                }
                catch (Exception exception)
                {
                    cleanupFailure = exception;
                }

                if (cluster is not null)
                {
                    cleanup = cluster.CleanupReport;
                }
                else if (runFailure is not null)
                {
                    cleanup = FindClusterStartupCleanup(runFailure) ?? cleanup with
                    {
                        Error = "Cluster startup failed before a cleanup report became available; no cleanup target was confirmed.",
                    };
                }
            }

            if (runFailure is not null || cleanupFailure is not null)
            {
                var failure = runFailure is not null && cleanupFailure is not null
                    ? new AggregateException(runFailure, cleanupFailure)
                    : runFailure ?? cleanupFailure!;
                var failurePath = await BenchmarkResultWriter.WriteFailureAsync(
                    options.OutputDirectory,
                    runId,
                    options.InstanceId,
                    startedAt,
                    loadedSpec,
                    effective,
                    cleanup,
                    execution,
                    engine,
                    failure,
                    CancellationToken.None);
                Console.Error.WriteLine($"Machine-readable failure result: {failurePath}");
                return ContainsCancellation(runFailure) && cancellation.IsCancellationRequested ? 130 : 1;
            }

            var resultPath = await BenchmarkResultWriter.WriteAsync(
                options.OutputDirectory,
                runId,
                options.InstanceId,
                startedAt,
                loadedSpec,
                effective,
                cleanup,
                execution!,
                CancellationToken.None);
            Console.WriteLine($"Machine-readable result: {resultPath}");
            return execution!.Measurement.Failed == 0 && execution.Measurement.Dropped == 0 ? 0 : 2;
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            Console.Error.WriteLine("Benchmark canceled.");
            return 130;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine(SecretRedactor.Redact(exception.ToString(), connectionStringEnvironment));
            return 1;
        }
        finally
        {
            Console.CancelKeyPress -= cancelHandler;
        }
    }

    internal static bool ContainsCancellation(Exception? exception)
    {
        if (exception is null)
        {
            return false;
        }

        if (exception is OperationCanceledException)
        {
            return true;
        }

        if (exception is AggregateException aggregate &&
            aggregate.Flatten().InnerExceptions.Any(ContainsCancellation))
        {
            return true;
        }

        return ContainsCancellation(exception.InnerException);
    }

    private static BackendCleanupReport? FindClusterStartupCleanup(Exception exception)
    {
        if (exception is BenchmarkClusterStartException startup)
        {
            return startup.CleanupReport;
        }

        if (exception is BackendProvisioningException provisioning)
        {
            return provisioning.CleanupReport;
        }

        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.Flatten().InnerExceptions)
            {
                if (FindClusterStartupCleanup(inner) is { } report)
                {
                    return report;
                }
            }
        }

        return exception.InnerException is null ? null : FindClusterStartupCleanup(exception.InnerException);
    }
}
