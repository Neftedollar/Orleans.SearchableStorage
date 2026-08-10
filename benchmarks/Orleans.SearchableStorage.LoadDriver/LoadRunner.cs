using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using HdrHistogram;

namespace Orleans.SearchableStorage.Benchmarks;

internal enum OperationKind
{
    Upsert,
    Read,
    ExactQuery,
    RangeQuery,
    Clear,
}

internal sealed class BenchmarkRunEngine
{
    private readonly BenchmarkSpec spec;
    private readonly IClusterClient? clusterClient;
    private readonly IBenchmarkOperationExecutor _operations;
    private readonly int clientOrdinal;
    private readonly int clientCount;
    private readonly Action? _lateCallDrainStarted;
    private readonly Func<string, bool, CancellationToken, Task<BenchmarkBarrierResult>>? _phaseBarrier;
    private readonly Func<string, CancellationToken, Task<BenchmarkBarrierResult>>? _phaseAbort;
    private readonly Action<OperationInvocation, bool>? _openLoopOfferObserved;

    public BenchmarkRunEngine(
        BenchmarkSpec spec,
        IClusterClient clusterClient,
        int clientOrdinal = 0,
        int clientCount = 1)
        : this(
            spec,
            clusterClient,
            new BenchmarkOperationClient(spec, clusterClient, clientOrdinal),
            clientOrdinal,
            clientCount,
            lateCallDrainStarted: null,
            phaseBarrier: null,
            phaseAbort: null,
            openLoopOfferObserved: null)
    {
    }

    internal BenchmarkRunEngine(
        BenchmarkSpec spec,
        IBenchmarkOperationExecutor operations,
        int clientOrdinal = 0,
        int clientCount = 1,
        Action? lateCallDrainStarted = null,
        Func<string, bool, CancellationToken, Task<BenchmarkBarrierResult>>? phaseBarrier = null,
        Func<string, CancellationToken, Task<BenchmarkBarrierResult>>? phaseAbort = null,
        Action<OperationInvocation, bool>? openLoopOfferObserved = null)
        : this(
            spec,
            clusterClient: null,
            operations,
            clientOrdinal,
            clientCount,
            lateCallDrainStarted,
            phaseBarrier,
            phaseAbort,
            openLoopOfferObserved)
    {
    }

    private BenchmarkRunEngine(
        BenchmarkSpec spec,
        IClusterClient? clusterClient,
        IBenchmarkOperationExecutor operations,
        int clientOrdinal,
        int clientCount,
        Action? lateCallDrainStarted,
        Func<string, bool, CancellationToken, Task<BenchmarkBarrierResult>>? phaseBarrier,
        Func<string, CancellationToken, Task<BenchmarkBarrierResult>>? phaseAbort,
        Action<OperationInvocation, bool>? openLoopOfferObserved)
    {
        this.spec = spec;
        this.clusterClient = clusterClient;
        _operations = operations;
        this.clientOrdinal = clientOrdinal;
        this.clientCount = clientCount;
        _lateCallDrainStarted = lateCallDrainStarted;
        _phaseBarrier = phaseBarrier;
        _phaseAbort = phaseAbort;
        _openLoopOfferObserved = openLoopOfferObserved;
    }

    public PhaseExecution? CompletedWarmup { get; private set; }

    public PopulationExecution? CompletedPopulation { get; private set; }

    public PopulationExecution? PartialPopulation { get; private set; }

    public PopulationExecution? CompletedRestoration { get; private set; }

    public PopulationExecution? PartialRestoration { get; private set; }

    public CorrectnessAuditExecution? CompletedInitialAudit { get; private set; }

    public CorrectnessAuditExecution? CompletedFinalAudit { get; private set; }

    public PhaseExecution? CompletedMeasurement { get; private set; }

    public PhaseExecution? FailedPhase { get; private set; }

    public async Task<BenchmarkExecution> RunAsync(CancellationToken cancellationToken)
    {
        var population = await RunSynchronizedPhaseAsync<PopulationExecution?>(
            "population-complete",
            async () =>
            {
                if (!spec.Population.Enabled)
                {
                    return null;
                }

                try
                {
                    var completed = await PopulateAsync("population", cancellationToken);
                    CompletedPopulation = completed;
                    return completed;
                }
                catch (BenchmarkPopulationException exception)
                {
                    PartialPopulation = exception.PartialExecution;
                    throw;
                }
            },
            cancellationToken);

        var initialAudit = await RunSynchronizedPhaseAsync<CorrectnessAuditExecution?>(
            "initial-audit-complete",
            async () =>
            {
                if (!spec.Audit.Enabled)
                {
                    return null;
                }

                var completed = await RunInitialCorrectnessAuditAsync(cancellationToken);
                CompletedInitialAudit = completed;
                return completed;
            },
            cancellationToken);

        var warmup = await RunSynchronizedPhaseAsync<PhaseExecution?>(
            "warmup-complete",
            async () =>
            {
                if (spec.Workload.WarmupSeconds <= 0)
                {
                    return null;
                }

                Console.WriteLine($"Warmup: {spec.Workload.WarmupSeconds}s ({spec.Workload.Mode})");
                try
                {
                    var completed = await RunPhaseAsync(
                        TimeSpan.FromSeconds(spec.Workload.WarmupSeconds),
                        recordHistograms: false,
                        cancellationToken);
                    EnsureWarmupSucceeded(completed);
                    CompletedWarmup = completed;
                    return completed;
                }
                catch (BenchmarkPhaseException exception)
                {
                    FailedPhase = exception.PartialExecution;
                    throw;
                }
            },
            cancellationToken);

        var restoration = await RunSynchronizedPhaseAsync<PopulationExecution?>(
            "restoration-complete",
            async () =>
            {
                if (warmup is null || !spec.Population.Enabled || !spec.Population.RestoreAfterWarmup)
                {
                    return null;
                }

                try
                {
                    var completed = await PopulateAsync("post-warmup restoration", cancellationToken);
                    CompletedRestoration = completed;
                    return completed;
                }
                catch (BenchmarkPopulationException exception)
                {
                    PartialRestoration = exception.PartialExecution;
                    throw;
                }
            },
            cancellationToken);

        Console.WriteLine($"Measurement: {spec.Workload.DurationSeconds}s ({spec.Workload.Mode})");
        var measurement = await RunSynchronizedPhaseAsync(
            "measurement-complete",
            async () =>
            {
                try
                {
                    var completed = await RunPhaseAsync(
                        TimeSpan.FromSeconds(spec.Workload.DurationSeconds),
                        recordHistograms: true,
                        cancellationToken);
                    CompletedMeasurement = completed;
                    EnsureMeasurementSucceeded(completed);
                    return completed;
                }
                catch (BenchmarkPhaseException exception)
                {
                    FailedPhase = exception.PartialExecution;
                    throw;
                }
            },
            cancellationToken);

        var finalAudit = await RunSynchronizedPhaseAsync<CorrectnessAuditExecution?>(
            "final-audit-complete",
            async () =>
            {
                if (!spec.Audit.Enabled)
                {
                    return null;
                }

                var completed = await RunFinalCorrectnessAuditAsync(cancellationToken);
                CompletedFinalAudit = completed;
                return completed;
            },
            cancellationToken);

        return new BenchmarkExecution(warmup, population, restoration, initialAudit, measurement, finalAudit);
    }

    internal static void EnsureWarmupSucceeded(PhaseExecution warmup)
    {
        ArgumentNullException.ThrowIfNull(warmup);
        if (warmup.Failed > 0 || warmup.Dropped > 0)
        {
            throw new BenchmarkPhaseException(
                warmup,
                new InvalidDataException(
                    $"Warmup completed with {warmup.Failed:N0} failed and {warmup.Dropped:N0} dropped operations."));
        }
    }

    internal static void EnsureMeasurementSucceeded(PhaseExecution measurement)
    {
        ArgumentNullException.ThrowIfNull(measurement);
        if (measurement.Failed > 0 || measurement.Dropped > 0)
        {
            throw new BenchmarkPhaseException(
                measurement,
                new InvalidDataException(
                    $"Measurement completed with {measurement.Failed:N0} failed and {measurement.Dropped:N0} dropped operations."));
        }
    }

    private async Task<T> RunSynchronizedPhaseAsync<T>(
        string phase,
        Func<Task<T>> executeAsync,
        CancellationToken cancellationToken)
    {
        Exception? primaryFailure = null;
        T result = default!;
        try
        {
            result = await executeAsync();
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception exception)
        {
            primaryFailure = exception;
        }

        BenchmarkBarrierResult barrierResult;
        try
        {
            // Every client must publish its outcome even after user cancellation. The barrier has
            // its own bounded operation and late-call-drain deadlines, so this cannot wait forever.
            barrierResult = await SynchronizeClientsAsync(
                phase,
                primaryFailure is null,
                primaryFailure is null ? cancellationToken : CancellationToken.None);
        }
        catch (Exception barrierFailure) when (primaryFailure is not null)
        {
            throw new AggregateException(primaryFailure, barrierFailure);
        }

        if (primaryFailure is not null)
        {
            ExceptionDispatchInfo.Capture(primaryFailure).Throw();
        }

        if (!barrierResult.AllSucceeded)
        {
            throw new DistributedBenchmarkPhaseException(
                phase,
                barrierResult.FailedClientOrdinals,
                barrierResult.DeadlineExceeded,
                barrierResult.MissingClientOrdinals);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return result;
    }

    private async Task<CorrectnessAuditExecution> RunInitialCorrectnessAuditAsync(CancellationToken cancellationToken)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        long pointChecks = 0;
        long exactQueryChecks = 0;
        long rangeQueryChecks = 0;
        Console.WriteLine(
            $"Post-population correctness audit: {spec.Audit.PointSampleCount:N0} deterministic point samples and " +
            $"{spec.Audit.QuerySampleCount:N0} exact/range samples...");

        for (var sample = clientOrdinal; sample < spec.Audit.PointSampleCount; sample += clientCount)
        {
            var ordinal = CorrectnessAuditPlan.SelectPointOrdinal(
                spec.Dataset,
                sample,
                spec.Audit.PointSampleCount);
            var actual = await WaitWithTimeoutAsync(
                _operations.ReadStateAsync(ordinal),
                TimeSpan.FromSeconds(spec.Audit.OperationTimeoutSeconds),
                TimeSpan.FromSeconds(spec.Audit.LateCallDrainTimeoutSeconds),
                cancellationToken);
            var expected = DeterministicData.CreateState(spec.Dataset, ordinal, revision: 0);
            if (!CorrectnessAuditPlan.StateEquals(expected, actual))
            {
                throw new InvalidDataException($"Correctness audit failed for point record ordinal {ordinal:N0}.");
            }

            pointChecks++;
        }

        if (clientOrdinal == 0)
        {
            for (var sample = 0; sample < spec.Audit.QuerySampleCount; sample++)
            {
                var ordinal = CorrectnessAuditPlan.SelectPointOrdinal(
                    spec.Dataset,
                    sample,
                    spec.Audit.QuerySampleCount);
                var exactValue = DeterministicData.GetExactValue(spec.Dataset, ordinal);
                var expectedExactKeys = new HashSet<string>(StringComparer.Ordinal);
                for (long candidate = 0; candidate < spec.Dataset.RecordCount; candidate++)
                {
                    if (string.Equals(
                        DeterministicData.GetExactValue(spec.Dataset, candidate),
                        exactValue,
                        StringComparison.Ordinal))
                    {
                        _ = expectedExactKeys.Add(DeterministicData.GetGrainKey(candidate));
                    }
                }

                var actualExactKeys = await WaitWithTimeoutAsync(
                    _operations.FindKeysAsync(exactValue),
                    TimeSpan.FromSeconds(spec.Audit.OperationTimeoutSeconds),
                    TimeSpan.FromSeconds(spec.Audit.LateCallDrainTimeoutSeconds),
                    cancellationToken);
                if (actualExactKeys.Count != actualExactKeys.Distinct(StringComparer.Ordinal).Count() ||
                    !expectedExactKeys.SetEquals(actualExactKeys))
                {
                    throw new InvalidDataException(
                        $"Exact-query membership audit for '{exactValue}' returned a different key set " +
                        $"({actualExactKeys.Count:N0} keys versus {expectedExactKeys.Count:N0} expected)." );
                }

                exactQueryChecks++;
                var lower = DeterministicData.SelectRangeStart(
                    spec.Dataset,
                    spec.Workload,
                    sample,
                    clientOrdinal: 0);
                var upper = checked(lower + spec.Workload.GetRangeWindow(spec.Dataset) - 1);
                var expectedRangeKeys = new HashSet<string>(StringComparer.Ordinal);
                for (long candidate = 0; candidate < spec.Dataset.RecordCount; candidate++)
                {
                    var value = DeterministicData.GetRangeValue(spec.Dataset, candidate);
                    if (value >= lower && value <= upper)
                    {
                        _ = expectedRangeKeys.Add(DeterministicData.GetGrainKey(candidate));
                    }
                }

                var actualRangeKeys = await WaitWithTimeoutAsync(
                    _operations.RangeKeysAsync(lower, upper),
                    TimeSpan.FromSeconds(spec.Audit.OperationTimeoutSeconds),
                    TimeSpan.FromSeconds(spec.Audit.LateCallDrainTimeoutSeconds),
                    cancellationToken);
                if (actualRangeKeys.Count != actualRangeKeys.Distinct(StringComparer.Ordinal).Count() ||
                    !expectedRangeKeys.SetEquals(actualRangeKeys))
                {
                    throw new InvalidDataException(
                        $"Range-query membership audit [{lower:N0}, {upper:N0}] returned a different key set " +
                        $"({actualRangeKeys.Count:N0} keys versus {expectedRangeKeys.Count:N0} expected)." );
                }

                rangeQueryChecks++;
            }
        }

        stopwatch.Stop();
        return new CorrectnessAuditExecution(
            startedAt,
            stopwatch.Elapsed.TotalSeconds,
            pointChecks,
            exactQueryChecks,
            rangeQueryChecks,
            CorrectnessAuditPlan.DescribePointCoverage(
                spec.Audit.PointSampleCount == spec.Dataset.RecordCount,
                clientOrdinal,
                clientCount));
    }

    internal async Task<CorrectnessAuditExecution> RunFinalCorrectnessAuditAsync(CancellationToken cancellationToken)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        var stateCache = new Dictionary<long, BenchmarkRecordState?>();
        long pointChecks = 0;
        long exactQueryChecks = 0;
        long rangeQueryChecks = 0;
        Console.WriteLine(
            $"Post-measurement correctness audit: {spec.Audit.PointSampleCount:N0} deterministic point samples and " +
            $"{spec.Audit.QuerySampleCount:N0} exact/range samples...");

        for (var sample = clientOrdinal; sample < spec.Audit.PointSampleCount; sample += clientCount)
        {
            var ordinal = CorrectnessAuditPlan.SelectPointOrdinal(
                spec.Dataset,
                sample,
                spec.Audit.PointSampleCount);
            _ = await ReadCurrentStateAsync(ordinal);
            pointChecks++;
        }

        if (clientOrdinal == 0)
        {
            for (var sample = 0; sample < spec.Audit.QuerySampleCount; sample++)
            {
                var ordinal = CorrectnessAuditPlan.SelectPointOrdinal(
                    spec.Dataset,
                    sample,
                    spec.Audit.QuerySampleCount);
                var exactValue = DeterministicData.GetExactValue(spec.Dataset, ordinal);
                var actualExactKeys = await WaitWithTimeoutAsync(
                    _operations.FindKeysAsync(exactValue),
                    TimeSpan.FromSeconds(spec.Audit.OperationTimeoutSeconds),
                    TimeSpan.FromSeconds(spec.Audit.LateCallDrainTimeoutSeconds),
                    cancellationToken);
                var expectedExactKeys = await GetCurrentExactKeysAsync(exactValue);
                CorrectnessAuditPlan.ValidateCurrentMembership(
                    $"Final exact-query membership audit for '{exactValue}'",
                    actualExactKeys,
                    expectedExactKeys,
                    spec.Dataset,
                    spec.Workload.QuerySelectivity.MaximumExpectedResultCount);
                exactQueryChecks++;

                var lower = DeterministicData.SelectRangeStart(
                    spec.Dataset,
                    spec.Workload,
                    sample,
                    clientOrdinal: 0);
                var upper = checked(lower + spec.Workload.GetRangeWindow(spec.Dataset) - 1);
                var actualRangeKeys = await WaitWithTimeoutAsync(
                    _operations.RangeKeysAsync(lower, upper),
                    TimeSpan.FromSeconds(spec.Audit.OperationTimeoutSeconds),
                    TimeSpan.FromSeconds(spec.Audit.LateCallDrainTimeoutSeconds),
                    cancellationToken);
                var expectedRangeKeys = await GetCurrentRangeKeysAsync(lower, upper);
                CorrectnessAuditPlan.ValidateCurrentMembership(
                    $"Final range-query membership audit [{lower:N0}, {upper:N0}]",
                    actualRangeKeys,
                    expectedRangeKeys,
                    spec.Dataset,
                    spec.Workload.QuerySelectivity.MaximumExpectedResultCount);
                rangeQueryChecks++;
            }
        }

        stopwatch.Stop();
        return new CorrectnessAuditExecution(
            startedAt,
            stopwatch.Elapsed.TotalSeconds,
            pointChecks,
            exactQueryChecks,
            rangeQueryChecks,
            CorrectnessAuditPlan.DescribePointCoverage(
                spec.Audit.PointSampleCount == spec.Dataset.RecordCount,
                clientOrdinal,
                clientCount));

        async Task<BenchmarkRecordState?> ReadCurrentStateAsync(long ordinal)
        {
            if (stateCache.TryGetValue(ordinal, out var cached))
            {
                return cached;
            }

            var actual = await WaitWithTimeoutAsync(
                _operations.ReadStateAsync(ordinal),
                TimeSpan.FromSeconds(spec.Audit.OperationTimeoutSeconds),
                TimeSpan.FromSeconds(spec.Audit.LateCallDrainTimeoutSeconds),
                cancellationToken);
            if (actual is null && spec.Population.Enabled && spec.Workload.Operations.Clear == 0)
            {
                throw new InvalidDataException(
                    $"Final correctness audit found missing state for record ordinal {ordinal:N0}, " +
                    "but the workload does not contain clear operations.");
            }

            if (!CorrectnessAuditPlan.CurrentStateIsSelfConsistent(spec.Dataset, ordinal, actual))
            {
                throw new InvalidDataException(
                    $"Final correctness audit found malformed state for record ordinal {ordinal:N0}.");
            }

            stateCache.Add(ordinal, actual);
            return actual;
        }

        async Task<HashSet<string>> GetCurrentExactKeysAsync(string exactValue)
        {
            var result = new HashSet<string>(StringComparer.Ordinal);
            for (long candidate = 0; candidate < spec.Dataset.RecordCount; candidate++)
            {
                if (!string.Equals(
                    DeterministicData.GetExactValue(spec.Dataset, candidate),
                    exactValue,
                    StringComparison.Ordinal))
                {
                    continue;
                }

                if (await ReadCurrentStateAsync(candidate) is not null)
                {
                    _ = result.Add(DeterministicData.GetGrainKey(candidate));
                }
            }

            return result;
        }

        async Task<HashSet<string>> GetCurrentRangeKeysAsync(int lower, int upper)
        {
            var result = new HashSet<string>(StringComparer.Ordinal);
            for (long candidate = 0; candidate < spec.Dataset.RecordCount; candidate++)
            {
                var value = DeterministicData.GetRangeValue(spec.Dataset, candidate);
                if (value < lower || value > upper)
                {
                    continue;
                }

                if (await ReadCurrentStateAsync(candidate) is not null)
                {
                    _ = result.Add(DeterministicData.GetGrainKey(candidate));
                }
            }

            return result;
        }
    }

    private async Task<BenchmarkBarrierResult> SynchronizeClientsAsync(
        string phase,
        bool succeeded,
        CancellationToken cancellationToken)
    {
        if (clientCount == 1)
        {
            return new BenchmarkBarrierResult
            {
                AllSucceeded = succeeded,
                FailedClientOrdinals = succeeded ? [] : [clientOrdinal],
            };
        }

        Console.WriteLine($"Waiting at distributed barrier '{phase}' ({clientOrdinal + 1}/{clientCount})...");
        var barrier = _phaseBarrier is null
            ? (clusterClient ?? throw new InvalidOperationException("A cluster client is required for distributed barriers."))
                .GetGrain<IBenchmarkBarrierGrain>(spec.Topology.ServiceId)
            : null;
        var barrierCall = _phaseBarrier is not null
            ? _phaseBarrier(phase, succeeded, cancellationToken)
            : barrier!.SignalAndWaitAsync(
                phase,
                clientOrdinal,
                clientCount,
                succeeded,
                spec.Topology.BarrierTimeoutSeconds);
        var abortLock = new object();
        Task<BenchmarkBarrierResult>? abortTask = null;

        Task<BenchmarkBarrierResult> StartAbort()
        {
            lock (abortLock)
            {
                if (abortTask is not null)
                {
                    return abortTask;
                }

                try
                {
                    abortTask = _phaseAbort is not null
                        ? _phaseAbort(phase, CancellationToken.None)
                        : barrier!.AbortPhaseAsync(
                            phase,
                            clientOrdinal,
                            clientCount,
                            spec.Topology.BarrierTimeoutSeconds);
                }
                catch (Exception exception)
                {
                    abortTask = Task.FromException<BenchmarkBarrierResult>(exception);
                }

                return abortTask;
            }
        }

        void StartAbortAndObserve()
        {
            _ = ObserveDetachedAsync(StartAbort());
        }

        using var cancellationRegistration = cancellationToken.Register(StartAbortAndObserve);
        try
        {
            var result = await WaitWithTimeoutAsync(
                barrierCall,
                TimeSpan.FromSeconds(checked(
                    spec.Topology.BarrierTimeoutSeconds
                    + BenchmarkRecordConstants.BarrierResultDeliveryMarginSeconds)),
                TimeSpan.FromSeconds(spec.Topology.BarrierLateCallDrainTimeoutSeconds),
                cancellationToken);
            if (cancellationToken.IsCancellationRequested)
            {
                StartAbortAndObserve();
                cancellationToken.ThrowIfCancellationRequested();
            }

            return result;
        }
        catch (Exception exception) when (
            exception is BenchmarkCallCanceledException or BenchmarkCallTimeoutException)
        {
            StartAbortAndObserve();
            throw;
        }
    }

    private async Task<PopulationExecution> PopulateAsync(string phaseName, CancellationToken cancellationToken)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        long nextShardIndex = -1;
        long completed = 0;
        var workers = Enumerable.Range(0, spec.Population.Concurrency)
            .Select(_ => PopulateWorkerAsync())
            .ToArray();

        Console.WriteLine(
            $"Running {phaseName}: {spec.Dataset.RecordCount:N0} deterministic records with concurrency {spec.Population.Concurrency:N0}...");
        try
        {
            await Task.WhenAll(workers);
        }
        catch (Exception exception)
        {
            stopwatch.Stop();
            throw new BenchmarkPopulationException(
                new PopulationExecution(phaseName, startedAt, stopwatch.Elapsed.TotalSeconds, completed),
                exception);
        }

        stopwatch.Stop();
        Console.WriteLine($"{phaseName} complete: {completed:N0} records in {stopwatch.Elapsed.TotalSeconds:N2}s");
        return new PopulationExecution(phaseName, startedAt, stopwatch.Elapsed.TotalSeconds, completed);

        async Task PopulateWorkerAsync()
        {
            while (true)
            {
                var shardIndex = Interlocked.Increment(ref nextShardIndex);
                if (!ClientStream.TryGetPopulationOrdinal(
                    spec.Dataset.RecordCount,
                    clientOrdinal,
                    clientCount,
                    shardIndex,
                    out var ordinal))
                {
                    return;
                }

                var operation = _operations.UpsertAsync(ordinal, revision: 0);
                await WaitWithTimeoutAsync(
                    operation,
                    TimeSpan.FromSeconds(spec.Population.OperationTimeoutSeconds),
                    TimeSpan.FromSeconds(spec.Population.LateCallDrainTimeoutSeconds),
                    cancellationToken);
                Interlocked.Increment(ref completed);
            }
        }
    }

    internal Task<PhaseExecution> RunPhaseAsync(
        TimeSpan duration,
        bool recordHistograms,
        CancellationToken cancellationToken)
    {
        return spec.Workload.Mode switch
        {
            LoadMode.ClosedLoop => RunClosedLoopAsync(duration, recordHistograms, cancellationToken),
            LoadMode.OpenLoop => RunOpenLoopAsync(duration, recordHistograms, cancellationToken),
            _ => throw new InvalidOperationException($"Unsupported load mode '{spec.Workload.Mode}'."),
        };
    }

    private async Task<PhaseExecution> RunClosedLoopAsync(
        TimeSpan duration,
        bool recordHistograms,
        CancellationToken cancellationToken)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var startTimestamp = Stopwatch.GetTimestamp();
        var stopTimestamp = startTimestamp + ToStopwatchTicks(duration);
        long sequence = -1;
        var workerMetrics = Enumerable.Range(0, spec.Workload.Concurrency)
            .Select(_ => new WorkerMetrics(recordHistograms))
            .ToArray();
        var workers = workerMetrics
            .Select(metrics => RunWorkerAsync(metrics))
            .ToArray();
        try
        {
            await Task.WhenAll(workers);
            cancellationToken.ThrowIfCancellationRequested();
            return CreateExecution();
        }
        catch (Exception exception)
        {
            throw new BenchmarkPhaseException(CreateExecution(), exception);
        }

        PhaseExecution CreateExecution()
        {
            return PhaseExecution.Create(
                startedAt,
                startTimestamp,
                stopTimestamp,
                Stopwatch.GetTimestamp(),
                workerMetrics,
                schedulerCounters: null,
                recordHistograms);
        }

        async Task RunWorkerAsync(WorkerMetrics metrics)
        {
            while (Stopwatch.GetTimestamp() < stopTimestamp)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var localSequence = Interlocked.Increment(ref sequence);
                var currentSequence = ClientStream.GetGlobalSequence(localSequence, clientOrdinal, clientCount);
                var kind = OperationSelector.Select(spec.Workload.Operations, spec.Dataset.Seed, currentSequence);
                var invocation = new OperationInvocation(currentSequence, kind, Stopwatch.GetTimestamp());
                metrics.RecordOffered(kind);
                await ExecuteInvocationAsync(invocation, metrics, endToEndLatency: false, cancellationToken);
            }
        }
    }

    private async Task<PhaseExecution> RunOpenLoopAsync(
        TimeSpan duration,
        bool recordHistograms,
        CancellationToken cancellationToken)
    {
        using var phaseCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var phaseToken = phaseCancellation.Token;
        var startedAt = DateTimeOffset.UtcNow;
        var startTimestamp = Stopwatch.GetTimestamp();
        var stopTimestamp = startTimestamp + ToStopwatchTicks(duration);
        var channel = Channel.CreateBounded<OperationInvocation>(new BoundedChannelOptions(spec.Workload.MaximumQueueDepth)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
        var schedulerCounters = new SchedulerCounters();
        var workerMetrics = Enumerable.Range(0, spec.Workload.Concurrency)
            .Select(_ => new WorkerMetrics(recordHistograms))
            .ToArray();
        var workerFailures = new ConcurrentQueue<Exception>();
        // 0 = no stop requested, 1 = worker fatal, 2 = scheduler fatal. The first concrete
        // failure is authoritative; cancellations induced in siblings are drain behavior.
        var stopCause = 0;
        var workers = workerMetrics
            .Select(metrics => ConsumeAsync(metrics))
            .ToArray();

        Exception? schedulerFailure = null;
        try
        {
            await ScheduleAsync();
        }
        catch (OperationCanceledException) when (
            !cancellationToken.IsCancellationRequested &&
            Volatile.Read(ref stopCause) == 1)
        {
            // The first worker fatal canceled the phase to stop further offers. Its concrete
            // exception is captured by the worker and remains the primary phase failure.
        }
        catch (Exception exception)
        {
            schedulerFailure = exception;
            _ = Interlocked.CompareExchange(ref stopCause, 2, 0);
            phaseCancellation.Cancel();
        }
        finally
        {
            // Complete normally so readers can drain queued work when the scheduler itself
            // faults without cancellation. Reader/operation failures are collected below.
            channel.Writer.TryComplete();
        }

        Exception? workerFailure = null;
        try
        {
            await Task.WhenAll(workers);
        }
        catch (Exception exception)
        {
            var capturedFailures = workerFailures.ToArray();
            workerFailure = capturedFailures.Length switch
            {
                0 when schedulerFailure is null => exception,
                0 => null,
                1 => capturedFailures[0],
                _ => new AggregateException(capturedFailures),
            };
        }

        // Accepted arrivals left in the bounded queue were never started. Once every worker has
        // stopped, classify them deterministically as dropped rather than silently losing them.
        while (channel.Reader.TryRead(out var queuedInvocation))
        {
            schedulerCounters.RecordDropped(queuedInvocation.Kind);
        }

        if (schedulerFailure is null && workerFailure is null)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (Exception exception)
            {
                schedulerFailure = exception;
            }
        }

        var execution = CreateExecution();
        Exception? accountingFailure = null;
        try
        {
            EnsureOpenLoopAccounting(execution);
        }
        catch (Exception exception)
        {
            accountingFailure = exception;
        }

        var phaseFailure = CombineFailures(
            CombineFailures(schedulerFailure, workerFailure),
            accountingFailure);
        if (phaseFailure is null)
        {
            return execution;
        }

        throw new BenchmarkPhaseException(execution, phaseFailure);

        PhaseExecution CreateExecution()
        {
            return PhaseExecution.Create(
                startedAt,
                startTimestamp,
                stopTimestamp,
                Stopwatch.GetTimestamp(),
                workerMetrics,
                schedulerCounters,
                recordHistograms);
        }

        async Task ScheduleAsync()
        {
            var intervalTicks = Stopwatch.Frequency / spec.Workload.TargetRatePerSecond;
            long sequence = 0;
            while (true)
            {
                phaseToken.ThrowIfCancellationRequested();
                var scheduledTimestamp = startTimestamp + (long)Math.Round(sequence * intervalTicks);
                if (scheduledTimestamp >= stopTimestamp)
                {
                    return;
                }

                await WaitUntilAsync(scheduledTimestamp, phaseToken);
                var globalSequence = ClientStream.GetGlobalSequence(sequence, clientOrdinal, clientCount);
                var kind = OperationSelector.Select(spec.Workload.Operations, spec.Dataset.Seed, globalSequence);
                schedulerCounters.RecordOffered(kind);
                var invocation = new OperationInvocation(globalSequence, kind, scheduledTimestamp);
                var accepted = channel.Writer.TryWrite(invocation);
                if (!accepted)
                {
                    schedulerCounters.RecordDropped(kind);
                }

                _openLoopOfferObserved?.Invoke(invocation, accepted);

                sequence++;
            }
        }

        async Task ConsumeAsync(WorkerMetrics metrics)
        {
            try
            {
                await foreach (var invocation in channel.Reader.ReadAllAsync(phaseToken))
                {
                    await ExecuteInvocationAsync(invocation, metrics, endToEndLatency: true, phaseToken);
                }
            }
            catch (Exception exception)
            {
                var firstFailure = Interlocked.CompareExchange(ref stopCause, 1, 0) == 0;
                if (firstFailure || cancellationToken.IsCancellationRequested)
                {
                    // An async Task which throws an OperationCanceledException transitions to
                    // Canceled and otherwise loses the concrete late-drain exception. Capture it
                    // before rethrowing so failure artifacts retain that evidence.
                    workerFailures.Enqueue(exception);
                }

                phaseCancellation.Cancel();
                throw;
            }
        }
    }

    internal static void EnsureOpenLoopAccounting(PhaseExecution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        foreach (var (kind, operation) in execution.Operations)
        {
            if (operation.Offered != checked(operation.Started + operation.Dropped))
            {
                throw new InvalidDataException(
                    $"Open-loop accounting for {kind} is inconsistent: offered={operation.Offered:N0}, " +
                    $"started={operation.Started:N0}, dropped={operation.Dropped:N0}.");
            }
        }
    }

    private static Exception? CombineFailures(Exception? primaryFailure, Exception? drainFailure)
    {
        if (primaryFailure is null)
        {
            return drainFailure;
        }

        return drainFailure is null
            ? primaryFailure
            : new AggregateException(primaryFailure, drainFailure);
    }

    private async Task ExecuteInvocationAsync(
        OperationInvocation invocation,
        WorkerMetrics metrics,
        bool endToEndLatency,
        CancellationToken cancellationToken)
    {
        var operationStarted = Stopwatch.GetTimestamp();
        metrics.RecordStarted(invocation.Kind);
        try
        {
            var resultCount = await WaitWithTimeoutAsync(
                _operations.ExecuteAsync(invocation),
                TimeSpan.FromSeconds(spec.Workload.OperationTimeoutSeconds),
                TimeSpan.FromSeconds(spec.Workload.LateCallDrainTimeoutSeconds),
                cancellationToken,
                _lateCallDrainStarted);
            var latencyStarted = endToEndLatency ? invocation.ScheduledTimestamp : operationStarted;
            metrics.RecordCompleted(
                invocation.Kind,
                Stopwatch.GetTimestamp() - latencyStarted,
                endToEndLatency ? operationStarted - invocation.ScheduledTimestamp : null,
                succeeded: true,
                resultCount,
                exception: null);
        }
        catch (BenchmarkCallTimeoutException exception)
        {
            // A timeout is run-fatal. WaitWithTimeoutAsync has already drained the
            // underlying Orleans call, so no late mutation can cross phase boundaries.
            var latencyStarted = endToEndLatency ? invocation.ScheduledTimestamp : operationStarted;
            metrics.RecordCompleted(
                invocation.Kind,
                Stopwatch.GetTimestamp() - latencyStarted,
                endToEndLatency ? operationStarted - invocation.ScheduledTimestamp : null,
                succeeded: false,
                resultCount: 0,
                exception,
                lateCallDrainEvidence: exception);
            throw;
        }
        catch (BenchmarkCallCanceledException exception)
        {
            var latencyStarted = endToEndLatency ? invocation.ScheduledTimestamp : operationStarted;
            metrics.RecordCompleted(
                invocation.Kind,
                Stopwatch.GetTimestamp() - latencyStarted,
                endToEndLatency ? operationStarted - invocation.ScheduledTimestamp : null,
                succeeded: false,
                resultCount: 0,
                exception,
                lateCallDrainEvidence: exception);
            throw;
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            var latencyStarted = endToEndLatency ? invocation.ScheduledTimestamp : operationStarted;
            metrics.RecordCompleted(
                invocation.Kind,
                Stopwatch.GetTimestamp() - latencyStarted,
                endToEndLatency ? operationStarted - invocation.ScheduledTimestamp : null,
                succeeded: false,
                resultCount: 0,
                exception);
        }
    }

    private static async Task WaitUntilAsync(long targetTimestamp, CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remainingTicks = targetTimestamp - Stopwatch.GetTimestamp();
            if (remainingTicks <= 0)
            {
                return;
            }

            var remaining = TimeSpan.FromSeconds(remainingTicks / (double)Stopwatch.Frequency);
            if (remaining > TimeSpan.FromMilliseconds(2))
            {
                await Task.Delay(remaining - TimeSpan.FromMilliseconds(1), cancellationToken);
            }
            else
            {
                await Task.Yield();
            }
        }
    }

    private static long ToStopwatchTicks(TimeSpan duration)
    {
        return checked((long)Math.Round(duration.TotalSeconds * Stopwatch.Frequency));
    }

    private static async Task WaitWithTimeoutAsync(
        Task operation,
        TimeSpan timeout,
        TimeSpan lateCallDrainTimeout,
        CancellationToken cancellationToken,
        Action? lateCallDrainStarted = null)
    {
        try
        {
            await operation.WaitAsync(timeout, cancellationToken);
        }
        catch (TimeoutException exception) when (IsUnderlyingTimeout(operation, exception))
        {
            throw;
        }
        catch (TimeoutException exception)
        {
            throw await DrainTimedOutOperationAsync(
                operation,
                timeout,
                lateCallDrainTimeout,
                exception,
                lateCallDrainStarted);
        }
        catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested)
        {
            var drain = await DrainOperationAsync(operation, lateCallDrainTimeout, lateCallDrainStarted);
            throw new BenchmarkCallCanceledException(
                lateCallDrainTimeout,
                drain.Duration,
                drain.Incomplete,
                exception,
                cancellationToken);
        }
    }

    private static async Task<T> WaitWithTimeoutAsync<T>(
        Task<T> operation,
        TimeSpan timeout,
        TimeSpan lateCallDrainTimeout,
        CancellationToken cancellationToken,
        Action? lateCallDrainStarted = null)
    {
        try
        {
            return await operation.WaitAsync(timeout, cancellationToken);
        }
        catch (TimeoutException exception) when (IsUnderlyingTimeout(operation, exception))
        {
            throw;
        }
        catch (TimeoutException exception)
        {
            throw await DrainTimedOutOperationAsync(
                operation,
                timeout,
                lateCallDrainTimeout,
                exception,
                lateCallDrainStarted);
        }
        catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested)
        {
            var drain = await DrainOperationAsync(operation, lateCallDrainTimeout, lateCallDrainStarted);
            throw new BenchmarkCallCanceledException(
                lateCallDrainTimeout,
                drain.Duration,
                drain.Incomplete,
                exception,
                cancellationToken);
        }
    }

    private static async Task<BenchmarkCallTimeoutException> DrainTimedOutOperationAsync(
        Task operation,
        TimeSpan operationTimeout,
        TimeSpan lateCallDrainTimeout,
        TimeoutException timeoutException,
        Action? lateCallDrainStarted = null)
    {
        var drain = await DrainOperationAsync(operation, lateCallDrainTimeout, lateCallDrainStarted);
        return new BenchmarkCallTimeoutException(
            operationTimeout,
            lateCallDrainTimeout,
            drain.Duration,
            drain.Incomplete,
            timeoutException);
    }

    private static bool IsUnderlyingTimeout(Task operation, TimeoutException exception)
    {
        return operation.Exception?.Flatten().InnerExceptions
            .Any(inner => ReferenceEquals(inner, exception)) == true;
    }

    private static async Task<(TimeSpan Duration, bool Incomplete)> DrainOperationAsync(
        Task operation,
        TimeSpan lateCallDrainTimeout,
        Action? lateCallDrainStarted = null)
    {
        lateCallDrainStarted?.Invoke();
        var drainStopwatch = Stopwatch.StartNew();
        var incomplete = false;
        try
        {
            await operation.WaitAsync(lateCallDrainTimeout, CancellationToken.None);
        }
        catch (TimeoutException) when (!operation.IsCompleted)
        {
            incomplete = true;
            _ = ObserveDetachedAsync(operation);
        }
        catch
        {
            // The underlying call completed during the bounded drain. Its failure is observed,
            // but the original operation deadline remains the run-fatal outcome.
        }

        drainStopwatch.Stop();
        return (drainStopwatch.Elapsed, incomplete);
    }

    internal static Task WaitWithTimeoutForTestingAsync(
        Task operation,
        TimeSpan timeout,
        TimeSpan lateCallDrainTimeout,
        Action lateCallDrainStarted,
        CancellationToken cancellationToken)
    {
        return WaitWithTimeoutAsync(
            operation,
            timeout,
            lateCallDrainTimeout,
            cancellationToken,
            lateCallDrainStarted);
    }

    private static async Task ObserveDetachedAsync(Task operation)
    {
        try
        {
            await operation;
        }
        catch
        {
            // A non-cancellable Orleans call outlived the hard drain deadline. The cluster is
            // stopped before a phase can advance; this observer only consumes a later failure.
        }
    }
}

internal readonly record struct OperationInvocation(long Sequence, OperationKind Kind, long ScheduledTimestamp);

internal static class ClientStream
{
    public static IEnumerable<long> GetPopulationOrdinals(long recordCount, int clientOrdinal, int clientCount)
    {
        for (long shardIndex = 0;
             TryGetPopulationOrdinal(recordCount, clientOrdinal, clientCount, shardIndex, out var ordinal);
             shardIndex++)
        {
            yield return ordinal;
        }
    }

    public static bool TryGetPopulationOrdinal(
        long recordCount,
        int clientOrdinal,
        int clientCount,
        long shardIndex,
        out long ordinal)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordCount);
        ArgumentOutOfRangeException.ThrowIfNegative(clientOrdinal);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(clientCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(clientOrdinal, clientCount);
        ArgumentOutOfRangeException.ThrowIfNegative(shardIndex);
        ordinal = checked(shardIndex * clientCount + clientOrdinal);
        return ordinal < recordCount;
    }

    public static long GetGlobalSequence(long localSequence, int clientOrdinal, int clientCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(localSequence);
        ArgumentOutOfRangeException.ThrowIfNegative(clientOrdinal);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(clientCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(clientOrdinal, clientCount);

        return checked(localSequence * clientCount + clientOrdinal);
    }
}

internal static class CorrectnessAuditPlan
{
    public static string DescribePointCoverage(bool allPoints, int clientOrdinal, int clientCount)
    {
        return clientCount == 1
            ? allPoints ? "all-points" : "deterministic-point-sample"
            : allPoints
                ? $"collective-all-points-shard-{clientOrdinal}-of-{clientCount}"
                : $"collective-deterministic-sample-shard-{clientOrdinal}-of-{clientCount}";
    }

    public static long SelectPointOrdinal(DatasetSpec dataset, int sampleIndex, int sampleCount)
    {
        ArgumentNullException.ThrowIfNull(dataset);
        ArgumentOutOfRangeException.ThrowIfNegative(sampleIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sampleCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(sampleIndex, sampleCount);
        if (sampleCount > dataset.RecordCount)
        {
            throw new ArgumentOutOfRangeException(nameof(sampleCount));
        }

        var offset = DeterministicData.Mix(dataset.Seed ^ 0x8CB92BA72F3D8DD7UL) % (ulong)dataset.RecordCount;
        var evenlySpaced = (ulong)(((UInt128)(uint)sampleIndex * (ulong)dataset.RecordCount) / (uint)sampleCount);
        return (long)((offset + evenlySpaced) % (ulong)dataset.RecordCount);
    }

    public static bool StateEquals(BenchmarkRecordState expected, BenchmarkRecordState? actual)
    {
        return actual is not null &&
            string.Equals(expected.ExactValue, actual.ExactValue, StringComparison.Ordinal) &&
            expected.RangeValue == actual.RangeValue &&
            expected.Revision == actual.Revision &&
            expected.Payload.AsSpan().SequenceEqual(actual.Payload);
    }

    public static bool CurrentStateIsSelfConsistent(
        DatasetSpec dataset,
        long ordinal,
        BenchmarkRecordState? actual)
    {
        ArgumentNullException.ThrowIfNull(dataset);
        ArgumentOutOfRangeException.ThrowIfNegative(ordinal);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(ordinal, dataset.RecordCount);

        return actual is null ||
            actual.Revision >= 0 && StateEquals(
                DeterministicData.CreateState(dataset, ordinal, actual.Revision),
                actual);
    }

    public static void ValidateCurrentMembership(
        string description,
        IReadOnlyList<string> actualKeys,
        IReadOnlySet<string> expectedKeys,
        DatasetSpec dataset,
        long maximumExpectedResultCount)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(description);
        ArgumentNullException.ThrowIfNull(actualKeys);
        ArgumentNullException.ThrowIfNull(expectedKeys);
        ArgumentNullException.ThrowIfNull(dataset);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumExpectedResultCount);

        if (actualKeys.Count > maximumExpectedResultCount || expectedKeys.Count > maximumExpectedResultCount)
        {
            throw new InvalidDataException(
                $"{description} exceeded the declared result cap of {maximumExpectedResultCount:N0} keys " +
                $"({actualKeys.Count:N0} returned, {expectedKeys.Count:N0} current).");
        }

        var actualSet = new HashSet<string>(StringComparer.Ordinal);
        foreach (var key in actualKeys)
        {
            if (!TryParseCanonicalOrdinal(key, dataset.RecordCount, out _))
            {
                throw new InvalidDataException($"{description} returned malformed or out-of-dataset key '{key}'.");
            }

            if (!actualSet.Add(key))
            {
                throw new InvalidDataException($"{description} returned duplicate key '{key}'.");
            }
        }

        if (!actualSet.SetEquals(expectedKeys))
        {
            throw new InvalidDataException(
                $"{description} returned a different key set " +
                $"({actualSet.Count:N0} keys versus {expectedKeys.Count:N0} current).");
        }
    }

    private static bool TryParseCanonicalOrdinal(string? key, long recordCount, out long ordinal)
    {
        ordinal = -1;
        if (key is null ||
            key.Length != 19 ||
            !key.StartsWith("record-", StringComparison.Ordinal) ||
            !long.TryParse(key.AsSpan(7), out ordinal) ||
            ordinal < 0 ||
            ordinal >= recordCount)
        {
            return false;
        }

        return string.Equals(key, DeterministicData.GetGrainKey(ordinal), StringComparison.Ordinal);
    }
}

internal static class OperationSelector
{
    public static OperationKind Select(OperationMixSpec mix, ulong seed, long sequence)
    {
        var selected = (int)(DeterministicData.Mix(seed ^ unchecked((ulong)sequence * 0x9E3779B97F4A7C15UL)) % (uint)mix.TotalWeight);
        if ((selected -= mix.Upsert) < 0)
        {
            return OperationKind.Upsert;
        }

        if ((selected -= mix.Read) < 0)
        {
            return OperationKind.Read;
        }

        if ((selected -= mix.ExactQuery) < 0)
        {
            return OperationKind.ExactQuery;
        }

        if ((selected -= mix.RangeQuery) < 0)
        {
            return OperationKind.RangeQuery;
        }

        return OperationKind.Clear;
    }
}

internal interface IBenchmarkOperationExecutor
{
    Task UpsertAsync(long ordinal, long revision);

    Task<long> ExecuteAsync(OperationInvocation invocation);

    Task<BenchmarkRecordState?> ReadStateAsync(long ordinal);

    Task<IReadOnlyList<string>> FindKeysAsync(string exactValue);

    Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper);
}

internal sealed class BenchmarkOperationClient : IBenchmarkOperationExecutor
{
    private const ulong KeySalt = 0x243F6A8885A308D3UL;
    private readonly BenchmarkSpec _spec;
    private readonly IClusterClient _clusterClient;
    private readonly ISearchableStorageClient? _searchable;
    private readonly int _clientOrdinal;

    public BenchmarkOperationClient(BenchmarkSpec spec, IClusterClient clusterClient, int clientOrdinal = 0)
    {
        _spec = spec;
        _clusterClient = clusterClient;
        _clientOrdinal = clientOrdinal;
        _searchable = spec.Storage.Path is StoragePath.Searchable
            ? new SearchableStorageClient(
                clusterClient,
                BenchmarkRecordConstants.StorageProviderName,
                spec.Storage.PartitionCount)
            : null;
    }

    public Task UpsertAsync(long ordinal, long revision)
    {
        var key = DeterministicData.GetGrainKey(ordinal);
        var state = DeterministicData.CreateState(_spec.Dataset, ordinal, revision);
        return _spec.Storage.Path switch
        {
            StoragePath.Searchable => _clusterClient.GetGrain<IBenchmarkRecordGrain>(key).UpsertAsync(state),
            StoragePath.Plain => _clusterClient.GetGrain<IPlainBenchmarkRecordGrain>(key).UpsertAsync(state),
            _ => throw new InvalidOperationException($"Unsupported storage path '{_spec.Storage.Path}'."),
        };
    }

    public async Task<long> ExecuteAsync(OperationInvocation invocation)
    {
        var ordinal = DeterministicData.SelectOrdinal(
            _spec.Dataset,
            _spec.Workload,
            invocation.Sequence,
            KeySalt ^ DeterministicData.DeriveClientSeed(_spec.Dataset.Seed, _clientOrdinal));
        var key = DeterministicData.GetGrainKey(ordinal);
        switch (invocation.Kind)
        {
            case OperationKind.Upsert:
                await UpsertAsync(ordinal, checked(invocation.Sequence + 1));
                return 0;
            case OperationKind.Read:
                return await ReadAsync(key) is null ? 0 : 1;
            case OperationKind.ExactQuery:
            {
                var searchable = GetRequiredSearchableClient();
                var value = DeterministicData.GetExactValue(_spec.Dataset, ordinal);
                var result = await searchable.FindAsync<BenchmarkRecordState, string>(
                    BenchmarkRecordConstants.StateName,
                    static state => state.ExactValue,
                    value);
                return result.Count;
            }
            case OperationKind.RangeQuery:
            {
                var searchable = GetRequiredSearchableClient();
                var lower = DeterministicData.SelectRangeStart(
                    _spec.Dataset,
                    _spec.Workload,
                    invocation.Sequence,
                    _clientOrdinal);
                var upper = checked(lower + _spec.Workload.GetRangeWindow(_spec.Dataset) - 1);
                var result = await searchable.RangeAsync<BenchmarkRecordState, int>(
                    BenchmarkRecordConstants.StateName,
                    static state => state.RangeValue,
                    lower,
                    upper);
                return result.Count;
            }
            case OperationKind.Clear:
                await ClearAsync(key);
                return 0;
            default:
                throw new InvalidOperationException($"Unsupported operation '{invocation.Kind}'.");
        }
    }

    public Task<BenchmarkRecordState?> ReadStateAsync(long ordinal)
    {
        return ReadAsync(DeterministicData.GetGrainKey(ordinal));
    }

    public async Task<IReadOnlyList<string>> FindKeysAsync(string exactValue)
    {
        var result = await GetRequiredSearchableClient().FindAsync<BenchmarkRecordState, string>(
            BenchmarkRecordConstants.StateName,
            static state => state.ExactValue,
            exactValue);
        return result.Select(static grainId => grainId.Key.ToString()).ToArray();
    }

    public async Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper)
    {
        var result = await GetRequiredSearchableClient().RangeAsync<BenchmarkRecordState, int>(
            BenchmarkRecordConstants.StateName,
            static state => state.RangeValue,
            lower,
            upper);
        return result.Select(static grainId => grainId.Key.ToString()).ToArray();
    }

    private Task<BenchmarkRecordState?> ReadAsync(string key)
    {
        return _spec.Storage.Path switch
        {
            StoragePath.Searchable => _clusterClient.GetGrain<IBenchmarkRecordGrain>(key).ReadAsync(),
            StoragePath.Plain => _clusterClient.GetGrain<IPlainBenchmarkRecordGrain>(key).ReadAsync(),
            _ => throw new InvalidOperationException($"Unsupported storage path '{_spec.Storage.Path}'."),
        };
    }

    private Task ClearAsync(string key)
    {
        return _spec.Storage.Path switch
        {
            StoragePath.Searchable => _clusterClient.GetGrain<IBenchmarkRecordGrain>(key).ClearAsync(),
            StoragePath.Plain => _clusterClient.GetGrain<IPlainBenchmarkRecordGrain>(key).ClearAsync(),
            _ => throw new InvalidOperationException($"Unsupported storage path '{_spec.Storage.Path}'."),
        };
    }

    private ISearchableStorageClient GetRequiredSearchableClient()
    {
        return _searchable
            ?? throw new InvalidOperationException("Search operations are unavailable on the plain storage baseline.");
    }
}

internal sealed class WorkerMetrics(bool recordHistograms)
{
    public const long LowestDiscernibleMicroseconds = 1;
    public const long HighestTrackableMicroseconds = 600_000_000;
    public const int SignificantDigits = 3;
    private readonly OperationMetricAccumulator[] _operations = Enum.GetValues<OperationKind>()
        .Select(_ => new OperationMetricAccumulator(recordHistograms))
        .ToArray();

    public IReadOnlyList<OperationMetricAccumulator> Operations => _operations;

    public void RecordOffered(OperationKind kind)
    {
        _operations[(int)kind].Offered++;
    }

    public void RecordStarted(OperationKind kind)
    {
        _operations[(int)kind].Started++;
    }

    public void RecordCompleted(
        OperationKind kind,
        long elapsedStopwatchTicks,
        long? queueDelayStopwatchTicks,
        bool succeeded,
        long resultCount,
        Exception? exception,
        ILateCallDrainEvidence? lateCallDrainEvidence = null)
    {
        var metrics = _operations[(int)kind];
        metrics.Completed++;
        if (succeeded)
        {
            metrics.Succeeded++;
        }
        else
        {
            metrics.Failed++;
            if (lateCallDrainEvidence is not null)
            {
                metrics.LateCallDrainAttempts++;
                metrics.LateCallDrainIncomplete += lateCallDrainEvidence.LateCallDrainIncomplete ? 1 : 0;
                metrics.LateCallDrainDurationSeconds += lateCallDrainEvidence.LateCallDrainDuration.TotalSeconds;
                if (lateCallDrainEvidence is BenchmarkCallTimeoutException)
                {
                    metrics.TimedOut++;
                }
            }

            var errorName = exception?.GetType().FullName ?? "unknown";
            metrics.Errors[errorName] = metrics.Errors.GetValueOrDefault(errorName) + 1;
        }

        metrics.ResultCount += resultCount;
        var latencyHistogram = succeeded ? metrics.SucceededLatency : metrics.FailedLatency;
        if (latencyHistogram is not null)
        {
            latencyHistogram.RecordValue(ToMicroseconds(elapsedStopwatchTicks, metrics));
        }

        if (metrics.QueueDelay is not null && queueDelayStopwatchTicks is { } queueDelay)
        {
            metrics.QueueDelay.RecordValue(ToMicroseconds(queueDelay, metrics));
        }
    }

    private static long ToMicroseconds(long stopwatchTicks, OperationMetricAccumulator metrics)
    {
        var microseconds = Math.Max(
            LowestDiscernibleMicroseconds,
            (long)Math.Round(stopwatchTicks * 1_000_000d / Stopwatch.Frequency));
        if (microseconds > HighestTrackableMicroseconds)
        {
            microseconds = HighestTrackableMicroseconds;
            metrics.HistogramClamped++;
        }

        return microseconds;
    }
}

internal sealed class OperationMetricAccumulator(bool recordHistograms)
{
    public long Offered { get; set; }

    public long Started { get; set; }

    public long Completed { get; set; }

    public long Succeeded { get; set; }

    public long Failed { get; set; }

    public long TimedOut { get; set; }

    public long LateCallDrainAttempts { get; set; }

    public long LateCallDrainIncomplete { get; set; }

    public double LateCallDrainDurationSeconds { get; set; }

    public long Dropped { get; set; }

    public long ResultCount { get; set; }

    public long HistogramClamped { get; set; }

    public LongHistogram? SucceededLatency { get; } = CreateHistogram(recordHistograms);

    public LongHistogram? FailedLatency { get; } = CreateHistogram(recordHistograms);

    public LongHistogram? QueueDelay { get; } = CreateHistogram(recordHistograms);

    public Dictionary<string, long> Errors { get; } = new(StringComparer.Ordinal);

    private static LongHistogram? CreateHistogram(bool enabled)
    {
        return enabled
            ? new LongHistogram(
                WorkerMetrics.LowestDiscernibleMicroseconds,
                WorkerMetrics.HighestTrackableMicroseconds,
                WorkerMetrics.SignificantDigits)
            : null;
    }
}

internal sealed class SchedulerCounters
{
    private readonly long[] _offered = new long[Enum.GetValues<OperationKind>().Length];
    private readonly long[] _dropped = new long[Enum.GetValues<OperationKind>().Length];

    public void RecordOffered(OperationKind kind)
    {
        Interlocked.Increment(ref _offered[(int)kind]);
    }

    public void RecordDropped(OperationKind kind)
    {
        Interlocked.Increment(ref _dropped[(int)kind]);
    }

    public long GetOffered(OperationKind kind) => Volatile.Read(ref _offered[(int)kind]);

    public long GetDropped(OperationKind kind) => Volatile.Read(ref _dropped[(int)kind]);
}

internal sealed record BenchmarkExecution(
    PhaseExecution? Warmup,
    PopulationExecution? Population,
    PopulationExecution? Restoration,
    CorrectnessAuditExecution? InitialAudit,
    PhaseExecution Measurement,
    CorrectnessAuditExecution? FinalAudit);

internal sealed record PopulationExecution(
    string Phase,
    DateTimeOffset StartedAtUtc,
    double DurationSeconds,
    long Completed);

internal sealed class BenchmarkPopulationException(
    PopulationExecution partialExecution,
    Exception innerException)
    : Exception("Benchmark population phase failed.", innerException)
{
    public PopulationExecution PartialExecution { get; } = partialExecution;
}

internal sealed record CorrectnessAuditExecution(
    DateTimeOffset StartedAtUtc,
    double DurationSeconds,
    long PointChecks,
    long ExactQueryChecks,
    long RangeQueryChecks,
    string PointCoverage);

internal sealed class BenchmarkPhaseException(PhaseExecution partialExecution, Exception innerException)
    : Exception("Benchmark phase failed.", innerException)
{
    public PhaseExecution PartialExecution { get; } = partialExecution;
}

internal sealed class DistributedBenchmarkPhaseException(
    string phase,
    IReadOnlyList<int> failedClientOrdinals,
    bool deadlineExceeded,
    IReadOnlyList<int> missingClientOrdinals)
    : Exception(
        deadlineExceeded
            ? $"Distributed benchmark phase '{phase}' exceeded its shared deadline; missing client ordinal(s): " +
                string.Join(", ", missingClientOrdinals)
            : $"Distributed benchmark phase '{phase}' failed on client ordinal(s): " +
                string.Join(", ", failedClientOrdinals))
{
    public string Phase { get; } = phase;

    public IReadOnlyList<int> FailedClientOrdinals { get; } = failedClientOrdinals.ToArray();

    public bool DeadlineExceeded { get; } = deadlineExceeded;

    public IReadOnlyList<int> MissingClientOrdinals { get; } = missingClientOrdinals.ToArray();
}

internal sealed class PhaseExecution
{
    private PhaseExecution(
        DateTimeOffset startedAtUtc,
        double scheduledDurationSeconds,
        double wallDurationSeconds,
        IReadOnlyDictionary<OperationKind, OperationExecution> operations)
    {
        StartedAtUtc = startedAtUtc;
        ScheduledDurationSeconds = scheduledDurationSeconds;
        WallDurationSeconds = wallDurationSeconds;
        Operations = operations;
    }

    public DateTimeOffset StartedAtUtc { get; }

    public double ScheduledDurationSeconds { get; }

    public double WallDurationSeconds { get; }

    public IReadOnlyDictionary<OperationKind, OperationExecution> Operations { get; }

    public long Offered => Operations.Values.Sum(static value => value.Offered);

    public long Completed => Operations.Values.Sum(static value => value.Completed);

    public long Failed => Operations.Values.Sum(static value => value.Failed);

    public long Dropped => Operations.Values.Sum(static value => value.Dropped);

    public static PhaseExecution Create(
        DateTimeOffset startedAtUtc,
        long startTimestamp,
        long stopTimestamp,
        long drainedTimestamp,
        IReadOnlyList<WorkerMetrics> workers,
        SchedulerCounters? schedulerCounters,
        bool recordHistograms)
    {
        var operations = new Dictionary<OperationKind, OperationExecution>();
        foreach (var kind in Enum.GetValues<OperationKind>())
        {
            var accumulator = new OperationMetricAccumulator(recordHistograms);
            foreach (var worker in workers)
            {
                var source = worker.Operations[(int)kind];
                accumulator.Offered += source.Offered;
                accumulator.Started += source.Started;
                accumulator.Completed += source.Completed;
                accumulator.Succeeded += source.Succeeded;
                accumulator.Failed += source.Failed;
                accumulator.TimedOut += source.TimedOut;
                accumulator.LateCallDrainAttempts += source.LateCallDrainAttempts;
                accumulator.LateCallDrainIncomplete += source.LateCallDrainIncomplete;
                accumulator.LateCallDrainDurationSeconds += source.LateCallDrainDurationSeconds;
                accumulator.ResultCount += source.ResultCount;
                accumulator.HistogramClamped += source.HistogramClamped;
                AddHistogram(accumulator.SucceededLatency, source.SucceededLatency);
                AddHistogram(accumulator.FailedLatency, source.FailedLatency);
                AddHistogram(accumulator.QueueDelay, source.QueueDelay);

                foreach (var (name, count) in source.Errors)
                {
                    accumulator.Errors[name] = accumulator.Errors.GetValueOrDefault(name) + count;
                }
            }

            if (schedulerCounters is not null)
            {
                accumulator.Offered = schedulerCounters.GetOffered(kind);
                accumulator.Dropped = schedulerCounters.GetDropped(kind);
            }

            operations[kind] = OperationExecution.FromAccumulator(accumulator);
        }

        return new PhaseExecution(
            startedAtUtc,
            (stopTimestamp - startTimestamp) / (double)Stopwatch.Frequency,
            (drainedTimestamp - startTimestamp) / (double)Stopwatch.Frequency,
            operations);

        static void AddHistogram(LongHistogram? target, LongHistogram? source)
        {
            if (target is not null && source is not null)
            {
                target.Add(source);
            }
        }
    }
}

internal sealed record OperationExecution(
    long Offered,
    long Started,
    long Completed,
    long Succeeded,
    long Failed,
    long TimedOut,
    long LateCallDrainAttempts,
    long LateCallDrainIncomplete,
    double LateCallDrainDurationSeconds,
    long Dropped,
    long ResultCount,
    long HistogramClamped,
    IReadOnlyDictionary<string, long> Errors,
    LongHistogram? SucceededLatency,
    LongHistogram? FailedLatency,
    LongHistogram? QueueDelay)
{
    public static OperationExecution FromAccumulator(OperationMetricAccumulator value)
    {
        return new OperationExecution(
            value.Offered,
            value.Started,
            value.Completed,
            value.Succeeded,
            value.Failed,
            value.TimedOut,
            value.LateCallDrainAttempts,
            value.LateCallDrainIncomplete,
            value.LateCallDrainDurationSeconds,
            value.Dropped,
            value.ResultCount,
            value.HistogramClamped,
            new Dictionary<string, long>(value.Errors, StringComparer.Ordinal),
            value.SucceededLatency,
            value.FailedLatency,
            value.QueueDelay);
    }
}

internal interface ILateCallDrainEvidence
{
    TimeSpan LateCallDrainTimeout { get; }

    TimeSpan LateCallDrainDuration { get; }

    bool LateCallDrainIncomplete { get; }
}

internal sealed class BenchmarkCallTimeoutException(
    TimeSpan operationTimeout,
    TimeSpan lateCallDrainTimeout,
    TimeSpan lateCallDrainDuration,
    bool lateCallDrainIncomplete,
    TimeoutException innerException)
    : TimeoutException(
        lateCallDrainIncomplete
            ? $"The operation exceeded its {operationTimeout.TotalSeconds:N3}s deadline and the underlying call did not finish within the additional {lateCallDrainTimeout.TotalSeconds:N3}s hard drain deadline."
            : $"The operation exceeded its {operationTimeout.TotalSeconds:N3}s deadline; the underlying call drained in {lateCallDrainDuration.TotalSeconds:N3}s.",
        innerException), ILateCallDrainEvidence
{
    public TimeSpan OperationTimeout { get; } = operationTimeout;

    public TimeSpan LateCallDrainTimeout { get; } = lateCallDrainTimeout;

    public TimeSpan LateCallDrainDuration { get; } = lateCallDrainDuration;

    public bool LateCallDrainIncomplete { get; } = lateCallDrainIncomplete;
}

internal sealed class BenchmarkCallCanceledException(
    TimeSpan lateCallDrainTimeout,
    TimeSpan lateCallDrainDuration,
    bool lateCallDrainIncomplete,
    OperationCanceledException innerException,
    CancellationToken cancellationToken)
    : OperationCanceledException(
        lateCallDrainIncomplete
            ? $"Cancellation was requested and the underlying call did not finish within the additional {lateCallDrainTimeout.TotalSeconds:N3}s hard drain deadline."
            : $"Cancellation was requested; the underlying call drained in {lateCallDrainDuration.TotalSeconds:N3}s.",
        innerException,
        cancellationToken), ILateCallDrainEvidence
{
    public TimeSpan LateCallDrainTimeout { get; } = lateCallDrainTimeout;

    public TimeSpan LateCallDrainDuration { get; } = lateCallDrainDuration;

    public bool LateCallDrainIncomplete { get; } = lateCallDrainIncomplete;
}
