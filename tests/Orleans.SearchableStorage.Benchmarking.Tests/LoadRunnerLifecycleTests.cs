using System.Diagnostics;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class LoadRunnerLifecycleTests
{
    private static readonly TimeSpan TestDeadline = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task OpenLoopCancellationWaitsForInFlightCallAndPreservesBothFailures()
    {
        var operation = new ControlledOperationExecutor(expectedConcurrency: 1);
        var drainStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var offersReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var offers = 0;
        var engine = new BenchmarkRunEngine(
            CreateSpec(LoadMode.OpenLoop, concurrency: 1),
            operation,
            lateCallDrainStarted: () => drainStarted.TrySetResult(),
            openLoopOfferObserved: (_, _) =>
            {
                if (Interlocked.Increment(ref offers) >= 32)
                {
                    offersReached.TrySetResult();
                }
            });
        using var cancellation = new CancellationTokenSource();

        var phaseTask = engine.RunPhaseAsync(
            TimeSpan.FromSeconds(30),
            recordHistograms: false,
            cancellation.Token);
        await operation.FirstStarted.Task.WaitAsync(TestDeadline);
        await offersReached.Task.WaitAsync(TestDeadline);

        cancellation.Cancel();
        await drainStarted.Task.WaitAsync(TestDeadline);

        Assert.False(phaseTask.IsCompleted);
        Assert.Equal(1, operation.Active);

        operation.Release();
        var failure = await Assert.ThrowsAsync<BenchmarkPhaseException>(() => phaseTask);
        var combined = Assert.IsType<AggregateException>(failure.InnerException);
        var innerFailures = combined.Flatten().InnerExceptions;

        Assert.IsAssignableFrom<OperationCanceledException>(innerFailures[0]);
        var drainFailure = Assert.Single(innerFailures.OfType<BenchmarkCallCanceledException>());
        Assert.False(drainFailure.LateCallDrainIncomplete);
        Assert.Equal(0, operation.Active);

        var operationResults = failure.PartialExecution.Operations.Values;
        Assert.Equal(1, operationResults.Sum(static value => value.Started));
        Assert.Equal(1, operationResults.Sum(static value => value.Completed));
        Assert.Equal(1, operationResults.Sum(static value => value.Failed));
        Assert.Equal(1, operationResults.Sum(static value => value.LateCallDrainAttempts));
        Assert.Equal(0, operationResults.Sum(static value => value.LateCallDrainIncomplete));
        Assert.True(operationResults.Sum(static value => value.Dropped) > 0);
        Assert.All(
            operationResults,
            static result => Assert.Equal(result.Offered, result.Started + result.Dropped));
    }

    [Fact]
    public async Task OpenLoopTimeoutStopsPromptlyAndClassifiesQueuedArrivals()
    {
        var operation = new ControlledOperationExecutor(expectedConcurrency: 1);
        var offersReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var offers = 0;
        var engine = new BenchmarkRunEngine(
            CreateSpec(
                LoadMode.OpenLoop,
                concurrency: 1,
                targetRatePerSecond: 10_000,
                maximumQueueDepth: 2,
                operationTimeoutSeconds: 1,
                lateCallDrainTimeoutSeconds: 1),
            operation,
            openLoopOfferObserved: (_, _) =>
            {
                if (Interlocked.Increment(ref offers) >= 10)
                {
                    offersReached.TrySetResult();
                }
            });

        var stopwatch = Stopwatch.StartNew();
        var phaseTask = engine.RunPhaseAsync(
            TimeSpan.FromSeconds(30),
            recordHistograms: false,
            CancellationToken.None);
        await operation.FirstStarted.Task.WaitAsync(TestDeadline);
        await offersReached.Task.WaitAsync(TestDeadline);

        var failure = await Assert.ThrowsAsync<BenchmarkPhaseException>(
            () => phaseTask.WaitAsync(TestDeadline));
        stopwatch.Stop();
        operation.Release();

        Assert.True(stopwatch.Elapsed < TestDeadline, $"Phase took {stopwatch.Elapsed}.");
        Assert.NotNull(FindException<BenchmarkCallTimeoutException>(failure));
        Assert.True(failure.PartialExecution.Dropped > 0);
        Assert.All(
            failure.PartialExecution.Operations.Values,
            static result => Assert.Equal(result.Offered, result.Started + result.Dropped));
    }

    [Fact]
    public void OpenLoopAccountingRejectsAnUnclassifiedArrival()
    {
        var scheduler = new SchedulerCounters();
        scheduler.RecordOffered(OperationKind.Read);
        var phase = PhaseExecution.Create(
            DateTimeOffset.UnixEpoch,
            startTimestamp: 0,
            stopTimestamp: Stopwatch.Frequency,
            drainedTimestamp: Stopwatch.Frequency,
            [new WorkerMetrics(recordHistograms: false)],
            scheduler,
            recordHistograms: false);

        var failure = Assert.Throws<InvalidDataException>(() =>
            BenchmarkRunEngine.EnsureOpenLoopAccounting(phase));

        Assert.Contains("offered=1", failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task TimeoutAndCancellationRecordCompleteAndIncompleteDrain(
        bool timeout,
        bool completeDrain)
    {
        var operation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var drainStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var operationTimeout = timeout ? TimeSpan.FromMilliseconds(20) : TestDeadline;
        var drainTimeout = TimeSpan.FromMilliseconds(100);

        var waitTask = BenchmarkRunEngine.WaitWithTimeoutForTestingAsync(
            operation.Task,
            operationTimeout,
            drainTimeout,
            () => drainStarted.TrySetResult(),
            cancellation.Token);
        if (!timeout)
        {
            cancellation.Cancel();
        }

        await drainStarted.Task.WaitAsync(TestDeadline);
        if (completeDrain)
        {
            operation.TrySetResult();
        }

        var failure = await Record.ExceptionAsync(() => waitTask.WaitAsync(TestDeadline));
        var evidence = Assert.IsAssignableFrom<ILateCallDrainEvidence>(failure);
        Assert.Equal(drainTimeout, evidence.LateCallDrainTimeout);
        Assert.Equal(!completeDrain, evidence.LateCallDrainIncomplete);

        if (timeout)
        {
            var timeoutFailure = Assert.IsType<BenchmarkCallTimeoutException>(failure);
            Assert.Equal(operationTimeout, timeoutFailure.OperationTimeout);
        }
        else
        {
            Assert.IsType<BenchmarkCallCanceledException>(failure);
        }

        operation.TrySetResult();
    }

    [Fact]
    public async Task UnderlyingProviderTimeoutIsNotMisclassifiedAsDriverDeadline()
    {
        var drainStarted = 0;
        var providerFailure = new TimeoutException("provider-owned timeout");

        var failure = await Assert.ThrowsAsync<TimeoutException>(() =>
            BenchmarkRunEngine.WaitWithTimeoutForTestingAsync(
                Task.FromException(providerFailure),
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(5),
                () => Interlocked.Increment(ref drainStarted),
                CancellationToken.None));

        Assert.Same(providerFailure, failure);
        Assert.Equal(0, Volatile.Read(ref drainStarted));
    }

    [Fact]
    public async Task ClosedLoopNeverExceedsConfiguredConcurrency()
    {
        const int concurrency = 3;
        var operation = new ControlledOperationExecutor(concurrency);
        var drainStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = new BenchmarkRunEngine(
            CreateSpec(LoadMode.ClosedLoop, concurrency),
            operation,
            lateCallDrainStarted: () => drainStarted.TrySetResult());
        using var cancellation = new CancellationTokenSource();

        var phaseTask = engine.RunPhaseAsync(
            TimeSpan.FromSeconds(30),
            recordHistograms: false,
            cancellation.Token);
        await operation.ExpectedConcurrencyReached.Task.WaitAsync(TestDeadline);

        Assert.Equal(concurrency, operation.Active);
        Assert.Equal(concurrency, operation.MaximumActive);
        Assert.Equal(concurrency, operation.InvocationCount);

        cancellation.Cancel();
        await drainStarted.Task.WaitAsync(TestDeadline);
        operation.Release();
        await Assert.ThrowsAsync<BenchmarkPhaseException>(() => phaseTask);

        Assert.Equal(0, operation.Active);
        Assert.Equal(concurrency, operation.MaximumActive);
        Assert.Equal(concurrency, operation.InvocationCount);
    }

    [Fact]
    public async Task PopulationFailurePreservesPartialProgress()
    {
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "partial-population-test",
            Dataset = new SpecReference { Path = "dataset.json", Sha256 = new string('0', 64) },
            Workload = new SpecReference { Path = "workload.json", Sha256 = new string('0', 64) },
            Population = new PopulationSpec { Concurrency = 1 },
            Audit = new CorrectnessAuditSpec { Enabled = false },
        };
        var spec = new BenchmarkSpec(
            scenario,
            new DatasetSpec
            {
                Id = "partial-population-test",
                RecordCount = 3,
                ExactValueCardinality = 3,
                RangeValueCardinality = 3,
            },
            new WorkloadSpec
            {
                Id = "partial-population-test",
                WarmupSeconds = 0,
                DurationSeconds = 1,
            });
        var engine = new BenchmarkRunEngine(spec, new FailingPopulationExecutor());

        var failure = await Assert.ThrowsAsync<BenchmarkPopulationException>(
            () => engine.RunAsync(CancellationToken.None));

        Assert.Same(failure.PartialExecution, engine.PartialPopulation);
        Assert.Equal("population", engine.PartialPopulation!.Phase);
        Assert.Equal(1, engine.PartialPopulation.Completed);
        Assert.Null(engine.CompletedPopulation);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void WarmupOperationErrorsAreRunFatalBeforeMeasurement(bool failed, bool dropped)
    {
        var worker = new WorkerMetrics(recordHistograms: false);
        SchedulerCounters? scheduler = null;
        if (failed)
        {
            worker.RecordStarted(OperationKind.Read);
            worker.RecordCompleted(
                OperationKind.Read,
                elapsedStopwatchTicks: 1,
                queueDelayStopwatchTicks: null,
                succeeded: false,
                resultCount: 0,
                exception: new InvalidOperationException("expected warmup failure"));
        }

        if (dropped)
        {
            scheduler = new SchedulerCounters();
            scheduler.RecordOffered(OperationKind.Read);
            scheduler.RecordDropped(OperationKind.Read);
        }

        var phase = PhaseExecution.Create(
            DateTimeOffset.UnixEpoch,
            startTimestamp: 0,
            stopTimestamp: Stopwatch.Frequency,
            drainedTimestamp: Stopwatch.Frequency,
            [worker],
            scheduler,
            recordHistograms: false);

        var failure = Assert.Throws<BenchmarkPhaseException>(
            () => BenchmarkRunEngine.EnsureWarmupSucceeded(phase));

        Assert.Same(phase, failure.PartialExecution);
        Assert.Equal(failed ? 1 : 0, phase.Failed);
        Assert.Equal(dropped ? 1 : 0, phase.Dropped);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void MeasurementOperationErrorsAreRunFatal(bool failed, bool dropped)
    {
        var phase = CreatePhaseWithErrors(failed, dropped);

        var failure = Assert.Throws<BenchmarkPhaseException>(
            () => BenchmarkRunEngine.EnsureMeasurementSucceeded(phase));

        Assert.Same(phase, failure.PartialExecution);
    }

    [Fact]
    public async Task CompletedMeasurementIsRetainedWhenOperationErrorsMakeTheRunFatal()
    {
        var spec = CreateCoordinatedRunSpec();
        var engine = new BenchmarkRunEngine(spec, new CompletedErrorRunExecutor(spec.Dataset));

        var failure = await Assert.ThrowsAsync<BenchmarkPhaseException>(
            () => engine.RunAsync(CancellationToken.None));

        Assert.NotNull(engine.CompletedMeasurement);
        Assert.Same(engine.CompletedMeasurement, failure.PartialExecution);
        Assert.True(engine.CompletedMeasurement!.Failed > 0);
        Assert.Null(engine.CompletedFinalAudit);
    }

    [Fact]
    public async Task RemoteMeasurementFailureStopsEveryClientBeforeFinalAudit()
    {
        var spec = CreateCoordinatedRunSpec();
        var barrier = new BenchmarkBarrierGrain();
        var barrierLock = new object();
        var trace = new List<(int Ordinal, string Phase, bool Succeeded)>();
        var firstOperations = new CoordinatedRunExecutor(spec.Dataset, failMeasurement: true);
        var secondOperations = new CoordinatedRunExecutor(spec.Dataset, failMeasurement: false);
        var first = CreateEngine(0, firstOperations);
        var second = CreateEngine(1, secondOperations);

        var firstTask = first.RunAsync(CancellationToken.None);
        var secondTask = second.RunAsync(CancellationToken.None);
        var firstFailure = await Record.ExceptionAsync(() => firstTask.WaitAsync(TestDeadline));
        var secondFailure = await Record.ExceptionAsync(() => secondTask.WaitAsync(TestDeadline));

        Assert.IsType<BenchmarkPhaseException>(firstFailure);
        var distributed = Assert.IsType<DistributedBenchmarkPhaseException>(secondFailure);
        Assert.Equal("measurement-complete", distributed.Phase);
        Assert.Equal([0], distributed.FailedClientOrdinals);
        Assert.Null(first.CompletedFinalAudit);
        Assert.Null(second.CompletedFinalAudit);
        Assert.Null(first.CompletedMeasurement);
        Assert.NotNull(second.CompletedMeasurement);
        Assert.Equal(1, firstOperations.ReadStateCalls);
        Assert.Equal(1, secondOperations.ReadStateCalls);

        var expectedPhases = new[]
        {
            "population-complete",
            "initial-audit-complete",
            "warmup-complete",
            "restoration-complete",
            "measurement-complete",
        };
        Assert.Equal(expectedPhases, trace.Where(static item => item.Ordinal == 0).Select(static item => item.Phase));
        Assert.Equal(expectedPhases, trace.Where(static item => item.Ordinal == 1).Select(static item => item.Phase));
        Assert.DoesNotContain(trace, static item => item.Phase == "final-audit-complete");
        Assert.Contains(trace, static item => item is (0, "measurement-complete", false));
        Assert.Contains(trace, static item => item is (1, "measurement-complete", true));

        BenchmarkRunEngine CreateEngine(int ordinal, CoordinatedRunExecutor operations)
        {
            return new BenchmarkRunEngine(
                spec,
                operations,
                clientOrdinal: ordinal,
                clientCount: 2,
                phaseBarrier: (phase, succeeded, _) =>
                {
                    lock (barrierLock)
                    {
                        trace.Add((ordinal, phase, succeeded));
                        return barrier.SignalAndWaitAsync(
                            phase,
                            ordinal,
                            2,
                            succeeded,
                            spec.Topology.BarrierTimeoutSeconds);
                    }
                });
        }
    }

    [Fact]
    public async Task CancellationWhileWaitingAtBarrierAbortsTheSharedPhase()
    {
        var spec = CreateCoordinatedRunSpec();
        var operations = new CoordinatedRunExecutor(spec.Dataset, failMeasurement: false);
        var barrierEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var sharedCompletion = new TaskCompletionSource<BenchmarkBarrierResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var abortCalls = 0;
        var engine = new BenchmarkRunEngine(
            spec,
            operations,
            clientOrdinal: 0,
            clientCount: 2,
            phaseBarrier: (phase, succeeded, _) =>
            {
                Assert.True(succeeded);
                Assert.Equal("population-complete", phase);
                barrierEntered.TrySetResult();
                return sharedCompletion.Task;
            },
            phaseAbort: (phase, _) =>
            {
                Assert.Equal("population-complete", phase);
                Interlocked.Increment(ref abortCalls);
                sharedCompletion.TrySetResult(new BenchmarkBarrierResult
                {
                    AllSucceeded = false,
                    FailedClientOrdinals = [0],
                });
                return sharedCompletion.Task;
            });
        using var cancellation = new CancellationTokenSource();

        var run = engine.RunAsync(cancellation.Token);
        await barrierEntered.Task.WaitAsync(TestDeadline);
        cancellation.Cancel();

        var failure = await Record.ExceptionAsync(() => run.WaitAsync(TestDeadline));
        Assert.IsAssignableFrom<OperationCanceledException>(failure);
        Assert.Equal(1, Volatile.Read(ref abortCalls));
        Assert.NotNull(engine.CompletedPopulation);
        Assert.Null(engine.CompletedInitialAudit);
        Assert.False((await sharedCompletion.Task).AllSucceeded);
    }

    [Fact]
    public async Task LocalPhaseFailureAggregatesOnlyBarrierTransportFailure()
    {
        var spec = CreateCoordinatedRunSpec();
        var operations = new CoordinatedRunExecutor(spec.Dataset, failMeasurement: true);
        var engine = new BenchmarkRunEngine(
            spec,
            operations,
            clientOrdinal: 0,
            clientCount: 2,
            phaseBarrier: (phase, succeeded, _) =>
            {
                if (phase == "measurement-complete")
                {
                    return Task.FromException<BenchmarkBarrierResult>(
                        new InvalidOperationException("barrier transport failure"));
                }

                return Task.FromResult(new BenchmarkBarrierResult
                {
                    AllSucceeded = succeeded,
                    FailedClientOrdinals = succeeded ? [] : [0],
                });
            });

        var failure = await Assert.ThrowsAsync<AggregateException>(
            () => engine.RunAsync(CancellationToken.None));
        var failures = failure.Flatten().InnerExceptions;

        Assert.Equal(2, failures.Count);
        Assert.IsType<BenchmarkPhaseException>(failures[0]);
        Assert.IsType<InvalidOperationException>(failures[1]);
    }

    private static PhaseExecution CreatePhaseWithErrors(bool failed, bool dropped)
    {
        var worker = new WorkerMetrics(recordHistograms: false);
        SchedulerCounters? scheduler = null;
        if (failed)
        {
            worker.RecordStarted(OperationKind.Read);
            worker.RecordCompleted(
                OperationKind.Read,
                elapsedStopwatchTicks: 1,
                queueDelayStopwatchTicks: null,
                succeeded: false,
                resultCount: 0,
                exception: new InvalidOperationException("expected measurement failure"));
        }

        if (dropped)
        {
            scheduler = new SchedulerCounters();
            scheduler.RecordOffered(OperationKind.Read);
            scheduler.RecordDropped(OperationKind.Read);
        }

        return PhaseExecution.Create(
            DateTimeOffset.UnixEpoch,
            startTimestamp: 0,
            stopTimestamp: Stopwatch.Frequency,
            drainedTimestamp: Stopwatch.Frequency,
            [worker],
            scheduler,
            recordHistograms: false);
    }

    private static TException? FindException<TException>(Exception exception)
        where TException : Exception
    {
        if (exception is TException matched)
        {
            return matched;
        }

        if (exception is AggregateException aggregate)
        {
            return aggregate.Flatten().InnerExceptions
                .Select(FindException<TException>)
                .FirstOrDefault(static value => value is not null);
        }

        return exception.InnerException is null ? null : FindException<TException>(exception.InnerException);
    }

    private static BenchmarkSpec CreateCoordinatedRunSpec()
    {
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "coordinated-run-test",
            Dataset = new SpecReference { Path = "dataset.json", Sha256 = new string('0', 64) },
            Workload = new SpecReference { Path = "workload.json", Sha256 = new string('0', 64) },
            Population = new PopulationSpec
            {
                Enabled = true,
                Concurrency = 1,
                OperationTimeoutSeconds = 5,
                LateCallDrainTimeoutSeconds = 1,
            },
            Audit = new CorrectnessAuditSpec
            {
                Enabled = true,
                PointSampleCount = 2,
                QuerySampleCount = 0,
                OperationTimeoutSeconds = 5,
                LateCallDrainTimeoutSeconds = 1,
            },
            Topology = new TopologySpec
            {
                BarrierTimeoutSeconds = 5,
                BarrierLateCallDrainTimeoutSeconds = 1,
            },
        };
        var dataset = new DatasetSpec
        {
            Id = "coordinated-run-test",
            Seed = 123,
            RecordCount = 2,
            ExactValueCardinality = 2,
            RangeValueCardinality = 2,
            PayloadBytes = 8,
        };
        var workload = new WorkloadSpec
        {
            Id = "coordinated-run-test",
            Mode = LoadMode.ClosedLoop,
            WarmupSeconds = 0,
            DurationSeconds = 1,
            Concurrency = 1,
            TargetRatePerSecond = 1,
            MaximumQueueDepth = 1,
            OperationTimeoutSeconds = 5,
            LateCallDrainTimeoutSeconds = 1,
            QuerySelectivity = new QuerySelectivitySpec
            {
                ExactFraction = 0.5,
                RangeFraction = 0.5,
                MaximumExpectedResultCount = 2,
            },
            Operations = new OperationMixSpec
            {
                Read = 1,
                Upsert = 0,
                ExactQuery = 0,
                RangeQuery = 0,
                Clear = 0,
            },
        };
        return new BenchmarkSpec(scenario, dataset, workload);
    }

    private static BenchmarkSpec CreateSpec(
        LoadMode mode,
        int concurrency,
        double targetRatePerSecond = 1_000,
        int maximumQueueDepth = 16,
        int operationTimeoutSeconds = 30,
        int lateCallDrainTimeoutSeconds = 5)
    {
        var scenario = new BenchmarkScenarioSpec
        {
            Name = "load-runner-lifecycle-test",
            Dataset = new SpecReference { Path = "dataset.json", Sha256 = new string('0', 64) },
            Workload = new SpecReference { Path = "workload.json", Sha256 = new string('0', 64) },
        };
        var dataset = new DatasetSpec
        {
            Id = "load-runner-lifecycle-test",
            Seed = 0x0123456789ABCDEF,
            RecordCount = 1_000,
            ExactValueCardinality = 128,
            RangeValueCardinality = 1_000,
            PayloadBytes = 32,
        };
        var workload = new WorkloadSpec
        {
            Id = "load-runner-lifecycle-test",
            Mode = mode,
            WarmupSeconds = 0,
            DurationSeconds = 30,
            Concurrency = concurrency,
            TargetRatePerSecond = targetRatePerSecond,
            MaximumQueueDepth = maximumQueueDepth,
            OperationTimeoutSeconds = operationTimeoutSeconds,
            LateCallDrainTimeoutSeconds = lateCallDrainTimeoutSeconds,
        };
        return new BenchmarkSpec(scenario, dataset, workload);
    }

    private sealed class CoordinatedRunExecutor(DatasetSpec dataset, bool failMeasurement)
        : IBenchmarkOperationExecutor
    {
        private int readStateCalls;

        public int ReadStateCalls => Volatile.Read(ref readStateCalls);

        public Task UpsertAsync(long ordinal, long revision)
        {
            _ = ordinal;
            _ = revision;
            return Task.CompletedTask;
        }

        public async Task<long> ExecuteAsync(OperationInvocation invocation)
        {
            _ = invocation;
            if (failMeasurement)
            {
                throw new BenchmarkCallTimeoutException(
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(1),
                    TimeSpan.Zero,
                    lateCallDrainIncomplete: false,
                    new TimeoutException("expected measurement timeout"));
            }

            await Task.Delay(2);
            return 1;
        }

        public Task<BenchmarkRecordState?> ReadStateAsync(long ordinal)
        {
            Interlocked.Increment(ref readStateCalls);
            return Task.FromResult<BenchmarkRecordState?>(
                DeterministicData.CreateState(dataset, ordinal, revision: 0));
        }

        public Task<IReadOnlyList<string>> FindKeysAsync(string exactValue) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper) => throw UnexpectedCall();

        private static InvalidOperationException UnexpectedCall()
        {
            return new InvalidOperationException("The coordinated run test does not use searchable query operations.");
        }
    }

    private sealed class CompletedErrorRunExecutor(DatasetSpec dataset) : IBenchmarkOperationExecutor
    {
        public Task UpsertAsync(long ordinal, long revision)
        {
            _ = ordinal;
            _ = revision;
            return Task.CompletedTask;
        }

        public async Task<long> ExecuteAsync(OperationInvocation invocation)
        {
            _ = invocation;
            await Task.Delay(2);
            throw new InvalidOperationException("expected measured operation failure");
        }

        public Task<BenchmarkRecordState?> ReadStateAsync(long ordinal)
        {
            return Task.FromResult<BenchmarkRecordState?>(
                DeterministicData.CreateState(dataset, ordinal, revision: 0));
        }

        public Task<IReadOnlyList<string>> FindKeysAsync(string exactValue) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper) => throw UnexpectedCall();

        private static InvalidOperationException UnexpectedCall()
        {
            return new InvalidOperationException("The completed-error test does not use searchable query operations.");
        }
    }

    private sealed class ControlledOperationExecutor(int expectedConcurrency) : IBenchmarkOperationExecutor
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _active;
        private int _invocationCount;
        private int _maximumActive;

        public TaskCompletionSource FirstStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource ExpectedConcurrencyReached { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Active => Volatile.Read(ref _active);

        public int InvocationCount => Volatile.Read(ref _invocationCount);

        public int MaximumActive => Volatile.Read(ref _maximumActive);

        public async Task<long> ExecuteAsync(OperationInvocation invocation)
        {
            _ = invocation;
            Interlocked.Increment(ref _invocationCount);
            var active = Interlocked.Increment(ref _active);
            UpdateMaximum(active);
            FirstStarted.TrySetResult();
            if (active == expectedConcurrency)
            {
                ExpectedConcurrencyReached.TrySetResult();
            }

            try
            {
                await _release.Task;
                return 0;
            }
            finally
            {
                Interlocked.Decrement(ref _active);
            }
        }

        public void Release() => _release.TrySetResult();

        public Task UpsertAsync(long ordinal, long revision) => throw UnexpectedCall();

        public Task<BenchmarkRecordState?> ReadStateAsync(long ordinal) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> FindKeysAsync(string exactValue) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper) => throw UnexpectedCall();

        private static InvalidOperationException UnexpectedCall()
        {
            return new InvalidOperationException("The lifecycle test executor only supports workload operations.");
        }

        private void UpdateMaximum(int active)
        {
            while (true)
            {
                var current = Volatile.Read(ref _maximumActive);
                if (active <= current || Interlocked.CompareExchange(ref _maximumActive, active, current) == current)
                {
                    return;
                }
            }
        }
    }

    private sealed class FailingPopulationExecutor : IBenchmarkOperationExecutor
    {
        private int calls;

        public Task UpsertAsync(long ordinal, long revision)
        {
            _ = ordinal;
            _ = revision;
            return Interlocked.Increment(ref calls) == 1
                ? Task.CompletedTask
                : Task.FromException(new InvalidOperationException("population failure"));
        }

        public Task<long> ExecuteAsync(OperationInvocation invocation) => throw UnexpectedCall();

        public Task<BenchmarkRecordState?> ReadStateAsync(long ordinal) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> FindKeysAsync(string exactValue) => throw UnexpectedCall();

        public Task<IReadOnlyList<string>> RangeKeysAsync(int lower, int upper) => throw UnexpectedCall();

        private static InvalidOperationException UnexpectedCall()
        {
            return new InvalidOperationException("The population test executor only supports upserts.");
        }
    }
}
