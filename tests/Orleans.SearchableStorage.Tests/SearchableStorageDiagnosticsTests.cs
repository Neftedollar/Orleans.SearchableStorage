using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Reflection;
using Microsoft.Extensions.Logging;
using Orleans.SearchableStorage.Diagnostics;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageDiagnosticsTests
{
    [Fact]
    public async Task SuccessRecordsOneOutcomeDurationAndBoundedWork()
    {
        var provider = UniqueProvider();
        using var measurements = new MeasurementCollector(provider);

        var result = await SearchableStorageDiagnostics.ObserveAsync(
            provider,
            "query.page",
            "execute",
            logger: null,
            lifecycle: false,
            () => Task.FromResult(7),
            static value => value);

        Assert.Equal(7, result);
        AssertSingleOperation(
            measurements,
            SearchableStorageDiagnostics.SuccessOutcome,
            expectedWork: 7,
            provider,
            "query.page");
    }

    [Fact]
    public async Task FailureRecordsOneOutcomeAndDurationWithoutWork()
    {
        var provider = UniqueProvider();
        using var measurements = new MeasurementCollector(provider);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            SearchableStorageDiagnostics.ObserveAsync(
                provider,
                "storage.write",
                "execute",
                logger: null,
                lifecycle: false,
                () => Task.FromException(new InvalidOperationException("private-value"))));

        AssertSingleOperation(
            measurements,
            SearchableStorageDiagnostics.FailureOutcome,
            expectedWork: null,
            provider,
            "storage.write");
    }

    [Fact]
    public async Task CancellationRecordsOneOutcomeAndDurationWithoutWork()
    {
        var provider = UniqueProvider();
        using var measurements = new MeasurementCollector(provider);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            SearchableStorageDiagnostics.ObserveAsync(
                provider,
                "query.facet.count",
                "execute",
                logger: null,
                lifecycle: false,
                () => Task.FromCanceled(new CancellationToken(canceled: true))));

        AssertSingleOperation(
            measurements,
            SearchableStorageDiagnostics.CancelledOutcome,
            expectedWork: null,
            provider,
            "query.facet.count");
    }

    [Theory]
    [InlineData(false, LogLevel.Debug)]
    [InlineData(true, LogLevel.Warning)]
    public async Task FailureLogContainsOnlySafeFieldsAndNeverCapturesException(
        bool lifecycle,
        LogLevel expectedLevel)
    {
        const string secret = "record-key-and-token-secret";
        var logger = new CapturingLogger();

        await Assert.ThrowsAsync<PrivateFailure>(() =>
            SearchableStorageDiagnostics.ObserveAsync(
                "provider-a",
                "persistence.compaction",
                "automatic",
                logger,
                lifecycle,
                () => Task.FromException(new PrivateFailure(secret))));

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(expectedLevel, entry.Level);
        Assert.Null(entry.Exception);
        Assert.DoesNotContain(secret, entry.Message, StringComparison.Ordinal);
        Assert.Equal(
            ["ErrorType", "EventId", "Operation", "Phase", "Provider", "{OriginalFormat}"],
            entry.Fields.Keys.Order(StringComparer.Ordinal).ToArray());
        Assert.Equal(typeof(PrivateFailure).FullName, entry.Fields["ErrorType"]);
        Assert.DoesNotContain(
            entry.Fields.Values,
            value => value?.ToString()?.Contains(secret, StringComparison.Ordinal) == true);
    }

    [Fact]
    public async Task HotPathSuccessDoesNotWriteACompletionLog()
    {
        var logger = new CapturingLogger();

        await SearchableStorageDiagnostics.ObserveAsync(
            "provider-a",
            "storage.read",
            "execute",
            logger,
            lifecycle: false,
            () => Task.CompletedTask);

        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task LifecycleCanSuppressRoutineSuccessWithoutSuppressingFailures()
    {
        var logger = new CapturingLogger();

        await SearchableStorageDiagnostics.ObserveAsync(
            "provider-a",
            "persistence.compaction",
            "automatic",
            logger,
            lifecycle: true,
            () => Task.CompletedTask,
            logSuccess: false);
        Assert.Empty(logger.Entries);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            SearchableStorageDiagnostics.ObserveAsync(
                "provider-a",
                "persistence.compaction",
                "automatic",
                logger,
                lifecycle: true,
                () => Task.FromException(new InvalidOperationException("private-value")),
                logSuccess: false));
        Assert.Equal(LogLevel.Warning, Assert.Single(logger.Entries).Level);
    }

    [Fact]
    public async Task ThrowingWorkExtractorCannotTurnACompletedOperationIntoFailure()
    {
        var provider = UniqueProvider();
        using var measurements = new MeasurementCollector(provider);

        var result = await SearchableStorageDiagnostics.ObserveAsync(
            provider,
            "query.page",
            "execute",
            logger: null,
            lifecycle: false,
            () => Task.FromResult(7),
            static _ => throw new InvalidOperationException("telemetry extractor"));

        Assert.Equal(7, result);
        AssertSingleOperation(
            measurements,
            SearchableStorageDiagnostics.SuccessOutcome,
            expectedWork: null,
            provider,
            "query.page");
    }

    [Theory]
    [InlineData(ObservedQueryTerminal.Find)]
    [InlineData(ObservedQueryTerminal.Range)]
    [InlineData(ObservedQueryTerminal.Linq)]
    [InlineData(ObservedQueryTerminal.Page)]
    public async Task ManagedSchemaGateFailureRecordsExactlyOneOuterQueryOutcome(
        ObservedQueryTerminal terminal)
    {
        var provider = UniqueProvider();
        using var measurements = new MeasurementCollector(provider);
        var gate = new ControlledSchemaGrain();
        gate.Response.SetException(new InvalidOperationException("managed schema gate failed"));
        var (grainFactory, recording) = DiagnosticsGrainFactoryProxy.Create(gate);
        var client = CreateManagedQueryClient(provider, grainFactory);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ExecuteManagedQueryTerminalAsync(client, terminal, CancellationToken.None));

        AssertSingleOperation(
            measurements,
            SearchableStorageDiagnostics.FailureOutcome,
            expectedWork: null,
            provider,
            GetObservedOperation(terminal));
        Assert.Equal(1, gate.GetCount);
        Assert.Equal(0, recording.PartitionLookupCount);
    }

    [Theory]
    [InlineData(ObservedQueryTerminal.Find)]
    [InlineData(ObservedQueryTerminal.Range)]
    [InlineData(ObservedQueryTerminal.Linq)]
    [InlineData(ObservedQueryTerminal.Page)]
    public async Task CancelledManagedSchemaGateRecordsOnceAndDetachesTheUnderlyingCall(
        ObservedQueryTerminal terminal)
    {
        var provider = UniqueProvider();
        using var measurements = new MeasurementCollector(provider);
        var gate = new ControlledSchemaGrain();
        var detached = new TaskCompletionSource<Task>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var (grainFactory, recording) = DiagnosticsGrainFactoryProxy.Create(gate);
        var client = CreateManagedQueryClient(
            provider,
            grainFactory,
            task => detached.TrySetResult(task));
        using var cancellation = new CancellationTokenSource();

        var query = ExecuteManagedQueryTerminalAsync(client, terminal, cancellation.Token);
        var request = await gate.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => query);
        Assert.Same(gate.Response.Task, await detached.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.False(gate.Response.Task.IsCompleted,
            "canceling the caller must not cancel the shared Orleans schema-control call");
        AssertSingleOperation(
            measurements,
            SearchableStorageDiagnostics.CancelledOutcome,
            expectedWork: null,
            provider,
            GetObservedOperation(terminal));
        Assert.Equal(1, gate.GetCount);
        Assert.Equal(0, recording.PartitionLookupCount);

        gate.Response.SetResult(new StorageIndexSchemaSnapshot
        {
            ProviderName = request.ProviderName,
            StateName = request.StateName,
            ActiveFingerprint = [.. request.Fingerprint],
        });
        _ = await gate.Response.Task;
    }

    [Theory]
    [InlineData(ObservedQueryTerminal.Linq)]
    [InlineData(ObservedQueryTerminal.Page)]
    public async Task SuccessfulQueryTerminalHasNoNestedDuplicateObservation(
        ObservedQueryTerminal terminal)
    {
        var provider = UniqueProvider();
        using var measurements = new MeasurementCollector(provider);
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "diagnostics-tests",
            Enumerable.Repeat((byte)0x5C, 32).ToArray());
        var client = new SearchableStorageClient(
            provider,
            [new UnusedPartitionGrain()],
            static () => Task.FromResult(true),
            options);
        var query = client.Query<ObservedQueryState>(ObservedStateName)
            .Where(static state => state.City == "Haifa" && state.City == "Jerusalem");

        if (terminal == ObservedQueryTerminal.Linq)
        {
            Assert.Empty(await query.ToGrainIdsAsync());
        }
        else
        {
            Assert.Empty((await query.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(10))).Items);
        }

        AssertSingleOperation(
            measurements,
            SearchableStorageDiagnostics.SuccessOutcome,
            expectedWork: 0,
            provider,
            GetObservedOperation(terminal));
    }

    private static void AssertSingleOperation(
        MeasurementCollector measurements,
        string outcome,
        long? expectedWork,
        string provider,
        string operation)
    {
        var count = Assert.Single(measurements.Counts);
        Assert.Equal(1, count.Value);
        AssertExpectedTags(count.Tags, outcome, provider, operation);

        var duration = Assert.Single(measurements.Durations);
        Assert.True(duration.Value >= 0);
        AssertExpectedTags(duration.Tags, outcome, provider, operation);

        if (expectedWork is null)
        {
            Assert.Empty(measurements.Work);
        }
        else
        {
            var work = Assert.Single(measurements.Work);
            Assert.Equal(expectedWork.Value, work.Value);
            AssertExpectedTags(work.Tags, outcome, provider, operation);
        }
    }

    private static void AssertExpectedTags(
        IReadOnlyDictionary<string, object?> tags,
        string outcome,
        string provider,
        string operation)
    {
        Assert.Equal(
            [
                SearchableStorageDiagnostics.OperationTagName,
                SearchableStorageDiagnostics.OutcomeTagName,
                SearchableStorageDiagnostics.PhaseTagName,
                SearchableStorageDiagnostics.ProviderTagName,
            ],
            tags.Keys.Order(StringComparer.Ordinal).ToArray());
        Assert.Equal(provider, tags[SearchableStorageDiagnostics.ProviderTagName]);
        Assert.Equal(operation, tags[SearchableStorageDiagnostics.OperationTagName]);
        Assert.Equal("execute", tags[SearchableStorageDiagnostics.PhaseTagName]);
        Assert.Equal(outcome, tags[SearchableStorageDiagnostics.OutcomeTagName]);
    }

    private sealed class MeasurementCollector : IDisposable
    {
        private readonly MeterListener _listener = new();

        private readonly string _provider;

        public MeasurementCollector(string provider)
        {
            _provider = provider;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (string.Equals(
                        instrument.Meter.Name,
                        SearchableStorageDiagnostics.MeterName,
                        StringComparison.Ordinal))
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                var copiedTags = CopyTags(tags);
                if (!MatchesProvider(copiedTags))
                {
                    return;
                }

                var measurement = new Measurement<long>(value, copiedTags);
                if (instrument.Name == SearchableStorageDiagnostics.OperationCountInstrumentName)
                {
                    Counts.Add(measurement);
                }
                else if (instrument.Name == SearchableStorageDiagnostics.OperationWorkInstrumentName)
                {
                    Work.Add(measurement);
                }
            });
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            {
                if (instrument.Name == SearchableStorageDiagnostics.OperationDurationInstrumentName)
                {
                    var copiedTags = CopyTags(tags);
                    if (MatchesProvider(copiedTags))
                    {
                        Durations.Add(new Measurement<double>(value, copiedTags));
                    }
                }
            });
            _listener.Start();
        }

        public List<Measurement<long>> Counts { get; } = [];

        public List<Measurement<double>> Durations { get; } = [];

        public List<Measurement<long>> Work { get; } = [];

        public void Dispose()
        {
            _listener.Dispose();
        }

        private static Dictionary<string, object?> CopyTags(
            ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var copy = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                copy.Add(tag.Key, tag.Value);
            }

            return copy;
        }

        private bool MatchesProvider(Dictionary<string, object?> tags)
        {
            return tags.TryGetValue(SearchableStorageDiagnostics.ProviderTagName, out var value)
                && string.Equals(value as string, _provider, StringComparison.Ordinal);
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<LogEntry> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var fields = state is IEnumerable<KeyValuePair<string, object?>> values
                ? values.ToDictionary(static value => value.Key, static value => value.Value)
                : new Dictionary<string, object?>();
            fields["EventId"] = eventId.Id;
            Entries.Add(new LogEntry(
                logLevel,
                formatter(state, exception),
                exception,
                fields));
        }
    }

    private sealed record LogEntry(
        LogLevel Level,
        string Message,
        Exception? Exception,
        IReadOnlyDictionary<string, object?> Fields);

    private sealed class PrivateFailure(string message) : Exception(message);

    private static string UniqueProvider() => $"diagnostics-test-{Guid.NewGuid():N}";

    private sealed record Measurement<T>(
        T Value,
        IReadOnlyDictionary<string, object?> Tags);

    private static SearchableStorageClient CreateManagedQueryClient(
        string provider,
        IGrainFactory grainFactory,
        Action<Task>? detachedFanoutObserver = null)
    {
        var registry = new SearchableStorageSchemaRegistry()
            .AddState<ObservedQueryState>(ObservedStateName)
            .CreateRegistry(provider);
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "diagnostics-tests",
            Enumerable.Repeat((byte)0x6D, 32).ToArray());
        return new SearchableStorageClient(
            grainFactory,
            provider,
            partitionCount: 1,
            options,
            registry,
            logger: null,
            detachedFanoutObserver);
    }

    private static Task ExecuteManagedQueryTerminalAsync(
        SearchableStorageClient client,
        ObservedQueryTerminal terminal,
        CancellationToken cancellationToken)
    {
        var query = client.Query<ObservedQueryState>(ObservedStateName)
            .Where(static state => state.City == "Haifa");
        return terminal switch
        {
            ObservedQueryTerminal.Find => client.FindAsync<ObservedQueryState, string>(
                ObservedStateName,
                static state => state.City,
                "Haifa",
                cancellationToken),
            ObservedQueryTerminal.Range => client.RangeAsync<ObservedQueryState, int>(
                ObservedStateName,
                static state => state.Score,
                1,
                10,
                cancellationToken: cancellationToken),
            ObservedQueryTerminal.Linq => query.ToGrainIdsAsync(cancellationToken),
            ObservedQueryTerminal.Page => query.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(10),
                cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(terminal), terminal, null),
        };
    }

    private static string GetObservedOperation(ObservedQueryTerminal terminal)
    {
        return terminal == ObservedQueryTerminal.Page ? "query.page" : "query.legacy";
    }

    private const string ObservedStateName = "diagnostics-state";

    public enum ObservedQueryTerminal
    {
        Find,
        Range,
        Linq,
        Page,
    }

    private sealed class ObservedQueryState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;

        [SearchableIndex(SearchableIndexKind.Range)]
        public int Score { get; init; }
    }

    private sealed class ControlledSchemaGrain : IStorageIndexSchemaGrain
    {
        private int _getCount;

        public TaskCompletionSource<StorageIndexSchemaSnapshot> Response { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<StorageIndexSchemaRequest> Started { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public int GetCount => Volatile.Read(ref _getCount);

        public Task<StorageIndexSchemaSnapshot> GetAsync(StorageIndexSchemaRequest request)
        {
            Interlocked.Increment(ref _getCount);
            Started.TrySetResult(request);
            return Response.Task;
        }

        public Task<StorageIndexSchemaSnapshot> BeginRebuildAsync(
            StorageIndexSchemaRequest request) => throw new NotSupportedException();

        public Task<StorageIndexSchemaSnapshot> AdvanceRebuildAsync(
            StorageIndexSchemaCommand command) => throw new NotSupportedException();
    }

    [SuppressMessage(
        "Performance",
        "CA1852:Seal internal types",
        Justification = "DispatchProxy generates a runtime subclass of this type.")]
    private class DiagnosticsGrainFactoryProxy : DispatchProxy
    {
        private IStorageIndexSchemaGrain _schema = null!;

        public int PartitionLookupCount { get; private set; }

        public static (IGrainFactory GrainFactory, DiagnosticsGrainFactoryProxy Recording) Create(
            IStorageIndexSchemaGrain schema)
        {
            var grainFactory = DispatchProxy.Create<IGrainFactory, DiagnosticsGrainFactoryProxy>();
            var recording = (DiagnosticsGrainFactoryProxy)(object)grainFactory;
            recording._schema = schema;
            return (grainFactory, recording);
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            if (targetMethod.Name == nameof(IGrainFactory.GetGrain)
                && targetMethod.IsGenericMethod)
            {
                var grainType = targetMethod.GetGenericArguments()[0];
                if (grainType == typeof(IStorageLayoutGrain))
                {
                    return new UnusedLayoutGrain();
                }

                if (grainType == typeof(IStorageIndexSchemaGrain))
                {
                    return _schema;
                }

                if (grainType == typeof(IStoragePartitionGrain))
                {
                    PartitionLookupCount++;
                    throw new InvalidOperationException(
                        "A query blocked by its managed schema gate must not resolve a partition.");
                }
            }

            throw new NotSupportedException(
                $"Unexpected grain-factory call '{targetMethod.Name}'.");
        }
    }

    private sealed class UnusedLayoutGrain : StorageLayoutGrainMovementTestDouble;

    private sealed class UnusedPartitionGrain : StoragePartitionGrainMovementTestDouble;
}
