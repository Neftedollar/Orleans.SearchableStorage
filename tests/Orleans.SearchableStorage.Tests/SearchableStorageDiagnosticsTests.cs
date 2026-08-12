using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Orleans.SearchableStorage.Diagnostics;

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
}
