using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Orleans.SearchableStorage.Diagnostics;

internal static partial class SearchableStorageDiagnostics
{
    internal const string MeterName = "Orleans.SearchableStorage";
    internal const string OperationCountInstrumentName =
        "orleans.searchable_storage.operation.count";
    internal const string OperationDurationInstrumentName =
        "orleans.searchable_storage.operation.duration";
    internal const string OperationWorkInstrumentName =
        "orleans.searchable_storage.operation.work";

    internal const string ProviderTagName = "provider";
    internal const string OperationTagName = "operation";
    internal const string PhaseTagName = "phase";
    internal const string OutcomeTagName = "outcome";

    internal const string SuccessOutcome = "success";
    internal const string FailureOutcome = "failure";
    internal const string CancelledOutcome = "cancelled";

    private static readonly Meter Meter = new(MeterName);
    private static readonly Counter<long> OperationCount = Meter.CreateCounter<long>(
        OperationCountInstrumentName,
        unit: "{operation}",
        description: "Completed searchable-storage operations by bounded outcome.");
    private static readonly Histogram<double> OperationDuration = Meter.CreateHistogram<double>(
        OperationDurationInstrumentName,
        unit: "s",
        description: "Searchable-storage operation duration in seconds.");
    private static readonly Histogram<long> OperationWork = Meter.CreateHistogram<long>(
        OperationWorkInstrumentName,
        unit: "{item}",
        description: "Bounded logical items completed by searchable-storage operations.");

    internal static async Task ObserveAsync(
        string provider,
        string operation,
        string phase,
        ILogger? logger,
        bool lifecycle,
        Func<Task> execute,
        long? successWork = null,
        bool logSuccess = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentNullException.ThrowIfNull(execute);
        ValidateWork(successWork);

        var started = Stopwatch.GetTimestamp();
        try
        {
            await execute();
        }
        catch (OperationCanceledException exception)
        {
            TryRecordOutcome(
                provider,
                operation,
                phase,
                CancelledOutcome,
                started,
                work: null);
            LogFailure(logger, lifecycle, provider, operation, phase, exception, cancelled: true);
            throw;
        }
        catch (Exception exception)
        {
            TryRecordOutcome(
                provider,
                operation,
                phase,
                FailureOutcome,
                started,
                work: null);
            LogFailure(logger, lifecycle, provider, operation, phase, exception, cancelled: false);
            throw;
        }

        TryRecordOutcome(
            provider,
            operation,
            phase,
            SuccessOutcome,
            started,
            successWork);
        if (lifecycle && logSuccess && logger is not null)
        {
            TryLog(() => LifecycleCompleted(
                logger,
                provider,
                operation,
                phase,
                successWork ?? 0));
        }
    }

    internal static async Task<T> ObserveAsync<T>(
        string provider,
        string operation,
        string phase,
        ILogger? logger,
        bool lifecycle,
        Func<Task<T>> execute,
        Func<T, long>? getSuccessWork = null,
        bool logSuccess = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentNullException.ThrowIfNull(execute);

        var started = Stopwatch.GetTimestamp();
        T result;
        try
        {
            result = await execute();
        }
        catch (OperationCanceledException exception)
        {
            TryRecordOutcome(
                provider,
                operation,
                phase,
                CancelledOutcome,
                started,
                work: null);
            LogFailure(logger, lifecycle, provider, operation, phase, exception, cancelled: true);
            throw;
        }
        catch (Exception exception)
        {
            TryRecordOutcome(
                provider,
                operation,
                phase,
                FailureOutcome,
                started,
                work: null);
            LogFailure(logger, lifecycle, provider, operation, phase, exception, cancelled: false);
            throw;
        }

        long? work = null;
        if (getSuccessWork is not null)
        {
            try
            {
                work = getSuccessWork(result);
                ValidateWork(work);
            }
            catch
            {
                // Telemetry enrichment must never change a successfully completed operation.
                work = null;
            }
        }

        TryRecordOutcome(
            provider,
            operation,
            phase,
            SuccessOutcome,
            started,
            work);
        if (lifecycle && logSuccess && logger is not null)
        {
            TryLog(() => LifecycleCompleted(logger, provider, operation, phase, work ?? 0));
        }

        return result;
    }

    private static void TryRecordOutcome(
        string provider,
        string operation,
        string phase,
        string outcome,
        long started,
        long? work)
    {
        try
        {
            var tags = new TagList
            {
                { ProviderTagName, provider },
                { OperationTagName, operation },
                { PhaseTagName, phase },
                { OutcomeTagName, outcome },
            };
            OperationCount.Add(1, tags);
            OperationDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags);
            if (work is not null)
            {
                OperationWork.Record(work.Value, tags);
            }
        }
        catch
        {
            // Metrics listeners are diagnostics and cannot change storage semantics.
        }
    }

    private static void LogFailure(
        ILogger? logger,
        bool lifecycle,
        string provider,
        string operation,
        string phase,
        Exception exception,
        bool cancelled)
    {
        if (logger is null)
        {
            return;
        }

        var errorType = exception.GetType().FullName ?? exception.GetType().Name;
        TryLog(() =>
        {
            if (cancelled && lifecycle)
            {
                LifecycleCancelled(logger, provider, operation, phase, errorType);
            }
            else if (cancelled)
            {
                RequestCancelled(logger, provider, operation, phase, errorType);
            }
            else if (lifecycle)
            {
                LifecycleFailed(logger, provider, operation, phase, errorType);
            }
            else
            {
                RequestFailed(logger, provider, operation, phase, errorType);
            }
        });
    }

    private static void TryLog(Action log)
    {
        try
        {
            log();
        }
        catch
        {
            // Logging providers are diagnostics and cannot change storage semantics.
        }
    }

    private static void ValidateWork(long? work)
    {
        if (work < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(work), work, "Observed work cannot be negative.");
        }
    }

    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Debug,
        Message = "Searchable-storage request failed for provider {Provider}; operation {Operation}; phase {Phase}; error type {ErrorType}.")]
    private static partial void RequestFailed(
        ILogger logger,
        string provider,
        string operation,
        string phase,
        string errorType);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Warning,
        Message = "Searchable-storage lifecycle operation failed for provider {Provider}; operation {Operation}; phase {Phase}; error type {ErrorType}.")]
    private static partial void LifecycleFailed(
        ILogger logger,
        string provider,
        string operation,
        string phase,
        string errorType);

    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Information,
        Message = "Searchable-storage lifecycle operation was cancelled for provider {Provider}; operation {Operation}; phase {Phase}; error type {ErrorType}.")]
    private static partial void LifecycleCancelled(
        ILogger logger,
        string provider,
        string operation,
        string phase,
        string errorType);

    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Information,
        Message = "Searchable-storage lifecycle operation completed for provider {Provider}; operation {Operation}; phase {Phase}; bounded work {WorkCount}.")]
    private static partial void LifecycleCompleted(
        ILogger logger,
        string provider,
        string operation,
        string phase,
        long workCount);

    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Debug,
        Message = "Searchable-storage request was cancelled for provider {Provider}; operation {Operation}; phase {Phase}; error type {ErrorType}.")]
    private static partial void RequestCancelled(
        ILogger logger,
        string provider,
        string operation,
        string phase,
        string errorType);
}
