using Orleans.Runtime;
using Orleans.Concurrency;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class BenchmarkRecordConstants
{
    // The configured outer barrier timeout and late-call drain are each capped at one hour.
    // Orleans' transport timeout must cover both windows so the driver-owned deadlines remain
    // authoritative instead of the default 30-second response timeout ending the grain call.
    public const string BarrierResponseTimeout = "02:05:00";

    public const int BarrierResultDeliveryMarginSeconds = 30;

    public const string StateName = "benchmark-record";

    public const string StorageProviderName = "BenchmarkSearchable";

    public const string PlainStorageProviderName = "Orleans.SearchableStorage.Benchmarks.PlainPhysical";
}

[GenerateSerializer]
public sealed class BenchmarkRecordState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string ExactValue { get; set; } = string.Empty;

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public int RangeValue { get; set; }

    [Id(2)]
    public long Revision { get; set; }

    [Id(3)]
    public byte[] Payload { get; set; } = [];
}

public interface IBenchmarkRecordGrain : IGrainWithStringKey
{
    Task<BenchmarkRecordState?> ReadAsync();

    Task UpsertAsync(BenchmarkRecordState state);

    Task ClearAsync();
}

public interface IPlainBenchmarkRecordGrain : IGrainWithStringKey
{
    Task<BenchmarkRecordState?> ReadAsync();

    Task UpsertAsync(BenchmarkRecordState state);

    Task ClearAsync();
}

public interface IBenchmarkBarrierGrain : IGrainWithStringKey
{
    [ResponseTimeout(BenchmarkRecordConstants.BarrierResponseTimeout)]
    Task<BenchmarkBarrierResult> SignalAndWaitAsync(
        string phase,
        int clientOrdinal,
        int clientCount,
        bool succeeded,
        int timeoutSeconds);

    [ResponseTimeout(BenchmarkRecordConstants.BarrierResponseTimeout)]
    Task<BenchmarkBarrierResult> AbortPhaseAsync(
        string phase,
        int clientOrdinal,
        int clientCount,
        int timeoutSeconds);
}

[GenerateSerializer]
public sealed class BenchmarkBarrierResult
{
    [Id(0)]
    public bool AllSucceeded { get; init; }

    [Id(1)]
    public int[] FailedClientOrdinals { get; init; } = [];

    [Id(2)]
    public bool DeadlineExceeded { get; init; }

    [Id(3)]
    public int[] MissingClientOrdinals { get; init; } = [];
}

[Reentrant]
public sealed class BenchmarkBarrierGrain : Grain, IBenchmarkBarrierGrain
{
    private readonly Dictionary<string, BarrierState> _phases = new(StringComparer.Ordinal);
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    public BenchmarkBarrierGrain()
        : this(static (timeout, cancellationToken) => Task.Delay(timeout, cancellationToken))
    {
    }

    internal BenchmarkBarrierGrain(Func<TimeSpan, CancellationToken, Task> delay)
    {
        ArgumentNullException.ThrowIfNull(delay);
        _delay = delay;
    }

    public Task<BenchmarkBarrierResult> SignalAndWaitAsync(
        string phase,
        int clientOrdinal,
        int clientCount,
        bool succeeded,
        int timeoutSeconds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentOutOfRangeException.ThrowIfNegative(clientOrdinal);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(clientCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(clientOrdinal, clientCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(timeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            timeoutSeconds,
            TopologySpec.MaximumBarrierTimeoutSeconds);

        if (!_phases.TryGetValue(phase, out var state))
        {
            state = new BarrierState(clientCount, timeoutSeconds);
            _phases.Add(phase, state);
            _ = CompleteAtDeadlineAsync(state);
        }
        else if (state.ClientCount != clientCount)
        {
            throw new InvalidOperationException(
                $"Barrier phase '{phase}' was configured for {state.ClientCount} clients, not {clientCount}.");
        }
        else if (state.TimeoutSeconds != timeoutSeconds)
        {
            throw new InvalidOperationException(
                $"Barrier phase '{phase}' was configured for a {state.TimeoutSeconds}-second deadline, " +
                $"not {timeoutSeconds} seconds.");
        }

        if (state.Arrivals.TryGetValue(clientOrdinal, out var previousSucceeded))
        {
            if (previousSucceeded != succeeded && !state.AbortedOrdinals.Contains(clientOrdinal))
            {
                throw new InvalidOperationException(
                    $"Client {clientOrdinal} reported conflicting outcomes for barrier phase '{phase}'.");
            }

            return state.Completion.Task;
        }

        if (state.Completion.Task.IsCompleted)
        {
            return state.Completion.Task;
        }

        state.Arrivals.Add(clientOrdinal, succeeded);
        if (state.Arrivals.Count == clientCount)
        {
            var failedClientOrdinals = state.Arrivals
                .Where(static arrival => !arrival.Value)
                .Select(static arrival => arrival.Key)
                .Order()
                .ToArray();
            TryComplete(state, new BenchmarkBarrierResult
            {
                AllSucceeded = failedClientOrdinals.Length == 0,
                FailedClientOrdinals = failedClientOrdinals,
            });
        }

        return state.Completion.Task;
    }

    public Task<BenchmarkBarrierResult> AbortPhaseAsync(
        string phase,
        int clientOrdinal,
        int clientCount,
        int timeoutSeconds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentOutOfRangeException.ThrowIfNegative(clientOrdinal);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(clientCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(clientOrdinal, clientCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(timeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            timeoutSeconds,
            TopologySpec.MaximumBarrierTimeoutSeconds);

        if (!_phases.TryGetValue(phase, out var state))
        {
            state = new BarrierState(clientCount, timeoutSeconds);
            _phases.Add(phase, state);
            _ = CompleteAtDeadlineAsync(state);
        }
        else if (state.ClientCount != clientCount || state.TimeoutSeconds != timeoutSeconds)
        {
            throw new InvalidOperationException(
                $"Barrier abort for phase '{phase}' does not match its client count or deadline.");
        }

        if (state.Completion.Task.IsCompleted)
        {
            return state.Completion.Task;
        }

        state.Arrivals[clientOrdinal] = false;
        state.AbortedOrdinals.Add(clientOrdinal);
        var failedClientOrdinals = state.Arrivals
            .Where(static arrival => !arrival.Value)
            .Select(static arrival => arrival.Key)
            .Order()
            .ToArray();
        TryComplete(state, new BenchmarkBarrierResult
        {
            AllSucceeded = false,
            FailedClientOrdinals = failedClientOrdinals,
        });
        return state.Completion.Task;
    }

    private async Task CompleteAtDeadlineAsync(BarrierState state)
    {
        try
        {
            await _delay(
                TimeSpan.FromSeconds(state.TimeoutSeconds),
                state.DeadlineCancellation.Token);
        }
        catch (OperationCanceledException) when (state.DeadlineCancellation.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _ = state.Completion.TrySetException(exception);
            return;
        }

        var failedClientOrdinals = Enumerable.Range(0, state.ClientCount)
            .Where(ordinal => !state.Arrivals.GetValueOrDefault(ordinal))
            .ToArray();
        _ = state.Completion.TrySetResult(new BenchmarkBarrierResult
        {
            AllSucceeded = false,
            FailedClientOrdinals = failedClientOrdinals,
            DeadlineExceeded = true,
            MissingClientOrdinals = Enumerable.Range(0, state.ClientCount)
                .Where(ordinal => !state.Arrivals.ContainsKey(ordinal))
                .ToArray(),
        });
    }

    private static void TryComplete(BarrierState state, BenchmarkBarrierResult result)
    {
        if (state.Completion.TrySetResult(result))
        {
            state.DeadlineCancellation.Cancel();
        }
    }

    private sealed class BarrierState(int clientCount, int timeoutSeconds)
    {
        public int ClientCount { get; } = clientCount;

        public int TimeoutSeconds { get; } = timeoutSeconds;

        public Dictionary<int, bool> Arrivals { get; } = [];

        public HashSet<int> AbortedOrdinals { get; } = [];

        public CancellationTokenSource DeadlineCancellation { get; } = new();

        public TaskCompletionSource<BenchmarkBarrierResult> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

public sealed class BenchmarkRecordGrain(
    [PersistentState(BenchmarkRecordConstants.StateName, BenchmarkRecordConstants.StorageProviderName)]
    IPersistentState<BenchmarkRecordState> persistentState)
    : Grain, IBenchmarkRecordGrain
{
    public Task<BenchmarkRecordState?> ReadAsync()
    {
        return Task.FromResult(persistentState.RecordExists ? persistentState.State : null);
    }

    public Task UpsertAsync(BenchmarkRecordState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        persistentState.State = state;
        return persistentState.WriteStateAsync();
    }

    public Task ClearAsync()
    {
        return persistentState.ClearStateAsync();
    }
}

public sealed class PlainBenchmarkRecordGrain(
    [PersistentState(BenchmarkRecordConstants.StateName, BenchmarkRecordConstants.PlainStorageProviderName)]
    IPersistentState<BenchmarkRecordState> persistentState)
    : Grain, IPlainBenchmarkRecordGrain
{
    public Task<BenchmarkRecordState?> ReadAsync()
    {
        return Task.FromResult(persistentState.RecordExists ? persistentState.State : null);
    }

    public Task UpsertAsync(BenchmarkRecordState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        persistentState.State = state;
        return persistentState.WriteStateAsync();
    }

    public Task ClearAsync()
    {
        return persistentState.ClearStateAsync();
    }
}
