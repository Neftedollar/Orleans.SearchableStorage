namespace Orleans.SearchableStorage.Tests.TestGrains;

[GenerateSerializer]
public sealed class NoIndexSchemaState
{
    [Id(0)]
    public string Value { get; set; } = string.Empty;
}

[GenerateSerializer]
public sealed class BlockingSchemaState
{
    private static RebuildGate? _gate;

    [Id(0)]
    public string StoredCity { get; set; } = string.Empty;

    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City
    {
        get
        {
            var gate = Volatile.Read(ref _gate);
            if (gate is not null)
            {
                // Indexed getters are synchronous. This test gate holds one real Orleans rebuild
                // turn inside materialization while the caller cancels only its own wait.
                gate.Entered.TrySetResult();
                gate.Release.Task.GetAwaiter().GetResult();
            }

            return StoredCity;
        }
    }

    public static void BeginBlocking()
    {
        var gate = new RebuildGate();
        if (Interlocked.CompareExchange(ref _gate, gate, null) is not null)
        {
            throw new InvalidOperationException("A schema rebuild gate is already active.");
        }
    }

    public static async Task WaitUntilBlockedAsync(TimeSpan timeout)
    {
        var gate = Volatile.Read(ref _gate)
            ?? throw new InvalidOperationException("No schema rebuild gate is active.");
        await gate.Entered.Task.WaitAsync(timeout);
    }

    public static void ReleaseBlockedGetter()
    {
        var gate = Interlocked.Exchange(ref _gate, null);
        gate?.Release.TrySetResult();
    }

    private sealed class RebuildGate
    {
        public TaskCompletionSource Entered { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
