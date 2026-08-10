using System.Globalization;
using System.Reflection;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class MultiClientTests
{
    private const int BarrierTimeoutSeconds = 120;

    [Fact]
    public void EmbeddedClientDefaultsToSingleClientCoordinates()
    {
        var options = DriverOptions.Parse(["run", "--spec", "scenario.json"]);

        Assert.Equal((0, 1), options.GetClientCoordinates(BenchmarkTestData.CreateSpec()));
    }

    [Theory]
    [InlineData("--client-ordinal", "-1", "nonnegative")]
    [InlineData("--client-count", "0", "positive")]
    public void ClientCoordinatesRejectInvalidNumbers(string option, string value, string message)
    {
        var exception = Assert.Throws<ArgumentException>(() => DriverOptions.Parse(
            ["run", "--spec", "scenario.json", option, value]));

        Assert.Contains(message, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ExternalClientRequiresBothCoordinates()
    {
        var options = DriverOptions.Parse(
            ["run", "--spec", "scenario.json", "--client-ordinal", "0"]);

        var spec = BenchmarkTestData.CreateSpec();
        spec.Topology.Mode = TopologyMode.External;
        Assert.Throws<InvalidOperationException>(() => options.GetClientCoordinates(spec));
    }

    [Fact]
    public void ClientOrdinalMustBeLessThanClientCount()
    {
        var options = DriverOptions.Parse(
        [
            "run", "--spec", "scenario.json",
            "--client-ordinal", "3",
            "--client-count", "3",
        ]);

        var spec = BenchmarkTestData.CreateSpec();
        spec.Topology.Mode = TopologyMode.External;
        Assert.Throws<InvalidOperationException>(() => options.GetClientCoordinates(spec));
    }

    [Fact]
    public void ClientOperationStreamsAreDistinctAndReproducible()
    {
        const int clientCount = 4;
        const int operationsPerClient = 25;
        var firstRun = GetStreams(clientCount, operationsPerClient);
        var secondRun = GetStreams(clientCount, operationsPerClient);

        Assert.Equal(firstRun.SelectMany(static stream => stream), secondRun.SelectMany(static stream => stream));
        Assert.Equal(
            Enumerable.Range(0, clientCount * operationsPerClient).Select(static value => (long)value),
            firstRun.SelectMany(static stream => stream).Order());
        for (var left = 0; left < clientCount; left++)
        {
            for (var right = left + 1; right < clientCount; right++)
            {
                Assert.Empty(firstRun[left].Intersect(firstRun[right]));
            }
        }
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(3, 8)]
    [InlineData(7, 103)]
    [InlineData(16, 1_000)]
    public void PopulationShardsCoverEveryOrdinalExactlyOnce(int clientCount, long recordCount)
    {
        var shards = Enumerable.Range(0, clientCount)
            .Select(clientOrdinal => ClientStream
                .GetPopulationOrdinals(recordCount, clientOrdinal, clientCount)
                .ToArray())
            .ToArray();
        var allOrdinals = shards.SelectMany(static shard => shard).ToArray();

        Assert.Equal(recordCount, allOrdinals.LongLength);
        Assert.Equal(recordCount, allOrdinals.Distinct().LongCount());
        Assert.Equal(Enumerable.Range(0, checked((int)recordCount)).Select(static value => (long)value), allOrdinals.Order());
    }

    [Fact]
    public void BillionRecordPopulationShardCanAddressItsLastOrdinalWithoutMaterialization()
    {
        const long recordCount = 1_000_000_000;
        const int clientOrdinal = 6;
        const int clientCount = 7;
        var lastShardIndex = (recordCount - 1 - clientOrdinal) / clientCount;

        var exists = ClientStream.TryGetPopulationOrdinal(
            recordCount,
            clientOrdinal,
            clientCount,
            lastShardIndex,
            out var ordinal);

        Assert.True(exists);
        Assert.InRange(ordinal, recordCount - clientCount, recordCount - 1);
    }

    [Fact]
    public async Task BarrierReturnsEveryFailedOrdinalAndAcceptsIdempotentStatus()
    {
        var barrier = new BenchmarkBarrierGrain();

        var first = barrier.SignalAndWaitAsync(
            "measurement-complete", 0, 3, succeeded: true, BarrierTimeoutSeconds);
        var duplicate = barrier.SignalAndWaitAsync(
            "measurement-complete", 0, 3, succeeded: true, BarrierTimeoutSeconds);
        var second = barrier.SignalAndWaitAsync(
            "measurement-complete", 1, 3, succeeded: false, BarrierTimeoutSeconds);

        Assert.Same(first, duplicate);
        Assert.False(first.IsCompleted);

        var third = barrier.SignalAndWaitAsync(
            "measurement-complete", 2, 3, succeeded: false, BarrierTimeoutSeconds);
        var results = await Task.WhenAll(first, second, third);

        Assert.All(results, static result => Assert.False(result.AllSucceeded));
        Assert.All(results, static result => Assert.Equal([1, 2], result.FailedClientOrdinals));
    }

    [Fact]
    public async Task BarrierRejectsConflictingStatusForTheSameOrdinal()
    {
        var barrier = new BenchmarkBarrierGrain();
        _ = barrier.SignalAndWaitAsync(
            "population-complete", 0, 2, succeeded: false, BarrierTimeoutSeconds);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            barrier.SignalAndWaitAsync(
                "population-complete", 0, 2, succeeded: true, BarrierTimeoutSeconds));

        Assert.Contains("conflicting outcomes", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BarrierRejectsConflictingStatusAfterPhaseCompletion()
    {
        var barrier = new BenchmarkBarrierGrain();
        await barrier.SignalAndWaitAsync(
            "population-complete", 0, 1, succeeded: true, BarrierTimeoutSeconds);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            barrier.SignalAndWaitAsync(
                "population-complete", 0, 1, succeeded: false, BarrierTimeoutSeconds));

        Assert.Contains("conflicting outcomes", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BarrierRejectsClientCountChangesWithinAPhase()
    {
        var barrier = new BenchmarkBarrierGrain();
        _ = barrier.SignalAndWaitAsync(
            "warmup-complete", 0, 2, succeeded: true, BarrierTimeoutSeconds);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            barrier.SignalAndWaitAsync(
                "warmup-complete", 1, 3, succeeded: true, BarrierTimeoutSeconds));

        Assert.Contains("configured for 2 clients", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BarrierReportsSuccessOnlyAfterEveryOrdinalSucceeds()
    {
        var barrier = new BenchmarkBarrierGrain();
        var second = barrier.SignalAndWaitAsync(
            "final-audit-complete", 1, 2, succeeded: true, BarrierTimeoutSeconds);

        Assert.False(second.IsCompleted);

        var first = barrier.SignalAndWaitAsync(
            "final-audit-complete", 0, 2, succeeded: true, BarrierTimeoutSeconds);
        var results = await Task.WhenAll(first, second);

        Assert.All(results, static result => Assert.True(result.AllSucceeded));
        Assert.All(results, static result => Assert.Empty(result.FailedClientOrdinals));
    }

    [Fact]
    public async Task BarrierDeadlinePublishesOneFailureOutcomeToLateClients()
    {
        var deadlineReached = new TaskCompletionSource<TimeSpan>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDeadline = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var barrier = new BenchmarkBarrierGrain((timeout, _) =>
        {
            deadlineReached.TrySetResult(timeout);
            return releaseDeadline.Task;
        });
        var first = barrier.SignalAndWaitAsync(
            "measurement-complete",
            clientOrdinal: 0,
            clientCount: 2,
            succeeded: true,
            timeoutSeconds: 7);

        Assert.Equal(TimeSpan.FromSeconds(7), await deadlineReached.Task);
        releaseDeadline.SetResult();
        var firstResult = await first;
        var lateResult = await barrier.SignalAndWaitAsync(
            "measurement-complete",
            clientOrdinal: 1,
            clientCount: 2,
            succeeded: true,
            timeoutSeconds: 7);

        Assert.False(firstResult.AllSucceeded);
        Assert.Equal([1], firstResult.FailedClientOrdinals);
        Assert.True(firstResult.DeadlineExceeded);
        Assert.Equal([1], firstResult.MissingClientOrdinals);
        Assert.Equal(firstResult.AllSucceeded, lateResult.AllSucceeded);
        Assert.Equal(firstResult.FailedClientOrdinals, lateResult.FailedClientOrdinals);
        Assert.Equal(firstResult.DeadlineExceeded, lateResult.DeadlineExceeded);
        Assert.Equal(firstResult.MissingClientOrdinals, lateResult.MissingClientOrdinals);
    }

    [Fact]
    public async Task BarrierAbortBeforeSignalFreezesFailureForLateCallers()
    {
        var barrier = new BenchmarkBarrierGrain();

        var aborted = await barrier.AbortPhaseAsync(
            "measurement-complete", 0, 2, BarrierTimeoutSeconds);
        var duplicate = await barrier.AbortPhaseAsync(
            "measurement-complete", 0, 2, BarrierTimeoutSeconds);
        var late = await barrier.SignalAndWaitAsync(
            "measurement-complete", 1, 2, succeeded: true, BarrierTimeoutSeconds);

        Assert.False(aborted.AllSucceeded);
        Assert.Equal([0], aborted.FailedClientOrdinals);
        Assert.False(aborted.DeadlineExceeded);
        Assert.Empty(aborted.MissingClientOrdinals);
        Assert.Equal(aborted.FailedClientOrdinals, duplicate.FailedClientOrdinals);
        Assert.Equal(aborted.FailedClientOrdinals, late.FailedClientOrdinals);
    }

    [Fact]
    public async Task BarrierAbortUnblocksHealthyPeerWithOneFrozenOutcome()
    {
        var barrier = new BenchmarkBarrierGrain();
        var healthy = barrier.SignalAndWaitAsync(
            "final-audit-complete", 1, 2, succeeded: true, BarrierTimeoutSeconds);

        Assert.False(healthy.IsCompleted);
        var aborted = await barrier.AbortPhaseAsync(
            "final-audit-complete", 0, 2, BarrierTimeoutSeconds);
        var healthyResult = await healthy;

        Assert.False(aborted.AllSucceeded);
        Assert.Equal([0], aborted.FailedClientOrdinals);
        Assert.Equal(aborted.FailedClientOrdinals, healthyResult.FailedClientOrdinals);
    }

    [Fact]
    public async Task BarrierRejectsDeadlineChangesWithinAPhase()
    {
        var barrier = new BenchmarkBarrierGrain();
        _ = barrier.SignalAndWaitAsync(
            "warmup-complete", 0, 2, succeeded: true, BarrierTimeoutSeconds);

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            barrier.SignalAndWaitAsync(
                "warmup-complete", 1, 2, succeeded: true, BarrierTimeoutSeconds + 1));

        Assert.Contains("deadline", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void BarrierTransportTimeoutCoversBothConfiguredDeadlineWindows()
    {
        var requiredMinimum = TimeSpan.FromSeconds(
            checked(
                TopologySpec.MaximumBarrierTimeoutSeconds * 2
                + BenchmarkRecordConstants.BarrierResultDeliveryMarginSeconds));
        foreach (var methodName in new[]
        {
            nameof(IBenchmarkBarrierGrain.SignalAndWaitAsync),
            nameof(IBenchmarkBarrierGrain.AbortPhaseAsync),
        })
        {
            var method = typeof(IBenchmarkBarrierGrain).GetMethod(methodName);
            var attribute = Assert.Single(method!.GetCustomAttributes<ResponseTimeoutAttribute>());

            Assert.Equal(
                TimeSpan.Parse(
                    BenchmarkRecordConstants.BarrierResponseTimeout,
                    CultureInfo.InvariantCulture),
                attribute.Timeout);
            Assert.True(attribute.Timeout > requiredMinimum);
        }
    }

    [Fact]
    public async Task OrleansBarrierProxySerializesSharedSuccessAndAbortOutcomes()
    {
        var spec = BenchmarkTestData.CreateSpec();
        await using var cluster = await BenchmarkHosting.StartClientClusterAsync(
            spec,
            CancellationToken.None);
        var barrier = cluster.Client.GetGrain<IBenchmarkBarrierGrain>(spec.Topology.ServiceId);

        var firstSuccess = barrier.SignalAndWaitAsync(
            "proxy-success", 0, 2, succeeded: true, spec.Topology.BarrierTimeoutSeconds);
        var secondSuccess = barrier.SignalAndWaitAsync(
            "proxy-success", 1, 2, succeeded: true, spec.Topology.BarrierTimeoutSeconds);
        var success = await Task.WhenAll(firstSuccess, secondSuccess).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.All(success, static result => Assert.True(result.AllSucceeded));
        Assert.All(success, static result => Assert.Empty(result.FailedClientOrdinals));

        var healthy = barrier.SignalAndWaitAsync(
            "proxy-abort", 1, 2, succeeded: true, spec.Topology.BarrierTimeoutSeconds);
        var aborted = barrier.AbortPhaseAsync(
            "proxy-abort", 0, 2, spec.Topology.BarrierTimeoutSeconds);
        var failure = await Task.WhenAll(healthy, aborted).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.All(failure, static result => Assert.False(result.AllSucceeded));
        Assert.All(failure, static result => Assert.Equal([0], result.FailedClientOrdinals));
    }

    private static long[][] GetStreams(int clientCount, int operationsPerClient)
    {
        return Enumerable.Range(0, clientCount)
            .Select(clientOrdinal => Enumerable.Range(0, operationsPerClient)
                .Select(localSequence => ClientStream.GetGlobalSequence(
                    localSequence,
                    clientOrdinal,
                    clientCount))
                .ToArray())
            .ToArray();
    }
}
