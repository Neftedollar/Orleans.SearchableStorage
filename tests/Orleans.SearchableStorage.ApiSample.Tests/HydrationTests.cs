using AwesomeAssertions;

namespace Orleans.SearchableStorage.ApiSample.Tests;

public sealed class HydrationTests
{
    [Fact]
    public async Task PageHydrationBoundsConcurrencyAndPreservesOrder()
    {
        var source = Enumerable.Range(0, VacancySearchEndpoints.HydrationConcurrencyLimit + 8)
            .ToArray();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var saturated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var invoked = 0;
        var inFlight = 0;
        var maximumInFlight = 0;

        var hydration = VacancySearchEndpoints.HydratePageAsync(
            source,
            async (value, cancellationToken) =>
            {
                var current = Interlocked.Increment(ref inFlight);
                InterlockedExtensions.Max(ref maximumInFlight, current);
                if (Interlocked.Increment(ref invoked)
                    == VacancySearchEndpoints.HydrationConcurrencyLimit)
                {
                    saturated.TrySetResult();
                }

                await release.Task.WaitAsync(cancellationToken);
                Interlocked.Decrement(ref inFlight);
                return value * 2;
            },
            CancellationToken.None);

        await saturated.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Volatile.Read(ref invoked).Should().Be(VacancySearchEndpoints.HydrationConcurrencyLimit);
        release.TrySetResult();

        var results = await hydration;
        results.Should().Equal(source.Select(static value => value * 2));
        maximumInFlight.Should().Be(VacancySearchEndpoints.HydrationConcurrencyLimit);
    }

    [Fact]
    public async Task PageHydrationPropagatesCancellationToTheBoundedWork()
    {
        using var cancellation = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hydration = VacancySearchEndpoints.HydratePageAsync(
            Enumerable.Range(0, 32).ToArray(),
            async (_, itemCancellation) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, itemCancellation);
                return 0;
            },
            cancellation.Token);

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();

        Func<Task> wait = async () => await hydration;
        await wait.Should().ThrowAsync<OperationCanceledException>();
    }
}

internal static class InterlockedExtensions
{
    public static void Max(ref int location, int value)
    {
        var current = Volatile.Read(ref location);
        while (current < value)
        {
            var observed = Interlocked.CompareExchange(ref location, value, current);
            if (observed == current)
            {
                return;
            }
            current = observed;
        }
    }
}
