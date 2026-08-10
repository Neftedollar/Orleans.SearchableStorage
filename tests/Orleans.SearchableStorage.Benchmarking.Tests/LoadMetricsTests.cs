using System.Diagnostics;
using HdrHistogram;

namespace Orleans.SearchableStorage.Benchmarks;

public sealed class LoadMetricsTests
{
    [Fact]
    public void OpenLoopCountersPreserveOfferedEqualsStartedPlusDropped()
    {
        var worker = new WorkerMetrics(recordHistograms: false);
        for (var index = 0; index < 3; index++)
        {
            worker.RecordStarted(OperationKind.Upsert);
        }

        var scheduler = new SchedulerCounters();
        for (var index = 0; index < 5; index++)
        {
            scheduler.RecordOffered(OperationKind.Upsert);
        }

        scheduler.RecordDropped(OperationKind.Upsert);
        scheduler.RecordDropped(OperationKind.Upsert);

        var phase = CreatePhase([worker], scheduler, recordHistograms: false);
        var upsert = phase.Operations[OperationKind.Upsert];

        Assert.Equal(5, upsert.Offered);
        Assert.Equal(3, upsert.Started);
        Assert.Equal(2, upsert.Dropped);
        Assert.Equal(upsert.Offered, upsert.Started + upsert.Dropped);
    }

    [Fact]
    public void CompatibleHdrHistogramsAreMergedByAddingRawCounts()
    {
        var first = CreateHistogram();
        first.RecordValue(10);
        first.RecordValue(100);
        var second = CreateHistogram();
        second.RecordValue(1_000);
        second.RecordValue(10_000);

        first.Add(second);

        Assert.Equal(4, first.TotalCount);
        Assert.Equal(first.HighestEquivalentValue(10_000), first.GetValueAtPercentile(100));
    }

    [Fact]
    public void IncompatibleHdrHistogramRangeIsRejected()
    {
        var destination = new LongHistogram(1, 1_000, 3);
        var source = new LongHistogram(1, 1_000_000, 3);
        source.RecordValue(500_000);

        Assert.Throws<ArgumentOutOfRangeException>(() => destination.Add(source));
    }

    [Fact]
    public void PhasePercentileComesFromMergedHistogramRatherThanAveragedPercentiles()
    {
        var lowTail = new WorkerMetrics(recordHistograms: true);
        var highTail = new WorkerMetrics(recordHistograms: true);
        var lowHistogram = lowTail.Operations[(int)OperationKind.Read].SucceededLatency!;
        var highHistogram = highTail.Operations[(int)OperationKind.Read].SucceededLatency!;
        for (var index = 0; index < 99; index++)
        {
            lowHistogram.RecordValue(100);
        }

        lowHistogram.RecordValue(10_000);
        for (var index = 0; index < 100; index++)
        {
            highHistogram.RecordValue(10_000);
        }

        var averageOfWorkerP50s =
            (lowHistogram.GetValueAtPercentile(50) + highHistogram.GetValueAtPercentile(50)) / 2d;
        var phase = CreatePhase([lowTail, highTail], scheduler: null, recordHistograms: true);
        var merged = phase.Operations[OperationKind.Read].SucceededLatency!;

        Assert.Equal(200, merged.TotalCount);
        Assert.Equal(merged.HighestEquivalentValue(10_000), merged.GetValueAtPercentile(50));
        Assert.NotEqual(averageOfWorkerP50s, merged.GetValueAtPercentile(50));
    }

    [Fact]
    public void LatenciesAboveHdrRangeAreClampedAndCounted()
    {
        var worker = new WorkerMetrics(recordHistograms: true);
        var aboveMaximumTicks = checked((long)Math.Ceiling(
            (WorkerMetrics.HighestTrackableMicroseconds + 1_000_000d) *
            Stopwatch.Frequency /
            1_000_000d));

        worker.RecordCompleted(
            OperationKind.Read,
            aboveMaximumTicks,
            aboveMaximumTicks,
            succeeded: true,
            resultCount: 1,
            exception: null);

        var metrics = worker.Operations[(int)OperationKind.Read];
        Assert.Equal(2, metrics.HistogramClamped);
        Assert.Equal(1, metrics.SucceededLatency!.TotalCount);
        Assert.Equal(1, metrics.QueueDelay!.TotalCount);
    }

    private static LongHistogram CreateHistogram()
    {
        return new LongHistogram(
            WorkerMetrics.LowestDiscernibleMicroseconds,
            WorkerMetrics.HighestTrackableMicroseconds,
            WorkerMetrics.SignificantDigits);
    }

    private static PhaseExecution CreatePhase(
        IReadOnlyList<WorkerMetrics> workers,
        SchedulerCounters? scheduler,
        bool recordHistograms)
    {
        var start = Stopwatch.GetTimestamp();
        return PhaseExecution.Create(
            DateTimeOffset.UtcNow,
            start,
            start + Stopwatch.Frequency,
            start + Stopwatch.Frequency,
            workers,
            scheduler,
            recordHistograms);
    }
}
