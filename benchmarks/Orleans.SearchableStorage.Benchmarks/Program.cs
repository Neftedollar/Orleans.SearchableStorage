using BenchmarkDotNet.Running;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 1
            && string.Equals(args[0], "--self-test", StringComparison.Ordinal))
        {
            BenchmarkSelfTest.Run();
            Console.WriteLine("All benchmark invariants passed.");
            return 0;
        }

        if (args.Length == 4
            && string.Equals(args[0], "--retained-memory-worker", StringComparison.Ordinal))
        {
            var recordCount = int.Parse(args[1], System.Globalization.CultureInfo.InvariantCulture);
            var distribution = (BenchmarkIndexDistribution)int.Parse(
                args[2],
                System.Globalization.CultureInfo.InvariantCulture);
            var representation = (DerivedIndexRepresentation)int.Parse(
                args[3],
                System.Globalization.CultureInfo.InvariantCulture);
            var result = BenchmarkEvidence.MeasureRetainedMemoryWorker(
                recordCount,
                distribution,
                representation);
            Console.WriteLine(BenchmarkEvidence.SerializeWorkerResult(result));
            return 0;
        }

        if (args.Length == 5
            && string.Equals(args[0], "--retained-memory-v2-worker", StringComparison.Ordinal))
        {
            var recordCount = int.Parse(args[1], System.Globalization.CultureInfo.InvariantCulture);
            var profile = (RetainedMemoryDatasetProfile)int.Parse(
                args[2],
                System.Globalization.CultureInfo.InvariantCulture);
            var indexCount = int.Parse(args[3], System.Globalization.CultureInfo.InvariantCulture);
            var representation = (DerivedIndexRepresentation)int.Parse(
                args[4],
                System.Globalization.CultureInfo.InvariantCulture);
            var result = BenchmarkEvidence.MeasureRetainedMemoryV2Worker(
                recordCount,
                profile,
                indexCount,
                representation);
            Console.WriteLine(BenchmarkEvidence.SerializeRetainedMemoryV2WorkerResult(result));
            return 0;
        }

        if (args.Length >= 1
            && string.Equals(args[0], "--retained-memory-v2", StringComparison.Ordinal))
        {
            var options = RetainedMemoryV2CommandOptions.Parse(args);
            var evidenceConfig = new SearchableStorageBenchmarkConfig();
            BenchmarkProvenance.Write(
                options.ArtifactsDirectory,
                evidenceConfig,
                BenchmarkProvenance.DeterministicEvidenceExecutionMode);
            await BenchmarkEvidence.WriteRetainedMemoryV2Async(
                options.ArtifactsDirectory,
                options.Quick,
                options.MaximumProductionToMaterializingRatioBasisPoints);
            Console.WriteLine("Retained-memory v2 comparison evidence written.");
            return 0;
        }

        if (args.Length is 2 or 3
            && args[0] is "--query-work-matrix" or "--retained-memory")
        {
            var quick = args.Length == 3
                && string.Equals(args[2], "--quick", StringComparison.Ordinal);
            if (args.Length == 3 && !quick)
            {
                throw new ArgumentException("The evidence command accepts only the optional --quick flag.");
            }

            var evidenceConfig = new SearchableStorageBenchmarkConfig();
            BenchmarkProvenance.Write(
                args[1],
                evidenceConfig,
                BenchmarkProvenance.DeterministicEvidenceExecutionMode);
            if (string.Equals(args[0], "--query-work-matrix", StringComparison.Ordinal))
            {
                BenchmarkEvidence.WriteQueryWorkMatrix(args[1], quick);
                Console.WriteLine("Query work-matrix evidence written.");
            }
            else
            {
                await BenchmarkEvidence.WriteRetainedMemoryAsync(args[1], quick);
                Console.WriteLine("Retained-memory evidence written.");
            }

            return 0;
        }

        var smoke = args.Contains("--smoke", StringComparer.Ordinal);
        var benchmarkArguments = smoke
            ? args.Where(static argument => !string.Equals(argument, "--smoke", StringComparison.Ordinal)).ToArray()
            : args;
        var config = new SearchableStorageBenchmarkConfig(smoke);
        BenchmarkProvenance.Write(
            BenchmarkProvenance.ResolveArtifactsPath(benchmarkArguments),
            config,
            smoke
                ? BenchmarkProvenance.BenchmarkDotNetInProcessDryRunExecutionMode
                : BenchmarkProvenance.BenchmarkDotNetExecutionMode);
        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(benchmarkArguments, config);
        return 0;
    }
}

internal sealed record RetainedMemoryV2CommandOptions(
    string ArtifactsDirectory,
    bool Quick,
    int? MaximumProductionToMaterializingRatioBasisPoints)
{
    private const string QuickOption = "--quick";
    private const string RatioOption = "--maximum-production-to-materializing-ratio-bps";

    public static RetainedMemoryV2CommandOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (args.Length < 2
            || !string.Equals(args[0], "--retained-memory-v2", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The retained-memory v2 command requires an artifacts directory.",
                nameof(args));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(args[1]);
        var quick = false;
        int? maximumRatio = null;
        for (var index = 2; index < args.Length; index++)
        {
            if (string.Equals(args[index], QuickOption, StringComparison.Ordinal))
            {
                if (quick)
                {
                    throw new ArgumentException("The --quick option was specified more than once.", nameof(args));
                }

                quick = true;
                continue;
            }

            if (string.Equals(args[index], RatioOption, StringComparison.Ordinal))
            {
                if (maximumRatio is not null || index + 1 >= args.Length)
                {
                    throw new ArgumentException(
                        "The ratio option must be specified once and followed by an integer basis-point value.",
                        nameof(args));
                }

                maximumRatio = int.Parse(
                    args[++index],
                    System.Globalization.CultureInfo.InvariantCulture);
                if (maximumRatio <= 0)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(args),
                        maximumRatio,
                        "The retained-memory ratio threshold must be positive.");
                }

                continue;
            }

            throw new ArgumentException(
                $"Unknown retained-memory v2 option '{args[index]}'.",
                nameof(args));
        }

        return new RetainedMemoryV2CommandOptions(args[1], quick, maximumRatio);
    }
}
