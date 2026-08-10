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
