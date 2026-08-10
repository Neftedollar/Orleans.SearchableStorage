using BenchmarkDotNet.Running;

namespace Orleans.SearchableStorage.Benchmarks;

internal static class Program
{
    public static int Main(string[] args)
    {
        if (args.Length == 1
            && string.Equals(args[0], "--self-test", StringComparison.Ordinal))
        {
            BenchmarkSelfTest.Run();
            Console.WriteLine("All benchmark invariants passed.");
            return 0;
        }

        var config = new SearchableStorageBenchmarkConfig();
        BenchmarkProvenance.Write(BenchmarkProvenance.ResolveArtifactsPath(args), config);
        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(args, config);
        return 0;
    }
}
