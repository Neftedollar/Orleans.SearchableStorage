using System.Globalization;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder;

public static class CorpusBuilderCli
{
    public static int Run(string[] args, TextWriter output, TextWriter error)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        try
        {
            if (args.Length == 0 || args[0] is "help" or "--help" or "-h")
            {
                WriteUsage(output);
                return args.Length == 0 ? 2 : 0;
            }

            return args[0] switch
            {
                "freeze" => RunFreeze(args[1..], output),
                "verify" => RunVerify(args[1..], output),
                _ => throw new ArgumentException($"Unknown command '{args[0]}'."),
            };
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            error.WriteLine($"error: {exception.Message}");
            return 2;
        }
    }

    private static int RunFreeze(string[] args, TextWriter output)
    {
        string? journal = null;
        string? destination = null;
        long memoryBytes = 64L * 1024 * 1024;
        var mergeFanIn = 32;
        var writeHex = false;
        var profiles = new List<CorpusProfileRequest>();

        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--journal":
                    journal = SingleValue(args, ref index, "--journal", journal);
                    break;
                case "--output":
                    destination = SingleValue(args, ref index, "--output", destination);
                    break;
                case "--memory-bytes":
                    memoryBytes = ParsePositiveInt64(Value(args, ref index, "--memory-bytes"), "--memory-bytes");
                    break;
                case "--memory-mib":
                    memoryBytes = checked(
                        ParsePositiveInt64(Value(args, ref index, "--memory-mib"), "--memory-mib")
                        * 1024
                        * 1024);
                    break;
                case "--merge-fan-in":
                    mergeFanIn = checked((int)ParsePositiveInt64(
                        Value(args, ref index, "--merge-fan-in"),
                        "--merge-fan-in"));
                    break;
                case "--profile":
                    profiles.Add(ParseProfile(Value(args, ref index, "--profile")));
                    break;
                case "--hex" when !writeHex:
                    writeHex = true;
                    break;
                case "--hex":
                    throw new ArgumentException("Option '--hex' may be supplied only once.");
                default:
                    throw new ArgumentException($"Unknown freeze option '{args[index]}'.");
            }
        }

        var result = CorpusFreezer.Freeze(
            new CorpusFreezeOptions
            {
                JournalPath = journal ?? throw new ArgumentException("Option '--journal' is required."),
                OutputDirectory = destination ?? throw new ArgumentException("Option '--output' is required."),
                MemoryBudgetBytes = memoryBytes,
                MergeFanIn = mergeFanIn,
                WriteHumanReadableHex = writeHex,
                Profiles = profiles,
            });
        output.WriteLine($"Frozen {result.AccountCount.ToString(CultureInfo.InvariantCulture)} account keys.");
        output.WriteLine($"Manifest: {result.ManifestPath}");
        output.WriteLine($"Initial spill runs: {result.InitialSpillRunCount.ToString(CultureInfo.InvariantCulture)}");
        return 0;
    }

    private static int RunVerify(string[] args, TextWriter output)
    {
        string? manifest = null;
        string? journal = null;
        var deep = false;
        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--manifest":
                    manifest = SingleValue(args, ref index, "--manifest", manifest);
                    break;
                case "--journal":
                    journal = SingleValue(args, ref index, "--journal", journal);
                    break;
                case "--deep" when !deep:
                    deep = true;
                    break;
                case "--deep":
                    throw new ArgumentException("Option '--deep' may be supplied only once.");
                default:
                    throw new ArgumentException($"Unknown verify option '{args[index]}'.");
            }
        }

        if (!deep)
        {
            throw new ArgumentException(
                "Qualification verification must be explicit: supply '--deep'.");
        }

        var result = CorpusVerifier.Verify(
            manifest ?? throw new ArgumentException("Option '--manifest' is required."),
            deep: true,
            journal);
        output.WriteLine(
            $"Deep verification passed for {result.AccountCount.ToString(CultureInfo.InvariantCulture)} account keys.");
        output.WriteLine(
            result.SourceJournalVerified
                ? "The supplied source journal hash also matches."
                : "Source journal was not supplied; its recorded hash was not independently checked.");
        return 0;
    }

    private static CorpusProfileRequest ParseProfile(string value)
    {
        var separator = value.IndexOf('=');
        if (separator <= 0 || separator == value.Length - 1 || value.IndexOf('=', separator + 1) >= 0)
        {
            throw new ArgumentException("A profile must use the form 'name=account-count'.");
        }

        return new CorpusProfileRequest(
            value[..separator],
            ParsePositiveInt64(value[(separator + 1)..], "--profile"));
    }

    private static string SingleValue(
        string[] args,
        ref int index,
        string option,
        string? previous)
    {
        if (previous is not null)
        {
            throw new ArgumentException($"Option '{option}' may be supplied only once.");
        }

        return Value(args, ref index, option);
    }

    private static string Value(string[] args, ref int index, string option)
    {
        index++;
        if (index >= args.Length || args[index].StartsWith("--", StringComparison.Ordinal))
        {
            throw new ArgumentException($"Option '{option}' requires a value.");
        }

        return args[index];
    }

    private static long ParsePositiveInt64(string value, string option)
    {
        if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
            || parsed <= 0)
        {
            throw new ArgumentException($"Option '{option}' requires a positive integer.");
        }

        return parsed;
    }

    private static void WriteUsage(TextWriter output)
    {
        output.WriteLine("SkyPulse metadata-only corpus builder");
        output.WriteLine();
        output.WriteLine("freeze --journal PATH --output DIRECTORY [--memory-mib N|--memory-bytes N]");
        output.WriteLine("       [--merge-fan-in N] [--profile name=count ...] [--hex]");
        output.WriteLine("verify --manifest PATH --deep [--journal PATH]");
        output.WriteLine();
        output.WriteLine("Input is a pre-sanitized append-only lifecycle-observation NDJSON journal.");
        output.WriteLine("This tool is not a network crawler and does not claim an atomic network snapshot.");
    }
}
