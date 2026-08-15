using System.Globalization;
using System.Net.WebSockets;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition;

public static class CorpusAcquisitionCli
{
    public static async Task<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken cancellationToken = default)
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
                "capture" => await CaptureAsync(args[1..], output, cancellationToken).ConfigureAwait(false),
                "route" => Route(args[1..], output),
                "verify-route" => VerifyRoute(args[1..], output),
                _ => throw new ArgumentException($"Unknown command '{args[0]}'."),
            };
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            // Messages are deliberately contract-level and never contain source frames, DIDs, handles,
            // response bodies, opaque cursors, or record data.
            error.WriteLine($"error: {SafeError(exception)}");
            return 2;
        }
    }

    private static async Task<int> CaptureAsync(
        string[] args,
        TextWriter output,
        CancellationToken cancellationToken)
    {
        string? destination = null;
        string? jetstream = null;
        string? jetstreamInstance = null;
        string? relay = null;
        string? relayInstance = null;
        var sweeps = 2;
        var pageLimit = 1000;
        var maximumPages = 100_000;
        long maximumFrames = 10_000_000;
        var closeWaitSeconds = 300;
        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--output":
                    destination = SingleValue(args, ref index, "--output", destination);
                    break;
                case "--jetstream":
                    jetstream = SingleValue(args, ref index, "--jetstream", jetstream);
                    break;
                case "--jetstream-instance":
                    jetstreamInstance = SingleValue(
                        args,
                        ref index,
                        "--jetstream-instance",
                        jetstreamInstance);
                    break;
                case "--relay":
                    relay = SingleValue(args, ref index, "--relay", relay);
                    break;
                case "--relay-instance":
                    relayInstance = SingleValue(args, ref index, "--relay-instance", relayInstance);
                    break;
                case "--sweeps":
                    sweeps = ParseInt(Value(args, ref index, "--sweeps"), "--sweeps");
                    break;
                case "--page-limit":
                    pageLimit = ParseInt(Value(args, ref index, "--page-limit"), "--page-limit");
                    break;
                case "--max-pages-per-sweep":
                    maximumPages = ParseInt(
                        Value(args, ref index, "--max-pages-per-sweep"),
                        "--max-pages-per-sweep");
                    break;
                case "--max-lifecycle-frames":
                    maximumFrames = ParseLong(
                        Value(args, ref index, "--max-lifecycle-frames"),
                        "--max-lifecycle-frames");
                    break;
                case "--close-wait-seconds":
                    closeWaitSeconds = ParseInt(
                        Value(args, ref index, "--close-wait-seconds"),
                        "--close-wait-seconds");
                    break;
                default:
                    throw new ArgumentException($"Unknown capture option '{args[index]}'.");
            }
        }

        var options = new AcquisitionOptions
        {
            OutputDirectory = destination ?? throw new ArgumentException("Option '--output' is required."),
            JetstreamEndpoint = ParseUri(
                jetstream ?? throw new ArgumentException("Option '--jetstream' is required."),
                "--jetstream"),
            JetstreamInstanceId = jetstreamInstance
                ?? throw new ArgumentException("Option '--jetstream-instance' is required."),
            RelayEndpoint = ParseUri(
                relay ?? throw new ArgumentException("Option '--relay' is required."),
                "--relay"),
            RelayInstanceId = relayInstance
                ?? throw new ArgumentException("Option '--relay-instance' is required."),
            FullSweepCount = sweeps,
            ListReposPageLimit = pageLimit,
            MaximumPagesPerSweep = maximumPages,
            MaximumLifecycleFrames = maximumFrames,
            CloseCursorWaitTimeout = TimeSpan.FromSeconds(closeWaitSeconds),
        };
        using var listRepos = new HttpListReposSource(options.MaximumListReposResponseBytes);
        var result = await CorpusAcquisitionRunner.RunAsync(
            options,
            new JetstreamV2LifecycleSource(),
            listRepos,
            cancellationToken).ConfigureAwait(false);
        output.WriteLine("Completed a bounded observed census (not an atomic or global snapshot).");
        output.WriteLine($"Journal observations: {result.Manifest.Counts.JournalObservations}");
        output.WriteLine($"Manifest: {result.ManifestPath}");
        return 0;
    }

    private static int Route(string[] args, TextWriter output)
    {
        string? acquisition = null;
        string? corpus = null;
        string? profile = null;
        string? destination = null;
        long memoryBytes = 64L * 1024 * 1024;
        var fanIn = 32;
        var batchRecords = 500;
        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--acquisition-manifest":
                    acquisition = SingleValue(
                        args,
                        ref index,
                        "--acquisition-manifest",
                        acquisition);
                    break;
                case "--corpus-manifest":
                    corpus = SingleValue(args, ref index, "--corpus-manifest", corpus);
                    break;
                case "--profile":
                    profile = SingleValue(args, ref index, "--profile", profile);
                    break;
                case "--output":
                    destination = SingleValue(args, ref index, "--output", destination);
                    break;
                case "--memory-mib":
                    memoryBytes = checked(ParseLong(
                        Value(args, ref index, "--memory-mib"),
                        "--memory-mib") * 1024 * 1024);
                    break;
                case "--merge-fan-in":
                    fanIn = ParseInt(Value(args, ref index, "--merge-fan-in"), "--merge-fan-in");
                    break;
                case "--batch-records":
                    batchRecords = ParseInt(
                        Value(args, ref index, "--batch-records"),
                        "--batch-records");
                    break;
                default:
                    throw new ArgumentException($"Unknown route option '{args[index]}'.");
            }
        }

        var result = PrivateRoutingExporter.Export(
            new PrivateRoutingExportOptions
            {
                AcquisitionManifestPath = acquisition
                    ?? throw new ArgumentException("Option '--acquisition-manifest' is required."),
                CorpusManifestPath = corpus
                    ?? throw new ArgumentException("Option '--corpus-manifest' is required."),
                ProfileName = profile ?? throw new ArgumentException("Option '--profile' is required."),
                OutputDirectory = destination ?? throw new ArgumentException("Option '--output' is required."),
                MemoryBudgetBytes = memoryBytes,
                MergeFanIn = fanIn,
                BatchRecordLimit = batchRecords,
            });
        output.WriteLine($"Private route records: {result.Manifest.Routing.AccountCount}");
        output.WriteLine($"Private route manifest: {result.ManifestPath}");
        return 0;
    }

    private static int VerifyRoute(string[] args, TextWriter output)
    {
        if (args.Length != 2 || args[0] != "--manifest")
        {
            throw new ArgumentException("verify-route requires exactly '--manifest PATH'.");
        }

        var manifest = PrivateRoutingExporter.Verify(args[1]);
        output.WriteLine(
            $"Verified exact ordered profile prefix: {manifest.Routing.AccountCount} private routes.");
        return 0;
    }

    private static string SafeError(Exception exception)
        => exception switch
        {
            AcquisitionContractException contract => $"acquisition contract failed ({contract.ReasonCode})",
            HttpRequestException => "an acquisition endpoint request failed; the run remains resumable",
            WebSocketException => "the Jetstream connection failed; the run remains resumable",
            OperationCanceledException => "the operation was canceled; the run remains resumable",
            _ => exception.Message,
        };

    private static Uri ParseUri(string value, string option)
        => Uri.TryCreate(value, UriKind.Absolute, out var parsed)
            ? parsed
            : throw new ArgumentException($"Option '{option}' requires an absolute URI.");

    private static string SingleValue(string[] args, ref int index, string option, string? prior)
    {
        if (prior is not null)
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

    private static int ParseInt(string value, string option)
        => int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
            && parsed > 0
                ? parsed
                : throw new ArgumentException($"Option '{option}' requires a positive integer.");

    private static long ParseLong(string value, string option)
        => long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
            && parsed > 0
                ? parsed
                : throw new ArgumentException($"Option '{option}' requires a positive integer.");

    private static void WriteUsage(TextWriter output)
    {
        output.WriteLine("SkyPulse privacy-preserving AT Protocol corpus acquisition");
        output.WriteLine();
        output.WriteLine("capture --output DIR --jetstream WSS_BASE --jetstream-instance ID");
        output.WriteLine("        --relay HTTPS_BASE --relay-instance ID [--sweeps N] [--page-limit N]");
        output.WriteLine("route --acquisition-manifest PATH --corpus-manifest PATH --profile NAME");
        output.WriteLine("      --output DIR [--memory-mib N] [--merge-fan-in N] [--batch-records N]");
        output.WriteLine("verify-route --manifest PATH");
    }
}
