namespace Orleans.SearchableStorage.Benchmarks;

public sealed class DriverOptionsTests
{
    [Fact]
    public void ParseAcceptsValidOverrides()
    {
        var options = DriverOptions.Parse(
        [
            "run",
            "--spec", "scenario.json",
            "--backend", "postgresql",
            "--implementation-path", "plain",
            "--topology", "external",
            "--gateways", "host-a:30000, host-b:30001",
            "--silo-port", "11112",
            "--gateway-port", "30002",
            "--run-id", "Run 42",
        ]);

        Assert.Equal("run", options.Command);
        Assert.Equal("scenario.json", options.SpecPath);
        Assert.Equal(StorageBackend.PostgreSql, options.Backend);
        Assert.Equal(StoragePath.Plain, options.ImplementationPath);
        Assert.Equal(TopologyMode.External, options.Topology);
        Assert.Equal(["host-a:30000", "host-b:30001"], options.GatewayEndpoints);
        Assert.Equal(11_112, options.SiloPort);
        Assert.Equal(30_002, options.GatewayPort);
    }

    [Theory]
    [MemberData(nameof(InvalidArguments))]
    public void ParseRejectsInvalidArguments(string[] args, string expectedMessage)
    {
        var exception = Assert.Throws<ArgumentException>(() => DriverOptions.Parse(args));

        Assert.Contains(expectedMessage, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    public static TheoryData<string[], string> InvalidArguments => new()
    {
        { ["explode", "--spec", "scenario.json"], "Unknown command" },
        { ["run"], "--spec" },
        { ["run", "--spec"], "form --name value" },
        { ["run", "spec", "scenario.json"], "form --name value" },
        { ["run", "--spec", "one", "--spec", "two"], "more than once" },
        { ["run", "--spec", "one", "--mystery", "two"], "Unknown option" },
        { ["run", "--spec", "one", "--backend", "file"], "Unknown storage backend" },
        { ["run", "--spec", "one", "--implementation-path", "mixed"], "Unknown implementation path" },
        { ["run", "--spec", "one", "--topology", "local"], "Unknown topology" },
        { ["run", "--spec", "one", "--silo-port", "0"], "1 through 65535" },
        { ["run", "--spec", "one", "--gateway-port", "65536"], "1 through 65535" },
    };

    [Fact]
    public void ParseWithoutArgumentsReportsHelp()
    {
        Assert.Throws<CommandLineHelpException>(() => DriverOptions.Parse([]));
    }

    [Theory]
    [InlineData("help")]
    [InlineData("--help")]
    [InlineData("-h")]
    public void ParseHelpArgumentReportsHelp(string argument)
    {
        Assert.Throws<CommandLineHelpException>(() => DriverOptions.Parse([argument]));
    }

    [Fact]
    public void ExternalTopologyRequiresExplicitSharedRunId()
    {
        var original = Environment.GetEnvironmentVariable("OSS_BENCHMARK_RUN_ID");
        Environment.SetEnvironmentVariable("OSS_BENCHMARK_RUN_ID", null);
        try
        {
            var spec = BenchmarkTestData.CreateSpec(
                StoragePath.Plain,
                new OperationMixSpec
                {
                    Upsert = 0,
                    Read = 1,
                    ExactQuery = 0,
                    RangeQuery = 0,
                });
            spec.Topology.Mode = TopologyMode.External;
            spec.Topology.GatewayEndpoints = ["127.0.0.1:30000"];
            var options = DriverOptions.Parse(["run", "--spec", "scenario.json"]);

            Assert.Throws<InvalidOperationException>(() => options.ApplyRunIdentity(spec));
        }
        finally
        {
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_RUN_ID", original);
        }
    }

    [Fact]
    public void ExternalProcessesRequireExplicitFullCommitAndDirtyProvenance()
    {
        var previousCommit = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT");
        var previousDirty = Environment.GetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY");
        try
        {
            var spec = BenchmarkTestData.CreateSpec();
            spec.Topology.Mode = TopologyMode.External;
            spec.Topology.GatewayEndpoints = ["127.0.0.1:30000"];

            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT", "main");
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", "false");
            Assert.Throws<InvalidOperationException>(
                () => DriverOptions.ValidateExternalExecutionProvenance(spec));

            Environment.SetEnvironmentVariable(
                "OSS_BENCHMARK_GIT_COMMIT",
                "0123456789abcdef0123456789abcdef01234567");
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", null);
            Assert.Throws<InvalidOperationException>(
                () => DriverOptions.ValidateExternalExecutionProvenance(spec));

            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", "false");
            DriverOptions.ValidateExternalExecutionProvenance(spec);
        }
        finally
        {
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_COMMIT", previousCommit);
            Environment.SetEnvironmentVariable("OSS_BENCHMARK_GIT_DIRTY", previousDirty);
        }
    }

    [Theory]
    [InlineData(1, 2)]
    [InlineData(0, 2)]
    public void EmbeddedTopologyRejectsDistributedClientCoordinates(int clientOrdinal, int clientCount)
    {
        var spec = BenchmarkTestData.CreateSpec();
        var options = new DriverOptions
        {
            Command = "run",
            SpecPath = "scenario.json",
            ClientOrdinal = clientOrdinal,
            ClientCount = clientCount,
        };

        var exception = Assert.Throws<InvalidOperationException>(() => options.GetClientCoordinates(spec));

        Assert.Contains("exactly one load client", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void GeneratedRunIdContainsTimestampAndSixtyFourBitsOfEntropy()
    {
        var runId = DriverOptions.CreateGeneratedRunId(
            new DateTimeOffset(2026, 8, 10, 1, 2, 3, TimeSpan.Zero),
            [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);

        Assert.Equal("20260810010203-0001020304050607", runId);
        Assert.Equal(31, runId.Length);
    }

    [Fact]
    public void RunIdNormalizationCollapsesHyphensAndProducesSafeServiceId()
    {
        var spec = BenchmarkTestData.CreateSpec();
        var options = new DriverOptions
        {
            Command = "run",
            SpecPath = "scenario.json",
            RunId = " Run___With  Multiple---Separators ",
        };

        var runId = options.ApplyRunIdentity(spec);

        Assert.DoesNotContain("--", runId, StringComparison.Ordinal);
        Assert.DoesNotContain("--", spec.Topology.ServiceId, StringComparison.Ordinal);
        BackendNamespace.ValidateServiceId(spec.Topology.ServiceId);
    }

    [Fact]
    public void TruncatedAzureContainerNamesIncludeCollisionResistantHash()
    {
        var first = CreateAzureSpec(new string('a', 62) + "1");
        var second = CreateAzureSpec(new string('a', 62) + "2");
        var options = new DriverOptions
        {
            Command = "run",
            SpecPath = "scenario.json",
            RunId = "shared-run",
        };

        options.ApplyRunIdentity(first);
        options.ApplyRunIdentity(second);

        Assert.NotEqual(first.Storage.AzureBlobContainer, second.Storage.AzureBlobContainer);
        Assert.InRange(first.Storage.AzureBlobContainer.Length, 3, 63);
        Assert.InRange(second.Storage.AzureBlobContainer.Length, 3, 63);
        Assert.DoesNotContain("--", first.Storage.AzureBlobContainer, StringComparison.Ordinal);
        Assert.DoesNotContain("--", second.Storage.AzureBlobContainer, StringComparison.Ordinal);
        first.Validate();
        second.Validate();
    }

    [Theory]
    [InlineData("unsafe*")]
    [InlineData("double--hyphen")]
    [InlineData("-leading")]
    public void BenchmarkSpecRejectsUnsafeServiceId(string serviceId)
    {
        var spec = BenchmarkTestData.CreateSpec();
        spec.Topology.ServiceId = serviceId;

        Assert.Throws<InvalidDataException>(spec.Validate);
    }

    private static BenchmarkSpec CreateAzureSpec(string containerName)
    {
        var spec = BenchmarkTestData.CreateSpec();
        spec.Storage.Backend = StorageBackend.AzureBlob;
        spec.Storage.ConnectionStringEnvironment = "OSS_BENCHMARK_TEST_CONNECTION";
        spec.Storage.AzureBlobContainer = containerName;
        return spec;
    }
}
