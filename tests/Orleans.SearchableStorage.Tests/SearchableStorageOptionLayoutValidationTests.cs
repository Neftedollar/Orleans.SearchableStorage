using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.Serialization.Serializers;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageOptionValidationTests
{
    [Fact]
    public void ValidateOnStartRejectsAnUnaddressableJournalRing()
    {
        const string providerName = "unaddressable-options";
        var services = new ServiceCollection();
        services.AddSearchableGrainStorage(
            providerName,
            options =>
            {
                options.PartitionCount = 1;
                options.JournalSegmentCapacity = 1;
                options.MaximumJournalReplayEntries = int.MaxValue;
                options.CompactionThreshold = 1;
                options.GrainStorageSerializer = StubGrainStorageSerializer.Instance;
            });
        using var serviceProvider = services.BuildServiceProvider();

        Action validate = () => serviceProvider.GetRequiredService<IStartupValidator>().Validate();

        validate.Should().Throw<OptionsValidationException>()
            .WithMessage("*addressable journal ring*");
    }

    [Fact]
    public void ValidateOnStartAcceptsTheLargestAddressableJournalRing()
    {
        const string providerName = "largest-addressable-options";
        var services = new ServiceCollection();
        services.AddSearchableGrainStorage(
            providerName,
            options =>
            {
                options.PartitionCount = 1;
                options.JournalSegmentCapacity = 1;
                options.MaximumJournalReplayEntries = int.MaxValue - 2;
                options.CompactionThreshold = 1;
                options.GrainStorageSerializer = StubGrainStorageSerializer.Instance;
            });
        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider.GetRequiredService<IStartupValidator>().Validate();
    }

    [Fact]
    public void DirectStorageConstructionRejectsAnUnaddressableJournalRingBeforeUsingRuntimeServices()
    {
        var options = new SearchableStorageOptions
        {
            PartitionCount = 1,
            JournalSegmentCapacity = 1,
            MaximumJournalReplayEntries = int.MaxValue,
            CompactionThreshold = 1,
            GrainStorageSerializer = StubGrainStorageSerializer.Instance,
        };
        var grainFactory = ThrowingProxy.Create<IGrainFactory>();
        var activatorProvider = ThrowingProxy.Create<IActivatorProvider>();

        Action create = () => _ = new SearchableGrainStorage(
            "unaddressable-constructor",
            options,
            grainFactory,
            activatorProvider,
            SearchableStateRegistry.Empty);

        create.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("maxReplayEntries")
            .WithMessage("*more journal slots than can be addressed*");
    }

    private sealed class StubGrainStorageSerializer : IGrainStorageSerializer
    {
        public static StubGrainStorageSerializer Instance { get; } = new();

        public BinaryData Serialize<T>(T input)
        {
            throw new NotSupportedException();
        }

        public T Deserialize<T>(BinaryData input)
        {
            throw new NotSupportedException();
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance",
        "CA1852:Seal internal types",
        Justification = "DispatchProxy generates a runtime subclass of this type.")]
    private class ThrowingProxy : DispatchProxy
    {
        public static T Create<T>()
            where T : class
        {
            return DispatchProxy.Create<T, ThrowingProxy>();
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            throw new InvalidOperationException(
                $"Runtime service '{targetMethod?.Name}' was used before options validation completed.");
        }
    }
}

public sealed class StorageLayoutValidationTests : IClassFixture<MemoryStorageFixture>
{
    private readonly MemoryStorageFixture _fixture;

    public StorageLayoutValidationTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task LayoutInitializationRejectsAnUnaddressableJournalRingBeforePersistence()
    {
        var providerName = $"unaddressable-layout-{Guid.NewGuid():N}";
        var layout = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var descriptor = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 1,
            journalSegmentCapacity: 1,
            maximumJournalReplayEntries: int.MaxValue);

        Func<Task> initialize = () => layout.InitializeAsync(descriptor);

        await initialize.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithMessage("*more journal slots than can be addressed*");
        (await layout.ValidateIdentityAsync(StorageLayout.CreateIdentity(providerName, partitionCount: 1)))
            .Should().BeFalse();
    }

    [Theory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit, false)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit, true)]
    public async Task LayoutInitializationRetryReconcilesAnAmbiguousPhysicalWrite(
        PhysicalWriteFaultStage stage,
        bool persistedAfterFailure)
    {
        var providerName = $"ambiguous-layout-{stage}-{Guid.NewGuid():N}";
        var layout = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var descriptor = StorageLayout.CreateDescriptor(providerName, partitionCount: 1);
        await WriteFaultInjectingGrainStorage.AddWriteFaultAsync(
            _fixture.Cluster.GrainFactory,
            layout.GetGrainId(),
            "layout",
            stage);

        Func<Task> initialize = () => layout.InitializeAsync(descriptor);
        var failure = await initialize.Should().ThrowAsync<OrleansException>();
        failure.Which.InnerException.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Be(WriteFaultInjectingGrainStorage.InjectedFailureMessage);

        await _fixture.Cluster.DeactivateAsync(layout);
        (await layout.ValidateAsync(descriptor)).Should().Be(persistedAfterFailure);

        await layout.InitializeAsync(descriptor);
        await _fixture.Cluster.DeactivateAsync(layout);
        (await layout.ValidateAsync(descriptor)).Should().BeTrue();
    }

    [Theory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit, false)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit, true)]
    public async Task VersionThreeMigrationRetryReconcilesAnAmbiguousPhysicalWrite(
        PhysicalWriteFaultStage stage,
        bool migratedAfterFailure)
    {
        var providerName = $"ambiguous-layout-migration-{stage}-{Guid.NewGuid():N}";
        var layout = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var physical = GetPhysicalStorage();
        var persisted = new GrainState<StorageLayoutState>
        {
            State = new StorageLayoutState
            {
                Initialized = true,
                FormatVersion = StorageLayout.LegacyFormatVersion,
                ProviderName = providerName,
                PartitionCount = 3,
                JournalSegmentCapacity = 17,
                MaximumJournalReplayEntries = 101,
            },
        };
        await physical.WriteStateAsync("layout", layout.GetGrainId(), persisted);
        var descriptor = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 3,
            journalSegmentCapacity: 17,
            maximumJournalReplayEntries: 101);
        await WriteFaultInjectingGrainStorage.AddWriteFaultAsync(
            _fixture.Cluster.GrainFactory,
            layout.GetGrainId(),
            "layout",
            stage);

        try
        {
            Func<Task> migrate = () => layout.InitializeRoutingAsync(descriptor);
            var failure = await migrate.Should().ThrowAsync<OrleansException>();
            failure.Which.InnerException.Should().BeOfType<InvalidOperationException>()
                .Which.Message.Should().Be(WriteFaultInjectingGrainStorage.InjectedFailureMessage);

            await _fixture.Cluster.DeactivateAsync(layout);
            var afterFailure = new GrainState<StorageLayoutState>();
            await physical.ReadStateAsync("layout", layout.GetGrainId(), afterFailure);
            afterFailure.State.FormatVersion.Should().Be(
                migratedAfterFailure
                    ? StorageLayout.MovementFormatVersion
                    : StorageLayout.LegacyFormatVersion);
            var etagAfterFailure = afterFailure.ETag;
            var callsAfterFailure = await WriteFaultInjectingGrainStorage.GetWriteCallCountAsync(
                _fixture.Cluster.GrainFactory,
                layout.GetGrainId(),
                "layout");
            callsAfterFailure.Should().Be(2);

            var recovered = await layout.InitializeRoutingAsync(descriptor);
            recovered.FormatVersion.Should().Be(StorageLayout.MovementFormatVersion);
            recovered.InitialPartitionCount.Should().Be(3);
            recovered.Epoch.Should().Be(1);
            recovered.CopySlotAssignments()
                .Select((owner, slot) => (owner, slot))
                .Should().OnlyContain(pair => pair.owner == pair.slot % 3);
            var afterRecovery = new GrainState<StorageLayoutState>();
            await physical.ReadStateAsync("layout", layout.GetGrainId(), afterRecovery);
            var callsAfterRecovery = await WriteFaultInjectingGrainStorage.GetWriteCallCountAsync(
                _fixture.Cluster.GrainFactory,
                layout.GetGrainId(),
                "layout");
            if (stage == PhysicalWriteFaultStage.BeforeCommit)
            {
                callsAfterRecovery.Should().Be(3);
                afterRecovery.ETag.Should().NotBe(etagAfterFailure);
            }
            else
            {
                callsAfterRecovery.Should().Be(2);
                afterRecovery.ETag.Should().Be(etagAfterFailure);
            }
        }
        finally
        {
            await _fixture.Cluster.DeactivateAsync(layout);
            var cleanup = new GrainState<StorageLayoutState>();
            await physical.ReadStateAsync("layout", layout.GetGrainId(), cleanup);
            if (cleanup.RecordExists)
            {
                await physical.ClearStateAsync("layout", layout.GetGrainId(), cleanup);
            }
        }
    }

    private IGrainStorage GetPhysicalStorage()
    {
        var silo = Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary);
        return silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
    }
}
