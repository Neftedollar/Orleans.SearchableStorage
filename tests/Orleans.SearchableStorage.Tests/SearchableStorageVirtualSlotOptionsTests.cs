using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageVirtualSlotOptionsTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ValidationRejectsNonPositiveVirtualSlotTarget(int virtualSlotTargetCount)
    {
        using var services = CreateValidatedServices(
            "invalid-target",
            options => options.VirtualSlotTargetCount = virtualSlotTargetCount);

        Action validate = () => services.GetRequiredService<IStartupValidator>().Validate();

        var exception = validate.Should().Throw<OptionsValidationException>()
            .WithMessage("*VirtualSlotTargetCount must be greater than zero*")
            .Which;
        exception.Failures.Should().Equal("VirtualSlotTargetCount must be greater than zero.");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ValidationReportsOnlyTheDedicatedNonPositivePartitionCountFailure(int partitionCount)
    {
        using var services = CreateValidatedServices(
            "invalid-partitions",
            options => options.PartitionCount = partitionCount);

        Action validate = () => services.GetRequiredService<IStartupValidator>().Validate();

        var exception = validate.Should().Throw<OptionsValidationException>()
            .WithMessage("*PartitionCount must be greater than zero*")
            .Which;
        exception.Failures.Should().Equal("PartitionCount must be greater than zero.");
    }

    [Theory]
    [InlineData(1, StorageLayout.MaximumVirtualSlotCount + 1)]
    [InlineData(StorageLayout.MaximumVirtualSlotCount + 1, 1)]
    [InlineData(3, StorageLayout.MaximumVirtualSlotCount)]
    [InlineData(1, int.MaxValue)]
    [InlineData(int.MaxValue, 1)]
    public void ValidationRejectsVirtualSlotLayoutsBeyondTheMapCap(
        int partitionCount,
        int virtualSlotTargetCount)
    {
        using var services = CreateValidatedServices(
            "unaddressable-slots",
            options =>
            {
                options.PartitionCount = partitionCount;
                options.VirtualSlotTargetCount = virtualSlotTargetCount;
            });

        Action validate = () => services.GetRequiredService<IStartupValidator>().Validate();

        validate.Should().Throw<OptionsValidationException>()
            .WithMessage($"*no more than {StorageLayout.MaximumVirtualSlotCount} virtual slots*");
    }

    [Fact]
    public void ValidationAcceptsTheMaximumExactlyAddressableVirtualSlotLayout()
    {
        const int partitionCount = 256;
        using var services = CreateValidatedServices(
            "maximum-slots",
            options =>
            {
                options.PartitionCount = partitionCount;
                options.VirtualSlotTargetCount = StorageLayout.MaximumVirtualSlotCount;
            });

        Action validate = () => services.GetRequiredService<IStartupValidator>().Validate();

        validate.Should().NotThrow();
        StorageLayout.DeriveVirtualSlotCount(
                partitionCount,
                StorageLayout.MaximumVirtualSlotCount)
            .Should().Be(StorageLayout.MaximumVirtualSlotCount);
    }

    [Fact]
    public async Task KeyedAdminRegistrationsAreSingletonsAndUseTheirProviderIdentity()
    {
        const string firstProvider = "admin-first";
        const string secondProvider = "admin-second";
        var (grainFactory, recordingFactory) = RecordingGrainFactoryProxy.Create();
        var services = new ServiceCollection();
        services.AddSingleton(grainFactory);
        AddProvider(services, firstProvider, partitionCount: 3);
        AddProvider(services, secondProvider, partitionCount: 5);
        using var serviceProvider = services.BuildServiceProvider();
        serviceProvider.GetRequiredService<IStartupValidator>().Validate();

        var first = serviceProvider.GetRequiredKeyedService<ISearchableStorageAdminClient>(firstProvider);
        var firstAgain = serviceProvider.GetRequiredKeyedService<ISearchableStorageAdminClient>(firstProvider);
        var second = serviceProvider.GetRequiredKeyedService<ISearchableStorageAdminClient>(secondProvider);

        firstAgain.Should().BeSameAs(first);
        second.Should().NotBeSameAs(first);
        (await first.GetLayoutAsync())!.InitialPartitionCount.Should().Be(3);
        (await second.GetLayoutAsync())!.InitialPartitionCount.Should().Be(5);
        recordingFactory.RequestedProviderNames.Should().Equal(firstProvider, secondProvider);
        recordingFactory.GetLayoutGrain(firstProvider).Identities.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new
            {
                ProviderName = firstProvider,
                PartitionCount = 3,
                FormatVersion = StorageLayout.MovementFormatVersion,
            });
        recordingFactory.GetLayoutGrain(secondProvider).Identities.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new
            {
                ProviderName = secondProvider,
                PartitionCount = 5,
                FormatVersion = StorageLayout.MovementFormatVersion,
            });
    }

    private static ServiceProvider CreateValidatedServices(
        string providerName,
        Action<SearchableStorageOptions> configure)
    {
        var services = new ServiceCollection();
        AddProvider(
            services,
            providerName,
            partitionCount: 1,
            configure);
        return services.BuildServiceProvider();
    }

    private static void AddProvider(
        IServiceCollection services,
        string providerName,
        int partitionCount,
        Action<SearchableStorageOptions>? configure = null)
    {
        services.AddSearchableGrainStorage(
            providerName,
            options =>
            {
                options.PartitionCount = partitionCount;
                options.GrainStorageSerializer = StubGrainStorageSerializer.Instance;
                configure?.Invoke(options);
            });
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

    [SuppressMessage(
        "Performance",
        "CA1852:Seal internal types",
        Justification = "DispatchProxy generates a runtime subclass of this type.")]
    private class RecordingGrainFactoryProxy : DispatchProxy
    {
        private readonly Dictionary<string, RecordingLayoutGrain> _layoutGrains = new(StringComparer.Ordinal);
        private readonly List<string> _requestedProviderNames = [];

        public IReadOnlyList<string> RequestedProviderNames => _requestedProviderNames;

        public static (IGrainFactory GrainFactory, RecordingGrainFactoryProxy Recording) Create()
        {
            var grainFactory = DispatchProxy.Create<IGrainFactory, RecordingGrainFactoryProxy>();
            return (grainFactory, (RecordingGrainFactoryProxy)(object)grainFactory);
        }

        public RecordingLayoutGrain GetLayoutGrain(string providerName)
        {
            return _layoutGrains[providerName];
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            if (targetMethod.Name == nameof(IGrainFactory.GetGrain)
                && targetMethod.IsGenericMethod
                && targetMethod.GetGenericArguments()[0] == typeof(IStorageLayoutGrain)
                && args is [{ } primaryKey, ..]
                && primaryKey is string providerName)
            {
                if (!_layoutGrains.TryGetValue(providerName, out var grain))
                {
                    grain = new RecordingLayoutGrain(providerName);
                    _layoutGrains.Add(providerName, grain);
                    _requestedProviderNames.Add(providerName);
                }

                return grain;
            }

            throw new NotSupportedException(
                $"Unexpected grain-factory call '{targetMethod.Name}'.");
        }
    }

    private sealed class RecordingLayoutGrain : StorageLayoutGrainMovementTestDouble, IStorageLayoutGrain
    {
        private readonly string _providerName;

        public RecordingLayoutGrain(string providerName)
        {
            _providerName = providerName;
        }

        public List<StorageLayoutIdentity> Identities { get; } = [];

        public Task<StorageLayoutSnapshot?> GetLayoutAsync(StorageLayoutIdentity identity)
        {
            Identities.Add(identity);
            var assignments = StorageLayout.CreateIdentityAssignments(
                identity.PartitionCount,
                identity.PartitionCount);
            return Task.FromResult<StorageLayoutSnapshot?>(StorageLayoutSnapshot.FromState(
                new StorageLayoutState
                {
                    Initialized = true,
                    FormatVersion = StorageLayout.MovementFormatVersion,
                    ProviderName = _providerName,
                    PartitionCount = identity.PartitionCount,
                    JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
                    MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
                    VirtualSlotCount = assignments.Length,
                    SlotAssignments = assignments,
                    Epoch = 1,
                }));
        }

        public Task InitializeAsync(StorageLayoutDescriptor descriptor)
        {
            throw new NotSupportedException();
        }

        public Task<StorageLayoutSnapshot> InitializeRoutingAsync(StorageLayoutDescriptor descriptor)
        {
            throw new NotSupportedException();
        }

        public Task<bool> ValidateAsync(StorageLayoutDescriptor descriptor)
        {
            throw new NotSupportedException();
        }

        public Task<bool> ValidateIdentityAsync(StorageLayoutIdentity identity)
        {
            throw new NotSupportedException();
        }

        public Task<StorageLayoutSnapshot?> GetCurrentLayoutAsync()
        {
            throw new NotSupportedException();
        }
    }
}
