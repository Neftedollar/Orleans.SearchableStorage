using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using AwesomeAssertions;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageSchemaCapabilityGateTests
{
    private const string ProviderName = "schema-capability-gate";
    private const string StateName = "vacancy";

    [Fact]
    public async Task CachedLegacyLayoutCannotLetEmptyPublicQueriesBypassManagedSchemaGate()
    {
        var legacyLayout = CreateLayout(managedSchemas: false);
        var managedLayout = CreateLayout(managedSchemas: true);
        var (grainFactory, recording) = CapabilityGrainFactoryProxy.Create(legacyLayout);
        var client = new SearchableStorageClient(
            grainFactory,
            ProviderName,
            partitionCount: 1,
            CreateQueryOptions());
        var contradictory = client.Query<GateState>(StateName)
            .Where(state => state.City == "Moscow" && state.City == "Kazan");

        (await contradictory.ToGrainIdsAsync()).Should().BeEmpty();
        recording.Layout.ReadCount.Should().Be(2,
            "the fresh capability probe does not replace the ordinary routing cache");
        recording.PartitionLookupCount.Should().Be(0);

        recording.Layout.Current = managedLayout;
        Func<Task>[] emptyTerminals =
        [
            async () => await contradictory.ToGrainIdsAsync(),
            async () => await contradictory.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(1)),
            async () => await contradictory.ToDistinctFacetValuePageAsync(
                state => state.City,
                new SearchableStorageFacetPageRequest(1)),
            async () => await contradictory.ToFacetValueCountsAsync(
                state => state.City,
                new SearchableStorageFacetRequest(
                    1,
                    SearchableStorageFacetAccuracy.Exact)),
            async () => await contradictory.ToFacetMinMaxAsync(state => state.City),
        ];

        foreach (var execute in emptyTerminals)
        {
            await execute.Should().ThrowExactlyAsync<SearchableStorageIndexSchemaException>()
                .WithMessage("*managed index schemas enabled*explicit managed schema binding*");
        }

        recording.Layout.ReadCount.Should().Be(2 + emptyTerminals.Length);
        recording.PartitionLookupCount.Should().Be(0,
            "contradictory plans remain local but each must probe the durable provider gate first");
    }

    [Fact]
    public async Task FirstAdoptionEnablementIntentFencesSchemaUnawareEmptyQueries()
    {
        var (grainFactory, recording) = CapabilityGrainFactoryProxy.Create(
            CreateLayout(managedSchemas: false, enablingSchemas: true));
        var client = new SearchableStorageClient(
            grainFactory,
            ProviderName,
            partitionCount: 1,
            CreateQueryOptions());
        var contradictory = client.Query<GateState>(StateName)
            .Where(state => state.City == "Moscow" && state.City == "Kazan");

        Func<Task> execute = async () => await contradictory.ToGrainIdsAsync();

        await execute.Should().ThrowExactlyAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*durably enabling managed index schemas*explicit managed schema binding*");
        recording.Layout.ReadCount.Should().Be(1);
        recording.PartitionLookupCount.Should().Be(0);
    }

    [Fact]
    public async Task PartialRegistryRejectsEveryOmittedStateEmptyTerminalWithoutAnRpc()
    {
        var (grainFactory, recording) = CapabilityGrainFactoryProxy.Create(
            CreateLayout(managedSchemas: false));
        var registry = new SearchableStorageSchemaRegistry()
            .AddState<GateState>("declared-state");
        var client = new SearchableStorageClient(
            grainFactory,
            ProviderName,
            partitionCount: 1,
            CreateQueryOptions(),
            registry);
        var omitted = client.Query<GateState>(StateName)
            .Where(state => state.City == "Moscow" && state.City == "Kazan");
        Func<Task>[] emptyTerminals =
        [
            async () => await omitted.ToGrainIdsAsync(),
            async () => await omitted.ToGrainIdPageAsync(
                new SearchableStorageQueryPageRequest(1)),
            async () => await omitted.ToDistinctFacetValuePageAsync(
                state => state.City,
                new SearchableStorageFacetPageRequest(1)),
            async () => await omitted.ToFacetValueCountsAsync(
                state => state.City,
                new SearchableStorageFacetRequest(
                    1,
                    SearchableStorageFacetAccuracy.Exact)),
            async () => await omitted.ToFacetMinMaxAsync(state => state.City),
        ];

        foreach (var execute in emptyTerminals)
        {
            await execute.Should().ThrowExactlyAsync<SearchableStorageIndexSchemaException>()
                .WithMessage("*managed schema declarations*not declared by this query client*");
        }

        recording.Layout.ReadCount.Should().Be(0);
        recording.Schema.GetCount.Should().Be(0);
        recording.PartitionLookupCount.Should().Be(0);
    }

    [Fact]
    public async Task ActiveRegisteredSchemaKeepsTheSteadyStateControlAndLayoutCaches()
    {
        var (grainFactory, recording) = CapabilityGrainFactoryProxy.Create(
            CreateLayout(managedSchemas: true));
        var registry = new SearchableStorageSchemaRegistry()
            .AddState<GateState>(StateName);
        var client = new SearchableStorageClient(
            grainFactory,
            ProviderName,
            partitionCount: 1,
            CreateQueryOptions(),
            registry);
        var contradictory = client.Query<GateState>(StateName)
            .Where(state => state.City == "Moscow" && state.City == "Kazan");

        (await contradictory.ToGrainIdsAsync()).Should().BeEmpty();
        (await contradictory.ToGrainIdsAsync()).Should().BeEmpty();

        recording.Schema.GetCount.Should().Be(1,
            "the first successful control read confirms the active fingerprint for this client");
        recording.Layout.ReadCount.Should().Be(1,
            "empty execution reads layout once and then retains the shared cached snapshot");
        recording.PartitionLookupCount.Should().Be(0);
    }

    [Fact]
    public async Task PreCanceledEmptyQueryDoesNotStartCapabilityProbe()
    {
        var (grainFactory, recording) = CapabilityGrainFactoryProxy.Create(
            CreateLayout(managedSchemas: false));
        var client = new SearchableStorageClient(
            grainFactory,
            ProviderName,
            partitionCount: 1,
            CreateQueryOptions());
        var contradictory = client.Query<GateState>(StateName)
            .Where(state => state.City == "Moscow" && state.City == "Kazan");
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task> execute = async () => await contradictory.ToGrainIdsAsync(
            cancellation.Token);

        await execute.Should().ThrowExactlyAsync<OperationCanceledException>();
        recording.Layout.ReadCount.Should().Be(0);
        recording.PartitionLookupCount.Should().Be(0);
    }

    [Fact]
    public async Task LaterFreshReadsDoNotReuseAPrePublicationInflightRead()
    {
        var legacyLayout = CreateLayout(managedSchemas: false);
        var managedLayout = CreateLayout(managedSchemas: true);
        var prePublicationCompletion = new TaskCompletionSource<StorageLayoutSnapshot?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var loadCount = 0;
        var cache = new StorageLayoutCache(() => Interlocked.Increment(ref loadCount) switch
        {
            1 => Task.FromResult<StorageLayoutSnapshot?>(legacyLayout),
            2 => prePublicationCompletion.Task,
            _ => Task.FromResult<StorageLayoutSnapshot?>(managedLayout),
        });

        (await cache.GetAsync()).Should().BeSameAs(legacyLayout);
        var prePublicationRead = cache.ReadFreshAsync();

        prePublicationRead.IsCompleted.Should().BeFalse();
        var postPublicationReads = Enumerable.Range(0, 8)
            .Select(_ => cache.ReadFreshAsync())
            .ToArray();

        loadCount.Should().Be(10,
            "every fresh caller must initiate its own authoritative read");
        (await Task.WhenAll(postPublicationReads))
            .Should().OnlyContain(layout => ReferenceEquals(layout, managedLayout));

        prePublicationCompletion.SetResult(legacyLayout);
        (await prePublicationRead).Should().BeSameAs(legacyLayout);
        (await cache.GetAsync()).Should().BeSameAs(legacyLayout,
            "a capability probe must not replace the shared routing cache");
        loadCount.Should().Be(10);
    }

    [Fact]
    public async Task CancelingFreshReadObservesItsDetachedLoadWithoutChangingTheCache()
    {
        var legacyLayout = CreateLayout(managedSchemas: false);
        var managedLayout = CreateLayout(managedSchemas: true);
        var detachedCompletion = new TaskCompletionSource<StorageLayoutSnapshot?>();
        var loadCount = 0;
        var cache = new StorageLayoutCache(() => Interlocked.Increment(ref loadCount) switch
        {
            1 => Task.FromResult<StorageLayoutSnapshot?>(legacyLayout),
            2 => detachedCompletion.Task,
            3 => Task.FromResult<StorageLayoutSnapshot?>(managedLayout),
            _ => throw new InvalidOperationException("Unexpected layout read."),
        });
        using var cancellation = new CancellationTokenSource();

        (await cache.GetAsync()).Should().BeSameAs(legacyLayout);
        var canceledWaiter = cache.ReadFreshAsync(cancellation.Token);
        await cancellation.CancelAsync();

        Func<Task> waitForCancellation = async () => await canceledWaiter;
        await waitForCancellation.Should().ThrowAsync<OperationCanceledException>();
        (await cache.ReadFreshAsync()).Should().BeSameAs(managedLayout,
            "a later probe must not inherit the canceled caller's earlier read");

        detachedCompletion.SetException(new InvalidOperationException("Detached read failed."));
        (await cache.GetAsync()).Should().BeSameAs(legacyLayout);
        loadCount.Should().Be(3);
    }

    private static SearchableStorageQueryOptions CreateQueryOptions()
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "schema-gate-tests",
            Enumerable.Repeat((byte)0x5A, 32).ToArray());
        return options;
    }

    private static StorageLayoutSnapshot CreateLayout(
        bool managedSchemas,
        bool enablingSchemas = false)
    {
        var state = new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = managedSchemas || enablingSchemas
                ? StorageLayout.IndexSchemaFormatVersion
                : StorageLayout.MovementFormatVersion,
            ProviderName = ProviderName,
            PartitionCount = 1,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = 1,
            SlotAssignments = [0],
            Epoch = 1,
            IndexSchemaProtocolVersion = managedSchemas
                ? StorageIndexSchema.ProtocolVersion
                : 0,
        };
        if (enablingSchemas)
        {
            state.IndexSchemaEnablement = new StorageIndexSchemaEnableIntent
            {
                EnablementId = Guid.Parse("3d1450fb-68af-47a1-9b4c-e0ed3398a605"),
                ProtocolVersion = StorageIndexSchema.ProtocolVersion,
                LayoutEpoch = state.Epoch,
                LayoutFingerprint = StorageLayoutFingerprint.Compute(
                    StorageLayoutSnapshot.FromState(state)),
            };
        }

        return StorageLayoutSnapshot.FromState(state);
    }

    private sealed class GateState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;
    }

    [SuppressMessage(
        "Performance",
        "CA1852:Seal internal types",
        Justification = "DispatchProxy generates a runtime subclass of this type.")]
    private class CapabilityGrainFactoryProxy : DispatchProxy
    {
        public MutableLayoutGrain Layout { get; private set; } = null!;

        public ActiveSchemaGrain Schema { get; } = new();

        public int PartitionLookupCount { get; private set; }

        public static (IGrainFactory GrainFactory, CapabilityGrainFactoryProxy Recording) Create(
            StorageLayoutSnapshot initialLayout)
        {
            var grainFactory = DispatchProxy.Create<IGrainFactory, CapabilityGrainFactoryProxy>();
            var recording = (CapabilityGrainFactoryProxy)(object)grainFactory;
            recording.Layout = new MutableLayoutGrain(initialLayout);
            return (grainFactory, recording);
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            if (targetMethod.Name == nameof(IGrainFactory.GetGrain)
                && targetMethod.IsGenericMethod)
            {
                var grainType = targetMethod.GetGenericArguments()[0];
                if (grainType == typeof(IStorageLayoutGrain))
                {
                    return Layout;
                }

                if (grainType == typeof(IStorageIndexSchemaGrain))
                {
                    return Schema;
                }

                if (grainType == typeof(IStoragePartitionGrain))
                {
                    PartitionLookupCount++;
                    throw new InvalidOperationException(
                        "An empty query must not resolve a storage partition.");
                }
            }

            throw new NotSupportedException(
                $"Unexpected grain-factory call '{targetMethod.Name}'.");
        }
    }

    private sealed class MutableLayoutGrain : StorageLayoutGrainMovementTestDouble, IStorageLayoutGrain
    {
        private StorageLayoutSnapshot _current;
        private int _readCount;

        public MutableLayoutGrain(StorageLayoutSnapshot current)
        {
            _current = current;
        }

        public StorageLayoutSnapshot Current
        {
            get => Volatile.Read(ref _current);
            set => Volatile.Write(ref _current, value);
        }

        public int ReadCount => Volatile.Read(ref _readCount);

        public Task<StorageLayoutSnapshot?> GetLayoutAsync(StorageLayoutIdentity identity)
        {
            Interlocked.Increment(ref _readCount);
            return Task.FromResult<StorageLayoutSnapshot?>(Current);
        }
    }

    private sealed class ActiveSchemaGrain : IStorageIndexSchemaGrain
    {
        private int _getCount;

        public int GetCount => Volatile.Read(ref _getCount);

        public Task<StorageIndexSchemaSnapshot> GetAsync(StorageIndexSchemaRequest request)
        {
            Interlocked.Increment(ref _getCount);
            return Task.FromResult(new StorageIndexSchemaSnapshot
            {
                ProviderName = request.ProviderName,
                StateName = request.StateName,
                ActiveFingerprint = [.. request.Fingerprint],
            });
        }

        public Task<StorageIndexSchemaSnapshot> BeginRebuildAsync(
            StorageIndexSchemaRequest request) => throw new NotSupportedException();

        public Task<StorageIndexSchemaSnapshot> AdvanceRebuildAsync(
            StorageIndexSchemaCommand command) => throw new NotSupportedException();
    }
}
