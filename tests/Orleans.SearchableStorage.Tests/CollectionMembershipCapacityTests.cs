using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Serialization.Activators;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class CollectionMembershipCapacityTests
{
    [Fact]
    public async Task OversizedMembershipFailsBeforeAuthorityAndAHealthyRetrySucceeds()
    {
        var layoutLoadCount = 0;
        var partition = new CountingPartition();
        var storage = new SearchableGrainStorage(
            "collection-capacity",
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                VirtualSlotTargetCount = 1,
                GrainStorageSerializer = SmallSerializer.Instance,
            },
            TestActivatorProvider.Instance,
            new StorageLayoutCache(() =>
            {
                layoutLoadCount++;
                return Task.FromResult<StorageLayoutSnapshot?>(CreateLayout());
            }),
            _ => partition);
        var state = new GrainState<CollectionMembershipState>
        {
            State = new CollectionMembershipState
            {
                Tags = Enumerable.Range(
                        0,
                        SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope + 1)
                    .Select(static value => $"tag-{value:D3}")
                    .ToArray(),
                AudienceIds = [],
                City = "Haifa",
                Salary = 1,
            },
        };
        var grainId = GrainId.Create("collection-capacity", "record");

        Func<Task> oversized = () => storage.WriteStateAsync("state", grainId, state);

        var failure = await oversized
            .Should()
            .ThrowExactlyAsync<SearchableStorageCapacityExceededException>();
        failure.Which.Boundary.Should().Be(StorageCapacityGuardrails.RecordScopeIndexEntries);
        failure.Which.Actual.Should()
            .Be(SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope + 1L);
        failure.Which.Limit.Should().Be(SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope);
        layoutLoadCount.Should().Be(0);
        partition.WriteCount.Should().Be(0);
        state.ETag.Should().BeNull();
        state.RecordExists.Should().BeFalse();

        state.State.Tags = state.State.Tags![..SearchableStorageCapacityLimits.MaximumIndexEntriesPerScope];

        await storage.WriteStateAsync("state", grainId, state);

        layoutLoadCount.Should().Be(1);
        partition.WriteCount.Should().Be(1);
        state.ETag.Should().Be("1");
        state.RecordExists.Should().BeTrue();
    }

    [Fact]
    public void DuplicateAndNullMembersProduceSortedUniqueCanonicalEntries()
    {
        var duplicateHeavyTags = Enumerable.Repeat<string?>("zeta", 80).ToList();
        duplicateHeavyTags.AddRange([null, "", "alpha", null, "alpha"]);
        var state = new CollectionMembershipState
        {
            Tags = [.. duplicateHeavyTags],
            AudienceIds = [3, null, 1, 3, 2, null, 1],
            City = "Jerusalem",
            Salary = 42,
        };

        var entries = Indexing.IndexMetadataProvider.Extract("state", state);
        var tags = entries
            .Where(static entry => entry.Scope.EndsWith("4:Tags", StringComparison.Ordinal))
            .Select(static entry => entry.Value.Text)
            .ToArray();
        var audiences = entries
            .Where(static entry => entry.Scope.EndsWith("11:AudienceIds", StringComparison.Ordinal))
            .Select(static entry => checked((int)entry.Value.SignedInteger))
            .ToArray();

        tags.Should().Equal("", "alpha", "zeta");
        audiences.Should().Equal(1, 2, 3);
    }

    private static StorageLayoutSnapshot CreateLayout()
    {
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = "collection-capacity",
            PartitionCount = 1,
            VirtualSlotCount = 1,
            SlotAssignments = [0],
            Epoch = 1,
        });
    }

    private sealed class SmallSerializer : IGrainStorageSerializer
    {
        public static SmallSerializer Instance { get; } = new();

        public BinaryData Serialize<T>(T input) => BinaryData.FromBytes([1]);

        public T Deserialize<T>(BinaryData input) => throw new NotSupportedException();
    }

    private sealed class TestActivatorProvider : IActivatorProvider
    {
        public static TestActivatorProvider Instance { get; } = new();

        public IActivator<T> GetActivator<T>() => TestActivator<T>.Instance;
    }

    private sealed class TestActivator<T> : IActivator<T>
    {
        public static TestActivator<T> Instance { get; } = new();

        public T Create() => Activator.CreateInstance<T>();
    }

    private sealed class CountingPartition
        : StoragePartitionGrainMovementTestDouble, IStoragePartitionGrain
    {
        public int WriteCount { get; private set; }

        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request)
        {
            WriteCount++;
            return Task.FromResult("1");
        }
    }
}
