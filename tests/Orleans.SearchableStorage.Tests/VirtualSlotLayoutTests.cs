using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class VirtualSlotLayoutTests
{
    [Theory]
    [InlineData(1, StorageLayout.DefaultVirtualSlotTargetCount, 16_384)]
    [InlineData(3, StorageLayout.DefaultVirtualSlotTargetCount, 16_386)]
    [InlineData(8, StorageLayout.DefaultVirtualSlotTargetCount, 16_384)]
    [InlineData(20_000, StorageLayout.DefaultVirtualSlotTargetCount, 20_000)]
    [InlineData(60_000, 160_000, 180_000)]
    [InlineData(StorageLayout.MaximumVirtualSlotCount, 1, StorageLayout.MaximumVirtualSlotCount)]
    public void VirtualSlotCountIsTheSmallestBoundedMultipleAtOrAboveTheTarget(
        int partitionCount,
        int targetCount,
        int expected)
    {
        StorageLayout.DeriveVirtualSlotCount(partitionCount, targetCount).Should().Be(expected);
    }

    [Theory]
    [InlineData(0, 1, "partitionCount")]
    [InlineData(1, 0, "virtualSlotTargetCount")]
    [InlineData(262_145, 1, "partitionCount")]
    [InlineData(1, 262_145, "virtualSlotTargetCount")]
    [InlineData(200_000, 262_144, "virtualSlotTargetCount")]
    public void VirtualSlotDerivationRejectsInvalidOrUnrepresentableMaps(
        int partitionCount,
        int targetCount,
        string parameterName)
    {
        Action derive = () => StorageLayout.DeriveVirtualSlotCount(partitionCount, targetCount);

        derive.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(parameterName);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(31)]
    [InlineData(257)]
    public void IdentityRoutingPreservesTheVersionThreeOwnerForEverySampledGrain(
        int partitionCount)
    {
        var virtualSlotCount = StorageLayout.DeriveVirtualSlotCount(
            partitionCount,
            StorageLayout.DefaultVirtualSlotTargetCount);
        var assignments = StorageLayout.CreateIdentityAssignments(partitionCount, virtualSlotCount);

        for (var index = 0; index < 10_000; index++)
        {
            var grainId = GrainId.Create("virtual-slot-property", $"grain-{index:D8}");
            var hash = (uint)grainId.GetUniformHashCode();
            var oldOwner = (int)(hash % (uint)partitionCount);
            var slot = StorageLayout.GetSlot(grainId, virtualSlotCount);

            assignments[slot].Should().Be(oldOwner);
        }
    }

    [Fact]
    public async Task FreshRoutingInitializationPersistsOneIdentityMapAndReturnsAnImmutableSnapshot()
    {
        const string providerName = "fresh-routing-layout";
        var state = new TestPersistentState<StorageLayoutState>();
        var grain = CreateGrain(state, providerName);
        var descriptor = StorageLayout.CreateDescriptor(providerName, partitionCount: 3);

        var snapshot = await grain.InitializeRoutingAsync(descriptor);

        state.WriteCount.Should().Be(1);
        state.State.FormatVersion.Should().Be(StorageLayout.CurrentFormatVersion);
        state.State.VirtualSlotCount.Should().Be(16_386);
        state.State.SlotAssignments.Should().HaveCount(16_386);
        state.State.Epoch.Should().Be(1);
        snapshot.FormatVersion.Should().Be(StorageLayout.CurrentFormatVersion);
        snapshot.ProviderName.Should().Be(providerName);
        snapshot.InitialPartitionCount.Should().Be(3);
        snapshot.VirtualSlotCount.Should().Be(16_386);
        snapshot.Epoch.Should().Be(1);
        var owners = snapshot.GetDistinctOwners();
        owners.Should().Equal(0, 1, 2);
        owners[0] = 2;
        snapshot.GetDistinctOwners().Should().Equal(0, 1, 2);
        snapshot.ContainsOwner(0).Should().BeTrue();
        snapshot.ContainsOwner(3).Should().BeFalse();

        var assignments = snapshot.CopySlotAssignments();
        assignments[0] = 2;

        snapshot.GetOwner(0).Should().Be(0);
        state.State.SlotAssignments[0].Should().Be(0);
        var secondSnapshot = await grain.GetCurrentLayoutAsync();
        secondSnapshot.Should().NotBeNull();
        secondSnapshot.Should().BeSameAs(snapshot);
        secondSnapshot!.CopySlotAssignments().Should().NotBeSameAs(assignments);
        secondSnapshot.GetOwner(0).Should().Be(0);
    }

    [Fact]
    public async Task ExistingRoutingLayoutRetainsItsPersistedSlotCountWhenTheSeedChanges()
    {
        const string providerName = "persisted-routing-count";
        var state = new TestPersistentState<StorageLayoutState>();
        var grain = CreateGrain(state, providerName);
        var original = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 3,
            virtualSlotTargetCount: 16_384);
        await grain.InitializeRoutingAsync(original);
        var changedSeed = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 3,
            virtualSlotTargetCount: StorageLayout.MaximumVirtualSlotCount + 1);

        var snapshot = await grain.InitializeRoutingAsync(changedSeed);

        state.WriteCount.Should().Be(1);
        snapshot.VirtualSlotCount.Should().Be(16_386);
        snapshot.CopySlotAssignments().Should().HaveCount(16_386);
    }

    [Fact]
    public async Task ExistingRoutingLayoutIsValidatedStructurallyWithoutApplyingTheCurrentSeedFloor()
    {
        const string providerName = "structural-routing-count";
        var state = new TestPersistentState<StorageLayoutState>
        {
            State = CreateVersionFourState(providerName, partitionCount: 8, virtualSlotCount: 8),
        };
        var grain = CreateGrain(state, providerName);
        var descriptor = StorageLayout.CreateDescriptor(providerName, partitionCount: 8);

        var snapshot = await grain.InitializeRoutingAsync(descriptor);

        state.WriteCount.Should().Be(0);
        snapshot.VirtualSlotCount.Should().Be(8);
        snapshot.CopySlotAssignments().Should().Equal(0, 1, 2, 3, 4, 5, 6, 7);
    }

    [Fact]
    public async Task VersionThreeMigrationUsesOneCasAndPreservesEveryExistingLayoutSetting()
    {
        const string providerName = "migrate-routing-layout";
        var state = new TestPersistentState<StorageLayoutState>
        {
            State = CreateVersionThreeState(
                providerName,
                partitionCount: 7,
                journalSegmentCapacity: 23,
                maximumJournalReplayEntries: 211),
        };
        var grain = CreateGrain(state, providerName);
        var descriptor = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 7,
            journalSegmentCapacity: 23,
            maximumJournalReplayEntries: 211);

        var snapshot = await grain.InitializeRoutingAsync(descriptor);

        state.WriteCount.Should().Be(1);
        state.State.FormatVersion.Should().Be(StorageLayout.CurrentFormatVersion);
        state.State.ProviderName.Should().Be(providerName);
        state.State.PartitionCount.Should().Be(7);
        state.State.JournalSegmentCapacity.Should().Be(23);
        state.State.MaximumJournalReplayEntries.Should().Be(211);
        state.State.VirtualSlotCount.Should().Be(16_387);
        state.State.Epoch.Should().Be(1);
        snapshot.CopySlotAssignments()
            .Select((owner, slot) => (owner, slot))
            .Should().OnlyContain(pair => pair.owner == pair.slot % 7);
    }

    [Theory]
    [InlineData(LegacyMismatch.Provider)]
    [InlineData(LegacyMismatch.PartitionCount)]
    [InlineData(LegacyMismatch.JournalCapacity)]
    [InlineData(LegacyMismatch.ReplayLimit)]
    [InlineData(LegacyMismatch.RoutingFields)]
    public async Task VersionThreeMigrationRequiresAnExactUnextendedLegacyLayout(
        LegacyMismatch mismatch)
    {
        const string providerName = "reject-legacy-mismatch";
        var legacy = CreateVersionThreeState(
            providerName,
            partitionCount: 8,
            journalSegmentCapacity: 16,
            maximumJournalReplayEntries: 128);
        var descriptor = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 8,
            journalSegmentCapacity: 16,
            maximumJournalReplayEntries: 128);
        switch (mismatch)
        {
            case LegacyMismatch.Provider:
                legacy.ProviderName = "different-provider";
                break;
            case LegacyMismatch.PartitionCount:
                legacy.PartitionCount++;
                break;
            case LegacyMismatch.JournalCapacity:
                legacy.JournalSegmentCapacity++;
                break;
            case LegacyMismatch.ReplayLimit:
                legacy.MaximumJournalReplayEntries++;
                break;
            case LegacyMismatch.RoutingFields:
                legacy.VirtualSlotCount = 8;
                legacy.SlotAssignments = StorageLayout.CreateIdentityAssignments(8, 8);
                legacy.Epoch = 1;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mismatch), mismatch, null);
        }

        var state = new TestPersistentState<StorageLayoutState> { State = legacy };
        var grain = CreateGrain(state, providerName);
        Func<Task> migrate = () => grain.InitializeRoutingAsync(descriptor);

        await migrate.Should().ThrowAsync<InvalidOperationException>();
        state.WriteCount.Should().Be(0);
        state.State.FormatVersion.Should().Be(StorageLayout.PreviousFormatVersion);
    }

    [Fact]
    public async Task LegacyValidationRemainsCompatibleWithTheUnmovedVersionFourLayout()
    {
        const string providerName = "legacy-compatible-routing";
        var state = new TestPersistentState<StorageLayoutState>();
        var grain = CreateGrain(state, providerName);
        var routingDescriptor = StorageLayout.CreateDescriptor(providerName, partitionCount: 8);
        await grain.InitializeRoutingAsync(routingDescriptor);
        var legacyDescriptor = CreateVersionThreeDescriptor(providerName, partitionCount: 8);
        var legacyIdentity = new StorageLayoutIdentity
        {
            FormatVersion = StorageLayout.PreviousFormatVersion,
            ProviderName = providerName,
            PartitionCount = 8,
        };

        (await grain.ValidateAsync(legacyDescriptor)).Should().BeTrue();
        (await grain.ValidateIdentityAsync(legacyIdentity)).Should().BeTrue();
        await grain.InitializeAsync(legacyDescriptor);

        state.WriteCount.Should().Be(1);
        state.State.FormatVersion.Should().Be(StorageLayout.CurrentFormatVersion);
        state.State.Epoch.Should().Be(1);
    }

    [Fact]
    public async Task RoutingReadsDoNotImplicitlyUpgradeAStoredVersionThreeLayout()
    {
        const string providerName = "read-does-not-migrate";
        var state = new TestPersistentState<StorageLayoutState>
        {
            State = CreateVersionThreeState(providerName, 8, 64, 4_096),
        };
        var grain = CreateGrain(state, providerName);
        var identity = StorageLayout.CreateIdentity(providerName, partitionCount: 8);

        Func<Task> byIdentity = () => grain.GetLayoutAsync(identity);
        Func<Task> current = () => grain.GetCurrentLayoutAsync();

        await byIdentity.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*InitializeRoutingAsync*");
        await current.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*InitializeRoutingAsync*");
        state.WriteCount.Should().Be(0);
        state.State.FormatVersion.Should().Be(StorageLayout.PreviousFormatVersion);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbiguousFreshOrMigrationWritePoisonsTheActivation(bool migrate)
    {
        const string providerName = "ambiguous-routing-layout";
        var injected = new InvalidOperationException("Ambiguous layout write.");
        var state = new TestPersistentState<StorageLayoutState>
        {
            State = migrate
                ? CreateVersionThreeState(providerName, 8, 64, 4_096)
                : new StorageLayoutState(),
            WriteException = injected,
        };
        var poisonCount = 0;
        var grain = new StorageLayoutGrain(state, providerName, () => poisonCount++);
        var descriptor = StorageLayout.CreateDescriptor(providerName, partitionCount: 8);

        Func<Task> initialize = () => grain.InitializeRoutingAsync(descriptor);
        await initialize.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);

        poisonCount.Should().Be(1);
        state.WriteCount.Should().Be(1);
        state.State.FormatVersion.Should().Be(
            migrate ? StorageLayout.PreviousFormatVersion : 0);

        Func<Task> read = () => grain.GetCurrentLayoutAsync();
        Func<Task> retry = () => grain.InitializeRoutingAsync(descriptor);
        await read.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*retiring after an ambiguous persistence outcome*");
        await retry.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*retiring after an ambiguous persistence outcome*");
        state.WriteCount.Should().Be(1);
    }

    [Theory]
    [InlineData(MalformedRoutingLayout.VirtualSlotCountBelowPartitionCount)]
    [InlineData(MalformedRoutingLayout.VirtualSlotCountAboveLimit)]
    [InlineData(MalformedRoutingLayout.VirtualSlotCountNotDivisible)]
    [InlineData(MalformedRoutingLayout.NullAssignments)]
    [InlineData(MalformedRoutingLayout.WrongAssignmentCount)]
    [InlineData(MalformedRoutingLayout.WrongOwner)]
    [InlineData(MalformedRoutingLayout.NegativeOwner)]
    [InlineData(MalformedRoutingLayout.OwnerBeyondInitialPartitions)]
    [InlineData(MalformedRoutingLayout.ZeroEpoch)]
    [InlineData(MalformedRoutingLayout.AdvancedEpoch)]
    [InlineData(MalformedRoutingLayout.ProviderMismatch)]
    public async Task MalformedVersionFourRoutingStateIsRejected(
        MalformedRoutingLayout malformed)
    {
        const string providerName = "malformed-routing-layout";
        var persisted = CreateVersionFourState(providerName, partitionCount: 8, virtualSlotCount: 16_384);
        switch (malformed)
        {
            case MalformedRoutingLayout.VirtualSlotCountBelowPartitionCount:
                persisted.VirtualSlotCount = 4;
                persisted.SlotAssignments = new int[4];
                break;
            case MalformedRoutingLayout.VirtualSlotCountAboveLimit:
                persisted.VirtualSlotCount = StorageLayout.MaximumVirtualSlotCount + 1;
                persisted.SlotAssignments = [];
                break;
            case MalformedRoutingLayout.VirtualSlotCountNotDivisible:
                persisted.VirtualSlotCount = 16_385;
                persisted.SlotAssignments = new int[16_385];
                break;
            case MalformedRoutingLayout.NullAssignments:
                persisted.SlotAssignments = null!;
                break;
            case MalformedRoutingLayout.WrongAssignmentCount:
                persisted.SlotAssignments = new int[16_383];
                break;
            case MalformedRoutingLayout.WrongOwner:
                persisted.SlotAssignments[0] = 1;
                break;
            case MalformedRoutingLayout.NegativeOwner:
                persisted.SlotAssignments[0] = -1;
                break;
            case MalformedRoutingLayout.OwnerBeyondInitialPartitions:
                persisted.SlotAssignments[0] = persisted.PartitionCount;
                break;
            case MalformedRoutingLayout.ZeroEpoch:
                persisted.Epoch = 0;
                break;
            case MalformedRoutingLayout.AdvancedEpoch:
                persisted.Epoch = 2;
                break;
            case MalformedRoutingLayout.ProviderMismatch:
                persisted.ProviderName = "another-provider";
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(malformed), malformed, null);
        }

        var state = new TestPersistentState<StorageLayoutState> { State = persisted };
        var grain = CreateGrain(state, providerName);
        Func<Task> read = () => grain.GetCurrentLayoutAsync();

        await read.Should().ThrowAsync<InvalidOperationException>();
        state.WriteCount.Should().Be(0);
    }

    [Fact]
    public async Task UninitializedLayoutReturnsNoCurrentOrIdentitySnapshot()
    {
        const string providerName = "empty-routing-layout";
        var state = new TestPersistentState<StorageLayoutState>();
        var grain = CreateGrain(state, providerName);

        (await grain.GetCurrentLayoutAsync()).Should().BeNull();
        (await grain.GetLayoutAsync(StorageLayout.CreateIdentity(providerName, 8))).Should().BeNull();
        state.WriteCount.Should().Be(0);
    }

    private static StorageLayoutGrain CreateGrain(
        TestPersistentState<StorageLayoutState> state,
        string providerName)
    {
        return new StorageLayoutGrain(state, providerName, requestDeactivation: static () => { });
    }

    private static StorageLayoutDescriptor CreateVersionThreeDescriptor(
        string providerName,
        int partitionCount,
        int journalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
        int maximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries)
    {
        return new StorageLayoutDescriptor
        {
            FormatVersion = StorageLayout.PreviousFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
            JournalSegmentCapacity = journalSegmentCapacity,
            MaximumJournalReplayEntries = maximumJournalReplayEntries,
        };
    }

    private static StorageLayoutState CreateVersionThreeState(
        string providerName,
        int partitionCount,
        int journalSegmentCapacity,
        int maximumJournalReplayEntries)
    {
        return new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.PreviousFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
            JournalSegmentCapacity = journalSegmentCapacity,
            MaximumJournalReplayEntries = maximumJournalReplayEntries,
        };
    }

    private static StorageLayoutState CreateVersionFourState(
        string providerName,
        int partitionCount,
        int virtualSlotCount)
    {
        return new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = virtualSlotCount,
            SlotAssignments = StorageLayout.CreateIdentityAssignments(partitionCount, virtualSlotCount),
            Epoch = 1,
        };
    }
}

public enum LegacyMismatch
{
    Provider = 0,
    PartitionCount = 1,
    JournalCapacity = 2,
    ReplayLimit = 3,
    RoutingFields = 4,
}

public enum MalformedRoutingLayout
{
    VirtualSlotCountBelowPartitionCount = 0,
    VirtualSlotCountAboveLimit = 1,
    VirtualSlotCountNotDivisible = 2,
    NullAssignments = 3,
    WrongAssignmentCount = 4,
    WrongOwner = 5,
    ZeroEpoch = 6,
    AdvancedEpoch = 7,
    ProviderMismatch = 8,
    NegativeOwner = 9,
    OwnerBeyondInitialPartitions = 10,
}
