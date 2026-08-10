using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal static class StorageLayout
{
    public const int CurrentFormatVersion = 4;
    public const int PreviousFormatVersion = 3;
    public const int DefaultVirtualSlotTargetCount = 16_384;
    public const int MaximumVirtualSlotCount = 262_144;

    public static StorageLayoutDescriptor CreateDescriptor(
        string providerName,
        int partitionCount,
        int journalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
        int maximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
        int virtualSlotTargetCount = DefaultVirtualSlotTargetCount)
    {
        return new StorageLayoutDescriptor
        {
            FormatVersion = CurrentFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
            JournalSegmentCapacity = journalSegmentCapacity,
            MaximumJournalReplayEntries = maximumJournalReplayEntries,
            VirtualSlotTargetCount = virtualSlotTargetCount,
        };
    }

    public static StorageLayoutIdentity CreateIdentity(string providerName, int partitionCount)
    {
        return new StorageLayoutIdentity
        {
            FormatVersion = CurrentFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
        };
    }

    public static int DeriveVirtualSlotCount(int partitionCount, int virtualSlotTargetCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualSlotTargetCount);

        if (partitionCount > MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitionCount),
                partitionCount,
                $"PartitionCount must not exceed the virtual-slot map limit of {MaximumVirtualSlotCount}.");
        }

        if (virtualSlotTargetCount > MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(virtualSlotTargetCount),
                virtualSlotTargetCount,
                $"VirtualSlotTargetCount must not exceed the virtual-slot map limit of {MaximumVirtualSlotCount}.");
        }

        var multiplier = checked(
            (virtualSlotTargetCount + (long)partitionCount - 1) / partitionCount);
        var virtualSlotCount = checked(multiplier * partitionCount);
        if (virtualSlotCount > MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(virtualSlotTargetCount),
                virtualSlotTargetCount,
                $"The smallest virtual-slot count divisible by PartitionCount exceeds the map limit of {MaximumVirtualSlotCount}.");
        }

        return checked((int)virtualSlotCount);
    }

    public static int[] CreateIdentityAssignments(int partitionCount, int virtualSlotCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualSlotCount);
        if (virtualSlotCount > MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(virtualSlotCount),
                virtualSlotCount,
                $"VirtualSlotCount must not exceed the map limit of {MaximumVirtualSlotCount}.");
        }

        if (virtualSlotCount < partitionCount || virtualSlotCount % partitionCount != 0)
        {
            throw new ArgumentException(
                "VirtualSlotCount must be greater than or equal to and divisible by PartitionCount.",
                nameof(virtualSlotCount));
        }

        var assignments = new int[virtualSlotCount];
        for (var slot = 0; slot < assignments.Length; slot++)
        {
            assignments[slot] = slot % partitionCount;
        }

        return assignments;
    }

    public static int GetSlot(GrainId grainId, int virtualSlotCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(virtualSlotCount);
        if (virtualSlotCount > MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(virtualSlotCount),
                virtualSlotCount,
                $"VirtualSlotCount must not exceed the map limit of {MaximumVirtualSlotCount}.");
        }

        return (int)((uint)grainId.GetUniformHashCode() % (uint)virtualSlotCount);
    }

    public static string CreatePartitionKey(string providerName, int partitionIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegative(partitionIndex);
        return $"{providerName}:{partitionIndex:D8}";
    }
}

[GenerateSerializer]
internal sealed class StorageLayoutDescriptor
{
    [Id(0)]
    public int FormatVersion { get; init; }

    [Id(1)]
    public required string ProviderName { get; init; }

    [Id(2)]
    public int PartitionCount { get; init; }

    [Id(3)]
    public int JournalSegmentCapacity { get; init; }

    [Id(4)]
    public int MaximumJournalReplayEntries { get; init; }

    /// <summary>
    /// Seeds the immutable virtual-slot count when a routing layout is first created.
    /// Existing version-4 layouts retain their persisted count when this default changes.
    /// </summary>
    [Id(5)]
    public int VirtualSlotTargetCount { get; init; }
}

[GenerateSerializer]
internal sealed class StorageLayoutIdentity
{
    [Id(0)]
    public int FormatVersion { get; init; }

    [Id(1)]
    public required string ProviderName { get; init; }

    [Id(2)]
    public int PartitionCount { get; init; }
}

/// <summary>
/// Provides an immutable routing view detached from the layout grain's mutable persistence state.
/// </summary>
[GenerateSerializer]
internal sealed class StorageLayoutSnapshot
{
    [NonSerialized]
    private int[]? _distinctOwners;

    [Id(0)]
    public int FormatVersion { get; private set; }

    [Id(1)]
    public string ProviderName { get; private set; } = string.Empty;

    [Id(2)]
    public int InitialPartitionCount { get; private set; }

    [Id(3)]
    public int VirtualSlotCount { get; private set; }

    [Id(4)]
    public long Epoch { get; private set; }

    [Id(5)]
    private int[] SlotAssignments { get; set; } = [];

    public int GetOwner(int slot)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(slot);
        if (slot >= SlotAssignments.Length)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slot),
                slot,
                "A slot must be within the routing layout.");
        }

        return SlotAssignments[slot];
    }

    public int[] CopySlotAssignments()
    {
        return [.. SlotAssignments];
    }

    public int[] GetDistinctOwners()
    {
        return [.. GetOrCreateDistinctOwners()];
    }

    public bool ContainsOwner(int owner)
    {
        return Array.BinarySearch(GetOrCreateDistinctOwners(), owner) >= 0;
    }

    internal static StorageLayoutSnapshot FromState(StorageLayoutState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(state.SlotAssignments);
        return new StorageLayoutSnapshot
        {
            FormatVersion = state.FormatVersion,
            ProviderName = state.ProviderName,
            InitialPartitionCount = state.PartitionCount,
            VirtualSlotCount = state.VirtualSlotCount,
            Epoch = state.Epoch,
            SlotAssignments = [.. state.SlotAssignments],
        };
    }

    private int[] GetOrCreateDistinctOwners()
    {
        var owners = Volatile.Read(ref _distinctOwners);
        if (owners is not null)
        {
            return owners;
        }

        var candidate = SlotAssignments.Distinct().Order().ToArray();
        return Interlocked.CompareExchange(ref _distinctOwners, candidate, comparand: null)
            ?? candidate;
    }
}

[GenerateSerializer]
internal sealed class StorageLayoutState
{
    [Id(0)]
    public bool Initialized { get; set; }

    [Id(1)]
    public int FormatVersion { get; set; }

    [Id(2)]
    public string ProviderName { get; set; } = string.Empty;

    [Id(3)]
    public int PartitionCount { get; set; }

    [Id(4)]
    public int JournalSegmentCapacity { get; set; }

    [Id(5)]
    public int MaximumJournalReplayEntries { get; set; }

    [Id(6)]
    public int VirtualSlotCount { get; set; }

    [Id(7)]
    public int[] SlotAssignments { get; set; } = [];

    [Id(8)]
    public long Epoch { get; set; }
}
