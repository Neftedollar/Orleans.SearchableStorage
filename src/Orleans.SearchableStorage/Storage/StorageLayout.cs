using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal enum StorageNamespaceMode
{
    Integrated = 0,
    IndexOnly = 1,
}

internal static class StorageLayout
{
    public const int LegacyFormatVersion = 3;
    public const int MovementFormatVersion = 4;
    public const int IndexSchemaFormatVersion = 5;
    public const int IndexOnlyFormatVersion = 6;
    public const int CurrentMovementProtocolVersion = 1;
    public const int CurrentIndexSchemaProtocolVersion = 1;
    public const int DefaultVirtualSlotTargetCount = 16_384;
    public const int MaximumVirtualSlotCount = 262_144;

    /// <summary>
    /// Version 4 is the movement-capable baseline. Version 5 has the same routing semantics and
    /// adds the durable, fail-closed managed-schema capability fence.
    /// </summary>
    public static bool IsRoutingFormatVersion(int formatVersion)
    {
        return formatVersion is MovementFormatVersion
            or IndexSchemaFormatVersion
            or IndexOnlyFormatVersion;
    }

    public static bool AreRoutingFormatsCompatible(int left, int right)
    {
        if (!IsRoutingFormatVersion(left) || !IsRoutingFormatVersion(right))
        {
            return false;
        }

        // Versions 4 and 5 are the same integrated-storage routing domain. Version 6 is a
        // deliberately separate downgrade fence for payload-free index namespaces.
        return (left == IndexOnlyFormatVersion) == (right == IndexOnlyFormatVersion);
    }

    public static int GetRoutingFingerprintFormatVersion(int formatVersion)
    {
        if (!IsRoutingFormatVersion(formatVersion))
        {
            throw new ArgumentOutOfRangeException(
                nameof(formatVersion),
                formatVersion,
                "A routing-capable layout format version is required.");
        }

        // The v5 transition changes durable capability, not assignments or routing identity.
        // Version 6 remains a distinct routing domain so its tokens cannot be replayed against an
        // otherwise identical integrated namespace.
        return formatVersion == IndexOnlyFormatVersion
            ? IndexOnlyFormatVersion
            : MovementFormatVersion;
    }

    public static StorageLayoutDescriptor CreateDescriptor(
        string providerName,
        int partitionCount,
        int journalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
        int maximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
        int virtualSlotTargetCount = DefaultVirtualSlotTargetCount,
        StorageNamespaceMode namespaceMode = StorageNamespaceMode.Integrated)
    {
        return new StorageLayoutDescriptor
        {
            ProviderName = providerName,
            PartitionCount = partitionCount,
            JournalSegmentCapacity = journalSegmentCapacity,
            MaximumJournalReplayEntries = maximumJournalReplayEntries,
            VirtualSlotTargetCount = virtualSlotTargetCount,
            NamespaceMode = namespaceMode,
            FormatVersion = namespaceMode == StorageNamespaceMode.IndexOnly
                ? IndexOnlyFormatVersion
                : MovementFormatVersion,
        };
    }

    public static StorageLayoutIdentity CreateIdentity(
        string providerName,
        int partitionCount,
        StorageNamespaceMode namespaceMode = StorageNamespaceMode.Integrated)
    {
        return new StorageLayoutIdentity
        {
            FormatVersion = namespaceMode == StorageNamespaceMode.IndexOnly
                ? IndexOnlyFormatVersion
                : MovementFormatVersion,
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
        ValidateOwnerIndex(partitionIndex, nameof(partitionIndex));
        return $"{providerName}:{partitionIndex:D8}";
    }

    public static void ValidateOwnerIndex(int owner, string parameterName)
    {
        if (owner < 0 || owner >= MaximumVirtualSlotCount)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                owner,
                $"A physical partition index must be between 0 and {MaximumVirtualSlotCount - 1}.");
        }
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

    /// <summary>
    /// Permanently distinguishes a payload-owning namespace from an index-only namespace.
    /// </summary>
    [Id(6)]
    public StorageNamespaceMode NamespaceMode { get; init; }
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

    [Id(6)]
    public int MovementProtocolVersion { get; private set; }

    [Id(7)]
    private StorageMovementEnableIntent? MovementEnablement { get; set; }

    [Id(8)]
    private StorageSlotMoveIntent? MoveIntent { get; set; }

    [Id(9)]
    private StorageSlotMoveReceipt? LastMoveReceipt { get; set; }

    [Id(10)]
    public int IndexSchemaProtocolVersion { get; private set; }

    [Id(11)]
    private StorageIndexSchemaEnableIntent? IndexSchemaEnablement { get; set; }

    [Id(12)]
    public StorageNamespaceMode NamespaceMode { get; private set; }

    public SearchableStorageMovementState MovementState => MovementEnablement is not null
        ? SearchableStorageMovementState.Enabling
        : MovementProtocolVersion == StorageLayout.CurrentMovementProtocolVersion
            ? SearchableStorageMovementState.Enabled
            : SearchableStorageMovementState.Disabled;

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

    public StorageMovementEnableIntent? CopyMovementEnablement()
    {
        return MovementEnablement?.Copy();
    }

    public StorageSlotMoveIntent? CopyMoveIntent()
    {
        return MoveIntent?.Copy();
    }

    public StorageSlotMoveReceipt? CopyLastMoveReceipt()
    {
        return LastMoveReceipt?.Copy();
    }

    public StorageIndexSchemaEnableIntent? CopyIndexSchemaEnablement()
    {
        return IndexSchemaEnablement?.Copy();
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
            MovementProtocolVersion = state.MovementProtocolVersion,
            MovementEnablement = state.MovementEnablement?.Copy(),
            MoveIntent = state.MoveIntent?.Copy(),
            LastMoveReceipt = state.LastMoveReceipt?.Copy(),
            IndexSchemaProtocolVersion = state.IndexSchemaProtocolVersion,
            IndexSchemaEnablement = state.IndexSchemaEnablement?.Copy(),
            NamespaceMode = state.NamespaceMode,
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

    [Id(9)]
    public int MovementProtocolVersion { get; set; }

    [Id(10)]
    public StorageMovementEnableIntent? MovementEnablement { get; set; }

    [Id(11)]
    public StorageSlotMoveIntent? MoveIntent { get; set; }

    [Id(12)]
    public StorageSlotMoveReceipt? LastMoveReceipt { get; set; }

    [Id(13)]
    public int IndexSchemaProtocolVersion { get; set; }

    [Id(14)]
    public StorageIndexSchemaEnableIntent? IndexSchemaEnablement { get; set; }

    /// <summary>
    /// Identifies whether this namespace owns serialized application payloads or only indexes.
    /// The default preserves every layout written before index-only namespaces existed.
    /// </summary>
    [Id(15)]
    public StorageNamespaceMode NamespaceMode { get; set; }

    public StorageLayoutState Copy()
    {
        return new StorageLayoutState
        {
            Initialized = Initialized,
            FormatVersion = FormatVersion,
            ProviderName = ProviderName,
            PartitionCount = PartitionCount,
            JournalSegmentCapacity = JournalSegmentCapacity,
            MaximumJournalReplayEntries = MaximumJournalReplayEntries,
            VirtualSlotCount = VirtualSlotCount,
            SlotAssignments = [.. SlotAssignments],
            Epoch = Epoch,
            MovementProtocolVersion = MovementProtocolVersion,
            MovementEnablement = MovementEnablement?.Copy(),
            MoveIntent = MoveIntent?.Copy(),
            LastMoveReceipt = LastMoveReceipt?.Copy(),
            IndexSchemaProtocolVersion = IndexSchemaProtocolVersion,
            IndexSchemaEnablement = IndexSchemaEnablement?.Copy(),
            NamespaceMode = NamespaceMode,
        };
    }
}

/// <summary>
/// Durably excludes movement while owners are enabled, records are scanned, and the provider
/// capability is published. The following state-control activation remains covered by the
/// documented traffic-quiescence precondition.
/// </summary>
[GenerateSerializer]
internal sealed class StorageIndexSchemaEnableIntent
{
    [Id(0)]
    public Guid EnablementId { get; set; }

    [Id(1)]
    public int ProtocolVersion { get; set; }

    [Id(2)]
    public long LayoutEpoch { get; set; }

    [Id(3)]
    public byte[] LayoutFingerprint { get; set; } = [];

    public StorageIndexSchemaEnableIntent Copy()
    {
        return new StorageIndexSchemaEnableIntent
        {
            EnablementId = EnablementId,
            ProtocolVersion = ProtocolVersion,
            LayoutEpoch = LayoutEpoch,
            LayoutFingerprint = [.. LayoutFingerprint],
        };
    }
}

/// <summary>
/// Persists resumable progress while every current owner is upgraded and pre-fenced under the
/// operator-enforced quiescence gate.
/// </summary>
[GenerateSerializer]
internal sealed class StorageMovementEnableIntent
{
    [Id(0)]
    public Guid EnablementId { get; set; }

    [Id(1)]
    public long SourceEpoch { get; set; }

    [Id(2)]
    public long PlannedEpoch { get; set; }

    [Id(3)]
    public int[] Owners { get; set; } = [];

    [Id(4)]
    public int NextOwnerIndex { get; set; }

    public StorageMovementEnableIntent Copy()
    {
        return new StorageMovementEnableIntent
        {
            EnablementId = EnablementId,
            SourceEpoch = SourceEpoch,
            PlannedEpoch = PlannedEpoch,
            Owners = [.. Owners],
            NextOwnerIndex = NextOwnerIndex,
        };
    }
}

/// <summary>
/// Persists the provider's sole slot-move intent. Partition controls own detailed page cursors and
/// replay identities; the layout owns phase and the atomic assignment commit.
/// </summary>
[GenerateSerializer]
internal sealed class StorageSlotMoveIntent
{
    [Id(0)]
    public Guid MoveId { get; set; }

    [Id(1)]
    public int Slot { get; set; }

    [Id(2)]
    public int SourceOwner { get; set; }

    [Id(3)]
    public int TargetOwner { get; set; }

    [Id(4)]
    public long SourceEpoch { get; set; }

    [Id(5)]
    public SearchableStorageSlotMovePhase Phase { get; set; }

    [Id(6)]
    public int TransferPageRecordLimit { get; set; }

    [Id(7)]
    public int TransferPageByteTarget { get; set; }

    [Id(8)]
    public long ExportedRecordCount { get; set; }

    [Id(9)]
    public long ExportedByteCount { get; set; }

    [Id(10)]
    public long DeletedRecordCount { get; set; }

    [Id(11)]
    public long DeletedByteCount { get; set; }

    public StorageSlotMoveIntent Copy()
    {
        return new StorageSlotMoveIntent
        {
            MoveId = MoveId,
            Slot = Slot,
            SourceOwner = SourceOwner,
            TargetOwner = TargetOwner,
            SourceEpoch = SourceEpoch,
            Phase = Phase,
            TransferPageRecordLimit = TransferPageRecordLimit,
            TransferPageByteTarget = TransferPageByteTarget,
            ExportedRecordCount = ExportedRecordCount,
            ExportedByteCount = ExportedByteCount,
            DeletedRecordCount = DeletedRecordCount,
            DeletedByteCount = DeletedByteCount,
        };
    }
}

/// <summary>
/// Retains one constant-size terminal receipt so a final lost acknowledgement can be retried by
/// move id after the active intent and all participant controls have been cleared.
/// </summary>
[GenerateSerializer]
internal sealed class StorageSlotMoveReceipt
{
    [Id(0)]
    public Guid MoveId { get; set; }

    [Id(1)]
    public int Slot { get; set; }

    [Id(2)]
    public int SourceOwner { get; set; }

    [Id(3)]
    public int TargetOwner { get; set; }

    [Id(4)]
    public long SourceEpoch { get; set; }

    [Id(5)]
    public long CompletionEpoch { get; set; }

    [Id(6)]
    public SearchableStorageSlotMovePhase TerminalPhase { get; set; }

    [Id(7)]
    public long ExportedRecordCount { get; set; }

    [Id(8)]
    public long ExportedByteCount { get; set; }

    [Id(9)]
    public long DeletedRecordCount { get; set; }

    [Id(10)]
    public long DeletedByteCount { get; set; }

    public StorageSlotMoveReceipt Copy()
    {
        return new StorageSlotMoveReceipt
        {
            MoveId = MoveId,
            Slot = Slot,
            SourceOwner = SourceOwner,
            TargetOwner = TargetOwner,
            SourceEpoch = SourceEpoch,
            CompletionEpoch = CompletionEpoch,
            TerminalPhase = TerminalPhase,
            ExportedRecordCount = ExportedRecordCount,
            ExportedByteCount = ExportedByteCount,
            DeletedRecordCount = DeletedRecordCount,
            DeletedByteCount = DeletedByteCount,
        };
    }
}

/// <summary>
/// Supplies immutable, validated transfer limits when planning one move.
/// </summary>
[GenerateSerializer]
internal sealed class StorageSlotMovePlanRequest
{
    [Id(0)]
    public int Slot { get; init; }

    [Id(1)]
    public int TargetOwner { get; init; }

    [Id(2)]
    public int MovementProtocolVersion { get; init; }

    [Id(3)]
    public int TransferPageRecordLimit { get; init; }

    [Id(4)]
    public int TransferPageByteTarget { get; init; }
}

/// <summary>
/// Binds a bounded advance or abort request to one stable move identity.
/// </summary>
[GenerateSerializer]
internal sealed class StorageSlotMoveCommand
{
    [Id(0)]
    public Guid MoveId { get; init; }

    [Id(1)]
    public int MovementProtocolVersion { get; init; }
}

/// <summary>
/// Returns layout-owned move state together with durable participant counters.
/// </summary>
[GenerateSerializer]
internal sealed class StorageSlotMoveProgressSnapshot
{
    [Id(0)]
    public required StorageSlotMoveIntent Intent { get; init; }

    [Id(1)]
    public long CurrentEpoch { get; init; }

    [Id(2)]
    public long ExportedRecordCount { get; init; }

    [Id(3)]
    public long ExportedByteCount { get; init; }

    [Id(4)]
    public long DeletedRecordCount { get; init; }

    [Id(5)]
    public long DeletedByteCount { get; init; }
}
