namespace Orleans.SearchableStorage.Storage;

[GenerateSerializer]
internal sealed class StoragePartitionProtocolRequest
{
    [Id(0)] public int ProtocolVersion { get; init; }
    [Id(1)] public int VirtualSlotCount { get; init; }
    [Id(2)] public long MinimumRoutingEpoch { get; init; }
    [Id(3)] public int JournalSegmentCapacity { get; init; }
    [Id(4)] public int MaximumJournalReplayEntries { get; init; }
    [Id(5)] public int IndexSchemaProtocolVersion { get; init; }
}

[GenerateSerializer]
internal sealed class StoragePartitionProtocolState
{
    [Id(0)] public int PersistenceFormatVersion { get; init; }
    [Id(1)] public int MovementProtocolVersion { get; init; }
    [Id(2)] public bool RoutedOperationsRequired { get; init; }
    [Id(3)] public long MinimumRoutingEpoch { get; init; }
    [Id(4)] public long CommittedSequence { get; init; }
    [Id(5)] public long NextVersion { get; init; }
    [Id(6)] public required StoragePartitionMoveControl MoveControl { get; init; }
    [Id(7)] public int IndexSchemaProtocolVersion { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMoveIdentity
{
    [Id(0)] public int ProtocolVersion { get; init; }
    [Id(1)] public Guid MoveId { get; init; }
    [Id(2)] public int Slot { get; init; }
    [Id(3)] public int VirtualSlotCount { get; init; }
    [Id(4)] public long SourceEpoch { get; init; }
    [Id(5)] public int SourceOwner { get; init; }
    [Id(6)] public int TargetOwner { get; init; }

    public StorageMoveIdentity Copy()
    {
        return new StorageMoveIdentity
        {
            ProtocolVersion = ProtocolVersion,
            MoveId = MoveId,
            Slot = Slot,
            VirtualSlotCount = VirtualSlotCount,
            SourceEpoch = SourceEpoch,
            SourceOwner = SourceOwner,
            TargetOwner = TargetOwner,
        };
    }
}

[GenerateSerializer]
internal sealed class StorageMoveTargetPrepareRequest
{
    [Id(0)] public required StorageMoveIdentity Move { get; init; }
    [Id(1)] public long FrozenNextVersion { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMovePageRequest
{
    [Id(0)] public required StorageMoveIdentity Move { get; init; }
    [Id(1)] public long PageOrdinal { get; init; }
    [Id(2)] public byte[]? AfterRecordKey { get; init; }
    [Id(3)] public int ItemLimit { get; init; }
    [Id(4)] public int ByteTarget { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMoveExportPage
{
    [Id(0)] public required StorageMoveIdentity Move { get; init; }
    [Id(1)] public long PageOrdinal { get; init; }
    [Id(2)] public byte[]? AfterRecordKey { get; init; }
    [Id(3)] public byte[]? NextRecordKey { get; init; }
    [Id(4)] public bool Exhausted { get; init; }
    [Id(5)] public long EncodedByteCount { get; init; }
    [Id(6)] public required List<StorageMoveRecord> Records { get; init; }
    [Id(7)] public required byte[] PageDigest { get; init; }
    [Id(8)] public long FrozenNextVersion { get; init; }
    [Id(9)] public int ItemLimit { get; init; }
    [Id(10)] public int ByteTarget { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMoveImportPageRequest
{
    [Id(0)] public required StorageMoveExportPage Page { get; init; }
}

internal enum StorageMoveDeleteMode
{
    SourceCleanup = 1,
    TargetAbort = 2,
}

[GenerateSerializer]
internal sealed class StorageMoveDeletePageRequest
{
    [Id(0)] public required StorageMoveIdentity Move { get; init; }
    [Id(1)] public StorageMoveDeleteMode Mode { get; init; }
    [Id(2)] public long PageOrdinal { get; init; }
    [Id(3)] public byte[]? AfterRecordKey { get; init; }
    [Id(4)] public int ItemLimit { get; init; }
    [Id(5)] public int ByteTarget { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMoveVisibilityFenceRequest
{
    [Id(0)] public required StorageMoveIdentity Move { get; init; }
    [Id(1)] public long CommittedEpoch { get; init; }
}

internal enum StorageMoveRetirementKind
{
    Completed = 1,
    Aborted = 2,
}

[GenerateSerializer]
internal sealed class StorageMoveRetireRequest
{
    [Id(0)] public required StorageMoveIdentity Move { get; init; }
    [Id(1)] public StorageMoveRetirementKind Kind { get; init; }
}

[GenerateSerializer]
internal sealed class StorageMovePageCommitResult
{
    [Id(0)] public required StoragePartitionProtocolState State { get; init; }
    [Id(1)] public long PageOrdinal { get; init; }
    [Id(2)] public byte[]? AfterRecordKey { get; init; }
    [Id(3)] public bool Exhausted { get; init; }
    [Id(4)] public required byte[] PageDigest { get; init; }
    [Id(5)] public long EncodedByteCount { get; init; }
}
