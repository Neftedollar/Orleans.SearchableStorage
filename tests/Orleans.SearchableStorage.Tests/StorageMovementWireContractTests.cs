using System.Globalization;
using System.Reflection;
using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageMovementWireContractTests
{
    [Fact]
    public void DurableManifestAndWalFieldsAreExactlyContiguousAndAppendOnly()
    {
        AssertExactFieldIds<StoragePartitionManifestState>(
            (nameof(StoragePartitionManifestState.Initialized), 0),
            (nameof(StoragePartitionManifestState.PersistenceFormatVersion), 1),
            (nameof(StoragePartitionManifestState.JournalSegmentCapacity), 2),
            (nameof(StoragePartitionManifestState.MaximumJournalReplayEntries), 3),
            (nameof(StoragePartitionManifestState.WriterEpoch), 4),
            (nameof(StoragePartitionManifestState.CommittedSequence), 5),
            (nameof(StoragePartitionManifestState.CommittedOperationId), 6),
            (nameof(StoragePartitionManifestState.NextVersion), 7),
            (nameof(StoragePartitionManifestState.ActiveSnapshot), 8),
            (nameof(StoragePartitionManifestState.PendingSnapshot), 9),
            (nameof(StoragePartitionManifestState.RetiringSnapshot), 10),
            (nameof(StoragePartitionManifestState.SnapshotGenerationHighWatermark), 11),
            (nameof(StoragePartitionManifestState.SnapshotSequence), 12),
            (nameof(StoragePartitionManifestState.PrunedSequence), 13),
            (nameof(StoragePartitionManifestState.MovementProtocolVersion), 14),
            (nameof(StoragePartitionManifestState.RoutedOperationsRequired), 15),
            (nameof(StoragePartitionManifestState.MinimumRoutingEpoch), 16),
            (nameof(StoragePartitionManifestState.MoveControl), 17));
        AssertExactFieldIds<StoragePartitionMoveControl>(
            (nameof(StoragePartitionMoveControl.IsPresent), 0),
            (nameof(StoragePartitionMoveControl.MoveId), 1),
            (nameof(StoragePartitionMoveControl.Slot), 2),
            (nameof(StoragePartitionMoveControl.VirtualSlotCount), 3),
            (nameof(StoragePartitionMoveControl.SourceEpoch), 4),
            (nameof(StoragePartitionMoveControl.SourceOwner), 5),
            (nameof(StoragePartitionMoveControl.TargetOwner), 6),
            (nameof(StoragePartitionMoveControl.Role), 7),
            (nameof(StoragePartitionMoveControl.Phase), 8),
            (nameof(StoragePartitionMoveControl.FrozenNextVersion), 9),
            (nameof(StoragePartitionMoveControl.ProgressAfterRecordKey), 10),
            (nameof(StoragePartitionMoveControl.NextPageOrdinal), 11),
            (nameof(StoragePartitionMoveControl.LastPageDigest), 12),
            (nameof(StoragePartitionMoveControl.ImportedRecordCount), 13),
            (nameof(StoragePartitionMoveControl.ImportedByteCount), 14),
            (nameof(StoragePartitionMoveControl.DeletedRecordCount), 15),
            (nameof(StoragePartitionMoveControl.DeletedByteCount), 16),
            (nameof(StoragePartitionMoveControl.LastPageRequestAfterRecordKey), 17),
            (nameof(StoragePartitionMoveControl.LastPageItemLimit), 18),
            (nameof(StoragePartitionMoveControl.LastPageByteTarget), 19),
            (nameof(StoragePartitionMoveControl.LastPageEncodedByteCount), 20));
        AssertExactFieldIds<StorageJournalEntry>(
            (nameof(StorageJournalEntry.Sequence), 0),
            (nameof(StorageJournalEntry.WriterEpoch), 1),
            (nameof(StorageJournalEntry.OperationId), 2),
            (nameof(StorageJournalEntry.PreviousOperationId), 3),
            (nameof(StorageJournalEntry.Operation), 4),
            (nameof(StorageJournalEntry.RecordKey), 5),
            (nameof(StorageJournalEntry.ExpectedETag), 6),
            (nameof(StorageJournalEntry.Record), 7),
            (nameof(StorageJournalEntry.NextVersionAfter), 8),
            (nameof(StorageJournalEntry.Move), 9));
        AssertExactFieldIds<StorageMoveJournalPayload>(
            (nameof(StorageMoveJournalPayload.MoveId), 0),
            (nameof(StorageMoveJournalPayload.Slot), 1),
            (nameof(StorageMoveJournalPayload.VirtualSlotCount), 2),
            (nameof(StorageMoveJournalPayload.SourceEpoch), 3),
            (nameof(StorageMoveJournalPayload.SourceOwner), 4),
            (nameof(StorageMoveJournalPayload.TargetOwner), 5),
            (nameof(StorageMoveJournalPayload.PageOrdinal), 6),
            (nameof(StorageMoveJournalPayload.AfterRecordKey), 7),
            (nameof(StorageMoveJournalPayload.NextRecordKey), 8),
            (nameof(StorageMoveJournalPayload.Exhausted), 9),
            (nameof(StorageMoveJournalPayload.PageDigest), 10),
            (nameof(StorageMoveJournalPayload.FrozenNextVersion), 11),
            (nameof(StorageMoveJournalPayload.Imports), 12),
            (nameof(StorageMoveJournalPayload.Deletes), 13),
            (nameof(StorageMoveJournalPayload.ItemLimit), 14),
            (nameof(StorageMoveJournalPayload.ByteTarget), 15),
            (nameof(StorageMoveJournalPayload.EncodedByteCount), 16));
        AssertExactFieldIds<StorageMoveRecord>(
            (nameof(StorageMoveRecord.RecordKey), 0),
            (nameof(StorageMoveRecord.Record), 1));
        AssertExactFieldIds<StorageMoveStoredRecord>(
            (nameof(StorageMoveStoredRecord.GrainType), 0),
            (nameof(StorageMoveStoredRecord.GrainKey), 1),
            (nameof(StorageMoveStoredRecord.Payload), 2),
            (nameof(StorageMoveStoredRecord.ETag), 3),
            (nameof(StorageMoveStoredRecord.IndexEntries), 4));
        AssertExactFieldIds<StorageMoveIndexEntry>(
            (nameof(StorageMoveIndexEntry.Scope), 0),
            (nameof(StorageMoveIndexEntry.Kind), 1),
            (nameof(StorageMoveIndexEntry.Value), 2));
        AssertExactFieldIds<StorageMoveIndexValue>(
            (nameof(StorageMoveIndexValue.Kind), 0),
            (nameof(StorageMoveIndexValue.Text), 1),
            (nameof(StorageMoveIndexValue.PrimitiveBits), 2));
        AssertExactFieldIds<StorageMoveDeleteRecord>(
            (nameof(StorageMoveDeleteRecord.RecordKey), 0),
            (nameof(StorageMoveDeleteRecord.ExpectedETag), 1));
        AssertExactFieldIds<StorageSnapshotState>(
            (nameof(StorageSnapshotState.Initialized), 0),
            (nameof(StorageSnapshotState.Tombstoned), 1),
            (nameof(StorageSnapshotState.Slot), 2),
            (nameof(StorageSnapshotState.Generation), 3),
            (nameof(StorageSnapshotState.SnapshotId), 4),
            (nameof(StorageSnapshotState.Sequence), 5),
            (nameof(StorageSnapshotState.OperationId), 6),
            (nameof(StorageSnapshotState.NextVersion), 7),
            (nameof(StorageSnapshotState.Records), 8),
            (nameof(StorageSnapshotState.RecordEncodingVersion), 9),
            (nameof(StorageSnapshotState.LosslessRecords), 10));
    }

    [Fact]
    public void PartitionMovementMessagesKeepExactContiguousFieldIds()
    {
        AssertExactFieldIds<StoragePartitionProtocolRequest>(
            (nameof(StoragePartitionProtocolRequest.ProtocolVersion), 0),
            (nameof(StoragePartitionProtocolRequest.VirtualSlotCount), 1),
            (nameof(StoragePartitionProtocolRequest.MinimumRoutingEpoch), 2),
            (nameof(StoragePartitionProtocolRequest.JournalSegmentCapacity), 3),
            (nameof(StoragePartitionProtocolRequest.MaximumJournalReplayEntries), 4));
        AssertExactFieldIds<StoragePartitionProtocolState>(
            (nameof(StoragePartitionProtocolState.PersistenceFormatVersion), 0),
            (nameof(StoragePartitionProtocolState.MovementProtocolVersion), 1),
            (nameof(StoragePartitionProtocolState.RoutedOperationsRequired), 2),
            (nameof(StoragePartitionProtocolState.MinimumRoutingEpoch), 3),
            (nameof(StoragePartitionProtocolState.CommittedSequence), 4),
            (nameof(StoragePartitionProtocolState.NextVersion), 5),
            (nameof(StoragePartitionProtocolState.MoveControl), 6));
        AssertExactFieldIds<StorageMoveIdentity>(
            (nameof(StorageMoveIdentity.ProtocolVersion), 0),
            (nameof(StorageMoveIdentity.MoveId), 1),
            (nameof(StorageMoveIdentity.Slot), 2),
            (nameof(StorageMoveIdentity.VirtualSlotCount), 3),
            (nameof(StorageMoveIdentity.SourceEpoch), 4),
            (nameof(StorageMoveIdentity.SourceOwner), 5),
            (nameof(StorageMoveIdentity.TargetOwner), 6));
        AssertExactFieldIds<StorageMoveTargetPrepareRequest>(
            (nameof(StorageMoveTargetPrepareRequest.Move), 0),
            (nameof(StorageMoveTargetPrepareRequest.FrozenNextVersion), 1));
        AssertExactFieldIds<StorageMovePageRequest>(
            (nameof(StorageMovePageRequest.Move), 0),
            (nameof(StorageMovePageRequest.PageOrdinal), 1),
            (nameof(StorageMovePageRequest.AfterRecordKey), 2),
            (nameof(StorageMovePageRequest.ItemLimit), 3),
            (nameof(StorageMovePageRequest.ByteTarget), 4));
        AssertExactFieldIds<StorageMoveExportPage>(
            (nameof(StorageMoveExportPage.Move), 0),
            (nameof(StorageMoveExportPage.PageOrdinal), 1),
            (nameof(StorageMoveExportPage.AfterRecordKey), 2),
            (nameof(StorageMoveExportPage.NextRecordKey), 3),
            (nameof(StorageMoveExportPage.Exhausted), 4),
            (nameof(StorageMoveExportPage.EncodedByteCount), 5),
            (nameof(StorageMoveExportPage.Records), 6),
            (nameof(StorageMoveExportPage.PageDigest), 7),
            (nameof(StorageMoveExportPage.FrozenNextVersion), 8),
            (nameof(StorageMoveExportPage.ItemLimit), 9),
            (nameof(StorageMoveExportPage.ByteTarget), 10));
        AssertExactFieldIds<StorageMoveImportPageRequest>(
            (nameof(StorageMoveImportPageRequest.Page), 0));
        AssertExactFieldIds<StorageMoveDeletePageRequest>(
            (nameof(StorageMoveDeletePageRequest.Move), 0),
            (nameof(StorageMoveDeletePageRequest.Mode), 1),
            (nameof(StorageMoveDeletePageRequest.PageOrdinal), 2),
            (nameof(StorageMoveDeletePageRequest.AfterRecordKey), 3),
            (nameof(StorageMoveDeletePageRequest.ItemLimit), 4),
            (nameof(StorageMoveDeletePageRequest.ByteTarget), 5));
        AssertExactFieldIds<StorageMoveVisibilityFenceRequest>(
            (nameof(StorageMoveVisibilityFenceRequest.Move), 0),
            (nameof(StorageMoveVisibilityFenceRequest.CommittedEpoch), 1));
        AssertExactFieldIds<StorageMoveRetireRequest>(
            (nameof(StorageMoveRetireRequest.Move), 0),
            (nameof(StorageMoveRetireRequest.Kind), 1));
        AssertExactFieldIds<StorageMovePageCommitResult>(
            (nameof(StorageMovePageCommitResult.State), 0),
            (nameof(StorageMovePageCommitResult.PageOrdinal), 1),
            (nameof(StorageMovePageCommitResult.AfterRecordKey), 2),
            (nameof(StorageMovePageCommitResult.Exhausted), 3),
            (nameof(StorageMovePageCommitResult.PageDigest), 4),
            (nameof(StorageMovePageCommitResult.EncodedByteCount), 5));
    }

    [Fact]
    public void AppendedMovementEnumValuesRemainFrozen()
    {
        AssertEnumValues<StorageJournalOperation>(0, 1, 2, 3, 4);
        AssertEnumValues<StoragePartitionMoveRole>(0, 1, 2);
        AssertEnumValues<StoragePartitionMovePhase>(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        AssertEnumValues<StorageMoveDeleteMode>(1, 2);
        AssertEnumValues<StorageMoveRetirementKind>(1, 2);
        AssertEnumValues<SearchableIndexKind>(0, 1);
        AssertEnumValues<IndexValueKind>(0, 1, 2, 3, 4, 5, 6, 7);
    }

    private static void AssertExactFieldIds<T>(params (string MemberName, uint Id)[] expected)
    {
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
        var actual = typeof(T).GetProperties(flags)
            .Select(static property => (
                property.Name,
                Attribute: property.GetCustomAttribute<IdAttribute>()))
            .Where(static item => item.Attribute is not null)
            .Select(static item => (item.Name, item.Attribute!.Id))
            .OrderBy(static item => item.Id)
            .ToArray();

        actual.Should().Equal(expected.Select(static item => (item.MemberName, item.Id)));
        actual.Select(static item => item.Id).Should().Equal(
            Enumerable.Range(0, expected.Length).Select(static value => (uint)value));
    }

    private static void AssertEnumValues<T>(params int[] expected)
        where T : struct, Enum
    {
        Enum.GetValues<T>()
            .Select(static value => Convert.ToInt32(value, CultureInfo.InvariantCulture))
            .Should()
            .Equal(expected);
    }
}
