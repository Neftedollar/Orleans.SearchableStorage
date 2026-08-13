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
        AssertExactFieldIds<StoredRecord>(
            (nameof(StoredRecord.GrainId), 0),
            (nameof(StoredRecord.Payload), 1),
            (nameof(StoredRecord.ETag), 2),
            (nameof(StoredRecord.IndexEntries), 3),
            (nameof(StoredRecord.IndexSchemaFingerprint), 4));
        AssertExactFieldIds<StorageWriteRequest>(
            (nameof(StorageWriteRequest.RecordKey), 0),
            (nameof(StorageWriteRequest.GrainId), 1),
            (nameof(StorageWriteRequest.Payload), 2),
            (nameof(StorageWriteRequest.ExpectedETag), 3),
            (nameof(StorageWriteRequest.IndexEntries), 4),
            (nameof(StorageWriteRequest.Persistence), 5),
            (nameof(StorageWriteRequest.IndexSchemaFingerprint), 6),
            (nameof(StorageWriteRequest.StateName), 7),
            (nameof(StorageWriteRequest.IndexSchemaProtocolVersion), 8),
            (nameof(StorageWriteRequest.Unconditional), 9));
        AssertExactFieldIds<StorageClearRequest>(
            (nameof(StorageClearRequest.RecordKey), 0),
            (nameof(StorageClearRequest.ExpectedETag), 1),
            (nameof(StorageClearRequest.Persistence), 2),
            (nameof(StorageClearRequest.StateName), 3),
            (nameof(StorageClearRequest.IndexSchemaFingerprint), 4),
            (nameof(StorageClearRequest.IndexSchemaProtocolVersion), 5),
            (nameof(StorageClearRequest.Unconditional), 6));
        AssertExactFieldIds<StoragePersistenceSettings>(
            (nameof(StoragePersistenceSettings.JournalSegmentCapacity), 0),
            (nameof(StoragePersistenceSettings.MaximumJournalReplayEntries), 1),
            (nameof(StoragePersistenceSettings.CompactionThreshold), 2),
            (nameof(StoragePersistenceSettings.NamespaceMode), 3));
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
            (nameof(StoragePartitionManifestState.MoveControl), 17),
            (nameof(StoragePartitionManifestState.IndexSchemaProtocolVersion), 18),
            (nameof(StoragePartitionManifestState.NamespaceMode), 19));
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
            (nameof(StorageMoveStoredRecord.IndexEntries), 4),
            (nameof(StorageMoveStoredRecord.IndexSchemaFingerprint), 5));
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
    public void ManagedIndexSchemaMessagesKeepExactContiguousFieldIds()
    {
        AssertExactFieldIds<StorageIndexSchemaRequest>(
            (nameof(StorageIndexSchemaRequest.ProviderName), 0),
            (nameof(StorageIndexSchemaRequest.StateName), 1),
            (nameof(StorageIndexSchemaRequest.SchemaKey), 2),
            (nameof(StorageIndexSchemaRequest.Fingerprint), 3),
            (nameof(StorageIndexSchemaRequest.ProtocolVersion), 4));
        AssertExactFieldIds<StorageIndexSchemaCommand>(
            (nameof(StorageIndexSchemaCommand.Schema), 0),
            (nameof(StorageIndexSchemaCommand.RebuildId), 1));
        AssertExactFieldIds<StorageIndexSchemaSnapshot>(
            (nameof(StorageIndexSchemaSnapshot.ProviderName), 0),
            (nameof(StorageIndexSchemaSnapshot.StateName), 1),
            (nameof(StorageIndexSchemaSnapshot.ActiveFingerprint), 2),
            (nameof(StorageIndexSchemaSnapshot.Rebuild), 3),
            (nameof(StorageIndexSchemaSnapshot.LastCompletedRecordCount), 4));
        AssertExactFieldIds<StorageIndexSchemaState>(
            (nameof(StorageIndexSchemaState.Initialized), 0),
            (nameof(StorageIndexSchemaState.ProtocolVersion), 1),
            (nameof(StorageIndexSchemaState.ProviderName), 2),
            (nameof(StorageIndexSchemaState.StateName), 3),
            (nameof(StorageIndexSchemaState.ActiveFingerprint), 4),
            (nameof(StorageIndexSchemaState.Rebuild), 5),
            (nameof(StorageIndexSchemaState.LastCompletedRecordCount), 6));
        AssertExactFieldIds<StorageIndexSchemaRebuildIntent>(
            (nameof(StorageIndexSchemaRebuildIntent.RebuildId), 0),
            (nameof(StorageIndexSchemaRebuildIntent.SchemaKey), 1),
            (nameof(StorageIndexSchemaRebuildIntent.TargetFingerprint), 2),
            (nameof(StorageIndexSchemaRebuildIntent.LayoutEpoch), 3),
            (nameof(StorageIndexSchemaRebuildIntent.LayoutFingerprint), 4),
            (nameof(StorageIndexSchemaRebuildIntent.OwnerCount), 5),
            (nameof(StorageIndexSchemaRebuildIntent.NextProtocolOwnerIndex), 6),
            (nameof(StorageIndexSchemaRebuildIntent.LayoutProtocolPublished), 7),
            (nameof(StorageIndexSchemaRebuildIntent.NextOwnerIndex), 8),
            (nameof(StorageIndexSchemaRebuildIntent.HasAfter), 9),
            (nameof(StorageIndexSchemaRebuildIntent.After), 10),
            (nameof(StorageIndexSchemaRebuildIntent.ProcessedRecordCount), 11));
        AssertExactFieldIds<StorageIndexSchemaRebuildPageRequest>(
            (nameof(StorageIndexSchemaRebuildPageRequest.ProviderName), 0),
            (nameof(StorageIndexSchemaRebuildPageRequest.StateName), 1),
            (nameof(StorageIndexSchemaRebuildPageRequest.SchemaKey), 2),
            (nameof(StorageIndexSchemaRebuildPageRequest.TargetFingerprint), 3),
            (nameof(StorageIndexSchemaRebuildPageRequest.LayoutEpoch), 4),
            (nameof(StorageIndexSchemaRebuildPageRequest.HasAfter), 5),
            (nameof(StorageIndexSchemaRebuildPageRequest.After), 6),
            (nameof(StorageIndexSchemaRebuildPageRequest.PageSize), 7),
            (nameof(StorageIndexSchemaRebuildPageRequest.Persistence), 8));
        AssertExactFieldIds<StorageIndexSchemaRebuildPageResult>(
            (nameof(StorageIndexSchemaRebuildPageResult.Exhausted), 0),
            (nameof(StorageIndexSchemaRebuildPageResult.HasAfter), 1),
            (nameof(StorageIndexSchemaRebuildPageResult.After), 2),
            (nameof(StorageIndexSchemaRebuildPageResult.ProcessedRecordCount), 3));
        AssertExactFieldIds<StorageIndexSchemaPartitionProtocolRequest>(
            (nameof(StorageIndexSchemaPartitionProtocolRequest.ProtocolVersion), 0),
            (nameof(StorageIndexSchemaPartitionProtocolRequest.ProviderName), 1),
            (nameof(StorageIndexSchemaPartitionProtocolRequest.LayoutEpoch), 2),
            (nameof(StorageIndexSchemaPartitionProtocolRequest.LayoutFingerprint), 3),
            (nameof(StorageIndexSchemaPartitionProtocolRequest.Persistence), 4));
        AssertExactFieldIds<StorageIndexSchemaLayoutProtocolRequest>(
            (nameof(StorageIndexSchemaLayoutProtocolRequest.ProtocolVersion), 0),
            (nameof(StorageIndexSchemaLayoutProtocolRequest.LayoutEpoch), 1),
            (nameof(StorageIndexSchemaLayoutProtocolRequest.LayoutFingerprint), 2),
            (nameof(StorageIndexSchemaLayoutProtocolRequest.EnablementId), 3));

        StorageIndexSchema.ProtocolVersion.Should().Be(1);
        StorageLayout.LegacyFormatVersion.Should().Be(3);
        StorageLayout.MovementFormatVersion.Should().Be(4);
        StorageLayout.IndexSchemaFormatVersion.Should().Be(5);
        StorageLayout.AreRoutingFormatsCompatible(4, 5).Should().BeTrue();
        StorageLayout.AreRoutingFormatsCompatible(3, 3).Should().BeFalse();
        StorageLayout.AreRoutingFormatsCompatible(6, 6).Should().BeTrue();
        StorageLayout.AreRoutingFormatsCompatible(5, 6).Should().BeFalse();
    }

    [Fact]
    public void PartitionMovementMessagesKeepExactContiguousFieldIds()
    {
        AssertExactFieldIds<StoragePartitionProtocolRequest>(
            (nameof(StoragePartitionProtocolRequest.ProtocolVersion), 0),
            (nameof(StoragePartitionProtocolRequest.VirtualSlotCount), 1),
            (nameof(StoragePartitionProtocolRequest.MinimumRoutingEpoch), 2),
            (nameof(StoragePartitionProtocolRequest.JournalSegmentCapacity), 3),
            (nameof(StoragePartitionProtocolRequest.MaximumJournalReplayEntries), 4),
            (nameof(StoragePartitionProtocolRequest.IndexSchemaProtocolVersion), 5),
            (nameof(StoragePartitionProtocolRequest.NamespaceMode), 6));
        AssertExactFieldIds<StoragePartitionProtocolState>(
            (nameof(StoragePartitionProtocolState.PersistenceFormatVersion), 0),
            (nameof(StoragePartitionProtocolState.MovementProtocolVersion), 1),
            (nameof(StoragePartitionProtocolState.RoutedOperationsRequired), 2),
            (nameof(StoragePartitionProtocolState.MinimumRoutingEpoch), 3),
            (nameof(StoragePartitionProtocolState.CommittedSequence), 4),
            (nameof(StoragePartitionProtocolState.NextVersion), 5),
            (nameof(StoragePartitionProtocolState.MoveControl), 6),
            (nameof(StoragePartitionProtocolState.IndexSchemaProtocolVersion), 7),
            (nameof(StoragePartitionProtocolState.NamespaceMode), 8));
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
        AssertEnumValues<StorageJournalOperation>(0, 1, 2, 3, 4, 5);
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
