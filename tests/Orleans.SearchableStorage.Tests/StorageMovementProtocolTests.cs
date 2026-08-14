using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageMovementProtocolTests
{
    [Fact]
    public void EncodedByteAccountingUsesLongAndRejectsOverflow()
    {
        StorageMovePageDigest.CheckedAddEncodedByteCount(int.MaxValue, int.MaxValue)
            .Should().Be(2L * int.MaxValue);

        Action overflow = () => StorageMovePageDigest.CheckedAddEncodedByteCount(long.MaxValue, 1);
        Action negativeCurrent = () => StorageMovePageDigest.CheckedAddEncodedByteCount(-1, 0);
        Action negativeItem = () => StorageMovePageDigest.CheckedAddEncodedByteCount(0, -1);

        overflow.Should().Throw<OverflowException>();
        negativeCurrent.Should().Throw<ArgumentOutOfRangeException>();
        negativeItem.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void CanonicalDigestPreservesInvalidUtf16CodeUnits()
    {
        var highSurrogate = new string((char)0xD800, 1);
        var lowSurrogate = new string((char)0xDC00, 1);
        var first = CreateDeletePayload(highSurrogate);
        var equal = first.Copy();
        var different = CreateDeletePayload(lowSurrogate);

        var firstDigest = StorageMovePageDigest.Compute(StorageJournalOperation.MoveDelete, first);
        var equalDigest = StorageMovePageDigest.Compute(StorageJournalOperation.MoveDelete, equal);
        var differentDigest = StorageMovePageDigest.Compute(StorageJournalOperation.MoveDelete, different);

        firstDigest.Should().Equal(equalDigest);
        firstDigest.Should().NotEqual(differentDigest);
    }

    [Fact]
    public void ExportAndDeletePagesUseAnOversizeSingletonAndOneOrdinalCursor()
    {
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["a"] = CreateRecord("a", "1", payloadLength: 128),
            ["b"] = CreateRecord("b", "2", payloadLength: 1),
        };
        var source = new StoragePartitionView(records, virtualSlotCount: 1);

        var first = StorageMovePageOperations.CreateExportRecords(
            source,
            slot: 0,
            afterRecordKey: null,
            itemLimit: 2,
            byteTarget: 1,
            out var firstCursor,
            out var firstExhausted,
            out var firstBytes);

        StorageMoveRecordCodec.DecodeRecordKey(first.Should().ContainSingle().Which)
            .Should().Be("a");
        firstBytes.Should().BeGreaterThan(1);
        StorageMoveRecordCodec.DecodeNullableText(firstCursor, nameof(firstCursor))
            .Should().Be("a");
        firstExhausted.Should().BeFalse();

        var second = StorageMovePageOperations.CreateExportRecords(
            source,
            slot: 0,
            afterRecordKey: firstCursor,
            itemLimit: 2,
            byteTarget: 1,
            out var secondCursor,
            out var secondExhausted,
            out var secondBytes);

        StorageMoveRecordCodec.DecodeRecordKey(second.Should().ContainSingle().Which)
            .Should().Be("b");
        secondBytes.Should().BeGreaterThan(1);
        StorageMoveRecordCodec.DecodeNullableText(secondCursor, nameof(secondCursor))
            .Should().Be("b");
        secondExhausted.Should().BeTrue();

        var target = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            virtualSlotCount: 1);
        StorageMovePageOperations.ApplyImports(target, [.. first, .. second]);
        target.Records.Should().HaveCount(2);
        target.Records["a"].Should().NotBeSameAs(records["a"]);

        var deletes = StorageMovePageOperations.CreateDeleteRecords(
            target,
            slot: 0,
            afterRecordKey: null,
            itemLimit: 2,
            byteTarget: StorageMoveProtocol.MaximumPageBytes,
            out var deleteCursor,
            out var deleteExhausted,
            out var deleteBytes);
        StorageMoveRecordCodec.DecodeNullableText(deleteCursor, nameof(deleteCursor))
            .Should().Be("b");
        deleteExhausted.Should().BeTrue();
        deleteBytes.Should().BeGreaterThan(0);

        StorageMovePageOperations.ApplyDeletes(target, deletes);
        target.Records.Should().BeEmpty();
        target.SlotCatalog!.GetRecordCount(0).Should().Be(0);
    }

    [Fact]
    public void AbortProgressPreservesImportTotalsAndUsesExactDeleteReceiptsAcrossRestart()
    {
        var move = CreateMoveIdentity();
        var imported = new StoragePartitionMoveControl
        {
            IsPresent = true,
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            Role = StoragePartitionMoveRole.Target,
            Phase = StoragePartitionMovePhase.TargetImportComplete,
            FrozenNextVersion = 20,
            ProgressAfterRecordKey = StorageMoveRecordCodec.EncodeText("import-z"),
            NextPageOrdinal = 2,
            LastPageDigest = new byte[StorageMovePageDigest.DigestLength],
            ImportedRecordCount = 5,
            ImportedByteCount = 500,
            LastPageRequestAfterRecordKey = StorageMoveRecordCodec.EncodeText("import-m"),
            LastPageItemLimit = 3,
            LastPageByteTarget = 200,
            LastPageEncodedByteCount = 120,
        };

        var reset = StoragePartitionGrain.ResetTargetAbortProgress(imported);
        reset.ImportedRecordCount.Should().Be(5);
        reset.ImportedByteCount.Should().Be(500);
        reset.DeletedRecordCount.Should().Be(0);
        reset.DeletedByteCount.Should().Be(0);
        reset.NextPageOrdinal.Should().Be(0);

        var first = StoragePartitionGrain.AdvanceDeletePageControl(
            reset,
            nextRecordKey: StorageMoveRecordCodec.EncodeText("delete-b"),
            pageDigest: Enumerable.Repeat((byte)1, StorageMovePageDigest.DigestLength).ToArray(),
            recordCount: 2,
            encodedByteCount: 40,
            requestAfterRecordKey: null,
            itemLimit: 2,
            byteTarget: 100,
            StoragePartitionMovePhase.TargetAbortDeleting);
        var afterRestart = first.Copy();
        var terminal = StoragePartitionGrain.AdvanceDeletePageControl(
            afterRestart,
            nextRecordKey: StorageMoveRecordCodec.EncodeText("delete-c"),
            pageDigest: Enumerable.Repeat((byte)2, StorageMovePageDigest.DigestLength).ToArray(),
            recordCount: 1,
            encodedByteCount: 30,
            requestAfterRecordKey: StorageMoveRecordCodec.EncodeText("delete-b"),
            itemLimit: 2,
            byteTarget: 100,
            StoragePartitionMovePhase.TargetAbortComplete);

        terminal.ImportedRecordCount.Should().Be(5);
        terminal.ImportedByteCount.Should().Be(500);
        terminal.DeletedRecordCount.Should().Be(3);
        terminal.DeletedByteCount.Should().Be(70);
        terminal.LastPageEncodedByteCount.Should().Be(30);

        var exactRetry = new StorageMoveDeletePageRequest
        {
            Move = move,
            Mode = StorageMoveDeleteMode.TargetAbort,
            PageOrdinal = 1,
            AfterRecordKey = StorageMoveRecordCodec.EncodeText("delete-b"),
            ItemLimit = 2,
            ByteTarget = 100,
        };
        StoragePartitionGrain.IsExactDuplicateDeleteRequest(terminal, exactRetry)
            .Should().BeTrue();

        StoragePartitionGrain.IsExactDuplicateDeleteRequest(
                terminal,
                CopyDeleteRequest(exactRetry, afterRecordKey: "altered"))
            .Should().BeFalse();
        StoragePartitionGrain.IsExactDuplicateDeleteRequest(
                terminal,
                CopyDeleteRequest(exactRetry, itemLimit: 1))
            .Should().BeFalse();
        StoragePartitionGrain.IsExactDuplicateDeleteRequest(
                terminal,
                CopyDeleteRequest(exactRetry, byteTarget: 99))
            .Should().BeFalse();
    }

    [Fact]
    public void LiveImportPreflightRejectsVersionCollisionAndWrongSlotBeforeWalCommit()
    {
        var move = new StorageMoveIdentity
        {
            ProtocolVersion = StorageMoveProtocol.Version,
            MoveId = Guid.NewGuid(),
            Slot = 0,
            VirtualSlotCount = 2,
            SourceEpoch = 1,
            SourceOwner = 0,
            TargetOwner = 1,
        };
        var control = new StoragePartitionMoveControl
        {
            FrozenNextVersion = 10,
        };
        var target = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            virtualSlotCount: 2);
        var valid = CreateExportPage(move, "valid", etag: "9", recordSlot: 0);

        StorageMovePageOperations.ValidateImportAgainstCurrentView(
            target,
            valid,
            control,
            nextVersion: 100);

        var impossibleVersion = CreateExportPage(move, "version", etag: "10", recordSlot: 0);
        var wrongSlot = CreateExportPage(move, "wrong-slot", etag: "9", recordSlot: 1);
        Action validateVersion = () => StorageMovePageOperations.ValidateImportAgainstCurrentView(
            target,
            impossibleVersion,
            control,
            nextVersion: 100);
        Action validateSlot = () => StorageMovePageOperations.ValidateImportAgainstCurrentView(
            target,
            wrongSlot,
            control,
            nextVersion: 100);

        StorageMovePageOperations.ApplyImports(target, valid.Records);
        Action validateCollision = () => StorageMovePageOperations.ValidateImportAgainstCurrentView(
            target,
            valid,
            control,
            nextVersion: 100);

        validateVersion.Should().Throw<InvalidOperationException>();
        validateSlot.Should().Throw<InvalidOperationException>();
        validateCollision.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void PhysicalOwnerUpperBoundIsEnforcedForWireManifestAndJournalState()
    {
        var maximumOwner = StorageLayout.MaximumVirtualSlotCount - 1;
        var identity = CreateMoveIdentity(
            sourceOwner: maximumOwner - 1,
            targetOwner: maximumOwner);
        StoragePartitionGrain.ValidateMoveIdentityBounds(identity);

        var manifest = CreateSourceManifest(identity);
        StoragePartitionPersistence.ValidateManifest(manifest, allowPreviousFormat: true);

        var advance = CreateAdvanceEntry(identity, frozenNextVersion: 1, nextVersionAfter: 1);
        StoragePersistenceStateValidation.ValidateJournalEntry(advance, nameof(advance));

        var invalidIdentity = CreateMoveIdentity(
            sourceOwner: maximumOwner,
            targetOwner: StorageLayout.MaximumVirtualSlotCount);
        Action validateIdentity = () => StoragePartitionGrain.ValidateMoveIdentityBounds(invalidIdentity);

        var invalidManifest = manifest.Copy();
        invalidManifest.MoveControl.TargetOwner = StorageLayout.MaximumVirtualSlotCount;
        Action validateManifest = () => StoragePartitionPersistence.ValidateManifest(
            invalidManifest,
            allowPreviousFormat: true);

        var invalidJournal = CreateAdvanceEntry(
            invalidIdentity,
            frozenNextVersion: 1,
            nextVersionAfter: 1);
        Action validateJournal = () => StoragePersistenceStateValidation.ValidateJournalEntry(
            invalidJournal,
            nameof(invalidJournal));

        validateIdentity.Should().Throw<ArgumentException>();
        validateManifest.Should().Throw<InvalidOperationException>();
        validateJournal.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void MalformedLosslessMovementTextAndNestedRecordBitsAreRejected()
    {
        var source = new StoredRecord
        {
            GrainId = GrainId.Create("movement-malformed", "record"),
            Payload = [1],
            ETag = "1",
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = "movement-malformed/value",
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create("value"),
                },
            ],
        };
        var valid = StorageMoveRecordCodec.Encode("record", source);
        var validValue = valid.Record.IndexEntries[0].Value;
        byte[] invalidDecimal = [.. validValue.PrimitiveBits];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(
            invalidDecimal.AsSpan(28, sizeof(int)),
            unchecked((int)0x7f00_0000));
        byte[] invalidBoolean = [.. validValue.PrimitiveBits];
        invalidBoolean[^1] = 2;

        var invalidRecords = new[]
        {
            new StorageMoveRecord
            {
                RecordKey = [0],
                Record = StorageMoveRecordCodec.Copy(valid.Record),
            },
            new StorageMoveRecord
            {
                RecordKey = [.. valid.RecordKey],
                Record = CopyRecord(
                    valid.Record,
                    value: new StorageMoveIndexValue
                    {
                        Kind = validValue.Kind,
                        Text = StorageMoveRecordCodec.CopyText(validValue.Text),
                        PrimitiveBits = [1],
                    }),
            },
            new StorageMoveRecord
            {
                RecordKey = [.. valid.RecordKey],
                Record = CopyRecord(
                    valid.Record,
                    value: new StorageMoveIndexValue
                    {
                        Kind = validValue.Kind,
                        Text = StorageMoveRecordCodec.CopyText(validValue.Text),
                        PrimitiveBits = invalidBoolean,
                    }),
            },
            new StorageMoveRecord
            {
                RecordKey = [.. valid.RecordKey],
                Record = CopyRecord(
                    valid.Record,
                    value: new StorageMoveIndexValue
                    {
                        Kind = validValue.Kind,
                        Text = StorageMoveRecordCodec.CopyText(validValue.Text),
                        PrimitiveBits = invalidDecimal,
                    }),
            },
        };

        foreach (var invalid in invalidRecords)
        {
            Action validate = () => StorageMoveRecordCodec.Validate(invalid, nameof(invalid));
            validate.Should().Throw<ArgumentException>();
        }
    }

    private static StorageMoveStoredRecord CopyRecord(
        StorageMoveStoredRecord source,
        StorageMoveIndexValue? value = null)
    {
        return new StorageMoveStoredRecord
        {
            GrainType = [.. source.GrainType],
            GrainKey = [.. source.GrainKey],
            Payload = [.. source.Payload!],
            ETag = [.. source.ETag],
            IndexEntries =
            [
                new StorageMoveIndexEntry
                {
                    Scope = [.. source.IndexEntries[0].Scope],
                    Kind = source.IndexEntries[0].Kind,
                    Value = value ?? source.IndexEntries[0].Value,
                },
            ],
        };
    }

    private static StorageMoveDeletePageRequest CopyDeleteRequest(
        StorageMoveDeletePageRequest request,
        string? afterRecordKey = null,
        int? itemLimit = null,
        int? byteTarget = null)
    {
        return new StorageMoveDeletePageRequest
        {
            Move = request.Move.Copy(),
            Mode = request.Mode,
            PageOrdinal = request.PageOrdinal,
            AfterRecordKey = afterRecordKey is null
                ? StorageMoveRecordCodec.CopyText(request.AfterRecordKey)
                : StorageMoveRecordCodec.EncodeText(afterRecordKey),
            ItemLimit = itemLimit ?? request.ItemLimit,
            ByteTarget = byteTarget ?? request.ByteTarget,
        };
    }

    private static StorageMoveExportPage CreateExportPage(
        StorageMoveIdentity move,
        string recordKey,
        string etag,
        int recordSlot)
    {
        var item = StorageMoveRecordCodec.Encode(
            recordKey,
            new StoredRecord
            {
                GrainId = CreateGrainInSlot(recordSlot, move.VirtualSlotCount),
                Payload = [1],
                ETag = etag,
                IndexEntries = [],
            });
        return new StorageMoveExportPage
        {
            Move = move.Copy(),
            PageOrdinal = 0,
            NextRecordKey = StorageMoveRecordCodec.EncodeText(recordKey),
            Exhausted = true,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(item),
            Records = [item],
            PageDigest = new byte[StorageMovePageDigest.DigestLength],
            FrozenNextVersion = 10,
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.MaximumPageBytes,
        };
    }

    private static GrainId CreateGrainInSlot(int slot, int virtualSlotCount)
    {
        for (var candidate = 0; ; candidate++)
        {
            var grainId = GrainId.Create("movement-import", $"record-{candidate}");
            if (StorageLayout.GetSlot(grainId, virtualSlotCount) == slot)
            {
                return grainId;
            }
        }
    }

    private static StoragePartitionManifestState CreateSourceManifest(StorageMoveIdentity move)
    {
        return new StoragePartitionManifestState
        {
            Initialized = true,
            PersistenceFormatVersion = StoragePersistence.MovementPersistenceFormatVersion,
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            NextVersion = 1,
            MovementProtocolVersion = StorageMoveProtocol.Version,
            RoutedOperationsRequired = true,
            MinimumRoutingEpoch = move.SourceEpoch,
            MoveControl = new StoragePartitionMoveControl
            {
                IsPresent = true,
                MoveId = move.MoveId,
                Slot = move.Slot,
                VirtualSlotCount = move.VirtualSlotCount,
                SourceEpoch = move.SourceEpoch,
                SourceOwner = move.SourceOwner,
                TargetOwner = move.TargetOwner,
                Role = StoragePartitionMoveRole.Source,
                Phase = StoragePartitionMovePhase.SourceFrozen,
                FrozenNextVersion = 1,
            },
        };
    }

    private static StorageJournalEntry CreateAdvanceEntry(
        StorageMoveIdentity move,
        long frozenNextVersion,
        long nextVersionAfter)
    {
        return new StorageJournalEntry
        {
            Sequence = 1,
            WriterEpoch = 1,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = Guid.Empty,
            Operation = StorageJournalOperation.AdvanceVersion,
            RecordKey = string.Empty,
            NextVersionAfter = nextVersionAfter,
            Move = new StorageMoveJournalPayload
            {
                MoveId = move.MoveId,
                Slot = move.Slot,
                VirtualSlotCount = move.VirtualSlotCount,
                SourceEpoch = move.SourceEpoch,
                SourceOwner = move.SourceOwner,
                TargetOwner = move.TargetOwner,
                FrozenNextVersion = frozenNextVersion,
            },
        };
    }

    private static StorageMoveJournalPayload CreateDeletePayload(string recordKey)
    {
        var item = StorageMoveRecordCodec.EncodeDelete(recordKey, "1");
        return new StorageMoveJournalPayload
        {
            MoveId = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Slot = 0,
            VirtualSlotCount = 1,
            SourceEpoch = 1,
            SourceOwner = 0,
            TargetOwner = 1,
            PageOrdinal = 0,
            NextRecordKey = StorageMoveRecordCodec.EncodeText(recordKey),
            Exhausted = true,
            FrozenNextVersion = 2,
            Deletes = [item],
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.MaximumPageBytes,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(item),
        };
    }

    internal static StorageMoveIdentity CreateMoveIdentity(
        int sourceOwner = 0,
        int targetOwner = 1)
    {
        return new StorageMoveIdentity
        {
            ProtocolVersion = StorageMoveProtocol.Version,
            MoveId = Guid.Parse("22222222-2222-2222-2222-222222222222"),
            Slot = 0,
            VirtualSlotCount = 1,
            SourceEpoch = 1,
            SourceOwner = sourceOwner,
            TargetOwner = targetOwner,
        };
    }

    internal static StoredRecord CreateRecord(
        string key,
        string etag,
        int payloadLength = 3)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("movement-protocol", key),
            Payload = new byte[payloadLength],
            ETag = etag,
            IndexEntries = [],
        };
    }
}
