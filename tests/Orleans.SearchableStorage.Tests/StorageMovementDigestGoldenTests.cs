using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StorageMovementDigestGoldenTests
{
    [Fact]
    public void CanonicalImportAndDeleteDigestsMatchFrozenGoldenVectors()
    {
        var highSurrogate = new string((char)0xD800, 1);
        var lowSurrogate = new string((char)0xDC00, 1);
        var moveId = Guid.Parse("12345678-9abc-def0-1234-56789abcdef0");
        var recordKey = $"b-{lowSurrogate}";
        var record = new StoredRecord
        {
            GrainId = CreateRawGrainIdInSlot(slot: 0, virtualSlotCount: 2),
            Payload = [0x00, 0x7f, 0x80, 0xff],
            ETag = "41",
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = $"scope-{lowSurrogate}",
                    Kind = SearchableIndexKind.Range,
                    Value = new IndexValue
                    {
                        Kind = IndexValueKind.String,
                        Text = highSurrogate,
                        SignedInteger = long.MinValue + 1,
                        UnsignedInteger = ulong.MaxValue - 2,
                        Decimal = -7922816251426433759354395.0335m,
                        FloatingPoint = BitConverter.Int64BitsToDouble(
                            unchecked((long)0x7ff8_0000_0000_0042UL)),
                        UtcTicks = 638_700_123_456_789_012,
                        Guid = Guid.Parse("fedcba98-7654-3210-fedc-ba9876543210"),
                        Boolean = true,
                    },
                },
            ],
        };
        var imported = StorageMoveRecordCodec.Encode(recordKey, record);
        var importPayload = new StorageMoveJournalPayload
        {
            MoveId = moveId,
            Slot = 0,
            VirtualSlotCount = 2,
            SourceEpoch = 7,
            SourceOwner = 0,
            TargetOwner = 1,
            PageOrdinal = 3,
            AfterRecordKey = StorageMoveRecordCodec.EncodeText($"a-{highSurrogate}"),
            NextRecordKey = StorageMoveRecordCodec.EncodeText(recordKey),
            Exhausted = true,
            FrozenNextVersion = 42,
            Imports = [imported],
            ItemLimit = 7,
            ByteTarget = 12_345,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(imported),
        };
        var deleted = StorageMoveRecordCodec.EncodeDelete($"c-{highSurrogate}", "41");
        var deletePayload = new StorageMoveJournalPayload
        {
            MoveId = moveId,
            Slot = 0,
            VirtualSlotCount = 2,
            SourceEpoch = 7,
            SourceOwner = 0,
            TargetOwner = 1,
            PageOrdinal = 4,
            AfterRecordKey = StorageMoveRecordCodec.EncodeText(recordKey),
            NextRecordKey = [.. deleted.RecordKey],
            Exhausted = false,
            FrozenNextVersion = 42,
            Deletes = [deleted],
            ItemLimit = 7,
            ByteTarget = 12_345,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(deleted),
        };

        var importHex = Convert.ToHexString(StorageMovePageDigest.Compute(
            StorageJournalOperation.Import,
            importPayload));
        var deleteHex = Convert.ToHexString(StorageMovePageDigest.Compute(
            StorageJournalOperation.MoveDelete,
            deletePayload));
        importHex.Should().Be(
            "A51829C81A8F95DDC26FCED1B6B133C2A308EC366878E480EEB9E55946D1A400");
        deleteHex.Should().Be(
            "D7755D53836AFD00625BAC6052E93EE6009FAABA6F1B35D2F273AB8BDFF099D6");
    }

    private static GrainId CreateRawGrainIdInSlot(int slot, int virtualSlotCount)
    {
        var type = new GrainType([0xff, 0x01, 0x80, 0x42]);
        var key = new byte[sizeof(int)];
        for (var candidate = 0; ; candidate++)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(key, candidate);
            var grainId = GrainId.Create(type, new IdSpan(key));
            if (StorageLayout.GetSlot(grainId, virtualSlotCount) == slot)
            {
                return grainId;
            }
        }
    }
}
