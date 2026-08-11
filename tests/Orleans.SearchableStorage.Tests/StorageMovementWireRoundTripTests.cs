using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageMovementWireRoundTripTests
{
    private readonly MemoryStorageFixture _fixture;

    public StorageMovementWireRoundTripTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExportPageProxyRoundTripPreservesLoneUtf16Surrogates()
    {
        var invalidText = new string((char)0xD800, 1);
        var recordKey = $"record-{invalidText}";
        var item = StorageMoveRecordCodec.Encode(
            recordKey,
            new StoredRecord
            {
                GrainId = Orleans.Runtime.GrainId.Create("movement-wire", "record"),
                Payload = [1, 2, 3],
                ETag = "1",
                IndexEntries =
                [
                    new IndexEntry
                    {
                        Scope = "movement-wire/value",
                        Kind = SearchableIndexKind.Hash,
                        Value = new IndexValue
                        {
                            Kind = IndexValueKind.String,
                            Text = invalidText,
                        },
                    },
                ],
            });
        var move = StorageMovementProtocolTests.CreateMoveIdentity();
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            PageOrdinal = 0,
            NextRecordKey = item.RecordKey,
            Exhausted = true,
            FrozenNextVersion = 2,
            Imports = [item],
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.MaximumPageBytes,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(item),
        };
        var page = new StorageMoveExportPage
        {
            Move = move,
            PageOrdinal = unsigned.PageOrdinal,
            NextRecordKey = unsigned.NextRecordKey,
            Exhausted = unsigned.Exhausted,
            EncodedByteCount = unsigned.EncodedByteCount,
            Records = unsigned.Imports,
            PageDigest = StorageMovePageDigest.Compute(StorageJournalOperation.Import, unsigned),
            FrozenNextVersion = unsigned.FrozenNextVersion,
            ItemLimit = unsigned.ItemLimit,
            ByteTarget = unsigned.ByteTarget,
        };

        var echo = _fixture.Cluster.GrainFactory.GetGrain<IStorageMoveWireEchoGrain>(
            Guid.NewGuid().ToString("N"));
        var roundTripped = await echo.EchoAsync(page);

        roundTripped.Records.Should().ContainSingle();
        StorageMoveRecordCodec.DecodeRecordKey(roundTripped.Records[0]).Should().Be(recordKey);
        var decoded = StorageMoveRecordCodec.Decode(roundTripped.Records[0].Record);
        decoded.IndexEntries.Should().ContainSingle();
        decoded.IndexEntries[0].Value.Text.Should().Be(invalidText);
    }

    [Fact]
    public async Task LosslessExportImportsThroughARealPartitionAndRecoversExactReceipts()
    {
        const int partitionCount = 2;
        const int virtualSlotCount = 2;
        const int slot = 0;
        const int sourceOwner = 0;
        const int targetOwner = 1;
        var providerName = $"movement-wire-{Guid.NewGuid():N}";
        var layout = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        _ = await layout.InitializeRoutingAsync(StorageLayout.CreateDescriptor(
            providerName,
            partitionCount,
            journalSegmentCapacity: 8,
            maximumJournalReplayEntries: 64,
            virtualSlotTargetCount: virtualSlotCount));
        var admin = new SearchableStorageAdminClient(
            _fixture.Cluster.GrainFactory,
            providerName,
            partitionCount,
            new SearchableStorageMovementOptions
            {
                TransferPageRecordLimit = 1,
                TransferPageByteTarget = StorageMoveProtocol.MaximumPageBytes,
            });
        _ = await admin.EnableMovementAsync();
        var planned = await admin.PlanMoveAsync(slot, targetOwner);
        planned.SourcePartitionIndex.Should().Be(sourceOwner);
        var move = new StorageMoveIdentity
        {
            ProtocolVersion = StorageMoveProtocol.Version,
            MoveId = planned.MoveId,
            Slot = slot,
            VirtualSlotCount = virtualSlotCount,
            SourceEpoch = planned.SourceEpoch,
            SourceOwner = sourceOwner,
            TargetOwner = targetOwner,
        };
        var target = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, targetOwner));
        var prepared = await target.PrepareMoveTargetAsync(new StorageMoveTargetPrepareRequest
        {
            Move = move,
            FrozenNextVersion = 2,
        });
        prepared.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetImporting);

        var highSurrogate = new string((char)0xD800, 1);
        var lowSurrogate = new string((char)0xDC00, 1);
        var recordKey = $"state-{highSurrogate}/AA/BB";
        var grainId = CreateRawGrainIdInSlot(slot, virtualSlotCount);
        var floatingBits = unchecked((long)0x7ff8_0000_0000_0042UL);
        var sourceRecord = new StoredRecord
        {
            GrainId = grainId,
            Payload = [0, 1, 2, 255],
            ETag = "1",
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = $"scope-{lowSurrogate}",
                    Kind = SearchableIndexKind.Hash,
                    Value = new IndexValue
                    {
                        Kind = IndexValueKind.String,
                        Text = highSurrogate,
                        SignedInteger = long.MinValue + 17,
                        UnsignedInteger = ulong.MaxValue - 19,
                        Decimal = -1234.5678m,
                        FloatingPoint = BitConverter.Int64BitsToDouble(floatingBits),
                        UtcTicks = long.MaxValue - 23,
                        Guid = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"),
                        Boolean = true,
                    },
                },
            ],
        };
        var sourceView = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                [recordKey] = sourceRecord,
            },
            virtualSlotCount);
        var exported = StorageMovePageOperations.CreateExportRecords(
            sourceView,
            slot,
            afterRecordKey: null,
            itemLimit: 1,
            byteTarget: StorageMoveProtocol.MaximumPageBytes,
            out var nextRecordKey,
            out var exhausted,
            out var encodedByteCount);
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            PageOrdinal = 0,
            NextRecordKey = StorageMoveRecordCodec.CopyText(nextRecordKey),
            Exhausted = exhausted,
            FrozenNextVersion = 2,
            Imports = exported.Select(static item => item.Copy()).ToList(),
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.MaximumPageBytes,
            EncodedByteCount = encodedByteCount,
        };
        var page = new StorageMoveExportPage
        {
            Move = move.Copy(),
            PageOrdinal = 0,
            NextRecordKey = StorageMoveRecordCodec.CopyText(nextRecordKey),
            Exhausted = exhausted,
            EncodedByteCount = encodedByteCount,
            Records = exported,
            PageDigest = StorageMovePageDigest.Compute(StorageJournalOperation.Import, unsigned),
            FrozenNextVersion = 2,
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.MaximumPageBytes,
        };
        var sourceProxy = _fixture.Cluster.GrainFactory.GetGrain<IStorageMoveWireEchoGrain>(
            Guid.NewGuid().ToString("N"));
        var proxyPage = await sourceProxy.EchoAsync(page);
        proxyPage.PageDigest.Should().Equal(page.PageDigest);
        StorageMovePageDigest.Compute(
                StorageJournalOperation.Import,
                CreateImportPayload(proxyPage))
            .Should().Equal(page.PageDigest);

        var receipt = await target.ImportMovePageAsync(new StorageMoveImportPageRequest
        {
            Page = proxyPage,
        });
        receipt.PageDigest.Should().Equal(page.PageDigest);
        StorageMoveRecordCodec.DecodeNullableText(receipt.AfterRecordKey, nameof(receipt))
            .Should().Be(recordKey);
        receipt.State.MoveControl.ImportedRecordCount.Should().Be(1);
        receipt.State.MoveControl.ImportedByteCount.Should().Be(encodedByteCount);

        await _fixture.Cluster.DeactivateAsync(target);
        var recovered = await target.GetMovementStateAsync();
        recovered.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetImportComplete);
        recovered.MoveControl.LastPageDigest.Should().Equal(page.PageDigest);
        StorageMoveRecordCodec.DecodeNullableText(
                recovered.MoveControl.ProgressAfterRecordKey,
                nameof(recovered))
            .Should().Be(recordKey);
        (await target.GetPersistenceInfoAsync()).RecordCount.Should().Be(1);

        var deleted = await target.DeleteMovePageAsync(new StorageMoveDeletePageRequest
        {
            Move = move.Copy(),
            Mode = StorageMoveDeleteMode.TargetAbort,
            PageOrdinal = 0,
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.MaximumPageBytes,
        });
        StorageMoveRecordCodec.DecodeNullableText(deleted.AfterRecordKey, nameof(deleted))
            .Should().Be(recordKey);
        deleted.State.MoveControl.ImportedRecordCount.Should().Be(1);
        deleted.State.MoveControl.DeletedRecordCount.Should().Be(1);

        await _fixture.Cluster.DeactivateAsync(target);
        var recoveredAfterDelete = await target.GetMovementStateAsync();
        recoveredAfterDelete.MoveControl.Phase.Should().Be(
            StoragePartitionMovePhase.TargetAbortComplete);
        recoveredAfterDelete.MoveControl.ImportedRecordCount.Should().Be(1);
        recoveredAfterDelete.MoveControl.DeletedRecordCount.Should().Be(1);
        (await target.GetPersistenceInfoAsync()).RecordCount.Should().Be(0);

        var decoded = StorageMoveRecordCodec.Decode(proxyPage.Records[0].Record);
        decoded.GrainId.Type.AsSpan().ToArray().Should().Equal(grainId.Type.AsSpan().ToArray());
        decoded.GrainId.Key.AsSpan().ToArray().Should().Equal(grainId.Key.AsSpan().ToArray());
        decoded.ETag.Should().Be(sourceRecord.ETag);
        decoded.IndexEntries[0].Scope.Should().Be(sourceRecord.IndexEntries[0].Scope);
        decoded.IndexEntries[0].Value.Text.Should().Be(highSurrogate);
        BitConverter.DoubleToInt64Bits(decoded.IndexEntries[0].Value.FloatingPoint)
            .Should().Be(floatingBits);
        decoded.IndexEntries[0].Value.Decimal.Should().Be(-1234.5678m);
    }

    private static StorageMoveJournalPayload CreateImportPayload(StorageMoveExportPage page)
    {
        return new StorageMoveJournalPayload
        {
            MoveId = page.Move.MoveId,
            Slot = page.Move.Slot,
            VirtualSlotCount = page.Move.VirtualSlotCount,
            SourceEpoch = page.Move.SourceEpoch,
            SourceOwner = page.Move.SourceOwner,
            TargetOwner = page.Move.TargetOwner,
            PageOrdinal = page.PageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(page.AfterRecordKey),
            NextRecordKey = StorageMoveRecordCodec.CopyText(page.NextRecordKey),
            Exhausted = page.Exhausted,
            FrozenNextVersion = page.FrozenNextVersion,
            Imports = page.Records.Select(static item => item.Copy()).ToList(),
            ItemLimit = page.ItemLimit,
            ByteTarget = page.ByteTarget,
            EncodedByteCount = page.EncodedByteCount,
        };
    }

    private static GrainId CreateRawGrainIdInSlot(int slot, int virtualSlotCount)
    {
        var type = new GrainType([0xff, 0x00, 0x80, 0x41]);
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

internal interface IStorageMoveWireEchoGrain : IGrainWithStringKey
{
    Task<StorageMoveExportPage> EchoAsync(StorageMoveExportPage page);
}

internal sealed class StorageMoveWireEchoGrain : Grain, IStorageMoveWireEchoGrain
{
    public Task<StorageMoveExportPage> EchoAsync(StorageMoveExportPage page)
    {
        return Task.FromResult(page);
    }
}
