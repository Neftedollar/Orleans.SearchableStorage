using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageMovementFaultRecoveryTests
{
    private static readonly StoragePersistenceSettings Settings = new()
    {
        JournalSegmentCapacity = 8,
        MaximumJournalReplayEntries = 64,
        CompactionThreshold = 64,
    };

    private readonly MemoryStorageFixture _fixture;

    public StorageMovementFaultRecoveryTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData(MovementWalCase.AdvanceVersion, PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(MovementWalCase.AdvanceVersion, PhysicalWriteFaultStage.AfterCommit)]
    [InlineData(MovementWalCase.Import, PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(MovementWalCase.Import, PhysicalWriteFaultStage.AfterCommit)]
    [InlineData(MovementWalCase.MoveDelete, PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(MovementWalCase.MoveDelete, PhysicalWriteFaultStage.AfterCommit)]
    internal async Task ChildWalBeforeCommitAndLostAcknowledgementResumeAfterReactivation(
        MovementWalCase operation,
        PhysicalWriteFaultStage faultStage)
    {
        var context = await CreateContextAsync(operation);
        var journal = GetJournal(context.PartitionKey, context.Entry.Sequence);
        await WriteFaultInjectingGrainStorage.AddWriteFaultAsync(
            _fixture.Cluster.GrainFactory,
            journal.GetGrainId(),
            "journal",
            faultStage,
            call: 1);

        Func<Task> commit = () => context.Persistence.CommitAsync(
            context.Entry,
            context.AdvancedControl);
        var failure = await commit.Should().ThrowAsync<Exception>();
        failure.Which.ToString().Should().Contain(
            WriteFaultInjectingGrainStorage.InjectedFailureMessage);
        context.PoisonCount().Should().Be(1);

        await _fixture.Cluster.DeactivateAsync(journal);
        var resumed = CreatePersistence(
            context.Manifest,
            context.PartitionKey,
            static () => { });
        var resumedRecords = await resumed.ActivateAsync();
        AssertPreCommitState(context, operation, resumed, resumedRecords);

        await resumed.PrepareForProtocolMutationAsync(resumedRecords);
        var (retryEntry, retryControl) = CreateAttempt(operation, resumed);
        await resumed.CommitAsync(retryEntry, retryControl);

        await _fixture.Cluster.DeactivateAsync(journal);
        await AssertFinalRecoveredStateAsync(context, operation);
    }

    [Theory]
    [InlineData(MovementWalCase.AdvanceVersion, false)]
    [InlineData(MovementWalCase.AdvanceVersion, true)]
    [InlineData(MovementWalCase.Import, false)]
    [InlineData(MovementWalCase.Import, true)]
    [InlineData(MovementWalCase.MoveDelete, false)]
    [InlineData(MovementWalCase.MoveDelete, true)]
    internal async Task ManifestBeforeCommitAndLostAcknowledgementResumeAfterReactivation(
        MovementWalCase operation,
        bool manifestCommitted)
    {
        var context = await CreateContextAsync(operation);
        context.Manifest.WriteException = new InvalidOperationException(
            "Ambiguous movement manifest write.");

        Func<Task> commit = () => context.Persistence.CommitAsync(
            context.Entry,
            context.AdvancedControl);
        await commit.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(context.Manifest.WriteException.Message);
        context.PoisonCount().Should().Be(1);

        // PersistManifest restores its last trusted in-memory value after an ambiguous result.
        // Selecting the attempted candidate models the provider having committed before losing
        // the acknowledgement; retaining the restored value models a before-commit failure.
        if (manifestCommitted)
        {
            context.Manifest.State = context.Manifest.LastWriteState!;
        }

        context.Manifest.WriteException = null;
        var journal = GetJournal(context.PartitionKey, context.Entry.Sequence);
        await _fixture.Cluster.DeactivateAsync(journal);
        var resumed = CreatePersistence(
            context.Manifest,
            context.PartitionKey,
            static () => { });
        var resumedRecords = await resumed.ActivateAsync();

        if (!manifestCommitted)
        {
            AssertPreCommitState(context, operation, resumed, resumedRecords);
            await resumed.PrepareForProtocolMutationAsync(resumedRecords);
            var (retryEntry, retryControl) = CreateAttempt(operation, resumed);
            await resumed.CommitAsync(retryEntry, retryControl);
            await _fixture.Cluster.DeactivateAsync(journal);
        }

        await AssertFinalRecoveredStateAsync(context, operation);
    }

    [Theory]
    [InlineData(MovementWalCase.Import, PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(MovementWalCase.Import, PhysicalWriteFaultStage.AfterCommit)]
    [InlineData(MovementWalCase.MoveDelete, PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(MovementWalCase.MoveDelete, PhysicalWriteFaultStage.AfterCommit)]
    internal async Task NonzeroPageChildWalFaultResumesAfterCommittedFirstPageAndReactivation(
        MovementWalCase operation,
        PhysicalWriteFaultStage faultStage)
    {
        var context = await CreateNonzeroPageContextAsync(operation);
        var journal = GetJournal(context.PartitionKey, context.Entry.Sequence);
        await WriteFaultInjectingGrainStorage.AddWriteFaultAsync(
            _fixture.Cluster.GrainFactory,
            journal.GetGrainId(),
            "journal",
            faultStage,
            call: 1);

        Func<Task> commit = () => context.Persistence.CommitAsync(
            context.Entry,
            context.AdvancedControl);
        var failure = await commit.Should().ThrowAsync<Exception>();
        failure.Which.ToString().Should().Contain(
            WriteFaultInjectingGrainStorage.InjectedFailureMessage);
        context.PoisonCount().Should().Be(1);

        await _fixture.Cluster.DeactivateAsync(journal);
        var resumed = CreatePersistence(context.Manifest, context.PartitionKey, static () => { });
        var records = await resumed.ActivateAsync();
        AssertPreCommitState(context, operation, resumed, records);
        await resumed.PrepareForProtocolMutationAsync(records);
        var (retryEntry, retryControl) = CreateAttempt(operation, resumed);
        await resumed.CommitAsync(retryEntry, retryControl);
        await _fixture.Cluster.DeactivateAsync(journal);
        await AssertFinalRecoveredStateAsync(context, operation);
    }

    [Theory]
    [InlineData(MovementWalCase.Import, false)]
    [InlineData(MovementWalCase.Import, true)]
    [InlineData(MovementWalCase.MoveDelete, false)]
    [InlineData(MovementWalCase.MoveDelete, true)]
    internal async Task NonzeroPageManifestFaultResumesAfterCommittedFirstPageAndReactivation(
        MovementWalCase operation,
        bool manifestCommitted)
    {
        var context = await CreateNonzeroPageContextAsync(operation);
        context.Manifest.WriteException = new InvalidOperationException(
            "Ambiguous nonzero-page manifest write.");

        Func<Task> commit = () => context.Persistence.CommitAsync(
            context.Entry,
            context.AdvancedControl);
        await commit.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(context.Manifest.WriteException.Message);
        context.PoisonCount().Should().Be(1);
        if (manifestCommitted)
        {
            context.Manifest.State = context.Manifest.LastWriteState!;
        }

        context.Manifest.WriteException = null;
        var journal = GetJournal(context.PartitionKey, context.Entry.Sequence);
        await _fixture.Cluster.DeactivateAsync(journal);
        var resumed = CreatePersistence(context.Manifest, context.PartitionKey, static () => { });
        var records = await resumed.ActivateAsync();
        if (!manifestCommitted)
        {
            AssertPreCommitState(context, operation, resumed, records);
            await resumed.PrepareForProtocolMutationAsync(records);
            var (retryEntry, retryControl) = CreateAttempt(operation, resumed);
            await resumed.CommitAsync(retryEntry, retryControl);
            await _fixture.Cluster.DeactivateAsync(journal);
        }

        await AssertFinalRecoveredStateAsync(context, operation);
    }

    private async Task<MovementFaultContext> CreateContextAsync(MovementWalCase operation)
    {
        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var partitionKey = $"movement-fault-{operation}-{Guid.NewGuid():N}:00000000";
        var poisonCount = 0;
        var persistence = CreatePersistence(manifest, partitionKey, () => poisonCount++);
        var move = StorageMovementProtocolTests.CreateMoveIdentity();
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);

        switch (operation)
        {
            case MovementWalCase.AdvanceVersion:
            {
                await persistence.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);
                await persistence.SetMoveControlAsync(CreateTargetControl(
                    move,
                    StoragePartitionMovePhase.TargetPrepared,
                    frozenNextVersion: 10));
                await persistence.PrepareForProtocolMutationAsync(records);
                break;
            }
            case MovementWalCase.Import:
            {
                await persistence.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);
                var prepared = CreateTargetControl(
                    move,
                    StoragePartitionMovePhase.TargetPrepared,
                    frozenNextVersion: 10);
                await persistence.SetMoveControlAsync(prepared);
                await persistence.PrepareForProtocolMutationAsync(records);
                var importing = prepared.Copy();
                importing.Phase = StoragePartitionMovePhase.TargetImporting;
                var advance = CreateAdvanceEntry(persistence, move, frozenNextVersion: 10);
                await persistence.CommitAsync(advance, importing);
                break;
            }
            case MovementWalCase.MoveDelete:
            {
                await persistence.PrepareForMutationAsync(records, Settings);
                var record = StorageMovementProtocolTests.CreateRecord("delete-record", "1");
                var upsert = new StorageJournalEntry
                {
                    Sequence = persistence.NextSequence,
                    WriterEpoch = persistence.WriterEpoch,
                    OperationId = Guid.NewGuid(),
                    PreviousOperationId = persistence.CommittedOperationId,
                    Operation = StorageJournalOperation.Upsert,
                    RecordKey = "delete-record",
                    Record = record,
                    NextVersionAfter = 2,
                };
                await persistence.CommitAsync(upsert);
                records.Add("delete-record", record);
                await persistence.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);
                var source = CreateSourceControl(
                    move,
                    StoragePartitionMovePhase.SourceHidden,
                    frozenNextVersion: 2);
                await persistence.SetMoveControlAsync(source, minimumRoutingEpoch: 2);
                await persistence.PrepareForProtocolMutationAsync(records);
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }

        var (entry, advancedControl) = CreateAttempt(operation, persistence);
        return new MovementFaultContext(
            manifest,
            persistence,
            partitionKey,
            entry,
            advancedControl,
            () => poisonCount);
    }

    private async Task<MovementFaultContext> CreateNonzeroPageContextAsync(
        MovementWalCase operation)
    {
        if (operation == MovementWalCase.Import)
        {
            var initial = await CreateContextAsync(operation);
            var persistence = initial.Persistence;
            var move = CreateMoveIdentity(persistence.MoveControl);
            var firstPayload = CreateImportPayload(
                move,
                persistence.MoveControl.FrozenNextVersion,
                pageOrdinal: 0,
                afterRecordKey: null,
                recordKey: "import-a",
                etag: "8",
                exhausted: false);
            var firstControl = StoragePartitionGrain.AdvanceImportPageControl(
                persistence.MoveControl,
                firstPayload.NextRecordKey,
                firstPayload.PageDigest,
                firstPayload.Imports.Count,
                firstPayload.EncodedByteCount,
                firstPayload.AfterRecordKey,
                firstPayload.ItemLimit,
                firstPayload.ByteTarget,
                StoragePartitionMovePhase.TargetImporting);
            var firstEntry = CreateJournalEntry(
                persistence,
                StorageJournalOperation.Import,
                firstPayload,
                persistence.NextVersion);
            await persistence.CommitAsync(firstEntry, firstControl);
            await _fixture.Cluster.DeactivateAsync(
                GetJournal(initial.PartitionKey, firstEntry.Sequence));

            var poisonCount = 0;
            var resumed = CreatePersistence(
                initial.Manifest,
                initial.PartitionKey,
                () => poisonCount++);
            var recoveredRecords = await resumed.ActivateAsync();
            resumed.MoveControl.NextPageOrdinal.Should().Be(1);
            recoveredRecords.Should().ContainKey("import-a");
            await resumed.PrepareForProtocolMutationAsync(recoveredRecords);
            var (entry, control) = CreateAttempt(operation, resumed);
            entry.Move!.PageOrdinal.Should().Be(1);
            return new MovementFaultContext(
                initial.Manifest,
                resumed,
                initial.PartitionKey,
                entry,
                control,
                () => poisonCount);
        }

        if (operation != MovementWalCase.MoveDelete)
        {
            throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }

        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var partitionKey = $"movement-fault-nonzero-delete-{Guid.NewGuid():N}:00000000";
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var persistenceBeforeRecovery = CreatePersistence(manifest, partitionKey, static () => { });
        await persistenceBeforeRecovery.PrepareForMutationAsync(records, Settings);
        var firstRecord = StorageMovementProtocolTests.CreateRecord("delete-a", "1");
        await persistenceBeforeRecovery.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistenceBeforeRecovery.NextSequence,
            WriterEpoch = persistenceBeforeRecovery.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = persistenceBeforeRecovery.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = "delete-a",
            Record = firstRecord,
            NextVersionAfter = 2,
        });
        records.Add("delete-a", firstRecord);
        var secondRecord = StorageMovementProtocolTests.CreateRecord("delete-b", "2");
        await persistenceBeforeRecovery.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistenceBeforeRecovery.NextSequence,
            WriterEpoch = persistenceBeforeRecovery.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = persistenceBeforeRecovery.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = "delete-b",
            Record = secondRecord,
            NextVersionAfter = 3,
        });
        records.Add("delete-b", secondRecord);
        await persistenceBeforeRecovery.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);
        var moveIdentity = StorageMovementProtocolTests.CreateMoveIdentity();
        await persistenceBeforeRecovery.SetMoveControlAsync(
            CreateSourceControl(
                moveIdentity,
                StoragePartitionMovePhase.SourceHidden,
                frozenNextVersion: 3),
            minimumRoutingEpoch: 2);
        await persistenceBeforeRecovery.PrepareForProtocolMutationAsync(records);
        var firstDelete = CreateDeletePayload(
            moveIdentity,
            frozenNextVersion: 3,
            pageOrdinal: 0,
            afterRecordKey: null,
            recordKey: "delete-a",
            etag: "1",
            exhausted: false);
        var afterFirstDelete = StoragePartitionGrain.AdvanceDeletePageControl(
            persistenceBeforeRecovery.MoveControl,
            firstDelete.NextRecordKey,
            firstDelete.PageDigest,
            firstDelete.Deletes.Count,
            firstDelete.EncodedByteCount,
            firstDelete.AfterRecordKey,
            firstDelete.ItemLimit,
            firstDelete.ByteTarget,
            StoragePartitionMovePhase.SourceDeleting);
        var firstDeleteEntry = CreateJournalEntry(
            persistenceBeforeRecovery,
            StorageJournalOperation.MoveDelete,
            firstDelete,
            persistenceBeforeRecovery.NextVersion);
        await persistenceBeforeRecovery.CommitAsync(firstDeleteEntry, afterFirstDelete);
        await _fixture.Cluster.DeactivateAsync(GetJournal(partitionKey, firstDeleteEntry.Sequence));

        var resumedPoisonCount = 0;
        var deletePersistence = CreatePersistence(manifest, partitionKey, () => resumedPoisonCount++);
        var recovered = await deletePersistence.ActivateAsync();
        deletePersistence.MoveControl.NextPageOrdinal.Should().Be(1);
        recovered.Should().ContainKey("delete-b").And.NotContainKey("delete-a");
        await deletePersistence.PrepareForProtocolMutationAsync(recovered);
        var (nextEntry, nextControl) = CreateAttempt(operation, deletePersistence);
        nextEntry.Move!.PageOrdinal.Should().Be(1);
        return new MovementFaultContext(
            manifest,
            deletePersistence,
            partitionKey,
            nextEntry,
            nextControl,
            () => resumedPoisonCount);
    }

    private static (StorageJournalEntry Entry, StoragePartitionMoveControl Control) CreateAttempt(
        MovementWalCase operation,
        StoragePartitionPersistence persistence)
    {
        var move = CreateMoveIdentity(persistence.MoveControl);
        return operation switch
        {
            MovementWalCase.AdvanceVersion => CreateAdvanceAttempt(persistence, move),
            MovementWalCase.Import => CreateImportAttempt(persistence, move),
            MovementWalCase.MoveDelete => CreateDeleteAttempt(persistence, move),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null),
        };
    }

    private static (StorageJournalEntry Entry, StoragePartitionMoveControl Control)
        CreateAdvanceAttempt(
            StoragePartitionPersistence persistence,
            StorageMoveIdentity move)
    {
        var current = persistence.MoveControl;
        var advanced = current.Copy();
        advanced.Phase = StoragePartitionMovePhase.TargetImporting;
        return (CreateAdvanceEntry(persistence, move, current.FrozenNextVersion), advanced);
    }

    private static (StorageJournalEntry Entry, StoragePartitionMoveControl Control)
        CreateImportAttempt(
            StoragePartitionPersistence persistence,
        StorageMoveIdentity move)
    {
        var current = persistence.MoveControl;
        var nonzeroPage = current.NextPageOrdinal > 0;
        var payload = CreateImportPayload(
            move,
            current.FrozenNextVersion,
            current.NextPageOrdinal,
            current.ProgressAfterRecordKey,
            nonzeroPage ? "import-b" : "import-record",
            "9",
            exhausted: true);
        var advanced = StoragePartitionGrain.AdvanceImportPageControl(
            current,
            payload.NextRecordKey,
            payload.PageDigest,
            payload.Imports.Count,
            payload.EncodedByteCount,
            payload.AfterRecordKey,
            payload.ItemLimit,
            payload.ByteTarget,
            StoragePartitionMovePhase.TargetImportComplete);
        return (CreateJournalEntry(
            persistence,
            StorageJournalOperation.Import,
            payload,
            persistence.NextVersion), advanced);
    }

    private static (StorageJournalEntry Entry, StoragePartitionMoveControl Control)
        CreateDeleteAttempt(
            StoragePartitionPersistence persistence,
        StorageMoveIdentity move)
    {
        var current = persistence.MoveControl;
        var nonzeroPage = current.NextPageOrdinal > 0;
        var payload = CreateDeletePayload(
            move,
            current.FrozenNextVersion,
            current.NextPageOrdinal,
            current.ProgressAfterRecordKey,
            nonzeroPage ? "delete-b" : "delete-record",
            nonzeroPage ? "2" : "1",
            exhausted: true);
        var advanced = StoragePartitionGrain.AdvanceDeletePageControl(
            current,
            payload.NextRecordKey,
            payload.PageDigest,
            payload.Deletes.Count,
            payload.EncodedByteCount,
            payload.AfterRecordKey,
            payload.ItemLimit,
            payload.ByteTarget,
            StoragePartitionMovePhase.SourceDeleteComplete);
        return (CreateJournalEntry(
            persistence,
            StorageJournalOperation.MoveDelete,
            payload,
            persistence.NextVersion), advanced);
    }

    private static StorageJournalEntry CreateAdvanceEntry(
        StoragePartitionPersistence persistence,
        StorageMoveIdentity move,
        long frozenNextVersion)
    {
        var payload = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            FrozenNextVersion = frozenNextVersion,
        };
        return CreateJournalEntry(
            persistence,
            StorageJournalOperation.AdvanceVersion,
            payload,
            Math.Max(persistence.NextVersion, frozenNextVersion));
    }

    private static StorageMoveJournalPayload CreateImportPayload(
        StorageMoveIdentity move,
        long frozenNextVersion)
    {
        return CreateImportPayload(
            move,
            frozenNextVersion,
            pageOrdinal: 0,
            afterRecordKey: null,
            recordKey: "import-record",
            etag: "9",
            exhausted: true);
    }

    private static StorageMoveJournalPayload CreateImportPayload(
        StorageMoveIdentity move,
        long frozenNextVersion,
        long pageOrdinal,
        byte[]? afterRecordKey,
        string recordKey,
        string etag,
        bool exhausted)
    {
        var item = StorageMoveRecordCodec.Encode(
            recordKey,
            StorageMovementProtocolTests.CreateRecord(recordKey, etag));
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            PageOrdinal = pageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(afterRecordKey),
            NextRecordKey = item.RecordKey,
            Exhausted = exhausted,
            FrozenNextVersion = frozenNextVersion,
            Imports = [item],
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.DefaultPageBytes,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(item),
        };
        return CopyWithDigest(StorageJournalOperation.Import, unsigned);
    }

    private static StorageMoveJournalPayload CreateDeletePayload(
        StorageMoveIdentity move,
        long frozenNextVersion)
    {
        return CreateDeletePayload(
            move,
            frozenNextVersion,
            pageOrdinal: 0,
            afterRecordKey: null,
            recordKey: "delete-record",
            etag: "1",
            exhausted: true);
    }

    private static StorageMoveJournalPayload CreateDeletePayload(
        StorageMoveIdentity move,
        long frozenNextVersion,
        long pageOrdinal,
        byte[]? afterRecordKey,
        string recordKey,
        string etag,
        bool exhausted)
    {
        var item = StorageMoveRecordCodec.EncodeDelete(recordKey, etag);
        var unsigned = new StorageMoveJournalPayload
        {
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            PageOrdinal = pageOrdinal,
            AfterRecordKey = StorageMoveRecordCodec.CopyText(afterRecordKey),
            NextRecordKey = item.RecordKey,
            Exhausted = exhausted,
            FrozenNextVersion = frozenNextVersion,
            Deletes = [item],
            ItemLimit = 1,
            ByteTarget = StorageMoveProtocol.DefaultPageBytes,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(item),
        };
        return CopyWithDigest(StorageJournalOperation.MoveDelete, unsigned);
    }

    private static StorageMoveJournalPayload CopyWithDigest(
        StorageJournalOperation operation,
        StorageMoveJournalPayload unsigned)
    {
        return new StorageMoveJournalPayload
        {
            MoveId = unsigned.MoveId,
            Slot = unsigned.Slot,
            VirtualSlotCount = unsigned.VirtualSlotCount,
            SourceEpoch = unsigned.SourceEpoch,
            SourceOwner = unsigned.SourceOwner,
            TargetOwner = unsigned.TargetOwner,
            PageOrdinal = unsigned.PageOrdinal,
            AfterRecordKey = unsigned.AfterRecordKey,
            NextRecordKey = unsigned.NextRecordKey,
            Exhausted = unsigned.Exhausted,
            PageDigest = StorageMovePageDigest.Compute(operation, unsigned),
            FrozenNextVersion = unsigned.FrozenNextVersion,
            Imports = unsigned.Imports,
            Deletes = unsigned.Deletes,
            ItemLimit = unsigned.ItemLimit,
            ByteTarget = unsigned.ByteTarget,
            EncodedByteCount = unsigned.EncodedByteCount,
        };
    }

    private static StorageJournalEntry CreateJournalEntry(
        StoragePartitionPersistence persistence,
        StorageJournalOperation operation,
        StorageMoveJournalPayload payload,
        long nextVersionAfter)
    {
        return new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = Guid.NewGuid(),
            PreviousOperationId = persistence.CommittedOperationId,
            Operation = operation,
            RecordKey = string.Empty,
            NextVersionAfter = nextVersionAfter,
            Move = payload,
        };
    }

    private static StoragePartitionMoveControl CreateTargetControl(
        StorageMoveIdentity move,
        StoragePartitionMovePhase phase,
        long frozenNextVersion)
    {
        var control = CreateControl(move, phase, frozenNextVersion);
        control.Role = StoragePartitionMoveRole.Target;
        return control;
    }

    private static StoragePartitionMoveControl CreateSourceControl(
        StorageMoveIdentity move,
        StoragePartitionMovePhase phase,
        long frozenNextVersion)
    {
        var control = CreateControl(move, phase, frozenNextVersion);
        control.Role = StoragePartitionMoveRole.Source;
        return control;
    }

    private static StoragePartitionMoveControl CreateControl(
        StorageMoveIdentity move,
        StoragePartitionMovePhase phase,
        long frozenNextVersion)
    {
        return new StoragePartitionMoveControl
        {
            IsPresent = true,
            MoveId = move.MoveId,
            Slot = move.Slot,
            VirtualSlotCount = move.VirtualSlotCount,
            SourceEpoch = move.SourceEpoch,
            SourceOwner = move.SourceOwner,
            TargetOwner = move.TargetOwner,
            Phase = phase,
            FrozenNextVersion = frozenNextVersion,
        };
    }

    private static StorageMoveIdentity CreateMoveIdentity(StoragePartitionMoveControl control)
    {
        return new StorageMoveIdentity
        {
            ProtocolVersion = StorageMoveProtocol.Version,
            MoveId = control.MoveId,
            Slot = control.Slot,
            VirtualSlotCount = control.VirtualSlotCount,
            SourceEpoch = control.SourceEpoch,
            SourceOwner = control.SourceOwner,
            TargetOwner = control.TargetOwner,
        };
    }

    private static void AssertPreCommitState(
        MovementFaultContext context,
        MovementWalCase operation,
        StoragePartitionPersistence persistence,
        IReadOnlyDictionary<string, StoredRecord> records)
    {
        var nonzeroPage = context.Entry.Move?.PageOrdinal > 0;
        switch (operation)
        {
            case MovementWalCase.AdvanceVersion:
                persistence.CommittedSequence.Should().Be(0);
                persistence.NextVersion.Should().Be(1);
                persistence.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetPrepared);
                records.Should().BeEmpty();
                break;
            case MovementWalCase.Import:
                persistence.CommittedSequence.Should().Be(nonzeroPage ? 2 : 1);
                persistence.NextVersion.Should().Be(10);
                persistence.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetImporting);
                persistence.MoveControl.NextPageOrdinal.Should().Be(nonzeroPage ? 1 : 0);
                if (nonzeroPage)
                {
                    records.Should().ContainKey("import-a");
                }
                else
                {
                    records.Should().BeEmpty();
                }

                break;
            case MovementWalCase.MoveDelete:
                persistence.CommittedSequence.Should().Be(nonzeroPage ? 3 : 1);
                persistence.NextVersion.Should().Be(nonzeroPage ? 3 : 2);
                persistence.MoveControl.Phase.Should().Be(nonzeroPage
                    ? StoragePartitionMovePhase.SourceDeleting
                    : StoragePartitionMovePhase.SourceHidden);
                persistence.MoveControl.NextPageOrdinal.Should().Be(nonzeroPage ? 1 : 0);
                records.Should().ContainKey(nonzeroPage ? "delete-b" : "delete-record");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }
    }

    private async Task AssertFinalRecoveredStateAsync(
        MovementFaultContext context,
        MovementWalCase operation)
    {
        var recovered = CreatePersistence(
            context.Manifest,
            context.PartitionKey,
            static () => { });
        var records = await recovered.ActivateAsync();
        var nonzeroPage = context.Entry.Move?.PageOrdinal > 0;
        switch (operation)
        {
            case MovementWalCase.AdvanceVersion:
                recovered.CommittedSequence.Should().Be(1);
                recovered.NextVersion.Should().Be(10);
                recovered.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetImporting);
                records.Should().BeEmpty();
                break;
            case MovementWalCase.Import:
                recovered.CommittedSequence.Should().Be(nonzeroPage ? 3 : 2);
                recovered.NextVersion.Should().Be(10);
                recovered.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.TargetImportComplete);
                recovered.MoveControl.ImportedRecordCount.Should().Be(nonzeroPage ? 2 : 1);
                if (nonzeroPage)
                {
                    records.Should().ContainKeys("import-a", "import-b");
                }
                else
                {
                    records.Should().ContainKey("import-record")
                        .WhoseValue.ETag.Should().Be("9");
                }

                break;
            case MovementWalCase.MoveDelete:
                recovered.CommittedSequence.Should().Be(nonzeroPage ? 4 : 2);
                recovered.NextVersion.Should().Be(nonzeroPage ? 3 : 2);
                recovered.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.SourceDeleteComplete);
                recovered.MoveControl.DeletedRecordCount.Should().Be(nonzeroPage ? 2 : 1);
                records.Should().BeEmpty();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }
    }

    private StoragePartitionPersistence CreatePersistence(
        TestPersistentState<StoragePartitionManifestState> manifest,
        string partitionKey,
        Action poisonActivation)
    {
        return new StoragePartitionPersistence(
            manifest,
            _fixture.Cluster.GrainFactory,
            partitionKey,
            poisonActivation,
            NullLogger<StoragePartitionPersistence>.Instance);
    }

    private IStorageJournalSegmentGrain GetJournal(string partitionKey, long sequence)
    {
        var absoluteSegmentIndex = StoragePersistence.GetAbsoluteSegmentIndex(
            sequence,
            Settings.JournalSegmentCapacity);
        var slotCount = StoragePersistence.GetJournalSlotCount(
            Settings.MaximumJournalReplayEntries,
            Settings.JournalSegmentCapacity);
        var slot = StoragePersistence.GetJournalSlotIndex(
            absoluteSegmentIndex,
            Settings.MaximumJournalReplayEntries,
            Settings.JournalSegmentCapacity);
        return _fixture.Cluster.GrainFactory.GetGrain<IStorageJournalSegmentGrain>(
            StoragePersistence.CreateJournalSlotKey(partitionKey, slot, slotCount));
    }

    private sealed record MovementFaultContext(
        TestPersistentState<StoragePartitionManifestState> Manifest,
        StoragePartitionPersistence Persistence,
        string PartitionKey,
        StorageJournalEntry Entry,
        StoragePartitionMoveControl AdvancedControl,
        Func<int> PoisonCount);
}

internal enum MovementWalCase
{
    AdvanceVersion = 1,
    Import = 2,
    MoveDelete = 3,
}
