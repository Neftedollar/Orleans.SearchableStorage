using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StorageIndexSchemaPersistenceTests
{
    private static readonly StoragePersistenceSettings Settings = new()
    {
        JournalSegmentCapacity = 2,
        MaximumJournalReplayEntries = 4,
        CompactionThreshold = 2,
    };

    private readonly MemoryStorageFixture _fixture;

    public StorageIndexSchemaPersistenceTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void PersistenceV5RequiresSchemaProtocolOneAndOlderFormatsRejectIt()
    {
        var v5WithoutCapability = CreateEmptyManifest(
            StoragePersistence.CurrentPersistenceFormatVersion,
            indexSchemaProtocolVersion: 0);
        var v4WithCapability = CreateEmptyManifest(
            StoragePersistence.MovementPersistenceFormatVersion,
            StorageIndexSchema.ProtocolVersion);
        var validV5 = CreateEmptyManifest(
            StoragePersistence.CurrentPersistenceFormatVersion,
            StorageIndexSchema.ProtocolVersion);

        var validateMissing = () => StoragePartitionPersistence.ValidateManifest(
            v5WithoutCapability,
            allowPreviousFormat: true);
        var validateOld = () => StoragePartitionPersistence.ValidateManifest(
            v4WithCapability,
            allowPreviousFormat: true);
        var validateCurrent = () => StoragePartitionPersistence.ValidateManifest(
            validV5,
            allowPreviousFormat: true);

        validateMissing.Should().Throw<InvalidOperationException>()
            .WithMessage("*lacks*schema*capability*");
        validateOld.Should().Throw<InvalidOperationException>()
            .WithMessage("*v3/v4*index-schema*");
        validateCurrent.Should().NotThrow();
    }

    [Theory]
    [InlineData(StoragePersistence.LegacyPersistenceFormatVersion)]
    [InlineData(StoragePersistence.MovementPersistenceFormatVersion)]
    public void OlderPersistenceFormatsRejectManagedWalAndSnapshotData(int formatVersion)
    {
        var fingerprint = CreateFingerprint();
        var managed = CreateRecord("1", fingerprint);
        var upsert = CreateRecordEntry(StorageJournalOperation.Upsert, managed);
        var reindex = CreateRecordEntry(StorageJournalOperation.Reindex, managed);
        var import = new StorageJournalEntry
        {
            Operation = StorageJournalOperation.Import,
            Move = new StorageMoveJournalPayload
            {
                Imports = [StorageMoveRecordCodec.Encode("vacancy/record", managed)],
            },
            RecordKey = string.Empty,
        };
        var descriptor = CreateSnapshotDescriptor();
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["vacancy/record"] = managed,
        };

        var validateUpsert = () => StoragePartitionPersistence.ValidateJournalCapability(
            upsert,
            formatVersion,
            indexSchemaProtocolVersion: 0,
            "Test journal");
        var validateReindex = () => StoragePartitionPersistence.ValidateJournalCapability(
            reindex,
            formatVersion,
            indexSchemaProtocolVersion: 0,
            "Test journal");
        var validateImport = () => StoragePartitionPersistence.ValidateJournalCapability(
            import,
            formatVersion,
            indexSchemaProtocolVersion: 0,
            "Test journal");
        var createSnapshot = () => StorageSnapshotFactory.Create(
            descriptor,
            records,
            formatVersion);

        validateUpsert.Should().Throw<InvalidOperationException>();
        validateReindex.Should().Throw<InvalidOperationException>();
        validateImport.Should().Throw<InvalidOperationException>();
        createSnapshot.Should().Throw<InvalidOperationException>()
            .WithMessage("*v3/v4 snapshot*managed*");
    }

    [Fact]
    public void PersistenceV5SchemaOneAcceptsManagedWalAndSnapshotData()
    {
        var fingerprint = CreateFingerprint();
        var managed = CreateRecord("1", fingerprint);
        var entries = new[]
        {
            CreateRecordEntry(StorageJournalOperation.Upsert, managed),
            CreateRecordEntry(StorageJournalOperation.Reindex, managed),
            new StorageJournalEntry
            {
                Operation = StorageJournalOperation.Import,
                Move = new StorageMoveJournalPayload
                {
                    Imports = [StorageMoveRecordCodec.Encode("vacancy/record", managed)],
                },
                RecordKey = string.Empty,
            },
        };

        foreach (var entry in entries)
        {
            var validate = () => StoragePartitionPersistence.ValidateJournalCapability(
                entry,
                StoragePersistence.CurrentPersistenceFormatVersion,
                StorageIndexSchema.ProtocolVersion,
                "Test journal");
            validate.Should().NotThrow();
        }

        var snapshot = StorageSnapshotFactory.Create(
            CreateSnapshotDescriptor(),
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["vacancy/record"] = managed,
            },
            StoragePersistence.CurrentPersistenceFormatVersion);
        var decoded = StorageSnapshotFactory.DecodeRecords(
            snapshot,
            StoragePersistence.CurrentPersistenceFormatVersion);

        decoded["vacancy/record"].IndexSchemaFingerprint.Should().Equal(fingerprint);
    }

    [Fact]
    public async Task ExplicitEnablementIsDurableForAnEmptyPartitionAndRecoversALostAcknowledgement()
    {
        var durableManifest = new TestPersistentState<StoragePartitionManifestState>();
        var durablePartitionKey = $"schema-empty-{Guid.NewGuid():N}:00000000";
        var durable = CreatePersistence(durableManifest, durablePartitionKey);

        await durable.EnableIndexSchemaProtocolAsync(Settings);

        durableManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.CurrentPersistenceFormatVersion);
        durableManifest.State.IndexSchemaProtocolVersion.Should()
            .Be(StorageIndexSchema.ProtocolVersion);
        durableManifest.State.CommittedSequence.Should().Be(0);
        (await CreatePersistence(durableManifest, durablePartitionKey).ActivateAsync())
            .Should().BeEmpty();

        var injected = new InvalidOperationException("Lost schema capability acknowledgement.");
        var ambiguousManifest = new TestPersistentState<StoragePartitionManifestState>
        {
            WriteException = injected,
        };
        var poisonCount = 0;
        var ambiguousPartitionKey = $"schema-lost-ack-{Guid.NewGuid():N}:00000000";
        var ambiguous = CreatePersistence(
            ambiguousManifest,
            ambiguousPartitionKey,
            () => poisonCount++);

        var enable = () => ambiguous.EnableIndexSchemaProtocolAsync(Settings);
        await enable.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);
        poisonCount.Should().Be(1);
        ambiguousManifest.LastWriteState.Should().NotBeNull();
        ambiguousManifest.LastWriteState!.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.CurrentPersistenceFormatVersion);
        ambiguousManifest.LastWriteState.IndexSchemaProtocolVersion.Should()
            .Be(StorageIndexSchema.ProtocolVersion);

        ambiguousManifest.State = ambiguousManifest.LastWriteState;
        ambiguousManifest.WriteException = null;
        var recovered = CreatePersistence(ambiguousManifest, ambiguousPartitionKey);
        (await recovered.ActivateAsync()).Should().BeEmpty();
        recovered.IndexSchemaProtocolVersion.Should().Be(StorageIndexSchema.ProtocolVersion);
    }

    [Fact]
    public async Task ExplicitEnablementNormalizesAPreMovementV3RoutingEpochDefault()
    {
        var manifest = new TestPersistentState<StoragePartitionManifestState>
        {
            State = CreateEmptyManifest(
                StoragePersistence.LegacyPersistenceFormatVersion,
                indexSchemaProtocolVersion: 0),
        };
        manifest.State.MinimumRoutingEpoch = 0;
        var partitionKey = $"schema-v3-default-epoch-{Guid.NewGuid():N}:00000000";
        var persistence = CreatePersistence(manifest, partitionKey);

        await persistence.EnableIndexSchemaProtocolAsync(Settings);

        manifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.CurrentPersistenceFormatVersion);
        manifest.State.IndexSchemaProtocolVersion.Should()
            .Be(StorageIndexSchema.ProtocolVersion);
        manifest.State.MinimumRoutingEpoch.Should().Be(1);
        StoragePartitionPersistence.ValidateManifest(
            manifest.State,
            allowPreviousFormat: true);
        (await CreatePersistence(manifest, partitionKey).ActivateAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task MovementOnlyStaysV4AndSchemaAwareMovementCannotDowngrade()
    {
        var movementManifest = new TestPersistentState<StoragePartitionManifestState>();
        var movementOnly = CreatePersistence(
            movementManifest,
            $"movement-only-{Guid.NewGuid():N}:00000000");

        await movementOnly.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);

        movementManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.MovementPersistenceFormatVersion);
        movementManifest.State.IndexSchemaProtocolVersion.Should().Be(0);

        var managedManifest = new TestPersistentState<StoragePartitionManifestState>();
        var managed = CreatePersistence(
            managedManifest,
            $"schema-then-movement-{Guid.NewGuid():N}:00000000");
        await managed.EnableIndexSchemaProtocolAsync(Settings);
        await managed.EnableMovementProtocolAsync(Settings, minimumRoutingEpoch: 1);

        managedManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.CurrentPersistenceFormatVersion);
        managedManifest.State.IndexSchemaProtocolVersion.Should()
            .Be(StorageIndexSchema.ProtocolVersion);

        var targetManifest = new TestPersistentState<StoragePartitionManifestState>();
        var target = CreatePersistence(
            targetManifest,
            $"schema-aware-target-{Guid.NewGuid():N}:00000001");
        await target.EnableMovementProtocolAsync(
            Settings,
            minimumRoutingEpoch: 1,
            indexSchemaProtocolVersion: StorageIndexSchema.ProtocolVersion);

        targetManifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.CurrentPersistenceFormatVersion);
        targetManifest.State.IndexSchemaProtocolVersion.Should()
            .Be(StorageIndexSchema.ProtocolVersion);
        target.CreateProtocolState().IndexSchemaProtocolVersion.Should()
            .Be(StorageIndexSchema.ProtocolVersion);
    }

    [Fact]
    public async Task ReindexCompactionAndReactivationPreserveManagedRecordIdentityAndVersions()
    {
        const string recordKey = "vacancy/record";
        var partitionKey = $"schema-reindex-snapshot-{Guid.NewGuid():N}:00000000";
        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var persistence = CreatePersistence(manifest, partitionKey);
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var legacy = new StoredRecord
        {
            GrainId = GrainId.Create("vacancy", "record"),
            Payload = [1, 2, 3],
            ETag = "1",
            IndexEntries = [],
        };

        await persistence.PrepareForMutationAsync(records, Settings);
        var upsertId = Guid.NewGuid();
        await persistence.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = upsertId,
            PreviousOperationId = persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = recordKey,
            Record = StoragePersistenceStateCopy.CopyRecord(legacy),
            NextVersionAfter = 2,
        });
        records.Add(recordKey, StoragePersistenceStateCopy.CopyRecord(legacy)!);

        await persistence.EnableIndexSchemaProtocolAsync(Settings);
        var fingerprint = CreateFingerprint();
        var boundScope = IndexSchemaIdentity.BindScope("vacancy-city", fingerprint);
        var replacement = new StoredRecord
        {
            GrainId = legacy.GrainId,
            Payload = [.. legacy.Payload],
            ETag = legacy.ETag,
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = boundScope,
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create("Moscow"),
                },
            ],
            IndexSchemaFingerprint = fingerprint,
        };
        await persistence.PrepareForMutationAsync(records, Settings);
        var reindexId = Guid.NewGuid();
        await persistence.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = reindexId,
            PreviousOperationId = upsertId,
            Operation = StorageJournalOperation.Reindex,
            RecordKey = recordKey,
            ExpectedETag = legacy.ETag,
            Record = StoragePersistenceStateCopy.CopyRecord(replacement),
            NextVersionAfter = 2,
        });
        records[recordKey] = StoragePersistenceStateCopy.CopyRecord(replacement)!;

        await persistence.CompactAsync(records);

        manifest.State.PersistenceFormatVersion.Should()
            .Be(StoragePersistence.CurrentPersistenceFormatVersion);
        manifest.State.IndexSchemaProtocolVersion.Should()
            .Be(StorageIndexSchema.ProtocolVersion);
        manifest.State.CommittedSequence.Should().Be(2);
        manifest.State.CommittedOperationId.Should().Be(reindexId);
        manifest.State.NextVersion.Should().Be(2);
        manifest.State.SnapshotSequence.Should().Be(2);
        manifest.State.ActiveSnapshot.NextVersion.Should().Be(2);
        manifest.State.ActiveSnapshot.OperationId.Should().Be(reindexId);

        var snapshot = await _fixture.Cluster.GrainFactory.GetGrain<IStorageSnapshotGrain>(
                StoragePersistence.CreateSnapshotSlotKey(
                    partitionKey,
                    manifest.State.ActiveSnapshot.Slot))
            .ReadAsync();
        var compacted = StorageSnapshotFactory.DecodeRecords(
            snapshot,
            StoragePersistence.CurrentPersistenceFormatVersion);
        AssertManagedRecord(compacted[recordKey], replacement, fingerprint, boundScope);

        var reactivated = CreatePersistence(manifest, partitionKey);
        var recovered = await reactivated.ActivateAsync();
        reactivated.CommittedSequence.Should().Be(2);
        reactivated.CommittedOperationId.Should().Be(reindexId);
        reactivated.NextVersion.Should().Be(2);
        reactivated.IndexSchemaProtocolVersion.Should().Be(StorageIndexSchema.ProtocolVersion);
        AssertManagedRecord(recovered[recordKey], replacement, fingerprint, boundScope);
    }

    [Fact]
    public async Task LostReindexManifestAcknowledgementReplaysTheCommittedManagedGeneration()
    {
        const string recordKey = "vacancy/lost-ack";
        var partitionKey = $"schema-reindex-lost-ack-{Guid.NewGuid():N}:00000000";
        var manifest = new TestPersistentState<StoragePartitionManifestState>();
        var poisonCount = 0;
        var persistence = CreatePersistence(manifest, partitionKey, () => poisonCount++);
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var legacy = new StoredRecord
        {
            GrainId = GrainId.Create("vacancy", "lost-ack"),
            Payload = [4, 5, 6],
            ETag = "1",
            IndexEntries = [],
        };

        await persistence.PrepareForMutationAsync(records, Settings);
        var upsertId = Guid.NewGuid();
        await persistence.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = upsertId,
            PreviousOperationId = persistence.CommittedOperationId,
            Operation = StorageJournalOperation.Upsert,
            RecordKey = recordKey,
            Record = StoragePersistenceStateCopy.CopyRecord(legacy),
            NextVersionAfter = 2,
        });
        records.Add(recordKey, StoragePersistenceStateCopy.CopyRecord(legacy)!);
        await persistence.EnableIndexSchemaProtocolAsync(Settings);

        var fingerprint = CreateFingerprint();
        var scope = IndexSchemaIdentity.BindScope("vacancy-city", fingerprint);
        var replacement = CreateRecord("1", fingerprint, scope);
        replacement = new StoredRecord
        {
            GrainId = legacy.GrainId,
            Payload = [.. legacy.Payload],
            ETag = legacy.ETag,
            IndexEntries = replacement.IndexEntries,
            IndexSchemaFingerprint = [.. fingerprint],
        };
        await persistence.PrepareForMutationAsync(records, Settings);
        var reindexId = Guid.NewGuid();
        var injected = new InvalidOperationException("Lost reindex manifest acknowledgement.");
        manifest.WriteException = injected;

        var commit = () => persistence.CommitAsync(new StorageJournalEntry
        {
            Sequence = persistence.NextSequence,
            WriterEpoch = persistence.WriterEpoch,
            OperationId = reindexId,
            PreviousOperationId = upsertId,
            Operation = StorageJournalOperation.Reindex,
            RecordKey = recordKey,
            ExpectedETag = legacy.ETag,
            Record = StoragePersistenceStateCopy.CopyRecord(replacement),
            NextVersionAfter = 2,
        });
        await commit.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);
        poisonCount.Should().Be(1);
        manifest.LastWriteState.Should().NotBeNull();
        manifest.LastWriteState!.CommittedSequence.Should().Be(2);
        manifest.LastWriteState.CommittedOperationId.Should().Be(reindexId);
        manifest.LastWriteState.NextVersion.Should().Be(2);

        manifest.State = manifest.LastWriteState;
        manifest.WriteException = null;
        var recoveredPersistence = CreatePersistence(manifest, partitionKey);
        var recovered = await recoveredPersistence.ActivateAsync();

        recoveredPersistence.CommittedOperationId.Should().Be(reindexId);
        recoveredPersistence.NextVersion.Should().Be(2);
        AssertManagedRecord(recovered[recordKey], replacement, fingerprint, scope);
    }

    [Fact]
    public void ManagedMovementImportsRequireFingerprintStateKeyAndBoundScopes()
    {
        const string providerName = "provider";
        const string stateName = "vacancy";
        var registration = new SearchableStateRegistration<ManagedVacancyState>(
            providerName,
            stateName);
        var registry = new SearchableStateRegistry([registration], options: null);
        var fingerprint = registration.Schema.Fingerprint;
        var validRecord = CreateRecord(
            "1",
            fingerprint,
            IndexSchemaIdentity.BindScope("vacancy-city", fingerprint));
        var valid = StorageMoveRecordCodec.Encode("vacancy/record", validRecord);

        var validate = () => StoragePartitionGrain.ValidateImportedSchemaBindings(
            providerName,
            StorageIndexSchema.ProtocolVersion,
            registry,
            [valid]);
        validate.Should().NotThrow();

        var wrongKey = StorageMoveRecordCodec.Encode("other/record", validRecord);
        var validateWrongKey = () => StoragePartitionGrain.ValidateImportedSchemaBindings(
            providerName,
            StorageIndexSchema.ProtocolVersion,
            registry,
            [wrongKey]);
        validateWrongKey.Should().Throw<InvalidOperationException>()
            .WithMessage("*record key*different state*");

        byte[] otherFingerprint = [.. fingerprint];
        otherFingerprint[0] ^= 0xFF;
        var wrongScope = StorageMoveRecordCodec.Encode(
            "vacancy/record",
            CreateRecord(
                "1",
                fingerprint,
                IndexSchemaIdentity.BindScope("vacancy-city", otherFingerprint)));
        var validateWrongScope = () => StoragePartitionGrain.ValidateImportedSchemaBindings(
            providerName,
            StorageIndexSchema.ProtocolVersion,
            registry,
            [wrongScope]);
        validateWrongScope.Should().Throw<InvalidOperationException>()
            .WithMessage("*index scope*different schema generation*");

        var unbound = StorageMoveRecordCodec.Encode(
            "vacancy/record",
            CreateRecord("1", fingerprint: null));
        var validateUnbound = () => StoragePartitionGrain.ValidateImportedSchemaBindings(
            providerName,
            StorageIndexSchema.ProtocolVersion,
            registry,
            [unbound]);
        validateUnbound.Should().Throw<InvalidOperationException>()
            .WithMessage("*without a matching local state registration*");
    }

    private StoragePartitionPersistence CreatePersistence(
        TestPersistentState<StoragePartitionManifestState> manifest,
        string partitionKey,
        Action? poisonActivation = null)
    {
        return new StoragePartitionPersistence(
            manifest,
            _fixture.Cluster.GrainFactory,
            partitionKey,
            poisonActivation ?? (static () => { }),
            NullLogger<StoragePartitionPersistence>.Instance);
    }

    private static StoragePartitionManifestState CreateEmptyManifest(
        int formatVersion,
        int indexSchemaProtocolVersion)
    {
        return new StoragePartitionManifestState
        {
            Initialized = true,
            PersistenceFormatVersion = formatVersion,
            JournalSegmentCapacity = Settings.JournalSegmentCapacity,
            MaximumJournalReplayEntries = Settings.MaximumJournalReplayEntries,
            NextVersion = 1,
            MinimumRoutingEpoch = 1,
            MoveControl = new StoragePartitionMoveControl(),
            IndexSchemaProtocolVersion = indexSchemaProtocolVersion,
        };
    }

    private static StorageJournalEntry CreateRecordEntry(
        StorageJournalOperation operation,
        StoredRecord record)
    {
        return new StorageJournalEntry
        {
            Operation = operation,
            RecordKey = "vacancy/record",
            Record = record,
        };
    }

    private static StorageSnapshotDescriptor CreateSnapshotDescriptor()
    {
        return new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = Guid.NewGuid(),
            Sequence = 1,
            OperationId = Guid.NewGuid(),
            NextVersion = 2,
        };
    }

    private static StoredRecord CreateRecord(
        string etag,
        byte[]? fingerprint,
        string? scope = null)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("vacancy", "record"),
            Payload = [1, 2, 3],
            ETag = etag,
            IndexEntries = scope is null
                ? []
                :
                [
                    new IndexEntry
                    {
                        Scope = scope,
                        Kind = SearchableIndexKind.Hash,
                        Value = IndexValue.Create("Moscow"),
                    },
                ],
            IndexSchemaFingerprint = fingerprint is null ? null : [.. fingerprint],
        };
    }

    private static byte[] CreateFingerprint() =>
        Enumerable.Range(0, IndexSchemaDefinition.FingerprintLength)
            .Select(static value => checked((byte)value))
            .ToArray();

    private static void AssertManagedRecord(
        StoredRecord actual,
        StoredRecord expected,
        byte[] fingerprint,
        string scope)
    {
        actual.GrainId.Should().Be(expected.GrainId);
        actual.Payload.Should().Equal(expected.Payload);
        actual.ETag.Should().Be(expected.ETag);
        actual.IndexSchemaFingerprint.Should().Equal(fingerprint);
        actual.IndexEntries.Should().ContainSingle();
        actual.IndexEntries[0].Scope.Should().Be(scope);
        actual.IndexEntries[0].Value.Text.Should().Be("Moscow");
    }

    private sealed class ManagedVacancyState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;
    }
}
