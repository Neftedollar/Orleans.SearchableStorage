using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

/// <summary>
/// Freezes the durable boundary between payload-owning storage and index-only namespaces.
/// These tests deliberately exercise persisted representations instead of treating the mode as
/// registration-only metadata: an older binary must reject format 6, and an index-only record
/// must remain distinguishable from an integrated record whose application payload is empty.
/// </summary>
public sealed class IndexOnlyDurableProtocolTests
{
    [Theory]
    [InlineData((int)StorageNamespaceMode.Integrated, StorageLayout.MovementFormatVersion)]
    [InlineData((int)StorageNamespaceMode.IndexOnly, StorageLayout.IndexOnlyFormatVersion)]
    public async Task FreshLayoutPersistsTheModeSpecificFormatAndCannotReopenInAnotherMode(
        int modeValue,
        int expectedFormatVersion)
    {
        var mode = (StorageNamespaceMode)modeValue;
        var providerName = $"mode-{mode}-{Guid.NewGuid():N}";
        var persistentState = new TestPersistentState<StorageLayoutState>();
        var layout = new StorageLayoutGrain(
            persistentState,
            providerName,
            requestDeactivation: static () => { });
        var descriptor = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 2,
            namespaceMode: mode);

        var initialized = await layout.InitializeRoutingAsync(descriptor);

        initialized.FormatVersion.Should().Be(expectedFormatVersion);
        initialized.NamespaceMode.Should().Be(mode);
        persistentState.State.FormatVersion.Should().Be(expectedFormatVersion);
        persistentState.State.NamespaceMode.Should().Be(mode);

        var otherMode = mode == StorageNamespaceMode.Integrated
            ? StorageNamespaceMode.IndexOnly
            : StorageNamespaceMode.Integrated;
        var incompatible = StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 2,
            namespaceMode: otherMode);

        Func<Task> reopen = async () => await layout.InitializeRoutingAsync(incompatible);

        await reopen.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*persisted*namespace mode*");
        persistentState.State.FormatVersion.Should().Be(expectedFormatVersion);
        persistentState.State.NamespaceMode.Should().Be(mode);
    }

    [Theory]
    [InlineData(StorageLayout.MovementFormatVersion, (int)StorageNamespaceMode.Integrated, true)]
    [InlineData(StorageLayout.MovementFormatVersion, (int)StorageNamespaceMode.IndexOnly, false)]
    [InlineData(StorageLayout.IndexSchemaFormatVersion, (int)StorageNamespaceMode.Integrated, true)]
    [InlineData(StorageLayout.IndexSchemaFormatVersion, (int)StorageNamespaceMode.IndexOnly, false)]
    [InlineData(StorageLayout.IndexOnlyFormatVersion, (int)StorageNamespaceMode.Integrated, false)]
    [InlineData(StorageLayout.IndexOnlyFormatVersion, (int)StorageNamespaceMode.IndexOnly, true)]
    public async Task PersistedRoutingFormatsAdmitOnlyTheirDefinedNamespaceMode(
        int formatVersion,
        int modeValue,
        bool valid)
    {
        var mode = (StorageNamespaceMode)modeValue;
        var providerName = $"persisted-mode-{Guid.NewGuid():N}";
        var persistentState = new TestPersistentState<StorageLayoutState>
        {
            State = CreateRoutingState(providerName, formatVersion, mode),
        };
        var layout = new StorageLayoutGrain(
            persistentState,
            providerName,
            requestDeactivation: static () => { });

        if (valid)
        {
            var snapshot = await layout.GetCurrentLayoutAsync();
            snapshot.Should().NotBeNull();
            snapshot!.FormatVersion.Should().Be(formatVersion);
            snapshot.NamespaceMode.Should().Be(mode);
        }
        else
        {
            Func<Task> read = async () => await layout.GetCurrentLayoutAsync();
            await read.Should().ThrowAsync<InvalidOperationException>();
        }
    }

    [Fact]
    public void AppendedModeFieldsDefaultEveryPreFormatSixDocumentToIntegrated()
    {
        var layout = new StorageLayoutState();
        var manifest = new StoragePartitionManifestState();

        layout.NamespaceMode.Should().Be(StorageNamespaceMode.Integrated);
        manifest.NamespaceMode.Should().Be(StorageNamespaceMode.Integrated);
    }

    [Theory]
    [InlineData(StoragePersistence.LegacyPersistenceFormatVersion, (int)StorageNamespaceMode.Integrated, true)]
    [InlineData(StoragePersistence.LegacyPersistenceFormatVersion, (int)StorageNamespaceMode.IndexOnly, false)]
    [InlineData(StoragePersistence.MovementPersistenceFormatVersion, (int)StorageNamespaceMode.Integrated, true)]
    [InlineData(StoragePersistence.MovementPersistenceFormatVersion, (int)StorageNamespaceMode.IndexOnly, false)]
    [InlineData(StoragePersistence.CurrentPersistenceFormatVersion, (int)StorageNamespaceMode.Integrated, true)]
    [InlineData(StoragePersistence.CurrentPersistenceFormatVersion, (int)StorageNamespaceMode.IndexOnly, false)]
    [InlineData(StoragePersistence.IndexOnlyPersistenceFormatVersion, (int)StorageNamespaceMode.Integrated, false)]
    [InlineData(StoragePersistence.IndexOnlyPersistenceFormatVersion, (int)StorageNamespaceMode.IndexOnly, true)]
    public void PartitionPersistenceFormatsAdmitOnlyTheirDefinedNamespaceMode(
        int formatVersion,
        int modeValue,
        bool valid)
    {
        var mode = (StorageNamespaceMode)modeValue;
        var manifest = CreateEmptyManifest(formatVersion, mode);

        Action validate = () => StoragePartitionPersistence.ValidateManifest(
            manifest,
            allowPreviousFormat: true);

        if (valid)
        {
            validate.Should().NotThrow();
        }
        else
        {
            validate.Should().Throw<InvalidOperationException>();
        }
    }

    [Fact]
    public void FormatSixCreatesASeparateRoutingContinuationDomain()
    {
        const string providerName = "routing-fingerprint-mode-boundary";
        var integrated = StorageLayoutSnapshot.FromState(CreateRoutingState(
            providerName,
            StorageLayout.IndexSchemaFormatVersion,
            StorageNamespaceMode.Integrated));
        var indexOnly = StorageLayoutSnapshot.FromState(CreateRoutingState(
            providerName,
            StorageLayout.IndexOnlyFormatVersion,
            StorageNamespaceMode.IndexOnly));

        StorageLayoutFingerprint.Compute(indexOnly)
            .Should().NotEqual(StorageLayoutFingerprint.Compute(integrated));
    }

    [Fact]
    public void NullPayloadIsReservedForIndexOnlyAndEmptyPayloadRemainsIntegrated()
    {
        var integrated = CreateRecord(payload: []);
        var indexOnly = CreateRecord(payload: null);

        StoredRecordNamespaceValidation.Validate(
            integrated,
            StorageNamespaceMode.Integrated);
        StoredRecordNamespaceValidation.Validate(
            indexOnly,
            StorageNamespaceMode.IndexOnly);

        Action integratedWithNull = () => StoredRecordNamespaceValidation.Validate(
            indexOnly,
            StorageNamespaceMode.Integrated);
        Action indexOnlyWithEmpty = () => StoredRecordNamespaceValidation.Validate(
            integrated,
            StorageNamespaceMode.IndexOnly);

        integratedWithNull.Should().Throw<InvalidOperationException>();
        indexOnlyWithEmpty.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void FormatSixSnapshotRoundTripsAnAbsentPayloadWithoutInventingEmptyBytes()
    {
        var descriptor = CreateSnapshotDescriptor();
        var record = CreateRecord(payload: null);
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["document/one"] = record,
        };

        var snapshot = StorageSnapshotFactory.Create(
            descriptor,
            records,
            StoragePersistence.IndexOnlyPersistenceFormatVersion);
        var recovered = StorageSnapshotFactory.DecodeRecords(
            snapshot,
            StoragePersistence.IndexOnlyPersistenceFormatVersion);

        snapshot.RecordEncodingVersion.Should()
            .Be(StorageSnapshotFactory.LosslessRecordEncodingVersion);
        snapshot.LosslessRecords.Should().ContainSingle();
        snapshot.LosslessRecords[0].Record.Payload.Should().BeNull();
        recovered.Should().ContainKey("document/one");
        recovered["document/one"].Payload.Should().BeNull();
        StoredRecordNamespaceValidation.ValidateAll(
            recovered.Values,
            StorageNamespaceMode.IndexOnly);
    }

    [Fact]
    public void MovementCodecAndDigestDistinguishAbsentPayloadFromEmptyPayload()
    {
        var integrated = StorageMoveRecordCodec.Encode(
            "document/one",
            CreateRecord(payload: []));
        var indexOnly = StorageMoveRecordCodec.Encode(
            "document/one",
            CreateRecord(payload: null));

        integrated.Record.Payload.Should().NotBeNull().And.BeEmpty();
        indexOnly.Record.Payload.Should().BeNull();
        StorageMoveRecordCodec.BinaryEquals(integrated, indexOnly).Should().BeFalse();
        StorageMoveRecordCodec.Decode(indexOnly.Record).Payload.Should().BeNull();

        var integratedPage = CreateImportPage(integrated);
        var indexOnlyPage = CreateImportPage(indexOnly);
        var integratedDigest = StorageMovePageDigest.Compute(
            StorageJournalOperation.Import,
            integratedPage);
        var indexOnlyDigest = StorageMovePageDigest.Compute(
            StorageJournalOperation.Import,
            indexOnlyPage);
        var exactRetryDigest = StorageMovePageDigest.Compute(
            StorageJournalOperation.Import,
            CreateImportPage(StorageMoveRecordCodec.Copy(indexOnly)));

        indexOnlyDigest.Should().NotEqual(integratedDigest);
        exactRetryDigest.Should().Equal(indexOnlyDigest);
    }

    private static StorageLayoutState CreateRoutingState(
        string providerName,
        int formatVersion,
        StorageNamespaceMode mode)
    {
        return new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = formatVersion,
            ProviderName = providerName,
            PartitionCount = 2,
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            VirtualSlotCount = 4,
            SlotAssignments = [0, 1, 0, 1],
            Epoch = 1,
            IndexSchemaProtocolVersion = formatVersion is StorageLayout.IndexSchemaFormatVersion
                or StorageLayout.IndexOnlyFormatVersion
                    ? StorageIndexSchema.ProtocolVersion
                    : 0,
            NamespaceMode = mode,
        };
    }

    private static StoragePartitionManifestState CreateEmptyManifest(
        int formatVersion,
        StorageNamespaceMode mode)
    {
        return new StoragePartitionManifestState
        {
            Initialized = true,
            PersistenceFormatVersion = formatVersion,
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            NextVersion = 1,
            MinimumRoutingEpoch = formatVersion == StoragePersistence.LegacyPersistenceFormatVersion
                ? 0
                : 1,
            MoveControl = new StoragePartitionMoveControl(),
            IndexSchemaProtocolVersion = StoragePersistence.SupportsIndexSchemas(formatVersion)
                ? StorageIndexSchema.ProtocolVersion
                : 0,
            NamespaceMode = mode,
        };
    }

    private static StoredRecord CreateRecord(byte[]? payload)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("index-only-protocol", "one"),
            Payload = payload,
            ETag = "1",
            IndexEntries = [],
            IndexSchemaFingerprint = Enumerable.Range(1, IndexSchemaDefinition.FingerprintLength)
                .Select(static value => checked((byte)value))
                .ToArray(),
        };
    }

    private static StorageSnapshotDescriptor CreateSnapshotDescriptor()
    {
        return new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = Guid.Parse("214db02a-6618-46f4-9cff-989bb13b990d"),
            Sequence = 1,
            OperationId = Guid.Parse("f0cb8996-7cc0-4f82-97c8-8ce487ab5c51"),
            NextVersion = 2,
        };
    }

    private static StorageMoveJournalPayload CreateImportPage(StorageMoveRecord record)
    {
        var imports = new List<StorageMoveRecord> { record };
        return new StorageMoveJournalPayload
        {
            MoveId = Guid.Parse("b58d5a3c-0cf8-4128-bb48-84e564e1f384"),
            Slot = 0,
            VirtualSlotCount = 1,
            SourceEpoch = 1,
            SourceOwner = 0,
            TargetOwner = 1,
            PageOrdinal = 0,
            Exhausted = true,
            FrozenNextVersion = 2,
            Imports = imports,
            Deletes = [],
            ItemLimit = 1,
            ByteTarget = SearchableStorageMovementOptions.DefaultTransferPageByteTarget,
            EncodedByteCount = StorageMovePageDigest.GetEncodedByteCount(imports),
            PageDigest = [],
        };
    }
}
