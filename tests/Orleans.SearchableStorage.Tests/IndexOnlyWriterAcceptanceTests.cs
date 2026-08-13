using System.Net;
using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexOnlyWriterAcceptanceTests
    : IClassFixture<IndexOnlyWriterAcceptanceFixture>
{
    private readonly IndexOnlyWriterAcceptanceFixture _fixture;

    public IndexOnlyWriterAcceptanceTests(IndexOnlyWriterAcceptanceFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExternalClientDiSelectsTheIndexOnlyRoutingIdentity()
    {
        await EnsureSchemasActiveAsync();
        var clientServices = _fixture.Cluster.ServiceProvider;

        clientServices.GetRequiredKeyedService<ISearchableStorageIndexWriter>(
                IndexOnlyAcceptanceNames.IndexProvider)
            .Should().NotBeNull();
        clientServices.GetRequiredKeyedService<ISearchableStorageAdminClient>(
                IndexOnlyAcceptanceNames.IndexProvider)
            .Should().NotBeNull();
        clientServices.GetKeyedService<IGrainStorage>(IndexOnlyAcceptanceNames.IndexProvider)
            .Should().BeNull();

        var query = clientServices.GetRequiredKeyedService<ISearchableStorageQueryClient>(
            IndexOnlyAcceptanceNames.IndexProvider);
        var missingCity = $"external-client-{Guid.NewGuid():N}";
        var matches = await query
            .Query<IndexOnlyAcceptanceState>(IndexOnlyAcceptanceNames.StateName)
            .Where(state => state.City == missingCity)
            .ToGrainIdsAsync();

        matches.Should().BeEmpty();
    }

    [Fact]
    public async Task ExternalPersistentStateAndIndexWriterInjectTogetherWithoutStorageCrossTalk()
    {
        await EnsureSchemasActiveAsync();
        var services = GetPrimaryServices();

        services.GetRequiredKeyedService<IGrainStorage>(IndexOnlyAcceptanceNames.FullProvider)
            .Should().NotBeNull();
        services.GetKeyedService<ISearchableStorageIndexWriter>(
                IndexOnlyAcceptanceNames.FullProvider)
            .Should().BeNull();
        services.GetRequiredKeyedService<ISearchableStorageIndexWriter>(
                IndexOnlyAcceptanceNames.IndexProvider)
            .Should().NotBeNull();
        services.GetKeyedService<IGrainStorage>(IndexOnlyAcceptanceNames.IndexProvider)
            .Should().BeNull();

        var key = $"coexist-{Guid.NewGuid():N}";
        var full = _fixture.Cluster.GrainFactory.GetGrain<IFullSearchStateGrain>(key);
        var external = _fixture.Cluster.GrainFactory.GetGrain<IExternallyStoredSearchStateGrain>(key);
        var fullState = new IndexOnlyAcceptanceState
        {
            City = $"full-{Guid.NewGuid():N}",
            Score = 10,
            UnindexedPayload = $"full-secret-{Guid.NewGuid():N}",
        };
        var externalState = new IndexOnlyAcceptanceState
        {
            City = $"external-{Guid.NewGuid():N}",
            Score = 20,
            UnindexedPayload = $"external-secret-{Guid.NewGuid():N}",
        };

        try
        {
            await full.SaveAsync(fullState);
            await external.SaveAsync(externalState);

            var fullMatches = await GetQuery(IndexOnlyAcceptanceNames.FullProvider)
                .Query<IndexOnlyAcceptanceState>(IndexOnlyAcceptanceNames.StateName)
                .Where(state => state.City == fullState.City)
                .ToGrainIdsAsync();
            var externalMatches = await GetQuery(IndexOnlyAcceptanceNames.IndexProvider)
                .Query<IndexOnlyAcceptanceState>(IndexOnlyAcceptanceNames.StateName)
                .Where(state => state.City == externalState.City)
                .ToGrainIdsAsync();
            var externalQuery = GetQuery(IndexOnlyAcceptanceNames.IndexProvider)
                .Query<IndexOnlyAcceptanceState>(IndexOnlyAcceptanceNames.StateName);
            var cityFacet = await externalQuery.ToFacetValueCountsAsync(
                state => state.City,
                new SearchableStorageFacetRequest(
                    topN: 10,
                    accuracy: SearchableStorageFacetAccuracy.Exact));
            var scoreBounds = await externalQuery.ToFacetMinMaxAsync(state => state.Score);

            fullMatches.Should().ContainSingle().Which.Should().Be(full.GetGrainId());
            externalMatches.Should().ContainSingle().Which.Should().Be(external.GetGrainId());
            cityFacet.Items.Should().ContainSingle(item => item.Value == externalState.City
                && item.Count == 1);
            scoreBounds.Should().NotBeNull();
            scoreBounds!.Minimum.Should().Be(externalState.Score);
            scoreBounds.Maximum.Should().Be(externalState.Score);
            (await full.GetAsync()).Should().BeEquivalentTo(fullState);
            (await external.GetAsync()).Should().BeEquivalentTo(externalState);

            await external.ClearAsync();

            (await GetQuery(IndexOnlyAcceptanceNames.IndexProvider)
                    .Query<IndexOnlyAcceptanceState>(IndexOnlyAcceptanceNames.StateName)
                    .Where(state => state.City == externalState.City)
                    .ToGrainIdsAsync())
                .Should().BeEmpty();
            (await GetQuery(IndexOnlyAcceptanceNames.FullProvider)
                    .Query<IndexOnlyAcceptanceState>(IndexOnlyAcceptanceNames.StateName)
                    .Where(state => state.City == fullState.City)
                    .ToGrainIdsAsync())
                .Should().ContainSingle().Which.Should().Be(full.GetGrainId());
            (await full.GetAsync()).Should().BeEquivalentTo(fullState);
        }
        finally
        {
            await Task.WhenAll(full.ClearAsync(), external.ClearAsync());
        }
    }

    [Fact]
    public async Task IndexOnlyWalSnapshotAndReactivationNeverRetainApplicationPayload()
    {
        await EnsureSchemasActiveAsync();
        var grain = _fixture.Cluster.GrainFactory.GetGrain<IExternallyStoredSearchStateGrain>(
            $"payload-{Guid.NewGuid():N}");
        var state = new IndexOnlyAcceptanceState
        {
            City = $"payload-city-{Guid.NewGuid():N}",
            Score = 37,
            UnindexedPayload = string.Concat(
                "must-never-enter-index-durability-",
                Guid.NewGuid().ToString("N"),
                new string('x', 32 * 1_024)),
        };

        try
        {
            await grain.SaveAsync(state);
            var (partition, partitionKey) = await GetIndexPartitionAsync(grain.GetGrainId());
            var beforeCompaction = await partition.GetPersistenceInfoAsync();
            var absoluteSegment = StoragePersistence.GetAbsoluteSegmentIndex(
                beforeCompaction.CommittedSequence,
                beforeCompaction.JournalSegmentCapacity);
            var journal = GetJournal(
                partitionKey,
                absoluteSegment,
                beforeCompaction.JournalSegmentCapacity,
                beforeCompaction.MaximumJournalReplayEntries);
            var segment = await journal.ReadAsync();
            var committed = segment.Entries
                .Where(entry => entry.Sequence <= beforeCompaction.CommittedSequence)
                .Select(static entry => entry.Record)
                .Where(static record => record is not null)
                .Single(record => record!.GrainId.Equals(grain.GetGrainId()));

            committed!.Payload.Should().BeNull();

            await partition.CompactAsync();
            var compacted = await partition.GetPersistenceInfoAsync();
            compacted.ActiveSnapshotGeneration.Should().BeGreaterThan(0);
            var snapshotSlot = checked((int)(
                (compacted.ActiveSnapshotGeneration - 1)
                % StoragePersistence.SnapshotSlotCount));
            var snapshot = await GetSnapshot(partitionKey, snapshotSlot).ReadAsync();
            var movedRecord = snapshot.LosslessRecords
                .Select(static item => item.Record)
                .Single(record => StorageMoveRecordCodec.Decode(record).GrainId
                    .Equals(grain.GetGrainId()));

            movedRecord.Payload.Should().BeNull();

            await _fixture.Cluster.DeactivateAsync(partition);

            var matches = await GetQuery(IndexOnlyAcceptanceNames.IndexProvider)
                .Query<IndexOnlyAcceptanceState>(IndexOnlyAcceptanceNames.StateName)
                .Where(candidate => candidate.City == state.City && candidate.Score == state.Score)
                .ToGrainIdsAsync();
            matches.Should().ContainSingle().Which.Should().Be(grain.GetGrainId());
            (await grain.GetAsync()).Should().BeEquivalentTo(state);
        }
        finally
        {
            await grain.ClearAsync();
        }
    }

    [Fact]
    public async Task MovementPreservesPayloadFreeRecordsAndQueryAuthority()
    {
        await EnsureSchemasActiveAsync();
        var grain = _fixture.Cluster.GrainFactory.GetGrain<IExternallyStoredSearchStateGrain>(
            $"movement-{Guid.NewGuid():N}");
        var state = new IndexOnlyAcceptanceState
        {
            City = $"movement-city-{Guid.NewGuid():N}",
            Score = 71,
            UnindexedPayload = $"movement-secret-{Guid.NewGuid():N}",
        };

        try
        {
            await grain.SaveAsync(state);
            var layoutGrain = _fixture.Cluster.GrainFactory
                .GetGrain<IStorageLayoutGrain>(IndexOnlyAcceptanceNames.IndexProvider);
            var before = await layoutGrain.GetCurrentLayoutAsync();
            before.Should().NotBeNull();
            var slot = StorageLayout.GetSlot(grain.GetGrainId(), before!.VirtualSlotCount);
            var sourceOwner = before.GetOwner(slot);
            var targetOwner = sourceOwner == 0 ? 1 : 0;
            var admin = GetPrimaryServices()
                .GetRequiredKeyedService<ISearchableStorageAdminClient>(
                    IndexOnlyAcceptanceNames.IndexProvider);

            _ = await admin.EnableMovementAsync();
            var planned = await admin.PlanMoveAsync(slot, targetOwner);
            var completed = await admin.ExecuteMoveAsync(planned.MoveId);

            completed.IsComplete.Should().BeTrue();
            var after = await layoutGrain.GetCurrentLayoutAsync();
            after.Should().NotBeNull();
            after!.GetOwner(slot).Should().Be(targetOwner);

            var (target, partitionKey) = await GetIndexPartitionAsync(grain.GetGrainId());
            target.GetPrimaryKeyString().Should().Be(StorageLayout.CreatePartitionKey(
                IndexOnlyAcceptanceNames.IndexProvider,
                targetOwner));
            await target.CompactAsync();
            var compacted = await target.GetPersistenceInfoAsync();
            var snapshotSlot = checked((int)(
                (compacted.ActiveSnapshotGeneration - 1)
                % StoragePersistence.SnapshotSlotCount));
            var snapshot = await GetSnapshot(partitionKey, snapshotSlot).ReadAsync();
            var moved = snapshot.LosslessRecords
                .Select(static item => StorageMoveRecordCodec.Decode(item.Record))
                .Single(record => record.GrainId.Equals(grain.GetGrainId()));

            moved.Payload.Should().BeNull();

            await _fixture.Cluster.DeactivateAsync(target);

            (await FindCityAsync(state.City))
                .Should().ContainSingle().Which.Should().Be(grain.GetGrainId());
            (await grain.GetAsync()).Should().BeEquivalentTo(state);
        }
        finally
        {
            await grain.ClearAsync();
        }
    }

    [Fact]
    public async Task RecoveryRejectsShadowedWalPayloadBeforePublishingPendingSnapshot()
    {
        var services = GetPrimaryServices();
        var physical = services.GetRequiredKeyedService<IGrainStorage>(
            IndexOnlyAcceptanceNames.InnerPhysicalProvider);
        var partitionKey = StorageLayout.CreatePartitionKey(
            IndexOnlyAcceptanceNames.RecoveryProvider,
            partitionIndex: 0);
        var partition = _fixture.Cluster.GrainFactory
            .GetGrain<IStoragePartitionGrain>(partitionKey);
        var journal = GetJournal(
            partitionKey,
            absoluteSegment: 0,
            segmentCapacity: 2,
            maximumReplayEntries: 4);
        var snapshot = GetSnapshot(partitionKey, slot: 0);
        var firstOperationId = Guid.NewGuid();
        var secondOperationId = Guid.NewGuid();
        var grainId = GrainId.Create(
            "index-only-recovery",
            Guid.NewGuid().ToString("N"));
        var fingerprint = Enumerable.Range(1, IndexSchemaDefinition.FingerprintLength)
            .Select(static value => checked((byte)value))
            .ToArray();
        var pending = new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = Guid.NewGuid(),
            Sequence = 2,
            OperationId = secondOperationId,
            NextVersion = 3,
        };
        var manifestState = new GrainState<StoragePartitionManifestState>
        {
            State = new StoragePartitionManifestState
            {
                Initialized = true,
                PersistenceFormatVersion = StoragePersistence.IndexOnlyPersistenceFormatVersion,
                JournalSegmentCapacity = 2,
                MaximumJournalReplayEntries = 4,
                WriterEpoch = 1,
                CommittedSequence = 2,
                CommittedOperationId = secondOperationId,
                NextVersion = 3,
                PendingSnapshot = pending,
                SnapshotGenerationHighWatermark = 1,
                IndexSchemaProtocolVersion = StorageIndexSchema.ProtocolVersion,
                NamespaceMode = StorageNamespaceMode.IndexOnly,
            },
        };
        var journalState = new GrainState<StorageJournalSegmentState>
        {
            State = new StorageJournalSegmentState
            {
                Initialized = true,
                Capacity = 2,
                AbsoluteSegmentIndex = 0,
                HighestWriterEpoch = 1,
                Entries =
                [
                    new StorageJournalEntry
                    {
                        Sequence = 1,
                        WriterEpoch = 1,
                        OperationId = firstOperationId,
                        PreviousOperationId = Guid.Empty,
                        Operation = StorageJournalOperation.Upsert,
                        RecordKey = "document/corrupt-authority",
                        Record = new StoredRecord
                        {
                            GrainId = grainId,
                            Payload = [0xca, 0xfe],
                            ETag = "1",
                            IndexEntries = [],
                            IndexSchemaFingerprint = [.. fingerprint],
                        },
                        NextVersionAfter = 2,
                    },
                    new StorageJournalEntry
                    {
                        Sequence = 2,
                        WriterEpoch = 1,
                        OperationId = secondOperationId,
                        PreviousOperationId = firstOperationId,
                        Operation = StorageJournalOperation.Upsert,
                        RecordKey = "document/corrupt-authority",
                        ExpectedETag = "1",
                        Record = new StoredRecord
                        {
                            GrainId = grainId,
                            Payload = null,
                            ETag = "2",
                            IndexEntries = [],
                            IndexSchemaFingerprint = [.. fingerprint],
                        },
                        NextVersionAfter = 3,
                    },
                ],
            },
        };

        await physical.WriteStateAsync("manifest", partition.GetGrainId(), manifestState);
        await physical.WriteStateAsync("journal", journal.GetGrainId(), journalState);

        try
        {
            Func<Task> activate = async () => await partition.GetPersistenceInfoAsync();

            var failure = await activate.Should().ThrowAsync<Exception>();
            failure.Which.ToString().Should().Contain(
                "An index-only namespace record must not contain an application payload.");
            (await WriteFaultInjectingGrainStorage.GetWriteCallCountAsync(
                    _fixture.Cluster.GrainFactory,
                    snapshot.GetGrainId(),
                    "snapshot"))
                .Should().Be(0);
            (await WriteFaultInjectingGrainStorage.GetWriteCallCountAsync(
                    _fixture.Cluster.GrainFactory,
                    partition.GetGrainId(),
                    "manifest"))
                .Should().Be(0);

            var unpublished = new GrainState<StorageSnapshotState>();
            await physical.ReadStateAsync("snapshot", snapshot.GetGrainId(), unpublished);
            unpublished.RecordExists.Should().BeFalse();
            var unchangedManifest = new GrainState<StoragePartitionManifestState>();
            await physical.ReadStateAsync(
                "manifest",
                partition.GetGrainId(),
                unchangedManifest);
            unchangedManifest.State.PendingSnapshot.IsPresent.Should().BeTrue();
            unchangedManifest.State.ActiveSnapshot.IsPresent.Should().BeFalse();
        }
        finally
        {
            await physical.ClearStateAsync("journal", journal.GetGrainId(), journalState);
            await physical.ClearStateAsync("manifest", partition.GetGrainId(), manifestState);
        }
    }

    [Fact]
    public async Task RecoveryRejectsSnapshotPayloadEvenWhenCommittedWalDeletesTheRecord()
    {
        var physical = GetPrimaryServices().GetRequiredKeyedService<IGrainStorage>(
            IndexOnlyAcceptanceNames.InnerPhysicalProvider);
        var providerName = $"IndexOnlyAcceptance.SnapshotRecovery.{Guid.NewGuid():N}";
        var partitionKey = StorageLayout.CreatePartitionKey(providerName, partitionIndex: 0);
        var partition = _fixture.Cluster.GrainFactory
            .GetGrain<IStoragePartitionGrain>(partitionKey);
        var snapshotGrain = GetSnapshot(partitionKey, slot: 0);
        var journal = GetJournal(
            partitionKey,
            absoluteSegment: 0,
            segmentCapacity: 2,
            maximumReplayEntries: 4);
        var snapshotOperationId = Guid.NewGuid();
        var deleteOperationId = Guid.NewGuid();
        var recordKey = "document/shadowed-snapshot-authority";
        var descriptor = new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = Guid.NewGuid(),
            Sequence = 1,
            OperationId = snapshotOperationId,
            NextVersion = 2,
        };
        var fingerprint = Enumerable.Range(1, IndexSchemaDefinition.FingerprintLength)
            .Select(static value => checked((byte)value))
            .ToArray();
        var snapshotState = new GrainState<StorageSnapshotState>
        {
            State = StorageSnapshotFactory.Create(
                descriptor,
                new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
                {
                    [recordKey] = new StoredRecord
                    {
                        GrainId = GrainId.Create(
                            "index-only-snapshot-recovery",
                            Guid.NewGuid().ToString("N")),
                        Payload = [0xca, 0xfe],
                        ETag = "1",
                        IndexEntries = [],
                        IndexSchemaFingerprint = fingerprint,
                    },
                },
                StoragePersistence.IndexOnlyPersistenceFormatVersion),
        };
        var manifestState = new GrainState<StoragePartitionManifestState>
        {
            State = new StoragePartitionManifestState
            {
                Initialized = true,
                PersistenceFormatVersion = StoragePersistence.IndexOnlyPersistenceFormatVersion,
                JournalSegmentCapacity = 2,
                MaximumJournalReplayEntries = 4,
                WriterEpoch = 1,
                CommittedSequence = 2,
                CommittedOperationId = deleteOperationId,
                NextVersion = 2,
                ActiveSnapshot = descriptor.Copy(),
                SnapshotGenerationHighWatermark = 1,
                SnapshotSequence = 1,
                IndexSchemaProtocolVersion = StorageIndexSchema.ProtocolVersion,
                NamespaceMode = StorageNamespaceMode.IndexOnly,
            },
        };
        var journalState = new GrainState<StorageJournalSegmentState>
        {
            State = new StorageJournalSegmentState
            {
                Initialized = true,
                Capacity = 2,
                AbsoluteSegmentIndex = 0,
                HighestWriterEpoch = 1,
                Entries =
                [
                    new StorageJournalEntry
                    {
                        Sequence = 2,
                        WriterEpoch = 1,
                        OperationId = deleteOperationId,
                        PreviousOperationId = snapshotOperationId,
                        Operation = StorageJournalOperation.Delete,
                        RecordKey = recordKey,
                        ExpectedETag = "1",
                        NextVersionAfter = 2,
                    },
                ],
            },
        };

        await physical.WriteStateAsync("snapshot", snapshotGrain.GetGrainId(), snapshotState);
        await physical.WriteStateAsync("manifest", partition.GetGrainId(), manifestState);
        await physical.WriteStateAsync("journal", journal.GetGrainId(), journalState);

        try
        {
            Func<Task> activate = async () => await partition.GetPersistenceInfoAsync();

            var failure = await activate.Should().ThrowAsync<Exception>();
            failure.Which.ToString().Should().Contain(
                "An index-only namespace record must not contain an application payload.");
            (await WriteFaultInjectingGrainStorage.GetWriteCallCountAsync(
                    _fixture.Cluster.GrainFactory,
                    partition.GetGrainId(),
                    "manifest"))
                .Should().Be(0);
        }
        finally
        {
            await physical.ClearStateAsync("journal", journal.GetGrainId(), journalState);
            await physical.ClearStateAsync("snapshot", snapshotGrain.GetGrainId(), snapshotState);
            await physical.ClearStateAsync("manifest", partition.GetGrainId(), manifestState);
        }
    }

    [Fact]
    public async Task SchemaProtocolModeMismatchIsRejectedBeforePartitionAuthorityIsCreated()
    {
        var providerName = $"IndexOnlyAcceptance.ProtocolFence.{Guid.NewGuid():N}";
        var layoutGrain = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var layout = await layoutGrain.InitializeRoutingAsync(StorageLayout.CreateDescriptor(
            providerName,
            partitionCount: 1,
            journalSegmentCapacity: 2,
            maximumJournalReplayEntries: 4,
            namespaceMode: StorageNamespaceMode.IndexOnly));
        var partition = _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, partitionIndex: 0));
        var request = new StorageIndexSchemaPartitionProtocolRequest
        {
            ProtocolVersion = StorageIndexSchema.ProtocolVersion,
            ProviderName = providerName,
            LayoutEpoch = layout.Epoch,
            LayoutFingerprint = StorageLayoutFingerprint.Compute(layout),
            Persistence = new StoragePersistenceSettings
            {
                JournalSegmentCapacity = 2,
                MaximumJournalReplayEntries = 4,
                CompactionThreshold = 4,
                NamespaceMode = StorageNamespaceMode.Integrated,
            },
        };

        Func<Task> enable = async () => await partition.EnableIndexSchemaProtocolAsync(request);

        await enable.Should().ThrowExactlyAsync<ArgumentException>()
            .WithMessage("*namespace mode*does not match*persisted routing layout*");
        (await WriteFaultInjectingGrainStorage.GetWriteCallCountAsync(
                _fixture.Cluster.GrainFactory,
                partition.GetGrainId(),
                "manifest"))
            .Should().Be(0);
        var physical = GetPrimaryServices().GetRequiredKeyedService<IGrainStorage>(
            IndexOnlyAcceptanceNames.InnerPhysicalProvider);
        var manifest = new GrainState<StoragePartitionManifestState>();
        await physical.ReadStateAsync("manifest", partition.GetGrainId(), manifest);
        manifest.RecordExists.Should().BeFalse();
    }

    [Fact]
    public async Task ExactRetryAfterLostAcknowledgementConvergesForUpsertAndRemove()
    {
        await EnsureSchemasActiveAsync();
        var writer = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageIndexWriter>(
                IndexOnlyAcceptanceNames.IndexProvider);
        var grainId = GrainId.Create("index-only-lost-ack", Guid.NewGuid().ToString("N"));
        var original = new IndexOnlyAcceptanceState
        {
            City = $"lost-ack-old-{Guid.NewGuid():N}",
            Score = 1,
            UnindexedPayload = $"old-secret-{Guid.NewGuid():N}",
        };
        var replacement = new IndexOnlyAcceptanceState
        {
            City = $"lost-ack-new-{Guid.NewGuid():N}",
            Score = 2,
            UnindexedPayload = $"new-secret-{Guid.NewGuid():N}",
        };

        try
        {
            await writer.UpsertAsync(IndexOnlyAcceptanceNames.StateName, grainId, original);
            var (partition, _) = await GetIndexPartitionAsync(grainId);
            await AddManifestLostAcknowledgementAsync(partition);

            Func<Task> ambiguousUpsert = () => writer.UpsertAsync(
                IndexOnlyAcceptanceNames.StateName,
                grainId,
                replacement);
            await AssertInjectedFailureAsync(ambiguousUpsert);

            await writer.UpsertAsync(
                IndexOnlyAcceptanceNames.StateName,
                grainId,
                replacement);

            (await FindCityAsync(original.City)).Should().BeEmpty();
            (await FindCityAsync(replacement.City))
                .Should().ContainSingle().Which.Should().Be(grainId);

            (partition, _) = await GetIndexPartitionAsync(grainId);
            await AddManifestLostAcknowledgementAsync(partition);
            Func<Task> ambiguousRemove = () => writer.RemoveAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName,
                grainId);
            await AssertInjectedFailureAsync(ambiguousRemove);

            await writer.RemoveAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName,
                grainId);
            await writer.RemoveAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName,
                grainId);

            (await FindCityAsync(replacement.City)).Should().BeEmpty();
        }
        finally
        {
            await writer.RemoveAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName,
                grainId);
        }
    }

    [Fact]
    public async Task BlindWriterAppliesCallerArrivalOrderWithoutAnOrderingGuarantee()
    {
        await EnsureSchemasActiveAsync();
        var writer = GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageIndexWriter>(
                IndexOnlyAcceptanceNames.IndexProvider);
        var grainId = GrainId.Create("index-only-arrival-order", Guid.NewGuid().ToString("N"));
        var first = new IndexOnlyAcceptanceState
        {
            City = $"arrival-first-{Guid.NewGuid():N}",
            Score = 1,
        };
        var second = new IndexOnlyAcceptanceState
        {
            City = $"arrival-second-{Guid.NewGuid():N}",
            Score = 2,
        };

        try
        {
            await writer.UpsertAsync(IndexOnlyAcceptanceNames.StateName, grainId, first);
            await writer.UpsertAsync(IndexOnlyAcceptanceNames.StateName, grainId, second);
            await writer.UpsertAsync(IndexOnlyAcceptanceNames.StateName, grainId, first);

            (await FindCityAsync(first.City)).Should().ContainSingle().Which.Should().Be(grainId);
            (await FindCityAsync(second.City)).Should().BeEmpty();

            await writer.RemoveAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName,
                grainId);
            await writer.UpsertAsync(IndexOnlyAcceptanceNames.StateName, grainId, second);

            (await FindCityAsync(second.City)).Should().ContainSingle().Which.Should().Be(grainId);
        }
        finally
        {
            await writer.RemoveAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName,
                grainId);
        }
    }

    private async Task EnsureSchemasActiveAsync()
    {
        var services = GetPrimaryServices();
        await services
            .GetRequiredKeyedService<ISearchableStorageAdminClient>(
                IndexOnlyAcceptanceNames.FullProvider)
            .RebuildIndexSchemaAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName);
        await services
            .GetRequiredKeyedService<ISearchableStorageAdminClient>(
                IndexOnlyAcceptanceNames.IndexProvider)
            .RebuildIndexSchemaAsync<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.StateName);
    }

    private IServiceProvider GetPrimaryServices()
    {
        return Assert.IsType<InProcessSiloHandle>(_fixture.Cluster.Primary).ServiceProvider;
    }

    private ISearchableStorageQueryClient GetQuery(string providerName)
    {
        return GetPrimaryServices()
            .GetRequiredKeyedService<ISearchableStorageQueryClient>(providerName);
    }

    private Task<IReadOnlyList<GrainId>> FindCityAsync(string city)
    {
        return GetQuery(IndexOnlyAcceptanceNames.IndexProvider)
            .FindAsync<IndexOnlyAcceptanceState, string>(
                IndexOnlyAcceptanceNames.StateName,
                state => state.City,
                city);
    }

    private async Task<(IStoragePartitionGrain Partition, string PartitionKey)>
        GetIndexPartitionAsync(GrainId grainId)
    {
        var layout = await _fixture.Cluster.GrainFactory
            .GetGrain<IStorageLayoutGrain>(IndexOnlyAcceptanceNames.IndexProvider)
            .GetCurrentLayoutAsync();
        layout.Should().NotBeNull();
        var slot = StorageLayout.GetSlot(grainId, layout!.VirtualSlotCount);
        var owner = layout.GetOwner(slot);
        var partitionKey = StorageLayout.CreatePartitionKey(
            IndexOnlyAcceptanceNames.IndexProvider,
            owner);
        return (
            _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(partitionKey),
            partitionKey);
    }

    private IStorageJournalSegmentGrain GetJournal(
        string partitionKey,
        long absoluteSegment,
        int segmentCapacity,
        int maximumReplayEntries)
    {
        var slotCount = StoragePersistence.GetJournalSlotCount(
            maximumReplayEntries,
            segmentCapacity);
        var slot = StoragePersistence.GetJournalSlotIndex(
            absoluteSegment,
            maximumReplayEntries,
            segmentCapacity);
        return _fixture.Cluster.GrainFactory.GetGrain<IStorageJournalSegmentGrain>(
            StoragePersistence.CreateJournalSlotKey(partitionKey, slot, slotCount));
    }

    private IStorageSnapshotGrain GetSnapshot(string partitionKey, int slot)
    {
        return _fixture.Cluster.GrainFactory.GetGrain<IStorageSnapshotGrain>(
            StoragePersistence.CreateSnapshotSlotKey(partitionKey, slot));
    }

    private Task AddManifestLostAcknowledgementAsync(IStoragePartitionGrain partition)
    {
        return WriteFaultInjectingGrainStorage.AddWriteFaultAsync(
            _fixture.Cluster.GrainFactory,
            partition.GetGrainId(),
            "manifest",
            PhysicalWriteFaultStage.AfterCommit);
    }

    private static async Task AssertInjectedFailureAsync(Func<Task> action)
    {
        var exception = await action.Should().ThrowAsync<Exception>();
        exception.Which.ToString().Should().Contain(
            WriteFaultInjectingGrainStorage.InjectedFailureMessage);
    }
}

public sealed class IndexOnlyWriterAcceptanceFixture : IAsyncLifetime
{
    public IndexOnlyWriterAcceptanceFixture()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        Cluster = builder.Build();
    }

    public TestCluster Cluster { get; }

    public Task InitializeAsync() => Cluster.DeployAsync();

    public async Task DisposeAsync()
    {
        try
        {
            await Cluster.StopAllSilosAsync();
        }
        finally
        {
            await Cluster.DisposeAsync();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            AddJsonMemoryStorage(siloBuilder, IndexOnlyAcceptanceNames.InnerPhysicalProvider);
            AddJsonMemoryStorage(siloBuilder, IndexOnlyAcceptanceNames.PayloadProvider);
            siloBuilder.Services.AddKeyedSingleton<IGrainStorage>(
                SearchableStorageConstants.PhysicalStorageProviderName,
                (services, _) => new WriteFaultInjectingGrainStorage(
                    services.GetRequiredKeyedService<IGrainStorage>(
                        IndexOnlyAcceptanceNames.InnerPhysicalProvider),
                    services.GetRequiredService<IGrainFactory>()));

            siloBuilder.AddSearchableGrainStorage(
                IndexOnlyAcceptanceNames.FullProvider,
                options => options.PartitionCount = 2);
            siloBuilder.AddSearchableStorageState<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.FullProvider,
                IndexOnlyAcceptanceNames.StateName);

            siloBuilder.AddSearchableIndex(
                IndexOnlyAcceptanceNames.IndexProvider,
                options =>
                {
                    options.PartitionCount = 2;
                    options.JournalSegmentCapacity = 8;
                    options.MaximumJournalReplayEntries = 128;
                    options.CompactionThreshold = 128;
                });
            siloBuilder.AddSearchableStorageState<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.IndexProvider,
                IndexOnlyAcceptanceNames.StateName);

            siloBuilder.AddSearchableIndex(
                IndexOnlyAcceptanceNames.RecoveryProvider,
                options =>
                {
                    options.PartitionCount = 1;
                    options.JournalSegmentCapacity = 2;
                    options.MaximumJournalReplayEntries = 4;
                    options.CompactionThreshold = 4;
                });
            siloBuilder.AddSearchableStorageState<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.RecoveryProvider,
                IndexOnlyAcceptanceNames.StateName);
        }

        private static void AddJsonMemoryStorage(ISiloBuilder siloBuilder, string providerName)
        {
            siloBuilder.AddMemoryGrainStorage(
                providerName,
                options => options.Configure<OrleansJsonSerializer>(
                    (storageOptions, serializer) =>
                    {
                        storageOptions.NumStorageGrains = 4;
                        storageOptions.GrainStorageSerializer =
                            new JsonGrainStorageSerializer(serializer);
                    }));
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.Configure<ClientMessagingOptions>(
                options => options.LocalAddress = IPAddress.Loopback);
            clientBuilder.Services.AddSearchableIndex(
                IndexOnlyAcceptanceNames.IndexProvider,
                options =>
                {
                    options.PartitionCount = 2;
                    options.JournalSegmentCapacity = 8;
                    options.MaximumJournalReplayEntries = 128;
                    options.CompactionThreshold = 128;
                });
            clientBuilder.Services.AddSearchableStorageState<IndexOnlyAcceptanceState>(
                IndexOnlyAcceptanceNames.IndexProvider,
                IndexOnlyAcceptanceNames.StateName);
        }
    }
}

internal static class IndexOnlyAcceptanceNames
{
    public const string InnerPhysicalProvider = "IndexOnlyAcceptance.Physical";
    public const string PayloadProvider = "IndexOnlyAcceptance.Payload";
    public const string FullProvider = "IndexOnlyAcceptance.Full";
    public const string IndexProvider = "IndexOnlyAcceptance.Index";
    public const string RecoveryProvider = "IndexOnlyAcceptance.Recovery";
    public const string StateName = "shared-document";
}

[GenerateSerializer]
public sealed class IndexOnlyAcceptanceState
{
    [Id(0)]
    [SearchableIndex(SearchableIndexKind.Hash)]
    public string City { get; set; } = string.Empty;

    [Id(1)]
    [SearchableIndex(SearchableIndexKind.Range)]
    public int Score { get; set; }

    [Id(2)]
    public string UnindexedPayload { get; set; } = string.Empty;
}

public interface IFullSearchStateGrain : IGrainWithStringKey
{
    Task<IndexOnlyAcceptanceState> GetAsync();

    Task SaveAsync(IndexOnlyAcceptanceState state);

    Task ClearAsync();
}

public sealed class FullSearchStateGrain : Grain, IFullSearchStateGrain
{
    private readonly IPersistentState<IndexOnlyAcceptanceState> _state;

    public FullSearchStateGrain(
        [PersistentState(
            IndexOnlyAcceptanceNames.StateName,
            IndexOnlyAcceptanceNames.FullProvider)]
        IPersistentState<IndexOnlyAcceptanceState> state)
    {
        _state = state;
    }

    public Task<IndexOnlyAcceptanceState> GetAsync() => Task.FromResult(_state.State);

    public async Task SaveAsync(IndexOnlyAcceptanceState state)
    {
        _state.State = state;
        await _state.WriteStateAsync();
    }

    public Task ClearAsync() => _state.ClearStateAsync();
}

public interface IExternallyStoredSearchStateGrain : IGrainWithStringKey
{
    Task<IndexOnlyAcceptanceState> GetAsync();

    Task SaveAsync(IndexOnlyAcceptanceState state);

    Task ClearAsync();
}

public sealed class ExternallyStoredSearchStateGrain
    : Grain, IExternallyStoredSearchStateGrain
{
    private readonly ISearchableStorageIndexWriter _index;
    private readonly IPersistentState<IndexOnlyAcceptanceState> _state;

    public ExternallyStoredSearchStateGrain(
        [PersistentState(
            IndexOnlyAcceptanceNames.StateName,
            IndexOnlyAcceptanceNames.PayloadProvider)]
        IPersistentState<IndexOnlyAcceptanceState> state,
        [FromKeyedServices(IndexOnlyAcceptanceNames.IndexProvider)]
        ISearchableStorageIndexWriter index)
    {
        _state = state;
        _index = index;
    }

    public Task<IndexOnlyAcceptanceState> GetAsync() => Task.FromResult(_state.State);

    public async Task SaveAsync(IndexOnlyAcceptanceState state)
    {
        _state.State = state;
        await _state.WriteStateAsync();
        await _index.UpsertAsync(
            IndexOnlyAcceptanceNames.StateName,
            this.GetGrainId(),
            state);
    }

    public async Task ClearAsync()
    {
        await _state.ClearStateAsync();
        await _index.RemoveAsync<IndexOnlyAcceptanceState>(
            IndexOnlyAcceptanceNames.StateName,
            this.GetGrainId());
    }
}
