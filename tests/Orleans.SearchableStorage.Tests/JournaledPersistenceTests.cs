using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests;

public abstract class JournaledPersistenceContractTests<TFixture>
    : FaultInjectingSearchableStorageContractTests<TFixture>
    where TFixture : class, ISearchableStorageFixture
{
    protected JournaledPersistenceContractTests(TFixture fixture)
        : base(fixture)
    {
    }

    [SkippableFact]
    public Task LiveSlotMoveUnderRoutedWritesSurvivesReactivationWithSingleAuthority()
    {
        return StorageMovementProviderContract.AssertMoveUnderLoadAsync(Fixture);
    }

    [SkippableFact]
    public async Task FacetsRebuildFromDurableRecordsAfterPartitionReactivation()
    {
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity: 4,
            maximumReplayEntries: 16,
            compactionThreshold: 16);
        var firstId = CreateGrainId();
        var secondId = CreateGrainId();
        var thirdId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            firstId,
            CreateState("alpha", 5));
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            secondId,
            CreateState("alpha", 20));
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            thirdId,
            CreateState("beta", 10));
        var partition = GetPartition(providerName);
        await Fixture.Cluster.DeactivateAsync(partition);
        var journal = GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity: 4,
            maximumReplayEntries: 16);
        var snapshot0 = GetSnapshot(providerName, slot: 0);
        var snapshot1 = GetSnapshot(providerName, slot: 1);
        var writesBefore = new[]
        {
            await GetWriteCallCountAsync(partition.GetGrainId(), "manifest"),
            await GetWriteCallCountAsync(journal.GetGrainId(), "journal"),
            await GetWriteCallCountAsync(snapshot0.GetGrainId(), "snapshot"),
            await GetWriteCallCountAsync(snapshot1.GetGrainId(), "snapshot"),
        };
        var query = CreateClient(providerName).Query<VacancyState>(VacancyGrain.StateName);

        var distinct = await query.ToDistinctFacetValuePageAsync(
            state => state.City,
            new SearchableStorageFacetPageRequest(10));
        var counts = await query.ToFacetValueCountsAsync(
            state => state.City,
            new SearchableStorageFacetRequest(2, SearchableStorageFacetAccuracy.Exact));
        var minMax = await query.ToFacetMinMaxAsync(state => state.Salary);

        distinct.Items.Should().Equal("alpha", "beta");
        distinct.ContinuationToken.Should().BeNull();
        counts.Items.Select(static item => (item.Value, item.Count)).Should().Equal(
            ("alpha", 2L),
            ("beta", 1L));
        minMax.Should().NotBeNull();
        minMax!.Minimum.Should().Be(5);
        minMax.Maximum.Should().Be(20);
        var writesAfter = new[]
        {
            await GetWriteCallCountAsync(partition.GetGrainId(), "manifest"),
            await GetWriteCallCountAsync(journal.GetGrainId(), "journal"),
            await GetWriteCallCountAsync(snapshot0.GetGrainId(), "snapshot"),
            await GetWriteCallCountAsync(snapshot1.GetGrainId(), "snapshot"),
        };
        writesAfter.Should().Equal(
            writesBefore,
            "facet reads and activation-derived ordered-view rebuilds must not write durable state");
    }

    [SkippableFact]
    public async Task CommittedJournalReplaysRecordsClearsAndIndexesAfterReactivation()
    {
        var providerName = CreateProviderName();
        var storage = CreateStorage(providerName, segmentCapacity: 2, maximumReplayEntries: 8, compactionThreshold: 8);
        var firstId = CreateGrainId();
        var secondId = CreateGrainId();
        var first = CreateState("old", 10);
        var second = CreateState("removed", 20);

        await storage.WriteStateAsync(VacancyGrain.StateName, firstId, first);
        first.State = new VacancyState { City = "current", Salary = 30 };
        await storage.WriteStateAsync(VacancyGrain.StateName, firstId, first);
        await storage.WriteStateAsync(VacancyGrain.StateName, secondId, second);
        await storage.ClearStateAsync(VacancyGrain.StateName, secondId, second);

        var partition = GetPartition(providerName);
        var before = await partition.GetPersistenceInfoAsync();
        before.CommittedSequence.Should().Be(4);
        before.SnapshotSequence.Should().Be(0);
        await Fixture.Cluster.DeactivateAsync(partition);

        var loadedFirst = await ReadStateAsync(storage, firstId);
        var loadedSecond = await ReadStateAsync(storage, secondId);
        var client = CreateClient(providerName);
        var oldMatches = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "old");
        var currentMatches = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "current");
        var currentSalaryMatches = await client.FindAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            30);
        var removedMatches = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "removed");

        loadedFirst.RecordExists.Should().BeTrue();
        loadedFirst.State.City.Should().Be("current");
        loadedFirst.State.Salary.Should().Be(30);
        loadedSecond.RecordExists.Should().BeFalse();
        oldMatches.Should().BeEmpty();
        currentMatches.Should().ContainSingle().Which.Should().Be(firstId);
        currentSalaryMatches.Should().ContainSingle().Which.Should().Be(firstId);
        removedMatches.Should().BeEmpty();
    }

    [SkippableFact]
    public async Task JournalRolloverKeepsEveryLiveSegmentBoundedAndReplayable()
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 6;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var ids = Enumerable.Range(0, 5).Select(_ => CreateGrainId()).ToArray();

        for (var index = 0; index < ids.Length; index++)
        {
            await storage.WriteStateAsync(
                VacancyGrain.StateName,
                ids[index],
                CreateState($"city-{index}", index));
        }

        var firstSegment = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();
        var secondSegment = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 1,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();
        var thirdSegment = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 2,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();

        firstSegment.Entries.Select(static entry => entry.Sequence).Should().Equal(1, 2);
        secondSegment.Entries.Select(static entry => entry.Sequence).Should().Equal(3, 4);
        thirdSegment.Entries.Select(static entry => entry.Sequence).Should().Equal(5);
        firstSegment.Entries.Should().HaveCountLessThanOrEqualTo(segmentCapacity);
        secondSegment.Entries.Should().HaveCountLessThanOrEqualTo(segmentCapacity);
        thirdSegment.Entries.Should().HaveCountLessThanOrEqualTo(segmentCapacity);

        var partition = GetPartition(providerName);
        await Fixture.Cluster.DeactivateAsync(partition);
        var loaded = await ReadStateAsync(storage, ids[^1]);
        var matches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "city-4");

        loaded.State.City.Should().Be("city-4");
        matches.Should().ContainSingle().Which.Should().Be(ids[^1]);
        (await partition.GetPersistenceInfoAsync()).CommittedSequence.Should().Be(5);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit, false, 1)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit, true, 2)]
    public async Task InitialWriterEpochFailureCannotCreateAVisibleOrDoubleCommittedMutation(
        PhysicalWriteFaultStage stage,
        bool epochWasDurable,
        long expectedRetryEpoch)
    {
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity: 2,
            maximumReplayEntries: 8,
            compactionThreshold: 8);
        var grainId = CreateGrainId();
        var state = CreateState("initial-epoch", 10);
        var partition = GetPartition(providerName);
        await AddWriteFaultAsync(partition.GetGrainId(), "manifest", stage);

        Func<Task> firstWrite = () => storage.WriteStateAsync(VacancyGrain.StateName, grainId, state);
        await AssertInjectedFailureAsync(firstWrite);

        var missing = await ReadStateAsync(storage, grainId);
        var failedInfo = await partition.GetPersistenceInfoAsync();
        var journalBeforeRetry = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity: 2,
            maximumReplayEntries: 8).ReadAsync();
        var missingMatches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            "initial-epoch");

        missing.RecordExists.Should().BeFalse();
        failedInfo.Initialized.Should().Be(epochWasDurable);
        failedInfo.CommittedSequence.Should().Be(0);
        journalBeforeRetry.Initialized.Should().BeFalse();
        missingMatches.Should().BeEmpty();

        await storage.WriteStateAsync(VacancyGrain.StateName, grainId, state);

        var committed = await ReadStateAsync(storage, grainId);
        var committedInfo = await partition.GetPersistenceInfoAsync();
        var matches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            "initial-epoch");
        committed.RecordExists.Should().BeTrue();
        committed.ETag.Should().Be("1");
        committedInfo.WriterEpoch.Should().Be(expectedRetryEpoch);
        committedInfo.CommittedSequence.Should().Be(1);
        matches.Should().ContainSingle().Which.Should().Be(grainId);

        await storage.ClearStateAsync(VacancyGrain.StateName, grainId, committed);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit, 1, 2)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit, 2, 3)]
    public async Task ExistingManifestWriterEpochFailureCannotAllocateOrDoubleCommitARecord(
        PhysicalWriteFaultStage stage,
        long expectedFailedEpoch,
        long expectedRetryEpoch)
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var firstId = CreateGrainId();
        var secondId = CreateGrainId();
        var first = CreateState("existing-epoch-first", 10);
        var second = CreateState("existing-epoch-second", 20);
        await storage.WriteStateAsync(VacancyGrain.StateName, firstId, first);
        var partition = GetPartition(providerName);
        await Fixture.Cluster.DeactivateAsync(partition);
        await AddWriteFaultAsync(partition.GetGrainId(), "manifest", stage);

        Func<Task> failedWrite = () => storage.WriteStateAsync(VacancyGrain.StateName, secondId, second);
        await AssertInjectedFailureAsync(failedWrite);

        var missing = await ReadStateAsync(storage, secondId);
        var failedInfo = await partition.GetPersistenceInfoAsync();
        var journalBeforeRetry = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();
        var missingMatches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            "existing-epoch-second");

        missing.RecordExists.Should().BeFalse();
        failedInfo.WriterEpoch.Should().Be(expectedFailedEpoch);
        failedInfo.CommittedSequence.Should().Be(1);
        journalBeforeRetry.Entries.Select(static entry => entry.Sequence).Should().Equal(1);
        missingMatches.Should().BeEmpty();

        await storage.WriteStateAsync(VacancyGrain.StateName, secondId, second);

        var committed = await ReadStateAsync(storage, secondId);
        var committedInfo = await partition.GetPersistenceInfoAsync();
        var journalAfterRetry = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();
        var matches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            "existing-epoch-second");
        committed.RecordExists.Should().BeTrue();
        committed.ETag.Should().Be("2");
        committedInfo.WriterEpoch.Should().Be(expectedRetryEpoch);
        committedInfo.CommittedSequence.Should().Be(2);
        journalAfterRetry.Entries.Select(static entry => entry.Sequence).Should().Equal(1, 2);
        matches.Should().ContainSingle().Which.Should().Be(secondId);

        var loadedFirst = await ReadStateAsync(storage, firstId);
        await storage.ClearStateAsync(VacancyGrain.StateName, firstId, loadedFirst);
        await storage.ClearStateAsync(VacancyGrain.StateName, secondId, committed);
    }

    [SkippableFact]
    public async Task SteadyMutationWritesOneBoundedJournalSegmentAndOneManifest()
    {
        const int segmentCapacity = 4;
        const int maximumReplayEntries = 16;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var firstId = CreateGrainId();
        var secondId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            firstId,
            CreateState("write-shape-first", 10));

        var partition = GetPartition(providerName);
        var journal = GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries);
        var snapshot0 = GetSnapshot(providerName, slot: 0);
        var snapshot1 = GetSnapshot(providerName, slot: 1);
        var manifestBefore = await GetWriteCallCountAsync(partition.GetGrainId(), "manifest");
        var journalBefore = await GetWriteCallCountAsync(journal.GetGrainId(), "journal");
        var snapshot0Before = await GetWriteCallCountAsync(snapshot0.GetGrainId(), "snapshot");
        var snapshot1Before = await GetWriteCallCountAsync(snapshot1.GetGrainId(), "snapshot");

        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            secondId,
            CreateState("write-shape-second", 20));

        (await GetWriteCallCountAsync(partition.GetGrainId(), "manifest"))
            .Should().Be(manifestBefore + 1);
        (await GetWriteCallCountAsync(journal.GetGrainId(), "journal"))
            .Should().Be(journalBefore + 1);
        (await GetWriteCallCountAsync(snapshot0.GetGrainId(), "snapshot"))
            .Should().Be(snapshot0Before);
        (await GetWriteCallCountAsync(snapshot1.GetGrainId(), "snapshot"))
            .Should().Be(snapshot1Before);

        var segment = await journal.ReadAsync();
        var info = await partition.GetPersistenceInfoAsync();
        segment.Entries.Should().HaveCount(2).And.HaveCountLessThanOrEqualTo(segmentCapacity);
        info.CommittedSequence.Should().Be(2);
        info.SnapshotSequence.Should().Be(0);

        var first = await ReadStateAsync(storage, firstId);
        var second = await ReadStateAsync(storage, secondId);
        await storage.ClearStateAsync(VacancyGrain.StateName, firstId, first);
        await storage.ClearStateAsync(VacancyGrain.StateName, secondId, second);
    }

    [SkippableFact]
    public async Task CompactionPublishesSnapshotBeforeRetiringCoveredJournalSegments()
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var ids = Enumerable.Range(0, 4).Select(_ => CreateGrainId()).ToArray();
        for (var index = 0; index < ids.Length; index++)
        {
            await storage.WriteStateAsync(
                VacancyGrain.StateName,
                ids[index],
                CreateState($"compact-{index}", index));
        }

        var partition = GetPartition(providerName);
        await partition.CompactAsync();

        var info = await partition.GetPersistenceInfoAsync();
        var snapshot = await GetSnapshot(providerName, slot: 0).ReadAsync();
        var firstSegment = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();
        var secondSegment = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 1,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();

        info.CommittedSequence.Should().Be(4);
        info.SnapshotSequence.Should().Be(4);
        info.PrunedSequence.Should().Be(4);
        info.ActiveSnapshotGeneration.Should().Be(1);
        info.PendingSnapshotGeneration.Should().Be(0);
        info.RetiringSnapshotGeneration.Should().Be(0);
        snapshot.Initialized.Should().BeTrue();
        snapshot.Tombstoned.Should().BeFalse();
        snapshot.Generation.Should().Be(1);
        snapshot.Records.Should().HaveCount(4);
        firstSegment.Tombstoned.Should().BeTrue();
        firstSegment.Entries.Should().BeEmpty();
        secondSegment.Tombstoned.Should().BeTrue();
        secondSegment.Entries.Should().BeEmpty();

        await Fixture.Cluster.DeactivateAsync(partition);
        (await ReadStateAsync(storage, ids[^1])).State.City.Should().Be("compact-3");
    }

    [SkippableFact]
    public async Task AutomaticCompactionRunsAtTheConfiguredThreshold()
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: 2);
        var firstId = CreateGrainId();
        var secondId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            firstId,
            CreateState("automatic-first", 10));
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            secondId,
            CreateState("automatic-second", 20));

        var partition = GetPartition(providerName);
        var info = await partition.GetPersistenceInfoAsync();
        var snapshot = await GetSnapshot(providerName, slot: 0).ReadAsync();
        var journal = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();

        info.CommittedSequence.Should().Be(2);
        info.SnapshotSequence.Should().Be(2);
        info.PrunedSequence.Should().Be(2);
        info.ActiveSnapshotGeneration.Should().Be(1);
        snapshot.Tombstoned.Should().BeFalse();
        snapshot.Records.Should().HaveCount(2);
        journal.Tombstoned.Should().BeTrue();
        journal.Entries.Should().BeEmpty();

        var first = await ReadStateAsync(storage, firstId);
        var second = await ReadStateAsync(storage, secondId);
        await storage.ClearStateAsync(VacancyGrain.StateName, firstId, first);
        await storage.ClearStateAsync(VacancyGrain.StateName, secondId, second);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit)]
    public async Task AutomaticCompactionFailureDoesNotTurnACommittedMutationIntoAFailure(
        PhysicalWriteFaultStage stage)
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: 2);
        var firstId = CreateGrainId();
        var secondId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            firstId,
            CreateState("automatic-failure-first", 10));
        var snapshot = GetSnapshot(providerName, slot: 0);
        await AddWriteFaultAsync(snapshot.GetGrainId(), "snapshot", stage);
        var second = CreateState("automatic-failure-second", 20);

        await storage.WriteStateAsync(VacancyGrain.StateName, secondId, second);

        (await GetWriteCallCountAsync(snapshot.GetGrainId(), "snapshot")).Should().Be(1);
        var loaded = await ReadStateAsync(storage, secondId);
        var info = await GetPartition(providerName).GetPersistenceInfoAsync();
        var matches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            candidate => candidate.City,
            "automatic-failure-second");

        loaded.RecordExists.Should().BeTrue();
        loaded.ETag.Should().Be("2");
        loaded.State.City.Should().Be("automatic-failure-second");
        info.CommittedSequence.Should().Be(2);
        info.SnapshotSequence.Should().Be(2);
        info.ActiveSnapshotGeneration.Should().Be(1);
        matches.Should().ContainSingle().Which.Should().Be(secondId);

        var first = await ReadStateAsync(storage, firstId);
        await storage.ClearStateAsync(VacancyGrain.StateName, firstId, first);
        await storage.ClearStateAsync(VacancyGrain.StateName, secondId, loaded);
    }

    [SkippableFact]
    public async Task RepeatedCompactionReusesExactlyTwoFencedSnapshotSlots()
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var partition = GetPartition(providerName);

        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            CreateGrainId(),
            CreateState("generation-1", 1));
        await partition.CompactAsync();
        (await GetSnapshot(providerName, slot: 0).ReadAsync()).Generation.Should().Be(1);

        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            CreateGrainId(),
            CreateState("generation-2", 2));
        await partition.CompactAsync();
        var retiredFirst = await GetSnapshot(providerName, slot: 0).ReadAsync();
        var activeSecond = await GetSnapshot(providerName, slot: 1).ReadAsync();
        retiredFirst.Generation.Should().Be(1);
        retiredFirst.Tombstoned.Should().BeTrue();
        retiredFirst.Records.Should().BeEmpty();
        activeSecond.Generation.Should().Be(2);
        activeSecond.Tombstoned.Should().BeFalse();

        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            CreateGrainId(),
            CreateState("generation-3", 3));
        await partition.CompactAsync();
        var activeThird = await GetSnapshot(providerName, slot: 0).ReadAsync();
        var retiredSecond = await GetSnapshot(providerName, slot: 1).ReadAsync();
        var info = await partition.GetPersistenceInfoAsync();

        activeThird.Generation.Should().Be(3);
        activeThird.Tombstoned.Should().BeFalse();
        activeThird.Records.Should().HaveCount(3);
        retiredSecond.Generation.Should().Be(2);
        retiredSecond.Tombstoned.Should().BeTrue();
        retiredSecond.Records.Should().BeEmpty();
        info.ActiveSnapshotGeneration.Should().Be(3);
        info.PendingSnapshotGeneration.Should().Be(0);
        info.RetiringSnapshotGeneration.Should().Be(0);
        info.SnapshotSequence.Should().Be(3);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit)]
    public async Task SnapshotWriteFailurePublishesReservedSnapshotOnNextCall(
        PhysicalWriteFaultStage stage)
    {
        var providerName = CreateProviderName();
        var storage = CreateStorage(providerName, segmentCapacity: 2, maximumReplayEntries: 8, compactionThreshold: 8);
        var grainId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            grainId,
            CreateState("snapshot-orphan", 10));
        var partition = GetPartition(providerName);
        var snapshot = GetSnapshot(providerName, slot: 0);
        await AddWriteFaultAsync(
            snapshot.GetGrainId(),
            "snapshot",
            stage);

        Func<Task> compact = () => partition.CompactAsync();
        await AssertInjectedFailureAsync(compact);

        var loaded = await ReadStateAsync(storage, grainId);
        var info = await partition.GetPersistenceInfoAsync();
        var durableSnapshot = await snapshot.ReadAsync();
        var matches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "snapshot-orphan");

        loaded.State.City.Should().Be("snapshot-orphan");
        matches.Should().ContainSingle().Which.Should().Be(grainId);
        info.SnapshotSequence.Should().Be(1);
        info.ActiveSnapshotGeneration.Should().Be(1);
        info.PendingSnapshotGeneration.Should().Be(0);
        durableSnapshot.Tombstoned.Should().BeFalse();
        durableSnapshot.Generation.Should().Be(1);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit, false)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit, true)]
    public async Task SnapshotReservationManifestFailureRecoversWithoutManualDeactivation(
        PhysicalWriteFaultStage stage,
        bool reservationWasDurable)
    {
        var providerName = CreateProviderName();
        var storage = CreateStorage(providerName, segmentCapacity: 2, maximumReplayEntries: 8, compactionThreshold: 8);
        var grainId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            grainId,
            CreateState("reservation-boundary", 10));
        var partition = GetPartition(providerName);
        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "manifest",
            stage);

        Func<Task> compact = () => partition.CompactAsync();
        await AssertInjectedFailureAsync(compact);

        await AssertRecordAndIndexesAsync(
            storage,
            providerName,
            grainId,
            "reservation-boundary",
            10);
        var info = await partition.GetPersistenceInfoAsync();

        info.PendingSnapshotGeneration.Should().Be(0);
        info.ActiveSnapshotGeneration.Should().Be(reservationWasDurable ? 1 : 0);
        info.SnapshotSequence.Should().Be(reservationWasDurable ? 1 : 0);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit)]
    public async Task SnapshotPublicationManifestFailureRecoversWithoutManualDeactivation(
        PhysicalWriteFaultStage stage)
    {
        var providerName = CreateProviderName();
        var storage = CreateStorage(providerName, segmentCapacity: 2, maximumReplayEntries: 8, compactionThreshold: 8);
        var grainId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            grainId,
            CreateState("publish-orphan", 10));
        var partition = GetPartition(providerName);
        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "manifest",
            stage,
            call: 2);

        Func<Task> compact = () => partition.CompactAsync();
        await AssertInjectedFailureAsync(compact);

        var loaded = await ReadStateAsync(storage, grainId);
        var info = await partition.GetPersistenceInfoAsync();
        var snapshot = await GetSnapshot(providerName, slot: 0).ReadAsync();

        loaded.State.City.Should().Be("publish-orphan");
        info.SnapshotSequence.Should().Be(1);
        info.ActiveSnapshotGeneration.Should().Be(1);
        info.PendingSnapshotGeneration.Should().Be(0);
        snapshot.Tombstoned.Should().BeFalse();
        snapshot.Generation.Should().Be(1);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit)]
    public async Task JournalRetirementFailureCompletesCleanupOnNextCall(
        PhysicalWriteFaultStage stage)
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var grainId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            grainId,
            CreateState("journal-retirement", 10));
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            CreateGrainId(),
            CreateState("journal-neighbor", 20));
        var partition = GetPartition(providerName);
        var journal = GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries);
        await AddWriteFaultAsync(journal.GetGrainId(), "journal", stage);

        Func<Task> compact = () => partition.CompactAsync();
        await AssertInjectedFailureAsync(compact);

        await AssertRecordAndIndexesAsync(
            storage,
            providerName,
            grainId,
            "journal-retirement",
            10);
        var info = await partition.GetPersistenceInfoAsync();
        var retired = await journal.ReadAsync();

        info.SnapshotSequence.Should().Be(2);
        info.PrunedSequence.Should().Be(2);
        retired.Tombstoned.Should().BeTrue();
        retired.Entries.Should().BeEmpty();
    }

    [SkippableFact]
    public async Task ActivationDoesNotServeRecordsWhenRequiredCleanupFailsAgain()
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var grainId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            grainId,
            CreateState("cleanup-retry", 10));
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            CreateGrainId(),
            CreateState("cleanup-retry-neighbor", 20));
        var partition = GetPartition(providerName);
        var journal = GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries);
        await AddWriteFaultAsync(
            journal.GetGrainId(),
            "journal",
            PhysicalWriteFaultStage.BeforeCommit);
        Func<Task> compact = () => partition.CompactAsync();
        await AssertInjectedFailureAsync(compact);
        await AddWriteFaultAsync(
            journal.GetGrainId(),
            "journal",
            PhysicalWriteFaultStage.BeforeCommit);
        var writesBeforeRecovery = await GetWriteCallCountAsync(journal.GetGrainId(), "journal");

        var loaded = await ReadStateAsync(storage, grainId);

        (await GetWriteCallCountAsync(journal.GetGrainId(), "journal"))
            .Should().Be(writesBeforeRecovery + 2);
        var info = await partition.GetPersistenceInfoAsync();
        var retired = await journal.ReadAsync();
        info.SnapshotSequence.Should().Be(2);
        info.PrunedSequence.Should().Be(2);
        retired.Tombstoned.Should().BeTrue();
        retired.Entries.Should().BeEmpty();
        loaded.RecordExists.Should().BeTrue();
        loaded.State.City.Should().Be("cleanup-retry");
        loaded.State.Salary.Should().Be(10);

        await AssertRecordAndIndexesAsync(
            storage,
            providerName,
            grainId,
            "cleanup-retry",
            10);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit)]
    public async Task SnapshotRetirementFailureCompletesCleanupOnNextCall(
        PhysicalWriteFaultStage stage)
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var grainId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            grainId,
            CreateState("snapshot-retirement", 10));
        var partition = GetPartition(providerName);
        await partition.CompactAsync();
        var firstSnapshot = GetSnapshot(providerName, slot: 0);
        await AddWriteFaultAsync(firstSnapshot.GetGrainId(), "snapshot", stage);
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            CreateGrainId(),
            CreateState("snapshot-neighbor", 20));

        Func<Task> compact = () => partition.CompactAsync();
        await AssertInjectedFailureAsync(compact);

        await AssertRecordAndIndexesAsync(
            storage,
            providerName,
            grainId,
            "snapshot-retirement",
            10);
        var info = await partition.GetPersistenceInfoAsync();
        var retired = await firstSnapshot.ReadAsync();
        var active = await GetSnapshot(providerName, slot: 1).ReadAsync();

        info.ActiveSnapshotGeneration.Should().Be(2);
        info.RetiringSnapshotGeneration.Should().Be(0);
        info.SnapshotSequence.Should().Be(2);
        retired.Tombstoned.Should().BeTrue();
        retired.Generation.Should().Be(1);
        retired.Records.Should().BeEmpty();
        active.Tombstoned.Should().BeFalse();
        active.Generation.Should().Be(2);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit)]
    public async Task CleanupManifestFailureRecoversRetiredChildrenOnNextCall(
        PhysicalWriteFaultStage stage)
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 8;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var grainId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            grainId,
            CreateState("cleanup-manifest", 10));
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            CreateGrainId(),
            CreateState("cleanup-neighbor", 20));
        var partition = GetPartition(providerName);
        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "manifest",
            stage,
            call: 3);

        Func<Task> compact = () => partition.CompactAsync();
        await AssertInjectedFailureAsync(compact);

        await AssertRecordAndIndexesAsync(
            storage,
            providerName,
            grainId,
            "cleanup-manifest",
            10);
        var info = await partition.GetPersistenceInfoAsync();
        var journal = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 0,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();

        info.SnapshotSequence.Should().Be(2);
        info.PrunedSequence.Should().Be(2);
        info.RetiringSnapshotGeneration.Should().Be(0);
        journal.Tombstoned.Should().BeTrue();
        journal.Entries.Should().BeEmpty();
    }

    [SkippableFact]
    public async Task RetiredJournalRingSlotCanBeReusedButRejectsItsDelayedOldSegment()
    {
        const int segmentCapacity = 1;
        const int maximumReplayEntries = 2;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var partition = GetPartition(providerName);
        var ids = Enumerable.Range(0, 5).Select(_ => CreateGrainId()).ToArray();
        StorageJournalEntry? delayedEntry = null;

        for (var index = 0; index < ids.Length; index++)
        {
            await storage.WriteStateAsync(
                VacancyGrain.StateName,
                ids[index],
                CreateState($"ring-{index}", index));
            if (index == 0)
            {
                delayedEntry = (await GetJournalSegment(
                    providerName,
                    absoluteSegmentIndex: 0,
                    segmentCapacity,
                    maximumReplayEntries).ReadAsync()).Entries.Should().ContainSingle().Which;
            }

            if (index < ids.Length - 1)
            {
                await partition.CompactAsync();
            }
        }

        var reusedSlot = GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 4,
            segmentCapacity,
            maximumReplayEntries);
        var reusedState = await reusedSlot.ReadAsync();
        reusedState.AbsoluteSegmentIndex.Should().Be(4);
        reusedState.Tombstoned.Should().BeFalse();
        reusedState.Entries.Select(static entry => entry.Sequence).Should().Equal(5);

        Func<Task> delayedStore = () => reusedSlot.StoreAsync(
            delayedEntry!,
            committedSequence: 0,
            committedOperationId: Guid.Empty,
            absoluteSegmentIndex: 0,
            segmentCapacity);
        await delayedStore.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*stale*");

        await Fixture.Cluster.DeactivateAsync(partition);
        await AssertRecordAndIndexesAsync(
            storage,
            providerName,
            ids[^1],
            "ring-4",
            4);
    }

    [SkippableTheory]
    [InlineData(PhysicalWriteFaultStage.BeforeCommit, true)]
    [InlineData(PhysicalWriteFaultStage.AfterCommit, false)]
    public async Task ClearFaultWrapperDistinguishesBeforeCommitFromLostAcknowledgement(
        PhysicalWriteFaultStage stage,
        bool expectedToExist)
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var physical = silo.ServiceProvider.GetRequiredKeyedService<IGrainStorage>(
            SearchableStorageConstants.PhysicalStorageProviderName);
        var grainId = GrainId.Create("clear-fault-probe", Guid.NewGuid().ToString("N"));
        var stateName = $"clear-fault-{Guid.NewGuid():N}";
        var state = new GrainState<List<string>> { State = ["value"] };
        await physical.WriteStateAsync(stateName, grainId, state);
        await WriteFaultInjectingGrainStorage.AddClearFaultAsync(
            Fixture.Cluster.GrainFactory,
            grainId,
            stateName,
            stage);

        Func<Task> clear = () => physical.ClearStateAsync(stateName, grainId, state);
        await AssertInjectedFailureAsync(clear);

        var loaded = new GrainState<List<string>>();
        await physical.ReadStateAsync(stateName, grainId, loaded);
        loaded.RecordExists.Should().Be(expectedToExist);
        if (expectedToExist)
        {
            loaded.State.Should().ContainSingle().Which.Should().Be("value");
            await physical.ClearStateAsync(stateName, grainId, loaded);
        }
    }

    [SkippableFact]
    public async Task HardReplayLimitBackpressuresBeforeAllocatingAnotherJournalEntry()
    {
        const int segmentCapacity = 2;
        const int maximumReplayEntries = 2;
        var providerName = CreateProviderName();
        var storage = CreateStorage(
            providerName,
            segmentCapacity,
            maximumReplayEntries,
            compactionThreshold: maximumReplayEntries);
        var firstId = CreateGrainId();
        var secondId = CreateGrainId();
        var thirdId = CreateGrainId();
        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            firstId,
            CreateState("first", 1));
        var partition = GetPartition(providerName);
        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "manifest",
            PhysicalWriteFaultStage.BeforeCommit,
            call: 2);

        await storage.WriteStateAsync(
            VacancyGrain.StateName,
            secondId,
            CreateState("second", 2));
        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "manifest",
            PhysicalWriteFaultStage.BeforeCommit);
        var third = CreateState("third", 3);

        Func<Task> write = () => storage.WriteStateAsync(VacancyGrain.StateName, thirdId, third);
        await AssertInjectedFailureAsync(write);

        var info = await partition.GetPersistenceInfoAsync();
        var unallocated = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 1,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();
        var firstMatches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "first");
        var secondMatches = await CreateClient(providerName).FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            "second");

        info.CommittedSequence.Should().Be(2);
        info.SnapshotSequence.Should().Be(0);
        info.RecordCount.Should().Be(2);
        unallocated.Initialized.Should().BeFalse();
        firstMatches.Should().ContainSingle().Which.Should().Be(firstId);
        secondMatches.Should().ContainSingle().Which.Should().Be(secondId);

        await AddWriteFaultAsync(
            partition.GetGrainId(),
            "manifest",
            PhysicalWriteFaultStage.BeforeCommit);
        await AssertInjectedFailureAsync(write);
        var stillBounded = await partition.GetPersistenceInfoAsync();
        var stillUnallocated = await GetJournalSegment(
            providerName,
            absoluteSegmentIndex: 1,
            segmentCapacity,
            maximumReplayEntries).ReadAsync();
        stillBounded.CommittedSequence.Should().Be(2);
        stillBounded.SnapshotSequence.Should().Be(0);
        stillUnallocated.Initialized.Should().BeFalse();

        await storage.WriteStateAsync(VacancyGrain.StateName, thirdId, third);
        var recovered = await partition.GetPersistenceInfoAsync();
        recovered.CommittedSequence.Should().Be(3);
        recovered.SnapshotSequence.Should().Be(2);
        recovered.RecordCount.Should().Be(3);
    }

    private SearchableGrainStorage CreateStorage(
        string providerName,
        int segmentCapacity,
        int maximumReplayEntries,
        int compactionThreshold)
    {
        var silo = Assert.IsType<InProcessSiloHandle>(Fixture.Cluster.Primary);
        var configuredOptions = silo.ServiceProvider
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(VacancyGrain.StorageProviderName);
        return ActivatorUtilities.CreateInstance<SearchableGrainStorage>(
            silo.ServiceProvider,
            providerName,
            new SearchableStorageOptions
            {
                PartitionCount = 1,
                JournalSegmentCapacity = segmentCapacity,
                MaximumJournalReplayEntries = maximumReplayEntries,
                CompactionThreshold = compactionThreshold,
                GrainStorageSerializer = configuredOptions.GrainStorageSerializer,
            });
    }

    private SearchableStorageClient CreateClient(string providerName)
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "journal-facet-tests",
            new byte[32]);
        return new SearchableStorageClient(
            Fixture.Cluster.GrainFactory,
            providerName,
            partitionCount: 1,
            options);
    }

    private IStoragePartitionGrain GetPartition(string providerName)
    {
        return Fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, partitionIndex: 0));
    }

    private IStorageJournalSegmentGrain GetJournalSegment(
        string providerName,
        long absoluteSegmentIndex,
        int segmentCapacity,
        int maximumReplayEntries)
    {
        var partitionKey = StorageLayout.CreatePartitionKey(providerName, partitionIndex: 0);
        var slotCount = StoragePersistence.GetJournalSlotCount(maximumReplayEntries, segmentCapacity);
        var slotIndex = StoragePersistence.GetJournalSlotIndex(
            absoluteSegmentIndex,
            maximumReplayEntries,
            segmentCapacity);
        return Fixture.Cluster.GrainFactory.GetGrain<IStorageJournalSegmentGrain>(
            StoragePersistence.CreateJournalSlotKey(partitionKey, slotIndex, slotCount));
    }

    private IStorageSnapshotGrain GetSnapshot(string providerName, int slot)
    {
        var partitionKey = StorageLayout.CreatePartitionKey(providerName, partitionIndex: 0);
        return Fixture.Cluster.GrainFactory.GetGrain<IStorageSnapshotGrain>(
            StoragePersistence.CreateSnapshotSlotKey(partitionKey, slot));
    }

    private Task AddWriteFaultAsync(
        GrainId grainId,
        string stateName,
        PhysicalWriteFaultStage stage,
        int call = 1)
    {
        return WriteFaultInjectingGrainStorage.AddWriteFaultAsync(
            Fixture.Cluster.GrainFactory,
            grainId,
            stateName,
            stage,
            call);
    }

    private Task<int> GetWriteCallCountAsync(GrainId grainId, string stateName)
    {
        return WriteFaultInjectingGrainStorage.GetWriteCallCountAsync(
            Fixture.Cluster.GrainFactory,
            grainId,
            stateName);
    }

    private static async Task AssertInjectedFailureAsync(Func<Task> action)
    {
        var exception = await action.Should().ThrowAsync<Exception>();
        exception.Which.ToString().Should().Contain(
            WriteFaultInjectingGrainStorage.InjectedFailureMessage);
    }

    private GrainId CreateGrainId()
    {
        return Fixture.Cluster.GrainFactory
            .GetGrain<IVacancyGrain>(Guid.NewGuid().ToString("N"))
            .GetGrainId();
    }

    private static GrainState<VacancyState> CreateState(string city, int salary)
    {
        return new GrainState<VacancyState>
        {
            State = new VacancyState { City = city, Salary = salary },
        };
    }

    private static async Task<GrainState<VacancyState>> ReadStateAsync(
        SearchableGrainStorage storage,
        GrainId grainId)
    {
        var state = new GrainState<VacancyState>();
        await storage.ReadStateAsync(VacancyGrain.StateName, grainId, state);
        return state;
    }

    private async Task AssertRecordAndIndexesAsync(
        SearchableGrainStorage storage,
        string providerName,
        GrainId grainId,
        string city,
        int salary)
    {
        var loaded = await ReadStateAsync(storage, grainId);
        var client = CreateClient(providerName);
        var cityMatches = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            city);
        var salaryMatches = await client.FindAsync<VacancyState, int>(
            VacancyGrain.StateName,
            state => state.Salary,
            salary);

        loaded.RecordExists.Should().BeTrue();
        loaded.State.City.Should().Be(city);
        loaded.State.Salary.Should().Be(salary);
        cityMatches.Should().ContainSingle().Which.Should().Be(grainId);
        salaryMatches.Should().ContainSingle().Which.Should().Be(grainId);
    }

    private static string CreateProviderName()
    {
        return $"journal-{Guid.NewGuid():N}";
    }
}
