using System.Collections.Concurrent;
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

internal static class StorageMovementProviderContract
{
    private const int InitialPartitionCount = 2;
    private const int VirtualSlotCount = 8;
    private const int MovedRecordCount = 5;
    private const int MaximumTransientQueryAttempts = 32;

    public static async Task AssertMoveUnderLoadAsync(ISearchableStorageFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        var providerName = $"movement-contract-{Guid.NewGuid():N}";
        var silo = Assert.IsType<InProcessSiloHandle>(fixture.Cluster.Primary);
        var configuredOptions = silo.ServiceProvider
            .GetRequiredService<IOptionsMonitor<SearchableStorageOptions>>()
            .Get(VacancyGrain.StorageProviderName);
        var providerOptions = new SearchableStorageOptions
        {
            PartitionCount = InitialPartitionCount,
            VirtualSlotTargetCount = VirtualSlotCount,
            JournalSegmentCapacity = 8,
            MaximumJournalReplayEntries = 64,
            CompactionThreshold = 32,
            GrainStorageSerializer = configuredOptions.GrainStorageSerializer,
        };
        var storage = ActivatorUtilities.CreateInstance<SearchableGrainStorage>(
            silo.ServiceProvider,
            providerName,
            providerOptions);
        var movementOptions = new SearchableStorageMovementOptions
        {
            TransferPageRecordLimit = 2,
            TransferPageByteTarget = 16 * 1024,
        };
        var admin = new SearchableStorageAdminClient(
            fixture.Cluster.GrainFactory,
            providerName,
            InitialPartitionCount,
            movementOptions);
        var queryOptions = new SearchableStorageQueryOptions();
        queryOptions.ContinuationProtection.CurrentKey =
            new SearchableStorageContinuationKey("movement-contract-v1", new byte[32]);
        var queryClient = new SearchableStorageClient(
            fixture.Cluster.GrainFactory,
            providerName,
            InitialPartitionCount,
            queryOptions);
        var allRecords = new Dictionary<GrainId, GrainState<VacancyState>>();
        using var writerTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        try
        {
            // A public storage write initializes layout format 4 before the explicit movement gate.
            var initializerId = CreateGrainId(fixture, providerName, ordinal: 0);
            var initializer = CreateState($"initializer-{providerName}", 1);
            await storage.WriteStateAsync(VacancyGrain.StateName, initializerId, initializer);
            allRecords.Add(initializerId, initializer);

            var enabled = await admin.EnableMovementAsync(writerTimeout.Token);
            enabled.MovementState.Should().Be(SearchableStorageMovementState.Enabled);
            enabled.MovementProtocolVersion.Should().Be(StorageMoveProtocol.Version);
            enabled.VirtualSlotCount.Should().Be(VirtualSlotCount);

            var rebalance = await admin.PlanRebalanceAsync(
                InitialPartitionCount + 1,
                writerTimeout.Token);
            rebalance.ActiveMove.Should().BeNull();
            rebalance.RequiredMoveCount.Should().BeGreaterThan(0);
            rebalance.NextMove.Should().NotBeNull();
            var next = rebalance.NextMove!;

            // The write above exists only to initialize the durable layout. Keep the transfer
            // fixture deterministic when its provider-scoped key happens to hash into the slot
            // selected by the rebalance planner.
            if (StorageLayout.GetSlot(initializerId, VirtualSlotCount) == next.Slot)
            {
                await storage.ClearStateAsync(VacancyGrain.StateName, initializerId, initializer);
                allRecords.Remove(initializerId).Should().BeTrue();
            }

            var commonCity = $"moved-{providerName}";
            var moved = CreateGrainIdsInSlot(
                fixture,
                providerName,
                next.Slot,
                VirtualSlotCount,
                MovedRecordCount,
                ordinalStart: 1_000);
            for (var index = 0; index < moved.Length; index++)
            {
                var state = CreateState(commonCity, 10_000 + index);
                await storage.WriteStateAsync(VacancyGrain.StateName, moved[index], state);
                allRecords.Add(moved[index], state);
            }

            var nonMovingSlot = (next.Slot + 1) % VirtualSlotCount;
            var nonMovingId = CreateGrainIdsInSlot(
                fixture,
                providerName,
                nonMovingSlot,
                VirtualSlotCount,
                count: 1,
                ordinalStart: 20_000)[0];
            var nonMovingState = CreateState($"non-moving-{providerName}", 20_000);
            await storage.WriteStateAsync(VacancyGrain.StateName, nonMovingId, nonMovingState);
            allRecords.Add(nonMovingId, nonMovingState);
            var singletonFacetValues = new HashSet<string>(StringComparer.Ordinal)
            {
                nonMovingState.State.City,
            };
            if (allRecords.ContainsKey(initializerId))
            {
                singletonFacetValues.Add(initializer.State.City);
            }

            var source = fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, next.SourcePartitionIndex));
            var target = fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, next.TargetPartitionIndex));
            _ = await AssertAbortCheckpointAsync(
                admin,
                storage,
                queryClient,
                source,
                target,
                next,
                moved,
                commonCity,
                singletonFacetValues,
                SearchableStorageSlotMovePhase.Planned,
                expectedStagedTargetRecords: 0,
                writerTimeout.Token);
            _ = await AssertAbortCheckpointAsync(
                admin,
                storage,
                queryClient,
                source,
                target,
                next,
                moved,
                commonCity,
                singletonFacetValues,
                SearchableStorageSlotMovePhase.SourceFrozen,
                expectedStagedTargetRecords: 0,
                writerTimeout.Token);
            var targetFencedHighWater = await AssertAbortCheckpointAsync(
                admin,
                storage,
                queryClient,
                source,
                target,
                next,
                moved,
                commonCity,
                singletonFacetValues,
                SearchableStorageSlotMovePhase.TargetVersionFenced,
                expectedStagedTargetRecords: 0,
                writerTimeout.Token);
            var partialImportHighWater = await AssertAbortCheckpointAsync(
                admin,
                storage,
                queryClient,
                source,
                target,
                next,
                moved,
                commonCity,
                singletonFacetValues,
                SearchableStorageSlotMovePhase.Copying,
                expectedStagedTargetRecords: movementOptions.TransferPageRecordLimit,
                writerTimeout.Token);
            var importCompleteHighWater = await AssertAbortCheckpointAsync(
                admin,
                storage,
                queryClient,
                source,
                target,
                next,
                moved,
                commonCity,
                singletonFacetValues,
                SearchableStorageSlotMovePhase.CopyComplete,
                expectedStagedTargetRecords: MovedRecordCount,
                writerTimeout.Token);
            var maximumAbortedHighWater = Math.Max(
                targetFencedHighWater,
                Math.Max(partialImportHighWater, importCompleteHighWater));

            var planned = await admin.PlanMoveAsync(
                next.Slot,
                next.TargetPartitionIndex,
                writerTimeout.Token);
            planned.Phase.Should().Be(SearchableStorageSlotMovePhase.Planned);
            var frozen = await admin.AdvanceMoveAsync(planned.MoveId, writerTimeout.Token);
            if (frozen.Phase == SearchableStorageSlotMovePhase.Planned)
            {
                frozen = await admin.AdvanceMoveAsync(planned.MoveId, writerTimeout.Token);
            }

            frozen.Phase.Should().Be(SearchableStorageSlotMovePhase.SourceFrozen);

            var unexpectedWriterFailures = new ConcurrentQueue<Exception>();
            var frozenAttemptObserved = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var postCommitWriteObserved = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var queryAttemptObserved = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var moveExecutionFinished = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var movingWriter = RetryMovingRecordUntilEnabledAsync(
                storage,
                moved[0],
                commonCity,
                frozenAttemptObserved,
                postCommitWriteObserved,
                unexpectedWriterFailures,
                writerTimeout.Token);
            var nonMovingWriter = ExerciseNonMovingWritesAsync(
                storage,
                nonMovingId,
                nonMovingState.State.City,
                unexpectedWriterFailures,
                writerTimeout.Token);
            var queryReader = ReadQueriesDuringMoveAsync(
                queryClient,
                moved,
                commonCity,
                queryAttemptObserved,
                moveExecutionFinished.Task,
                unexpectedWriterFailures,
                writerTimeout.Token);

            await frozenAttemptObserved.Task.WaitAsync(writerTimeout.Token);
            await queryAttemptObserved.Task.WaitAsync(writerTimeout.Token);
            await AssertMovedStateWithRetriesAsync(
                storage,
                queryClient,
                moved,
                commonCity,
                expectedMinimumSalary: 10_000,
                writerTimeout.Token);
            SearchableStorageSlotMoveProgress completed;
            var observedSuccessfulPhases = new HashSet<SearchableStorageSlotMovePhase>
            {
                frozen.Phase,
            };
            try
            {
                var progress = frozen;
                while (!progress.IsComplete)
                {
                    progress = await admin.AdvanceMoveAsync(planned.MoveId, writerTimeout.Token);
                    observedSuccessfulPhases.Add(progress.Phase);
                    await AssertMovedStateWithRetriesAsync(
                        storage,
                        queryClient,
                        moved,
                        commonCity,
                        expectedMinimumSalary: 10_000,
                        writerTimeout.Token);
                }

                completed = progress;
            }
            finally
            {
                moveExecutionFinished.TrySetResult();
            }
            completed.Phase.Should().Be(SearchableStorageSlotMovePhase.Completed);
            completed.IsComplete.Should().BeTrue();
            completed.ExportedRecordCount.Should().BeGreaterThanOrEqualTo(MovedRecordCount);
            completed.DeletedRecordCount.Should().Be(completed.ExportedRecordCount);
            completed.ExportedByteCount.Should().BeGreaterThan(0);
            completed.DeletedByteCount.Should().BeGreaterThan(0);
            foreach (var phase in Enum.GetValues<SearchableStorageSlotMovePhase>())
            {
                if (phase >= SearchableStorageSlotMovePhase.SourceFrozen
                    && phase != SearchableStorageSlotMovePhase.Aborting
                    && phase != SearchableStorageSlotMovePhase.Aborted)
                {
                    observedSuccessfulPhases.Should().Contain(
                        phase,
                        $"the real provider move must expose and query phase {phase}");
                }
            }

            await postCommitWriteObserved.Task.WaitAsync(writerTimeout.Token);
            var queryObservationCount = await queryReader;
            await Task.WhenAll(movingWriter, nonMovingWriter);
            unexpectedWriterFailures.Should().BeEmpty();
            queryObservationCount.Should().BeGreaterThanOrEqualTo(
                2,
                "at least one frozen-source and one concurrent ownership-transition query must complete");

            var targetAfterPostCommitWrite = await target.GetMovementStateAsync();
            targetAfterPostCommitWrite.NextVersion.Should().BeGreaterThan(
                maximumAbortedHighWater,
                "an aborted import high-water fence must never be rolled back or reused");

            var currentLayout = await admin.GetLayoutAsync(writerTimeout.Token);
            currentLayout.Should().NotBeNull();
            currentLayout!.Epoch.Should().Be(completed.CurrentEpoch);
            currentLayout.ActiveMove.Should().BeNull();
            currentLayout.Partitions.Sum(static partition => partition.SlotCount)
                .Should().Be(VirtualSlotCount);

            await fixture.Cluster.DeactivateAsync(source);
            await fixture.Cluster.DeactivateAsync(target);

            await AssertMovedStateAsync(
                storage,
                queryClient,
                moved,
                commonCity,
                expectedMinimumSalary: 10_001);

            var routedRead = new RoutedStorageReadRequest
            {
                RecordKey = CreateStoredRecordKey(VacancyGrain.StateName, moved[0]),
                GrainId = moved[0],
                Slot = next.Slot,
                Epoch = currentLayout.Epoch,
            };
            var staleEpochRead = new RoutedStorageReadRequest
            {
                RecordKey = routedRead.RecordKey,
                GrainId = routedRead.GrainId,
                Slot = routedRead.Slot,
                Epoch = checked(currentLayout.Epoch - 1),
            };
            Func<Task> fencedOldEpochRead = () => source.ReadRoutedAsync(staleEpochRead);
            var fenced = (await fencedOldEpochRead
                .Should().ThrowAsync<StorageRouteMismatchException>()).Which;
            fenced.ExpectedEpoch.Should().Be(staleEpochRead.Epoch);
            fenced.CurrentEpoch.Should().Be(currentLayout.Epoch);
            fenced.RequestedPartition.Should().Be(next.SourcePartitionIndex);

            Func<Task> staleSourceRead = () => source.ReadRoutedAsync(routedRead);
            var mismatch = (await staleSourceRead
                .Should().ThrowAsync<StorageRouteMismatchException>()).Which;
            mismatch.RequestedPartition.Should().Be(next.SourcePartitionIndex);
            mismatch.CurrentOwner.Should().Be(next.TargetPartitionIndex);

            var targetRead = await target.ReadRoutedAsync(routedRead);
            targetRead.Found.Should().BeTrue();

            var sourceBeforeRejectedMutations = await source.GetPersistenceInfoAsync();
            var routedWrite = new RoutedStorageWriteRequest
            {
                Request = new StorageWriteRequest
                {
                    RecordKey = routedRead.RecordKey,
                    GrainId = routedRead.GrainId,
                    Payload = [.. targetRead.Payload!],
                    ExpectedETag = targetRead.ETag,
                    IndexEntries = [],
                    Persistence = CreatePersistenceSettings(providerOptions),
                },
                Slot = routedRead.Slot,
                Epoch = routedRead.Epoch,
            };
            var oldEpochWrite = new RoutedStorageWriteRequest
            {
                Request = routedWrite.Request,
                Slot = routedWrite.Slot,
                Epoch = staleEpochRead.Epoch,
            };
            var routedClear = new RoutedStorageClearRequest
            {
                Request = new StorageClearRequest
                {
                    RecordKey = routedRead.RecordKey,
                    ExpectedETag = targetRead.ETag,
                    Persistence = CreatePersistenceSettings(providerOptions),
                },
                Slot = routedRead.Slot,
                Epoch = routedRead.Epoch,
                GrainId = routedRead.GrainId,
            };

            Func<Task> staleSourceWrite = () => source.WriteRoutedAsync(routedWrite);
            Func<Task> oldEpochSourceWrite = () => source.WriteRoutedAsync(oldEpochWrite);
            Func<Task> staleSourceClear = () => source.ClearRoutedAsync(routedClear);
            await staleSourceWrite.Should().ThrowAsync<StorageRouteMismatchException>();
            await oldEpochSourceWrite.Should().ThrowAsync<StorageRouteMismatchException>();
            await staleSourceClear.Should().ThrowAsync<StorageRouteMismatchException>();

            var sourceAfterRejectedMutations = await source.GetPersistenceInfoAsync();
            sourceAfterRejectedMutations.RecordCount.Should().Be(sourceBeforeRejectedMutations.RecordCount);
            var targetAfterRejectedMutations = await target.ReadRoutedAsync(routedRead);
            targetAfterRejectedMutations.ETag.Should().Be(targetRead.ETag);
            targetAfterRejectedMutations.Payload.Should().Equal(targetRead.Payload!);
            await AssertMovedStateAsync(
                storage,
                queryClient,
                moved,
                commonCity,
                expectedMinimumSalary: 10_001);

            var active = await admin.GetMoveAsync(writerTimeout.Token);
            active.Should().BeNull();
        }
        finally
        {
            foreach (var (grainId, state) in allRecords)
            {
                try
                {
                    var current = new GrainState<VacancyState>();
                    await storage.ReadStateAsync(VacancyGrain.StateName, grainId, current);
                    if (current.RecordExists)
                    {
                        await storage.ClearStateAsync(VacancyGrain.StateName, grainId, current);
                    }
                }
                catch
                {
                    // Preserve the primary protocol assertion. The backend fixture owns namespace
                    // cleanup and will remove this unique provider's remaining physical records.
                    _ = state;
                }
            }
        }
    }

    private static async Task<long> AssertAbortCheckpointAsync(
        SearchableStorageAdminClient admin,
        SearchableGrainStorage storage,
        SearchableStorageClient queryClient,
        IStoragePartitionGrain source,
        IStoragePartitionGrain target,
        SearchableStorageSlotMovePlan move,
        GrainId[] moved,
        string city,
        IReadOnlySet<string> singletonFacetValues,
        SearchableStorageSlotMovePhase checkpoint,
        int expectedStagedTargetRecords,
        CancellationToken cancellationToken)
    {
        var etagsBefore = await ReadEtagsAsync(storage, moved, cancellationToken);
        var planned = await admin.PlanMoveAsync(
            move.Slot,
            move.TargetPartitionIndex,
            cancellationToken);
        planned.Phase.Should().Be(SearchableStorageSlotMovePhase.Planned);

        var progress = checkpoint == SearchableStorageSlotMovePhase.Planned
            ? planned
            : await AdvanceToPhaseAsync(admin, planned, checkpoint, cancellationToken);
        progress.Phase.Should().Be(checkpoint);
        progress.ExportedRecordCount.Should().Be(expectedStagedTargetRecords);
        progress.CurrentEpoch.Should().Be(planned.SourceEpoch);
        progress.CanAbort.Should().BeTrue();

        var sourceAtCheckpoint = await source.GetMovementStateAsync();
        var targetAtCheckpoint = await target.GetMovementStateAsync();
        var targetPersistence = await target.GetPersistenceInfoAsync();
        targetPersistence.RecordCount.Should().Be(expectedStagedTargetRecords);

        var frozenNextVersion = 0L;
        if (checkpoint >= SearchableStorageSlotMovePhase.SourceFrozen)
        {
            sourceAtCheckpoint.MoveControl.IsPresent.Should().BeTrue();
            sourceAtCheckpoint.MoveControl.Phase.Should().Be(StoragePartitionMovePhase.SourceFrozen);
            frozenNextVersion = sourceAtCheckpoint.MoveControl.FrozenNextVersion;
            frozenNextVersion.Should().BeGreaterThan(0);
        }

        if (checkpoint >= SearchableStorageSlotMovePhase.TargetVersionFenced)
        {
            targetAtCheckpoint.MoveControl.IsPresent.Should().BeTrue();
            targetAtCheckpoint.MoveControl.ImportedRecordCount
                .Should().Be(expectedStagedTargetRecords);
            targetAtCheckpoint.NextVersion.Should().BeGreaterThanOrEqualTo(frozenNextVersion);
        }

        if (expectedStagedTargetRecords > 0)
        {
            await AssertFacetAuthorityIgnoresStagedTargetAsync(
                queryClient,
                city,
                moved.Length,
                singletonFacetValues,
                cancellationToken);
        }

        await AssertMovedStateAsync(
            storage,
            queryClient,
            moved,
            city,
            expectedMinimumSalary: 10_000);

        var aborted = await admin.AbortMoveAsync(planned.MoveId, cancellationToken);
        aborted.Phase.Should().Be(SearchableStorageSlotMovePhase.Aborted);
        aborted.IsComplete.Should().BeTrue();
        aborted.CurrentEpoch.Should().Be(planned.SourceEpoch);

        var sourceAfterAbort = await source.GetMovementStateAsync();
        var targetAfterAbort = await target.GetMovementStateAsync();
        sourceAfterAbort.MoveControl.IsPresent.Should().BeFalse();
        targetAfterAbort.MoveControl.IsPresent.Should().BeFalse();
        (await target.GetPersistenceInfoAsync()).RecordCount.Should().Be(0);
        if (checkpoint >= SearchableStorageSlotMovePhase.TargetVersionFenced)
        {
            targetAfterAbort.NextVersion.Should().BeGreaterThanOrEqualTo(
                frozenNextVersion,
                "target version fences survive rollback even after every staged record is deleted");
        }

        (await admin.GetMoveAsync(cancellationToken)).Should().BeNull();
        await AssertMovedStateAsync(
            storage,
            queryClient,
            moved,
            city,
            expectedMinimumSalary: 10_000);
        await AssertEtagsAsync(storage, etagsBefore, cancellationToken);
        return frozenNextVersion;
    }

    private static async Task<SearchableStorageSlotMoveProgress> AdvanceToPhaseAsync(
        SearchableStorageAdminClient admin,
        SearchableStorageSlotMoveProgress progress,
        SearchableStorageSlotMovePhase expectedPhase,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 16 && progress.Phase != expectedPhase; attempt++)
        {
            progress.IsComplete.Should().BeFalse();
            progress = await admin.AdvanceMoveAsync(progress.MoveId, cancellationToken);
        }

        progress.Phase.Should().Be(
            expectedPhase,
            "one bounded advance must eventually reach the requested durable checkpoint");
        return progress;
    }

    private static async Task AssertFacetAuthorityIgnoresStagedTargetAsync(
        SearchableStorageClient client,
        string city,
        int expectedCount,
        IReadOnlySet<string> singletonFacetValues,
        CancellationToken cancellationToken)
    {
        var query = client.Query<VacancyState>(VacancyGrain.StateName);
        var exact = await query.ToFacetValueCountsAsync(
            state => state.City,
            new SearchableStorageFacetRequest(
                topN: 1,
                accuracy: SearchableStorageFacetAccuracy.Exact),
            cancellationToken);
        exact.IsExact.Should().BeTrue();
        exact.Items.Should().ContainSingle();
        exact.Items[0].Value.Should().Be(city);
        exact.Items[0].Count.Should().Be(expectedCount);

        var approximate = await query.ToFacetValueCountsAsync(
            state => state.City,
            new SearchableStorageFacetRequest(
                topN: 1,
                accuracy: SearchableStorageFacetAccuracy.Approximate),
            cancellationToken);
        approximate.Items.Should().ContainSingle();
        var approximateWinnerIsMovedValue = string.Equals(
            approximate.Items[0].Value,
            city,
            StringComparison.Ordinal);
        if (!approximateWinnerIsMovedValue)
        {
            singletonFacetValues.Should().Contain(
                approximate.Items[0].Value,
                "an approximate nominee must still belong to this provider's known authoritative values");
        }

        approximate.Items[0].Count.Should().Be(
            approximateWinnerIsMovedValue ? expectedCount : 1,
            "every approximate nominee is ownership-filtered and exact-probed before it is returned");
        approximate.MaximumOmittedCount.Should().BeGreaterThanOrEqualTo(
            approximateWinnerIsMovedValue ? 1 : expectedCount,
            "the certificate must cover every omitted known count even when value-ordered candidate paging misses the true winner");
    }

    private static async Task<Dictionary<GrainId, string?>> ReadEtagsAsync(
        SearchableGrainStorage storage,
        GrainId[] grainIds,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<GrainId, string?>(grainIds.Length);
        foreach (var grainId in grainIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var state = new GrainState<VacancyState>();
            await storage.ReadStateAsync(VacancyGrain.StateName, grainId, state);
            state.RecordExists.Should().BeTrue();
            result.Add(grainId, state.ETag);
        }

        return result;
    }

    private static async Task AssertEtagsAsync(
        SearchableGrainStorage storage,
        IReadOnlyDictionary<GrainId, string?> expected,
        CancellationToken cancellationToken)
    {
        foreach (var (grainId, etag) in expected)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var state = new GrainState<VacancyState>();
            await storage.ReadStateAsync(VacancyGrain.StateName, grainId, state);
            state.RecordExists.Should().BeTrue();
            state.ETag.Should().Be(etag, "aborting a move must not rewrite authoritative source records");
        }
    }

    private static async Task<int> ReadQueriesDuringMoveAsync(
        SearchableStorageClient client,
        GrainId[] expected,
        string city,
        TaskCompletionSource firstAttemptObserved,
        Task moveExecutionFinished,
        ConcurrentQueue<Exception> unexpectedFailures,
        CancellationToken cancellationToken)
    {
        var observations = 0;
        var transientAttempts = 0;
        try
        {
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var exact = await client.FindAsync<VacancyState, string>(
                        VacancyGrain.StateName,
                        state => state.City,
                        city,
                        cancellationToken);
                    exact.Should().BeEquivalentTo(expected);
                    exact.Should().OnlyHaveUniqueItems();

                    var counts = await client.Query<VacancyState>(VacancyGrain.StateName)
                        .Where(state => state.City == city)
                        .ToFacetValueCountsAsync(
                            state => state.City,
                            new SearchableStorageFacetRequest(
                                topN: 1,
                                accuracy: SearchableStorageFacetAccuracy.Exact),
                            cancellationToken);
                    counts.IsExact.Should().BeTrue();
                    counts.Items.Should().ContainSingle();
                    counts.Items[0].Value.Should().Be(city);
                    counts.Items[0].Count.Should().Be(expected.Length);
                    observations++;
                    firstAttemptObserved.TrySetResult();
                }
                catch (Exception exception) when (IsRetryableMovementQueryFailure(exception))
                {
                    transientAttempts++;
                    if (transientAttempts > MaximumTransientQueryAttempts)
                    {
                        throw new InvalidOperationException(
                            $"Movement queries did not complete after {MaximumTransientQueryAttempts} "
                            + "documented concurrent-change or second-route retries.",
                            exception);
                    }

                    await Task.Delay(1, cancellationToken);
                }
            }
            while (!moveExecutionFinished.IsCompleted);
        }
        catch (Exception exception)
        {
            unexpectedFailures.Enqueue(exception);
            firstAttemptObserved.TrySetResult();
        }

        return observations;
    }

    private static bool IsRetryableMovementQueryFailure(Exception exception)
    {
        return exception is SearchableStorageFacetConcurrentChangeException
            or StorageRouteMismatchException;
    }

    private static async Task RetryMovingRecordUntilEnabledAsync(
        SearchableGrainStorage storage,
        GrainId grainId,
        string city,
        TaskCompletionSource frozenAttemptObserved,
        TaskCompletionSource postCommitWriteObserved,
        ConcurrentQueue<Exception> unexpectedFailures,
        CancellationToken cancellationToken)
    {
        var revision = 30_000;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var current = new GrainState<VacancyState>();
                await storage.ReadStateAsync(VacancyGrain.StateName, grainId, current);
                current.State.City = city;
                current.State.Salary = revision++;
                await storage.WriteStateAsync(VacancyGrain.StateName, grainId, current);
                postCommitWriteObserved.TrySetResult();
                return;
            }
            catch (InvalidOperationException exception)
                when (exception.ToString().Contains("mutation-frozen", StringComparison.Ordinal))
            {
                frozenAttemptObserved.TrySetResult();
                await Task.Delay(1, cancellationToken);
            }
            catch (Exception exception)
            {
                unexpectedFailures.Enqueue(exception);
                frozenAttemptObserved.TrySetResult();
                postCommitWriteObserved.TrySetResult();
                return;
            }
        }
    }

    private static async Task ExerciseNonMovingWritesAsync(
        SearchableGrainStorage storage,
        GrainId grainId,
        string city,
        ConcurrentQueue<Exception> unexpectedFailures,
        CancellationToken cancellationToken)
    {
        try
        {
            for (var revision = 40_000; revision < 40_008; revision++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var current = new GrainState<VacancyState>();
                await storage.ReadStateAsync(VacancyGrain.StateName, grainId, current);
                current.State.City = city;
                current.State.Salary = revision;
                await storage.WriteStateAsync(VacancyGrain.StateName, grainId, current);
                await Task.Delay(2, cancellationToken);
            }
        }
        catch (Exception exception)
        {
            unexpectedFailures.Enqueue(exception);
        }
    }

    private static async Task AssertMovedStateAsync(
        SearchableGrainStorage storage,
        SearchableStorageClient client,
        GrainId[] moved,
        string city,
        int expectedMinimumSalary)
    {
        foreach (var grainId in moved)
        {
            var state = new GrainState<VacancyState>();
            await storage.ReadStateAsync(VacancyGrain.StateName, grainId, state);
            state.RecordExists.Should().BeTrue();
            state.State.City.Should().Be(city);
        }

        var exact = await client.FindAsync<VacancyState, string>(
            VacancyGrain.StateName,
            state => state.City,
            city);
        exact.Should().BeEquivalentTo(moved);

        var query = client.Query<VacancyState>(VacancyGrain.StateName)
            .Where(state => state.City == city);
        var counts = await query.ToFacetValueCountsAsync(
            state => state.City,
            new SearchableStorageFacetRequest(
                topN: 1,
                accuracy: SearchableStorageFacetAccuracy.Exact));
        counts.IsExact.Should().BeTrue();
        counts.Items.Should().ContainSingle();
        counts.Items[0].Value.Should().Be(city);
        counts.Items[0].Count.Should().Be(moved.Length);

        var extrema = await query.ToFacetMinMaxAsync(state => state.Salary);
        extrema.Should().NotBeNull();
        extrema!.Minimum.Should().BeGreaterThanOrEqualTo(expectedMinimumSalary);
        extrema.Maximum.Should().BeGreaterThanOrEqualTo(extrema.Minimum);
    }

    private static async Task AssertMovedStateWithRetriesAsync(
        SearchableGrainStorage storage,
        SearchableStorageClient client,
        GrainId[] moved,
        string city,
        int expectedMinimumSalary,
        CancellationToken cancellationToken)
    {
        Exception? lastFailure = null;
        for (var attempt = 0; attempt < MaximumTransientQueryAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await AssertMovedStateAsync(
                    storage,
                    client,
                    moved,
                    city,
                    expectedMinimumSalary);
                return;
            }
            catch (Exception exception) when (IsRetryableMovementQueryFailure(exception))
            {
                lastFailure = exception;
                await Task.Delay(1, cancellationToken);
            }
        }

        throw new InvalidOperationException(
            $"Movement membership did not stabilize after {MaximumTransientQueryAttempts} attempts.",
            lastFailure);
    }

    private static GrainId CreateGrainId(
        ISearchableStorageFixture fixture,
        string providerName,
        int ordinal)
    {
        return fixture.Cluster.GrainFactory
            .GetGrain<IVacancyGrain>($"{providerName}-{ordinal:D8}")
            .GetGrainId();
    }

    private static GrainId[] CreateGrainIdsInSlot(
        ISearchableStorageFixture fixture,
        string providerName,
        int slot,
        int virtualSlotCount,
        int count,
        int ordinalStart)
    {
        var result = new List<GrainId>(count);
        for (var ordinal = ordinalStart; ordinal < ordinalStart + 100_000 && result.Count < count; ordinal++)
        {
            var grainId = CreateGrainId(fixture, providerName, ordinal);
            if (StorageLayout.GetSlot(grainId, virtualSlotCount) == slot)
            {
                result.Add(grainId);
            }
        }

        if (result.Count != count)
        {
            throw new InvalidOperationException(
                $"Could not create {count} deterministic grain ids in virtual slot {slot}.");
        }

        return [.. result];
    }

    private static GrainState<VacancyState> CreateState(string city, int salary)
    {
        return new GrainState<VacancyState>
        {
            State = new VacancyState { City = city, Salary = salary },
        };
    }

    private static string CreateStoredRecordKey(string stateName, GrainId grainId)
    {
        return string.Concat(
            stateName,
            "/",
            Convert.ToHexString(grainId.Type.AsSpan()),
            "/",
            Convert.ToHexString(grainId.Key.AsSpan()));
    }

    private static StoragePersistenceSettings CreatePersistenceSettings(
        SearchableStorageOptions options) => new()
    {
        JournalSegmentCapacity = options.JournalSegmentCapacity,
        MaximumJournalReplayEntries = options.MaximumJournalReplayEntries,
        CompactionThreshold = options.CompactionThreshold,
    };
}
