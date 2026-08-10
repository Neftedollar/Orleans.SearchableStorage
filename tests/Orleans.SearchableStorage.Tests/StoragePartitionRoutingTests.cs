using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePartitionRoutingTests : IClassFixture<MemoryStorageFixture>
{
    private readonly MemoryStorageFixture _fixture;

    public StoragePartitionRoutingTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData("read")]
    [InlineData("write")]
    [InlineData("clear")]
    public async Task PointOperationsRejectAValidOwnerSlotWhichDoesNotMatchTheGrain(
        string operation)
    {
        var context = await CreateContextAsync(partitionCount: 2);
        var (grainId, slot) = CreateGrainOwnedBy(context.Layout, owner: 0);
        var differentOwnedSlot = Enumerable.Range(0, context.Layout.VirtualSlotCount)
            .First(candidate => candidate != slot && context.Layout.GetOwner(candidate) == 0);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var recordKey = $"missing/{Guid.NewGuid():N}";

        Func<Task> execute = operation switch
        {
            "read" => () => partition.ReadRoutedAsync(new RoutedStorageReadRequest
            {
                RecordKey = recordKey,
                GrainId = grainId,
                Slot = differentOwnedSlot,
                Epoch = context.Layout.Epoch,
            }),
            "write" => () => partition.WriteRoutedAsync(new RoutedStorageWriteRequest
            {
                Request = CreateWriteRequest(recordKey, grainId, "wrong-slot"),
                Slot = differentOwnedSlot,
                Epoch = context.Layout.Epoch,
            }),
            "clear" => () => partition.ClearRoutedAsync(new RoutedStorageClearRequest
            {
                Request = new StorageClearRequest
                {
                    RecordKey = recordKey,
                    Persistence = CreatePersistenceSettings(),
                },
                GrainId = grainId,
                Slot = differentOwnedSlot,
                Epoch = context.Layout.Epoch,
            }),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };

        await execute.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*does not match the grain's derived slot*");
        var persistence = await partition.GetPersistenceInfoAsync();
        persistence.Initialized.Should().BeFalse();
        persistence.RecordCount.Should().Be(0);
    }

    [Fact]
    public async Task MissingClearReportsTheCurrentOwnerBeforeAcknowledgingAbsence()
    {
        var context = await CreateContextAsync(partitionCount: 2);
        var (grainId, slot) = CreateGrainOwnedBy(context.Layout, owner: 1);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var request = new RoutedStorageClearRequest
        {
            Request = new StorageClearRequest
            {
                RecordKey = $"missing/{Guid.NewGuid():N}",
                Persistence = CreatePersistenceSettings(),
            },
            GrainId = grainId,
            Slot = slot,
            Epoch = context.Layout.Epoch,
        };

        Func<Task> clear = () => partition.ClearRoutedAsync(request);

        var mismatch = (await clear.Should().ThrowAsync<StorageRouteMismatchException>()).Which;
        mismatch.ExpectedEpoch.Should().Be(context.Layout.Epoch);
        mismatch.CurrentEpoch.Should().Be(context.Layout.Epoch);
        mismatch.RequestedPartition.Should().Be(0);
        mismatch.Slot.Should().Be(slot);
        mismatch.CurrentOwner.Should().Be(1);
    }

    [Fact]
    public async Task FoundPointOperationsRejectARecordKeyBoundToAnotherGrain()
    {
        var context = await CreateContextAsync(partitionCount: 2);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var (storedGrain, slot) = CreateGrainOwnedBy(context.Layout, owner: 0);
        var routedGrain = CreateDifferentGrainInSlot(context.Layout, slot, storedGrain);
        var recordKey = $"identity/{Guid.NewGuid():N}";
        var etag = await partition.WriteAsync(CreateWriteRequest(recordKey, storedGrain, "identity"));

        try
        {
            Func<Task> read = () => partition.ReadRoutedAsync(new RoutedStorageReadRequest
            {
                RecordKey = recordKey,
                GrainId = routedGrain,
                Slot = slot,
                Epoch = context.Layout.Epoch,
            });
            Func<Task> clear = () => partition.ClearRoutedAsync(new RoutedStorageClearRequest
            {
                Request = CreateClearRequest(recordKey, etag),
                GrainId = routedGrain,
                Slot = slot,
                Epoch = context.Layout.Epoch,
            });

            await read.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*identifies a different grain*");
            await clear.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*identifies a different grain*");
        }
        finally
        {
            await partition.ClearAsync(CreateClearRequest(recordKey, etag));
        }
    }

    [Fact]
    public async Task RouteMismatchPrecedesStaleETagChecksAndDoesNotMutateTheRecord()
    {
        var context = await CreateContextAsync(partitionCount: 2);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var (grainId, slot) = CreateGrainOwnedBy(context.Layout, owner: 0);
        var recordKey = $"route-before-etag/{Guid.NewGuid():N}";
        var etag = await partition.WriteAsync(CreateWriteRequest(recordKey, grainId, "original", salary: 10));
        var before = await partition.GetPersistenceInfoAsync();

        try
        {
            Func<Task> write = () => partition.WriteRoutedAsync(new RoutedStorageWriteRequest
            {
                Request = CreateWriteRequest(
                    recordKey,
                    grainId,
                    "replacement",
                    salary: 20,
                    expectedETag: "stale"),
                Slot = slot,
                Epoch = checked(context.Layout.Epoch + 1),
            });
            Func<Task> clear = () => partition.ClearRoutedAsync(new RoutedStorageClearRequest
            {
                Request = CreateClearRequest(recordKey, "stale"),
                GrainId = grainId,
                Slot = slot,
                Epoch = checked(context.Layout.Epoch + 1),
            });

            await write.Should().ThrowAsync<StorageRouteMismatchException>();
            await clear.Should().ThrowAsync<StorageRouteMismatchException>();

            var after = await partition.GetPersistenceInfoAsync();
            after.Should().BeEquivalentTo(before);
            var stored = await partition.ReadAsync(recordKey);
            stored.Found.Should().BeTrue();
            stored.ETag.Should().Be(etag);
            (await partition.FindAsync(new ExactIndexQuery
            {
                Scope = "state/city",
                Kind = SearchableIndexKind.Hash,
                Value = IndexValue.Create("original"),
            })).Should().ContainSingle().Which.Should().Be(grainId);
            (await partition.FindAsync(new ExactIndexQuery
            {
                Scope = "state/city",
                Kind = SearchableIndexKind.Hash,
                Value = IndexValue.Create("replacement"),
            })).Should().BeEmpty();
        }
        finally
        {
            await partition.ClearAsync(CreateClearRequest(recordKey, etag));
        }
    }

    [Fact]
    public async Task RoutedQueriesFilterLegacyRecordsWhichBelongToAnotherOwner()
    {
        var context = await CreateContextAsync(partitionCount: 2);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var (ownedGrain, _) = CreateGrainOwnedBy(context.Layout, owner: 0);
        var (foreignGrain, _) = CreateGrainOwnedBy(context.Layout, owner: 1);
        var value = $"routing-{Guid.NewGuid():N}";
        var ownedKey = $"owned/{Guid.NewGuid():N}";
        var foreignKey = $"foreign/{Guid.NewGuid():N}";
        var ownedEtag = await partition.WriteAsync(
            CreateWriteRequest(ownedKey, ownedGrain, value, salary: 10));
        var foreignEtag = await partition.WriteAsync(
            CreateWriteRequest(foreignKey, foreignGrain, value, salary: 10));

        try
        {
            var query = new ExactIndexQuery
            {
                Scope = "state/city",
                Kind = SearchableIndexKind.Hash,
                Value = IndexValue.Create(value),
            };

            var range = new RangeIndexQuery
            {
                Scope = "state/salary",
                LowerBound = IndexValue.Create(5),
                UpperBound = IndexValue.Create(15),
                IncludeLowerBound = true,
                IncludeUpperBound = true,
            };
            var plan = new PartitionQueryPlan
            {
                Operation = PartitionQueryOperation.And,
                Left = new PartitionQueryPlan
                {
                    Operation = PartitionQueryOperation.Exact,
                    Scope = query.Scope,
                    IndexKind = query.Kind,
                    Value = query.Value,
                },
                Right = new PartitionQueryPlan
                {
                    Operation = PartitionQueryOperation.Range,
                    Scope = range.Scope,
                    LowerBound = range.LowerBound,
                    UpperBound = range.UpperBound,
                    IncludeLowerBound = range.IncludeLowerBound,
                    IncludeUpperBound = range.IncludeUpperBound,
                },
            };

            var legacyExact = await partition.FindAsync(query);
            var routedExact = await partition.FindRoutedAsync(new RoutedExactIndexQuery
            {
                Query = query,
                Epoch = context.Layout.Epoch,
            });
            var legacyRange = await partition.RangeAsync(range);
            var routedRange = await partition.RangeRoutedAsync(new RoutedRangeIndexQuery
            {
                Query = range,
                Epoch = context.Layout.Epoch,
            });
            var legacyPlan = await partition.QueryAsync(plan);
            var routedPlan = await partition.QueryRoutedAsync(new RoutedPartitionQuery
            {
                Query = plan,
                Epoch = context.Layout.Epoch,
            });

            legacyExact.Should().BeEquivalentTo([ownedGrain, foreignGrain]);
            legacyRange.Should().BeEquivalentTo([ownedGrain, foreignGrain]);
            legacyPlan.Should().BeEquivalentTo([ownedGrain, foreignGrain]);
            routedExact.Should().ContainSingle().Which.Should().Be(ownedGrain);
            routedRange.Should().ContainSingle().Which.Should().Be(ownedGrain);
            routedPlan.Should().ContainSingle().Which.Should().Be(ownedGrain);
        }
        finally
        {
            await partition.ClearAsync(CreateClearRequest(ownedKey, ownedEtag));
            await partition.ClearAsync(CreateClearRequest(foreignKey, foreignEtag));
        }
    }

    [Fact]
    public async Task MalformedRoutedPlanIsRejectedBeforeRouteValidation()
    {
        var context = await CreateContextAsync(partitionCount: 2);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var request = new RoutedPartitionQuery
        {
            Query = new PartitionQueryPlan
            {
                Operation = (PartitionQueryOperation)int.MaxValue,
            },
            Epoch = checked(context.Layout.Epoch + 1),
        };

        Func<Task> query = async () => await partition.QueryRoutedAsync(request);

        await query.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithMessage("*Unknown partition query operation*");
    }

    [Fact]
    public async Task BoundedPageRoundTripsThroughOrleansAndTracksCommittedMutations()
    {
        const string stateName = "bounded-routing-state";
        var context = await CreateContextAsync(partitionCount: 1);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var (grainId, _) = CreateGrainOwnedBy(context.Layout, owner: 0);
        var recordKey = CreateCanonicalRecordKey(stateName, grainId);
        var etag = await partition.WriteAsync(
            CreateWriteRequest(recordKey, grainId, "initial", salary: 10));
        var initialPlan = ExactCityPlan("initial");

        try
        {
            var initial = await partition.QueryPageRoutedAsync(
                CreatePageRequest(context.Layout, stateName, initialPlan));

            initial.Items.Should().ContainSingle().Which.Should().Be(grainId);
            initial.Exhausted.Should().BeTrue();
            initial.HasFrontier.Should().BeFalse();
            initial.StopReason.Should().Be(PartitionQueryPageStopReason.Exhausted);
            initial.Work.TotalOperationCount.Should().Be(7);
            initial.ItemByteCount.Should().Be(GrainIdCanonicalOrder.GetEncodedLength(grainId));
            initial.ProtocolVersion.Should().Be(QueryProtocol.PagingVersion);
            initial.OrderingVersion.Should().Be(QueryProtocol.OrderingVersion);
            initial.WorkPolicyVersion.Should().Be(QueryProtocol.WorkPolicyVersion);
            initial.ResponseFamily.Should().Be(PartitionQueryResponseFamily.GrainIdPage);
            initial.Epoch.Should().Be(context.Layout.Epoch);
            initial.QueryFingerprint.Should().Equal(QueryPlanFingerprint.Compute(stateName, initialPlan));
            initial.LayoutFingerprint.Should().Equal(StorageLayoutFingerprint.Compute(context.Layout));

            var rangePlan = SalaryRangePlan(0, 20);
            var range = await partition.QueryPageRoutedAsync(
                CreatePageRequest(context.Layout, stateName, rangePlan));
            range.Items.Should().ContainSingle().Which.Should().Be(grainId);
            range.Work.PostingSeekCount.Should().Be(2);
            range.Work.RangeBucketVisitCount.Should().Be(1);
            range.Work.RangeMergeOperationCount.Should().Be(1);
            range.Work.TotalOperationCount.Should().Be(11);

            await _fixture.Cluster.DeactivateAsync(partition);
            partition = GetPartition(context.ProviderName, partitionIndex: 0);
            var rebuilt = await partition.QueryPageRoutedAsync(
                CreatePageRequest(context.Layout, stateName, initialPlan));
            rebuilt.Items.Should().ContainSingle().Which.Should().Be(grainId);
            rebuilt.Work.Should().BeEquivalentTo(initial.Work);

            var insufficient = CopyPageRequest(
                CreatePageRequest(context.Layout, stateName, initialPlan),
                workBudget: 2);
            Func<Task> insufficientCall = async () =>
                await partition.QueryPageRoutedAsync(insufficient);
            var budgetException = (await insufficientCall
                .Should().ThrowAsync<PartitionQueryBudgetTooSmallException>()).Which;
            budgetException.RequestedLimit.Should().Be(2);
            budgetException.MinimumRequired.Should().Be(3);
            budgetException.Reason.Should().Be(PartitionQueryPageStopReason.WorkBudget);

            var replacementEtag = await partition.WriteAsync(
                CreateWriteRequest(
                    recordKey,
                    grainId,
                    "replacement",
                    salary: 20,
                    expectedETag: etag));
            etag = replacementEtag;

            (await partition.QueryPageRoutedAsync(
                    CreatePageRequest(context.Layout, stateName, initialPlan)))
                .Items.Should().BeEmpty();
            (await partition.QueryPageRoutedAsync(
                    CreatePageRequest(context.Layout, stateName, ExactCityPlan("replacement"))))
                .Items.Should().ContainSingle().Which.Should().Be(grainId);
        }
        finally
        {
            await partition.ClearAsync(CreateClearRequest(recordKey, etag));
        }

        (await partition.QueryPageRoutedAsync(
                CreatePageRequest(context.Layout, stateName, ExactCityPlan("replacement"))))
            .Items.Should().BeEmpty();
    }

    [Fact]
    public async Task BoundedPageValidatesPlanFingerprintAndCapsBeforeStaleRouteLookup()
    {
        const string stateName = "bounded-validation-state";
        var context = await CreateContextAsync(partitionCount: 1);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var plan = ExactCityPlan("value");
        var staleEpoch = checked(context.Layout.Epoch + 1);

        var malformed = CreatePageRequest(context.Layout, stateName, plan);
        malformed = CopyPageRequest(
            malformed,
            query: new PartitionQueryPlan { Operation = (PartitionQueryOperation)int.MaxValue },
            epoch: staleEpoch);
        Func<Task> malformedCall = async () => await partition.QueryPageRoutedAsync(malformed);
        await malformedCall.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithMessage("*Unknown partition query operation*");

        var wrongFingerprint = CopyPageRequest(
            CreatePageRequest(context.Layout, stateName, plan),
            epoch: staleEpoch,
            queryFingerprint: new byte[32]);
        Func<Task> fingerprintCall = async () => await partition.QueryPageRoutedAsync(wrongFingerprint);
        await fingerprintCall.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*query fingerprint does not match*");

        var oversizedWork = CopyPageRequest(
            CreatePageRequest(context.Layout, stateName, plan),
            epoch: staleEpoch,
            workBudget: checked(SearchableStorageQueryOptions.MaximumPartitionWorkBudget + 1));
        Func<Task> capCall = async () => await partition.QueryPageRoutedAsync(oversizedWork);
        await capCall.Should().ThrowAsync<ArgumentOutOfRangeException>()
            .WithMessage("*work budget must be between*");

        var validButStale = CopyPageRequest(
            CreatePageRequest(context.Layout, stateName, plan),
            epoch: staleEpoch);
        Func<Task> staleCall = async () => await partition.QueryPageRoutedAsync(validButStale);
        await staleCall.Should().ThrowAsync<StorageRouteMismatchException>();
    }

    [Fact]
    public async Task BoundedPageRejectsCallerLayoutFingerprintAgainstAuthoritativeSnapshot()
    {
        const string stateName = "bounded-layout-state";
        var context = await CreateContextAsync(partitionCount: 1);
        var partition = GetPartition(context.ProviderName, partitionIndex: 0);
        var request = CopyPageRequest(
            CreatePageRequest(context.Layout, stateName, ExactCityPlan("value")),
            layoutFingerprint: new byte[32]);

        Func<Task> query = async () => await partition.QueryPageRoutedAsync(request);

        await query.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*layout fingerprint does not match*");
    }

    [Theory]
    [InlineData("provider", "provider:00000003", 3)]
    [InlineData("provider:with:colons", "provider:with:colons:00000042", 42)]
    public void PartitionKeyParserUsesTheLastCanonicalSuffix(
        string expectedProvider,
        string partitionKey,
        int expectedPartition)
    {
        var identity = StoragePartitionGrain.ParsePartitionKey(partitionKey);

        identity.ProviderName.Should().Be(expectedProvider);
        identity.PartitionIndex.Should().Be(expectedPartition);
    }

    [Theory]
    [InlineData("provider")]
    [InlineData("provider:1")]
    [InlineData("provider:0000000x")]
    [InlineData(":00000001")]
    public void PartitionKeyParserRejectsNonCanonicalKeys(string partitionKey)
    {
        var parse = () => StoragePartitionGrain.ParsePartitionKey(partitionKey);

        parse.Should().Throw<InvalidOperationException>()
            .WithMessage("*eight-digit partition index*");
    }

    private async Task<RoutingContext> CreateContextAsync(int partitionCount)
    {
        var providerName = $"partition-routing-{Guid.NewGuid():N}";
        var layoutGrain = _fixture.Cluster.GrainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        var layout = await layoutGrain.InitializeRoutingAsync(
            StorageLayout.CreateDescriptor(providerName, partitionCount));
        return new RoutingContext(providerName, layout);
    }

    private IStoragePartitionGrain GetPartition(string providerName, int partitionIndex)
    {
        return _fixture.Cluster.GrainFactory.GetGrain<IStoragePartitionGrain>(
            StorageLayout.CreatePartitionKey(providerName, partitionIndex));
    }

    private static (GrainId GrainId, int Slot) CreateGrainOwnedBy(
        StorageLayoutSnapshot layout,
        int owner)
    {
        for (var attempt = 0; attempt < 100_000; attempt++)
        {
            var grainId = GrainId.Create("partition-routing", Guid.NewGuid().ToString("N"));
            var slot = StorageLayout.GetSlot(grainId, layout.VirtualSlotCount);
            if (layout.GetOwner(slot) == owner)
            {
                return (grainId, slot);
            }
        }

        throw new InvalidOperationException($"Could not create a grain assigned to partition {owner}.");
    }

    private static GrainId CreateDifferentGrainInSlot(
        StorageLayoutSnapshot layout,
        int slot,
        GrainId excluded)
    {
        for (var attempt = 0; attempt < 500_000; attempt++)
        {
            var grainId = GrainId.Create("partition-routing", Guid.NewGuid().ToString("N"));
            if (!grainId.Equals(excluded)
                && StorageLayout.GetSlot(grainId, layout.VirtualSlotCount) == slot)
            {
                return grainId;
            }
        }

        throw new InvalidOperationException($"Could not create a second grain assigned to virtual slot {slot}.");
    }

    private static StorageWriteRequest CreateWriteRequest(
        string recordKey,
        GrainId grainId,
        string value,
        int salary = 0,
        string? expectedETag = null)
    {
        return new StorageWriteRequest
        {
            RecordKey = recordKey,
            GrainId = grainId,
            Payload = [1, 2, 3],
            ExpectedETag = expectedETag,
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = "state/city",
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create(value),
                },
                new IndexEntry
                {
                    Scope = "state/salary",
                    Kind = SearchableIndexKind.Range,
                    Value = IndexValue.Create(salary),
                },
            ],
            Persistence = CreatePersistenceSettings(),
        };
    }

    private static StorageClearRequest CreateClearRequest(string recordKey, string etag)
    {
        return new StorageClearRequest
        {
            RecordKey = recordKey,
            ExpectedETag = etag,
            Persistence = CreatePersistenceSettings(),
        };
    }

    private static StoragePersistenceSettings CreatePersistenceSettings()
    {
        return new StoragePersistenceSettings
        {
            JournalSegmentCapacity = StoragePersistence.DefaultJournalSegmentCapacity,
            MaximumJournalReplayEntries = StoragePersistence.DefaultMaximumJournalReplayEntries,
            CompactionThreshold = StoragePersistence.DefaultCompactionThreshold,
        };
    }

    private static PartitionQueryPlan ExactCityPlan(string value)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = "state/city",
            IndexKind = SearchableIndexKind.Hash,
            Value = IndexValue.Create(value),
        };
    }

    private static PartitionQueryPlan SalaryRangePlan(int lower, int upper)
    {
        return new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = "state/salary",
            LowerBound = IndexValue.Create(lower),
            UpperBound = IndexValue.Create(upper),
            IncludeLowerBound = true,
            IncludeUpperBound = true,
        };
    }

    private static RoutedPartitionQueryPageRequest CreatePageRequest(
        StorageLayoutSnapshot layout,
        string stateName,
        PartitionQueryPlan query)
    {
        return new RoutedPartitionQueryPageRequest
        {
            Query = query,
            Epoch = layout.Epoch,
            WorkBudget = SearchableStorageQueryOptions.DefaultPartitionWorkBudget,
            ItemLimit = SearchableStorageQueryOptions.DefaultPartitionResponseItems,
            ByteLimit = SearchableStorageQueryOptions.DefaultPartitionResponseBytes,
            ProtocolVersion = QueryProtocol.PagingVersion,
            OrderingVersion = QueryProtocol.OrderingVersion,
            WorkPolicyVersion = QueryProtocol.WorkPolicyVersion,
            ResponseFamily = PartitionQueryResponseFamily.GrainIdPage,
            QueryFingerprint = QueryPlanFingerprint.Compute(stateName, query),
            LayoutFormatVersion = layout.FormatVersion,
            LayoutFingerprint = StorageLayoutFingerprint.Compute(layout),
            StateName = stateName,
        };
    }

    private static RoutedPartitionQueryPageRequest CopyPageRequest(
        RoutedPartitionQueryPageRequest source,
        PartitionQueryPlan? query = null,
        long? epoch = null,
        long? workBudget = null,
        byte[]? queryFingerprint = null,
        byte[]? layoutFingerprint = null)
    {
        return new RoutedPartitionQueryPageRequest
        {
            Query = query ?? source.Query,
            Epoch = epoch ?? source.Epoch,
            HasAfter = source.HasAfter,
            After = source.After,
            WorkBudget = workBudget ?? source.WorkBudget,
            ItemLimit = source.ItemLimit,
            ByteLimit = source.ByteLimit,
            ProtocolVersion = source.ProtocolVersion,
            OrderingVersion = source.OrderingVersion,
            WorkPolicyVersion = source.WorkPolicyVersion,
            ResponseFamily = source.ResponseFamily,
            QueryFingerprint = queryFingerprint ?? source.QueryFingerprint,
            LayoutFormatVersion = source.LayoutFormatVersion,
            LayoutFingerprint = layoutFingerprint ?? source.LayoutFingerprint,
            StateName = source.StateName,
        };
    }

    private static string CreateCanonicalRecordKey(string stateName, GrainId grainId)
    {
        return string.Concat(
            stateName,
            "/",
            Convert.ToHexString(grainId.Type.AsSpan()),
            "/",
            Convert.ToHexString(grainId.Key.AsSpan()));
    }

    private sealed record RoutingContext(string ProviderName, StorageLayoutSnapshot Layout);
}
