using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageQueryOptionsTests
{
    [Fact]
    public void DefaultsAreBoundedAndDoNotRequireAContinuationKeyAtStartup()
    {
        var options = new SearchableStorageQueryOptions();

        options.PageSizeLimit.Should().Be(SearchableStorageQueryOptions.MaximumPageSize);
        options.PartitionWorkBudget.Should().Be(SearchableStorageQueryOptions.DefaultPartitionWorkBudget);
        options.PartitionResponseItemLimit.Should().Be(SearchableStorageQueryOptions.DefaultPartitionResponseItems);
        options.PartitionResponseByteLimit.Should().Be(SearchableStorageQueryOptions.DefaultPartitionResponseBytes);
        options.CoordinatorBufferedItemLimit.Should().Be(SearchableStorageQueryOptions.DefaultCoordinatorBufferedItems);
        options.CoordinatorBufferedByteLimit.Should().Be(SearchableStorageQueryOptions.DefaultCoordinatorBufferedBytes);
        options.PageByteLimit.Should().Be(SearchableStorageQueryOptions.DefaultPageBytes);
        options.ContinuationTokenByteLimit.Should().Be(SearchableStorageQueryOptions.DefaultContinuationTokenBytes);
        options.LegacyAggregateWorkLimit.Should().Be(SearchableStorageQueryOptions.DefaultLegacyAggregateWork);
        options.LegacyResultItemLimit.Should().Be(SearchableStorageQueryOptions.DefaultLegacyResultItems);
        options.LegacyResultByteLimit.Should().Be(SearchableStorageQueryOptions.DefaultLegacyResultBytes);
        options.LegacyRoundLimit.Should().Be(SearchableStorageQueryOptions.DefaultLegacyRounds);
        options.FacetTopNLimit.Should().Be(SearchableStorageQueryOptions.DefaultFacetTopN);
        options.FacetAggregateWorkLimit.Should().Be(SearchableStorageQueryOptions.DefaultFacetAggregateWork);
        options.FacetRoundLimit.Should().Be(SearchableStorageQueryOptions.DefaultFacetRounds);
        options.FacetAggregateItemLimit.Should().Be(SearchableStorageQueryOptions.DefaultFacetAggregateItems);
        options.FacetAggregateByteLimit.Should().Be(SearchableStorageQueryOptions.DefaultFacetAggregateBytes);
        SearchableStorageQueryOptions.DefaultLegacyResultItems.Should().Be(
            SearchableStorageQueryOptions.DefaultPageSize
            * SearchableStorageQueryOptions.DefaultLegacyRounds);
        options.ContinuationProtection.CurrentKey.Should().BeNull();

        var failures = SearchableStorageQueryOptionsValidator.GetFailures(
            options,
            requireCurrentKey: false);
        failures.Should().BeEmpty();
        SearchableStorageQueryConfiguration.Create(options).CurrentKey.Should().BeNull();
    }

    [Fact]
    public void PublicPagingFailsClosedWhenTheCurrentKeyIsMissing()
    {
        var configuration = SearchableStorageQueryConfiguration.Create(
            new SearchableStorageQueryOptions());
        var codec = new ContinuationTokenCodec("provider", configuration);
        var binding = CreateBinding();

        Action protect = () => _ = codec.Protect(
            new ContinuationTokenPayload(binding, GrainId.Create("paging", "after")));

        protect.Should().Throw<SearchableStorageQueryConfigurationException>()
            .WithMessage("*current*continuation-protection key*");
    }

    [Fact]
    public void ContinuationKeyAndConfigurationSnapshotDefensivelyCopyMutableInputs()
    {
        var currentBytes = Enumerable.Range(1, 32).Select(static value => (byte)value).ToArray();
        var oldBytes = Enumerable.Range(33, 32).Select(static value => (byte)value).ToArray();
        var expectedCurrent = currentBytes.ToArray();
        var expectedOld = oldBytes.ToArray();
        var options = new SearchableStorageQueryOptions
        {
            PartitionWorkBudget = 1234,
            FacetTopNLimit = 17,
            FacetAggregateWorkLimit = 18,
            FacetRoundLimit = 19,
            FacetAggregateItemLimit = 20,
            FacetAggregateByteLimit = 21,
        };
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "current",
            currentBytes);
        options.ContinuationProtection.DecryptionKeys.Add(
            new SearchableStorageContinuationKey("old", oldBytes));

        Array.Fill(currentBytes, (byte)0);
        Array.Fill(oldBytes, (byte)0);
        var snapshot = SearchableStorageQueryConfiguration.Create(options);

        options.PartitionWorkBudget = 4321;
        options.FacetTopNLimit = 27;
        options.FacetAggregateWorkLimit = 28;
        options.FacetRoundLimit = 29;
        options.FacetAggregateItemLimit = 30;
        options.FacetAggregateByteLimit = 31;
        options.ContinuationProtection.CurrentKey = new SearchableStorageContinuationKey(
            "replacement",
            new byte[32]);
        options.ContinuationProtection.DecryptionKeys.Clear();

        snapshot.PartitionWorkBudget.Should().Be(1234);
        snapshot.FacetTopNLimit.Should().Be(17);
        snapshot.FacetAggregateWorkLimit.Should().Be(18);
        snapshot.FacetRoundLimit.Should().Be(19);
        snapshot.FacetAggregateItemLimit.Should().Be(20);
        snapshot.FacetAggregateByteLimit.Should().Be(21);
        snapshot.CurrentKey!.KeyId.Should().Be("current");
        snapshot.CurrentKey.CopyKeyMaterial().Should().Equal(expectedCurrent);
        snapshot.DecryptionKeys.Should().ContainSingle()
            .Which.KeyId.Should().Be("old");
        snapshot.DecryptionKeys[0].CopyKeyMaterial().Should().Equal(expectedOld);
    }

    [Fact]
    public void KeyMaterialMustBeExactly256Bits()
    {
        Action tooShort = () => _ = new SearchableStorageContinuationKey("short", new byte[31]);
        Action tooLong = () => _ = new SearchableStorageContinuationKey("long", new byte[33]);

        tooShort.Should().Throw<ArgumentException>().WithMessage("*exactly 32 bytes*");
        tooLong.Should().Throw<ArgumentException>().WithMessage("*exactly 32 bytes*");
    }

    [Fact]
    public void ConfigurationRejectsDuplicateCurrentAndDecryptOnlyKeyIds()
    {
        var options = new SearchableStorageQueryOptions();
        options.ContinuationProtection.CurrentKey = Key("same", 1);
        options.ContinuationProtection.DecryptionKeys.Add(Key("same", 2));

        Action capture = () => _ = SearchableStorageQueryConfiguration.Create(options);

        capture.Should().Throw<SearchableStorageQueryConfigurationException>()
            .WithMessage("*identifiers must be unique*same*");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1025)]
    public void ConfigurationRejectsPageSizeLimitsOutsideTheHardBoundary(int value)
    {
        var options = new SearchableStorageQueryOptions { PageSizeLimit = value };

        Action capture = () => _ = SearchableStorageQueryConfiguration.Create(options);

        capture.Should().Throw<SearchableStorageQueryConfigurationException>()
            .WithMessage("*PageSizeLimit*");
    }

    [Fact]
    public void ConfigurationRejectsEveryFacetLimitOutsideItsHardBoundary()
    {
        var invalid = new (string Name, Action<SearchableStorageQueryOptions> Set)[]
        {
            (nameof(SearchableStorageQueryOptions.FacetTopNLimit),
                options => options.FacetTopNLimit = 0),
            (nameof(SearchableStorageQueryOptions.FacetTopNLimit),
                options => options.FacetTopNLimit = SearchableStorageQueryOptions.MaximumFacetTopN + 1),
            (nameof(SearchableStorageQueryOptions.FacetAggregateWorkLimit),
                options => options.FacetAggregateWorkLimit = 0),
            (nameof(SearchableStorageQueryOptions.FacetAggregateWorkLimit),
                options => options.FacetAggregateWorkLimit = SearchableStorageQueryOptions.MaximumFacetAggregateWork + 1),
            (nameof(SearchableStorageQueryOptions.FacetRoundLimit),
                options => options.FacetRoundLimit = 0),
            (nameof(SearchableStorageQueryOptions.FacetRoundLimit),
                options => options.FacetRoundLimit = SearchableStorageQueryOptions.MaximumFacetRounds + 1),
            (nameof(SearchableStorageQueryOptions.FacetAggregateItemLimit),
                options => options.FacetAggregateItemLimit = 0),
            (nameof(SearchableStorageQueryOptions.FacetAggregateItemLimit),
                options => options.FacetAggregateItemLimit = SearchableStorageQueryOptions.MaximumFacetAggregateItems + 1),
            (nameof(SearchableStorageQueryOptions.FacetAggregateByteLimit),
                options => options.FacetAggregateByteLimit = 0),
            (nameof(SearchableStorageQueryOptions.FacetAggregateByteLimit),
                options => options.FacetAggregateByteLimit = SearchableStorageQueryOptions.MaximumFacetAggregateBytes + 1),
        };

        foreach (var (name, set) in invalid)
        {
            var options = new SearchableStorageQueryOptions();
            set(options);

            Action capture = () => _ = SearchableStorageQueryConfiguration.Create(options);

            capture.Should().Throw<SearchableStorageQueryConfigurationException>()
                .WithMessage($"*{name}*");
        }
    }

    [Fact]
    public void ConfigurationRejectsAPageByteLimitAboveTheCoordinatorBuffer()
    {
        var options = new SearchableStorageQueryOptions
        {
            PageByteLimit = 2,
            CoordinatorBufferedByteLimit = 1,
        };

        Action capture = () => _ = SearchableStorageQueryConfiguration.Create(options);

        capture.Should().Throw<SearchableStorageQueryConfigurationException>()
            .WithMessage("*PageByteLimit*CoordinatorBufferedByteLimit*");
    }

    [Fact]
    public void PolicyApportionsPartitionResponsesAcrossOwners()
    {
        var options = new SearchableStorageQueryOptions
        {
            PartitionResponseItemLimit = 100,
            PartitionResponseByteLimit = 1000,
            CoordinatorBufferedItemLimit = 12,
            CoordinatorBufferedByteLimit = 120,
            PageByteLimit = 100,
        };
        var configuration = SearchableStorageQueryConfiguration.Create(options);

        var policy = QueryExecutionPolicy.Create(configuration, pageSize: 7, ownerCount: 3);

        policy.PageSize.Should().Be(7);
        policy.PartitionResponseItemLimit.Should().Be(4);
        policy.PartitionResponseByteLimit.Should().Be(40);
    }

    [Fact]
    public void PolicyDoesNotRequestMoreItemsFromOneOwnerThanThePublicPageCanUse()
    {
        var configuration = SearchableStorageQueryConfiguration.Create(
            new SearchableStorageQueryOptions());

        var policy = QueryExecutionPolicy.Create(configuration, pageSize: 7, ownerCount: 1);

        policy.PartitionResponseItemLimit.Should().Be(7);
    }

    [Fact]
    public void PolicyRejectsAnOwnerCountWhichCannotReceiveAPositiveShare()
    {
        var options = new SearchableStorageQueryOptions
        {
            CoordinatorBufferedItemLimit = 1,
        };
        var configuration = SearchableStorageQueryConfiguration.Create(options);

        Action create = () => _ = QueryExecutionPolicy.Create(
            configuration,
            pageSize: 1,
            ownerCount: 2);

        create.Should().Throw<SearchableStorageQueryConfigurationException>();
    }

    [Fact]
    public async Task ClientConstructionSnapshotsLiveLimitsAndKeyRing()
    {
        var options = new SearchableStorageQueryOptions
        {
            PageSizeLimit = 1,
        };
        options.ContinuationProtection.CurrentKey = Key("captured", 7);
        options.ContinuationProtection.DecryptionKeys.Add(Key("captured-old", 8));
        var client = new SearchableStorageClient(
            "snapshot-provider",
            [new SnapshotPartition()],
            static () => Task.FromResult(true),
            options);

        options.PageSizeLimit = 2;
        options.ContinuationProtection.CurrentKey = Key("replacement", 9);
        options.ContinuationProtection.DecryptionKeys.Clear();

        Func<Task> oversized = () => client
            .Query<SnapshotState>("state")
            .Where(static state => state.Value == 7)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(2));
        await oversized.Should().ThrowAsync<ArgumentOutOfRangeException>();

        var page = await client
            .Query<SnapshotState>("state")
            .Where(static state => state.Value == 7)
            .ToGrainIdPageAsync(new SearchableStorageQueryPageRequest(1));
        var reader = new CanonicalBinaryReader(DecodeToken(page.ContinuationToken!));
        _ = reader.ReadInt32();
        _ = reader.ReadInt32();
        reader.ReadString(SearchableStorageContinuationKey.MaximumKeyIdBytes, true)
            .Should().Be("captured");
    }

    private static SearchableStorageContinuationKey Key(string id, byte fill)
    {
        var material = new byte[32];
        Array.Fill(material, fill);
        return new SearchableStorageContinuationKey(id, material);
    }

    private static ContinuationTokenBinding CreateBinding()
    {
        return new ContinuationTokenBinding(
            "provider",
            PartitionQueryResponseFamily.GrainIdPage,
            Enumerable.Repeat((byte)1, 32).ToArray(),
            QueryProtocol.OrderingVersion,
            layoutFormatVersion: 4,
            routingEpoch: 1,
            Enumerable.Repeat((byte)2, 32).ToArray(),
            new QueryExecutionPolicy(1, 1, 1, 1, 1, 1, 1));
    }

    private static byte[] DecodeToken(string token)
    {
        var padding = new string('=', (4 - token.Length % 4) % 4);
        return Convert.FromBase64String(
            token.Replace('-', '+').Replace('_', '/') + padding);
    }

    private sealed class SnapshotState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public int Value { get; init; }
    }

    private sealed class SnapshotPartition : StoragePartitionGrainMovementTestDouble, IStoragePartitionGrain
    {
        public Task<PartitionQueryPageResult> QueryPageRoutedAsync(
            RoutedPartitionQueryPageRequest request)
        {
            return Task.FromResult(new PartitionQueryPageResult
            {
                Items = [],
                HasFrontier = true,
                Frontier = GrainId.Create("snapshot-frontier", "after"),
                Exhausted = false,
                StopReason = PartitionQueryPageStopReason.WorkBudget,
                Work = new PartitionQueryPageWork { OrderedCandidateVisitCount = 1 },
                ProtocolVersion = request.ProtocolVersion,
                OrderingVersion = request.OrderingVersion,
                WorkPolicyVersion = request.WorkPolicyVersion,
                ResponseFamily = request.ResponseFamily,
                Epoch = request.Epoch,
                QueryFingerprint = [.. request.QueryFingerprint],
                LayoutFormatVersion = request.LayoutFormatVersion,
                LayoutFingerprint = [.. request.LayoutFingerprint],
            });
        }

        public Task<PartitionDistinctFacetPageResult> QueryDistinctFacetPageRoutedAsync(
            RoutedPartitionDistinctFacetPageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCandidatePageResult> QueryFacetCandidatesRoutedAsync(
            RoutedPartitionFacetCandidatePageRequest request) => throw new NotSupportedException();

        public Task<PartitionFacetCountSliceResult> QueryFacetCountSliceRoutedAsync(
            RoutedPartitionFacetCountSliceRequest request) => throw new NotSupportedException();

        public Task<StorageReadResult> ReadAsync(string recordKey) => throw new NotSupportedException();

        public Task<StorageReadResult> ReadRoutedAsync(RoutedStorageReadRequest request) => throw new NotSupportedException();

        public Task<string> WriteAsync(StorageWriteRequest request) => throw new NotSupportedException();

        public Task<string> WriteRoutedAsync(RoutedStorageWriteRequest request) => throw new NotSupportedException();

        public Task ClearAsync(StorageClearRequest request) => throw new NotSupportedException();

        public Task ClearRoutedAsync(RoutedStorageClearRequest request) => throw new NotSupportedException();

        public Task<GrainId[]> FindAsync(ExactIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> FindRoutedAsync(RoutedExactIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> RangeAsync(RangeIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> RangeRoutedAsync(RoutedRangeIndexQuery query) => throw new NotSupportedException();

        public Task<GrainId[]> QueryAsync(PartitionQueryPlan query) => throw new NotSupportedException();

        public Task<GrainId[]> QueryRoutedAsync(RoutedPartitionQuery query) => throw new NotSupportedException();

        public Task CompactAsync() => throw new NotSupportedException();

        public Task<StoragePartitionPersistenceInfo> GetPersistenceInfoAsync() => throw new NotSupportedException();
    }
}
