using Microsoft.Extensions.Options;
using Orleans.SearchableStorage.Qualification.SkyPulse;
using Orleans.SearchableStorage.Qualification.SkyPulse.Web;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web.Tests;

public sealed class QuerySessionTests
{
    [Fact]
    public async Task RemovalOfCurrentMemberRequiresImmediateResync()
    {
        var projection = CreateProjection(123);
        var key = projection.AccountKey;
        var page = new SkyPulseQueryPage([SkyPulseQueryRow.FromProjection(projection)], null);
        var session = new QuerySession(
            Guid.NewGuid(),
            new SkyPulseQueryRequest { PageSize = 10 },
            page,
            updateBufferCapacity: 4,
            DateTimeOffset.UtcNow);

        Assert.True(session.TryRequireResyncFor(key.ToString()));

        await using var events = session.ReadEventsAsync().GetAsyncEnumerator();
        Assert.True(await events.MoveNextAsync());
        Assert.Equal(QuerySessionEventKind.ResyncRequired, events.Current.Kind);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(101)]
    public void RequestRejectsPageSizesOutsideBound(int pageSize)
    {
        var request = new SkyPulseQueryRequest { PageSize = pageSize };

        Assert.Throws<ArgumentOutOfRangeException>(request.Validate);
    }

    [Fact]
    public void RequestRejectsContinuationLongerThanPackageDefaultLimit()
    {
        var request = new SkyPulseQueryRequest
        {
            ContinuationToken = new string('a', SkyPulseQueryRequest.MaximumContinuationTokenLength + 1),
        };

        Assert.Throws<ArgumentException>(request.Validate);
    }

    [Fact]
    public async Task RegistryRejectsAdapterPageOverOneHundredRows()
    {
        var rows = Enumerable.Range(0, 101)
            .Select(index => SkyPulseQueryRow.FromProjection(CreateProjection(index)))
            .ToArray();
        var query = new MutablePageQuery(new SkyPulseQueryPage(rows, null));
        var registry = CreateRegistry(query);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => registry.CreateAsync(new SkyPulseQueryRequest { PageSize = 100 }));
    }

    [Fact]
    public async Task PublisherRoutesOnlyCurrentPageRows()
    {
        var included = CreateProjection(1);
        var excluded = CreateProjection(2);
        var query = new MutablePageQuery(Page(included));
        var registry = CreateRegistry(query);
        var publisher = new ProjectionUpdatePublisher(
            new InMemoryProjectionStore(),
            new NoOpProjectionIndexWriter(),
            registry);
        var snapshot = await registry.CreateAsync(new SkyPulseQueryRequest());
        Assert.True(registry.TryGet(snapshot.SessionId, out var session));

        Assert.Equal(0, await publisher.PublishAsync(excluded));
        Assert.Equal(1, await publisher.PublishAsync(included));

        await using var events = session.ReadEventsAsync().GetAsyncEnumerator();
        Assert.True(await events.MoveNextAsync());
        Assert.Equal(QuerySessionEventKind.Projection, events.Current.Kind);
        Assert.Equal(included.AccountKey.ToString(), events.Current.Projection!.GrainId);
    }

    [Fact]
    public async Task CurrentPageMemberLeavingTheFilterRequiresResync()
    {
        var included = CreateProjection(12, currentPostCount: 10);
        var query = new MutablePageQuery(Page(included));
        var registry = CreateRegistry(query);
        var publisher = new ProjectionUpdatePublisher(
            new InMemoryProjectionStore(),
            new NoOpProjectionIndexWriter(),
            registry);
        var snapshot = await registry.CreateAsync(new SkyPulseQueryRequest
        {
            CurrentPostCount = new LongRangeFilter { Minimum = 10 },
        });
        Assert.True(registry.TryGet(snapshot.SessionId, out var session));

        Assert.Equal(1, await publisher.PublishAsync(CreateProjection(12, currentPostCount: 9)));

        await using var events = session.ReadEventsAsync().GetAsyncEnumerator();
        Assert.True(await events.MoveNextAsync());
        Assert.Equal(QuerySessionEventKind.ResyncRequired, events.Current.Kind);
        Assert.Null(events.Current.Projection);
    }

    [Fact]
    public async Task BufferOverflowRequestsResyncInsteadOfDroppingSilently()
    {
        var projection = CreateProjection(3);
        var query = new MutablePageQuery(Page(projection));
        var registry = CreateRegistry(query, bufferCapacity: 1);
        var publisher = new ProjectionUpdatePublisher(
            new InMemoryProjectionStore(),
            new NoOpProjectionIndexWriter(),
            registry);
        var snapshot = await registry.CreateAsync(new SkyPulseQueryRequest());
        Assert.True(registry.TryGet(snapshot.SessionId, out var session));

        Assert.Equal(1, await publisher.PublishAsync(projection));
        Assert.Equal(0, await publisher.PublishAsync(CreateProjection(3, currentPostCount: 11)));

        await using var events = session.ReadEventsAsync().GetAsyncEnumerator();
        Assert.True(await events.MoveNextAsync());
        Assert.Equal(QuerySessionEventKind.ResyncRequired, events.Current.Kind);
        Assert.Null(events.Current.Projection);
        Assert.False(await events.MoveNextAsync());
    }

    [Fact]
    public async Task RefreshReplacesMembershipBeforeFurtherRouting()
    {
        var first = CreateProjection(4);
        var second = CreateProjection(5);
        var query = new MutablePageQuery(Page(first));
        var registry = CreateRegistry(query);
        var publisher = new ProjectionUpdatePublisher(
            new InMemoryProjectionStore(),
            new NoOpProjectionIndexWriter(),
            registry);
        var snapshot = await registry.CreateAsync(new SkyPulseQueryRequest());
        Assert.True(registry.TryGet(snapshot.SessionId, out var session));

        query.Page = Page(second);
        var refreshed = await registry.RefreshAsync(snapshot.SessionId);

        Assert.NotNull(refreshed);
        Assert.False(session.Contains(first.AccountKey.ToString()));
        Assert.True(session.Contains(second.AccountKey.ToString()));
        Assert.Equal(0, await publisher.PublishAsync(first));
        Assert.Equal(1, await publisher.PublishAsync(second));

        await using var events = session.ReadEventsAsync().GetAsyncEnumerator();
        Assert.True(await events.MoveNextAsync());
        Assert.Equal(second.AccountKey.ToString(), events.Current.Projection!.GrainId);
    }

    [Fact]
    public async Task ExpiredSessionsAreRemoved()
    {
        var clock = new MutableTimeProvider(new DateTimeOffset(2026, 8, 14, 0, 0, 0, TimeSpan.Zero));
        var query = new MutablePageQuery(Page(CreateProjection(6)));
        var registry = CreateRegistry(query, timeProvider: clock, timeToLive: TimeSpan.FromMinutes(5));
        await registry.CreateAsync(new SkyPulseQueryRequest());

        clock.Advance(TimeSpan.FromMinutes(5));

        Assert.Equal(1, registry.RemoveExpired());
        Assert.Equal(0, registry.Count);
    }

    [Fact]
    public async Task RegistryEnforcesAndReleasesConcurrentSessionLimit()
    {
        var query = new MutablePageQuery(Page(CreateProjection(9)));
        var registry = CreateRegistry(query, maximumConcurrentSessions: 1);

        var first = await registry.CreateAsync(new SkyPulseQueryRequest());
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => registry.CreateAsync(new SkyPulseQueryRequest()));

        Assert.True(registry.Remove(first.SessionId));
        var replacement = await registry.CreateAsync(new SkyPulseQueryRequest());
        Assert.NotEqual(first.SessionId, replacement.SessionId);
    }

    [Fact]
    public async Task InMemoryQueryAppliesFixedInclusiveRange()
    {
        var store = new InMemoryProjectionStore();
        await store.UpsertAsync(CreateProjection(7, currentPostCount: 9));
        await store.UpsertAsync(CreateProjection(8, currentPostCount: 10));
        var query = new InMemorySkyPulsePageQuery(store);

        var page = await query.QueryAsync(new SkyPulseQueryRequest
        {
            CurrentPostCount = new LongRangeFilter { Minimum = 10, Maximum = 10 },
        });

        var row = Assert.Single(page.Rows);
        Assert.Equal(AccountKey.FromDid("did:plc:contract8").ToString(), row.GrainId);
    }

    private static QuerySessionRegistry CreateRegistry(
        ISkyPulsePageQuery query,
        int bufferCapacity = 8,
        TimeProvider? timeProvider = null,
        TimeSpan? timeToLive = null,
        int maximumConcurrentSessions = 256)
    {
        var options = Options.Create(new QuerySessionOptions
        {
            MaximumConcurrentSessions = maximumConcurrentSessions,
            UpdateBufferCapacity = bufferCapacity,
            SessionTimeToLive = timeToLive ?? TimeSpan.FromMinutes(10),
        });

        return new QuerySessionRegistry(query, options, timeProvider ?? TimeProvider.System);
    }

    private static SkyPulseQueryPage Page(params AccountProjection[] projections)
        => new(projections.Select(SkyPulseQueryRow.FromProjection).ToArray(), null);

    private static AccountProjection CreateProjection(int index, long currentPostCount = 10)
    {
        var accountKey = AccountKey.FromDid($"did:plc:contract{index}");
        var admission = FrozenCorpusAllowlist.FromCanonicalOrder([accountKey])
            .CreateAdmission(new CappedCorpusProfile("contract-only", 1));

        return admission.CreateProjection(
            accountKey,
            lastActivityMinuteUtc: 1_000 + index,
            createdRecordCounts: new RollingWindowCounts(2, 3, 4),
            updatedRecordCounts: new RollingWindowCounts(1, 2, 3),
            deletedRecordCounts: new RollingWindowCounts(0, 1, 2),
            currentPostCount,
            currentFollowingCount: 5,
            currentFollowerCount: 6,
            postCreateCounts: new RollingWindowCounts(1, 2, 3),
            receivedEngagementCreates30Days: 7);
    }

    private sealed class MutablePageQuery(SkyPulseQueryPage page) : ISkyPulsePageQuery
    {
        public SkyPulseQueryPage Page { get; set; } = page;

        public Task<SkyPulseQueryPage> QueryAsync(
            SkyPulseQueryRequest request,
            CancellationToken cancellationToken = default)
        {
            request.Validate();
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Page);
        }
    }

    private sealed class MutableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan duration) => _utcNow += duration;
    }

    private sealed class NoOpProjectionIndexWriter : IProjectionIndexWriter
    {
        public ValueTask UpsertAsync(
            AccountProjection projection,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(projection);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask RemoveAsync(
            AccountKey accountKey,
            CancellationToken cancellationToken = default)
        {
            if (!accountKey.IsValid)
            {
                throw new ArgumentException("A valid account key is required.", nameof(accountKey));
            }

            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }
    }
}
