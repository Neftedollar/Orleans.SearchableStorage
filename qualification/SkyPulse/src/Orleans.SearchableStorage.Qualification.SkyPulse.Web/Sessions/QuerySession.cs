using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

public enum QuerySessionEventKind
{
    Projection,
    ResyncRequired,
}

public sealed record QuerySessionEvent(QuerySessionEventKind Kind, SkyPulseQueryRow? Projection);

/// <summary>
/// Holds one bounded current page and a bounded stream of updates for only that page.
/// </summary>
public sealed class QuerySession
{
    private readonly object _membershipLock = new();
    private readonly Channel<BufferedProjection> _updates;
    private HashSet<string> _currentGrainIds = new(StringComparer.Ordinal);
    private SkyPulseQueryPage _currentPage = new(Array.Empty<SkyPulseQueryRow>(), null);
    private long _membershipVersion;
    private long _lastAccessUtcTicks;
    private int _requiresResync;
    private int _eventReaderLease;

    internal QuerySession(
        Guid id,
        SkyPulseQueryRequest request,
        SkyPulseQueryPage initialPage,
        int updateBufferCapacity,
        DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(request);

        Id = id;
        Request = request;
        _updates = Channel.CreateBounded<BufferedProjection>(
            new BoundedChannelOptions(updateBufferCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });

        UpdatePage(initialPage, now);
    }

    public Guid Id { get; }

    public SkyPulseQueryRequest Request { get; }

    public bool RequiresResync => Volatile.Read(ref _requiresResync) != 0;

    public DateTimeOffset LastAccessUtc
        => new(Interlocked.Read(ref _lastAccessUtcTicks), TimeSpan.Zero);

    public SkyPulseQueryPage CurrentPage
    {
        get
        {
            lock (_membershipLock)
            {
                return _currentPage;
            }
        }
    }

    public bool Contains(string grainId)
    {
        ArgumentException.ThrowIfNullOrEmpty(grainId);

        lock (_membershipLock)
        {
            return _currentGrainIds.Contains(grainId);
        }
    }

    public void Touch(DateTimeOffset now)
        => Interlocked.Exchange(ref _lastAccessUtcTicks, now.UtcTicks);

    internal void UpdatePage(SkyPulseQueryPage page, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(page.Rows);

        if (RequiresResync)
        {
            throw new InvalidOperationException("A session that requires resynchronization cannot be refreshed.");
        }

        if (page.Rows.Count > SkyPulseQueryRequest.MaximumPageSize)
        {
            throw new InvalidOperationException(
                $"A query adapter returned {page.Rows.Count} rows; the session limit is "
                + $"{SkyPulseQueryRequest.MaximumPageSize}.");
        }

        var grainIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in page.Rows)
        {
            ArgumentNullException.ThrowIfNull(row);
            if (!AccountKey.TryParse(row.GrainId, out _))
            {
                throw new InvalidOperationException("A query adapter returned a non-canonical grain ID.");
            }

            if (!grainIds.Add(row.GrainId))
            {
                throw new InvalidOperationException("A query adapter returned a duplicate grain ID.");
            }
        }

        lock (_membershipLock)
        {
            _currentGrainIds = grainIds;
            _currentPage = new SkyPulseQueryPage(
                Array.AsReadOnly(page.Rows.ToArray()),
                page.ContinuationToken);
            _membershipVersion++;
        }

        Touch(now);
    }

    internal bool TryPublish(SkyPulseQueryRow projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        if (RequiresResync)
        {
            return false;
        }

        long version;
        lock (_membershipLock)
        {
            if (!_currentGrainIds.Contains(projection.GrainId))
            {
                return false;
            }

            version = _membershipVersion;
        }

        if (!Request.Matches(projection))
        {
            return TryRequireResync();
        }

        if (_updates.Writer.TryWrite(new BufferedProjection(version, projection)))
        {
            return true;
        }

        TryRequireResync();

        return false;
    }

    internal bool TryRequireResyncFor(string grainId)
    {
        ArgumentException.ThrowIfNullOrEmpty(grainId);
        if (RequiresResync)
        {
            return false;
        }

        lock (_membershipLock)
        {
            if (!_currentGrainIds.Contains(grainId))
            {
                return false;
            }
        }

        return TryRequireResync();
    }

    internal void Complete() => _updates.Writer.TryComplete();

    internal bool TryAcquireEventReader()
        => Interlocked.CompareExchange(ref _eventReaderLease, 1, 0) == 0;

    internal void ReleaseEventReader()
        => Interlocked.Exchange(ref _eventReaderLease, 0);

    public async IAsyncEnumerable<QuerySessionEvent> ReadEventsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!RequiresResync && await _updates.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (!RequiresResync && _updates.Reader.TryRead(out var update))
            {
                if (IsCurrent(update))
                {
                    yield return new QuerySessionEvent(QuerySessionEventKind.Projection, update.Projection);
                }
            }
        }

        if (RequiresResync)
        {
            yield return new QuerySessionEvent(QuerySessionEventKind.ResyncRequired, null);
        }
    }

    private bool IsCurrent(BufferedProjection update)
    {
        lock (_membershipLock)
        {
            return update.MembershipVersion == _membershipVersion
                && _currentGrainIds.Contains(update.Projection.GrainId);
        }
    }

    private bool TryRequireResync()
    {
        if (Interlocked.Exchange(ref _requiresResync, 1) != 0)
        {
            return false;
        }

        _updates.Writer.TryComplete();
        return true;
    }

    private sealed record BufferedProjection(long MembershipVersion, SkyPulseQueryRow Projection);
}
