using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Owns short-lived bounded query sessions and routes projection updates by current-page membership.
/// </summary>
public sealed class QuerySessionRegistry
{
    private readonly ConcurrentDictionary<Guid, QuerySession> _sessions = new();
    private readonly ISkyPulsePageQuery _pageQuery;
    private readonly QuerySessionOptions _options;
    private readonly TimeProvider _timeProvider;
    private int _reservedSessionCount;

    public QuerySessionRegistry(
        ISkyPulsePageQuery pageQuery,
        IOptions<QuerySessionOptions> options,
        TimeProvider timeProvider)
    {
        _pageQuery = pageQuery;
        _options = options.Value;
        _options.Validate();
        _timeProvider = timeProvider;
    }

    public int Count => _sessions.Count;

    public async Task<QuerySessionSnapshot> CreateAsync(
        SkyPulseQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        request.Validate();

        var reservedCount = Interlocked.Increment(ref _reservedSessionCount);
        if (reservedCount > _options.MaximumConcurrentSessions)
        {
            Interlocked.Decrement(ref _reservedSessionCount);
            throw new InvalidOperationException(
                $"The bounded query-session limit of {_options.MaximumConcurrentSessions} has been reached.");
        }

        try
        {
            var page = await _pageQuery.QueryAsync(request, cancellationToken).ConfigureAwait(false);
            var now = _timeProvider.GetUtcNow();
            var session = new QuerySession(
                Guid.NewGuid(),
                request,
                page,
                _options.UpdateBufferCapacity,
                now);

            if (!_sessions.TryAdd(session.Id, session))
            {
                throw new InvalidOperationException("The query session ID collided with an existing session.");
            }

            return Snapshot(session);
        }
        catch
        {
            Interlocked.Decrement(ref _reservedSessionCount);
            throw;
        }
    }

    public async Task<QuerySessionSnapshot?> RefreshAsync(
        Guid sessionId,
        CancellationToken cancellationToken = default)
    {
        if (!TryGet(sessionId, out var session))
        {
            return null;
        }

        var page = await _pageQuery.QueryAsync(session.Request, cancellationToken).ConfigureAwait(false);
        session.UpdatePage(page, _timeProvider.GetUtcNow());
        return Snapshot(session);
    }

    public bool TryGet(Guid sessionId, out QuerySession session)
    {
        if (_sessions.TryGetValue(sessionId, out session!))
        {
            session.Touch(_timeProvider.GetUtcNow());
            return true;
        }

        return false;
    }

    public bool Remove(Guid sessionId)
    {
        if (!_sessions.TryRemove(sessionId, out var session))
        {
            return false;
        }

        session.Complete();
        Interlocked.Decrement(ref _reservedSessionCount);
        return true;
    }

    public int RemoveExpired()
    {
        var now = _timeProvider.GetUtcNow();
        var removed = 0;
        foreach (var pair in _sessions)
        {
            if (now - pair.Value.LastAccessUtc < _options.SessionTimeToLive)
            {
                continue;
            }

            if (Remove(pair.Key))
            {
                removed++;
            }
        }

        return removed;
    }

    internal int Publish(SkyPulseQueryRow projection)
    {
        var routed = 0;
        foreach (var session in _sessions.Values)
        {
            if (session.TryPublish(projection))
            {
                routed++;
            }
        }

        return routed;
    }

    internal int PublishRemoval(AccountKey accountKey)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        var grainId = accountKey.ToString();
        var routed = 0;
        foreach (var session in _sessions.Values)
        {
            if (session.TryRequireResyncFor(grainId))
            {
                routed++;
            }
        }

        return routed;
    }

    internal void Touch(QuerySession session) => session.Touch(_timeProvider.GetUtcNow());

    private QuerySessionSnapshot Snapshot(QuerySession session)
        => new(
            session.Id,
            session.CurrentPage,
            session.LastAccessUtc + _options.SessionTimeToLive);
}
