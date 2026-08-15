using System.Diagnostics.CodeAnalysis;
using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Runtime;

/// <summary>
/// Writes one payload-free projection to the co-located ephemeral searchable index.
/// </summary>
public interface IRuntimeProjectionIndexWriter
{
    ValueTask UpsertAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default);

    ValueTask RemoveAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Holds and verifies the database-wide single-dispatcher incarnation reservation.
/// </summary>
public interface IProjectionDispatcherIncarnation : IAsyncDisposable
{
    ValueTask<bool> IsHeldAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Defines all durable operations used around the non-transactional index boundary.
/// </summary>
public interface IDurableProjectionDispatchStore
{
    Task<IProjectionDispatcherIncarnation?> TryAcquireIncarnationAsync(
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<ProjectionSnapshot>> ReadDesiredProjectionPageAsync(
        AccountKey? afterAccountKeyExclusive,
        int batchSize,
        CancellationToken cancellationToken = default);

    Task<bool> MaterializeDesiredProjectionAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default);

    Task<bool> FinalizeRebuildProjectionAsync(
        ProjectionSnapshot projection,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<ProjectionOutboxLease>> LeaseProjectionsAsync(
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task<bool> PrepareProjectionHydrationAsync(
        ProjectionOutboxLease lease,
        CancellationToken cancellationToken = default);

    Task<bool> FinalizeProjectionAsync(
        ProjectionOutboxLease lease,
        CancellationToken cancellationToken = default);

    Task<bool> FailProjectionAsync(
        ProjectionOutboxLease lease,
        DateTimeOffset availableAtUtc,
        string errorCode,
        string errorMessage,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Exposes the rebuild gate that HTTP readiness can consume later.
/// </summary>
public interface IProjectionReadiness
{
    bool IsReady { get; }

    string Status { get; }
}

/// <summary>
/// Hard-stops a process whose non-transactional index result is ambiguous.
/// </summary>
public interface IFatalProcessTerminator
{
    [DoesNotReturn]
    void Terminate(string message, Exception? exception = null);
}

/// <summary>
/// Production terminator for an ambiguity that cannot be retried in the same Memory-silo incarnation.
/// </summary>
public sealed class EnvironmentFatalProcessTerminator : IFatalProcessTerminator
{
    [DoesNotReturn]
    public void Terminate(string message, Exception? exception = null)
        => Environment.FailFast(message, exception);
}
