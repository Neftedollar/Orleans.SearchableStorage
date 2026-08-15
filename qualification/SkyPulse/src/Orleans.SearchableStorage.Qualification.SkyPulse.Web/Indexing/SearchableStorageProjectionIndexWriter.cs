using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.SearchableStorage;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

/// <summary>
/// Writes only the derived metadata projection to Orleans.SearchableStorage index-only mode.
/// </summary>
public sealed class SearchableStorageProjectionIndexWriter(
    [FromKeyedServices(SkyPulseIndexContract.ProviderName)] ISearchableStorageIndexWriter writer)
    : IProjectionIndexWriter
{
    public async ValueTask UpsertAsync(
        AccountProjection projection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(projection);

        await writer.UpsertAsync(
                SkyPulseIndexContract.StateName,
                ToGrainId(projection.AccountKey),
                AccountIndexState.FromProjection(projection),
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask RemoveAsync(
        AccountKey accountKey,
        CancellationToken cancellationToken = default)
    {
        if (!accountKey.IsValid)
        {
            throw new ArgumentException("A valid account key is required.", nameof(accountKey));
        }

        await writer.RemoveAsync<AccountIndexState>(
                SkyPulseIndexContract.StateName,
                ToGrainId(accountKey),
                cancellationToken)
            .ConfigureAwait(false);
    }

    internal static GrainId ToGrainId(AccountKey accountKey)
        => GrainId.Create(SkyPulseIndexContract.GrainType, accountKey.ToString());
}
