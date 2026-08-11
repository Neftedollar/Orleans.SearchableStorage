using Microsoft.Extensions.Options;

namespace Orleans.SearchableStorage.Querying;

internal sealed record QueryExecutionPolicy(
    int PageSize,
    long PartitionWorkBudget,
    int PartitionResponseItemLimit,
    int PartitionResponseByteLimit,
    int CoordinatorBufferedItemLimit,
    int CoordinatorBufferedByteLimit,
    int PageByteLimit)
{
    public static QueryExecutionPolicy Create(
        SearchableStorageQueryConfiguration configuration,
        int pageSize,
        int ownerCount)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ownerCount);

        if (pageSize > configuration.PageSizeLimit)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                pageSize,
                $"PageSize must not exceed the configured limit of {configuration.PageSizeLimit}.");
        }

        var apportionedItems = configuration.CoordinatorBufferedItemLimit / ownerCount;
        var apportionedBytes = configuration.CoordinatorBufferedByteLimit / ownerCount;
        if (apportionedItems <= 0 || apportionedBytes <= 0)
        {
            throw new SearchableStorageQueryConfigurationException(
                "The coordinator query-buffer limits cannot provide a positive bounded response "
                + $"for {ownerCount} storage owners.");
        }

        return new QueryExecutionPolicy(
            pageSize,
            configuration.PartitionWorkBudget,
            Math.Min(pageSize, Math.Min(configuration.PartitionResponseItemLimit, apportionedItems)),
            Math.Min(configuration.PartitionResponseByteLimit, apportionedBytes),
            configuration.CoordinatorBufferedItemLimit,
            configuration.CoordinatorBufferedByteLimit,
            configuration.PageByteLimit);
    }
}

internal sealed class SearchableStorageQueryConfiguration
{
    private SearchableStorageQueryConfiguration(SearchableStorageQueryOptions options)
    {
        PageSizeLimit = options.PageSizeLimit;
        PartitionWorkBudget = options.PartitionWorkBudget;
        PartitionResponseItemLimit = options.PartitionResponseItemLimit;
        PartitionResponseByteLimit = options.PartitionResponseByteLimit;
        CoordinatorBufferedItemLimit = options.CoordinatorBufferedItemLimit;
        CoordinatorBufferedByteLimit = options.CoordinatorBufferedByteLimit;
        PageByteLimit = options.PageByteLimit;
        ContinuationTokenByteLimit = options.ContinuationTokenByteLimit;
        LegacyAggregateWorkLimit = options.LegacyAggregateWorkLimit;
        LegacyResultItemLimit = options.LegacyResultItemLimit;
        LegacyResultByteLimit = options.LegacyResultByteLimit;
        LegacyRoundLimit = options.LegacyRoundLimit;
        FacetTopNLimit = options.FacetTopNLimit;
        FacetAggregateWorkLimit = options.FacetAggregateWorkLimit;
        FacetRoundLimit = options.FacetRoundLimit;
        FacetAggregateItemLimit = options.FacetAggregateItemLimit;
        FacetAggregateByteLimit = options.FacetAggregateByteLimit;

        var protection = options.ContinuationProtection;
        CurrentKey = protection.CurrentKey is null
            ? null
            : ContinuationProtectionKey.CopyFrom(protection.CurrentKey);
        DecryptionKeys = Array.AsReadOnly(
            protection.DecryptionKeys
                .Select(ContinuationProtectionKey.CopyFrom)
                .ToArray());
    }

    public int PageSizeLimit { get; }

    public long PartitionWorkBudget { get; }

    public int PartitionResponseItemLimit { get; }

    public int PartitionResponseByteLimit { get; }

    public int CoordinatorBufferedItemLimit { get; }

    public int CoordinatorBufferedByteLimit { get; }

    public int PageByteLimit { get; }

    public int ContinuationTokenByteLimit { get; }

    public long LegacyAggregateWorkLimit { get; }

    public int LegacyResultItemLimit { get; }

    public int LegacyResultByteLimit { get; }

    public int LegacyRoundLimit { get; }

    public int FacetTopNLimit { get; }

    public long FacetAggregateWorkLimit { get; }

    public int FacetRoundLimit { get; }

    public int FacetAggregateItemLimit { get; }

    public int FacetAggregateByteLimit { get; }

    public ContinuationProtectionKey? CurrentKey { get; }

    public IReadOnlyList<ContinuationProtectionKey> DecryptionKeys { get; }

    public static SearchableStorageQueryConfiguration Create(SearchableStorageQueryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        SearchableStorageQueryOptionsValidator.ThrowIfInvalid(options, requireCurrentKey: false);
        return new SearchableStorageQueryConfiguration(options);
    }
}

internal sealed class ContinuationProtectionKey
{
    private readonly byte[] _keyMaterial;

    private ContinuationProtectionKey(string keyId, byte[] keyMaterial)
    {
        KeyId = keyId;
        _keyMaterial = keyMaterial;
    }

    public string KeyId { get; }

    public byte[] CopyKeyMaterial() => [.. _keyMaterial];

    public static ContinuationProtectionKey CopyFrom(SearchableStorageContinuationKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new ContinuationProtectionKey(key.KeyId, key.CopyKeyMaterial());
    }
}

internal sealed class SearchableStorageQueryOptionsValidator
    : IValidateOptions<SearchableStorageOptions>
{
    public ValidateOptionsResult Validate(string? name, SearchableStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = GetFailures(options.Query, requireCurrentKey: false);
        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    internal static void ThrowIfInvalid(
        SearchableStorageQueryOptions options,
        bool requireCurrentKey)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = GetFailures(options, requireCurrentKey);
        if (failures.Count > 0)
        {
            throw new SearchableStorageQueryConfigurationException(
                string.Join(" ", failures));
        }
    }

    internal static IReadOnlyList<string> GetFailures(
        SearchableStorageQueryOptions options,
        bool requireCurrentKey)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();

        AddRangeFailure(
            failures,
            nameof(options.PageSizeLimit),
            options.PageSizeLimit,
            SearchableStorageQueryOptions.MaximumPageSize);
        AddRangeFailure(
            failures,
            nameof(options.PartitionWorkBudget),
            options.PartitionWorkBudget,
            SearchableStorageQueryOptions.MaximumPartitionWorkBudget);
        AddRangeFailure(
            failures,
            nameof(options.PartitionResponseItemLimit),
            options.PartitionResponseItemLimit,
            SearchableStorageQueryOptions.MaximumPartitionResponseItems);
        AddRangeFailure(
            failures,
            nameof(options.PartitionResponseByteLimit),
            options.PartitionResponseByteLimit,
            SearchableStorageQueryOptions.MaximumPartitionResponseBytes);
        AddRangeFailure(
            failures,
            nameof(options.CoordinatorBufferedItemLimit),
            options.CoordinatorBufferedItemLimit,
            SearchableStorageQueryOptions.MaximumCoordinatorBufferedItems);
        AddRangeFailure(
            failures,
            nameof(options.CoordinatorBufferedByteLimit),
            options.CoordinatorBufferedByteLimit,
            SearchableStorageQueryOptions.MaximumCoordinatorBufferedBytes);
        AddRangeFailure(
            failures,
            nameof(options.PageByteLimit),
            options.PageByteLimit,
            SearchableStorageQueryOptions.MaximumPageBytes);
        AddRangeFailure(
            failures,
            nameof(options.ContinuationTokenByteLimit),
            options.ContinuationTokenByteLimit,
            SearchableStorageQueryOptions.MaximumContinuationTokenBytes);
        AddRangeFailure(
            failures,
            nameof(options.LegacyAggregateWorkLimit),
            options.LegacyAggregateWorkLimit,
            SearchableStorageQueryOptions.MaximumLegacyAggregateWork);
        AddRangeFailure(
            failures,
            nameof(options.LegacyResultItemLimit),
            options.LegacyResultItemLimit,
            SearchableStorageQueryOptions.MaximumLegacyResultItems);
        AddRangeFailure(
            failures,
            nameof(options.LegacyResultByteLimit),
            options.LegacyResultByteLimit,
            SearchableStorageQueryOptions.MaximumLegacyResultBytes);
        AddRangeFailure(
            failures,
            nameof(options.LegacyRoundLimit),
            options.LegacyRoundLimit,
            SearchableStorageQueryOptions.MaximumLegacyRounds);
        AddRangeFailure(
            failures,
            nameof(options.FacetTopNLimit),
            options.FacetTopNLimit,
            SearchableStorageQueryOptions.MaximumFacetTopN);
        AddRangeFailure(
            failures,
            nameof(options.FacetAggregateWorkLimit),
            options.FacetAggregateWorkLimit,
            SearchableStorageQueryOptions.MaximumFacetAggregateWork);
        AddRangeFailure(
            failures,
            nameof(options.FacetRoundLimit),
            options.FacetRoundLimit,
            SearchableStorageQueryOptions.MaximumFacetRounds);
        AddRangeFailure(
            failures,
            nameof(options.FacetAggregateItemLimit),
            options.FacetAggregateItemLimit,
            SearchableStorageQueryOptions.MaximumFacetAggregateItems);
        AddRangeFailure(
            failures,
            nameof(options.FacetAggregateByteLimit),
            options.FacetAggregateByteLimit,
            SearchableStorageQueryOptions.MaximumFacetAggregateBytes);

        if (options.PageByteLimit > options.CoordinatorBufferedByteLimit)
        {
            failures.Add(
                $"{nameof(options.PageByteLimit)} must not exceed "
                + $"{nameof(options.CoordinatorBufferedByteLimit)}.");
        }

        ValidateKeyRing(options.ContinuationProtection, requireCurrentKey, failures);
        return failures;
    }

    private static void ValidateKeyRing(
        SearchableStorageContinuationProtectionOptions protection,
        bool requireCurrentKey,
        List<string> failures)
    {
        ArgumentNullException.ThrowIfNull(protection);
        if (requireCurrentKey && protection.CurrentKey is null)
        {
            failures.Add(
                $"{nameof(SearchableStorageContinuationProtectionOptions.CurrentKey)} must be "
                + "configured before public query paging can be used.");
        }

        var keyIds = new HashSet<string>(StringComparer.Ordinal);
        if (protection.CurrentKey is not null)
        {
            keyIds.Add(protection.CurrentKey.KeyId);
        }

        for (var index = 0; index < protection.DecryptionKeys.Count; index++)
        {
            var key = protection.DecryptionKeys[index];
            if (key is null)
            {
                failures.Add(
                    $"{nameof(SearchableStorageContinuationProtectionOptions.DecryptionKeys)} "
                    + $"contains a null key at index {index}.");
                continue;
            }

            if (!keyIds.Add(key.KeyId))
            {
                failures.Add(
                    "Continuation-protection key identifiers must be unique; "
                    + $"'{key.KeyId}' is configured more than once.");
            }
        }
    }

    private static void AddRangeFailure(
        List<string> failures,
        string name,
        int value,
        int maximum)
    {
        if (value <= 0 || value > maximum)
        {
            failures.Add($"{name} must be between 1 and {maximum}.");
        }
    }

    private static void AddRangeFailure(
        List<string> failures,
        string name,
        long value,
        long maximum)
    {
        if (value <= 0 || value > maximum)
        {
            failures.Add($"{name} must be between 1 and {maximum}.");
        }
    }
}
