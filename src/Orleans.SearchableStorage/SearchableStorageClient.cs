using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.SearchableStorage.Diagnostics;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage;

/// <summary>
/// Queries storage partitions through an Orleans grain factory.
/// </summary>
public sealed partial class SearchableStorageClient : ISearchableStorageQueryClient
{
    private readonly string _providerName;
    private readonly Func<int, IStoragePartitionGrain> _getPartition;
    private readonly StorageLayoutCache _layoutCache;
    private readonly SearchableStorageQueryConfiguration _queryConfiguration;
    private readonly ContinuationTokenCodec _tokenCodec;
    private readonly Action<Task> _observeDetachedFanout;
    private readonly SearchableStateRegistry _stateRegistry;
    private readonly Func<string, IStorageIndexSchemaGrain>? _getIndexSchema;
    private readonly ILogger<SearchableStorageClient>? _logger;
    private readonly ActiveSchemaValidationCache _activeSchemas = new();

    /// <summary>
    /// Initializes a client for one searchable storage provider.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to contact storage grains.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="partitionCount">The partition count configured for that provider.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grainFactory"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionCount"/> is not positive.</exception>
    /// <remarks>
    /// This direct constructor targets integrated <c>IGrainStorage</c> namespaces. Configure an
    /// index-only external client through <see cref="SearchableStorageSiloBuilderExtensions.AddSearchableIndex(Microsoft.Extensions.DependencyInjection.IServiceCollection, string, Action{SearchableStorageOptions}?)"/>
    /// and resolve its keyed query client instead.
    /// </remarks>
    public SearchableStorageClient(IGrainFactory grainFactory, string providerName, int partitionCount)
        : this(grainFactory, providerName, partitionCount, new SearchableStorageQueryOptions())
    {
    }

    /// <summary>
    /// Initializes a client for one searchable storage provider with bounded-query options.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to contact storage grains.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="partitionCount">The partition count configured for that provider.</param>
    /// <param name="queryOptions">The provider-scoped bounded-query and continuation settings.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grainFactory"/> or <paramref name="queryOptions"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionCount"/> is not positive.</exception>
    /// <remarks>
    /// This direct constructor targets integrated <c>IGrainStorage</c> namespaces. Configure an
    /// index-only external client through <see cref="SearchableStorageSiloBuilderExtensions.AddSearchableIndex(Microsoft.Extensions.DependencyInjection.IServiceCollection, string, Action{SearchableStorageOptions}?)"/>
    /// and resolve its keyed query client instead.
    /// </remarks>
    public SearchableStorageClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount,
        SearchableStorageQueryOptions queryOptions)
        : this(
            grainFactory,
            providerName,
            partitionCount,
            queryOptions,
            SearchableStateRegistry.Empty)
    {
    }

    /// <summary>
    /// Initializes a client with bounded-query options and explicitly declared managed schemas.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to contact storage grains.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="partitionCount">The partition count configured for that provider.</param>
    /// <param name="queryOptions">The provider-scoped bounded-query and continuation settings.</param>
    /// <param name="schemaRegistry">The state schemas declared by this client process.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="grainFactory"/>, <paramref name="queryOptions"/>, or
    /// <paramref name="schemaRegistry"/> is null.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionCount"/> is not positive.</exception>
    /// <remarks>
    /// This direct constructor targets integrated <c>IGrainStorage</c> namespaces. Configure an
    /// index-only external client through <see cref="SearchableStorageSiloBuilderExtensions.AddSearchableIndex(Microsoft.Extensions.DependencyInjection.IServiceCollection, string, Action{SearchableStorageOptions}?)"/>
    /// and register the same state declarations on that service collection.
    /// </remarks>
    public SearchableStorageClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount,
        SearchableStorageQueryOptions queryOptions,
        SearchableStorageSchemaRegistry schemaRegistry)
        : this(
            grainFactory,
            providerName,
            partitionCount,
            queryOptions,
            CreateStateRegistry(providerName, schemaRegistry))
    {
    }

    internal SearchableStorageClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount,
        SearchableStorageQueryOptions queryOptions,
        SearchableStateRegistry stateRegistry,
        ILogger<SearchableStorageClient>? logger = null,
        Action<Task>? detachedFanoutObserver = null)
        : this(
            grainFactory,
            providerName,
            partitionCount,
            queryOptions,
            stateRegistry,
            StorageNamespaceMode.Integrated,
            logger,
            detachedFanoutObserver)
    {
    }

    internal SearchableStorageClient(
        IGrainFactory grainFactory,
        string providerName,
        int partitionCount,
        SearchableStorageQueryOptions queryOptions,
        SearchableStateRegistry stateRegistry,
        StorageNamespaceMode namespaceMode,
        ILogger<SearchableStorageClient>? logger = null,
        Action<Task>? detachedFanoutObserver = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(queryOptions);
        ArgumentNullException.ThrowIfNull(stateRegistry);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);
        if (!Enum.IsDefined(namespaceMode))
        {
            throw new ArgumentOutOfRangeException(nameof(namespaceMode));
        }

        _providerName = providerName;
        _queryConfiguration = SearchableStorageQueryConfiguration.Create(queryOptions);
        _tokenCodec = new ContinuationTokenCodec(providerName, _queryConfiguration);
        _observeDetachedFanout = detachedFanoutObserver ?? ObserveDetachedFanout;
        _stateRegistry = stateRegistry;
        _logger = logger;
        _getIndexSchema = stateName => grainFactory.GetGrain<IStorageIndexSchemaGrain>(
            StorageIndexSchema.CreateGrainKey(providerName, stateName));
        var layout = StorageLayout.CreateIdentity(providerName, partitionCount, namespaceMode);
        var layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        _layoutCache = new StorageLayoutCache(() => layoutGrain.GetLayoutAsync(layout));
        var partitions = new ConcurrentDictionary<int, IStoragePartitionGrain>();
        _getPartition = index => partitions.GetOrAdd(
            index,
            partitionIndex => grainFactory.GetGrain<IStoragePartitionGrain>(
                StorageLayout.CreatePartitionKey(providerName, partitionIndex)));
    }

    internal SearchableStorageClient(
        string providerName,
        IReadOnlyList<IStoragePartitionGrain> partitions,
        Func<Task<bool>> validateLayout,
        SearchableStorageQueryOptions? queryOptions = null,
        ContinuationTokenCodec? tokenCodec = null,
        Action<Task>? detachedFanoutObserver = null,
        ILogger<SearchableStorageClient>? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(partitions);
        ArgumentNullException.ThrowIfNull(validateLayout);
        ArgumentOutOfRangeException.ThrowIfZero(partitions.Count);

        _providerName = providerName;
        _queryConfiguration = SearchableStorageQueryConfiguration.Create(
            queryOptions ?? new SearchableStorageQueryOptions());
        _tokenCodec = tokenCodec ?? new ContinuationTokenCodec(providerName, _queryConfiguration);
        _observeDetachedFanout = detachedFanoutObserver ?? ObserveDetachedFanout;
        _stateRegistry = SearchableStateRegistry.Empty;
        _logger = logger;
        _getIndexSchema = null;
        var staticLayout = CreateStaticLayout(providerName, partitions.Count);
        _layoutCache = new StorageLayoutCache(
            async () => await validateLayout() ? staticLayout : null);
        _getPartition = index => partitions[index];
    }

    internal SearchableStorageClient(
        string providerName,
        StorageLayoutCache layoutCache,
        Func<int, IStoragePartitionGrain> getPartition,
        SearchableStorageQueryOptions? queryOptions = null,
        ContinuationTokenCodec? tokenCodec = null,
        Action<Task>? detachedFanoutObserver = null,
        ILogger<SearchableStorageClient>? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(layoutCache);
        ArgumentNullException.ThrowIfNull(getPartition);

        _providerName = providerName;
        _queryConfiguration = SearchableStorageQueryConfiguration.Create(
            queryOptions ?? new SearchableStorageQueryOptions());
        _tokenCodec = tokenCodec ?? new ContinuationTokenCodec(providerName, _queryConfiguration);
        _observeDetachedFanout = detachedFanoutObserver ?? ObserveDetachedFanout;
        _stateRegistry = SearchableStateRegistry.Empty;
        _logger = logger;
        _getIndexSchema = null;
        _layoutCache = layoutCache;
        _getPartition = getPartition;
    }

    /// <inheritdoc />
    public IQueryable<TState> Query<TState>(string stateName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        var provider = new SearchableStorageQueryProvider<TState>(this, stateName);
        return new SearchableStorageQuery<TState>(provider);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<GrainId>> FindAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue value,
        CancellationToken cancellationToken = default)
    {
        // Translation and argument checks deliberately remain in the async core. Public query
        // terminals therefore keep their faulted-Task contract while observation spans the
        // complete translation, remote schema gate, and partition fanout.
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "query.legacy",
            "execute",
            _logger,
            lifecycle: false,
            () => FindCoreAsync(stateName, propertySelector, value, cancellationToken),
            static items => items.Count);
    }

    private async Task<IReadOnlyList<GrainId>> FindCoreAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue value,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var schema = GetRegisteredSchema<TState>(stateName);
        var index = IndexMetadataProvider.GetSelectedIndex(
            stateName,
            propertySelector,
            schema?.Fingerprint);
        var indexValue = CreateQueryValue(index, value, nameof(value));

        var query = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Exact,
            Scope = index.Scope,
            IndexKind = index.Kind,
            Value = indexValue,
        };

        await EnsureSchemaActiveAsync<TState>(stateName, cancellationToken);
        return await ExecuteLegacyQueryAsync(stateName, query, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<GrainId>> RangeAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue lowerBound,
        TValue upperBound,
        bool includeLowerBound = true,
        bool includeUpperBound = true,
        CancellationToken cancellationToken = default)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "query.legacy",
            "execute",
            _logger,
            lifecycle: false,
            () => RangeCoreAsync(
                stateName,
                propertySelector,
                lowerBound,
                upperBound,
                includeLowerBound,
                includeUpperBound,
                cancellationToken),
            static items => items.Count);
    }

    private async Task<IReadOnlyList<GrainId>> RangeCoreAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue lowerBound,
        TValue upperBound,
        bool includeLowerBound,
        bool includeUpperBound,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var schema = GetRegisteredSchema<TState>(stateName);
        var index = IndexMetadataProvider.GetSelectedIndex(
            stateName,
            propertySelector,
            schema?.Fingerprint);
        if (index.Kind != SearchableIndexKind.Range)
        {
            throw new ArgumentException("Range queries require a property marked with SearchableIndexKind.Range.", nameof(propertySelector));
        }

        var lowerValue = CreateQueryValue(index, lowerBound, nameof(lowerBound));
        var upperValue = CreateQueryValue(index, upperBound, nameof(upperBound));
        if (lowerValue.CompareTo(upperValue) > 0)
        {
            throw new ArgumentException("The lower range bound must not be greater than the upper range bound.", nameof(lowerBound));
        }

        var query = new PartitionQueryPlan
        {
            Operation = PartitionQueryOperation.Range,
            Scope = index.Scope,
            LowerBound = lowerValue,
            UpperBound = upperValue,
            IncludeLowerBound = includeLowerBound,
            IncludeUpperBound = includeUpperBound,
        };

        await EnsureSchemaActiveAsync<TState>(stateName, cancellationToken);
        return await ExecuteLegacyQueryAsync(stateName, query, cancellationToken);
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return $"{nameof(SearchableStorageClient)}({_providerName})";
    }

    internal Task<IReadOnlyList<GrainId>> ExecuteQueryAsync<TState>(
        string stateName,
        Expression expression,
        CancellationToken cancellationToken)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "query.legacy",
            "execute",
            _logger,
            lifecycle: false,
            () => ExecuteQueryCoreAsync<TState>(stateName, expression, cancellationToken),
            static items => items.Count);
    }

    private async Task<IReadOnlyList<GrainId>> ExecuteQueryCoreAsync<TState>(
        string stateName,
        Expression expression,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var schema = GetRegisteredSchema<TState>(stateName);
        var plan = QueryTranslator.Translate<TState>(stateName, expression, schema?.Fingerprint);
        var partitionPlan = PartitionQueryPlanFactory.Create(plan);
        await EnsureSchemaActiveAsync<TState>(
            stateName,
            cancellationToken,
            requireFreshUnregisteredCapability:
                partitionPlan.Operation == PartitionQueryOperation.Empty);
        return await ExecuteLegacyQueryAsync(stateName, partitionPlan, cancellationToken);
    }

    internal Task<SearchableStorageQueryPage> ExecuteQueryPageAsync<TState>(
        string stateName,
        Expression expression,
        SearchableStorageQueryPageRequest request,
        CancellationToken cancellationToken)
    {
        return SearchableStorageDiagnostics.ObserveAsync(
            _providerName,
            "query.page",
            "execute",
            _logger,
            lifecycle: false,
            () => ExecuteQueryPageCoreAsync<TState>(
                stateName,
                expression,
                request,
                cancellationToken),
            static page => page.Items.Count);
    }

    private async Task<SearchableStorageQueryPage> ExecuteQueryPageCoreAsync<TState>(
        string stateName,
        Expression expression,
        SearchableStorageQueryPageRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        var schema = GetRegisteredSchema<TState>(stateName);
        var plan = QueryTranslator.Translate<TState>(stateName, expression, schema?.Fingerprint);
        var partitionPlan = PartitionQueryPlanFactory.Create(plan);
        ValidatePublicPagePreconditions(request);
        await EnsureSchemaActiveAsync<TState>(
            stateName,
            cancellationToken,
            requireFreshUnregisteredCapability:
                partitionPlan.Operation == PartitionQueryOperation.Empty);
        return await ExecutePublicPageAsync(
            stateName,
            partitionPlan,
            request,
            cancellationToken);
    }

    private static IndexValue CreateQueryValue<TValue>(SelectedIndex index, TValue value, string parameterName)
    {
        if (value is null)
        {
            throw new ArgumentNullException(parameterName, "Null values are not indexed.");
        }

        var runtimeType = value.GetType();
        if (runtimeType != index.Converter.RuntimeValueType)
        {
            throw new ArgumentException(
                $"The query value type '{runtimeType}' does not match indexed property type '{index.Converter.RuntimeValueType}'.",
                parameterName);
        }

        return index.Converter.ConvertObject(value)
            ?? throw new InvalidOperationException("A non-null query value unexpectedly converted to null.");
    }

    private IndexSchemaDefinition? GetRegisteredSchema<TState>(string stateName)
    {
        return _stateRegistry.Find<TState>(_providerName, stateName)?.Schema;
    }

    private async Task EnsureSchemaActiveAsync<TState>(
        string stateName,
        CancellationToken cancellationToken,
        bool requireFreshUnregisteredCapability = false)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var registration = _stateRegistry.Find<TState>(_providerName, stateName);
        if (registration is null)
        {
            if (_stateRegistry.ContainsProvider(_providerName))
            {
                throw new SearchableStorageIndexSchemaException(
                    $"Provider '{_providerName}' has managed schema declarations, but state "
                    + $"'{stateName}' is not declared by this query client. Include every state "
                    + $"used by the provider in its {nameof(SearchableStorageSchemaRegistry)}.");
            }

            // A schema-unaware request normally reaches a partition, whose durable provider gate
            // rejects it after managed schemas are enabled. An empty plan is answered locally, so
            // the client must probe the capability through a read initiated by this operation
            // instead of trusting either the cache or another caller's earlier read.
            var layout = requireFreshUnregisteredCapability
                ? await _layoutCache.ReadFreshAsync(cancellationToken)
                : await _layoutCache.GetAsync(cancellationToken);
            var schemaProtocolPublished = layout?.IndexSchemaProtocolVersion
                == StorageIndexSchema.ProtocolVersion;
            var schemaEnablementActive = layout?.CopyIndexSchemaEnablement() is not null;
            if (schemaProtocolPublished || schemaEnablementActive)
            {
                var capabilityState = schemaProtocolPublished
                    ? "has managed index schemas enabled"
                    : "is durably enabling managed index schemas";
                throw new SearchableStorageIndexSchemaException(
                    $"Provider '{_providerName}' {capabilityState} and requires explicit managed "
                    + "schema binding, but state "
                    + $"'{stateName}' is not declared by this query client. Supply a "
                    + $"{nameof(SearchableStorageSchemaRegistry)} containing every state used by "
                    + "the provider.");
            }

            return;
        }

        if (_activeSchemas.IsActive(registration))
        {
            return;
        }

        var control = _getIndexSchema
            ?? throw new InvalidOperationException("The managed index-schema control is unavailable.");
        var call = control(stateName).GetAsync(StorageIndexSchema.CreateRequest(registration));
        StorageIndexSchemaSnapshot snapshot;
        try
        {
            snapshot = await call.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _observeDetachedFanout(call);
            throw;
        }
        if (snapshot.Rebuild is not null)
        {
            throw new SearchableStorageIndexSchemaException(
                $"Index schema rebuild '{snapshot.Rebuild.RebuildId}' is still running for state "
                + $"'{stateName}'. Keep searchable traffic quiesced until it completes.");
        }

        if (snapshot.ActiveFingerprint is null
            || !IndexSchemaIdentity.FixedTimeEquals(
                snapshot.ActiveFingerprint,
                registration.Schema.Fingerprint))
        {
            throw new SearchableStorageIndexSchemaException(
                $"The registered index schema for state '{stateName}' is not active. Quiesce "
                + "searchable traffic and run RebuildIndexSchemaAsync<TState> first.");
        }

        _activeSchemas.MarkActive(registration);
    }

    private static SearchableStateRegistry CreateStateRegistry(
        string providerName,
        SearchableStorageSchemaRegistry schemaRegistry)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(schemaRegistry);
        return schemaRegistry.CreateRegistry(providerName);
    }

    private async Task<SearchableStorageQueryPage> ExecutePublicPageAsync(
        string stateName,
        PartitionQueryPlan query,
        SearchableStorageQueryPageRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var queryFingerprint = QueryPlanFingerprint.Compute(stateName, query);
        var isContinuation = request.ContinuationToken is not null;
        var layout = await _layoutCache.GetAsync(cancellationToken);
        if (layout is null)
        {
            if (isContinuation)
            {
                throw new SearchableStorageInvalidContinuationTokenException(
                    "A continuation cannot resume an uninitialized storage namespace.");
            }

            return new SearchableStorageQueryPage([], continuationToken: null);
        }

        if (query.Operation == PartitionQueryOperation.Empty)
        {
            if (isContinuation)
            {
                throw new SearchableStorageInvalidContinuationTokenException(
                    "An empty query has no valid continuation.");
            }

            return new SearchableStorageQueryPage([], continuationToken: null);
        }

        for (var attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var owners = layout.GetDistinctOwners();
            var policy = QueryExecutionPolicy.Create(
                _queryConfiguration,
                request.PageSize,
                owners.Length);
            var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
            var binding = CreateTokenBinding(
                queryFingerprint,
                layout,
                layoutFingerprint,
                policy);
            var cursor = isContinuation
                ? QueryCursor.AfterValue(
                    _tokenCodec.Unprotect(request.ContinuationToken!, binding).After)
                : QueryCursor.Initial;

            try
            {
                var page = await ExecutePageAttemptAsync(
                    stateName,
                    query,
                    queryFingerprint,
                    layout,
                    layoutFingerprint,
                    owners,
                    policy,
                    cursor,
                    cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var continuationToken = page.HasContinuation
                    ? _tokenCodec.Protect(
                        new ContinuationTokenPayload(binding, page.ContinuationAfter))
                    : null;
                cancellationToken.ThrowIfCancellationRequested();
                return new SearchableStorageQueryPage(page.Items, continuationToken);
            }
            catch (PartitionQueryBudgetTooSmallException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The searchable-storage query page cannot make progress within its bounded execution limits.",
                    exception);
            }
            catch (StorageRouteMismatchException exception) when (isContinuation)
            {
                throw new SearchableStorageStaleContinuationTokenException(
                    "The routing layout changed while resuming the searchable-storage query.",
                    exception);
            }
            catch (StorageRouteMismatchException) when (attempt == 0)
            {
                _layoutCache.Invalidate(layout);
                layout = await _layoutCache.GetAsync(cancellationToken)
                    ?? throw new InvalidOperationException(
                        "The storage layout disappeared while a routed query was refreshing.");
            }
        }
    }

    private void ValidatePublicPagePreconditions(SearchableStorageQueryPageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.PageSize > _queryConfiguration.PageSizeLimit)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.PageSize,
                $"PageSize must not exceed the configured limit of {_queryConfiguration.PageSizeLimit}.");
        }

        if (_queryConfiguration.CurrentKey is null)
        {
            throw new SearchableStorageQueryConfigurationException(
                "A current continuation-protection key must be configured before public query paging can be used.");
        }
    }

    private async Task<IReadOnlyList<GrainId>> ExecuteLegacyQueryAsync(
        string stateName,
        PartitionQueryPlan query,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var queryFingerprint = QueryPlanFingerprint.Compute(stateName, query);
        var layout = await _layoutCache.GetAsync(cancellationToken);
        if (layout is null || query.Operation == PartitionQueryOperation.Empty)
        {
            return [];
        }

        var results = new List<GrainId>(
            Math.Min(
                _queryConfiguration.LegacyResultItemLimit,
                SearchableStorageQueryOptions.DefaultPageSize));
        var cursor = QueryCursor.Initial;
        var allowRouteRefresh = true;
        long totalWork = 0;
        var totalBytes = 0;
        var rounds = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (rounds >= _queryConfiguration.LegacyRoundLimit)
            {
                throw CreateLegacyLimitException("round");
            }

            rounds = checked(rounds + 1);
            var owners = layout.GetDistinctOwners();
            var remainingWork = _queryConfiguration.LegacyAggregateWorkLimit - totalWork;
            var remainingItems = _queryConfiguration.LegacyResultItemLimit - results.Count;
            var remainingBytes = _queryConfiguration.LegacyResultByteLimit - totalBytes;
            if (remainingWork <= 0 || remainingItems <= 0 || remainingBytes <= 0)
            {
                throw CreateLegacyLimitException("aggregate");
            }

            var apportionedWork = remainingWork / owners.Length;
            if (apportionedWork <= 0)
            {
                throw CreateLegacyLimitException("logical-work");
            }

            var pageSize = Math.Min(
                Math.Min(
                    SearchableStorageQueryOptions.DefaultPageSize,
                    _queryConfiguration.PageSizeLimit),
                remainingItems);
            var policy = QueryExecutionPolicy.Create(
                _queryConfiguration,
                pageSize,
                owners.Length) with
            {
                PartitionWorkBudget = Math.Min(
                    _queryConfiguration.PartitionWorkBudget,
                    apportionedWork),
                PageByteLimit = Math.Min(
                    _queryConfiguration.PageByteLimit,
                    remainingBytes),
            };
            var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);

            PageAttemptResult page;
            try
            {
                page = await ExecutePageAttemptAsync(
                    stateName,
                    query,
                    queryFingerprint,
                    layout,
                    layoutFingerprint,
                    owners,
                    policy,
                    cursor,
                    cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (StorageRouteMismatchException) when (allowRouteRefresh && !cursor.HasAfter)
            {
                allowRouteRefresh = false;
                _layoutCache.Invalidate(layout);
                layout = await _layoutCache.GetAsync(cancellationToken)
                    ?? throw new InvalidOperationException(
                        "The storage layout disappeared while a routed query was refreshing.");
                continue;
            }
            catch (PartitionQueryBudgetTooSmallException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The searchable-storage compatibility query cannot make progress within its bounded execution limits.",
                    exception);
            }

            allowRouteRefresh = false;
            try
            {
                totalWork = checked(totalWork + page.TotalWork);
                totalBytes = checked(totalBytes + page.ItemByteCount);
            }
            catch (OverflowException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The searchable-storage compatibility query exceeded an aggregate limit.",
                    exception);
            }

            if (totalWork > _queryConfiguration.LegacyAggregateWorkLimit
                || (page.HasContinuation
                    && totalWork == _queryConfiguration.LegacyAggregateWorkLimit))
            {
                throw CreateLegacyLimitException("logical-work");
            }

            if (totalBytes > _queryConfiguration.LegacyResultByteLimit
                || (page.HasContinuation
                    && totalBytes == _queryConfiguration.LegacyResultByteLimit))
            {
                throw CreateLegacyLimitException("result-byte");
            }

            var nextItemCount = checked(results.Count + page.Items.Length);
            if (nextItemCount > _queryConfiguration.LegacyResultItemLimit
                || (page.HasContinuation
                    && nextItemCount == _queryConfiguration.LegacyResultItemLimit))
            {
                throw CreateLegacyLimitException("result-item");
            }

            if (page.HasContinuation && rounds == _queryConfiguration.LegacyRoundLimit)
            {
                throw CreateLegacyLimitException("round");
            }

            results.AddRange(page.Items);
            if (!page.HasContinuation)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return results;
            }

            cursor = QueryCursor.AfterValue(page.ContinuationAfter);
        }
    }

    private async Task<PageAttemptResult> ExecutePageAttemptAsync(
        string stateName,
        PartitionQueryPlan query,
        byte[] queryFingerprint,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        int[] owners,
        QueryExecutionPolicy policy,
        QueryCursor cursor,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var responseRequirements = QueryResponseRequirements.Create(query);
        var schema = _stateRegistry.Find(_providerName, stateName)?.Schema;
        var calls = new PartitionPageCall[owners.Length];
        for (var index = 0; index < owners.Length; index++)
        {
            var owner = owners[index];
            var request = new RoutedPartitionQueryPageRequest
            {
                Query = query,
                Epoch = layout.Epoch,
                HasAfter = cursor.HasAfter,
                After = cursor.After,
                WorkBudget = policy.PartitionWorkBudget,
                ItemLimit = policy.PartitionResponseItemLimit,
                ByteLimit = policy.PartitionResponseByteLimit,
                ProtocolVersion = QueryProtocol.PagingVersion,
                OrderingVersion = QueryProtocol.OrderingVersion,
                WorkPolicyVersion = QueryProtocol.WorkPolicyVersion,
                ResponseFamily = PartitionQueryResponseFamily.GrainIdPage,
                QueryFingerprint = [.. queryFingerprint],
                LayoutFormatVersion = layout.FormatVersion,
                LayoutFingerprint = [.. layoutFingerprint],
                StateName = stateName,
                IndexSchemaFingerprint = schema?.Fingerprint,
                IndexSchemaProtocolVersion = schema is null
                    ? 0
                    : StorageIndexSchema.ProtocolVersion,
            };
            calls[index] = new PartitionPageCall(owner, StartPageQuery(owner, request));
        }

        var responses = await WaitForFanoutAsync(calls, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        return ValidateAndMergeResponses(
            responses,
            responseRequirements,
            queryFingerprint,
            layout,
            layoutFingerprint,
            policy,
            cursor);
    }

    private Task<PartitionQueryPageResult> StartPageQuery(
        int owner,
        RoutedPartitionQueryPageRequest request)
    {
        try
        {
            return _getPartition(owner).QueryPageRoutedAsync(request)
                ?? Task.FromException<PartitionQueryPageResult>(
                    new InvalidOperationException(
                        $"Storage owner {owner} returned a null partition query task."));
        }
        catch (Exception exception)
        {
            // Normalize synchronous test doubles, lookup failures, and custom Orleans proxies into
            // the same all-owner completion and deterministic failure-classification path.
            return Task.FromException<PartitionQueryPageResult>(exception);
        }
    }

    private static StorageLayoutSnapshot CreateStaticLayout(string providerName, int partitionCount)
    {
        var assignments = StorageLayout.CreateIdentityAssignments(partitionCount, partitionCount);
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.MovementFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
            VirtualSlotCount = partitionCount,
            SlotAssignments = assignments,
            Epoch = 1,
        });
    }

    private ContinuationTokenBinding CreateTokenBinding(
        byte[] queryFingerprint,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy)
    {
        return new ContinuationTokenBinding(
            _providerName,
            PartitionQueryResponseFamily.GrainIdPage,
            queryFingerprint,
            QueryProtocol.OrderingVersion,
            layout.FormatVersion,
            layout.Epoch,
            layoutFingerprint,
            policy);
    }

    private static PageAttemptResult ValidateAndMergeResponses(
        PartitionQueryPageResult[] responses,
        QueryResponseRequirements responseRequirements,
        byte[] queryFingerprint,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy,
        QueryCursor cursor)
    {
        var bufferedItems = 0;
        var bufferedBytes = 0;
        long totalWork = 0;
        try
        {
            for (var index = 0; index < responses.Length; index++)
            {
                var response = responses[index]
                    ?? throw InvalidPartitionResponse(index, "returned a null response");
                ValidatePartitionResponse(
                    index,
                    response,
                    responseRequirements,
                    queryFingerprint,
                    layout,
                    layoutFingerprint,
                    policy,
                    cursor);
                bufferedItems = checked(bufferedItems + response.Items.Length);
                bufferedBytes = checked(bufferedBytes + response.ItemByteCount);
                totalWork = checked(totalWork + response.Work.TotalOperationCount);
            }
        }
        catch (OverflowException exception)
        {
            throw new InvalidOperationException(
                "A partition page response overflowed coordinator accounting.",
                exception);
        }

        if (bufferedItems > policy.CoordinatorBufferedItemLimit
            || bufferedBytes > policy.CoordinatorBufferedByteLimit)
        {
            throw new InvalidOperationException(
                "Partition page responses exceeded the coordinator buffer policy.");
        }

        return MergePartitionResponses(responses, policy, totalWork);
    }

    private static void ValidatePartitionResponse(
        int responseIndex,
        PartitionQueryPageResult response,
        QueryResponseRequirements responseRequirements,
        byte[] queryFingerprint,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy,
        QueryCursor cursor)
    {
        if (response.ProtocolVersion != QueryProtocol.PagingVersion
            || response.OrderingVersion != QueryProtocol.OrderingVersion
            || response.WorkPolicyVersion != QueryProtocol.WorkPolicyVersion
            || response.ResponseFamily != PartitionQueryResponseFamily.GrainIdPage)
        {
            throw InvalidPartitionResponse(responseIndex, "returned incompatible protocol metadata");
        }

        if (response.Epoch != layout.Epoch
            || !StorageLayout.AreRoutingFormatsCompatible(response.LayoutFormatVersion, layout.FormatVersion)
            || response.LayoutFingerprint is null
            || !StorageLayoutFingerprint.Equals(response.LayoutFingerprint, layoutFingerprint))
        {
            throw InvalidPartitionResponse(responseIndex, "returned mismatched routing metadata");
        }

        if (response.QueryFingerprint is null
            || !QueryPlanFingerprint.Equals(response.QueryFingerprint, queryFingerprint))
        {
            throw InvalidPartitionResponse(responseIndex, "returned a mismatched query fingerprint");
        }

        if (response.Items is null || response.Work is null)
        {
            throw InvalidPartitionResponse(responseIndex, "omitted a required payload");
        }

        var totalOperationCount = response.Work.TotalOperationCount;
        if (response.Items.Length > policy.PartitionResponseItemLimit
            || response.ItemByteCount < 0
            || response.ItemByteCount > policy.PartitionResponseByteLimit
            || HasNegativeWorkComponent(response.Work)
            || totalOperationCount > policy.PartitionWorkBudget)
        {
            throw InvalidPartitionResponse(responseIndex, "exceeded its effective response policy");
        }

        if (!Enum.IsDefined(response.StopReason)
            || response.Exhausted != (response.StopReason == PartitionQueryPageStopReason.Exhausted)
            || response.Exhausted == response.HasFrontier
            || (response.HasFrontier && response.Frontier.IsDefault)
            || (response.Exhausted && !response.Frontier.IsDefault))
        {
            throw InvalidPartitionResponse(responseIndex, "returned an invalid frontier or stop reason");
        }

        if (!Enum.IsDefined(response.Work.AccessPath)
            || response.Work.AccessPath == PartitionQueryAccessPath.None)
        {
            throw InvalidPartitionResponse(responseIndex, "returned an invalid scalar access path");
        }

        if (!HasConsistentWorkEvidence(
                response,
                responseRequirements,
                policy,
                totalOperationCount,
                cursor.HasAfter))
        {
            throw InvalidPartitionResponse(responseIndex, "returned inconsistent scalar work evidence");
        }

        if (response.HasFrontier
            && cursor.HasAfter
            && GrainIdCanonicalOrder.Compare(response.Frontier, cursor.After) <= 0)
        {
            throw InvalidPartitionResponse(responseIndex, "returned a non-progressing frontier");
        }

        GrainId? previous = null;
        foreach (var item in response.Items)
        {
            if (item.IsDefault)
            {
                throw InvalidPartitionResponse(responseIndex, "returned a default GrainId");
            }

            if (cursor.HasAfter && GrainIdCanonicalOrder.Compare(item, cursor.After) <= 0)
            {
                throw InvalidPartitionResponse(responseIndex, "returned an item at or before the input boundary");
            }

            if (response.HasFrontier
                && GrainIdCanonicalOrder.Compare(item, response.Frontier) > 0)
            {
                throw InvalidPartitionResponse(responseIndex, "returned an item beyond its safe frontier");
            }

            if (previous is { } preceding
                && GrainIdCanonicalOrder.Compare(preceding, item) >= 0)
            {
                throw InvalidPartitionResponse(responseIndex, "returned items which are not sorted and distinct");
            }

            previous = item;
        }

        var encodedBytes = GetEncodedLength(response.Items);
        if (encodedBytes != response.ItemByteCount)
        {
            throw InvalidPartitionResponse(responseIndex, "reported an incorrect encoded item size");
        }
    }

    private static bool HasNegativeWorkComponent(PartitionQueryPageWork work)
    {
        return work.OrderedCandidateVisitCount < 0
            || work.RecordProbeCount < 0
            || work.PredicateNodeProbeCount < 0
            || work.IndexEntryProbeCount < 0
            || work.OwnershipProbeCount < 0
            || work.PostingSeekCount < 0
            || work.RangeBucketVisitCount < 0
            || work.RangeMergeOperationCount < 0
            || work.ResultMaterializationCount < 0
            || work.PlannerNodeVisitCount < 0
            || work.PlannerMetadataReadCount < 0
            || work.PostingCandidateVisitCount < 0
            || work.CatalogCandidateVisitCount < 0
            || work.HeapOperationCount < 0
            || work.UnionOperationCount < 0;
    }

    private static bool HasConsistentWorkEvidence(
        PartitionQueryPageResult response,
        QueryResponseRequirements requirements,
        QueryExecutionPolicy policy,
        long totalOperationCount,
        bool hasInputBoundary)
    {
        var work = response.Work;
        if (work.PlannerNodeVisitCount != requirements.WireNodeCount
            || !requirements.Allows(work.AccessPath)
            || (response.StopReason == PartitionQueryPageStopReason.WorkBudget
                && totalOperationCount != policy.PartitionWorkBudget)
            || (response.StopReason == PartitionQueryPageStopReason.ItemLimit
                && response.Items.Length != policy.PartitionResponseItemLimit)
            || (response.StopReason == PartitionQueryPageStopReason.ByteLimit
                && response.Items.Length == 0)
            || work.ResultMaterializationCount != response.Items.LongLength
            || work.ResultMaterializationCount > work.RecordProbeCount
            || work.ResultMaterializationCount > work.PredicateNodeProbeCount
            || work.ResultMaterializationCount > work.OwnershipProbeCount
            || work.OwnershipProbeCount > work.OrderedCandidateVisitCount
            || (response.HasFrontier && work.OwnershipProbeCount == 0)
            || (work.RecordProbeCount > 0 && work.OwnershipProbeCount == 0)
            || (work.PredicateNodeProbeCount > 0 && work.RecordProbeCount == 0)
            || (work.IndexEntryProbeCount > 0 && work.PredicateNodeProbeCount == 0)
            || (work.PostingCandidateVisitCount > 0
                && work.OrderedCandidateVisitCount == 0)
            || (work.CatalogCandidateVisitCount > 0
                && work.OrderedCandidateVisitCount == 0)
            || (work.HeapOperationCount > 0 && work.PostingCandidateVisitCount == 0)
            || (work.RangeMergeOperationCount > 0 && work.PostingCandidateVisitCount == 0)
            || (work.UnionOperationCount > 0 && work.PostingCandidateVisitCount == 0)
            || !requirements.HasMinimumMaterializedPredicateWork(
                response.Items.LongLength,
                work))
        {
            return false;
        }

        var unownedCandidateCount =
            work.OrderedCandidateVisitCount - work.OwnershipProbeCount;
        var unpredicatedRecordCount = Math.Max(
            0,
            work.RecordProbeCount - work.PredicateNodeProbeCount);
        // A work-budget stop may occur after the cursor exposes one candidate but before its
        // ownership charge fits, or after one record probe but before its root predicate charge.
        // Only one stage can be incomplete, and every other successful stop follows complete
        // candidate groups.
        var maximumIncompleteStageCount =
            response.StopReason == PartitionQueryPageStopReason.WorkBudget ? 1 : 0;
        if (unownedCandidateCount > maximumIncompleteStageCount
            || unpredicatedRecordCount
                > maximumIncompleteStageCount - unownedCandidateCount)
        {
            return false;
        }

        // Empty planning can still charge node, metadata, seek, and range-bucket discovery work.
        // It cannot emit evidence from an opened candidate source or record evaluation.
        return work.AccessPath switch
        {
            PartitionQueryAccessPath.Empty =>
                requirements.MeetsEmptyPlanningLowerBound(work)
                && response.Exhausted
                && !response.HasFrontier
                && response.Frontier.IsDefault
                && response.Items.Length == 0
                && work.OrderedCandidateVisitCount == 0
                && work.RecordProbeCount == 0
                && work.PredicateNodeProbeCount == 0
                && work.IndexEntryProbeCount == 0
                && work.OwnershipProbeCount == 0
                && work.ResultMaterializationCount == 0
                && work.PostingCandidateVisitCount == 0
                && work.CatalogCandidateVisitCount == 0
                && work.HeapOperationCount == 0
                && work.UnionOperationCount == 0
                && work.RangeMergeOperationCount == 0,
            PartitionQueryAccessPath.ExactPosting =>
                requirements.MeetsAccessPathPlanningLowerBound(work.AccessPath, work)
                && (hasInputBoundary || work.OrderedCandidateVisitCount > 0)
                && work.CatalogCandidateVisitCount == 0
                && work.HeapOperationCount == 0
                && work.UnionOperationCount == 0
                && work.RangeMergeOperationCount == 0
                && work.PostingCandidateVisitCount >= work.OrderedCandidateVisitCount,
            PartitionQueryAccessPath.RangeMerge =>
                requirements.MeetsAccessPathPlanningLowerBound(work.AccessPath, work)
                && (hasInputBoundary || work.OrderedCandidateVisitCount > 0)
                && work.RangeBucketVisitCount >= 1
                && work.CatalogCandidateVisitCount == 0
                && work.UnionOperationCount == 0
                && work.PostingCandidateVisitCount >= work.OrderedCandidateVisitCount
                && work.HeapOperationCount >= work.OrderedCandidateVisitCount
                && work.RangeMergeOperationCount >= work.OrderedCandidateVisitCount,
            PartitionQueryAccessPath.Union =>
                requirements.MeetsAccessPathPlanningLowerBound(work.AccessPath, work)
                && (hasInputBoundary || work.OrderedCandidateVisitCount > 0)
                && work.CatalogCandidateVisitCount == 0
                && work.PostingCandidateVisitCount >= work.OrderedCandidateVisitCount
                && work.UnionOperationCount >= work.OrderedCandidateVisitCount,
            PartitionQueryAccessPath.Catalog =>
                work.PostingSeekCount >= 1
                && work.PostingCandidateVisitCount == 0
                && work.HeapOperationCount == 0
                && work.UnionOperationCount == 0
                && work.RangeMergeOperationCount == 0
                && work.CatalogCandidateVisitCount >= work.OrderedCandidateVisitCount,
            _ => false,
        };
    }

    private static PageAttemptResult MergePartitionResponses(
        PartitionQueryPageResult[] responses,
        QueryExecutionPolicy policy,
        long totalWork)
    {
        var allExhausted = true;
        var hasGlobalFrontier = false;
        var globalFrontier = default(GrainId);
        foreach (var response in responses)
        {
            if (response.Exhausted)
            {
                continue;
            }

            allExhausted = false;
            if (!hasGlobalFrontier
                || GrainIdCanonicalOrder.Compare(response.Frontier, globalFrontier) < 0)
            {
                hasGlobalFrontier = true;
                globalFrontier = response.Frontier;
            }
        }

        var pending = new PriorityQueue<MergeCursor, GrainId>(GrainIdCanonicalOrder.Comparer);
        for (var responseIndex = 0; responseIndex < responses.Length; responseIndex++)
        {
            var items = responses[responseIndex].Items;
            if (items.Length > 0
                && (!hasGlobalFrontier
                    || GrainIdCanonicalOrder.Compare(items[0], globalFrontier) <= 0))
            {
                pending.Enqueue(new MergeCursor(responseIndex, ItemIndex: 0), items[0]);
            }
        }

        var pageItems = new List<GrainId>(policy.PageSize);
        var pageBytes = 0;
        var truncated = false;
        var hasLast = false;
        var last = default(GrainId);
        while (pending.TryDequeue(out var cursor, out var item))
        {
            if (!hasLast || GrainIdCanonicalOrder.Compare(last, item) != 0)
            {
                var itemBytes = GrainIdCanonicalOrder.GetEncodedLength(item);
                if (pageItems.Count >= policy.PageSize
                    || itemBytes > policy.PageByteLimit - pageBytes)
                {
                    if (pageItems.Count == 0)
                    {
                        throw new SearchableStorageQueryLimitExceededException(
                            "One matching GrainId cannot fit the configured public page-byte limit.");
                    }

                    truncated = true;
                    break;
                }

                pageItems.Add(item);
                pageBytes = checked(pageBytes + itemBytes);
                last = item;
                hasLast = true;
            }

            var nextItemIndex = cursor.ItemIndex + 1;
            var responseItems = responses[cursor.ResponseIndex].Items;
            if (nextItemIndex < responseItems.Length)
            {
                var next = responseItems[nextItemIndex];
                if (!hasGlobalFrontier
                    || GrainIdCanonicalOrder.Compare(next, globalFrontier) <= 0)
                {
                    pending.Enqueue(
                        new MergeCursor(cursor.ResponseIndex, nextItemIndex),
                        next);
                }
            }
        }

        if (truncated)
        {
            return new PageAttemptResult(
                [.. pageItems],
                HasContinuation: true,
                ContinuationAfter: pageItems[^1],
                TotalWork: totalWork,
                ItemByteCount: pageBytes);
        }

        if (allExhausted)
        {
            return new PageAttemptResult(
                [.. pageItems],
                HasContinuation: false,
                ContinuationAfter: default,
                TotalWork: totalWork,
                ItemByteCount: pageBytes);
        }

        if (!hasGlobalFrontier)
        {
            throw new InvalidOperationException(
                "A non-terminal partition page attempt did not produce a global frontier.");
        }

        return new PageAttemptResult(
            [.. pageItems],
            HasContinuation: true,
            ContinuationAfter: globalFrontier,
            TotalWork: totalWork,
            ItemByteCount: pageBytes);
    }

    private static int GetEncodedLength(IEnumerable<GrainId> items)
    {
        var result = 0;
        foreach (var item in items)
        {
            result = checked(result + GrainIdCanonicalOrder.GetEncodedLength(item));
        }

        return result;
    }

    private static InvalidOperationException InvalidPartitionResponse(int index, string reason)
    {
        return new InvalidOperationException($"Storage owner response {index} {reason}.");
    }

    private static SearchableStorageQueryLimitExceededException CreateLegacyLimitException(
        string limit)
    {
        return new SearchableStorageQueryLimitExceededException(
            $"The searchable-storage compatibility query exceeded its {limit} limit.");
    }

    private async Task<PartitionQueryPageResult[]> WaitForFanoutAsync(
        PartitionPageCall[] calls,
        CancellationToken cancellationToken)
    {
        var aggregate = Task.WhenAll(calls.Select(static call => call.Task));
        try
        {
            return await aggregate.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Orleans calls do not accept this local cancellation token. Observe their eventual
            // completion so a later transport or partition failure cannot become unobserved.
            _observeDetachedFanout(aggregate);
            throw;
        }
        catch
        {
            foreach (var call in calls)
            {
                if (call.Task.Exception is not { } taskFailure)
                {
                    continue;
                }

                var nonRoutingFailure = taskFailure
                    .Flatten()
                    .InnerExceptions
                    .FirstOrDefault(static failure => failure is not StorageRouteMismatchException);
                if (nonRoutingFailure is not null)
                {
                    ExceptionDispatchInfo.Capture(nonRoutingFailure).Throw();
                }
            }

            var canceledCall = calls.FirstOrDefault(static call => call.Task.IsCanceled);
            if (canceledCall is not null)
            {
                _ = await canceledCall.Task;
                throw new InvalidOperationException("A canceled partition task completed successfully.");
            }

            var newestMismatch = calls
                .SelectMany(static call => call.Task.Exception?
                    .Flatten()
                    .InnerExceptions
                    .OfType<StorageRouteMismatchException>()
                    .Select(exception => new OwnedRouteMismatch(call.Owner, exception))
                    ?? [])
                .OrderByDescending(static candidate => candidate.Exception.CurrentEpoch)
                .ThenBy(static candidate => candidate.Owner)
                .FirstOrDefault();
            if (newestMismatch is not null)
            {
                ExceptionDispatchInfo.Capture(newestMismatch.Exception).Throw();
            }

            throw new InvalidOperationException(
                "A partition page fan-out failed without an observable failure.");
        }
    }

    private sealed record PartitionPageCall(
        int Owner,
        Task<PartitionQueryPageResult> Task);

    private sealed record OwnedRouteMismatch(
        int Owner,
        StorageRouteMismatchException Exception);

    private readonly record struct MergeCursor(int ResponseIndex, int ItemIndex);

    private readonly record struct QueryCursor(bool HasAfter, GrainId After)
    {
        public static QueryCursor Initial => default;

        public static QueryCursor AfterValue(GrainId after) => new(HasAfter: true, after);
    }

    private sealed record PageAttemptResult(
        GrainId[] Items,
        bool HasContinuation,
        GrainId ContinuationAfter,
        long TotalWork,
        int ItemByteCount);

    private static async Task ObserveCompletionAsync(Task task)
    {
        await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private static void ObserveDetachedFanout(Task task)
    {
        _ = ObserveCompletionAsync(task);
    }
}
