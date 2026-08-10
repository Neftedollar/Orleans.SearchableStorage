using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Runtime.ExceptionServices;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage;

/// <summary>
/// Queries storage partitions through an Orleans grain factory.
/// </summary>
public sealed class SearchableStorageClient : ISearchableStorageQueryClient
{
    private readonly string _providerName;
    private readonly Func<int, IStoragePartitionGrain> _getPartition;
    private readonly StorageLayoutCache _layoutCache;

    /// <summary>
    /// Initializes a client for one searchable storage provider.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to contact storage grains.</param>
    /// <param name="providerName">The searchable storage-provider name.</param>
    /// <param name="partitionCount">The partition count configured for that provider.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grainFactory"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="providerName"/> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionCount"/> is not positive.</exception>
    public SearchableStorageClient(IGrainFactory grainFactory, string providerName, int partitionCount)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);

        _providerName = providerName;
        var layout = StorageLayout.CreateIdentity(providerName, partitionCount);
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
        Func<Task<bool>> validateLayout)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(partitions);
        ArgumentNullException.ThrowIfNull(validateLayout);
        ArgumentOutOfRangeException.ThrowIfZero(partitions.Count);

        _providerName = providerName;
        var staticLayout = CreateStaticLayout(providerName, partitions.Count);
        _layoutCache = new StorageLayoutCache(
            async () => await validateLayout() ? staticLayout : null);
        _getPartition = index => partitions[index];
    }

    internal SearchableStorageClient(
        string providerName,
        StorageLayoutCache layoutCache,
        Func<int, IStoragePartitionGrain> getPartition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(layoutCache);
        ArgumentNullException.ThrowIfNull(getPartition);

        _providerName = providerName;
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
    public async Task<IReadOnlyList<GrainId>> FindAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue value,
        CancellationToken cancellationToken = default)
    {
        var index = IndexMetadataProvider.GetSelectedIndex(stateName, propertySelector);
        var indexValue = CreateQueryValue(index, value, nameof(value));

        var query = new ExactIndexQuery
        {
            Scope = index.Scope,
            Kind = index.Kind,
            Value = indexValue,
        };

        return await ExecuteRoutedQueryAsync(
            (partition, epoch) => partition.FindRoutedAsync(new RoutedExactIndexQuery
            {
                Query = query,
                Epoch = epoch,
            }),
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<GrainId>> RangeAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue lowerBound,
        TValue upperBound,
        bool includeLowerBound = true,
        bool includeUpperBound = true,
        CancellationToken cancellationToken = default)
    {
        var index = IndexMetadataProvider.GetSelectedIndex(stateName, propertySelector);
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

        var query = new RangeIndexQuery
        {
            Scope = index.Scope,
            LowerBound = lowerValue,
            UpperBound = upperValue,
            IncludeLowerBound = includeLowerBound,
            IncludeUpperBound = includeUpperBound,
        };

        return await ExecuteRoutedQueryAsync(
            (partition, epoch) => partition.RangeRoutedAsync(new RoutedRangeIndexQuery
            {
                Query = query,
                Epoch = epoch,
            }),
            cancellationToken);
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return $"{nameof(SearchableStorageClient)}({_providerName})";
    }

    internal async Task<IReadOnlyList<GrainId>> ExecuteQueryAsync<TState>(
        string stateName,
        Expression expression,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var plan = QueryTranslator.Translate<TState>(stateName, expression);
        var layout = await _layoutCache.GetAsync(cancellationToken);
        if (layout is null)
        {
            return [];
        }

        if (plan is EmptyQueryPlan)
        {
            return [];
        }

        var partitionPlan = PartitionQueryPlanFactory.Create(plan);
        return await ExecuteRoutedQueryAsync(
            layout,
            (partition, epoch) => partition.QueryRoutedAsync(new RoutedPartitionQuery
            {
                Query = partitionPlan,
                Epoch = epoch,
            }),
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

    private async Task<IReadOnlyList<GrainId>> ExecuteRoutedQueryAsync(
        Func<IStoragePartitionGrain, long, Task<GrainId[]>> query,
        CancellationToken cancellationToken)
    {
        var layout = await _layoutCache.GetAsync(cancellationToken);
        if (layout is null)
        {
            return [];
        }

        return await ExecuteRoutedQueryAsync(layout, query, cancellationToken);
    }

    private async Task<IReadOnlyList<GrainId>> ExecuteRoutedQueryAsync(
        StorageLayoutSnapshot layout,
        Func<IStoragePartitionGrain, long, Task<GrainId[]>> query,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        for (var attempt = 0; ; attempt++)
        {
            var tasks = layout
                .GetDistinctOwners()
                .Select(owner => StartRoutedQuery(owner, layout.Epoch, query));
            try
            {
                var results = await WaitForFanoutAsync(tasks, cancellationToken);
                return Merge(results);
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

    private Task<GrainId[]> StartRoutedQuery(
        int owner,
        long epoch,
        Func<IStoragePartitionGrain, long, Task<GrainId[]>> query)
    {
        try
        {
            return query(_getPartition(owner), epoch);
        }
        catch (Exception exception)
        {
            // Normalize synchronous test doubles, client lookup failures, and custom Orleans
            // proxies into the same all-partitions completion path as faulted RPC tasks.
            return Task.FromException<GrainId[]>(exception);
        }
    }

    private static StorageLayoutSnapshot CreateStaticLayout(string providerName, int partitionCount)
    {
        var assignments = StorageLayout.CreateIdentityAssignments(partitionCount, partitionCount);
        return StorageLayoutSnapshot.FromState(new StorageLayoutState
        {
            Initialized = true,
            FormatVersion = StorageLayout.CurrentFormatVersion,
            ProviderName = providerName,
            PartitionCount = partitionCount,
            VirtualSlotCount = partitionCount,
            SlotAssignments = assignments,
            Epoch = 1,
        });
    }

    private static GrainId[] Merge(IEnumerable<GrainId[]> results)
    {
        return results
            .SelectMany(static result => result)
            .Distinct()
            .Order()
            .ToArray();
    }

    private static async Task<T[]> WaitForFanoutAsync<T>(
        IEnumerable<Task<T>> tasks,
        CancellationToken cancellationToken)
    {
        var partitionTasks = tasks.ToArray();
        var aggregate = Task.WhenAll(partitionTasks);
        try
        {
            return await aggregate.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Orleans calls do not accept this local cancellation token. Observe their eventual
            // completion so a later transport or partition failure cannot become unobserved.
            _ = ObserveCompletionAsync(aggregate);
            throw;
        }
        catch
        {
            var failures = aggregate.Exception?.Flatten().InnerExceptions;
            var nonRoutingFailure = failures?.FirstOrDefault(
                static failure => failure is not StorageRouteMismatchException);
            if (nonRoutingFailure is not null)
            {
                ExceptionDispatchInfo.Capture(nonRoutingFailure).Throw();
            }

            var canceledPartition = partitionTasks.FirstOrDefault(static task => task.IsCanceled);
            if (canceledPartition is not null)
            {
                // Task.WhenAll is faulted rather than canceled when another child also faults, and
                // canceled children are absent from AggregateException.InnerExceptions. Surface the
                // cancellation before classifying the remaining failures as routing-only.
                _ = await canceledPartition;
                throw new InvalidOperationException("A canceled partition task completed successfully.");
            }

            if (failures is not null)
            {
                var newestMismatch = failures
                    .OfType<StorageRouteMismatchException>()
                    .MaxBy(static failure => failure.CurrentEpoch);
                if (newestMismatch is not null)
                {
                    ExceptionDispatchInfo.Capture(newestMismatch).Throw();
                }
            }

            throw;
        }
    }

    private static async Task ObserveCompletionAsync(Task task)
    {
        await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
