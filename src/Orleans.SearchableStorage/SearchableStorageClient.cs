using System.Linq.Expressions;
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
    private readonly Func<Task<bool>> _validateLayout;
    private readonly object _layoutLock = new();
    private readonly IStoragePartitionGrain[] _partitions;
    private Task<bool>? _layoutValidationTask;

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
        var layout = StorageLayout.CreateDescriptor(providerName, partitionCount);
        var layoutGrain = grainFactory.GetGrain<IStorageLayoutGrain>(providerName);
        _validateLayout = () => layoutGrain.ValidateAsync(layout);
        _partitions = new IStoragePartitionGrain[partitionCount];
        for (var index = 0; index < partitionCount; index++)
        {
            _partitions[index] = grainFactory.GetGrain<IStoragePartitionGrain>(StorageLayout.CreatePartitionKey(providerName, index));
        }
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
        _validateLayout = validateLayout;
        _partitions = partitions.ToArray();
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
        if (!await IsLayoutInitializedAsync(cancellationToken))
        {
            return [];
        }

        var query = new ExactIndexQuery
        {
            Scope = index.Scope,
            Kind = index.Kind,
            Value = indexValue,
        };

        var tasks = _partitions.Select(partition => partition.FindAsync(query));
        var results = await WaitForFanoutAsync(tasks, cancellationToken);
        return Merge(results);
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

        if (!await IsLayoutInitializedAsync(cancellationToken))
        {
            return [];
        }

        var query = new RangeIndexQuery
        {
            Scope = index.Scope,
            LowerBound = lowerValue,
            UpperBound = upperValue,
            IncludeLowerBound = includeLowerBound,
            IncludeUpperBound = includeUpperBound,
        };

        var tasks = _partitions.Select(partition => partition.RangeAsync(query));
        var results = await WaitForFanoutAsync(tasks, cancellationToken);
        return Merge(results);
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
        if (!await IsLayoutInitializedAsync(cancellationToken))
        {
            return [];
        }

        var partitionPlan = PartitionQueryPlanFactory.Create(plan);
        var tasks = _partitions.Select(partition => partition.QueryAsync(partitionPlan));
        var results = await WaitForFanoutAsync(tasks, cancellationToken);
        return Merge(results);
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

    private async Task<bool> IsLayoutInitializedAsync(CancellationToken cancellationToken)
    {
        Task<bool> validationTask;
        lock (_layoutLock)
        {
            validationTask = _layoutValidationTask ??= _validateLayout();
        }

        try
        {
            var initialized = await validationTask.WaitAsync(cancellationToken);
            if (!initialized)
            {
                ResetLayoutValidation(validationTask);
            }

            return initialized;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            ResetLayoutValidation(validationTask);
            _ = ObserveCompletionAsync(validationTask);
            throw;
        }
        catch
        {
            ResetLayoutValidation(validationTask);
            throw;
        }
    }

    private void ResetLayoutValidation(Task<bool> validationTask)
    {
        lock (_layoutLock)
        {
            if (ReferenceEquals(_layoutValidationTask, validationTask))
            {
                _layoutValidationTask = null;
            }
        }
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
        var aggregate = Task.WhenAll(tasks);
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
    }

    private static async Task ObserveCompletionAsync(Task task)
    {
        await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
