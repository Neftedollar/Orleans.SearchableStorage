using System.Linq.Expressions;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage;

public sealed partial class SearchableStorageClient
{
    internal async Task<SearchableStorageDistinctFacetPage<TValue>> ExecuteDistinctFacetPageAsync<TState, TValue>(
        string stateName,
        Expression queryExpression,
        LambdaExpression propertySelector,
        SearchableStorageFacetPageRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        if (request.PageSize > _queryConfiguration.PageSizeLimit)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.PageSize,
                $"PageSize must not exceed the configured limit of {_queryConfiguration.PageSizeLimit}.");
        }

        var facet = CreateFacetPlan<TState, TValue>(
            stateName,
            queryExpression,
            propertySelector);
        await EnsureSchemaActiveAsync<TState>(
            stateName,
            cancellationToken,
            requireFreshUnregisteredCapability:
                facet.Query.Operation == PartitionQueryOperation.Empty);
        var isContinuation = request.ContinuationToken is not null;
        if (facet.Query.Operation == PartitionQueryOperation.Empty)
        {
            if (isContinuation)
            {
                throw new SearchableStorageInvalidContinuationTokenException(
                    "An empty facet query cannot resume a continuation.");
            }

            return new SearchableStorageDistinctFacetPage<TValue>([], continuationToken: null);
        }

        EnsureFacetContinuationProtection();
        var layout = await _layoutCache.GetAsync(cancellationToken);
        if (layout is null)
        {
            if (isContinuation)
            {
                throw new SearchableStorageInvalidContinuationTokenException(
                    "A facet continuation cannot resume an uninitialized storage namespace.");
            }

            return new SearchableStorageDistinctFacetPage<TValue>([], continuationToken: null);
        }

        var routeRefreshAvailable = !isContinuation;
        var dataRestartAvailable = true;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var owners = layout.GetDistinctOwners();
            var policy = QueryExecutionPolicy.Create(
                _queryConfiguration,
                request.PageSize,
                owners.Length);
            var layoutFingerprint = StorageLayoutFingerprint.Compute(layout);
            var binding = CreateFacetTokenBinding(
                facet.Fingerprint,
                layout,
                layoutFingerprint,
                policy);
            var after = isContinuation
                ? _tokenCodec.Unprotect(request.ContinuationToken!, binding).AfterFacetValue
                    ?? throw new SearchableStorageInvalidContinuationTokenException()
                : null;
            try
            {
                var page = await ExecuteDistinctFacetAttemptAsync(
                    facet,
                    layout,
                    layoutFingerprint,
                    owners,
                    policy,
                    after,
                    cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var token = page.HasContinuation
                    ? _tokenCodec.Protect(
                        ContinuationTokenPayload.CreateFacet(binding, page.ContinuationAfter!))
                    : null;
                var typed = page.Items
                    .Select(value => IndexValueMaterializer.Materialize<TValue>(
                        value,
                        facet.Index.Converter))
                    .ToArray();
                return new SearchableStorageDistinctFacetPage<TValue>(typed, token);
            }
            catch (PartitionQueryBudgetTooSmallException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The distinct facet cannot make progress within its partition limits.",
                    exception);
            }
            catch (StorageFacetValueUnsupportedException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The distinct facet encountered a stored value outside the supported canonical facet domain.",
                    exception);
            }
            catch (StorageFacetDataChangedException) when (dataRestartAvailable)
            {
                dataRestartAvailable = false;
                continue;
            }
            catch (StorageFacetDataChangedException exception)
            {
                throw new SearchableStorageFacetConcurrentChangeException(
                    "Partition data changed again while producing one distinct facet page.",
                    exception);
            }
            catch (StorageRouteMismatchException exception) when (isContinuation)
            {
                throw new SearchableStorageStaleContinuationTokenException(
                    "The routing layout changed while resuming the distinct facet.",
                    exception);
            }
            catch (StorageRouteMismatchException) when (routeRefreshAvailable)
            {
                routeRefreshAvailable = false;
                _layoutCache.Invalidate(layout);
                layout = await _layoutCache.GetAsync(cancellationToken)
                    ?? throw new InvalidOperationException(
                        "The storage layout disappeared while the facet route was refreshing.");
            }
        }
    }

    internal async Task<SearchableStorageFacetResult<TValue>> ExecuteFacetValueCountsAsync<TState, TValue>(
        string stateName,
        Expression queryExpression,
        LambdaExpression propertySelector,
        SearchableStorageFacetRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        if (request.TopN > _queryConfiguration.FacetTopNLimit)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                request.TopN,
                $"TopN must not exceed the configured facet limit of {_queryConfiguration.FacetTopNLimit}.");
        }

        var facet = CreateFacetPlan<TState, TValue>(
            stateName,
            queryExpression,
            propertySelector);
        await EnsureSchemaActiveAsync<TState>(
            stateName,
            cancellationToken,
            requireFreshUnregisteredCapability:
                facet.Query.Operation == PartitionQueryOperation.Empty);
        if (facet.Query.Operation == PartitionQueryOperation.Empty)
        {
            return new SearchableStorageFacetResult<TValue>(
                [],
                isExact: true,
                maximumOmittedCount: 0);
        }

        var result = await ExecuteFacetTerminalWithRestartAsync(
            async (layout, layoutFingerprint, owners, cancellation) =>
                await ExecuteValueCountsAttemptAsync(
                    facet,
                    request,
                    layout,
                    layoutFingerprint,
                    owners,
                    cancellation),
            static () => new FacetCountAttemptResult([], IsExact: true, MaximumOmittedCount: 0),
            cancellationToken);
        var typed = result.Items.Select(item => new SearchableStorageFacetValueCount<TValue>(
            IndexValueMaterializer.Materialize<TValue>(item.Value, facet.Index.Converter),
            item.Count)).ToArray();
        return new SearchableStorageFacetResult<TValue>(
            typed,
            result.IsExact,
            result.MaximumOmittedCount);
    }

    internal async Task<SearchableStorageFacetMinMax<TValue>?> ExecuteFacetMinMaxAsync<TState, TValue>(
        string stateName,
        Expression queryExpression,
        LambdaExpression propertySelector,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var facet = CreateFacetPlan<TState, TValue>(
            stateName,
            queryExpression,
            propertySelector);
        await EnsureSchemaActiveAsync<TState>(
            stateName,
            cancellationToken,
            requireFreshUnregisteredCapability:
                facet.Query.Operation == PartitionQueryOperation.Empty);
        if (facet.Query.Operation == PartitionQueryOperation.Empty)
        {
            return null;
        }

        var result = await ExecuteFacetTerminalWithRestartAsync(
            async (layout, layoutFingerprint, owners, cancellation) =>
                await ExecuteMinMaxAttemptAsync(
                    facet,
                    layout,
                    layoutFingerprint,
                    owners,
                    cancellation),
            static () => null,
            cancellationToken);
        if (result is null)
        {
            return null;
        }

        return new SearchableStorageFacetMinMax<TValue>(
            IndexValueMaterializer.Materialize<TValue>(result.Minimum, facet.Index.Converter),
            IndexValueMaterializer.Materialize<TValue>(result.Maximum, facet.Index.Converter));
    }

    private async Task<TResult> ExecuteFacetTerminalWithRestartAsync<TResult>(
        Func<StorageLayoutSnapshot, byte[], int[], CancellationToken, Task<TResult>> execute,
        Func<TResult> createEmpty,
        CancellationToken cancellationToken)
    {
        var layout = await _layoutCache.GetAsync(cancellationToken);
        if (layout is null)
        {
            return createEmpty();
        }

        var routeRefreshAvailable = true;
        var dataRestartAvailable = true;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var result = await execute(
                    layout,
                    StorageLayoutFingerprint.Compute(layout),
                    layout.GetDistinctOwners(),
                    cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                return result;
            }
            catch (PartitionQueryBudgetTooSmallException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The facet cannot make progress within its partition limits.",
                    exception);
            }
            catch (StorageFacetValueUnsupportedException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The facet encountered a stored value outside the supported canonical facet domain.",
                    exception);
            }
            catch (StorageFacetDataChangedException) when (dataRestartAvailable)
            {
                dataRestartAvailable = false;
                continue;
            }
            catch (StorageFacetDataChangedException exception)
            {
                throw new SearchableStorageFacetConcurrentChangeException(
                    "Partition data changed again while the exact facet was executing.",
                    exception);
            }
            catch (StorageRouteMismatchException) when (routeRefreshAvailable)
            {
                routeRefreshAvailable = false;
                _layoutCache.Invalidate(layout);
                layout = await _layoutCache.GetAsync(cancellationToken)
                    ?? throw new InvalidOperationException(
                        "The storage layout disappeared while the facet route was refreshing.");
            }
        }
    }

    private FacetPlan CreateFacetPlan<TState, TValue>(
        string stateName,
        Expression queryExpression,
        LambdaExpression propertySelector)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentNullException.ThrowIfNull(queryExpression);
        ArgumentNullException.ThrowIfNull(propertySelector);
        if (propertySelector is not Expression<Func<TState, TValue>> typedSelector)
        {
            throw new ArgumentException(
                "The facet selector must select a property from the query element type.",
                nameof(propertySelector));
        }

        var schema = GetRegisteredSchema<TState>(stateName);
        var index = IndexMetadataProvider.GetSelectedIndex(
            stateName,
            typedSelector,
            schema?.Fingerprint);
        if (index.Converter.ValueType != typeof(TValue))
        {
            throw new ArgumentException(
                $"The facet selector result type '{typeof(TValue)}' must exactly match indexed property type "
                + $"'{index.Converter.ValueType}'; conversions and boxing are not supported.",
                nameof(propertySelector));
        }

        var plan = PartitionQueryPlanFactory.Create(
            QueryTranslator.TranslateFacet<TState>(
                stateName,
                queryExpression,
                schema?.Fingerprint));
        return new FacetPlan(
            stateName,
            plan,
            index,
            FacetQueryFingerprint.Compute(stateName, plan, index.Scope, index.Kind));
    }

    private void EnsureFacetContinuationProtection()
    {
        if (_queryConfiguration.CurrentKey is null)
        {
            throw new SearchableStorageQueryConfigurationException(
                "A current continuation-protection key is required for distinct facet paging.");
        }
    }

    private ContinuationTokenBinding CreateFacetTokenBinding(
        byte[] fingerprint,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy)
    {
        return new ContinuationTokenBinding(
            _providerName,
            PartitionQueryResponseFamily.DistinctFacetValuePage,
            CreateFacetContinuationFingerprint(fingerprint),
            QueryProtocol.FacetValueOrderingVersion,
            layout.FormatVersion,
            layout.Epoch,
            layoutFingerprint,
            policy);
    }

    private byte[] CreateFacetContinuationFingerprint(byte[] facetFingerprint)
    {
        using var writer = new CanonicalBinaryWriter();
        writer.WriteRawBytes(facetFingerprint);
        writer.WriteInt32(QueryProtocol.FacetWorkPolicyVersion);
        writer.WriteInt64(_queryConfiguration.FacetAggregateWorkLimit);
        writer.WriteInt32(_queryConfiguration.FacetAggregateItemLimit);
        writer.WriteInt32(_queryConfiguration.FacetAggregateByteLimit);
        writer.WriteInt32(_queryConfiguration.FacetRoundLimit);
        return SHA256.HashData(writer.WrittenSpan);
    }

    private async Task<DistinctFacetAttemptResult> ExecuteDistinctFacetAttemptAsync(
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        int[] owners,
        QueryExecutionPolicy policy,
        IndexValue? after,
        CancellationToken cancellationToken)
    {
        var budget = new FacetAggregateBudget(_queryConfiguration);
        var states = owners.Select(owner => new FacetOwnerState(owner) { AfterValue = after }).ToArray();
        var responses = await FetchDistinctPagesAsync(
            facet,
            layout,
            layoutFingerprint,
            states,
            policy,
            budget,
            cancellationToken);

        var allExhausted = responses.All(static response => response.Exhausted);
        IndexValue? globalFrontier = null;
        foreach (var response in responses)
        {
            if (!response.Exhausted
                && (globalFrontier is null || response.Frontier!.CompareTo(globalFrontier) < 0))
            {
                globalFrontier = response.Frontier;
            }
        }

        var candidates = new SortedSet<IndexValue>();
        foreach (var response in responses)
        {
            foreach (var value in response.Items)
            {
                if (globalFrontier is null || value.CompareTo(globalFrontier) <= 0)
                {
                    candidates.Add(value);
                }
            }
        }

        var page = new List<IndexValue>(policy.PageSize);
        var pageBytes = 0;
        foreach (var value in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = await ProbeValueAcrossOwnersAsync(
                facet,
                value,
                layout,
                layoutFingerprint,
                states,
                budget,
                cancellationToken);
            if (count == 0)
            {
                continue;
            }

            var encoded = IndexValueCanonicalEncoding.GetEncodedLength(value);
            if (encoded > policy.PageByteLimit)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "One distinct facet value cannot fit the configured public page-byte limit.");
            }

            if (encoded > policy.PageByteLimit - pageBytes)
            {
                return new DistinctFacetAttemptResult(
                    [.. page],
                    HasContinuation: true,
                    ContinuationAfter: page[^1]);
            }

            page.Add(value);
            pageBytes = checked(pageBytes + encoded);
            if (page.Count == policy.PageSize)
            {
                return new DistinctFacetAttemptResult(
                    [.. page],
                    HasContinuation: true,
                    ContinuationAfter: value);
            }
        }

        if (allExhausted)
        {
            return new DistinctFacetAttemptResult(
                [.. page],
                HasContinuation: false,
                ContinuationAfter: null);
        }

        if (globalFrontier is null)
        {
            throw new InvalidOperationException(
                "A non-terminal distinct facet turn omitted its global value frontier.");
        }

        return new DistinctFacetAttemptResult(
            [.. page],
            HasContinuation: true,
            ContinuationAfter: globalFrontier);
    }

    private async Task<FacetCountAttemptResult> ExecuteValueCountsAttemptAsync(
        FacetPlan facet,
        SearchableStorageFacetRequest request,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        int[] owners,
        CancellationToken cancellationToken)
    {
        var candidatePageSize = Math.Min(
            _queryConfiguration.PageSizeLimit,
            Math.Max(1, (request.TopN + owners.Length - 1) / owners.Length));
        var policy = QueryExecutionPolicy.Create(
            _queryConfiguration,
            candidatePageSize,
            owners.Length);
        var budget = new FacetAggregateBudget(_queryConfiguration);
        var states = owners.Select(static owner => new FacetOwnerState(owner)).ToArray();
        var nominated = new HashSet<IndexValue>();
        var exactCounts = new Dictionary<IndexValue, long>();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var newCandidates = await FetchCandidatePagesAsync(
                facet,
                layout,
                layoutFingerprint,
                states,
                policy,
                budget,
                cancellationToken);
            foreach (var value in newCandidates.Order())
            {
                if (!nominated.Add(value))
                {
                    continue;
                }

                exactCounts.Add(
                    value,
                    await ProbeValueAcrossOwnersAsync(
                        facet,
                        value,
                        layout,
                        layoutFingerprint,
                        states,
                        budget,
                        cancellationToken));
            }

            var ranked = exactCounts
                .Where(static pair => pair.Value > 0)
                .OrderByDescending(static pair => pair.Value)
                .ThenBy(static pair => pair.Key)
                .Select(static pair => new FacetCountItem(pair.Key, pair.Value))
                .ToArray();
            var allExhausted = states.All(static state => state.Exhausted);
            var unseenBound = SumUnseenBounds(states);
            var cutoffProven = ranked.Length >= request.TopN
                && ranked[request.TopN - 1].Count > unseenBound;
            var isExact = allExhausted || cutoffProven;
            if (request.Accuracy == SearchableStorageFacetAccuracy.Approximate || isExact)
            {
                var returned = ranked.Take(request.TopN).ToArray();
                var nominatedOmitted = ranked.Skip(request.TopN)
                    .Select(static item => item.Count)
                    .DefaultIfEmpty(0)
                    .Max();
                return new FacetCountAttemptResult(
                    returned,
                    isExact,
                    Math.Max(unseenBound, nominatedOmitted));
            }

            if (newCandidates.Count == 0 && !allExhausted)
            {
                // Every owner still advanced its canonical value frontier; another bounded turn
                // can reveal a value which was already nominated by another owner.
                continue;
            }
        }
    }

    private async Task<FacetMinMaxAttemptResult?> ExecuteMinMaxAttemptAsync(
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        int[] owners,
        CancellationToken cancellationToken)
    {
        var pageSize = Math.Min(
            SearchableStorageQueryOptions.DefaultPageSize,
            _queryConfiguration.PageSizeLimit);
        var policy = QueryExecutionPolicy.Create(
            _queryConfiguration,
            pageSize,
            owners.Length);
        var budget = new FacetAggregateBudget(_queryConfiguration);
        var states = owners.Select(static owner => new FacetOwnerState(owner)).ToArray();
        var seen = new HashSet<IndexValue>();
        IndexValue? minimum = null;
        IndexValue? maximum = null;
        while (!states.All(static state => state.Exhausted))
        {
            var candidates = await FetchCandidatePagesAsync(
                facet,
                layout,
                layoutFingerprint,
                states,
                policy,
                budget,
                cancellationToken);
            foreach (var value in candidates.Order())
            {
                if (!seen.Add(value))
                {
                    continue;
                }

                var count = await ProbeValueAcrossOwnersAsync(
                    facet,
                    value,
                    layout,
                    layoutFingerprint,
                    states,
                    budget,
                    cancellationToken);
                if (count == 0)
                {
                    continue;
                }

                if (minimum is null || value.CompareTo(minimum) < 0)
                {
                    minimum = value;
                }

                if (maximum is null || value.CompareTo(maximum) > 0)
                {
                    maximum = value;
                }
            }
        }

        return minimum is null
            ? null
            : new FacetMinMaxAttemptResult(minimum, maximum!);
    }

    private async Task<PartitionDistinctFacetPageResult[]> FetchDistinctPagesAsync(
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        FacetOwnerState[] states,
        QueryExecutionPolicy policy,
        FacetAggregateBudget budget,
        CancellationToken cancellationToken)
    {
        var allocation = budget.AllocateTurn(states.Length, requiresItems: true, requiresBytes: true);
        policy = policy with
        {
            PartitionWorkBudget = Math.Min(policy.PartitionWorkBudget, allocation.WorkPerOwner),
            PartitionResponseItemLimit = Math.Min(policy.PartitionResponseItemLimit, allocation.ItemsPerOwner),
            PartitionResponseByteLimit = Math.Min(policy.PartitionResponseByteLimit, allocation.BytesPerOwner),
        };
        var schema = _stateRegistry.Find(_providerName, facet.StateName)?.Schema;
        var calls = new OwnedFacetCall<PartitionDistinctFacetPageResult>[states.Length];
        for (var index = 0; index < states.Length; index++)
        {
            var state = states[index];
            var wire = new RoutedPartitionDistinctFacetPageRequest
            {
                Query = facet.Query,
                FacetScope = facet.Index.Scope,
                FacetKind = facet.Index.Kind,
                Epoch = layout.Epoch,
                After = state.AfterValue,
                WorkBudget = policy.PartitionWorkBudget,
                ItemLimit = policy.PartitionResponseItemLimit,
                ByteLimit = policy.PartitionResponseByteLimit,
                ProtocolVersion = QueryProtocol.PagingVersion,
                OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
                WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
                ResponseFamily = PartitionQueryResponseFamily.DistinctFacetValuePage,
                RequestFingerprint = [.. facet.Fingerprint],
                LayoutFormatVersion = layout.FormatVersion,
                LayoutFingerprint = [.. layoutFingerprint],
                StateName = facet.StateName,
                HasExpectedDataVersion = state.HasDataVersion,
                ExpectedDataVersion = state.DataVersion,
                IndexSchemaFingerprint = schema?.Fingerprint,
                IndexSchemaProtocolVersion = schema is null
                    ? 0
                    : StorageIndexSchema.ProtocolVersion,
            };
            calls[index] = new OwnedFacetCall<PartitionDistinctFacetPageResult>(
                state.Owner,
                StartDistinctFacetPage(state.Owner, wire));
        }

        var responses = await WaitForFacetFanoutAsync(calls, cancellationToken);
        for (var index = 0; index < responses.Length; index++)
        {
            var response = responses[index]
                ?? throw InvalidFacetResponse(index, "returned a null response");
            ValidateDistinctResponse(
                index,
                response,
                states[index],
                facet,
                layout,
                layoutFingerprint,
                policy);
            states[index].PinDataVersion(response.DataVersion);
            budget.Record(
                response.Work,
                response.Items.Length,
                response.ItemByteCount);
        }

        return responses;
    }

    private async Task<HashSet<IndexValue>> FetchCandidatePagesAsync(
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        FacetOwnerState[] states,
        QueryExecutionPolicy policy,
        FacetAggregateBudget budget,
        CancellationToken cancellationToken)
    {
        var active = states.Where(static state => !state.Exhausted).ToArray();
        if (active.Length == 0)
        {
            return [];
        }

        var allocation = budget.AllocateTurn(active.Length, requiresItems: true, requiresBytes: true);
        policy = policy with
        {
            PartitionWorkBudget = Math.Min(policy.PartitionWorkBudget, allocation.WorkPerOwner),
            PartitionResponseItemLimit = Math.Min(policy.PartitionResponseItemLimit, allocation.ItemsPerOwner),
            PartitionResponseByteLimit = Math.Min(policy.PartitionResponseByteLimit, allocation.BytesPerOwner),
        };
        var schema = _stateRegistry.Find(_providerName, facet.StateName)?.Schema;
        var calls = new OwnedFacetCall<PartitionFacetCandidatePageResult>[active.Length];
        for (var index = 0; index < active.Length; index++)
        {
            var state = active[index];
            var wire = new RoutedPartitionFacetCandidatePageRequest
            {
                Query = facet.Query,
                FacetScope = facet.Index.Scope,
                FacetKind = facet.Index.Kind,
                Epoch = layout.Epoch,
                AfterValue = state.AfterValue,
                WorkBudget = policy.PartitionWorkBudget,
                ItemLimit = policy.PartitionResponseItemLimit,
                ByteLimit = policy.PartitionResponseByteLimit,
                ProtocolVersion = QueryProtocol.PagingVersion,
                OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
                WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
                ResponseFamily = PartitionQueryResponseFamily.FacetValueCountCandidates,
                RequestFingerprint = [.. facet.Fingerprint],
                LayoutFormatVersion = layout.FormatVersion,
                LayoutFingerprint = [.. layoutFingerprint],
                StateName = facet.StateName,
                HasExpectedDataVersion = state.HasDataVersion,
                ExpectedDataVersion = state.DataVersion,
                IndexSchemaFingerprint = schema?.Fingerprint,
                IndexSchemaProtocolVersion = schema is null
                    ? 0
                    : StorageIndexSchema.ProtocolVersion,
            };
            calls[index] = new OwnedFacetCall<PartitionFacetCandidatePageResult>(
                state.Owner,
                StartFacetCandidatePage(state.Owner, wire));
        }

        var responses = await WaitForFacetFanoutAsync(calls, cancellationToken);
        var candidates = new HashSet<IndexValue>();
        for (var index = 0; index < responses.Length; index++)
        {
            var response = responses[index]
                ?? throw InvalidFacetResponse(index, "returned a null response");
            var state = active[index];
            var rawProgress = ValidateCandidateResponse(
                index,
                response,
                state,
                facet,
                layout,
                layoutFingerprint,
                policy);
            state.PinDataVersion(response.DataVersion);
            state.TotalRawCount = rawProgress.Total;
            state.VisitedRawCount = rawProgress.Visited;
            state.UnseenCountUpperBound = rawProgress.Unseen;
            state.Exhausted = response.Exhausted;
            state.AfterValue = response.Exhausted ? null : response.FrontierValue;
            budget.Record(response.Work, response.Items.Length, response.ItemByteCount);
            foreach (var item in response.Items)
            {
                candidates.Add(item.Value);
            }
        }

        return candidates;
    }

    private async Task<long> ProbeValueAcrossOwnersAsync(
        FacetPlan facet,
        IndexValue value,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        FacetOwnerState[] ownerStates,
        FacetAggregateBudget budget,
        CancellationToken cancellationToken)
    {
        var probes = ownerStates.Select(static state => new FacetProbeState(state)).ToArray();
        long globalCount = 0;
        while (probes.Any(static probe => !probe.Exhausted))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var active = probes.Where(static probe => !probe.Exhausted).ToArray();
            var allocation = budget.AllocateTurn(active.Length, requiresItems: true, requiresBytes: false);
            var schema = _stateRegistry.Find(_providerName, facet.StateName)?.Schema;
            var calls = new OwnedFacetCall<PartitionFacetCountSliceResult>[active.Length];
            for (var index = 0; index < active.Length; index++)
            {
                var probe = active[index];
                var wire = new RoutedPartitionFacetCountSliceRequest
                {
                    Query = facet.Query,
                    FacetScope = facet.Index.Scope,
                    FacetKind = facet.Index.Kind,
                    Value = value,
                    Epoch = layout.Epoch,
                    HasAfter = probe.HasAfter,
                    After = probe.After,
                    WorkBudget = Math.Min(_queryConfiguration.PartitionWorkBudget, allocation.WorkPerOwner),
                    ProtocolVersion = QueryProtocol.PagingVersion,
                    OrderingVersion = QueryProtocol.FacetValueOrderingVersion,
                    WorkPolicyVersion = QueryProtocol.FacetWorkPolicyVersion,
                    ResponseFamily = PartitionQueryResponseFamily.FacetValueCountProbe,
                    RequestFingerprint = [.. facet.Fingerprint],
                    LayoutFormatVersion = layout.FormatVersion,
                    LayoutFingerprint = [.. layoutFingerprint],
                    StateName = facet.StateName,
                    HasExpectedDataVersion = true,
                    ExpectedDataVersion = probe.Owner.DataVersion,
                    IndexSchemaFingerprint = schema?.Fingerprint,
                    IndexSchemaProtocolVersion = schema is null
                        ? 0
                        : StorageIndexSchema.ProtocolVersion,
                };
                calls[index] = new OwnedFacetCall<PartitionFacetCountSliceResult>(
                    probe.Owner.Owner,
                    StartFacetCountSlice(probe.Owner.Owner, wire));
            }

            var responses = await WaitForFacetFanoutAsync(calls, cancellationToken);
            for (var index = 0; index < responses.Length; index++)
            {
                var response = responses[index]
                    ?? throw InvalidFacetResponse(index, "returned a null response");
                var probe = active[index];
                ValidateCountSliceResponse(
                    index,
                    response,
                    probe,
                    facet,
                    layout,
                    layoutFingerprint,
                    Math.Min(_queryConfiguration.PartitionWorkBudget, allocation.WorkPerOwner));
                probe.Owner.PinDataVersion(response.DataVersion);
                try
                {
                    globalCount = checked(globalCount + response.CountDelta);
                }
                catch (OverflowException exception)
                {
                    throw new SearchableStorageQueryLimitExceededException(
                        "The exact facet count exceeded the supported count range.",
                        exception);
                }
                probe.Exhausted = response.Exhausted;
                probe.HasAfter = response.HasFrontier;
                probe.After = response.Frontier;
                budget.Record(response.Work, itemCount: 1, itemByteCount: 0);
            }
        }

        return globalCount;
    }

    private Task<PartitionDistinctFacetPageResult> StartDistinctFacetPage(
        int owner,
        RoutedPartitionDistinctFacetPageRequest request)
    {
        try
        {
            return _getPartition(owner).QueryDistinctFacetPageRoutedAsync(request)
                ?? Task.FromException<PartitionDistinctFacetPageResult>(
                    new InvalidOperationException($"Storage owner {owner} returned a null distinct-facet task."));
        }
        catch (Exception exception)
        {
            return Task.FromException<PartitionDistinctFacetPageResult>(exception);
        }
    }

    private Task<PartitionFacetCandidatePageResult> StartFacetCandidatePage(
        int owner,
        RoutedPartitionFacetCandidatePageRequest request)
    {
        try
        {
            return _getPartition(owner).QueryFacetCandidatesRoutedAsync(request)
                ?? Task.FromException<PartitionFacetCandidatePageResult>(
                    new InvalidOperationException($"Storage owner {owner} returned a null facet-candidate task."));
        }
        catch (Exception exception)
        {
            return Task.FromException<PartitionFacetCandidatePageResult>(exception);
        }
    }

    private Task<PartitionFacetCountSliceResult> StartFacetCountSlice(
        int owner,
        RoutedPartitionFacetCountSliceRequest request)
    {
        try
        {
            return _getPartition(owner).QueryFacetCountSliceRoutedAsync(request)
                ?? Task.FromException<PartitionFacetCountSliceResult>(
                    new InvalidOperationException($"Storage owner {owner} returned a null facet-count task."));
        }
        catch (Exception exception)
        {
            return Task.FromException<PartitionFacetCountSliceResult>(exception);
        }
    }

    private async Task<T[]> WaitForFacetFanoutAsync<T>(
        OwnedFacetCall<T>[] calls,
        CancellationToken cancellationToken)
    {
        var aggregate = Task.WhenAll(calls.Select(static call => call.Task));
        try
        {
            return await aggregate.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _observeDetachedFanout(aggregate);
            throw;
        }
        catch
        {
            foreach (var call in calls.OrderBy(static call => call.Owner))
            {
                if (call.Task.Exception is not { } failure)
                {
                    continue;
                }

                var preferred = failure.Flatten().InnerExceptions.FirstOrDefault(
                    static exception => exception is not StorageRouteMismatchException
                        and not StorageFacetDataChangedException);
                if (preferred is not null)
                {
                    ExceptionDispatchInfo.Capture(preferred).Throw();
                }
            }

            var canceled = calls.FirstOrDefault(static call => call.Task.IsCanceled);
            if (canceled is not null)
            {
                _ = await canceled.Task;
            }

            var dataChanged = calls
                .SelectMany(static call => call.Task.Exception?.Flatten().InnerExceptions
                    .OfType<StorageFacetDataChangedException>()
                    .Select(exception => (call.Owner, Exception: exception)) ?? [])
                .OrderBy(static candidate => candidate.Owner)
                .FirstOrDefault();
            if (dataChanged.Exception is not null)
            {
                ExceptionDispatchInfo.Capture(dataChanged.Exception).Throw();
            }

            var mismatch = calls
                .SelectMany(static call => call.Task.Exception?.Flatten().InnerExceptions
                    .OfType<StorageRouteMismatchException>()
                    .Select(exception => (call.Owner, Exception: exception)) ?? [])
                .OrderByDescending(static candidate => candidate.Exception.CurrentEpoch)
                .ThenBy(static candidate => candidate.Owner)
                .FirstOrDefault();
            if (mismatch.Exception is not null)
            {
                ExceptionDispatchInfo.Capture(mismatch.Exception).Throw();
            }

            throw new InvalidOperationException(
                "A partition facet fan-out failed without an observable failure.");
        }
    }

    private static long SumUnseenBounds(IEnumerable<FacetOwnerState> states)
    {
        try
        {
            long total = 0;
            foreach (var state in states)
            {
                total = checked(total + state.UnseenCountUpperBound);
            }

            return total;
        }
        catch (OverflowException exception)
        {
            throw new SearchableStorageQueryLimitExceededException(
                "The certified facet omitted-count bound exceeded the supported count range.",
                exception);
        }
    }

    private static void ValidateDistinctResponse(
        int index,
        PartitionDistinctFacetPageResult response,
        FacetOwnerState state,
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy)
    {
        ValidateFacetResponseMetadata(
            index,
            response.ProtocolVersion,
            response.OrderingVersion,
            response.WorkPolicyVersion,
            response.ResponseFamily,
            PartitionQueryResponseFamily.DistinctFacetValuePage,
            response.Epoch,
            response.RequestFingerprint,
            response.LayoutFormatVersion,
            response.LayoutFingerprint,
            response.DataVersion,
            response.Work,
            state,
            facet,
            layout,
            layoutFingerprint,
            policy.PartitionWorkBudget);
        if (response.Items is null
            || response.Items.Length > policy.PartitionResponseItemLimit
            || response.ItemByteCount < 0
            || response.ItemByteCount > policy.PartitionResponseByteLimit)
        {
            throw InvalidFacetResponse(index, "exceeded the distinct response policy");
        }

        ValidateValuePage(
            index,
            response.Items,
            response.Frontier,
            response.Exhausted,
            response.StopReason,
            state.AfterValue,
            facet.Index.Converter);
        var bytes = response.Items.Sum(IndexValueCanonicalEncoding.GetEncodedLength);
        if (bytes != response.ItemByteCount)
        {
            throw InvalidFacetResponse(index, "reported an incorrect distinct encoded size");
        }
    }

    private static CandidateRawProgress ValidateCandidateResponse(
        int index,
        PartitionFacetCandidatePageResult response,
        FacetOwnerState state,
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        QueryExecutionPolicy policy)
    {
        ValidateFacetResponseMetadata(
            index,
            response.ProtocolVersion,
            response.OrderingVersion,
            response.WorkPolicyVersion,
            response.ResponseFamily,
            PartitionQueryResponseFamily.FacetValueCountCandidates,
            response.Epoch,
            response.RequestFingerprint,
            response.LayoutFormatVersion,
            response.LayoutFingerprint,
            response.DataVersion,
            response.Work,
            state,
            facet,
            layout,
            layoutFingerprint,
            policy.PartitionWorkBudget);
        if (response.Items is null
            || response.Items.Length > policy.PartitionResponseItemLimit
            || response.ItemByteCount < 0
            || response.ItemByteCount > policy.PartitionResponseByteLimit
            || response.PageRawCount < 0
            || response.TotalRawCount < 0)
        {
            throw InvalidFacetResponse(index, "exceeded the candidate response policy");
        }

        var values = new IndexValue[response.Items.Length];
        var bytes = 0;
        long reportedPageRawCount = 0;
        try
        {
            for (var itemIndex = 0; itemIndex < response.Items.Length; itemIndex++)
            {
                var item = response.Items[itemIndex];
                if (item is null || item.Value is null || item.RawCount <= 0)
                {
                    throw InvalidFacetResponse(index, "returned an invalid raw candidate count");
                }

                ValidateCanonicalFacetValue(index, item.Value, facet.Index.Converter);
                values[itemIndex] = item.Value;
                reportedPageRawCount = checked(reportedPageRawCount + item.RawCount);
                bytes = checked(bytes + IndexValueCanonicalEncoding.GetEncodedLength(item.Value) + sizeof(long));
            }
        }
        catch (OverflowException)
        {
            throw InvalidFacetResponse(index, "overflowed its candidate accounting");
        }

        ValidateValuePage(
            index,
            values,
            response.FrontierValue,
            response.Exhausted,
            response.StopReason,
            state.AfterValue,
            facet.Index.Converter);

        long visited;
        try
        {
            visited = checked(state.VisitedRawCount + response.PageRawCount);
        }
        catch (OverflowException)
        {
            throw InvalidFacetResponse(index, "overflowed its cumulative raw-count proof");
        }

        if (reportedPageRawCount != response.PageRawCount
            || (state.HasTotalRawCount && state.TotalRawCount != response.TotalRawCount)
            || visited > response.TotalRawCount
            || (response.Exhausted && visited != response.TotalRawCount)
            || (!response.Exhausted && visited >= response.TotalRawCount))
        {
            throw InvalidFacetResponse(index, "returned an inconsistent raw-count proof");
        }

        if (bytes != response.ItemByteCount)
        {
            throw InvalidFacetResponse(index, "reported an incorrect candidate encoded size");
        }

        return new CandidateRawProgress(
            response.TotalRawCount,
            visited,
            response.TotalRawCount - visited);
    }

    private static void ValidateCountSliceResponse(
        int index,
        PartitionFacetCountSliceResult response,
        FacetProbeState probe,
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        long workBudget)
    {
        ValidateFacetResponseMetadata(
            index,
            response.ProtocolVersion,
            response.OrderingVersion,
            response.WorkPolicyVersion,
            response.ResponseFamily,
            PartitionQueryResponseFamily.FacetValueCountProbe,
            response.Epoch,
            response.RequestFingerprint,
            response.LayoutFormatVersion,
            response.LayoutFingerprint,
            response.DataVersion,
            response.Work,
            probe.Owner,
            facet,
            layout,
            layoutFingerprint,
            workBudget);
        if (response.HasFrontier)
        {
            try
            {
                GrainIdCanonicalOrder.Validate(response.Frontier, nameof(response));
            }
            catch (ArgumentException)
            {
                throw InvalidFacetResponse(index, "returned a malformed count-slice frontier");
            }
        }

        if (response.CountDelta < 0
            || response.CountDelta > response.Work.CountIncrementCount
            || !Enum.IsDefined(response.StopReason)
            || response.Exhausted != (response.StopReason == PartitionQueryPageStopReason.Exhausted)
            || (!response.Exhausted
                && response.StopReason != PartitionQueryPageStopReason.WorkBudget)
            || response.Exhausted == response.HasFrontier
            || (response.HasFrontier && response.Frontier.IsDefault)
            || (response.Exhausted && !response.Frontier.IsDefault)
            || (response.HasFrontier
                && probe.HasAfter
                && GrainIdCanonicalOrder.Compare(response.Frontier, probe.After) <= 0))
        {
            throw InvalidFacetResponse(index, "returned an invalid count-slice frontier");
        }
    }

    private static void ValidateFacetResponseMetadata(
        int index,
        int protocolVersion,
        int orderingVersion,
        int workPolicyVersion,
        PartitionQueryResponseFamily family,
        PartitionQueryResponseFamily expectedFamily,
        long epoch,
        byte[] requestFingerprint,
        int layoutFormatVersion,
        byte[] responseLayoutFingerprint,
        long dataVersion,
        PartitionFacetWork work,
        FacetOwnerState state,
        FacetPlan facet,
        StorageLayoutSnapshot layout,
        byte[] layoutFingerprint,
        long workBudget)
    {
        long totalOperationCount;
        try
        {
            totalOperationCount = work?.TotalOperationCount ?? 0;
        }
        catch (OverflowException)
        {
            throw InvalidFacetResponse(index, "overflowed its facet-work accounting");
        }

        if (protocolVersion != QueryProtocol.PagingVersion
            || orderingVersion != QueryProtocol.FacetValueOrderingVersion
            || workPolicyVersion != QueryProtocol.FacetWorkPolicyVersion
            || family != expectedFamily
            || epoch != layout.Epoch
            || !StorageLayout.AreRoutingFormatsCompatible(layoutFormatVersion, layout.FormatVersion)
            || requestFingerprint is null
            || !QueryPlanFingerprint.Equals(requestFingerprint, facet.Fingerprint)
            || responseLayoutFingerprint is null
            || !StorageLayoutFingerprint.Equals(responseLayoutFingerprint, layoutFingerprint)
            || dataVersion < 0
            || (state.HasDataVersion && dataVersion != state.DataVersion)
            || work is null
            || HasNegativeFacetWork(work)
            || totalOperationCount > workBudget)
        {
            throw InvalidFacetResponse(index, "returned incompatible facet metadata");
        }
    }

    private static void ValidateValuePage(
        int index,
        IndexValue[] items,
        IndexValue? frontier,
        bool exhausted,
        PartitionQueryPageStopReason reason,
        IndexValue? after,
        IndexValueConverter converter)
    {
        if (!Enum.IsDefined(reason)
            || exhausted != (reason == PartitionQueryPageStopReason.Exhausted)
            || exhausted == (frontier is not null)
            || (!exhausted && items.Length == 0))
        {
            throw InvalidFacetResponse(index, "returned an invalid value frontier");
        }

        if (after is not null)
        {
            ValidateCanonicalFacetValue(index, after, converter);
        }

        if (frontier is not null)
        {
            ValidateCanonicalFacetValue(index, frontier, converter);
        }

        IndexValue? previous = null;
        foreach (var item in items)
        {
            if (item is null)
            {
                throw InvalidFacetResponse(index, "returned a null canonical value");
            }

            ValidateCanonicalFacetValue(index, item, converter);
            if ((after is not null && item.CompareTo(after) <= 0)
                || (frontier is not null && item.CompareTo(frontier) > 0)
                || (previous is not null && item.CompareTo(previous) <= 0))
            {
                throw InvalidFacetResponse(index, "returned values outside canonical page order");
            }

            previous = item;
        }

        if (frontier is not null
            && ((after is not null && frontier.CompareTo(after) <= 0)
                || previous is null
                || frontier.CompareTo(previous) != 0))
        {
            throw InvalidFacetResponse(index, "returned a non-progressing or skipped value frontier");
        }
    }

    private static void ValidateCanonicalFacetValue(
        int index,
        IndexValue value,
        IndexValueConverter? converter = null)
    {
        try
        {
            IndexValueCanonicalEncoding.Validate(value, nameof(value));
            _ = IndexValueCanonicalEncoding.GetEncodedLength(value);
            if (converter is not null)
            {
                IndexValueMaterializer.Validate(value, converter);
            }
        }
        catch (Exception exception) when (exception is ArgumentException
            or CanonicalEncodingLimitExceededException
            or InvalidOperationException
            or OverflowException)
        {
            throw InvalidFacetResponse(index, "returned a malformed canonical value");
        }
    }

    private static bool HasNegativeFacetWork(PartitionFacetWork work)
    {
        return work.ValueSeekCount < 0
            || work.ValueVisitCount < 0
            || work.GrainGroupVisitCount < 0
            || work.OwnershipProbeCount < 0
            || work.RecordProbeCount < 0
            || work.PredicateNodeProbeCount < 0
            || work.IndexEntryProbeCount < 0
            || work.CountIncrementCount < 0
            || work.ResultMaterializationCount < 0;
    }

    private static InvalidOperationException InvalidFacetResponse(int index, string reason)
    {
        return new InvalidOperationException($"Storage owner facet response {index} {reason}.");
    }

    private sealed record FacetPlan(
        string StateName,
        PartitionQueryPlan Query,
        SelectedIndex Index,
        byte[] Fingerprint);

    private sealed class FacetOwnerState(int owner)
    {
        public int Owner { get; } = owner;
        public IndexValue? AfterValue { get; set; }
        public bool Exhausted { get; set; }
        public long UnseenCountUpperBound { get; set; }
        public bool HasTotalRawCount => TotalRawCount >= 0;
        public long TotalRawCount { get; set; } = -1;
        public long VisitedRawCount { get; set; }
        public bool HasDataVersion { get; private set; }
        public long DataVersion { get; private set; }

        public void PinDataVersion(long value)
        {
            if (value < 0 || (HasDataVersion && value != DataVersion))
            {
                throw new InvalidOperationException("A facet owner returned an inconsistent data version.");
            }

            HasDataVersion = true;
            DataVersion = value;
        }
    }

    private sealed class FacetProbeState(FacetOwnerState owner)
    {
        public FacetOwnerState Owner { get; } = owner;
        public bool HasAfter { get; set; }
        public Orleans.Runtime.GrainId After { get; set; }
        public bool Exhausted { get; set; }
    }

    private readonly record struct CandidateRawProgress(long Total, long Visited, long Unseen);

    private sealed class FacetAggregateBudget
    {
        private readonly SearchableStorageQueryConfiguration _configuration;
        private long _work;
        private int _items;
        private int _bytes;
        private int _turns;

        public FacetAggregateBudget(SearchableStorageQueryConfiguration configuration)
        {
            _configuration = configuration;
        }

        public FacetTurnAllocation AllocateTurn(
            int ownerCount,
            bool requiresItems,
            bool requiresBytes)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ownerCount);
            if (_turns >= _configuration.FacetRoundLimit)
            {
                throw Limit("round");
            }

            _turns++;
            var remainingWork = _configuration.FacetAggregateWorkLimit - _work;
            var remainingItems = _configuration.FacetAggregateItemLimit - _items;
            var remainingBytes = _configuration.FacetAggregateByteLimit - _bytes;
            var workPerOwner = remainingWork / ownerCount;
            var itemsPerOwner = remainingItems / ownerCount;
            var bytesPerOwner = remainingBytes / ownerCount;
            if (workPerOwner <= 0
                || (requiresItems && itemsPerOwner <= 0)
                || (requiresBytes && bytesPerOwner <= 0))
            {
                throw Limit("remaining fan-out");
            }

            return new FacetTurnAllocation(
                workPerOwner,
                requiresItems ? itemsPerOwner : int.MaxValue,
                requiresBytes ? bytesPerOwner : int.MaxValue);
        }

        public void Record(PartitionFacetWork work, int itemCount, int itemByteCount)
        {
            try
            {
                _work = checked(_work + work.TotalOperationCount);
                _items = checked(_items + itemCount);
                _bytes = checked(_bytes + itemByteCount);
            }
            catch (OverflowException exception)
            {
                throw new SearchableStorageQueryLimitExceededException(
                    "The facet aggregate accounting overflowed.",
                    exception);
            }

            if (_work > _configuration.FacetAggregateWorkLimit)
            {
                throw Limit("logical-work");
            }

            if (_items > _configuration.FacetAggregateItemLimit)
            {
                throw Limit("candidate-item");
            }

            if (_bytes > _configuration.FacetAggregateByteLimit)
            {
                throw Limit("candidate-byte");
            }
        }

        private static SearchableStorageQueryLimitExceededException Limit(string limit)
        {
            return new SearchableStorageQueryLimitExceededException(
                $"The facet exceeded its aggregate {limit} limit.");
        }
    }

    private sealed record OwnedFacetCall<T>(int Owner, Task<T> Task);

    private readonly record struct FacetTurnAllocation(
        long WorkPerOwner,
        int ItemsPerOwner,
        int BytesPerOwner);

    private sealed record DistinctFacetAttemptResult(
        IndexValue[] Items,
        bool HasContinuation,
        IndexValue? ContinuationAfter);

    private sealed record FacetCountItem(IndexValue Value, long Count);

    private sealed record FacetCountAttemptResult(
        FacetCountItem[] Items,
        bool IsExact,
        long MaximumOmittedCount);

    private sealed record FacetMinMaxAttemptResult(IndexValue Minimum, IndexValue Maximum);
}
