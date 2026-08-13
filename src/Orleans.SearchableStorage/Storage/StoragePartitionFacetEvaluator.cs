using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using System.Text;

namespace Orleans.SearchableStorage.Storage;

/// <summary>Bounded, stateless facet primitives over one serially consistent partition view.</summary>
internal static class StoragePartitionFacetEvaluator
{
    public static PartitionDistinctFacetPageResult EvaluateDistinctPageValidated(
        RoutedPartitionDistinctFacetPageRequest request,
        StoragePartitionView view,
        StorageLayoutSnapshot routing,
        byte[] requestFingerprint,
        byte[] layoutFingerprint)
    {
        var work = new FacetWorkAccumulator(request.WorkBudget);
        if (!work.TryRecordValueSeek())
        {
            throw WorkTooSmall(request.WorkBudget, work);
        }

        using var cursor = view.OrderedIndexes.CreateFacetValueCursor(
            request.FacetScope,
            request.FacetKind,
            request.After);
        var items = new List<IndexValue>(request.ItemLimit);
        IndexValue? frontier = null;
        var itemBytes = 0;
        while (cursor.HasCurrent)
        {
            if (items.Count == request.ItemLimit)
            {
                return Stop(PartitionQueryPageStopReason.ItemLimit);
            }

            if (!work.TryRecordValueVisit()
                || !cursor.TakeCurrentAndAdvance(out var bucket))
            {
                return StopForWork();
            }

            var encoded = GetFacetEncodedLength(bucket.Value);
            if (encoded > request.ByteLimit)
            {
                throw new PartitionQueryBudgetTooSmallException(
                    request.ByteLimit,
                    encoded,
                    PartitionQueryPageStopReason.ByteLimit);
            }

            if (encoded > request.ByteLimit - itemBytes)
            {
                return Stop(PartitionQueryPageStopReason.ByteLimit);
            }

            if (!work.TryRecordResultMaterialization())
            {
                return StopForWork();
            }

            items.Add(bucket.Value);
            itemBytes = checked(itemBytes + encoded);
            frontier = bucket.Value;
        }

        return CreateDistinctResult(
            request,
            routing,
            requestFingerprint,
            layoutFingerprint,
            [.. items],
            frontier: null,
            exhausted: true,
            PartitionQueryPageStopReason.Exhausted,
            itemBytes,
            work.Snapshot);

        PartitionDistinctFacetPageResult StopForWork()
        {
            if (frontier is null)
            {
                throw WorkTooSmall(request.WorkBudget, work);
            }

            return Stop(PartitionQueryPageStopReason.WorkBudget);
        }

        PartitionDistinctFacetPageResult Stop(PartitionQueryPageStopReason reason)
        {
            if (frontier is null)
            {
                throw new PartitionQueryBudgetTooSmallException(
                    reason == PartitionQueryPageStopReason.ByteLimit
                        ? request.ByteLimit
                        : request.WorkBudget,
                    reason == PartitionQueryPageStopReason.ByteLimit
                        ? 1
                        : checked(work.TotalOperationCount + 1),
                    reason);
            }

            return CreateDistinctResult(
                request,
                routing,
                requestFingerprint,
                layoutFingerprint,
                [.. items],
                frontier,
                exhausted: false,
                reason,
                itemBytes,
                work.Snapshot);
        }
    }

    public static PartitionFacetCandidatePageResult EvaluateCandidatePageValidated(
        RoutedPartitionFacetCandidatePageRequest request,
        StoragePartitionView view,
        StorageLayoutSnapshot routing,
        byte[] requestFingerprint,
        byte[] layoutFingerprint)
    {
        var work = new FacetWorkAccumulator(request.WorkBudget);
        if (!work.TryRecordValueSeek())
        {
            throw WorkTooSmall(request.WorkBudget, work);
        }

        using var cursor = view.OrderedIndexes.CreateFacetValueCursor(
            request.FacetScope,
            request.FacetKind,
            request.AfterValue);
        var items = new List<PartitionFacetCandidate>(request.ItemLimit);
        IndexValue? frontier = null;
        var itemBytes = 0;
        long pageRawCount = 0;
        var totalRawCount = view.OrderedIndexes.GetFacetRecordCount(
            request.FacetScope,
            request.FacetKind);
        while (cursor.HasCurrent)
        {
            if (items.Count == request.ItemLimit)
            {
                return Stop(PartitionQueryPageStopReason.ItemLimit);
            }

            if (!work.TryRecordValueVisit()
                || !cursor.TakeCurrentAndAdvance(out var bucket))
            {
                return StopForWork();
            }

            var encoded = checked(
                GetFacetEncodedLength(bucket.Value) + sizeof(long));
            if (encoded > request.ByteLimit)
            {
                throw new PartitionQueryBudgetTooSmallException(
                    request.ByteLimit,
                    encoded,
                    PartitionQueryPageStopReason.ByteLimit);
            }

            if (encoded > request.ByteLimit - itemBytes)
            {
                return Stop(PartitionQueryPageStopReason.ByteLimit);
            }

            if (!work.TryRecordResultMaterialization())
            {
                return StopForWork();
            }

            var rawCount = bucket.Posting.RecordCount;
            items.Add(new PartitionFacetCandidate { Value = bucket.Value, RawCount = rawCount });
            pageRawCount = checked(pageRawCount + rawCount);
            itemBytes = checked(itemBytes + encoded);
            frontier = bucket.Value;
        }

        return CreateCandidateResult(
            request,
            routing,
            requestFingerprint,
            layoutFingerprint,
            [.. items],
            frontier: null,
            exhausted: true,
            pageRawCount,
            totalRawCount,
            PartitionQueryPageStopReason.Exhausted,
            itemBytes,
            work.Snapshot);

        PartitionFacetCandidatePageResult StopForWork()
        {
            if (frontier is null)
            {
                throw WorkTooSmall(request.WorkBudget, work);
            }

            return Stop(PartitionQueryPageStopReason.WorkBudget);
        }

        PartitionFacetCandidatePageResult Stop(PartitionQueryPageStopReason reason)
        {
            if (frontier is null)
            {
                throw new PartitionQueryBudgetTooSmallException(
                    reason == PartitionQueryPageStopReason.ByteLimit
                        ? request.ByteLimit
                        : request.WorkBudget,
                    reason == PartitionQueryPageStopReason.ByteLimit
                        ? 1
                        : checked(work.TotalOperationCount + 1),
                    reason);
            }

            return CreateCandidateResult(
                request,
                routing,
                requestFingerprint,
                layoutFingerprint,
                [.. items],
                frontier,
                exhausted: false,
                pageRawCount,
                totalRawCount,
                reason,
                itemBytes,
                work.Snapshot);
        }
    }

    public static PartitionFacetCountSliceResult EvaluateCountSliceValidated(
        RoutedPartitionFacetCountSliceRequest request,
        StoragePartitionView view,
        StorageLayoutSnapshot routing,
        int partitionIndex,
        byte[] requestFingerprint,
        byte[] layoutFingerprint)
    {
        var work = new FacetWorkAccumulator(request.WorkBudget);
        if (!work.TryRecordValueSeek())
        {
            throw WorkTooSmall(request.WorkBudget, work);
        }

        var posting = view.OrderedIndexes.GetExactPosting(
            request.FacetScope,
            request.FacetKind,
            request.Value);
        using var groups = posting.CreateCursorAfter(request.HasAfter, request.After);
        if (!groups.HasCurrent)
        {
            return CreateCountResult(
                request,
                routing,
                requestFingerprint,
                layoutFingerprint,
                countDelta: 0,
                hasFrontier: false,
                frontier: default,
                exhausted: true,
                PartitionQueryPageStopReason.Exhausted,
                work.Snapshot);
        }

        long committedDelta = 0;
        var hasFrontier = false;
        var frontier = default(Orleans.Runtime.GrainId);
        while (groups.HasCurrent)
        {
            if (!work.TryRecordGrainGroupVisit()
                || !groups.TakeCurrentAndAdvance(out var grainId))
            {
                return StopForWork();
            }

            if (!work.TryRecordOwnershipProbe())
            {
                return StopForWork();
            }

            long groupDelta = 0;
            var slot = StorageLayout.GetSlot(grainId, routing.VirtualSlotCount);
            if (routing.GetOwner(slot) == partitionIndex
                && posting.TryGetRecordRefs(grainId, out var recordRefs))
            {
                foreach (var recordRef in recordRefs)
                {
                    if (!work.TryRecordRecordProbe())
                    {
                        return StopForWork();
                    }

                    var record = view.RecordRefs.GetRecord(recordRef);

                    var predicate = EvaluateRecord(request.Query, record, ref work);
                    if (!predicate.Completed)
                    {
                        return StopForWork();
                    }

                    if (predicate.Matches)
                    {
                        if (!work.TryRecordCountIncrement())
                        {
                            return StopForWork();
                        }

                        groupDelta = checked(groupDelta + 1);
                    }
                }
            }

            committedDelta = checked(committedDelta + groupDelta);
            frontier = grainId;
            hasFrontier = true;
        }

        return CreateCountResult(
            request,
            routing,
            requestFingerprint,
            layoutFingerprint,
            committedDelta,
            hasFrontier: false,
            frontier: default,
            exhausted: true,
            PartitionQueryPageStopReason.Exhausted,
            work.Snapshot);

        PartitionFacetCountSliceResult StopForWork()
        {
            if (!hasFrontier)
            {
                throw WorkTooSmall(request.WorkBudget, work);
            }

            return CreateCountResult(
                request,
                routing,
                requestFingerprint,
                layoutFingerprint,
                committedDelta,
                hasFrontier: true,
                frontier,
                exhausted: false,
                PartitionQueryPageStopReason.WorkBudget,
                work.Snapshot);
        }
    }

    private static PredicateEvaluation EvaluateRecord(
        PartitionQueryPlan query,
        StoredRecord record,
        ref FacetWorkAccumulator work)
    {
        if (!work.TryRecordPredicateNodeProbe())
        {
            return PredicateEvaluation.Incomplete;
        }

        switch (query.Operation)
        {
            case PartitionQueryOperation.All:
                return PredicateEvaluation.Match;
            case PartitionQueryOperation.Empty:
                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.Exact:
                foreach (var entry in record.IndexEntries)
                {
                    if (!work.TryRecordIndexEntryProbe())
                    {
                        return PredicateEvaluation.Incomplete;
                    }

                    if (entry.Kind == query.IndexKind
                        && string.Equals(entry.Scope, query.Scope, StringComparison.Ordinal)
                        && entry.Value.Equals(query.Value))
                    {
                        return PredicateEvaluation.Match;
                    }
                }

                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.Range:
                foreach (var entry in record.IndexEntries)
                {
                    if (!work.TryRecordIndexEntryProbe())
                    {
                        return PredicateEvaluation.Incomplete;
                    }

                    if (entry.Kind == SearchableIndexKind.Range
                        && string.Equals(entry.Scope, query.Scope, StringComparison.Ordinal)
                        && IsWithinRange(entry.Value, query))
                    {
                        return PredicateEvaluation.Match;
                    }
                }

                return PredicateEvaluation.NoMatch;
            case PartitionQueryOperation.And:
            {
                var left = EvaluateRecord(query.Left!, record, ref work);
                return !left.Completed || !left.Matches
                    ? left
                    : EvaluateRecord(query.Right!, record, ref work);
            }
            case PartitionQueryOperation.Or:
            {
                var left = EvaluateRecord(query.Left!, record, ref work);
                return !left.Completed || left.Matches
                    ? left
                    : EvaluateRecord(query.Right!, record, ref work);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(query), query.Operation, "Unknown query operation.");
        }
    }

    private static bool IsWithinRange(IndexValue value, PartitionQueryPlan query)
    {
        if (query.LowerBound is not null)
        {
            var comparison = value.CompareTo(query.LowerBound);
            if (comparison < 0 || (comparison == 0 && !query.IncludeLowerBound))
            {
                return false;
            }
        }

        if (query.UpperBound is not null)
        {
            var comparison = value.CompareTo(query.UpperBound);
            if (comparison > 0 || (comparison == 0 && !query.IncludeUpperBound))
            {
                return false;
            }
        }

        return true;
    }

    private static PartitionDistinctFacetPageResult CreateDistinctResult(
        RoutedPartitionDistinctFacetPageRequest request,
        StorageLayoutSnapshot routing,
        byte[] requestFingerprint,
        byte[] layoutFingerprint,
        IndexValue[] items,
        IndexValue? frontier,
        bool exhausted,
        PartitionQueryPageStopReason reason,
        int itemBytes,
        PartitionFacetWork work)
    {
        return new PartitionDistinctFacetPageResult
        {
            Items = items,
            Frontier = frontier,
            Exhausted = exhausted,
            StopReason = reason,
            Work = work,
            ItemByteCount = itemBytes,
            ProtocolVersion = request.ProtocolVersion,
            OrderingVersion = request.OrderingVersion,
            WorkPolicyVersion = request.WorkPolicyVersion,
            ResponseFamily = request.ResponseFamily,
            Epoch = routing.Epoch,
            RequestFingerprint = [.. requestFingerprint],
            LayoutFormatVersion = routing.FormatVersion,
            LayoutFingerprint = [.. layoutFingerprint],
        };
    }

    private static PartitionFacetCandidatePageResult CreateCandidateResult(
        RoutedPartitionFacetCandidatePageRequest request,
        StorageLayoutSnapshot routing,
        byte[] requestFingerprint,
        byte[] layoutFingerprint,
        PartitionFacetCandidate[] items,
        IndexValue? frontier,
        bool exhausted,
        long pageRawCount,
        long totalRawCount,
        PartitionQueryPageStopReason reason,
        int itemBytes,
        PartitionFacetWork work)
    {
        return new PartitionFacetCandidatePageResult
        {
            Items = items,
            FrontierValue = frontier,
            Exhausted = exhausted,
            PageRawCount = pageRawCount,
            TotalRawCount = totalRawCount,
            StopReason = reason,
            Work = work,
            ItemByteCount = itemBytes,
            ProtocolVersion = request.ProtocolVersion,
            OrderingVersion = request.OrderingVersion,
            WorkPolicyVersion = request.WorkPolicyVersion,
            ResponseFamily = request.ResponseFamily,
            Epoch = routing.Epoch,
            RequestFingerprint = [.. requestFingerprint],
            LayoutFormatVersion = routing.FormatVersion,
            LayoutFingerprint = [.. layoutFingerprint],
        };
    }

    private static PartitionFacetCountSliceResult CreateCountResult(
        RoutedPartitionFacetCountSliceRequest request,
        StorageLayoutSnapshot routing,
        byte[] requestFingerprint,
        byte[] layoutFingerprint,
        long countDelta,
        bool hasFrontier,
        Orleans.Runtime.GrainId frontier,
        bool exhausted,
        PartitionQueryPageStopReason reason,
        PartitionFacetWork work)
    {
        return new PartitionFacetCountSliceResult
        {
            CountDelta = countDelta,
            HasFrontier = hasFrontier,
            Frontier = frontier,
            Exhausted = exhausted,
            StopReason = reason,
            Work = work,
            ProtocolVersion = request.ProtocolVersion,
            OrderingVersion = request.OrderingVersion,
            WorkPolicyVersion = request.WorkPolicyVersion,
            ResponseFamily = request.ResponseFamily,
            Epoch = routing.Epoch,
            RequestFingerprint = [.. requestFingerprint],
            LayoutFormatVersion = routing.FormatVersion,
            LayoutFingerprint = [.. layoutFingerprint],
        };
    }

    private static PartitionQueryBudgetTooSmallException WorkTooSmall(
        long budget,
        FacetWorkAccumulator work)
    {
        return new PartitionQueryBudgetTooSmallException(
            budget,
            checked(work.TotalOperationCount + 1),
            PartitionQueryPageStopReason.WorkBudget);
    }

    private static int GetFacetEncodedLength(IndexValue value)
    {
        try
        {
            return IndexValueCanonicalEncoding.GetEncodedLength(value);
        }
        catch (Exception exception) when (exception is EncoderFallbackException
            or CanonicalEncodingLimitExceededException
            or ArgumentException
            or InvalidOperationException
            or OverflowException)
        {
            throw new StorageFacetValueUnsupportedException(exception);
        }
    }

    private readonly record struct PredicateEvaluation(bool Completed, bool Matches)
    {
        public static PredicateEvaluation Incomplete => new(false, false);
        public static PredicateEvaluation NoMatch => new(true, false);
        public static PredicateEvaluation Match => new(true, true);
    }

    private struct FacetWorkAccumulator(long budget)
    {
        private long _total;
        private long _valueSeeks;
        private long _valueVisits;
        private long _grainGroups;
        private long _ownership;
        private long _records;
        private long _predicateNodes;
        private long _indexEntries;
        private long _countIncrements;
        private long _materializations;

        public readonly long TotalOperationCount => _total;

        public readonly PartitionFacetWork Snapshot => new()
        {
            ValueSeekCount = _valueSeeks,
            ValueVisitCount = _valueVisits,
            GrainGroupVisitCount = _grainGroups,
            OwnershipProbeCount = _ownership,
            RecordProbeCount = _records,
            PredicateNodeProbeCount = _predicateNodes,
            IndexEntryProbeCount = _indexEntries,
            CountIncrementCount = _countIncrements,
            ResultMaterializationCount = _materializations,
        };

        public bool TryRecordValueSeek() => Record(ref _valueSeeks);
        public bool TryRecordValueVisit() => Record(ref _valueVisits);
        public bool TryRecordGrainGroupVisit() => Record(ref _grainGroups);
        public bool TryRecordOwnershipProbe() => Record(ref _ownership);
        public bool TryRecordRecordProbe() => Record(ref _records);
        public bool TryRecordPredicateNodeProbe() => Record(ref _predicateNodes);
        public bool TryRecordIndexEntryProbe() => Record(ref _indexEntries);
        public bool TryRecordCountIncrement() => Record(ref _countIncrements);
        public bool TryRecordResultMaterialization() => Record(ref _materializations);

        private bool Record(ref long component)
        {
            if (_total >= budget)
            {
                return false;
            }

            component = checked(component + 1);
            _total = checked(_total + 1);
            return true;
        }
    }
}
