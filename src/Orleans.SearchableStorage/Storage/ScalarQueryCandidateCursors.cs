using Orleans.Runtime;
using Orleans.SearchableStorage.Querying;

namespace Orleans.SearchableStorage.Storage;

internal static partial class ScalarQueryAccessPathPlanner
{
    private sealed class FinalCandidateCursor(IRawCandidateCursor source)
        : IOrderedCandidateCursor
    {
        public bool HasCandidate => source.HasCandidate;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            var result = source.TryTakeNext(ref work, out candidate);
            if (result != CandidateTakeResult.Candidate)
            {
                return result;
            }

            if (!work.TryRecordOrderedCandidateVisit())
            {
                candidate = default;
                return CandidateTakeResult.WorkBudget;
            }

            return CandidateTakeResult.Candidate;
        }

        public void Dispose() => source.Dispose();
    }

    private sealed class PostingRawCandidateCursor(OrderedGrainGroupCursor cursor)
        : IRawCandidateCursor
    {
        public bool HasCandidate => cursor.HasCurrent;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            if (!cursor.HasCurrent)
            {
                candidate = default;
                return CandidateTakeResult.Exhausted;
            }

            if (!work.TryRecordPostingCandidateVisit())
            {
                candidate = default;
                return CandidateTakeResult.WorkBudget;
            }

            if (!cursor.TakeCurrentAndAdvance(out candidate))
            {
                throw new InvalidOperationException(
                    "An ordered posting cursor lost its prefetched candidate.");
            }

            return CandidateTakeResult.Candidate;
        }

        public void Dispose() => cursor.Dispose();
    }

    private sealed class CatalogRawCandidateCursor(OrderedGrainGroupCursor cursor)
        : IRawCandidateCursor
    {
        public bool HasCandidate => cursor.HasCurrent;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            if (!cursor.HasCurrent)
            {
                candidate = default;
                return CandidateTakeResult.Exhausted;
            }

            if (!work.TryRecordCatalogCandidateVisit())
            {
                candidate = default;
                return CandidateTakeResult.WorkBudget;
            }

            if (!cursor.TakeCurrentAndAdvance(out candidate))
            {
                throw new InvalidOperationException(
                    "An ordered catalog cursor lost its prefetched candidate.");
            }

            return CandidateTakeResult.Candidate;
        }

        public void Dispose() => cursor.Dispose();
    }

    private sealed class RangeMergeRawCandidateCursor(
        RangeMergeHeap heap,
        IReadOnlyList<OrderedGrainGroupCursor> postingCursors)
        : IRawCandidateCursor
    {
        public bool HasCandidate => heap.Count > 0;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            candidate = default;
            if (heap.Count == 0)
            {
                return CandidateTakeResult.Exhausted;
            }

            if (!heap.TryDequeue(ref work, out var node))
            {
                return CandidateTakeResult.WorkBudget;
            }

            var grouped = node.GrainId;
            if (!TryLoadAndEnqueue(node.Posting, node.SourceOrdinal, heap, ref work))
            {
                return CandidateTakeResult.WorkBudget;
            }

            while (heap.Count > 0)
            {
                if (!heap.TryCompareRootTo(grouped, ref work, out var comparison))
                {
                    return CandidateTakeResult.WorkBudget;
                }

                if (comparison != 0)
                {
                    break;
                }

                if (!heap.TryDequeue(ref work, out node)
                    || !TryLoadAndEnqueue(node.Posting, node.SourceOrdinal, heap, ref work))
                {
                    return CandidateTakeResult.WorkBudget;
                }
            }

            candidate = grouped;
            return CandidateTakeResult.Candidate;
        }

        public static bool TryLoadAndEnqueue(
            OrderedGrainGroupCursor posting,
            int sourceOrdinal,
            RangeMergeHeap heap,
            ref PageWorkAccumulator work)
        {
            if (!posting.HasCurrent)
            {
                return true;
            }

            // The append-only posting charge and the legacy range-occurrence charge both precede
            // advancing the posting, preserving the meaning of the v1 work component.
            if (!work.TryRecordPostingCandidateVisit()
                || !work.TryRecordRangeMergeOperation()
                || !posting.TakeCurrentAndAdvance(out var grainId))
            {
                return false;
            }

            return heap.TryEnqueue(
                new RangeMergeNode(grainId, sourceOrdinal, posting),
                ref work);
        }

        public void Dispose()
        {
            foreach (var posting in postingCursors)
            {
                posting.Dispose();
            }
        }
    }

    private sealed class UnionRawCandidateCursor(
        IRawCandidateCursor first,
        IRawCandidateCursor second)
        : IRawCandidateCursor
    {
        private bool _hasFirst;
        private GrainId _first;
        private bool _hasSecond;
        private GrainId _second;

        public bool HasCandidate => _hasFirst || _hasSecond || first.HasCandidate || second.HasCandidate;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            candidate = default;
            var firstResult = TryFill(first, ref _hasFirst, ref _first, ref work);
            if (firstResult == CandidateTakeResult.WorkBudget)
            {
                return firstResult;
            }

            var secondResult = TryFill(second, ref _hasSecond, ref _second, ref work);
            if (secondResult == CandidateTakeResult.WorkBudget)
            {
                return secondResult;
            }

            if (!_hasFirst && !_hasSecond)
            {
                return CandidateTakeResult.Exhausted;
            }

            if (!_hasFirst)
            {
                candidate = _second;
                _hasSecond = false;
                return CandidateTakeResult.Candidate;
            }

            if (!_hasSecond)
            {
                candidate = _first;
                _hasFirst = false;
                return CandidateTakeResult.Candidate;
            }

            if (!work.TryRecordUnionOperation())
            {
                return CandidateTakeResult.WorkBudget;
            }

            var comparison = GrainIdCanonicalOrder.Compare(_first, _second);
            if (comparison <= 0)
            {
                candidate = _first;
                _hasFirst = false;
                if (comparison == 0)
                {
                    _hasSecond = false;
                }
            }
            else
            {
                candidate = _second;
                _hasSecond = false;
            }

            return CandidateTakeResult.Candidate;
        }

        private static CandidateTakeResult TryFill(
            IRawCandidateCursor source,
            ref bool hasCandidate,
            ref GrainId candidate,
            ref PageWorkAccumulator work)
        {
            if (hasCandidate || !source.HasCandidate)
            {
                return hasCandidate
                    ? CandidateTakeResult.Candidate
                    : CandidateTakeResult.Exhausted;
            }

            if (!work.TryRecordUnionOperation())
            {
                return CandidateTakeResult.WorkBudget;
            }

            var result = source.TryTakeNext(ref work, out candidate);
            if (result == CandidateTakeResult.Candidate)
            {
                hasCandidate = true;
            }

            return result;
        }

        public void Dispose()
        {
            first.Dispose();
            second.Dispose();
        }
    }

    private sealed class EmptyRawCandidateCursor : IRawCandidateCursor
    {
        public static EmptyRawCandidateCursor Instance { get; } = new();

        public bool HasCandidate => false;

        public CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate)
        {
            candidate = default;
            return CandidateTakeResult.Exhausted;
        }

        public void Dispose()
        {
        }
    }

    private interface IRawCandidateCursor : IDisposable
    {
        bool HasCandidate { get; }

        CandidateTakeResult TryTakeNext(
            ref PageWorkAccumulator work,
            out GrainId candidate);
    }

    private sealed class RangeMergeHeap
    {
        private readonly List<RangeMergeNode> _nodes = [];

        public int Count => _nodes.Count;

        public bool TryEnqueue(RangeMergeNode node, ref PageWorkAccumulator work)
        {
            if (!work.TryRecordHeapOperation())
            {
                return false;
            }

            _nodes.Add(node);
            var child = _nodes.Count - 1;
            while (child > 0)
            {
                var parent = (child - 1) / 2;
                if (!TryCompareNodes(child, parent, ref work, out var comparison))
                {
                    return false;
                }

                if (comparison >= 0)
                {
                    break;
                }

                if (!work.TryRecordHeapOperation())
                {
                    return false;
                }

                (_nodes[parent], _nodes[child]) = (_nodes[child], _nodes[parent]);
                child = parent;
            }

            return true;
        }

        public bool TryDequeue(
            ref PageWorkAccumulator work,
            out RangeMergeNode node)
        {
            if (_nodes.Count == 0)
            {
                node = default;
                return false;
            }

            if (!work.TryRecordHeapOperation())
            {
                node = default;
                return false;
            }

            node = _nodes[0];
            var lastIndex = _nodes.Count - 1;
            var last = _nodes[lastIndex];
            _nodes.RemoveAt(lastIndex);
            if (_nodes.Count == 0)
            {
                return true;
            }

            if (!work.TryRecordHeapOperation())
            {
                return false;
            }

            _nodes[0] = last;
            var parent = 0;
            while (true)
            {
                var left = checked((parent * 2) + 1);
                if (left >= _nodes.Count)
                {
                    return true;
                }

                var selected = left;
                var right = left + 1;
                if (right < _nodes.Count)
                {
                    if (!TryCompareNodes(
                            right,
                            left,
                            ref work,
                            out var childComparison))
                    {
                        return false;
                    }

                    if (childComparison < 0)
                    {
                        selected = right;
                    }
                }

                if (!TryCompareNodes(
                        selected,
                        parent,
                        ref work,
                        out var parentComparison))
                {
                    return false;
                }

                if (parentComparison >= 0)
                {
                    return true;
                }

                if (!work.TryRecordHeapOperation())
                {
                    return false;
                }

                (_nodes[parent], _nodes[selected]) = (_nodes[selected], _nodes[parent]);
                parent = selected;
            }
        }

        public bool TryCompareRootTo(
            GrainId grainId,
            ref PageWorkAccumulator work,
            out int comparison)
        {
            if (_nodes.Count == 0)
            {
                comparison = 1;
                return true;
            }

            if (!work.TryRecordRangeMergeOperation())
            {
                comparison = 0;
                return false;
            }

            comparison = GrainIdCanonicalOrder.Compare(_nodes[0].GrainId, grainId);
            return true;
        }

        private bool TryCompareNodes(
            int leftIndex,
            int rightIndex,
            ref PageWorkAccumulator work,
            out int comparison)
        {
            if (!work.TryRecordRangeMergeOperation())
            {
                comparison = 0;
                return false;
            }

            var left = _nodes[leftIndex];
            var right = _nodes[rightIndex];
            comparison = GrainIdCanonicalOrder.Compare(left.GrainId, right.GrainId);
            if (comparison == 0)
            {
                comparison = left.SourceOrdinal.CompareTo(right.SourceOrdinal);
            }

            return true;
        }
    }

    private readonly record struct RangeMergeNode(
        GrainId GrainId,
        int SourceOrdinal,
        OrderedGrainGroupCursor Posting);

    private static PartitionQueryBudgetTooSmallException CreateWorkBudgetTooSmall(
        RoutedPartitionQueryPageRequest request,
        PageWorkAccumulator work)
    {
        return new PartitionQueryBudgetTooSmallException(
            request.WorkBudget,
            checked(work.TotalOperationCount + 1),
            PartitionQueryPageStopReason.WorkBudget);
    }
}
