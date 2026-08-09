using Orleans.SearchableStorage.Indexing;

namespace Orleans.SearchableStorage.Querying;

internal abstract record QueryPlan;

internal sealed record EmptyQueryPlan : QueryPlan
{
    public static EmptyQueryPlan Instance { get; } = new();
}

internal sealed record ExactQueryPlan(
    SelectedIndex Index,
    IndexValue Value) : QueryPlan;

internal sealed record RangeQueryPlan(
    SelectedIndex Index,
    IndexValue? LowerBound,
    bool IncludeLowerBound,
    IndexValue? UpperBound,
    bool IncludeUpperBound) : QueryPlan;

internal sealed record AndQueryPlan(
    QueryPlan Left,
    QueryPlan Right) : QueryPlan;

internal sealed record OrQueryPlan(
    QueryPlan Left,
    QueryPlan Right) : QueryPlan;
