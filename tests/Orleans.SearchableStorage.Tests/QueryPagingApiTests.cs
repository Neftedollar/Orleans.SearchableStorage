using System.Collections;
using System.Linq.Expressions;
using AwesomeAssertions;
using Orleans.Runtime;

namespace Orleans.SearchableStorage.Tests;

public sealed class QueryPagingApiTests
{
    [Fact]
    public async Task ExternalPagedProviderCanOptIntoThePageTerminal()
    {
        var provider = new ExternalPagedProvider<QueryState>();
        var query = new ExternalQuery<QueryState>(provider).Where(state => state.Value == 7);
        var request = new SearchableStorageQueryPageRequest(13, "continuation");
        using var cancellation = new CancellationTokenSource();

        var page = await query.ToGrainIdPageAsync(request, cancellation.Token);

        page.Items.Should().BeEmpty();
        provider.Expression.Should().BeSameAs(query.Expression);
        provider.Request.Should().BeSameAs(request);
        provider.CancellationToken.Should().Be(cancellation.Token);
    }

    [Fact]
    public void ExistingAsyncProviderDoesNotImplicitlyOptIntoPaging()
    {
        var query = new ExternalQuery<QueryState>(new LegacyOnlyProvider<QueryState>());

        Action execute = () => _ = query.ToGrainIdPageAsync(
            new SearchableStorageQueryPageRequest(1));

        execute.Should().Throw<NotSupportedException>()
            .WithMessage("*ISearchableStoragePagedQueryProvider*");
    }

    [Fact]
    public void PageDefensivelyCopiesItems()
    {
        var original = new List<GrainId> { GrainId.Create("paging", "first") };
        var page = new SearchableStorageQueryPage(original, "next");

        original[0] = GrainId.Create("paging", "changed");
        original.Add(GrainId.Create("paging", "second"));

        page.Items.Should().ContainSingle()
            .Which.Should().Be(GrainId.Create("paging", "first"));
        page.ContinuationToken.Should().Be("next");
        Action mutate = () => ((IList<GrainId>)page.Items).Add(GrainId.Create("paging", "third"));
        mutate.Should().Throw<NotSupportedException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void PageRequestRequiresAPositiveSize(int pageSize)
    {
        Action create = () => _ = new SearchableStorageQueryPageRequest(pageSize);

        create.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void PageRequestRejectsAnEmptyContinuation(string continuation)
    {
        Action create = () => _ = new SearchableStorageQueryPageRequest(1, continuation);

        create.Should().Throw<ArgumentException>();
    }

    private sealed class QueryState
    {
        public int Value { get; init; }
    }

    private sealed class ExternalPagedProvider<TState> : IQueryProvider, ISearchableStoragePagedQueryProvider
    {
        public Expression? Expression { get; private set; }

        public SearchableStorageQueryPageRequest? Request { get; private set; }

        public CancellationToken CancellationToken { get; private set; }

        public IQueryable CreateQuery(Expression expression) => new ExternalQuery<TState>(this, expression);

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (typeof(TElement) != typeof(TState))
            {
                throw new NotSupportedException();
            }

            return (IQueryable<TElement>)(object)new ExternalQuery<TState>(this, expression);
        }

        public object? Execute(Expression expression) => throw new NotSupportedException();

        public TResult Execute<TResult>(Expression expression) => throw new NotSupportedException();

        public Task<SearchableStorageQueryPage> ExecuteToGrainIdPageAsync(
            Expression expression,
            SearchableStorageQueryPageRequest request,
            CancellationToken cancellationToken)
        {
            Expression = expression;
            Request = request;
            CancellationToken = cancellationToken;
            return Task.FromResult(new SearchableStorageQueryPage([], continuationToken: null));
        }
    }

    private sealed class LegacyOnlyProvider<TState> : IQueryProvider, ISearchableStorageAsyncQueryProvider
    {
        public IQueryable CreateQuery(Expression expression) => new ExternalQuery<TState>(this, expression);

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (typeof(TElement) != typeof(TState))
            {
                throw new NotSupportedException();
            }

            return (IQueryable<TElement>)(object)new ExternalQuery<TState>(this, expression);
        }

        public object? Execute(Expression expression) => throw new NotSupportedException();

        public TResult Execute<TResult>(Expression expression) => throw new NotSupportedException();

        public Task<IReadOnlyList<GrainId>> ExecuteToGrainIdsAsync(
            Expression expression,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IReadOnlyList<GrainId>>([]);
        }
    }

    private sealed class ExternalQuery<TState> : IOrderedQueryable<TState>
    {
        public ExternalQuery(IQueryProvider provider)
        {
            Provider = provider;
            Expression = System.Linq.Expressions.Expression.Constant(this);
        }

        public ExternalQuery(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public Type ElementType => typeof(TState);

        public Expression Expression { get; }

        public IQueryProvider Provider { get; }

        public IEnumerator<TState> GetEnumerator() => throw new NotSupportedException();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
