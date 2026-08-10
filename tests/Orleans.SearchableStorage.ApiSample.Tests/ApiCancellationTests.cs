using System.Collections;
using System.Linq.Expressions;
using System.Threading.Channels;
using AwesomeAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.SearchableStorage.ApiSample.Tests;

[Collection(ApiSampleTestGroup.Name)]
public sealed class ApiCancellationTests : IClassFixture<CancellationWebApplicationFactory>
{
    private readonly CancellationWebApplicationFactory _factory;
    private readonly HttpClient _client;

    public ApiCancellationTests(CancellationWebApplicationFactory factory)
    {
        _factory = factory;
        _client = factory.CreateClient();
    }

    [Theory]
    [InlineData("/vacancies/search/by-city?city=Helsinki")]
    [InlineData("/vacancies/search/by-salary?lower=5&upper=8")]
    public async Task HttpRequestCancellationReachesBlockedQueryClient(string path)
    {
        using var cancellation = new CancellationTokenSource();

        var request = _client.GetAsync(path, cancellation.Token);
        var invocation = await _factory.QueryClient.NextInvocationAsync().AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();

        Func<Task> waitForResponse = async () => await request;
        await waitForResponse.Should().ThrowAsync<OperationCanceledException>();
        await invocation.Canceled.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }
}

public sealed class CancellationWebApplicationFactory : WebApplicationFactory<Program>
{
    public BlockingSearchQueryClient QueryClient { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureTestServices(services =>
        {
            services.AddKeyedSingleton<ISearchableStorageQueryClient>(
                VacancyGrain.StorageProviderName,
                (_, _) => QueryClient);
        });
    }
}

public sealed class BlockingSearchQueryClient : ISearchableStorageQueryClient
{
    private readonly Channel<BlockedInvocation> _invocations =
        Channel.CreateUnbounded<BlockedInvocation>();

    public IQueryable<TState> Query<TState>(string stateName)
    {
        return new BlockingQuery<TState>(new BlockingQueryProvider(this));
    }

    public Task<IReadOnlyList<GrainId>> FindAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue value,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<IReadOnlyList<GrainId>> RangeAsync<TState, TValue>(
        string stateName,
        Expression<Func<TState, TValue>> propertySelector,
        TValue lowerBound,
        TValue upperBound,
        bool includeLowerBound = true,
        bool includeUpperBound = true,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public ValueTask<BlockedInvocation> NextInvocationAsync()
    {
        return _invocations.Reader.ReadAsync();
    }

    private async Task<IReadOnlyList<GrainId>> ExecuteAsync(
        CancellationToken cancellationToken)
    {
        var invocation = new BlockedInvocation();
        using var registration = cancellationToken.Register(
            static state => ((BlockedInvocation)state!).Canceled.TrySetResult(),
            invocation);
        await _invocations.Writer.WriteAsync(invocation, CancellationToken.None);
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        return [];
    }

    public sealed class BlockedInvocation
    {
        public TaskCompletionSource Canceled { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class BlockingQueryProvider(BlockingSearchQueryClient owner)
        : IQueryProvider, ISearchableStorageAsyncQueryProvider
    {
        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = expression.Type.GetGenericArguments().Single();
            var queryType = typeof(BlockingQuery<>).MakeGenericType(elementType);
            return (IQueryable)Activator.CreateInstance(queryType, this, expression)!;
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new BlockingQuery<TElement>(this, expression);
        }

        public object? Execute(Expression expression)
        {
            throw new NotSupportedException();
        }

        public TResult Execute<TResult>(Expression expression)
        {
            throw new NotSupportedException();
        }

        public Task<IReadOnlyList<GrainId>> ExecuteToGrainIdsAsync(
            Expression expression,
            CancellationToken cancellationToken)
        {
            return owner.ExecuteAsync(cancellationToken);
        }
    }

    private sealed class BlockingQuery<TState> : IOrderedQueryable<TState>
    {
        public BlockingQuery(IQueryProvider provider)
        {
            Provider = provider;
            Expression = System.Linq.Expressions.Expression.Constant(this);
        }

        public BlockingQuery(IQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public Type ElementType => typeof(TState);

        public Expression Expression { get; }

        public IQueryProvider Provider { get; }

        public IEnumerator<TState> GetEnumerator()
        {
            throw new NotSupportedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
