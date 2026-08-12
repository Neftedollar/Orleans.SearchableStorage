using System.Linq.Expressions;
using AwesomeAssertions;

namespace Orleans.SearchableStorage.ApiSample.Tests;

public sealed class MembershipApiCompatibilityTests
{
    [Fact]
    public void DocumentedMembershipPredicatesUseTheExactSupportedMethods()
    {
        const string skill = "C#";
        int? audienceId = 42;

        Expression<Func<CandidateState, bool>> arrayPredicate =
            state => Enumerable.Contains(state.Skills, skill);
        Expression<Func<CandidateState, bool>> listPredicate =
            state => state.AudienceIds.Contains(audienceId);

        var arrayCall = Assert.IsAssignableFrom<MethodCallExpression>(arrayPredicate.Body);
        arrayCall.Method.DeclaringType.Should().Be(typeof(Enumerable));
        arrayCall.Method.Name.Should().Be(nameof(Enumerable.Contains));
        arrayCall.Method.IsGenericMethod.Should().BeTrue();
        arrayCall.Arguments.Should().HaveCount(2);

        var listCall = Assert.IsAssignableFrom<MethodCallExpression>(listPredicate.Body);
        listCall.Method.DeclaringType.Should().Be<List<int?>>();
        listCall.Method.Name.Should().Be(nameof(List<int?>.Contains));
        listCall.Object!.Type.Should().Be<List<int?>>();
        listCall.Arguments.Should().ContainSingle();
    }

    [Fact]
    public void DocumentedWhereInCallIsDeferredAndSnapshotsItsBoundedInput()
    {
        var selectedCities = new List<string> { "Haifa", "Tel Aviv" };

        var query = Array.Empty<CandidateState>()
            .AsQueryable()
            .WhereIn(state => state.City, selectedCities);
        selectedCities[0] = "changed-after-call";
        selectedCities.Add("Jerusalem");

        var marker = Assert.IsAssignableFrom<MethodCallExpression>(query.Expression);
        marker.Method.DeclaringType.Should().Be(typeof(SearchableStorageQueryableExtensions));
        marker.Method.Name.Should().Be(nameof(SearchableStorageQueryableExtensions.WhereIn));
        var snapshotExpression = Assert.IsAssignableFrom<ConstantExpression>(marker.Arguments[2]);
        var snapshot = Assert.IsAssignableFrom<IReadOnlyList<string>>(snapshotExpression.Value);
        snapshot.Should().Equal("Haifa", "Tel Aviv");

        IReadOnlyList<string> tooMany = Enumerable.Range(
                0,
                SearchableStorageQueryLimits.MaximumWhereInValues + 1)
            .Select(static value => $"city-{value}")
            .ToArray();
        var act = () => Array.Empty<CandidateState>()
            .AsQueryable()
            .WhereIn(state => state.City, tooMany);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    private sealed class CandidateState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string?[] Skills { get; set; } = [];

        [SearchableIndex(SearchableIndexKind.Hash)]
        public List<int?> AudienceIds { get; set; } = [];

        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; set; } = string.Empty;
    }
}
