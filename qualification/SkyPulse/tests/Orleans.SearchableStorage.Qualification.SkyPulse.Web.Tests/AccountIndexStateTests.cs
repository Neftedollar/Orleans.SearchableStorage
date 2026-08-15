using System.Reflection;
using Orleans.SearchableStorage;
using Orleans.SearchableStorage.Qualification.SkyPulse.Web;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web.Tests;

public sealed class AccountIndexStateTests
{
    [Fact]
    public void FrozenSchemaContainsExactlySeventeenScalarRangeIndexes()
    {
        var expectedNames = new[]
        {
            nameof(AccountIndexState.LastActivityMinuteUtc),
            nameof(AccountIndexState.CreatedRecordCount1Day),
            nameof(AccountIndexState.CreatedRecordCount7Days),
            nameof(AccountIndexState.CreatedRecordCount30Days),
            nameof(AccountIndexState.UpdatedRecordCount1Day),
            nameof(AccountIndexState.UpdatedRecordCount7Days),
            nameof(AccountIndexState.UpdatedRecordCount30Days),
            nameof(AccountIndexState.DeletedRecordCount1Day),
            nameof(AccountIndexState.DeletedRecordCount7Days),
            nameof(AccountIndexState.DeletedRecordCount30Days),
            nameof(AccountIndexState.CurrentPostCount),
            nameof(AccountIndexState.CurrentFollowingCount),
            nameof(AccountIndexState.CurrentFollowerCount),
            nameof(AccountIndexState.PostCreates1Day),
            nameof(AccountIndexState.PostCreates7Days),
            nameof(AccountIndexState.PostCreates30Days),
            nameof(AccountIndexState.ReceivedEngagementCreates30Days),
        };

        var indexed = typeof(AccountIndexState)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Select(property => new
            {
                Property = property,
                Attribute = property.GetCustomAttribute<SearchableIndexAttribute>(),
            })
            .Where(static item => item.Attribute is not null)
            .OrderBy(static item => item.Property.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(expectedNames.Order(StringComparer.Ordinal), indexed.Select(static item => item.Property.Name));
        Assert.All(indexed, static item => Assert.Equal(typeof(long), item.Property.PropertyType));
        Assert.All(indexed, static item => Assert.Equal(SearchableIndexKind.Range, item.Attribute!.Kind));
        Assert.All(indexed, static item => Assert.Null(item.Attribute!.Name));
    }
}
