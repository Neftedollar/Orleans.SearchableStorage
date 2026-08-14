using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.SearchableStorage.Tests.TestGrains;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexOnlyDependencyInjectionTests
{
    [Fact]
    public void FullStorageAndIndexOnlyRegistrationsExposeOnlyTheirWriteCapability()
    {
        const string fullProvider = "full-provider";
        const string indexProvider = "index-provider";
        var services = new ServiceCollection();

        services.AddSearchableGrainStorage(fullProvider);
        services.AddSearchableStorageState<VacancyState>(fullProvider, "vacancy");
        services.AddSearchableIndex(indexProvider);
        services.AddSearchableStorageState<VacancyState>(indexProvider, "vacancy");

        var fullTypes = GetKeyedServiceTypes(services, fullProvider);
        var indexTypes = GetKeyedServiceTypes(services, indexProvider);

        fullTypes.Should().Contain(typeof(IGrainStorage));
        fullTypes.Should().Contain(typeof(ISearchableStorageQueryClient));
        fullTypes.Should().Contain(typeof(ISearchableStorageAdminClient));
        fullTypes.Should().NotContain(typeof(ISearchableStorageIndexWriter));

        indexTypes.Should().Contain(typeof(ISearchableStorageIndexWriter));
        indexTypes.Should().Contain(typeof(ISearchableStorageQueryClient));
        indexTypes.Should().Contain(typeof(ISearchableStorageAdminClient));
        indexTypes.Should().NotContain(typeof(IGrainStorage));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OneProviderKeyCannotBeRegisteredInBothNamespaceModes(bool fullFirst)
    {
        const string providerName = "ambiguous-provider";
        var services = new ServiceCollection();
        if (fullFirst)
        {
            services.AddSearchableGrainStorage(providerName);
        }
        else
        {
            services.AddSearchableIndex(providerName);
        }

        Action registerOtherMode = fullFirst
            ? () => services.AddSearchableIndex(providerName)
            : () => services.AddSearchableGrainStorage(providerName);

        registerOtherMode.Should().Throw<InvalidOperationException>()
            .WithMessage("*already registered*different*mode*");
    }

    private static Type[] GetKeyedServiceTypes(IServiceCollection services, string key)
    {
        return services
            .Where(descriptor => descriptor.IsKeyedService
                && string.Equals(descriptor.ServiceKey as string, key, StringComparison.Ordinal))
            .Select(static descriptor => descriptor.ServiceType)
            .Distinct()
            .ToArray();
    }
}
