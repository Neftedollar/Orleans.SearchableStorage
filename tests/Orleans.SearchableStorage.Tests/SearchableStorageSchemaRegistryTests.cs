using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class SearchableStorageSchemaRegistryTests
{
    [Fact]
    public void PublicRegistryCapturesProviderSpecificStateDeclarations()
    {
        var configuration = new SearchableStorageSchemaRegistry()
            .AddState<SchemaState>("vacancy", applicationSchemaVersion: 7);

        var registry = configuration.CreateRegistry("searchable");
        var registration = registry.Find<SchemaState>("searchable", "vacancy");

        registration.Should().NotBeNull();
        registration!.ProviderName.Should().Be("searchable");
        registration.Schema.ApplicationSchemaVersion.Should().Be(7);
        registry.ContainsProvider("searchable").Should().BeTrue();
        registry.ContainsProvider("another-provider").Should().BeFalse();
    }

    [Fact]
    public void ClientRegistrySnapshotIsUnaffectedByLaterConfigurationChanges()
    {
        var configuration = new SearchableStorageSchemaRegistry()
            .AddState<SchemaState>("first");
        var snapshot = configuration.CreateRegistry("searchable");

        configuration.AddState<AlternateSchemaState>("second");

        snapshot.Find("searchable", "first").Should().NotBeNull();
        snapshot.Find("searchable", "first")!.Schema.ApplicationSchemaVersion.Should().Be(1);
        snapshot.Find("searchable", "second").Should().BeNull();
    }

    [Fact]
    public void DuplicateStateNamesAndInvalidApplicationVersionsAreRejected()
    {
        var configuration = new SearchableStorageSchemaRegistry()
            .AddState<SchemaState>("vacancy");

        var duplicate = () => configuration.AddState<AlternateSchemaState>("vacancy", 2);
        var invalidVersion = () => new SearchableStorageSchemaRegistry()
            .AddState<SchemaState>("other", applicationSchemaVersion: 0);

        duplicate.Should().Throw<InvalidOperationException>()
            .WithMessage("*already declared*");
        invalidVersion.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void ApplicationVersionChangesFingerprintButNotStateIdentity()
    {
        var implicitFirst = IndexMetadataProvider.GetSchemaDefinition<SchemaState>("vacancy");
        var first = IndexMetadataProvider.GetSchemaDefinition<SchemaState>("vacancy", 1);
        var second = IndexMetadataProvider.GetSchemaDefinition<SchemaState>("vacancy", 2);

        implicitFirst.Fingerprint.Should().Equal(first.Fingerprint);
        first.SchemaKey.Should().Equal(second.SchemaKey);
        first.Fingerprint.Should().NotEqual(second.Fingerprint);
    }

    [Fact]
    public void ManagedSchemaIdentityAndControlKeyKeepGoldenVectors()
    {
        var first = IndexMetadataProvider.GetSchemaDefinition<SchemaState>("vacancy", 1);
        var second = IndexMetadataProvider.GetSchemaDefinition<SchemaState>("vacancy", 2);

        Convert.ToHexString(first.SchemaKey).Should().Be(
            "2CE1FCA04093615EDCF30C84D2F0AEDF430C702CABFB751770AC3C6B6657F1E4");
        Convert.ToHexString(first.Fingerprint).Should().Be(
            "5FA05C623681E2917EDC2A60CE6532A491B73D3D496CBF05A2BC242543BFF552");
        Convert.ToHexString(second.Fingerprint).Should().Be(
            "AC2AD6DF7EC7FCC8CDE29C6C04944E5DFDD2687800CBE868BD9A25EBB8D7C60F");
        StorageIndexSchema.CreateGrainKey("searchable", "vacancy").Should().Be(
            "D9C0C312A33E04461896873AAB1F5DC15E602A198D7197784D0E96F2B542B35C");
        IndexSchemaIdentity.ControlKeyDomain.Should().Be("oss:index-schema-control");
        IndexSchemaIdentity.ControlKeyVersion.Should().Be(1);
    }

    [Fact]
    public void SuccessfulActiveFingerprintCacheDoesNotAcceptAnotherVersion()
    {
        var first = new SearchableStateRegistration<SchemaState>("provider", "vacancy", 1);
        var second = new SearchableStateRegistration<SchemaState>("provider", "vacancy", 2);
        var cache = new ActiveSchemaValidationCache();

        cache.IsActive(first).Should().BeFalse();
        cache.MarkActive(first);

        cache.IsActive(first).Should().BeTrue();
        cache.IsActive(second).Should().BeFalse();
    }

    [Fact]
    public void RegistryResolvesCopiedFingerprintsWithinTheirProvider()
    {
        var registration = new SearchableStateRegistration<SchemaState>(
            "provider",
            "vacancy");
        var registry = new SearchableStateRegistry([registration], options: null);

        registry.FindByFingerprint(
                "provider",
                [.. registration.Schema.Fingerprint])
            .Should().BeSameAs(registration);
        registry.FindByFingerprint(
                "another-provider",
                [.. registration.Schema.Fingerprint])
            .Should().BeNull();
    }

    [Fact]
    public void RegistryRejectsDuplicateProviderFingerprintAtConstruction()
    {
        var first = new SearchableStateRegistration<SchemaState>("provider", "first");
        var second = new SearchableStateRegistration<AlternateSchemaState>("provider", "second");
        first.Schema.Fingerprint.CopyTo(second.Schema.Fingerprint, 0);

        var action = () => new SearchableStateRegistry([first, second], options: null);

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*duplicate managed schema fingerprints*");
    }

    [Fact]
    public void SiloServiceRegistrationIncludesApplicationVersion()
    {
        var services = new ServiceCollection();

        services.AddSearchableStorageState<SchemaState>(
            "provider",
            "vacancy",
            applicationSchemaVersion: 11);

        using var provider = services.BuildServiceProvider();
        var registration = provider.GetServices<ISearchableStateRegistration>().Single();
        registration.Schema.ApplicationSchemaVersion.Should().Be(11);
    }

    [Fact]
    public void LegacyServiceRegistrationOverloadDelegatesToVersionOne()
    {
        var services = new ServiceCollection();

        services.AddSearchableStorageState<SchemaState>("provider", "vacancy");

        using var provider = services.BuildServiceProvider();
        provider.GetServices<ISearchableStateRegistration>()
            .Single().Schema.ApplicationSchemaVersion.Should().Be(1);
    }

    [Fact]
    public async Task LegacyAdminSchemaOverloadsDelegateToVersionOne()
    {
        var implementation = new VersionCapturingAdminClient();
        ISearchableStorageAdminClient client = implementation;
        using var cancellation = new CancellationTokenSource();

        _ = await client.GetIndexSchemaAsync<SchemaState>("vacancy", cancellation.Token);
        _ = await client.RebuildIndexSchemaAsync<SchemaState>("vacancy", cancellation.Token);

        implementation.GetVersion.Should().Be(1);
        implementation.RebuildVersion.Should().Be(1);
        implementation.GetCancellation.Should().Be(cancellation.Token);
        implementation.RebuildCancellation.Should().Be(cancellation.Token);
    }

    [Fact]
    public void DirectClientSchemaConstructorsRemainAdditive()
    {
        var basic = typeof(SearchableStorageClient).GetConstructor(
            [typeof(IGrainFactory), typeof(string), typeof(int)]);
        var legacyWithQueryOptions = typeof(SearchableStorageClient).GetConstructor(
            [
                typeof(IGrainFactory),
                typeof(string),
                typeof(int),
                typeof(SearchableStorageQueryOptions),
            ]);
        var ambiguousRegistryOverload = typeof(SearchableStorageClient).GetConstructor(
            [
                typeof(IGrainFactory),
                typeof(string),
                typeof(int),
                typeof(SearchableStorageSchemaRegistry),
            ]);
        var configuredWithQueryOptions = typeof(SearchableStorageClient).GetConstructor(
            [
                typeof(IGrainFactory),
                typeof(string),
                typeof(int),
                typeof(SearchableStorageQueryOptions),
                typeof(SearchableStorageSchemaRegistry),
            ]);

        basic.Should().NotBeNull();
        legacyWithQueryOptions.Should().NotBeNull();
        ambiguousRegistryOverload.Should().BeNull(
            "an existing four-argument call with null query options must remain source-compatible");
        configuredWithQueryOptions.Should().NotBeNull();
    }

    private sealed class VersionCapturingAdminClient : ISearchableStorageAdminClient
    {
        public int? GetVersion { get; private set; }

        public int? RebuildVersion { get; private set; }

        public CancellationToken GetCancellation { get; private set; }

        public CancellationToken RebuildCancellation { get; private set; }

        public Task<SearchableStorageLayout?> GetLayoutAsync(
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult<SearchableStorageLayout?>(null);
        }

        public Task<SearchableStorageIndexSchemaStatus> GetIndexSchemaAsync<TState>(
            string stateName,
            int applicationSchemaVersion,
            CancellationToken cancellationToken)
        {
            GetVersion = applicationSchemaVersion;
            GetCancellation = cancellationToken;
            return Task.FromResult(CreateStatus(stateName));
        }

        public Task<SearchableStorageIndexSchemaStatus> RebuildIndexSchemaAsync<TState>(
            string stateName,
            int applicationSchemaVersion,
            CancellationToken cancellationToken)
        {
            RebuildVersion = applicationSchemaVersion;
            RebuildCancellation = cancellationToken;
            return Task.FromResult(CreateStatus(stateName));
        }

        private static SearchableStorageIndexSchemaStatus CreateStatus(string stateName)
        {
            return new SearchableStorageIndexSchemaStatus
            {
                StateName = stateName,
                State = SearchableStorageIndexSchemaState.Uninitialized,
                ProcessedRecordCount = 0,
            };
        }
    }

    private sealed class SchemaState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string City { get; init; } = string.Empty;
    }

    private sealed class AlternateSchemaState
    {
        [SearchableIndex(SearchableIndexKind.Range)]
        public int Salary { get; init; }
    }
}
