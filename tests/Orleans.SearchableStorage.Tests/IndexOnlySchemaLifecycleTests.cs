using AwesomeAssertions;
using Microsoft.Extensions.Options;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class IndexOnlySchemaLifecycleTests
{
    private const string ProviderName = "index-only-schema-lifecycle";
    private const string StateName = "document";

    [Fact]
    public async Task ActiveFingerprintMakesTheSameSchemaAnIdempotentNoWrite()
    {
        var registration = new SearchableStateRegistration<IndexOnlySchemaState>(
            ProviderName,
            StateName);
        var persisted = CreateActiveState(registration.Schema.Fingerprint);
        var grain = CreateGrain(persisted, registration);
        var request = StorageIndexSchema.CreateRequest(registration);

        var snapshot = await grain.BeginRebuildAsync(request);

        snapshot.ActiveFingerprint.Should().Equal(registration.Schema.Fingerprint);
        snapshot.Rebuild.Should().BeNull();
        persisted.WriteCount.Should().Be(0);
    }

    [Fact]
    public async Task IncompatibleSchemaIsRejectedBeforeIntentOrGenerationMutation()
    {
        var registration = new SearchableStateRegistration<IndexOnlySchemaState>(
            ProviderName,
            StateName);
        var incompatibleFingerprint = registration.Schema.Fingerprint.ToArray();
        incompatibleFingerprint[0] ^= 0xff;
        var persisted = CreateActiveState(incompatibleFingerprint);
        var before = persisted.State.Copy();
        var grain = CreateGrain(persisted, registration);
        var request = StorageIndexSchema.CreateRequest(registration);

        Func<Task> begin = async () => await grain.BeginRebuildAsync(request);

        await begin.Should().ThrowExactlyAsync<SearchableStorageIndexSchemaException>()
            .WithMessage("*index-only*cannot rebuild*payload*");
        persisted.WriteCount.Should().Be(0);
        persisted.State.ActiveFingerprint.Should().Equal(before.ActiveFingerprint!);
        persisted.State.Rebuild.Should().BeNull();
        persisted.State.LastCompletedRecordCount.Should().Be(before.LastCompletedRecordCount);
    }

    private static TestPersistentState<StorageIndexSchemaState> CreateActiveState(
        byte[] fingerprint)
    {
        return new TestPersistentState<StorageIndexSchemaState>
        {
            State = new StorageIndexSchemaState
            {
                Initialized = true,
                ProtocolVersion = StorageIndexSchema.ProtocolVersion,
                ProviderName = ProviderName,
                StateName = StateName,
                ActiveFingerprint = [.. fingerprint],
                LastCompletedRecordCount = 7,
            },
        };
    }

    private static StorageIndexSchemaGrain CreateGrain(
        TestPersistentState<StorageIndexSchemaState> persisted,
        ISearchableStateRegistration registration)
    {
        var options = new SearchableStorageOptions
        {
            NamespaceMode = StorageNamespaceMode.IndexOnly,
        };
        return new StorageIndexSchemaGrain(
            persisted,
            new SearchableStateRegistry([registration], options: null),
            new FixedOptionsMonitor<SearchableStorageOptions>(options),
            StorageIndexSchema.CreateGrainKey(ProviderName, StateName));
    }

    private sealed class FixedOptionsMonitor<TOptions>(TOptions value)
        : IOptionsMonitor<TOptions>
    {
        public TOptions CurrentValue => value;

        public TOptions Get(string? name) => value;

        public IDisposable OnChange(Action<TOptions, string?> listener) => NoopDisposable.Instance;
    }

    private sealed class NoopDisposable : IDisposable
    {
        public static NoopDisposable Instance { get; } = new();

        public void Dispose()
        {
        }
    }

    private sealed class IndexOnlySchemaState
    {
        [SearchableIndex(SearchableIndexKind.Hash)]
        public string Value { get; set; } = string.Empty;
    }
}
