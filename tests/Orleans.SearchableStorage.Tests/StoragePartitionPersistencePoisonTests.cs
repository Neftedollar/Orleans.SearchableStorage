using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;
using Orleans.SearchableStorage.Tests.Infrastructure;

namespace Orleans.SearchableStorage.Tests;

[CollectionDefinition(Name)]
public sealed class DurableProtocolMemoryFixtureGroup : ICollectionFixture<MemoryStorageFixture>
{
    public const string Name = "Durable protocol memory tests";
}

[Collection(DurableProtocolMemoryFixtureGroup.Name)]
public sealed class StoragePartitionPersistencePoisonTests
{
    private readonly MemoryStorageFixture _fixture;

    public StoragePartitionPersistencePoisonTests(MemoryStorageFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AmbiguousManifestWritePoisonsTheCoordinatorBeforeItCanReuseRestoredState()
    {
        var injected = new InvalidOperationException("Ambiguous manifest write.");
        var state = new TestPersistentState<StoragePartitionManifestState>
        {
            WriteException = injected,
        };
        var poisonCount = 0;
        var persistence = new StoragePartitionPersistence(
            state,
            _fixture.Cluster.GrainFactory,
            $"poison-{Guid.NewGuid():N}",
            () => poisonCount++,
            NullLogger<StoragePartitionPersistence>.Instance);
        var settings = new StoragePersistenceSettings
        {
            JournalSegmentCapacity = 2,
            MaximumJournalReplayEntries = 4,
            CompactionThreshold = 4,
        };

        Func<Task> firstMutation = () => persistence.PrepareForMutationAsync(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            settings);
        await firstMutation.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage(injected.Message);

        poisonCount.Should().Be(1);
        state.WriteCount.Should().Be(1);
        state.State.Initialized.Should().BeFalse();

        Action readCommitPoint = () => _ = persistence.CommittedSequence;
        Func<Task> retryMutation = () => persistence.PrepareForMutationAsync(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal),
            settings);

        readCommitPoint.Should().Throw<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous manifest write*");
        await retryMutation.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be reused after an ambiguous manifest write*");
        state.WriteCount.Should().Be(1);
    }
}

internal sealed class TestPersistentState<T> : IPersistentState<T>
    where T : class, new()
{
    public T State { get; set; } = new();

    public string? Etag { get; private set; }

    public bool RecordExists { get; private set; }

    public int WriteCount { get; private set; }

    public Exception? WriteException { get; init; }

    public Task ClearStateAsync()
    {
        State = new T();
        Etag = null;
        RecordExists = false;
        return Task.CompletedTask;
    }

    public Task ReadStateAsync()
    {
        return Task.CompletedTask;
    }

    public Task WriteStateAsync()
    {
        WriteCount++;
        RecordExists = true;
        Etag = WriteCount.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return WriteException is null ? Task.CompletedTask : Task.FromException(WriteException);
    }
}
