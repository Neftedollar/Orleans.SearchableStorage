using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Orleans.Runtime;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

[BenchmarkCategory("Persistence", "Journal")]
[ThreadingDiagnoser]
public class JournalAppendBenchmarks
{
    private const int EntriesPerInvocation = 64;
    private StorageJournalEntry[] _entries = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _entries = Enumerable.Range(1, EntriesPerInvocation)
            .Select(sequence => BenchmarkData.CreateUpsertEntry(sequence, sequence - 1))
            .ToArray();
    }

    [Benchmark(OperationsPerInvoke = EntriesPerInvocation)]
    public async Task<int> AppendBoundedJournalSegment()
    {
        var state = await AppendFixtureAsync();
        return state.State.Entries.Count;
    }

    internal async Task ValidateFixtureAsync()
    {
        var state = await AppendFixtureAsync();
        if (!state.State.Initialized
            || state.State.Capacity != EntriesPerInvocation
            || state.State.AbsoluteSegmentIndex != 0
            || state.State.HighestWriterEpoch != 1
            || state.State.Tombstoned
            || state.State.Entries.Count != EntriesPerInvocation
            || state.WriteCount != EntriesPerInvocation
            || !state.RecordExists
            || !string.Equals(
                state.Etag,
                EntriesPerInvocation.ToString(System.Globalization.CultureInfo.InvariantCulture),
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "The bounded journal append fixture did not persist the exact segment metadata and write count.");
        }

        for (var index = 0; index < _entries.Length; index++)
        {
            var expected = _entries[index];
            var actual = state.State.Entries[index];
            if (!StoragePersistenceStateEquality.JournalEntryEquals(expected, actual)
                || ReferenceEquals(expected, actual)
                || expected.Record is not null && ReferenceEquals(expected.Record, actual.Record)
                || expected.Record?.Payload is not null
                    && ReferenceEquals(expected.Record.Payload, actual.Record?.Payload)
                || expected.Record?.IndexEntries is not null
                    && ReferenceEquals(expected.Record.IndexEntries, actual.Record?.IndexEntries))
            {
                throw new InvalidOperationException(
                    $"The bounded journal append fixture changed or aliased entry {index}.");
            }
        }
    }

    private async Task<BenchmarkPersistentState<StorageJournalSegmentState>> AppendFixtureAsync()
    {
        var state = new BenchmarkPersistentState<StorageJournalSegmentState>();
        var grain = new StorageJournalSegmentGrain(state, requestDeactivation: static () => { });
        var committedSequence = 0L;
        var committedOperationId = Guid.Empty;
        foreach (var entry in _entries)
        {
            await grain.StoreAsync(
                entry,
                committedSequence,
                committedOperationId,
                absoluteSegmentIndex: 0,
                segmentCapacity: EntriesPerInvocation);
            committedSequence = entry.Sequence;
            committedOperationId = entry.OperationId;
        }

        return state;
    }
}

[BenchmarkCategory("Persistence", "Replay")]
public class JournalReplayBenchmarks
{
    private const int SnapshotRecordCount = 4_096;
    private StorageSnapshotState _snapshot = null!;
    private StorageJournalEntry[] _entries = null!;

    [Params(64, 4_096)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _snapshot = new StorageSnapshotState
        {
            Initialized = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = BenchmarkData.CreateOperationId(10_000_001),
            Sequence = SnapshotRecordCount,
            OperationId = BenchmarkData.CreateOperationId(10_000_002),
            NextVersion = SnapshotRecordCount + 1,
            Records = BenchmarkData.CreateRecords(SnapshotRecordCount),
        };
        _entries = Enumerable.Range(1, EntryCount)
            .Select(CreateReplayEntry)
            .ToArray();

        var expectedNextVersion = checked(_snapshot.NextVersion + EntryCount);
        if (ReplayValidatedJournal() != expectedNextVersion
            || MaterializeSnapshotAndReplay() != expectedNextVersion)
        {
            throw new InvalidOperationException("The deterministic replay fixture is inconsistent.");
        }

        ValidateFixture();
    }

    [Benchmark]
    public long ReplayValidatedJournal()
    {
        var records = new Dictionary<string, StoredRecord>(_snapshot.Records, StringComparer.Ordinal);
        return Replay(records).NextVersion;
    }

    [Benchmark]
    public long MaterializeSnapshotAndReplay()
    {
        var descriptor = StorageSnapshotDescriptor.FromSnapshot(_snapshot);
        var materialized = StorageSnapshotFactory.Create(descriptor, _snapshot.Records);
        return Replay(materialized.Records).NextVersion;
    }

    private StorageJournalEntry CreateReplayEntry(int offset)
    {
        var recordIndex = offset - 1;
        var sequence = checked(_snapshot.Sequence + offset);
        var version = checked(_snapshot.NextVersion + offset - 1);
        return new StorageJournalEntry
        {
            Sequence = sequence,
            WriterEpoch = 1,
            OperationId = BenchmarkData.CreateOperationId(checked(30_000_000 + offset)),
            PreviousOperationId = offset == 1
                ? _snapshot.OperationId
                : BenchmarkData.CreateOperationId(checked(30_000_000 + offset - 1)),
            Operation = StorageJournalOperation.Upsert,
            RecordKey = BenchmarkData.CreateRecordKey(recordIndex),
            ExpectedETag = checked(recordIndex + 1).ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            Record = BenchmarkData.CreateRecord(
                recordIndex,
                salary: checked(recordIndex + 1_000_000),
                city: checked(recordIndex + 1_000),
                etag: version.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            NextVersionAfter = checked(version + 1),
        };
    }

    internal void ValidateFixture()
    {
        var records = new Dictionary<string, StoredRecord>(_snapshot.Records, StringComparer.Ordinal);
        var outcome = Replay(records);
        var expectedRecords = new Dictionary<string, StoredRecord>(_snapshot.Records, StringComparer.Ordinal);
        foreach (var entry in _entries)
        {
            expectedRecords[entry.RecordKey] = entry.Record
                ?? throw new InvalidOperationException("The replay fixture contains a non-upsert entry.");
        }

        var expected = _snapshot.Copy();
        expected.NextVersion = outcome.NextVersion;
        expected.OperationId = outcome.OperationId;
        expected.Records = expectedRecords;
        var actual = expected.Copy();
        actual.Records = records;
        if (!StoragePersistenceStateEquality.SnapshotEquals(expected, actual)
            || outcome.OperationId != _entries[^1].OperationId
            || ReferenceEquals(records[_entries[0].RecordKey], _entries[0].Record)
            || ReferenceEquals(records[_entries[0].RecordKey].Payload, _entries[0].Record!.Payload))
        {
            throw new InvalidOperationException("Validated journal replay did not reproduce the expected records and commit point.");
        }
    }

    private ReplayOutcome Replay(Dictionary<string, StoredRecord> records)
    {
        var recoveredOperationIds = new HashSet<Guid> { _snapshot.OperationId };
        var nextVersion = _snapshot.NextVersion;
        var operationId = _snapshot.OperationId;
        var expectedSequence = checked(_snapshot.Sequence + 1);
        var capacity = new StorageCapacityTracker(records);
        foreach (var entry in _entries)
        {
            StorageJournalReplay.ApplyEntry(
                records,
                entry,
                expectedSequence,
                maximumWriterEpoch: 1,
                recoveredOperationIds,
                ref nextVersion,
                ref operationId,
                capacity);
            expectedSequence++;
        }

        return new ReplayOutcome(nextVersion, operationId);
    }

    private readonly record struct ReplayOutcome(long NextVersion, Guid OperationId);
}

[BenchmarkCategory("Persistence", "Snapshot")]
public class SnapshotConstructionBenchmarks
{
    private Dictionary<string, StoredRecord> _records = null!;
    private StorageSnapshotDescriptor _descriptor = null!;

    [Params(1_024, 16_384)]
    public int RecordCount { get; set; }

    [Params(64, 1_024)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _records = BenchmarkData.CreateRecords(RecordCount, PayloadSize);
        _descriptor = new StorageSnapshotDescriptor
        {
            IsPresent = true,
            Slot = 0,
            Generation = 1,
            SnapshotId = BenchmarkData.CreateOperationId(20_000_001),
            Sequence = RecordCount,
            OperationId = BenchmarkData.CreateOperationId(20_000_002),
            NextVersion = RecordCount + 1,
        };

        var snapshot = StorageSnapshotFactory.Create(_descriptor, _records);
        if (snapshot.Records.Count != RecordCount
            || ReferenceEquals(snapshot.Records, _records))
        {
            throw new InvalidOperationException("The deterministic snapshot fixture is inconsistent.");
        }

        ValidateFixture();
    }

    [Benchmark]
    public object ConstructCompactionSnapshot() =>
        StorageSnapshotFactory.Create(_descriptor, _records);

    internal void ValidateFixture()
    {
        var snapshot = StorageSnapshotFactory.Create(_descriptor, _records);
        var expected = new StorageSnapshotState
        {
            Initialized = true,
            Slot = _descriptor.Slot,
            Generation = _descriptor.Generation,
            SnapshotId = _descriptor.SnapshotId,
            Sequence = _descriptor.Sequence,
            OperationId = _descriptor.OperationId,
            NextVersion = _descriptor.NextVersion,
            Records = _records,
        };
        var firstKey = _records.Keys.First();
        if (!StoragePersistenceStateEquality.SnapshotEquals(expected, snapshot)
            || !StoragePersistenceStateEquality.DescriptorEquals(_descriptor, snapshot)
            || ReferenceEquals(snapshot.Records[firstKey], _records[firstKey])
            || ReferenceEquals(snapshot.Records[firstKey].Payload, _records[firstKey].Payload)
            || ReferenceEquals(snapshot.Records[firstKey].IndexEntries, _records[firstKey].IndexEntries))
        {
            throw new InvalidOperationException("Compaction snapshot construction did not preserve or detach its durable payload.");
        }
    }
}

internal sealed class BenchmarkPersistentState<T> : IPersistentState<T>
    where T : class, new()
{
    public T State { get; set; } = new();

    public string? Etag { get; private set; }

    public bool RecordExists { get; private set; }

    public int WriteCount { get; private set; }

    public Task ClearStateAsync()
    {
        State = new T();
        Etag = null;
        RecordExists = false;
        return Task.CompletedTask;
    }

    public Task ReadStateAsync() => Task.CompletedTask;

    public Task WriteStateAsync()
    {
        WriteCount++;
        RecordExists = true;
        Etag = WriteCount.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return Task.CompletedTask;
    }
}
