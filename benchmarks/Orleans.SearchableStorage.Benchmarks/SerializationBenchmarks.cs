using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Serialization.Session;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Benchmarks;

[BenchmarkCategory("Serialization", "Wire")]
public class QueryPlanSerializationBenchmarks
{
    private ServiceProvider _serviceProvider = null!;
    private Serializer<PartitionQueryPlan> _serializer = null!;
    private SerializerSession _session = null!;
    private PartitionQueryPlan _plan = null!;
    private byte[] _serialized = null!;
    private byte[] _destination = null!;

    [Params(4, 64)]
    public int LeafCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _serviceProvider = SerializerServices.Create();
        _serializer = _serviceProvider.GetRequiredService<Serializer<PartitionQueryPlan>>();
        _session = _serviceProvider.GetRequiredService<SerializerSessionPool>().GetSession();
        var expression = QueryPlanConstructionBenchmarks.CreateQueryExpression(LeafCount);
        _plan = PartitionQueryPlanFactory.Create(
            QueryTranslator.Translate<QueryPlanConstructionBenchmarks.QueryState>(
                "benchmark-state",
                expression));
        _serialized = _serializer.SerializeToArray(_plan);
        _destination = new byte[Math.Max(4_096, checked(_serialized.Length * 2))];

        var copy = _serializer.Deserialize(_serialized, _session);
        _session.Reset();
        QueryPlanValidator.Validate(copy);
        ValidateFixture(copy);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _session.Dispose();
        _serviceProvider.Dispose();
    }

    [Benchmark]
    public int SerializePartitionQueryPlan()
    {
        _session.Reset();
        return _serializer.Serialize(_plan, _destination, _session);
    }

    [Benchmark]
    public int DeserializePartitionQueryPlan()
    {
        _session.Reset();
        var copy = _serializer.Deserialize(_serialized, _session);
        return CountPlanNodes(copy);
    }

    internal void ValidateFixture()
    {
        _session.Reset();
        var copy = _serializer.Deserialize(_serialized, _session);
        _session.Reset();
        ValidateFixture(copy);
    }

    internal void ValidateSerializedFixture(int serializedLength)
    {
        if (serializedLength <= 0 || serializedLength > _destination.Length)
        {
            throw new InvalidOperationException(
                "The query-plan benchmark returned an invalid serialized length.");
        }

        _session.Reset();
        var copy = _serializer.Deserialize(_destination[..serializedLength], _session);
        _session.Reset();
        QueryPlanValidator.Validate(copy);
        ValidateFixture(copy);
    }

    private void ValidateFixture(PartitionQueryPlan copy)
    {
        if (!PlanEquals(_plan, copy) || !PlanIsDetached(_plan, copy))
        {
            throw new InvalidOperationException(
                "The query-plan serialization fixture did not preserve and detach the complete wire plan.");
        }
    }

    private static bool PlanEquals(PartitionQueryPlan expected, PartitionQueryPlan actual) =>
        expected.Operation == actual.Operation
        && string.Equals(expected.Scope, actual.Scope, StringComparison.Ordinal)
        && expected.IndexKind == actual.IndexKind
        && IndexValueEquals(expected.Value, actual.Value)
        && IndexValueEquals(expected.LowerBound, actual.LowerBound)
        && IndexValueEquals(expected.UpperBound, actual.UpperBound)
        && expected.IncludeLowerBound == actual.IncludeLowerBound
        && expected.IncludeUpperBound == actual.IncludeUpperBound
        && ChildEquals(expected.Left, actual.Left)
        && ChildEquals(expected.Right, actual.Right);

    private static bool ChildEquals(PartitionQueryPlan? expected, PartitionQueryPlan? actual) =>
        expected is null ? actual is null : actual is not null && PlanEquals(expected, actual);

    private static bool IndexValueEquals(IndexValue? expected, IndexValue? actual) =>
        expected is null ? actual is null : actual is not null && expected.Equals(actual);

    private static bool PlanIsDetached(PartitionQueryPlan expected, PartitionQueryPlan actual)
    {
        if (ReferenceEquals(expected, actual)
            || expected.Value is not null && ReferenceEquals(expected.Value, actual.Value)
            || expected.LowerBound is not null && ReferenceEquals(expected.LowerBound, actual.LowerBound)
            || expected.UpperBound is not null && ReferenceEquals(expected.UpperBound, actual.UpperBound))
        {
            return false;
        }

        return ChildIsDetached(expected.Left, actual.Left)
            && ChildIsDetached(expected.Right, actual.Right);
    }

    private static bool ChildIsDetached(PartitionQueryPlan? expected, PartitionQueryPlan? actual) =>
        expected is null ? actual is null : actual is not null && PlanIsDetached(expected, actual);

    private static int CountPlanNodes(PartitionQueryPlan plan) =>
        1
        + (plan.Left is null ? 0 : CountPlanNodes(plan.Left))
        + (plan.Right is null ? 0 : CountPlanNodes(plan.Right));
}

[BenchmarkCategory("Serialization", "Persistence")]
public class JournalSerializationBenchmarks
{
    private ServiceProvider _serviceProvider = null!;
    private Serializer<StorageJournalSegmentState> _serializer = null!;
    private SerializerSession _session = null!;
    private StorageJournalSegmentState _segment = null!;
    private byte[] _serialized = null!;
    private byte[] _destination = null!;

    [Params(1, 64)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _serviceProvider = SerializerServices.Create();
        _serializer = _serviceProvider.GetRequiredService<Serializer<StorageJournalSegmentState>>();
        _session = _serviceProvider.GetRequiredService<SerializerSessionPool>().GetSession();
        _segment = new StorageJournalSegmentState
        {
            Initialized = true,
            Capacity = 64,
            AbsoluteSegmentIndex = 0,
            HighestWriterEpoch = 1,
            Entries = Enumerable.Range(1, EntryCount)
                .Select(sequence => BenchmarkData.CreateUpsertEntry(sequence, sequence - 1))
                .ToList(),
        };
        _serialized = _serializer.SerializeToArray(_segment);
        _destination = new byte[Math.Max(8_192, checked(_serialized.Length * 2))];

        var copy = _serializer.Deserialize(_serialized, _session);
        _session.Reset();
        ValidateFixture(copy);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _session.Dispose();
        _serviceProvider.Dispose();
    }

    [Benchmark]
    public int SerializeJournalSegment()
    {
        _session.Reset();
        return _serializer.Serialize(_segment, _destination, _session);
    }

    [Benchmark]
    public long DeserializeJournalSegment()
    {
        _session.Reset();
        var copy = _serializer.Deserialize(_serialized, _session);
        return copy.Entries[^1].Sequence;
    }

    internal void ValidateFixture()
    {
        _session.Reset();
        var copy = _serializer.Deserialize(_serialized, _session);
        _session.Reset();
        ValidateFixture(copy);
    }

    internal void ValidateSerializedFixture(int serializedLength)
    {
        if (serializedLength <= 0 || serializedLength > _destination.Length)
        {
            throw new InvalidOperationException(
                "The journal benchmark returned an invalid serialized length.");
        }

        _session.Reset();
        var copy = _serializer.Deserialize(_destination[..serializedLength], _session);
        _session.Reset();
        ValidateFixture(copy);
    }

    private void ValidateFixture(StorageJournalSegmentState copy)
    {
        if (copy.Initialized != _segment.Initialized
            || copy.Capacity != _segment.Capacity
            || copy.AbsoluteSegmentIndex != _segment.AbsoluteSegmentIndex
            || copy.HighestWriterEpoch != _segment.HighestWriterEpoch
            || copy.Tombstoned != _segment.Tombstoned
            || copy.Entries.Count != _segment.Entries.Count)
        {
            throw new InvalidOperationException(
                "The journal serialization fixture did not preserve the complete segment metadata.");
        }

        for (var index = 0; index < _segment.Entries.Count; index++)
        {
            var expected = _segment.Entries[index];
            var actual = copy.Entries[index];
            if (!StoragePersistenceStateEquality.JournalEntryEquals(expected, actual)
                || ReferenceEquals(expected, actual)
                || expected.Record is not null && ReferenceEquals(expected.Record, actual.Record)
                || expected.Record?.Payload is not null
                    && ReferenceEquals(expected.Record.Payload, actual.Record?.Payload)
                || expected.Record?.IndexEntries is not null
                    && ReferenceEquals(expected.Record.IndexEntries, actual.Record?.IndexEntries))
            {
                throw new InvalidOperationException(
                    $"The journal serialization fixture changed or aliased entry {index}.");
            }
        }
    }
}

internal static class SerializerServices
{
    public static ServiceProvider Create()
    {
        return new ServiceCollection()
            .AddSerializer(builder => builder.AddAssembly(typeof(StorageJournalSegmentState).Assembly))
            .BuildServiceProvider();
    }
}
