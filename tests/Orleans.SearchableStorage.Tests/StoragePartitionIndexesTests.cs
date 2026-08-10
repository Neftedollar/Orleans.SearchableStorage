using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePartitionIndexesTests
{
    [Fact]
    public void BuildProjectsEveryHashAndRangeEntry()
    {
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["first"] = CreateRecord(
                CreateHashEntry("city", "Helsinki"),
                CreateRangeEntry("salary", 10)),
            ["second"] = CreateRecord(
                CreateHashEntry("city", "Helsinki"),
                CreateRangeEntry("salary", 20)),
            ["different"] = CreateRecord(
                CreateHashEntry("city", "London"),
                CreateRangeEntry("salary", 30)),
        };

        var indexes = StoragePartitionIndexes.Build(records);
        var range = new HashSet<string>(StringComparer.Ordinal);
        indexes.UnionRange(
            "salary",
            IndexValue.FromSignedInteger(10),
            IndexValue.FromSignedInteger(20),
            includeLowerBound: true,
            includeUpperBound: true,
            range);

        indexes.FindHashEntries("city", IndexValue.Create("Helsinki"))
            .Should().BeEquivalentTo(["first", "second"]);
        indexes.FindHashEntries("city", IndexValue.Create("London"))
            .Should().ContainSingle().Which.Should().Be("different");
        indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10))
            .Should().ContainSingle().Which.Should().Be("first");
        range.Should().BeEquivalentTo(["first", "second"]);
    }

    [Fact]
    public void ReplacingRecordLeavesUnrelatedBucketInstancesUntouched()
    {
        var oldRecord = CreateRecord(
            CreateHashEntry("city", "Helsinki"),
            CreateRangeEntry("salary", 10));
        var unrelated = CreateRecord(
            CreateHashEntry("department", "engineering"),
            CreateRangeEntry("level", 5));
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["target"] = oldRecord,
                ["unrelated"] = unrelated,
            });
        var unrelatedHash = indexes.FindHashEntries("department", IndexValue.Create("engineering"));
        var unrelatedRange = indexes.FindRangeEntries("level", IndexValue.FromSignedInteger(5));
        var replacement = CreateRecord(
            CreateHashEntry("city", "London"),
            CreateRangeEntry("salary", 20));

        indexes.RemoveRecord("target", oldRecord);
        indexes.AddRecord("target", replacement);

        indexes.FindHashEntries("department", IndexValue.Create("engineering"))
            .Should().BeSameAs(unrelatedHash);
        indexes.FindRangeEntries("level", IndexValue.FromSignedInteger(5))
            .Should().BeSameAs(unrelatedRange);
        unrelatedHash.Should().ContainSingle().Which.Should().Be("unrelated");
        unrelatedRange.Should().ContainSingle().Which.Should().Be("unrelated");
        indexes.FindHashEntries("city", IndexValue.Create("Helsinki")).Should().BeEmpty();
        indexes.FindHashEntries("city", IndexValue.Create("London"))
            .Should().ContainSingle().Which.Should().Be("target");
        indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10)).Should().BeEmpty();
        indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(20))
            .Should().ContainSingle().Which.Should().Be("target");
    }

    [Fact]
    public void PartitionViewUsesIncrementalBucketsForTheMutationPathUsedByTheGrain()
    {
        var target = CreateRecord(
            CreateHashEntry("city", "Helsinki"),
            CreateRangeEntry("salary", 10));
        var unrelated = CreateRecord(
            CreateHashEntry("department", "engineering"),
            CreateRangeEntry("level", 5));
        var view = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["target"] = target,
                ["unrelated"] = unrelated,
            });
        var unrelatedHash = view.Indexes.FindHashEntries(
            "department",
            IndexValue.Create("engineering"));
        var unrelatedRange = view.Indexes.FindRangeEntries(
            "level",
            IndexValue.FromSignedInteger(5));
        var replacement = CreateRecord(
            CreateHashEntry("city", "London"),
            CreateRangeEntry("salary", 20));

        view.ApplyUpsert("target", replacement);

        view.Records["target"].Should().BeSameAs(replacement);
        view.Indexes.FindHashEntries("department", IndexValue.Create("engineering"))
            .Should().BeSameAs(unrelatedHash);
        view.Indexes.FindRangeEntries("level", IndexValue.FromSignedInteger(5))
            .Should().BeSameAs(unrelatedRange);
        view.Indexes.FindHashEntries("city", IndexValue.Create("Helsinki")).Should().BeEmpty();
        view.Indexes.FindHashEntries("city", IndexValue.Create("London"))
            .Should().ContainSingle().Which.Should().Be("target");

        view.ApplyDelete("target");

        view.Records.Should().NotContainKey("target");
        view.Indexes.FindHashEntries("department", IndexValue.Create("engineering"))
            .Should().BeSameAs(unrelatedHash);
        view.Indexes.FindRangeEntries("level", IndexValue.FromSignedInteger(5))
            .Should().BeSameAs(unrelatedRange);
        unrelatedHash.Should().ContainSingle().Which.Should().Be("unrelated");
        unrelatedRange.Should().ContainSingle().Which.Should().Be("unrelated");
    }

    [Fact]
    public void RemovingRecordRetainsNeighborsInSharedHashAndRangeBuckets()
    {
        var target = CreateRecord(
            CreateHashEntry("city", "Helsinki"),
            CreateRangeEntry("salary", 10));
        var neighbor = CreateRecord(
            CreateHashEntry("city", "Helsinki"),
            CreateRangeEntry("salary", 10));
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["target"] = target,
                ["neighbor"] = neighbor,
            });
        var sharedHash = indexes.FindHashEntries("city", IndexValue.Create("Helsinki"));
        var sharedRange = indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10));

        indexes.RemoveRecord("target", target);

        indexes.FindHashEntries("city", IndexValue.Create("Helsinki"))
            .Should().BeSameAs(sharedHash);
        indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10))
            .Should().BeSameAs(sharedRange);
        sharedHash.Should().ContainSingle().Which.Should().Be("neighbor");
        sharedRange.Should().ContainSingle().Which.Should().Be("neighbor");
    }

    [Fact]
    public void DuplicateEntriesAndRepeatedAddsAreIdempotent()
    {
        var hash = CreateHashEntry("city", "Helsinki");
        var range = CreateRangeEntry("salary", 10);
        var record = CreateRecord(hash, hash, range, range);
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal));

        indexes.AddRecord("record", record);
        indexes.AddRecord("record", record);

        indexes.FindHashEntries("city", IndexValue.Create("Helsinki"))
            .Should().ContainSingle().Which.Should().Be("record");
        indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10))
            .Should().ContainSingle().Which.Should().Be("record");

        indexes.RemoveRecord("record", record);

        indexes.FindHashEntries("city", IndexValue.Create("Helsinki")).Should().BeEmpty();
        indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10)).Should().BeEmpty();
    }

    [Fact]
    public void RemovingLastRecordLeavesIndependentEmptyLookupResults()
    {
        var record = CreateRecord(
            CreateHashEntry("city", "Helsinki"),
            CreateRangeEntry("salary", 10));
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["record"] = record,
            });

        indexes.RemoveRecord("record", record);
        var missingHash = indexes.FindHashEntries("city", IndexValue.Create("Helsinki"));
        var missingRange = indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10));
        missingHash.Add("caller-owned");
        missingRange.Add("caller-owned");
        var destination = new HashSet<string>(["existing"], StringComparer.Ordinal);
        indexes.UnionRange(
            "salary",
            lowerBound: null,
            upperBound: null,
            includeLowerBound: false,
            includeUpperBound: false,
            destination);

        indexes.FindHashEntries("city", IndexValue.Create("Helsinki")).Should().BeEmpty();
        indexes.FindRangeEntries("salary", IndexValue.FromSignedInteger(10)).Should().BeEmpty();
        destination.Should().ContainSingle().Which.Should().Be("existing");
    }

    [Fact]
    public void InvalidKindIsRejectedBeforeAnyEntryIsAdded()
    {
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
        var record = CreateRecord(
            CreateHashEntry("valid", "value"),
            new IndexEntry
            {
                Scope = "invalid",
                Kind = (SearchableIndexKind)int.MaxValue,
                Value = IndexValue.Create("value"),
            });

        var action = () => indexes.AddRecord("record", record);

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("Unknown index kind*");
        indexes.FindHashEntries("valid", IndexValue.Create("value")).Should().BeEmpty();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void InvalidScopeIsRejectedBeforeAnyEntryIsAdded(string? scope)
    {
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
        var record = CreateRecord(
            CreateHashEntry("valid", "value"),
            new IndexEntry
            {
                Scope = scope!,
                Kind = SearchableIndexKind.Hash,
                Value = IndexValue.Create("value"),
            });

        var action = () => indexes.AddRecord("record", record);

        action.Should().Throw<ArgumentException>();
        indexes.FindHashEntries("valid", IndexValue.Create("value")).Should().BeEmpty();
    }

    [Fact]
    public void ScopeValueAndRecordKeysUseOrdinalComparison()
    {
        var indexes = StoragePartitionIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
        var lower = CreateRecord(
            CreateHashEntry("scope", "value"),
            CreateRangeEntry("range", 10));
        var upperRecordKey = CreateRecord(
            CreateHashEntry("scope", "value"),
            CreateRangeEntry("range", 10));
        var upperScope = CreateRecord(
            CreateHashEntry("SCOPE", "value"),
            CreateRangeEntry("RANGE", 10));
        var upperValue = CreateRecord(CreateHashEntry("scope", "VALUE"));

        indexes.AddRecord("record", lower);
        indexes.AddRecord("RECORD", upperRecordKey);
        indexes.AddRecord("upper-scope", upperScope);
        indexes.AddRecord("upper-value", upperValue);

        indexes.FindHashEntries("scope", IndexValue.Create("value"))
            .Should().BeEquivalentTo(["record", "RECORD"]);
        indexes.FindHashEntries("SCOPE", IndexValue.Create("value"))
            .Should().ContainSingle().Which.Should().Be("upper-scope");
        indexes.FindHashEntries("Scope", IndexValue.Create("value")).Should().BeEmpty();
        indexes.FindHashEntries("scope", IndexValue.Create("VALUE"))
            .Should().ContainSingle().Which.Should().Be("upper-value");
        indexes.FindRangeEntries("range", IndexValue.FromSignedInteger(10))
            .Should().BeEquivalentTo(["record", "RECORD"]);
        indexes.FindRangeEntries("RANGE", IndexValue.FromSignedInteger(10))
            .Should().ContainSingle().Which.Should().Be("upper-scope");
        indexes.FindRangeEntries("Range", IndexValue.FromSignedInteger(10)).Should().BeEmpty();
    }

    private static StoredRecord CreateRecord(params IndexEntry[] entries)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("storage-partition-indexes-test", "record"),
            Payload = [],
            ETag = "1",
            IndexEntries = [.. entries],
        };
    }

    private static IndexEntry CreateHashEntry(string scope, string value)
    {
        return new IndexEntry
        {
            Scope = scope,
            Kind = SearchableIndexKind.Hash,
            Value = IndexValue.Create(value),
        };
    }

    private static IndexEntry CreateRangeEntry(string scope, long value)
    {
        return new IndexEntry
        {
            Scope = scope,
            Kind = SearchableIndexKind.Range,
            Value = IndexValue.FromSignedInteger(value),
        };
    }
}
