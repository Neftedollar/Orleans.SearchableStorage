using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePartitionCompactIndexTests
{
    [Fact]
    public void RemovedRecordReferenceIsReusedOnlyAfterItStopsResolving()
    {
        var recordRefs = StoragePartitionRecordRefs.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal));
        var first = CreateRecord("first", []);
        var second = CreateRecord("second", []);

        var firstRef = recordRefs.Add("first", first);
        recordRefs.Remove("first", first);

        Action resolveRemoved = () => recordRefs.GetRecord(firstRef);
        resolveRemoved.Should().Throw<InvalidOperationException>();

        var secondRef = recordRefs.Add("second", second);

        secondRef.Should().Be(firstRef);
        recordRefs.GetRecordKey(secondRef).Should().Be("second");
        recordRefs.GetRecord(secondRef).Should().BeSameAs(second);
    }

    [Fact]
    public void DuplicateGrainGroupCollapsesBackToInlineSingleton()
    {
        var first = CreateRecord("shared", []);
        var second = CreateRecord("shared", []);
        var records = new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
        {
            ["first"] = first,
            ["second"] = second,
        };
        var recordRefs = StoragePartitionRecordRefs.Build(records);
        var firstRef = recordRefs.GetRequiredRef("first");
        var secondRef = recordRefs.GetRequiredRef("second");
        var group = new OrderedGrainGroup(first.GrainId);

        group.AddRecordRef(firstRef, recordRefs.RecordKeyComparer).Should().BeTrue();
        group.AddRecordRef(secondRef, recordRefs.RecordKeyComparer).Should().BeTrue();
        group.MultipleRecordRefs.Should().NotBeNull();
        group.RecordRefCount.Should().Be(2);

        group.RemoveRecordRef(firstRef).Should().BeTrue();

        group.MultipleRecordRefs.Should().BeNull();
        group.SingleRecordRef.Should().Be(secondRef);
        group.RecordRefs.Should().ContainSingle().Which.Should().Be(secondRef);
    }

    [Fact]
    public void BuildSharesEqualScopeAndValueInstancesAcrossStoredRecords()
    {
        var firstScope = new string("compact/city".ToCharArray());
        var secondScope = new string("compact/city".ToCharArray());
        var firstValue = IndexValue.Create(new string("Helsinki".ToCharArray()));
        var secondValue = IndexValue.Create(new string("Helsinki".ToCharArray()));
        var first = CreateRecord(
            "first",
            [CreateEntry(firstScope, firstValue)]);
        var second = CreateRecord(
            "second",
            [CreateEntry(secondScope, secondValue)]);

        firstScope.Should().NotBeSameAs(secondScope);
        firstValue.Should().NotBeSameAs(secondValue);

        _ = StoragePartitionOrderedIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["first"] = first,
                ["second"] = second,
            });

        first.IndexEntries[0].Scope.Should().BeSameAs(second.IndexEntries[0].Scope);
        first.IndexEntries[0].Value.Should().BeSameAs(second.IndexEntries[0].Value);
    }

    [Fact]
    public void CanonicalSharingDoesNotChangeDurableRecordBytes()
    {
        var first = CreateRecord(
            "first",
            [CreateEntry(new string("compact/city".ToCharArray()), IndexValue.Create("Helsinki"))]);
        var second = CreateRecord(
            "second",
            [CreateEntry(new string("compact/city".ToCharArray()), IndexValue.Create("Helsinki"))]);
        var firstBefore = StorageMoveRecordCodec.Encode("first", first);
        var secondBefore = StorageMoveRecordCodec.Encode("second", second);

        _ = StoragePartitionOrderedIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["first"] = first,
                ["second"] = second,
            });

        StorageMoveRecordCodec.BinaryEquals(
            firstBefore,
            StorageMoveRecordCodec.Encode("first", first)).Should().BeTrue();
        StorageMoveRecordCodec.BinaryEquals(
            secondBefore,
            StorageMoveRecordCodec.Encode("second", second)).Should().BeTrue();
    }

    [Fact]
    public void SemanticBucketEqualityNeverCanonicalizesDifferentDurableValueBits()
    {
        var firstValue = new IndexValue
        {
            Kind = IndexValueKind.SignedInteger,
            SignedInteger = 1,
            Text = "inactive-a",
            Decimal = 1.0m,
            FloatingPoint = 0d,
        };
        var secondValue = new IndexValue
        {
            Kind = IndexValueKind.SignedInteger,
            SignedInteger = 1,
            Text = new string('z', 256),
            Decimal = 1.00m,
            FloatingPoint = BitConverter.Int64BitsToDouble(long.MinValue),
        };
        firstValue.CompareTo(secondValue).Should().Be(0);
        var first = CreateRecord("first", [CreateEntry("compact/value", firstValue)]);
        var second = CreateRecord("second", [CreateEntry("compact/value", secondValue)]);
        var firstBefore = StorageMoveRecordCodec.Encode("first", first);
        var secondBefore = StorageMoveRecordCodec.Encode("second", second);

        _ = StoragePartitionOrderedIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                ["first"] = first,
                ["second"] = second,
            });

        StorageMoveRecordCodec.BinaryEquals(
            firstBefore,
            StorageMoveRecordCodec.Encode("first", first)).Should().BeTrue();
        StorageMoveRecordCodec.BinaryEquals(
            secondBefore,
            StorageMoveRecordCodec.Encode("second", second)).Should().BeTrue();
        first.IndexEntries[0].Value.Should().NotBeSameAs(second.IndexEntries[0].Value);
    }

    private static StoredRecord CreateRecord(string key, List<IndexEntry> entries)
    {
        return new StoredRecord
        {
            GrainId = GrainId.Create("compact-index", key),
            Payload = [],
            ETag = "1",
            IndexEntries = entries,
        };
    }

    private static IndexEntry CreateEntry(string scope, IndexValue value)
    {
        return new IndexEntry
        {
            Scope = scope,
            Kind = SearchableIndexKind.Hash,
            Value = value,
        };
    }
}
