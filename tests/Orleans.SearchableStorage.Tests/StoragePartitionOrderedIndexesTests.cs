using AwesomeAssertions;
using Orleans.Runtime;
using Orleans.SearchableStorage.Indexing;
using Orleans.SearchableStorage.Querying;
using Orleans.SearchableStorage.Storage;

namespace Orleans.SearchableStorage.Tests;

public sealed class StoragePartitionOrderedIndexesTests
{
    private const string StateName = "ordered-state";
    private const string CityScope = "ordered/city";
    private const string SalaryScope = "ordered/salary";

    [Fact]
    public void ActivationBuildSortsCatalogAndExactPostingsByCanonicalGrainId()
    {
        var grainIds = new[]
        {
            GrainId.Create("z-type", "middle"),
            GrainId.Create("a-type", "last"),
            GrainId.Create("a-type", "first"),
            GrainId.Create("m-type", "key"),
        };
        var records = grainIds
            .Reverse()
            .ToDictionary(
                CreateRecordKey,
                grainId => CreateRecord(grainId, "Helsinki", 10),
                StringComparer.Ordinal);

        var indexes = StoragePartitionOrderedIndexes.Build(records);

        var expected = grainIds.Order(GrainIdCanonicalOrder.Comparer).ToArray();
        indexes.GetStateCatalog(StateName).CopyGrainIds().Should().Equal(expected);
        indexes.GetExactPosting(CityScope, SearchableIndexKind.Hash, IndexValue.Create("Helsinki"))
            .CopyGrainIds().Should().Equal(expected);
        indexes.GetExactPosting(SalaryScope, SearchableIndexKind.Range, IndexValue.Create(10))
            .CopyGrainIds().Should().Equal(expected);
    }

    [Fact]
    public void DuplicateRecordKeysForOneGrainFormOneCanonicalCandidateGroup()
    {
        var grainId = GrainId.Create("ordered-type", "duplicate");
        var canonicalKey = CreateRecordKey(grainId);
        var duplicateKey = string.Concat(canonicalKey, "-duplicate");
        var indexes = StoragePartitionOrderedIndexes.Build(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                [canonicalKey] = CreateRecord(grainId, "Helsinki", 10),
                [duplicateKey] = CreateRecord(grainId, "Helsinki", 10),
            });

        var catalog = indexes.GetStateCatalog(StateName);
        var posting = indexes.GetExactPosting(
            CityScope,
            SearchableIndexKind.Hash,
            IndexValue.Create("Helsinki"));

        catalog.CopyGrainIds().Should().ContainSingle().Which.Should().Be(grainId);
        posting.CopyGrainIds().Should().ContainSingle().Which.Should().Be(grainId);
        catalog.TryGetRecordKeys(grainId, out var recordKeys).Should().BeTrue();
        recordKeys.Should().Equal(canonicalKey, duplicateKey);
        indexes.GetFacetRecordCount(CityScope, SearchableIndexKind.Hash).Should().Be(2);
        indexes.GetFacetRecordCount(SalaryScope, SearchableIndexKind.Range).Should().Be(2);
    }

    [Fact]
    public void UpsertAndDeleteKeepCatalogHashAndRangePostingsEquivalentToLiveRecords()
    {
        var grainId = GrainId.Create("ordered-type", "mutable");
        var recordKey = CreateRecordKey(grainId);
        var original = CreateRecord(grainId, "Helsinki", 10);
        var replacement = CreateRecord(grainId, "London", 20);
        var view = new StoragePartitionView(
            new Dictionary<string, StoredRecord>(StringComparer.Ordinal)
            {
                [recordKey] = original,
            });

        view.ApplyUpsert(recordKey, replacement);

        view.OrderedIndexes.GetStateCatalog(StateName).CopyGrainIds()
            .Should().ContainSingle().Which.Should().Be(grainId);
        view.OrderedIndexes.GetExactPosting(
            CityScope,
            SearchableIndexKind.Hash,
            IndexValue.Create("Helsinki")).CopyGrainIds().Should().BeEmpty();
        view.OrderedIndexes.GetExactPosting(
            SalaryScope,
            SearchableIndexKind.Range,
            IndexValue.Create(10)).CopyGrainIds().Should().BeEmpty();
        view.OrderedIndexes.GetExactPosting(
            CityScope,
            SearchableIndexKind.Hash,
            IndexValue.Create("London")).CopyGrainIds()
            .Should().ContainSingle().Which.Should().Be(grainId);
        view.OrderedIndexes.GetExactPosting(
            SalaryScope,
            SearchableIndexKind.Range,
            IndexValue.Create(20)).CopyGrainIds()
            .Should().ContainSingle().Which.Should().Be(grainId);
        view.OrderedIndexes.GetFacetRecordCount(CityScope, SearchableIndexKind.Hash).Should().Be(1);
        view.OrderedIndexes.GetFacetRecordCount(SalaryScope, SearchableIndexKind.Range).Should().Be(1);

        view.ApplyDelete(recordKey);

        view.Records.Should().BeEmpty();
        view.OrderedIndexes.GetStateCatalog(StateName).CopyGrainIds().Should().BeEmpty();
        view.OrderedIndexes.GetExactPosting(
            CityScope,
            SearchableIndexKind.Hash,
            IndexValue.Create("London")).CopyGrainIds().Should().BeEmpty();
        view.OrderedIndexes.GetExactPosting(
            SalaryScope,
            SearchableIndexKind.Range,
            IndexValue.Create(20)).CopyGrainIds().Should().BeEmpty();
        view.OrderedIndexes.GetFacetRecordCount(CityScope, SearchableIndexKind.Hash).Should().Be(0);
        view.OrderedIndexes.GetFacetRecordCount(SalaryScope, SearchableIndexKind.Range).Should().Be(0);
    }

    [Fact]
    public void ExclusiveSeekReturnsTheFirstStrictlyGreaterCanonicalGroup()
    {
        var grainIds = Enumerable.Range(0, 8)
            .Select(index => GrainId.Create("ordered-type", $"key-{index:D2}"))
            .ToArray();
        var records = grainIds.ToDictionary(
            CreateRecordKey,
            grainId => CreateRecord(grainId, "Helsinki", 10),
            StringComparer.Ordinal);
        var catalog = StoragePartitionOrderedIndexes.Build(records).GetStateCatalog(StateName);

        ReadCursor(catalog, hasAfter: false, default).Should().Equal(grainIds);
        for (var index = 0; index < grainIds.Length; index++)
        {
            ReadCursor(catalog, hasAfter: true, grainIds[index])
                .Should().Equal(grainIds[(index + 1)..]);
        }
    }

    [Fact]
    public void CatalogAndPostingsUseBalancedTreesInsteadOfShiftBasedSortedLists()
    {
        AssertUsesBalancedTree(OrderedGrainGroupsFields());
        AssertUsesBalancedTree(OrderedRangeIndexFields());

        static Type[] OrderedGrainGroupsFields() => typeof(OrderedGrainGroups).GetFields(
                System.Reflection.BindingFlags.Instance
                | System.Reflection.BindingFlags.NonPublic)
            .Select(static field => field.FieldType)
            .ToArray();

        static Type[] OrderedRangeIndexFields() => typeof(OrderedRangeIndex).GetFields(
                System.Reflection.BindingFlags.Instance
                | System.Reflection.BindingFlags.NonPublic)
            .Select(static field => field.FieldType)
            .ToArray();

        static void AssertUsesBalancedTree(IEnumerable<Type> fieldTypes)
        {
            fieldTypes.Should().Contain(type => type.IsGenericType
                && type.GetGenericTypeDefinition() == typeof(SortedSet<>));
            fieldTypes.Should().NotContain(type => type.IsGenericType
                && type.GetGenericTypeDefinition() == typeof(SortedList<,>));
        }
    }

    [Fact]
    public void RangeBucketCursorSeeksIntoOnlyTheRequestedOrderedWindow()
    {
        var records = Enumerable.Range(1, 4)
            .Select(static index => index * 10)
            .Select(value =>
            {
                var grainId = GrainId.Create("ordered-type", $"range-{value:D2}");
                return (CreateRecordKey(grainId), CreateRecord(grainId, "Helsinki", value));
            })
            .ToDictionary(static pair => pair.Item1, static pair => pair.Item2, StringComparer.Ordinal);
        var indexes = StoragePartitionOrderedIndexes.Build(records);

        var selection = indexes.CreateRangeBucketCursor(
            SalaryScope,
            IndexValue.Create(20),
            IndexValue.Create(30));

        selection.TotalBucketCount.Should().Be(4);
        using var cursor = selection.Cursor;
        var values = new List<long>();
        while (cursor.HasCurrent)
        {
            cursor.TakeCurrentAndAdvance(out var bucket).Should().BeTrue();
            values.Add(bucket.Value.SignedInteger);
        }

        values.Should().HaveCount(2);
        values[0].Should().Be(20);
        values[1].Should().Be(30);
    }

    [Fact]
    public void IncrementalMutationRemainsEquivalentToFreshRebuildAcrossCatalogAndPostings()
    {
        var live = new Dictionary<string, StoredRecord>(StringComparer.Ordinal);
        var view = new StoragePartitionView(live);
        var random = new Random(0x5eed);
        var keys = Enumerable.Range(0, 24)
            .Select(index => GrainId.Create("ordered-type", $"property-{index:D2}"))
            .Select(grainId => (GrainId: grainId, RecordKey: CreateRecordKey(grainId)))
            .ToArray();
        var cities = new[] { "Helsinki", "London", "Oslo" };
        var salaries = new[] { 10, 20, 30, 40 };

        for (var operation = 0; operation < 160; operation++)
        {
            var target = keys[random.Next(keys.Length)];
            if (random.Next(4) == 0)
            {
                view.ApplyDelete(target.RecordKey);
            }
            else
            {
                view.ApplyUpsert(
                    target.RecordKey,
                    CreateRecord(
                        target.GrainId,
                        cities[random.Next(cities.Length)],
                        salaries[random.Next(salaries.Length)]));
            }

            if (operation % 11 == 0 || operation == 159)
            {
                AssertEquivalentToRebuild(view, cities, salaries);
            }
        }
    }

    private static void AssertEquivalentToRebuild(
        StoragePartitionView view,
        IEnumerable<string> cities,
        IEnumerable<int> salaries)
    {
        var rebuilt = StoragePartitionOrderedIndexes.Build(view.Records);
        view.OrderedIndexes.GetStateCatalog(StateName).CopyGrainIds()
            .Should().Equal(rebuilt.GetStateCatalog(StateName).CopyGrainIds());
        view.OrderedIndexes.GetFacetRecordCount(CityScope, SearchableIndexKind.Hash)
            .Should().Be(rebuilt.GetFacetRecordCount(CityScope, SearchableIndexKind.Hash));
        view.OrderedIndexes.GetFacetRecordCount(SalaryScope, SearchableIndexKind.Range)
            .Should().Be(rebuilt.GetFacetRecordCount(SalaryScope, SearchableIndexKind.Range));
        ReadFacetBuckets(view.OrderedIndexes, CityScope, SearchableIndexKind.Hash)
            .Should().Equal(ReadFacetBuckets(rebuilt, CityScope, SearchableIndexKind.Hash));
        ReadFacetBuckets(view.OrderedIndexes, SalaryScope, SearchableIndexKind.Range)
            .Should().Equal(ReadFacetBuckets(rebuilt, SalaryScope, SearchableIndexKind.Range));

        foreach (var city in cities)
        {
            view.OrderedIndexes.GetExactPosting(
                    CityScope,
                    SearchableIndexKind.Hash,
                    IndexValue.Create(city)).CopyGrainIds()
                .Should().Equal(rebuilt.GetExactPosting(
                    CityScope,
                    SearchableIndexKind.Hash,
                    IndexValue.Create(city)).CopyGrainIds());
        }

        foreach (var salary in salaries)
        {
            view.OrderedIndexes.GetExactPosting(
                    SalaryScope,
                    SearchableIndexKind.Range,
                    IndexValue.Create(salary)).CopyGrainIds()
                .Should().Equal(rebuilt.GetExactPosting(
                    SalaryScope,
                    SearchableIndexKind.Range,
                    IndexValue.Create(salary)).CopyGrainIds());
        }
    }

    private static GrainId[] ReadCursor(
        OrderedGrainGroups groups,
        bool hasAfter,
        GrainId after)
    {
        using var cursor = groups.CreateCursorAfter(hasAfter, after);
        var result = new List<GrainId>();
        while (cursor.HasCurrent)
        {
            cursor.TakeCurrentAndAdvance(out var grainId).Should().BeTrue();
            result.Add(grainId);
        }

        return [.. result];
    }

    private static (IndexValue Value, int RawCount)[] ReadFacetBuckets(
        StoragePartitionOrderedIndexes indexes,
        string scope,
        SearchableIndexKind kind)
    {
        using var cursor = indexes.CreateFacetValueCursor(scope, kind, after: null);
        var result = new List<(IndexValue, int)>();
        while (cursor.HasCurrent)
        {
            cursor.TakeCurrentAndAdvance(out var bucket).Should().BeTrue();
            result.Add((bucket.Value, bucket.Posting.RecordCount));
        }

        return [.. result];
    }

    private static StoredRecord CreateRecord(GrainId grainId, string city, int salary)
    {
        return new StoredRecord
        {
            GrainId = grainId,
            Payload = [],
            ETag = "1",
            IndexEntries =
            [
                new IndexEntry
                {
                    Scope = CityScope,
                    Kind = SearchableIndexKind.Hash,
                    Value = IndexValue.Create(city),
                },
                new IndexEntry
                {
                    Scope = SalaryScope,
                    Kind = SearchableIndexKind.Range,
                    Value = IndexValue.Create(salary),
                },
            ],
        };
    }

    private static string CreateRecordKey(GrainId grainId)
    {
        return string.Concat(
            StateName,
            "/",
            Convert.ToHexString(grainId.Type.AsSpan()),
            "/",
            Convert.ToHexString(grainId.Key.AsSpan()));
    }
}
