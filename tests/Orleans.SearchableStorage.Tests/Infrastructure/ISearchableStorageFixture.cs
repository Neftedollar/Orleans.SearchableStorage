using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

public interface ISearchableStorageFixture
{
    TestCluster Cluster { get; }

    int PartitionCount { get; }
}
