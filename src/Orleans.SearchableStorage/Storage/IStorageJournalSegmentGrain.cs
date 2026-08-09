namespace Orleans.SearchableStorage.Storage;

internal interface IStorageJournalSegmentGrain : IGrainWithStringKey
{
    Task StoreAsync(
        StorageJournalEntry entry,
        long committedSequence,
        Guid committedOperationId,
        long absoluteSegmentIndex,
        int segmentCapacity);

    Task<StorageJournalSegmentState> ReadAsync();

    Task RetireAsync(long absoluteSegmentIndex);
}
