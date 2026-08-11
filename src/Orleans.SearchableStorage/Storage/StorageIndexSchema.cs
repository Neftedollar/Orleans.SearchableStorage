using Orleans.Runtime;

namespace Orleans.SearchableStorage.Storage;

internal static class StorageIndexSchema
{
    public const int ProtocolVersion = 1;
    public const int RebuildPageSize = 64;

    public static string CreateGrainKey(string providerName, string stateName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        var definition = Indexing.IndexSchemaIdentity.CreateControlKey(providerName, stateName);
        return Convert.ToHexString(definition);
    }

    public static StorageIndexSchemaRequest CreateRequest(
        Indexing.ISearchableStateRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        return new StorageIndexSchemaRequest
        {
            ProviderName = registration.ProviderName,
            StateName = registration.StateName,
            SchemaKey = [.. registration.Schema.SchemaKey],
            Fingerprint = [.. registration.Schema.Fingerprint],
            ProtocolVersion = ProtocolVersion,
        };
    }

    public static StorageIndexSchemaRequest CreateRequest(
        string providerName,
        Indexing.IndexSchemaDefinition schema)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(schema);
        return new StorageIndexSchemaRequest
        {
            ProviderName = providerName,
            StateName = schema.StateName,
            SchemaKey = [.. schema.SchemaKey],
            Fingerprint = [.. schema.Fingerprint],
            ProtocolVersion = ProtocolVersion,
        };
    }
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaRequest
{
    [Id(0)] public required string ProviderName { get; init; }
    [Id(1)] public required string StateName { get; init; }
    [Id(2)] public required byte[] SchemaKey { get; init; }
    [Id(3)] public required byte[] Fingerprint { get; init; }
    [Id(4)] public int ProtocolVersion { get; init; }
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaCommand
{
    [Id(0)] public required StorageIndexSchemaRequest Schema { get; init; }
    [Id(1)] public Guid RebuildId { get; init; }
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaSnapshot
{
    [Id(0)] public required string ProviderName { get; init; }
    [Id(1)] public required string StateName { get; init; }
    [Id(2)] public byte[]? ActiveFingerprint { get; init; }
    [Id(3)] public StorageIndexSchemaRebuildIntent? Rebuild { get; init; }
    [Id(4)] public long LastCompletedRecordCount { get; init; }
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaState
{
    [Id(0)] public bool Initialized { get; set; }
    [Id(1)] public int ProtocolVersion { get; set; }
    [Id(2)] public string ProviderName { get; set; } = string.Empty;
    [Id(3)] public string StateName { get; set; } = string.Empty;
    [Id(4)] public byte[]? ActiveFingerprint { get; set; }
    [Id(5)] public StorageIndexSchemaRebuildIntent? Rebuild { get; set; }
    [Id(6)] public long LastCompletedRecordCount { get; set; }

    public StorageIndexSchemaState Copy() => new()
    {
        Initialized = Initialized,
        ProtocolVersion = ProtocolVersion,
        ProviderName = ProviderName,
        StateName = StateName,
        ActiveFingerprint = ActiveFingerprint is null ? null : [.. ActiveFingerprint],
        Rebuild = Rebuild?.Copy(),
        LastCompletedRecordCount = LastCompletedRecordCount,
    };
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaRebuildIntent
{
    [Id(0)] public Guid RebuildId { get; set; }
    [Id(1)] public required byte[] SchemaKey { get; set; }
    [Id(2)] public required byte[] TargetFingerprint { get; set; }
    [Id(3)] public long LayoutEpoch { get; set; }
    [Id(4)] public required byte[] LayoutFingerprint { get; set; }
    [Id(5)] public int OwnerCount { get; set; }
    [Id(6)] public int NextProtocolOwnerIndex { get; set; }
    [Id(7)] public bool LayoutProtocolPublished { get; set; }
    [Id(8)] public int NextOwnerIndex { get; set; }
    [Id(9)] public bool HasAfter { get; set; }
    [Id(10)] public GrainId After { get; set; }
    [Id(11)] public long ProcessedRecordCount { get; set; }

    public StorageIndexSchemaRebuildIntent Copy() => new()
    {
        RebuildId = RebuildId,
        SchemaKey = [.. SchemaKey],
        TargetFingerprint = [.. TargetFingerprint],
        LayoutEpoch = LayoutEpoch,
        LayoutFingerprint = [.. LayoutFingerprint],
        OwnerCount = OwnerCount,
        NextProtocolOwnerIndex = NextProtocolOwnerIndex,
        LayoutProtocolPublished = LayoutProtocolPublished,
        NextOwnerIndex = NextOwnerIndex,
        HasAfter = HasAfter,
        After = After,
        ProcessedRecordCount = ProcessedRecordCount,
    };
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaRebuildPageRequest
{
    [Id(0)] public required string ProviderName { get; init; }
    [Id(1)] public required string StateName { get; init; }
    [Id(2)] public required byte[] SchemaKey { get; init; }
    [Id(3)] public required byte[] TargetFingerprint { get; init; }
    [Id(4)] public long LayoutEpoch { get; init; }
    [Id(5)] public bool HasAfter { get; init; }
    [Id(6)] public GrainId After { get; init; }
    [Id(7)] public int PageSize { get; init; }
    [Id(8)] public required StoragePersistenceSettings Persistence { get; init; }
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaRebuildPageResult
{
    [Id(0)] public bool Exhausted { get; init; }
    [Id(1)] public bool HasAfter { get; init; }
    [Id(2)] public GrainId After { get; init; }
    [Id(3)] public int ProcessedRecordCount { get; init; }
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaPartitionProtocolRequest
{
    [Id(0)] public int ProtocolVersion { get; init; }
    [Id(1)] public required string ProviderName { get; init; }
    [Id(2)] public long LayoutEpoch { get; init; }
    [Id(3)] public required byte[] LayoutFingerprint { get; init; }
    [Id(4)] public required StoragePersistenceSettings Persistence { get; init; }
}

[GenerateSerializer]
internal sealed class StorageIndexSchemaLayoutProtocolRequest
{
    [Id(0)] public int ProtocolVersion { get; init; }
    [Id(1)] public long LayoutEpoch { get; init; }
    [Id(2)] public required byte[] LayoutFingerprint { get; init; }
    [Id(3)] public Guid EnablementId { get; init; }
}

internal interface IStorageIndexSchemaGrain : IGrainWithStringKey
{
    Task<StorageIndexSchemaSnapshot> GetAsync(StorageIndexSchemaRequest request);
    Task<StorageIndexSchemaSnapshot> BeginRebuildAsync(StorageIndexSchemaRequest request);
    Task<StorageIndexSchemaSnapshot> AdvanceRebuildAsync(StorageIndexSchemaCommand command);
}
