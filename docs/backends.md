# Physical storage backends

Orleans.SearchableStorage stores each durable layout and partition through the Orleans provider registered as `SearchableStorageConstants.PhysicalStorageProviderName`. Application grains continue to reference the searchable provider name; changing the physical provider does not change their `PersistentState` attributes or query calls.

The repository contract suite validates the official Orleans 10.2.2 providers for PostgreSQL, Redis, and Azure Blob Storage. Backend independence means that the searchable record, index, layout, ETag, reactivation, and failure-boundary semantics are identical. It does not make the latency, capacity, backup, or availability properties of those systems identical.

Register exactly one physical provider before the searchable provider.

## PostgreSQL

Reference `Microsoft.Orleans.Persistence.AdoNet` and the Npgsql ADO.NET driver. Install the official Orleans [`PostgreSQL-Main.sql`](https://github.com/dotnet/orleans/blob/v10.2.2/src/AdoNet/Shared/PostgreSQL-Main.sql) and [`PostgreSQL-Persistence.sql`](https://github.com/dotnet/orleans/blob/v10.2.2/src/AdoNet/Orleans.Persistence.AdoNet/PostgreSQL-Persistence.sql) scripts in the target database. The operational SQL copied into this repository retains its pinned source URL and the full upstream MIT notice inline.

```csharp
siloBuilder.AddAdoNetGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName,
    options =>
    {
        options.Invariant = "Npgsql";
        options.ConnectionString = configuration.GetConnectionString("SearchableStorage")!;
        options.DeleteStateOnClear = true;
    });

siloBuilder.AddSearchableGrainStorage(
    "Searchable",
    options => options.PartitionCount = 32);
```

The database schema and the configured search path are deployment concerns. The integration fixture creates an isolated schema per run, loads the scripts from the Orleans 10.2.2 tag, and drops that schema after the contract completes.

## Redis

Reference `Microsoft.Orleans.Persistence.Redis`. Do not configure `EntryExpiry` for durable production data: the Orleans provider documents it as an ephemeral-environment option which can allow state to expire while an activation still exists.

```csharp
siloBuilder.AddRedisGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName,
    options =>
    {
        options.ConfigurationOptions = ConfigurationOptions.Parse(
            configuration.GetConnectionString("SearchableStorage")!);
        options.DeleteStateOnClear = true;
    });

siloBuilder.AddSearchableGrainStorage(
    "Searchable",
    options => options.PartitionCount = 32);
```

Redis grain-state keys are scoped by the Orleans service id. Keep `ClusterOptions.ServiceId` stable across restarts which must see the same data.

## Azure Blob Storage

Reference `Microsoft.Orleans.Persistence.AzureStorage`. The same configuration works with an authenticated Azure `BlobServiceClient`; the integration suite supplies an Azurite connection string.

```csharp
siloBuilder.AddAzureBlobGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName,
    options =>
    {
        options.BlobServiceClient = new BlobServiceClient(
            configuration.GetConnectionString("SearchableStorage")!);
        options.ContainerName = "searchable-storage";
        options.DeleteStateOnClear = true;
    });

siloBuilder.AddSearchableGrainStorage(
    "Searchable",
    options => options.PartitionCount = 32);
```

Use a dedicated valid Azure container name for the searchable storage namespace and include it in backup and retention policy.

## Run the backend contract locally

The repository includes pinned PostgreSQL, Redis, and Azurite containers:

```bash
docker compose --file tests/backends.compose.yml up --detach --wait

ORLEANS_SEARCHABLE_STORAGE_RUN_BACKEND_TESTS=true \
  dotnet test tests/Orleans.SearchableStorage.Tests \
  --filter "Category=BackendIntegration"

docker compose --file tests/backends.compose.yml down --volumes
```

The explicit opt-in prevents a normal unit-test run from contacting local infrastructure. With the opt-in enabled, these variables override the compose defaults:

| Variable | Default |
| --- | --- |
| `ORLEANS_SEARCHABLE_STORAGE_POSTGRES_CONNECTION_STRING` | `Host=127.0.0.1;Port=5432;Database=postgres;Username=postgres;Password=postgres` |
| `ORLEANS_SEARCHABLE_STORAGE_REDIS_CONNECTION_STRING` | `127.0.0.1:6379,abortConnect=false` |
| `ORLEANS_SEARCHABLE_STORAGE_AZURE_BLOB_CONNECTION_STRING` | `UseDevelopmentStorage=true` |

The pinned Azurite 3.36.0 container implements API version `2025-11-05`, while Azure.Storage.Blobs 12.27.0 defaults to `2026-02-06`. It therefore uses `--skipApiVersionCheck` so the newer header can reach Azurite's implemented Blob operations. The storage contract still exercises and asserts every operation; this setting does not add unsupported Azure behavior. Remove the flag after the pinned emulator natively supports `2026-02-06`.

Use disposable test resources. The fixtures create unique PostgreSQL schemas, Orleans service ids, and Blob containers, then remove their data after the run. Provider-specific contract cases seed both owned and unrelated sentinels and prove that cleanup removes only the owned namespace. The compose command removes the container volumes as a final cleanup.
