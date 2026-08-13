# Index-only mode

Index-only mode keeps the searchable index in Orleans.SearchableStorage while application payloads
remain in storage chosen and owned by the application. It is an explicit alternative to the
integrated `IGrainStorage` mode, not a switch on an existing provider namespace.

## Choose the ownership model

| Mode | Registration | Keyed write capability | Payload owner |
| --- | --- | --- | --- |
| Integrated storage | `AddSearchableGrainStorage` | Orleans `IGrainStorage` through `IPersistentState<T>` | Orleans.SearchableStorage stores the serialized payload and its indexes in one journaled mutation. |
| Index only | `AddSearchableIndex` | `ISearchableStorageIndexWriter` | The application stores and hydrates the payload elsewhere; Orleans.SearchableStorage persists only derived index data. |

Both registrations expose keyed `ISearchableStorageQueryClient`, `ISearchableStorageClient`, and
`ISearchableStorageAdminClient`. `AddSearchableIndex` deliberately does not register `IGrainStorage`,
and `AddSearchableGrainStorage` deliberately does not register the index writer. The two modes can
coexist in one silo under different provider names. A provider name cannot be registered or opened
in both modes.

The same `IServiceCollection.AddSearchableIndex` path configures an external Orleans client. Resolve
its keyed query/admin services from that client service provider. The public
`SearchableStorageClient` and `SearchableStorageAdminClient` constructors remain integrated-only;
using the keyed registration is the type-safe way to select the format-6 identity without a public
mode flag.

## Register and write an index

Register the physical provider used for the index internals, the index namespace, and every state
schema:

```csharp
siloBuilder.AddMemoryGrainStorage(
    SearchableStorageConstants.PhysicalStorageProviderName);

siloBuilder.AddSearchableIndex(
    "CompanyIndex",
    options => options.PartitionCount = 32);

siloBuilder.AddSearchableStorageState<CompanyState>(
    "CompanyIndex",
    "company",
    applicationSchemaVersion: 1);
```

An external Orleans client uses the corresponding service-collection overloads:

```csharp
clientBuilder.Services.AddSearchableIndex(
    "CompanyIndex",
    options => options.PartitionCount = 32);
clientBuilder.Services.AddSearchableStorageState<CompanyState>(
    "CompanyIndex",
    "company",
    applicationSchemaVersion: 1);

var search = clientServices.GetRequiredKeyedService<ISearchableStorageQueryClient>(
    "CompanyIndex");
```

The writer accepts the same application state type used by the external payload store. It invokes
the cached getters for properties marked with `[SearchableIndex]`, converts those values into index
entries, and discards the object after the call. It does not serialize or retain the application
payload, including unindexed properties.

The application can inject its ordinary state store and the index writer into the same grain:

```csharp
public sealed class CompanyGrain(
    [PersistentState("company", "ApplicationState")]
    IPersistentState<CompanyState> state,
    [FromKeyedServices("CompanyIndex")]
    ISearchableStorageIndexWriter index) : Grain
{
    public async Task SaveAsync(CompanyState value)
    {
        state.State = value;
        await state.WriteStateAsync();
        await index.UpsertAsync("company", this.GetGrainId(), value);
    }

    public async Task ClearAsync()
    {
        await state.ClearStateAsync();
        await index.RemoveAsync<CompanyState>("company", this.GetGrainId());
    }
}
```

`UpsertAsync` unconditionally replaces every indexed value for the `(stateName, GrainId)` key.
`RemoveAsync` unconditionally removes that key and succeeds when it is already absent. Exact retries
therefore converge to the same visible index state, although an acknowledged-or-not retry may append
another durable journal entry.

## Consistency belongs to the caller

The payload write and index mutation are two independent operations. The library does not provide a
cross-store transaction, outbox, source version, mutation identifier, tombstone, stale-event check,
or deduplication layer. Calls are applied in the order in which the owning index partition receives
them; the last arrival wins. A delayed older event can therefore replace a newer index value.

The application must choose and implement its required consistency policy. Common choices include:

- serialize payload and index changes per logical key when the grain is the sole writer;
- persist an application outbox with the payload, deliver it in key order, and retry until the index
  call succeeds;
- run a periodic reconciliation or complete replay from the authoritative payload store.

Cancellation stops the caller waiting for an Orleans request but cannot recall a mutation which has
already reached a partition. After an ambiguous failure, retry the intended final state or reconcile
from the authoritative store. Do not infer that the index mutation was not committed from the
client-side exception alone.

Queries return matching `GrainId` values under the same bounded query and continuation contracts as
integrated storage. The application hydrates those identifiers from its external payload store and
must tolerate the chosen index-projection lag—for example, a recently returned identifier whose
payload was deleted before a delayed index removal arrived.

## Schema lifecycle

Index-only mode still requires the managed schema to be active before writes or queries. On a fresh
namespace, run `RebuildIndexSchemaAsync<TState>` for every registered state. This initializes the
empty index layout and activates its declared fingerprint; there are no payload records to scan.
Repeating the operation with the same active fingerprint is safe.

After a fingerprint is active, an incompatible rebuild is rejected. The library cannot derive new
entries because it deliberately retained no payloads. To change indexed fields, index kinds, CLR
domains, codec meaning, or the application schema version:

1. register a new index-only provider name with the new declarations;
2. activate every schema in that empty namespace;
3. replay the authoritative external corpus through its keyed writer;
4. validate the new namespace and switch query consumers;
5. retire the old namespace only under the application's reviewed retention and rollback policy.

This is a namespace replacement and replay, not the integrated provider's in-place payload rebuild.
See the [managed schema lifecycle](index-schema-lifecycle.md) for the shared gates and the mode-specific
procedure.

## Durable boundary

An index-only namespace uses layout and partition-persistence format 6. A null durable payload is
reserved for that mode; an integrated record still has a non-null payload even when its serialized
byte array is empty. Recovery, snapshots, journals, compaction, and movement preserve and validate
that distinction.

Format 6 is also a downgrade fence: binaries which understand only integrated formats 3 through 5
must not open an index-only namespace. It uses the same virtual-slot derivation, assignment, and
movement algorithms but a deliberately distinct routing-fingerprint and continuation domain. A token
from an otherwise identical integrated namespace therefore cannot resume against index-only mode.
Use a homogeneous rollout for every participant which accesses it, and back up its layout, schema
controls, manifests, journals, and snapshots as one namespace. The externally owned payload store
requires its own backup and recovery policy.
