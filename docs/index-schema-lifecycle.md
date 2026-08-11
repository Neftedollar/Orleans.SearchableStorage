# Managed index schema lifecycle

Searchable index entries are derived data, but their interpretation is durable. An indexed property
name, index kind, CLR value domain, canonical codec, state type, state name, and application-owned
schema version together identify one index generation. Changing the index/type/version identity
without rebuilding can mix incompatible entries or make older records disappear from queries.

Renaming the Orleans persistent `stateName` is different: record keys, the partition catalog, and
the schema-control key all contain that name. A rebuild under the new name cannot discover records
stored under the old one. Keep the state name stable or perform an explicit data migration/record
rewrite which also accounts for the old control document; this runbook does not rename state data.

This runbook describes the version-1 managed-schema protocol. It is a quiesced, resumable rebuild,
not online DDL and not a general data-migration framework.

## The capability is provider-wide and one-way

Managed schemas are optional only until the first rebuild starts for a provider namespace. That
rebuild durably fences schema maintenance in the layout, upgrades every current partition owner to
persistence format 5, scans every record belonging to that rebuild's state name, and only then
publishes the schema capability. The final publication clears the layout intent, and the next
control turn activates that state's fingerprint. It does not activate any other registered state.
Complete every registered state's rebuild in the same first-adoption maintenance window before
resuming provider traffic or movement. Later-generation rebuilds use the same layout maintenance
fence even though the provider capability is already published. Participant upgrades and the
provider capability are one-way; there is no disable or binary-rollback operation.

After the capability is enabled, every Orleans state name stored through that searchable provider
must have exactly one CLR type and schema version registered on every silo. This includes state
types with no indexed properties. Every direct query client must declare every state name it uses.
Updated participants which declare managed schemas for the provider reject any state missing from
their local declaration set immediately. Routed point reads remain available because they do not
interpret index entries, but writes, clears, one-shot queries, pages, and facets require an active
generation. A process with no declarations at all for that provider is schema-unaware even when its
binary contains the feature; exclude it during the same homogeneous restart verification as an old
binary. Schema-unaware calls either perform a provider-capability check or reach an upgraded
partition which rejects their unbound RPC. A genuinely older binary can also answer a contradictory
query locally without issuing any RPC, so no server-side fence can reject that particular operation.
This is why verification that no schema-unaware participant remains is mandatory before the first
rebuild begins.

Treat the first call to `RebuildIndexSchemaAsync` as the irreversible provider migration point. If
it is interrupted while owners are being upgraded, some old calls may already be rejected before
the layout capability is visible. Keep traffic paused, resume the same rebuild, and then finish the
remaining registered states before reopening the provider.

## Declare every participant

Register each provider/state pair on every silo during startup. Increment the positive application
version when index meaning changes in a way that the declarations cannot express, such as a changed
normalization rule:

```csharp
siloBuilder.AddSearchableGrainStorage("Searchable", options =>
{
    options.PartitionCount = 32;
});

siloBuilder.AddSearchableStorageState<VacancyState>(
    "Searchable",
    "vacancy",
    applicationSchemaVersion: 2);
```

The fingerprint changes automatically when the state type identity, state name, sorted index
names, index kinds, CLR value identities, or built-in codec versions change. Runtime values and
serialized application records are not fingerprint inputs. `applicationSchemaVersion` exists for
semantic changes outside those structural inputs; increment it deliberately, not on every deploy.

Registration is deliberately fail-closed, not a passive compatibility declaration. From startup,
writes, clears, and queries for that registered state require its exact fingerprint to be active in
the control document. Deploy new or changed declarations only inside the quiesced adoption window;
registration by itself does not backfill records or activate the generation.

An Orleans client constructed outside the silo DI container must supply its own complete registry.
The client captures a snapshot of the registry in its constructor, so finish configuring it first:

```csharp
var schemas = new SearchableStorageSchemaRegistry()
    .AddState<VacancyState>("vacancy", applicationSchemaVersion: 2)
    .AddState<CompanyState>("company", applicationSchemaVersion: 1);

var search = new SearchableStorageClient(
    grainFactory,
    providerName: "Searchable",
    partitionCount: 32,
    queryOptions,
    schemas);
```

The direct client's declaration must match the silo registration. Supplying a registry to a client
does not register a type on the silos and does not activate a generation.

## First adoption and later generation changes

Use a maintenance window:

1. Before deploying the first new binary or changed registration, quiesce writes, clears, queries,
   pages, facets, and virtual-slot movement. For first adoption, pause the entire provider because
   its partition capability is provider-wide. Keep it paused throughout the maintenance window.
2. Back up the complete physical-provider namespace after traffic is paused, including layout,
   partition manifests, journals, snapshots, and every `index-schema` control document.
3. Deploy the same schema-capable binary and the complete registration set to every silo. Update
   every external query/admin process as well, and verify that no older participant remains before
   starting a rebuild. Do not expose the provider to traffic during this mixed-version interval.
4. For each registered state, run the same type and application version used at startup:

   ```csharp
   var status = await admin.RebuildIndexSchemaAsync<VacancyState>(
       "vacancy",
       applicationSchemaVersion: 2,
       cancellationToken);
   ```

5. For every registered state, verify its matching `GetIndexSchemaAsync<TState>` call reports
   `Active`, and record its fingerprint and processed-record count. Resume provider traffic or
   movement only after all registered states pass that check. Publishing the capability for the
   first state does not activate the others.

The overloads without an application version mean version 1. Using those overloads after registering
another version is a configuration error, not a request to inspect an older generation.

`RebuildIndexSchemaAsync` can be the first operation in a fresh provider namespace. It initializes
the persisted routing layout from the provider options, enables the capability, and activates an
empty generation for the requested state; a dummy state read is not required. Other registered
states remain uninitialized until their own rebuilds complete. A status read alone remains
non-creating and reports `Uninitialized` while the layout is absent.

## What the rebuild commits

The admin method is a client-side loop over control-grain turns. The control document stores one
rebuild identifier, the target generation, the routing layout identity, the next owner, a canonical
`GrainId` frontier, and the processed-record count. Each partition scan request covers at most 64
catalog records, while the public helper may run any number of pages before activation. A page can
also trigger the provider's existing retained whole-partition compaction, so 64 is a record-page
limit, not a hard work, memory, or wall-clock bound. Cancellation stops only the calling loop; an
in-flight turn can still commit, and calling the same method again resumes the durable intent.

The first advance creates or resumes a layout maintenance intent keyed by the rebuild
identifier before touching an owner. Current owners are upgraded to format 5 one at a time, then all
record pages for the requested state run while movement remains fenced. After the last page, one
turn publishes or confirms provider schema protocol 1 and clears the layout intent; a separate final
control commit activates the target fingerprint. Status can therefore briefly report `Rebuilding`
with the complete count after every record for that state is covered but before `Active` is
committed.

While `Rebuilding`, the public status also reports the durable operation—owner enablement, record
scanning, or generation activation—and the total, enabled, and fully scanned owner counts. These
fields are `null` outside a rebuild. They make empty and no-index state scans observable even when
the processed-record count remains zero; they are checkpoints, not an estimate of remaining
wall-clock time.

Each changed record receives a replayable `Reindex` journal entry. It preserves the serialized
payload, `GrainId`, ETag, and object-version allocator while replacing only derived index entries and
the record's schema fingerprint. A retry skips records already carrying the target fingerprint.
The public count is the number of records covered by fully committed control pages in the current
scan. A partially executed page can have reindexed records while the visible count is unchanged;
the retry covers those records again safely. The final active count describes the completed scan,
not the number of records whose bytes changed.

Virtual-slot movement and schema rebuild cannot execute at the same time once the layout maintenance
intent is held. If a completed layout change is observed before that fence is acquired—including
recovery of a durable control intent whose first advance never committed the fence—the control pins
the new layout and restarts the owner scan from zero. Already rebuilt records remain valid and are
skipped. The processed count also restarts from zero, so keep traffic quiesced until the restarted
scan reaches `Active`.

## Failure and recovery

Control and partition persistence use the same conditional whole-state writes as the rest of the
provider. A failure before a control commit leaves the previous cursor durable. A commit whose
acknowledgement is lost can leave the next cursor or the final active generation durable. The
activation retires after either ambiguous outcome; a retry on a fresh activation accepts both cases
and converges. Do not delete the intent or synthesize progress manually.

Rebuilding requires the currently configured `IGrainStorageSerializer` and registered CLR state
type to deserialize every retained payload which does not already carry the target fingerprint.
The schema protocol does not migrate application payloads and cannot repair a serializer-breaking
state change. If deserialization fails, the turn fails and the durable cursor remains resumable;
records completed earlier in that page may already carry the target fingerprint. Restore a
payload-compatible serializer/type or restore/rewrite the bad application data under a reviewed
migration, then call the same rebuild again. Do not change the application version merely to bypass
an unreadable payload.

Application deserialization and indexed-getter failures cross the Orleans proxy as a plain
`InvalidOperationException`. Its message identifies the provider, state name, `GrainId`, record key,
physical owner, and original exception type so a human operator can locate the payload and deployment
responsible. Application-controlled exception messages, raw index values, and payload values are
deliberately omitted from the remote diagnostic. That failed record page does not advance the
durable control cursor or count. Preserve the rebuild id, repair the serializer/type/data, and
resume the same rebuild; do not start a competing generation to hide the failure.

Point reads do not validate an index generation, but they still use the application's serializer.
"Point reads remain available" therefore means the index gate does not block them; it is not a
promise that an incompatible payload can be deserialized.

## Queries and continuations across a generation change

Managed physical scopes include the active schema fingerprint, so old and new index entries cannot
be combined. While a state is uninitialized, rebuilding, or active under a different fingerprint,
managed writes, clears, queries, pages, and facets throw
`SearchableStorageIndexSchemaException` before using that generation.

Paging and distinct-facet continuations are authenticated against the translated query or facet
plan. That plan contains generation-bound scopes. A token created before first adoption or before a
generation change is invalid after activation of the new fingerprint; discard it and start a new
page sequence. Do not interpret this as the routing-specific
`SearchableStorageStaleContinuationTokenException`: a schema change normally fails token binding as
an invalid continuation, while a routing-epoch change has its own stale-token contract.

## Durable formats and retention

The schema gate upgrades a routing layout from format 4 to format 5. When the persisted layout is
still format 3, the first rebuild begins by running the same idempotent routing initializer which
adopts it to format 4; no separate dummy storage operation is required. Query and status reads
remain non-migrating. Layout format 5 preserves the same virtual-slot placement, routing fingerprint
domain, and epoch while appending the provider schema capability and per-rebuild maintenance intent.
Partition persistence has three supported formats:

- format 3 is the legacy journal/snapshot representation with neither movement nor managed schemas;
- format 4 adds movement state and lossless snapshot encoding;
- format 5 adds the partition schema-capability field and per-record schema fingerprints.

The first schema rebuild can upgrade a supported format-3 or format-4 owner directly to format 5.
Existing records remain readable by the new binary and are reindexed through normal WAL commits.
Formats 1 and 2 still require a separate migration or complete rewrite. After a partition or layout
is upgraded for schemas, a format-3/4 or schema-unaware binary is not a supported rollback target.

Each registered provider/state pair has a durable `index-schema` control document in the same
physical provider as the layout and partitions. It is small but load-bearing: include it in backup,
restore, replication, and retention policies. Do not apply TTLs or lifecycle expiry independently
to control, layout, manifest, journal, or snapshot values, and do not clear a control document after
activation merely to save space.

If a control document alone is lost while layout and partition data survive, the provider remains
schema-enabled. A restarted process or any participant with a cold validation cache reads the
missing control and fails closed because no generation is active. Already validated participants do
not poll the control on every steady-state request, so out-of-band deletion is not an immediate
distributed stop signal: quiesce traffic and restart affected participants during recovery. Inspect
the layout before recreating anything. When no schema-maintenance intent remains there, a quiesced
rebuild using the exact complete registration can reconstruct derived indexes and recreate the
active control state. If the control was lost during a rebuild and the layout still holds its
maintenance intent, the public API cannot safely invent the missing rebuild identifier or schema
identity. Restore a consistent backup containing the matching control, layout, and partition state,
or use a reviewed physical-recovery procedure; do not delete the layout intent or start a different
rebuild. Loss of layout, manifest, journal, or snapshot data is a physical-backend recovery event,
not a schema rebuild.
