# Testing strategy

This document defines the maintainer-facing testing policy for Orleans.SearchableStorage. Every pull request must receive a test-sufficiency review in addition to the general engineering review and any domain-specific review.

## Required review lens

The reviewer must map changed behavior to tests instead of approving a raw test count. The review considers:

- successful public API behavior;
- validation and unsupported inputs;
- storage concurrency and ETag conflicts;
- failures before persistence and ambiguous failures after commit;
- activation loss, rehydration, and serializer compatibility;
- deterministic execution across more than one storage partition;
- cancellation and retry behavior where applicable;
- user-facing samples at their executable boundary;
- every physical backend claimed as supported.

Missing coverage must be called out explicitly in the pull request with a reason and a follow-up scope. Line and branch coverage reports are diagnostic evidence and do not replace behavioral review.

## Test layers

### Value and metadata tests

Fast unit tests protect index-value normalization, comparison/hash equivalence, ordering, supported CLR types, PolyType model construction and caching, inherited and nullable property shapes, attribute metadata, selector validation, recursive version-independent and cached scope identities, comparer-based range-bucket canonicalization, open and bounded range traversal, query expression translation, deferred captured values, reversed operands, compiler-generated integral, decimal, enum, and BCL-operator promotions, rejected semantic-changing conversions and custom comparison methods, fractional and adjacent floating-point bounds, NaN and infinity, out-of-domain numeric bounds, all equal-bound inclusivity combinations, bound combination, unsupported syntax, and query-plan simplification. Boundary tests use the production plan constants to cover accepted and rejected depth and node counts, conversion-chain limits, balanced and chained predicates, order-preserving AND/OR rebalancing, semantic and wire cycles or shared subtrees, hidden child graphs, and payload on the wrong wire node kind.

Client execution tests use a narrow internal constructor with controlled `IStoragePartitionGrain`
implementations. This seam deterministically proves one complete request per partition, sorted and
deduplicated merge behavior, empty-plan short-circuiting after layout validation, rejection before
fan-out when expression limits are exceeded, no fail-fast partial result, observation of immediate
and late partition failures, and cancellation while calls are blocked. It is intentionally limited to the
fan-out boundary; real `TestCluster` tests cover Orleans dispatch, generated serialization, and
partition-grain execution.

### Storage contract tests

The reusable contract exercises normal `IGrainStorage` behavior for indexed object state and non-object state without indexes, exact and range primitives, nested `IQueryable` intersection and union, empty plans, nullable indexed values, compiler-promoted byte and enum queries through real Orleans serialization, inclusive and exclusive one-sided and equal bounds, deterministic sorted deduplication, cancellation, updates, clears, layout validation, ETags, deterministic multi-partition fan-out, activation rehydration, malformed wire-plan rejection followed by a healthy call, protection against boolean mutation of live index buckets, and physical-write failure boundaries. Every supported physical provider must run the same contract.

Serializer and API contract tests freeze the required non-null fields and IDs of the existing
bounded range message, the IDs and nullable bounds of the new non-persisted query plan, and a nested
plan round trip through the configured Orleans serializer. Compile-time test implementations keep
the old direct-client interface independent from the opt-in query interface and exercise an
external public async terminal provider.

The memory, PostgreSQL, Redis, and Azure Blob fixtures inherit this same contract class; backend tests do not copy or weaken its assertions.

### Backend-specific tests

Provider fixtures add environment setup, cleanup, serializer selection, and registration assertions which cannot be expressed in the shared contract. All four fixtures use `JsonGrainStorageSerializer`, two in-process silos, eight storage partitions, and the same fault-injecting decorator around the physical provider.

- PostgreSQL uses `Microsoft.Orleans.Persistence.AdoNet` with Npgsql. The fixture creates an isolated schema and applies the unmodified operational schema from the Orleans 10.2.2 source tag, apart from source-attribution comments.
- Redis uses `Microsoft.Orleans.Persistence.Redis` with a unique Orleans service id.
- Azure Blob uses `Microsoft.Orleans.Persistence.AzureStorage` with a unique container and runs against Azurite in CI.

The package versions and conditional-test pattern follow the Orleans 10.2.2 repository: `Xunit.SkippableFact` marks the reusable external contract, and fixture preconditions skip it unless `ORLEANS_SEARCHABLE_STORAGE_RUN_BACKEND_TESTS` is explicitly enabled. Npgsql, Azure.Storage.Blobs, and StackExchange.Redis are direct test dependencies because the fixtures prepare and remove backend resources in addition to configuring the Orleans providers.

The dedicated CI job starts the pinned images from `tests/backends.compose.yml`, runs only tests tagged `BackendIntegration`, rejects a result containing skipped backend tests, uploads its TRX and coverage artifacts, and removes the volumes even after failure. For local execution and connection-string overrides, see [physical storage backends](backends.md).

### Executable sample tests

The API sample is tested through HTTP using ASP.NET Core `WebApplicationFactory`. These tests ensure the documented host starts, keyed Orleans services resolve, writes reach the searchable provider, the focused `IQueryable` API returns indexed ids, and deletes remove both state and index entries. A keyed blocking query client verifies that HTTP request cancellation reaches both search endpoints and their async terminal operation while it is in flight.

## Coverage artifacts

CI collects Coverlet line and branch coverage together with test results. The regular job covers unit, memory-contract, and sample tests; the backend job records the external contract separately so a skipped local run cannot be mistaken for backend validation. Coverage changes guide review toward untested branches, but no pull request can satisfy the test-sufficiency requirement by meeting a percentage alone.
