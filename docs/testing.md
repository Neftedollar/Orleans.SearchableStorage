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

Fast unit tests protect index-value normalization, equality, hashing, ordering, supported CLR types, attribute metadata, selector validation, and collision-free scope identities.

### Storage contract tests

The reusable contract exercises normal `IGrainStorage` behavior, exact and range queries, updates, clears, layout validation, ETags, deterministic multi-partition fan-out, activation rehydration, and physical-write failure boundaries. Every supported physical provider must run the same contract.

### Backend-specific tests

Provider fixtures add failure injection, serializer selection, environment setup, and backend behavior which cannot be expressed in the shared contract. PostgreSQL and Redis are required integration targets. Object-storage providers run in a separately configured environment.

### Executable sample tests

The API sample is tested through HTTP using ASP.NET Core `WebApplicationFactory`. These tests ensure the documented host starts, keyed Orleans services resolve, writes reach the searchable provider, queries return indexed ids, and deletes remove both state and index entries.

## Coverage artifacts

CI collects Coverlet line and branch coverage together with test results. Coverage changes guide review toward untested branches, but no pull request can satisfy the test-sufficiency requirement by meeting a percentage alone.
