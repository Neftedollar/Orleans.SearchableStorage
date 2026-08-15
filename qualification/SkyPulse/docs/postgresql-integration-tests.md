# PostgreSQL integration tests

The PostgreSQL integration suite exercises the real Npgsql persistence boundary against a
disposable database. Every test drops and recreates the dedicated `skypulse` schema. Never point
the suite at a database which contains data that must be retained.

Local execution is opt-in:

```bash
export SKYPULSE_POSTGRES_CONNECTION_STRING='Host=localhost;Port=5432;Database=skypulse_qualification;Username=postgres;Password=postgres;SSL Mode=Disable'
dotnet test tests/Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.IntegrationTests
```

Without the environment variable, local integration facts are reported as skipped. Before any
qualification run, CI must gain a separate required job backed by a digest-pinned PostgreSQL
service which always sets the variable and rejects every skipped result in its TRX report. That
workflow is not present yet. An ordinary build, or local discovery which reports skipped tests,
is not evidence that the PostgreSQL integration suite ran.

The suite covers migration reapplication and catalog-drift detection, delivery and semantic
deduplication, whole-transaction rollback, ordered exclusive leases, the required projection
prepare/index/finalize order, deletion finalization, stale recalculation races, and
evidence-authorized bounded retention. The rolling-window scenario seeds a bucket on an exact
one-day boundary and proves that the worker atomically advances account state, decayed desired
values, the next due time, and a new unfinished outbox version without manufacturing a source
delivery. It also contains restartable, one-row-page scenarios for
repository-sync dependency drain and inactive-account cleanup, including the rule that every
intermediate delivery remains Pending and only the final durable commit permits acknowledgement.
The suite also contains TAP delivery tests which prove exact frame-digest reservation, fixed-code
quarantine, acknowledgement-safe lost-ACK redelivery, and idempotent exact-prefix account
bootstrap without overwriting progressed state. A vertical scenario composes that bootstrap with
historical record replay, the final `repo_sync` barrier, a live record commit, and an applied-event
redelivery through the real stores. These tests remain evidence only when the environment-gated
PostgreSQL facts actually execute rather than report as skipped.
