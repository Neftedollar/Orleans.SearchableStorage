# Contributing

Orleans.SearchableStorage follows the engineering conventions used by the .NET and Microsoft Orleans projects.

## Development rules

- Use C# and the SDK pinned in `global.json`.
- Keep all repository-facing text in English, including source comments, documentation, commits, issues, and pull requests.
- Do not add co-author trailers or AI attribution to commit messages.
- Keep pull requests focused and route every change through a pull request.
- Add comments for durability, consistency, concurrency, and other non-obvious invariants. Comments should explain intent and trade-offs instead of restating the code.
- Use PolyType type shapes for runtime inspection, member access, and generic type-driven behavior. Direct reflection is limited to framework boundaries such as reading an expression-tree member identity and must not become an independent metadata path.
- Native AOT and trimming are not supported; do not add source-generation constraints to application grain-state types solely for this project.

## Documentation policy

Every pull request must include an intentional update to both kinds of documentation:

- User documentation covers observable behavior, public APIs, configuration, examples, compatibility, and limitations.
- Internal documentation covers architecture, invariants, consistency and durability boundaries, important design decisions, and maintainer guidance.

Code comments are part of internal documentation when they explain a local invariant, but they do not replace design documentation for a change which affects multiple components.

## Review policy

Every pull request requires a general engineering review. A specialized review, such as a storage, concurrency, serialization, or performance review, supplements the general review and never replaces it.

The general review covers correctness, maintainability, clarity, unnecessary complexity, established design patterns, testability, and consistency with current .NET and Orleans engineering practices.

Every pull request also requires an explicit test-sufficiency review. This is a separate review lens and cannot be replaced by the general or domain-specific review. It evaluates whether the changed behavior is protected by meaningful tests across success, validation, failure, durability, concurrency, distributed execution, serialization, and relevant physical backends. Raw test count and line coverage are evidence, not acceptance criteria by themselves.

The pull request must record intentional test gaps and explain why they are deferred. See [docs/testing.md](docs/testing.md) for the review checklist and test layers.

Every actionable review comment must have an explicit disposition before merge. When feedback is addressed, reply in the original discussion with the pull request or commit which contains the fix and the validation which supports it, then resolve the thread. If the fix is implemented in a later pull request, link that pull request from the original discussion as well. Do not leave an addressed thread open or mark an unimplemented change as resolved.

## Validation

Run the following before requesting review:

```bash
dotnet restore Orleans.SearchableStorage.slnx
dotnet build Orleans.SearchableStorage.slnx --no-restore
dotnet test Orleans.SearchableStorage.slnx --no-build --collect "XPlat Code Coverage"
```

The same storage contract tests will be exercised against multiple physical Orleans persistence providers. PostgreSQL and Redis are required backends. An object-storage backend, such as Azure Blob Storage or an S3-compatible provider, will be tested on a separately configured integration environment.
