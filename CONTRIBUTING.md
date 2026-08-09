# Contributing

Orleans.SearchableStorage follows the engineering conventions used by the .NET and Microsoft Orleans projects.

## Development rules

- Use C# and the SDK pinned in `global.json`.
- Keep all repository-facing text in English, including source comments, documentation, commits, issues, and pull requests.
- Do not add co-author trailers or AI attribution to commit messages.
- Keep pull requests focused and route every change through a pull request.
- Add comments for durability, consistency, concurrency, and other non-obvious invariants. Comments should explain intent and trade-offs instead of restating the code.

## Documentation policy

Every pull request must include an intentional update to both kinds of documentation:

- User documentation covers observable behavior, public APIs, configuration, examples, compatibility, and limitations.
- Internal documentation covers architecture, invariants, consistency and durability boundaries, important design decisions, and maintainer guidance.

Code comments are part of internal documentation when they explain a local invariant, but they do not replace design documentation for a change which affects multiple components.

## Review policy

Every pull request requires a general engineering review. A specialized review, such as a storage, concurrency, serialization, or performance review, supplements the general review and never replaces it.

The general review covers correctness, maintainability, clarity, unnecessary complexity, established design patterns, testability, and consistency with current .NET and Orleans engineering practices.

## Validation

Run the following before requesting review:

```bash
dotnet restore Orleans.SearchableStorage.slnx
dotnet build Orleans.SearchableStorage.slnx --no-restore
dotnet test Orleans.SearchableStorage.slnx --no-build
```

The same storage contract tests will be exercised against multiple physical Orleans persistence providers. PostgreSQL and Redis are required backends. An object-storage backend, such as Azure Blob Storage or an S3-compatible provider, will be tested on a separately configured integration environment.
