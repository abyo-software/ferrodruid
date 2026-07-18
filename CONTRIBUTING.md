# Contributing to FerroDruid

Thank you for your interest in contributing to FerroDruid.

## Contributor License Agreement (required)

All contributions require a signed Contributor License Agreement with
abyo software 合同会社 (abyo software LLC). The CLA grants abyo software
the rights needed to distribute FerroDruid under the Business Source
License 1.1, under commercial terms, and under the scheduled Change
License (Apache-2.0) — see the full texts:

- [Individual CLA](.github/CLA/individual-cla.md) — accepted
  electronically: CLA Assistant will prompt you on your first pull
  request.
- [Corporate CLA](.github/CLA/corporate-cla.md) — for contributions
  made on behalf of a company; submit by email to aws-support@abyo.net.

Pull requests cannot be merged until the CLA check passes.

## Getting Started

### Prerequisites

- Rust 1.85+ (see `rust-toolchain.toml`)
- Cargo (included with Rust)
- Docker (optional, for integration tests)

### Build

```bash
cargo check --workspace
cargo build --workspace
```

### Test

```bash
cargo test --workspace
```

### Lint

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

## Development Workflow

### 1. Fork and Branch

```bash
git clone https://github.com/<your-username>/ferrodruid.git
cd ferrodruid
git checkout -b feature/your-feature-name
```

### 2. Make Changes

Follow the code quality standards described below.

### 3. Test

Run the full test suite before submitting:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

All tests must pass. Clippy must report 0 warnings.

### 4. Commit

Write small commits with one logical change per commit. Use English commit
messages in the imperative mood:

```
Add timeseries query support for period granularity

Implement period-based granularity (PT1H, P1D, P1M, etc.) for timeseries
queries. This enables time-bucketing with arbitrary period durations.
```

### 5. Pull Request

Open a pull request against `main`. Include:

- A clear description of what the change does
- How it was tested
- Any compatibility implications

## Code Quality Standards

These are enforced from day one. PRs that violate these standards will not
be merged.

### Unsafe Code

`#![forbid(unsafe_code)]` is required in every crate. If you believe unsafe
code is necessary (e.g., SIMD optimization), it must:

- Be less than 50 lines
- Include a `// SAFETY:` comment explaining the invariant
- Be reviewed by a maintainer

### No unwrap()

`unwrap()` is not allowed in non-test code. Use `?`, `unwrap_or`,
`unwrap_or_else`, or explicit `match` instead.

### No TODO/FIXME/HACK

All temporary markers must be resolved before the PR is submitted. If
something is intentionally deferred, document it in the relevant module's
documentation or in `docs/known-limitations.md`.

### Documentation

`#![deny(missing_docs)]` is enforced in every crate. All public items
(functions, structs, enums, traits, modules) must have documentation comments.

### SPDX Headers

Every `.rs` and `.toml` file must start with:

```
// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)
```

### Formatting

Run `cargo fmt` before committing. The CI pipeline will reject unformatted code.

### Clippy

All code must pass `cargo clippy --workspace --all-targets -- -D warnings`.
No `#[allow(clippy::...)]` annotations without a documented justification.

## Architecture Guidelines

### Clean-Room Requirement

FerroDruid is a clean-room implementation. Do NOT reference Apache Druid
source code when contributing. Implementation must be based solely on:

- Public Apache Druid documentation (druid.apache.org)
- Published API specifications
- Wire protocol analysis of public APIs

### Crate Boundaries

Do not add dependencies on other abyo software internal crates. Tantivy
is used directly from crates.io.

### Dependency Policy

New dependencies must:

- Be permissively licensed (MIT, Apache-2.0, BSD, ISC)
- Be published on crates.io (no git dependencies)
- Not introduce GPL/AGPL/LGPL/SSPL transitive dependencies
- Be justified in the PR description

## Testing Guidelines

### Unit Tests

- Place unit tests in `#[cfg(test)]` modules within the source file
- Test both happy path and error cases
- Use descriptive test names: `test_groupby_with_having_filter_excludes_rows`

### Integration Tests

- Place integration tests in the `tests/` directory
- Use the appropriate compat test suite (`tests/*-compat/`)
- New features must include compatibility tests against Druid documentation

### Test Data

- Use small, focused test data (not production datasets)
- Test data files go in `tests/fixtures/`
- All test data must be synthetic (no real user data)

## Reporting Issues

Use GitHub Issues for bug reports and feature requests. For security
vulnerabilities, see `SECURITY.md`.

### Bug Reports

Include:

- Rust version (`rustc --version`)
- OS and architecture
- Steps to reproduce
- Expected vs actual behavior
- Relevant log output

## License

FerroDruid is licensed under the Business Source License 1.1 (see
`LICENSE`); each version converts to the Apache License 2.0 four years
after its public release. By contributing, you agree to the terms of
the [Contributor License Agreement](#contributor-license-agreement-required),
which allows abyo software to distribute your contribution under those
licenses and under commercial terms.
