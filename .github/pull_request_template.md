<!-- SPDX-License-Identifier: BUSL-1.1 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

## Summary

<!-- What does this change do, and why? -->

## Linked issue

<!-- e.g. Closes #123. Please open an issue first for non-trivial changes. -->

## Checklist

- [ ] **CLA**: I have signed the FerroDruid CLA (CLA Assistant will prompt
      on this pull request if I have not).
- [ ] **Clean room**: I have not consulted Apache Druid source code for this
      change (documented behavior only).
- [ ] `cargo fmt --check` passes.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes with
      0 warnings.
- [ ] `cargo test --workspace` is green.
- [ ] No `unwrap()` in non-test code.
- [ ] No `TODO` / `FIXME` / `HACK` markers introduced.
- [ ] SPDX header present on every new `.rs` and `.toml` file.
- [ ] Docs updated for any public API change (`deny(missing_docs)` is
      enforced).
