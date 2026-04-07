---
name: release-rebase
description: Use this skill when updating this repo to match a new `jito-solana` release line such as `v4.0`. It is for branch creation, toolchain and Cargo version alignment, mixed Solana dependency cleanup, API/struct migration work, validation, CI readiness, and final commit plus summary generation.
---

# Release Rebase

Rebase this plugin to a target `jito-solana` release branch with a compile-first workflow. Use it when the repo needs to move from one Solana/Agave release line to another and the likely work includes dependency graph cleanup and source updates.

## Workflow

1. Create a release branch named `vX.X` from the current branch, for example `v4.0`.
2. Inspect the matching `jito-solana` `vX.X` branch and update `rust-toolchain.toml` to match its Rust channel and required components.
3. Update `Cargo.toml` to the target release line, including `agave-geyser-plugin-interface = "=X.Y.Z"` and any related Solana/Agave crate versions needed to stay on one coherent release line.
4. Run `cargo check --workspace` early. Fix version skew first:
   - remove mixed `3.x` and `4.x` Solana crate graphs
   - align local path crate versions with the workspace version
   - add required crate features when the target release expects them
5. Fix compile errors caused by API or struct changes:
   - moved modules or renamed imports
   - new required struct fields
   - changed enum variants or method names
   - conversion code that now bridges different crate types
6. Re-run `cargo check --workspace` until it passes.
7. Run `cargo clippy --all-features -- -D clippy::all`.
8. Run `cargo test --workspace`.
9. If the repo has CI or additional validation commands, run the same checks locally before finishing.
10. Commit only when validation passes. Use `git commit --no-gpg-sign`.
11. Produce a short summary covering:
   - release target
   - dependency/toolchain updates
   - source/API fixes
   - verification performed

## Working Rules

- Prefer changing the minimum set of versions needed to get onto the target release line cleanly.
- Do not assume a version bump is enough; expect type mismatches from duplicate Solana crates.
- Treat compile failures as migration guidance. The first errors usually reveal the next required dependency or API alignment step.
- Keep local path dependencies pinned to the workspace release version if the workspace uses exact versions.
- Avoid reverting unrelated user changes.
- If runtime or CI validation cannot be run, say so explicitly in the final summary.

## Typical Failure Modes

- Root `Cargo.toml` still pins local path crates to the old exact version.
- `cargo` resolves both old and new `solana-*` or `agave-*` crates, causing trait or `Into` mismatches between nearly identical types.
- New release lines move types into different modules.
- Structs gain new fields and test fixtures fail after library code compiles.
- Some `solana-transaction-status` APIs require explicit unstable feature flags on newer Agave lines.

## Output Expectations

When using this skill, finish with:

- the branch name created or updated
- whether `cargo check --workspace` passed
- whether `cargo clippy --all-features -- -D clippy::all` passed
- whether `cargo test --workspace` passed
- any CI follow-up still required
- the commit hash if a commit was created
