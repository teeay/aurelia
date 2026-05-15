# Aurelia Publishing

Status: Developed

## Objectives

- Publish a single `aurelia` crate to crates.io that contains the full
  capability surface of the workspace, with no separately published
  internal crates.
- Keep the workspace's multi-crate development model untouched: the
  publishing flow does not modify the root `Cargo.toml`, the `src/lib`
  crate, or any internal crate under `src/crates/`.
- Make the merge driven by a small declarative configuration so that
  adding a new internal crate to the published artefact in the future
  is a one-line change, not a rewrite of the publishing tool.
- Validate the workspace in its native shape *before* attempting any
  merge, so a broken workspace cannot poison the publish tree.
- Validate (build, unit tests, clippy, dry-run) the merged crate as
  the *exact* artefact that will be uploaded to crates.io, in an
  isolated, regenerated working directory.
- Keep the final `cargo publish` step manual, run by a human in the
  generated `publish/aurelia/` directory once both validation passes
  succeed.

## Release Process

The operator-driven release procedure is:

1. Run `scripts/prep-publish.sh` and resolve any failures in the workspace.
2. Run `scripts/publish.sh`. On success, the publish tree at `publish/<target_crate>/` has been regenerated and has passed every check including `cargo publish --dry-run --allow-dirty`.
3. Inspect the generated `publish/<target_crate>/` tree by hand on first use and after any change to the publishing tooling.
4. Manually run `(cd publish/<target_crate> && cargo publish)` to push the release to crates.io.
5. Tag the workspace commit (e.g. `v0.1.0`) by hand, outside the publishing tooling.

## Technical Details

### Publishing Guardrail Model

The publish tooling uses a positive-control model: only source forms, manifest sections, and
package metadata described in this document are supported by the generator. Unsupported shapes must
fail before a publish tree is treated as valid. This applies both to unsupported dependency tables
and to crate-root artifacts that are not part of the current merge contract.

The publish-tree generator enforces these boundaries:

- Macro invocations that resolve to `#[macro_export]` macros stay rooted at the merged crate root.
- Valid Rust file-level documentation and attributes remain before injected module declarations.
- Manifest synthesis rejects unsupported dependency tables instead of silently omitting them.
- Generated metadata and dependency surfaces are explicitly guarded so missing or unexpected
  publish inputs fail early.

### Configuration Model

A single `[workspace.metadata.aurelia-publish]` table in the root `Cargo.toml` drives the entire pipeline. This is the *only* place adding a new internal crate to the published artefact has to be touched.

```toml
[workspace.metadata.aurelia-publish]
# The publishable crate. Its package name on crates.io.
target_crate = "aurelia"

# Internal crates to merge into the target crate, in declaration order.
# `name` is the workspace package name; `module` is the module path under
# the target crate (defaults to the package name minus the "aurelia-"
# prefix, with hyphens replaced by underscores).
internal_crates = [
  { name = "aurelia-ids" },
  { name = "aurelia-data" },
  { name = "aurelia-platform" },
  { name = "aurelia-logging" },
  { name = "aurelia-peering" },
  { name = "aurelia-resolver" },
]

# Crates to ignore entirely (never merged, never expected to be merged).
excluded_crates = ["xtask"]
```

Defaults and derivations:

- `module` defaults to the crate name with the `aurelia-` prefix removed and hyphens converted to underscores. For example, `aurelia-peering` → `peering`, `aurelia-resolver` → `resolver`, `aurelia-foo-bar` → `foo_bar`. Override by setting `module` explicitly.
- The identifier the rewriter looks for in source is the crate name with hyphens replaced by underscores (matching `extern crate` semantics). For `aurelia-peering` this is `aurelia_peering`; for `aurelia-resolver` this is `aurelia_resolver`.
- Source directory is resolved via `cargo metadata` and the package's `manifest_path`; layout under `src/crates/<dir>/` is not assumed.

Validation performed when `PublishConfig` loads:

- The target crate exists and is a workspace member.
- Every `internal_crates[*].name` exists, is a workspace member, and has `publish = false` (refuses to merge a crate that is intended to be independently published).
- Every `excluded_crates[*]` entry exists and is a workspace member. Misspelled excluded entries
  fail the run instead of being ignored.
- Every workspace member is accounted for: it is either the target crate, listed in `internal_crates`, or listed in `excluded_crates`. An unaccounted-for member fails the run with a clear message — this is the guardrail that catches "someone added a new crate and forgot to declare its disposition".
- Module names are unique.

### Repository Layout

```
/Cargo.toml                                  # workspace; declares members and [workspace.metadata.aurelia-publish]
/.gitignore                                  # excludes /publish/
/.cargo/config.toml                          # `cargo xtask` alias
/LICENSE, /NOTICE, /README.md                # sources of truth, copied into the publish tree
/src/lib/                                    # target crate (`aurelia`)
/src/crates/{ids,data,platform,logging,peering,resolver}/  # internal crates merged into the published artefact
/src/crates/xtask/                           # publishing tool
/publish/                                    # git-ignored; regenerated by `cargo xtask publish-tree`
/publish/aurelia/Cargo.toml                  # synthesised manifest
/publish/aurelia/src/lib.rs                  # rewritten copy of the target crate's lib.rs
/publish/aurelia/src/<module>/               # rewritten copy per internal crate (lib.rs → mod.rs)
/publish/aurelia/{LICENSE,NOTICE,README.md}  # copies
```

### Tooling: `xtask`

- Workspace member at `src/crates/xtask`. `publish = false`. Not part of any release.
- Binary entry point exposed as `cargo xtask` via a `.cargo/config.toml` alias:

  ```toml
  [alias]
  xtask = "run --quiet --package xtask --"
  ```

- Subcommands implemented with `clap` derive:
  - `publish-tree` — regenerate + full validation (fmt-check, build, test, clippy, `cargo publish --dry-run --allow-dirty`) inside the publish tree. Default mode invoked by `scripts/publish.sh`.
  - `publish-tree --check` — regenerate + build + `cargo publish --dry-run --allow-dirty` only. Faster than the full pipeline; useful when adding a new internal crate or iterating on the publishing tooling itself.
  - `publish-tree --keep` — skip the wipe step for debugging. If the publish root already exists,
    stale files from earlier generation runs can remain; the command warns when this flag is used
    against an existing publish tree.
- The xtask deliberately has no `--publish` mode. The real `cargo publish` step is always run by hand by the operator inside `publish/<target_crate>/` after `scripts/publish.sh` succeeds. This keeps the irreversible step out of automation.
- All filesystem operations go through `fs_err` for clearer errors.

### Tree Generation Pipeline

The pipeline operates in this order:

1. **Resolve config** — load `[workspace.metadata.aurelia-publish]` and validate.
2. **Wipe** — `rm -rf publish/<target_crate>/` (skipped if `--keep`).
3. **Copy target crate sources** — walk `<target_crate manifest dir>/src` and copy each `.rs` file through the rewriter pipeline to `publish/<target_crate>/src/`.
4. **Copy internal crate sources** — for each entry in `internal_crates`, walk its `src/` and copy through the rewriter pipeline to `publish/<target_crate>/src/<module>/`. Each internal crate's `lib.rs` is renamed to `mod.rs` so Rust's module resolution picks it up as the module root.
5. **Inject module declarations** — read the new `lib.rs`, skip the valid Rust file header, insert `mod <module>;` lines for each configured internal crate (in declaration order) before the first item declaration.
6. **Copy LICENSE, NOTICE, README** — straight copy, no rewrite.
7. **Synthesise Cargo.toml** — see below.
8. **Auto-format** — run `cargo fmt` inside the publish tree so subsequent fmt-check validation is meaningful: any drift after this step is a real regression.

The pipeline never reads or writes outside of `publish/`, the workspace `Cargo.toml`, and the per-crate `Cargo.toml`s (read-only for the latter two).

### Source Inclusion Policy

The publish tree includes exactly:

- Rust and non-Rust files under the target crate's `src/` tree.
- Rust and non-Rust files under each configured internal crate's `src/` tree.
- The workspace root `LICENSE`, `NOTICE`, and `README.md`.
- The generated `Cargo.toml` for the merged crate.

The current merge contract does not include internal crate-root artifacts such as `build.rs`,
crate-local `README.md` files, `examples/`, or `benches/`. Introducing those artifacts in a target
or internal crate requires a reviewed publish-tooling update. The generator must fail clearly if a
configured target or internal crate contains a `build.rs` or unsupported dependency table that would
change build behavior when omitted.

### Identifier Rewriter

Each `.rs` file is rewritten in three ordered stages at copy time. The order matters: each stage's output is the next stage's input, and reordering would cause double-prefixing or unresolved identifiers.

**Stage 1 — Retarget in-crate `crate::` paths (internal crate files only).** Every `\bcrate::` path reference becomes `crate::<self_module>::`. Inside `aurelia-peering`, `crate::transport` becomes `crate::peering::transport`. This stage is skipped for the target crate, where `crate::` already addresses the merged root. The `\b` boundary plus the literal `::` suffix means `pub(crate)` is left alone. Macro invocations of the form `crate::IDENT!` are excluded from this retargeting because `#[macro_export]` macros live at the merged crate root.

**Stage 2 — Hoist macro invocations.** Every `\baurelia_<x>::IDENT!` becomes `crate::IDENT!`. `#[macro_export]` macros are hoisted to the merged crate root by Rust regardless of the module they were declared in, so cross-crate macro calls cannot use the per-module path. Stage 2 runs after stage 1 so the freshly produced `crate::IDENT!` is not affected by stage 1's `crate::` rewrite.

**Stage 3 — Rewrite cross-crate identifiers.** A single `Regex` with alternation over the configured internal crate identifiers, anchored on `\b` on both sides:

```text
pattern = r"\b(aurelia_(?:ids|data|platform|logging|peering|resolver))\b"
```

For each match, the rewriter looks up the captured identifier and substitutes `crate::<module>`. The pattern (and therefore the alternation) is built from the resolved config, not hardcoded.

Stage 3 behaviour, asserted by unit tests:

- `aurelia_peering::Domus` → `crate::peering::Domus`
- `aurelia_platform::runtime::handle` → `crate::platform::runtime::handle`
- `aurelia_resolver::SimpleResolver` → `crate::resolver::SimpleResolver`
- `use aurelia_peering;` → `use crate::peering;`
- `use aurelia_peering::{A, B};` → `use crate::peering::{A, B};`
- `aurelia_peeringx`, `xaurelia_peering`, `aurelia_peering_extra` — unchanged
- `crate::log_info!()` inside an internal crate — unchanged and resolved at the merged crate root
- `$crate::__limited_event!()` inside an internal crate — unchanged
- `// aurelia_peering` — rewritten (acceptable; reads correctly against the merged crate)
- `"aurelia_peering"` (string literal) — rewritten. The current workspace contains no string-literal occurrences of internal crate identifiers; if a future crate introduces one with runtime meaning, the rewriter must be narrowed to skip string literals via a `syn`-based traversal.

### Module Injection Header Handling

Injected internal-crate module declarations are inserted after the target crate's valid Rust file
header and before the first item. The header scanner must preserve:

- Leading regular line comments and blank lines.
- Inner line docs (`//! ...`).
- Inner block docs (`/*! ... */`), including multi-line block docs.
- Inner attributes (`#![...]`).

The scanner must not insert item declarations before file-level inner docs or inner attributes,
because doing so changes their meaning and can make the merged crate invalid Rust.

### Cargo.toml Synthesis

The synthesised `publish/<target_crate>/Cargo.toml` is built from the target crate's manifest with the following transformations:

1. **Workspace inheritance flattening.** Every `<field>.workspace = true` is replaced with the literal value resolved from the workspace root. Affects `edition`, `rust-version`, `license`, `repository`, `homepage`, `authors` at minimum.
2. **`readme` rewrite.** `readme = "../../README.md"` (workspace-relative) becomes `readme = "README.md"` (publish-tree-relative).
3. **Workspace dependency inheritance flattening.** Every `[dependencies]` or `[dev-dependencies]`
   entry with `workspace = true` is resolved against root `[workspace.dependencies]` before
   internal dependency removal and dependency union. The publish manifest must never contain
   dependency entries with `workspace = true`.
4. **Dependency removal.** Every `[dependencies]` or `[dev-dependencies]` entry that refers to a configured internal crate is removed. The classifier matches the dependency key, the table-form `package` name, and the dependency `path` resolved relative to the manifest containing the dependency. Path comparison is lexical and normalized, so aliases such as `foo = { package = "aurelia-ids", path = "../ids" }` are removed by package and path, not only by the key `foo`.
5. **Dependency union.** For each remaining dependency, and for every external dependency of every configured internal crate, merge into a single `[dependencies]` table. Same for `[dev-dependencies]` and `[features]`. Internal dependencies are skipped with the same package/path classifier used by dependency removal.
6. **Feature merging rules:**
   - For a dependency that appears multiple times across crates, the version requirement must match
     exactly as text. Equivalent-looking but differently written requirements such as `1` and
     `1.0` are treated as conflicts. This is intentional: the workspace dependency table is the
     canonical source for shared dependency versions, and publish synthesis should detect drift
     rather than normalise it.
   - The `features` list is the union.
   - The `optional` flag is the OR of inputs.
   - The `default-features` flag is the AND of inputs (any crate disabling defaults wins).
7. **`[features]` table merge.** Union of feature names; the dependency-activation list for each name is the union across crates. Conflicting feature definitions (same name, different bodies) fail the run with a clear diff.
   Feature entries that point at internal crate features are expanded into the merged internal
   feature's external dependency activations. For example, `aurelia-peering/actix` becomes the
   merged peering `actix` feature body, such as `dep:actix`, because `aurelia-peering` is not a
   dependency in the published crate.
8. **Unsupported dependency-table guard.** Target and internal manifests must not use
   `[build-dependencies]` or target-specific dependency tables such as
   `[target.'cfg(...)'.dependencies]`, `[target.'cfg(...)'.dev-dependencies]`, or
   `[target.'cfg(...)'.build-dependencies]`. These tables are outside the current publish merge
   contract and must fail before manifest generation succeeds.
9. **Generated manifest guard.** The synthesised manifest is validated before it is written. It must not contain unresolved `workspace = true` dependency inheritance, dependencies keyed to internal crate names, dependencies whose `package` names are internal crate packages, local `path` dependencies, or feature entries that reference internal dependency activations or internal crate feature edges.
10. **`[lib] path`** is set to `src/lib.rs` regardless of whether the source target manifest had a
    `[lib]` table, since the publish tree always uses the canonical layout.

The output is written via `toml_edit` to preserve human-readable formatting.

### Workspace Dependency Inheritance

Root `[workspace.dependencies]` owns external dependency versions for the workspace. Member crates
inherit those external dependencies with `workspace = true` and add local properties only where the
consumer needs them.

The workspace dependency table covers external dependencies shared by the runtime, public library,
internal crates, test support, and workspace tooling:

```toml
[workspace.dependencies]
actix = "0.13"
anyhow = "1"
async-trait = "0.1"
asn1-rs = "0.6"
bitflags = "2"
bytes = "1"
caducus = "0.2.2"
cargo_metadata = "0.18"
clap = "4"
fs-err = "3"
futures = "0.3"
libc = "0.2"
rcgen = "0.12"
regex = "1"
ring = "0.17"
rustls-pki-types = "1"
serde = "1"
serde_json = "1"
tokio = "1"
tokio-rustls = "0.26"
toml_edit = "0.22"
tracing = "0.1"
walkdir = "2"
x509-parser = "0.16"
zeroize = "1"
```

Internal Aurelia crates remain explicit path dependencies in member manifests. The publish-tree
pipeline identifies the internal crate set from `[workspace.metadata.aurelia-publish]`; path
dependencies are used as one detection signal when removing references to those configured internal
crates from generated dependency tables. Examples include:

```toml
aurelia-data = { path = "../data" }
aurelia-ids = { path = "../ids" }
```

Member manifests express local feature requirements on inherited dependencies:

```toml
tokio = { workspace = true, features = ["sync"] }
clap = { workspace = true, features = ["derive"] }
serde = { workspace = true, features = ["derive"] }
actix = { workspace = true, optional = true }
```

The publish-tree manifest synthesizer resolves an inherited dependency entry by starting from the
root workspace dependency entry and then applying local member properties:

- `version`, `registry`, `git`, `branch`, `tag`, `rev`, and `path` come from the root workspace
  dependency entry. External published dependencies should use registry versions, not local paths.
- Local `features` are unioned with root dependency `features`.
- Local `optional = true` is preserved; the effective value is the OR of root and local values.
- Local `default-features = false` is preserved; the effective value is the AND of root and local
  values, so any disabling entry wins.
- Local `package` is allowed only when it matches the root entry's `package` value.
- Local `version`, `path`, `git`, `branch`, `tag`, `rev`, or `registry` on a `workspace = true`
  dependency is rejected. Version and source selection belong in root `[workspace.dependencies]`.
- A dependency inheritance entry without a matching root `[workspace.dependencies]` entry fails the
  publish-tree generation with a message naming the crate, table, and dependency.

Examples:

```toml
# Root
[workspace.dependencies]
tokio = "1"
serde = { version = "1", features = ["std"] }
actix = "0.13"

# Member crate
[dependencies]
tokio = { workspace = true, features = ["sync", "time"] }
serde = { workspace = true, features = ["derive"] }
actix = { workspace = true, optional = true }
```

The publish-tree dependency entries are equivalent to:

```toml
tokio = { version = "1", features = ["sync", "time"] }
serde = { version = "1", features = ["std", "derive"] }
actix = { version = "0.13", optional = true }
```

Inherited dependencies are flattened before internal dependencies are dropped and before dependency
union across internal crates. This means the existing dependency conflict checks continue to compare
fully resolved dependency entries.

### Validation Pipelines

There are two validation passes. Both must succeed before `cargo publish` is run by hand.

**Pass 1 — Workspace prep (run by `scripts/prep-publish.sh`).** Operates on the unmodified workspace. The publish tree is not touched. The point is to refuse to merge a workspace that is not already healthy in its native shape.

| Step | Command | Purpose |
|------|---------|---------|
| Format | `cargo fmt --all -- --check` | Workspace formatting must be clean before any merge. |
| Build | `cargo build --workspace --all-targets` | All crates compile, including tests and examples. |
| Test policy scan | `scripts/testing/check-async-test-timeouts.py` | Enforces async test deadlines, calibrated timeout wrappers, E2E fixed-sleep allowlists, and stale test-helper-crate rules before release. |
| Test | `cargo test --workspace --all-targets --all-features` | Full workspace test suite, including feature-gated tests and integration tests under `src/crates/*/tests/` that are *not* carried into the publish tree. |
| Lint | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Workspace lint policy on the canonical sources. |

`cargo publish --dry-run` is intentionally **not** in this pass: the workspace shape cannot be published, and running it here would only produce confusing errors. The dry-run lives in Pass 2.

**Pass 2 — Publish-tree validation (run by `cargo xtask publish-tree`, invoked from `scripts/publish.sh`).** Operates inside `publish/<target_crate>/` with that as the working directory.

| Step | Command | Purpose |
|------|---------|---------|
| Format | `cargo fmt --check` | Catches rewriter formatting damage. |
| Build | `cargo build` | Catches identifier rewrite regressions and dependency union errors. |
| Test | `cargo test --all-targets --all-features` | Runs unit, integration, doctest, and public-feature tests from the merged sources. |
| Lint | `cargo clippy --all-targets --all-features -- -D warnings` | Lint policy enforced on the merged shape. |
| Dry-run | `cargo publish --dry-run --allow-dirty` | Catches manifest-level issues (missing fields, license-file mismatches, `include`/`exclude` problems). `--allow-dirty` is required because `publish/` is generated under the workspace and is git-ignored. |

Each step's stdout/stderr is streamed; the first non-zero exit aborts the pass with a one-line summary identifying the failing step.

### User-Facing Scripts

Two scripts in `scripts/` are the only entry points the operator interacts with.

**`scripts/prep-publish.sh`** — workspace-shape sanity, standalone-runnable.

```bash
#!/usr/bin/env bash
set -euo pipefail
cargo fmt --all -- --check
cargo build --workspace --all-targets
scripts/testing/check-async-test-timeouts.py
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
echo "prep-publish: workspace is clean."
```

**`scripts/publish.sh`** — full publish-readiness gate. Runs prep first (so a broken workspace is never merged), then regenerates and validates the publish tree.

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$HERE/prep-publish.sh"
cargo xtask publish-tree
cat <<'EOF'

publish.sh: all checks passed.
To publish, run:

  (cd publish/aurelia && cargo publish)

EOF
```

Properties:

- Both scripts use `set -euo pipefail` so any failing step aborts cleanly.
- `publish.sh` always invokes `prep-publish.sh` as a hard precondition; there is no flag to skip it.
- Neither script ever runs `cargo publish` itself — that step is always manual.
- Both scripts are pure shells; the actual logic lives in `cargo` and the `xtask` binary, so the scripts stay short and reviewable.

### Failure Modes and Guardrails

| Scenario | How it is detected |
|----------|--------------------|
| New workspace member added without declaring it in the publish config | `PublishConfig` validation fails: "workspace member `aurelia-foo` is not listed in `internal_crates` or `excluded_crates`". |
| Internal crate accidentally marked `publish = true` while listed for merging | `PublishConfig` validation fails: "internal crate `aurelia-foo` must be `publish = false`". |
| Two internal crates resolve to the same module name | `PublishConfig` validation fails. |
| Identifier rewrite breaks compilation in the merged form | `cargo build` in the publish tree fails. |
| Dependency-version conflict across internal crates | `synthesize` fails with the exact conflicting version strings. Equivalent-looking but differently written requirements remain conflicts. |
| Member dependency inherits a missing workspace dependency | `synthesize` fails with a message naming the crate, dependency table, and dependency. |
| Member dependency uses `workspace = true` and locally overrides version/source fields | `synthesize` fails before the publish manifest is written. |
| Misspelled `excluded_crates` entry | `PublishConfig` validation fails because every excluded crate must resolve to a real workspace member. |
| Target or internal crate uses an unsupported dependency table | `synthesize` fails before the publish manifest is written. |
| Target or internal crate introduces `build.rs` | Tree generation fails because build scripts are outside the current source inclusion contract. |
| Internal dependency is aliased under a non-internal dependency key | The internal dependency classifier detects the internal `package` name or normalized local path and removes it before manifest write. |
| Internal dependency or local path dependency remains in the generated manifest | The generated manifest guard fails before the publish manifest is written. |
| Internal crate feature edge remains in a public feature list | The generated manifest guard fails before the publish manifest is written. |
| Workspace dependency inheritance remains in the generated manifest | The generated manifest guard fails before the publish manifest is written. |
| Drift between workspace HEAD and the publish tree | Detected at the next release: `scripts/publish.sh` regenerates from current workspace state and re-runs all checks before any publish. |
| Operator runs the merge against a workspace that does not build or pass tests | `scripts/publish.sh` invokes `scripts/prep-publish.sh` first and aborts before generation. |
| String-literal identifier rewrite causes a runtime regression | Caught by `cargo test` in the publish tree, provided a relevant test exists. Mitigated by narrowing the rewriter to skip string literals via `syn` traversal if a future crate introduces a meaningful occurrence. |

### Testing Scope

The publish flow is itself code with tests.

- **xtask unit tests** (in `src/crates/xtask/src/{rewrite,config}.rs`):
  - `rewrite`: bare paths, `use` statements, doc comments, near-miss substrings (`aurelia_peeringx`, `xaurelia_peering`, `foo_aurelia_peering`, `aurelia_peering_extra`), multi-occurrence lines, string literals, resolver crate identifiers, empty/single/multi-crate configurations.
  - `config`: default-derivation of module names from crate names (`aurelia-peering` → `peering`, `aurelia-foo-bar` → `foo_bar`).
  - `generate`: internal `crate::` path retargeting, exported macro invocation preservation,
    `$crate` preservation, and module declaration insertion after valid Rust file headers.
  - `manifest`: workspace dependency inheritance for target and internal crates, dev-dependency
    inheritance, feature union, optional/default-feature merging, missing inherited dependencies,
    local source override rejection, internal feature expansion, dependency conflict behavior,
    aliased internal dependency removal by package name and path, generated manifest guards for
    leftover internal packages, local path dependencies, internal feature edges, unsupported
    dependency table rejection, unconditional canonical `[lib]` generation, preservation of
    first-merged table-form dependencies, and positive coverage that expected external dependencies
    remain after synthesis.
  - `validate`: publish-tree validation command construction, including the all-targets and
    all-features test command in full validation mode and the shorter build/dry-run command set in
    check mode.
- **End-to-end via the merged tree:**
  - `scripts/publish.sh` regenerates `publish/aurelia/` and runs the full workspace and publish-tree validation passes against the real workspace. The publish-tree's `cargo test` exercises every `#[cfg(test)] mod tests` block from the merged sources, which is the primary correctness signal that the three-stage rewriter has produced a buildable, behaviourally correct crate.
- **Workspace tests are unaffected:**
  - `cargo test --workspace` continues to run from the unmodified workspace and exercises integration tests under `src/crates/*/tests/` that do not transit the publish tree. `scripts/prep-publish.sh` is the canonical wrapper.

### Adding a New Internal Crate

To absorb a new internal crate (for example, `aurelia-discovery`) into the published artefact:

1. Add the crate as a workspace member with `publish = false` under `src/crates/<name>/`, following existing crate conventions.
2. Append one line to `[workspace.metadata.aurelia-publish] internal_crates`:

   ```toml
   { name = "aurelia-discovery" },
   ```

   Or `{ name = "aurelia-discovery", module = "discovery" }` to override the default module name.
3. From `src/lib`, refer to the new crate as a normal path dependency and re-export whichever symbols become part of the public `aurelia` API.
4. Run `scripts/publish.sh` (or `cargo xtask publish-tree --check` for a faster build-only check). The xtask picks up the new crate, generates the merged tree, and validates it. No xtask code changes required.

If the new crate is dev-only or test-only and should *not* be merged, add it to `excluded_crates` instead.

### Versioning

Only `aurelia` is published. Internal crates' `version` fields are not user-visible and are kept synchronised as a workspace convention. The published `aurelia` version is the only number that follows SemVer relative to the public API.

### Public API Migration Notes

Auth configurations that contain PKCS#8 private-key material are intentionally non-`Clone`.
Applications that reload auth material must retain their original source location or their own
protected source bytes and construct a fresh `Pkcs8AuthConfig` for each reload. Private-key fields
use `Pkcs8PrivateKey`; callers can pass ordinary key bytes with `Vec<u8>::into()` or pass
`zeroize::Zeroizing<Vec<u8>>` directly with `.into()` when the bytes are already managed in a
zeroizing wrapper.

### Out of Scope

- Multi-version backports, yanking, and crates.io owner management.
- Publishing additional separate crates (for example, an `aurelia-cli` binary). If that becomes desired, it is a separate caravaggio.
- Auto-tagging git on successful publish; the tag step is manual.
- Reusing the workspace `Cargo.lock` for the publish tree; the publish tree generates its own lock during validation.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
