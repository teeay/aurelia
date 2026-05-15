# Aurelia Structure

## Repository Notes

### Purpose

- Multi-crate Rust workspace centered on the `aurelia` library crate.
- Supporting crates live under `src/crates/*`.

### Layout

- Workspace root: `Cargo.toml`
- Main crate: `src/lib`
- Supporting crates: `src/crates/*`
- Docs: `docs/`
- Scripts: `scripts/`
- Test apps: `testing/`

### Async Policy

- All operations must be asynchronous and enforce timeouts.
- If an operation cannot be asynchronous, it must be explicitly flagged and approved by the user before implementation.

### IDs

- `docs/ids.md` is the gold source for all IDs used across the workspace.
- The shared internal crate `aurelia-ids` (under `src/crates/ids`) owns all ID definitions and the
  single `AureliaError` type used across the library.

### Errors

- Aurelia uses a single error type, `AureliaError`, end-to-end across all crates.
- Per-crate error enums are not permitted; all errors must be expressed as `AureliaError` with an
  `ErrorId` and optional message.
- `docs/errors.md` is the gold source for error semantics and expected handling.

### Common Commands

- Build: `cargo build`
- Test: `cargo test`
- Check: `cargo check`
- Format: `cargo fmt`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`

## Objectives

- Define the A0/A1/A2/A3 layer model for the Aurelia library.
- Describe how the workspace crates map to each layer.
- Ensure all A2/A3 communication flows through A1 domus behavior.

## Technical Details

### Layer Model

- **A0 (Transport Authentication):**
  - The fundamental authenticated transport layer: PKCS#8 certificate-backed Unix socket auth for
    the socket backend and mTLS for the TCP backend.
  - Completes before any A1 `hello` frames are exchanged.
  - Backend-specific authentication details live in `docs/peering/socket-transport.md` and
    `docs/peering/tcp-transport.md`.

- **A1 (Message and Blob Transfer):**
  - The message and blob transfer layer responsible for delivery and callis/taberna management.
  - Implemented by the `aurelia-peering` crate in `src/crates/peering`.
  - Consumes the shared `RouteResolver`, `DomusAddr`, and `TransportKind` contract from
    `aurelia-data` and enforces transport semantics for resolved routes.
  - Compile time resolution and generics preferred over dyn.

- **A2 (Aurelia Services):**
  - Supporting service capabilities provided by the `aurelia` library on top of A1.
  - All A2 components communicate through A1.
  - Reusable resolver implementations are provided by `aurelia-resolver`.

- **A3 (Application Layer):**
  - The host application that uses Aurelia services.
  - All A3 communication with other A2/A3 entities goes through A1.

### Logging

Logging levels, rate-limited logging, and logging utility semantics are defined in
`docs/logging.md`. Log ID assignment is defined in `docs/ids.md`.

### Dependency Policy

- Dependencies that require C toolchains, native compilation, or bundled native libraries must be
  avoided unless the user explicitly approves them.
- Rust-native dependencies are preferred when they can satisfy the same requirement.
- TLS and cryptographic provider choices must be explicit in manifests; default feature sets that
  pull in native providers are not permitted.

### Concurrency Policy

- In live library/runtime code, poisonable synchronization types are prohibited:
  `std::sync::Mutex`, `std::sync::RwLock`, and `std::sync::Once`.
- If no optimal or specialized non-poisonable mechanism exists, mutation must be performed via channel- or queue-based patterns.
- Standard pattern: asynchronous, thread-safe mutation with synchronous value reads.
- **REQUIREMENT: QUEUES AND CHANNELS MAY ONLY BE INTRODUCED WHEN THE USER SPECIFICALLY PERMITS THEM; RANDOM OR UNNECESSARY QUEUES/CHANNELS ARE PROHIBITED. QUEUES/CHANNELS ARE ONLY PERMITTED FOR CONCURRENT MUTATION AND SYNCHRONOUS READ STRUCTURES WHEN THE USER EXPLICITLY APPROVES ADDING THEM.**

### Communication Constraints

- All communication across A2 and A3 entities must transit A1.
- A2 and A3 entities always behave like domus from the perspective of A1.

### Public API Boundary

The published `aurelia` API must expose domain-level contracts, not internal implementation
structures.

- The `aurelia` crate is the only supported application entry point.
- Internal implementation crates such as `aurelia-peering` may expose compatibility symbols for
  workspace use, but those exports do not define the supported application API.
- Internal structs, traits, type aliases, and enums must not be re-exported unless they are
  explicitly part of the supported user contract.
- Core public contracts such as `AureliaError`, `ErrorId`, `Domus`, `Taberna`,
  `TabernaRequest`, `TabernaRequestParts`, `TabernaCompletion`, `MessageCodec`, configuration,
  authentication, and address types are intentional public API.
- Public report payloads must use primitive numeric types instead of internal numeric aliases
  unless the alias itself is part of the public domain contract.
- Internal enums that feed public reports must be converted to stable `&'static str` labels before
  entering the public structure. The conversion function must be private, use a `_label` suffix,
  and live next to the enum definition.
- Do not export label constants unless a specific API requirement justifies adding those symbols.
- Application callbacks must not be allowed to choose internal Aurelia transport error IDs. A3
  rejection is represented by `TabernaRequest::reject()` and maps only to
  `RemoteTabernaRejected`; codec and queue errors are produced by the layer that owns them.

### Workspace Structure

- `src/lib`:
  - The main publishable `aurelia` crate.
  - Houses A2 service capabilities as they are introduced.
  - Provides the Aurelia wrapper that owns the Aurelia runtime and is the only supported entry
    point. See `docs/runtime.md` for the runtime ownership model.

- `src/crates/peering`:
  - `aurelia-peering` (A1) implementation.
  - Consumes the shared data contract and owns transport enforcement for resolved routes.

- `src/crates/resolver`:
  - `aurelia-resolver` (internal, `publish = false`) reusable resolver
    implementations.
  - Provides `SimpleResolver`, which depends on the shared `RouteResolver`
    trait and is re-exported by the top-level `aurelia` crate.

- `src/crates/data`:
  - `aurelia-data` (internal, `publish = false`) shared domain data and boundary contracts.
  - Owns `DomusAddr`, `TransportKind`, and `RouteResolver` so resolver implementations do not
    depend on the peering transport implementation.
  - The top-level `aurelia` crate re-exports these contracts directly for application code.

- `src/crates/platform`:
  - `aurelia-platform` (internal, `publish = false`) cross-crate platform services.
  - Owns the singleton Aurelia runtime used by internal components for background work and cleanup.
  - Not re-exported by the top-level `aurelia` crate; it is an implementation detail.

- `src/crates/ids`:
  - `aurelia-ids` (internal, `publish = false`) shared IDs + `AureliaError`.
  - Public shared ID and error API is re-exported directly by the top-level `aurelia` crate.

- `src/crates/logging`:
  - `aurelia-logging` (internal, `publish = false`) logging utilities.
  - Provides rate-limited logging support used by the peering crate and merged into the published
    `aurelia` crate.

- `src/crates/xtask`:
  - `xtask` (internal, `publish = false`) workspace tooling.
  - Provides publish-tree generation and validation commands.

- `docs/peering`:
  - A1 (peering) Caravaggio documentation.

- `docs/resolver`:
  - Resolver implementation Caravaggio documentation.

### Aurelia Runtime Wrapper

- The internal `aurelia-platform` crate owns the single Tokio runtime for the library, and the
  `aurelia` crate provides the top-level `Aurelia` wrapper that initialises it.
- All Aurelia components (including Domus and future capabilities) must use this Aurelia runtime for internal background work and cleanup.
- The runtime handle is internal to the library and not part of the public API surface.
- The Aurelia wrapper should be as thin as possible and prefer re-exported types to allow compile-time optimization with minimal indirection.
- All Aurelia capabilities must be accessed through the Aurelia API wrapper rather than sub-crate entry points.
- See `docs/runtime.md` for requirements and implementation details.

### Distribution Guardrails

- All internal Aurelia component crates are marked `publish = false`; only the `aurelia` crate is intended for publication.
- The publishable `aurelia` crate is assembled from the workspace by merging the internal crates into a single self-contained crate at publish time. The merged tree is generated under a git-ignored `publish/` directory; the workspace itself is never modified by the publishing flow. See `docs/publish.md` for the tooling and procedure.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
