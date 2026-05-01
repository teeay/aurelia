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
  - The fundamental authenticated transport layer: Unix socket auth for the socket backend and
    mTLS for the TCP backend.
  - Completes before any A1 `hello` frames are exchanged.
  - Backend-specific authentication details live in `docs/peering/socket-transport.md` and
    `docs/peering/tcp-transport.md`.

- **A1 (Message and Blob Transfer):**
  - The message and blob transfer layer responsible for delivery and callis/taberna management.
  - Implemented by the `aurelia-peering` crate in `src/crates/peering`.
  - Compile time resolution and generics preferred over dyn.

- **A2 (Aurelia Services):**
  - Supporting service capabilities provided by the `aurelia` library on top of A1.
  - All A2 components communicate through A1.

- **A3 (Application Layer):**
  - The host application that uses Aurelia services.
  - All A3 communication with other A2/A3 entities goes through A1.

### Logging Levels (Operational)

Logging levels and intent:

- `info`: Operational milestones that establish progress or state (for example, connection established or handshake completed).
- `warn`: Errors that are handled without jeopardizing system functionality.
- `error`: Errors that jeopardize system functionality or require operator attention.
- `debug`: Major internal activities within a function that explain control flow and decisions.
- `trace`: All significant steps that are worth logging for deep troubleshooting.

Rate-limited logging and log ID assignment are defined in `docs/logging.md` and `docs/ids.md`.

### Concurrency Policy

- Poisonable synchronization types are prohibited: `std::sync::Mutex`, `std::sync::RwLock`, and `std::sync::Once`.
- If no optimal or specialized non-poisonable mechanism exists, mutation must be performed via channel- or queue-based patterns.
- Standard pattern: asynchronous, thread-safe mutation with synchronous value reads.
- **REQUIREMENT: QUEUES AND CHANNELS MAY ONLY BE INTRODUCED WHEN THE USER SPECIFICALLY PERMITS THEM; RANDOM OR UNNECESSARY QUEUES/CHANNELS ARE PROHIBITED. QUEUES/CHANNELS ARE ONLY PERMITTED FOR CONCURRENT MUTATION AND SYNCHRONOUS READ STRUCTURES WHEN THE USER EXPLICITLY APPROVES ADDING THEM.**

### Communication Constraints

- All communication across A2 and A3 entities must transit A1.
- A2 and A3 entities always behave like domus from the perspective of A1.

### Workspace Structure

- `src/lib`:
  - The main publishable `aurelia` crate.
  - Houses A2 service capabilities as they are introduced.
  - Provides the Aurelia wrapper that owns the Aurelia runtime and is the only supported entry
    point. See `docs/runtime.md` for the runtime ownership model.

- `src/crates/peering`:
  - `aurelia-peering` (A1) implementation.

- `src/crates/ids`:
  - `aurelia-ids` (internal, `publish = false`) shared IDs + `AureliaError`.

- `src/crates/testing`:
  - `aurelia-testing` utilities for test harness support across layers.

- `docs/peering`:
  - A1 (peering) Caravaggio documentation.

### Aurelia Runtime Wrapper

- The `aurelia` crate provides a top-level `Aurelia` wrapper that owns a single Tokio runtime for the library.
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
