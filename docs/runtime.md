# Aurelia Runtime

Status: Developed

## Objectives

- Provide a single Aurelia-level wrapper that owns the runtime for the library.
- Own one Tokio runtime for Aurelia that stays alive for the lifetime of the host application.
- Ensure Aurelia internal background and cleanup work runs on the Aurelia runtime, independent of any ambient runtime.
- Define how Aurelia's runtime coexists with application runtimes without creating coupling or hidden blocking.
- Keep the runtime handle internal to Aurelia; it is not part of the public API surface.
- Require all Aurelia components to use the Aurelia runtime for background work and cleanup.

## Technical Details

### Runtime Ownership

- The internal `aurelia-platform` crate owns a single `tokio::runtime::Runtime` created once per
  process.
- The runtime is stored in a `static OnceLock<Runtime>` and never shut down early.
- `OnceLock` is non-poisoning and is used here instead of `std::sync::Once`, which is prohibited by
  the workspace concurrency policy for live library/runtime code.
- The runtime handle is cloned and passed into all Aurelia subsystems that need to spawn background work.

### Runtime Usage Across Aurelia

- The Aurelia wrapper calls `aurelia_platform::runtime::ensure()` so application code still
  initialises Aurelia through `Aurelia::new()`.
- All Aurelia components must use the Aurelia runtime for background tasks and drop cleanup.
- No Aurelia component may depend on an ambient runtime via `Handle::try_current()` for cleanup.
- The runtime handle is strictly internal to the merged Aurelia library and never part of the
  public API surface.
- Any synchronous API that needs async work must schedule it onto the Aurelia runtime.

### Runtime Interaction With Application Runtimes

- Aurelia runtime is dedicated to Aurelia internal tasks and does not require the application to enter it.
- If the host application has its own Tokio runtime, both runtimes run concurrently on separate thread pools.
- Aurelia async APIs can still be awaited on the application runtime; the internal background work runs on the Aurelia runtime via the injected handle.
- No blocking calls are made on the application runtime by the Aurelia wrapper; `block_on` (if exposed) is limited to the Aurelia runtime.

### Testing Scope

- Unit tests for runtime initialization and internal handle wiring.
- Integration tests that drop Aurelia capability handles outside an ambient runtime and verify cleanup side effects.
- End-to-end suites in `scripts/testing/` that exercise Aurelia capabilities.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
