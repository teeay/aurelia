# Simple Resolver

Status: Developed

## Objectives

- Provide the map-based `SimpleResolver` in the internal `aurelia-resolver`
  crate.
- Use the shared `RouteResolver` trait and `DomusAddr` type from `aurelia-data`.
- Expose `SimpleResolver` from the public `aurelia::SimpleResolver` path.
- Keep resolver logic deterministic, asynchronous, in-memory, and
  transport-neutral.
- Merge `aurelia-resolver` into the published `aurelia` crate through the
  publish-tree and public extract flows.

## Technical Details

### Overview

`SimpleResolver` is a small, in-memory `RouteResolver` implementation that maps
`TabernaId` to `DomusAddr`. It is intended for tests, local deployments, and
higher layers that wire explicit mappings without a discovery system.

The implementation lives in the internal `aurelia-resolver` crate at
`src/crates/resolver`. The resolver contract dependency is:

```text
aurelia-resolver -> aurelia-data
```

The full crate dependency set is intentionally narrow:

- `aurelia-data`: `RouteResolver` and `DomusAddr` contract.
- `aurelia-ids`: `TabernaId`, `AureliaError`, and `ErrorId`.
- `tokio/sync`: non-poisoning in-memory route storage.
- `async-trait`: implementation of the shared async resolver trait.

### Crate Layout

```text
src/crates/resolver/
  Cargo.toml
  src/lib.rs
```

The crate manifest is:

```toml
[package]
name = "aurelia-resolver"
version = "0.1.0"
edition.workspace = true
license.workspace = true
repository.workspace = true
publish = false

description = "Resolver implementations for Aurelia."

[lib]
path = "src/lib.rs"

[dependencies]
async-trait = "0.1"
tokio = { version = "1", features = ["sync"] }
aurelia-ids = { path = "../ids" }
aurelia-data = { path = "../data" }
```

### Data Model

- `routes`: `HashMap<TabernaId, DomusAddr>`

The routes map is protected by a single `tokio::sync::RwLock` to keep the API
lightweight and easy to reason about. The lock is non-poisoning and compatible
with the repository concurrency policy.

### API Surface

```rust
use std::collections::HashMap;

use aurelia_ids::{AureliaError, ErrorId, TabernaId};
use aurelia_data::{DomusAddr, RouteResolver};
use tokio::sync::RwLock;

pub struct SimpleResolver {
    inner: RwLock<HashMap<TabernaId, DomusAddr>>,
}

impl SimpleResolver {
    pub fn new() -> Self;
    pub async fn insert(&self, taberna_id: TabernaId, domus: DomusAddr);
    pub async fn remove(&self, taberna_id: TabernaId);
    pub async fn clear_all(&self);
}

#[async_trait::async_trait]
impl RouteResolver for SimpleResolver {
    async fn resolve(&self, taberna_id: TabernaId) -> Result<DomusAddr, AureliaError>;
}
```

### Resolution Semantics

- If `taberna_id` exists in `routes`, return the stored `DomusAddr`.
- If `taberna_id` is unknown, return `AureliaError` with
  `ErrorId::UnknownTaberna`.

`SimpleResolver` does not perform transport-type enforcement. The peering layer
enforces that the returned `DomusAddr` matches the local transport type derived
from the Domus address.

### Concurrency and Performance

- Reads are lock-shared; writes are lock-exclusive.
- All operations are in-memory and non-blocking except for the lock.
- No background task, channel, or queue is introduced.

### Public API

The top-level `aurelia` crate re-exports the resolver:

```rust
pub use aurelia_data::RouteResolver;
pub use aurelia_resolver::SimpleResolver;
```

The public user path is:

```rust
use aurelia::SimpleResolver;
```

`aurelia_peering::SimpleResolver` is not part of the supported public API.

### Publishing and Public Extract

The resolver crate is an internal crate and is merged into the published
`aurelia` crate. The root publish metadata includes:

```toml
[workspace.metadata.aurelia-publish]
internal_crates = [
  { name = "aurelia-ids" },
  { name = "aurelia-data" },
  { name = "aurelia-logging" },
  { name = "aurelia-peering" },
  { name = "aurelia-resolver" },
]
```

The generated publish tree contains:

```text
publish/aurelia/src/resolver/mod.rs
```

The publish-tree rewriter maps:

```text
aurelia_resolver -> crate::resolver
aurelia_data -> crate::data
aurelia_ids -> crate::ids
```

The generated `publish/aurelia/Cargo.toml` must not contain an
`aurelia-resolver` dependency. Its non-internal dependencies are merged into the
single published crate manifest.

The public extract publishing path validates crates.io publication from the
generated publish tree. Publication runs from `publish/aurelia`.

### Example Usage

```rust
use std::sync::Arc;

use aurelia::{DomusAddr, SimpleResolver};

let resolver = Arc::new(SimpleResolver::new());
resolver.insert(taberna_id, DomusAddr::Tcp(peer)).await;

let domus = aurelia
    .domus_builder(config, local_addr, auth, resolver.clone())
    .build()
    .await?;
```

### Testing Scope

- `aurelia-resolver` unit tests cover insert, replacement, remove, clear, empty-clear,
  missing-remove, `DomusAddr::Tcp`, `DomusAddr::Socket`, concurrent resolve during route updates,
  and unknown-tabernas behavior.
- `aurelia-peering` tests validate `RouteResolver` behavior with peering-local
  test resolvers.
- `aurelia` tests and examples validate the public re-export path.
- `xtask` tests validate publish-tree rewriting for `aurelia_resolver`.
- `cargo xtask publish-tree --check` validates the merged public crate shape.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
