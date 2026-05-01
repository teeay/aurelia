# Simple Resolver

Status: Developed

## Objectives

- Provide a map-based `RouteResolver` implementation for higher layers.
- Keep resolver logic deterministic, synchronous-in-memory, and transport-neutral.

## Technical Details

### Overview

`SimpleResolver` is a small, in-memory `RouteResolver` that maps `TabernaId` to `DomusAddr`. It is intended
for tests, local deployments, and higher layers that want to wire explicit mappings without a discovery system.

### Data Model

- `routes`: `HashMap<TabernaId, DomusAddr>`

The routes map is protected by a single `RwLock` to keep the API lightweight and easy to reason about.

### API Surface

```rust
pub struct SimpleResolver {
    inner: tokio::sync::RwLock<SimpleResolverState>,
}

struct SimpleResolverState {
    routes: std::collections::HashMap<TabernaId, DomusAddr>,
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
- Else return `AureliaError` with `ErrorId::UnknownTaberna`.

`SimpleResolver` does not perform transport-type enforcement. The peering layer enforces that the returned
`DomusAddr` matches the local transport type derived from the Domus address.

### Concurrency and Performance

- Reads are lock-shared; writes are lock-exclusive.
- All operations are in-memory and non-blocking except for the lock.

### Example Usage

```rust
let resolver = Arc::new(SimpleResolver::new());
resolver.insert(taberna_id, DomusAddr::Tcp(peer)).await;

let domus = DomusBuilder::new(config, local_addr, auth, resolver.clone())
    .build()
    .await?;
```

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
