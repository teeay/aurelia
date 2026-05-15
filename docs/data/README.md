# Aurelia Data

Status: Developed

## Objectives

- Own shared, transport-neutral Aurelia domain data used across internal crates.
- Keep resolver implementations independent from the peering transport implementation.
- Provide the routing boundary contract consumed by peering, resolver, testing, and the public
  `aurelia` crate.

## Technical Details

The `aurelia-data` crate lives at `src/crates/data` and is merged into the published `aurelia`
crate. It owns:

- `DomusAddr`: transport address identity for TCP and Unix socket domus peers.
- `TransportKind`: the transport family derived from a `DomusAddr`.
- `RouteResolver`: the async boundary contract that maps `TabernaId` to `DomusAddr`.

`aurelia-data` depends only on `aurelia-ids` and `async-trait`. It must not depend on
`aurelia-peering`, resolver implementations, logging, or transport-specific dependencies.

The top-level `aurelia` crate re-exports these contracts from `aurelia-data`:

```rust
pub use aurelia_data::{DomusAddr, RouteResolver, TransportKind};
```

Application code imports these contracts from `aurelia::{DomusAddr, RouteResolver, TransportKind}`.
The `aurelia_data` crate is an internal workspace boundary, not the supported application import
path.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
