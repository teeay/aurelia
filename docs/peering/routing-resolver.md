# Peering Routing Resolver

Status: Developed

## Objectives

- Define the resolver boundary for taberna-to-peer routing.
- Specify local, remote, and unknown resolution outcomes.
- Keep discovery and directory logic outside this crate.

## Technical Details

### Resolver Boundary

The transport depends on a resolver for taberna reachability. `aurelia-peering`
consumes the `RouteResolver` trait, `DomusAddr`, and `TransportKind` from the shared
`aurelia-data` crate. Resolver implementations depend on that shared contract, not on the peering
transport implementation.

```rust
#[async_trait::async_trait]
trait RouteResolver {
    async fn resolve(&self, taberna_id: u64) -> Result<DomusAddr, AureliaError>;
}
```

Resolution is asynchronous; the transport enforces timeout behavior around resolver calls.

### Resolution Rules

- If the destination taberna is local, deliver locally.
- If the destination taberna resolves to a peer domus address, send remotely on the primary callis.
- Blob callis delivery uses the same peer address as primary; the resolver must not introduce a distinct address for blob traffic.
- If the destination taberna is unknown, fail immediately.

The peering crate does not perform discovery, probing, or route computation.
Resolvers do not report reachability or availability; they only map taberna IDs
to a domus address or return `AureliaError` with `ErrorId::UnknownTaberna` (not
found) or `ErrorId::PeerUnavailable` (resolver failure) with an optional
message.

### Concrete Resolver Implementations

Reusable resolver implementations are documented under `docs/resolver/`.
`SimpleResolver` is defined in `docs/resolver/simple-resolver.md`.

### Transport Neutrality (Single Transport)

Each Aurelia instance operates on a single transport type (TCP or socket) derived once at build time from
the local Domus address. The resolver returns a `DomusAddr` that must match the local transport type.
Returning a different transport type is an error and must fail resolution.

```rust
pub enum DomusAddr {
    Tcp(std::net::SocketAddr),
    Socket(std::path::PathBuf),
}
```

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
