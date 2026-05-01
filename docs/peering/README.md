# Peering

Status: Developed

## Objectives

- Provide a persistent, mTLS-authenticated, asynchronous message fabric between peers.
- Deliver transport-only guarantees: route messages, apply backpressure, and complete sends only on remote ingress ACK.
- Maintain peer and taberna isolation so one slow target does not block others.
- Support a separate blob callis to isolate large transfers.
- Integrate cleanly with A2/A3 via routing and codec boundaries.

## Non-Objectives

- Business semantics, request/response semantics, or application-level processing guarantees.
- Configuration management, discovery policy, quorum/consensus, or taberna directory ownership.

## Technical Details

This overview is intentionally light on technical detail. The authoritative design documentation lives in the Caravaggio documents below.

### Validation

The peering crate is validated from committed commands:

- `cargo check --workspace`
- `cargo test --workspace`
- `cargo test --workspace --all-features`
- `scripts/testing/run-peering-e2e.sh`

The peering end-to-end runner is the baseline network validation path and is expected to run from the committed repository layout via `scripts/testing/run-peering-e2e.sh`.

### Module Structure

- `src/crates/peering/src/transport.rs`: public transport surface, wiring, and re-exports only.
- `src/crates/peering/src/transport/listener.rs`: TCP/TLS listener and inbound accept path.
- `src/crates/peering/src/transport/peer.rs`: peer state mutation loop and snapshot access.
- `src/crates/peering/src/transport/primary.rs`: primary callis reconnect/dial policy.
- `src/crates/peering/src/transport/primary_dispatch.rs`: primary outbound dispatcher (queue + callis selection).
- `src/crates/peering/src/transport/handshake.rs`: hello negotiation and inbound/outbound callis establishment.
- `src/crates/peering/src/transport/frame.rs`: frame IO helpers and enqueue/dequeue helpers.
- `src/crates/peering/src/transport/callis.rs`: callis tasks, `CallisHandle`, `OutboundFrame`.
- `src/crates/peering/src/transport/tls.rs`: certificate/URI validation helpers.
- `src/crates/peering/src/transport/blob/mod.rs`: blob subsystem façade, manager, retained frame tracking, and re-exports.
- `src/crates/peering/src/transport/blob/dispatch.rs`: outbound blob scheduling and ACK coordination.
- `src/crates/peering/src/transport/blob/receive.rs`: inbound blob request/start/chunk handling.
- `src/crates/peering/src/transport/tests/*.rs`: unit tests for transport logic, kept out of module files per `docs/testing.md`.
- `src/crates/peering/src/peering/mod.rs`: internal peering API surface and re-exports.
- `src/crates/peering/src/peering/dispatch.rs`: routing + remote dispatch (facade is routing, not wire send).
- `src/crates/peering/src/peering/builder.rs`: `RouteLocalRemoteBuilder` setup and configuration.
- `src/crates/peering/src/domus.rs`: Domus public API surface and wiring.
- `src/crates/peering/src/delivery.rs`: local delivery to taberna sinks and stream sinks (shared by dispatch paths and transport receive).

### Module Structure Details

- `delivery.rs` is `pub(crate)` only and is not part of the public API surface.
- `delivery.rs` owns local delivery logic for:
  - taberna sink delivery with timeout/error mapping
  - stream sink open/accept/close/abort for blob flows
- Peer state mutation is isolated to `transport/peer.rs`; dispatchers read snapshots and write directly to callis writers.
- `PeerSession::receive_message` remains the inbound entrypoint for non-blob frames, but becomes a thin wrapper:
  - dedupe/ACK bookkeeping stays in `session.rs`
  - actual sink delivery is delegated to `delivery::deliver_message(...)`
- Transport receive boundaries:
  - non-blob inbound frames call `PeerSession::receive_message` (which delegates to `delivery.rs`)
  - blob inbound frames call `delivery.rs` directly (stream sink handling bypasses `PeerSession`)
- Peering dispatch boundaries:
  - `peering/dispatch.rs` handles routing + remote dispatch for non-blob sends; blob dispatch lives in `transport/blob/dispatch.rs`
  - local delivery in these paths calls into `delivery.rs` (no duplicated sink logic)
- `src/crates/peering/src/session.rs`: inbound receive, dedupe, and ACK handling (kept as-is unless split later).

### Role and Connection State Transitions

Peer roles are `listener` and `originator`. All peers start as listeners.

| Current Role | Event | Action | Next Role |
| --- | --- | --- | --- |
| listener | startup | wait `listener_delay` (default 5s), accept inbound primary connections | listener |
| listener | outbound messages queued and no active primary connection (after a previously established connection) | wait `listener_reconnect_timeout` (default 20s), then dial peer | originator |
| listener | outbound messages queued and no active primary connection (startup, no prior connection) | after `listener_delay`, dial peer | originator |
| listener | inbound primary connection established | run hello handshake | listener |
| originator | outbound messages queued and no active primary connection | dial peer immediately | originator |
| originator | connection break | follow reconnect backoff schedule | originator |
| originator | crash/restart | restart in listener mode | listener |

### Document Map

- `docs/peering/services-provided.md`: services and configuration surface provided by peering.
- `docs/peering/transport-model.md`: callis lifecycle, delivery, and failure semantics.
- `docs/peering/connection-limits.md`: A0 connection admission and callis limits.
- `docs/peering/blobs.md`: blob transfer overlay semantics and blob configuration details.
- `docs/peering/ring-buffer.md`: blob ring buffer design and API.
- `docs/peering/taberna-model.md`: taberna sink contract and registry behavior.
- `docs/peering/routing-resolver.md`: route resolution boundaries and rules.
- `docs/peering/reliability-store.md`: retained messages, ACK tracking, replay, and dedupe.
- `docs/peering/wire-protocol.md`: wire header, framing, and transport control messages.
- `docs/peering/backpressure-queues.md`: queue bounds, inflight limits, and scheduling policy.
- `docs/peering/codec-integration.md`: typed message codec and adapter strategy.
- `docs/peering/e2e-tests.md`: peering-specific end-to-end scenarios and plan.
- `docs/peering/socket-transport.md`: socket transport backend and A0 authentication.
- `docs/peering/tcp-transport.md`: TCP transport backend and A0 authentication.
- `docs/peering/simple-resolver.md`: map-based resolver for higher-level integrations.

### Logging Requirements

- Repository-wide logging requirements and levels are defined in `docs/aurelia.md`.

### Cross-Cutting References

- `docs/ids.md` is the gold source for ID definitions used by the peering crate.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
