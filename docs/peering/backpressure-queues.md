# Backpressure and Queue Bounds

Status: Developed

## Objectives

- Define bounded queues and inflight limits for reliable transport.
- Define taberna ingress backpressure behavior.
- Define callis scheduling and priority rules.

## Technical Details

### Configuration Surface

All backpressure and timeout settings are configured through a constructor or builder and must support in-flight updates via atomic config swap. A lightweight config access handle is exposed so higher-level crates can read and update these settings without binding to the internal store.

```rust
pub struct DomusConfigAccess {
    // internal store is opaque
}

impl DomusConfigAccess {
    pub async fn snapshot(&self) -> DomusConfig;
    pub async fn update(&self, next: DomusConfig) -> Result<DomusConfig, AureliaError>;
}
```

Send timeout is the single end-to-end bound for sender completion and must include any delay from queueing, reconnects, and ACK wait.
Configuration validation failures return `AureliaError` with `ErrorId::InvalidConfig`.

Admission and connection limits are also part of backpressure policy:

- `inbound_handshake_limit_total`: maximum in-flight inbound handshakes across all peers.
- `inbound_handshake_limit_per_peer`: per-peer in-flight handshake limit while under the high-water mark.
- `max_parallel_callis_per_peer`: cap on active callis per peer (primary + blob).

### Bounded Outbound Queue

Per peer (and typically per callis), maintain a bounded queue of messages awaiting transmission. If full, the sender awaits capacity in async mode with a timeout. On timeout, the send fails with `send-timeout` as defined in `docs/ids.md`.

Defaults:

- Send queue size: 128.
- Send timeout: 30 seconds.

Send timeout is an end-to-end bound that includes time spent waiting for dispatcher capacity and
callis availability. Peer state mutation must never be blocked by message traffic.

### Bounded Inflight Window

Per peer, maintain a bounded number of transmitted but unacknowledged messages. When full, the sender awaits capacity with a timeout before placing more application messages on the wire. On timeout, the send fails with `send-timeout` as defined in `docs/ids.md`. Capacity for critical transport control traffic is always preserved.

Defaults:

- Inflight window size: 16.

### Bounded Taberna Ingress

Remote tabernae may refuse enqueue when full. The receiver must attempt immediate enqueue into the
destination taberna inbox. If the inbox is full, the message fails immediately with `taberna-busy`
as defined in `docs/ids.md`. If enqueue succeeds, the receiver waits for taberna acceptance with a
timeout; on timeout, the message fails with `taberna-busy`. In these cases:

- Do not ACK.
- Return a transport failure (`taberna-busy`).
- Do not introduce additional inbound buffering beyond the taberna inbox.

Defaults:

- Accept timeout: 5 seconds.
- Taberna accept queue size: 2.

### Callis Scheduling

Primary callis scheduling uses strict priority tiers with FIFO ordering within each tier.
All outbound traffic is treated as messages; A1 control traffic is represented as messages whose
`MessageType` falls in the A1 range defined in `docs/ids.md`.

- A1: transport control and transport-critical traffic.
- A2: Aurelia service messages.
- A3: application messages.

Socket transport mirrors TCP: blob traffic uses a separate blob callis. Priority rules apply per callis, and blob traffic must not starve transport control or normal application messages on the primary callis.

### Handshake Admission and Callis Caps

Inbound callis are gated in A0 before A1 `hello` by the pre-authentication admission limits
described in `docs/peering/transport-model.md` and `docs/peering/connection-limits.md`. This bounds
concurrent handshake work across all peers.

Active callis are capped per peer via `max_parallel_callis_per_peer`. This is enforced independently
of handshake admission to keep long-lived connection counts within bounds.

### Blob Streaming Backpressure

Blob traffic is streamed and chunked. A1 must:

- Apply per-stream backpressure (do not allow a single stream to monopolize the callis).
- Enforce a maximum chunk size to keep control/message frames schedulable.
- Allow multiple blob streams to interleave on the same callis.
- Preserve priority for control frames over blob chunks.

Blob chunk sizing, ACK window, buffering, and ordering semantics are defined in `docs/peering/blobs.md`.

### Total Blob Buffer Cap (Per Domus)

A1 enforces a total blob buffer capacity across all peers, with separate caps for outbound and inbound flows. Defaults and reservation rules are defined in `docs/peering/blobs.md`.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
