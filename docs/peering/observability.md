# Peering Observability (DomusReporting)

Status: Developed

## Objectives

- Provide a dedicated observability surface (`DomusReporting`) for operational metrics, peer identity visibility, and real-time events/errors without embedding policy into A1.
- Consolidate counters and event dispatch into a single `observability` module and wire it into the transport/handshake paths.
- Resolve review issues `P013` and `P014` by exposing connectivity queries and higher-layer error hooks.
- Ensure data collection is always-on while allowing event/error feeds to be connected at any time.

## Technical Details

### API Surface (A1-Observable, Policy-Free)

Expose a secondary interface from `Domus`:

```rust
impl Domus {
    pub fn reporting(&self) -> DomusReporting;
}
```

Provide a build-time helper to attach feeds before transport start:

```rust
impl DomusBuilder {
    pub async fn build_with_reporting(self) -> Result<(Domus, DomusReportingFeeds), AureliaError>;
}
```

`DomusReportingFeeds` contains pre-subscribed `broadcast::Receiver` instances for events and errors,
created before `transport.start()` to capture initialization and early inbound connections.

`DomusReporting` provides:

- `snapshot()` -> `DomusMetrics`
- `snapshot_and_reset()` -> `DomusMetricsDelta`
- `connected_peer_identities()` -> `Vec<DomusAddr>`
- `connected_peers()` -> `Vec<PeerIdentityReport>` (optional richer view)
- `subscribe_events()` -> `broadcast::Receiver<DomusReportingEvent>`
- `subscribe_errors()` -> `broadcast::Receiver<(u64, AureliaError)>`
- `errors_since(seq, limit)` -> `Vec<(u64, AureliaError)>`

Data collection is always-on. Feeds are best-effort and do not replay history beyond the bounded error
ring buffer and total counters.

### Event and Error Naming Convention

- All normal events **must** end in `Event`.
- All errors **must** end in `Error`.

### Key Events

Key events are any lifecycle or operational transitions that change operator posture.

Recommended `DomusReportingEvent` variants:

- `PeerConnectedEvent { peer: DomusAddr, callis: CallisKind, fresh_session: bool }`
- `PeerDisconnectedEvent { peer: DomusAddr, callis: CallisKind, reason: DisconnectReason }`
- `PeerDialFailedEvent { peer: DomusAddr, callis: CallisKind, error_id: ErrorId }`
- `PrimaryCallisConnectedEvent { peer: DomusAddr, callis_id: CallisId }`
- `PrimaryCallisDisconnectedEvent { peer: DomusAddr, callis_id: CallisId, reason: DisconnectReason }`
- `BlobCallisConnectedEvent { peer: DomusAddr, callis_id: CallisId, settings: BlobCallisSettingsReport }`
- `BlobCallisDisconnectedEvent { peer: DomusAddr, callis_id: CallisId, reason: DisconnectReason }`
- `PeerSessionRestartedEvent { peer: DomusAddr, reason: RestartReason }`
- `BackpressureTriggeredEvent { peer: DomusAddr, taberna_id: TabernaId }`
- `ConfigReloadedEvent`
- `AuthReloadedEvent`
- `ShutdownStartedEvent`
- `ShutdownCompleteEvent`

### Errors

Errors are emitted as `(seq, AureliaError)` where `seq` is a monotonically increasing sequence
number. `AureliaError.kind` identifies the error ID, and `AureliaError.message` includes
key-value context (for example `peer=...`, `phase=...`, or `local_addr=...`) when available.

Errors are retained in a bounded ring buffer (sequence-numbered) for `errors_since`.

### Metrics

`DomusMetrics` (absolute) and `DomusMetricsDelta` (since last snapshot) must include:

- `current_peers`
- `current_primary_callis`
- `current_blob_callis`
- `peak_peers`
- `peak_primary_callis`
- `peak_blob_callis`
- `total_primary_opened`
- `total_primary_closed`
- `total_blob_opened`
- `total_blob_closed`
- `total_dial_attempts`
- `total_dial_failures`
- `total_identity_mismatch`
- `total_protocol_violation`
- `created_at`
- `last_snapshot_at`

### Observability Module Layout

Create `src/crates/peering/src/observability.rs`:

- `DomusReporting` type that wraps a shared `Arc<ObservabilityStore>`.
- `ObservabilityStore` contains:
  - atomic counters
  - a single-owner state task that owns the peer identity set and error ring buffer
  - `mpsc::Sender<ObservabilityCommand>` for mutation via channel/queue pattern
  - `broadcast::Sender<DomusReportingEvent>`
  - `broadcast::Sender<(u64, AureliaError)>`
  - bounded ring buffer for errors with a monotonically increasing `seq`
- Helper methods for incrementing counters and emitting events/errors (delegating state mutation to the owner task).

### Wiring Points (Non-Exhaustive)

- Dial attempts/failures: transport handshake/dial paths.
- Peer connected/disconnected: `PeerStateUpdate::Connected` and disconnect paths.
- Identity mismatch: `validate_backend_identity` and transport backend auth validation.
- Protocol violation: any error with `ErrorId::ProtocolViolation`.
- Shutdown: `Domus::shutdown` and transport shutdown flow.

### Testing Scope

- Unit tests for `observability`:
  - snapshot increments and reset behavior
  - ring buffer bounds and `errors_since` ordering
  - event/error broadcast delivery
- Integration tests:
  - connected peer identities reflect actual live peers
  - identity mismatch emits `ErrorId::AddressMismatch`
  - connect/disconnect emits the correct `*Event` variants

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
