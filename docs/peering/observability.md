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

- `snapshot()` -> `Result<DomusMetrics, AureliaError>`
- `snapshot_and_reset()` -> `Result<DomusMetricsDelta, AureliaError>`
- `connected_peer_identities()` -> `Result<Vec<DomusAddr>, AureliaError>`
- `connected_peers()` -> `Result<Vec<PeerIdentityReport>, AureliaError>` (optional richer view)
- `subscribe_events()` -> `broadcast::Receiver<DomusReportingEvent>`
- `subscribe_errors()` -> `broadcast::Receiver<(u64, AureliaError)>`
- `errors_since(seq, limit)` -> `Result<Vec<(u64, AureliaError)>, AureliaError>`

Data collection is always-on. Feeds are best-effort and do not replay history beyond the bounded error
ring buffer and total counters.
If the observability task is unavailable, reporting query methods return `AureliaError` with
`ErrorId::SnapshotNotAvailable`; they must not fabricate empty snapshots or empty query results.
Internal producer emission is also best-effort: transport, handshake, peer-state, config, and
shutdown paths must use non-blocking observability emission and must not wait for observability
queue capacity. Under saturation, producer-side observability commands may be dropped; reporting
query APIs remain request/reply operations and may await their responses.

`PeerIdentityReport` reports the peer address plus active primary and blob callis counts using
`primary_callis_count` and `blob_callis_count`.

### Event and Error Naming Convention

- All normal events **must** end in `Event`.
- All errors **must** end in `Error`.
- Public event payloads are report data, not the internal peering model. Numeric values are exposed
  as primitives (`u64`, `u32`, etc.) and internal enums are converted to stable lower-kebab
  `&'static str` labels before event construction. The private conversion functions use a
  `_label` suffix and live next to the enum definitions.
- `AureliaError` and `ErrorId` remain typed values where actual errors are reported because they
  are core public error contracts.

### Key Events

Key events are any lifecycle or operational transitions that change operator posture.

Recommended `DomusReportingEvent` variants:

- `PeerConnectedEvent { peer: DomusAddr, fresh_session: bool }`
- `PeerDisconnectedEvent { peer: DomusAddr, reason: &'static str }`
- `PeerDialFailedEvent { peer: DomusAddr, callis: &'static str, error_id: ErrorId }`
- `PrimaryCallisConnectedEvent { peer: DomusAddr, callis_id: u64 }`
- `PrimaryCallisDisconnectedEvent { peer: DomusAddr, callis_id: u64, reason: &'static str }`
- `BlobCallisConnectedEvent { peer: DomusAddr, callis_id: u64, settings: BlobCallisSettingsReport }`
- `BlobCallisDisconnectedEvent { peer: DomusAddr, callis_id: u64, reason: &'static str }`
- `PeerSessionRestartedEvent { peer: DomusAddr, reason: &'static str }`
- `BackpressureTriggeredEvent { peer: DomusAddr, taberna_id: u64 }`
- `OutboundQueueOverrunEvent { peer: DomusAddr, tier: &'static str, limit: u64, msg_type: u32 }`
- `ConfigReloadedEvent`
- `AuthReloadedEvent`
- `ShutdownStartedEvent`
- `ShutdownCompleteEvent`

`OutboundQueueOverrunEvent` is emitted whenever A1 cannot insert an item into an outbound ready
queue because the tier is full. A3 overruns correspond to public send admission rejection with
`local-queue-full`. A1/A2 overruns identify library-managed queue pressure. The event must not
include payload bytes.

The primary outbound store is the producer for outbound retained-lane overrun events. It emits the
event for every retained-lane `Full` rejection path:

- initial tracked-message admission;
- bare A1 frame insertion;
- response insertion.

Replay after callis loss uses retained slot state transitions (`ReplayReady`). Overrun events are
emitted at retained-lane capacity decision points: original admission, bare A1 insertion, and
response insertion.

If a completion-bearing item is failed with `local-queue-full`, the overrun event is emitted before
the completion is failed. If a bare A1 frame is dropped because the A1 queue is full, the overrun
event is emitted before the drop.

Queue tier labels are `"a1"`, `"a2"`, and `"a3"`. Callis labels are `"primary"` and `"blob"`.
Disconnect and restart reasons use stable lower-kebab labels such as `"remote-closed"`,
`"peer-restarted"`, and `"fresh-session"`.

### Errors

Errors are emitted as `(seq, AureliaError)` where `seq` is a monotonically increasing sequence
number. `AureliaError.kind` identifies the error ID, and `AureliaError.message` includes
key-value context (for example `peer=...`, `phase=...`, or `local_addr=...`) when available.

Errors are retained in a bounded ring buffer (sequence-numbered) for `errors_since`.

### Metrics

`DomusMetrics` (absolute) and `DomusMetricsDelta` (since last reset) must include:

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
- `total_outbound_queue_overruns`
- `total_a1_queue_overruns`
- `total_a2_queue_overruns`
- `total_a3_queue_overruns`
- `created_at`
- `last_reset_at`
- `last_snapshot_at`

`created_at` is the time the metrics state was created. `last_reset_at` is the most recent
`snapshot_and_reset` boundary. `last_snapshot_at` is the time the returned snapshot was produced.
For `DomusMetricsDelta`, cumulative counters are measured since `last_reset_at`, and interval
peaks are reset to the current gauge values after the delta snapshot is produced.

### Observability Module Layout

Observability is implemented under `src/crates/peering/src/observability/`:

- `mod.rs`: module facade, construction helpers, and internal wiring.
- `types.rs`: public reporting data types and event enums.
- `reporting.rs`: `DomusReporting` and `DomusReportingFeeds`.
- `handle.rs`: internal `ObservabilityHandle` producer methods.
- `actor.rs`: `ObservabilityStore`, `ObservabilityCommand`, actor state, reducer, effects, and
  `run_observability`.

The module contains:

- `DomusReporting` type that wraps a shared `Arc<ObservabilityStore>`.
- `ObservabilityStore` with:
  - a single-owner state task that owns the metrics state, peer identity set, and error ring buffer
  - `mpsc::Sender<ObservabilityCommand>` for mutation via channel/queue pattern
  - `broadcast::Sender<DomusReportingEvent>`
  - `broadcast::Sender<(u64, AureliaError)>`
  - bounded ring buffer for errors with a monotonically increasing `seq`
- Helper methods for incrementing counters and emitting events/errors (delegating state mutation to the owner task).
- Internal producer helpers use non-blocking `try_send` into the command channel. Observability
  must not apply backpressure to transport hot paths.

The single-owner state task is a **core task** as defined in `docs/concurrency.md`. It is
already compliant with the patterns in that document: a `while let Some(cmd) =
rx.recv().await` consumer over a single `mpsc::Receiver<ObservabilityCommand>` (Pattern 4
"single mpsc consumer" shape), with no shared mutable state visible to other tasks. New
observability commands must be added through this channel rather than through additional
locks or notify primitives.

### Observability Actor Reducers

The observability task remains a single-owner actor with one `mpsc::Receiver<ObservabilityCommand>`
and one owned `ObservabilityState`. Command handling is split into deterministic reducers with
explicit outputs; the public reporting API is unchanged.

Reducer structure:

- A synchronous command reducer takes `&mut ObservabilityState` plus one
  `ObservabilityCommand` and returns an effect set containing events to broadcast, errors to
  broadcast/store, and oneshot replies to complete.
- Metrics updates, peer-count updates, error-ring updates, and event construction are explicit
  steps inside the reducer or named helper reducers.
- Broadcast sends and oneshot replies are effects. They run after state mutation for the
  command is complete.
- `Snapshot`, `SnapshotAndReset`, `ConnectedPeers`, `ConnectedPeerIdentities`, and
  `ErrorsSince` commands produce replies from the same state snapshot used by the reducer.
- Duplicate disconnects do not underflow current counts. Duplicate connects do not inflate
  `current_peers` beyond the peer's first active callis transition.
- Error sequence numbers remain monotonically increasing even if error broadcast receivers
  lag or are absent.

Primitive requirements:

- No additional locks, notifies, or background tasks may be added to mutate observability state.
- The actor remains an `mpsc` command consumer; event and error `broadcast` channels remain
  best-effort effect sinks.
- Slow or absent subscribers must not block command processing.

Testing coverage:

- Unit tests must cover reducer behavior for primary/blob connect, primary/blob disconnect,
  duplicate disconnect, duplicate connect, fresh-session event emission, dial failure, protocol
  violation, identity mismatch, auth/config reload, shutdown started/completed, snapshot,
  snapshot-and-reset, connected peers, and `errors_since`.
- Tests must assert event ordering relative to metrics updates for a command.
- Tests must assert bounded error ring capacity and monotonic sequence numbers.
- Integration tests must continue to verify connected peer reporting and address mismatch error
  reporting from the transport paths.

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
