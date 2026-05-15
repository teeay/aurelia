# Peering Transport Model

Status: Developed

## Objectives

- Define callis lifecycle and transport role transitions.
- Define delivery and failure semantics for transport operations.
- Clarify what transport does and does not guarantee.
- Per-callis receive loops, primary callis transmitters, and blob callis transmitters follow the
  wakeup patterns in `docs/concurrency.md` and avoid head-of-line blocking.
- All transport core tasks comply with the workspace concurrency rules.
- Make the peer state task and blob transmitter paths exhaustively auditable: each event has an
  explicit transition, an explicit effect set, a deterministic effect execution order, and
  focused tests that validate the sequencing boundary.
- Reduce the remaining audit burden in peer state, callback rendezvous, BlobManager, and
  concurrency-sensitive tests without changing transport semantics.
- Each peer handle owns the peer-scoped task set and spawns every peer-owned active task through
  the Aurelia runtime handle.
- A single primary outbound store owns the full outbound work item across ready,
  writing, inflight, replay, expiry, and ACK handling.

## Technical Details

### Per-Callis A0 Authentication

Every primary and blob callis is admitted by its own complete A0 transport authentication path
before A1 `hello`.

TCP primary callis, TCP blob callis, and test-opened second or third TCP callis perform TCP
connect, mTLS, certificate URI SAN validation, callback connection, callback nonce validation, and
connect-back proof validation independently.

Socket primary callis, socket blob callis, and test-opened second or third socket callis perform
the full socket connect-back authentication handshake independently.

The backend repeated-callis test surface is test-only. It drives real backend `dial()` and
`accept()` calls so tests exercise production A0 handshakes. Normal production peer management
opens callis through the existing primary and blob lifecycle; no public API is provided for
application code to open arbitrary secondary or tertiary primary callis.

### Callis Setup Deadline

`callis_connect_timeout` is the A1 callis setup budget. It starts when A1 decides a primary or
blob callis is required for fresh connection or reconnect, and it ends when the A1 `hello` /
`hello-response` exchange completes.

The callis setup budget covers the complete connection setup path:

- backend dial or accepted authenticated stream setup;
- TCP socket connect and mTLS for TCP;
- Unix socket connect and socket authentication for socket transport;
- A0 callback validation;
- A1 `hello` write;
- A1 `hello-response` read and validation.

`callis_connect_timeout` is inside the A3 `send_timeout` budget. A send may spend part of its
overall `send_timeout` waiting for a callis to be established; retained message delivery and ACK
waiting use the remaining item deadline.

Configuration requirements:

- default: 15 seconds;
- minimum: greater than zero;
- maximum: 5 minutes;
- must be less than or equal to `send_timeout`.

Timeout failure during callis setup reports the dial/connection attempt as failed and returns to
the existing reconnect or send-failure state machine for that peer. A timed-out callis setup must
not leave a primary or blob callis registered as active.

### A0 Timeout Scope

A0 authentication owns transport identity establishment before A1 can exchange `hello` frames.
The A0 timeout starts at the first point A0 work can consume transport/authentication resources:

- inbound TCP: immediately after raw TCP accept and successful pre-authentication permit
  acquisition;
- inbound socket: immediately after Unix socket accept and successful pre-authentication permit
  acquisition;
- outbound TCP: when the backend begins TCP connect for the callis;
- outbound socket: when the backend begins Unix socket connect for the callis.

The TCP A0 timeout is `tcp_handshake_timeout`, with default 10 seconds. It covers TCP connect,
mTLS, certificate URI SAN validation, callback connection, callback nonce validation, and
connect-back proof validation. `tcp_callback_timeout` remains a narrower cap for the callback
sub-step.

The socket A0 timeout is `socket_handshake_timeout`, with default 5 seconds. It covers socket
connect, first auth-frame read, certificate validation, callback connection, callback nonce
validation, and challenge/proof validation. `socket_callback_timeout` remains a
narrower cap for the callback sub-step.

Timeout errors use stable local diagnostic messages:

- TCP A0 timeout: `PeerUnavailable` with `tcp handshake timeout`;
- TCP callback timeout: `PeerUnavailable` with `tcp callback timeout`;
- socket A0 timeout: `PeerUnavailable` with `socket handshake timeout`;
- socket callback timeout: `PeerUnavailable` with `socket callback timeout`;
- socket filesystem timeout while probing or cleaning socket paths: `PeerUnavailable` with
  `socket filesystem timeout`.

Inbound pre-authentication permits are released on A0 success, validation failure, timeout, or
connection close. A stalled peer must not be able to hold an inbound pre-authentication permit
beyond the configured A0 timeout.

### Inbound Backend Authentication Concurrency

Inbound backend accept loops must continue accepting raw transport streams while earlier streams
are still performing A0 authentication, subject to the configured pre-authentication limits.

For TCP and socket transports, each accepted raw stream enters backend-owned authentication work
spawned through the Aurelia runtime handle. The spawned work owns its pre-authentication permit,
applies the backend A0 timeout to the complete authentication path, and returns only successfully
authenticated A1-ready streams through the backend accepted-stream queue. Backend-internal callback
connections that complete A0 rendezvous state are handled as internal events and do not surface as
accepted A1 streams.

TCP and socket backends share the inbound A0 permit, timeout, and accepted-result dispatch
scaffold. Backend-specific code supplies only the authenticated-stream construction body: TCP
performs TLS accept before A0 validation, while socket transport runs the socket A0 validation
directly on the accepted Unix stream. Timeout diagnostics remain backend-specific.

Backend A0 authentication work is detached from a single `accept()` await. If an `accept()` caller
is cancelled after raw streams have been accepted, already spawned authentication work continues
until it succeeds, fails, times out, or the backend is dropped. Accepted-stream queue capacity is
the maximum validated `inbound_handshake_limit_total` so completions can be queued up to the
configured admission ceiling.

### Inbound Error Frame Validation

Inbound `error` control frames carry an `error_id` defined by `docs/ids.md`. Unknown `error_id`
values are malformed A1 wire input and are handled as `protocol-violation`. The raw unknown ID is
included in the local diagnostic message. Unknown inbound error IDs are not mapped to
`peer-unavailable`.

### Peer Runtime Ownership

Each remote peer is represented at runtime by a `PeerHandle`. The handle owns the peer-scoped task
set, passive peer state graph, and active work required to communicate with that peer.
`PeerRuntime` is a construction helper that assembles this component graph and returns the owning
handle.

Required ownership model:

- `PeerHandle` is the peer-facing handle/facade and lifetime owner. It exposes operations, holds
  peer-scoped state, and owns the task set used for peer-owned active work. It is not itself an
  actor loop.
- The peer construction path assembles the peer component graph:
  - `PeerSession`;
  - `PrimaryDispatchManager`;
  - `BlobManager`;
  - peer state update channel and snapshot channel;
  - primary/blob availability notifies;
  - shutdown watches/notifies;
  - callis tracker and handshake gates;
  - current peer identity/dial address state;
  - observability and limited logging handles.
- The peer handle owns a task set for all peer-scoped active work. Task spawning must go through
  the injected Aurelia `tokio::runtime::Handle`, not through ambient `tokio::spawn`.
- The peer task set is the owner of final cleanup. Spawned task closures may receive a cloneable
  peer task spawner, but they must not receive or hold the owning task set. Spawner clones may keep
  the abort-handle registration channel open; they must not keep task-set shutdown or final abort
  cleanup alive.
- Passive state containers may expose async entry points for active work, but they must not spawn
  their own long-running tasks during construction.
- Peer teardown first signals the existing shutdown/close primitives, then lets tasks exit through
  their normal loop conditions. Stored task abort handles may be used only as final cleanup when
  the peer handle is dropped and a task has not exited. The task-set owner must have an explicit
  shutdown signal to its manager task so final abort cleanup can run even while a stuck spawned task
  still holds a spawner clone.
- Peer shutdown requests are latching state. A peer state task must observe shutdown even when the
  request is made before the task re-enters its wait.

Peer-owned active tasks include:

- peer state task;
- one primary transmitter task per active primary callis;
- blob transmitter tasks owned per active blob callis, as specified by `docs/peering/blobs.md`;
- primary outbound reclaimer;
- graceful-close close-frame helper;
- dial scaffolds for primary and blob callis;
- callis reader tasks spawned after successful handshakes.

This ownership boundary exists so runtime policy, task lifecycle, peer context, observability, and
shutdown sequencing are auditable from one peer-scoped structure. Lower-level components still own
their data invariants; the peer handle owns when and where their active loops run.

#### Passive Component Boundaries

`PeerSession` owns ACK/reliability semantics and uses an injected primary outbound component; it
must not construct the outbound store internally. The primary outbound component owns retained
lane state, keyed ACK lookup, replay state, and expiry state, but active per-callis transmitter
loops and the reclaimer loop are spawned through the peer handle's task spawner. `BlobManager`
owns blob state and outbound work availability. Blob callis transmitters are spawned through the
peer handle's task spawner and lease work directly from `BlobManager` as defined in
`docs/peering/blobs.md`.

The constructor direction is:

```rust
PeerRuntime::new(peer_context) {
    let primary_context = PrimaryDispatchManagerContext {
        runtime_handle,
        initial_send_queue_size,
        overrun_reporter,
    };
    let (primary_dispatch, reclaimer) = PrimaryDispatchManager::new(primary_context);
    let session = PeerSession::new(allocator, config, runtime_handle, primary_dispatch);
    let blob = BlobManager::new(...);
    let handle = PeerHandle { session, primary_dispatch, blob, ... };
    spawner.spawn(reclaimer.run());
    spawner.spawn(run_peer_state(...));
    // Each successful blob callis handshake spawns:
    // spawner.spawn(run_blob_callis_reader(...));
    // spawner.spawn(run_blob_callis_transmitter(callis_id, writer, blob, ...));
    // Each successful primary callis handshake spawns:
    // spawner.spawn(run_primary_callis_reader(...));
    // spawner.spawn(run_primary_callis_transmitter(callis_id, writer, primary_dispatch, ...));
}
```

The concrete helper structs used to pass constructor arguments may vary, but the ownership boundary
must not: peer-scoped active tasks are spawned through the peer-owned task set, and passive
containers do not depend on an ambient runtime.

#### Peer Task Lifecycle

The peer-owned task set must record an abort handle for every long-running peer-owned task it
spawns.
The owning task set must not be cloned into spawned tasks. A separate `PeerTaskSpawner`-style handle
may be cloned into peer-scoped helpers that need to spawn nested peer-owned tasks. Dropping the
owning task set signals the task-set manager to abort all recorded handles immediately; this cleanup
must not depend on every spawner clone being dropped first.
Tasks should normally exit when:

- the peer state update channel is closed;
- the peer shutdown watch changes;
- the associated passive component is dropped and the task's weak reference can no longer be
  upgraded;
- the callis reader/writer observes close, cancellation, or I/O failure.

Caducus queues created by peer-scoped components must also use the injected Aurelia runtime handle
explicitly through `MpscBuilder::runtime`. This applies to any peer-scoped caducus queue.
Constructors must not rely on caducus' ambient `Handle::try_current()` path.

Task loops that wait on `Notify` must follow `docs/concurrency.md` Pattern 1. Any weak-owned task
that can become idle forever must hold a separate wake handle, and the passive component's `Drop`
must wake that handle so the task can observe failed weak upgrade and exit.

### Cross-Component Concurrency Audit

The transport model coordinates the concurrency ownership boundaries across the peering
Caravaggio documents. Detailed component requirements live in each component's primary document:

- BlobManager and blob receiver waits: `docs/peering/blobs.md`.
- Primary retained outbound lanes, ACK tracking, replay, and outbound backpressure:
  `docs/peering/backpressure-queues.md`.
- TCP callback rendezvous: `docs/peering/tcp-transport.md`.
- Socket callback rendezvous: `docs/peering/socket-transport.md`.
- Observability actor: `docs/peering/observability.md`.
- Test timeout policy: `docs/testing.md`.
- Peering end-to-end coverage: `docs/peering/e2e-tests.md`.

Implementations keep the guarantees defined in this document for peer state, primary transmitters,
blob transmitters, callis lifecycle, listener ownership, and fresh-session cleanup. New queues or
channels are allowed only when the owning Caravaggio permits that primitive and the primitive
choice is justified by
`docs/concurrency.md`.

#### Maintainability and Validation Requirements

The transport implementation is structured so concurrency-sensitive branches are auditable in
small, named ownership domains:

- peer state event execution exposes each event's transition, effects, and sequencing boundary
  without requiring unrelated branches to be read;
- TCP and socket callback rendezvous share common lifecycle mechanics while backend-specific
  validation remains in the owning transport modules;
- BlobManager state is split into domain modules for callis pool, outbound streams, inbound
  streams, reservations, and dispatch snapshots;
- concurrency tests use short, scenario-specific operation deadlines on local loopback paths
  instead of production-oriented defaults;
- E2E tests exercise realistic concurrent traffic and failure overlap without becoming a broad
  stress suite with nondeterministic timing.

Changes to these areas must add or update the narrowest useful tests first, run those targeted
tests before wider suites, and keep scenario timeouts short enough that missed wakeups or close
timeout bugs surface promptly.

#### Peer State Task Executor Decomposition

The peer state task remains the authoritative owner of `PeerState`. The synchronous
`PeerStateMachine` transition boundary is retained, and async event execution is delegated through
named executor helpers so effect paths are inspectable without changing behavior.

Required structure:

- Each `PeerStateUpdate` variant must have one named async executor helper or one small match arm
  that delegates to a helper.
- The synchronous transition must produce `PeerStateEffects` before async side effects run.
- Async executor helpers may mutate `PrimaryDispatchManager`, `PeerSession`, `BlobManager`, callis
  shutdown senders, and observability, but they must not directly publish peer snapshots.
- The peer state loop epilogue remains the only place that updates impaired-state timing and
  publishes `PeerStateSnapshot`.
- Peer-state event executors return post-epilogue observability effects. The state task emits
  those effects only after stale-state cleanup has completed and the loop epilogue has published
  the latest snapshot for the event.
- Fresh-session restart cleanup must remain state-task-synchronous through the stale-state cleanup
  boundary before the fresh primary handle becomes dispatch-visible.
- Graceful close may retain its deadline-bound helper task for close-frame flushing, but the state
  task must enter closing and fail new non-A1 work before spawning that helper.

Testing requirements:

- Direct transition tests must continue to cover all `PeerStateUpdate` variants and effect sets.
- Event-executor tests must cover primary connect, primary disconnect, blob connect, blob
  disconnect, fresh-session restart, graceful close, and teardown deadline behavior.
- Integration tests must continue to cover reconnect replay, blob reconnect replay, peer graceful
  close without listener shutdown, remote close without listener shutdown, and stalled-peer
  isolation.

### Exhaustively Auditable Core Tasks

#### Listener Ownership Boundary

The domus listener is transport-wide, not peer-wide.

- Peer-level `GracefulClose`, `ConnectionClosed(RemoteClose)`, and per-peer teardown must not
  send the transport listener shutdown signal.
- `Transport::shutdown` / domus shutdown is the boundary that stops the listener accept loop.
- Peer close may reject or close callis associated with that peer, fail new non-A1 outbound work,
  and drain peer-owned primary/blob handles, but it must leave the listener available for other
  peers and for restarted instances of the same peer.
- `PeerHandle::Drop` must not signal listener shutdown either. Replacing a session-closing
  handle in `peer_handle_for` releases the previous `Arc<PeerHandle>`; if its destructor stopped
  the listener, the peer's TCP callback during the next outbound dial would be refused and
  recovery after a peer-initiated graceful close would deadlock.
- Tests must prove peer graceful close, remote peer close, and dropping a replaced peer handle do
  not stop the listener and that a later inbound callis can still be accepted.

#### Peer State Event Contract

The peer state task must handle each `PeerStateUpdate` event in two explicit phases:

1. Apply a deterministic transition to `PeerState` and produce `PeerStateEffects`.
2. Execute effects in the single shared order defined below.

Each event must have direct transition tests that assert both post-state and effect set:

- `EnsurePrimaryDial`:
  - No effect when closing, when a primary handle is active, when primary dial is already in
    flight, when capacity is exhausted, or when there is no pending primary outbound work.
  - Otherwise set `primary_dial = Dialing`, update role to `Originator` when required, and emit
    exactly one primary dial effect.
  - `primary_dial` stores dial activity only. Suppression by an active primary is derived from the
    active primary pool and the dial activity state.
- `Connected(Primary)`:
  - If closing or the session is closing, emit only a close effect for the new handle; the handle
    must not enter the dispatch-visible snapshot.
  - If active, clear primary dial state, reset primary reconnect backoff, set `had_primary = true`,
    add the new handle to the active primary pool, and wake primary dispatch.
  - If `fresh_session = true`, existing primary handles and stale blob state must be cleaned
    before any follow-up blob dial decision is evaluated.
- `Connected(Blob)`:
  - If closing, emit only a close effect for the new handle.
  - If active, validate negotiated blob settings, clear blob dial state/backoff, add the callis to
  `BlobManager`, publish blob observability, and wake blob transmitters if active streams exist.
- `DialFailed(Primary)`:
  - Clear primary dial state for the failed attempt.
  - If a primary is active, do not schedule another primary dial.
  - If no primary is active and pending primary outbound work exists, emit one reconnect effect
    using the configured backoff and impaired-window rules.
  - If no work remains, remain idle.
- `DialFailed(Blob)`:
  - Clear blob dial state for the failed attempt.
  - If no primary is active, do not schedule blob dial.
  - If active blob streams exist and no blob callis exists, emit one blob reconnect effect using
    blob reconnect backoff and capacity rules.
- `Disconnect(Primary)` and `ConnectionClosed(Primary)`:
  - Remove only the selected closed/disconnected primary handles.
  - Publish the reduced snapshot.
  - If no primary remains and retained/pending primary work exists, emit at most one reconnect
    effect.
  - If no primary remains and no work exists, remain idle and allow the impaired-window epilogue
    to decide teardown.
- `Disconnect(Blob)` and `ConnectionClosed(Blob)`:
  - Remove only the selected blob callis or drain all blob callis when the event requests all.
  - Requeue inflight blob frames for transient loss.
  - Reassign active streams before evaluating whether a blob reconnect is needed.
  - Emit at most one blob reconnect effect when active streams remain and a primary is active.
- `EnsureBlobDial`:
  - No effect when closing, when no primary is active, when blob dial is already in flight, when a
    blob callis exists, or when there are no active blob streams.
  - Otherwise emit exactly one blob dial effect.
- `GracefulClose`:
  - Enter closing exactly once.
  - Spawn one peer shutdown task with an absolute close deadline computed at trigger time as
    `Instant::now() + config.send_timeout`.
  - Latch retained-store shutdown so new A1/A2/A3 work is rejected, retained A2/A3 is failed, and
    primary transmitters drain only retained A1 ACK/ERROR responses.
  - Set close intents for active primary calles only after retained A1 ACK/ERROR responses are
    empty or the close deadline expires.
  - Drain peer primary/blob handles during teardown.
  - Do not stop the domus listener.

#### Fresh-Session Restart Boundary

A fresh primary session means the remote peer restarted or no longer agrees with the retained
session state. The surviving peer keeps the same peer handle, but stale transport state must be
invalidated deterministically before new blob activity is evaluated.

Required sequence:

1. Drain existing primary handles from `PeerState`.
2. Fail retained A2/A3 tracked messages with `peer-restarted`.
3. Clear primary control entries that cannot be replayed on the fresh session.
4. Fail active blob sender/receiver streams with `peer-restarted`.
5. Drain blob callis handles and close their write paths.
6. Clear blob outbound rings, in-flight lookup, finish write state, pending blob requests,
   reservations, and completed-stream caches.
7. Reset blob callis history.
8. Add the fresh primary handle and publish the new primary snapshot.
9. Only after the cleanup boundary is complete may `EnsureBlobDial` or blob transmitters observe
   blob lifecycle state and decide whether a new blob callis is needed.

Fresh-session handling also clears the inbound primary-message dedupe history. Peer message IDs
are scoped to a remote peer process/session, so a restarted peer may legitimately reuse low
message IDs such as `0`. The originator-side fresh-session path must invalidate old dedupe entries
before accepting messages from the new session, otherwise valid post-restart deliveries could be
ACKed as duplicates without reaching the destination taberna.

This cleanup must not be a fire-and-forget task whose completion is invisible to the peer state
task. If any work is delegated to helper tasks, the effect contract must state what is already
complete before the next state transition is processed.

#### Blob Transmitter Wakeup Contract

Blob work availability uses `Notify::notify_one` because active blob callis transmitters use a
handoff pattern for ordinary work availability. Blob transmitters are core tasks and must use the
same auditable shape required by `docs/concurrency.md`:

1. Construct the `notified()` waiter.
2. Claim at most one outbound blob work item for the transmitter's callis.
3. Re-check lifecycle state and request `EnsureBlobDial` when active streams exist without a blob
   callis.
4. Await the armed waiter only after the claim/recheck step.

Tests must cover work that becomes available before waiter construction, between waiter
construction and claim, and after transmitters park.

#### Effect Execution Order

Peer-state events execute in a fixed order:

1. Apply the synchronous `PeerStateMachine` transition and produce `PeerStateEffects`.
2. Mutate peer-owned queues, session state, and blob state needed to make the transition visible
   to waiters.
3. Execute dial, close, and restart effects through the shared effect helpers.
4. Run the loop epilogue exactly once: update impaired-state timing and publish the latest
   `PeerStateSnapshot`.
5. Emit observability for the resource transitions performed by the event.

The peer state task does not publish snapshots from individual match arms. Primary and blob dial
spawns go through the shared dial-effect executors. Fresh-session cleanup is awaited by the state
task before the fresh primary is added to `PeerState`; graceful close delegates deadline-bound
close-frame flushing to one helper task after the peer has entered closing and new non-A1 work has
been failed.

### Peer Session

A peer session represents the logical transport relationship with a peer. It may span transient
reconnects of the underlying socket, preserving retained tracked messages when the peer has not
restarted.

### Peer Handle Lifecycle (Simple)

The peer handle is an ephemeral, per-peer structure. The lifecycle is intentionally simple and **does not** use epochs or other session counters.

Phases:

- **Idle:** no callis are active and no reconnect attempts are in flight.
- **Active:** one or more callis are established. The first callis is always the primary. Blob callis may be established on demand.
- **Impaired:** all callis are down due to a network error. The peer handle remains in place,
  retains tracked outbound messages, and continues to accept outbound sends within configured
  limits. Reconnect attempts run using existing callis management and role/backoff logic. The
  impaired window is bounded by `send_timeout`.
- **Closing:** a negotiated peer close has begun. The domus listener remains running. New outbound
  sends for this peer fail immediately with `peer-unavailable`. The peer shutdown task owns the
  close deadline, computed as `Instant::now() + config.send_timeout`, fails retained A2/A3 work,
  drains retained A1 ACK/ERROR responses until empty or deadline, then closes the active primary
  calles.
- **Teardown:** the peer handle is destroyed. All retained A2/A3 tracked messages are failed
  locally with `peer-unavailable`, and retained A1 ACK/ERROR responses are dropped.

### Peer State Ownership and Branch Sequencing

The peer handle lifecycle is owned by one peer state core task. The state task owns only
mutable lifecycle state and returns side-effect instructions for async work. It must not
perform message delivery or hold traffic hostage while it is waiting for I/O.

Ownership boundaries:

- `PeerState` owns role, active primary handles, active blob-callis metadata, dial state,
  reconnect counters, closing state, and impaired timing.
- `PrimaryDispatchManager` owns outbound primary lanes, keyed ACK lookup, replay state, slot state,
  and ACK waiters.
- Each active primary callis has one transmitter task that owns that callis' write half and claims
  work directly from `PrimaryDispatchManager`.
- The per-callis receive loop owns inbound frame parsing and emission of ACK/ERROR
  outcomes for frames it received.
- `BlobManager` owns blob streams, blob callis pool state, and blob-callis generation
  publication.

The peer state task publishes a `PeerStateSnapshot` after every state transition that can
change dispatch visibility. The snapshot is a latched `watch` value and is the only
read-side contract used by dispatch tasks for active primary handles.

#### Primary Dispatch Sequencing

The primary send path must follow this sequence:

1. `Domus::send` / `Transport::send_remote` resolves the peer and inserts a tracked outbound item
   into the retained store with a fixed deadline derived from `send_timeout`.
2. If no active primary exists, the peer state task schedules primary dial when retained work is
   present and capacity allows.
3. Each active primary callis owns a transmitter task. That task claims the next dispatchable item
   directly from `PrimaryDispatchManager` by strict lane priority: A1 ACK, A1 ERROR control,
   A2 tracked, then A3 tracked.
4. The peer state task starts a primary dial only when all of these are true:
   the peer is not closing, no active primary handle exists, no primary dial is already
   in flight, primary callis capacity is available, and the retained store has work.
5. When a primary callis becomes active, its transmitter waits on retained-store work, shutdown,
   and peer/callis close signals.
6. To write, the callis transmitter claims a retained slot under the store lock, transitions that
   slot to `Writing { callis_id }`, clones the slot-owned immutable `Arc` write data, drops the
   store lock, and writes the frame to its stream write half under the original message deadline.
7. A send completes only when the receive loop on the sender side observes an ACK or ERROR
   frame and resolves the corresponding retained tracked item by `PeerMessageId`.

Primary transmitters must never rely on keepalive ticks for normal forward progress. Keepalive is
a periodic maintenance source only.

#### Primary State Branches

Each peer state transition has mandatory branch behavior:

- `EnsurePrimaryDial`:
  - If closing, do nothing.
  - If any primary handle is active, do nothing.
  - If a primary dial is already in flight, do nothing.
  - If no primary outbound work exists, do nothing.
  - Otherwise set primary dial state to `Dialing`, set role to `Originator` when required,
    and return exactly one `SpawnPrimaryDial` effect.
- `Connected(Primary)`:
  - If closing, schedule a close for the new handle and do not add it to the active
    snapshot.
  - If not closing, add the handle to the active primary pool before publishing the
    snapshot.
  - Clear primary dial state, reset primary reconnect backoff, set `had_primary = true`,
    and wake primary dispatch in the same state-task iteration.
  - If the handshake indicates a fresh session after reconnect disagreement, fail retained
    A2/A3 tracked messages with `peer-restarted`, fail retained blob streams, and leave retained
    A1 ACK/ERROR response state for the new active primary.
- `DialFailed(Primary)`:
  - Clear primary dial state for the failed attempt.
  - If any primary handle is active, do not schedule another primary dial.
  - If no primary is active and retained work exists, schedule the next dial using reconnect
    backoff, bounded by the impaired window.
  - If no work remains, remain idle.
- `ConnectionClosed(Primary)`:
  - Remove only the closed handle from the active primary pool.
  - Sweep retained slots in `Writing { callis_id }` for the closed callis before forgetting the
    handle. Tracked work returns to `ReplayReady` on transient loss if its deadline has not
    elapsed; retained A1 ACK/ERROR responses return to `Ready` until their cleanup deadline.
  - If other primary handles remain, publish the reduced snapshot and do not reconnect.
  - If no primary handles remain and the peer is not closing, mark eligible tracked inflight work
    replay-ready and schedule reconnect only when retained work exists.
  - If no primary handles remain and no work exists, move to idle.
- `Disconnect(Primary)`:
  - Local disconnect removes the selected primary handles, sends shutdown to their callis
    tasks, sweeps retained `Writing` slots for each removed callis, marks eligible tracked
    inflight work replay-ready if the disconnect is transient, and then follows the same
    no-primary branch as `ConnectionClosed(Primary)`.
- `GracefulClose`:
  - Enter `Closing` exactly once.
  - Spawn exactly one peer shutdown task with an absolute close deadline computed at trigger time
    as `Instant::now() + config.send_timeout`.
  - Set the retained-store shutdown flag so new A1/A2/A3 work is rejected and primary
    transmitters enter A1-response-drain mode.
  - Fail retained A2/A3 tracked messages immediately with `peer-unavailable`.
  - Stop the primary outbound reclaimer; the shutdown task owns shutdown cleanup from this
    point.
  - Retain primary handles in the published snapshot while retained A1 ACK/ERROR responses drain.
  - Do not set close intents at shutdown entry. Close intents are set by the shutdown task only
    after retained A1 responses are empty or the close deadline expires.
  - Do not stop the domus listener.

#### Simultaneous Bidirectional Dial

When both peers send to each other at the same time, each side may have an outbound primary
dial in flight while also accepting an inbound primary. This is a normal branch, not an
error path.

Required behavior:

- An inbound primary handshake is accepted when capacity allows even if an outbound primary
  dial is already in flight.
- The first successfully connected primary immediately becomes dispatch-visible.
- A later duplicate primary connection is handled deterministically: either retain both
  active primary handles for round-robin dispatch, or explicitly close the later handle.
  The implementation must choose one behavior and unit-test it.
- A failed outbound dial must not clear or invalidate an already active inbound primary.
- A failed outbound dial must not start another dial while any primary handle is active.
- The full-mesh four-peer test is the integration gate for this behavior.

#### Closing and Teardown Sequencing

Closing and teardown are different states.

- Immediate callis close and graceful peer shutdown are separate operations. A specific primary
  callis may be closed immediately by setting its close intent; this does not imply peer graceful
  shutdown.
- Graceful peer shutdown first drains retained A1 ACK/ERROR responses, then sets close intents for
  active primary calles.
- Teardown is terminal and drops remaining retained A1 response work after the close deadline.
- `PeerState.primary` must not be drained merely because closing started. Draining primary
  handles is allowed only after the shutdown task has either observed retained A1 response drain
  and set close intents, or the close deadline has expired and teardown begins.
- `CallisTracker::wait_for_zero` is a verification wait during shutdown, not the mechanism
  that makes shutdown correct. A healthy close path should complete promptly and not rely
  on waiting for `2 * send_timeout`.

### Peer Handle Invariants

**No Epochs / Peer Handle Retains Rights**

- The peer handle is the sole retainer of rights to emit ACK/ERROR and complete inbound waiters.
- Epoch counters or any equivalent session counters are prohibited.
- The inbound receive loop must not gate ACK/ERROR emission on epochs or session generations.
- When the peer handle is torn down, all pending inbound waiters (message and blob) are cancelled
  and must not emit ACK/ERROR after teardown.

**No Certificate Pinning Across Callis**

- A1 does not pin peer certificates. Each callis is admitted on its own A0 authentication (mTLS for
  TCP, socket auth for socket).
- Different valid certificates from the same peer address are accepted across callis. This enables
  smooth certificate rotation with no connection drop.
- Identity binding is `peer_addr` only. The address-mismatch guard (`validate_backend_identity`) is
  the sole post-authentication identity check.

**Listener Ownership Boundary**

- Peer-level negotiated close and remote peer close never stop the domus listener.
- The listener accept loop is stopped only by `Transport::shutdown` / domus shutdown.
- A closing peer handle rejects new non-A1 outbound work for that peer and drains peer-owned
  callis, while the listener remains available for other peers and for restarted instances of the
  same peer.

**Reconnect Window Bounded by `send_timeout`**

- When **all** callis to a peer are down, the peer enters the impaired state and starts a
  reconnect window bounded by `send_timeout`.
- Reconnect attempts continue using existing callis management and role/backoff logic.
- If no reconnect succeeds within `send_timeout`, the peer handle is torn down immediately.
- Once teardown begins, no further reconnect attempts are permitted.

**Reconnect Disagreement Reuses the Same Peer Handle**

- If a reconnect handshake is attempted and the peer does not echo `RECONNECT`, the receiver's
  session is gone (the peer was restarted from the receiver's perspective).
- The originator treats the new connection as a fresh session on the **same** peer handle:
  retained A2/A3 tracked messages are failed locally with `peer-restarted`, retained blob streams
  are failed locally, retained A1 ACK/ERROR response work is handled by the primary outbound store
  on the new callis, and the new callis becomes the active primary.

**Teardown Semantics**

- Teardown fails all retained A2/A3 tracked messages locally with `peer-unavailable`.
- Retained A1 ACK/ERROR responses are dropped without emitting further ACK/ERROR frames.
- All inflight blob streams are failed locally and their reservations are released.
- The peer handle cancels all reconnect attempts.

### Inbound Callis Receive Loop

Inbound callis handling must be managed by a **single per-callis worker** that owns the receive
loop and **never spawns a task per message**. The receive loop is responsible only for frame
parsing/validation, accounting, and scheduling of delivery outcomes. It must **not** block on
taberna acceptance, blob callis readiness, or other completion waits.

The receive loop is a **core task** as defined in `docs/concurrency.md` and must follow the
patterns in that document. The core task discipline section defines the mandatory
wakeup-and-drain rules for this loop.

Requirements:

- **Single worker:** exactly one receive loop per callis; no per-message or per-blob `spawn` on the
  inbound path.
- **Non-blocking scheduling:** after frame validation, message/blob delivery is scheduled as an
  in-flight operation and the loop immediately returns to reading the next frame.
- **Bounded by timeouts:** any wait for taberna acceptance or blob readiness must be enforced with
  `accept_timeout`/`send_timeout`-derived deadlines so in-flight operations cannot stall forever.
- **Accept timeout source:** taberna accept timeouts are enforced by the ingress channel TTL
  (from `accept_timeout`) and expiry reclamation; no separate per-message timeout futures are
  permitted.
- **Immediate enqueue attempt:** the loop attempts to place inbound messages directly into the
  destination taberna inbox. If the inbox is full, it must emit `taberna-busy` immediately and must
  not wait for capacity.
- **Caducus ingress channel:** inbound taberna delivery uses the `caducus` MPSC channel with
  TTL/expiry. Expiry reclamation drives `taberna-busy` timeouts via the per-entry expiry report
  channel; the receive loop must clean up any accept waiter for that message before emitting the
  timeout.
- **Event-driven wakeups:** the loop must wake on **any** of:
  - a new inbound frame,
  - completion of an in-flight delivery (ACK/ERROR readiness),
  - timeout of an in-flight delivery,
  - shutdown signal.
- **ACK/ERROR ownership:** ACK/ERROR frames are emitted by the same receive loop worker (not by a
  per-message task), and must be routed through the primary dispatch manager for primary callis.
- **No extra buffering contract:** this loop does not introduce new queues/channels unless
  explicitly approved; it uses in-flight tracking and async polling only.
- **Primary transmission exclusivity:** for primary callis, only that callis' transmitter task may
  write to the stream write half. Other code must not write primary frames directly; it must insert
  A1 work into the primary dispatch manager.

### Peer State Commands

The peer state task owns peer lifecycle transitions. Its command channel carries two classes of
updates:

- **Reliable lifecycle events:** `Connected`, `ConnectionClosed`, `Disconnect`, and
  `GracefulClose` represent specific lifecycle facts or one-shot actions. Producers must enqueue
  them reliably and preserve ordering through the peer state task.
- **Idempotent ensure commands:** `EnsurePrimaryDial` and `EnsureBlobDial` are wakeup requests.
  They mean "evaluate whether this peer should open the relevant callis." The underlying work is
  already visible through retained outbound state or blob lifecycle state, so callers must not wait
  for command-channel capacity before entering their normal send/ACK/blob timeout path. Dropping an
  ensure command because another peer-state update is already queued is valid; the next peer-state
  iteration re-reads the authoritative state and can make the same dial decision.

### Domus + Taberna Shutdown

Shutdown is explicit at each level and must not introduce additional taberna tracking beyond the
registry. When shutdown is initiated, all queued taberna requests are **dropped** (drain means
drop) and reported as `domus-closed`.

Requirements:

- **Registry is the single source of tabernae:** the taberna registry remains the only place where
  tabernae are tracked. Domus must not introduce parallel taberna tracking.
- **Domus shutdown cascades:** `Domus::shutdown` must invoke `TabernaRegistry::shutdown` before
  shutdown completes so local taberna ingress is closed immediately.
- **Taberna shutdown hook:** `TabernaInbox` gains `shutdown` (default no-op) so the registry can
  shut down inboxes without knowing their concrete type.
- **Caducus channel shutdown:** the ingress channel's shutdown marks the channel closed and
  immediately drains all queued entries via the per-item shutdown report channel (no delivery to
  the consumer, no expiry-based handling).
- **Error semantics:** dropped taberna requests must resolve their accept waiters with
  `domus-closed` (new `ErrorId`), not `remote-taberna-rejected` or `taberna-busy`.
- **Taberna receive semantics:** `Taberna::next` returns `Result<TabernaRequest<Codec>, AureliaError>`.
  `receive-timeout` is returned on timeout, and `domus-closed` is returned once shutdown has been
  triggered and no further messages will arrive.
- **Inbound waiters:** inbound callis accept waiters must resolve immediately on taberna shutdown
  so the callis receive loop emits ERROR frames with `domus-closed` using the normal ACK/ERROR
  path.

### Taberna Ingress Channel

The taberna ingress channel — the bounded MPSC channel with TTL/expiry that backs every
`Taberna<Codec>` and `TabernaInboxHandle<Codec>` — is provided by the `caducus` crate. Caducus
supplies the storage, the reclaimer task, snapshot-on-clone sender semantics, and TTL-driven
expiry. The peering crate provides only the report-channel implementations that map expiry and
shutdown back to taberna accept-waiter resolution.

Crate binding:

- `caducus` is a direct dependency of the peering crate.
- Caducus requires a Tokio runtime handle. Domus passes `self.runtime_handle.clone()` to
  `MpscBuilder::runtime` at build time; the implicit `Handle::try_current()` path is not used.

Channel construction (per registered taberna):

- `Domus::taberna(id, codec)` builds the channel via
  `caducus::MpscBuilder::<TabernaRequest<Codec>>::new(taberna_accept_queue_size, accept_timeout)`,
  attaches a per-domus `TabernaShutdownReport<Codec>` via `.shutdown_channel(...)`, sets the
  runtime handle, and stores the resulting `MpscSender` inside `TabernaInboxHandle` and the
  `Receiver` inside `Taberna`.
- `TabernaInboxHandle::new` installs `TabernaExpiryReport<Codec>` via
  `MpscSender::set_expiry_channel`. Each enqueued `TabernaRequest` snapshots this channel at
  send time, so an in-flight item keeps the report channel it was enqueued with even if the
  sender's channel is later replaced.

Report channels:

- `TabernaExpiryReport<Codec>` implements `caducus::ReportChannel<TabernaRequest<Codec>>` and
  resolves the request's completion guard through `TabernaRequest::expire()` so the accept waiter
  observes `taberna-busy`.
- `TabernaShutdownReport<Codec>` implements `caducus::ReportChannel<TabernaRequest<Codec>>` and
  resolves the request's completion guard through `TabernaRequest::shutdown()` so the accept waiter
  observes `domus-closed`.
- Both `send` impls return `Ok(())` unconditionally; the reclaimer invokes them under unwind
  isolation.

Send path semantics:

- `MpscSender::send(item)` returns `Result<(), CaducusError<TabernaRequest<Codec>>>`. The
  `TabernaInboxHandle::enqueue` adapter maps `CaducusErrorKind::Full(_)` to
  `ErrorId::TabernaBusy` and `CaducusErrorKind::Shutdown(_)` to `ErrorId::DomusClosed`.
  The rejected `TabernaRequest` is dropped on the error path; no `oneshot::Receiver` was returned
  to the caller, so the completion guard fallback is unobserved.
- TTL/expiry remains the only source of taberna `accept_timeout`; no per-message timeout
  futures exist anywhere on the inbound path.

Receive path semantics:

- `Receiver::next` is deadline-based: the `Taberna::next(timeout_override)` adapter converts the
  duration to `Some(Instant::now() + timeout_override)` (defaulting to 1 s when no override is
  given). `CaducusErrorKind::Timeout` maps to `ErrorId::ReceiveTimeout`;
  `CaducusErrorKind::Shutdown(_)` maps to `ErrorId::DomusClosed`.
- `Taberna<Codec>` holds the `Receiver` directly. There is no mutex around it. `Receiver::next`
  takes `&self`, the receiver is single-owner, and the public `Taberna::next` is single-consumer
  by contract.

Configuration updates:

- `DomusConfigStore` caches `taberna_accept_queue_size` and `accept_timeout` in atomics whenever
  validated config is created or updated.
- `TabernaInboxHandle::refresh_limits` reads those cached values synchronously and calls
  `MpscSender::update_capacity(n)` and `MpscSender::update_ttl(d)` only when the values differ
  from the inbox's last applied limits. The hot ingress path must not acquire the config lock for
  these two values.
- The `update_ttl` `Result` is discarded; `DomusConfigBuilder` validates the value at construction
  so `InvalidArgument` cannot occur on the live path. Capacity shrinks may evict head items via
  their expiry channel — this matches the documented bound: a capacity change applies to the next
  reclamation tick, not deferred indefinitely.

Shutdown semantics:

- `MpscSender::shutdown()` is **synchronous** and idempotent. `TabernaInbox::shutdown` keeps its
  async-trait signature for registry-driven dispatch; the body invokes the sync call. By the
  time `shutdown_and_report` returns, every queued `TabernaRequest` has been routed to its
  shutdown channel, so all corresponding accept waiters have resolved with `domus-closed`.
- Dropping the last `MpscSender` clone triggers the same shutdown-and-drain. In production the
  registry holds the only `Arc<TabernaInbox>`; registry unregister cascades to sender drop,
  which closes the channel. Combined with the explicit `Domus::shutdown` path this is at-most
  one effective shutdown per channel (caducus de-duplicates).
- Dropping the `Receiver` (i.e. the `Taberna<Codec>` handle) also triggers a shutdown of the
  channel. This matches the public taberna contract: when the application drops its taberna,
  the inbox is closed, queued requests are reported as `domus-closed`, and any subsequent
  inbound enqueue fails with `domus-closed`.

### Calles

- Primary callis: main persistent mTLS connection carrying transport control and application messages.
- Blob callis: optional secondary mTLS connection for isolating large transfers.

Between two domus there may be zero or more calles. If any exist:

- One callis must be the message callis.
- An additional blob callis may exist.

The transport initiates one message callis and one blob callis per peer relationship. The receiver accepts multiple calles of each type so simultaneous bidirectional dial and future parallel callis support remain valid.

Calles are independent connections and must not block each other. Connection lifecycle is per callis.

Multiple calles of the same type may exist between two peers. When multiple primary calles are active, outbound selection is round-robin across the active callis handles.

The first callis is always the primary. Blob callis may only be established once a primary callis session is active.

### A1 Surface vs. Calles

A1 exposes only message and blob semantics to A2/A3. Calles are internal to A1; the transport chooses how to realize message/blob delivery for each address family.

### Transport Backend Boundary

Transport is split into:

- **Transport backend:** bind/accept/dial plus transport authentication. The backend returns an authenticated bidirectional stream and the peer’s Domus address.
- **A1 callis lifecycle:** `hello`/`hello-response`, session resumption, scheduling, and delivery semantics.

A0 transport authentication (TLS or socket auth) completes **before** any A1 `hello` frames are exchanged.

Each Aurelia instance operates with a single transport backend (TCP or socket) derived from the local
Domus address used to create the instance. Backend selection happens once at instantiation and is
immutable for the lifetime of the Domus. A1 rejects any peer address that does not match the local
transport type.

The production backend is represented by a closed enum over the supported backend implementations:
TCP and socket. The production stream type is represented by a closed enum over the authenticated
stream types returned by those backends. Callis reader and writer tasks are generic over the
`TransportBackend::Stream` associated type, so production callis I/O reaches the selected concrete
stream through enum delegation.

The `TransportBackend` trait is the compile-time backend contract for tests and internal backend
substitutions. Concrete test backends provide their own listener and stream associated types
directly through the trait.

### Auth Reload (Smooth Rotation)

`Domus::reload_auth(Pkcs8AuthConfig)` swaps the backend's auth material atomically. It is
non-disruptive:

- Existing TLS / socket-auth sessions continue with the credentials they were established with.
- The next outbound dial uses the new material.
- The next inbound accept uses the new material for its A0 authentication.

There is no per-peer breaker, no forced disconnect, and no callis-quiesce wait. Retained outbound
state is unaffected. A peer presenting a different (validly authenticated)
certificate on a subsequent callis is accepted at the same `peer_addr`.

### Inbound Handshake Admission Control (A0)

Inbound callis are subject to admission control in A0 **before transport authentication** and
**before** A1 `hello` frames are exchanged. Admission control applies to **all callis types**
(primary + blob) and is enforced globally. The authoritative A0 requirements live in
`docs/peering/connection-limits.md`.

Configuration (defaults in parentheses):

- `inbound_handshake_limit_total` (64): maximum number of in-flight inbound handshakes across all
  peers. If the limit is exceeded, the inbound callis is closed immediately without A1 `hello`.

Admission is best-effort and race-tolerant; it is intended to bound resource use, not to provide
strict fairness.

### Per-Peer Handshake Limit (A1)

- `inbound_handshake_limit_per_peer` (3): maximum number of in-flight A1 handshakes for a single
  peer.

### Parallel Callis Limit (Per Peer, A1)

In addition to handshake admission, each peer has a configurable cap on **active** callis enforced
in A1. This limit is distinct from in-flight handshakes.

- `max_parallel_callis_per_peer` (8): maximum number of active callis per peer (primary + blob).

When the limit is reached, new inbound callis for that peer are rejected and outbound dial attempts
are suppressed until capacity is available.

### Primary Callis Lifecycle

1. Open: establish an authenticated transport connection to the peer listener (mTLS for TCP, socket auth for socket).
2. Handshake: originator sends `hello` with header flags. Receiver responds with `hello-response` and header flags per `docs/peering/wire-protocol.md`.
3. Active: transport control and application messages flow. Keepalive is only sent on the primary callis when idle.
4. Close: normal transport close (TLS close or socket drop). Remote treats this as a transient disconnect and follows the reconnect policy.

### Blob Callis Lifecycle

- The blob callis is optional and may be established only when a primary callis session is active.
- Open: establish a second authenticated transport connection to the same listener.
- Handshake: use the standard `hello` exchange with the `BLOB` header flag set to identify the callis type. Negotiation details are defined in `docs/peering/wire-protocol.md`.
- Active: large-transfer traffic only. No keepalive on the blob callis.
- Close: normal transport close (TLS close or socket drop). Primary callis remains unaffected.

If a blob callis connection arrives without an active primary session, the receiver closes it immediately. Attempting to open a blob callis when no primary callis is active is an error (`blob-callis-without-primary`). A blob callis may remain open even if the primary callis is broken, but a new blob callis cannot be opened unless a primary callis is active.

Blob traffic is streamed and may multiplex multiple concurrent blob streams on the blob callis.

### Blob Transfer Stream Adapters (Domus-Aligned)

#### Public Surface Requirements

- A1 does not expose multiplexed blob stream interfaces to A2/A3. Stream IDs and multiplexing are internal only.
- Domus and Taberna public APIs do not require `TabernaStreamSource` or `TabernaStreamSink` in higher layers.
- The only send entry point is typed `send` with a blob flag in `SendOptions`.

#### Adapter Requirements

- **Typed send only:** Domus exposes a single `send` method. The method is typed via a codec and is the only send entry point.
- **Blob flag:** A blob transfer is requested by setting the blob flag in `SendOptions` passed to `send`.
- **No blob length parameter:** Reservations are derived solely from the negotiated chunk size and ack window; there is no blob length field in the public API.
- **Paired blob window config:** The local chunk size and ACK window are configured as one
  `blob_window` pair. Builder and update callers must change the pair together through
  `BlobWindowConfig` / `DomusConfigBuilder::blob_window(chunk_size, ack_window)`.
- **Sender stream handle:** When the blob request is accepted, the sender receives a `BlobSender` stream handle that implements `tokio::io::AsyncWrite + Unpin + Send`.
- **Receiver stream handle:** The receiver obtains an optional `BlobReceiver` inside the same accept call as the message. `BlobReceiver` implements `tokio::io::AsyncRead + Unpin + Send`.
- **Single accept path:** There is no separate `accept_blob` step. The `BlobReceiver` is provided in the message accept call. Rejecting the message rejects the blob and no stream is established.
- **Stream shutdown semantics:** Clean shutdown maps to the final chunk flagged `LAST_CHUNK`. If the last read is shorter than the negotiated chunk size, that partial chunk is the last chunk. No empty chunk is required unless the total transfer length is zero.
- **Internal multiplexing only:** Chunking, ACK window enforcement, and multiplexing remain internal to A1; the sender/receiver handles are simple stream adapters over per-stream bounded windows.
- **Local dispatch parity:** Local blob delivery may short-circuit to an in-memory stream internally, but the adapter semantics, reservations, and visibility to higher layers are identical to remote dispatch.

#### Adapter Integration Requirements (Internal)

- **No additional buffering layer:** `BlobSender` and `BlobReceiver` must read from and write to the existing per-stream windowed chunk store. No extra channels, queues, or buffers are introduced.
- **Outbound adapter:** `BlobSender` is a thin adapter that writes into the per-stream outbound path that already enforces chunk sizing and ACK window limits.
- **Inbound adapter:** `BlobReceiver` is a thin adapter that reads from the per-stream receive window that already enforces ordering and window limits.
- **Capacity enforcement:** The existing per-stream window and reservation logic remain the sole buffer limits; the adapters must not bypass or duplicate them.
- **Error propagation:** Transport errors and peer aborts must surface as stream errors on `BlobSender`/`BlobReceiver`.

#### BlobSender Adapter Semantics (Required)

- **Creation timing:** `BlobSender` is created only after the blob request message is accepted by the remote taberna **and** the blob callis is ready. `Domus::send` returns `SendOutcome::Blob { sender }` only after this point.
- **Stream identity:** `BlobSender` is bound to `stream_id = peer_msg_id` of the accepted blob request, matching the stream identity rules in `docs/peering/blobs.md`.
- **Outbound reservation:** At stream creation, A1 reserves `chunk_size * ack_window_chunks` bytes against `blob_outbound_buffer_bytes`. If the reservation fails, `Domus::send` fails with `blob-buffer-full` and no sender handle is returned.
- **Write semantics:** `BlobSender` implements `tokio::io::AsyncWrite`. `poll_write` accepts bytes into the per-stream outbound window, may return `Pending` when the window is full or no blob callis is available, may hold at most a single partial chunk (`< chunk_size`) as staging, and returns an error if the stream has failed or completed.
- **Flush semantics:** `poll_flush` waits until any staged partial chunk has been accepted into the outbound window and all queued chunks are dispatchable from the outbound ring. It does **not** wait for `blob-transfer-complete`.
- **Shutdown semantics:** `poll_shutdown` finalizes the stream: if a partial chunk exists, emit it with `LAST_CHUNK`; if no bytes were ever written, emit a zero-length chunk with `LAST_CHUNK`; after emitting the last chunk, wait for `blob-transfer-complete` or an error. Success releases the outbound reservation.
- **Drop behavior:** Dropping a `BlobSender` without `shutdown` aborts the stream. A1 must fail the stream locally, release the outbound reservation, and, if possible, send an `error` control message for the stream on the blob callis. Until a dedicated abort error ID exists, the abort is surfaced as `peer-unavailable` to the remote side.
- **Timeouts:** Sender-side waits for ACKs and completion are bounded by `send_timeout`, consistent with the blob outbound ring transmitter behavior.

#### BlobReceiver Adapter Semantics

- **Delivery timing:** `BlobReceiver` is delivered as `TabernaRequest::blob_receiver` alongside the request message. It is **inactive** until the taberna calls `TabernaRequest::accept()`.
- **Activation on accept:** When `accept` succeeds, A1 binds the receiver to the per-stream inbound window and enables stream delivery. If `accept` fails or `reject` is called, the receiver is discarded and no blob stream is established.
- **Inbound reservation:** Inbound reservation (`chunk_size * ack_window_chunks`) is made before presenting the `BlobReceiver`. If reservation fails, the request is rejected with `blob-buffer-full` and no receiver is provided.
- **Read semantics:** `BlobReceiver` implements `tokio::io::AsyncRead`. `poll_read` yields bytes in-order irrespective of chunk boundaries, returns `Pending` until the stream is accepted and data is available, and returns `Ok(0)` only after the `LAST_CHUNK` has been fully delivered.
- **Backpressure:** The receiver’s read cadence governs inbound flow. When the per-stream inbound window is full, A1 applies its existing buffering and idle-timeout rules (`blob-stream-idle-timeout`).
- **Drop behavior:** Dropping a `BlobReceiver` after accept aborts the stream. A1 must fail the stream locally, release the inbound reservation, and, if possible, send an `error` control message for the stream on the blob callis. Until a dedicated abort error ID exists, the abort is surfaced as `peer-unavailable` to the remote side.
- **Error propagation:** Transport errors, protocol violations, and peer aborts surface as read errors on `BlobReceiver`.

### Peer Identity

A1 authenticates peers using certificates for both TCP and socket transports. Certificates are supplied by A2/A3. The semantics of how certificates are issued and what they represent are outside A1.

Domus identity is the transport address itself (IP:port for TCP or absolute socket path for UNIX sockets). There is no other domus identifier.

Transport-specific identity binding, SAN requirements, and A0 authentication flows are defined in:

- `docs/peering/tcp-transport.md`
- `docs/peering/socket-transport.md`

A1 does not pin peer certificates. Each callis is admitted on its own A0 authentication; different
valid certs from the same peer are accepted, allowing smooth rotation.

### Hello Handshake

Header flags:

- `RECONNECT`: sender is attempting to resume an existing peer session.
- `BLOB`: this connection is a blob callis; absence means primary callis.

Rules:

- Transport authentication must complete before any A1 `hello` frames are exchanged.
- The originator sends `hello` with `RECONNECT` set only when resuming a prior session.
- The originator sets `BLOB` when opening a blob callis; primary callis must not set `BLOB`.
- The receiver replies with `hello-response` and echoes `RECONNECT` only if it can resume the session. The response must preserve the callis type (if `BLOB` is set in the request, it remains set in the response).
- If a `hello` arrives without `RECONNECT`, the receiver must reply without `RECONNECT`, and
  retained tracked messages are invalidated.
- If a `hello` arrives with `RECONNECT`, the receiver replies with `RECONNECT` only if it can
  resume; otherwise it replies without `RECONNECT` and retained tracked messages are invalidated.
- A blob callis (`BLOB` set) may be established only when a primary callis session is active. If a blob callis arrives without an active primary callis, the receiver closes it and returns `blob-callis-without-primary`.

Hello payload formats and encoding details are defined in `docs/peering/wire-protocol.md`.

Crash scenarios:

- Originator crash: it restarts in listener mode. Any subsequent outbound messages follow the listener delay rule before initiating a new originator connection.
- Listener crash: it restarts in listener mode. If it receives a `hello` with `RECONNECT` from the originator, it must respond without `RECONNECT` to indicate a fresh session.

### Keepalive

Keepalive is only sent on the primary callis and only when the callis is inactive. Default interval is 15 seconds. Keepalive configuration is set via constructor or builder and supports in-flight updates through `DomusConfigAccess`.

Timeout and retry settings apply uniformly to TCP and socket sessions unless explicitly overridden by a transport-specific configuration surface.

### Frame Size Limits

- A1 rejects frames with `payload_len` greater than the configured maximum.
- The default maximum payload size is 8 MiB and is configurable via `DomusConfig` (`max_payload_len`).

### Send Management and Callis Availability

#### Peer State Mutation Path

Peer state is a concurrency-enabled resource and is mutated only via a single channel that
serializes state updates. This channel is **not** a control channel and carries **no** message
traffic. It exists solely to update peer state (dial results, callis lifecycle events, reconnect
decisions). Peer state provides synchronous snapshots for other tasks to read without mutation.

Primary traffic is handled by per-primary-callis transmitter tasks that read peer state snapshots
and claim work directly from the primary outbound store. Blob traffic is handled by blob callis
transmitter tasks that lease work directly from `BlobManager`. Traffic must never block peer state
mutation.

Primary send management:

- Outbound primary traffic is stored and scheduled by one per-peer primary dispatch manager owned
  by the peer handle and driven by peer-owned tasks.
- The primary outbound store is constructed during peer handle construction and injected into
  `PeerSession`.
- The primary outbound store owns the retained lanes and capacity policy defined in
  `docs/peering/backpressure-queues.md`.
- A1 ACK items carry only the acknowledged `PeerMessageId`, an insertion sequence, and a deadline.
  They do not retain message payload and are dropped after a successful stream write. Their
  10-minute deadline is a cleanup horizon for impaired peers, not sender-completion semantics.
- A1 ERROR control items may carry payload data. ERROR frames carry
  `ErrorPayload { error_id, message }`; the message is UTF-8 and bounded to `1024` bytes, so the
  maximum ERROR payload is `1028` bytes.
- Retained ACK and ERROR insertion is deduplicated by `PeerMessageId` before capacity checks. If
  either response lane already holds a response for that `PeerMessageId`, a second ACK or ERROR
  insertion emits a limited `warn` log and is dropped. Duplicate ACK attempts and duplicate ERROR
  attempts must use distinct warning messages. If the relevant lane is full for a non-duplicate
  response, the overrun is reported and the response is dropped.
- `CLOSE` and `KEEPALIVE` use the per-callis immediate control path.
- A1 retained work is limited to bare ACK and ERROR one-shot responses; completion-bearing A1
  admission is rejected.
- Tracked A2/A3 items are owned directly by their retained lane.
- Each retained slot stores the outbound item, original absolute deadline, priority lane,
  insertion sequence, and the single authoritative slot state (`Empty`, `Ready`, `ReplayReady`,
  `Writing`, or `Inflight`).
- The non-concurrent data module owns preallocated slot storage, free-list management, keyed
  lookup by `PeerMessageId`, lane ordering, deadline indexing, and grow/shrink target-capacity
  rules. It exposes synchronous state-transition methods only.
- The concurrency wrapper owns the mutex, primary-work notify, reclaimer notify, retained-empty
  notify, and shutdown state. It must not hold the store lock across stream writes or other
  awaits.
- Each active primary callis has one transmitter task. The transmitter owns that callis' stream
  write half, claims work directly from the retained store, and writes to the stream.
- Each primary callis transmitter claims work in strict priority order at claim time:
  A1 ACK, A1 ERROR control, A2 tracked, A3 tracked. Within tracked lanes, replay-ready work is
  selected before fresh ready work for the same lane, and FIFO order is based on the retained
  insertion sequence.
- To transmit, a callis transmitter claims one slot under the store lock, changes the slot state
  to `Writing { callis_id }`, clones the slot-owned immutable `Arc` write data, drops the store
  lock, and writes the frame by reference to its own stream write half.
- The callis handoff is an `Arc` reference-count clone. The slot remains the owner of the
  message/control metadata while the transmitter holds a temporary write handle.
- On write success, bare retained A1 ACK/ERROR slots are cleared to `Empty`; tracked slots transition to
  `Inflight { last_sent_callis_id: callis_id }`.
- On write failure, tracked slots return to `Ready` or `ReplayReady` without resetting their
  deadline unless the peer/callis failure resolves them. Bare retained A1 ACK/ERROR slots return
  to `Ready` on transient failure.
- Writer completion is conditional. It is applied only when the slot still has the matching
  claimed item identity and is still `Writing { callis_id }`; stale completions after timeout,
  ACK, close, restart, or slot reuse are ignored.
- On callis teardown, the retained store sweeps `Writing { callis_id }` for that callis so a
  mid-write task exit cannot strand a slot.
- Multiple active primary callis transmitters may write concurrently after claiming distinct
  retained slots. Strict priority is enforced when each transmitter claims work; an already
  claimed frame is not preempted mid-write.
- If retained work exists and no live primary callis exists, the peer state task requests a dial;
  retained messages remain pending in the outbound store until a new callis is available or their
  deadline expires.
- There are **no per-callis outbound queues** for primary traffic. The retained outbound store
  feeds active primary callis transmitters directly.

Immediate per-callis control:

- Each active primary callis has a single immediate close intent slot owned by its transmitter
  task. This is a latch, not a queue, and it targets that specific `CallisId`.
- Peer state sets the close intent for a callis that must send `CLOSE`. The transmitter checks the
  close intent before claiming retained work. If present, it writes the close frame directly to its
  own stream, closes the TCP or socket connection, reports the close outcome to peer state, and
  exits its loop. There is no retained close item, no inflight close state, and no later wakeup for
  that transmitter after close intent is handled.
- Setting close intent must wake the targeted callis transmitter immediately through that
  transmitter's close-control wake source. The implementation must not rely on keepalive ticks,
  retained-work notifications, or unrelated peer-state changes to observe a close intent.
- `KEEPALIVE` is generated locally by the transmitter from its keepalive tick. On a keepalive tick,
  the transmitter first rechecks close intent and retained work; it writes keepalive directly only
  when the callis is idle and no retained item is immediately claimable.
- Close and keepalive writes are never replayed. Write failure marks the callis failed/closed and
  lets the peer state path decide whether retained tracked work should replay on another callis or
  reconnect.
- There is no mid-frame preemption. If the transmitter is already writing a retained frame, close
  is the next action after that write completes or fails.

Sender completion-interest drop leaves accepted transport work in the primary outbound store.
Transport owns the item until ACK, timeout, close, peer restart, or permanent failure. Later result
delivery is best-effort if the oneshot receiver has gone away.

Blob send management:

- Per-stream outbound buffering and ACK window semantics are defined in `docs/peering/blobs.md`.
- Each active blob callis owns one peer-owned transmitter task. The transmitter owns that callis
  write half and leases outbound work directly from `BlobManager`.
- Blob work availability is woken by the `BlobManager` notify when any active stream ring or
  finish write can be written.
- Stream fairness is global across blob streams. `BlobManager` scans active stream rings from one
  round-robin cursor shared by all blob callis transmitters. The cursor advances after each
  successful stream lease and is used for both replay-ready and fresh-ready chunk scans.
- Blob callis selection uses live handles only; closed handles are pruned immediately.
- If no live blob callis remain and there are active streams, blob sends pause and peer state
  requests a blob dial; frames are not routed to closed callis.
- Blob traffic uses callis transmitter tasks that lease work directly from `BlobManager`; the
  callis write half is not mediated by per-callis outbound queue or channel storage.

### Core Task Discipline

This subsection defines the core-task requirements for the transport implementation. These
rules are mandatory for every transport core task and are validated by unit and integration tests.

#### Scope

The following are core tasks under `docs/concurrency.md`:

- **Per-callis receive loop** — `transport/callis.rs`. Owns inbound frame parsing, waiter
  promotion, deadline-driven housekeeping for blob-callis-readiness waiters.
- **Primary callis transmitter task** — owns one primary callis write half, claims retained slots
  directly from the primary outbound store, and reports write completion by item identity and
  callis identity.
- **Peer state core task** — `transport/peer.rs`. Owns `PeerState`, dial/connect/disconnect
  transitions, impaired-window timing, and snapshot publication.
- **Primary outbound reclaimer** — owns deadline-driven expiry for primary outbound slots using
  weak ownership of the primary outbound store.
- **Blob callis transmitter task** — owns one blob callis write half, leases outbound blob work
  directly from `BlobManager`, and reports write completion by item identity and callis identity.
- **Listener accept core task** — `transport/listener.rs`. Owns the inbound accept loop.
- **Observability core task** — `observability/actor.rs::run_observability`. Owns metrics state
  and the broadcast channels. Already compliant via single-mpsc-consumer pattern.

#### Mandatory rules for core tasks

All core tasks must:

1. **Follow Pattern 1 (armed-before-recheck) from `docs/concurrency.md`** for every
   `Notify`-based wakeup. `notified()` futures must be constructed before the state read
   that decides whether to wait. This rule is mandatory; conditional `tokio::select!` arm
   guards (`if has_X`) are not permitted as a substitute.
2. **Notify outside the lock** (Pattern 2). Producers that mutate shared state and notify
   consumers must drop the lock before notifying.
3. **Use the right primitive for the job** (Pattern 4). State that has a "current value"
   semantics (e.g. "is there a callis in the pool?", "what is the latest peer snapshot?")
   must be exposed via `tokio::sync::watch` rather than `Notify::notify_waiters`. The
   `watch::Receiver::changed()` consumer pattern eliminates the missed-notify race
   structurally.
4. **Run a single epilogue at the bottom of the loop body** (Pattern 5) for invariants
   that must hold after every iteration. Per-arm side effects that involve "publish
   snapshot", "update derived deadline", or similar invariants are not permitted.
5. **Stay decomposed** (Pattern 6). Peer-state branch logic must live in per-event helpers
   or a synchronous state-machine helper. A core task body that crosses ~150 lines or
   contains nested matches deeper than two levels must be split until each branch can be
   unit-tested directly.

#### Peer state effect dispatch

The peer state core task has two steps per event:

1. Apply the event to `PeerState` and produce `PeerStateEffects`.
2. Execute the effects in a deterministic order.

Effect order is:

1. Local state mutation.
2. Queue/session/blob mutations needed to make the state visible to waiters.
3. Snapshot publication.
4. Notify transmitter waiters.
5. Spawn dial/close helper tasks.
6. Observability reporting.

Snapshot publication must happen before spawning follow-up work that assumes the snapshot
is visible. For example, after `Connected(Primary)`, the primary handle must be present in
the snapshot before any queued replay work can depend on dispatch progress.

#### Receive loop wakeup (replaces existing "event-driven wakeups" rule)

The per-callis receive loop must wake on **any** of:

- a new inbound frame,
- completion of an in-flight delivery (ACK/ERROR readiness),
- a `BlobManager` callis-pool generation change (via `watch::Receiver::changed()`, not
  `Notify`),
- timeout of an in-flight delivery,
- shutdown signal.

All wakeup sources must be armed before the loop's drain step. The drain step processes
every actionable waiter in one pass and returns a snapshot of outcomes the caller forwards.
A drain step that finds no work is a no-op, not an error.

Tests:

- Unit tests for `drain_accept_waiters` and `drain_blob_callis_waiters` must prove that
  ready message, ready blob-accept, blob-callis-ready, timeout, and cancellation branches
  each emit exactly one outcome and leave no stale waiter behind.
- Integration tests must include concurrent primary and blob traffic so receive-loop
  waiter progress is exercised while other frames continue to arrive.

#### Receiver ACKs blob requests deterministically

The receiver-side blob-request ack path must observe a callis-pool transition without
race. With `BlobManager::callis_notify` migrated to `watch<u64>`, the receive loop's
`watch::Receiver::changed()` arm latches on the pool generation; the consumer always sees
the latest generation regardless of whether it was registered when the producer's bump
fired. The 30-second `send_timeout` then becomes a true outer ceiling for failure rather
than a normal-path latency.

Tests:

- `transport_blob_transfers_in_parallel` must run under an explicit suite timeout and must
  not rely on the default 30-second `send_timeout` as the normal progress mechanism.
- BlobManager unit tests must cover callis generation changes that happen before and after
  waiter registration.

#### Primary transmitter wakeup

Each primary callis transmitter is a core task. When it cannot send immediately, it must wait on
exactly these progress sources:

- retained-store primary-work notify,
- peer-state snapshot changes,
- callis shutdown/close signals,
- keepalive tick.

The keepalive tick remains a periodic-progress source, but must not be load-bearing for
correctness on a busy peer. Each transmitter owns priority selection after every wake and must
re-check A1 ACK, A1 ERROR control, A2 tracked, then A3 tracked before deciding to park again.
The retained-store reclaimer owns deadline expiry. The retained store owns strict priority and
slot claiming; the callis transmitter owns only its stream write half and write-result reporting.

Normal retained-work wakeups use `Notify::notify_one` for ordinary work availability and propagate
additional `notify_one` calls only when more dispatchable work remains after a claim or write
completion. `notify_waiters` is reserved for shutdown, peer close, config/capacity changes,
retained-empty test waits, and component drop. Each transmitter follows the arm-before-recheck
pattern: arm the primary-work waiter, attempt `claim_next(callis_id)`, then await only if no item
was claimed.

Tests:

- `transport_primary_progress_independent_of_keepalive_tick` must assert serial sends after
  warmup complete without clustering around the keepalive interval.
- `transport_primary_burst_single_callis` must assert multiple concurrent sends through
  one primary complete within the configured send budget and all ACK waiters resolve.
- `transport_primary_parallel_4_senders_to_one_receiver_a3` must exercise A3 priority
  messages and bounded shutdown.

#### Peer state epilogue

The peer state core task must maintain derived state in one epilogue-equivalent path per
event. The epilogue must:

- update `impaired_since` from the post-event state,
- publish a snapshot if the visible primary handle pool changed,
- wake primary transmitters if primary availability changed or queued replay work was added,
- not publish snapshots from individual match arms.

Tests:

- Peer-state unit tests must assert the snapshot after each `PeerStateUpdate` branch,
  especially `Connected(Primary)`, `DialFailed(Primary)`, `ConnectionClosed(Primary)`,
  and `GracefulClose`.
- The full-mesh integration test must prove snapshot publication and transmitter wakeups
  are sufficient under simultaneous multi-peer dial.

#### Graceful close and shutdown tests

Shutdown correctness is tested independently from delivery:

- `transport_primary_parallel_shutdown_completes_promptly` must establish a primary, send
  an A3 message, shut down both peers, and assert shutdown finishes under an explicit
  timeout without waiting for `2 * send_timeout`.
- Peer-state and retained-store tests must prove `GracefulClose` spawns one shutdown task,
  computes one absolute deadline, latches shutdown, stops the reclaimer, fails retained A2/A3,
  drains retained A1 ACK/ERROR responses, then sets close intent for active primary calles.
- A transmitter test must prove close intent wakes the targeted transmitter, writes `CLOSE`,
  closes the TCP or socket connection, and exits the loop without a later wakeup.
- A shutdown timeout test must prove remaining retained A1 ACK/ERROR responses are dropped when
  the absolute close deadline expires.
- A transport shutdown test must prove peer callis drain waits run concurrently across the peer set
  and are bounded by one peer wait window.
- A callis-tracker test must continue to validate the arm-before-recheck wait pattern, but
  passing that test is not sufficient to prove shutdown correctness.

#### Primary outbound reclaimer

The primary outbound store is passive state. It must expose the state operations needed
for active expiry, but it must not spawn its own reclaimer or depend on an ambient runtime.
The peer handle spawns the primary outbound reclaimer through the Aurelia runtime handle. The
reclaimer actively expires messages and bare control items at their original deadlines,
independent of whether the sender is still awaiting the result, and follows the same discipline as
caducus:

- derive the earliest deadline from retained store state;
- arm the wakeup before rechecking the earliest deadline;
- remove due items under the store lock;
- report `send-timeout` for tracked messages outside the lock;
- drop expired bare A1 items after overrun/expiry reporting;
- ignore stale deadline-index entries that no longer match the retained slot and item identity;
- wake retained-empty and transmitter waiters after state mutation;
- hold weak ownership of the concurrency wrapper and exit when the wrapper is dropped.

Tests:

- Unit tests must prove earliest-deadline expiry, non-head earlier deadline expiry, ACK-before-
  expiry, stale deadline entries, empty-store insert wakeups, earlier-deadline insert wakeups,
  reclaimer shutdown on wrapper drop, lane capacity growth/shrink, and retained-empty wakeups.
- Integration tests must prove a retained message can expire from ready, sending, inflight, and
  replay-ready states without relying on the caller's `wait_for_ack` future as the only timeout
  path.

### Isolation Requirements

- Peer isolation: one peer must not block another.
- Callis isolation: blob callis issues must not stall primary callis traffic and vice versa.

### Graceful Shutdown

When a peer shutdown is negotiated (using the existing shutdown protocol):

1. Keep the domus listener running; peer close is scoped to the peer handle and its callis.
2. Spawn one peer shutdown task and compute one absolute close deadline immediately as
   `Instant::now() + config.send_timeout`.
3. Set the retained-store shutdown flag. From this point, new A1/A2/A3 outbound work is rejected,
   retained A2/A3 tracked messages are failed with `peer-unavailable`, and the primary reclaimer
   stops because shutdown owns cleanup.
4. Wake all active primary transmitters. While the shutdown flag is set and close intent is not set
   for that callis, a transmitter claims only retained A1 ACK/ERROR response items. It must not
   claim A2/A3 work.
5. The shutdown task waits until retained A1 ACK/ERROR response lanes are empty or the absolute
   close deadline expires.
6. If retained A1 responses become empty before the deadline, the shutdown task sets close intent
   for each active primary callis and wakes each targeted transmitter. Each transmitter writes its
   close frame, closes its TCP or socket connection, and exits its loop.
7. If the deadline expires first, the shutdown task drops remaining retained A1 ACK/ERROR
   responses and proceeds to teardown without waiting for further primary transmitter progress.
8. At teardown, stop reconnect attempts, drain primary handles, signal remaining callis shutdown,
   and rely on existing BlobManager teardown to fail active blob streams and release reservations.

The close path must be event-driven. `Transport::shutdown` sends graceful close to every peer, then
waits for peer callis counts concurrently across the peer set as verification. Correct shutdown is
achieved by close sequencing and callis shutdown signals, not by waiting for the full verification
timeout.

`Transport::shutdown` / domus shutdown is the separate transport-wide boundary that stops the
listener accept loop.

### Delivery Semantics

A send completes successfully only when the sender receives an ACK confirming that the destination taberna on the remote peer accepted the message into its ingress boundary. This does not imply application-level processing.

A message is considered delivered when all of the following are true:

- The message was transmitted to the remote peer.
- The remote peer validated the message.
- The remote destination taberna exists.
- The remote destination taberna accepted enqueue into its ingress boundary.
- The remote peer sent an ACK for the peer message ID.

Blob transfer delivery semantics and completion rules are defined in `docs/peering/blobs.md`.

### Failure Semantics

A send fails when delivery cannot be achieved. Typical failure causes include:

- Unknown destination taberna.
- Local outbound queue full.
- Peer unavailable or connection failure beyond recovery.
- Remote taberna rejected enqueue.
- Protocol failure.
- Send timeout.
- Peer crash resulting in invalidation of retained tracked messages.
- Peer signaled `close`, rejecting further delivery on the callis.

Externally, a unified send error is acceptable. Internally, specific causes must be preserved for observability and retry logic.

Error IDs and their taxonomy are defined in `docs/ids.md`. Error payload formats are defined in `docs/peering/wire-protocol.md`.

### Reconnect vs. New Connection

- Transient network failure: reconnect to the same peer session; retained tracked messages remain
  valid and may be replayed.
- Peer crash or restart: the new connection is treated as a fresh session; retained tracked
  messages on the surviving peer are invalidated and must fail locally.
- Close control message (`close`): treated as an intentional shutdown by the sender; the receiver must stop delivery on that callis, fail inflight, and must not reconnect on that session.
- Unexpected connection close (EOF/IO error/local disconnect): treated as a transient disconnect; the receiver must not emit `error` for in-flight deliveries and must allow replay after reconnect.
- Reconnect disagreement: if a reconnect handshake is attempted and the peer does not echo
   `RECONNECT`, the originator treats the new connection as a fresh session on the **same** peer
   handle. Retained A2/A3 tracked messages are failed locally with `peer-restarted`, retained
   blob streams are failed locally, and retained A1 ACK/ERROR response work is handled by the retained
   primary outbound store on the new callis.

Replay after transient reconnect uses the tracked message's original deadline. A replayed message
must not receive a new `send_timeout` budget when it is marked replay-ready in the retained store.

### Reconnect Backoff

Once a peer is in originator mode, reconnect attempts use the following schedule after an immediate reconnect attempt:

- 1 second
- 2 seconds
- 4 seconds
- then every 4 seconds

Reconnect configuration is set via constructor or builder and supports in-flight updates through `DomusConfigAccess`.

### Originator and Listener Roles

- A peer starts in listener mode.
- When it needs to initiate outbound communication, it becomes the originator for the primary callis and dials the remote.
- After a crash/restart, the previous originator returns to listener mode.
- If an inbound primary is accepted while an outbound primary dial is in flight, the inbound
  primary may satisfy the peer's pending outbound work immediately. The outbound dial result is
  then handled as a duplicate success or stale failure according to the simultaneous-dial rules.
- Listener/originator role is a dial-scheduling preference, not permission to ignore a valid
  inbound primary. A listener-side inbound primary must publish a usable snapshot before any
  queued local outbound work waits for another dial.

### Listener Delay and Reconnection Timeout

- Listener delay: 5 seconds at startup, during which the peer only listens and does not initiate outbound connections.
- Listener reconnection timeout: 20 seconds. This only applies after a connection was previously established. If the connection breaks and the listener has queued outbound messages, it waits this duration before switching to originator mode and dialing the peer.
- Reconnect window: when **all** callis to a peer are down, the peer enters the impaired state and
  attempts reconnects as normal. If no reconnect succeeds within `send_timeout`, the peer handle is
  torn down and all retained tracked messages are failed locally.

### Transport-Specific Authentication

Transport authentication and any A0 validation occurs below A1 and must complete before any `hello` frames are exchanged.
Refer to the dedicated transport documents for details:

- `docs/peering/tcp-transport.md`
- `docs/peering/socket-transport.md`

### Blob Callis Semantics

Blob traffic always uses a separate blob callis. The blob callis is a distinct connection from the primary callis,
identified by the `BLOB` hello flag and subject to the blob callis lifecycle described above.

### Transport Scope

The peering crate is transport-only. It does not provide:

- Application semantics.
- Request/response coordination.
- Business logic guarantees.
- Discovery or membership policy.

A2/A3 layers are responsible for those behaviors.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
