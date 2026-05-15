# Backpressure and Queue Bounds

Status: Developed

## Objectives

- Define bounded retained outbound lanes for reliable primary transport.
- Define taberna ingress backpressure behavior.
- Define callis scheduling and priority rules.

## Technical Details

### Configuration Surface

All backpressure and timeout settings are configured through a constructor or builder and must
support in-flight updates via atomic config swap. A lightweight config access handle is exposed so
higher-level crates can read and update these settings without binding to the internal store.

```rust
pub struct DomusConfigAccess {
    // internal store is opaque
}

impl DomusConfigAccess {
    pub async fn snapshot(&self) -> DomusConfig;
    pub async fn update(&self, next: DomusConfig) -> Result<DomusConfig, AureliaError>;
}
```

Send timeout is the single end-to-end bound for sender completion and must include any delay from
retention, reconnects, replay, and ACK wait. Configuration validation failures return
`AureliaError` with `ErrorId::InvalidConfig`.

Admission and connection limits are also part of backpressure policy:

- `inbound_handshake_limit_total`: maximum in-flight inbound handshakes across all peers.
- `inbound_handshake_limit_per_peer`: per-peer in-flight handshake limit while under the
  high-water mark.
- `max_parallel_callis_per_peer`: cap on active callis per peer (primary + blob).

### Primary Outbound Store

Each peer owns one primary outbound store. The store is the capacity boundary for primary
outbound work. It owns accepted work directly from admission until the work is resolved by ACK,
ERROR, timeout, close, restart, retained ACK/ERROR transmission, or permanent failure.

The retained lanes store the full work item, or a lane-specific compact wrapper for bare A1
ACK/ERROR response control that does not require completion state. Each retained slot is the
authoritative owner of its item and lifecycle state.

The primary outbound store is constructed by the peer runtime/controller defined in
`docs/peering/transport-model.md`. The component exposes passive mutation APIs and separate active
task runners. The peer runtime spawns the transmitter and reclaimer with the Aurelia runtime
handle.

### Retained Lanes and Capacities

Primary outbound work is partitioned into four retained lanes:

| Lane | Contents | Capacity |
| --- | --- | --- |
| A1 ACK | ACK-only control items that carry only the acknowledged `PeerMessageId`. | `send_queue_size * 16` |
| A1 ERROR control | ERROR response frames without sender completion state. | `send_queue_size * 2` |
| A2 tracked | Completion-bearing Aurelia service messages. | `128` |
| A3 tracked | Completion-bearing application messages. | `send_queue_size` |

`send_queue_size` is local configuration and is not negotiated between peers. It is a count-based
capacity proxy, not a byte budget. Runtime config refreshes update the A1 ACK, A1 ERROR control,
and A3 target capacities from the latest local `send_queue_size`; A2 remains fixed at `128`.

The default `send_queue_size` is `128`, which gives these default primary lane capacities:

- A1 ACK: `2048`.
- A1 ERROR control: `256`.
- A2 tracked: `128`.
- A3 tracked: `128`.

A tracked item continues to count against its retained lane while it is ready, being sent,
inflight, or replay-ready.

Blob stream outbound chunk pressure is separate from the primary retained lanes. It is controlled
by per-stream outbound rings, `blob_window.ack_window`, and the blob buffer caps documented in
`docs/peering/blobs.md`. Bulk blob chunks must not be admitted into the primary outbound store or
into a second blob payload store.

### Retained Item Shapes

Tracked A2/A3 messages are stored directly in retained slots. The slot metadata is the single
lifecycle authority for ready, replay-ready, writing, inflight, and empty states:

```rust
struct RetainedSlot {
    item: Option<OutboundItem>,
    original_deadline: Instant,
    lane: Lane,
    seq: u64,
    state: SlotState,
}

enum SlotState {
    Empty,
    Ready,
    ReplayReady { last_sent_callis_id: CallisId },
    Writing { callis_id: CallisId },
    Inflight { last_sent_callis_id: CallisId },
}

enum OutboundItem {
    Ack(AckItem),
    Error(A1ErrorItem),
    Tracked(TrackedOutbound),
}

struct TrackedOutbound {
    msg: PeerMessage,
    ack_tx: oneshot::Sender<Result<(), AureliaError>>,
}
```

`TrackedOutbound` is valid for A2/A3 outbound work. A1 retained work contains only bare ACK and
ERROR response items. Completion-bearing A1 admission is a protocol violation.

Bare retained A1 lane items use smaller wrappers:

```rust
struct AckItem {
    peer_msg_id: PeerMessageId,
    deadline: Instant,
    seq: u64,
}

struct A1ErrorItem {
    peer_msg_id: PeerMessageId,
    error: ErrorPayload,
    deadline: Instant,
    seq: u64,
}
```

Bare A1 ACK and retained A1 ERROR items are stamped at insertion with
`Instant::now() + A1_RESPONSE_CLEANUP_TTL`, where `A1_RESPONSE_CLEANUP_TTL` is 10 minutes. This is
not a sender-completion timeout and does not mean ACKs or transport errors conceptually expire. It
is only a cleanup horizon for severely impaired peers so stale bare response control does not
remain retained forever.

ACK items are intentionally tiny: a message ID, deadline, and ordering metadata. The larger
`send_queue_size * 16` ACK lane capacity is acceptable because it does not retain application
payloads. Dropping ACKs causes the remote sender to time out, so ACK capacity is deliberately
generous. Retained A1 ERROR control remains bounded to `send_queue_size * 2`; ERROR frames may carry
`ErrorPayload`, whose message string is bounded to `1024` UTF-8 bytes and whose maximum payload is
`1028` bytes including the `error_id`. A2 uses a fixed `128`-item service lane so library service
traffic is isolated from application A3 capacity without being scaled by application queue size.

`CLOSE` and `KEEPALIVE` are not retained items. They bypass the retained store entirely and are
handled by the per-callis immediate control path in `docs/peering/transport-model.md`.

### Data Module

The retained store has a non-concurrent data module. It owns deterministic state transitions and is
unit-testable without Tokio.

Required data responsibilities:

- preallocated lane slot storage;
- a free-list for each bounded lane;
- target-capacity and live-count tracking for each lane;
- grow and shrink logic for capacities derived from `send_queue_size`;
- FIFO ordering for slots in `Ready` state in each lane;
- replay ordering for retained tracked slots in `ReplayReady` state after transient callis loss;
- keyed lookup from `PeerMessageId` to tracked slot for ACK, ERROR, close, restart, and
  fail-one operations;
- pending response lookup from `PeerMessageId` to retained ACK or ERROR slot so duplicate
  response insertion is a silent drop;
- deadline index ordered by each item's original deadline;
- stale deadline-index protection using the slot identity and item identity;
- state transitions between `Empty`, `Ready`, `ReplayReady`, `Writing`, and `Inflight`;
- strict-priority selection across A1 ACK, A1 ERROR control, A2 tracked, and A3 tracked lanes.

Capacity shrink follows caducus-style target-capacity semantics. Shrinking a lane does not evict
live retained items. New admission to that lane is rejected until the live count falls below the
new target. Growth makes additional slots available and wakes primary-work waiters if work was
blocked only by capacity.

### Concurrency Wrapper

The retained store has a thin concurrency wrapper around the data module. The wrapper owns async
coordination primitives but delegates all state mutation to the data module.

Required wrapper responsibilities:

- one async mutex around the data module;
- one primary-work notify for newly dispatchable work and replay readiness;
- one reclaimer notify for earlier deadlines, inserted first work, shutdown, and capacity changes
  that affect expiry state;
- one retained-empty notify for shutdown and test waits;
- a shutdown flag visible to all mutation and task paths;
- an A1-response-empty predicate and wake path for the peer shutdown task;
- an overrun reporter that emits limited logs and observability events for every lane-full path;
- no lock held across stream writes, observability awaits, limited-log awaits, or completion
  `oneshot` sends.

The wrapper is a multi-producer/multi-consumer retained store. Producers include public send
admission, ACK insertion, retained ERROR insertion, replay marking, close/restart cleanup, and
config refresh. Consumers are the active primary callis transmitter tasks. All producer, consumer,
keyed removal, expiry, and capacity-change paths serialize only for the short data-module mutation
under the mutex; stream writes and async effects happen after the lock is released.

The concrete signatures may differ, but the ownership boundary must not: the callis transmitter's
claim operation is the only primary work consumer API, and it must atomically select by strict
priority and transition the selected slot to `Writing { callis_id }`. Write completion identifies
the claimed item by its retained slot and item identity. For tracked, ACK, and ERROR items that
identity includes the `PeerMessageId` and lane-local `seq`. Completion is applied only when the
slot still contains the same item identity and is still
`Writing` for the same `callis_id`; stale completions after timeout, ACK, close, restart, or slot
reuse are ignored without a separate generation field or writer-record type.

ACK and ERROR insertion first check the pending response lookup. If a retained response for the
same `PeerMessageId` already exists in the ACK or ERROR lane, the new response request is dropped
and a limited warning is logged. The warning message must distinguish duplicate ACK attempts from
duplicate ERROR attempts because either one indicates an internal invariant breach: A1 should
attempt only one terminal response for a peer message ID. Duplicate response attempts are not
reported as queue overruns because one retained response is already enough to resolve the remote
message. The pending response lookup is cleared when that retained ACK or ERROR is written,
expired, dropped during teardown, or removed by any other retained-store cleanup path.

The claimed write data is immutable and `Arc` backed. Claiming clones the `Arc` into the callis
transmitter's local write handle, drops the store lock, and writes using that handle. The retained
slot remains the authoritative owner of lifecycle metadata, while the transmitter holds only an
`Arc` read handle for the stream write.

Normal work wakeups use `Notify::notify_one`, not `notify_waiters`. When an insertion, replay
mark, write completion, or expiry creates or leaves dispatchable work, the wrapper wakes one
primary callis transmitter. After a transmitter successfully claims a slot, if more dispatchable
work remains, it wakes one additional transmitter before starting its stream write. This creates a
handoff chain for bursts without waking every active transmitter for each item.

`notify_waiters` is reserved for broad state changes where every waiter must recheck: shutdown,
peer close, config/capacity changes, retained-empty test waits, and component drop. All primary
callis transmitter waits still use the arm-before-recheck pattern:

```rust
loop {
    let notified = outbound.primary_work_notified();
    tokio::pin!(notified);

    if let Some(claimed) = outbound.claim_next(callis_id).await {
        return claimed;
    }

    notified.await;
}
```

This permits multiple consumers without duplicate claims or lost wakeups. Fairness between active
callis transmitters is not guaranteed and is not required; item ordering is controlled by the
retained store's strict-priority claim logic.

Mutation methods return small effect batches. Async effects are executed after releasing the store
lock. Effects include completion sends, overrun reports, close requests, dial requests, and task
wakeups.

### Shutdown Drain Mode

The peer shutdown task is the only owner of primary outbound shutdown cleanup. When it sets
the retained-store shutdown flag:

- new A1 ACK/ERROR, A2, and A3 insertion is rejected or dropped according to the shutdown reason;
- retained A2/A3 tracked items are failed immediately with the shutdown error and completed
  outside the store lock;
- retained A1 ACK/ERROR responses remain live only so transmitters can drain them before the close
  deadline;
- primary transmitters may claim only A1 ACK/ERROR responses while shutdown is set;
- the primary reclaimer stops; deadline expiry is not responsible for retained-store cleanup for
  that peer;
- every transmitter waiter, reclaimer waiter, retained-empty waiter, and A1-response-empty waiter
  is woken after the flag is set.

The A1-response-empty predicate is true only when no retained ACK or ERROR response is in `Ready`
or `Writing`. It is the shutdown task's drain condition. The shutdown deadline is the absolute
deadline computed by the peer shutdown task as `Instant::now() + config.send_timeout` at shutdown
trigger time. If that deadline expires before the predicate becomes true, the shutdown task drops
remaining retained ACK/ERROR responses and continues to peer teardown.

### Slot State Transitions

The retained slot state machine is authoritative. No other structure may carry a second copy of
the same lifecycle state.

| Event | Empty | Ready | ReplayReady | Writing | Inflight |
| --- | --- | --- | --- | --- | --- |
| Insert accepted item | Move to `Ready`. | Reject or no-op; the slot is live. | Reject or no-op; the slot is live. | Reject or no-op; the slot is live. | Reject or no-op; the slot is live. |
| Claim by primary callis | No-op. | Move to `Writing { callis_id }` if the item is eligible for that callis. | Move to `Writing { callis_id }` if the item is eligible for that callis. | No-op. | No-op. |
| Write succeeds | No-op. | No-op. | No-op. | Bare retained A1 ACK/ERROR clears to `Empty`; tracked items move to `Inflight { last_sent_callis_id: callis_id }`. | No-op. |
| Temporary write failure | No-op. | No-op. | No-op. | Tracked items return to `Ready` or `ReplayReady` without resetting the deadline; bare retained A1 ACK/ERROR returns to `Ready` if the peer failure is transient. | No-op. |
| Callis teardown while writing | No-op. | No-op. | No-op. | If `callis_id` matches the dying callis, tracked items return to `ReplayReady`; bare retained A1 ACK/ERROR returns to `Ready` until its cleanup deadline. | No-op. |
| ACK for tracked item | No-op. | Matching tracked item clears to `Empty` and completes `Ok(())`. | Matching tracked item clears to `Empty` and completes `Ok(())`. | Matching tracked item clears to `Empty` and completes `Ok(())`; any later writer completion is stale. | Matching tracked item clears to `Empty` and completes `Ok(())`. |
| ERROR for tracked item | No-op. | Matching tracked item clears to `Empty` and completes with the mapped error. | Matching tracked item clears to `Empty` and completes with the mapped error. | Matching tracked item clears to `Empty` and completes with the mapped error; any later writer completion is stale. | Matching tracked item clears to `Empty` and completes with the mapped error. |
| Deadline reached | No-op. | Clear to `Empty`; tracked items complete with `send-timeout`, bare retained A1 ACK/ERROR is dropped. | Clear to `Empty`; tracked items complete with `send-timeout`, bare retained A1 ACK/ERROR is dropped. | Clear to `Empty`; tracked items complete with `send-timeout`, bare retained A1 ACK/ERROR is dropped; any later writer completion is stale. | Clear to `Empty`; tracked items complete with `send-timeout`. |
| Transient primary loss | No-op. | No change. | No change. | Handled by callis teardown while writing. | Move tracked items to `ReplayReady { last_sent_callis_id }`. |
| Fresh session, close, or permanent failure | No-op. | Resolve or drop according to the close/restart rule. | Resolve or drop according to the close/restart rule. | Resolve or drop according to the close/restart rule; any later writer completion is stale. | Resolve or drop according to the close/restart rule. |

Capacity shrink does not change slot state. It changes only the target capacity used for future
admission decisions.

### Admission and Overrun Semantics

Admission is non-blocking. Aurelia either accepts the item into the retained store immediately or
returns/fires a local failure.

Lane-full behavior:

- A3 tracked: reject public send with `local-queue-full`; do not retain the message.
- A2 tracked: fail the completion-bearing item with `local-queue-full`.
- A1 duplicate ACK attempt: emit a limited `warn` log and drop the duplicate if any response for
  the same `PeerMessageId` is already retained in the ACK or ERROR lane.
- A1 duplicate ERROR attempt: emit a limited `warn` log and drop the duplicate if any response for
  the same `PeerMessageId` is already retained in the ACK or ERROR lane.
- A1 ACK full: report the overrun and drop the ACK.
- A1 ERROR control full: report the overrun and drop the ERROR.

The primary dispatch manager is the authoritative owner of outbound overrun reporting.
Every lane-full path must report through the same overrun reporter before returning or dropping:

- public send admission;
- ACK lane insertion;
- A1 ERROR control insertion;
- retry after temporary callis/send unavailability;
- reconnect replay marking.

Overrun reporting uses the peer runtime's current peer identity, `DomusConfigAccess`, limited
logging registry, and `ObservabilityHandle`.

### Per-Callis Primary Transmitters

Each active primary callis has exactly one peer-owned transmitter task spawned by the peer runtime
after the primary handshake succeeds. The transmitter owns that callis' stream write half, claims
work directly from the retained store, and writes it to the stream.

Required behavior:

- strict priority order at claim time is A1 ACK, A1 ERROR control, A2 tracked, then A3 tracked;
- FIFO order is preserved within each lane for fresh ready work;
- replay-ready tracked work is selected before fresh ready work in the same tracked lane;
- the transmitter claims one slot under the store lock, changes slot state to
  `Writing { callis_id }`, clones the slot-owned immutable `Arc` write data, and releases the
  store lock;
- the transmitter releases the store lock before awaiting the stream write;
- the stream write is bounded by the retained item's `original_deadline`;
- a successful bare retained A1 ACK/ERROR transmission clears the slot to `Empty`;
- a successful tracked transmission changes the slot to
  `Inflight { last_sent_callis_id: callis_id }`;
- a temporary write failure returns tracked slots to `Ready` or `ReplayReady` while
  preserving their original deadline;
- permanent close/restart failures resolve tracked items and release retained capacity;
- write completion is applied only when the slot still contains the claimed item identity and is
  still `Writing` for that transmitter's `callis_id`;
- stale write completions after timeout, ACK, close, or restart are ignored.

Multiple primary callis transmitters may run concurrently. They serialize only while claiming a
slot from the retained store; after a claim, each transmitter writes to its own stream without
holding the store lock. Strict priority is enforced when a transmitter claims work. Already claimed
frames are not preempted mid-write.

Normal work availability uses the `notify_one` handoff protocol defined in the concurrency wrapper
section. A transmitter that wakes and finds no work rearms the waiter before rechecking.

When retained work exists and no primary callis is active, the peer state task requests
`EnsurePrimaryDial`. Retained messages remain pending until a callis transmitter can claim them or
their deadline expires.

On callis teardown, the retained store sweeps `Writing { callis_id }` slots for the dying callis
before the callis handle is forgotten. This prevents a mid-write callis failure from leaving work
stuck in `Writing`. Tracked items become `ReplayReady` if the peer failure is transient and the
deadline has not elapsed. Bare retained A1 ACK/ERROR returns to `Ready` until its cleanup
deadline.

### Reclaimer Task

The reclaimer is a peer-owned active task spawned by the peer runtime. It enforces the original
deadline for retained work in every live state: ready, replay-ready, writing, and inflight.
When the retained-store shutdown flag is set, the reclaimer wakes and exits; shutdown cleanup is
performed by the peer shutdown task.

Required behavior:

- derive the earliest deadline from the retained store's deadline index;
- arm the notify waiter before rechecking the earliest deadline, following
  `docs/concurrency.md` Pattern 1;
- sleep until the earliest deadline or wake early when a new earlier deadline is inserted;
- remove all due items under the store lock;
- ignore stale deadline-index entries that do not match the current slot identity and item
  identity;
- report `send-timeout` for expired tracked items outside the lock;
- drop expired bare A1 items after reporting where appropriate;
- wake retained-empty and primary-work waiters after removals;
- hold weak ownership of the retained store so dropping the peer runtime lets the task exit.

Timeout remains an end-to-end property of the accepted item's original deadline. Reconnect replay,
temporary writer failure, and state transitions must never reset it.

### ACK, ERROR, Close, and Replay Paths

Inbound ACK and ERROR frames remove tracked items by `PeerMessageId` through the retained store's
keyed lookup. ACK sends `Ok(())` to the completion sender. ERROR sends the mapped
`AureliaError`. If the completion receiver was dropped, the failed send is ignored after releasing
all transport state.

Transient primary loss marks eligible tracked inflight items
`ReplayReady { last_sent_callis_id }` and wakes the transmitter. Replay preserves the original
deadline and the retained lane capacity slot.

Fresh-session restart invalidates retained A2/A3 tracked messages and clears inbound dedupe as
defined in `docs/peering/reliability-store.md` and `docs/peering/transport-model.md`.

### Bounded Taberna Ingress

Remote tabernae may refuse enqueue when full. The receiver must attempt immediate enqueue into the
destination taberna inbox. If the inbox is full, the message fails immediately with
`taberna-busy` as defined in `docs/ids.md`. If enqueue succeeds, the receiver waits for taberna
acceptance with a timeout; on timeout, the message fails with `taberna-busy`. In these cases:

- Do not ACK.
- Return a transport failure (`taberna-busy`).
- Do not introduce additional inbound buffering beyond the taberna inbox.

Defaults:

- Accept timeout: 5 seconds.
- Taberna accept queue size: 2.

### Callis Scheduling

Primary callis scheduling uses strict priority tiers with FIFO ordering within each tier.
All outbound traffic is represented as primary outbound work whose `MessageType` falls in the
A1, A2, or A3 ranges defined in `docs/ids.md`.

- A1: transport control and transport-critical traffic.
- A2: Aurelia service messages.
- A3: application messages.

Socket transport mirrors TCP: blob traffic uses a separate blob callis. Priority rules apply per
callis, and blob traffic must not starve transport control or normal application messages on the
primary callis.

### Handshake Admission and Callis Caps

Inbound callis are gated in A0 before A1 `hello` by the pre-authentication admission limits
described in `docs/peering/transport-model.md` and `docs/peering/connection-limits.md`. This
bounds concurrent handshake work across all peers.

Active callis are capped per peer via `max_parallel_callis_per_peer`. This is enforced
independently of handshake admission to keep long-lived connection counts within bounds.

### Blob Streaming Backpressure

Blob traffic is streamed and chunked. A1 must:

- apply per-stream backpressure so a single stream cannot monopolize the callis;
- enforce a maximum chunk size to keep control/message frames schedulable;
- allow multiple blob streams to interleave on the same callis;
- preserve priority for control frames over blob chunks.

Blob chunk sizing, ACK window, buffering, and ordering semantics are defined in
`docs/peering/blobs.md`.

### Total Blob Buffer Cap (Per Domus)

A1 enforces a total blob buffer capacity across all peers, with separate caps for outbound and
inbound flows. Defaults and reservation rules are defined in `docs/peering/blobs.md`.

### Testing Coverage

Unit tests must cover:

- lane admission limits for A1 ACK, A1 ERROR control, A2 tracked, and A3 tracked;
- runtime grow and shrink behavior for `send_queue_size`-derived capacities;
- ACK lane overrun and drop behavior;
- duplicate ACK and duplicate ERROR response insertion limited-warning behavior;
- A1 ERROR control overrun behavior, including ERROR payloads;
- A3 full rejection with `local-queue-full`;
- strict lane priority and FIFO order within each lane;
- replay-ready priority over fresh ready work inside a tracked lane;
- keyed ACK and ERROR removal;
- timeout expiry from `Ready`, `ReplayReady`, `Writing`, and `Inflight`;
- callis teardown recovery from `Writing { callis_id }`;
- stale write completion ignored by item identity and callis identity;
- stale deadline-index protection;
- empty-store reclaimer wakeup;
- earlier-deadline reclaimer wakeup;
- weak-owned reclaimer shutdown on store drop;
- reclaimer exit when the retained-store shutdown flag is set;
- shutdown-mode rejection of new A1/A2/A3 insertion;
- shutdown-mode failure of retained A2/A3 tracked items;
- A1-response-empty notification after retained ACK/ERROR drain;
- no lock held across stream writes or completion reporting.

Integration tests must prove:

- A3 saturation rejects immediately without hidden sender-side buffering;
- outbound queue overruns are observable for public send, ACK insertion, A1 ERROR control,
  retry, and reconnect-replay paths;
- ACKs release retained lane capacity;
- transient reconnect replays retained tracked messages directly from the retained store;
- config reload increases and shrinks `send_queue_size`-derived lane capacities.

Tests must use observable completion or explicit bounded timeouts; they must not rely on unbounded
sleeps.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
