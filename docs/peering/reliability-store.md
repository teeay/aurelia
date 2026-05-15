# Reliability Store and ACK Tracking

Status: Developed

## Objectives

- Define retention and replay of unacknowledged messages.
- Define ACK correlation and dedupe behavior.
- Define invalidation behavior after peer restart.
- Define active expiry using the original sender deadline.

## Technical Details

### Peer Message IDs

Peer message IDs are transport-private identifiers used for ACK correlation, replay, and dedupe.
Their canonical definition and type live in `docs/ids.md`.

Allocation policy:

- a monotonically incrementing `u32` counter per peer session;
- wraps on overflow with normal `u32` rollover;
- no reserved values or special-case ranges;
- ID space is `2^32` entries, far exceeding the default retained lane capacities;
- the peer message ID space is shared across all calles and connections between the two peers.

Blob transfer frames are a special case:

- the blob transfer stream ID is the `peer_msg_id` of the original blob request message, carried
  as `request_msg_id` in blob frames;
- `blob-transfer-chunk` frames are acknowledged with the standard `ack` control message; each
  frame has its own wire header `peer_msg_id`;
- receiver-side dedupe for blob chunks is keyed by `(request_msg_id, chunk_id)`;
- duplicates must still be ACKed;
- the original blob request is an application message on the primary callis with the `BLOB`
  header flag set; its ACK is gated on blob callis readiness and pending stream registration;
- on reconnect, retained unacknowledged chunk frames are replayed starting from the lowest missing
  `chunk_id` per stream;
- receiver-side stream idle timeout is `2 * send_timeout`;
- late chunks for unknown or completed streams are rejected with `error`
  (`blob-stream-not-found`);
- if the sender exceeds the negotiated ACK window for a stream, the receiver responds with
  `error` (`blob-ack-window-exceeded`) and fails that stream.

### Retained Outbound Ownership

When A1 accepts a completion-bearing message into outbound dispatch, A1 owns it until one of the
following occurs:

- ACK received.
- ERROR received.
- Original deadline reached.
- Permanent transport failure declared.
- Peer restart or reconnect disagreement invalidates retained state.
- Peer close or domus shutdown fails the message locally.

There is no separate ACK timeout. `send_timeout` is the single end-to-end bound for sender-side
completion and includes retention, dispatch, reconnect, replay, and ACK wait.

Dropping the sender-side completion waiter leaves an A1-accepted message in the retained store. The
sender may stop waiting for the result, but the retained store continues to resolve the message
through ACK, ERROR, expiry, close, peer restart, or permanent failure. Completion delivery to the
caller is best-effort: if the oneshot receiver has been dropped when the store resolves the
message, the store ignores the failed result send and still releases all transport state.

### Retained Slot and Tracked Outbound Wrapper

The retained slot shape, slot state enum, lane capacities, and state-transition table are
authoritatively defined in `docs/peering/backpressure-queues.md`. Reliability owns the
completion, deadline, ACK/ERROR correlation, and replay semantics for tracked items stored in
those slots.

`original_deadline` is captured exactly once when A1 accepts the message:

```rust
let original_deadline = Instant::now() + config.send_timeout;
```

The same absolute deadline is preserved across all state transitions:

- initial insertion into a retained lane;
- transition from ready or replay-ready to writing by a primary callis transmitter;
- transition from writing to inflight after the stream write succeeds;
- retry after a temporary callis write failure;
- replay after transient reconnect;
- final ACK, ERROR, timeout, close, restart, or permanent failure resolution.

If a message cannot be inserted because `original_deadline <= Instant::now()`, the message fails
locally with `send-timeout`.

If an A3 message cannot be inserted because the A3 retained lane is full, A1 rejects admission
immediately with `local-queue-full`. The message is not retained for replay and does not consume
timeout budget inside the reliability store.

### Retained Store Shape

The primary reliability store is the primary outbound store defined in
`docs/peering/backpressure-queues.md`. It is split into:

- a non-concurrent data module that owns retained slots, lane ordering, keyed lookup, deadline
  indexing, and state transitions;
- a concurrency wrapper that owns async mutex/notify coordination and active task entry points.

The data module stores tracked items directly in bounded lane slots. It also stores compact bare
A1 ACK and A1 ERROR items for response control frames that do not have sender completion state.
`CLOSE` and `KEEPALIVE` are not reliability-retained items; they are immediate per-callis control
handled by the transport model.

Required data indexes:

- `PeerMessageId -> slot` keyed lookup for tracked items;
- `PeerMessageId -> slot` pending response lookup for retained ACK and ERROR items, used only to
  suppress duplicate response insertion;
- lane FIFO queues for fresh ready tracked and bare items;
- lane replay queues for tracked items marked `ReplayReady`;
- earliest-deadline index for all retained items;
- free-list per bounded lane;
- slot identity plus item identity for stale deadline-index and stale write-completion
  protection.

The lane slot is the owner of the retained work.

Primary callis transmitters claim retained slots directly. A claim changes a slot from `Ready` or
`ReplayReady` to `Writing { callis_id }` and returns `Arc`-backed immutable frame/message data for
the stream write. That `Arc` clone is a temporary read handle for the transmitter.

The retained store supports multiple concurrent primary callis transmitters through an atomic
claim API guarded by the store mutex. A slot can be claimed by at most one transmitter because the
claim and `Writing` transition occur in the same data-module mutation.

Write completion is checked against the retained slot's current item identity and callis identity.
For tracked items, item identity includes the `PeerMessageId` and lane-local `seq`. A write result
is applied only if the slot still contains the claimed item and is still `Writing` for the same
`callis_id`. Stale write completions after timeout, ACK, close, restart, fresh-session cleanup,
or slot reuse are ignored.

### Capacity and Backpressure

Capacity is lane-specific and counts retained work across every state. Lane capacities and their
rationales are defined in `docs/peering/backpressure-queues.md`.

There is no additional primary inflight capacity bucket. A tracked message continues to consume
its retained lane slot while it is `Ready`, `ReplayReady`, `Writing`, or `Inflight`.

Runtime capacity updates apply target-capacity semantics:

- growth makes additional slots available immediately;
- shrink does not evict live retained items;
- while live count exceeds the new target, new admission to that lane is rejected;
- capacity changes wake transmitter, reclaimer, and retained-empty waiters when their predicates
  may have changed.

Blob chunk ACK windowing is separate from primary message retention and is documented in
`docs/peering/blobs.md`.

### Active Expiry

The retained store has one peer-owned reclaimer task. The peer runtime/controller defined in
`docs/peering/transport-model.md` spawns the reclaimer through the Aurelia runtime handle. The
passive store constructor must not spawn it through ambient `tokio::spawn`.
On peer shutdown, the retained-store shutdown flag wakes the reclaimer and it exits; the peer
shutdown task fails retained tracked work and drains or drops retained A1 ACK/ERROR responses.

The reclaimer enforces the stored deadline for retained work in every live state:

- `Ready`;
- `ReplayReady`;
- `Writing`;
- `Inflight`.

Required reclaimer loop shape:

```rust
loop {
    let waiter = notify.notified();
    tokio::pin!(waiter);

    let Some(store) = weak.upgrade() else {
        break;
    };
    let next = store.earliest_deadline().await;

    match next {
        None => {
            drop(store);
            waiter.await;
        }
        Some(deadline) if deadline > Instant::now() => {
            drop(store);
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {}
                _ = &mut waiter => continue,
            }
        }
        Some(_) => {
            let expired = store.remove_expired(Instant::now()).await;
            drop(store);
            report_expired(expired).await;
        }
    }
}
```

The exact function names may differ, but the ordering must remain `arm waiter -> recheck deadline
state -> await`. This is required for both the empty-store insert race and the earlier-deadline
insert race.

Expiry reporting rules:

- expired tracked messages are completed with `Err(AureliaError::new(ErrorId::SendTimeout))`;
- expired bare A1 ACK/ERROR items are dropped after any required reporting; their 10-minute
  deadline is a cleanup horizon, not sender completion semantics;
- completion sends happen outside the store lock;
- stale deadline-index entries are ignored;
- retained capacity is released before waiters are woken.

### ACK and ERROR Correlation

Inbound ACK and ERROR frames address tracked messages by `PeerMessageId`.

ACK handling:

1. Remove the tracked item by key.
2. Release the retained lane slot.
3. Send `Ok(())` to the completion sender outside the lock.
4. Ignore the result if the completion receiver was dropped.

ERROR handling:

1. Remove the tracked item by key.
2. Release the retained lane slot.
3. Convert the inbound error payload to `AureliaError`.
4. Send `Err(error)` to the completion sender outside the lock.
5. Ignore the result if the completion receiver was dropped.

If the item was already expired, failed, or invalidated, the ACK/ERROR is stale and is ignored.

Outbound ACK and ERROR insertion is deduplicated before capacity checks. If a response for the
same `PeerMessageId` is already retained in either response lane, the second insertion request is
dropped after a limited warning. Duplicate ACK attempts and duplicate ERROR attempts must use
distinct warning messages so operational logs identify which terminal response path violated the
single-response invariant. This is not an error path because one retained response already
represents the required reply to the remote message.

### Replay After Transient Reconnect

If all primary callis are lost but the peer has not restarted:

- retained tracked items in `Inflight` are marked `ReplayReady`;
- retained tracked items in `Writing` for a dying callis are swept by callis teardown and marked
  `ReplayReady` if their original deadline has not elapsed;
- replay-ready items remain in their original retained lane slot;
- replay preserves `original_deadline`;
- replay does not create a new queue entry detached from the retained item;
- replay-ready items are selected before fresh ready items within the same tracked lane;
- lane priority remains A1, A2, then A3.

The next active primary callis transmitter retransmits from the retained store after a primary
callis becomes available.

Reconnect attempts are bounded by `send_timeout` while the peer is impaired. If all callis are
down and no reconnect succeeds within `send_timeout`, the peer handle is torn down and retained
unacknowledged messages are failed locally.

### Invalidation After Peer Restart

If the remote peer restarts and a fresh connection is established:

- retained A2/A3 tracked messages on the surviving peer are invalidated;
- those messages fail locally with `peer-restarted`;
- active blob sender/receiver streams associated with those messages are failed locally;
- inbound primary-message dedupe history is cleared before accepting messages from the fresh
  session.

### Invalidation After Reconnect Disagreement

If a reconnect handshake is attempted and the peer does not echo `RECONNECT`, the originator
treats the new connection as a fresh session on the same peer handle. Retained A2/A3 tracked
messages are failed locally with `peer-restarted`, retained blob streams are failed locally, and
retained A1 ACK/ERROR response state and immediate per-callis control are handled according to the
close/restart rules in `docs/peering/transport-model.md`.

### Receiver Dedupe

Receiver-side dedupe protects against duplicate delivery during transient reconnect/replay. Dedupe
keys include authenticated peer identity and peer message ID.

The retained dedupe history is bounded to twice the retained completion-bearing replay pressure,
with a minimum history of 128 peer message IDs:

```rust
max(128, 2 * total_completion_bearing_retained_capacity)
```

For the current primary lane model, `total_completion_bearing_retained_capacity` is the sum of
A2 tracked capacity and A3 tracked capacity. Bare A1 ACK and bare A1 ERROR control lanes are
excluded because they do not represent inbound application/service delivery that can be replayed to
a taberna. Completion-bearing A1 work is not valid.

The scaled bound tracks normal outbound replay pressure, while the floor keeps duplicate
suppression effective for tiny test or deployment configurations whose retained capacities are
intentionally very small.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
