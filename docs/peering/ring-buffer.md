# Ring Buffer

Status: Developed

## Objectives

- Provide a single asynchronous ring buffer primitive for blob transfers with no intermediate channels.
- Support outbound chunking, ACK tracking, window enforcement, and completion signaling.
- Support inbound out-of-order buffering, gap detection, and ordered delivery to taberna streams.
- Provide a reusable, testable internal API for blob transport code paths.

## Technical Details

### Outbound Lifecycle State Machine

The outbound ring buffer uses an explicit lifecycle state machine. The state machine is separate
from chunk assembly so invalid lifecycle combinations are not representable in the outbound state.

Lifecycle states:

- `Open`: sender bytes may still be accepted and chunks may be drained.
- `Sealing`: sender input has ended; the ring may still need to materialize the final chunk before
  completion can be awaited.
- `AwaitingComplete`: the final chunk has been materialized and the ring is waiting for the remote
  `blob-transfer-complete` frame.
- `Completed`: the remote `blob-transfer-complete` frame has been observed.
- `Failed(AureliaError)`: the stream failed; all waiters return the stored error.
- `Closed`: the local owner closed the ring; new sender/drain work is rejected or ends.

Chunk assembly state is separate from lifecycle state. `write_buf`, `pending_full`, and the
"any chunk has been created" marker belong to the chunk domain, not to lifecycle.

The implementation is split into internal modules used by peering's blob transport:

- `ring_buffer/mod.rs`: crate-internal re-exports and shared helpers.
- `ring_buffer/outbound.rs`: `OutboundRingBuffer` public methods and wait helpers.
- `ring_buffer/outbound/state.rs`: outbound lifecycle enum and state composition.
- `ring_buffer/outbound/chunks.rs`: chunk assembly, ready/inflight slots, ACK map, capacity.
- `ring_buffer/inbound.rs`: inbound ring buffer behavior.

### Concurrency Patterns

Ring buffer wait helpers (`wait_for_sendable`, `wait_for_inflight_drain`,
`wait_for_complete`, `wait_for_capacity`, and `wait_for_space`) follow the armed-before-recheck
shape from `docs/concurrency.md` Pattern 1: each helper constructs `notify.notified()` and pins it
BEFORE the predicate check, drops the lock, then awaits the pinned waiter (with a deadline where
applicable). Producers fire `notify.notify_waiters()` after dropping their lock; consumers
therefore observe the post-mutation state on every iteration. New wait helpers added to this
module must follow the same shape.

The wait helpers intentionally remain explicit by predicate so each caller's terminal
condition is readable at the call site.

### Module Location and Exposure

- Implementation: `src/crates/peering/src/ring_buffer/`
- Unit tests: `src/crates/peering/src/tests/ring_buffer.rs`

### OutboundRingBuffer

Outbound ring buffers own both data and accounting for a single blob transfer stream.

API surface:

- `new(chunk_size, window_size) -> Result<Self, AureliaError>`: validates non-zero sizes.
- `push_bytes(data, send_timeout) -> Result<usize, AureliaError>`: accepts bytes from the sender stream, chunks to `chunk_size`, and respects `window_size`.
- `seal(send_timeout) -> Result<(), AureliaError>`: finalizes the stream and marks the last chunk.
- `wait_for_sendable() -> Result<bool, AureliaError>`: waits until a ring-owned chunk can be leased or the ring is closed/complete.
- `lease_next_chunk_for_write(callis_id, peer_msg_id) -> Option<ChunkWriteLease>`: leases the next dispatchable ring slot, transitions it to `Writing`, and returns temporary read access to the slot bytes.
- `mark_chunk_inflight(lease)`: transitions a leased chunk from `Writing` to `InFlight`.
- `mark_callis_replay_ready(callis_id)`: transitions matching `Writing` and `InFlight` chunks back to `ReplayReady` after callis loss.
- `live_chunk_count()` / `inflight_chunk_count()`: returns ring-owned chunk counts from slot state.
- `has_dispatchable_replay()` / `has_dispatchable_fresh()`: exposes ring-owned dispatchability checks.
- `note_ack(peer_msg_id)`: advances the ring on ACK.
- `note_error(peer_msg_id, err)`: marks failure and wakes waiters.
- `wait_for_inflight_drain(deadline)` / `wait_for_complete(deadline)` / `mark_complete()`: lifecycle helpers.
- `fail(err)` / `close()`: terminal error/closure signaling.

Internal design:

- `OutboundLifecycle` owns sender/terminal progression (`Open`, `Sealing`, `AwaitingComplete`,
  `Completed`, `Failed`, `Closed`).
- `OutboundChunks` owns `write_buf`, `pending_full`, chunk IDs, fixed slots, ACK maps, in-flight
  chunk counts, and capacity checks. Dispatchability is derived from ring slot state.
- Each fixed outbound slot is either `Empty`, `Ready`, `Writing`, `InFlight`, or `ReplayReady`.
  ACK, ERROR, stream failure, and local close make occupied slots `Empty`; the slot structure stays
  available for reuse inside the ring.
- `pending_full: Option<Bytes>` holds the most recent full chunk until there is subsequent data or
  `seal()` is called. This ensures the final chunk on exact chunk boundaries is correctly marked
  `LAST_CHUNK` without emitting empty chunks.
- `window_size` applies to every non-empty slot plus `pending_full`.

Concurrency model:

- Mutations guarded by `tokio::sync::Mutex`.
- `tokio::sync::Notify` wakes waiters when chunks are added, ACKed, or terminal states change.
- No `mpsc` channels or parallel buffer structures are used for outbound chunk storage.

### InboundRingBuffer

Inbound ring buffers store received chunks and deliver them in order to the receiver stream.

API surface:

- `new(chunk_size, window_size) -> Result<Self, AureliaError>`: validates non-zero sizes.
- `insert_chunk(chunk_id, data, is_last) -> Result<InboundInsertOutcome, InboundInsertError>`:
  - Validates `chunk_size`.
  - Dedupe on repeated `chunk_id`.
  - Rejects `chunk_id` outside the window (`WindowExceeded`).
  - Rejects missing required chunk when the buffer is full and later chunks are already present (`MissingChunk`).
- `take_next() -> Option<Bytes>`: drains the next contiguous chunk.
- `wait_for_space(deadline) -> bool`: waits until `received.len() < window_size`.
- `is_complete()`: completion inspection helper.

Internal design:

- `received: HashMap<u64, Bytes>` stores buffered chunks keyed by `chunk_id`.
- `next_expected` and `next_deliver` track ordering.
- `last_chunk_id` tracks completion once `LAST_CHUNK` arrives.

### Integration with Blob Transfers

- `BlobSenderStream` pushes bytes into `OutboundRingBuffer`.
- Blob transmitters lease ring slots through `BlobManager`, write `blob-transfer-chunk` frames
  from temporary read access to the slot bytes, and report write completion by lease identity.
- ACKs from the blob callis call `note_ack`, releasing capacity for more data.
- `BlobReceiverStream` drains `InboundRingBuffer` via `take_next` to deliver data in-order to the taberna stream.
- Receiver ACK handling and missing-chunk failures are driven by `InboundRingBuffer::insert_chunk`.

Blob-specific transmitter integration is specified in `docs/peering/blobs.md`. Leasing a chunk
for write transitions ring-owned state to writing and grants temporary read access to the chunk
bytes. The bytes stay in the ring until ACK, ERROR, stream failure, or local close. Replay-ready
and in-flight chunks continue to count against the per-stream ring window.

### Error and Timeout Handling

- Invalid configuration returns `ProtocolViolation`.
- `SendTimeout` and `PeerUnavailable` surface through outbound waiters.
- Missing-chunk and window-exceeded conditions map to blob transfer errors in `transport/blob/receive.rs`.

### Testing Coverage

- Unit tests validate chunk boundaries, last-chunk marking on exact and empty boundaries,
  lifecycle transitions, capacity backpressure, failure propagation, close wakeup, and inbound
  ordering/error cases.
- Unit tests validate end-to-end outbound-to-inbound round trips through the crate-internal API.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
