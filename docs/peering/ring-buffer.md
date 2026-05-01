# Ring Buffer

Status: Developed

## Objectives

- Provide a single asynchronous ring buffer primitive for blob transfers with no intermediate channels.
- Support outbound chunking, ACK tracking, window enforcement, and completion signaling.
- Support inbound out-of-order buffering, gap detection, and ordered delivery to taberna streams.
- Expose a reusable, testable API for blob transport code paths.

## Technical Details

### Module Location and Exposure

- Implementation: `src/crates/peering/src/ring_buffer.rs`
- Public module export: `aurelia_peering::ring_buffer`
- Unit tests: `src/crates/peering/src/tests/ring_buffer.rs`
- Integration tests: `src/crates/peering/tests/ring_buffer.rs`

### OutboundRingBuffer

Outbound ring buffers own both data and accounting for a single blob transfer stream.

API surface:

- `new(chunk_size, window_size) -> Result<Self, AureliaError>`: validates non-zero sizes.
- `push_bytes(data, send_timeout) -> Result<usize, AureliaError>`: accepts bytes from the sender stream, chunks to `chunk_size`, and respects `window_size`.
- `seal(send_timeout) -> Result<(), AureliaError>`: finalizes the stream and marks the last chunk.
- `wait_for_sendable() -> Result<bool, AureliaError>`: waits until a chunk can be drained or the ring is closed/complete.
- `take_next_chunk(peer_msg_id) -> Option<OutboundChunk>`: reserves the next chunk for send and tracks it as in-flight.
- `note_ack(peer_msg_id)`: advances the ring on ACK.
- `note_error(peer_msg_id, err)`: marks failure and wakes waiters.
- `register_control(peer_msg_id)` / `wait_for_control(peer_msg_id, deadline)`: tracks control frames such as `blob-transfer-start` and `blob-transfer-complete`.
- `wait_for_inflight_drain(deadline)` / `wait_for_complete(deadline)` / `mark_complete()`: lifecycle helpers.
- `fail(err)` / `close()`: terminal error/closure signaling.

Internal design:

- `write_buf: BytesMut` buffers incoming bytes until chunk boundaries.
- `pending_full: Option<Bytes>` holds the most recent full chunk until there is subsequent data or `seal()` is called. This ensures the final chunk on exact chunk boundaries is correctly marked `LAST_CHUNK` without emitting empty chunks.
- `slots: HashMap<u64, OutboundSlot>` stores buffered chunks by `chunk_id` in either `Ready` or `InFlight` state.
- `ready_queue: VecDeque<u64>` tracks send order.
- `ack_map: HashMap<PeerMessageId, u64>` maps ACKs back to chunk IDs.
- `inflight` counts active in-flight chunks.
- `control: HashMap<PeerMessageId, ControlStatus>` tracks start/complete control ACKs.
- `window_size` applies to total buffered chunks including `pending_full`.

Concurrency model:

- Mutations guarded by `tokio::sync::Mutex`.
- `tokio::sync::Notify` wakes waiters when chunks are added, ACKed, or terminal states change.
- No `mpsc` channels or parallel buffer structures are used; the ring buffer is the channel.

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
- `len()`, `is_complete()`, `next_expected()`: inspection helpers.

Internal design:

- `received: HashMap<u64, Bytes>` stores buffered chunks keyed by `chunk_id`.
- `next_expected` and `next_deliver` track ordering.
- `last_chunk_id` tracks completion once `LAST_CHUNK` arrives.

### Integration with Blob Transfers

- `BlobSenderStream` pushes bytes into `OutboundRingBuffer` and relies on `wait_for_sendable` + `take_next_chunk` to drain into `blob-transfer-chunk` frames.
- ACKs from the blob callis call `note_ack`, releasing capacity for more data.
- `BlobReceiverStream` drains `InboundRingBuffer` via `take_next` to deliver data in-order to the taberna stream.
- Receiver ACK handling and missing-chunk failures are driven by `InboundRingBuffer::insert_chunk`.

### Error and Timeout Handling

- Invalid configuration returns `ProtocolViolation`.
- `SendTimeout` and `PeerUnavailable` surface through outbound waiters.
- Missing-chunk and window-exceeded conditions map to blob transfer errors in `transport/blob/receive.rs`.

### Testing Coverage

- Unit tests validate chunk boundaries, last-chunk marking on exact boundaries, capacity backpressure, and inbound ordering/error cases.
- Integration tests validate end-to-end outbound-to-inbound round trips using the public API.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
