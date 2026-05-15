# Blob Transfers

Status: Developed

## Objectives

- Define the A1.5 stateful blob overlay built on A1 message transport.
- Keep outbound chunk bytes in bounded per-stream rings until ACK, ERROR, stream failure, or local
  close.
- Allow many blob streams and many blob callis transmitter tasks to make progress through one
  shared stream-leasing policy.
- Preserve blob sender and receiver APIs while enforcing negotiated chunk and ACK-window limits.

## Technical Details

### Transfer Lifecycle

Blob transfers use the primary callis for request admission and blob callis connections for bulk
stream data.

1. A sender opens a blob request by sending the application message on the primary callis with the
   wire `BLOB` flag set. The request `peer_msg_id` is the blob stream ID.
2. The receiver validates the destination taberna and reserves inbound blob capacity.
3. The receiver ensures a blob callis is available. The blob callis hello negotiates
   `chunk_size` and `ack_window_chunks`.
4. Once the blob stream is accepted and a compatible blob callis exists, the receiver activates
   inbound stream state and ACKs the primary blob request.
5. The sender creates one outbound ring for the stream with the negotiated limits and exposes a
   `BlobSenderStream` over that ring.
6. Blob callis transmitters lease chunks from BlobManager and write `blob-transfer-chunk` frames
   directly from ring-owned bytes.
7. The receiver ACKs each chunk frame only after the chunk is accepted into the bounded A1 receiver
   delivery path. Chunk admission never waits for consumer-side ring space on the blob callis
   reader path.
8. After the final chunk is accepted into the bounded A1 receiver delivery path, the receiver sends
   `blob-transfer-complete` for the stream.
9. The sender completes once its outbound ring observes the complete frame and all chunk slots have
   reached terminal state.

Blob completion is an A1 delivery-boundary signal only. It never means A3 processed, interpreted,
or accepted the blob contents. Application-level blob outcome semantics must be expressed by A3
messages.

The blob callis transmits chunk data, stream completion, and close frames. Blob stream setup,
admission, and generic request errors stay on the primary/control path and do not create blob
callis write state.

### Outbound Stream State

Each outbound blob stream owns exactly one `OutboundRingBuffer`.

```rust
enum OutboundBlobStreamState {
    Open,
    Sealing,
    AwaitingComplete,
    Completed,
    Failed(AureliaError),
    Closed,
}
```

- `Open` accepts sender bytes while ring capacity exists.
- `Sealing` materializes the final chunk.
- `AwaitingComplete` means the final chunk has been emitted and the sender is waiting for all chunk
  slots to drain and for the receiver's A1 delivery-boundary `blob-transfer-complete`.
- Terminal states wake sender waiters and release reservations.
- Stream state does not contain a `callis_id`.

### Ring-Owned Chunk Slots

The outbound ring is the only bulk payload owner. Slots are fixed by `ack_window_chunks`; occupied
slots count against the stream window until terminal completion.

```rust
enum OutboundChunkSlot {
    Empty,
    Ready {
        chunk_id: u64,
        data: Bytes,
        is_last: bool,
        seq: u64,
    },
    Writing {
        chunk_id: u64,
        data: Bytes,
        peer_msg_id: PeerMessageId,
        callis_id: CallisId,
        is_last: bool,
        seq: u64,
    },
    InFlight {
        chunk_id: u64,
        data: Bytes,
        peer_msg_id: PeerMessageId,
        callis_id: CallisId,
        is_last: bool,
        seq: u64,
    },
    ReplayReady {
        chunk_id: u64,
        data: Bytes,
        previous_callis_id: CallisId,
        is_last: bool,
        seq: u64,
    },
}
```

Transitions:

- `Empty -> Ready`: the ring materializes a full or final chunk.
- `Ready -> Writing`: a transmitter receives a write lease.
- `ReplayReady -> Writing`: a transmitter receives a retry write lease.
- `Writing -> InFlight`: BlobManager installs the `peer_msg_id` in-flight lookup and marks the
  ring slot in-flight before returning the lease to the transmitter.
- `Writing|InFlight -> ReplayReady`: the stamped blob callis is lost.
- `InFlight -> Empty`: the peer ACKs the chunk.
- Any occupied state -> `Empty`: ERROR, stream failure, or local close.

`Writing` is an internal transition state for lease creation. A leased chunk is visible as
`InFlight` before network I/O starts so a fast ACK can resolve the ring slot immediately after the
frame reaches the peer. A socket write failure tears down the callis and rolls every `Writing` or
`InFlight` slot stamped with that `callis_id` back to `ReplayReady`.

`seq` increments every time a slot enters `Writing`. Replay applies only when `chunk_id`,
`peer_msg_id`, `callis_id`, and `seq` match the write lease. This prevents stale completion from an
earlier write attempt from changing a reused or retried slot.

The ring owns these accounting APIs:

- `live_chunk_count()`;
- `inflight_chunk_count()`;
- `has_dispatchable_replay()`;
- `has_dispatchable_fresh()`;
- `has_window_capacity()`.

BlobManager asks rings for these facts and does not keep duplicate chunk counters, payload copies,
or replay trackers.

### Write Leases

`ChunkWriteLease` is temporary read access to one ring-owned slot for one write attempt.

```rust
struct ChunkWriteLease {
    chunk_id: u64,
    data: Bytes,
    is_last: bool,
    peer_msg_id: PeerMessageId,
    callis_id: CallisId,
    slot_seq: u64,
}
```

The lease does not move ownership out of the ring. `Bytes` provides a read handle to slot storage
while the slot remains in `Writing` or later. The transmitter reports the attempted write result
with the lease identity:

```rust
lease_next_chunk_for_write(callis_id, peer_msg_id) -> Option<ChunkWriteLease>
mark_chunk_inflight(&lease)
```

### Blob Write Type

Blob callis transmitter work is represented by one enum:

```rust
enum BlobWriteLease {
    Ack {
        peer_msg_id: PeerMessageId,
    },
    Error {
        peer_msg_id: PeerMessageId,
        payload: Bytes,
    },
    Chunk {
        stream_id: PeerMessageId,
        peer_msg_id: PeerMessageId,
        chunk: ChunkWriteLease,
    },
    Finish {
        stream_id: PeerMessageId,
        peer_msg_id: PeerMessageId,
        payload: Bytes,
    },
}
```

`Chunk` is the bulk path and carries only ring lease identity plus stream/message IDs.
`Ack` acknowledges inbound chunk and completion frames. `Error` reports failed inbound chunk
processing with `MSG_ERROR`. `Finish` writes `blob-transfer-complete` after final A1 receiver
delivery. Blob close is written directly by the transmitter shutdown path and has no stream write
state.

Blob response control mirrors primary response control. ACK, ERROR, and COMPLETE response writes
use bounded reserved-capacity lanes derived from `send_queue_size`: ACK capacity is
`send_queue_size * 16`, ERROR capacity is `send_queue_size * 2`, and COMPLETE capacity is
`send_queue_size * 2`. Pending response writes are deduplicated by response `peer_msg_id`. ACK and
ERROR response writes are keyed by response `peer_msg_id` and are not stream-associated. Chunk and
COMPLETE writes carry the blob stream ID.

### BlobManager Ownership

BlobManager owns coordination metadata, not bulk bytes:

- active blob callis pool and negotiated settings;
- stream ID to outbound ring;
- stream ID to negotiated stream settings;
- `peer_msg_id` lookup for in-flight chunk writes;
- outbound ACK, ERROR, and COMPLETE response write slots;
- inbound pending and active receiver streams;
- inbound/outbound reservation records;
- blob work notification and callis-pool generation notification.

BlobManager stores only shared stream-leasing metadata for outbound chunks. `callis_id` is recorded
on a write lease and on `Writing`/`InFlight` ring slots so callis loss can roll back the attempts
made on that callis.

### Global MPMC Leasing

Each active blob callis has one transmitter task. Every transmitter calls:

```rust
lease_next_blob_write(callis_id) -> Option<BlobWriteLease>
finish_blob_write_attempt(&lease, callis_id, Result<(), AureliaError>)
```

Lease order:

1. queued outbound `Ack` and `Finish` writes;
2. replay-ready chunks across active stream rings from the shared round-robin cursor;
3. fresh ready chunks across active stream rings from the shared round-robin cursor.

Any active transmitter may lease work from any active stream compatible with the negotiated blob
settings. A transmitter asks BlobManager for the next write, and BlobManager scans active stream
rings from the shared cursor without stream-to-callis assignment state. The cursor advances after a
successful stream lease.

BlobManager snapshots active streams and the shared cursor under the outbound state lock, then
leases from stream rings without holding the manager lock. Cursor advancement is serialized under
the outbound state lock after a successful lease. Ring slots enforce per-slot identity, so
concurrent transmitters cannot receive duplicate leases for one occupied slot. If more
dispatchable work remains after a lease, BlobManager wakes one additional transmitter with
`Notify::notify_one`.

### Transmitter Task

The blob transmitter owns one blob callis write half. It arms the work notifier before checking
for work, then leases and writes one item at a time:

```rust
loop {
    let work_waiter = blob.work_handle().notified();
    tokio::pin!(work_waiter);

    if let Some(lease) = blob.lease_next_blob_write(callis_id).await {
        let result = write_blob_lease(&mut writer, &lease, deadline).await;
        blob.finish_blob_write_attempt(&lease, callis_id, result).await;
        continue;
    }

    tokio::select! {
        _ = shutdown.changed() => write_close_and_exit(),
        _ = writer_shutdown.changed() => break,
        _ = &mut work_waiter => {}
    }
}
```

A leased chunk moves to `InFlight` before the lease is returned to the transmitter, so a fast ACK
can resolve the slot immediately after the frame reaches the peer. A failed write disconnects the
failed blob callis, rolls stamped `Writing` and `InFlight` slots back to `ReplayReady`, and leaves
the bytes available for retransmission.

Callis writer ownership is explicit. Primary callis responses are routed through
`PrimaryDispatchManager`; blob callis responses are routed through BlobManager response lanes. The
callis reader never hands frames to the writer through a direct mpsc writer queue.

### Chunk Wire Encoding

Outbound chunk frames have this wire shape:

```text
outer_frame_header || blob_chunk_inner_header || chunk_bytes
```

The transmitter writes the outer Aurelia frame header, the fixed blob chunk inner header
(`request_msg_id`, `chunk_id`, flags, chunk length), and the leased chunk bytes as separate
buffers. Sequential bounded writes for the two small headers plus the leased chunk slice are
valid; vectored I/O is also valid.

Chunk bytes are written directly from the lease. The outbound chunk path must not build a flat
payload buffer or copy chunk bytes into `Vec`, `BytesMut`, `Bytes::copy_from_slice`, or an
equivalent full-payload staging object.

### ACK, ERROR, Complete, and Reconnect

- ACK for a chunk resolves BlobManager's in-flight lookup by `peer_msg_id`, then calls the owning
  ring's `note_ack(peer_msg_id)`.
- ERROR for an in-flight chunk resolves the same lookup and calls `note_error(peer_msg_id, err)`.
- ERROR from inbound chunk processing is written on the blob callis as an outbound `Error` write
  so the sender observes the receiver's concrete error ID instead of timing out.
- ERROR naming a stream ID fails the outbound or inbound stream and releases reservations.
- Unknown ACKs are ignored.
- `blob-transfer-complete` marks the stream's outbound ring complete.
- Callis loss removes that callis from the active pool and asks every active outbound ring to mark
  `Writing` and `InFlight` slots stamped with that `callis_id` as `ReplayReady`.
- Blob reconnect compares negotiated settings against each active stream's stored settings. A
  setting mismatch fails affected streams with `peer-restarted`.
- Fresh primary sessions fail active blob streams, clear BlobManager outbound and inbound state,
  release reservations, drain blob callis handles, and reset callis history before blob dial
  decisions observe the state.

### Buffer and Window Limits

Each outbound stream ring is bounded by negotiated `ack_window_chunks`. The window counts:

- `Ready`;
- `Writing`;
- `InFlight`;
- `ReplayReady`;
- the pending full chunk held so exact-boundary final chunks can be marked correctly.

The global outbound and inbound blob buffer caps bound admitted stream reservations across peers.
The reservation size is `chunk_size * ack_window_chunks` for each stream. Reservations are released
when streams complete, fail, or close.

`BlobSender::poll_write` accepts only bytes that can immediately enter ring-owned state. It may
fill the ring's existing partial chunk buffer, materialize as many full chunks as available ring
slots permit, and store at most one partial chunk buffer. If the remaining caller bytes cannot
enter ring-owned state, `poll_write` returns `Ok(n)` for the accepted byte count. It returns
`Pending` only when no byte can be accepted and a capacity waiter has been armed. The implementation
must not copy the entire caller buffer before ring capacity is known.

### Inbound Chunk Rules

Inbound rings buffer out-of-order chunks within the negotiated ACK window and deliver to
`BlobReceiverStream` in `chunk_id` order.

- Duplicate chunks are acknowledged and ignored after the first accepted copy.
- Chunks beyond the receiver window fail with `blob-ack-window-exceeded`.
- Chunks that arrive while the receiver's bounded A1 chunk store is full fail the stream with
  `blob-buffer-full`; the blob callis reader does not wait for consumer-side space.
- A `LAST_CHUNK` with earlier missing chunks fails with `blob-stream-missing-chunk`.
- A full receiver window with the next required chunk missing fails with
  `blob-stream-missing-chunk`.
- Oversized chunks fail with `protocol-violation`.
- Receiver idle timeout is measured from stream activation and each accepted chunk.
- Inbound blob idle timeout is actively reaped. Each accepted chunk resets the stream deadline. On
  expiry, A1 fails the receiver with `blob-stream-idle-timeout`, removes the stream, releases its
  inbound reservation, and emits the documented error response when a blob callis is available.
- Blob frames for inactive stream IDs are protocol violations unless the frame kind has an explicit
  duplicate rule in this document.

### Public API

The public blob API consists of:

- `SendOptions::BLOB` requests a blob-capable send.
- `SendOutcome::Blob { sender }` returns a `BlobSender`.
- `BlobSender` implements async write and pushes bytes into the stream ring.
- Accepted inbound blob requests deliver a `BlobReceiver` to the taberna sink.
- `BlobReceiver` implements async read over the inbound ring.

### Testing Coverage

- Ring unit tests cover `Empty`, `Ready`, `Writing`, `InFlight`, `ReplayReady`, ACK cleanup,
  ERROR cleanup, stale write completion, callis replay marking, and window accounting.
- BlobManager unit tests cover stable stream scanning, replay-before-fresh ordering, concurrent
  transmitter lease uniqueness, callis-loss replay, fast ACK, stream cleanup, and reservation
  release.
- Blob transmitter tests cover direct writer ownership, write-failure replay from ring slots,
  direct ACK and close writes, and zero-copy chunk frame writes from leased bytes.
- Transport integration tests cover parallel blob transfers, blob callis unavailability, reconnect
  replay with matching settings, reconnect failure on incompatible settings, transfer beyond the
  ACK window, and end-to-end chunk delivery.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
