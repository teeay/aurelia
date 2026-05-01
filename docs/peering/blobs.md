# Blob Transfers

Status: Developed

## Objectives

- Define the A1.5 stateful blob overlay built on A1 message transport.
- Specify blob transfer lifecycle, buffering rules, and reconnect behavior.
- Define blob-specific configuration semantics and error handling.

## Technical Details

### A1.5 Overlay Summary

Blob transfers are a stateful overlay on A1’s stateless message transport. The blob request is carried as an application message on the primary callis, and the subsequent stream is carried over the blob callis using reserved control frames defined in `docs/peering/wire-protocol.md`. The Domus public API exposes blob transfers via `SendOptions { blob: true }` and returns a `BlobSender` on acceptance; receivers obtain a `BlobReceiver` alongside the typed message.

### Blob Transfer Lifecycle

1. A3 initiates a blob transfer by sending an application message on the primary callis with the wire header flag `BLOB` set. The application `msg_type` remains in the header; A1 treats the payload as opaque.
2. The receiver validates and delivers the request to the destination taberna. If the taberna rejects, the request fails with the normal transport error and no blob stream is started.
3. If the request is accepted, the receiver ensures a blob callis is available. If none is open, it opens one and negotiates `chunk_size` and `ack_window_chunks` in the blob callis hello handshake (see `wire-protocol.md`). Blob callis establishment occurs within the blob request timeout. If establishment fails, the request fails and is not ACKed.
4. Once the blob callis is available, the receiver ACKs the blob request message on the primary callis. The request’s `peer_msg_id` becomes the stream ID (`request_msg_id`).
5. The sender sends `blob-transfer-start` on the blob callis with `request_msg_id` set to the original request’s `peer_msg_id`, then waits for its ACK.
6. The sender streams `blob-transfer-chunk` frames on the blob callis. Each frame carries the `request_msg_id`, a monotonically increasing `chunk_id` (starting at 0), and the `LAST_CHUNK` flag on the final chunk. The sender must respect the negotiated `chunk_size` and must not exceed the negotiated `ack_window_chunks` in flight.
7. The receiver dedupes chunks by `(request_msg_id, chunk_id)` and ACKs each chunk frame as it is received.
8. The receiver delivers chunks to the stream sink only in `chunk_id` order, buffering out-of-order chunks within the negotiated `ack_window_chunks`.
9. If the receiver's buffer is full and the next required chunk is still missing, or if a `LAST_CHUNK` arrives with any earlier chunk missing, it must fail the stream with `error` (`blob-stream-missing-chunk`) and include `last_delivered_chunk_id` and `delivered_bytes` in the error message (payload format in `wire-protocol.md`).
10. After delivering all chunks in order (including the `LAST_CHUNK`), the receiver sends `blob-transfer-complete` for the stream.
11. The sender considers the blob transfer successful once it receives `blob-transfer-complete`. Any missing ACKs, errors, or protocol violations fail the transfer.
12. If the blob callis disconnects mid-transfer, the sender reconnects and resumes the stream by replaying unacknowledged chunks starting from the lowest missing `chunk_id`. Replayed chunks are deduped and ACKed by the receiver.
13. If blob callis negotiation values change on reconnect (chunk size or window), all in-flight blob streams are failed with `peer-restarted` semantics.
14. Receiver-side stream idle timeout: the receiver must track stream activity and fail any stream that is idle for more than `2 * send_timeout`. On timeout, the receiver drops stream state and responds with `error` (`blob-stream-idle-timeout`) when a subsequent chunk frame arrives. This timeout does not start until `blob-transfer-start` is received.

### Dispatch and State Isolation

- BlobSender/BlobReceiver operate directly on the per-stream chunk windows and do not interact with peer state.
- A dedicated blob dispatch task drains the per-stream outbound windows directly into the blob callis writer.
- No new channels, queues, or buffers exist between the window-based chunk buffers and the transports.

### Total Blob Buffer Cap

A1 enforces a total blob buffer capacity across all peers, with separate caps for outbound and inbound flows.

- Outbound cap: reject new blob requests immediately with `blob-buffer-full` if the outbound buffer cap would be exceeded.
- Inbound cap: reject new blob requests immediately with `blob-buffer-full` (no ACK) if the inbound buffer cap would be exceeded.
- Buffer reservations are made at stream open (before any transfer frames are sent) and released when a stream completes or fails.
- Local blob delivery reserves against the same outbound and inbound caps; in-memory streams are not exempt.

### Blob Configuration Semantics

- `blob_chunk_size`: local maximum chunk size. The negotiated chunk size for a blob callis is the minimum of the peers’ configured values.
- `blob_ack_window`: local maximum in-flight chunk count. The negotiated ACK window is the minimum of the peers’ configured values.
- `blob_outbound_buffer_bytes`: total outbound reservation cap across all peers. Default 256 MiB.
- `blob_inbound_buffer_bytes`: total inbound reservation cap across all peers. Default 256 MiB.

Blob configuration values are negotiated and enforced per blob callis; the wire payload formats are defined in `docs/peering/wire-protocol.md`.
Blob buffer caps are validated and clamped to the configured limits described in `docs/peering/services-provided.md`.

#### Chunk Size Guidance

- TCP: chunk size should be derived from the path MTU with headroom for IP/TCP and Aurelia headers. When MTU is unknown, use a conservative fallback of 1200 bytes.
- Socket: default chunk size is 128 KiB. Keep it configurable and avoid multi‑megabyte chunks to prevent head‑of‑line blocking.
- The negotiated chunk size is agreed during the blob callis hello handshake and applies for the lifetime of the blob callis.

#### ACK Window Guidance

- The receiver negotiates `ack_window_chunks` during the blob callis hello handshake and the agreed value applies for the lifetime of the blob callis.
- The sender must keep in-flight chunks at or below this window and may choose a smaller local window.
- A1 should maintain a ring buffer of size `ack_window_chunks` for receiver-side tracking; the sender maintains a ring buffer for its chosen window.
- Ring buffer implementation details live in `docs/peering/ring-buffer.md`.
- The window applies per stream; multiple concurrent streams must each respect the negotiated limit.
- On blob callis reconnect, replayed chunks still consume window capacity; the sender must not enqueue new chunks until replayed in-flight chunks are ACKed.

Defaults:

- TCP: 1200-byte chunk size and 1024 chunks in flight.
- Socket: 128 KiB chunk size and 32 chunks in flight.

These defaults are required and are applied based on the local transport kind unless explicitly overridden in configuration.

### Error Semantics

- Blob-specific error IDs are defined in `docs/ids.md`.
- Error payload formats and required fields are defined in `docs/peering/wire-protocol.md`.

### Logging Scope

Logging levels are defined in `docs/aurelia.md`. Minimum blob transfer logging scope:

- Blob request accepted/rejected and the reason.
- Blob callis establishment attempt, success, failure, and retry decisions.
- Blob transfer start received and the outcome (idempotent accept vs error).
- Chunk receipt (with stream id + chunk id) and stream completion (`blob-transfer-complete`).
- Stream failures (idle timeout, missing chunk, ack window exceeded, unknown stream).

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
