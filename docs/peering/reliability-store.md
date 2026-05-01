# Reliability Store and ACK Tracking

Status: Developed

## Objectives

- Define retention and replay of unacknowledged messages.
- Define ACK correlation and dedupe behavior.
- Define invalidation behavior after peer restart.

## Technical Details

### Peer Message IDs

Peer message IDs are transport-private identifiers used for ACK correlation, replay, and dedupe. Their canonical definition and type live in `docs/ids.md`.

Allocation policy:

- A monotonically incrementing `u32` counter per peer session.
- Wraps on overflow with normal `u32` rollover.
- No reserved values or special-case ranges.
- ID space is `2^32` (~4.29B), far exceeding the default queue size of 128.
- The peer message ID space is shared across all calles and connections between the two peers.

Blob transfer frames are a special case:

- The blob transfer stream ID is the `peer_msg_id` of the original blob request message (carried as `request_msg_id` in blob frames).
- `blob-transfer-start` and `blob-transfer-chunk` frames are acknowledged with the standard `ack` control message; each frame still has its own wire header `peer_msg_id`.
- Receiver-side dedupe for blob chunks is keyed by `(request_msg_id, chunk_id)`. Duplicates must still be ACKed.
- The original blob request is an application message on the primary callis with the `BLOB` header flag set; its ACK is gated on blob callis readiness and pending stream registration.
- On reconnect, retained unacknowledged chunk frames are replayed starting from the lowest missing `chunk_id` per stream. Replayed chunks still count toward the in-flight window.
- Receiver-side stream idle timeout is `2 * send_timeout`. If a stream exceeds this idle threshold, the receiver drops stream state and responds with `error` (`blob-stream-idle-timeout`) when a subsequent chunk frame arrives.
- Late chunks for unknown or completed streams are rejected with `error` (`blob-stream-not-found`).
- If the sender exceeds the negotiated ACK window for a stream, the receiver responds with `error` (`blob-ack-window-exceeded`) and fails that stream.

### Retained Messages

When a message is sent to a remote peer, it must be retained until one of the following occurs:

- ACK received.
- Permanent transport failure declared.
- Peer restart/new connection invalidates inflight messages.
- Timeout or expiry policy reached.

There is no separate ACK timeout in v1. Send timeout is the single end-to-end bound for sender-side completion and must include any delay from queueing, reconnects, and ACK wait.

### Replay After Transient Reconnect

If the connection drops but the peer has not restarted:

- Reconnect.
- Resume transport.
- Replay retained unacknowledged messages as needed.

Reconnect attempts are bounded by `send_timeout` while the peer is impaired. If all callis are down
and no reconnect succeeds within `send_timeout`, the peer handle is torn down and retained
unacknowledged messages are failed locally.

### Invalidation After Peer Restart

If the remote peer restarts and a fresh connection is established:

- Retained inflight messages on the surviving peer are invalidated.
- Those messages fail locally.
- Any in-flight blob transfers associated with those messages are failed as well.

### Invalidation After Reconnect Disagreement

If a reconnect handshake is attempted and the peer does not echo `RECONNECT`, the originator
treats the new connection as a fresh session on the **same** peer handle. Retained inflight
messages are invalidated and failed locally with `peer-restarted`, retained inflight blob streams
are failed locally, and queued non-A1 messages are re-dispatched on the new callis.

### Receiver Dedupe

Receiver-side dedupe is required only to protect against duplicate delivery during transient reconnect/replay. Dedupe keys include authenticated peer identity and peer message ID.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
