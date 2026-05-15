# Aurelia Errors (Gold Source)

This document defines Aurelia's single error type and the semantics for each error ID.
All crates must use `AureliaError` from the internal `aurelia-ids` crate.

## Error Type

- `AureliaError` carries:
  - `ErrorId` (the canonical error ID)
  - optional message (UTF-8, max 1024 bytes)
- No other error enums or per-crate error types are permitted.

## Error Ownership

Each error ID is owned by the layer that can actually observe and produce the condition. Public
application code must not manufacture A1/library transport outcomes.

- `RemoteTabernaRejected` is the only negative outcome an A3 receiver can produce after a
  `TabernaRequest` has been delivered. `TabernaRequest::reject()` always maps to this ID.
- `TabernaBusy` is a receiver-side Aurelia condition: the destination taberna ingress queue or
  accept path cannot accept the request in time.
- `LocalQueueFull` is a sender-side Aurelia condition: outbound admission/backpressure rejected
  work before it could be sent.
- `EncodeFailure` is produced when the sender-side application codec fails before outbound
  admission.
- `DecodeFailure` is produced when the receiver-side application codec fails before a
  `TabernaRequest` is delivered to A3.

Application-specific failure details must travel in application-level messages, not by selecting
Aurelia transport error IDs.

## Error IDs and Semantics

IDs are defined in `docs/ids.md`. Semantics below describe when each error must be raised.

- `unknown-taberna` (1): The destination taberna cannot be resolved locally or via the resolver.
- `local-queue-full` (2): Local ingress/backpressure rejects enqueue due to saturated retained
  lane or inbox capacity, including A3 outbound retained-lane admission rejection.
- `peer-unavailable` (3): The peer is unavailable (connection loss, dial failure, or backend failure).
- `remote-taberna-rejected` (4): The remote taberna explicitly rejected the request.
- `connection-lost` (5): A connection dropped while a transport operation was in progress.
- `peer-restarted` (6): The peer session restarted and invalidated inflight state.
- `protocol-violation` (7): A protocol rule or public send contract was violated, including
  invalid flags, framing, state, application sends below the A3 message-type range, or outbound
  payloads that exceed the configured/wire-representable maximum.
- `unsupported-version` (8): The peer used an unsupported protocol version.
- `encode-failure` (9): Encoding to the wire format failed.
- `decode-failure` (10): Decoding from the wire format failed.
- `taberna-busy` (11): Taberna ingress could not accept the request within the accept timeout or the inbox was saturated.
- `send-timeout` (12): End-to-end send/ACK timeout elapsed for work accepted into outbound
  dispatch.
- `blob-callis-without-primary` (13): A blob callis was requested without an active primary callis.
- `blob-ack-window-exceeded` (14): The blob ACK window was exceeded.
- `blob-stream-not-found` (15): A referenced blob stream ID does not exist.
- `blob-stream-out-of-order` (16): Blob chunks were received out of order.
- `blob-stream-idle-timeout` (17): A blob stream exceeded its idle timeout.
- `blob-stream-missing-chunk` (18): A blob stream ended with missing chunks.
- `blob-buffer-full` (19): Global blob buffer limits were exceeded.
- `address-mismatch` (20): Transport identity validation failed (address mismatch).
- `taberna-already-registered` (21): A taberna was already registered for the requested `TabernaId`.
- `invalid-config` (22): Configuration validation failed (invalid limits or constraints).
- `domus-closed` (23): Domus shutdown closed local taberna ingress; queued accepts were dropped.
- `receive-timeout` (24): `Taberna::next` timed out waiting for a message.
- `snapshot-not-available` (25): A reporting snapshot or reporting query could not be completed
  because the observability task is unavailable.
- `taberna-shutdown` (26): A destination taberna ingress shut down independently of whole-domus
  shutdown, such as an Actix recipient closing while its bridge is still active.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
