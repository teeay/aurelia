# Aurelia Errors (Gold Source)

This document defines Aurelia's single error type and the semantics for each error ID.
All crates must use `AureliaError` from the internal `aurelia-ids` crate.

## Error Type

- `AureliaError` carries:
  - `ErrorId` (the canonical error ID)
  - optional message (UTF-8, max 1024 characters)
- No other error enums or per-crate error types are permitted.

## Error IDs and Semantics

IDs are defined in `docs/ids.md`. Semantics below describe when each error must be raised.

- `unknown-taberna` (1): The destination taberna cannot be resolved locally or via the resolver.
- `local-queue-full` (2): Local ingress/backpressure rejects the enqueue due to a full queue.
- `peer-unavailable` (3): The peer is unavailable (connection loss, dial failure, or backend failure).
- `remote-taberna-rejected` (4): The remote taberna explicitly rejected the request.
- `connection-lost` (5): A connection dropped while a transport operation was in progress.
- `peer-restarted` (6): The peer session restarted and invalidated inflight state.
- `protocol-violation` (7): A protocol rule was violated (invalid flags, framing, or state).
- `unsupported-version` (8): The peer used an unsupported protocol version.
- `encode-failure` (9): Encoding to the wire format failed.
- `decode-failure` (10): Decoding from the wire format failed.
- `taberna-busy` (11): Taberna ingress could not accept the request within the accept timeout or the inbox was saturated.
- `send-timeout` (12): End-to-end send/ACK timeout elapsed.
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

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
