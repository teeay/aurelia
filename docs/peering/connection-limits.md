# Connection Limits (A0)

Status: Developed

## Objectives

- Enforce inbound **pre-authentication** handshake limits in A0 for both TCP and socket transports.
- Prevent thundering-herd admission spikes before A1 work begins.
- Preserve per-peer A1 limits for concurrent handshakes and active callis.

## Technical Details

### A0 Pre-Authentication Admission Control

- Admission applies **before transport authentication** begins and **before** any A1 `hello` frames.
- Limits apply to all inbound callis (primary + blob) equally.
- Limits are enforced globally:
  - `inbound_handshake_limit_total` (default 64)
- The hard cap rejects additional handshakes; no high-watermark threshold is used.

### What Counts As an In-Flight Handshake

- An in-flight handshake is any inbound connection that has been accepted by the transport listener
  but has not yet completed A0 authentication/validation (including TLS for TCP).
- The counter is incremented immediately after accept and **before** any A0 authentication begins.
- The counter is decremented when A0 authentication completes, fails, or times out.
- A fully authenticated stream that proceeds to A1 `hello` is **not** counted as an in-flight
  handshake.

### TCP (A0) Admission Flow

- Accept inbound TCP connection and immediately attempt to acquire the global A0 handshake slot.
- If no slot is available, close the raw TCP stream without starting TLS.
- If no slot is available, close the stream and return `PeerUnavailable`.
- If a slot is available, proceed with TLS and then A0 connect-back validation/auth messages.
- Release the slot on success, failure, or timeout.

### Socket (A0) Admission Flow

- Accept inbound socket connection.
- Immediately attempt to acquire the global A0 handshake slot.
- If no slot is available, close the stream before any auth frames are read and return `PeerUnavailable`.
- If a slot is available, proceed with socket auth handshake (connect-back or resume).
- Release the slot on success, failure, or timeout.

### Distinguishing Handshakes From Full Connections

- The limit is **not** a cap on total established connections.
- It is a cap on concurrent A0 authentication work only.
- Existing established callis are governed separately by A1 per-peer callis limits.

### Rejection Semantics

- A0 admission rejections close the connection without any A1 `error` frame.
- No peer-facing error payload is sent because the connection is dropped before A1 begins.

### A1 Per-Peer Handshake Limit

- `inbound_handshake_limit_per_peer` caps per-peer in-flight A1 handshakes (default 3).
- This remains in A1 and does not move into A0.

### A1 Active Callis Cap

- `max_parallel_callis_per_peer` limits active authenticated streams per peer (primary + blob).
- Default cap is `8`.
- This remains in A1 and does not move into A0.

### Shared Accounting Utility (A0)

- A0 admission checks share a common accounting module used by both TCP and socket backends to
  ensure consistent behavior and logging.

### Logging

- Use rate-limited logging with log IDs defined in `docs/ids.md`.
- Emit a rate-limited error when the hard limit is reached.
- No other admission logs are emitted for pre-authentication gating.

### A1 Per-Peer Limit Logging

- Emit a rate-limited info log when rejecting a peer handshake due to the per-peer A1 limit.
- Emit a warning when rejecting a peer callis due to the per-peer active callis cap.

### Testing Scope

- TCP: simulate concurrent inbound connects using low limits; verify rejections without A1 hello.
- Socket: same as TCP using socket transport integration tests.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
