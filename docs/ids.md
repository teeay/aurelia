# Aurelia IDs (Gold Source)

This document is the canonical list of IDs used across the Aurelia workspace.
All crates must reference this file when defining, consuming, or documenting IDs.
All ID definitions are implemented in the internal `aurelia-ids` crate (`src/crates/ids`).
The public `aurelia` crate re-exports shared ID and error API directly from `aurelia-ids` so
cross-layer identifiers do not appear to be owned by the A1 peering implementation.

Error semantics are tracked in `docs/errors.md`. This document remains the authoritative
source for ID values and ranges.

## Peering (Transport)

- Taberna ID: `u64`
  - Globally addressable taberna identifier for routing and delivery.
- Message Type ID: `u32`
  - Application or transport-reserved message type used to interpret payloads.
  - Transport-reserved types are defined below and referenced by `docs/peering/wire-protocol.md`.
- Peer Message ID: `u32`
  - Transport-private identifier allocated per peer session for ACK correlation, replay, and dedupe.

### Message Type Priority Ranges (A1/A2/A3)

Message type IDs are also used for outbound priority classification on the primary callis.
Ranges are defined as:

- All outbound traffic is treated as **messages**. A1 control traffic is represented as messages whose
  `MessageType` falls in the A1 range; A2 and A3 are also message ranges.

- **A1 (transport control and transport-critical):** `0x0000_0000` - `0x0000_FFFF`
- **A2 (Aurelia services):** `0x0001_0000` - `0x00FF_FFFF`
- **A3 (application):** `0x0100_0000` - `0xFFFF_FFFF`

The priority range constants and classifier are implemented in `aurelia-ids`. Peering and any
future workspace crate that needs message priority classification must call the shared classifier
instead of duplicating raw numeric range checks.

Applications and tests must not hand-write low numeric message type IDs for A3 traffic. Use the
public `a3_message_type(offset)` helper, re-exported by `aurelia`, to derive application message
types from the A3 base. `a3_message_type(0)` returns `0x0100_0000`; `try_a3_message_type(offset)`
returns `None` if the offset would exceed the `u32` message type range.
Public application sends must reject message types below `A3_MESSAGE_TYPE_BASE` before local
delivery or remote routing. A2 message types are reserved for Aurelia service traffic and are not
accepted from `MessageCodec::encode_app`.

### Transport-Reserved Message Type IDs (v1)

IDs are assigned sequentially starting at `1`.

- `1`: hello
- `2`: hello-response
- `3`: keepalive
- `4`: ack
- `5`: close
- `6`: error
- `7`: reserved
- `8`: blob-transfer-chunk
- `9`: blob-transfer-complete

### Wire Header Flags (v1)

Header flags are defined in `docs/peering/wire-protocol.md`.

- `0x0001`: `BLOB`
- `0x0002`: `RECONNECT`

### Peering Error IDs (v1)

IDs are assigned sequentially starting at `1`.
The `aurelia-ids` crate defines `ErrorId` with the same macro-generated pattern used for `LogId`;
`ErrorId::ALL`, `ErrorId::as_u32`, and `TryFrom<u32>` are generated from the variant list so the
numeric registry cannot drift from the enum definition.

- `1`: unknown-taberna
- `2`: local-queue-full
- `3`: peer-unavailable
- `4`: remote-taberna-rejected
- `5`: connection-lost
- `6`: peer-restarted
- `7`: protocol-violation
- `8`: unsupported-version
- `9`: encode-failure
- `10`: decode-failure
- `11`: taberna-busy
- `12`: send-timeout
- `13`: blob-callis-without-primary
- `14`: blob-ack-window-exceeded
- `15`: blob-stream-not-found
- `16`: blob-stream-out-of-order
- `17`: blob-stream-idle-timeout
- `18`: blob-stream-missing-chunk
- `19`: blob-buffer-full
- `20`: address-mismatch
- `21`: taberna-already-registered
- `22`: invalid-config
- `23`: domus-closed
- `24`: receive-timeout
- `25`: snapshot-not-available
- `26`: taberna-shutdown

### Error Message Limits

- Error messages must be UTF-8 and bounded to a maximum of 1024 bytes.

## Logging (Rate-Limited)

Logging event IDs are `u32` values used to gate rate-limited logs. IDs must be unique across the
workspace.

- `1001`: inbound-handshake-hard-limit-reached
- `1002`: inbound-handshake-per-peer-limit-rejected
- `1003`: per-peer-callis-limit-rejected
- `1004`: outbound-ready-queue-overrun
- `1005`: duplicate-outbound-ack-response
- `1006`: duplicate-outbound-error-response

If an ID is used by any crate, it must be listed here with its type and purpose.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
