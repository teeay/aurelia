# Aurelia IDs (Gold Source)

This document is the canonical list of IDs used across the Aurelia workspace.
All crates must reference this file when defining, consuming, or documenting IDs.
All ID definitions are implemented in the internal `aurelia-ids` crate (`src/crates/ids`).

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

### Transport-Reserved Message Type IDs (v1)

IDs are assigned sequentially starting at `1`.

- `1`: hello
- `2`: hello-response
- `3`: keepalive
- `4`: ack
- `5`: close
- `6`: error
- `7`: blob-transfer-start
- `8`: blob-transfer-chunk
- `9`: blob-transfer-complete

### Wire Header Flags (v1)

Header flags are defined in `docs/peering/wire-protocol.md`.

- `0x0001`: `BLOB`
- `0x0002`: `RECONNECT`

### Peering Error IDs (v1)

IDs are assigned sequentially starting at `1`.

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

### Error Message Limits

- Error messages must be UTF-8 and bounded to a maximum of 1024 characters.

## Logging (Rate-Limited)

Logging event IDs are `u32` values used to gate rate-limited logs. IDs must be unique across the
workspace.

- `1001`: inbound-handshake-hard-limit-reached
- `1002`: inbound-handshake-per-peer-limit-rejected
- `1003`: per-peer-callis-limit-rejected

If an ID is used by any crate, it must be listed here with its type and purpose.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
