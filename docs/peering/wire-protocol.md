# Peering Wire Protocol

Status: Developed

## Objectives

- Define the wire header and framing expectations.
- Define reserved transport control message types.
- Establish versioning and validation rules.

## Technical Details

### Wire Header (v1)

The wire protocol uses a compact fixed header followed by opaque payload bytes. The header includes a payload length prefix for the main message body. Only the header elements defined here are interpreted by A1.

```rust
struct WireHeader {
    version: u16,
    flags: u16,
    msg_type: u32,
    peer_msg_id: u32,
    src_taberna: u64,
    dst_taberna: u64,
    payload_len: u32,
}
```

#### Endianness and Byte Layout

- Endianness: network byte order (big-endian) for all multi-byte fields.
- No magic or preamble is used.
- Header size is 32 bytes, followed immediately by `payload_len` bytes.

Byte layout (offsets in bytes):

- 0..2: `version` (u16, big-endian)
- 2..4: `flags` (u16, big-endian)
- 4..8: `msg_type` (u32, big-endian)
- 8..12: `peer_msg_id` (u32, big-endian)
- 12..20: `src_taberna` (u64, big-endian)
- 20..28: `dst_taberna` (u64, big-endian)
- 28..32: `payload_len` (u32, big-endian)

#### Wire Header Flags

Header flags apply to all frames. Unless specified, flags must be zero. The `hello`/`hello-response` use
header flags for reconnect and callis type; application messages use the blob flag to initiate blob transfers.
All other control frames must set header flags to zero.

- `0x0001` (`BLOB`): indicates a blob callis when used on `hello`/`hello-response`, and indicates a blob
  transfer initiation when set on an application message on the primary callis. Not valid on other control frames.
- `0x0002` (`RECONNECT`): on `hello`, signals that the originator believes the peer session is the
  same epoch (the peer has not restarted) and asks the receiver to replay any retained inflight.
  On `hello-response`, the receiver echoes `RECONNECT` only if its peer session is still live;
  otherwise the originator treats the connection as a fresh session. Not valid on other frames.

### Reserved Transport Control Messages

The following message types are reserved for transport control and are defined in `docs/ids.md`:

- hello
- hello-response
- keepalive
- ack
- close
- error
- blob-transfer-chunk
- blob-transfer-complete

### Control Message Payloads

All transport control messages use the standard wire header. Unless explicitly defined otherwise, control message payloads are empty (length 0).

#### Hello / Hello-Response Payload

Primary callis hello payloads are empty. Blob callis hello payloads carry the negotiated blob
parameters (see below).

Rules:

- Header flag handling, reconnect semantics, and callis lifecycle rules are defined in `docs/peering/transport-model.md`.
- A0 transport authentication (TLS or socket auth) occurs below A1; A1 hello payloads are unchanged across transports.

#### Blob Callis Hello Negotiation

Blob callis handshake extends the hello payload to negotiate chunk sizing and ACK window. These additional fields are present only when the `BLOB` header flag is set.

```text
hello (blob callis):
u32 proposed_chunk_size
u32 proposed_ack_window_chunks

hello-response (blob callis):
u32 agreed_chunk_size
u32 agreed_ack_window_chunks
```

Rules:

- `proposed_chunk_size` and `proposed_ack_window_chunks` must be non-zero.
- The receiver replies with `agreed_*` values that are `<=` the proposed values, typically `min(local_limit, proposed)`.
- If the receiver cannot accept any non-zero values, it must close the callis and send `error` (`protocol-violation`).
- If the initiator receives `agreed_*` values that exceed its proposal, it must close the callis and treat it as a protocol violation.
- The negotiated values apply for the lifetime of the blob callis. Changes require closing and re-opening the blob callis.

#### Close Payload

The `close` control message has an empty payload. Callis shutdown semantics are defined in `docs/peering/transport-model.md`.

### Blob Transfer Messages

Blob transfers are initiated by an application message on the primary callis and then streamed on the blob callis using A1-reserved control frames. The application `msg_type` is retained on the initial request; A1 control messages have their own `msg_type` values.

Message types:

- `blob-transfer-chunk`
- `blob-transfer-complete`

Payloads (byte layout after the standard header):

```text
blob-transfer-chunk:
u32 request_msg_id
u64 chunk_id
u16 flags
u32 chunk_len
chunk bytes (length = chunk_len)

blob-transfer-complete:
u32 request_msg_id
```

Flags:

- `0x0001` (`LAST_CHUNK`): chunk is the final chunk in the stream.

Rules:

- The blob transfer stream ID is the `peer_msg_id` of the original blob request message sent on the primary callis. This ID is carried as `request_msg_id` in all blob transfer frames.
- Processing and lifecycle semantics for blob transfers are defined in `docs/peering/blobs.md`.

### Error Control Message Payload

The `error` control message uses the same header and framing as all other messages. Its payload is:

```rust
struct ErrorPayload {
    error_id: u32,
    message: String, // UTF-8, bounded to 1024 bytes
}
```

Error IDs are defined in `docs/ids.md`.
Receivers must reject unknown `error_id` values as `protocol-violation` and include the raw ID in
local diagnostics. Unknown inbound error IDs must not be coerced to `peer-unavailable`.

`blob-stream-missing-chunk` error payload requirements:

- The `message` must include `last_delivered_chunk_id` and `delivered_bytes`.
- `delivered_bytes` is computed as `delivered_chunks * negotiated_chunk_size` because the error is raised before the final chunk is delivered.

`blob-buffer-full` error payload requirements:

- The `message` should include whether the rejection was `inbound` or `outbound`, the configured buffer cap in bytes, and that the decision is reservation-based at stream open.

### Validation Rules

- Unknown protocol versions must be rejected.
- Payload length must match the frame body size.
- Reserved message types are handled by transport control handlers only.
- Payload length must not exceed the configured maximum (`DomusConfig::max_payload_len`, default 8 MiB).
- Public application sends enforce the same payload maximum before local delivery or remote
  routing. The outbound path must also reject payload lengths that cannot be represented in the
  `u32` wire header.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
