# Codec and Application Integration

Status: Developed

## Objectives

- Define the codec boundary for typed application messages.
- Keep transport internals operating on raw message type and payload bytes.
- Provide adapter strategy for Actix without coupling the core crate.

## Technical Details

### Codec Boundary

```rust
trait MessageCodec: Send + Sync + 'static {
    type AppMessage;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError>;
    fn decode_app(&self, msg_type: u32, payload: &[u8]) -> Result<Self::AppMessage, AureliaError>;
}

struct EncodedMessage {
    msg_type: u32,
    payload: bytes::Bytes,
}
```

The transport operates on message type and payload bytes. Typed callers use this codec boundary through:

- `Domus::send<Codec>(..., SendOptions) -> SendOutcome` for outbound typed messages.
- `Domus::taberna(id, codec) -> Taberna<Codec>` for inbound typed delivery; decoding happens before a `TabernaRequest` is surfaced and decode failures map to `AureliaError` with `ErrorId::DecodeFailure`.

`encode_error(...)` and `decode_error(...)` helpers are provided for convenience when mapping codec failures to `AureliaError`.

### Actix Integration

Actix support is implemented as an optional adapter in `src/crates/peering/src/actix_adapter.rs` rather than as a core transport dependency.

The Actix adapter continues to decode inbound payloads with the supplied codec and forwards typed messages to an Actix `Recipient`.

`ActixEndpointSink<C>` requires:

- `C: MessageCodec`
- `C::AppMessage: actix::Message + Send + 'static`
- `<<C as MessageCodec>::AppMessage as actix::Message>::Result: Send`

The adapter decodes the raw transport payload with the supplied codec and forwards the typed message to an Actix `Recipient`. Decode failures map to `AureliaError` with `ErrorId::DecodeFailure`; mailbox failures map to `AureliaError` with `ErrorId::RemoteTabernaRejected`.

### Validation

- Typed codec helpers are covered by the core unit and integration suites.
- The Actix adapter has targeted happy-path, decode-failure, and mailbox-failure tests.
- The optional feature is validated by `cargo test --workspace --all-features`.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
