# Codec and Application Integration

Status: Developed

## Objectives

- Define the codec boundary for typed application messages.
- Keep transport internals operating on raw message type and payload bytes.
- Provide adapter strategy for Actix without coupling the core crate.
- Ensure Actix-backed tabernae preserve blob receivers through typed delivery.

## Technical Details

### Codec Boundary

```rust
trait MessageCodec: Send + Sync + 'static {
    type AppMessage;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError>;
    fn decode_app(&self, msg_type: MessageType, payload: &[u8]) -> Result<Self::AppMessage, AureliaError>;
}

struct EncodedMessage {
    msg_type: MessageType,
    payload: bytes::Bytes,
}
```

`MessageType` is the shared `u32` alias from `aurelia-ids`. The transport operates on message type
and payload bytes. Typed callers use this codec boundary through:

- `Domus::send<Codec>(..., SendOptions) -> SendOutcome` for outbound typed messages.
- `Domus::taberna(id, codec) -> Taberna<Codec>` for inbound typed delivery; decoding happens
  synchronously on the inbound callis reader path before a `TabernaRequest` is queued or surfaced,
  and decode failures map to `AureliaError` with `ErrorId::DecodeFailure`.

`encode_error(...)` and `decode_error(...)` helpers are provided for convenience when mapping codec failures to `AureliaError`.

Codecs are expected to be cheap, deterministic, and non-blocking. They may perform format and
schema validation required to convert bytes into the application message type. Semantic or
business validation belongs in the application after receiving the decoded message, not in the
codec, because heavy decode work delays subsequent inbound frames on the same primary callis.

### Actix Integration

Actix support is implemented as an optional adapter in `src/crates/peering/src/actix_adapter.rs`
rather than as a core transport dependency. Public users enable it through
`aurelia = { features = ["actix"] }` and import the adapter types from `aurelia`.

The Actix bridge is a convenience layer on top of the public `Taberna<Codec>` API. It must not be
an A1 inbox implementation, must not implement `TabernaInbox`, and must not register directly with
the taberna registry. Any `Domus::actix_taberna(...)` convenience entry point must delegate through
the same public taberna registration path as `Domus::taberna(id, codec)` and then attach the Actix
bridge to the returned `Taberna<Codec>`.

The bridge is driven from the Actix/application side. If a task is used to drain taberna requests
and forward them to Actix, that task is part of the Actix adapter/application integration and must
not be an Aurelia-runtime task spawned by A1 to wait for Actix actor completion.

The Actix adapter must preserve the full taberna delivery contract. An inbound request is not only
a decoded application message; it may also carry a `BlobReceiver`. The Actix bridge therefore uses
one envelope delivery type for all inbound Actix taberna messages:

```rust
pub struct ActixTabernaDelivery<M> {
    pub message: M,
    pub blob_receiver: Option<BlobReceiver>,
}

impl<M> actix::Message for ActixTabernaDelivery<M>
where
    M: Send + 'static,
{
    type Result = ();
}
```

The Actix bridge delivers this envelope to:

```rust
Recipient<ActixTabernaDelivery<C::AppMessage>>
```

Actor handlers must not return `AureliaError` and must not choose Aurelia transport error IDs. A3
error semantics belong in application messages carried by the application codec.

The bridge behavior is:

- Receive `TabernaRequest<Codec>` values from `Taberna::next(...)`.
- Move the decoded `Codec::AppMessage` and optional `BlobReceiver` into
  `ActixTabernaDelivery<C::AppMessage>` without copying application payloads or dropping the blob
  receiver.
- Forward the delivery with `Recipient::try_send`, not `Recipient::send(...).await`.
- Treat `try_send` success as Aurelia taberna acceptance and complete the original
  `TabernaRequest` with `accept`.
- Treat `SendError::Full` as Aurelia taberna busy and complete the original request with the
  `taberna-busy` outcome.
- Treat `SendError::Closed` as Aurelia taberna shutdown and complete the original request with the
  taberna shutdown outcome. This is taberna shutdown, not domus shutdown.
- Never wait for Actix handler completion before resolving Aurelia acceptance.

Actors that do not expect blobs still receive the same envelope and observe
`blob_receiver == None` for ordinary messages. Actors that receive blob-bearing requests own the
`BlobReceiver` and may read it asynchronously using the normal blob streaming contract.

The Actix adapter must not acknowledge a blob-bearing request after discarding its receiver. It
uses the safe public `TabernaRequest::into_parts()` split to move the message, blob receiver, and
completion guard out of `TabernaRequest`; it does not clone application messages. Public A3 code
can complete a split request only with accept or reject.

### Validation

- Typed codec helpers are covered by the core unit and integration suites.
- The Actix adapter has targeted non-blob delivery, blob receiver preservation, full-mailbox,
  closed-recipient, and no-A3-Aurelia-error tests.
- The optional feature is validated by `cargo test --workspace --all-features`.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
