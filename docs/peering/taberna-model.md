# Peering Taberna Model

Status: Developed

## Objectives

- Define the typed taberna contract for inbound message acceptance.
- Define registration, deregistration, and local resolution behavior for taberna handles.
- Ensure taberna isolation so one slow target does not block others.

## Technical Details

### Typed Taberna Contract

A taberna represents a typed local ingress boundary. It surfaces fully decoded messages and
optional blob receivers to the application while keeping A1 response state in a completion guard
with private internals.

```rust
pub struct TabernaRequest<Codec: MessageCodec> {
    pub message: Codec::AppMessage,
    pub blob_receiver: Option<BlobReceiver>,
    completion: TabernaCompletion,
}

pub struct TabernaRequestParts<Codec: MessageCodec> {
    pub message: Codec::AppMessage,
    pub blob_receiver: Option<BlobReceiver>,
    pub completion: TabernaCompletion,
}

pub struct TabernaCompletion {
    // private A1 response and wake state
}

impl<Codec: MessageCodec> TabernaRequest<Codec> {
    pub fn accept(self);
    pub fn reject(self);
    pub fn into_parts(self) -> TabernaRequestParts<Codec>;
}

impl TabernaCompletion {
    pub fn accept(self);
    pub fn reject(self);
    pub(crate) fn busy(self);
    pub(crate) fn domus_closed(self);
    pub(crate) fn taberna_shutdown(self);
}

pub struct Taberna<Codec: MessageCodec> {
    pub async fn next(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> Result<TabernaRequest<Codec>, AureliaError>;
}
```

Rules:

- Decoding is performed before a `TabernaRequest` is surfaced; decode failures map to `AureliaError` with `ErrorId::DecodeFailure`.
- `Taberna::next` returns `receive-timeout` when no message arrives before the timeout (default
  1 second).
- `Taberna::next` returns `domus-closed` when shutdown has been triggered and no further messages
  will arrive.
- `TabernaRequest::accept` completes the message as accepted into the local ingress boundary.
  It is fire-and-complete from A3's perspective; internal sender-side wait state is not surfaced
  to the accepting application.
- `TabernaRequest::reject` maps only to `remote-taberna-rejected`; application-specific failure
  details must travel in application messages.
- `TabernaRequest::into_parts` safely separates application-owned payload (`message` and
  `blob_receiver`) from the A1 completion guard. It is public API for applications and adapters
  that need to move payload ownership independently from completion. It must not use `unsafe`.
- Dropping an unsplit `TabernaRequest` rejects through its completion guard.
- Dropping a split `TabernaCompletion` without an explicit decision rejects with
  `remote-taberna-rejected`.
- Public A3 code can complete a request only with `accept` or `reject`. Internal A1 paths use
  crate-private completion methods for queue expiry, domus shutdown, and adapter-owned taberna
  shutdown outcomes.
- The message payload can be dropped independently once the completion guard has been retained or
  resolved. Dropping a `BlobReceiver` still follows the blob abort semantics defined in
  `docs/peering/blobs.md`.

### Blob Streaming Contract

Blob transfers are streamed through standard async adapters:

- `BlobSender`: `tokio::io::AsyncWrite + Unpin + Send`
- `BlobReceiver`: `tokio::io::AsyncRead + Unpin + Send`

Rules:

- A1 must never require a full blob payload in memory.
- Chunking and backpressure are enforced in A1.
- Dropping a sender or receiver aborts the stream and surfaces `peer-unavailable` to the remote side.
- Blob chunk size and ACK window are configured on the domus; defaults are defined in `docs/peering/blobs.md`.

### Taberna Registry

The taberna registry is a concrete internal struct owned by Domus. It is not a trait, not held as a
dynamic, and is the single implementation used by Domus. Domus constructs it internally; there is
no registry injection or external configuration. The registry binds taberna IDs to internal ingress
queues and resolves locally registered tabernae for delivery before any remote resolution is
attempted. Taberna handles are created by `Domus::taberna(id, codec)` and are implicitly
deregistered when dropped.

`TabernaInbox` is internal transport scaffolding. Applications register inbound endpoints through
`Domus::taberna`; they do not implement or name `TabernaInbox`. Optional adapters, including
Actix, must live above this public API and consume `Taberna<Codec>` rather than registering a
separate A1 inbox implementation.

### Taberna Shutdown

Shutdown is explicit and registry-driven. Domus calls `TabernaRegistry::shutdown`, which invokes a
`shutdown` hook on each registered taberna inbox and clears the registry. Shutdown must:

- Close the taberna ingress channel immediately.
- Resolve all queued `TabernaRequest` entries through their completion guards as `domus-closed`
  without delivering them to `Taberna::next`.
- Resolve any pending accept waiters with `domus-closed` so inbound callis can emit ERROR frames.

### Taberna Isolation

The transport must not serialize all deliveries through a single blocking path. A slow or full taberna must only affect messages targeting that taberna.

### Adapters

The crate provides an optional Actix adapter in `src/crates/peering/src/actix_adapter.rs`. Public
users enable it through the top-level `aurelia` crate's `actix` feature. The Actix adapter is a
convenience bridge over `Taberna<Codec>`; any convenience constructor must delegate through
`Domus::taberna(id, codec)` and then drive the returned taberna from the Actix/application side.

Adapters must preserve the `TabernaRequest` delivery contract. If a request carries a
`BlobReceiver`, an adapter that accepts the request must surface that receiver intact to the
application boundary. An adapter must not acknowledge a blob-bearing request after discarding the
receiver.

The Actix bridge forwards requests with `Recipient::try_send` and maps mailbox admission outcomes
to taberna outcomes:

- `Ok(())`: the Actix mailbox accepted the delivery; complete the original `TabernaRequest` with
  `accept`.
- `SendError::Full`: the Actix mailbox rejected admission because it is full; complete the original
  request with `taberna-busy`.
- `SendError::Closed`: the Actix recipient is closed; complete the original request with the
  taberna shutdown outcome. This condition is taberna shutdown, not domus shutdown.

Actix handlers receive `ActixTabernaDelivery<M>` with `type Result = ()`. They must not return
`AureliaError`, and they must not choose Aurelia transport error IDs. A3 error semantics belong in
application messages.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
