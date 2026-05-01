# Peering Taberna Model

Status: Developed

## Objectives

- Define the typed taberna contract for inbound message acceptance.
- Define registration, deregistration, and local resolution behavior for taberna handles.
- Ensure taberna isolation so one slow target does not block others.

## Technical Details

### Typed Taberna Contract

A taberna represents a typed local ingress boundary. It surfaces fully decoded messages and optional blob receivers to the application.

```rust
pub struct TabernaRequest<Codec: MessageCodec> {
    pub message: Codec::AppMessage,
    pub blob_receiver: Option<BlobReceiver>,
}

impl<Codec: MessageCodec> TabernaRequest<Codec> {
    pub async fn accept(self) -> Result<(), AureliaError>;
    pub async fn reject(self, err: AureliaError);
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
- `TabernaRequest::accept` completes only when the message is accepted into the local ingress boundary.

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

### Taberna Shutdown

Shutdown is explicit and registry-driven. Domus calls `TabernaRegistry::shutdown`, which invokes a
`shutdown` hook on each registered taberna inbox and clears the registry. Shutdown must:

- Close the taberna ingress channel immediately.
- Drop all queued `TabernaRequest` entries (drain means drop, no delivery to `Taberna::next`).
- Resolve any pending accept waiters with `domus-closed` so inbound callis can emit ERROR frames.

### Taberna Isolation

The transport must not serialize all deliveries through a single blocking path. A slow or full taberna must only affect messages targeting that taberna.

### Adapters

The crate provides an optional Actix adapter in `src/crates/peering/src/actix_adapter.rs`. Other taberna styles integrate by consuming `Taberna<Codec>` and driving a request loop.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
