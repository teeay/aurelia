# Peering Services Provided

Status: Developed

## Objectives

- Define the services and contracts the peering crate provides to higher layers.
- Describe integration boundaries (routing, codecs) for A1.
- Enumerate the configuration surface and point to authoritative semantics.

## Technical Details

### Per-Callis A0 Timeout Semantics

`socket_handshake_timeout` and `tcp_handshake_timeout` cover complete per-callis A0
authentication, including connect-back validation, for every primary and blob callis.

A1 `RECONNECT` remains part of the peer-session `hello` semantics. No public application API is
added for arbitrary secondary or tertiary primary callis; repeated callis are a test-only
backend/transport verification surface.

### Aurelia Wrapper Entry Point

- Public access to Domus must be provided through the `aurelia` crate wrapper, not by constructing peering types directly.
- The Aurelia wrapper should be thin and prefer re-exported types so the compiler can optimize away indirection.
- The Aurelia runtime handle is internal to the library and must not be exposed in the public API.
- Peering documentation defines the Domus surface but assumes the entry point is the Aurelia wrapper.

### Service Surface (A1)

Peering provides the following services to higher layers:

- **Message delivery:** Send a message to a remote taberna and complete only after remote ingress ACK.
- **Accepted outbound ownership:** Once A1 accepts an outbound message into retained transport
  ownership, reliability owns it until ACK, timeout, close, peer restart, or permanent failure.
  Dropping the caller's local wait interest leaves accepted work retained; result delivery is
  best-effort if the caller is no longer waiting.
- **A3 admission bound:** `send_queue_size` is the per-peer A3 retained outbound capacity. It
  counts accepted A3 messages while they are ready, being sent, inflight, or replay-ready. If the
  A3 retained lane is full, A1 rejects the send immediately with `local-queue-full` and the
  message is not accepted into dispatch.
- **Library-managed A1/A2 capacity:** A1 ACKs, retained A1 ERROR responses, and A2 service
  messages use library-managed retained capacities defined in `docs/peering/backpressure-queues.md`.
  A1 close and keepalive frames are immediate per-callis control, not retained queue items.
- **Blob transfer initiation:** Send a blob request on the primary callis and coordinate the blob callis stream lifecycle.
- **Blob streaming:** Stream blob chunks through `BlobSender` and `BlobReceiver` with ordered delivery and explicit completion.
- **Internal taberna registry:** Domus owns a single concrete taberna registry; there is no registry injection and no registry exposure in the public API.

### Domus Typed Send + Blob Handles

- **Typed `send` only:** Domus exposes a single method named `send`. It accepts a codec and an app message, encodes it into `(msg_type, payload)`, and uses A1 for delivery. There is no `send_typed` or `send_blob` in the public API.
- **No untyped send:** All untyped send entry points are removed. `send` is the only public API for outbound delivery.
- **Blob flag on send:** The caller requests a blob transfer by setting a flag in `SendOptions` passed to `send`.
- **No blob length parameter:** `SendOptions` contains only the blob flag; reservations are derived solely from negotiated chunk size and ack window.
- **Sender handle:** If the blob request is accepted, the sender receives a `BlobSender` stream sink for writing the blob bytes.
- **Public blob send lifecycle:** Call `send(..., SendOptions::BLOB)`, match
  `SendOutcome::Blob { sender }`, write bytes to the sender with `AsyncWriteExt::write_all`, then
  call `AsyncWriteExt::shutdown` to seal the stream.
- **Receiver handle on accept:** The inbound taberna receives an optional `BlobReceiver` alongside the typed message in the same accept call. If the taberna rejects the message, the `BlobReceiver` is discarded and no blob stream is established.
- **Public blob receive lifecycle:** Inspect `TabernaRequest::blob_receiver`, call
  `TabernaRequest::accept` once the application accepts the message, then read the receiver with
  `AsyncRead`/`AsyncReadExt` until EOF.
- **Optional means empty:** When the request is not a blob, `blob_receiver` is `None` and no stream is created.
- **Single taberna implementation:** Domus provides a single `Taberna` implementation per
  `TabernaId`. `Domus::taberna` returns the `Taberna` handle for local ingress.
- **Implicit deregistration:** Dropping the `Taberna` handle must deregister the taberna from local resolution.
- **Duplicate registration:** Registering an existing `TabernaId` returns `AureliaError` with `ErrorId::TabernaAlreadyRegistered`.
- **No stream registration:** There is no separate stream registration API. Stream multiplexing and demultiplexing are internal to A1 and are not part of the public surface.
- **Local dispatch parity:** Local blob delivery may use an in-memory stream internally, but the public API, reservation rules, and `BlobSender`/`BlobReceiver` semantics must remain identical to remote dispatch.
- **Typed inbound delivery:** Inbound taberna delivery is typed via the supplied `MessageCodec`. Decode failures must be surfaced as `AureliaError` with `ErrorId::DecodeFailure` and rejected without exposing raw message bytes in the public API.
- **Timed receive:** `Taberna::next` supports an optional timeout with a default of 1 second to satisfy the async + timeout policy.
- **Next return semantics:** `Taberna::next` returns `receive-timeout` when no message is received before the timeout; returns `domus-closed` on shutdown.

#### Required Public API (Domus)

```rust
pub struct SendOptions {
    pub blob: bool,
}

pub enum SendOutcome {
    MessageOnly,
    Blob { sender: BlobSender },
}

impl<RR> Domus<RR>
where
    RR: RouteResolver,
{
    pub async fn send<Codec: MessageCodec>(
        &self,
        codec: &Codec,
        taberna_id: TabernaId,
        message: &Codec::AppMessage,
        options: SendOptions,
    ) -> Result<SendOutcome, AureliaError>;

    pub async fn taberna<Codec: MessageCodec>(
        &self,
        id: TabernaId,
        codec: Codec,
    ) -> Result<Taberna<Codec>, AureliaError>;
}
```

#### Required Public API (Inbound Taberna)

```rust
pub struct TabernaRequest<Codec: MessageCodec> {
    pub message: Codec::AppMessage,
    pub blob_receiver: Option<BlobReceiver>,
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
}

pub struct Taberna<Codec: MessageCodec> {
    pub async fn next(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> Result<TabernaRequest<Codec>, AureliaError>;
}
```

#### Required Stream Handles

- `BlobSender`: implements `tokio::io::AsyncWrite + Unpin + Send`.
- `BlobReceiver`: implements `tokio::io::AsyncRead + Unpin + Send`.
- Clean shutdown maps to the final chunk flagged `LAST_CHUNK`. If the final read is shorter than `chunk_size`, the partial chunk is the last chunk. No empty chunk is required unless the total transfer length is zero.

#### Required Acceptance Semantics

- A1 delivers each inbound message as a `TabernaRequest`.
- If `TabernaRequest::accept()` is called and `blob_receiver` is `Some`, A1 establishes the blob stream and feeds data into the receiver.
- If `TabernaRequest::reject()` is called, A1 rejects the request and no blob stream is established.
- `TabernaRequest::into_parts()` safely splits the application payload from the completion guard
  without cloning the payload. A3 can complete split requests only with accept or reject.
- A `TabernaCompletion` is a single-use guard. Dropping it without calling `accept` or `reject`
  resolves the inbound request as `remote-taberna-rejected`.


### Integration Boundaries

- **Routing resolver:** A1 resolves remote peers using the shared `aurelia-data` routing contract
  and expects `DomusAddr`.
- **Codec boundary:** A1 remains transport-only; typed codecs live above A1 and are referenced via `codec-integration.md`.

### Domus Address

Routing and dispatch use the `aurelia-data` `DomusAddr`, which matches the local Domus transport
type.

```rust
pub enum DomusAddr {
    Tcp(std::net::SocketAddr),
    Socket(std::path::PathBuf),
}
```

`DomusAddr` encodes transport identity only; it carries no authentication material. TLS or socket auth
configuration is provided separately and cannot be derived from the address.

The resolver must return the address type associated with the active transport (TCP or socket). Returning a
mismatched address type is an error and must fail resolution.

### Domus Service Encapsulation

Higher layers interact with a Domus instance that encapsulates peering, configuration, and taberna
registration. The Domus is the service boundary for A1 and should be extended as new capabilities are added.
Domus and DomusBuilder are the only public API surface for peering; transport internals (for example,
`Transport` and internal routing helpers) are not exported.

```rust
pub struct Pkcs8PrivateKey {
    // zeroizing private-key bytes after ownership transfer; Debug is redacted.
}

impl From<Vec<u8>> for Pkcs8PrivateKey;
impl From<zeroize::Zeroizing<Vec<u8>>> for Pkcs8PrivateKey;

pub struct Pkcs8DerConfig {
    pub ca_der: Vec<u8>,
    pub cert_der: Vec<u8>,
    pub pkcs8_key_der: Pkcs8PrivateKey,
}

pub struct Pkcs8PemConfig {
    pub ca_pem: Vec<u8>,
    pub cert_pem: Vec<u8>,
    pub pkcs8_key_pem: Pkcs8PrivateKey,
}

pub enum Pkcs8AuthConfig {
    Pkcs8Der(Pkcs8DerConfig),
    Pkcs8Pem(Pkcs8PemConfig),
}

pub struct DomusBuilder<RR>
where
    RR: RouteResolver,
{
    // fields are private; use the builder methods below.
}

impl<RR> DomusBuilder<RR>
where
    RR: RouteResolver,
{
    pub fn new(
        config: DomusConfig,
        local_addr: DomusAddr,
        auth: Pkcs8AuthConfig,
        resolver: Arc<RR>,
    ) -> Self;
    pub async fn build(self) -> Result<Domus<RR>, AureliaError>;
    pub async fn build_with_reporting(self)
        -> Result<(Domus<RR>, DomusReportingFeeds), AureliaError>;
}

pub struct Domus<RR>
where
    RR: RouteResolver,
{
    config: DomusConfigAccess,
    // peering + transport internals omitted from the public contract.
}
```

Domus operations for taberna registration:

```rust
impl<RR> Domus<RR>
where
    RR: RouteResolver,
{
    pub fn config(&self) -> DomusConfigAccess;
    pub fn local_addr(&self) -> DomusAddr;
    pub fn reporting(&self) -> DomusReporting;
    pub async fn taberna<Codec: MessageCodec>(
        &self,
        id: TabernaId,
        codec: Codec,
    ) -> Result<Taberna<Codec>, AureliaError>;
    pub async fn send<Codec: MessageCodec>(
        &self,
        codec: &Codec,
        taberna_id: TabernaId,
        message: &Codec::AppMessage,
        options: SendOptions,
    ) -> Result<SendOutcome, AureliaError>;
    pub async fn reload_auth(&self, auth: Pkcs8AuthConfig) -> Result<(), AureliaError>;
    pub async fn shutdown(&self);
}
```

### Domus Reporting

`DomusReporting` exposes always-on observability for metrics, peer identity queries, and real-time
event/error feeds. `build_with_reporting` returns pre-subscribed feeds for initialization events;
`reporting()` can be used to subscribe at any time. Snapshot and query methods return
`AureliaError` with `ErrorId::SnapshotNotAvailable` if the observability task is unavailable.

`DomusConfigAccess` is a lightweight handle that wraps the internal config store and exposes only
`snapshot` and `update` for in-flight configuration changes. `update` returns the applied config
or a validation error.

Domus derives the transport backend once at build time from the local `DomusAddr` and does not allow
runtime switching. When the local address is `DomusAddr::Socket`, all resolver output must be
`DomusAddr::Socket`. When the local address is `DomusAddr::Tcp`, all resolver output must be
`DomusAddr::Tcp`.

Auth material is transport-agnostic. `Pkcs8AuthConfig` accepts DER or PEM inputs; both TCP and
socket transports consume the same PKCS#8 material. Implementation details of the TLS stack are
not part of the public API surface.
Private-key fields are held in zeroizing memory at the public API boundary. Secret-bearing auth
types must not derive `Debug` or `Clone`; `Debug` output is redacted and must not include private
key bytes. Zeroization starts once key bytes are owned by `Pkcs8PrivateKey`; any plain buffers or
copies held before construction remain the caller's responsibility. Callers that already manage
key bytes in `zeroize::Zeroizing<Vec<u8>>` can convert that wrapper directly into
`Pkcs8PrivateKey` without first unwrapping it into a plain `Vec<u8>`.

### Configuration Surface

Configuration is provided via the peering configuration interfaces. Semantics are defined in the authoritative documents listed below.

- `send_queue_size`, `accept_timeout`, `taberna_accept_queue_size`: see `docs/peering/backpressure-queues.md`.
  Primary dispatch uses `send_queue_size` as the A3 retained capacity and derives A1 retained
  capacities from it.
- `send_timeout`, `callis_connect_timeout`, `keepalive_interval`, `listener_delay`, `listener_reconnect_timeout`, `reconnect_backoff`, `socket_callback_timeout`, `socket_handshake_timeout`, `tcp_callback_timeout`, `tcp_handshake_timeout`: see `docs/peering/transport-model.md`.
- `inbound_handshake_limit_total`, `inbound_handshake_limit_per_peer`, `max_parallel_callis_per_peer`: see `docs/peering/transport-model.md` and `docs/peering/backpressure-queues.md`.
- `blob_window`, `blob_outbound_buffer_bytes`, `blob_inbound_buffer_bytes`: see `docs/peering/blobs.md`.
- `max_payload_len`: see `docs/peering/wire-protocol.md`.

### Configuration Limits and Validation

`DomusConfigBuilder::build()`, `DomusBuilder::build()`, `DomusBuilder::build_with_reporting()`,
and `DomusConfigAccess::update()` validate limits and return fail-fast errors for invalid direct
`DomusConfig` values. All limits below are enforced as fail-fast errors except for blob buffer
caps, which are clamped. Validation failures return `AureliaError` with
`ErrorId::InvalidConfig`.

| Setting | Min | Max | Notes |
| --- | --- | --- | --- |
| `send_queue_size` | `1` | `4096` | Per-peer A3 retained outbound capacity; A1 retained capacities are derived from it. |
| `taberna_accept_queue_size` | `1` | `1024` | Per-taberna accept queue length. |
| `max_payload_len` | `1` | `64 MiB` | Wire payload maximum. |
| `inbound_handshake_limit_total` | `1` | `1024` | A0 inbound handshake limit. |
| `inbound_handshake_limit_per_peer` | `1` | `64` | Must be `<= inbound_handshake_limit_total`. |
| `max_parallel_callis_per_peer` | `1` | `64` | Active callis per peer. |
| `blob_window.chunk_size` | `1` | `1 MiB` | Blob callis chunk size; set together with `blob_window.ack_window` through `DomusConfigBuilder::blob_window(chunk_size, ack_window)`. |
| `blob_window.ack_window` | `1` | `4096` | Blob in-flight chunk window; set together with `blob_window.chunk_size`. |
| `blob_outbound_buffer_bytes` | `blob_window.chunk_size * blob_window.ack_window` | `min(8 GiB, 50% system RAM)` | Clamped with a warning. |
| `blob_inbound_buffer_bytes` | `blob_window.chunk_size * blob_window.ack_window` | `min(8 GiB, 50% system RAM)` | Clamped with a warning. |
| `send_timeout` | `> 0` | `5 minutes` | End-to-end send bound. |
| `callis_connect_timeout` | `> 0` | `5 minutes` | End-to-end primary/blob callis setup bound; must be `<= send_timeout`. |
| `accept_timeout` | `> 0` | `1 minute` | Must be `<= send_timeout`. |
| `keepalive_interval` | `0` | `5 minutes` | Transport keepalive interval. |
| `listener_delay` | `0` | `1 minute` | Initial listener delay. |
| `listener_reconnect_timeout` | `0` | `5 minutes` | Listener reconnection wait. |
| `socket_callback_timeout` | `0` | `1 minute` | Socket A0 callback timeout. |
| `socket_handshake_timeout` | `0` | `2 minutes` | Socket A0 handshake timeout. |
| `tcp_callback_timeout` | `0` | `2 minutes` | TCP A0 callback timeout. |
| `tcp_handshake_timeout` | `0` | `5 minutes` | TCP A0 handshake timeout. |
| `reconnect_backoff` | `0 entries` | `16 entries` | Each entry must be `<= 5 minutes`. |

Socket authentication timeouts are configured through the Domus public interface using `DomusConfig` and
`DomusConfigBuilder` (builder methods named `socket_callback_timeout` and `socket_handshake_timeout`):

- `socket_callback_timeout` (default 2 seconds): connect-back timeout during the first primary handshake.
- `socket_handshake_timeout` (default 5 seconds): total socket A0 connect-back handshake timeout.

TCP authentication timeouts are configured through the Domus public interface using `DomusConfig` and
`DomusConfigBuilder` (builder methods named `tcp_callback_timeout` and `tcp_handshake_timeout`):

- `tcp_callback_timeout` (default 10 seconds): connect-back timeout during the TCP A0 handshake.
- `tcp_handshake_timeout` (default 10 seconds): total TCP A0 handshake timeout.

Callis setup is configured through `DomusConfig::callis_connect_timeout` and
`DomusConfigBuilder::callis_connect_timeout`:

- `callis_connect_timeout` (default 15 seconds): total callis setup timeout from A1 connect/reconnect
  trigger through authenticated A0 transport setup and successful A1 `hello` / `hello-response`.

### Error Surface

- Error IDs and taxonomy: `docs/ids.md`.
- Error payload formats: `docs/peering/wire-protocol.md`.

### Logging Requirements

- Logging levels and repository-wide requirements are defined in `docs/aurelia.md`.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
