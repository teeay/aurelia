# Peering Services Provided

Status: Developed

## Objectives

- Define the services and contracts the peering crate provides to higher layers.
- Describe integration boundaries (routing, codecs) for A1.
- Enumerate the configuration surface and point to authoritative semantics.

## Technical Details

### Aurelia Wrapper Entry Point

- Public access to Domus must be provided through the `aurelia` crate wrapper, not by constructing peering types directly.
- The Aurelia wrapper should be thin and prefer re-exported types so the compiler can optimize away indirection.
- The Aurelia runtime handle is internal to the library and must not be exposed in the public API.
- Peering documentation defines the Domus surface but assumes the entry point is the Aurelia wrapper.

### Service Surface (A1)

Peering provides the following services to higher layers:

- **Message delivery:** Send a message to a remote taberna and complete only after remote ingress ACK.
- **Blob transfer initiation:** Send a blob request on the primary callis and coordinate the blob callis stream lifecycle.
- **Blob streaming:** Stream blob chunks through `BlobSender` and `BlobReceiver` with ordered delivery and explicit completion.
- **Internal taberna registry:** Domus owns a single concrete taberna registry; there is no registry injection and no registry exposure in the public API.

### Domus Typed Send + Blob Handles

- **Typed `send` only:** Domus exposes a single method named `send`. It accepts a codec and an app message, encodes it into `(msg_type, payload)`, and uses A1 for delivery. There is no `send_typed` or `send_blob` in the public API.
- **No untyped send:** All untyped send entry points are removed. `send` is the only public API for outbound delivery.
- **Blob flag on send:** The caller requests a blob transfer by setting a flag in `SendOptions` passed to `send`.
- **No blob length parameter:** `SendOptions` contains only the blob flag; reservations are derived solely from negotiated chunk size and ack window.
- **Sender handle:** If the blob request is accepted, the sender receives a `BlobSender` stream sink for writing the blob bytes.
- **Receiver handle on accept:** The inbound taberna receives an optional `BlobReceiver` alongside the typed message in the same accept call. If the taberna rejects the message, the `BlobReceiver` is discarded and no blob stream is established.
- **Optional means empty:** When the request is not a blob, `blob_receiver` is `None` and no stream is created.
- **Single taberna implementation:** Domus provides a single `Taberna` implementation per `TabernaId`. `Domus::taberna` returns the `Taberna` handle instead of accepting a sink parameter.
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

#### Required Stream Handles

- `BlobSender`: implements `tokio::io::AsyncWrite + Unpin + Send`.
- `BlobReceiver`: implements `tokio::io::AsyncRead + Unpin + Send`.
- Clean shutdown maps to the final chunk flagged `LAST_CHUNK`. If the final read is shorter than `chunk_size`, the partial chunk is the last chunk. No empty chunk is required unless the total transfer length is zero.

#### Required Acceptance Semantics

- A1 delivers each inbound message as a `TabernaRequest`.
- If `TabernaRequest::accept()` is called and `blob_receiver` is `Some`, A1 establishes the blob stream and feeds data into the receiver.
- If `TabernaRequest::reject()` is called, A1 rejects the request and no blob stream is established.


### Integration Boundaries

- **Routing resolver:** A1 resolves remote peers using the routing resolver contract and expects `DomusAddr`.
- **Codec boundary:** A1 remains transport-only; typed codecs live above A1 and are referenced via `codec-integration.md`.

### Domus Address

Routing and dispatch use a Domus address that matches the local Domus transport type.

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
`Transport` and `RouteLocalRemoteBuilder`) are not exported.

```rust
pub struct Pkcs8DerConfig {
    pub ca_der: Vec<u8>,
    pub cert_der: Vec<u8>,
    pub pkcs8_key_der: Vec<u8>,
}

pub struct Pkcs8PemConfig {
    pub ca_pem: Vec<u8>,
    pub cert_pem: Vec<u8>,
    pub pkcs8_key_pem: Vec<u8>,
}

pub enum Pkcs8AuthConfig {
    Pkcs8Der(Pkcs8DerConfig),
    Pkcs8Pem(Pkcs8PemConfig),
}

pub enum DomusAuthConfig {
    Pkcs8(Pkcs8AuthConfig),
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
        auth: DomusAuthConfig,
        resolver: Arc<RR>,
        runtime_handle: tokio::runtime::Handle,
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
    pub async fn reload_auth(&self, auth: DomusAuthConfig) -> Result<(), AureliaError>;
    pub async fn shutdown(&self);
}
```

### Domus Reporting

`DomusReporting` exposes always-on observability for metrics, peer identity queries, and real-time
event/error feeds. `build_with_reporting` returns pre-subscribed feeds for initialization events;
`reporting()` can be used to subscribe at any time.

`DomusConfigAccess` is a lightweight handle that wraps the internal config store and exposes only
`snapshot` and `update` for in-flight configuration changes. `update` returns the applied config
or a validation error.

Domus derives the transport backend once at build time from the local `DomusAddr` and does not allow
runtime switching. When the local address is `DomusAddr::Socket`, all resolver output must be
`DomusAddr::Socket`. When the local address is `DomusAddr::Tcp`, all resolver output must be
`DomusAddr::Tcp`.

Auth material is transport-agnostic. `DomusAuthConfig` currently exposes a single option, `Pkcs8`,
with DER or PEM inputs. Both TCP and socket transports accept the same PKCS8 material. Implementation
details of the TLS stack are not part of the public API surface.

### Configuration Surface

Configuration is provided via the peering configuration interfaces. Semantics are defined in the authoritative documents listed below.

- `send_queue_size`, `inflight_window`, `accept_timeout`, `taberna_accept_queue_size`: see `docs/peering/backpressure-queues.md`.
- `send_timeout`, `keepalive_interval`, `listener_delay`, `listener_reconnect_timeout`, `reconnect_backoff`, `socket_callback_timeout`, `socket_handshake_timeout`, `tcp_callback_timeout`, `tcp_handshake_timeout`: see `docs/peering/transport-model.md`.
- `inbound_handshake_limit_total`, `inbound_handshake_limit_per_peer`, `max_parallel_callis_per_peer`: see `docs/peering/transport-model.md` and `docs/peering/backpressure-queues.md`.
- `blob_chunk_size`, `blob_ack_window`, `blob_outbound_buffer_bytes`, `blob_inbound_buffer_bytes`: see `docs/peering/blobs.md`.
- `max_payload_len`: see `docs/peering/wire-protocol.md`.
- `limited_log_interval`: see `docs/logging.md`.

### Configuration Limits and Validation

`DomusConfigBuilder::build()` validates limits and returns `Result<DomusConfig, AureliaError>`.
All limits below are enforced as fail-fast errors except for blob buffer caps, which are clamped.
Validation failures return `AureliaError` with `ErrorId::InvalidConfig`.

| Setting | Min | Max | Notes |
| --- | --- | --- | --- |
| `send_queue_size` | `1` | `4096` | Per-peer outbound queue length. |
| `inflight_window` | `1` | `1024` | Per-peer unacked window size. |
| `taberna_accept_queue_size` | `1` | `1024` | Per-taberna accept queue length. |
| `max_payload_len` | `1` | `64 MiB` | Wire payload maximum. |
| `inbound_handshake_limit_total` | `1` | `1024` | A0 inbound handshake limit. |
| `inbound_handshake_limit_per_peer` | `1` | `64` | Must be `<= inbound_handshake_limit_total`. |
| `max_parallel_callis_per_peer` | `1` | `64` | Active callis per peer. |
| `blob_chunk_size` | `1` | `1 MiB` | Blob callis chunk size. |
| `blob_ack_window` | `1` | `4096` | Blob in-flight chunk window. |
| `blob_outbound_buffer_bytes` | `blob_chunk_size * blob_ack_window` | `min(8 GiB, 50% system RAM)` | Clamped with a warning. |
| `blob_inbound_buffer_bytes` | `blob_chunk_size * blob_ack_window` | `min(8 GiB, 50% system RAM)` | Clamped with a warning. |
| `send_timeout` | `> 0` | `5 minutes` | End-to-end send bound. |
| `accept_timeout` | `> 0` | `1 minute` | Must be `<= send_timeout`. |
| `keepalive_interval` | `0` | `5 minutes` | Transport keepalive interval. |
| `listener_delay` | `0` | `1 minute` | Initial listener delay. |
| `listener_reconnect_timeout` | `0` | `5 minutes` | Listener reconnection wait. |
| `socket_callback_timeout` | `0` | `1 minute` | Socket A0 callback timeout. |
| `socket_handshake_timeout` | `0` | `2 minutes` | Socket A0 handshake timeout. |
| `tcp_callback_timeout` | `0` | `2 minutes` | TCP A0 callback timeout. |
| `tcp_handshake_timeout` | `0` | `5 minutes` | TCP A0 handshake timeout. |
| `limited_log_interval` | `0` | `1 hour` | Limited logging suppression interval. |
| `reconnect_backoff` | `0 entries` | `16 entries` | Each entry must be `<= 5 minutes`. |

Socket authentication timeouts are configured through the Domus public interface using `DomusConfig` and
`DomusConfigBuilder` (builder methods named `socket_callback_timeout` and `socket_handshake_timeout`):

- `socket_callback_timeout` (default 2 seconds): connect-back timeout during the first primary handshake.
- `socket_handshake_timeout` (default 5 seconds): total A0 handshake timeout (connect-back or resume).

TCP authentication timeouts are configured through the Domus public interface using `DomusConfig` and
`DomusConfigBuilder` (builder methods named `tcp_callback_timeout` and `tcp_handshake_timeout`):

- `tcp_callback_timeout` (default 10 seconds): connect-back timeout during the TCP A0 handshake.
- `tcp_handshake_timeout` (default 20 seconds): total TCP A0 handshake timeout.

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
