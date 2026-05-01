# Socket Transport

Status: Developed

## Objectives

- Provide a UNIX socket transport backend that is transparent to A1 once authenticated.
- Perform peer authentication in A0 (below A1) with a dedicated socket auth handshake (no A1 wire protocol changes).
- Preserve domus identity semantics using canonicalized socket paths and certificate SANs.

## Technical Details

### Transport Backend Boundary

The peering transport depends on a backend that yields authenticated, bidirectional streams. The backend is responsible for binding, accepting, dialing, and validating peer identity. A1 only sees a stream plus the authenticated domus address.

The socket transport is intended for communication between modules within the same process or
within the same host trust boundary. Socket paths are expected to be protected by filesystem
permissions so untrusted writers cannot create or replace the socket file. The peering layer does
not attempt to manage concurrent process lifecycles; if a socket path is replaced between removal
and bind, the bind fails and the caller must handle the startup error.

A0 connection limits for inbound callis are defined in `docs/peering/connection-limits.md` and are enforced before any A1 `hello`.

Inbound sockets are counted as in-flight handshakes from accept until the socket auth handshake
completes, fails, or times out. If the global A0 limit is reached, the socket is closed immediately
before any auth frames are read.

```rust
#[async_trait::async_trait]
pub trait TransportBackend: Send + Sync {
    type Addr: Clone + Eq + std::hash::Hash + std::fmt::Display + Send + Sync;
    type Listener: Send + 'static;
    type Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static;

    async fn bind(&self, local: Self::Addr) -> Result<Self::Listener, AureliaError>;
    async fn accept(&self, listener: &mut Self::Listener)
        -> Result<(Self::Stream, Self::Addr), AureliaError>; // authenticated
    async fn dial(&self, peer: &Self::Addr)
        -> Result<(Self::Stream, Self::Addr), AureliaError>; // authenticated
}
```

The shared A1 transport logic uses `Stream` only; no transport-specific types leak upward.

Each Aurelia instance derives a single transport backend from the local `DomusAddr` used to create it.
Backend selection happens once at instantiation and is immutable for the lifetime of the Domus. When the
local address is `DomusAddr::Socket`, all resolver output must be `DomusAddr::Socket`, and any TCP
address is rejected.

Auth material is supplied via `DomusAuthConfig::Pkcs8` (DER or PEM). PEM inputs are parsed into DER internally
before use using `rustls-pki-types`; the on-wire socket auth handshake always transmits DER certificate bytes.

### Domus Address

Domus identity is the transport address itself.

```rust
pub enum DomusAddr {
    Tcp(std::net::SocketAddr),
    Socket(std::path::PathBuf), // absolute, canonicalized
}
```

The backend must return the authenticated `DomusAddr`. The resolver, peer maps, and dispatch surfaces use the same type.

### Socket Authentication Handshake (A0, Connect-Back)

Socket authentication occurs before any A1 `hello` frames. It is a dedicated, length-prefixed handshake that validates
peer identity and returns a verified `DomusAddr::Socket`.

The connect-back handshake is required for the first primary callis to a peer (when no live peer
session exists). Additional primary callis and blob callis on a live peer session use the
simplified `AUTH_RESUME` handshake described below; resume does not bypass certificate validation
and does not require any per-peer pin.

There are two channels during connect-back:

- **Primary channel:** the original initiator -> receiver connection.
- **Callback channel:** a temporary receiver -> initiator connection to the initiator’s claimed socket path.

The handshake succeeds only if both channels complete, and each side plays back the other channel’s nonce as described
below.

#### Framing

Each auth message is framed as:

```text
u32 len
len bytes of payload
```

All multi-byte integers are big-endian.

#### Message Types

```text
u8 msg_type
u8 version
```

Where `version = 1` and:

- `1`: `AUTH_INIT`
- `2`: `AUTH_CHALLENGE`
- `3`: `AUTH_PROOF`
- `4`: `CALLBACK_INIT`
- `5`: `AUTH_RESUME`

In every message, `origin_path` is the sender’s canonical socket path and `destination_path` is the receiver’s canonical
socket path.

Bounds:

- `origin_path_len` and `destination_path_len` must be `> 0` and `<= PATH_MAX` for the host OS.
- `nonce_len`, `callback_nonce_len`, and `echo_nonce_len` must be exactly 32 bytes.
- `session_nonce_len` must be exactly 128 bytes.

#### AUTH_INIT (initiator -> receiver, primary channel)

```text
u8 msg_type = 1
u8 version = 1
u16 origin_path_len
u16 destination_path_len
u32 cert_len
u16 nonce_len
u16 callback_nonce_len
origin_path bytes (UTF-8)
destination_path bytes (UTF-8)
cert bytes (DER)
nonce_a bytes
nonce_a_cb bytes
```

Validation:

- `origin_path` and `destination_path` must be absolute, canonicalized paths.
- The receiver must verify the certificate chain against the configured CA and ensure the URI SAN is
  `aurelia+unix://<origin_path>`.
- `destination_path` must match the receiver’s local socket path.

On success, the receiver **must** open the callback channel to `origin_path` and wait for the callback step to complete
before sending `AUTH_CHALLENGE`. If the connect-back fails or times out, authentication fails and the primary channel is
closed.

#### CALLBACK_INIT (receiver -> initiator, callback channel)

```text
u8 msg_type = 4
u8 version = 1
u16 origin_path_len
u16 destination_path_len
u16 callback_nonce_len
u16 echo_nonce_len
origin_path bytes (UTF-8)
destination_path bytes (UTF-8)
nonce_b_cb bytes
echo_nonce_a_cb bytes
```

The receiver generates `nonce_b_cb` for the callback channel and must echo `nonce_a_cb` from `AUTH_INIT` as
`echo_nonce_a_cb`.

Validation (initiator):

- Ensure `echo_nonce_a_cb == nonce_a_cb` from `AUTH_INIT`.
- Ensure `destination_path` matches the initiator’s local socket path.
After validation, the initiator stores `nonce_b_cb` and closes the callback channel. The callback is only accepted if the
primary channel `AUTH_CHALLENGE` later verifies the receiver’s certificate; otherwise the stored nonce is discarded.

#### AUTH_CHALLENGE (receiver -> initiator, primary channel)

```text
u8 msg_type = 2
u8 version = 1
u16 origin_path_len
u16 destination_path_len
u32 cert_len
u16 nonce_len
u32 signature_len
origin_path bytes (UTF-8)
destination_path bytes (UTF-8)
cert bytes (DER)
nonce_b bytes
signature bytes
```

The signature is over:

```text
nonce_a || nonce_b || u16(origin_path_len) || u16(destination_path_len) || origin_path || destination_path
```

Validation:

- Initiator verifies receiver cert chain and URI SAN `aurelia+unix://<origin_path>`.
- Initiator verifies signature using the receiver certificate.
- Initiator must already have received `CALLBACK_INIT` to obtain `nonce_b_cb` for replay on the primary channel.

#### AUTH_PROOF (initiator -> receiver, primary channel)

```text
u8 msg_type = 3
u8 version = 1
u16 echo_nonce_len
u32 signature_len
echo_nonce_b_cb bytes
signature bytes
```

The signature is over:

```text
nonce_b || nonce_a || nonce_b_cb || u16(origin_path_len) || u16(destination_path_len) || origin_path || destination_path
```

Receiver verifies signature using the initiator certificate.

Validation:

- Ensure `echo_nonce_b_cb == nonce_b_cb` from `CALLBACK_INIT`.

#### Completion

On success, both sides treat the connection as authenticated and return the peer identity as the
canonicalized socket path. A1 `hello`/`hello-response` begins only after this handshake completes.

#### Ordering and Timing

The connect-back handshake is strictly sequential:

1. `AUTH_INIT` (primary)
2. `CALLBACK_INIT` (callback)
3. `AUTH_CHALLENGE` (primary)
4. `AUTH_PROOF` (primary)

The callback connect timeout is configurable as `socket_callback_timeout` with a default of 2 seconds. There are no
retries; if the callback fails or times out, the primary handshake fails and the connection is closed.

The overall A0 handshake timeout is configurable as `socket_handshake_timeout` with a default of 5 seconds. The
initiator starts this timer when it sends the first auth frame (`AUTH_INIT` for connect-back, `AUTH_RESUME` for resume).
The receiver starts its timer when it receives the first auth frame. The handshake must complete within that window on
both sides or the connection is closed.

### Resume Handshake

A1 does not pin per-peer certificates or session nonces. Each callis is admitted on its own A0
authentication, which is sufficient because the certificate chain validation against the
configured CA establishes peer identity on every connection. Different valid certificates from the
same peer path are accepted across callis (smooth rotation).

#### AUTH_RESUME (initiator -> receiver, additional connections)

Additional primary callis (parallel primaries) and all blob callis use a single-step resume
handshake once the peer session is live.

```text
u8 msg_type = 5
u8 version = 1
u16 origin_path_len
u16 destination_path_len
u32 cert_len
u16 session_nonce_len
u32 signature_len
origin_path bytes (UTF-8)
destination_path bytes (UTF-8)
cert bytes (DER)
session_nonce bytes
signature bytes
```

The signature is over:

```text
session_nonce || u16(origin_path_len) || u16(destination_path_len) || origin_path || destination_path
```

Validation:

- The receiver must already have a live peer session for the originating path; otherwise the
  connection is rejected (full connect-back is required for the first primary callis, and blob
  callis are not allowed without an active primary).
- `origin_path` and `destination_path` must be canonicalized and the `destination_path` must match the local socket path.
- The certificate must verify against the configured CA and present the expected URI SAN.
- The signature must verify with the provided certificate.

### Canonicalization

The backend must canonicalize any configured or resolved socket path before use and reject any
non-absolute or non-canonical paths. All handshake validation uses canonicalized paths only.

### Failure Behavior

Any validation failure closes the socket without emitting A1 `hello`/`error` frames. If the connect-back fails or times
out, authentication fails and the primary channel is closed. If the A0 handshake exceeds
`socket_handshake_timeout`, the connection is closed. The transport layer surfaces a `PeerUnavailable` or
`ProtocolViolation` error to A1 depending on the failure class.

### Callis Semantics

The socket backend mirrors TCP:

- Primary and blob callis are separate connections to the same socket path.
- Callis type is determined by the A1 `hello` flags (specifically `BLOB`).
- The connect-back handshake is required for the first primary callis to a peer (when no live
  peer session exists).
- Additional primary callis and blob callis on a live peer session use `AUTH_RESUME`.

### Auth Reload (Smooth Rotation)

`Transport::reload_auth` swaps the backend's auth material atomically. Existing socket sessions are
unaffected; the next outbound dial and the next inbound accept use the new material. There is no
per-peer breaker, no forced disconnect, and no pin to release. A peer presenting a different
(validly authenticated) certificate on a subsequent callis is accepted at the same socket path.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
