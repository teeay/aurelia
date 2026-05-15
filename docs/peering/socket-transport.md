# Socket Transport

Status: Developed

## Objectives

- Provide a UNIX socket transport backend that is transparent to A1 once authenticated.
- Perform certificate-backed authentication in A0 (below A1) with a dedicated socket auth
  handshake (no A1 wire protocol changes).
- Preserve domus identity semantics using canonicalized socket paths and certificate SANs.

## Technical Details

### Per-Callis A0 Authentication

Every socket callis uses the full connect-back authentication handshake before A1 `hello`.

The socket A0 message set must be:

```text
1: AUTH_INIT
2: AUTH_CHALLENGE
3: AUTH_PROOF
4: CALLBACK_INIT
```

This includes the first primary callis, any second or third primary callis opened by tests, and
all blob callis. The backend must validate canonical origin/destination paths, certificate chain,
URI SAN, callback nonce echo, challenge signature, and proof signature for each callis
independently.

Any message type outside this set has no compatibility meaning. If an unexpected message type
appears as the first A0 message on an inbound socket, the backend rejects it as
`ProtocolViolation`.

Tests open repeated socket callis through real backend `dial()` and `accept()` calls. They prove
first, second, and third callis all complete full connect-back and return
`DomusAddr::Socket(origin_path)`. Existing callback-before-wait, callback-after-wait, callback
timeout, nonce/path/certificate mismatch, stale callback, simultaneous connect-back, and smooth
auth rotation coverage remains valid.

### Transport Backend Boundary

The peering transport depends on a backend that yields authenticated, bidirectional streams. The backend is responsible for binding, accepting, dialing, and validating peer identity. A1 only sees a stream plus the authenticated domus address.

The socket backend contributes the socket variant to the production backend and stream enums
defined in `docs/peering/transport-model.md`. Its authenticated stream type is `UnixStream`, and
A1 callis logic receives it through the `TransportBackend::Stream` associated type.

The socket transport is intended for communication between modules within the same process or
within the same host trust boundary. Socket paths are expected to be protected by filesystem
permissions so untrusted writers cannot create or replace the socket file. Identity is still
validated with PKCS#8 certificate material and socket-path URI SANs. The peering layer does not
attempt to manage concurrent process lifecycles; if a socket path is replaced between removal and
bind, the bind fails and the caller must handle the startup error.

A0 connection limits for inbound callis are defined in `docs/peering/connection-limits.md` and are enforced before any A1 `hello`.

Inbound sockets are counted as in-flight handshakes from accept until the socket auth handshake
completes, fails, or times out. If the global A0 limit is reached, the socket is closed immediately
before any auth frames are read.

`SocketBackend::accept` must keep accepting raw Unix sockets while earlier sockets are still in
A0 authentication. Each accepted socket is handed to an authentication task spawned through the
Aurelia runtime handle. The task owns the pre-authentication permit, applies
`socket_handshake_timeout` to the full A0 path, and sends authenticated streams or errors back to
the backend accept queue. Callback-channel messages that complete socket rendezvous state do not
return from the backend `accept` call.

Socket A0 authentication tasks are detached from a single `accept()` await. If an `accept()` caller
is cancelled after raw sockets have been accepted, already spawned authentication tasks continue
until success, failure, timeout, or backend drop. The backend accept queue capacity is the maximum
validated `inbound_handshake_limit_total`.

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

Each Aurelia instance derives a single transport backend from the shared `aurelia-data` `DomusAddr`
used to create it.
Backend selection happens once at instantiation and is immutable for the lifetime of the Domus. When the
local address is `DomusAddr::Socket`, all resolver output must be `DomusAddr::Socket`, and any TCP
address is rejected.

Auth material is supplied via `Pkcs8AuthConfig` (DER or PEM). PEM inputs are parsed into DER
internally before use using `rustls-pki-types`; the on-wire socket auth handshake transmits DER
certificate chains. DER config supplies a one-certificate chain. PEM config preserves every
certificate parsed from `cert_pem` in chain order.

### Domus Address

Domus identity is the transport address itself.

```rust
pub enum DomusAddr {
    Tcp(std::net::SocketAddr),
    Socket(std::path::PathBuf), // absolute, canonicalized
}
```

The backend must return the authenticated `DomusAddr`. The resolver, peer maps, and dispatch
surfaces use the same shared data type.

### Socket Authentication Handshake (A0, Connect-Back)

Socket authentication occurs before any A1 `hello` frames. It is a dedicated, length-prefixed handshake that validates
peer identity and returns a verified `DomusAddr::Socket`.

The connect-back handshake is required for every socket callis. This includes the first primary
callis, additional primary callis opened by tests, and blob callis.

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

In every message, `origin_path` is the sender’s canonical socket path and `destination_path` is the receiver’s canonical
socket path.

Bounds:

- `origin_path_len` and `destination_path_len` must be `> 0` and `<= PATH_MAX` for the host OS.
- `nonce_len`, `callback_nonce_len`, and `echo_nonce_len` must be exactly 32 bytes.

#### AUTH_INIT (initiator -> receiver, primary channel)

```text
u8 msg_type = 1
u8 version = 1
u16 origin_path_len
u16 destination_path_len
u16 cert_count
repeated cert_count times:
  u32 cert_len
u16 nonce_len
u16 callback_nonce_len
origin_path bytes (UTF-8)
destination_path bytes (UTF-8)
cert bytes (DER), in chain order
nonce_a bytes
nonce_a_cb bytes
```

Validation:

- `origin_path` and `destination_path` must be absolute, canonicalized paths.
- The receiver must verify the leaf certificate against the configured CA using the remaining
  presented certificates as intermediates and ensure the leaf URI SAN is
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
u16 cert_count
repeated cert_count times:
  u32 cert_len
u16 nonce_len
u32 signature_len
origin_path bytes (UTF-8)
destination_path bytes (UTF-8)
cert bytes (DER), in chain order
nonce_b bytes
signature bytes
```

The signature is over:

```text
nonce_a || nonce_b || u16(origin_path_len) || u16(destination_path_len) || origin_path || destination_path
```

Validation:

- Initiator verifies receiver cert chain and URI SAN `aurelia+unix://<origin_path>` on the leaf.
- Initiator verifies signature using the receiver leaf certificate public key.
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

Receiver verifies signature using the initiator leaf certificate public key.

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
initiator starts this timer when the backend begins socket connect for the callis. The receiver starts this timer
immediately after accepting the socket and acquiring the inbound pre-authentication permit. The timeout covers the
first auth-frame read, certificate validation, callback connection, callback validation, challenge/proof validation,
and proof validation. The handshake must complete within that window on both sides or the connection is closed.
The A1 `callis_connect_timeout` is the outer callis setup budget and can expire first.

### Certificate Chains and URI SANs

Every socket auth message that presents a certificate presents the full configured certificate
chain in leaf-first order. Verification uses the first certificate as the leaf and passes the
remaining certificates to WebPKI as intermediates. A missing intermediate in a multi-tier CA
hierarchy is a `ProtocolViolation`.

The leaf certificate must contain an Aurelia URI SAN of the form
`aurelia+unix://<canonical-path>`. Multiple Aurelia URI SAN entries are accepted only when every
entry canonicalizes to the same path. Conflicting Aurelia URI SAN entries are a
`ProtocolViolation`.

Socket proof signatures bind identity through certificate validation and public-key ownership:
the verifier first validates the certificate chain and URI SAN, then verifies the proof signature
with the leaf certificate public key. The signed challenge bytes do not include a certificate
hash.

RSA socket proof signatures use RSA-PSS-SHA256. ECDSA P-256 uses ECDSA-P256-SHA256-ASN.1, ECDSA
P-384 uses ECDSA-P384-SHA384-ASN.1, and Ed25519 uses Ed25519.

### Socket Callback Rendezvous State

The socket backend owns a callback rendezvous map for connect-back validation. This map connects
the primary channel's `AUTH_INIT`/`AUTH_CHALLENGE` flow to the callback channel's `CALLBACK_INIT`
flow as a small state machine with latched completion.

Required state transitions:

1. **Pending registered:** the initiator inserts `nonce_a_cb`, expected canonical peer path, and a
   one-shot reply before it waits for the callback.
2. **Callback arrived:** the callback accept path validates message type, version, canonical
   origin/destination paths, `echo_nonce_a_cb`, and peer certificate URI SAN before completing the
   one-shot reply with `nonce_b_cb`.
3. **Primary completed:** after the primary channel validates `AUTH_CHALLENGE` and sends
   `AUTH_PROOF`, the pending entry must already be consumed.
4. **Failure cleanup:** timeout, nonce mismatch, path mismatch, certificate mismatch, callback
   channel close, primary channel close, and task cancellation must remove the pending entry
   exactly once.

Primitive behavior:

- Callback arrival uses a latched primitive such as `oneshot`; it does not depend on a
  non-latching `Notify` to observe callback arrival.
- The pending callback map does not hold its lock while performing socket I/O, filesystem
  canonicalization, certificate validation, callback dial, or waiting for the callback reply.
- A callback that arrives before the primary task begins awaiting the reply still satisfies the
  primary task.
- A callback that arrives after timeout cleanup is rejected and does not recreate state.
- Smooth auth reload does not mutate or invalidate already registered callback rendezvous entries;
  each entry validates against the auth material and canonical peer path captured for that
  handshake.

Testing coverage:

- Unit or backend tests must cover callback-before-wait, callback-after-wait, callback timeout,
  nonce mismatch cleanup, path mismatch cleanup, certificate mismatch cleanup, simultaneous
  connect-back, smooth auth reload during callback, and stale callback rejection after timeout.
- Integration tests must cover socket connect-back success, repeated full-auth callis, removed
  message type rejection, callback path mismatch, callback timeout, and smooth auth rotation.

### Shared Callback Rendezvous Requirements

Socket may share callback rendezvous lifecycle mechanics with the TCP backend, but socket-specific
authentication and canonical path validation must remain owned by the socket transport.

The shared mechanics may cover:

- registration of a pending callback entry before the primary task waits;
- latched completion through a one-shot reply;
- exactly-once removal on success, timeout, cancellation, channel close, or validation failure;
- stale callback rejection after timeout cleanup;
- lock-free waiting, where no rendezvous map lock is held across socket I/O, filesystem
  canonicalization, certificate validation, callback dial, or waiting for the callback reply.

The shared helper must not own:

- socket certificate URI SAN validation;
- expected canonical origin and destination path validation;
- nonce payload parsing or protocol message type/version validation;
- smooth-auth material selection for an individual handshake.

Testing coverage:

- Socket tests must continue to cover callback-before-wait and callback-after-wait against the
  shared lifecycle helper.
- Socket mismatch and timeout tests must prove failed callbacks remove pending state exactly once
  and do not satisfy a later primary handshake.
- Smooth auth reload tests must prove an already registered socket callback validates against the
  handshake-captured material rather than whatever material is current after reload.

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
- The connect-back handshake is required for every primary and blob callis.
- Additional primary callis opened by tests use the same full connect-back handshake.

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
