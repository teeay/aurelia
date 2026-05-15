# TCP Transport

Status: Developed

## Objectives

- Define the TCP/mTLS transport backend boundary for A1.
- Specify the TCP A0 connect-back validation flow.

## Technical Details

### Per-Callis A0 Authentication

Every TCP callis uses full mTLS plus connect-back validation before A1 `hello`.

The TCP A0 message set must be:

```text
1: AUTH_INIT       (nonce_a_cb: 32 bytes)
2: CALLBACK_INIT   (nonce_b_cb: 32 bytes, echo_nonce_a_cb: 32 bytes)
3: AUTH_CHALLENGE  (empty)
4: AUTH_PROOF      (echo_nonce_b_cb: 32 bytes)
```

This includes the first primary callis, any second or third primary callis opened by tests, and all blob callis.
For each callis, the backend must validate TLS server-name/IP SAN, Aurelia TCP URI SAN, callback
connection, callback nonce echo, and connect-back proof independently.

Any message type outside this set has no compatibility meaning. If an unexpected message type
appears as the first TCP A0 message after TLS, the backend rejects it as `ProtocolViolation`.

Tests open repeated TCP callis through real backend `dial()` and `accept()` calls. They prove
first, second, and third callis all complete full mTLS plus connect-back and return
`DomusAddr::Tcp(peer_addr)`. Existing callback-before-wait, callback-after-wait, callback timeout,
nonce/address/certificate mismatch, stale callback, simultaneous connect-back, and smooth auth
rotation coverage remains valid.

### Transport Backend Boundary

The TCP backend performs mTLS authentication and A0 validation before returning an authenticated stream and
`DomusAddr::Tcp` identity to A1. `DomusAddr` is owned by `aurelia-data`; A1 does not interpret
transport-specific types beyond enforcing the active transport kind.

The TCP backend contributes the TCP variant to the production backend and stream enums defined in
`docs/peering/transport-model.md`. Its authenticated stream type is `TlsStream<TcpStream>`, and
A1 callis logic receives it through the `TransportBackend::Stream` associated type.

A0 connection limits for inbound callis are defined in `docs/peering/connection-limits.md` and are enforced before any A1 `hello`.

Inbound TCP streams are counted as in-flight handshakes from accept until TLS and A0 connect-back
validation complete, fail, or time out. If the global A0 limit is reached, the raw TCP stream is
closed immediately before any TLS handshake begins.

### Domus Address

TCP identity is the transport address itself.

```rust
pub enum DomusAddr {
    Tcp(std::net::SocketAddr),
    Socket(std::path::PathBuf),
}
```

### Auth Material (mTLS)

Auth material is supplied via `Pkcs8AuthConfig` (DER or PEM). Both inbound and outbound TCP callis
use full mTLS verification in A0.

Rustls is configured with the `ring` provider. AWS-LC is not part of the Aurelia dependency graph.
`tokio-rustls` manifests must disable default features and opt into the required Rustls features
explicitly, including `ring`, so TLS does not pull in C/native provider builds.

TCP authentication uses two identity checks:

- TLS server-name validation uses the peer IP address and therefore requires a matching IP SAN.
- Aurelia validates a URI SAN of the form `aurelia+tcp://<ip>:<port>` after the TLS handshake.

Both identities must match the expected peer address for the callis. An IP SAN / Aurelia URI SAN
mismatch is a transport identity failure and the connection is closed before A1 `hello`.

### A0 Connect-Back Validation

TCP connections complete a dedicated connect-back validation step before A1 `hello`. The flow is symmetric;
if both peers initiate concurrently, both handshakes may succeed and A1 resolves duplicate callis normally.

Configuration:

- `tcp_callback_timeout` (default 10 seconds): dial timeout for the callback connection.
- `tcp_handshake_timeout` (default 10 seconds): total A0 handshake timeout.

`tcp_handshake_timeout` covers the complete TCP A0 stage for a callis: TCP connect or accepted
raw stream, mTLS, certificate URI SAN validation, callback connection, callback nonce validation,
and connect-back proof validation. The A1 `callis_connect_timeout` is the outer callis setup
budget and can expire first.

Framing:

- There is **no** length prefix and **no** version field.
- The first byte is the message type, which determines the fixed payload length.
- Any unexpected message type or invalid payload length is a `ProtocolViolation`.

Message types (fixed payloads):

```text
1: AUTH_INIT       (nonce_a_cb: 32 bytes)
2: CALLBACK_INIT   (nonce_b_cb: 32 bytes, echo_nonce_a_cb: 32 bytes)
3: AUTH_CHALLENGE  (empty)
4: AUTH_PROOF      (echo_nonce_b_cb: 32 bytes)
```

Primary channel (initiator -> receiver):

1. Initiator sends `AUTH_INIT(nonce_a_cb)`.
2. Receiver opens a callback connection to the initiator’s claimed TCP address and sends
   `CALLBACK_INIT(nonce_b_cb, echo_nonce_a_cb)`.
3. Receiver sends `AUTH_CHALLENGE` on the primary channel.
4. Initiator replies with `AUTH_PROOF(echo_nonce_b_cb)`.
5. Receiver validates `echo_nonce_b_cb` and completes transport authentication.

Callback channel (receiver -> initiator):

- Initiator verifies the callback peer certificate is the expected peer and validates `echo_nonce_a_cb`.

Failures:

- Any message type mismatch or nonce mismatch is a `ProtocolViolation`.
- Any timeout results in `PeerUnavailable` and the connection is closed.

Additional callis:

- Every callis (primary or blob) performs full mTLS + connect-back validation independently.

### TCP Callback Rendezvous State

The TCP backend owns a callback rendezvous map for connect-back validation. This map is the only
state that connects the primary channel's `AUTH_INIT`/`AUTH_CHALLENGE` flow to the callback
channel's `CALLBACK_INIT` flow. The rendezvous is a small state machine with latched completion.

Required state transitions:

1. **Pending registered:** the initiator inserts `nonce_a_cb`, expected peer address, expected
   certificate identity, and a one-shot reply before it waits for the callback.
2. **Callback arrived:** the callback accept path validates message type, nonce echo,
   authenticated peer certificate, and expected peer address before completing the one-shot reply.
3. **Primary completed:** after the primary channel validates `AUTH_CHALLENGE` and sends
   `AUTH_PROOF`, the pending entry must already be consumed.
4. **Failure cleanup:** timeout, nonce mismatch, address mismatch, certificate mismatch, callback
   channel close, primary channel close, and task cancellation must remove the pending entry
   exactly once.

Primitive behavior:

- Callback arrival uses a latched primitive such as `oneshot`; it does not depend on a
  non-latching `Notify` to observe callback arrival.
- The pending callback map does not hold its lock while performing TLS I/O, TCP dial/accept I/O,
  certificate validation, or waiting for the callback reply.
- A callback that arrives before the primary task begins awaiting the reply still satisfies the
  primary task.
- A callback that arrives after timeout cleanup is rejected and does not recreate state.
- Smooth auth reload does not mutate or invalidate already registered callback rendezvous entries;
  each entry validates against the auth material and peer identity captured for that handshake.

Testing coverage:

- Unit or backend tests must cover callback-before-wait, callback-after-wait, callback timeout,
  nonce mismatch cleanup, address mismatch cleanup, certificate mismatch cleanup, simultaneous
  connect-back, smooth auth reload during callback, and stale callback rejection after timeout.
- Integration tests must continue to cover TCP connect-back success, repeated full-auth callis,
  removed message type rejection, callback port mismatch, callback timeout, and smooth auth
  rotation.

### Shared Callback Rendezvous Requirements

TCP may share callback rendezvous lifecycle mechanics with the socket backend, but TCP-specific
authentication and address validation must remain owned by the TCP transport.

The shared mechanics may cover:

- registration of a pending callback entry before the primary task waits;
- latched completion through a one-shot reply;
- exactly-once removal on success, timeout, cancellation, channel close, or validation failure;
- stale callback rejection after timeout cleanup;
- lock-free waiting, where no rendezvous map lock is held across TLS I/O, TCP dial/accept I/O,
  certificate validation, or waiting for the callback reply.

The shared helper must not own:

- TCP certificate identity validation;
- expected peer socket-address validation;
- nonce payload parsing or protocol message type validation;
- smooth-auth material selection for an individual handshake.

Testing coverage:

- TCP tests must continue to cover callback-before-wait and callback-after-wait against the shared
  lifecycle helper.
- TCP mismatch and timeout tests must prove failed callbacks remove pending state exactly once and
  do not satisfy a later primary handshake.
- Smooth auth reload tests must prove an already registered TCP callback validates against the
  handshake-captured material rather than whatever material is current after reload.

### Blob Callis

Blob callis for TCP use the same A0 connect-back validation as primary callis.

### Auth Reload (Smooth Rotation)

`Transport::reload_auth` swaps the backend's TLS material atomically. Existing TLS sessions are
unaffected; the next outbound dial and the next inbound accept use the new material. There is no
per-peer breaker, no forced disconnect, and no pin to release. A peer presenting a different
(validly authenticated) certificate on a subsequent callis is accepted at the same TCP address.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
