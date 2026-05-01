# TCP Transport

Status: Developed

## Objectives

- Define the TCP/mTLS transport backend boundary for A1.
- Specify the TCP A0 connect-back validation flow.

## Technical Details

### Transport Backend Boundary

The TCP backend performs mTLS authentication and A0 validation before returning an authenticated stream and
`DomusAddr::Tcp` identity to A1. A1 does not interpret transport-specific types.

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

Auth material is supplied via `DomusAuthConfig::Pkcs8` (DER or PEM). Both inbound and outbound TCP callis use
full mTLS verification in A0. Certificates must include a URI SAN of the form:

- `aurelia+tcp://<ip>:<port>`

If the TLS stack requires server-name validation, the matching IP SAN must also be present.

### A0 Connect-Back Validation

TCP connections complete a dedicated connect-back validation step before A1 `hello`. The flow is symmetric;
if both peers initiate concurrently, both handshakes may succeed and A1 resolves duplicate callis normally.

Configuration:

- `tcp_callback_timeout` (default 10 seconds): dial timeout for the callback connection.
- `tcp_handshake_timeout` (default 20 seconds): total A0 handshake timeout.

Framing:

- There is **no** length prefix and **no** version field.
- The first byte is the message type, which determines the fixed payload length.
- Any unexpected message type or invalid payload length is a `ProtocolViolation`.

Message types (fixed payloads):

```text
1: AUTH_INIT       (nonce_a: 32 bytes, nonce_a_cb: 32 bytes)
2: CALLBACK_INIT   (nonce_b_cb: 32 bytes, echo_nonce_a_cb: 32 bytes)
3: AUTH_CHALLENGE  (nonce_b: 32 bytes)
4: AUTH_PROOF      (echo_nonce_b_cb: 32 bytes)
5: AUTH_RESUME     (session_nonce: 128 bytes)
```

Primary channel (initiator -> receiver):

1. Initiator sends `AUTH_INIT(nonce_a, nonce_a_cb)`.
2. Receiver opens a callback connection to the initiator’s claimed TCP address and sends
   `CALLBACK_INIT(nonce_b_cb, echo_nonce_a_cb)`.
3. Receiver sends `AUTH_CHALLENGE(nonce_b)` on the primary channel.
4. Initiator replies with `AUTH_PROOF(echo_nonce_b_cb)`.
5. Receiver validates `echo_nonce_b_cb` and completes transport authentication.

Callback channel (receiver -> initiator):

- Initiator verifies the callback peer certificate is the expected peer and validates `echo_nonce_a_cb`.

Failures:

- Any message type mismatch or nonce mismatch is a `ProtocolViolation`.
- Any timeout results in `PeerUnavailable` and the connection is closed.

Session nonce:

- The session nonce bundle is:
  `session_nonce = nonce_a || nonce_b || nonce_a_cb || nonce_b_cb`
- The session nonce is used during connect-back as proof of address ownership and is not retained
  after the handshake completes.

Additional callis:

- Every callis (primary or blob) performs full mTLS + connect-back validation independently.
- `AUTH_RESUME` is reserved for the simplified resume handshake described below; it does not
  bypass mTLS validation and does not require any per-peer pin.

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
