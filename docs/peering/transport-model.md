# Peering Transport Model

Status: Developed

## Objectives

- Define callis lifecycle and transport role transitions.
- Define delivery and failure semantics for transport operations.
- Clarify what transport does and does not guarantee.

## Technical Details

### Peer Session

A peer session represents the logical transport relationship with a peer. It may span transient reconnects of the underlying socket, preserving retained inflight messages when the peer has not restarted.

### Peer Handle Lifecycle (Simple)

The peer handle is an ephemeral, per-peer structure. The lifecycle is intentionally simple and **does not** use epochs or other session counters.

Phases:

- **Idle:** no callis are active and no reconnect attempts are in flight.
- **Active:** one or more callis are established. The first callis is always the primary. Blob callis may be established on demand.
- **Impaired:** all callis are down due to a network error. The peer handle remains in place, retains inflight messages, and continues to accept outbound sends within configured limits. Reconnect attempts run using existing callis management and role/backoff logic. The impaired window is bounded by `send_timeout`.
- **Closing:** a negotiated shutdown has begun. The listener is shut down (no inbound callis accepted). New outbound sends fail immediately with `peer-unavailable`. Only A1 control traffic is flushed.
- **Teardown:** the peer handle is destroyed. All pending and inflight messages are failed locally with `peer-unavailable`, and any inflight A1 messages are dropped.

### Peer Handle Invariants

**No Epochs / Peer Handle Retains Rights**

- The peer handle is the sole retainer of rights to emit ACK/ERROR and complete inbound waiters.
- Epoch counters or any equivalent session counters are prohibited.
- The inbound receive loop must not gate ACK/ERROR emission on epochs or session generations.
- When the peer handle is torn down, all pending inbound waiters (message and blob) are cancelled
  and must not emit ACK/ERROR after teardown.

**No Certificate Pinning Across Callis**

- A1 does not pin peer certificates or session nonces. Each callis is admitted on its own A0
  authentication (mTLS for TCP, socket auth for socket).
- Different valid certificates from the same peer address are accepted across callis. This enables
  smooth certificate rotation with no connection drop.
- Identity binding is `peer_addr` only. The address-mismatch guard (`validate_backend_identity`) is
  the sole post-authentication identity check.

**Listener Shutdown on Negotiated Close**

- When shutdown is negotiated, the listener is shut down (the accept loop is stopped).
- No inbound callis are accepted after shutdown begins.
- This shutdown applies to the domus listener, not just the peer handle, and is required on both
  sides of the negotiated shutdown.

**Reconnect Window Bounded by `send_timeout`**

- When **all** callis to a peer are down, the peer enters the impaired state and starts a
  reconnect window bounded by `send_timeout`.
- Reconnect attempts continue using existing callis management and role/backoff logic.
- If no reconnect succeeds within `send_timeout`, the peer handle is torn down immediately.
- Once teardown begins, no further reconnect attempts are permitted.

**Reconnect Disagreement Reuses the Same Peer Handle**

- If a reconnect handshake is attempted and the peer does not echo `RECONNECT`, the receiver's
  session is gone (the peer was restarted from the receiver's perspective).
- The originator treats the new connection as a fresh session on the **same** peer handle: retained
  inflight non-A1 messages are failed locally with `peer-restarted`, retained inflight blob streams
  are failed locally, queued non-A1 messages are re-dispatched on the new callis, and the new
  callis becomes the active primary.

**Teardown Semantics**

- Teardown fails all pending and inflight non‑A1 messages locally with `peer-unavailable`.
- Inflight A1 messages are dropped without emitting further ACK/ERROR frames.
- All inflight blob streams are failed locally and their reservations are released.
- The peer handle cancels all reconnect attempts.

### Inbound Callis Receive Loop

Inbound callis handling must be managed by a **single per-callis worker** that owns the receive
loop and **never spawns a task per message**. The receive loop is responsible only for frame
parsing/validation, accounting, and scheduling of delivery outcomes. It must **not** block on
taberna acceptance, blob callis readiness, or other completion waits.

Requirements:

- **Single worker:** exactly one receive loop per callis; no per-message or per-blob `spawn` on the
  inbound path.
- **Non-blocking scheduling:** after frame validation, message/blob delivery is scheduled as an
  in-flight operation and the loop immediately returns to reading the next frame.
- **Bounded by timeouts:** any wait for taberna acceptance or blob readiness must be enforced with
  `accept_timeout`/`send_timeout`-derived deadlines so in-flight operations cannot stall forever.
- **Accept timeout source:** taberna accept timeouts are enforced by the ingress channel TTL
  (from `accept_timeout`) and expiry reclamation; no separate per-message timeout futures are
  permitted.
- **Immediate enqueue attempt:** the loop attempts to place inbound messages directly into the
  destination taberna inbox. If the inbox is full, it must emit `taberna-busy` immediately and must
  not wait for capacity.
- **Caducus ingress channel:** inbound taberna delivery uses the `caducus` MPSC channel with
  TTL/expiry. Expiry reclamation drives `taberna-busy` timeouts via the per-entry expiry report
  channel; the receive loop must clean up any accept waiter for that message before emitting the
  timeout.
- **Event-driven wakeups:** the loop must wake on **any** of:
  - a new inbound frame,
  - completion of an in-flight delivery (ACK/ERROR readiness),
  - timeout of an in-flight delivery,
  - shutdown signal.
- **ACK/ERROR ownership:** ACK/ERROR frames are emitted by the same receive loop worker (not by a
  per-message task), and must be routed through the existing A1 dispatch path for primary callis.
- **No extra buffering contract:** this loop does not introduce new queues/channels unless
  explicitly approved; it uses in-flight tracking and async polling only.
- **Primary dispatch exclusivity:** for primary callis, the callis writer channel is owned by the
  primary dispatch worker. Other code must not send directly on the callis writer channel; it must
  enqueue A1 frames via `PrimaryDispatchQueue`.

### Domus + Taberna Shutdown

Shutdown is explicit at each level and must not introduce additional taberna tracking beyond the
registry. When shutdown is initiated, all queued taberna requests are **dropped** (drain means
drop) and reported as `domus-closed`.

Requirements:

- **Registry is the single source of tabernae:** the taberna registry remains the only place where
  tabernae are tracked. Domus must not introduce parallel taberna tracking.
- **Domus shutdown cascades:** `Domus::shutdown` must invoke `TabernaRegistry::shutdown` before
  shutdown completes so local taberna ingress is closed immediately.
- **Taberna shutdown hook:** `TabernaInbox` gains `shutdown` (default no-op) so the registry can
  shut down inboxes without knowing their concrete type.
- **Caducus channel shutdown:** the ingress channel's shutdown marks the channel closed and
  immediately drains all queued entries via the per-item shutdown report channel (no delivery to
  the consumer, no expiry-based handling).
- **Error semantics:** dropped taberna requests must resolve their accept waiters with
  `domus-closed` (new `ErrorId`), not `remote-taberna-rejected` or `taberna-busy`.
- **Taberna receive semantics:** `Taberna::next` returns `Result<TabernaRequest<Codec>, AureliaError>`.
  `receive-timeout` is returned on timeout, and `domus-closed` is returned once shutdown has been
  triggered and no further messages will arrive.
- **Inbound waiters:** inbound callis accept waiters must resolve immediately on taberna shutdown
  so the callis receive loop emits ERROR frames with `domus-closed` using the normal ACK/ERROR
  path.

### Taberna Ingress Channel

The taberna ingress channel — the bounded MPSC channel with TTL/expiry that backs every
`Taberna<Codec>` and `TabernaInboxHandle<Codec>` — is provided by the `caducus` crate. Caducus
supplies the storage, the reclaimer task, snapshot-on-clone sender semantics, and TTL-driven
expiry. The peering crate provides only the report-channel implementations that map expiry and
shutdown back to taberna accept-waiter resolution.

Crate binding:

- `caducus` is a direct dependency of the peering crate.
- Caducus requires a Tokio runtime handle. Domus passes `self.runtime_handle.clone()` to
  `MpscBuilder::runtime` at build time; the implicit `Handle::try_current()` path is not used.

Channel construction (per registered taberna):

- `Domus::taberna(id, codec)` builds the channel via
  `caducus::MpscBuilder::<TabernaRequest<Codec>>::new(taberna_accept_queue_size, accept_timeout)`,
  attaches a per-domus `TabernaShutdownReport<Codec>` via `.shutdown_channel(...)`, sets the
  runtime handle, and stores the resulting `MpscSender` inside `TabernaInboxHandle` and the
  `Receiver` inside `Taberna`.
- `TabernaInboxHandle::new` installs `TabernaExpiryReport<Codec>` via
  `MpscSender::set_expiry_channel`. Each enqueued `TabernaRequest` snapshots this channel at
  send time, so an in-flight item keeps the report channel it was enqueued with even if the
  sender's channel is later replaced.

Report channels:

- `TabernaExpiryReport<Codec>` implements `caducus::ReportChannel<TabernaRequest<Codec>>` and
  resolves the request's accept waiter by invoking `TabernaRequest::expire()` (which sends
  `taberna-busy` to the `oneshot`).
- `TabernaShutdownReport<Codec>` implements `caducus::ReportChannel<TabernaRequest<Codec>>` and
  invokes `TabernaRequest::shutdown()` (which sends `domus-closed`).
- Both `send` impls return `Ok(())` unconditionally; the reclaimer invokes them under unwind
  isolation.

Send path semantics:

- `MpscSender::send(item)` returns `Result<(), CaducusError<TabernaRequest<Codec>>>`. The
  `TabernaInboxHandle::enqueue` adapter maps `CaducusErrorKind::Full(_)` to
  `ErrorId::TabernaBusy` and `CaducusErrorKind::Shutdown(_)` to `ErrorId::DomusClosed`.
  The rejected `TabernaRequest` is dropped on the error path; its `Drop` impl is unobserved
  because no `oneshot::Receiver` was returned to the caller.
- TTL/expiry remains the only source of taberna `accept_timeout`; no per-message timeout
  futures exist anywhere on the inbound path.

Receive path semantics:

- `Receiver::next` is deadline-based: the `Taberna::next(timeout_override)` adapter converts the
  duration to `Some(Instant::now() + timeout_override)` (defaulting to 1 s when no override is
  given). `CaducusErrorKind::Timeout` maps to `ErrorId::ReceiveTimeout`;
  `CaducusErrorKind::Shutdown(_)` maps to `ErrorId::DomusClosed`.
- `Taberna<Codec>` holds the `Receiver` directly. There is no mutex around it. `Receiver::next`
  takes `&self`, the receiver is single-owner, and the public `Taberna::next` is single-consumer
  by contract.

Configuration updates:

- `TabernaInboxHandle::refresh_limits` calls `MpscSender::update_capacity(n)` and
  `MpscSender::update_ttl(d)` whenever `DomusConfig` snapshots show a change in
  `taberna_accept_queue_size` or `accept_timeout`. The `update_ttl` `Result` is discarded;
  `DomusConfigBuilder` validates the value at construction so `InvalidArgument` cannot occur on
  the live path. Capacity shrinks may evict head items via their expiry channel — this matches
  the documented bound: a capacity change applies to the next reclamation tick, not deferred
  indefinitely.

Shutdown semantics:

- `MpscSender::shutdown()` is **synchronous** and idempotent. `TabernaInbox::shutdown` keeps its
  async-trait signature for registry-driven dispatch; the body invokes the sync call. By the
  time `shutdown_and_report` returns, every queued `TabernaRequest` has been routed to its
  shutdown channel, so all corresponding accept waiters have resolved with `domus-closed`.
- Dropping the last `MpscSender` clone triggers the same shutdown-and-drain. In production the
  registry holds the only `Arc<TabernaInbox>`; registry unregister cascades to sender drop,
  which closes the channel. Combined with the explicit `Domus::shutdown` path this is at-most
  one effective shutdown per channel (caducus de-duplicates).
- Dropping the `Receiver` (i.e. the `Taberna<Codec>` handle) also triggers a shutdown of the
  channel. This matches the public taberna contract: when the application drops its taberna,
  the inbox is closed, queued requests are reported as `domus-closed`, and any subsequent
  inbound enqueue fails with `domus-closed`.

### Calles

- Primary callis: main persistent mTLS connection carrying transport control and application messages.
- Blob callis: optional secondary mTLS connection for isolating large transfers.

Between two domus there may be zero or more calles. If any exist:

- One callis must be the message callis.
- An additional blob callis may exist.

Today, only one message callis and one blob callis are initiated. The receiver must accept multiple calles of each type so parallel callis can be introduced later.

Calles are independent connections and must not block each other. Connection lifecycle is per callis.

Multiple calles of the same type may exist between two peers. This is not implemented in v1 and is out of scope for now, but the transport design must preserve the ability to add parallel calles later. When multiple calles of the same type are active, outbound selection is round-robin across the active callis handles.

The first callis is always the primary. Blob callis may only be established once a primary callis session is active.

### A1 Surface vs. Calles

A1 exposes only message and blob semantics to A2/A3. Calles are internal to A1; the transport chooses how to realize message/blob delivery for each address family.

### Transport Backend Boundary

Transport is split into:

- **Transport backend:** bind/accept/dial plus transport authentication. The backend returns an authenticated bidirectional stream and the peer’s Domus address.
- **A1 callis lifecycle:** `hello`/`hello-response`, session resumption, scheduling, and delivery semantics.

A0 transport authentication (TLS or socket auth) completes **before** any A1 `hello` frames are exchanged.

Each Aurelia instance operates with a single transport backend (TCP or socket) derived from the local
Domus address used to create the instance. Backend selection happens once at instantiation and is
immutable for the lifetime of the Domus. A1 rejects any peer address that does not match the local
transport type.

### Auth Reload (Smooth Rotation)

`Domus::reload_auth(DomusAuthConfig)` swaps the backend's auth material atomically. It is
non-disruptive:

- Existing TLS / socket-auth sessions continue with the credentials they were established with.
- The next outbound dial uses the new material.
- The next inbound accept uses the new material for its A0 authentication.

There is no per-peer breaker, no forced disconnect, and no callis-quiesce wait. Outbound queues
and retained inflight are unaffected. A peer presenting a different (validly authenticated)
certificate on a subsequent callis is accepted at the same `peer_addr`.

### Inbound Handshake Admission Control (A0)

Inbound callis are subject to admission control in A0 **before transport authentication** and
**before** A1 `hello` frames are exchanged. Admission control applies to **all callis types**
(primary + blob) and is enforced globally. The authoritative A0 requirements live in
`docs/peering/connection-limits.md`.

Configuration (defaults in parentheses):

- `inbound_handshake_limit_total` (64): maximum number of in-flight inbound handshakes across all
  peers. If the limit is exceeded, the inbound callis is closed immediately without A1 `hello`.

Admission is best-effort and race-tolerant; it is intended to bound resource use, not to provide
strict fairness.

### Per-Peer Handshake Limit (A1)

- `inbound_handshake_limit_per_peer` (3): maximum number of in-flight A1 handshakes for a single
  peer.

### Parallel Callis Limit (Per Peer, A1)

In addition to handshake admission, each peer has a configurable cap on **active** callis enforced
in A1. This limit is distinct from in-flight handshakes.

- `max_parallel_callis_per_peer` (8): maximum number of active callis per peer (primary + blob).

When the limit is reached, new inbound callis for that peer are rejected and outbound dial attempts
are suppressed until capacity is available.

### Primary Callis Lifecycle

1. Open: establish an authenticated transport connection to the peer listener (mTLS for TCP, socket auth for socket).
2. Handshake: originator sends `hello` with header flags. Receiver responds with `hello-response` and header flags per `docs/peering/wire-protocol.md`.
3. Active: transport control and application messages flow. Keepalive is only sent on the primary callis when idle.
4. Close: normal transport close (TLS close or socket drop). Remote treats this as a transient disconnect and follows the reconnect policy.

### Blob Callis Lifecycle

- The blob callis is optional and may be established only when a primary callis session is active.
- Open: establish a second authenticated transport connection to the same listener.
- Handshake: use the standard `hello` exchange with the `BLOB` header flag set to identify the callis type. Negotiation details are defined in `docs/peering/wire-protocol.md`.
- Active: large-transfer traffic only. No keepalive on the blob callis.
- Close: normal transport close (TLS close or socket drop). Primary callis remains unaffected.

If a blob callis connection arrives without an active primary session, the receiver closes it immediately. Attempting to open a blob callis when no primary callis is active is an error (`blob-callis-without-primary`). A blob callis may remain open even if the primary callis is broken, but a new blob callis cannot be opened unless a primary callis is active.

Blob traffic is streamed and may multiplex multiple concurrent blob streams on the blob callis.

### Blob Transfer Stream Adapters (Domus-Aligned)

#### Public Surface Requirements

- A1 does not expose multiplexed blob stream interfaces to A2/A3. Stream IDs and multiplexing are internal only.
- Domus and Taberna public APIs do not require `TabernaStreamSource` or `TabernaStreamSink` in higher layers.
- The only send entry point is typed `send` with a blob flag in `SendOptions`.

#### Adapter Requirements

- **Typed send only:** Domus exposes a single `send` method. The method is typed via a codec and is the only send entry point.
- **Blob flag:** A blob transfer is requested by setting the blob flag in `SendOptions` passed to `send`.
- **No blob length parameter:** Reservations are derived solely from the negotiated chunk size and ack window; there is no blob length field in the public API.
- **Sender stream handle:** When the blob request is accepted, the sender receives a `BlobSender` stream handle that implements `tokio::io::AsyncWrite + Unpin + Send`.
- **Receiver stream handle:** The receiver obtains an optional `BlobReceiver` inside the same accept call as the message. `BlobReceiver` implements `tokio::io::AsyncRead + Unpin + Send`.
- **Single accept path:** There is no separate `accept_blob` step. The `BlobReceiver` is provided in the message accept call. Rejecting the message rejects the blob and no stream is established.
- **Stream shutdown semantics:** Clean shutdown maps to the final chunk flagged `LAST_CHUNK`. If the last read is shorter than the negotiated chunk size, that partial chunk is the last chunk. No empty chunk is required unless the total transfer length is zero.
- **Internal multiplexing only:** Chunking, ACK window enforcement, and multiplexing remain internal to A1; the sender/receiver handles are simple stream adapters over per-stream bounded windows.
- **Local dispatch parity:** Local blob delivery may short-circuit to an in-memory stream internally, but the adapter semantics, reservations, and visibility to higher layers are identical to remote dispatch.

#### Adapter Integration Requirements (Internal)

- **No additional buffering layer:** `BlobSender` and `BlobReceiver` must read from and write to the existing per-stream windowed chunk store. No extra channels, queues, or buffers are introduced.
- **Outbound adapter:** `BlobSender` is a thin adapter that writes into the per-stream outbound path that already enforces chunk sizing and ACK window limits.
- **Inbound adapter:** `BlobReceiver` is a thin adapter that reads from the per-stream receive window that already enforces ordering and window limits.
- **Capacity enforcement:** The existing per-stream window and reservation logic remain the sole buffer limits; the adapters must not bypass or duplicate them.
- **Error propagation:** Transport errors and peer aborts must surface as stream errors on `BlobSender`/`BlobReceiver`.

#### BlobSender Adapter Semantics (Required)

- **Creation timing:** `BlobSender` is created only after the blob request message is accepted by the remote taberna **and** the blob callis is ready. `Domus::send` returns `SendOutcome::Blob { sender }` only after this point.
- **Stream identity:** `BlobSender` is bound to `stream_id = peer_msg_id` of the accepted blob request, matching the stream identity rules in `docs/peering/blobs.md`.
- **Outbound reservation:** At stream creation, A1 reserves `chunk_size * ack_window_chunks` bytes against `blob_outbound_buffer_bytes`. If the reservation fails, `Domus::send` fails with `blob-buffer-full` and no sender handle is returned.
- **Write semantics:** `BlobSender` implements `tokio::io::AsyncWrite`. `poll_write` accepts bytes into the per-stream outbound window, may return `Pending` when the window is full or no blob callis is available, may hold at most a single partial chunk (`< chunk_size`) as staging, and returns an error if the stream has failed or completed.
- **Flush semantics:** `poll_flush` waits until any staged partial chunk has been accepted into the outbound window and all queued chunks have been handed to the blob dispatch loop. It does **not** wait for `blob-transfer-complete`.
- **Shutdown semantics:** `poll_shutdown` finalizes the stream: if a partial chunk exists, emit it with `LAST_CHUNK`; if no bytes were ever written, emit a zero-length chunk with `LAST_CHUNK`; after emitting the last chunk, wait for `blob-transfer-complete` or an error. Success releases the outbound reservation.
- **Drop behavior:** Dropping a `BlobSender` without `shutdown` aborts the stream. A1 must fail the stream locally, release the outbound reservation, and, if possible, send an `error` control message for the stream on the blob callis. Until a dedicated abort error ID exists, the abort is surfaced as `peer-unavailable` to the remote side.
- **Timeouts:** Sender-side waits for ACKs and completion are bounded by `send_timeout`, consistent with existing blob dispatch behavior.

#### BlobReceiver Adapter Semantics

- **Delivery timing:** `BlobReceiver` is delivered as `TabernaRequest::blob_receiver` alongside the request message. It is **inactive** until the taberna calls `TabernaRequest::accept()`.
- **Activation on accept:** When `accept` succeeds, A1 binds the receiver to the per-stream inbound window and enables stream delivery. If `accept` fails or `reject` is called, the receiver is discarded and no blob stream is established.
- **Inbound reservation:** Inbound reservation (`chunk_size * ack_window_chunks`) is made before presenting the `BlobReceiver`. If reservation fails, the request is rejected with `blob-buffer-full` and no receiver is provided.
- **Read semantics:** `BlobReceiver` implements `tokio::io::AsyncRead`. `poll_read` yields bytes in-order irrespective of chunk boundaries, returns `Pending` until the stream is accepted and data is available, and returns `Ok(0)` only after the `LAST_CHUNK` has been fully delivered.
- **Backpressure:** The receiver’s read cadence governs inbound flow. When the per-stream inbound window is full, A1 applies its existing buffering and idle-timeout rules (`blob-stream-idle-timeout`).
- **Drop behavior:** Dropping a `BlobReceiver` after accept aborts the stream. A1 must fail the stream locally, release the inbound reservation, and, if possible, send an `error` control message for the stream on the blob callis. Until a dedicated abort error ID exists, the abort is surfaced as `peer-unavailable` to the remote side.
- **Error propagation:** Transport errors, protocol violations, and peer aborts surface as read errors on `BlobReceiver`.

### Peer Identity

A1 authenticates peers using certificates for both TCP and socket transports. Certificates are supplied by A2/A3. The semantics of how certificates are issued and what they represent are outside A1.

Domus identity is the transport address itself (IP:port for TCP or absolute socket path for UNIX sockets). There is no other domus identifier.

Transport-specific identity binding, SAN requirements, and A0 authentication flows are defined in:

- `docs/peering/tcp-transport.md`
- `docs/peering/socket-transport.md`

A1 does not pin peer certificates or session nonces. Each callis is admitted on its own A0
authentication; different valid certs from the same peer are accepted, allowing smooth rotation.

### Hello Handshake

Header flags:

- `RECONNECT`: sender is attempting to resume an existing peer session.
- `BLOB`: this connection is a blob callis; absence means primary callis.

Rules:

- Transport authentication must complete before any A1 `hello` frames are exchanged.
- The originator sends `hello` with `RECONNECT` set only when resuming a prior session.
- The originator sets `BLOB` when opening a blob callis; primary callis must not set `BLOB`.
- The receiver replies with `hello-response` and echoes `RECONNECT` only if it can resume the session. The response must preserve the callis type (if `BLOB` is set in the request, it remains set in the response).
- If a `hello` arrives without `RECONNECT`, the receiver must reply without `RECONNECT`, and retained inflight messages are invalidated.
- If a `hello` arrives with `RECONNECT`, the receiver replies with `RECONNECT` only if it can resume; otherwise it replies without `RECONNECT` and retained inflight messages are invalidated.
- A blob callis (`BLOB` set) may be established only when a primary callis session is active. If a blob callis arrives without an active primary callis, the receiver closes it and returns `blob-callis-without-primary`.

Hello payload formats and encoding details are defined in `docs/peering/wire-protocol.md`.

Crash scenarios:

- Originator crash: it restarts in listener mode. Any subsequent outbound messages follow the listener delay rule before initiating a new originator connection.
- Listener crash: it restarts in listener mode. If it receives a `hello` with `RECONNECT` from the originator, it must respond without `RECONNECT` to indicate a fresh session.

### Keepalive

Keepalive is only sent on the primary callis and only when the callis is inactive. Default interval is 15 seconds. Keepalive configuration is set via constructor or builder and supports in-flight updates through `DomusConfigAccess`.

Timeout and retry settings apply uniformly to TCP and socket sessions unless explicitly overridden by a transport-specific configuration surface.

### Frame Size Limits

- A1 rejects frames with `payload_len` greater than the configured maximum.
- The default maximum payload size is 8 MiB and is configurable via `DomusConfig` (`max_payload_len`).

### Send Management and Callis Availability

#### Peer State Mutation Path

Peer state is a concurrency-enabled resource and is mutated only via a single channel that
serializes state updates. This channel is **not** a control channel and carries **no** message
traffic. It exists solely to update peer state (dial results, callis lifecycle events, reconnect
decisions). Peer state provides synchronous snapshots for other tasks to read without mutation.

Primary and blob traffic are handled by dedicated dispatch tasks that read peer state snapshots
and write directly to callis writer streams. Traffic must never block peer state mutation.

Primary send management:

- Outbound primary traffic is stored and scheduled by a single per-peer primary dispatch queue owned by the primary dispatch task.
- The primary dispatch queue maintains three FIFO tiers: **A1**, **A2**, and **A3**. Classification is by `MessageType` ranges defined in `docs/ids.md`.
- All outbound traffic is treated as **messages**; A1 control traffic is represented as messages whose `MessageType` falls in the A1 range.
- Scheduling is strict priority: while A1 has entries, A2/A3 are not considered; while A2 has entries, A3 is not considered.
- When an item must be retried, it is requeued at the **end** of its tier queue (FIFO within tier).
- The primary dispatch task is woken by a single availability notify, irrespective of the number of active callis.
- A callis may only signal availability when it is open and able to accept **one** outbound frame.
- When multiple primary callis are open and idle, selection is round-robin across live handles only. If only one callis is idle/available, it is selected immediately.
- If a selected primary handle is closed or rejects send, it is removed from the pool immediately, the item is requeued, and the dispatcher continues.
- `PeerSession::mark_dispatched` must be invoked only after the outbound frame is successfully handed to the callis writer.
- If no live primary callis remain and there is queued work, the primary dispatch task requests a dial by sending a peer-state mutation; queued messages remain pending until a new callis is available.
- There are **no per-callis outbound queues** for primary traffic; the single dispatch queue feeds all live callis directly.

Blob send management:

- Per-stream outbound buffering and ACK window semantics are defined in `docs/peering/blobs.md`.
- A dedicated blob dispatch task drains per-stream buffers in round-robin order to preserve interleaving fairness.
- The blob dispatch task is woken by a single availability notify when any live blob callis can accept another frame.
- When multiple blob callis are open and idle, selection is round-robin across live handles only. If only one callis is idle/available, it is selected immediately.
- Blob callis selection uses live handles only; closed handles are pruned immediately and streams are reassigned when possible.
- If no live blob callis remain and there are active streams, the dispatch task pauses sends and requests a blob dial via the peer-state mutation channel; frames are not routed to closed callis.
- There are no per-callis outbound queues for blob traffic; a single dispatcher feeds live callis.

### Isolation Requirements

- Peer isolation: one peer must not block another.
- Callis isolation: blob callis issues must not stall primary callis traffic and vice versa.

### Graceful Shutdown

When a peer shutdown is negotiated (using the existing shutdown protocol):

1. Shut down the listener so no inbound callis are accepted on either side.
2. Immediately fail all new outbound sends with `peer-unavailable`.
3. Immediately reject any queued inbound items with `peer-unavailable`.
4. Flush all A1 control messages (ACK/ERROR/close). No new non-A1 messages are sent.
5. After A1 messages are flushed, exchange final close control messages and disconnect.
6. If the final close cannot be negotiated, the standard `send_timeout` applies after A1 flush.
7. At final close or timeout, enter teardown: drop inflight A1 messages, fail remaining inflight
   messages with `peer-unavailable`, and stop all reconnect attempts.

### Delivery Semantics

A send completes successfully only when the sender receives an ACK confirming that the destination taberna on the remote peer accepted the message into its ingress boundary. This does not imply application-level processing.

A message is considered delivered when all of the following are true:

- The message was transmitted to the remote peer.
- The remote peer validated the message.
- The remote destination taberna exists.
- The remote destination taberna accepted enqueue into its ingress boundary.
- The remote peer sent an ACK for the peer message ID.

Blob transfer delivery semantics and completion rules are defined in `docs/peering/blobs.md`.

### Failure Semantics

A send fails when delivery cannot be achieved. Typical failure causes include:

- Unknown destination taberna.
- Local outbound queue full.
- Peer unavailable or connection failure beyond recovery.
- Remote taberna rejected enqueue.
- Protocol failure.
- Send timeout.
- Peer crash resulting in invalidation of retained inflight messages.
- Peer signaled `close`, rejecting further delivery on the callis.

Externally, a unified send error is acceptable. Internally, specific causes must be preserved for observability and retry logic.

Error IDs and their taxonomy are defined in `docs/ids.md`. Error payload formats are defined in `docs/peering/wire-protocol.md`.

### Reconnect vs. New Connection

- Transient network failure: reconnect to the same peer session; retained inflight messages remain valid and may be replayed.
- Peer crash or restart: the new connection is treated as a fresh session; retained inflight messages on the surviving peer are invalidated and must fail locally.
- Close control message (`close`): treated as an intentional shutdown by the sender; the receiver must stop delivery on that callis, fail inflight, and must not reconnect on that session.
- Unexpected connection close (EOF/IO error/local disconnect): treated as a transient disconnect; the receiver must not emit `error` for in-flight deliveries and must allow replay after reconnect.
 - Reconnect disagreement: if a reconnect handshake is attempted and the peer does not echo
   `RECONNECT`, the originator treats the new connection as a fresh session on the **same** peer
   handle. Retained inflight non-A1 messages are failed locally with `peer-restarted`, retained
   inflight blob streams are failed locally, and queued non-A1 messages are re-dispatched on the
   new callis.

### Reconnect Backoff

Once a peer is in originator mode, reconnect attempts use the following schedule after an immediate reconnect attempt:

- 1 second
- 2 seconds
- 4 seconds
- then every 4 seconds

Reconnect configuration is set via constructor or builder and supports in-flight updates through `DomusConfigAccess`.

### Originator and Listener Roles

- A peer starts in listener mode.
- When it needs to initiate outbound communication, it becomes the originator for the primary callis and dials the remote.
- After a crash/restart, the previous originator returns to listener mode.

### Listener Delay and Reconnection Timeout

- Listener delay: 5 seconds at startup, during which the peer only listens and does not initiate outbound connections.
- Listener reconnection timeout: 20 seconds. This only applies after a connection was previously established. If the connection breaks and the listener has queued outbound messages, it waits this duration before switching to originator mode and dialing the peer.
- Reconnect window: when **all** callis to a peer are down, the peer enters the impaired state and
  attempts reconnects as normal. If no reconnect succeeds within `send_timeout`, the peer handle is
  torn down and all pending/inflight messages are failed locally.

### Transport-Specific Authentication

Transport authentication and any A0 validation occurs below A1 and must complete before any `hello` frames are exchanged.
Refer to the dedicated transport documents for details:

- `docs/peering/tcp-transport.md`
- `docs/peering/socket-transport.md`

### Blob Callis Semantics

Blob traffic always uses a separate blob callis. The blob callis is a distinct connection from the primary callis,
identified by the `BLOB` hello flag and subject to the blob callis lifecycle described above.

### Transport Scope

The peering crate is transport-only. It does not provide:

- Application semantics.
- Request/response coordination.
- Business logic guarantees.
- Discovery or membership policy.

A2/A3 layers are responsible for those behaviors.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
