# Glossary

### Domus

A running instance of an application/runtime participating in the fabric. The plural is also domus. A domus can be:

* an instance of the same application communicating with other instances
* an instance of a different application that shares the same binary protocol, communicating over the same network

Additional properties:

* has identity
* hosts zero or more local tabernae
* maintains transport relationships with remote domus

### Imperium

One complete fabric containing all services, domus, and tabernae. The plural is Imperia.

An Imperium can either be IP-based or socket-based, transports cannot be mixed.

### Peer

A remote domus as viewed from the local domus.

Per-peer state includes:

* connection state
* outbound queues
* inflight messages
* ACK tracking
* dedupe state
* reachability

### Taberna

A globally addressable message ingress target hosted on a domus. The plural is tabernae.

* identified by `u64`
* may be backed by an actor mailbox, channel, queue, or other adapter
* a peer may host multiple tabernae

### Taberna sink

The local adapter responsible for accepting a message into a taberna’s ingress boundary.

Delivery at A1 means the taberna sink accepted enqueue.

### Taberna registry

The internal concrete mapping from taberna ID to taberna sink. It is owned by Domus and is not
configurable or injectable.

### Route

Knowledge of how a destination taberna is reached.

A route resolves to one of:

* local
* reachable via a specific peer
* unknown

### Route resolver

The component that determines how to reach a destination taberna.

### Peer session

The logical transport relationship with a peer used for reliable delivery state.

It may span reconnects of the underlying socket(s).

### Callis

An independent transport path within a peer session. The plural is calles.
Abbreviated as CLS by users, but callis/calles must be used by AI assistants.
Do not abbreviate in documentation; use callis/calles.

Between two domus there may be zero or more calles. If any exist:

* one callis must be the message callis
* an additional blob callis may exist

Only one message callis and one blob callis are initiated today, but the receiver can accept multiple calles of each type in preparation for future parallel callis support.

Implemented as a persistent mTLS connection that is automatically reconnected on error, might be torn down on normal close/exit.

### Primary callis

The main persistent mTLS connection to a peer.

Carries:

* transport-critical control
* transport-normal control
* application messages

### Blob callis

An optional secondary mTLS connection to the same peer.

Used for traffic isolation for large-transfer messages.
Still transport-level only.

### Peer message ID

A transport-private `u32` identifier allocated from a peer-wide unique sequence space.

* unique within the peer session over the active dedupe/replay horizon
* used for ACK correlation, replay, and dedupe
* not exposed to A2/A3
* may be used on both primary and blob calles

A shared peer message ID space does **not** imply callis coupling by itself.

### ACK

Confirmation from the remote peer that a message identified by peer message ID was accepted into the destination taberna ingress boundary.

### Inflight message

A sent but not yet ACKed message retained by the sender for possible replay.

### Outbound queue

A bounded queue of messages awaiting transmission to a peer, typically tracked per callis and/or per peer.

### Dedupe

Receiver-side prevention of duplicate acceptance of the same peer message.

### Transport control message

A reserved fabric-level message used for transport/runtime behavior only.

Examples:

* hello
* keepalive
* ack

### Application message

Any non-reserved message type carried by the fabric without semantic interpretation.

### Delivery

For A1, a message is delivered when the remote peer has ACKed that the destination taberna accepted enqueue.

### Send failure

Failure of the fabric to achieve delivery.

Externally this may be presented as one failure class, with structured internal cause.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
