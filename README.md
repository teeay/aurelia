# Aurelia

**An embeddable service mesh for Rust applications.**

Aurelia gives Rust services a built-in, secure, peer-to-peer fabric — no
sidecar, no control plane, no extra runtime to deploy. You add it as a
crate, your application gets authenticated mesh networking.

[![Crates.io](https://img.shields.io/crates/v/aurelia.svg)](https://crates.io/crates/aurelia)
[![Docs.rs](https://img.shields.io/docsrs/aurelia)](https://docs.rs/aurelia)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Status: early preview. The public API may change before 1.0.

## Why Aurelia

Most service-mesh tooling assumes Kubernetes, sidecars, and an operator
team. That is overkill for a great many Rust applications — embedded
fleets, edge devices, internal control planes, peer-to-peer tools, and
single-binary services that still need to talk to each other safely.

Aurelia takes the opposite approach:

- **Embedded.** Ships as a Rust crate. No sidecar, no daemon, no extra
  process to supervise.
- **Secure by default.** mTLS over TCP and authenticated Unix-socket
  transport. Peer identity is verified before any application traffic
  flows.
- **Async, with timeouts everywhere.** All operations are asynchronous
  and bounded; nothing in the hot path can hang silently.
- **Layered and replaceable.** A clean A0 (transport auth) / A1
  (messaging) / A2 (services) / A3 (your app) split lets you adopt only
  what you need.

## What you get today

The current preview ships the foundations:

- **Authenticated transports.** mTLS over TCP and authenticated Unix
  sockets, with handshake completion before any application frames.
- **Peering layer (A1).** Message and blob transfer, peer addressing,
  connection lifecycle, and a pluggable route resolver.
- **Single error model.** One `AureliaError` type across the stack, with
  stable error IDs — no per-crate error zoos to translate between.
- **A thin, owned runtime.** Aurelia owns its Tokio runtime so your
  application does not have to think about it.

Higher-level A2 service capabilities (discovery, RPC, etc.) are on the
roadmap and will land on top of the same A1 fabric.

## Quick look

```rust
use std::sync::Arc;
use aurelia::{Aurelia, DomusConfig, DomusAddr, DomusAuthConfig, SimpleResolver};

let aurelia = Aurelia::new();

let domus = aurelia
    .domus_builder(
        DomusConfig::default(),
        DomusAddr::from("local"),
        DomusAuthConfig::pkcs8_pem(/* ... */),
        Arc::new(SimpleResolver::default()),
    )
    .build()
    .await?;
```

See [docs.rs/aurelia](https://docs.rs/aurelia) for the full API.

## Project home

- Website: [teeay.dev/aurelia](https://teeay.dev/aurelia)
- Source: [github.com/teeay/aurelia](https://github.com/teeay/aurelia)
- Docs: [docs.rs/aurelia](https://docs.rs/aurelia)

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
Copyright © 2026 Zivatar Limited. See [NOTICE](NOTICE).

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in Aurelia shall be licensed as above, without
any additional terms or conditions.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
