// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

#[test]
fn handshake_gate_enforces_per_peer_limit() {
    let config = DomusConfig {
        inbound_handshake_limit_per_peer: 2,
        ..Default::default()
    };

    let limited = aurelia_logging::limited::init_limited_logging(
        aurelia_logging::limited::log_ids::LIMITED_LOG_IDS,
        Duration::from_secs(0),
    );
    let gate = HandshakeGate::new(limited.registry());
    let peer = Arc::new(AtomicUsize::new(0));

    let _permit_one = gate
        .try_acquire(&config, &peer)
        .expect("expected first permit");
    let _permit_two = gate
        .try_acquire(&config, &peer)
        .expect("expected second permit");
    assert!(gate.try_acquire(&config, &peer).is_none());
}
