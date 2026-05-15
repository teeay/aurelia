// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::transport::limits::PreAuthGate;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

const LIMIT_TEST_TIMEOUT: Duration = Duration::from_millis(500);

#[tokio::test]
async fn preauth_gate_enforces_limit() {
    tokio::time::timeout(LIMIT_TEST_TIMEOUT, async {
        let config = DomusConfig {
            inbound_handshake_limit_total: 1,
            ..Default::default()
        };
        let access = DomusConfigAccess::from_config(config);
        let gate = PreAuthGate::new();

        let first = gate.try_acquire(&access).await;
        assert!(first.is_some());
        let second = gate.try_acquire(&access).await;
        assert!(second.is_none());
        drop(first);
        let third = gate.try_acquire(&access).await;
        assert!(third.is_some());
    })
    .await
    .expect("async test timed out");
}

#[test]
fn handshake_gate_enforces_per_peer_limit() {
    let config = DomusConfig {
        inbound_handshake_limit_per_peer: 2,
        ..Default::default()
    };

    let limited = aurelia_logging::limited::init_limited_logging(Duration::from_secs(0));
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
