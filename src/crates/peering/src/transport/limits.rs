// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::config::DomusConfigAccess;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct PreAuthGate {
    inflight: AtomicUsize,
}

impl PreAuthGate {
    pub(super) fn new() -> Self {
        Self {
            inflight: AtomicUsize::new(0),
        }
    }

    pub(super) async fn try_acquire(
        &self,
        config: &DomusConfigAccess,
    ) -> Option<PreAuthPermit<'_>> {
        let limit = config.snapshot().await.inbound_handshake_limit_total.max(1);
        let next = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        if next > limit {
            let registry = config.limited_registry();
            aurelia_logging::warn_limited!(
                registry,
                aurelia_ids::LogId::HandshakeTotalLimit,
                inflight = next,
                limit,
                "pre-auth handshake limit reached; closing inbound connection"
            );
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(PreAuthPermit { gate: self })
    }
}

pub(super) struct PreAuthPermit<'a> {
    gate: &'a PreAuthGate,
}

impl Drop for PreAuthPermit<'_> {
    fn drop(&mut self) {
        self.gate.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}
