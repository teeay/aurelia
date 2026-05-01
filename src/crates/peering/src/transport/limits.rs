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
            aurelia_logging::error_limited!(
                registry,
                aurelia_logging::limited::log_ids::HANDSHAKE_TOTAL_LIMIT,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DomusConfig;

    #[tokio::test]
    async fn preauth_gate_enforces_limit() {
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
    }
}
