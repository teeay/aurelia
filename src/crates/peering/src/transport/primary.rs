// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Fallback only used when reconnect backoff config is empty or missing its final entry.
const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_secs(4);

pub(super) fn remove_primary_handle(
    handles: &mut VecDeque<CallisHandle>,
    id: CallisId,
) -> Option<CallisHandle> {
    let pos = handles.iter().position(|handle| handle.id == id)?;
    handles.remove(pos)
}

pub(super) async fn compute_listener_delay(
    config: &DomusConfigAccess,
    role: PeerRole,
    had_primary: bool,
) -> Duration {
    let cfg = config.snapshot().await;
    match role {
        PeerRole::Originator => Duration::from_secs(0),
        PeerRole::Listener => {
            if had_primary {
                cfg.listener_reconnect_timeout
            } else {
                cfg.listener_delay
            }
        }
    }
}

pub(super) async fn compute_reconnect_delay(
    config: &DomusConfigAccess,
    attempt: usize,
) -> Duration {
    if attempt == 0 {
        return Duration::from_secs(0);
    }
    let cfg = config.snapshot().await;
    if cfg.reconnect_backoff.is_empty() {
        return DEFAULT_RECONNECT_BACKOFF;
    }
    let idx = attempt.saturating_sub(1);
    if idx >= cfg.reconnect_backoff.len() {
        *cfg.reconnect_backoff
            .last()
            .unwrap_or(&DEFAULT_RECONNECT_BACKOFF)
    } else {
        cfg.reconnect_backoff[idx]
    }
}

pub(super) async fn current_dial_addr(
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
) -> Option<DomusAddr> {
    let guard = dial_addr.lock().await;
    guard.clone()
}
