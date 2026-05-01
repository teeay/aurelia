// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::observability::ObservabilityHandle;

#[allow(clippy::too_many_arguments)]
pub(super) async fn ensure_primary_dial<B>(
    session: &Arc<PeerSession>,
    state: &mut PeerState,
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
    backend: &Arc<B>,
    config: &DomusConfigAccess,
    blob: &Arc<BlobManager>,
    registry: &Arc<TabernaRegistry>,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    primary_available: &Arc<Notify>,
    primary_dispatch: &Arc<PrimaryDispatchQueue>,
    callis_tracker: &CallisTracker,
    observability: &ObservabilityHandle,
    has_pending: bool,
) where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    if !state.primary.is_empty() || state.dialing_primary {
        return;
    }
    if !should_reconnect_primary(session, state, has_pending).await {
        return;
    }
    let max_parallel = config.snapshot().await.max_parallel_callis_per_peer.max(1);
    if callis_tracker.count() >= max_parallel {
        return;
    }
    state.dialing_primary = true;
    let delay = if state.role == PeerRole::Listener {
        compute_listener_delay(config, state.role, state.had_primary).await
    } else {
        compute_reconnect_delay(config, state.reconnect_attempt).await
    };
    state.reconnect_attempt = state.reconnect_attempt.saturating_add(1);
    if let Some(addr) = current_dial_addr(dial_addr).await {
        let runtime_handle = session.runtime_handle();
        spawn_dial_task(
            addr,
            Arc::clone(backend),
            config.clone(),
            session.clone(),
            blob.clone(),
            registry.clone(),
            delay,
            peer_state_tx.clone(),
            Arc::clone(primary_available),
            Arc::clone(primary_dispatch),
            callis_tracker.clone(),
            observability.clone(),
            runtime_handle,
        );
    }
    if state.role == PeerRole::Listener {
        state.role = PeerRole::Originator;
    }
}

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
        return Duration::from_secs(4);
    }
    let idx = attempt.saturating_sub(1);
    if idx >= cfg.reconnect_backoff.len() {
        *cfg.reconnect_backoff
            .last()
            .unwrap_or(&Duration::from_secs(4))
    } else {
        cfg.reconnect_backoff[idx]
    }
}

pub(super) async fn should_reconnect_primary(
    session: &Arc<PeerSession>,
    state: &PeerState,
    has_pending: bool,
) -> bool {
    if state.closing || session.is_closing() {
        return false;
    }
    if has_pending {
        return true;
    }
    session.has_inflight().await
}

pub(super) async fn current_dial_addr(
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
) -> Option<DomusAddr> {
    let guard = dial_addr.lock().await;
    guard.clone()
}
