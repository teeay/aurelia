// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::observability::{
    BlobCallisSettingsReport, DisconnectReason, HandshakePhase, ObservabilityHandle,
};
use crate::session::CancelReason;
use crate::transport::primary::ensure_primary_dial;

pub(super) struct PeerHandle<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    dial_addr: Arc<Mutex<Option<DomusAddr>>>,
    primary_active: Arc<AtomicBool>,
    allocator: Arc<PeerMessageIdAllocator>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    config: DomusConfigAccess,
    backend: Arc<B>,
    callis_tracker: CallisTracker,
    handshake_gate: HandshakeGate,
    inbound_handshakes: Arc<AtomicUsize>,
    pub(super) session: Arc<PeerSession>,
    pub(super) peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    primary_dispatch: Arc<PrimaryDispatchQueue>,
    shutdown_rx: watch::Receiver<bool>,
    listener_shutdown_tx: watch::Sender<bool>,
    shutdown_notify: Arc<Notify>,
    pub(super) primary_available: Arc<Notify>,
    observability: ObservabilityHandle,
    runtime_handle: tokio::runtime::Handle,
}

pub(super) enum PeerStateUpdate {
    Connected {
        callis: CallisKind,
        info: ConnectionInfo,
    },
    DialFailed(CallisKind),
    Disconnect {
        callis: CallisKind,
        id: Option<CallisId>,
    },
    ConnectionClosed {
        callis: CallisKind,
        id: CallisId,
        reason: CancelReason,
    },
    EnsurePrimaryDial,
    EnsureBlobDial,
    GracefulClose,
}

#[derive(Clone)]
pub(super) struct PeerStateSnapshot {
    pub(super) primary_handles: Vec<CallisHandle>,
}

impl PeerStateSnapshot {
    fn new() -> Self {
        Self {
            primary_handles: Vec::new(),
        }
    }
}

pub(super) struct ConnectionInfo {
    pub(super) handle: CallisHandle,
    pub(super) replay: Vec<InflightMessage>,
    pub(super) fresh_session: bool,
    pub(super) blob_settings: Option<BlobCallisSettings>,
    pub(super) blob_resume: bool,
}

pub(super) struct PeerState {
    pub(super) role: PeerRole,
    pub(super) had_primary: bool,
    pub(super) primary: VecDeque<CallisHandle>,
    pub(super) reconnect_attempt: usize,
    pub(super) dialing_primary: bool,
    pub(super) dialing_blob: bool,
    pub(super) blob_reconnect_attempt: usize,
    pub(super) closing: bool,
    pub(super) impaired_since: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerRole {
    Listener,
    Originator,
}

#[derive(Clone)]
pub(super) struct CallisHandle {
    pub(super) id: CallisId,
    // Primary callis writes are owned by the primary dispatch worker. Other code must enqueue
    // A1 frames via PrimaryDispatchQueue instead of sending on this channel directly.
    pub(super) tx: mpsc::Sender<OutboundFrame>,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) available: Arc<AtomicBool>,
}

#[derive(Clone)]
pub(super) enum OutboundFrame {
    Ack {
        peer_msg_id: PeerMessageId,
    },
    Message(PeerMessage),
    Control {
        msg_type: MessageType,
        peer_msg_id: PeerMessageId,
        payload: Bytes,
    },
    Close,
}

async fn wait_for_inflight_or_timeout(session: &PeerSession, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    session.wait_for_inflight_empty(deadline).await
}

async fn has_callis_capacity(config: &DomusConfigAccess, callis_tracker: &CallisTracker) -> bool {
    let max_parallel = config.snapshot().await.max_parallel_callis_per_peer.max(1);
    callis_tracker.count() < max_parallel
}

impl<B> PeerHandle<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        dial_addr: Option<DomusAddr>,
        registry: Arc<TabernaRegistry>,
        config: DomusConfigAccess,
        blob_buffers: Arc<BlobBufferTracker>,
        backend: Arc<B>,
        handshake_gate: HandshakeGate,
        observability: ObservabilityHandle,
        shutdown_rx: watch::Receiver<bool>,
        listener_shutdown_tx: watch::Sender<bool>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let allocator = Arc::new(PeerMessageIdAllocator::default());
        let session = Arc::new(PeerSession::new(
            Arc::clone(&allocator),
            config.clone(),
            runtime_handle.clone(),
        ));
        let blob_available = Arc::new(Notify::new());
        let blob = Arc::new(BlobManager::new(
            Arc::clone(&blob_buffers),
            Arc::clone(&blob_available),
            Arc::clone(&allocator),
        ));
        let callis_tracker = CallisTracker::new();
        let inbound_handshakes = Arc::new(AtomicUsize::new(0));
        let (peer_state_tx, peer_state_rx) = mpsc::channel(256);
        let (snapshot_tx, snapshot_rx) = watch::channel(PeerStateSnapshot::new());
        let shutdown_notify = Arc::new(Notify::new());
        let primary_available = Arc::new(Notify::new());
        let primary_dispatch = session.primary_dispatch();
        let handle = Self {
            dial_addr: Arc::new(Mutex::new(dial_addr)),
            primary_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            allocator,
            blob,
            registry,
            config,
            backend,
            callis_tracker,
            handshake_gate,
            inbound_handshakes,
            session,
            peer_state_tx,
            primary_dispatch,
            shutdown_rx,
            listener_shutdown_tx,
            shutdown_notify: Arc::clone(&shutdown_notify),
            primary_available: Arc::clone(&primary_available),
            observability,
            runtime_handle,
        };
        handle.spawn_state_task(peer_state_rx, snapshot_tx);
        handle.spawn_primary_dispatcher(snapshot_rx);
        handle.spawn_blob_dispatcher();
        handle
    }

    fn spawn_primary_dispatcher(&self, snapshot_rx: watch::Receiver<PeerStateSnapshot>) {
        let session = Arc::clone(&self.session);
        let queue = Arc::clone(&self.primary_dispatch);
        let primary_available = Arc::clone(&self.primary_available);
        let peer_state_tx = self.peer_state_tx.clone();
        let config = self.config.clone();
        self.runtime_handle.spawn(async move {
            run_primary_dispatcher(
                session,
                queue,
                snapshot_rx,
                primary_available,
                peer_state_tx,
                config,
            )
            .await;
        });
    }

    fn spawn_blob_dispatcher(&self) {
        let blob = Arc::clone(&self.blob);
        let notify = blob.dispatch_handle();
        let peer_state_tx = self.peer_state_tx.clone();
        self.runtime_handle.spawn(async move {
            loop {
                notify.notified().await;
                if peer_state_tx.is_closed() {
                    break;
                }
                dispatch_blob(&blob, &peer_state_tx, &notify).await;
                if blob.has_active_streams().await && !blob.has_callis().await {
                    let _ = peer_state_tx.send(PeerStateUpdate::EnsureBlobDial).await;
                }
            }
        });
    }

    fn spawn_state_task(
        &self,
        mut peer_state_rx: mpsc::Receiver<PeerStateUpdate>,
        snapshot_tx: watch::Sender<PeerStateSnapshot>,
    ) {
        let dial_addr = Arc::clone(&self.dial_addr);
        let primary_active = Arc::clone(&self.primary_active);
        let blob = Arc::clone(&self.blob);
        let registry = Arc::clone(&self.registry);
        let config = self.config.clone();
        let backend = Arc::clone(&self.backend);
        let session = Arc::clone(&self.session);
        let mut shutdown_rx = self.shutdown_rx.clone();
        let shutdown_notify = Arc::clone(&self.shutdown_notify);
        let peer_state_tx = self.peer_state_tx.clone();
        let primary_dispatch = Arc::clone(&self.primary_dispatch);
        let primary_available = Arc::clone(&self.primary_available);
        let callis_tracker = self.callis_tracker.clone();
        let listener_shutdown_tx = self.listener_shutdown_tx.clone();
        let observability = self.observability.clone();
        let runtime_handle = self.runtime_handle.clone();
        self.runtime_handle.spawn(async move {
            let mut state = PeerState {
                role: PeerRole::Listener,
                had_primary: false,
                primary: VecDeque::new(),
                reconnect_attempt: 0,
                dialing_primary: false,
                dialing_blob: false,
                blob_reconnect_attempt: 0,
                closing: false,
                impaired_since: None,
            };
            publish_snapshot(&snapshot_tx, &state);
            loop {
                let send_timeout = config.snapshot().await.send_timeout;
                let impaired_deadline = state.impaired_since.map(|since| since + send_timeout);
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = shutdown_notify.notified() => {
                        break;
                    }
                    _ = async {
                        if let Some(deadline) = impaired_deadline {
                            tokio::time::sleep_until(deadline).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {
                        if state.impaired_since.is_none() {
                            continue;
                        }
                        if state.closing || session.is_closing() {
                            state.impaired_since = None;
                            publish_snapshot(&snapshot_tx, &state);
                            continue;
                        }
                        let all_down = state.primary.is_empty() && !blob.has_callis().await;
                        if !all_down {
                            state.impaired_since = None;
                            publish_snapshot(&snapshot_tx, &state);
                            continue;
                        }
                        warn!("peer handle teardown: reconnect window expired");
                        teardown_peer_handle(
                            &mut state,
                            &primary_active,
                            &session,
                            &primary_dispatch,
                            &blob,
                            &dial_addr,
                            &observability,
                            DisconnectReason::LocalRequest,
                        )
                        .await;
                        publish_snapshot(&snapshot_tx, &state);
                    }
                    update = peer_state_rx.recv() => {
                        let Some(update) = update else { break; };
                        match update {
                            PeerStateUpdate::Connected { callis, info } => {
                                let peer = current_dial_addr(&dial_addr).await;
                                match callis {
                                    CallisKind::Primary => {
                                        if state.closing || session.is_closing() {
                                            spawn_send_close(
                                                Arc::clone(&primary_dispatch),
                                                CallisKind::Primary,
                                                vec![info.handle],
                                                runtime_handle.clone(),
                                            );
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        }
                                        info!(
                                            callis_id = info.handle.id,
                                            fresh_session = info.fresh_session,
                                            "primary callis ready"
                                        );
                                        state.had_primary = true;
                                        state.reconnect_attempt = 0;
                                        state.dialing_primary = false;
                                        if info.fresh_session {
                                            let drained: Vec<_> = state.primary.drain(..).collect();
                                            primary_active.store(false, Ordering::SeqCst);
                                            primary_dispatch.clear().await;
                                            if let Some(peer_addr) = peer.clone() {
                                                for handle in &drained {
                                                    observability
                                                        .primary_disconnected(
                                                            peer_addr.clone(),
                                                            handle.id,
                                                            DisconnectReason::PeerRestarted,
                                                        )
                                                        .await;
                                                }
                                            }
                                            spawn_send_close(
                                                Arc::clone(&primary_dispatch),
                                                CallisKind::Primary,
                                                drained,
                                                runtime_handle.clone(),
                                            );
                                            let blob = Arc::clone(&blob);
                                            let observability = observability.clone();
                                            let peer_for_blob = peer.clone();
                                            runtime_handle.spawn(async move {
                                                blob.fail_all_streams(AureliaError::new(ErrorId::PeerRestarted)).await;
                                                let (blob_handles, _streams) = blob.drain_callis().await;
                                                blob.reset_callis_history();
                                                for handle in blob_handles {
                                                    if let Some(peer_addr) = peer_for_blob.clone() {
                                                        observability
                                                            .blob_disconnected(
                                                                peer_addr,
                                                                handle.id,
                                                                DisconnectReason::PeerRestarted,
                                                            )
                                                            .await;
                                                    }
                                                    let _ = handle.tx.send(OutboundFrame::Close).await;
                                                }
                                            });
                                        }
                                        let callis_id = info.handle.id;
                                        state.primary.push_back(info.handle);
                                        primary_active.store(!state.primary.is_empty(), Ordering::SeqCst);
                                        if !info.replay.is_empty() {
                                            let mut replays = Vec::with_capacity(info.replay.len());
                                            for inflight in info.replay {
                                                replays.push(inflight.peer_msg_id);
                                            }
                                            primary_dispatch.push_front_many(replays).await;
                                        }
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                        if let Some(peer) = peer.clone() {
                                            observability
                                                .primary_connected(
                                                    peer.clone(),
                                                    callis_id,
                                                    info.fresh_session,
                                                )
                                                .await;
                                        }
                                    }
                                    CallisKind::Blob => {
                                        if state.closing {
                                            spawn_send_close(
                                                Arc::clone(&primary_dispatch),
                                                CallisKind::Blob,
                                                vec![info.handle],
                                                runtime_handle.clone(),
                                            );
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        }
                                        state.dialing_blob = false;
                                        state.blob_reconnect_attempt = 0;
                                        let Some(settings) = info.blob_settings else {
                                            warn!(
                                                callis_id = info.handle.id,
                                                "blob callis missing settings; closing"
                                            );
                                            spawn_send_close(
                                                Arc::clone(&primary_dispatch),
                                                CallisKind::Blob,
                                                vec![info.handle],
                                                runtime_handle.clone(),
                                            );
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        };
                                        info!(
                                            callis_id = info.handle.id,
                                            chunk_size = settings.chunk_size,
                                            ack_window_chunks = settings.ack_window_chunks,
                                            resume = info.blob_resume,
                                            "blob callis ready"
                                        );
                                        let callis_id = info.handle.id;
                                        blob.add_callis(info.handle, settings, info.blob_resume)
                                            .await;
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                        if let Some(peer) = peer {
                                            observability
                                                .blob_connected(
                                                    peer,
                                                    callis_id,
                                                    BlobCallisSettingsReport {
                                                        chunk_size: settings.chunk_size,
                                                        ack_window_chunks: settings.ack_window_chunks,
                                                    },
                                                )
                                                .await;
                                        }
                                    }
                                }
                            }
                            PeerStateUpdate::DialFailed(callis) => {
                                warn!(callis = ?callis, "callis dial failed");
                                match callis {
                                    CallisKind::Primary => {
                                        state.dialing_primary = false;
                                        let has_pending = !primary_dispatch.is_empty().await;
                                        if should_reconnect_primary(&session, &state, has_pending).await {
                                            let delay = compute_reconnect_delay(&config, state.reconnect_attempt).await;
                                            state.reconnect_attempt = state.reconnect_attempt.saturating_add(1);
                                            if !has_callis_capacity(&config, &callis_tracker).await {
                                                state.dialing_primary = false;
                                                update_impaired_since(
                                                    &mut state,
                                                    &blob,
                                                    &session,
                                                )
                                                .await;
                                                publish_snapshot(&snapshot_tx, &state);
                                                continue;
                                            }
                                            state.dialing_primary = true;
                                            if let Some(addr) = current_dial_addr(&dial_addr).await {
                                                spawn_dial_task(
                                                    addr,
                                                    backend.clone(),
                                                    config.clone(),
                                                    session.clone(),
                                                    blob.clone(),
                                                    registry.clone(),
                                                    delay,
                                                    peer_state_tx.clone(),
                                                    Arc::clone(&primary_available),
                                                    Arc::clone(&primary_dispatch),
                                                    callis_tracker.clone(),
                                                    observability.clone(),
                                                    runtime_handle.clone(),
                                                );
                                            }
                                        }
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                    }
                                    CallisKind::Blob => {
                                        state.dialing_blob = false;
                                        if !primary_active.load(Ordering::SeqCst) {
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        }
                                        if blob.has_active_streams().await {
                                            let delay = compute_reconnect_delay(&config, state.blob_reconnect_attempt).await;
                                            state.blob_reconnect_attempt =
                                                state.blob_reconnect_attempt.saturating_add(1);
                                            if !has_callis_capacity(&config, &callis_tracker).await {
                                                state.dialing_blob = false;
                                                update_impaired_since(
                                                    &mut state,
                                                    &blob,
                                                    &session,
                                                )
                                                .await;
                                                publish_snapshot(&snapshot_tx, &state);
                                                continue;
                                            }
                                            state.dialing_blob = true;
                                            if let Some(addr) = current_dial_addr(&dial_addr).await {
                                                spawn_blob_dial_task(
                                                    addr,
                                                    backend.clone(),
                                                    config.clone(),
                                                    session.clone(),
                                                    registry.clone(),
                                                    blob.clone(),
                                                    delay,
                                                    peer_state_tx.clone(),
                                                    callis_tracker.clone(),
                                                    observability.clone(),
                                                    runtime_handle.clone(),
                                                );
                                            }
                                        }
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                    }
                                }
                            }
                            PeerStateUpdate::Disconnect { callis, id } => {
                                match callis {
                                    CallisKind::Primary => {
                                        let mut removed = Vec::new();
                                        if let Some(id) = id {
                                            if let Some(handle) = remove_primary_handle(&mut state.primary, id) {
                                                removed.push(handle);
                                            }
                                        } else {
                                            removed.extend(state.primary.drain(..));
                                        }
                                        for handle in removed {
                                            let _ = handle.shutdown.send(true);
                                            if let Some(peer) = current_dial_addr(&dial_addr).await {
                                                observability
                                                    .primary_disconnected(
                                                        peer,
                                                        handle.id,
                                                        DisconnectReason::LocalRequest,
                                                    )
                                                    .await;
                                            }
                                        }
                                        info!(
                                            callis_id = ?id,
                                            remaining = state.primary.len(),
                                            "primary callis disconnected"
                                        );
                                        primary_active.store(!state.primary.is_empty(), Ordering::SeqCst);
                                        let has_pending = !primary_dispatch.is_empty().await;
                                        ensure_primary_dial(
                                            &session,
                                            &mut state,
                                            &dial_addr,
                                            &backend,
                                            &config,
                                            &blob,
                                            &registry,
                                            &peer_state_tx,
                                            &primary_available,
                                            &primary_dispatch,
                                            &callis_tracker,
                                            &observability,
                                            has_pending,
                                        )
                                        .await;
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                    }
                                    CallisKind::Blob => {
                                        let mut handles = Vec::new();
                                        let streams = if let Some(id) = id {
                                            blob.requeue_inflight_for_callis(id).await;
                                            let (handle, streams) = blob.remove_callis(id).await;
                                            if let Some(handle) = handle {
                                                handles.push(handle);
                                            }
                                            streams
                                        } else {
                                            blob.requeue_all_inflight().await;
                                            let (drained, streams) = blob.drain_callis().await;
                                            handles.extend(drained);
                                            streams
                                        };
                                        for handle in handles {
                                            let _ = handle.shutdown.send(true);
                                            if let Some(peer) = current_dial_addr(&dial_addr).await {
                                                observability
                                                    .blob_disconnected(
                                                        peer,
                                                        handle.id,
                                                        DisconnectReason::LocalRequest,
                                                    )
                                                    .await;
                                            }
                                        }
                                        blob.reassign_streams(streams).await;
                                        let has_callis = blob.has_callis().await;
                                        info!(
                                            callis_id = ?id,
                                            has_callis,
                                            "blob callis disconnected"
                                        );
                                        state.dialing_blob = false;
                                        if state.closing {
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        }
                                        if !primary_active.load(Ordering::SeqCst) {
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        }
                                        if !blob.has_callis().await && blob.has_active_streams().await {
                                            let delay = compute_reconnect_delay(&config, state.blob_reconnect_attempt).await;
                                            state.blob_reconnect_attempt =
                                                state.blob_reconnect_attempt.saturating_add(1);
                                            if !has_callis_capacity(&config, &callis_tracker).await {
                                                state.dialing_blob = false;
                                                update_impaired_since(
                                                    &mut state,
                                                    &blob,
                                                    &session,
                                                )
                                                .await;
                                                publish_snapshot(&snapshot_tx, &state);
                                                continue;
                                            }
                                            state.dialing_blob = true;
                                            if let Some(addr) = current_dial_addr(&dial_addr).await {
                                                spawn_blob_dial_task(
                                                    addr,
                                                    backend.clone(),
                                                    config.clone(),
                                                    session.clone(),
                                                    registry.clone(),
                                                    blob.clone(),
                                                    delay,
                                                    peer_state_tx.clone(),
                                                    callis_tracker.clone(),
                                                    observability.clone(),
                                                    runtime_handle.clone(),
                                                );
                                            }
                                        }
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                    }
                                }
                            }
                            PeerStateUpdate::ConnectionClosed { callis, id, reason } => {
                                let disconnect_reason = match reason {
                                    CancelReason::RemoteClose => DisconnectReason::RemoteClosed,
                                    CancelReason::LocalShutdown => DisconnectReason::Shutdown,
                                    CancelReason::ConnectionLost | CancelReason::None => {
                                        DisconnectReason::ConnectionClosed
                                    }
                                };
                                if reason == CancelReason::RemoteClose {
                                    let _ = listener_shutdown_tx.send(true);
                                }
                                match callis {
                                    CallisKind::Primary => {
                                        if let Some(handle) = remove_primary_handle(&mut state.primary, id) {
                                            if let Some(peer) = current_dial_addr(&dial_addr).await {
                                                observability
                                                    .primary_disconnected(
                                                        peer,
                                                        handle.id,
                                                        disconnect_reason,
                                                    )
                                                    .await;
                                            }
                                        }
                                        info!(
                                            callis_id = id,
                                            remaining = state.primary.len(),
                                            "primary callis closed"
                                        );
                                        primary_active.store(!state.primary.is_empty(), Ordering::SeqCst);
                                        let has_pending = !primary_dispatch.is_empty().await;
                                        if state.primary.is_empty()
                                            && should_reconnect_primary(&session, &state, has_pending).await
                                        {
                                            let delay = if state.role == PeerRole::Listener {
                                                compute_listener_delay(&config, state.role, state.had_primary).await
                                            } else {
                                                compute_reconnect_delay(&config, state.reconnect_attempt).await
                                            };
                                            state.reconnect_attempt =
                                                state.reconnect_attempt.saturating_add(1);
                                            if !state.dialing_primary {
                                                if !has_callis_capacity(&config, &callis_tracker).await {
                                                    update_impaired_since(
                                                        &mut state,
                                                        &blob,
                                                        &session,
                                                    )
                                                    .await;
                                                    publish_snapshot(&snapshot_tx, &state);
                                                    continue;
                                                }
                                                state.dialing_primary = true;
                                                if state.role == PeerRole::Listener {
                                                    state.role = PeerRole::Originator;
                                                }
                                                if let Some(addr) = current_dial_addr(&dial_addr).await {
                                                    spawn_dial_task(
                                                        addr,
                                                        backend.clone(),
                                                        config.clone(),
                                                        session.clone(),
                                                        blob.clone(),
                                                        registry.clone(),
                                                        delay,
                                                        peer_state_tx.clone(),
                                                        Arc::clone(&primary_available),
                                                        Arc::clone(&primary_dispatch),
                                                        callis_tracker.clone(),
                                                        observability.clone(),
                                                        runtime_handle.clone(),
                                                    );
                                                }
                                            }
                                        }
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                    }
                                    CallisKind::Blob => {
                                        blob.requeue_inflight_for_callis(id).await;
                                        let (handle, streams) = blob.remove_callis(id).await;
                                        if let Some(handle) = handle {
                                            if let Some(peer) = current_dial_addr(&dial_addr).await {
                                                observability
                                                    .blob_disconnected(
                                                        peer,
                                                        handle.id,
                                                        disconnect_reason,
                                                    )
                                                    .await;
                                            }
                                        }
                                        blob.reassign_streams(streams).await;
                                        let has_callis = blob.has_callis().await;
                                        info!(
                                            callis_id = id,
                                            has_callis,
                                            "blob callis closed"
                                        );
                                        state.dialing_blob = false;
                                        if state.closing {
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        }
                                        if !primary_active.load(Ordering::SeqCst) {
                                            update_impaired_since(
                                                &mut state,
                                                &blob,
                                                &session,
                                            )
                                            .await;
                                            publish_snapshot(&snapshot_tx, &state);
                                            continue;
                                        }
                                        if !blob.has_callis().await && blob.has_active_streams().await {
                                            let delay = compute_reconnect_delay(&config, state.blob_reconnect_attempt).await;
                                            state.blob_reconnect_attempt =
                                                state.blob_reconnect_attempt.saturating_add(1);
                                            if !has_callis_capacity(&config, &callis_tracker).await {
                                                state.dialing_blob = false;
                                                update_impaired_since(
                                                    &mut state,
                                                    &blob,
                                                    &session,
                                                )
                                                .await;
                                                publish_snapshot(&snapshot_tx, &state);
                                                continue;
                                            }
                                            state.dialing_blob = true;
                                            if let Some(addr) = current_dial_addr(&dial_addr).await {
                                                spawn_blob_dial_task(
                                                    addr,
                                                    backend.clone(),
                                                    config.clone(),
                                                    session.clone(),
                                                    registry.clone(),
                                                    blob.clone(),
                                                    delay,
                                                    peer_state_tx.clone(),
                                                    callis_tracker.clone(),
                                                    observability.clone(),
                                                    runtime_handle.clone(),
                                                );
                                            }
                                        }
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                    }
                                }
                            }
                            PeerStateUpdate::EnsurePrimaryDial => {
                                let has_pending = !primary_dispatch.is_empty().await;
                                ensure_primary_dial(
                                    &session,
                                    &mut state,
                                    &dial_addr,
                                    &backend,
                                    &config,
                                    &blob,
                                    &registry,
                                    &peer_state_tx,
                                    &primary_available,
                                    &primary_dispatch,
                                    &callis_tracker,
                                    &observability,
                                    has_pending,
                                )
                                .await;
                                update_impaired_since(
                                    &mut state,
                                    &blob,
                                    &session,
                                )
                                .await;
                                publish_snapshot(&snapshot_tx, &state);
                            }
                            PeerStateUpdate::EnsureBlobDial => {
                                if state.closing {
                                    update_impaired_since(
                                        &mut state,
                                        &blob,
                                        &session,
                                    )
                                    .await;
                                    publish_snapshot(&snapshot_tx, &state);
                                    continue;
                                }
                                if !primary_active.load(Ordering::SeqCst) {
                                    update_impaired_since(
                                        &mut state,
                                        &blob,
                                        &session,
                                    )
                                    .await;
                                    publish_snapshot(&snapshot_tx, &state);
                                    continue;
                                }
                                if state.dialing_blob {
                                    update_impaired_since(
                                        &mut state,
                                        &blob,
                                        &session,
                                    )
                                    .await;
                                    publish_snapshot(&snapshot_tx, &state);
                                    continue;
                                }
                                if blob.has_callis().await {
                                    update_impaired_since(
                                        &mut state,
                                        &blob,
                                        &session,
                                    )
                                    .await;
                                    publish_snapshot(&snapshot_tx, &state);
                                    continue;
                                }
                                if blob.has_active_streams().await {
                                    let delay = compute_reconnect_delay(&config, state.blob_reconnect_attempt).await;
                                    state.blob_reconnect_attempt =
                                        state.blob_reconnect_attempt.saturating_add(1);
                                    if !has_callis_capacity(&config, &callis_tracker).await {
                                        state.dialing_blob = false;
                                        update_impaired_since(
                                            &mut state,
                                            &blob,
                                            &session,
                                        )
                                        .await;
                                        publish_snapshot(&snapshot_tx, &state);
                                        continue;
                                    }
                                    state.dialing_blob = true;
                                    if let Some(addr) = current_dial_addr(&dial_addr).await {
                                        spawn_blob_dial_task(
                                            addr,
                                            backend.clone(),
                                            config.clone(),
                                            session.clone(),
                                            registry.clone(),
                                            blob.clone(),
                                            delay,
                                            peer_state_tx.clone(),
                                            callis_tracker.clone(),
                                            observability.clone(),
                                            runtime_handle.clone(),
                                        );
                                    }
                                }
                                update_impaired_since(
                                    &mut state,
                                    &blob,
                                    &session,
                                )
                                .await;
                                publish_snapshot(&snapshot_tx, &state);
                            }
                            PeerStateUpdate::GracefulClose => {
                                if state.closing {
                                    update_impaired_since(
                                        &mut state,
                                        &blob,
                                        &session,
                                    )
                                    .await;
                                    publish_snapshot(&snapshot_tx, &state);
                                    continue;
                                }
                                let _ = listener_shutdown_tx.send(true);
                                state.closing = true;
                                session.begin_close();
                                primary_dispatch
                                    .drain_new_on_close(AureliaError::new(ErrorId::PeerUnavailable))
                                    .await;

                                state.dialing_primary = false;
                                state.dialing_blob = false;

                                let send_timeout = config.snapshot().await.send_timeout;
                                let session_close = Arc::clone(&session);
                                let blob_close = Arc::clone(&blob);
                                let primary_dispatch_close = Arc::clone(&primary_dispatch);
                                let handles: Vec<_> = state.primary.drain(..).collect();
                                primary_active.store(false, Ordering::SeqCst);
                                runtime_handle.spawn(async move {
                                    if !wait_for_inflight_or_timeout(&session_close, send_timeout).await {
                                        warn!("graceful close timed out waiting for inflight");
                                    }
                                    for primary in handles {
                                        primary_dispatch_close
                                            .enqueue_a1_frame(OutboundFrame::Close, Some(primary.id))
                                            .await;
                                    }
                                    blob_close
                                        .fail_all_streams(AureliaError::new(ErrorId::PeerUnavailable))
                                        .await;
                                    let (blob_handles, _streams) = blob_close.drain_callis().await;
                                    for handle in blob_handles {
                                        let _ = handle.tx.send(OutboundFrame::Close).await;
                                    }
                                });
                                update_impaired_since(
                                    &mut state,
                                    &blob,
                                    &session,
                                )
                                .await;
                                publish_snapshot(&snapshot_tx, &state);
                            }
                        }
                    }
                }
            }
        });
    }

    pub(super) async fn inbound(
        &self,
        authenticated: super::backend::AuthenticatedStream<B::Stream, DomusAddr>,
    ) {
        let super::backend::AuthenticatedStream { mut stream, .. } = authenticated;
        let cfg = self.config.snapshot().await;
        let permit = match self
            .handshake_gate
            .try_acquire(&cfg, &self.inbound_handshakes)
        {
            Some(permit) => permit,
            None => {
                let _ = stream.shutdown().await;
                return;
            }
        };
        let config = self.config.clone();
        let session = Arc::clone(&self.session);
        let blob = Arc::clone(&self.blob);
        let registry = Arc::clone(&self.registry);
        let primary_active = Arc::clone(&self.primary_active);
        let primary_available = Arc::clone(&self.primary_available);
        let primary_dispatch = Arc::clone(&self.primary_dispatch);
        let peer_state_tx = self.peer_state_tx.clone();
        let callis_tracker = self.callis_tracker.clone();
        let observability = self.observability.clone();
        let dial_addr = Arc::clone(&self.dial_addr);
        let listener_shutdown_rx = self.listener_shutdown_tx.subscribe();
        self.runtime_handle.spawn(async move {
            let _permit = permit;
            match accept_inbound(
                config,
                session,
                blob,
                registry,
                primary_active,
                primary_available,
                primary_dispatch,
                stream,
                peer_state_tx.clone(),
                callis_tracker,
                listener_shutdown_rx,
            )
            .await
            {
                Ok((callis, info)) => {
                    let _ = peer_state_tx
                        .send(PeerStateUpdate::Connected { callis, info })
                        .await;
                }
                Err(err) => {
                    if let Some(peer) = current_dial_addr(&dial_addr).await {
                        match err.kind {
                            ErrorId::ProtocolViolation => {
                                observability.protocol_violation(peer, err.kind).await;
                            }
                            ErrorId::SendTimeout => {
                                observability
                                    .handshake_timeout(peer, HandshakePhase::InboundHello)
                                    .await;
                            }
                            _ => {}
                        }
                    }
                }
            }
        });
    }

    pub(super) async fn wait_for_callis_zero(&self, timeout: Duration) -> Result<(), AureliaError> {
        self.callis_tracker.wait_for_zero(timeout).await
    }

    pub(super) async fn send(
        &self,
        taberna_id: TabernaId,
        msg_type: MessageType,
        payload: Bytes,
    ) -> Result<(), AureliaError> {
        let (_message, waiter) = self
            .session
            .create_outgoing(0, taberna_id, msg_type, 0, payload)
            .await?;
        debug!(taberna_id, msg_type, "outbound message queued");
        self.session.wait_for_ack(waiter).await
    }

    pub(super) async fn ensure_blob_callis(
        &self,
    ) -> Result<(CallisId, BlobCallisSettings), AureliaError> {
        if !self.primary_active.load(Ordering::SeqCst) {
            return Err(AureliaError::new(ErrorId::BlobCallisWithoutPrimary));
        }
        if let Some((callis_id, _handle, settings)) = self.blob.select_callis().await {
            return Ok((callis_id, settings));
        }
        let _ = self
            .peer_state_tx
            .send(PeerStateUpdate::EnsureBlobDial)
            .await;
        let timeout = self.config.snapshot().await.send_timeout;
        self.blob.wait_for_callis(timeout).await?;
        let (callis_id, _handle, settings) = self
            .blob
            .select_callis()
            .await
            .ok_or_else(|| AureliaError::new(ErrorId::PeerUnavailable))?;
        Ok((callis_id, settings))
    }

    pub(super) async fn send_blob(
        &self,
        taberna_id: TabernaId,
        msg_type: MessageType,
        payload: Bytes,
    ) -> Result<crate::BlobSender, AureliaError> {
        if self.session.is_closing() {
            return Err(AureliaError::new(ErrorId::PeerUnavailable));
        }
        let cfg = self.config.snapshot().await;

        let (message, waiter) = self
            .session
            .create_outgoing(0, taberna_id, msg_type, WireFlags::BLOB.bits(), payload)
            .await?;
        let stream_id = message.peer_msg_id;
        let reservation_bytes =
            (cfg.blob_chunk_size as u64).saturating_mul(cfg.blob_ack_window as u64);
        if !self
            .blob
            .reserve_outbound(stream_id, reservation_bytes, cfg.blob_outbound_buffer_bytes)
            .await
        {
            let err = blob_buffer_full_error("outbound", cfg.blob_outbound_buffer_bytes);
            let _ = self.session.handle_error(stream_id, err.clone()).await;
            return Err(err);
        }
        debug!(
            taberna_id,
            stream_id, msg_type, "outbound blob request opened"
        );

        if let Err(err) = self.session.wait_for_ack(waiter).await {
            warn!(
                taberna_id,
                stream_id,
                error = %err,
                "outbound blob request failed before ack"
            );
            self.blob.release_outbound(stream_id).await;
            return Err(err);
        }

        let (callis_id, settings) = match self.ensure_blob_callis().await {
            Ok(settings) => settings,
            Err(err) => {
                self.blob.release_outbound(stream_id).await;
                return Err(err);
            }
        };
        if settings.chunk_size == 0 || settings.ack_window_chunks == 0 {
            let err = AureliaError::new(ErrorId::ProtocolViolation);
            self.blob.release_outbound(stream_id).await;
            return Err(err);
        }
        info!(
            taberna_id,
            stream_id, callis_id, "outbound blob request acked"
        );

        let ring = self
            .blob
            .register_outbound_stream(stream_id, callis_id, settings)
            .await?;
        let send_timeout = cfg.send_timeout;

        let start_peer_msg_id = self.allocator.next();
        let start_payload = BlobTransferStartPayload {
            request_msg_id: stream_id,
        };
        let start_frame = OutboundFrame::Control {
            msg_type: MSG_BLOB_TRANSFER_START,
            peer_msg_id: start_peer_msg_id,
            payload: Bytes::from(start_payload.to_bytes().to_vec()),
        };
        let start_deadline = Instant::now() + send_timeout;
        if let Err(err) = send_blob_control_and_wait_ack(
            &self.blob,
            &ring,
            stream_id,
            start_peer_msg_id,
            RetainedBlobKind::Start,
            start_frame,
            start_deadline,
            &self.peer_state_tx,
        )
        .await
        {
            self.blob.unregister_outbound_stream(stream_id).await;
            warn!(
                taberna_id,
                stream_id,
                error = %err,
                "outbound blob transfer start failed"
            );
            return Err(err);
        }
        let sender_stream = crate::transport::blob::io::BlobSenderStream::new(
            Arc::clone(&self.blob),
            stream_id,
            ring,
            send_timeout,
            self.runtime_handle.clone(),
        );
        Ok(crate::BlobSender::new(Box::new(sender_stream)))
    }

    pub(super) async fn shutdown(&self) {
        self.shutdown_notify.notify_waiters();
    }

    // Test-only: exercised by E2E/feature-gated harnesses.
    #[allow(dead_code)]
    pub(super) async fn disconnect(&self) {
        let _ = self
            .peer_state_tx
            .send(PeerStateUpdate::Disconnect {
                callis: CallisKind::Primary,
                id: None,
            })
            .await;
    }

    // Test-only: exercised by E2E/feature-gated harnesses.
    #[allow(dead_code)]
    pub(super) async fn disconnect_blob(&self) {
        let _ = self
            .peer_state_tx
            .send(PeerStateUpdate::Disconnect {
                callis: CallisKind::Blob,
                id: None,
            })
            .await;
    }

    pub(super) async fn graceful_close(&self) {
        self.session.begin_close();
        let _ = self
            .peer_state_tx
            .send(PeerStateUpdate::GracefulClose)
            .await;
    }

    pub(super) async fn update_dial_addr(&self, addr: DomusAddr) {
        let mut guard = self.dial_addr.lock().await;
        *guard = Some(addr);
    }
}

async fn update_impaired_since(
    state: &mut PeerState,
    blob: &Arc<BlobManager>,
    session: &Arc<PeerSession>,
) {
    if state.closing || session.is_closing() || !state.had_primary {
        state.impaired_since = None;
        return;
    }
    let all_down = state.primary.is_empty() && !blob.has_callis().await;
    if all_down {
        if state.impaired_since.is_none() {
            state.impaired_since = Some(Instant::now());
        }
    } else {
        state.impaired_since = None;
    }
}

#[allow(clippy::too_many_arguments)]
async fn teardown_peer_handle(
    state: &mut PeerState,
    primary_active: &Arc<AtomicBool>,
    session: &Arc<PeerSession>,
    primary_dispatch: &Arc<PrimaryDispatchQueue>,
    blob: &Arc<BlobManager>,
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
    observability: &ObservabilityHandle,
    disconnect_reason: DisconnectReason,
) {
    if state.closing {
        return;
    }
    state.impaired_since = None;
    state.closing = true;
    session.begin_close();
    let error = AureliaError::new(ErrorId::PeerUnavailable);
    primary_dispatch.fail_non_a1(error.clone()).await;

    state.dialing_primary = false;
    state.dialing_blob = false;
    state.reconnect_attempt = 0;
    state.blob_reconnect_attempt = 0;

    let peer = current_dial_addr(dial_addr).await;
    let handles: Vec<_> = state.primary.drain(..).collect();
    primary_active.store(false, Ordering::SeqCst);
    for handle in handles {
        let _ = handle.shutdown.send(true);
        if let Some(peer) = peer.clone() {
            observability
                .primary_disconnected(peer, handle.id, disconnect_reason)
                .await;
        }
    }
    blob.fail_all_streams(error.clone()).await;
    let (blob_handles, _streams) = blob.drain_callis().await;
    for handle in blob_handles {
        let _ = handle.shutdown.send(true);
        if let Some(peer) = peer.clone() {
            observability
                .blob_disconnected(peer, handle.id, disconnect_reason)
                .await;
        }
    }
}

fn publish_snapshot(snapshot_tx: &watch::Sender<PeerStateSnapshot>, state: &PeerState) {
    let snapshot = PeerStateSnapshot {
        primary_handles: state.primary.iter().cloned().collect(),
    };
    let _ = snapshot_tx.send(snapshot);
}

fn spawn_send_close(
    primary_dispatch: Arc<PrimaryDispatchQueue>,
    callis: CallisKind,
    handles: Vec<CallisHandle>,
    runtime_handle: tokio::runtime::Handle,
) {
    if handles.is_empty() {
        return;
    }
    runtime_handle.spawn(async move {
        match callis {
            CallisKind::Primary => {
                for handle in handles {
                    primary_dispatch
                        .enqueue_a1_frame(OutboundFrame::Close, Some(handle.id))
                        .await;
                }
            }
            CallisKind::Blob => {
                for handle in handles {
                    let _ = handle.tx.send(OutboundFrame::Close).await;
                }
            }
        }
    });
}
