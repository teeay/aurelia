// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::observability::{
    BlobCallisSettingsReport, DisconnectReason, HandshakePhase, ObservabilityHandle,
};
use crate::session::CancelReason;

pub(super) struct PeerHandle<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    dial_addr: Arc<Mutex<Option<DomusAddr>>>,
    primary_active: Arc<AtomicBool>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    config: DomusConfigAccess,
    backend: Arc<B>,
    callis_tracker: CallisTracker,
    handshake_gate: HandshakeGate,
    inbound_handshakes: Arc<AtomicUsize>,
    pub(super) session: Arc<PeerSession>,
    pub(super) peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    shutdown_rx: watch::Receiver<bool>,
    listener_shutdown_tx: watch::Sender<bool>,
    peer_shutdown_tx: watch::Sender<bool>,
    observability: ObservabilityHandle,
    runtime_handle: tokio::runtime::Handle,
    _task_set: Arc<PeerTaskSet>,
    task_spawner: PeerTaskSpawner,
}

struct PeerRuntime<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    handle: PeerHandle<B>,
    #[cfg(test)]
    snapshot_rx: watch::Receiver<PeerStateSnapshot>,
}

struct PeerRuntimeParts<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
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
}

struct OutboundReservation {
    blob: Arc<BlobManager>,
    stream_id: PeerMessageId,
    runtime_handle: tokio::runtime::Handle,
    active: bool,
}

impl OutboundReservation {
    fn new(
        blob: Arc<BlobManager>,
        stream_id: PeerMessageId,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            blob,
            stream_id,
            runtime_handle,
            active: true,
        }
    }

    fn commit(&mut self) {
        self.active = false;
    }

    async fn release(&mut self) {
        if self.active {
            self.active = false;
            self.blob.release_outbound(self.stream_id).await;
        }
    }
}

impl Drop for OutboundReservation {
    fn drop(&mut self) {
        if self.active {
            let blob = Arc::clone(&self.blob);
            let stream_id = self.stream_id;
            self.runtime_handle.spawn(async move {
                blob.release_outbound(stream_id).await;
            });
        }
    }
}

impl<B> PeerRuntime<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    fn new(parts: PeerRuntimeParts<B>) -> Self {
        let PeerRuntimeParts {
            dial_addr,
            registry,
            config,
            blob_buffers,
            backend,
            handshake_gate,
            observability,
            shutdown_rx,
            listener_shutdown_tx,
            runtime_handle,
        } = parts;

        let allocator = Arc::new(PeerMessageIdAllocator::default());
        let task_set = PeerTaskSet::new(&runtime_handle);
        let task_spawner = task_set.spawner();
        let dial_addr = Arc::new(Mutex::new(dial_addr));
        let overrun_reporter = OutboundQueueOverrunReporter {
            peer: Arc::clone(&dial_addr),
            config: config.clone(),
            observability: observability.clone(),
        };
        let (primary_dispatch, primary_tasks) =
            PrimaryDispatchManager::new(PrimaryDispatchManagerContext {
                initial_send_queue_size: config.send_queue_size(),
                overrun_reporter: Some(overrun_reporter),
            });
        let session = Arc::new(PeerSession::new(
            Arc::clone(&allocator),
            config.clone(),
            runtime_handle.clone(),
            Arc::clone(&primary_dispatch),
        ));
        let blob_available = Arc::new(Notify::new());
        let blob = Arc::new(BlobManager::new(
            Arc::clone(&blob_buffers),
            Arc::clone(&blob_available),
            Arc::clone(&allocator),
            config.send_queue_size(),
        ));
        task_spawner.spawn({
            let blob = Arc::clone(&blob);
            let shutdown_rx = shutdown_rx.clone();
            async move {
                blob.run_inbound_idle_reaper(shutdown_rx).await;
            }
        });
        let callis_tracker = CallisTracker::new();
        let inbound_handshakes = Arc::new(AtomicUsize::new(0));
        let (peer_state_tx, peer_state_rx) = mpsc::channel(256);
        let (snapshot_tx, snapshot_rx) = watch::channel(PeerStateSnapshot::new());
        let (peer_shutdown_tx, peer_shutdown_rx) = watch::channel(false);
        let handle = PeerHandle {
            dial_addr,
            primary_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
            peer_shutdown_tx,
            observability,
            runtime_handle,
            _task_set: task_set,
            task_spawner,
        };
        handle
            .task_spawner
            .spawn(async move { primary_tasks.run().await });
        handle.spawn_state_task(peer_state_rx, snapshot_tx, peer_shutdown_rx);
        #[cfg(not(test))]
        let _ = snapshot_rx;
        Self {
            handle,
            #[cfg(test)]
            snapshot_rx,
        }
    }

    fn into_handle(self) -> PeerHandle<B> {
        self.handle
    }

    #[cfg(test)]
    fn into_handle_and_snapshot(self) -> (PeerHandle<B>, watch::Receiver<PeerStateSnapshot>) {
        (self.handle, self.snapshot_rx)
    }
}

struct PeerStateTaskContext<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    dial_addr: Arc<Mutex<Option<DomusAddr>>>,
    primary_active: Arc<AtomicBool>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    config: DomusConfigAccess,
    backend: Arc<B>,
    session: Arc<PeerSession>,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    primary_dispatch: Arc<PrimaryDispatchManager>,
    callis_tracker: CallisTracker,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
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
#[allow(dead_code)]
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
    pub(super) replay: Vec<PeerMessage>,
    pub(super) fresh_session: bool,
    pub(super) blob_settings: Option<BlobCallisSettings>,
    pub(super) blob_resume: bool,
}

pub(super) struct PeerState {
    pub(super) role: PeerRole,
    pub(super) had_primary: bool,
    pub(super) primary: VecDeque<CallisHandle>,
    pub(super) reconnect_attempt: usize,
    pub(super) primary_dial: PrimaryDialState,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PrimaryDialState {
    Idle,
    Dialing,
}

/// Effects are intentionally separate from the peer state machine. The machine owns state
/// transitions; this value is the unordered set of side effects that the peer owner executes after
/// the state lock is released. Use an enum only if the effects become mutually exclusive or
/// order-constrained.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct PeerStateEffects {
    pub(super) publish_snapshot: bool,
    pub(super) spawn_primary_dial: bool,
    pub(super) spawn_blob_dial: bool,
    pub(super) close_new_primary: bool,
    pub(super) close_new_blob: bool,
    pub(super) promote_listener_to_originator: bool,
    pub(super) enqueue_primary_close: bool,
    pub(super) shutdown_primary_handle: bool,
    pub(super) shutdown_blob_handle: bool,
    pub(super) fail_tracked: bool,
    pub(super) fail_blob_streams: bool,
}

#[derive(Default)]
struct PeerStatePostEffects {
    observability: Vec<PeerObservabilityEffect>,
}

enum PeerObservabilityEffect {
    PrimaryConnected {
        peer: DomusAddr,
        callis_id: CallisId,
        fresh_session: bool,
    },
    BlobConnected {
        peer: DomusAddr,
        callis_id: CallisId,
        settings: BlobCallisSettingsReport,
    },
    PrimaryDisconnected {
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    },
    BlobDisconnected {
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    },
}

impl PeerStatePostEffects {
    fn primary_connected(&mut self, peer: DomusAddr, callis_id: CallisId, fresh_session: bool) {
        self.observability
            .push(PeerObservabilityEffect::PrimaryConnected {
                peer,
                callis_id,
                fresh_session,
            });
    }

    fn blob_connected(
        &mut self,
        peer: DomusAddr,
        callis_id: CallisId,
        settings: BlobCallisSettingsReport,
    ) {
        self.observability
            .push(PeerObservabilityEffect::BlobConnected {
                peer,
                callis_id,
                settings,
            });
    }

    fn primary_disconnected(
        &mut self,
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    ) {
        self.observability
            .push(PeerObservabilityEffect::PrimaryDisconnected {
                peer,
                callis_id,
                reason,
            });
    }

    fn blob_disconnected(
        &mut self,
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    ) {
        self.observability
            .push(PeerObservabilityEffect::BlobDisconnected {
                peer,
                callis_id,
                reason,
            });
    }

    fn emit(self, observability: &ObservabilityHandle) {
        for effect in self.observability {
            match effect {
                PeerObservabilityEffect::PrimaryConnected {
                    peer,
                    callis_id,
                    fresh_session,
                } => {
                    observability.primary_connected(peer, callis_id, fresh_session);
                }
                PeerObservabilityEffect::BlobConnected {
                    peer,
                    callis_id,
                    settings,
                } => {
                    observability.blob_connected(peer, callis_id, settings);
                }
                PeerObservabilityEffect::PrimaryDisconnected {
                    peer,
                    callis_id,
                    reason,
                } => {
                    observability.primary_disconnected(peer, callis_id, reason);
                }
                PeerObservabilityEffect::BlobDisconnected {
                    peer,
                    callis_id,
                    reason,
                } => {
                    observability.blob_disconnected(peer, callis_id, reason);
                }
            }
        }
    }
}

pub(super) struct PeerStateMachine;

impl PeerStateMachine {
    pub(super) fn ensure_primary_dial(
        state: &mut PeerState,
        has_primary_work: bool,
        has_capacity: bool,
    ) -> PeerStateEffects {
        if state.closing {
            return PeerStateEffects::default();
        }
        if !state.primary.is_empty() {
            return PeerStateEffects::default();
        }
        if state.primary_dial == PrimaryDialState::Dialing || !has_primary_work || !has_capacity {
            return PeerStateEffects::default();
        }
        state.primary_dial = PrimaryDialState::Dialing;
        PeerStateEffects {
            spawn_primary_dial: true,
            promote_listener_to_originator: state.role == PeerRole::Listener,
            ..Default::default()
        }
    }

    pub(super) fn connected_primary(
        state: &mut PeerState,
        session_closing: bool,
        fresh_session: bool,
    ) -> PeerStateEffects {
        if state.closing || session_closing {
            return PeerStateEffects {
                close_new_primary: true,
                ..Default::default()
            };
        }
        state.had_primary = true;
        state.reconnect_attempt = 0;
        state.primary_dial = PrimaryDialState::Idle;
        PeerStateEffects {
            publish_snapshot: true,
            fail_tracked: fresh_session,
            fail_blob_streams: fresh_session,
            ..Default::default()
        }
    }

    pub(super) fn connected_blob(
        state: &mut PeerState,
        has_blob_settings: bool,
    ) -> PeerStateEffects {
        if state.closing {
            return PeerStateEffects {
                close_new_blob: true,
                ..Default::default()
            };
        }
        state.dialing_blob = false;
        state.blob_reconnect_attempt = 0;
        if !has_blob_settings {
            return PeerStateEffects {
                close_new_blob: true,
                ..Default::default()
            };
        }
        PeerStateEffects::default()
    }

    pub(super) fn dial_failed_primary(
        state: &mut PeerState,
        has_primary_work: bool,
        has_capacity: bool,
    ) -> PeerStateEffects {
        if !state.primary.is_empty() {
            state.primary_dial = PrimaryDialState::Idle;
            return PeerStateEffects::default();
        }
        state.primary_dial = PrimaryDialState::Idle;
        Self::ensure_primary_dial(state, has_primary_work, has_capacity)
    }

    pub(super) fn primary_removed(
        state: &mut PeerState,
        has_primary_work: bool,
        has_capacity: bool,
    ) -> PeerStateEffects {
        if !state.primary.is_empty() {
            state.primary_dial = PrimaryDialState::Idle;
            return PeerStateEffects {
                publish_snapshot: true,
                ..Default::default()
            };
        }
        state.primary_dial = PrimaryDialState::Idle;
        let mut effects = Self::ensure_primary_dial(state, has_primary_work, has_capacity);
        effects.publish_snapshot = true;
        effects
    }

    pub(super) fn ensure_blob_dial(
        state: &mut PeerState,
        has_primary: bool,
        has_callis: bool,
        has_active_streams: bool,
        has_capacity: bool,
    ) -> PeerStateEffects {
        if state.closing
            || !has_primary
            || state.dialing_blob
            || has_callis
            || !has_active_streams
            || !has_capacity
        {
            return PeerStateEffects::default();
        }
        state.dialing_blob = true;
        PeerStateEffects {
            spawn_blob_dial: true,
            ..Default::default()
        }
    }

    pub(super) fn dial_failed_blob(
        state: &mut PeerState,
        has_primary: bool,
        has_callis: bool,
        has_active_streams: bool,
        has_capacity: bool,
    ) -> PeerStateEffects {
        state.dialing_blob = false;
        Self::ensure_blob_dial(
            state,
            has_primary,
            has_callis,
            has_active_streams,
            has_capacity,
        )
    }

    pub(super) fn blob_removed(
        state: &mut PeerState,
        has_primary: bool,
        has_callis: bool,
        has_active_streams: bool,
        has_capacity: bool,
    ) -> PeerStateEffects {
        state.dialing_blob = false;
        Self::ensure_blob_dial(
            state,
            has_primary,
            has_callis,
            has_active_streams,
            has_capacity,
        )
    }

    pub(super) fn graceful_close(state: &mut PeerState) -> PeerStateEffects {
        if state.closing {
            return PeerStateEffects::default();
        }
        state.closing = true;
        state.primary_dial = PrimaryDialState::Idle;
        state.dialing_blob = false;
        PeerStateEffects {
            publish_snapshot: true,
            enqueue_primary_close: true,
            fail_tracked: true,
            fail_blob_streams: true,
            ..Default::default()
        }
    }
}

#[derive(Clone)]
pub(super) struct CallisHandle {
    pub(super) id: CallisId,
    pub(super) tx: CallisTx,
    pub(super) shutdown: watch::Sender<bool>,
}

#[derive(Clone)]
pub(super) enum CallisTx {
    Primary,
    Blob,
}

impl CallisTx {
    pub(super) fn is_closed(&self) -> bool {
        match self {
            Self::Primary => true,
            Self::Blob => false,
        }
    }
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
}

async fn has_callis_capacity(config: &DomusConfigAccess, callis_tracker: &CallisTracker) -> bool {
    let max_parallel = config.snapshot().await.max_parallel_callis_per_peer.max(1);
    callis_tracker.count() < max_parallel
}

// These peer executor helpers keep their dependencies explicit even when the argument list is
// long. A context bag would hide ownership authority across session, dispatch, blob, backend, and
// observability state; explicit parameters make each side effect's reach visible at the call site.
/// Capacity check + reservation slot setup + spawn a blob dial task. Returns `true` if the
/// dial was scheduled (or skipped because no addr is known); `false` if capacity is
/// exhausted (caller should bail out of its arm). Centralises the four near-identical
/// blob dial scheduling sites.
#[allow(clippy::too_many_arguments)]
async fn schedule_blob_dial<B>(
    state: &mut PeerState,
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
    backend: &Arc<B>,
    config: &DomusConfigAccess,
    session: &Arc<PeerSession>,
    registry: &Arc<TabernaRegistry>,
    blob: &Arc<BlobManager>,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    callis_tracker: &CallisTracker,
    observability: &ObservabilityHandle,
    task_spawner: &PeerTaskSpawner,
) -> bool
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    let delay = compute_reconnect_delay(config, state.blob_reconnect_attempt).await;
    state.blob_reconnect_attempt = state.blob_reconnect_attempt.saturating_add(1);
    if !has_callis_capacity(config, callis_tracker).await {
        state.dialing_blob = false;
        return false;
    }
    state.dialing_blob = true;
    if let Some(addr) = current_dial_addr(dial_addr).await {
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
            task_spawner.clone(),
        );
    }
    true
}

/// Capacity check + reservation slot setup + spawn a primary dial task. Returns `true` if
/// the dial was scheduled (or skipped because no addr is known); `false` if capacity is
/// exhausted (caller should bail).
#[allow(clippy::too_many_arguments)]
async fn launch_primary_dial<B>(
    state: &mut PeerState,
    delay: Duration,
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
    backend: &Arc<B>,
    config: &DomusConfigAccess,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
    registry: &Arc<TabernaRegistry>,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    primary_active: &Arc<AtomicBool>,
    primary_dispatch: &Arc<PrimaryDispatchManager>,
    callis_tracker: &CallisTracker,
    observability: &ObservabilityHandle,
    task_spawner: &PeerTaskSpawner,
) -> bool
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    if !has_callis_capacity(config, callis_tracker).await {
        state.primary_dial = PrimaryDialState::Idle;
        return false;
    }
    state.primary_dial = PrimaryDialState::Dialing;
    if let Some(addr) = current_dial_addr(dial_addr).await {
        spawn_dial_task(
            addr,
            backend.clone(),
            config.clone(),
            session.clone(),
            blob.clone(),
            registry.clone(),
            delay,
            peer_state_tx.clone(),
            Arc::clone(primary_active),
            Arc::clone(primary_dispatch),
            callis_tracker.clone(),
            observability.clone(),
            task_spawner.clone(),
        );
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn execute_primary_dial_effect<B>(
    state: &mut PeerState,
    effects: &PeerStateEffects,
    delay: Duration,
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
    backend: &Arc<B>,
    config: &DomusConfigAccess,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
    registry: &Arc<TabernaRegistry>,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    primary_active: &Arc<AtomicBool>,
    primary_dispatch: &Arc<PrimaryDispatchManager>,
    callis_tracker: &CallisTracker,
    observability: &ObservabilityHandle,
    task_spawner: &PeerTaskSpawner,
) -> bool
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    if !effects.spawn_primary_dial {
        return true;
    }
    state.reconnect_attempt = state.reconnect_attempt.saturating_add(1);
    if !launch_primary_dial(
        state,
        delay,
        dial_addr,
        backend,
        config,
        session,
        blob,
        registry,
        peer_state_tx,
        primary_active,
        primary_dispatch,
        callis_tracker,
        observability,
        task_spawner,
    )
    .await
    {
        return false;
    }
    if effects.promote_listener_to_originator {
        state.role = PeerRole::Originator;
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn execute_blob_dial_effect<B>(
    state: &mut PeerState,
    effects: &PeerStateEffects,
    dial_addr: &Arc<Mutex<Option<DomusAddr>>>,
    backend: &Arc<B>,
    config: &DomusConfigAccess,
    session: &Arc<PeerSession>,
    registry: &Arc<TabernaRegistry>,
    blob: &Arc<BlobManager>,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    callis_tracker: &CallisTracker,
    observability: &ObservabilityHandle,
    task_spawner: &PeerTaskSpawner,
) -> bool
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    if !effects.spawn_blob_dial {
        return true;
    }
    schedule_blob_dial(
        state,
        dial_addr,
        backend,
        config,
        session,
        registry,
        blob,
        peer_state_tx,
        callis_tracker,
        observability,
        task_spawner,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_fresh_primary_restart_effect(
    state: &mut PeerState,
    peer: Option<DomusAddr>,
    primary_active: &Arc<AtomicBool>,
    primary_dispatch: &Arc<PrimaryDispatchManager>,
    blob: &Arc<BlobManager>,
    task_spawner: &PeerTaskSpawner,
    post_effects: &mut PeerStatePostEffects,
) {
    let drained: Vec<_> = state.primary.drain(..).collect();
    primary_active.store(false, Ordering::SeqCst);
    primary_dispatch.clear().await;
    if let Some(peer_addr) = peer.clone() {
        for handle in &drained {
            post_effects.primary_disconnected(
                peer_addr.clone(),
                handle.id,
                DisconnectReason::PeerRestarted,
            );
        }
    }
    spawn_send_close(
        Arc::clone(primary_dispatch),
        CallisKind::Primary,
        drained,
        task_spawner.clone(),
    );
    blob.fail_all_streams(AureliaError::new(ErrorId::PeerRestarted))
        .await;
    let blob_handles = blob.drain_callis().await;
    blob.reset_callis_history();
    for handle in blob_handles {
        if let Some(peer_addr) = peer.clone() {
            post_effects.blob_disconnected(peer_addr, handle.id, DisconnectReason::PeerRestarted);
        }
        let _ = handle.shutdown.send(true);
    }
}

async fn execute_graceful_close_effect(
    state: &PeerState,
    session: &Arc<PeerSession>,
    primary_dispatch: &Arc<PrimaryDispatchManager>,
    blob: &Arc<BlobManager>,
    config: &DomusConfigAccess,
    task_spawner: &PeerTaskSpawner,
) {
    session.begin_close();
    primary_dispatch
        .begin_shutdown(AureliaError::new(ErrorId::PeerUnavailable))
        .await;

    let send_timeout = config.snapshot().await.send_timeout;
    let blob_close = Arc::clone(blob);
    let primary_dispatch_close = Arc::clone(primary_dispatch);
    let handles: Vec<_> = state.primary.iter().cloned().collect();
    task_spawner.spawn(async move {
        let deadline = Instant::now() + send_timeout;
        let drained = primary_dispatch_close
            .wait_for_a1_response_empty(deadline)
            .await;
        if drained {
            for primary in &handles {
                primary_dispatch_close.request_close(primary.id).await;
            }
        } else {
            warn!("graceful close timed out waiting for retained A1 responses");
            primary_dispatch_close.drop_a1_responses().await;
            for primary in &handles {
                let _ = primary.shutdown.send(true);
            }
        }
        blob_close
            .fail_all_streams(AureliaError::new(ErrorId::PeerUnavailable))
            .await;
        let blob_handles = blob_close.drain_callis().await;
        for handle in blob_handles {
            let _ = handle.shutdown.send(true);
        }
    });
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
        PeerRuntime::new(PeerRuntimeParts {
            dial_addr,
            registry,
            config,
            blob_buffers,
            backend,
            handshake_gate,
            observability,
            shutdown_rx,
            listener_shutdown_tx,
            runtime_handle,
        })
        .into_handle()
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_for_tests(
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
    ) -> (Self, watch::Receiver<PeerStateSnapshot>) {
        PeerRuntime::new(PeerRuntimeParts {
            dial_addr,
            registry,
            config,
            blob_buffers,
            backend,
            handshake_gate,
            observability,
            shutdown_rx,
            listener_shutdown_tx,
            runtime_handle,
        })
        .into_handle_and_snapshot()
    }

    fn spawn_state_task(
        &self,
        mut peer_state_rx: mpsc::Receiver<PeerStateUpdate>,
        snapshot_tx: watch::Sender<PeerStateSnapshot>,
        mut peer_shutdown_rx: watch::Receiver<bool>,
    ) {
        let dial_addr = Arc::clone(&self.dial_addr);
        let primary_active = Arc::clone(&self.primary_active);
        let blob = Arc::clone(&self.blob);
        let registry = Arc::clone(&self.registry);
        let config = self.config.clone();
        let backend = Arc::clone(&self.backend);
        let session = Arc::clone(&self.session);
        let mut shutdown_rx = self.shutdown_rx.clone();
        let peer_state_tx = self.peer_state_tx.clone();
        let primary_dispatch = Arc::clone(&self.primary_dispatch);
        let callis_tracker = self.callis_tracker.clone();
        let observability = self.observability.clone();
        let task_spawner = self.task_spawner.clone();
        self.task_spawner.spawn(async move {
            let mut state = PeerState {
                role: PeerRole::Listener,
                had_primary: false,
                primary: VecDeque::new(),
                reconnect_attempt: 0,
                primary_dial: PrimaryDialState::Idle,
                dialing_blob: false,
                blob_reconnect_attempt: 0,
                closing: false,
                impaired_since: None,
            };
            let ctx = PeerStateTaskContext {
                dial_addr: Arc::clone(&dial_addr),
                primary_active: Arc::clone(&primary_active),
                blob: Arc::clone(&blob),
                registry: Arc::clone(&registry),
                config: config.clone(),
                backend: Arc::clone(&backend),
                session: Arc::clone(&session),
                peer_state_tx: peer_state_tx.clone(),
                primary_dispatch: Arc::clone(&primary_dispatch),
                callis_tracker: callis_tracker.clone(),
                observability: observability.clone(),
                task_spawner: task_spawner.clone(),
            };
            publish_snapshot(&snapshot_tx, &state);
            loop {
                let mut post_effects = PeerStatePostEffects::default();
                let send_timeout = config.snapshot().await.send_timeout;
                let impaired_deadline = state.impaired_since.map(|since| since + send_timeout);
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = peer_shutdown_rx.changed() => {
                        if *peer_shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = async {
                        if let Some(deadline) = impaired_deadline {
                            tokio::time::sleep_until(deadline).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {
                        // Tear down only if the impaired conditions still hold. Epilogue
                        // handles update_impaired_since / publish_snapshot for all paths.
                        let blob_snapshot = blob.lifecycle_snapshot().await;
                        if state.impaired_since.is_some()
                            && !state.closing
                            && !session.is_closing()
                            && state.primary.is_empty()
                            && !blob_snapshot.has_callis
                        {
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
                        }
                    }
                    update = peer_state_rx.recv() => {
                        let Some(update) = update else { break; };
                        ctx.execute_update(&mut state, update, &mut post_effects).await;
                    }
                }
                // Loop epilogue (docs/concurrency.md Pattern 5): runs after every
                // non-shutdown select arm to maintain the impaired_since invariant and
                // publish the latest peer-state snapshot.
                update_impaired_since(&mut state, &blob, &session).await;
                publish_snapshot(&snapshot_tx, &state);
                post_effects.emit(&observability);
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
        let primary_dispatch = Arc::clone(&self.primary_dispatch);
        let peer_state_tx = self.peer_state_tx.clone();
        let callis_tracker = self.callis_tracker.clone();
        let observability = self.observability.clone();
        let dial_addr = Arc::clone(&self.dial_addr);
        let listener_shutdown_rx = self.listener_shutdown_tx.subscribe();
        let task_spawner = self.task_spawner.clone();
        self.task_spawner.spawn(async move {
            let _permit = permit;
            let peer = current_dial_addr(&dial_addr).await;
            match accept_inbound_with_observability(
                peer.clone(),
                config,
                session,
                blob,
                registry,
                primary_active,
                primary_dispatch,
                stream,
                peer_state_tx.clone(),
                callis_tracker,
                listener_shutdown_rx,
                observability.clone(),
                task_spawner,
            )
            .await
            {
                Ok((callis, info)) => {
                    let _ = peer_state_tx
                        .send(PeerStateUpdate::Connected { callis, info })
                        .await;
                }
                Err(err) => {
                    if let Some(peer) = peer {
                        match err.kind {
                            ErrorId::ProtocolViolation => {
                                observability.protocol_violation(peer, err.kind);
                            }
                            ErrorId::SendTimeout => {
                                observability.handshake_timeout(peer, HandshakePhase::Inbound);
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
        let (_message, waiter) = match self
            .session
            .create_outgoing(0, taberna_id, msg_type, 0, payload)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return Err(err);
            }
        };
        debug!(taberna_id, msg_type, "outbound message queued");
        try_signal_peer_state_ensure(&self.peer_state_tx, PeerStateUpdate::EnsurePrimaryDial)?;
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
        try_signal_peer_state_ensure(&self.peer_state_tx, PeerStateUpdate::EnsureBlobDial)?;
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

        let (message, waiter) = match self
            .session
            .create_outgoing(0, taberna_id, msg_type, WireFlags::BLOB.bits(), payload)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return Err(err);
            }
        };
        let stream_id = message.peer_msg_id;
        try_signal_peer_state_ensure(&self.peer_state_tx, PeerStateUpdate::EnsurePrimaryDial)?;
        let reservation_bytes = (cfg.blob_window.chunk_size() as u64)
            .saturating_mul(cfg.blob_window.ack_window() as u64);
        if !self
            .blob
            .reserve_outbound(stream_id, reservation_bytes, cfg.blob_outbound_buffer_bytes)
            .await
        {
            let err = blob_buffer_full_error("outbound", cfg.blob_outbound_buffer_bytes);
            let _ = self.session.handle_error(stream_id, err.clone()).await;
            return Err(err);
        }
        let mut reservation = OutboundReservation::new(
            Arc::clone(&self.blob),
            stream_id,
            self.runtime_handle.clone(),
        );
        debug!(
            taberna_id,
            stream_id, msg_type, "outbound blob request opened"
        );

        let sender = async {
            if let Err(err) = self.session.wait_for_ack(waiter).await {
                warn!(
                    taberna_id,
                    stream_id,
                    error = %err,
                    "outbound blob request failed before ack"
                );
                return Err(err);
            }

            let (callis_id, settings) = self.ensure_blob_callis().await?;
            if settings.chunk_size == 0 || settings.ack_window_chunks == 0 {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            info!(
                taberna_id,
                stream_id, callis_id, "outbound blob request acked"
            );

            let ring = self
                .blob
                .register_outbound_stream(stream_id, settings)
                .await?;
            let send_timeout = cfg.send_timeout;

            let sender_stream = crate::transport::blob::io::BlobSenderStream::new(
                Arc::clone(&self.blob),
                stream_id,
                ring,
                send_timeout,
                self.runtime_handle.clone(),
            );
            reservation.commit();
            Ok(crate::BlobSender::new(Box::new(sender_stream)))
        }
        .await;

        if sender.is_err() {
            reservation.release().await;
        }
        sender
    }

    pub(super) async fn shutdown(&self) {
        let _ = self.peer_shutdown_tx.send(true);
    }

    async fn disconnect_callis(&self, callis: CallisKind) {
        let _ = self
            .peer_state_tx
            .send(PeerStateUpdate::Disconnect { callis, id: None })
            .await;
    }

    // Test-only: exercised by E2E/feature-gated harnesses.
    #[allow(dead_code)]
    pub(super) async fn disconnect(&self) {
        self.disconnect_callis(CallisKind::Primary).await;
    }

    // Test-only: exercised by E2E/feature-gated harnesses.
    #[allow(dead_code)]
    pub(super) async fn disconnect_blob(&self) {
        self.disconnect_callis(CallisKind::Blob).await;
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

#[cfg(test)]
impl<B> PeerHandle<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    pub(super) fn blob_for_tests(&self) -> Arc<BlobManager> {
        Arc::clone(&self.blob)
    }

    pub(super) fn open_callis_for_tests(&self) {
        self.callis_tracker.open();
    }
}

impl<B> Drop for PeerHandle<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    fn drop(&mut self) {
        // listener_shutdown_tx belongs to the Transport and is fired only by
        // Transport::shutdown. A peer handle being torn down (e.g. replaced by
        // peer_handle_for after a graceful close) must not stop the global
        // listener — that would refuse subsequent inbound dials and block the
        // peer's TCP callback during re-dial after the peer restarts.
        let _ = self.peer_shutdown_tx.send(true);
    }
}

impl<B> PeerStateTaskContext<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    async fn execute_update(
        &self,
        state: &mut PeerState,
        update: PeerStateUpdate,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        match update {
            PeerStateUpdate::Connected { callis, info } => {
                self.execute_connected(state, callis, info, post_effects)
                    .await
            }
            PeerStateUpdate::DialFailed(callis) => self.execute_dial_failed(state, callis).await,
            PeerStateUpdate::Disconnect { callis, id } => {
                self.execute_disconnect(state, callis, id, post_effects)
                    .await
            }
            PeerStateUpdate::ConnectionClosed { callis, id, reason } => {
                self.execute_connection_closed(state, callis, id, reason, post_effects)
                    .await
            }
            PeerStateUpdate::EnsurePrimaryDial => self.execute_ensure_primary_dial(state).await,
            PeerStateUpdate::EnsureBlobDial => self.execute_ensure_blob_dial(state).await,
            PeerStateUpdate::GracefulClose => self.execute_graceful_close(state).await,
        }
    }

    async fn execute_connected(
        &self,
        state: &mut PeerState,
        callis: CallisKind,
        info: ConnectionInfo,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        let peer = current_dial_addr(&self.dial_addr).await;
        match callis {
            CallisKind::Primary => {
                self.execute_connected_primary(state, info, peer, post_effects)
                    .await
            }
            CallisKind::Blob => {
                self.execute_connected_blob(state, info, peer, post_effects)
                    .await
            }
        }
    }

    async fn execute_connected_primary(
        &self,
        state: &mut PeerState,
        info: ConnectionInfo,
        peer: Option<DomusAddr>,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        let effects = PeerStateMachine::connected_primary(
            state,
            self.session.is_closing(),
            info.fresh_session,
        );
        if effects.close_new_primary {
            spawn_send_close(
                Arc::clone(&self.primary_dispatch),
                CallisKind::Primary,
                vec![info.handle],
                self.task_spawner.clone(),
            );
            return false;
        }
        info!(
            callis_id = info.handle.id,
            fresh_session = info.fresh_session,
            "primary callis ready"
        );
        if effects.fail_blob_streams {
            execute_fresh_primary_restart_effect(
                state,
                peer.clone(),
                &self.primary_active,
                &self.primary_dispatch,
                &self.blob,
                &self.task_spawner,
                post_effects,
            )
            .await;
        }
        let callis_id = info.handle.id;
        state.primary.push_back(info.handle);
        self.primary_active
            .store(!state.primary.is_empty(), Ordering::SeqCst);
        if !info.replay.is_empty() {
            let replays = info
                .replay
                .into_iter()
                .map(|inflight| inflight.peer_msg_id)
                .collect();
            self.primary_dispatch
                .mark_tracked_replay_ready(replays)
                .await;
        }

        if let Some(peer) = peer {
            post_effects.primary_connected(peer, callis_id, info.fresh_session);
        }
        true
    }

    async fn execute_connected_blob(
        &self,
        state: &mut PeerState,
        info: ConnectionInfo,
        peer: Option<DomusAddr>,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        let effects = PeerStateMachine::connected_blob(state, info.blob_settings.is_some());
        if effects.close_new_blob && state.closing {
            spawn_send_close(
                Arc::clone(&self.primary_dispatch),
                CallisKind::Blob,
                vec![info.handle],
                self.task_spawner.clone(),
            );
            return false;
        }
        let Some(settings) = info.blob_settings else {
            warn!(
                callis_id = info.handle.id,
                "blob callis missing settings; closing"
            );
            spawn_send_close(
                Arc::clone(&self.primary_dispatch),
                CallisKind::Blob,
                vec![info.handle],
                self.task_spawner.clone(),
            );
            return false;
        };
        info!(
            callis_id = info.handle.id,
            chunk_size = settings.chunk_size,
            ack_window_chunks = settings.ack_window_chunks,
            resume = info.blob_resume,
            "blob callis ready"
        );
        let callis_id = info.handle.id;
        self.blob
            .add_callis(info.handle, settings, info.blob_resume)
            .await;

        if let Some(peer) = peer {
            post_effects.blob_connected(
                peer,
                callis_id,
                BlobCallisSettingsReport {
                    chunk_size: settings.chunk_size,
                    ack_window_chunks: settings.ack_window_chunks,
                },
            );
        }
        true
    }

    async fn execute_dial_failed(&self, state: &mut PeerState, callis: CallisKind) -> bool {
        warn!(callis = ?callis, "callis dial failed");
        match callis {
            CallisKind::Primary => self.execute_dial_failed_primary(state).await,
            CallisKind::Blob => self.execute_dial_failed_blob(state).await,
        }
    }

    async fn execute_disconnect(
        &self,
        state: &mut PeerState,
        callis: CallisKind,
        id: Option<CallisId>,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        match callis {
            CallisKind::Primary => {
                self.execute_primary_disconnect(
                    state,
                    id,
                    DisconnectReason::LocalRequest,
                    post_effects,
                )
                .await
            }
            CallisKind::Blob => {
                self.execute_blob_disconnect(
                    state,
                    id,
                    DisconnectReason::LocalRequest,
                    post_effects,
                )
                .await
            }
        }
    }

    async fn execute_connection_closed(
        &self,
        state: &mut PeerState,
        callis: CallisKind,
        id: CallisId,
        reason: CancelReason,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        let disconnect_reason = match reason {
            CancelReason::RemoteClose => DisconnectReason::RemoteClosed,
            CancelReason::LocalShutdown => DisconnectReason::Shutdown,
            CancelReason::ConnectionLost | CancelReason::None => DisconnectReason::ConnectionClosed,
        };
        match callis {
            CallisKind::Primary => {
                self.execute_primary_closed(state, id, disconnect_reason, post_effects)
                    .await
            }
            CallisKind::Blob => {
                self.execute_blob_closed(state, id, disconnect_reason, post_effects)
                    .await
            }
        }
    }

    async fn execute_primary_disconnect(
        &self,
        state: &mut PeerState,
        id: Option<CallisId>,
        reason: DisconnectReason,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        let mut removed = Vec::new();
        if let Some(id) = id {
            if let Some(handle) = remove_primary_handle(&mut state.primary, id) {
                self.primary_dispatch.mark_callis_replay_ready(id).await;
                removed.push(handle);
            }
        } else {
            removed.extend(state.primary.drain(..));
            for handle in &removed {
                self.primary_dispatch
                    .mark_callis_replay_ready(handle.id)
                    .await;
            }
        }
        for handle in removed {
            let _ = handle.shutdown.send(true);
            if let Some(peer) = current_dial_addr(&self.dial_addr).await {
                post_effects.primary_disconnected(peer, handle.id, reason);
            }
        }
        info!(
            callis_id = ?id,
            remaining = state.primary.len(),
            "primary callis disconnected"
        );
        self.execute_after_primary_removed(state, true).await
    }

    async fn execute_primary_closed(
        &self,
        state: &mut PeerState,
        id: CallisId,
        reason: DisconnectReason,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        if let Some(handle) = remove_primary_handle(&mut state.primary, id) {
            self.primary_dispatch.mark_callis_replay_ready(id).await;
            if let Some(peer) = current_dial_addr(&self.dial_addr).await {
                post_effects.primary_disconnected(peer, handle.id, reason);
            }
        }
        info!(
            callis_id = id,
            remaining = state.primary.len(),
            "primary callis closed"
        );
        self.execute_after_primary_removed(state, !state.closing)
            .await
    }

    async fn execute_after_primary_removed(
        &self,
        state: &mut PeerState,
        allow_new_dial: bool,
    ) -> bool {
        self.primary_active
            .store(!state.primary.is_empty(), Ordering::SeqCst);
        let has_pending = !self.primary_dispatch.is_empty().await;
        let has_primary_work = allow_new_dial
            && !self.session.is_closing()
            && (has_pending || self.session.has_inflight().await);
        let has_capacity = self.has_callis_capacity().await;
        let effects = PeerStateMachine::primary_removed(state, has_primary_work, has_capacity);
        if effects.spawn_primary_dial {
            let delay = self.primary_dial_delay(state).await;
            return self.execute_primary_dial(state, &effects, delay).await;
        }
        true
    }

    async fn execute_blob_disconnect(
        &self,
        state: &mut PeerState,
        id: Option<CallisId>,
        reason: DisconnectReason,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        let mut handles = Vec::new();
        if let Some(id) = id {
            self.blob.requeue_inflight_for_callis(id).await;
            let handle = self.blob.remove_callis(id).await;
            if let Some(handle) = handle {
                handles.push(handle);
            }
        } else {
            self.blob.requeue_all_inflight().await;
            let drained = self.blob.drain_callis().await;
            handles.extend(drained);
        }
        for handle in handles {
            let _ = handle.shutdown.send(true);
            if let Some(peer) = current_dial_addr(&self.dial_addr).await {
                post_effects.blob_disconnected(peer, handle.id, reason);
            }
        }
        let has_callis = self.blob.has_callis().await;
        info!(callis_id = ?id, has_callis, "blob callis disconnected");
        self.execute_after_blob_removed(state).await
    }

    async fn execute_blob_closed(
        &self,
        state: &mut PeerState,
        id: CallisId,
        reason: DisconnectReason,
        post_effects: &mut PeerStatePostEffects,
    ) -> bool {
        self.blob.requeue_inflight_for_callis(id).await;
        let handle = self.blob.remove_callis(id).await;
        if let Some(handle) = handle {
            if let Some(peer) = current_dial_addr(&self.dial_addr).await {
                post_effects.blob_disconnected(peer, handle.id, reason);
            }
        }
        let has_callis = self.blob.has_callis().await;
        info!(callis_id = id, has_callis, "blob callis closed");
        self.execute_after_blob_removed(state).await
    }

    async fn execute_after_blob_removed(&self, state: &mut PeerState) -> bool {
        let blob_snapshot = self.blob.lifecycle_snapshot().await;
        let has_capacity = self.has_callis_capacity().await;
        let effects = PeerStateMachine::blob_removed(
            state,
            self.primary_active.load(Ordering::SeqCst),
            blob_snapshot.has_callis,
            blob_snapshot.has_active_streams,
            has_capacity,
        );
        !effects.spawn_blob_dial || self.execute_blob_dial(state, &effects).await
    }

    async fn has_primary_work(&self) -> bool {
        let has_pending = !self.primary_dispatch.is_empty().await;
        !self.session.is_closing() && (has_pending || self.session.has_inflight().await)
    }

    async fn has_callis_capacity(&self) -> bool {
        has_callis_capacity(&self.config, &self.callis_tracker).await
    }

    async fn primary_dial_delay(&self, state: &PeerState) -> Duration {
        if state.role == PeerRole::Listener {
            compute_listener_delay(&self.config, state.role, state.had_primary).await
        } else {
            compute_reconnect_delay(&self.config, state.reconnect_attempt).await
        }
    }

    async fn execute_primary_dial(
        &self,
        state: &mut PeerState,
        effects: &PeerStateEffects,
        delay: Duration,
    ) -> bool {
        execute_primary_dial_effect(
            state,
            effects,
            delay,
            &self.dial_addr,
            &self.backend,
            &self.config,
            &self.session,
            &self.blob,
            &self.registry,
            &self.peer_state_tx,
            &self.primary_active,
            &self.primary_dispatch,
            &self.callis_tracker,
            &self.observability,
            &self.task_spawner,
        )
        .await
    }

    async fn execute_blob_dial(&self, state: &mut PeerState, effects: &PeerStateEffects) -> bool {
        execute_blob_dial_effect(
            state,
            effects,
            &self.dial_addr,
            &self.backend,
            &self.config,
            &self.session,
            &self.registry,
            &self.blob,
            &self.peer_state_tx,
            &self.callis_tracker,
            &self.observability,
            &self.task_spawner,
        )
        .await
    }

    async fn execute_ensure_primary_dial(&self, state: &mut PeerState) -> bool {
        let has_primary_work = self.has_primary_work().await;
        let has_capacity = self.has_callis_capacity().await;
        let effects = PeerStateMachine::ensure_primary_dial(state, has_primary_work, has_capacity);
        if effects.spawn_primary_dial {
            let delay = self.primary_dial_delay(state).await;
            return self.execute_primary_dial(state, &effects, delay).await;
        }
        true
    }

    async fn execute_dial_failed_primary(&self, state: &mut PeerState) -> bool {
        let has_primary_work = self.has_primary_work().await;
        let max_parallel = self
            .config
            .snapshot()
            .await
            .max_parallel_callis_per_peer
            .max(1);
        let has_capacity = self.callis_tracker.count() < max_parallel;
        let effects = PeerStateMachine::dial_failed_primary(state, has_primary_work, has_capacity);
        if effects.spawn_primary_dial {
            let delay = compute_reconnect_delay(&self.config, state.reconnect_attempt).await;
            return self.execute_primary_dial(state, &effects, delay).await;
        }
        true
    }

    async fn execute_dial_failed_blob(&self, state: &mut PeerState) -> bool {
        let blob_snapshot = self.blob.lifecycle_snapshot().await;
        let has_capacity = self.has_callis_capacity().await;
        let effects = PeerStateMachine::dial_failed_blob(
            state,
            self.primary_active.load(Ordering::SeqCst),
            blob_snapshot.has_callis,
            blob_snapshot.has_active_streams,
            has_capacity,
        );
        !effects.spawn_blob_dial || self.execute_blob_dial(state, &effects).await
    }

    async fn execute_ensure_blob_dial(&self, state: &mut PeerState) -> bool {
        let blob_snapshot = self.blob.lifecycle_snapshot().await;
        let has_capacity = self.has_callis_capacity().await;
        let effects = PeerStateMachine::ensure_blob_dial(
            state,
            self.primary_active.load(Ordering::SeqCst),
            blob_snapshot.has_callis,
            blob_snapshot.has_active_streams,
            has_capacity,
        );
        !effects.spawn_blob_dial || self.execute_blob_dial(state, &effects).await
    }

    async fn execute_graceful_close(&self, state: &mut PeerState) -> bool {
        let effects = PeerStateMachine::graceful_close(state);
        if !effects.publish_snapshot {
            return false;
        }
        execute_graceful_close_effect(
            state,
            &self.session,
            &self.primary_dispatch,
            &self.blob,
            &self.config,
            &self.task_spawner,
        )
        .await;
        true
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
    let blob_snapshot = blob.lifecycle_snapshot().await;
    let all_down = state.primary.is_empty() && !blob_snapshot.has_callis;
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
    primary_dispatch: &Arc<PrimaryDispatchManager>,
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

    state.primary_dial = PrimaryDialState::Idle;
    state.dialing_blob = false;
    state.reconnect_attempt = 0;
    state.blob_reconnect_attempt = 0;

    let peer = current_dial_addr(dial_addr).await;
    let handles: Vec<_> = state.primary.drain(..).collect();
    primary_active.store(false, Ordering::SeqCst);
    for handle in handles {
        let _ = handle.shutdown.send(true);
        if let Some(peer) = peer.clone() {
            observability.primary_disconnected(peer, handle.id, disconnect_reason);
        }
    }
    blob.fail_all_streams(error.clone()).await;
    let blob_handles = blob.drain_callis().await;
    for handle in blob_handles {
        let _ = handle.shutdown.send(true);
        if let Some(peer) = peer.clone() {
            observability.blob_disconnected(peer, handle.id, disconnect_reason);
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
    primary_dispatch: Arc<PrimaryDispatchManager>,
    callis: CallisKind,
    handles: Vec<CallisHandle>,
    task_spawner: PeerTaskSpawner,
) {
    if handles.is_empty() {
        return;
    }
    task_spawner.spawn(async move {
        // This helper is used only for handles that are not visible in peer-state
        // snapshots. Active primary handles must be closed through PrimaryDispatchManager.
        match callis {
            CallisKind::Primary => {
                for handle in handles {
                    primary_dispatch.request_close(handle.id).await;
                }
            }
            CallisKind::Blob => {
                for handle in handles {
                    let _ = handle.shutdown.send(true);
                }
            }
        }
    });
}
