// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use bytes::Bytes;
#[cfg(test)]
use tokio::sync::watch;
use tokio::sync::{oneshot, Mutex, Notify};
use tokio::time::{timeout, Instant};
use tracing::{debug, trace, warn};

use crate::config::{DomusConfig, DomusConfigAccess};
#[cfg(test)]
use crate::config::{DomusConfigBuilder, DomusConfigStore};
use crate::limiter::DynamicLimiter;
use crate::message_id::PeerMessageIdAllocator;
use crate::reliability::InflightMessage;
use crate::taberna::TabernaRegistry;
use crate::transport::primary_dispatch::PrimaryDispatchQueue;
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_ids::{MessageType, PeerMessageId, TabernaId};

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub struct BackpressureConfig {
    pub send_queue_size: usize,
    pub inflight_window: usize,
    pub send_timeout: Duration,
}

#[cfg(test)]
impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            send_queue_size: 128,
            inflight_window: 16,
            send_timeout: Duration::from_secs(30),
        }
    }
}

struct DedupeState {
    set: HashSet<PeerMessageId>,
    order: VecDeque<PeerMessageId>,
    pending: HashMap<PeerMessageId, Vec<oneshot::Sender<DedupeDecision>>>,
}

impl DedupeState {
    fn new() -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            pending: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.set.clear();
        self.order.clear();
        self.pending.clear();
    }
}

#[derive(Clone, Copy)]
struct LimitSnapshot {
    send_queue_size: usize,
    inflight_window: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerMessage {
    pub peer_msg_id: PeerMessageId,
    pub src_taberna: TabernaId,
    pub dst_taberna: TabernaId,
    pub msg_type: MessageType,
    pub flags: u16,
    pub payload: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelReason {
    None,
    ConnectionLost,
    RemoteClose,
    LocalShutdown,
}

impl CancelReason {
    pub(crate) fn should_error(self) -> bool {
        matches!(
            self,
            CancelReason::RemoteClose | CancelReason::LocalShutdown
        )
    }
}

#[derive(Debug)]
pub enum ReceiveOutcome {
    Ack(PeerMessageId),
    Error(AureliaError),
    Skip,
}

pub(crate) enum ReceiveSchedule {
    Immediate(ReceiveOutcome),
    Pending(PendingReceive),
}

pub(crate) struct PendingReceive {
    #[cfg(test)]
    pub(crate) peer_msg_id: PeerMessageId,
    pub(crate) dst_taberna: TabernaId,
    pub(crate) accept_rx: oneshot::Receiver<Result<(), AureliaError>>,
}

pub(crate) enum DedupeBegin {
    New,
    Duplicate(DedupeDecision),
}

pub(crate) enum DedupeDecision {
    Ack,
    Error(AureliaError),
    Abandoned,
}

pub struct AckWaiter {
    rx: Option<oneshot::Receiver<Result<(), AureliaError>>>,
    peer_msg_id: PeerMessageId,
    deadline: Instant,
    session: std::sync::Weak<PeerSessionInner>,
    runtime_handle: tokio::runtime::Handle,
}

impl Drop for AckWaiter {
    fn drop(&mut self) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        let peer_msg_id = self.peer_msg_id;
        let runtime_handle = self.runtime_handle.clone();
        runtime_handle.spawn(async move {
            session
                .cancel_outgoing(peer_msg_id, AureliaError::new(ErrorId::ConnectionLost))
                .await;
        });
    }
}

pub struct PeerSession {
    inner: Arc<PeerSessionInner>,
}

struct PeerSessionInner {
    allocator: Arc<PeerMessageIdAllocator>,
    dedupe: Mutex<DedupeState>,
    dispatch: Arc<PrimaryDispatchQueue>,
    send_queue: Arc<DynamicLimiter>,
    inflight_window: Arc<DynamicLimiter>,
    config: DomusConfigAccess,
    limit_snapshot: Mutex<Option<LimitSnapshot>>,
    active: AtomicBool,
    closing: AtomicBool,
    runtime_handle: tokio::runtime::Handle,
}

impl PeerSession {
    pub fn new(
        allocator: Arc<PeerMessageIdAllocator>,
        config: DomusConfigAccess,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let dispatch = PrimaryDispatchQueue::new();
        Self {
            inner: Arc::new(PeerSessionInner {
                allocator,
                dedupe: Mutex::new(DedupeState::new()),
                dispatch,
                send_queue: DynamicLimiter::new(0),
                inflight_window: DynamicLimiter::new(0),
                config,
                limit_snapshot: Mutex::new(None),
                active: AtomicBool::new(false),
                closing: AtomicBool::new(false),
                runtime_handle,
            }),
        }
    }

    pub(crate) fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.inner.runtime_handle.clone()
    }

    pub(crate) fn primary_dispatch(&self) -> Arc<PrimaryDispatchQueue> {
        Arc::clone(&self.inner.dispatch)
    }

    #[cfg(test)]
    pub fn with_backpressure(
        allocator: Arc<PeerMessageIdAllocator>,
        config: BackpressureConfig,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let peering_config = DomusConfigBuilder::new()
            .send_queue_size(config.send_queue_size)
            .inflight_window(config.inflight_window)
            .send_timeout(config.send_timeout)
            .accept_timeout(config.send_timeout)
            .build()
            .expect("valid domus config");
        let store = Arc::new(DomusConfigStore::new(peering_config));
        let config = DomusConfigAccess::new(store, None);
        Self::new(allocator, config, runtime_handle)
    }

    pub async fn create_outgoing(
        &self,
        src_taberna: TabernaId,
        dst_taberna: TabernaId,
        msg_type: MessageType,
        flags: u16,
        payload: Bytes,
    ) -> Result<(PeerMessage, AckWaiter), AureliaError> {
        if self.inner.closing.load(Ordering::SeqCst) {
            warn!("rejecting outgoing message: session closing");
            return Err(AureliaError::new(ErrorId::PeerUnavailable));
        }

        let config = self.inner.config.snapshot().await;
        self.inner.refresh_limits(&config).await;
        let deadline = Instant::now() + config.send_timeout;
        let queue_permit = match self.inner.send_queue.acquire(deadline).await {
            Ok(permit) => permit,
            Err(err) => return Err(err),
        };

        let peer_msg_id = self.inner.allocator.next();
        let message = PeerMessage {
            peer_msg_id,
            src_taberna,
            dst_taberna,
            msg_type,
            flags,
            payload,
        };
        debug!(
            peer_msg_id,
            src_taberna, dst_taberna, msg_type, flags, "outgoing message enqueued"
        );
        let rx = self
            .inner
            .dispatch
            .enqueue_new(message.clone(), deadline, queue_permit)
            .await;
        Ok((
            message,
            AckWaiter {
                rx: Some(rx),
                peer_msg_id,
                deadline,
                session: Arc::downgrade(&self.inner),
                runtime_handle: self.inner.runtime_handle.clone(),
            },
        ))
    }

    pub async fn prepare_dispatch(&self, peer_msg_id: PeerMessageId) -> Result<(), AureliaError> {
        if self.inner.closing.load(Ordering::SeqCst) {
            self.inner
                .cancel_outgoing(peer_msg_id, AureliaError::new(ErrorId::PeerUnavailable))
                .await;
            return Err(AureliaError::new(ErrorId::PeerUnavailable));
        }

        let deadline = match self.inner.dispatch.deadline(peer_msg_id).await {
            Some(deadline) => deadline,
            None => {
                let _ = self.inner.dispatch.release_queue_permit(peer_msg_id).await;
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        };
        if Instant::now() >= deadline {
            self.inner
                .fail_inflight(peer_msg_id, AureliaError::new(ErrorId::SendTimeout))
                .await;
            return Err(AureliaError::new(ErrorId::SendTimeout));
        }

        let config = self.inner.config.snapshot().await;
        self.inner.refresh_limits(&config).await;
        let inflight_permit = match self.inner.inflight_window.acquire(deadline).await {
            Ok(permit) => permit,
            Err(err) => {
                self.inner.fail_inflight(peer_msg_id, err.clone()).await;
                return Err(err);
            }
        };

        if !self
            .inner
            .dispatch
            .attach_inflight_permit(peer_msg_id, inflight_permit)
            .await
        {
            self.inner
                .fail_inflight(peer_msg_id, AureliaError::new(ErrorId::SendTimeout))
                .await;
            return Err(AureliaError::new(ErrorId::SendTimeout));
        }

        Ok(())
    }

    #[cfg(test)]
    pub async fn mark_dispatched(&self, peer_msg_id: PeerMessageId) -> Result<(), AureliaError> {
        self.prepare_dispatch(peer_msg_id).await?;
        self.commit_dispatch(peer_msg_id).await;
        Ok(())
    }

    pub async fn commit_dispatch(&self, peer_msg_id: PeerMessageId) {
        let _ = self.inner.dispatch.release_queue_permit(peer_msg_id).await;
    }

    pub async fn rollback_dispatch(&self, peer_msg_id: PeerMessageId) {
        let _ = self
            .inner
            .dispatch
            .detach_inflight_permit(peer_msg_id)
            .await;
    }

    pub async fn handle_ack(&self, peer_msg_id: PeerMessageId) -> bool {
        let acked = self.inner.dispatch.ack(peer_msg_id).await;
        if acked {
            trace!(peer_msg_id, "ack received");
        }
        acked
    }

    pub async fn handle_error(&self, peer_msg_id: PeerMessageId, error: AureliaError) -> bool {
        let failed = self
            .inner
            .dispatch
            .fail_one(peer_msg_id, error.clone())
            .await;
        if failed {
            warn!(peer_msg_id, error = %error, "message failed");
        }
        failed
    }

    pub async fn has_inflight(&self) -> bool {
        self.inner.dispatch.has_entries().await
    }

    pub async fn wait_for_inflight_empty(&self, deadline: Instant) -> bool {
        self.inner.dispatch.wait_for_inflight_empty(deadline).await
    }

    pub async fn handle_close(&self) {
        self.inner.closing.store(true, Ordering::SeqCst);
        let _ = self
            .inner
            .dispatch
            .fail_all(AureliaError::new(ErrorId::PeerUnavailable))
            .await;
        warn!("session closing: inflight cleared");
    }

    pub fn begin_close(&self) {
        self.inner.closing.store(true, Ordering::SeqCst);
    }

    pub async fn wait_for_ack(&self, mut waiter: AckWaiter) -> Result<(), AureliaError> {
        let remaining = waiter.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.inner
                .fail_inflight(waiter.peer_msg_id, AureliaError::new(ErrorId::SendTimeout))
                .await;
            warn!(peer_msg_id = waiter.peer_msg_id, "ack wait timeout");
            return Err(AureliaError::new(ErrorId::SendTimeout));
        }

        let Some(rx) = waiter.rx.take() else {
            self.inner
                .fail_inflight(
                    waiter.peer_msg_id,
                    AureliaError::new(ErrorId::ConnectionLost),
                )
                .await;
            warn!(
                peer_msg_id = waiter.peer_msg_id,
                "ack waiter receiver missing"
            );
            return Err(AureliaError::new(ErrorId::ConnectionLost));
        };
        match timeout(remaining, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.inner
                    .fail_inflight(
                        waiter.peer_msg_id,
                        AureliaError::new(ErrorId::ConnectionLost),
                    )
                    .await;
                warn!(peer_msg_id = waiter.peer_msg_id, "ack wait connection lost");
                Err(AureliaError::new(ErrorId::ConnectionLost))
            }
            Err(_) => {
                self.inner
                    .fail_inflight(waiter.peer_msg_id, AureliaError::new(ErrorId::SendTimeout))
                    .await;
                warn!(peer_msg_id = waiter.peer_msg_id, "ack wait timeout");
                Err(AureliaError::new(ErrorId::SendTimeout))
            }
        }
    }

    pub async fn handle_hello_response(&self, reconnect: bool) -> Vec<InflightMessage> {
        if reconnect {
            self.inner.dispatch.inflight_messages().await
        } else {
            let _ = self
                .inner
                .dispatch
                .fail_all(AureliaError::new(ErrorId::PeerRestarted))
                .await;
            Vec::new()
        }
    }

    pub async fn accept_hello(&self, reconnect: bool) -> bool {
        if reconnect && self.inner.active.load(Ordering::SeqCst) {
            true
        } else {
            let _ = self
                .inner
                .dispatch
                .fail_all(AureliaError::new(ErrorId::PeerRestarted))
                .await;
            self.inner.active.store(true, Ordering::SeqCst);
            let mut guard = self.inner.dedupe.lock().await;
            guard.clear();
            false
        }
    }

    #[cfg(test)]
    pub async fn mark_restarted(&self) {
        self.inner.active.store(false, Ordering::SeqCst);
        self.inner.closing.store(false, Ordering::SeqCst);
        let mut guard = self.inner.dedupe.lock().await;
        guard.clear();
    }

    pub fn is_active(&self) -> bool {
        self.inner.active.load(Ordering::SeqCst)
    }

    pub fn is_closing(&self) -> bool {
        self.inner.closing.load(Ordering::SeqCst)
    }

    pub fn set_active(&self, active: bool) {
        self.inner.active.store(active, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub async fn receive_message_cancelable(
        &self,
        message: PeerMessage,
        registry: &TabernaRegistry,
        mut cancel_rx: watch::Receiver<CancelReason>,
    ) -> ReceiveOutcome {
        let schedule = self.receive_message_schedule(message, registry, None).await;
        let pending_peer_msg_id = match &schedule {
            ReceiveSchedule::Pending(pending) => Some(pending.peer_msg_id),
            ReceiveSchedule::Immediate(_) => None,
        };
        tokio::select! {
            _ = cancel_rx.changed() => {
                let cancel_reason = *cancel_rx.borrow();
                if cancel_reason.should_error() {
                    let err = AureliaError::new(ErrorId::PeerUnavailable);
                    if let Some(peer_msg_id) = pending_peer_msg_id {
                        self.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    }
                    ReceiveOutcome::Error(err)
                } else {
                    if let Some(peer_msg_id) = pending_peer_msg_id {
                        self.dedupe_abandon(peer_msg_id).await;
                    }
                    ReceiveOutcome::Skip
                }
            }
            result = async {
                match schedule {
                    ReceiveSchedule::Immediate(outcome) => outcome,
                    ReceiveSchedule::Pending(pending) => {
                        match pending.accept_rx.await {
                            Ok(Ok(())) => {
                                self.dedupe_complete(pending.peer_msg_id, Ok(())).await;
                                ReceiveOutcome::Ack(pending.peer_msg_id)
                            }
                            Ok(Err(err)) => {
                                if err.kind == ErrorId::TabernaBusy {
                                    warn!(peer_msg_id = pending.peer_msg_id, dst_taberna = pending.dst_taberna, "taberna busy on inbound message");
                                } else {
                                    warn!(
                                        peer_msg_id = pending.peer_msg_id,
                                        dst_taberna = pending.dst_taberna,
                                        error = %err,
                                        "taberna rejected inbound message"
                                    );
                                }
                                self.dedupe_complete(pending.peer_msg_id, Err(err.clone())).await;
                                ReceiveOutcome::Error(err)
                            }
                            Err(_) => {
                                let err = AureliaError::new(ErrorId::RemoteTabernaRejected);
                                self.dedupe_complete(pending.peer_msg_id, Err(err.clone())).await;
                                ReceiveOutcome::Error(err)
                            }
                        }
                    }
                }
            } => result
        }
    }

    pub async fn receive_message_schedule(
        &self,
        message: PeerMessage,
        registry: &TabernaRegistry,
        notify: Option<Arc<Notify>>,
    ) -> ReceiveSchedule {
        if self.inner.closing.load(Ordering::SeqCst) {
            return ReceiveSchedule::Immediate(ReceiveOutcome::Error(AureliaError::new(
                ErrorId::PeerUnavailable,
            )));
        }

        debug!(
            peer_msg_id = message.peer_msg_id,
            dst_taberna = message.dst_taberna,
            msg_type = message.msg_type,
            "received inbound message"
        );

        let peer_msg_id = message.peer_msg_id;
        let dst_taberna = message.dst_taberna;
        let msg_type = message.msg_type;
        let payload = message.payload;

        let Some(sink) = registry.resolve_local(dst_taberna).await else {
            warn!(peer_msg_id, dst_taberna, "unknown taberna");
            return ReceiveSchedule::Immediate(ReceiveOutcome::Error(AureliaError::new(
                ErrorId::UnknownTaberna,
            )));
        };

        match self.dedupe_begin(peer_msg_id).await {
            DedupeBegin::Duplicate(result) => {
                trace!(peer_msg_id, "deduped inbound message");
                let outcome = match result {
                    DedupeDecision::Ack => ReceiveOutcome::Ack(peer_msg_id),
                    DedupeDecision::Error(err) => ReceiveOutcome::Error(err),
                    DedupeDecision::Abandoned => ReceiveOutcome::Skip,
                };
                return ReceiveSchedule::Immediate(outcome);
            }
            DedupeBegin::New => {}
        }

        let accept_rx = match sink.enqueue(msg_type, payload, None, notify).await {
            Ok(rx) => rx,
            Err(err) => {
                self.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                return ReceiveSchedule::Immediate(ReceiveOutcome::Error(err));
            }
        };

        ReceiveSchedule::Pending(PendingReceive {
            #[cfg(test)]
            peer_msg_id,
            dst_taberna,
            accept_rx,
        })
    }

    #[cfg(test)]
    pub async fn is_duplicate(&self, peer_msg_id: PeerMessageId) -> bool {
        let guard = self.inner.dedupe.lock().await;
        guard.set.contains(&peer_msg_id)
    }

    pub(crate) async fn dedupe_begin(&self, peer_msg_id: PeerMessageId) -> DedupeBegin {
        let mut guard = self.inner.dedupe.lock().await;
        if guard.set.contains(&peer_msg_id) {
            return DedupeBegin::Duplicate(DedupeDecision::Ack);
        }
        if let Some(waiters) = guard.pending.get_mut(&peer_msg_id) {
            let (tx, rx) = oneshot::channel();
            waiters.push(tx);
            drop(guard);
            let result = rx.await.unwrap_or(DedupeDecision::Abandoned);
            return DedupeBegin::Duplicate(result);
        }
        guard.pending.insert(peer_msg_id, Vec::new());
        DedupeBegin::New
    }

    pub(crate) async fn dedupe_complete(
        &self,
        peer_msg_id: PeerMessageId,
        result: Result<(), AureliaError>,
    ) {
        let cfg = self.inner.config.snapshot().await;
        let waiters = {
            let mut guard = self.inner.dedupe.lock().await;
            let waiters = guard.pending.remove(&peer_msg_id).unwrap_or_default();
            if result.is_ok() && guard.set.insert(peer_msg_id) {
                guard.order.push_back(peer_msg_id);
                let limit = dedupe_limit(&cfg);
                while guard.order.len() > limit {
                    if let Some(evicted) = guard.order.pop_front() {
                        guard.set.remove(&evicted);
                    }
                }
            }
            waiters
        };
        for waiter in waiters {
            let outcome = match &result {
                Ok(()) => DedupeDecision::Ack,
                Err(err) => DedupeDecision::Error(err.clone()),
            };
            let _ = waiter.send(outcome);
        }
    }

    pub(crate) async fn dedupe_abandon(&self, peer_msg_id: PeerMessageId) {
        let waiters = {
            let mut guard = self.inner.dedupe.lock().await;
            guard.pending.remove(&peer_msg_id).unwrap_or_default()
        };
        for waiter in waiters {
            let _ = waiter.send(DedupeDecision::Abandoned);
        }
    }
}

impl PeerSessionInner {
    async fn refresh_limits(&self, config: &DomusConfig) {
        let mut guard = self.limit_snapshot.lock().await;
        let update = match *guard {
            Some(snapshot) => {
                snapshot.send_queue_size != config.send_queue_size
                    || snapshot.inflight_window != config.inflight_window
            }
            None => true,
        };
        if update {
            self.send_queue.set_limit(config.send_queue_size);
            self.inflight_window.set_limit(config.inflight_window);
            *guard = Some(LimitSnapshot {
                send_queue_size: config.send_queue_size,
                inflight_window: config.inflight_window,
            });
        }
    }

    async fn fail_inflight(&self, peer_msg_id: PeerMessageId, error: AureliaError) {
        let _ = self.dispatch.fail_one(peer_msg_id, error).await;
    }

    async fn cancel_outgoing(&self, peer_msg_id: PeerMessageId, error: AureliaError) {
        self.fail_inflight(peer_msg_id, error).await;
    }
}

fn dedupe_limit(config: &DomusConfig) -> usize {
    let base = config
        .send_queue_size
        .saturating_add(config.inflight_window);
    base.saturating_mul(2).max(128)
}
