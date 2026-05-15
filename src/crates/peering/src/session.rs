// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{oneshot, Mutex, Notify};
use tokio::time::{timeout, Instant};
use tracing::{debug, trace, warn};

use crate::config::{DomusConfig, DomusConfigAccess};
use crate::message_id::PeerMessageIdAllocator;
use crate::taberna::TabernaRegistry;
use crate::transport::primary_dispatch::PrimaryDispatchManager;
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_ids::{MessageType, PeerMessageId, TabernaId};

// Keep duplicate suppression useful even when tests or small deployments configure tiny
// send/inflight windows; the formula below still scales the history above this floor.
const MIN_DEDUPE_HISTORY: usize = 128;

struct DedupeState {
    set: HashSet<PeerMessageId>,
    order: VecDeque<PeerMessageId>,
    pending: HashMap<PeerMessageId, Vec<DedupeWaiter>>,
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
    PendingDuplicate(PendingDuplicateReceive),
}

pub(crate) struct PendingReceive {
    pub(crate) dst_taberna: TabernaId,
    pub(crate) accept_rx: oneshot::Receiver<Result<(), AureliaError>>,
}

pub(crate) struct PendingDuplicateReceive {
    pub(crate) decision_rx: oneshot::Receiver<DedupeDecision>,
}

pub(crate) enum DedupeBegin {
    New,
    Duplicate(DedupeDecision),
    PendingDuplicate(oneshot::Receiver<DedupeDecision>),
}

pub(crate) enum DedupeDecision {
    Ack,
    Error(AureliaError),
    Abandoned,
}

struct DedupeWaiter {
    tx: oneshot::Sender<DedupeDecision>,
    notify: Option<Arc<Notify>>,
}

pub struct AckWaiter {
    rx: Option<oneshot::Receiver<Result<(), AureliaError>>>,
    peer_msg_id: PeerMessageId,
    deadline: Instant,
}

pub struct PeerSession {
    inner: Arc<PeerSessionInner>,
}

struct PeerSessionInner {
    allocator: Arc<PeerMessageIdAllocator>,
    dedupe: Mutex<DedupeState>,
    dispatch: Arc<PrimaryDispatchManager>,
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
        dispatch: Arc<PrimaryDispatchManager>,
    ) -> Self {
        Self {
            inner: Arc::new(PeerSessionInner {
                allocator,
                dedupe: Mutex::new(DedupeState::new()),
                dispatch,
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
            .enqueue_new(message.clone(), deadline)
            .await?;
        Ok((
            message,
            AckWaiter {
                rx: Some(rx),
                peer_msg_id,
                deadline,
            },
        ))
    }

    pub async fn prepare_dispatch(&self, peer_msg_id: PeerMessageId) -> Result<(), AureliaError> {
        if self.inner.closing.load(Ordering::SeqCst) {
            self.inner
                .fail_inflight(peer_msg_id, AureliaError::new(ErrorId::PeerUnavailable))
                .await;
            return Err(AureliaError::new(ErrorId::PeerUnavailable));
        }

        let deadline = match self.inner.dispatch.deadline(peer_msg_id).await {
            Some(deadline) => deadline,
            None => {
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

        Ok(())
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
            return self
                .fail_ack_wait(
                    waiter.peer_msg_id,
                    AureliaError::new(ErrorId::SendTimeout),
                    "ack wait timeout",
                )
                .await;
        }

        let Some(rx) = waiter.rx.take() else {
            return self
                .fail_ack_wait(
                    waiter.peer_msg_id,
                    AureliaError::new(ErrorId::ConnectionLost),
                    "ack waiter receiver missing",
                )
                .await;
        };
        match timeout(remaining, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.fail_ack_wait(
                    waiter.peer_msg_id,
                    AureliaError::new(ErrorId::ConnectionLost),
                    "ack wait connection lost",
                )
                .await
            }
            Err(_) => {
                self.fail_ack_wait(
                    waiter.peer_msg_id,
                    AureliaError::new(ErrorId::SendTimeout),
                    "ack wait timeout",
                )
                .await
            }
        }
    }

    pub async fn handle_hello_response(&self, reconnect: bool) -> Vec<PeerMessage> {
        if reconnect {
            self.inner.dispatch.inflight_messages().await
        } else {
            let _ = self
                .inner
                .dispatch
                .fail_all(AureliaError::new(ErrorId::PeerRestarted))
                .await;
            let mut guard = self.inner.dedupe.lock().await;
            guard.clear();
            Vec::new()
        }
    }

    pub async fn accept_hello(&self, reconnect: bool) -> bool {
        if reconnect && self.inner.active.load(Ordering::SeqCst) {
            true
        } else {
            if self.inner.active.swap(true, Ordering::SeqCst) {
                let _ = self
                    .inner
                    .dispatch
                    .fail_all(AureliaError::new(ErrorId::PeerRestarted))
                    .await;
            }
            let mut guard = self.inner.dedupe.lock().await;
            guard.clear();
            false
        }
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

        match self.dedupe_begin(peer_msg_id, notify.clone()).await {
            DedupeBegin::Duplicate(result) => {
                trace!(peer_msg_id, "deduped inbound message");
                let outcome = match result {
                    DedupeDecision::Ack => ReceiveOutcome::Ack(peer_msg_id),
                    DedupeDecision::Error(err) => ReceiveOutcome::Error(err),
                    DedupeDecision::Abandoned => ReceiveOutcome::Skip,
                };
                return ReceiveSchedule::Immediate(outcome);
            }
            DedupeBegin::PendingDuplicate(decision_rx) => {
                trace!(peer_msg_id, "pending duplicate inbound message");
                return ReceiveSchedule::PendingDuplicate(PendingDuplicateReceive { decision_rx });
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
            dst_taberna,
            accept_rx,
        })
    }

    pub(crate) async fn dedupe_begin(
        &self,
        peer_msg_id: PeerMessageId,
        notify: Option<Arc<Notify>>,
    ) -> DedupeBegin {
        let mut guard = self.inner.dedupe.lock().await;
        if guard.set.contains(&peer_msg_id) {
            return DedupeBegin::Duplicate(DedupeDecision::Ack);
        }
        if let Some(waiters) = guard.pending.get_mut(&peer_msg_id) {
            let (tx, rx) = oneshot::channel();
            waiters.push(DedupeWaiter { tx, notify });
            return DedupeBegin::PendingDuplicate(rx);
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
            let notify = waiter.notify;
            let _ = waiter.tx.send(outcome);
            if let Some(notify) = notify {
                notify.notify_one();
            }
        }
    }

    pub(crate) async fn dedupe_abandon(&self, peer_msg_id: PeerMessageId) {
        let waiters = {
            let mut guard = self.inner.dedupe.lock().await;
            guard.pending.remove(&peer_msg_id).unwrap_or_default()
        };
        for waiter in waiters {
            let notify = waiter.notify;
            let _ = waiter.tx.send(DedupeDecision::Abandoned);
            if let Some(notify) = notify {
                notify.notify_one();
            }
        }
    }

    async fn fail_ack_wait(
        &self,
        peer_msg_id: PeerMessageId,
        error: AureliaError,
        message: &'static str,
    ) -> Result<(), AureliaError> {
        self.inner.fail_inflight(peer_msg_id, error.clone()).await;
        warn!(peer_msg_id, "{}", message);
        Err(error)
    }
}

#[cfg(test)]
impl PeerSession {
    pub(crate) fn primary_dispatch(&self) -> Arc<PrimaryDispatchManager> {
        Arc::clone(&self.inner.dispatch)
    }

    pub fn with_backpressure(
        allocator: Arc<PeerMessageIdAllocator>,
        config: DomusConfig,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let store = Arc::new(crate::config::DomusConfigStore::new(config));
        let config = DomusConfigAccess::new(store, None);
        Self::new(
            allocator,
            config,
            runtime_handle.clone(),
            PrimaryDispatchManager::new_for_tests(runtime_handle),
        )
    }

    pub async fn mark_dispatched(&self, peer_msg_id: PeerMessageId) -> Result<(), AureliaError> {
        self.inner
            .dispatch
            .mark_dispatched_for_tests(peer_msg_id)
            .await
    }

    pub async fn mark_restarted(&self) {
        self.inner.active.store(false, Ordering::SeqCst);
        self.inner.closing.store(false, Ordering::SeqCst);
        let mut guard = self.inner.dedupe.lock().await;
        guard.clear();
    }

    pub async fn receive_message_cancelable(
        &self,
        message: PeerMessage,
        registry: &TabernaRegistry,
        mut cancel_rx: tokio::sync::watch::Receiver<CancelReason>,
    ) -> ReceiveOutcome {
        let peer_msg_id = message.peer_msg_id;
        let schedule = self.receive_message_schedule(message, registry, None).await;
        #[derive(Clone, Copy)]
        enum PendingOwner {
            Original(PeerMessageId),
            Duplicate,
            None,
        }
        let pending_owner = match &schedule {
            ReceiveSchedule::Pending(_) => PendingOwner::Original(peer_msg_id),
            ReceiveSchedule::PendingDuplicate(_) => PendingOwner::Duplicate,
            ReceiveSchedule::Immediate(_) => PendingOwner::None,
        };
        tokio::select! {
            _ = cancel_rx.changed() => {
                let cancel_reason = *cancel_rx.borrow();
                if cancel_reason.should_error() {
                    let err = AureliaError::new(ErrorId::PeerUnavailable);
                    if let PendingOwner::Original(peer_msg_id) = pending_owner {
                        self.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    }
                    ReceiveOutcome::Error(err)
                } else {
                    if let PendingOwner::Original(peer_msg_id) = pending_owner {
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
                                self.dedupe_complete(peer_msg_id, Ok(())).await;
                                ReceiveOutcome::Ack(peer_msg_id)
                            }
                            Ok(Err(err)) => {
                                if err.kind == ErrorId::TabernaBusy {
                                    warn!(peer_msg_id, dst_taberna = pending.dst_taberna, "taberna busy on inbound message");
                                } else {
                                    warn!(
                                        peer_msg_id,
                                        dst_taberna = pending.dst_taberna,
                                        error = %err,
                                        "taberna rejected inbound message"
                                    );
                                }
                                self.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                                ReceiveOutcome::Error(err)
                            }
                            Err(_) => {
                                let err = AureliaError::new(ErrorId::RemoteTabernaRejected);
                                self.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                                ReceiveOutcome::Error(err)
                            }
                        }
                    }
                    ReceiveSchedule::PendingDuplicate(pending) => {
                        match pending.decision_rx.await.unwrap_or(DedupeDecision::Abandoned) {
                            DedupeDecision::Ack => ReceiveOutcome::Ack(peer_msg_id),
                            DedupeDecision::Error(err) => ReceiveOutcome::Error(err),
                            DedupeDecision::Abandoned => ReceiveOutcome::Skip,
                        }
                    }
                }
            } => result
        }
    }

    pub async fn is_duplicate(&self, peer_msg_id: PeerMessageId) -> bool {
        let guard = self.inner.dedupe.lock().await;
        guard.set.contains(&peer_msg_id)
    }
}

impl PeerSessionInner {
    async fn refresh_limits(&self, config: &DomusConfig) {
        let mut guard = self.limit_snapshot.lock().await;
        let update = match *guard {
            Some(snapshot) => snapshot.send_queue_size != config.send_queue_size,
            None => true,
        };
        if update {
            self.dispatch.set_capacity(config.send_queue_size).await;
            *guard = Some(LimitSnapshot {
                send_queue_size: config.send_queue_size,
            });
        }
    }

    async fn fail_inflight(&self, peer_msg_id: PeerMessageId, error: AureliaError) {
        let _ = self.dispatch.fail_one(peer_msg_id, error).await;
    }
}

fn dedupe_limit(config: &DomusConfig) -> usize {
    config
        .send_queue_size
        .saturating_mul(2)
        .max(MIN_DEDUPE_HISTORY)
}
