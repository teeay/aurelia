// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::session::CancelReason;
use crate::transport::blob::BlobWriteLease;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::Notify;
use tokio::time::{sleep_until, Instant};

struct InboundCallisState {
    waiters: HashMap<PeerMessageId, InboundWaiter>,
    accept_notify: Arc<Notify>,
    cancel_reason: CancelReason,
}

impl InboundCallisState {
    fn new() -> Self {
        Self {
            waiters: HashMap::new(),
            accept_notify: Arc::new(Notify::new()),
            cancel_reason: CancelReason::None,
        }
    }

    fn accept_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.accept_notify)
    }

    fn cancel_reason(&self) -> CancelReason {
        self.cancel_reason
    }

    fn record_cancel_reason(
        &mut self,
        cancel_tx: &watch::Sender<CancelReason>,
        reason: CancelReason,
    ) {
        if self.cancel_reason == CancelReason::None {
            self.cancel_reason = reason;
        }
        set_cancel_reason(cancel_tx, reason);
    }

    fn observe_cancel_reason(&mut self, reason: CancelReason) {
        self.cancel_reason = reason;
    }

    fn insert_waiter(&mut self, peer_msg_id: PeerMessageId, waiter: InboundWaiter) {
        self.waiters.insert(peer_msg_id, waiter);
    }

    fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }

    fn next_blob_deadline(&self) -> Option<Instant> {
        next_blob_deadline(&self.waiters)
    }

    async fn drain_ready(
        &mut self,
        session: &Arc<PeerSession>,
        blob: &Arc<BlobManager>,
        peer: Option<&DomusAddr>,
        observability: &ObservabilityHandle,
    ) -> Vec<InboundOutcome> {
        let mut outcomes = drain_accept_waiters_with_observability(
            &mut self.waiters,
            session,
            blob,
            peer,
            observability,
        )
        .await;
        outcomes.extend(drain_blob_callis_waiters(&mut self.waiters, session, blob).await);
        outcomes
    }

    async fn cancel_all(
        &mut self,
        session: &Arc<PeerSession>,
        blob: &Arc<BlobManager>,
        cancel_reason: CancelReason,
    ) {
        cancel_waiters(&mut self.waiters, session, blob, cancel_reason).await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_callis_task<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    stream: S,
    callis_id: CallisId,
    primary_dispatch: Option<Arc<PrimaryDispatchManager>>,
    shutdown_rx: watch::Receiver<bool>,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis: CallisKind,
    availability_notify: Option<Arc<Notify>>,
    callis_tracker: CallisTracker,
    peer: Option<DomusAddr>,
    observability: ObservabilityHandle,
    task_spawner: PeerTaskSpawner,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    callis_tracker.open();
    let task_spawner_inner = task_spawner.clone();
    task_spawner.spawn(async move {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (internal_shutdown_tx, internal_shutdown_rx) = watch::channel(false);
        let internal_shutdown_tx_reader = internal_shutdown_tx.clone();
        let internal_shutdown_tx_writer = internal_shutdown_tx.clone();
        let mut reader_shutdown = internal_shutdown_rx.clone();
        let mut reader_shutdown_external = shutdown_rx.clone();
        let writer_shutdown = internal_shutdown_rx.clone();
        let (cancel_tx, cancel_rx) = watch::channel(CancelReason::None);
        let mut cancel_rx_reader = cancel_rx.clone();
        let primary_dispatch_reader = primary_dispatch.clone();
        let peer_state_tx_reader = peer_state_tx.clone();

        let config_read = config.clone();
        let session_reader = Arc::clone(&session);
        let blob_reader = Arc::clone(&blob);
        let peer_reader = peer.clone();
        let observability_reader = observability.clone();
        let reader_task = task_spawner_inner.spawn_join(async move {
            // Core receive task: each select arm only records progress. The next iteration drains
            // actionable waiters before reading another frame. Accept completion uses notify_one
            // permit semantics; callis generation and shutdown/cancel signals use latching watch
            // channels.
            let mut inbound = InboundCallisState::new();
            let mut max_payload = config_read.snapshot().await.max_payload_len;
            let mut read_closed = false;
            let mut frame_reader = super::frame::FrameReadState::default();
            // Subscribe to the callis-pool generation watch BEFORE the first drain so that
            // any add_callis bump fired between subscribe and the first has_callis check
            // is observed by the next changed().
            let mut callis_gen_rx = blob_reader.subscribe_callis_gen();
            loop {
                // Drain any waiters that are already actionable.
                let outcomes = inbound
                    .drain_ready(
                        &session_reader,
                        &blob_reader,
                        peer_reader.as_ref(),
                        &observability_reader,
                    )
                    .await;
                for outcome in outcomes {
                    send_inbound_outcome(
                        callis,
                        Some(&blob_reader),
                        primary_dispatch_reader.as_ref(),
                        outcome,
                    )
                    .await;
                }

                if read_closed && inbound.is_empty() {
                    break;
                }

                let blob_deadline = inbound.next_blob_deadline();
                let deadline_sleep = sleep_until(
                    blob_deadline
                        .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(3600)),
                );
                tokio::pin!(deadline_sleep);
                // Arm the Notify-based waiter before awaiting (Pattern 1).
                let accept_notify = inbound.accept_notify();
                let accept_waiter = accept_notify.notified();
                tokio::pin!(accept_waiter);

                tokio::select! {
                    _ = reader_shutdown.changed() => {
                        if *reader_shutdown.borrow() {
                            read_closed = true;
                            inbound.record_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                        }
                    }
                    _ = reader_shutdown_external.changed() => {
                        if *reader_shutdown_external.borrow() {
                            read_closed = true;
                            inbound.record_cancel_reason(&cancel_tx, CancelReason::LocalShutdown);
                        }
                    }
                    _ = cancel_rx_reader.changed() => {
                        let cancel_reason = *cancel_rx_reader.borrow();
                        inbound.observe_cancel_reason(cancel_reason);
                        if cancel_reason != CancelReason::None {
                            inbound
                                .cancel_all(&session_reader, &blob_reader, cancel_reason)
                                .await;
                        }
                    }
                    _ = &mut accept_waiter => {}
                    _ = callis_gen_rx.changed() => {}
                    _ = &mut deadline_sleep => {}
                    result = frame_reader.read_next(&mut reader, max_payload), if !read_closed => {
                        match result {
                            Ok(Some((header, payload))) => {
                                match handle_inbound_frame_with_observability(
                                    Arc::clone(&registry),
                                    Arc::clone(&session_reader),
                                    Arc::clone(&blob_reader),
                                    config_read.clone(),
                                    peer_state_tx_reader.clone(),
                                    callis_id,
                                    primary_dispatch_reader.clone(),
                                    header,
                                    payload,
                                    inbound.cancel_reason(),
                                    inbound.accept_notify(),
                                    &cancel_tx,
                                    peer_reader.as_ref(),
                                    &observability_reader,
                                )
                                .await
                                {
                                    Ok(InboundAction::None) => {}
                                    Ok(InboundAction::Outcome(outcome)) => {
                                        send_inbound_outcome(
                                            callis,
                                            Some(&blob_reader),
                                            primary_dispatch_reader.as_ref(),
                                            outcome,
                                        )
                                        .await;
                                    }
                                    Ok(InboundAction::Waiter { peer_msg_id, waiter }) => {
                                        inbound.insert_waiter(peer_msg_id, waiter);
                                        // Drain at top of next iteration handles it.
                                    }
                                    Err(err) => {
                                        if err.kind == ErrorId::ProtocolViolation {
                                            if let Some(peer) = peer_reader.as_ref() {
                                                observability_reader
                                                    .protocol_violation(peer.clone(), err.kind);
                                            }
                                        }
                                        let _ = internal_shutdown_tx_reader.send(true);
                                        inbound.record_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                                        break;
                                    }
                                }
                                max_payload = config_read.snapshot().await.max_payload_len;
                            }
                            Ok(None) => {
                                let _ = internal_shutdown_tx_reader.send(true);
                                read_closed = true;
                                inbound.record_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                            }
                            Err(_) => {
                                let _ = internal_shutdown_tx_reader.send(true);
                                read_closed = true;
                                inbound.record_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                            }
                        }
                    }
                }
            }
        });

        let availability_notify = availability_notify.clone();
        let writer_task = task_spawner_inner.spawn_join(async move {
            if callis == CallisKind::Primary {
                if let Some(dispatch) = primary_dispatch {
                    run_primary_transmitter(
                        config.clone(),
                        Arc::clone(&session),
                        dispatch,
                        callis_id,
                        &mut writer,
                        shutdown_rx,
                        writer_shutdown,
                        internal_shutdown_tx_writer,
                    )
                    .await;
                }
                return;
            }
            let _ = availability_notify;
            run_blob_transmitter(
                config.clone(),
                Arc::clone(&blob),
                callis_id,
                &mut writer,
                shutdown_rx,
                writer_shutdown,
                internal_shutdown_tx_writer,
            )
            .await;
        });

        let _ = tokio::join!(reader_task, writer_task);
        let cancel_reason = match *cancel_rx.borrow() {
            CancelReason::None => CancelReason::ConnectionLost,
            reason => reason,
        };
        callis_tracker.close();
        let _ = peer_state_tx
            .send(PeerStateUpdate::ConnectionClosed {
                callis,
                id: callis_id,
                reason: cancel_reason,
            })
            .await;
    });
}

pub(super) async fn run_blob_transmitter<W>(
    config: DomusConfigAccess,
    blob: Arc<BlobManager>,
    callis_id: CallisId,
    writer: &mut W,
    mut shutdown_rx: watch::Receiver<bool>,
    mut writer_shutdown: watch::Receiver<bool>,
    internal_shutdown_tx: watch::Sender<bool>,
) where
    W: AsyncWrite + Unpin,
{
    let work_notify = blob.work_handle();
    loop {
        let work_waiter = work_notify.notified();
        tokio::pin!(work_waiter);

        if let Some(dispatch) = blob.lease_next_blob_write(callis_id).await {
            let deadline = Instant::now() + config.snapshot().await.send_timeout;
            let write = async {
                match &dispatch {
                    BlobWriteLease::Ack { peer_msg_id } => {
                        send_control_frame(writer, MSG_ACK, 0, *peer_msg_id, &[]).await
                    }
                    BlobWriteLease::Error {
                        peer_msg_id,
                        payload,
                    } => {
                        send_control_frame(writer, MSG_ERROR, 0, *peer_msg_id, payload.as_ref())
                            .await
                    }
                    BlobWriteLease::Chunk {
                        stream_id,
                        peer_msg_id,
                        chunk: lease,
                    } => {
                        let mut flags = BlobChunkFlags::empty();
                        if lease.is_last {
                            flags |= BlobChunkFlags::LAST_CHUNK;
                        }
                        send_blob_chunk_frame(
                            writer,
                            *peer_msg_id,
                            *stream_id,
                            lease.chunk_id,
                            flags,
                            &lease.data,
                        )
                        .await
                    }
                    BlobWriteLease::Finish {
                        peer_msg_id,
                        payload,
                        ..
                    } => {
                        send_control_frame(
                            writer,
                            MSG_BLOB_TRANSFER_COMPLETE,
                            0,
                            *peer_msg_id,
                            payload.as_ref(),
                        )
                        .await
                    }
                }
            };
            let result = tokio::time::timeout_at(deadline, write).await;
            match result {
                Ok(Ok(())) => {
                    blob.finish_blob_write_attempt(&dispatch, callis_id, Ok(()))
                        .await;
                }
                Ok(Err(_)) | Err(_) => {
                    blob.finish_blob_write_attempt(
                        &dispatch,
                        callis_id,
                        Err(AureliaError::new(ErrorId::PeerUnavailable)),
                    )
                    .await;
                    let _ = internal_shutdown_tx.send(true);
                    break;
                }
            }
            continue;
        }

        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    let deadline = Instant::now() + config.snapshot().await.send_timeout;
                    let _ = tokio::time::timeout_at(
                        deadline,
                        send_control_frame(writer, MSG_CLOSE, 0, 0, &[]),
                    )
                    .await;
                    break;
                }
            }
            _ = writer_shutdown.changed() => {
                if *writer_shutdown.borrow() {
                    break;
                }
            }
            _ = &mut work_waiter => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_primary_transmitter<W>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    dispatch: Arc<PrimaryDispatchManager>,
    callis_id: CallisId,
    writer: &mut W,
    mut shutdown_rx: watch::Receiver<bool>,
    mut writer_shutdown: watch::Receiver<bool>,
    internal_shutdown_tx: watch::Sender<bool>,
) where
    W: AsyncWrite + Unpin,
{
    let mut last_send = Instant::now();
    let mut keepalive_tick = tokio::time::interval(Duration::from_millis(200));
    let close_notifier = dispatch.close_notifier(callis_id).await;
    keepalive_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let work_waiter = dispatch.notifier().notified();
        let close_waiter = close_notifier.notified();
        tokio::pin!(work_waiter);
        tokio::pin!(close_waiter);

        if dispatch.take_close_intent(callis_id).await {
            let deadline = Instant::now() + config.snapshot().await.send_timeout;
            let result =
                tokio::time::timeout_at(deadline, send_control_frame(writer, MSG_CLOSE, 0, 0, &[]))
                    .await;
            let _ = result;
            let _ = internal_shutdown_tx.send(true);
            break;
        }

        if let Some(lease) = dispatch.claim_next(callis_id).await {
            let deadline = lease.deadline;
            if Instant::now() >= deadline {
                dispatch
                    .complete_claim(lease, Err(AureliaError::new(ErrorId::SendTimeout)))
                    .await;
                continue;
            }

            if let Some(peer_msg_id) = super::primary_dispatch::claim_message_id(&lease) {
                if let Err(err) = session.prepare_dispatch(peer_msg_id).await {
                    dispatch.complete_claim(lease, Err(err)).await;
                    continue;
                }
            }

            let frame = super::primary_dispatch::claim_to_frame(&lease);
            let result =
                tokio::time::timeout_at(deadline, send_outbound_frame(writer, frame)).await;
            match result {
                Ok(Ok(())) => {
                    last_send = Instant::now();
                    dispatch.complete_claim(lease, Ok(())).await;
                }
                Ok(Err(err)) => {
                    dispatch.complete_claim(lease, Err(err)).await;
                    let _ = internal_shutdown_tx.send(true);
                    break;
                }
                Err(_) => {
                    dispatch
                        .complete_claim(lease, Err(AureliaError::new(ErrorId::SendTimeout)))
                        .await;
                }
            }
            continue;
        }

        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = writer_shutdown.changed() => {
                if *writer_shutdown.borrow() {
                    break;
                }
            }
            _ = &mut work_waiter => {}
            _ = &mut close_waiter => {}
            _ = keepalive_tick.tick() => {
                if session.is_closing() {
                    continue;
                }
                let cfg = config.snapshot().await;
                if Instant::now().duration_since(last_send) < cfg.keepalive_interval {
                    continue;
                }
                let deadline = Instant::now() + cfg.send_timeout;
                let result = tokio::time::timeout_at(
                    deadline,
                    send_control_frame(writer, MSG_KEEPALIVE, 0, 0, &[]),
                )
                .await;
                match result {
                    Ok(Ok(())) => last_send = Instant::now(),
                    Ok(Err(_)) | Err(_) => {
                        let _ = internal_shutdown_tx.send(true);
                        break;
                    }
                }
            }
        }
    }
    dispatch.clear_close_intent(callis_id).await;
}

pub(super) enum InboundOutcome {
    Ack(PeerMessageId),
    Error {
        peer_msg_id: PeerMessageId,
        err: AureliaError,
    },
    Skip,
}

pub(super) enum InboundWaiter {
    Message(MessageWaiter),
    BlobAccept(BlobAcceptWaiter),
    BlobCallis(BlobCallisWaiter),
    Duplicate(DuplicateWaiter),
}

pub(super) struct MessageWaiter {
    pub(super) dst_taberna: TabernaId,
    pub(super) accept_rx: oneshot::Receiver<Result<(), AureliaError>>,
}

pub(super) struct BlobAcceptWaiter {
    pub(super) pending: BlobAcceptPending,
}

pub(super) struct BlobCallisWaiter {
    pub(super) pending: BlobAcceptPending,
    pub(super) deadline: Instant,
}

pub(super) struct DuplicateWaiter {
    pub(super) decision_rx: oneshot::Receiver<DedupeDecision>,
}

pub(super) enum InboundAction {
    None,
    Outcome(InboundOutcome),
    Waiter {
        peer_msg_id: PeerMessageId,
        waiter: InboundWaiter,
    },
}

fn set_cancel_reason(cancel_tx: &watch::Sender<CancelReason>, reason: CancelReason) {
    if *cancel_tx.borrow() == CancelReason::None {
        let _ = cancel_tx.send(reason);
    }
}

pub(super) async fn send_inbound_outcome(
    callis: CallisKind,
    blob: Option<&Arc<BlobManager>>,
    primary_dispatch: Option<&Arc<PrimaryDispatchManager>>,
    outcome: InboundOutcome,
) {
    let send_a1 = |frame: OutboundFrame| {
        let primary_dispatch = primary_dispatch.cloned();
        async move {
            if callis == CallisKind::Primary {
                if let Some(dispatch) = primary_dispatch.as_ref() {
                    dispatch.enqueue_a1_frame(frame).await;
                    return;
                }
            }
            if callis == CallisKind::Blob {
                if let Some(blob) = blob {
                    if !enqueue_blob_response_write(blob, None, frame).await {
                        warn!("cannot enqueue blob inbound response");
                    }
                }
            } else {
                warn!("cannot send inbound response without an outbound callis channel");
            }
        }
    };
    match outcome {
        InboundOutcome::Ack(peer_msg_id) => {
            send_a1(OutboundFrame::Ack { peer_msg_id }).await;
        }
        InboundOutcome::Error { peer_msg_id, err } => {
            let payload = ErrorPayload::new(err.kind.as_u32(), err.to_string()).to_bytes();
            send_a1(OutboundFrame::Control {
                msg_type: MSG_ERROR,
                peer_msg_id,
                payload: Bytes::from(payload),
            })
            .await;
        }
        InboundOutcome::Skip => {}
    }
}

fn next_blob_deadline(waiters: &HashMap<PeerMessageId, InboundWaiter>) -> Option<Instant> {
    waiters
        .values()
        .filter_map(|waiter| match waiter {
            InboundWaiter::BlobCallis(waiter) => Some(waiter.deadline),
            _ => None,
        })
        .min()
}

enum ReadyKind {
    Message(AcceptReady),
    Blob(AcceptReady),
    Duplicate(DedupeDecision),
}

enum AcceptReady {
    Accepted,
    Rejected(AureliaError),
    Closed,
}

fn collect_ready_waiters(
    waiters: &mut HashMap<PeerMessageId, InboundWaiter>,
) -> Vec<(PeerMessageId, ReadyKind)> {
    let mut ready = Vec::new();
    for (peer_msg_id, waiter) in waiters.iter_mut() {
        match waiter {
            InboundWaiter::Message(waiter) => {
                if let Some(result) = accept_ready(waiter.accept_rx.try_recv()) {
                    ready.push((*peer_msg_id, ReadyKind::Message(result)));
                }
            }
            InboundWaiter::BlobAccept(waiter) => {
                if let Some(result) = accept_ready(waiter.pending.accept_rx.try_recv()) {
                    ready.push((*peer_msg_id, ReadyKind::Blob(result)));
                }
            }
            InboundWaiter::Duplicate(waiter) => match waiter.decision_rx.try_recv() {
                Ok(result) => ready.push((*peer_msg_id, ReadyKind::Duplicate(result))),
                Err(TryRecvError::Closed) => ready.push((
                    *peer_msg_id,
                    ReadyKind::Duplicate(DedupeDecision::Abandoned),
                )),
                Err(TryRecvError::Empty) => {}
            },
            InboundWaiter::BlobCallis(_) => {}
        }
    }
    ready
}

fn accept_ready(result: Result<Result<(), AureliaError>, TryRecvError>) -> Option<AcceptReady> {
    match result {
        Ok(Ok(())) => Some(AcceptReady::Accepted),
        Ok(Err(err)) => Some(AcceptReady::Rejected(err)),
        Err(TryRecvError::Closed) => Some(AcceptReady::Closed),
        Err(TryRecvError::Empty) => None,
    }
}

#[cfg(test)]
pub(super) async fn drain_accept_waiters(
    waiters: &mut HashMap<PeerMessageId, InboundWaiter>,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
) -> Vec<InboundOutcome> {
    let observability = ObservabilityHandle::noop();
    drain_accept_waiters_with_observability(waiters, session, blob, None, &observability).await
}

async fn drain_accept_waiters_with_observability(
    waiters: &mut HashMap<PeerMessageId, InboundWaiter>,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
    peer: Option<&DomusAddr>,
    observability: &ObservabilityHandle,
) -> Vec<InboundOutcome> {
    let ready = collect_ready_waiters(waiters);
    let mut outcomes = Vec::new();
    for (peer_msg_id, ready_kind) in ready {
        let Some(waiter) = waiters.remove(&peer_msg_id) else {
            continue;
        };
        match (waiter, ready_kind) {
            (InboundWaiter::Message(waiter), ReadyKind::Message(result)) => {
                if let Some(outcome) =
                    drain_message_waiter(peer_msg_id, waiter, result, session, peer, observability)
                        .await
                {
                    outcomes.push(outcome);
                }
            }
            (InboundWaiter::BlobAccept(waiter), ReadyKind::Blob(result)) => {
                if let Some(outcome) = drain_blob_accept_waiter(
                    peer_msg_id,
                    waiter,
                    result,
                    waiters,
                    session,
                    blob,
                    peer,
                    observability,
                )
                .await
                {
                    outcomes.push(outcome);
                }
            }
            (InboundWaiter::Duplicate(_), ReadyKind::Duplicate(result)) => {
                outcomes.push(drain_duplicate_waiter(peer_msg_id, result));
            }
            (InboundWaiter::BlobCallis(waiter), _) => {
                waiters.insert(peer_msg_id, InboundWaiter::BlobCallis(waiter));
            }
            (waiter, _) => {
                waiters.insert(peer_msg_id, waiter);
            }
        }
    }

    outcomes
}

fn drain_duplicate_waiter(peer_msg_id: PeerMessageId, result: DedupeDecision) -> InboundOutcome {
    match result {
        DedupeDecision::Ack => InboundOutcome::Ack(peer_msg_id),
        DedupeDecision::Error(err) => InboundOutcome::Error { peer_msg_id, err },
        DedupeDecision::Abandoned => InboundOutcome::Skip,
    }
}

async fn drain_message_waiter(
    peer_msg_id: PeerMessageId,
    waiter: MessageWaiter,
    result: AcceptReady,
    session: &Arc<PeerSession>,
    peer: Option<&DomusAddr>,
    observability: &ObservabilityHandle,
) -> Option<InboundOutcome> {
    match result {
        AcceptReady::Accepted => {
            session.dedupe_complete(peer_msg_id, Ok(())).await;
            Some(InboundOutcome::Ack(peer_msg_id))
        }
        AcceptReady::Rejected(err) => {
            if err.kind == ErrorId::TabernaBusy {
                if let Some(peer) = peer {
                    observability.backpressure_triggered(peer.clone(), waiter.dst_taberna);
                }
                warn!(
                    peer_msg_id,
                    dst_taberna = waiter.dst_taberna,
                    "taberna busy on inbound message"
                );
            } else {
                warn!(
                    peer_msg_id,
                    dst_taberna = waiter.dst_taberna,
                    error = %err,
                    "taberna rejected inbound message"
                );
            }
            session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
            Some(InboundOutcome::Error { peer_msg_id, err })
        }
        AcceptReady::Closed => {
            let err = AureliaError::new(ErrorId::RemoteTabernaRejected);
            session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
            Some(InboundOutcome::Error { peer_msg_id, err })
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_blob_accept_waiter(
    peer_msg_id: PeerMessageId,
    waiter: BlobAcceptWaiter,
    result: AcceptReady,
    waiters: &mut HashMap<PeerMessageId, InboundWaiter>,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
    peer: Option<&DomusAddr>,
    observability: &ObservabilityHandle,
) -> Option<InboundOutcome> {
    let pending = waiter.pending;
    match result {
        AcceptReady::Accepted => {
            pending
                .receiver_state
                .accepted
                .store(true, Ordering::SeqCst);
            pending.receiver_state.notify.notify_waiters();
            blob.add_pending_request(
                peer_msg_id,
                pending.dst_taberna,
                Arc::clone(&pending.receiver_state),
            )
            .await;

            if !session.is_active() {
                blob.drop_pending_request(peer_msg_id).await;
                let err = AureliaError::new(ErrorId::BlobCallisWithoutPrimary);
                pending.receiver_state.fail(err.clone()).await;
                warn!(
                    taberna_id = pending.dst_taberna,
                    peer_msg_id,
                    error = %err,
                    "blob request rejected"
                );
                session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                return Some(InboundOutcome::Error { peer_msg_id, err });
            }

            let has_callis = blob.has_callis().await;
            if !has_callis {
                info!(
                    taberna_id = pending.dst_taberna,
                    peer_msg_id, "blob callis establishment requested"
                );
                if let Err(err) = try_signal_peer_state_ensure(
                    &pending.peer_state_tx,
                    PeerStateUpdate::EnsureBlobDial,
                ) {
                    pending.receiver_state.fail(err.clone()).await;
                    session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    return Some(InboundOutcome::Error { peer_msg_id, err });
                }
            }
            if has_callis {
                let Some(settings) = blob.current_settings().await else {
                    let err = AureliaError::new(ErrorId::BlobCallisWithoutPrimary);
                    pending.receiver_state.fail(err.clone()).await;
                    session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    return Some(InboundOutcome::Error { peer_msg_id, err });
                };
                if let Err(err) = blob.activate_pending_request(peer_msg_id, settings).await {
                    pending.receiver_state.fail(err.clone()).await;
                    session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    return Some(InboundOutcome::Error { peer_msg_id, err });
                }
                session.dedupe_complete(peer_msg_id, Ok(())).await;
                info!(
                    taberna_id = pending.dst_taberna,
                    peer_msg_id, "blob request accepted"
                );
                Some(InboundOutcome::Ack(peer_msg_id))
            } else {
                let deadline = Instant::now() + pending.send_timeout;
                waiters.insert(
                    peer_msg_id,
                    InboundWaiter::BlobCallis(BlobCallisWaiter { pending, deadline }),
                );
                None
            }
        }
        AcceptReady::Rejected(err) => {
            blob.release_inbound(peer_msg_id).await;
            if err.kind == ErrorId::TabernaBusy {
                if let Some(peer) = peer {
                    observability.backpressure_triggered(peer.clone(), pending.dst_taberna);
                }
            }
            warn!(
                taberna_id = pending.dst_taberna,
                peer_msg_id,
                error = %err,
                "blob request rejected"
            );
            session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
            Some(InboundOutcome::Error { peer_msg_id, err })
        }
        AcceptReady::Closed => {
            blob.release_inbound(peer_msg_id).await;
            let err = AureliaError::new(ErrorId::RemoteTabernaRejected);
            warn!(
                taberna_id = pending.dst_taberna,
                peer_msg_id,
                error = %err,
                "blob request rejected"
            );
            session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
            Some(InboundOutcome::Error { peer_msg_id, err })
        }
    }
}

pub(super) async fn drain_blob_callis_waiters(
    waiters: &mut HashMap<PeerMessageId, InboundWaiter>,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
) -> Vec<InboundOutcome> {
    let mut outcomes = Vec::new();
    let mut ids = Vec::new();
    for (peer_msg_id, waiter) in waiters.iter() {
        if matches!(waiter, InboundWaiter::BlobCallis(_)) {
            ids.push(*peer_msg_id);
        }
    }

    for peer_msg_id in ids {
        let Some(InboundWaiter::BlobCallis(waiter)) = waiters.remove(&peer_msg_id) else {
            continue;
        };
        if Instant::now() >= waiter.deadline {
            blob.drop_pending_request(peer_msg_id).await;
            let err = AureliaError::new(ErrorId::SendTimeout);
            waiter.pending.receiver_state.fail(err.clone()).await;
            warn!(
                taberna_id = waiter.pending.dst_taberna,
                peer_msg_id,
                error = %err,
                "blob callis establishment failed"
            );
            session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
            outcomes.push(InboundOutcome::Error { peer_msg_id, err });
            continue;
        }

        if blob.has_callis().await {
            let Some(settings) = blob.current_settings().await else {
                let err = AureliaError::new(ErrorId::BlobCallisWithoutPrimary);
                waiter.pending.receiver_state.fail(err.clone()).await;
                session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                outcomes.push(InboundOutcome::Error { peer_msg_id, err });
                continue;
            };
            if let Err(err) = blob.activate_pending_request(peer_msg_id, settings).await {
                waiter.pending.receiver_state.fail(err.clone()).await;
                session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                outcomes.push(InboundOutcome::Error { peer_msg_id, err });
                continue;
            }
            session.dedupe_complete(peer_msg_id, Ok(())).await;
            info!(
                taberna_id = waiter.pending.dst_taberna,
                peer_msg_id, "blob callis ready"
            );
            outcomes.push(InboundOutcome::Ack(peer_msg_id));
        } else {
            waiters.insert(peer_msg_id, InboundWaiter::BlobCallis(waiter));
        }
    }

    outcomes
}

pub(super) async fn cancel_waiters(
    waiters: &mut HashMap<PeerMessageId, InboundWaiter>,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
    cancel_reason: CancelReason,
) {
    let mut cancelled = HashMap::new();
    std::mem::swap(waiters, &mut cancelled);
    for (peer_msg_id, waiter) in cancelled {
        match waiter {
            InboundWaiter::Message(_) => {
                if cancel_reason.should_error() {
                    let err = AureliaError::new(ErrorId::PeerUnavailable);
                    session.dedupe_complete(peer_msg_id, Err(err)).await;
                } else {
                    session.dedupe_abandon(peer_msg_id).await;
                }
            }
            InboundWaiter::BlobAccept(_) => {
                blob.release_inbound(peer_msg_id).await;
                if cancel_reason.should_error() {
                    let err = AureliaError::new(ErrorId::PeerUnavailable);
                    session.dedupe_complete(peer_msg_id, Err(err)).await;
                } else {
                    session.dedupe_abandon(peer_msg_id).await;
                }
            }
            InboundWaiter::BlobCallis(_) => {
                blob.drop_pending_request(peer_msg_id).await;
                if cancel_reason.should_error() {
                    let err = AureliaError::new(ErrorId::PeerUnavailable);
                    session.dedupe_complete(peer_msg_id, Err(err)).await;
                } else {
                    session.dedupe_abandon(peer_msg_id).await;
                }
            }
            InboundWaiter::Duplicate(_) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) async fn handle_inbound_frame(
    registry: Arc<TabernaRegistry>,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    config: DomusConfigAccess,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_id: CallisId,
    primary_dispatch: Option<Arc<PrimaryDispatchManager>>,
    header: WireHeader,
    payload: Vec<u8>,
    cancel_reason: CancelReason,
    accept_notify: Arc<Notify>,
    cancel_tx: &watch::Sender<CancelReason>,
) -> Result<InboundAction, AureliaError> {
    let observability = ObservabilityHandle::noop();
    handle_inbound_frame_with_observability(
        registry,
        session,
        blob,
        config,
        peer_state_tx,
        callis_id,
        primary_dispatch,
        header,
        payload,
        cancel_reason,
        accept_notify,
        cancel_tx,
        None,
        &observability,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_inbound_frame_with_observability(
    registry: Arc<TabernaRegistry>,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    config: DomusConfigAccess,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_id: CallisId,
    primary_dispatch: Option<Arc<PrimaryDispatchManager>>,
    header: WireHeader,
    payload: Vec<u8>,
    cancel_reason: CancelReason,
    accept_notify: Arc<Notify>,
    cancel_tx: &watch::Sender<CancelReason>,
    peer: Option<&DomusAddr>,
    observability: &ObservabilityHandle,
) -> Result<InboundAction, AureliaError> {
    let flags = WireFlags::from_bits(header.flags)
        .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
    if flags.contains(WireFlags::RECONNECT) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    if disallows_flags(header.msg_type) && !flags.is_empty() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    match header.msg_type {
        MSG_ACK => {
            session.handle_ack(header.peer_msg_id).await;
            blob.handle_ack(header.peer_msg_id).await;
            Ok(InboundAction::None)
        }
        MSG_ERROR => {
            let error = ErrorPayload::from_bytes(&payload)?;
            let kind = ErrorId::try_from(error.error_id).map_err(|_| {
                AureliaError::with_message(
                    ErrorId::ProtocolViolation,
                    format!("unknown inbound error_id {}", error.error_id),
                )
            })?;
            let err = AureliaError::with_message(kind, error.message);
            warn!(
                peer_msg_id = header.peer_msg_id,
                error_id = err.kind.as_u32(),
                "received error frame"
            );
            session.handle_error(header.peer_msg_id, err.clone()).await;
            blob.handle_error(header.peer_msg_id, err).await;
            Ok(InboundAction::None)
        }
        MSG_CLOSE => {
            set_cancel_reason(cancel_tx, CancelReason::RemoteClose);
            session.handle_close().await;
            Err(AureliaError::new(ErrorId::PeerUnavailable))
        }
        MSG_KEEPALIVE => Ok(InboundAction::None),
        MSG_HELLO | MSG_HELLO_RESPONSE => Err(AureliaError::new(ErrorId::ProtocolViolation)),
        MSG_RESERVED_7 => Err(AureliaError::new(ErrorId::ProtocolViolation)),
        MSG_BLOB_TRANSFER_CHUNK => {
            let cfg = config.snapshot().await;
            let idle_timeout = cfg.send_timeout * 2;
            let settings = blob
                .settings_for_callis(callis_id)
                .await
                .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
            let result = handle_blob_transfer_chunk(
                blob.as_ref(),
                &payload,
                idle_timeout,
                settings.ack_window_chunks,
                settings.chunk_size,
            )
            .await;
            if let Some(BlobChunkOutcome::Complete(stream_id)) =
                respond_with_blob(blob.as_ref(), header.peer_msg_id, result).await
            {
                let complete_id = blob.next_peer_msg_id();
                let payload = BlobTransferCompletePayload {
                    request_msg_id: stream_id,
                };
                send_blob_response_frame(
                    blob.as_ref(),
                    stream_id,
                    OutboundFrame::Control {
                        msg_type: MSG_BLOB_TRANSFER_COMPLETE,
                        peer_msg_id: complete_id,
                        payload: Bytes::from(payload.to_bytes().to_vec()),
                    },
                )
                .await;
            }
            Ok(InboundAction::None)
        }
        MSG_BLOB_TRANSFER_COMPLETE => {
            let complete = BlobTransferCompletePayload::from_bytes(&payload)?;
            blob.handle_complete(complete.request_msg_id).await?;
            send_blob_response_frame(
                blob.as_ref(),
                complete.request_msg_id,
                OutboundFrame::Ack {
                    peer_msg_id: header.peer_msg_id,
                },
            )
            .await;
            Ok(InboundAction::None)
        }
        _ => {
            if flags.contains(WireFlags::BLOB) {
                if primary_dispatch.is_none() {
                    return Err(AureliaError::new(ErrorId::ProtocolViolation));
                }
                let peer_msg_id = header.peer_msg_id;
                if cancel_reason != CancelReason::None {
                    return Ok(InboundAction::Outcome(InboundOutcome::Skip));
                }
                let schedule = handle_blob_request(
                    registry,
                    Arc::clone(&session),
                    Arc::clone(&blob),
                    config,
                    peer_state_tx,
                    header,
                    payload,
                    Some(Arc::clone(&accept_notify)),
                )
                .await;
                match schedule {
                    Ok(BlobRequestSchedule::Immediate(BlobRequestOutcome::Ack)) => {
                        Ok(InboundAction::Outcome(InboundOutcome::Ack(peer_msg_id)))
                    }
                    Ok(BlobRequestSchedule::Immediate(BlobRequestOutcome::Skip)) => {
                        Ok(InboundAction::Outcome(InboundOutcome::Skip))
                    }
                    Ok(BlobRequestSchedule::Pending(pending)) => Ok(InboundAction::Waiter {
                        peer_msg_id,
                        waiter: InboundWaiter::BlobAccept(BlobAcceptWaiter { pending }),
                    }),
                    Ok(BlobRequestSchedule::PendingDuplicate(decision_rx)) => {
                        Ok(InboundAction::Waiter {
                            peer_msg_id,
                            waiter: InboundWaiter::Duplicate(DuplicateWaiter { decision_rx }),
                        })
                    }
                    Err(err) => {
                        if err.kind == ErrorId::TabernaBusy {
                            if let Some(peer) = peer {
                                observability
                                    .backpressure_triggered(peer.clone(), header.dst_taberna);
                            }
                        }
                        Ok(InboundAction::Outcome(InboundOutcome::Error {
                            peer_msg_id,
                            err,
                        }))
                    }
                }
            } else if flags.is_empty() {
                let message = PeerMessage {
                    peer_msg_id: header.peer_msg_id,
                    src_taberna: header.src_taberna,
                    dst_taberna: header.dst_taberna,
                    msg_type: header.msg_type,
                    flags: header.flags,
                    payload: Bytes::from(payload),
                };
                let peer_msg_id = header.peer_msg_id;
                if cancel_reason != CancelReason::None {
                    if cancel_reason.should_error() {
                        let err = AureliaError::new(ErrorId::PeerUnavailable);
                        return Ok(InboundAction::Outcome(InboundOutcome::Error {
                            peer_msg_id,
                            err,
                        }));
                    }
                    return Ok(InboundAction::Outcome(InboundOutcome::Skip));
                }
                let schedule = session
                    .receive_message_schedule(
                        message,
                        registry.as_ref(),
                        Some(Arc::clone(&accept_notify)),
                    )
                    .await;
                match schedule {
                    ReceiveSchedule::Immediate(outcome) => {
                        let mapped = match outcome {
                            ReceiveOutcome::Ack(peer_msg_id) => InboundOutcome::Ack(peer_msg_id),
                            ReceiveOutcome::Error(err) => {
                                if err.kind == ErrorId::TabernaBusy {
                                    if let Some(peer) = peer {
                                        observability.backpressure_triggered(
                                            peer.clone(),
                                            header.dst_taberna,
                                        );
                                    }
                                }
                                InboundOutcome::Error { peer_msg_id, err }
                            }
                            ReceiveOutcome::Skip => InboundOutcome::Skip,
                        };
                        Ok(InboundAction::Outcome(mapped))
                    }
                    ReceiveSchedule::Pending(pending) => Ok(InboundAction::Waiter {
                        peer_msg_id,
                        waiter: InboundWaiter::Message(MessageWaiter {
                            dst_taberna: pending.dst_taberna,
                            accept_rx: pending.accept_rx,
                        }),
                    }),
                    ReceiveSchedule::PendingDuplicate(pending) => Ok(InboundAction::Waiter {
                        peer_msg_id,
                        waiter: InboundWaiter::Duplicate(DuplicateWaiter {
                            decision_rx: pending.decision_rx,
                        }),
                    }),
                }
            } else {
                Err(AureliaError::new(ErrorId::ProtocolViolation))
            }
        }
    }
}

fn disallows_flags(msg_type: MessageType) -> bool {
    matches!(
        msg_type,
        MSG_ACK
            | MSG_ERROR
            | MSG_CLOSE
            | MSG_KEEPALIVE
            | MSG_RESERVED_7
            | MSG_BLOB_TRANSFER_CHUNK
            | MSG_BLOB_TRANSFER_COMPLETE
    )
}

async fn respond_with_blob<T>(
    blob: &BlobManager,
    peer_msg_id: PeerMessageId,
    result: Result<T, AureliaError>,
) -> Option<T> {
    match result {
        Ok(value) => {
            send_blob_response_frame(blob, peer_msg_id, OutboundFrame::Ack { peer_msg_id }).await;
            Some(value)
        }
        Err(err) => {
            let payload = ErrorPayload::new(err.kind.as_u32(), err.to_string()).to_bytes();
            send_blob_response_frame(
                blob,
                peer_msg_id,
                OutboundFrame::Control {
                    msg_type: MSG_ERROR,
                    peer_msg_id,
                    payload: Bytes::from(payload),
                },
            )
            .await;
            None
        }
    }
}

async fn send_blob_response_frame(
    blob: &BlobManager,
    stream_id: PeerMessageId,
    frame: OutboundFrame,
) {
    let _ = enqueue_blob_response_write(blob, Some(stream_id), frame).await;
}

async fn enqueue_blob_response_write(
    blob: &BlobManager,
    stream_id: Option<PeerMessageId>,
    frame: OutboundFrame,
) -> bool {
    match frame {
        OutboundFrame::Ack { peer_msg_id } => {
            blob.enqueue_blob_write(BlobWriteLease::Ack { peer_msg_id })
                .await;
            true
        }
        OutboundFrame::Control {
            msg_type: MSG_ERROR,
            peer_msg_id,
            payload,
        } => {
            blob.enqueue_blob_write(BlobWriteLease::Error {
                peer_msg_id,
                payload,
            })
            .await;
            true
        }
        OutboundFrame::Control {
            msg_type: MSG_BLOB_TRANSFER_COMPLETE,
            peer_msg_id,
            payload,
        } => {
            let stream_id = stream_id.unwrap_or_else(|| {
                BlobTransferCompletePayload::from_bytes(&payload)
                    .map(|complete| complete.request_msg_id)
                    .unwrap_or(peer_msg_id)
            });
            blob.enqueue_blob_write(BlobWriteLease::Finish {
                stream_id,
                peer_msg_id,
                payload,
            })
            .await;
            true
        }
        _ => false,
    }
}
