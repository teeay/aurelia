// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::session::CancelReason;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::Notify;
use tokio::time::{sleep_until, Instant};

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_callis_task<S>(
    config: DomusConfigAccess,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    registry: Arc<TabernaRegistry>,
    stream: S,
    callis_id: CallisId,
    primary_dispatch: Option<Arc<PrimaryDispatchQueue>>,
    outbound_tx: mpsc::Sender<OutboundFrame>,
    mut outbound_rx: mpsc::Receiver<OutboundFrame>,
    mut shutdown_rx: watch::Receiver<bool>,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis: CallisKind,
    available: Arc<AtomicBool>,
    availability_notify: Option<Arc<Notify>>,
    callis_tracker: CallisTracker,
    runtime_handle: tokio::runtime::Handle,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    callis_tracker.open();
    let spawn_handle = runtime_handle.clone();
    spawn_handle.spawn(async move {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (internal_shutdown_tx, internal_shutdown_rx) = watch::channel(false);
        let internal_shutdown_tx_reader = internal_shutdown_tx.clone();
        let internal_shutdown_tx_writer = internal_shutdown_tx.clone();
        let mut reader_shutdown = internal_shutdown_rx.clone();
        let mut reader_shutdown_external = shutdown_rx.clone();
        let mut writer_shutdown = internal_shutdown_rx.clone();
        let outbound_tx = outbound_tx.clone();
        let (cancel_tx, cancel_rx) = watch::channel(CancelReason::None);
        let mut cancel_rx_reader = cancel_rx.clone();
        let primary_dispatch_reader = primary_dispatch.clone();
        let peer_state_tx_reader = peer_state_tx.clone();

        let config_read = config.clone();
        let reader_task = runtime_handle.clone().spawn(async move {
            let accept_notify = Arc::new(Notify::new());
            let mut waiters: HashMap<PeerMessageId, InboundWaiter> = HashMap::new();
            let mut max_payload = config_read.snapshot().await.max_payload_len;
            let mut read_closed = false;
            let mut cancel_reason = CancelReason::None;
            loop {
                let next_deadline = next_blob_deadline(&waiters);
                let deadline = next_deadline.unwrap_or_else(Instant::now);
                let has_blob_callis_waiters = has_blob_callis_waiters(&waiters);
                tokio::select! {
                    _ = reader_shutdown.changed() => {
                        if *reader_shutdown.borrow() {
                            read_closed = true;
                            set_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                        }
                    }
                    _ = reader_shutdown_external.changed() => {
                        if *reader_shutdown_external.borrow() {
                            read_closed = true;
                            set_cancel_reason(&cancel_tx, CancelReason::LocalShutdown);
                        }
                    }
                    _ = cancel_rx_reader.changed() => {
                        cancel_reason = *cancel_rx_reader.borrow();
                        if cancel_reason != CancelReason::None {
                            cancel_waiters(
                                &mut waiters,
                                &session,
                                &blob,
                                cancel_reason,
                            ).await;
                        }
                    }
                    _ = accept_notify.notified(), if !waiters.is_empty() => {
                        let outcomes = drain_accept_waiters(
                            &mut waiters,
                            &session,
                            &blob,
                        ).await;
                        for outcome in outcomes {
                            send_inbound_outcome(
                                callis,
                                primary_dispatch_reader.as_ref(),
                                &outbound_tx,
                                outcome,
                            )
                            .await;
                        }
                    }
                    _ = blob.callis_notify().notified(), if has_blob_callis_waiters => {
                        let outcomes = drain_blob_callis_waiters(
                            &mut waiters,
                            &session,
                            &blob,
                        ).await;
                        for outcome in outcomes {
                            send_inbound_outcome(
                                callis,
                                primary_dispatch_reader.as_ref(),
                                &outbound_tx,
                                outcome,
                            )
                            .await;
                        }
                    }
                    _ = sleep_until(deadline), if next_deadline.is_some() => {
                        let outcomes = drain_blob_callis_waiters(
                            &mut waiters,
                            &session,
                            &blob,
                        ).await;
                        for outcome in outcomes {
                            send_inbound_outcome(
                                callis,
                                primary_dispatch_reader.as_ref(),
                                &outbound_tx,
                                outcome,
                            )
                            .await;
                        }
                    }
                    result = read_frame(&mut reader, max_payload), if !read_closed => {
                        match result {
                            Ok(Some((header, payload))) => {
                                match handle_inbound_frame(
                                    Arc::clone(&registry),
                                    Arc::clone(&session),
                                    Arc::clone(&blob),
                                    config_read.clone(),
                                    peer_state_tx_reader.clone(),
                                    callis_id,
                                    callis,
                                    primary_dispatch_reader.clone(),
                                    header,
                                    payload,
                                    outbound_tx.clone(),
                                    cancel_reason,
                                    Arc::clone(&accept_notify),
                                    &cancel_tx,
                                )
                                .await
                                {
                                    Ok(InboundAction::None) => {}
                                    Ok(InboundAction::Outcome(outcome)) => {
                                        send_inbound_outcome(
                                            callis,
                                            primary_dispatch_reader.as_ref(),
                                            &outbound_tx,
                                            outcome,
                                        )
                                        .await;
                                    }
                                    Ok(InboundAction::Waiter { peer_msg_id, waiter }) => {
                                        waiters.insert(peer_msg_id, waiter);
                                        let outcomes = drain_accept_waiters(
                                            &mut waiters,
                                            &session,
                                            &blob,
                                        ).await;
                                        for outcome in outcomes {
                                            send_inbound_outcome(
                                                callis,
                                                primary_dispatch_reader.as_ref(),
                                                &outbound_tx,
                                                outcome,
                                            )
                                            .await;
                                        }
                                    }
                                    Err(_) => {
                                        let _ = internal_shutdown_tx_reader.send(true);
                                        set_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                                        break;
                                    }
                                }
                                max_payload = config_read.snapshot().await.max_payload_len;
                            }
                            Ok(None) => {
                                let _ = internal_shutdown_tx_reader.send(true);
                                read_closed = true;
                                set_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                            }
                            Err(_) => {
                                let _ = internal_shutdown_tx_reader.send(true);
                                read_closed = true;
                                set_cancel_reason(&cancel_tx, CancelReason::ConnectionLost);
                            }
                        }
                    }
                }
                if read_closed && waiters.is_empty() {
                    break;
                }
            }
        });

        let available = Arc::clone(&available);
        let availability_notify = availability_notify.clone();
        let writer_task = runtime_handle.spawn(async move {
            loop {
                available.store(true, Ordering::SeqCst);
                if let Some(notify) = availability_notify.as_ref() {
                    notify.notify_one();
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
                    maybe_frame = outbound_rx.recv() => {
                        match maybe_frame {
                            Some(frame) => {
                                available.store(false, Ordering::SeqCst);
                                let send_result = match frame {
                                    OutboundFrame::Close => {
                                        send_control_frame(&mut writer, MSG_CLOSE, 0, 0, &[]).await
                                    }
                                    frame => send_outbound_frame(&mut writer, frame).await,
                                };
                                if send_result.is_err() {
                                    let _ = internal_shutdown_tx_writer.send(true);
                                    break;
                                }
                            }
                            None => {
                                let _ = internal_shutdown_tx_writer.send(true);
                                break;
                            }
                        }
                    }
                }
            }
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
    primary_dispatch: Option<&Arc<PrimaryDispatchQueue>>,
    outbound_tx: &mpsc::Sender<OutboundFrame>,
    outcome: InboundOutcome,
) {
    let send_a1 = |frame: OutboundFrame| {
        let outbound_tx = outbound_tx.clone();
        let primary_dispatch = primary_dispatch.cloned();
        async move {
            if callis == CallisKind::Primary {
                if let Some(dispatch) = primary_dispatch.as_ref() {
                    dispatch.enqueue_a1_frame(frame, None).await;
                    return;
                }
            }
            let _ = outbound_tx.send(frame).await;
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

fn has_blob_callis_waiters(waiters: &HashMap<PeerMessageId, InboundWaiter>) -> bool {
    waiters
        .values()
        .any(|waiter| matches!(waiter, InboundWaiter::BlobCallis(_)))
}

pub(super) async fn drain_accept_waiters(
    waiters: &mut HashMap<PeerMessageId, InboundWaiter>,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
) -> Vec<InboundOutcome> {
    enum ReadyKind {
        Message(Result<Result<(), AureliaError>, TryRecvError>),
        Blob(Result<Result<(), AureliaError>, TryRecvError>),
    }

    let mut ready = Vec::new();
    for (peer_msg_id, waiter) in waiters.iter_mut() {
        match waiter {
            InboundWaiter::Message(waiter) => {
                let result = waiter.accept_rx.try_recv();
                if !matches!(result, Err(TryRecvError::Empty)) {
                    ready.push((*peer_msg_id, ReadyKind::Message(result)));
                }
            }
            InboundWaiter::BlobAccept(waiter) => {
                let result = waiter.pending.accept_rx.try_recv();
                if !matches!(result, Err(TryRecvError::Empty)) {
                    ready.push((*peer_msg_id, ReadyKind::Blob(result)));
                }
            }
            InboundWaiter::BlobCallis(_) => {}
        }
    }

    let mut outcomes = Vec::new();
    for (peer_msg_id, ready_kind) in ready {
        let Some(waiter) = waiters.remove(&peer_msg_id) else {
            continue;
        };
        match (waiter, ready_kind) {
            (InboundWaiter::Message(waiter), ReadyKind::Message(result)) => match result {
                Ok(Ok(())) => {
                    session.dedupe_complete(peer_msg_id, Ok(())).await;
                    outcomes.push(InboundOutcome::Ack(peer_msg_id));
                }
                Ok(Err(err)) => {
                    if err.kind == ErrorId::TabernaBusy {
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
                    outcomes.push(InboundOutcome::Error { peer_msg_id, err });
                }
                Err(TryRecvError::Closed) => {
                    let err = AureliaError::new(ErrorId::RemoteTabernaRejected);
                    session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    outcomes.push(InboundOutcome::Error { peer_msg_id, err });
                }
                Err(TryRecvError::Empty) => {
                    waiters.insert(
                        peer_msg_id,
                        InboundWaiter::Message(MessageWaiter {
                            dst_taberna: waiter.dst_taberna,
                            accept_rx: waiter.accept_rx,
                        }),
                    );
                }
            },
            (InboundWaiter::BlobAccept(waiter), ReadyKind::Blob(result)) => match result {
                Ok(Ok(())) => {
                    waiter
                        .pending
                        .receiver_state
                        .accepted
                        .store(true, Ordering::SeqCst);
                    waiter.pending.receiver_state.notify.notify_waiters();
                    blob.add_pending_request(
                        peer_msg_id,
                        waiter.pending.dst_taberna,
                        Arc::clone(&waiter.pending.receiver_state),
                    )
                    .await;

                    if !session.is_active() {
                        blob.drop_pending_request(peer_msg_id).await;
                        let err = AureliaError::new(ErrorId::BlobCallisWithoutPrimary);
                        {
                            let mut guard = waiter.pending.receiver_state.error.lock().await;
                            *guard = Some(err.clone());
                        }
                        waiter
                            .pending
                            .receiver_state
                            .completed
                            .store(true, Ordering::SeqCst);
                        waiter.pending.receiver_state.notify.notify_waiters();
                        warn!(
                            taberna_id = waiter.pending.dst_taberna,
                            peer_msg_id,
                            error = %err,
                            "blob request rejected"
                        );
                        session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                        outcomes.push(InboundOutcome::Error { peer_msg_id, err });
                        continue;
                    }

                    let has_callis = blob.has_callis().await;
                    if !has_callis {
                        info!(
                            taberna_id = waiter.pending.dst_taberna,
                            peer_msg_id, "blob callis establishment requested"
                        );
                        let _ = waiter
                            .pending
                            .peer_state_tx
                            .send(PeerStateUpdate::EnsureBlobDial)
                            .await;
                    }
                    if has_callis {
                        session.dedupe_complete(peer_msg_id, Ok(())).await;
                        info!(
                            taberna_id = waiter.pending.dst_taberna,
                            peer_msg_id, "blob request accepted"
                        );
                        outcomes.push(InboundOutcome::Ack(peer_msg_id));
                    } else {
                        let deadline = Instant::now() + waiter.pending.send_timeout;
                        waiters.insert(
                            peer_msg_id,
                            InboundWaiter::BlobCallis(BlobCallisWaiter {
                                pending: waiter.pending,
                                deadline,
                            }),
                        );
                    }
                }
                Ok(Err(err)) => {
                    blob.release_inbound(peer_msg_id).await;
                    warn!(
                        taberna_id = waiter.pending.dst_taberna,
                        peer_msg_id,
                        error = %err,
                        "blob request rejected"
                    );
                    session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    outcomes.push(InboundOutcome::Error { peer_msg_id, err });
                }
                Err(TryRecvError::Closed) => {
                    blob.release_inbound(peer_msg_id).await;
                    let err = AureliaError::new(ErrorId::RemoteTabernaRejected);
                    warn!(
                        taberna_id = waiter.pending.dst_taberna,
                        peer_msg_id,
                        error = %err,
                        "blob request rejected"
                    );
                    session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
                    outcomes.push(InboundOutcome::Error { peer_msg_id, err });
                }
                Err(TryRecvError::Empty) => {
                    waiters.insert(
                        peer_msg_id,
                        InboundWaiter::BlobAccept(BlobAcceptWaiter {
                            pending: waiter.pending,
                        }),
                    );
                }
            },
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
            {
                let mut guard = waiter.pending.receiver_state.error.lock().await;
                *guard = Some(err.clone());
            }
            waiter
                .pending
                .receiver_state
                .completed
                .store(true, Ordering::SeqCst);
            waiter.pending.receiver_state.notify.notify_waiters();
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
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_inbound_frame(
    registry: Arc<TabernaRegistry>,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    config: DomusConfigAccess,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    callis_id: CallisId,
    _callis: CallisKind,
    primary_dispatch: Option<Arc<PrimaryDispatchQueue>>,
    header: WireHeader,
    payload: Vec<u8>,
    outbound_tx: mpsc::Sender<OutboundFrame>,
    cancel_reason: CancelReason,
    accept_notify: Arc<Notify>,
    cancel_tx: &watch::Sender<CancelReason>,
) -> Result<InboundAction, AureliaError> {
    let flags = WireFlags::from_bits(header.flags)
        .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
    if flags.contains(WireFlags::RECONNECT) {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    match header.msg_type {
        MSG_ACK => {
            if !flags.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            session.handle_ack(header.peer_msg_id).await;
            blob.handle_ack(header.peer_msg_id).await;
            Ok(InboundAction::None)
        }
        MSG_ERROR => {
            if !flags.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            let error = ErrorPayload::from_bytes(&payload)?;
            let kind = ErrorId::try_from(error.error_id).unwrap_or(ErrorId::PeerUnavailable);
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
            if !flags.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            set_cancel_reason(cancel_tx, CancelReason::RemoteClose);
            session.handle_close().await;
            Err(AureliaError::new(ErrorId::PeerUnavailable))
        }
        MSG_KEEPALIVE => {
            if !flags.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            Ok(InboundAction::None)
        }
        MSG_HELLO | MSG_HELLO_RESPONSE => Err(AureliaError::new(ErrorId::ProtocolViolation)),
        MSG_BLOB_TRANSFER_START => {
            if !flags.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            let cfg = config.snapshot().await;
            let completion_ttl = cfg.send_timeout * 2;
            let settings = blob
                .settings_for_callis(callis_id)
                .await
                .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
            let result =
                handle_blob_transfer_start(blob.as_ref(), &payload, completion_ttl, settings).await;
            if let Err(err) = result {
                let payload = ErrorPayload::new(err.kind.as_u32(), err.to_string()).to_bytes();
                let _ = outbound_tx
                    .send(OutboundFrame::Control {
                        msg_type: MSG_ERROR,
                        peer_msg_id: header.peer_msg_id,
                        payload: Bytes::from(payload),
                    })
                    .await;
            } else {
                let _ = outbound_tx
                    .send(OutboundFrame::Ack {
                        peer_msg_id: header.peer_msg_id,
                    })
                    .await;
            }
            Ok(InboundAction::None)
        }
        MSG_BLOB_TRANSFER_CHUNK => {
            if !flags.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
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
            match result {
                Ok(outcome) => {
                    let _ = outbound_tx
                        .send(OutboundFrame::Ack {
                            peer_msg_id: header.peer_msg_id,
                        })
                        .await;
                    if let BlobChunkOutcome::Complete(stream_id) = outcome {
                        let complete_id = blob.next_peer_msg_id();
                        let payload = BlobTransferCompletePayload {
                            request_msg_id: stream_id,
                        };
                        let _ = outbound_tx
                            .send(OutboundFrame::Control {
                                msg_type: MSG_BLOB_TRANSFER_COMPLETE,
                                peer_msg_id: complete_id,
                                payload: Bytes::from(payload.to_bytes().to_vec()),
                            })
                            .await;
                    }
                }
                Err(err) => {
                    let payload = ErrorPayload::new(err.kind.as_u32(), err.to_string()).to_bytes();
                    let _ = outbound_tx
                        .send(OutboundFrame::Control {
                            msg_type: MSG_ERROR,
                            peer_msg_id: header.peer_msg_id,
                            payload: Bytes::from(payload),
                        })
                        .await;
                }
            }
            Ok(InboundAction::None)
        }
        MSG_BLOB_TRANSFER_COMPLETE => {
            if !flags.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            let complete = BlobTransferCompletePayload::from_bytes(&payload)?;
            blob.handle_complete(complete.request_msg_id).await;
            let _ = outbound_tx
                .send(OutboundFrame::Ack {
                    peer_msg_id: header.peer_msg_id,
                })
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
                    Err(err) => Ok(InboundAction::Outcome(InboundOutcome::Error {
                        peer_msg_id,
                        err,
                    })),
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
                }
            } else {
                Err(AureliaError::new(ErrorId::ProtocolViolation))
            }
        }
    }
}
