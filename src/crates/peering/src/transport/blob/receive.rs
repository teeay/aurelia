// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::transport::blob::io::BlobReceiverStream;
use crate::BlobReceiver;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tokio::sync::Notify;

#[allow(clippy::too_many_arguments)]
pub(in crate::transport) async fn handle_blob_request(
    registry: Arc<TabernaRegistry>,
    session: Arc<PeerSession>,
    blob: Arc<BlobManager>,
    config: DomusConfigAccess,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
    header: WireHeader,
    payload: Vec<u8>,
    notify: Option<Arc<Notify>>,
) -> Result<BlobRequestSchedule, AureliaError> {
    let peer_msg_id = header.peer_msg_id;
    debug!(
        taberna_id = header.dst_taberna,
        peer_msg_id, "blob request received"
    );
    match session
        .dedupe_begin(peer_msg_id, notify.as_ref().map(Arc::clone))
        .await
    {
        crate::session::DedupeBegin::Duplicate(result) => {
            let outcome = match result {
                crate::session::DedupeDecision::Ack => {
                    Ok(BlobRequestSchedule::Immediate(BlobRequestOutcome::Ack))
                }
                crate::session::DedupeDecision::Error(err) => Err(err),
                crate::session::DedupeDecision::Abandoned => {
                    Ok(BlobRequestSchedule::Immediate(BlobRequestOutcome::Skip))
                }
            };
            trace!(
                taberna_id = header.dst_taberna,
                peer_msg_id,
                "blob request deduped"
            );
            return outcome;
        }
        crate::session::DedupeBegin::PendingDuplicate(decision_rx) => {
            trace!(
                taberna_id = header.dst_taberna,
                peer_msg_id,
                "pending duplicate blob request"
            );
            return Ok(BlobRequestSchedule::PendingDuplicate(decision_rx));
        }
        crate::session::DedupeBegin::New => {}
    }

    let inbox = match registry.resolve_local(header.dst_taberna).await {
        Some(inbox) => inbox,
        None => {
            let err = AureliaError::new(ErrorId::UnknownTaberna);
            warn!(
                taberna_id = header.dst_taberna,
                peer_msg_id,
                error = %err,
                "blob request rejected"
            );
            session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
            return Err(err);
        }
    };

    let cfg = config.snapshot().await;
    let reservation_bytes =
        (cfg.blob_window.chunk_size() as u64).saturating_mul(cfg.blob_window.ack_window() as u64);
    if !blob
        .reserve_inbound(
            peer_msg_id,
            reservation_bytes,
            cfg.blob_inbound_buffer_bytes,
        )
        .await
    {
        let err = blob_buffer_full_error("inbound", cfg.blob_inbound_buffer_bytes);
        warn!(
            taberna_id = header.dst_taberna,
            peer_msg_id,
            error = %err,
            "blob request rejected"
        );
        session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
        return Err(err);
    }

    let receiver_state = Arc::new(BlobReceiverState {
        notify: Arc::new(Notify::new()),
        accepted: AtomicBool::new(false),
        completed: AtomicBool::new(false),
        error: Mutex::new(None),
        completion_ttl: cfg.send_timeout * 2,
        idle_timeout: cfg.send_timeout * 2,
    });
    let receiver = BlobReceiver::new(Box::new(BlobReceiverStream::new(
        Arc::clone(&blob),
        peer_msg_id,
        Arc::clone(&receiver_state),
        session.runtime_handle(),
    )));

    let accept_rx = match inbox
        .enqueue(
            header.msg_type,
            Bytes::from(payload),
            Some(receiver),
            notify,
        )
        .await
    {
        Ok(rx) => rx,
        Err(err) => {
            blob.release_inbound(peer_msg_id).await;
            warn!(
                taberna_id = header.dst_taberna,
                peer_msg_id,
                error = %err,
                "blob request rejected"
            );
            session.dedupe_complete(peer_msg_id, Err(err.clone())).await;
            return Err(err);
        }
    };

    Ok(BlobRequestSchedule::Pending(BlobAcceptPending {
        dst_taberna: header.dst_taberna,
        accept_rx,
        receiver_state,
        send_timeout: cfg.send_timeout,
        peer_state_tx,
    }))
}

pub(crate) fn blob_buffer_full_error(direction: &str, cap_bytes: u64) -> AureliaError {
    AureliaError::with_message(
        ErrorId::BlobBufferFull,
        format!(
            "direction={} cap_bytes={} reservation_based=true",
            direction, cap_bytes
        ),
    )
}

pub(in crate::transport) async fn handle_blob_transfer_chunk(
    blob: &BlobManager,
    payload: &[u8],
    idle_timeout: Duration,
    ack_window: u32,
    chunk_size: u32,
) -> Result<BlobChunkOutcome, AureliaError> {
    let chunk = BlobTransferChunkPayload::from_bytes(payload)?;
    let stream_id = chunk.request_msg_id;

    if ack_window == 0 || chunk_size == 0 {
        warn!(stream_id, "blob chunk rejected: invalid window/chunk size");
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    if chunk.chunk.len() > chunk_size as usize {
        let err = AureliaError::new(ErrorId::ProtocolViolation);
        let _ = blob.fail_inbound_stream(stream_id, err.clone()).await;
        warn!(
            stream_id,
            "blob chunk rejected: chunk exceeds negotiated size"
        );
        return Err(err);
    }

    let mut expect_close = false;
    let mut error: Option<AureliaError> = None;

    let (ring, notify, receiver_state, taberna_id) =
        match blob.recv_chunk_state(stream_id, idle_timeout).await {
            BlobRecvChunkState::Missing => {
                warn!(stream_id, "blob chunk rejected: stream not found");
                return Err(AureliaError::new(ErrorId::BlobStreamNotFound));
            }
            BlobRecvChunkState::RecentlyCompleted => {
                debug!(stream_id, "blob chunk deduped after completion");
                return Ok(BlobChunkOutcome::Continue);
            }
            BlobRecvChunkState::IdleTimedOut {
                taberna_id: state_taberna_id,
                receiver,
            } => {
                warn!(stream_id, "blob chunk rejected: idle timeout");
                error = Some(AureliaError::new(ErrorId::BlobStreamIdleTimeout));
                (None, None, Some(receiver), Some(state_taberna_id))
            }
            BlobRecvChunkState::Active {
                taberna_id: state_taberna_id,
                receiver,
                ring: state_ring,
                notify: state_notify,
            } => (
                Some(state_ring),
                Some(state_notify),
                Some(receiver),
                Some(state_taberna_id),
            ),
        };

    if error.is_none() {
        if let Some(ring) = ring.as_ref() {
            match ring
                .insert_chunk(
                    chunk.chunk_id,
                    chunk.chunk.clone(),
                    chunk.flags.contains(BlobChunkFlags::LAST_CHUNK),
                )
                .await
            {
                Ok(crate::ring_buffer::InboundInsertOutcome::Duplicate) => {
                    blob.refresh_recv_deadline(stream_id, idle_timeout).await;
                }
                Ok(crate::ring_buffer::InboundInsertOutcome::Stored {
                    complete,
                    wait_for_space: _,
                }) => {
                    blob.refresh_recv_deadline(stream_id, idle_timeout).await;
                    if complete {
                        expect_close = true;
                        if let Some(receiver) = receiver_state.as_ref() {
                            receiver.completed.store(true, Ordering::SeqCst);
                        }
                    }
                }
                Err(err) => error = Some(err),
            }
        }
    }

    if let Some(err) = error {
        if !blob.fail_inbound_stream(stream_id, err.clone()).await {
            if let Some(receiver) = receiver_state.clone() {
                receiver.fail(err.clone()).await;
            }
        }
        if let Some(taberna_id) = taberna_id {
            warn!(
                taberna_id,
                stream_id,
                error = %err,
                "blob chunk rejected"
            );
        }
        return Err(err);
    }

    if let Some(notify) = notify {
        notify.notify_waiters();
    }

    if expect_close {
        if let Some(taberna_id) = taberna_id {
            info!(taberna_id, stream_id, "blob transfer complete");
        }
        return Ok(BlobChunkOutcome::Complete(stream_id));
    }

    Ok(BlobChunkOutcome::Continue)
}
