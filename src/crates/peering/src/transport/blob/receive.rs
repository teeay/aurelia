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
    match session.dedupe_begin(peer_msg_id).await {
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
    let reservation_bytes = (cfg.blob_chunk_size as u64).saturating_mul(cfg.blob_ack_window as u64);
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

pub(in crate::transport) async fn handle_blob_transfer_start(
    blob: &BlobManager,
    payload: &[u8],
    recent_ttl: Duration,
    settings: BlobCallisSettings,
) -> Result<(), AureliaError> {
    let start = BlobTransferStartPayload::from_bytes(payload)?;
    let stream_id = start.request_msg_id;
    {
        let recv = blob.recv_streams.lock().await;
        if recv.contains_key(&stream_id) {
            debug!(stream_id, "blob transfer start deduped");
            return Ok(());
        }
    }
    if blob.recently_completed(stream_id, recent_ttl).await {
        debug!(stream_id, "blob transfer start deduped after completion");
        return Ok(());
    }
    let Some(pending) = blob.take_pending_request(stream_id).await else {
        warn!(stream_id, "blob transfer start without pending request");
        return Err(AureliaError::new(ErrorId::BlobStreamNotFound));
    };
    let taberna_id = pending.taberna_id;
    let ring = Arc::new(crate::ring_buffer::InboundRingBuffer::new(
        settings.chunk_size as usize,
        settings.ack_window_chunks as usize,
    )?);
    blob.insert_recv_stream(stream_id, taberna_id, Arc::clone(&pending.receiver), ring)
        .await;
    pending.receiver.notify.notify_waiters();
    info!(taberna_id, stream_id, "blob transfer start accepted");
    Ok(())
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
        warn!(
            stream_id,
            "blob chunk rejected: chunk exceeds negotiated size"
        );
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }

    let mut expect_close = false;
    let mut wait_for_space = false;
    let mut remove_stream = false;
    let mut error: Option<AureliaError> = None;
    let mut ring: Option<Arc<crate::ring_buffer::InboundRingBuffer>> = None;
    let mut notify: Option<Arc<Notify>> = None;
    let mut receiver_state: Option<Arc<BlobReceiverState>> = None;
    let mut taberna_id: Option<TabernaId> = None;

    {
        let mut recv = blob.recv_streams.lock().await;
        let now = Instant::now();
        let Some(state) = recv.get_mut(&stream_id) else {
            warn!(stream_id, "blob chunk rejected: stream not found");
            if blob.recently_completed(stream_id, idle_timeout).await {
                debug!(stream_id, "blob chunk deduped after completion");
                return Ok(BlobChunkOutcome::Complete(stream_id));
            }
            return Err(AureliaError::new(ErrorId::BlobStreamNotFound));
        };

        if now.duration_since(state.last_activity) > idle_timeout {
            warn!(stream_id, "blob chunk rejected: idle timeout");
            error = Some(AureliaError::new(ErrorId::BlobStreamIdleTimeout));
            remove_stream = true;
        } else {
            state.last_activity = now;
            ring = Some(Arc::clone(&state.ring));
            receiver_state = Some(Arc::clone(&state.receiver));
            notify = Some(Arc::clone(&state.receiver.notify));
            taberna_id = Some(state.taberna_id);
        }
    }

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
                Ok(crate::ring_buffer::InboundInsertOutcome::Duplicate) => {}
                Ok(crate::ring_buffer::InboundInsertOutcome::Stored {
                    complete,
                    wait_for_space: wait,
                }) => {
                    wait_for_space = wait;
                    if complete {
                        expect_close = true;
                        if let Some(receiver) = receiver_state.as_ref() {
                            receiver.completed.store(true, Ordering::SeqCst);
                        }
                    }
                }
                Err(err) => {
                    if err.kind == ErrorId::BlobStreamMissingChunk {
                        remove_stream = true;
                    }
                    error = Some(err);
                }
            }
        }
    }

    if let Some(err) = error {
        if let Some(receiver) = receiver_state.clone() {
            {
                let mut guard = receiver.error.lock().await;
                *guard = Some(err.clone());
            }
            receiver.completed.store(true, Ordering::SeqCst);
            receiver.notify.notify_waiters();
        }
        if remove_stream {
            let _ = blob.remove_recv_stream(stream_id).await;
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

    if wait_for_space {
        let deadline = Instant::now() + idle_timeout;
        let Some(ring) = ring else {
            return Ok(BlobChunkOutcome::Continue);
        };
        if !ring.wait_for_space(deadline).await {
            if let Some(receiver) = receiver_state {
                let err = AureliaError::new(ErrorId::BlobStreamIdleTimeout);
                {
                    let mut guard = receiver.error.lock().await;
                    *guard = Some(err.clone());
                }
                receiver.completed.store(true, Ordering::SeqCst);
                receiver.notify.notify_waiters();
            }
            let _ = blob.remove_recv_stream(stream_id).await;
            return Err(AureliaError::new(ErrorId::BlobStreamIdleTimeout));
        }
    }

    if expect_close {
        if let Some(taberna_id) = taberna_id {
            info!(taberna_id, stream_id, "blob transfer complete");
        }
        return Ok(BlobChunkOutcome::Complete(stream_id));
    }

    Ok(BlobChunkOutcome::Continue)
}
