// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[allow(clippy::too_many_arguments)]
pub(in crate::transport) async fn send_blob_control_and_wait_ack(
    blob: &BlobManager,
    ring: &crate::ring_buffer::OutboundRingBuffer,
    stream_id: PeerMessageId,
    peer_msg_id: PeerMessageId,
    retained_kind: RetainedBlobKind,
    frame: OutboundFrame,
    deadline: Instant,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
) -> Result<(), AureliaError> {
    ring.register_control(peer_msg_id).await;
    blob.retain_frame(stream_id, peer_msg_id, retained_kind, frame.clone())
        .await;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(AureliaError::new(ErrorId::SendTimeout));
        }
        let Some((callis_id, handle)) = blob.take_available_callis().await else {
            if tokio::time::timeout(remaining, blob.dispatch_handle().notified())
                .await
                .is_err()
            {
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
            continue;
        };
        match tokio::time::timeout(remaining, handle.tx.send(frame.clone())).await {
            Ok(Ok(())) => {
                blob.track_inflight(stream_id, peer_msg_id, callis_id, false)
                    .await;
                break;
            }
            Ok(Err(_)) => {
                let _ = peer_state_tx
                    .send(PeerStateUpdate::Disconnect {
                        callis: CallisKind::Blob,
                        id: Some(callis_id),
                    })
                    .await;
                blob.schedule_replay_for_streams([stream_id]).await;
                blob.notify_dispatch();
                continue;
            }
            Err(_) => {
                handle.available.store(true, Ordering::SeqCst);
                blob.notify_dispatch();
                return Err(AureliaError::new(ErrorId::SendTimeout));
            }
        }
    }
    ring.wait_for_control(peer_msg_id, deadline).await
}

pub(in crate::transport) async fn dispatch_blob(
    blob: &Arc<BlobManager>,
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    notify: &Arc<Notify>,
) {
    loop {
        let Some((callis_id, handle)) = blob.take_available_callis().await else {
            return;
        };
        let selected = match blob.next_replay_frame().await {
            Some(frame) => Some(frame),
            None => blob.next_chunk_frame().await,
        };
        let Some(dispatch) = selected else {
            handle.available.store(true, Ordering::SeqCst);
            notify.notify_one();
            return;
        };
        if let Some(kind) = dispatch.retained_kind {
            blob.retain_frame(
                dispatch.stream_id,
                dispatch.peer_msg_id,
                kind,
                dispatch.frame.clone(),
            )
            .await;
        }
        match handle.tx.send(dispatch.frame.clone()).await {
            Ok(()) => {
                blob.track_inflight(
                    dispatch.stream_id,
                    dispatch.peer_msg_id,
                    callis_id,
                    dispatch.is_chunk,
                )
                .await;
            }
            Err(_) => {
                let _ = peer_state_tx
                    .send(PeerStateUpdate::Disconnect {
                        callis: CallisKind::Blob,
                        id: Some(callis_id),
                    })
                    .await;
                blob.schedule_replay_for_streams([dispatch.stream_id]).await;
                blob.notify_dispatch();
            }
        }
    }
}
