// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::session::CancelReason;
use crate::transport::blob::io::{BlobReceiverStream, BlobSenderStream};
use crate::transport::blob::BlobReceiverState;
use crate::transport::callis::{drain_accept_waiters, send_inbound_outcome, InboundAction};
use crate::BlobReceiver;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::sync::Notify;

fn new_receiver(
    blob: &Arc<BlobManager>,
    stream_id: PeerMessageId,
) -> (Arc<BlobReceiverState>, BlobReceiver) {
    let state = Arc::new(BlobReceiverState {
        notify: Arc::new(Notify::new()),
        accepted: AtomicBool::new(true),
        completed: AtomicBool::new(false),
        error: Mutex::new(None),
        completion_ttl: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(1),
    });
    let stream = BlobReceiverStream::new(
        Arc::clone(blob),
        stream_id,
        Arc::clone(&state),
        tokio::runtime::Handle::current(),
    );
    let receiver = BlobReceiver::new(Box::new(stream));
    (state, receiver)
}

async fn apply_inbound_action(
    action: InboundAction,
    session: &Arc<PeerSession>,
    blob: &Arc<BlobManager>,
    primary_dispatch: &Arc<PrimaryDispatchQueue>,
    outbound_tx: &mpsc::Sender<OutboundFrame>,
) {
    match action {
        InboundAction::None => {}
        InboundAction::Outcome(outcome) => {
            send_inbound_outcome(
                CallisKind::Primary,
                Some(primary_dispatch),
                outbound_tx,
                outcome,
            )
            .await;
        }
        InboundAction::Waiter {
            peer_msg_id,
            waiter,
        } => {
            let mut waiters = HashMap::new();
            waiters.insert(peer_msg_id, waiter);
            let outcomes = drain_accept_waiters(&mut waiters, session, blob).await;
            for outcome in outcomes {
                send_inbound_outcome(
                    CallisKind::Primary,
                    Some(primary_dispatch),
                    outbound_tx,
                    outcome,
                )
                .await;
            }
        }
    }
}

fn blob_settings(chunk_size: u32, ack_window_chunks: u32) -> BlobCallisSettings {
    BlobCallisSettings {
        chunk_size,
        ack_window_chunks,
    }
}

async fn start_stream(
    blob: &Arc<BlobManager>,
    stream_id: PeerMessageId,
    settings: BlobCallisSettings,
) -> BlobReceiver {
    let (state, receiver) = new_receiver(blob, stream_id);
    blob.add_pending_request(stream_id, 7, Arc::clone(&state))
        .await;
    let payload = BlobTransferStartPayload {
        request_msg_id: stream_id,
    }
    .to_bytes();
    handle_blob_transfer_start(blob, &payload, Duration::from_secs(1), settings)
        .await
        .expect("start");
    receiver
}

#[tokio::test]
async fn send_blob_frame_waits_for_matching_ack() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let (tx, mut rx) = mpsc::channel(8);
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let available = Arc::new(AtomicBool::new(true));
    let handle = CallisHandle {
        id: callis_id,
        tx,
        shutdown: shutdown_tx,
        available: Arc::clone(&available),
    };
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    blob.add_callis(handle, settings, true).await;
    let _events_tx = spawn_blob_dispatcher(Arc::clone(&blob)).await;

    let stream_id = 1;
    let peer_msg_id = 7;
    let ring = blob
        .register_outbound_stream(stream_id, callis_id, settings)
        .await
        .expect("ring");
    let blob_ref = Arc::clone(&blob);
    let available_ref = Arc::clone(&available);
    let notify = blob.dispatch_handle();
    let (peer_state_tx, _peer_state_rx) = mpsc::channel(1);
    let ack_task = tokio::spawn(async move {
        if let Some(OutboundFrame::Control {
            peer_msg_id: sent_id,
            ..
        }) = rx.recv().await
        {
            assert_eq!(sent_id, peer_msg_id);
            blob_ref.handle_ack(peer_msg_id).await;
            available_ref.store(true, Ordering::SeqCst);
            notify.notify_waiters();
        }
    });

    let frame = OutboundFrame::Control {
        msg_type: MSG_BLOB_TRANSFER_START,
        peer_msg_id,
        payload: Bytes::new(),
    };
    let deadline = Instant::now() + Duration::from_secs(1);
    let result = send_blob_control_and_wait_ack(
        blob.as_ref(),
        ring.as_ref(),
        stream_id,
        peer_msg_id,
        RetainedBlobKind::Start,
        frame,
        deadline,
        &peer_state_tx,
    )
    .await;
    ack_task.await.expect("ack task");
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_blob_frame_reports_error_event() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let (tx, mut rx) = mpsc::channel(8);
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let available = Arc::new(AtomicBool::new(true));
    let handle = CallisHandle {
        id: callis_id,
        tx,
        shutdown: shutdown_tx,
        available: Arc::clone(&available),
    };
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    blob.add_callis(handle, settings, true).await;
    let _events_tx = spawn_blob_dispatcher(Arc::clone(&blob)).await;

    let stream_id = 2;
    let peer_msg_id = 9;
    let ring = blob
        .register_outbound_stream(stream_id, callis_id, settings)
        .await
        .expect("ring");
    let blob_ref = Arc::clone(&blob);
    let available_ref = Arc::clone(&available);
    let notify = blob.dispatch_handle();
    let err_task = tokio::spawn(async move {
        if let Some(OutboundFrame::Control {
            peer_msg_id: sent_id,
            ..
        }) = rx.recv().await
        {
            assert_eq!(sent_id, peer_msg_id);
            blob_ref
                .handle_error(peer_msg_id, AureliaError::new(ErrorId::PeerUnavailable))
                .await;
            available_ref.store(true, Ordering::SeqCst);
            notify.notify_waiters();
        }
    });

    let frame = OutboundFrame::Control {
        msg_type: MSG_BLOB_TRANSFER_START,
        peer_msg_id,
        payload: Bytes::new(),
    };
    let deadline = Instant::now() + Duration::from_secs(1);
    let (peer_state_tx, _peer_state_rx) = mpsc::channel(1);
    let result = send_blob_control_and_wait_ack(
        blob.as_ref(),
        ring.as_ref(),
        stream_id,
        peer_msg_id,
        RetainedBlobKind::Start,
        frame,
        deadline,
        &peer_state_tx,
    )
    .await;
    err_task.await.expect("error task");
    assert!(matches!(result, Err(err) if err.kind == ErrorId::PeerUnavailable));
}

#[tokio::test]
async fn blob_transfer_start_registers_stream() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 42;
    let (state, receiver) = new_receiver(&blob, stream_id);
    blob.add_pending_request(stream_id, 7, state).await;

    let payload = BlobTransferStartPayload {
        request_msg_id: stream_id,
    }
    .to_bytes();
    let result = timeout(
        Duration::from_secs(1),
        handle_blob_transfer_start(&blob, &payload, Duration::from_secs(1), blob_settings(4, 4)),
    )
    .await
    .expect("start timeout");
    assert!(result.is_ok());
    let recv = timeout(Duration::from_secs(1), blob.recv_streams.lock())
        .await
        .expect("recv lock timeout");
    assert!(recv.contains_key(&stream_id));
    drop(recv);
    let active = timeout(Duration::from_secs(1), blob.has_active_streams())
        .await
        .expect("active timeout");
    assert!(active);
    drop(receiver);
}

#[tokio::test]
async fn blob_transfer_start_open_timeout_is_error() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 19;
    let payload = BlobTransferStartPayload {
        request_msg_id: stream_id,
    }
    .to_bytes();

    let err =
        handle_blob_transfer_start(&blob, &payload, Duration::from_secs(1), blob_settings(4, 4))
            .await
            .expect_err("expected missing pending request");
    assert_eq!(err.kind, ErrorId::BlobStreamNotFound);
}

#[tokio::test]
async fn blob_transfer_start_is_idempotent() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 11;
    let (state, receiver) = new_receiver(&blob, stream_id);
    blob.add_pending_request(stream_id, 9, state).await;

    let payload = BlobTransferStartPayload {
        request_msg_id: stream_id,
    }
    .to_bytes();
    timeout(
        Duration::from_secs(1),
        handle_blob_transfer_start(&blob, &payload, Duration::from_secs(1), blob_settings(4, 4)),
    )
    .await
    .expect("start timeout")
    .expect("start");
    timeout(
        Duration::from_secs(1),
        handle_blob_transfer_start(&blob, &payload, Duration::from_secs(1), blob_settings(4, 4)),
    )
    .await
    .expect("start duplicate timeout")
    .expect("start duplicate");

    let recv = timeout(Duration::from_secs(1), blob.recv_streams.lock())
        .await
        .expect("recv lock timeout");
    assert_eq!(recv.len(), 1);
    drop(receiver);
}

#[tokio::test]
async fn blob_transfer_chunk_happy_path() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 99;
    let mut receiver = start_stream(&blob, stream_id, blob_settings(3, 4)).await;

    let first = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"abc"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &first, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 0");

    let last = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 1,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"def"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &last, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 1");

    use tokio::io::AsyncReadExt;
    let mut data = Vec::new();
    timeout(Duration::from_secs(1), receiver.read_to_end(&mut data))
        .await
        .expect("read timeout")
        .expect("read data");
    assert_eq!(data, b"abcdef");
}

#[tokio::test]
async fn blob_transfer_chunk_out_of_order_is_buffered() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 77;
    let mut receiver = start_stream(&blob, stream_id, blob_settings(3, 4)).await;

    let read_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut data = Vec::new();
        timeout(Duration::from_secs(1), receiver.read_to_end(&mut data))
            .await
            .expect("read timeout")
            .expect("read data");
        data
    });

    let first = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"abc"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &first, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 0");

    let out_of_order = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 2,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"two"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &out_of_order, Duration::from_secs(1), 4, 3)
        .await
        .expect("buffered chunk 2");

    let middle = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 1,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"one"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &middle, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 1");

    let last = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 3,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"end"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &last, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 3");

    let data = read_task.await.expect("read task");
    assert_eq!(data, b"abconetwoend");
}

#[tokio::test]
async fn blob_transfer_chunk_ack_window_exceeded_is_error() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 66;
    let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

    let too_far = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 4,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"oops"),
    }
    .to_bytes();
    let err = handle_blob_transfer_chunk(&blob, &too_far, Duration::from_secs(1), 4, 4)
        .await
        .expect_err("expected ack window exceeded");
    assert_eq!(err.kind, ErrorId::BlobAckWindowExceeded);
}

#[tokio::test]
async fn blob_transfer_chunk_rejects_oversized_chunk() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 67;
    let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

    let too_large = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"toolong"),
    }
    .to_bytes();
    let err = handle_blob_transfer_chunk(&blob, &too_large, Duration::from_secs(1), 4, 4)
        .await
        .expect_err("expected oversized chunk error");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[tokio::test]
async fn blob_transfer_chunk_unknown_stream_is_error() {
    let blob = BlobManager::new_for_tests();
    let payload = BlobTransferChunkPayload {
        request_msg_id: 123,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"nope"),
    }
    .to_bytes();
    let err = handle_blob_transfer_chunk(&blob, &payload, Duration::from_secs(1), 4, 4)
        .await
        .expect_err("expected not found");
    assert_eq!(err.kind, ErrorId::BlobStreamNotFound);
}

#[tokio::test]
async fn blob_transfer_chunk_idle_timeout_is_error() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 88;
    let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

    {
        let mut recv = blob.recv_streams.lock().await;
        let state = recv.get_mut(&stream_id).expect("stream");
        state.last_activity = Instant::now() - Duration::from_secs(5);
    }

    let payload = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"late"),
    }
    .to_bytes();
    let err = handle_blob_transfer_chunk(&blob, &payload, Duration::from_secs(1), 4, 4)
        .await
        .expect_err("expected idle timeout");
    assert_eq!(err.kind, ErrorId::BlobStreamIdleTimeout);
    let recv = blob.recv_streams.lock().await;
    assert!(!recv.contains_key(&stream_id));
}

#[tokio::test]
async fn blob_transfer_chunk_dedupes_after_completion_concurrently() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 101;
    let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

    let final_payload = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"done"),
    }
    .to_bytes();
    let outcome = handle_blob_transfer_chunk(&blob, &final_payload, Duration::from_secs(1), 4, 4)
        .await
        .expect("final chunk");
    assert!(matches!(outcome, BlobChunkOutcome::Complete(id) if id == stream_id));

    let dup_payload = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"done"),
    }
    .to_bytes();
    let fut1 = handle_blob_transfer_chunk(&blob, &dup_payload, Duration::from_secs(1), 4, 4);
    let fut2 = handle_blob_transfer_chunk(&blob, &dup_payload, Duration::from_secs(1), 4, 4);
    let (res1, res2) = tokio::join!(fut1, fut2);

    assert!(
        matches!(res1, Ok(BlobChunkOutcome::Complete(id)) if id == stream_id)
            || matches!(res1, Ok(BlobChunkOutcome::Continue))
    );
    assert!(
        matches!(res2, Ok(BlobChunkOutcome::Complete(id)) if id == stream_id)
            || matches!(res2, Ok(BlobChunkOutcome::Continue))
    );
}
#[tokio::test]
async fn blob_transfer_chunk_missing_before_last_is_error() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 121;
    let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

    let last = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 1,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"late"),
    }
    .to_bytes();
    let err = handle_blob_transfer_chunk(&blob, &last, Duration::from_secs(1), 4, 4)
        .await
        .expect_err("expected missing chunk");
    assert_eq!(err.kind, ErrorId::BlobStreamMissingChunk);
}

#[tokio::test]
async fn blob_transfer_chunk_missing_buffer_full_is_error() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id = 122;
    let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

    let first = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"zero"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &first, Duration::from_secs(1), 4, 4)
        .await
        .expect("chunk 0");

    let second = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 2,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"two"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &second, Duration::from_secs(1), 4, 4)
        .await
        .expect("chunk 2");

    let third = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 3,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"tre"),
    }
    .to_bytes();
    handle_blob_transfer_chunk(&blob, &third, Duration::from_secs(1), 4, 4)
        .await
        .expect("chunk 3");

    let payload = BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 4,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"for"),
    }
    .to_bytes();
    let err = handle_blob_transfer_chunk(&blob, &payload, Duration::from_secs(1), 4, 4)
        .await
        .expect_err("expected missing chunk");
    assert_eq!(err.kind, ErrorId::BlobStreamMissingChunk);
}

#[tokio::test]
async fn blob_request_registers_pending_and_acks() {
    let registry = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![99]));
    let taberna_id = 42;
    registry.register(taberna_id, sink.clone()).await.unwrap();

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    session.set_active(true);
    let blob = Arc::new(BlobManager::new_for_tests());
    let (callis_tx, _callis_rx) = mpsc::channel(4);
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let available = Arc::new(AtomicBool::new(true));
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    blob.add_callis(
        CallisHandle {
            id: callis_id,
            tx: callis_tx,
            shutdown: shutdown_tx,
            available: Arc::clone(&available),
        },
        settings,
        true,
    )
    .await;

    let (outbound_tx, _outbound_rx) = mpsc::channel(4);
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let callis_id = next_callis_id();
    let primary_dispatch = session.primary_dispatch();

    let payload = b"hey".to_vec();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::BLOB.bits(),
        msg_type: 99,
        peer_msg_id: 10,
        src_taberna: 1,
        dst_taberna: taberna_id,
        payload_len: payload.len() as u32,
    };

    let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
    let accept_notify = Arc::new(Notify::new());
    let action = handle_inbound_frame(
        Arc::clone(&registry),
        Arc::clone(&session),
        Arc::clone(&blob),
        config.clone(),
        events_tx,
        callis_id,
        CallisKind::Primary,
        Some(Arc::clone(&primary_dispatch)),
        header,
        payload,
        outbound_tx.clone(),
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");
    apply_inbound_action(action, &session, &blob, &primary_dispatch, &outbound_tx).await;

    let frame = timeout(Duration::from_secs(1), async {
        loop {
            if let Some(frame) = primary_dispatch.pop_a1_frame().await {
                break frame;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ack timeout");
    assert!(matches!(frame, OutboundFrame::Ack { peer_msg_id: 10 }));
    assert!(session.is_duplicate(10).await);
    assert!(blob.take_pending_request(10).await.is_some());
    let received = sink.take().await;
    assert_eq!(received.len(), 1);
    assert!(received[0].2.is_some());
}

#[tokio::test]
async fn blob_request_missing_taberna_is_error() {
    let registry = Arc::new(TabernaRegistry::new());
    let taberna_id = 77;

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    session.set_active(true);
    let blob = Arc::new(BlobManager::new_for_tests());
    let (outbound_tx, _outbound_rx) = mpsc::channel(4);
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let callis_id = next_callis_id();
    let primary_dispatch = session.primary_dispatch();

    let payload = b"hey".to_vec();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::BLOB.bits(),
        msg_type: 99,
        peer_msg_id: 11,
        src_taberna: 1,
        dst_taberna: taberna_id,
        payload_len: payload.len() as u32,
    };

    let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
    let accept_notify = Arc::new(Notify::new());
    let action = handle_inbound_frame(
        Arc::clone(&registry),
        Arc::clone(&session),
        Arc::clone(&blob),
        config.clone(),
        events_tx,
        callis_id,
        CallisKind::Primary,
        Some(Arc::clone(&primary_dispatch)),
        header,
        payload,
        outbound_tx.clone(),
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");
    apply_inbound_action(action, &session, &blob, &primary_dispatch, &outbound_tx).await;

    let frame = timeout(Duration::from_secs(1), async {
        loop {
            if let Some(frame) = primary_dispatch.pop_a1_frame().await {
                break frame;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("error timeout");
    let OutboundFrame::Control {
        msg_type, payload, ..
    } = frame
    else {
        panic!("expected error frame");
    };
    assert_eq!(msg_type, MSG_ERROR);
    let decoded = ErrorPayload::from_bytes(&payload).expect("error payload");
    assert_eq!(decoded.error_id, ErrorId::UnknownTaberna.as_u32());
}

#[tokio::test]
async fn blob_request_without_primary_is_error() {
    let registry = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![99]));
    let taberna_id = 88;
    registry.register(taberna_id, sink).await.unwrap();

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    let blob = Arc::new(BlobManager::new_for_tests());
    let (outbound_tx, _outbound_rx) = mpsc::channel(4);
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let callis_id = next_callis_id();
    let primary_dispatch = session.primary_dispatch();

    let payload = b"hey".to_vec();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::BLOB.bits(),
        msg_type: 99,
        peer_msg_id: 12,
        src_taberna: 1,
        dst_taberna: taberna_id,
        payload_len: payload.len() as u32,
    };

    let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
    let accept_notify = Arc::new(Notify::new());
    let action = handle_inbound_frame(
        Arc::clone(&registry),
        Arc::clone(&session),
        Arc::clone(&blob),
        config.clone(),
        events_tx,
        callis_id,
        CallisKind::Primary,
        Some(Arc::clone(&primary_dispatch)),
        header,
        payload,
        outbound_tx.clone(),
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");
    apply_inbound_action(action, &session, &blob, &primary_dispatch, &outbound_tx).await;

    let frame = timeout(Duration::from_secs(1), async {
        loop {
            if let Some(frame) = primary_dispatch.pop_a1_frame().await {
                break frame;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("error timeout");
    let OutboundFrame::Control {
        msg_type, payload, ..
    } = frame
    else {
        panic!("expected error frame");
    };
    assert_eq!(msg_type, MSG_ERROR);
    let decoded = ErrorPayload::from_bytes(&payload).expect("error payload");
    assert_eq!(decoded.error_id, ErrorId::BlobCallisWithoutPrimary.as_u32());
    assert!(blob.take_pending_request(12).await.is_none());
}

#[tokio::test]
async fn blob_transfer_complete_is_acked_and_signals_stream() {
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    let blob = Arc::new(BlobManager::new_for_tests());
    let (outbound_tx, mut outbound_rx) = mpsc::channel(4);
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);

    let stream_id = 200;
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    let ring = blob
        .register_outbound_stream(stream_id, next_callis_id(), settings)
        .await
        .expect("ring");

    let payload = BlobTransferCompletePayload {
        request_msg_id: stream_id,
    }
    .to_bytes()
    .to_vec();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: 0,
        msg_type: MSG_BLOB_TRANSFER_COMPLETE,
        peer_msg_id: 77,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: payload.len() as u32,
    };

    let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
    let accept_notify = Arc::new(Notify::new());
    handle_inbound_frame(
        Arc::clone(&registry),
        Arc::clone(&session),
        Arc::clone(&blob),
        config.clone(),
        events_tx,
        next_callis_id(),
        CallisKind::Blob,
        None,
        header,
        payload,
        outbound_tx,
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");

    let frame = timeout(Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("ack timeout")
        .expect("ack frame");
    assert!(matches!(frame, OutboundFrame::Ack { peer_msg_id: 77 }));

    let deadline = Instant::now() + Duration::from_secs(1);
    ring.wait_for_complete(deadline)
        .await
        .expect("complete wait");
}

#[tokio::test]
async fn blob_callis_selection_skips_closed_handles() {
    let blob = BlobManager::new_for_tests();
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let available = Arc::new(AtomicBool::new(true));
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    blob.add_callis(
        CallisHandle {
            id: callis_id,
            tx,
            shutdown: shutdown_tx,
            available,
        },
        settings,
        true,
    )
    .await;

    assert!(blob.take_available_callis().await.is_none());
    assert!(!blob.has_callis().await);
}

#[tokio::test]
async fn blob_reassigns_streams_on_callis_removal() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };

    let (tx1, _rx1) = mpsc::channel(8);
    let (shutdown_tx1, _shutdown_rx1) = watch::channel(false);
    let callis_id1 = next_callis_id();
    let available1 = Arc::new(AtomicBool::new(true));
    blob.add_callis(
        CallisHandle {
            id: callis_id1,
            tx: tx1,
            shutdown: shutdown_tx1,
            available: Arc::clone(&available1),
        },
        settings,
        true,
    )
    .await;

    let (tx2, mut rx2) = mpsc::channel(8);
    let (shutdown_tx2, _shutdown_rx2) = watch::channel(false);
    let callis_id2 = next_callis_id();
    let available2 = Arc::new(AtomicBool::new(true));
    blob.add_callis(
        CallisHandle {
            id: callis_id2,
            tx: tx2,
            shutdown: shutdown_tx2,
            available: Arc::clone(&available2),
        },
        settings,
        true,
    )
    .await;

    let stream_id = 501;
    let _ring = blob
        .register_outbound_stream(stream_id, callis_id1, settings)
        .await
        .expect("ring");
    let frame = OutboundFrame::Control {
        msg_type: MSG_BLOB_TRANSFER_START,
        peer_msg_id: 55,
        payload: Bytes::new(),
    };
    blob.retain_frame(stream_id, 55, RetainedBlobKind::Start, frame.clone())
        .await;

    let _events_tx = spawn_blob_dispatcher(Arc::clone(&blob)).await;

    let (_handle, streams) = blob.remove_callis(callis_id1).await;
    blob.reassign_streams(streams).await;

    let sent = timeout(Duration::from_secs(1), rx2.recv())
        .await
        .expect("frame timeout")
        .expect("frame");
    assert!(matches!(
        sent,
        OutboundFrame::Control {
            msg_type: MSG_BLOB_TRANSFER_START,
            ..
        }
    ));
}

#[tokio::test]
async fn blob_replay_resends_retained_frames_in_order() {
    let blob = Arc::new(BlobManager::new_for_tests());
    let (tx_initial, _rx_initial) = mpsc::channel(8);
    let (shutdown_tx_initial, _shutdown_rx_initial) = watch::channel(false);
    let callis_id = next_callis_id();
    let initial_available = Arc::new(AtomicBool::new(true));
    let initial_handle = CallisHandle {
        id: callis_id,
        tx: tx_initial,
        shutdown: shutdown_tx_initial,
        available: Arc::clone(&initial_available),
    };
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };

    let stream_id = 5;
    let start_frame = OutboundFrame::Control {
        msg_type: MSG_BLOB_TRANSFER_START,
        peer_msg_id: 10,
        payload: Bytes::new(),
    };
    let chunk0 = OutboundFrame::Control {
        msg_type: MSG_BLOB_TRANSFER_CHUNK,
        peer_msg_id: 11,
        payload: Bytes::from(
            BlobTransferChunkPayload {
                request_msg_id: stream_id,
                chunk_id: 0,
                flags: BlobChunkFlags::empty(),
                chunk: Bytes::from_static(b"a"),
            }
            .to_bytes(),
        ),
    };
    let chunk1 = OutboundFrame::Control {
        msg_type: MSG_BLOB_TRANSFER_CHUNK,
        peer_msg_id: 12,
        payload: Bytes::from(
            BlobTransferChunkPayload {
                request_msg_id: stream_id,
                chunk_id: 1,
                flags: BlobChunkFlags::LAST_CHUNK,
                chunk: Bytes::from_static(b"b"),
            }
            .to_bytes(),
        ),
    };

    blob.retain_frame(
        stream_id,
        12,
        RetainedBlobKind::Chunk { chunk_id: 1 },
        chunk1.clone(),
    )
    .await;
    blob.retain_frame(stream_id, 10, RetainedBlobKind::Start, start_frame.clone())
        .await;
    blob.retain_frame(
        stream_id,
        11,
        RetainedBlobKind::Chunk { chunk_id: 0 },
        chunk0.clone(),
    )
    .await;

    blob.add_callis(initial_handle, settings, true).await;
    let _ring = blob
        .register_outbound_stream(stream_id, callis_id, settings)
        .await
        .expect("ring");
    let (_handle, _streams) = blob.remove_callis(callis_id).await;

    let (tx, mut rx) = mpsc::channel(8);
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let resume_available = Arc::new(AtomicBool::new(true));
    let resume_handle = CallisHandle {
        id: next_callis_id(),
        tx,
        shutdown: shutdown_tx,
        available: Arc::clone(&resume_available),
    };
    let _events_tx = spawn_blob_dispatcher(Arc::clone(&blob)).await;
    blob.add_callis(resume_handle, settings, true).await;

    let mut frames = Vec::new();
    let notify = blob.dispatch_handle();
    for _ in 0..3 {
        if let Some(frame) = rx.recv().await {
            frames.push(frame);
            resume_available.store(true, Ordering::SeqCst);
            notify.notify_one();
        }
    }
    assert_eq!(frames.len(), 3);
    assert!(matches!(
        frames[0],
        OutboundFrame::Control {
            msg_type: MSG_BLOB_TRANSFER_START,
            ..
        }
    ));
    let first_chunk = match &frames[1] {
        OutboundFrame::Control { payload, .. } => {
            BlobTransferChunkPayload::from_bytes(payload).expect("decode")
        }
        _ => panic!("expected chunk frame"),
    };
    let second_chunk = match &frames[2] {
        OutboundFrame::Control { payload, .. } => {
            BlobTransferChunkPayload::from_bytes(payload).expect("decode")
        }
        _ => panic!("expected chunk frame"),
    };
    assert_eq!(first_chunk.chunk_id, 0);
    assert_eq!(second_chunk.chunk_id, 1);
}

#[tokio::test]
async fn blob_resume_false_clears_retained_frames() {
    let blob = BlobManager::new_for_tests();
    let (tx, _rx) = mpsc::channel(8);
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let available = Arc::new(AtomicBool::new(true));
    let handle = CallisHandle {
        id: callis_id,
        tx,
        shutdown: shutdown_tx,
        available: Arc::clone(&available),
    };

    blob.retain_frame(
        1,
        55,
        RetainedBlobKind::Start,
        OutboundFrame::Control {
            msg_type: MSG_BLOB_TRANSFER_START,
            peer_msg_id: 55,
            payload: Bytes::new(),
        },
    )
    .await;

    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    blob.add_callis(handle, settings, false).await;

    let retained = blob.retained.lock().await;
    assert!(retained.is_empty());
}

#[tokio::test]
async fn blob_callis_missing_settings_is_closed() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);
    let handle = PeerHandle::new(
        None,
        Arc::clone(&registry),
        config.clone(),
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(config.limited_registry()),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        shutdown_rx,
        listener_shutdown_tx,
        tokio::runtime::Handle::current(),
    );

    let (tx, mut rx) = mpsc::channel(8);
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let available = Arc::new(AtomicBool::new(true));
    let info = ConnectionInfo {
        handle: CallisHandle {
            id: next_callis_id(),
            tx,
            shutdown: callis_shutdown_tx,
            available,
        },
        replay: Vec::new(),
        fresh_session: false,
        blob_settings: None,
        blob_resume: false,
    };

    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Blob,
            info,
        })
        .await
        .expect("send connected");

    let frame = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("close timeout")
        .expect("close frame");
    assert!(matches!(frame, OutboundFrame::Close));
}

#[test]
fn blob_receiver_drop_cleans_up_without_runtime() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id: PeerMessageId = 11;
    let state = Arc::new(BlobReceiverState {
        notify: Arc::new(Notify::new()),
        accepted: AtomicBool::new(true),
        completed: AtomicBool::new(false),
        error: Mutex::new(None),
        completion_ttl: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(1),
    });
    runtime.block_on(async {
        blob.add_pending_request(stream_id, 7, Arc::clone(&state))
            .await;
    });

    let stream = BlobReceiverStream::new(
        Arc::clone(&blob),
        stream_id,
        Arc::clone(&state),
        runtime.handle().clone(),
    );

    drop(stream);

    runtime.block_on(async {
        let mut cleared = false;
        for _ in 0..20 {
            if !blob.has_active_streams().await {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(cleared, "blob receiver drop should clear active streams");
    });
}

#[test]
fn blob_sender_drop_cleans_up_without_runtime() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let blob = Arc::new(BlobManager::new_for_tests());
    let stream_id: PeerMessageId = 21;
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    let ring = runtime
        .block_on(async { blob.register_outbound_stream(stream_id, 1, settings).await })
        .expect("ring");
    let sender = BlobSenderStream::new(
        Arc::clone(&blob),
        stream_id,
        ring,
        Duration::from_secs(1),
        runtime.handle().clone(),
    );

    drop(sender);

    runtime.block_on(async {
        let mut cleared = false;
        for _ in 0..20 {
            if !blob.has_active_streams().await {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(cleared, "blob sender drop should clear active streams");
    });
}
