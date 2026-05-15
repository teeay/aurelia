// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::session::CancelReason;
use crate::transport::blob::io::{BlobReceiverStream, BlobSenderStream};
use crate::transport::blob::{BlobChunkOutcome, BlobReceiverState, BlobWriteLease};
use crate::transport::callis::handle_inbound_frame;
use crate::transport::callis::{drain_accept_waiters, send_inbound_outcome, InboundAction};
use crate::BlobReceiver;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::sync::Notify;

const BLOB_MANAGER_TEST_TIMEOUT: Duration = Duration::from_secs(5);
const APP_MSG_TYPE: u32 = crate::a3_message_type(99);

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
    primary_dispatch: &Arc<PrimaryDispatchManager>,
) {
    match action {
        InboundAction::None => {}
        InboundAction::Outcome(outcome) => {
            send_inbound_outcome(
                CallisKind::Primary,
                Some(blob),
                Some(primary_dispatch),
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
                    Some(blob),
                    Some(primary_dispatch),
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

fn encode_blob_chunk_payload(payload: &BlobTransferChunkPayload) -> Vec<u8> {
    let mut out = Vec::with_capacity(BlobTransferChunkPayload::HEADER_LEN + payload.chunk.len());
    out.extend_from_slice(&payload.request_msg_id.to_be_bytes());
    out.extend_from_slice(&payload.chunk_id.to_be_bytes());
    out.extend_from_slice(&payload.flags.bits().to_be_bytes());
    out.extend_from_slice(&(payload.chunk.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload.chunk);
    out
}

fn leased_chunk(write: &BlobWriteLease) -> &crate::ring_buffer::ChunkWriteLease {
    match write {
        BlobWriteLease::Chunk { chunk, .. } => chunk,
        BlobWriteLease::Ack { .. }
        | BlobWriteLease::Error { .. }
        | BlobWriteLease::Finish { .. } => {
            panic!("expected chunk write lease")
        }
    }
}

fn new_blob_manager() -> BlobManager {
    BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::new(PeerMessageIdAllocator::default()),
        128,
    )
}

fn new_blob_manager_with_send_queue(send_queue_size: usize) -> BlobManager {
    BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::new(PeerMessageIdAllocator::default()),
        send_queue_size,
    )
}

async fn start_stream(
    blob: &Arc<BlobManager>,
    stream_id: PeerMessageId,
    settings: BlobCallisSettings,
) -> BlobReceiver {
    let (state, receiver) = new_receiver(blob, stream_id);
    blob.add_pending_request(stream_id, 7, Arc::clone(&state))
        .await;
    blob.activate_pending_request(stream_id, settings)
        .await
        .expect("activate stream");
    receiver
}

#[tokio::test]
async fn blob_pending_request_activation_registers_stream() {
    let blob = Arc::new(new_blob_manager());
    let stream_id = 42;
    let (state, receiver) = new_receiver(&blob, stream_id);
    blob.add_pending_request(stream_id, 7, state).await;

    let result = timeout(
        Duration::from_secs(1),
        blob.activate_pending_request(stream_id, blob_settings(4, 4)),
    )
    .await
    .expect("activation timeout");
    assert!(result.is_ok());
    let contains = timeout(Duration::from_secs(1), blob.recv_stream_exists(stream_id))
        .await
        .expect("recv lookup timeout");
    assert!(contains);
    let active = timeout(Duration::from_secs(1), blob.has_active_streams())
        .await
        .expect("active timeout");
    assert!(active);
    drop(receiver);
}

#[tokio::test]
async fn blob_pending_request_activation_without_request_is_error() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let stream_id = 19;

        let err = blob
            .activate_pending_request(stream_id, blob_settings(4, 4))
            .await
            .expect_err("expected missing pending request");
        assert_eq!(err.kind, ErrorId::BlobStreamNotFound);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_pending_request_activation_is_idempotent() {
    let blob = Arc::new(new_blob_manager());
    let stream_id = 11;
    let (state, receiver) = new_receiver(&blob, stream_id);
    blob.add_pending_request(stream_id, 9, state).await;

    timeout(
        Duration::from_secs(1),
        blob.activate_pending_request(stream_id, blob_settings(4, 4)),
    )
    .await
    .expect("activation timeout")
    .expect("activate");
    timeout(
        Duration::from_secs(1),
        blob.activate_pending_request(stream_id, blob_settings(4, 4)),
    )
    .await
    .expect("activation duplicate timeout")
    .expect("activation duplicate");

    use tokio::io::AsyncReadExt;

    let mut receiver = receiver;
    let mut data = Vec::new();
    let chunk = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"done"),
    });
    handle_blob_transfer_chunk(&blob, &chunk, Duration::from_secs(1), 4, 4)
        .await
        .expect("chunk after duplicate start");
    timeout(Duration::from_secs(1), receiver.read_to_end(&mut data))
        .await
        .expect("read timeout")
        .expect("read chunk");
    assert_eq!(data, b"done");
    drop(receiver);
}

#[tokio::test]
async fn blob_transfer_chunk_happy_path() {
    let blob = Arc::new(new_blob_manager());
    let stream_id = 99;
    let mut receiver = start_stream(&blob, stream_id, blob_settings(3, 4)).await;

    let first = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"abc"),
    });
    handle_blob_transfer_chunk(&blob, &first, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 0");

    let last = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 1,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"def"),
    });
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
async fn blob_transfer_chunk_full_receiver_returns_without_idle_wait() {
    let blob = Arc::new(new_blob_manager());
    let stream_id = 199;
    let _receiver = start_stream(&blob, stream_id, blob_settings(4, 1)).await;

    let first = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"one"),
    });
    handle_blob_transfer_chunk(&blob, &first, Duration::from_secs(60), 1, 4)
        .await
        .expect("first chunk accepted");

    let second = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 1,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"two"),
    });
    let err = timeout(
        Duration::from_millis(100),
        handle_blob_transfer_chunk(&blob, &second, Duration::from_secs(60), 1, 4),
    )
    .await
    .expect("full receiver must not wait for idle timeout")
    .expect_err("full receiver must reject the stream");
    assert_eq!(err.kind, ErrorId::BlobBufferFull);
    assert!(!blob.recv_stream_exists(stream_id).await);
}

#[tokio::test]
async fn blob_transfer_chunk_out_of_order_is_buffered() {
    let blob = Arc::new(new_blob_manager());
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

    let first = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"abc"),
    });
    handle_blob_transfer_chunk(&blob, &first, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 0");

    let out_of_order = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 2,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"two"),
    });
    handle_blob_transfer_chunk(&blob, &out_of_order, Duration::from_secs(1), 4, 3)
        .await
        .expect("buffered chunk 2");

    let middle = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 1,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"one"),
    });
    handle_blob_transfer_chunk(&blob, &middle, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 1");

    let last = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: stream_id,
        chunk_id: 3,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"end"),
    });
    handle_blob_transfer_chunk(&blob, &last, Duration::from_secs(1), 4, 3)
        .await
        .expect("chunk 3");

    let data = read_task.await.expect("read task");
    assert_eq!(data, b"abconetwoend");
}

#[tokio::test]
async fn blob_transfer_chunk_ack_window_exceeded_is_error() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let stream_id = 66;
        let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

        let too_far = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 4,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"oops"),
        });
        let err = handle_blob_transfer_chunk(&blob, &too_far, Duration::from_secs(1), 4, 4)
            .await
            .expect_err("expected ack window exceeded");
        assert_eq!(err.kind, ErrorId::BlobAckWindowExceeded);
        assert!(!blob.recv_stream_exists(stream_id).await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_transfer_chunk_rejects_oversized_chunk() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let stream_id = 67;
        let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

        let too_large = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 0,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"toolong"),
        });
        let err = handle_blob_transfer_chunk(&blob, &too_large, Duration::from_secs(1), 4, 4)
            .await
            .expect_err("expected oversized chunk error");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert!(!blob.recv_stream_exists(stream_id).await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_transfer_chunk_unknown_stream_is_error() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = new_blob_manager();
        let payload = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: 123,
            chunk_id: 0,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"nope"),
        });
        let err = handle_blob_transfer_chunk(&blob, &payload, Duration::from_secs(1), 4, 4)
            .await
            .expect_err("expected not found");
        assert_eq!(err.kind, ErrorId::BlobStreamNotFound);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_transfer_chunk_idle_timeout_is_error() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let stream_id = 88;
        let state = Arc::new(BlobReceiverState {
            notify: Arc::new(Notify::new()),
            accepted: AtomicBool::new(true),
            completed: AtomicBool::new(false),
            error: Mutex::new(None),
            completion_ttl: Duration::from_secs(1),
            idle_timeout: Duration::from_millis(1),
        });
        let stream = BlobReceiverStream::new(
            Arc::clone(&blob),
            stream_id,
            Arc::clone(&state),
            tokio::runtime::Handle::current(),
        );
        let _receiver = BlobReceiver::new(Box::new(stream));
        blob.add_pending_request(stream_id, 7, Arc::clone(&state))
            .await;
        blob.activate_pending_request(stream_id, blob_settings(4, 4))
            .await
            .expect("activate");
        tokio::time::sleep(Duration::from_millis(5)).await;

        let payload = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 0,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"late"),
        });
        let err = handle_blob_transfer_chunk(&blob, &payload, Duration::from_millis(1), 4, 4)
            .await
            .expect_err("expected idle timeout");
        assert_eq!(err.kind, ErrorId::BlobStreamIdleTimeout);
        assert!(!blob.recv_stream_exists(stream_id).await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_inbound_idle_reaper_expires_without_more_chunks() {
    let blob = Arc::new(new_blob_manager());
    let stream_id = 188;
    let state = Arc::new(BlobReceiverState {
        notify: Arc::new(Notify::new()),
        accepted: AtomicBool::new(true),
        completed: AtomicBool::new(false),
        error: Mutex::new(None),
        completion_ttl: Duration::from_secs(1),
        idle_timeout: Duration::from_millis(10),
    });
    let _receiver = BlobReceiver::new(Box::new(BlobReceiverStream::new(
        Arc::clone(&blob),
        stream_id,
        Arc::clone(&state),
        tokio::runtime::Handle::current(),
    )));
    blob.add_pending_request(stream_id, 7, Arc::clone(&state))
        .await;
    blob.activate_pending_request(stream_id, blob_settings(4, 4))
        .await
        .expect("activate");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let reaper_blob = Arc::clone(&blob);
    let reaper = tokio::spawn(async move {
        reaper_blob.run_inbound_idle_reaper(shutdown_rx).await;
    });

    timeout(Duration::from_secs(1), async {
        loop {
            if !blob.recv_stream_exists(stream_id).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("idle reaper timeout");
    let err = state.error.lock().await.clone().expect("receiver error");
    assert_eq!(err.kind, ErrorId::BlobStreamIdleTimeout);
    let _ = shutdown_tx.send(true);
    reaper.await.expect("reaper join");
}

#[tokio::test]
async fn blob_transfer_chunk_dedupes_after_completion_concurrently() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let stream_id = 101;
        let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

        let final_payload = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 0,
            flags: BlobChunkFlags::LAST_CHUNK,
            chunk: Bytes::from_static(b"done"),
        });
        let outcome =
            handle_blob_transfer_chunk(&blob, &final_payload, Duration::from_secs(1), 4, 4)
                .await
                .expect("final chunk");
        assert!(matches!(outcome, BlobChunkOutcome::Complete(id) if id == stream_id));

        let dup_payload = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 0,
            flags: BlobChunkFlags::LAST_CHUNK,
            chunk: Bytes::from_static(b"done"),
        });
        let fut1 = handle_blob_transfer_chunk(&blob, &dup_payload, Duration::from_secs(1), 4, 4);
        let fut2 = handle_blob_transfer_chunk(&blob, &dup_payload, Duration::from_secs(1), 4, 4);
        let (res1, res2) = tokio::join!(fut1, fut2);

        assert!(matches!(res1, Ok(BlobChunkOutcome::Continue)));
        assert!(matches!(res2, Ok(BlobChunkOutcome::Continue)));
    })
    .await
    .expect("async test timed out");
}
#[tokio::test]
async fn blob_transfer_chunk_missing_before_last_is_error() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let stream_id = 121;
        let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

        let last = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 1,
            flags: BlobChunkFlags::LAST_CHUNK,
            chunk: Bytes::from_static(b"late"),
        });
        let err = handle_blob_transfer_chunk(&blob, &last, Duration::from_secs(1), 4, 4)
            .await
            .expect_err("expected missing chunk");
        assert_eq!(err.kind, ErrorId::BlobStreamMissingChunk);
        assert!(!blob.recv_stream_exists(stream_id).await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_transfer_chunk_missing_buffer_full_is_error() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let stream_id = 122;
        let _receiver = start_stream(&blob, stream_id, blob_settings(4, 4)).await;

        let first = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 0,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"zero"),
        });
        handle_blob_transfer_chunk(&blob, &first, Duration::from_secs(1), 4, 4)
            .await
            .expect("chunk 0");

        let second = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 2,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"two"),
        });
        handle_blob_transfer_chunk(&blob, &second, Duration::from_secs(1), 4, 4)
            .await
            .expect("chunk 2");

        let third = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 3,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"tre"),
        });
        handle_blob_transfer_chunk(&blob, &third, Duration::from_secs(1), 4, 4)
            .await
            .expect("chunk 3");

        let payload = encode_blob_chunk_payload(&BlobTransferChunkPayload {
            request_msg_id: stream_id,
            chunk_id: 4,
            flags: BlobChunkFlags::empty(),
            chunk: Bytes::from_static(b"for"),
        });
        let err = handle_blob_transfer_chunk(&blob, &payload, Duration::from_secs(1), 4, 4)
            .await
            .expect_err("expected missing chunk");
        assert_eq!(err.kind, ErrorId::BlobStreamMissingChunk);
        assert!(!blob.recv_stream_exists(stream_id).await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_request_registers_pending_and_acks() {
    let registry = Arc::new(TabernaRegistry::new());
    let sink = Arc::new(RecordingSink::new(vec![APP_MSG_TYPE]));
    let taberna_id = 42;
    registry.register(taberna_id, sink.clone()).await.unwrap();

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    ));
    session.set_active(true);
    let blob = Arc::new(new_blob_manager());
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    blob.add_callis(
        CallisHandle {
            id: callis_id,
            tx: CallisTx::Blob,
            shutdown: shutdown_tx,
        },
        settings,
        true,
    )
    .await;
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let callis_id = next_callis_id();
    let primary_dispatch = session.primary_dispatch();

    let payload = b"hey".to_vec();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::BLOB.bits(),
        msg_type: APP_MSG_TYPE,
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
        Some(Arc::clone(&primary_dispatch)),
        header,
        payload,
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");
    apply_inbound_action(action, &session, &blob, &primary_dispatch).await;

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
    assert!(blob.recv_stream_exists(10).await);
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
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    ));
    session.set_active(true);
    let blob = Arc::new(new_blob_manager());
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let callis_id = next_callis_id();
    let primary_dispatch = session.primary_dispatch();

    let payload = b"hey".to_vec();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::BLOB.bits(),
        msg_type: APP_MSG_TYPE,
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
        Some(Arc::clone(&primary_dispatch)),
        header,
        payload,
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");
    apply_inbound_action(action, &session, &blob, &primary_dispatch).await;

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
    let sink = Arc::new(RecordingSink::new(vec![APP_MSG_TYPE]));
    let taberna_id = 88;
    registry.register(taberna_id, sink).await.unwrap();

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    ));
    let blob = Arc::new(new_blob_manager());
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let callis_id = next_callis_id();
    let primary_dispatch = session.primary_dispatch();

    let payload = b"hey".to_vec();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::BLOB.bits(),
        msg_type: APP_MSG_TYPE,
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
        Some(Arc::clone(&primary_dispatch)),
        header,
        payload,
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");
    apply_inbound_action(action, &session, &blob, &primary_dispatch).await;

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
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    ));
    let blob = Arc::new(new_blob_manager());
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);

    let stream_id = 200;
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    let ring = blob
        .register_outbound_stream(stream_id, settings)
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
        None,
        header,
        payload,
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .expect("handle inbound");

    let frame = timeout(
        Duration::from_secs(1),
        blob.lease_next_blob_write(next_callis_id()),
    )
    .await
    .expect("ack timeout")
    .expect("ack frame");
    assert!(matches!(frame, BlobWriteLease::Ack { peer_msg_id: 77 }));

    let deadline = Instant::now() + Duration::from_secs(1);
    ring.wait_for_complete(deadline)
        .await
        .expect("complete wait");
}

#[tokio::test]
async fn blob_response_control_dedupes_by_peer_msg_id() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = new_blob_manager();

        assert!(
            blob.enqueue_blob_write(BlobWriteLease::Ack { peer_msg_id: 55 })
                .await
        );
        assert!(
            !blob
                .enqueue_blob_write(BlobWriteLease::Ack { peer_msg_id: 55 })
                .await
        );

        let write = blob
            .lease_next_blob_write(next_callis_id())
            .await
            .expect("deduped ack");
        assert!(matches!(write, BlobWriteLease::Ack { peer_msg_id: 55 }));
        blob.finish_blob_write_attempt(&write, next_callis_id(), Ok(()))
            .await;
        assert!(blob.lease_next_blob_write(next_callis_id()).await.is_none());
    })
    .await
    .expect("async test timed out");
}

#[test]
#[should_panic(expected = "blob buffer release underflow")]
fn blob_buffer_release_underflow_is_guarded() {
    let tracker = BlobBufferTracker::default();
    tracker.release_outbound(1);
}

#[tokio::test]
async fn blob_response_ack_lane_is_bounded() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = new_blob_manager_with_send_queue(1);
        for peer_msg_id in 0..16 {
            assert!(
                blob.enqueue_blob_write(BlobWriteLease::Ack { peer_msg_id })
                    .await,
                "ack {peer_msg_id} should fit"
            );
        }
        assert!(
            !blob
                .enqueue_blob_write(BlobWriteLease::Ack { peer_msg_id: 16 })
                .await
        );
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_response_cleanup_removes_stream_writes() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = new_blob_manager();
        let stream_id = 90;
        let peer_msg_id = 91;
        let payload = BlobTransferCompletePayload {
            request_msg_id: stream_id,
        };
        assert!(
            blob.enqueue_blob_write(BlobWriteLease::Finish {
                stream_id,
                peer_msg_id,
                payload: Bytes::from(payload.to_bytes().to_vec()),
            })
            .await
        );

        blob.fail_stream(stream_id, AureliaError::new(ErrorId::PeerUnavailable))
            .await;
        assert!(blob.lease_next_blob_write(next_callis_id()).await.is_none());
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_response_cleanup_keeps_unscoped_ack_and_error_writes() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = new_blob_manager();
        let stream_id = 900;
        blob.register_outbound_stream(stream_id, blob_settings(4, 4))
            .await
            .expect("register stream");
        assert!(
            blob.enqueue_blob_write(BlobWriteLease::Ack {
                peer_msg_id: stream_id,
            })
            .await
        );
        assert!(
            blob.enqueue_blob_write(BlobWriteLease::Error {
                peer_msg_id: stream_id + 1,
                payload: Bytes::from_static(b"error"),
            })
            .await
        );

        blob.unregister_outbound_stream(stream_id).await;

        let ack = blob
            .lease_next_blob_write(next_callis_id())
            .await
            .expect("ack write");
        assert!(matches!(
            ack,
            BlobWriteLease::Ack { peer_msg_id } if peer_msg_id == stream_id
        ));
        let err = blob
            .lease_next_blob_write(next_callis_id())
            .await
            .expect("error write");
        assert!(matches!(
            err,
            BlobWriteLease::Error { peer_msg_id, .. } if peer_msg_id == stream_id + 1
        ));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_complete_for_inactive_outbound_stream_is_protocol_violation() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = new_blob_manager();
        let err = blob
            .handle_complete(77)
            .await
            .expect_err("inactive complete rejected");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_callis_generation_bumps_on_add_remove_and_drain() {
    let blob = new_blob_manager();
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };

    let mut add_rx = blob.subscribe_callis_gen();
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    blob.add_callis(
        CallisHandle {
            id: callis_id,
            tx: CallisTx::Blob,
            shutdown: shutdown_tx,
        },
        settings,
        true,
    )
    .await;
    timeout(Duration::from_secs(1), add_rx.changed())
        .await
        .expect("add generation timeout")
        .expect("add generation");

    let mut remove_rx = blob.subscribe_callis_gen();
    let _handle = blob.remove_callis(callis_id).await;
    timeout(Duration::from_secs(1), remove_rx.changed())
        .await
        .expect("remove generation timeout")
        .expect("remove generation");

    let mut drain_rx = blob.subscribe_callis_gen();
    for _ in 0..2 {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        blob.add_callis(
            CallisHandle {
                id: next_callis_id(),
                tx: CallisTx::Blob,
                shutdown: shutdown_tx,
            },
            settings,
            true,
        )
        .await;
    }
    let _handles = blob.drain_callis().await;
    timeout(Duration::from_secs(1), drain_rx.changed())
        .await
        .expect("drain generation timeout")
        .expect("drain generation");
}

#[tokio::test]
async fn blob_transmitter_writes_ring_chunk_without_writer_channel() {
    let blob = Arc::new(new_blob_manager());
    let callis_id = next_callis_id();
    let settings = BlobCallisSettings {
        chunk_size: 3,
        ack_window_chunks: 4,
    };
    let ring = blob
        .register_outbound_stream(777, settings)
        .await
        .expect("ring");
    ring.push_bytes(b"abcd", Duration::from_secs(1))
        .await
        .expect("push");

    let (writer, mut reader) = tokio::io::duplex(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_writer_shutdown_tx, writer_shutdown_rx) = watch::channel(false);
    let (internal_shutdown_tx, _internal_shutdown_rx) = watch::channel(false);
    let config = DomusConfigAccess::from_config(DomusConfig {
        send_timeout: Duration::from_secs(1),
        ..DomusConfig::default()
    });
    let blob_ref = Arc::clone(&blob);
    let transmitter = tokio::spawn(async move {
        let mut writer = writer;
        crate::transport::callis::run_blob_transmitter(
            config,
            blob_ref,
            callis_id,
            &mut writer,
            shutdown_rx,
            writer_shutdown_rx,
            internal_shutdown_tx,
        )
        .await;
    });

    blob.notify_work();
    let mut frame_reader = crate::transport::frame::FrameReadState::default();
    let (header, payload) = timeout(
        Duration::from_secs(1),
        frame_reader.read_next(&mut reader, 1024),
    )
    .await
    .expect("read timeout")
    .expect("read frame")
    .expect("frame");
    assert_eq!(header.msg_type, MSG_BLOB_TRANSFER_CHUNK);
    let chunk = BlobTransferChunkPayload::from_bytes(&payload).expect("decode chunk");
    assert_eq!(chunk.request_msg_id, 777);
    assert_eq!(chunk.chunk_id, 0);
    assert_eq!(chunk.chunk, Bytes::from_static(b"abc"));

    let _ = shutdown_tx.send(true);
    timeout(Duration::from_secs(1), transmitter)
        .await
        .expect("transmitter stop timeout")
        .expect("transmitter join");
}

#[tokio::test]
async fn blob_transmitter_writes_ack_without_writer_channel() {
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigAccess::from_config(DomusConfig {
        send_timeout: Duration::from_secs(1),
        ..DomusConfig::default()
    });
    let handler_config = config.clone();
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    ));
    session.set_active(true);

    let blob = Arc::new(new_blob_manager());
    let callis_id = next_callis_id();
    let settings = BlobCallisSettings {
        chunk_size: 3,
        ack_window_chunks: 4,
    };
    let (shutdown_handle_tx, _shutdown_handle_rx) = watch::channel(false);
    blob.add_callis(
        CallisHandle {
            id: callis_id,
            tx: CallisTx::Blob,
            shutdown: shutdown_handle_tx,
        },
        settings,
        true,
    )
    .await;

    let mut receiver = start_stream(&blob, 901, settings).await;
    let (writer, mut reader) = tokio::io::duplex(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_writer_shutdown_tx, writer_shutdown_rx) = watch::channel(false);
    let (internal_shutdown_tx, _internal_shutdown_rx) = watch::channel(false);
    let blob_ref = Arc::clone(&blob);
    let transmitter = tokio::spawn(async move {
        let mut writer = writer;
        crate::transport::callis::run_blob_transmitter(
            config,
            blob_ref,
            callis_id,
            &mut writer,
            shutdown_rx,
            writer_shutdown_rx,
            internal_shutdown_tx,
        )
        .await;
    });

    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
    let payload = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: 901,
        chunk_id: 0,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"abc"),
    });
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: 0,
        msg_type: MSG_BLOB_TRANSFER_CHUNK,
        peer_msg_id: 902,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: payload.len() as u32,
    };
    let action = handle_inbound_frame(
        registry,
        Arc::clone(&session),
        Arc::clone(&blob),
        handler_config,
        events_tx,
        callis_id,
        None,
        header,
        payload,
        CancelReason::None,
        Arc::new(Notify::new()),
        &cancel_tx,
    )
    .await
    .expect("handle inbound chunk");
    assert!(matches!(action, InboundAction::None));

    let mut frame_reader = crate::transport::frame::FrameReadState::default();
    let (header, payload) = timeout(
        Duration::from_secs(1),
        frame_reader.read_next(&mut reader, 1024),
    )
    .await
    .expect("read timeout")
    .expect("read frame")
    .expect("frame");
    assert_eq!(header.msg_type, MSG_ACK);
    assert_eq!(header.peer_msg_id, 902);
    assert!(payload.is_empty());

    use tokio::io::AsyncReadExt;
    let mut data = Vec::new();
    timeout(Duration::from_secs(1), receiver.read_to_end(&mut data))
        .await
        .expect("receiver read timeout")
        .expect("receiver read");
    assert_eq!(data, b"abc");

    let _ = shutdown_tx.send(true);
    timeout(Duration::from_secs(1), transmitter)
        .await
        .expect("transmitter stop timeout")
        .expect("transmitter join");
}

#[tokio::test]
async fn blob_transmitter_writes_error_without_writer_channel() {
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigAccess::from_config(DomusConfig {
        send_timeout: Duration::from_secs(1),
        ..DomusConfig::default()
    });
    let handler_config = config.clone();
    let session = Arc::new(PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    ));
    session.set_active(true);

    let blob = Arc::new(new_blob_manager());
    let callis_id = next_callis_id();
    let settings = BlobCallisSettings {
        chunk_size: 3,
        ack_window_chunks: 4,
    };
    let (shutdown_handle_tx, _shutdown_handle_rx) = watch::channel(false);
    blob.add_callis(
        CallisHandle {
            id: callis_id,
            tx: CallisTx::Blob,
            shutdown: shutdown_handle_tx,
        },
        settings,
        true,
    )
    .await;
    let _receiver = start_stream(&blob, 911, settings).await;

    let (writer, mut reader) = tokio::io::duplex(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_writer_shutdown_tx, writer_shutdown_rx) = watch::channel(false);
    let (internal_shutdown_tx, _internal_shutdown_rx) = watch::channel(false);
    let blob_ref = Arc::clone(&blob);
    let transmitter = tokio::spawn(async move {
        let mut writer = writer;
        crate::transport::callis::run_blob_transmitter(
            config,
            blob_ref,
            callis_id,
            &mut writer,
            shutdown_rx,
            writer_shutdown_rx,
            internal_shutdown_tx,
        )
        .await;
    });

    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
    let payload = encode_blob_chunk_payload(&BlobTransferChunkPayload {
        request_msg_id: 911,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"oversized"),
    });
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: 0,
        msg_type: MSG_BLOB_TRANSFER_CHUNK,
        peer_msg_id: 912,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: payload.len() as u32,
    };
    let action = handle_inbound_frame(
        registry,
        Arc::clone(&session),
        Arc::clone(&blob),
        handler_config,
        events_tx,
        callis_id,
        None,
        header,
        payload,
        CancelReason::None,
        Arc::new(Notify::new()),
        &cancel_tx,
    )
    .await
    .expect("handle inbound chunk");
    assert!(matches!(action, InboundAction::None));

    let mut frame_reader = crate::transport::frame::FrameReadState::default();
    let (header, payload) = timeout(
        Duration::from_secs(1),
        frame_reader.read_next(&mut reader, 1024),
    )
    .await
    .expect("read timeout")
    .expect("read frame")
    .expect("frame");
    assert_eq!(header.msg_type, MSG_ERROR);
    assert_eq!(header.peer_msg_id, 912);
    let decoded = ErrorPayload::from_bytes(&payload).expect("error payload");
    assert_eq!(decoded.error_id, ErrorId::ProtocolViolation.as_u32());

    let _ = shutdown_tx.send(true);
    timeout(Duration::from_secs(1), transmitter)
        .await
        .expect("transmitter stop timeout")
        .expect("transmitter join");
}

#[tokio::test]
async fn blob_transmitter_write_failure_replays_ring_slot() {
    struct FailingWriter;

    impl tokio::io::AsyncWrite for FailingWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write failed",
            )))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    let blob = Arc::new(new_blob_manager());
    let failed_callis = next_callis_id();
    let retry_callis = next_callis_id();
    let settings = BlobCallisSettings {
        chunk_size: 3,
        ack_window_chunks: 4,
    };
    let ring = blob
        .register_outbound_stream(778, settings)
        .await
        .expect("ring");
    ring.push_bytes(b"abcd", Duration::from_secs(1))
        .await
        .expect("push");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_writer_shutdown_tx, writer_shutdown_rx) = watch::channel(false);
    let (internal_shutdown_tx, internal_shutdown_rx) = watch::channel(false);
    let config = DomusConfigAccess::from_config(DomusConfig {
        send_timeout: Duration::from_secs(1),
        ..DomusConfig::default()
    });
    let blob_ref = Arc::clone(&blob);
    let transmitter = tokio::spawn(async move {
        let mut writer = FailingWriter;
        crate::transport::callis::run_blob_transmitter(
            config,
            blob_ref,
            failed_callis,
            &mut writer,
            shutdown_rx,
            writer_shutdown_rx,
            internal_shutdown_tx,
        )
        .await;
    });

    blob.notify_work();
    timeout(Duration::from_secs(1), transmitter)
        .await
        .expect("transmitter timeout")
        .expect("transmitter join");
    assert!(*internal_shutdown_rx.borrow());

    let replay = blob
        .lease_next_blob_write(retry_callis)
        .await
        .expect("replay lease");
    let replay_chunk = leased_chunk(&replay);
    assert_eq!(replay.stream_id(), Some(778));
    assert_eq!(replay_chunk.chunk_id, 0);
    assert_eq!(replay_chunk.data, Bytes::from_static(b"abc"));
    assert_eq!(replay_chunk.callis_id, retry_callis);

    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn blob_chunk_wire_write_uses_leased_chunk_slice() {
    struct RecordingWriter {
        writes: Vec<(usize, usize)>,
    }

    impl tokio::io::AsyncWrite for RecordingWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.writes.push((buf.len(), buf.as_ptr() as usize));
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    let chunk = Bytes::from_static(b"leased-bytes");
    let chunk_ptr = chunk.as_ptr() as usize;
    let mut writer = RecordingWriter { writes: Vec::new() };

    crate::transport::frame::send_blob_chunk_frame(
        &mut writer,
        44,
        55,
        66,
        BlobChunkFlags::LAST_CHUNK,
        &chunk,
    )
    .await
    .expect("write chunk");

    assert_eq!(writer.writes.len(), 3);
    assert_eq!(writer.writes[1].0, BlobTransferChunkPayload::HEADER_LEN);
    assert_eq!(writer.writes[2], (chunk.len(), chunk_ptr));
}

#[tokio::test]
async fn blob_write_lease_installs_inflight_before_write_completion() {
    let blob = Arc::new(new_blob_manager());
    let callis_id = next_callis_id();
    let settings = blob_settings(3, 1);
    let ring = blob
        .register_outbound_stream(778, settings)
        .await
        .expect("ring");
    ring.push_bytes(b"abcd", Duration::from_secs(1))
        .await
        .expect("push");

    let dispatch = blob
        .lease_next_blob_write(callis_id)
        .await
        .expect("dispatch");
    assert!(dispatch.is_chunk());
    assert_eq!(ring.inflight_chunk_count().await, 1);

    blob.handle_ack(dispatch.peer_msg_id()).await;
    assert_eq!(ring.inflight_chunk_count().await, 0);
    blob.finish_blob_write_attempt(&dispatch, callis_id, Ok(()))
        .await;

    timeout(
        Duration::from_secs(1),
        ring.push_bytes(b"ef", Duration::from_secs(1)),
    )
    .await
    .expect("capacity should be released")
    .expect("push after fast ack");
}

#[tokio::test]
async fn blob_sender_poll_write_returns_bounded_partial_progress() {
    let blob = Arc::new(new_blob_manager());
    let stream_id = 7790;
    let settings = blob_settings(4, 1);
    let ring = blob
        .register_outbound_stream(stream_id, settings)
        .await
        .expect("ring");
    let mut sender = BlobSenderStream::new(
        Arc::clone(&blob),
        stream_id,
        ring,
        Duration::from_secs(1),
        tokio::runtime::Handle::current(),
    );

    use tokio::io::AsyncWriteExt;
    let accepted = timeout(Duration::from_secs(1), sender.write(b"abcdefghij"))
        .await
        .expect("write timeout")
        .expect("write");
    assert!(accepted > 0);
    assert!(accepted < 10);
}

#[tokio::test]
async fn blob_write_failure_replays_chunk_from_ring_slot() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let callis_id = next_callis_id();
        let settings = blob_settings(3, 1);
        let ring = blob
            .register_outbound_stream(779, settings)
            .await
            .expect("ring");
        ring.push_bytes(b"abcd", Duration::from_secs(1))
            .await
            .expect("push");

        let first = blob
            .lease_next_blob_write(callis_id)
            .await
            .expect("first dispatch");
        let first_lease = leased_chunk(&first).clone();
        blob.finish_blob_write_attempt(
            &first,
            callis_id,
            Err(AureliaError::new(ErrorId::PeerUnavailable)),
        )
        .await;

        let replay = blob
            .lease_next_blob_write(callis_id)
            .await
            .expect("replay dispatch");
        let replay_lease = leased_chunk(&replay).clone();
        assert_eq!(replay_lease.chunk_id, first_lease.chunk_id);
        assert_eq!(replay_lease.data.as_ptr(), first_lease.data.as_ptr());
        assert!(replay_lease.slot_seq > first_lease.slot_seq);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_leasing_is_not_bound_to_stream_callis_assignment() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let first_callis = next_callis_id();
        let second_callis = next_callis_id();
        let settings = blob_settings(3, 4);
        let ring = blob
            .register_outbound_stream(780, settings)
            .await
            .expect("ring");
        ring.push_bytes(b"abcdef", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");

        let first = blob
            .lease_next_blob_write(first_callis)
            .await
            .expect("first dispatch");
        let second = blob
            .lease_next_blob_write(second_callis)
            .await
            .expect("second dispatch");

        let first_lease = leased_chunk(&first);
        let second_lease = leased_chunk(&second);
        assert_eq!(first_lease.chunk_id, 0);
        assert_eq!(first_lease.callis_id, first_callis);
        assert_eq!(second_lease.chunk_id, 1);
        assert_eq!(second_lease.callis_id, second_callis);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_callis_loss_replays_only_slots_written_on_that_callis() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let failed_callis = next_callis_id();
        let live_callis = next_callis_id();
        let settings = blob_settings(3, 4);
        let ring = blob
            .register_outbound_stream(781, settings)
            .await
            .expect("ring");
        ring.push_bytes(b"abcdef", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");

        let first = blob
            .lease_next_blob_write(failed_callis)
            .await
            .expect("first dispatch");
        let second = blob
            .lease_next_blob_write(live_callis)
            .await
            .expect("second dispatch");
        assert_eq!(leased_chunk(&first).chunk_id, 0);
        assert_eq!(leased_chunk(&second).chunk_id, 1);

        blob.requeue_inflight_for_callis(failed_callis).await;
        let replay = blob
            .lease_next_blob_write(live_callis)
            .await
            .expect("replay dispatch");
        let replay_lease = leased_chunk(&replay);
        assert_eq!(replay_lease.chunk_id, 0);
        assert_eq!(replay_lease.callis_id, live_callis);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_stream_leasing_uses_round_robin_and_prefers_replay() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let callis_id = next_callis_id();
        let settings = blob_settings(1, 4);
        for stream_id in [820, 821, 822] {
            let ring = blob
                .register_outbound_stream(stream_id, settings)
                .await
                .expect("ring");
            ring.push_bytes(b"x", Duration::from_secs(1))
                .await
                .expect("push");
            ring.seal(Duration::from_secs(1)).await.expect("seal");
        }

        let first = blob.lease_next_blob_write(callis_id).await.expect("first");
        let second = blob.lease_next_blob_write(callis_id).await.expect("second");
        let third = blob.lease_next_blob_write(callis_id).await.expect("third");
        assert_eq!(first.stream_id(), Some(820));
        assert_eq!(second.stream_id(), Some(821));
        assert_eq!(third.stream_id(), Some(822));

        blob.finish_blob_write_attempt(
            &second,
            callis_id,
            Err(AureliaError::new(ErrorId::PeerUnavailable)),
        )
        .await;
        let replay = blob.lease_next_blob_write(callis_id).await.expect("replay");
        assert_eq!(replay.stream_id(), Some(821));
        assert_eq!(
            leased_chunk(&replay).chunk_id,
            leased_chunk(&second).chunk_id
        );
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_round_robin_rotates_across_ready_streams() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let callis_id = next_callis_id();
        let settings = blob_settings(1, 2);

        let busy = blob
            .register_outbound_stream(840, settings)
            .await
            .expect("busy ring");
        busy.push_bytes(b"ab", Duration::from_secs(1))
            .await
            .expect("push busy");
        busy.seal(Duration::from_secs(1)).await.expect("seal busy");

        let sibling = blob
            .register_outbound_stream(841, settings)
            .await
            .expect("sibling ring");
        sibling
            .push_bytes(b"x", Duration::from_secs(1))
            .await
            .expect("push sibling");
        sibling
            .seal(Duration::from_secs(1))
            .await
            .expect("seal sibling");

        let first = blob.lease_next_blob_write(callis_id).await.expect("first");
        let second = blob.lease_next_blob_write(callis_id).await.expect("second");
        let third = blob.lease_next_blob_write(callis_id).await.expect("third");

        assert_eq!(first.stream_id(), Some(840));
        assert_eq!(second.stream_id(), Some(841));
        assert_eq!(third.stream_id(), Some(840));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_multiple_transmitters_do_not_receive_duplicate_chunk_leases() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let settings = blob_settings(1, 4);
        let ring = blob
            .register_outbound_stream(850, settings)
            .await
            .expect("ring");
        ring.push_bytes(b"abcd", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");

        let first_callis = next_callis_id();
        let second_callis = next_callis_id();
        let first_blob = Arc::clone(&blob);
        let second_blob = Arc::clone(&blob);
        let (first, second) = tokio::join!(
            first_blob.lease_next_blob_write(first_callis),
            second_blob.lease_next_blob_write(second_callis),
        );

        let first = first.expect("first lease");
        let second = second.expect("second lease");
        let first_chunk = leased_chunk(&first);
        let second_chunk = leased_chunk(&second);
        assert_eq!(first.stream_id(), Some(850));
        assert_eq!(second.stream_id(), Some(850));
        assert_ne!(first_chunk.chunk_id, second_chunk.chunk_id);
        assert_eq!(first_chunk.callis_id, first_callis);
        assert_eq!(second_chunk.callis_id, second_callis);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_pull_scan_skips_stream_without_ready_chunk() {
    tokio::time::timeout(BLOB_MANAGER_TEST_TIMEOUT, async {
        let blob = Arc::new(new_blob_manager());
        let settings = blob_settings(1, 4);
        for stream_id in [860, 861] {
            let ring = blob
                .register_outbound_stream(stream_id, settings)
                .await
                .expect("ring");
            ring.push_bytes(b"x", Duration::from_secs(1))
                .await
                .expect("push");
            ring.seal(Duration::from_secs(1)).await.expect("seal");
        }

        let slow_callis = next_callis_id();
        let other_callis = next_callis_id();
        let slow = blob
            .lease_next_blob_write(slow_callis)
            .await
            .expect("slow lease");
        let other = blob
            .lease_next_blob_write(other_callis)
            .await
            .expect("other lease");

        assert_eq!(slow.stream_id(), Some(860));
        assert_eq!(leased_chunk(&slow).callis_id, slow_callis);
        assert_eq!(other.stream_id(), Some(861));
        assert_eq!(leased_chunk(&other).callis_id, other_callis);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn blob_leasing_wakes_another_transmitter_when_more_work_remains() {
    let blob = Arc::new(new_blob_manager());
    let settings = blob_settings(1, 4);
    for stream_id in [870, 871] {
        let ring = blob
            .register_outbound_stream(stream_id, settings)
            .await
            .expect("ring");
        ring.push_bytes(b"x", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");
    }

    let work = blob.work_handle();
    let waiter = work.notified();
    tokio::pin!(waiter);

    let first = blob
        .lease_next_blob_write(next_callis_id())
        .await
        .expect("first lease");
    assert_eq!(first.stream_id(), Some(870));
    timeout(Duration::from_secs(1), &mut waiter)
        .await
        .expect("more work notification");
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
    let (callis_shutdown_tx, mut callis_shutdown_rx) = watch::channel(false);
    let info = ConnectionInfo {
        handle: CallisHandle {
            id: next_callis_id(),
            tx: CallisTx::Blob,
            shutdown: callis_shutdown_tx,
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

    timeout(Duration::from_secs(1), callis_shutdown_rx.changed())
        .await
        .expect("close timeout")
        .expect("shutdown signal");
    assert!(*callis_shutdown_rx.borrow());
}

#[test]
fn blob_receiver_drop_cleans_up_without_runtime() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let blob = Arc::new(new_blob_manager());
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
    let blob = Arc::new(new_blob_manager());
    let stream_id: PeerMessageId = 21;
    let settings = BlobCallisSettings {
        chunk_size: 4,
        ack_window_chunks: 4,
    };
    let ring = runtime
        .block_on(async { blob.register_outbound_stream(stream_id, settings).await })
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
