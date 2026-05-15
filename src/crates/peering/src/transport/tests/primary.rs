// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::DomusConfigBuilder;
use crate::session::CancelReason;
use tokio::io::AsyncWriteExt;

const PRIMARY_TEST_TIMEOUT: Duration = Duration::from_millis(500);

#[tokio::test]
async fn primary_callis_priority_prefers_a1_then_a2_then_a3() {
    tokio::time::timeout(PRIMARY_TEST_TIMEOUT, async {
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let session = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config,
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );
        let queue = session.primary_dispatch();

        queue
            .enqueue_a1_frame(OutboundFrame::Ack { peer_msg_id: 33 })
            .await;

        let (msg_a3, _waiter_a3) = session
            .create_outgoing(
                1,
                2,
                crate::a3_message_type(0),
                0,
                Bytes::from_static(b"a3"),
            )
            .await
            .expect("a3 enqueue");

        let (msg_a2, _waiter_a2) = session
            .create_outgoing(1, 2, 0x0001_0000, 0, Bytes::from_static(b"a2"))
            .await
            .expect("a2 enqueue");

        let item1 = queue.claim_next(1).await.expect("a1 item");
        assert_eq!(item1.item.peer_msg_id(), 33);

        let item2 = queue.claim_next(1).await.expect("a2 item");
        assert_eq!(item2.item.peer_msg_id(), msg_a2.peer_msg_id);

        let item3 = queue.claim_next(1).await.expect("a3 item");
        assert_eq!(item3.item.peer_msg_id(), msg_a3.peer_msg_id);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn read_frame_rejects_payload_over_max() {
    tokio::time::timeout(PRIMARY_TEST_TIMEOUT, async {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let header = WireHeader {
            version: PROTOCOL_VERSION,
            flags: 0,
            msg_type: MSG_HELLO,
            peer_msg_id: 1,
            src_taberna: 0,
            dst_taberna: 0,
            payload_len: 9,
        };
        writer
            .write_all(&header.encode())
            .await
            .expect("write header");

        let err = read_frame(&mut reader, 4)
            .await
            .expect_err("expected payload length error");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn frame_read_state_preserves_partial_header_after_cancelled_poll() {
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let mut read_state = frame::FrameReadState::default();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: 0,
        msg_type: MSG_KEEPALIVE,
        peer_msg_id: 7,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: 0,
    };
    let encoded = header.encode();

    writer
        .write_all(&encoded[..5])
        .await
        .expect("write partial header");
    writer.flush().await.expect("flush partial header");

    timeout(
        Duration::from_millis(20),
        read_state.read_next(&mut reader, 1024),
    )
    .await
    .expect_err("partial header read should remain pending");

    writer
        .write_all(&encoded[5..])
        .await
        .expect("write header remainder");
    writer.flush().await.expect("flush header remainder");

    let (got_header, payload) = timeout(
        Duration::from_millis(250),
        read_state.read_next(&mut reader, 1024),
    )
    .await
    .expect("frame read should complete")
    .expect("frame read should succeed")
    .expect("frame should be present");
    assert_eq!(got_header.peer_msg_id, header.peer_msg_id);
    assert_eq!(got_header.msg_type, header.msg_type);
    assert!(payload.is_empty());
}

#[tokio::test]
async fn frame_read_state_preserves_partial_payload_after_cancelled_poll() {
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let mut read_state = frame::FrameReadState::default();
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: 0,
        msg_type: MSG_KEEPALIVE,
        peer_msg_id: 8,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: 4,
    };
    let encoded = header.encode();

    writer.write_all(&encoded).await.expect("write header");
    writer
        .write_all(b"ab")
        .await
        .expect("write partial payload");
    writer.flush().await.expect("flush partial payload");

    timeout(
        Duration::from_millis(20),
        read_state.read_next(&mut reader, 1024),
    )
    .await
    .expect_err("partial payload read should remain pending");

    writer
        .write_all(b"cd")
        .await
        .expect("write payload remainder");
    writer.flush().await.expect("flush payload remainder");

    let (got_header, payload) = timeout(
        Duration::from_millis(250),
        read_state.read_next(&mut reader, 1024),
    )
    .await
    .expect("frame read should complete")
    .expect("frame read should succeed")
    .expect("frame should be present");
    assert_eq!(got_header.peer_msg_id, header.peer_msg_id);
    assert_eq!(got_header.msg_type, header.msg_type);
    assert_eq!(payload, b"abcd");
}

#[tokio::test]
async fn teardown_after_send_timeout_fails_pending_with_peer_unavailable() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(50))
        .callis_connect_timeout(Duration::from_millis(50))
        .accept_timeout(Duration::from_millis(50))
        .build()
        .expect("config");
    let config: DomusConfigAccess = DomusConfigAccess::from_config(config);
    let limited_registry = config.limited_registry();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);
    let handle = Arc::new(PeerHandle::new(
        None,
        Arc::clone(&registry),
        config,
        Arc::new(BlobBufferTracker::default()),
        Arc::clone(&backend),
        HandshakeGate::new(limited_registry),
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        shutdown_rx,
        listener_shutdown_tx,
        tokio::runtime::Handle::current(),
    ));
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let callis_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id,
                    tx: CallisTx::Primary,
                    shutdown: callis_shutdown_tx,
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect callis");

    handle
        .peer_state_tx
        .send(PeerStateUpdate::ConnectionClosed {
            callis: CallisKind::Primary,
            id: callis_id,
            reason: CancelReason::ConnectionLost,
        })
        .await
        .expect("close callis");

    tokio::time::sleep(Duration::from_millis(10)).await;

    let err = timeout(
        Duration::from_secs(1),
        handle.send(1, crate::a3_message_type(0), Bytes::from_static(b"late")),
    )
    .await
    .expect("send timeout")
    .expect_err("expected peer unavailable");
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
}
