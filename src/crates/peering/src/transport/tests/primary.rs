// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::DomusConfigBuilder;
use crate::session::CancelReason;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn primary_callis_priority_prefers_a1_then_a2_then_a3() {
    use crate::transport::primary_dispatch::{DispatchItem, PriorityTier};

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let session = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
    );
    let queue = session.primary_dispatch();

    queue
        .enqueue_a1_frame(OutboundFrame::Ack { peer_msg_id: 33 }, None)
        .await;

    let (_msg_a3, _waiter_a3) = session
        .create_outgoing(1, 2, 0x0100_0000, 0, Bytes::from_static(b"a3"))
        .await
        .expect("a3 enqueue");

    let (_msg_a2, _waiter_a2) = session
        .create_outgoing(1, 2, 0x0001_0000, 0, Bytes::from_static(b"a2"))
        .await
        .expect("a2 enqueue");

    let item1 = queue.pop_next().await.expect("a1 item");
    assert!(matches!(item1, DispatchItem::A1Frame(_)));

    let item2 = queue.pop_next().await.expect("a2 item");
    assert!(matches!(
        item2,
        DispatchItem::Message {
            tier: PriorityTier::A2,
            ..
        }
    ));

    let item3 = queue.pop_next().await.expect("a3 item");
    assert!(matches!(
        item3,
        DispatchItem::Message {
            tier: PriorityTier::A3,
            ..
        }
    ));
}

#[tokio::test]
async fn read_frame_rejects_payload_over_max() {
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
}

#[tokio::test]
async fn primary_round_robin_picks_next_idle_callis() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);
    let handle = Arc::new(PeerHandle::new(
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
    ));

    let (tx1, mut rx1) = mpsc::channel(8);
    let (shutdown_tx1, _shutdown_rx1) = watch::channel(false);
    let available1 = Arc::new(AtomicBool::new(true));
    let callis_id1 = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id1,
                    tx: tx1,
                    shutdown: shutdown_tx1,
                    available: Arc::clone(&available1),
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect callis 1");

    let (tx2, mut rx2) = mpsc::channel(8);
    let (shutdown_tx2, _shutdown_rx2) = watch::channel(false);
    let available2 = Arc::new(AtomicBool::new(true));
    let callis_id2 = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id2,
                    tx: tx2,
                    shutdown: shutdown_tx2,
                    available: Arc::clone(&available2),
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect callis 2");

    let send_first = {
        let handle = Arc::clone(&handle);
        tokio::spawn(async move {
            handle
                .send(10, 20, Bytes::from_static(b"first"))
                .await
                .map_err(|err| err.kind)
        })
    };
    let frame1 = timeout(Duration::from_secs(1), rx1.recv())
        .await
        .expect("frame1 timeout")
        .expect("frame1");
    let msg1 = match frame1 {
        OutboundFrame::Message(message) => message,
        _ => panic!("expected message on callis 1"),
    };
    handle.session.handle_ack(msg1.peer_msg_id).await;
    available1.store(true, Ordering::SeqCst);
    handle.primary_available.notify_waiters();
    let result1 = send_first.await.expect("send1 join");
    assert_eq!(result1, Ok(()));

    let send_second = {
        let handle = Arc::clone(&handle);
        tokio::spawn(async move {
            handle
                .send(11, 21, Bytes::from_static(b"second"))
                .await
                .map_err(|err| err.kind)
        })
    };
    let frame2 = timeout(Duration::from_secs(1), rx2.recv())
        .await
        .expect("frame2 timeout")
        .expect("frame2");
    let msg2 = match frame2 {
        OutboundFrame::Message(message) => message,
        _ => panic!("expected message on callis 2"),
    };
    handle.session.handle_ack(msg2.peer_msg_id).await;
    available2.store(true, Ordering::SeqCst);
    handle.primary_available.notify_waiters();
    let result2 = send_second.await.expect("send2 join");
    assert_eq!(result2, Ok(()));
}

#[tokio::test]
async fn primary_prunes_closed_callis_before_send() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let (listener_shutdown_tx, _listener_shutdown_rx) = watch::channel(false);
    let handle = Arc::new(PeerHandle::new(
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
    ));

    let (closed_tx, closed_rx) = mpsc::channel(1);
    drop(closed_rx);
    let (closed_shutdown_tx, _closed_shutdown_rx) = watch::channel(false);
    let closed_available = Arc::new(AtomicBool::new(true));
    let closed_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: closed_id,
                    tx: closed_tx,
                    shutdown: closed_shutdown_tx,
                    available: Arc::clone(&closed_available),
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect closed callis");

    let (open_tx, mut open_rx) = mpsc::channel(8);
    let (open_shutdown_tx, _open_shutdown_rx) = watch::channel(false);
    let open_available = Arc::new(AtomicBool::new(true));
    let open_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: open_id,
                    tx: open_tx,
                    shutdown: open_shutdown_tx,
                    available: Arc::clone(&open_available),
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect open callis");

    let send_task = {
        let handle = Arc::clone(&handle);
        tokio::spawn(async move {
            handle
                .send(12, 22, Bytes::from_static(b"hello"))
                .await
                .map_err(|err| err.kind)
        })
    };
    let frame = timeout(Duration::from_secs(1), open_rx.recv())
        .await
        .expect("open frame timeout")
        .expect("open frame");
    let msg = match frame {
        OutboundFrame::Message(message) => message,
        _ => panic!("expected message on open callis"),
    };
    handle.session.handle_ack(msg.peer_msg_id).await;
    open_available.store(true, Ordering::SeqCst);
    handle.primary_available.notify_waiters();
    let result = send_task.await.expect("send join");
    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn primary_dispatch_releases_handle_after_send_timeout() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(50))
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

    let (tx, _rx) = mpsc::channel(1);
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let available = Arc::new(AtomicBool::new(true));
    let callis_id = next_callis_id();
    tx.try_send(OutboundFrame::Close)
        .expect("prefill callis channel");

    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id,
                    tx,
                    shutdown: shutdown_tx,
                    available: Arc::clone(&available),
                },
                replay: Vec::new(),
                fresh_session: false,
                blob_settings: None,
                blob_resume: false,
            },
        })
        .await
        .expect("connect callis");

    let notified = handle.primary_available.notified();

    let (_message, _waiter) = handle
        .session
        .create_outgoing(1, 2, 0x0100_0000, 0, Bytes::from_static(b"blocked"))
        .await
        .expect("enqueue");

    timeout(Duration::from_millis(50), async {
        loop {
            if !available.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dispatcher should reserve handle");

    timeout(Duration::from_millis(150), notified)
        .await
        .expect("dispatcher did not release handle after timeout");
    assert!(available.load(Ordering::SeqCst));
}

#[tokio::test]
async fn teardown_after_send_timeout_fails_pending_with_peer_unavailable() {
    let backend = Arc::new(TestBackend);
    let registry = Arc::new(TabernaRegistry::new());
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(50))
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

    let (tx, _rx) = mpsc::channel(8);
    let (callis_shutdown_tx, _callis_shutdown_rx) = watch::channel(false);
    let available = Arc::new(AtomicBool::new(true));
    let callis_id = next_callis_id();
    handle
        .peer_state_tx
        .send(PeerStateUpdate::Connected {
            callis: CallisKind::Primary,
            info: ConnectionInfo {
                handle: CallisHandle {
                    id: callis_id,
                    tx,
                    shutdown: callis_shutdown_tx,
                    available,
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
        handle.send(1, 0x0001_0000, Bytes::from_static(b"late")),
    )
    .await
    .expect("send timeout")
    .expect_err("expected peer unavailable");
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
}
