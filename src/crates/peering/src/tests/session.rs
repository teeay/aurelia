// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{watch, Mutex, Notify};

use crate::config::{DomusConfig, DomusConfigAccess, DomusConfigBuilder};
use crate::message_id::PeerMessageIdAllocator;
use crate::session::{BackpressureConfig, CancelReason, PeerMessage, PeerSession, ReceiveOutcome};
use crate::taberna::{TabernaInbox, TabernaRegistry};
use aurelia_ids::{AureliaError, ErrorId};

struct RecordingInbox {
    received: Mutex<Vec<(u32, Bytes)>>,
    expected_msg_types: Vec<u32>,
}

impl RecordingInbox {
    fn new(expected_msg_types: Vec<u32>) -> Self {
        Self {
            received: Mutex::new(Vec::new()),
            expected_msg_types,
        }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for RecordingInbox {
    async fn enqueue(
        &self,
        msg_type: u32,
        payload: Bytes,
        _blob_receiver: Option<crate::BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        if !self.expected_msg_types.contains(&msg_type) {
            return Err(AureliaError::new(ErrorId::RemoteTabernaRejected));
        }
        self.received.lock().await.push((msg_type, payload));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(Ok(()));
        if let Some(notify) = notify.as_ref() {
            notify.notify_one();
        }
        Ok(rx)
    }
}

async fn deliver_and_ack(
    sender: &PeerSession,
    receiver: &PeerSession,
    registry: &TabernaRegistry,
    message: PeerMessage,
) {
    sender
        .mark_dispatched(message.peer_msg_id)
        .await
        .expect("mark dispatched");
    let (_cancel_tx, cancel_rx) = watch::channel(CancelReason::None);
    match receiver
        .receive_message_cancelable(message, registry, cancel_rx)
        .await
    {
        ReceiveOutcome::Ack(peer_msg_id) => {
            sender.handle_ack(peer_msg_id).await;
        }
        ReceiveOutcome::Error(err) => panic!("unexpected error: {err}"),
        ReceiveOutcome::Skip => panic!("unexpected skip"),
    }
}

#[tokio::test]
async fn remote_delivery_with_ack() {
    let registry = TabernaRegistry::new();
    let sink = Arc::new(RecordingInbox::new(vec![42]));
    registry.register(10, sink).await.unwrap();

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());

    let sender = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    );
    let receiver = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    );

    let (message, waiter) = sender
        .create_outgoing(1, 10, 42, 0, Bytes::from_static(b"hello"))
        .await
        .expect("enqueue outgoing");

    deliver_and_ack(&sender, &receiver, &registry, message).await;

    sender.wait_for_ack(waiter).await.expect("ack completes");
}

#[tokio::test]
async fn transient_reconnect_replay() {
    let registry = TabernaRegistry::new();
    let sink = Arc::new(RecordingInbox::new(vec![7]));
    registry.register(11, sink).await.unwrap();

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());

    let sender = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    );
    let receiver = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    );

    let (message, waiter) = sender
        .create_outgoing(1, 11, 7, 0, Bytes::from_static(b"payload"))
        .await
        .expect("enqueue outgoing");

    let pending = sender.handle_hello_response(true).await;
    assert_eq!(pending.len(), 1);
    let replay = PeerMessage {
        peer_msg_id: pending[0].peer_msg_id,
        src_taberna: pending[0].src_taberna,
        dst_taberna: pending[0].dst_taberna,
        msg_type: pending[0].msg_type,
        flags: pending[0].flags,
        payload: pending[0].payload.clone(),
    };

    deliver_and_ack(&sender, &receiver, &registry, replay).await;

    sender.wait_for_ack(waiter).await.expect("ack completes");
    drop(message);
}

#[tokio::test]
async fn lost_ack_replay_is_deduplicated() {
    let registry = TabernaRegistry::new();
    let sink = Arc::new(RecordingInbox::new(vec![8]));
    registry.register(12, sink.clone()).await.unwrap();

    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());

    let sender = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    );
    let receiver = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config.clone(),
        tokio::runtime::Handle::current(),
    );

    let (message, waiter) = sender
        .create_outgoing(1, 12, 8, 0, Bytes::from_static(b"dedupe"))
        .await
        .expect("enqueue outgoing");
    sender
        .mark_dispatched(message.peer_msg_id)
        .await
        .expect("mark dispatched");

    let (_cancel_tx, cancel_rx) = watch::channel(CancelReason::None);
    match receiver
        .receive_message_cancelable(message.clone(), &registry, cancel_rx)
        .await
    {
        ReceiveOutcome::Ack(peer_msg_id) => assert_eq!(peer_msg_id, message.peer_msg_id),
        ReceiveOutcome::Error(err) => panic!("unexpected error: {err}"),
        ReceiveOutcome::Skip => panic!("unexpected skip"),
    }

    let pending = sender.handle_hello_response(true).await;
    assert_eq!(pending.len(), 1);
    let replay = PeerMessage {
        peer_msg_id: pending[0].peer_msg_id,
        src_taberna: pending[0].src_taberna,
        dst_taberna: pending[0].dst_taberna,
        msg_type: pending[0].msg_type,
        flags: pending[0].flags,
        payload: pending[0].payload.clone(),
    };

    let (_cancel_tx, cancel_rx) = watch::channel(CancelReason::None);
    match receiver
        .receive_message_cancelable(replay, &registry, cancel_rx)
        .await
    {
        ReceiveOutcome::Ack(peer_msg_id) => {
            sender.handle_ack(peer_msg_id).await;
        }
        ReceiveOutcome::Error(err) => panic!("unexpected error: {err}"),
        ReceiveOutcome::Skip => panic!("unexpected skip"),
    }

    sender.wait_for_ack(waiter).await.expect("ack completes");

    let received = sink.received.lock().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, 8);
    assert_eq!(received[0].1, Bytes::from_static(b"dedupe"));
}

#[tokio::test]
async fn peer_restart_invalidates_inflight() {
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let sender = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
    );

    let (_message, waiter) = sender
        .create_outgoing(1, 12, 7, 0, Bytes::from_static(b"payload"))
        .await
        .expect("enqueue outgoing");

    let pending = sender.handle_hello_response(false).await;
    assert!(pending.is_empty());

    let err = sender
        .wait_for_ack(waiter)
        .await
        .expect_err("expected peer restart failure");
    assert_eq!(err.kind, ErrorId::PeerRestarted);
}

#[tokio::test]
async fn send_queue_timeout() {
    let config = BackpressureConfig {
        send_queue_size: 1,
        inflight_window: 1,
        send_timeout: Duration::from_millis(5),
    };
    let session = PeerSession::with_backpressure(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
    );

    let _first = session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"a"))
        .await
        .expect("first enqueue");

    let err = match session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"b"))
        .await
    {
        Ok(_) => panic!("queue should be full"),
        Err(err) => err,
    };

    assert_eq!(err.kind, ErrorId::SendTimeout);
}

#[tokio::test]
async fn inflight_window_timeout() {
    let config = BackpressureConfig {
        send_queue_size: 2,
        inflight_window: 1,
        send_timeout: Duration::from_millis(5),
    };
    let session = PeerSession::with_backpressure(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
    );

    let (first, _waiter) = session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"a"))
        .await
        .expect("first enqueue");
    session
        .mark_dispatched(first.peer_msg_id)
        .await
        .expect("first dispatch");

    let (second, _waiter) = session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"b"))
        .await
        .expect("second enqueue");

    let err = session
        .mark_dispatched(second.peer_msg_id)
        .await
        .expect_err("inflight full");

    assert_eq!(err.kind, ErrorId::SendTimeout);
}

#[tokio::test]
async fn close_rejects_outbound_and_fails_inflight() {
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(5))
        .accept_timeout(Duration::from_millis(5))
        .build()
        .expect("valid domus config");
    let store: DomusConfigAccess = DomusConfigAccess::from_config(config);
    let session = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        store,
        tokio::runtime::Handle::current(),
    );

    let (_message, waiter) = session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"a"))
        .await
        .expect("enqueue");

    session.handle_close().await;

    let err = match session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"b"))
        .await
    {
        Ok(_) => panic!("close rejects outbound"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::PeerUnavailable);

    let err = session
        .wait_for_ack(waiter)
        .await
        .expect_err("inflight should fail on close");
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
}

#[tokio::test]
async fn dropped_waiter_releases_queue() {
    let config = BackpressureConfig {
        send_queue_size: 1,
        inflight_window: 1,
        send_timeout: Duration::from_millis(10),
    };
    let session = PeerSession::with_backpressure(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
    );

    let (_message, waiter) = session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"a"))
        .await
        .expect("first enqueue");

    drop(waiter);
    tokio::time::sleep(Duration::from_millis(5)).await;

    let _next = session
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"b"))
        .await
        .expect("queue released after drop");
}
