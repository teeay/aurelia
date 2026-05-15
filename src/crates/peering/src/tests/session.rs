// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{watch, Mutex, Notify};

use crate::config::{DomusConfig, DomusConfigAccess, DomusConfigBuilder};
use crate::message_id::PeerMessageIdAllocator;
use crate::session::{CancelReason, DedupeDecision, PeerMessage, PeerSession, ReceiveOutcome};
use crate::session::{PendingDuplicateReceive, ReceiveSchedule};
use crate::taberna::{TabernaInbox, TabernaRegistry};
use crate::transport::primary_dispatch::PrimaryDispatchManager;
use aurelia_ids::{AureliaError, ErrorId};

use crate::a3_message_type;

const SESSION_TEST_TIMEOUT: Duration = Duration::from_secs(1);

struct RecordingInbox {
    received: Mutex<Vec<(u32, Bytes)>>,
    expected_msg_types: Vec<u32>,
}

struct HoldingInbox {
    response_tx: Mutex<Option<tokio::sync::oneshot::Sender<Result<(), AureliaError>>>>,
}

impl HoldingInbox {
    fn new() -> Self {
        Self {
            response_tx: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl TabernaInbox for HoldingInbox {
    async fn enqueue(
        &self,
        _msg_type: u32,
        _payload: Bytes,
        _blob_receiver: Option<crate::BlobReceiver>,
        _notify: Option<Arc<Notify>>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.response_tx.lock().await = Some(tx);
        Ok(rx)
    }
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
        assert!(
            self.expected_msg_types.contains(&msg_type),
            "unexpected msg_type {msg_type}; expected one of {:?}",
            self.expected_msg_types
        );
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

fn backpressure_config(send_queue_size: usize, send_timeout: Duration) -> DomusConfig {
    DomusConfigBuilder::new()
        .send_queue_size(send_queue_size)
        .send_timeout(send_timeout)
        .callis_connect_timeout(send_timeout)
        .accept_timeout(send_timeout)
        .build()
        .expect("valid domus config")
}

fn test_session(config: DomusConfigAccess) -> PeerSession {
    PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    )
}

fn inbound_message(peer_msg_id: u32, msg_type: u32, dst_taberna: u64) -> PeerMessage {
    PeerMessage {
        peer_msg_id,
        src_taberna: 1,
        dst_taberna,
        msg_type,
        flags: 0,
        payload: Bytes::from_static(b"pending"),
    }
}

async fn pending_duplicate(
    session: &PeerSession,
    registry: &TabernaRegistry,
    message: PeerMessage,
    notify: Arc<Notify>,
) -> PendingDuplicateReceive {
    match session
        .receive_message_schedule(message.clone(), registry, Some(Arc::clone(&notify)))
        .await
    {
        ReceiveSchedule::Pending(_) => {}
        _ => panic!("expected first delivery to be pending"),
    }

    match session
        .receive_message_schedule(message, registry, Some(notify))
        .await
    {
        ReceiveSchedule::PendingDuplicate(pending) => pending,
        _ => panic!("expected duplicate delivery to be pending without blocking"),
    }
}

#[tokio::test]
async fn pending_duplicate_delivery_resolves_ack_without_blocking_reader() {
    let registry = TabernaRegistry::new();
    let msg_type = a3_message_type(100);
    registry
        .register(30, Arc::new(HoldingInbox::new()))
        .await
        .expect("register inbox");
    let config = DomusConfigAccess::from_config(DomusConfig::default());
    let session = test_session(config);
    let message = inbound_message(900, msg_type, 30);
    let notify = Arc::new(Notify::new());

    let pending = pending_duplicate(&session, &registry, message, Arc::clone(&notify)).await;

    session.dedupe_complete(900, Ok(())).await;
    tokio::time::timeout(Duration::from_millis(100), notify.notified())
        .await
        .expect("duplicate waiter wake");
    match pending.decision_rx.await.expect("duplicate decision") {
        DedupeDecision::Ack => {}
        _ => panic!("expected duplicate ack"),
    }
}

#[tokio::test]
async fn pending_duplicate_delivery_resolves_original_error() {
    let registry = TabernaRegistry::new();
    let msg_type = a3_message_type(101);
    registry
        .register(31, Arc::new(HoldingInbox::new()))
        .await
        .expect("register inbox");
    let config = DomusConfigAccess::from_config(DomusConfig::default());
    let session = test_session(config);
    let message = inbound_message(901, msg_type, 31);
    let notify = Arc::new(Notify::new());

    let pending = pending_duplicate(&session, &registry, message, Arc::clone(&notify)).await;

    session
        .dedupe_complete(901, Err(AureliaError::new(ErrorId::TabernaBusy)))
        .await;
    tokio::time::timeout(Duration::from_millis(100), notify.notified())
        .await
        .expect("duplicate waiter wake");
    match pending.decision_rx.await.expect("duplicate decision") {
        DedupeDecision::Error(err) => assert_eq!(err.kind, ErrorId::TabernaBusy),
        _ => panic!("expected duplicate error"),
    }
}

#[tokio::test]
async fn pending_duplicate_delivery_resolves_abandoned() {
    let registry = TabernaRegistry::new();
    let msg_type = a3_message_type(102);
    registry
        .register(32, Arc::new(HoldingInbox::new()))
        .await
        .expect("register inbox");
    let config = DomusConfigAccess::from_config(DomusConfig::default());
    let session = test_session(config);
    let message = inbound_message(902, msg_type, 32);
    let notify = Arc::new(Notify::new());

    let pending = pending_duplicate(&session, &registry, message, Arc::clone(&notify)).await;

    session.dedupe_abandon(902).await;
    tokio::time::timeout(Duration::from_millis(100), notify.notified())
        .await
        .expect("duplicate waiter wake");
    match pending.decision_rx.await.expect("duplicate decision") {
        DedupeDecision::Abandoned => {}
        _ => panic!("expected duplicate abandon"),
    }
}

#[tokio::test]
async fn remote_delivery_with_ack() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let registry = TabernaRegistry::new();
        let msg_type = a3_message_type(42);
        let sink = Arc::new(RecordingInbox::new(vec![msg_type]));
        registry.register(10, sink).await.unwrap();

        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());

        let sender = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );
        let receiver = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );

        let (message, waiter) = sender
            .create_outgoing(1, 10, msg_type, 0, Bytes::from_static(b"hello"))
            .await
            .expect("enqueue outgoing");

        deliver_and_ack(&sender, &receiver, &registry, message).await;

        sender.wait_for_ack(waiter).await.expect("ack completes");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn initial_inbound_hello_preserves_pending_outbound_dispatch() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let session = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config,
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );
        let queue = session.primary_dispatch();
        let (message, _waiter) = session
            .create_outgoing(
                1,
                10,
                crate::a3_message_type(1),
                0,
                Bytes::from_static(b"pending"),
            )
            .await
            .expect("enqueue outgoing");

        assert!(!session.accept_hello(false).await);
        assert!(
            queue.message(message.peer_msg_id).await.is_some(),
            "initial inbound primary must not clear pending outbound work"
        );
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn transient_reconnect_replay() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let registry = TabernaRegistry::new();
        let msg_type = a3_message_type(7);
        let sink = Arc::new(RecordingInbox::new(vec![msg_type]));
        registry.register(11, sink).await.unwrap();

        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());

        let sender = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );
        let receiver = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );

        let (message, waiter) = sender
            .create_outgoing(1, 11, msg_type, 0, Bytes::from_static(b"payload"))
            .await
            .expect("enqueue outgoing");

        let pending = sender.handle_hello_response(true).await;
        assert_eq!(pending.len(), 1);
        let replay = pending[0].clone();

        deliver_and_ack(&sender, &receiver, &registry, replay).await;

        sender.wait_for_ack(waiter).await.expect("ack completes");
        drop(message);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn lost_ack_replay_is_deduplicated() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let registry = TabernaRegistry::new();
        let msg_type = a3_message_type(8);
        let sink = Arc::new(RecordingInbox::new(vec![msg_type]));
        registry.register(12, sink.clone()).await.unwrap();

        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());

        let sender = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );
        let receiver = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );

        let (message, waiter) = sender
            .create_outgoing(1, 12, msg_type, 0, Bytes::from_static(b"dedupe"))
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
        assert_eq!(received[0].0, msg_type);
        assert_eq!(received[0].1, Bytes::from_static(b"dedupe"));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peer_restart_invalidates_inflight() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let msg_type = a3_message_type(7);
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let sender = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config,
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );

        let (_message, waiter) = sender
            .create_outgoing(1, 12, msg_type, 0, Bytes::from_static(b"payload"))
            .await
            .expect("enqueue outgoing");

        let pending = sender.handle_hello_response(false).await;
        assert!(pending.is_empty());

        let err = sender
            .wait_for_ack(waiter)
            .await
            .expect_err("expected peer restart failure");
        assert_eq!(err.kind, ErrorId::PeerRestarted);
    })
    .await
    .expect("async test timed out");
}

async fn deliver_peer_message(receiver: &PeerSession, registry: &TabernaRegistry, payload: Bytes) {
    let message = PeerMessage {
        peer_msg_id: 0,
        src_taberna: 1,
        dst_taberna: 12,
        msg_type: a3_message_type(9),
        flags: 0,
        payload,
    };
    let (_cancel_tx, cancel_rx) = watch::channel(CancelReason::None);
    match receiver
        .receive_message_cancelable(message, registry, cancel_rx)
        .await
    {
        ReceiveOutcome::Ack(0) => {}
        ReceiveOutcome::Ack(peer_msg_id) => panic!("unexpected ack id: {peer_msg_id}"),
        ReceiveOutcome::Error(err) => panic!("unexpected error: {err}"),
        ReceiveOutcome::Skip => panic!("unexpected skip"),
    }
}

#[tokio::test]
async fn peer_restart_clears_inbound_dedupe_history() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let registry = TabernaRegistry::new();
        let sink = Arc::new(RecordingInbox::new(vec![a3_message_type(9)]));
        registry.register(12, sink.clone()).await.unwrap();

        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let receiver = PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            config,
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        );

        deliver_peer_message(&receiver, &registry, Bytes::from_static(b"before-restart")).await;
        receiver.handle_hello_response(false).await;
        deliver_peer_message(&receiver, &registry, Bytes::from_static(b"after-restart")).await;

        let received = sink.received.lock().await;
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].1, Bytes::from_static(b"before-restart"));
        assert_eq!(received[1].1, Bytes::from_static(b"after-restart"));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn send_queue_full_rejects_immediately() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let session = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(1, Duration::from_millis(5)),
            tokio::runtime::Handle::current(),
        );

        let _first = session
            .create_outgoing(1, 2, crate::a3_message_type(0), 0, Bytes::from_static(b"a"))
            .await
            .expect("first enqueue");

        let err = match session
            .create_outgoing(1, 2, crate::a3_message_type(0), 0, Bytes::from_static(b"b"))
            .await
        {
            Ok(_) => panic!("queue should be full"),
            Err(err) => err,
        };

        assert_eq!(err.kind, ErrorId::LocalQueueFull);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn retained_slot_capacity_counts_dispatched_work() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let session = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(1, Duration::from_millis(5)),
            tokio::runtime::Handle::current(),
        );

        let (first, _waiter) = session
            .create_outgoing(1, 2, crate::a3_message_type(0), 0, Bytes::from_static(b"a"))
            .await
            .expect("first enqueue");
        session
            .mark_dispatched(first.peer_msg_id)
            .await
            .expect("first dispatch");

        let err = match session
            .create_outgoing(1, 2, crate::a3_message_type(0), 0, Bytes::from_static(b"b"))
            .await
        {
            Ok(_) => panic!("retained slot remains occupied while dispatched"),
            Err(err) => err,
        };

        assert_eq!(err.kind, ErrorId::LocalQueueFull);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn retained_store_reaps_expired_dispatched_message() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let msg_type = a3_message_type(1);
        let session = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(2, Duration::from_millis(10)),
            tokio::runtime::Handle::current(),
        );

        let (message, waiter) = session
            .create_outgoing(1, 2, msg_type, 0, Bytes::from_static(b"a"))
            .await
            .expect("enqueue");
        session
            .mark_dispatched(message.peer_msg_id)
            .await
            .expect("mark dispatched");

        tokio::time::sleep(Duration::from_millis(25)).await;

        let err = session
            .wait_for_ack(waiter)
            .await
            .expect_err("retained reaper fails expired dispatched message");
        assert_eq!(err.kind, ErrorId::SendTimeout);
        assert!(
            !session.has_inflight().await,
            "expired inflight message should be removed"
        );
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn retained_store_reaps_expired_queued_message() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let session = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(2, Duration::from_millis(10)),
            tokio::runtime::Handle::current(),
        );

        let (_message, waiter) = session
            .create_outgoing(1, 2, crate::a3_message_type(0), 0, Bytes::from_static(b"a"))
            .await
            .expect("enqueue");

        tokio::time::sleep(Duration::from_millis(25)).await;

        let err = session
            .wait_for_ack(waiter)
            .await
            .expect_err("retained reaper fails expired queued message");
        assert_eq!(err.kind, ErrorId::SendTimeout);
        assert!(
            !session.has_inflight().await,
            "expired queued message should be removed"
        );
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn close_rejects_outbound_and_fails_inflight() {
    let msg_type = a3_message_type(1);
    let config = DomusConfigBuilder::new()
        .send_timeout(Duration::from_millis(5))
        .callis_connect_timeout(Duration::from_millis(5))
        .accept_timeout(Duration::from_millis(5))
        .build()
        .expect("valid domus config");
    let store: DomusConfigAccess = DomusConfigAccess::from_config(config);
    let session = PeerSession::new(
        Arc::new(PeerMessageIdAllocator::default()),
        store,
        tokio::runtime::Handle::current(),
        PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
    );

    let (_message, waiter) = session
        .create_outgoing(1, 2, msg_type, 0, Bytes::from_static(b"a"))
        .await
        .expect("enqueue");

    session.handle_close().await;

    let err = match session
        .create_outgoing(1, 2, msg_type, 0, Bytes::from_static(b"b"))
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
async fn dropped_waiter_does_not_cancel_queued_work() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let session = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(1, Duration::from_millis(100)),
            tokio::runtime::Handle::current(),
        );

        let (message, waiter) = session
            .create_outgoing(1, 2, crate::a3_message_type(0), 0, Bytes::from_static(b"a"))
            .await
            .expect("first enqueue");

        drop(waiter);

        assert!(
            session
                .primary_dispatch()
                .message(message.peer_msg_id)
                .await
                .is_some(),
            "dropped waiter must not remove queued work"
        );

        let err = match session
            .create_outgoing(1, 2, crate::a3_message_type(0), 0, Bytes::from_static(b"b"))
            .await
        {
            Ok(_) => panic!("queue remains occupied by accepted work"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::LocalQueueFull);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dropped_waiter_does_not_cancel_inflight_work() {
    tokio::time::timeout(SESSION_TEST_TIMEOUT, async {
        let msg_type = a3_message_type(1);
        let session = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(1, Duration::from_millis(10)),
            tokio::runtime::Handle::current(),
        );

        let (message, waiter) = session
            .create_outgoing(1, 2, msg_type, 0, Bytes::from_static(b"a"))
            .await
            .expect("first enqueue");
        session
            .mark_dispatched(message.peer_msg_id)
            .await
            .expect("mark dispatched");

        drop(waiter);

        assert!(
            session
                .primary_dispatch()
                .is_inflight(message.peer_msg_id)
                .await
                .unwrap_or(false),
            "dropped waiter must not remove inflight work"
        );
    })
    .await
    .expect("async test timed out");
}
