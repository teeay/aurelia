// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{watch, Notify};
use tokio::time::advance;

use crate::config::{DomusConfig, DomusConfigAccess, DomusConfigBuilder};
use crate::message_id::PeerMessageIdAllocator;
use crate::session::{CancelReason, PeerMessage, PeerSession, ReceiveOutcome};
use crate::taberna::{TabernaInbox, TabernaRegistry};
use crate::transport::primary_dispatch::PrimaryDispatchManager;
use aurelia_ids::{AureliaError, ErrorId};

use crate::a3_message_type;

const FAULT_TEST_TIMEOUT: Duration = Duration::from_secs(1);

struct RecordingInbox {
    received: tokio::sync::Mutex<Vec<(u32, Bytes)>>,
    expected_msg_types: Vec<u32>,
}

impl RecordingInbox {
    fn new(expected_msg_types: Vec<u32>) -> Self {
        Self {
            received: tokio::sync::Mutex::new(Vec::new()),
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

#[tokio::test(start_paused = true)]
async fn delayed_ack_eventually_completes() {
    tokio::time::timeout(FAULT_TEST_TIMEOUT, async {
        let registry = TabernaRegistry::new();
        let msg_type = a3_message_type(9);
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
            .create_outgoing(1, 10, msg_type, 0, Bytes::from_static(b"delayed"))
            .await
            .expect("enqueue outgoing");

        advance(Duration::from_secs(5)).await;
        deliver_and_ack(&sender, &receiver, &registry, message).await;
        sender.wait_for_ack(waiter).await.expect("ack completes");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dropped_ack_times_out() {
    tokio::time::timeout(FAULT_TEST_TIMEOUT, async {
        let msg_type = a3_message_type(9);
        let sender = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(1, Duration::from_millis(10)),
            tokio::runtime::Handle::current(),
        );

        let (_message, waiter) = sender
            .create_outgoing(1, 10, msg_type, 0, Bytes::from_static(b"drop"))
            .await
            .expect("enqueue outgoing");

        let err = sender
            .wait_for_ack(waiter)
            .await
            .expect_err("expected send timeout");
        assert_eq!(err.kind, ErrorId::SendTimeout);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn half_open_connection_is_detected_by_timeout() {
    tokio::time::timeout(FAULT_TEST_TIMEOUT, async {
        let msg_type = a3_message_type(1);
        let sender = PeerSession::with_backpressure(
            Arc::new(PeerMessageIdAllocator::default()),
            backpressure_config(1, Duration::from_millis(10)),
            tokio::runtime::Handle::current(),
        );

        let (_message, waiter) = sender
            .create_outgoing(1, 2, msg_type, 0, Bytes::from_static(b"half-open"))
            .await
            .expect("enqueue outgoing");

        let err = sender
            .wait_for_ack(waiter)
            .await
            .expect_err("expected send timeout");
        assert_eq!(err.kind, ErrorId::SendTimeout);
    })
    .await
    .expect("async test timed out");
}
