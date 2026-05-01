// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{watch, Notify};
use tokio::time::advance;

use crate::config::{DomusConfig, DomusConfigAccess};
use crate::message_id::PeerMessageIdAllocator;
use crate::session::{BackpressureConfig, CancelReason, PeerMessage, PeerSession, ReceiveOutcome};
use crate::taberna::{TabernaInbox, TabernaRegistry};
use aurelia_ids::{AureliaError, ErrorId};

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

#[tokio::test(start_paused = true)]
async fn delayed_ack_eventually_completes() {
    let registry = TabernaRegistry::new();
    let sink = Arc::new(RecordingInbox::new(vec![9]));
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
        .create_outgoing(1, 10, 9, 0, Bytes::from_static(b"delayed"))
        .await
        .expect("enqueue outgoing");

    advance(Duration::from_secs(5)).await;
    deliver_and_ack(&sender, &receiver, &registry, message).await;
    sender.wait_for_ack(waiter).await.expect("ack completes");
}

#[tokio::test]
async fn dropped_ack_times_out() {
    let config = BackpressureConfig {
        send_queue_size: 1,
        inflight_window: 1,
        send_timeout: Duration::from_millis(10),
    };
    let sender = PeerSession::with_backpressure(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
    );

    let (_message, waiter) = sender
        .create_outgoing(1, 10, 9, 0, Bytes::from_static(b"drop"))
        .await
        .expect("enqueue outgoing");

    let err = sender
        .wait_for_ack(waiter)
        .await
        .expect_err("expected send timeout");
    assert_eq!(err.kind, ErrorId::SendTimeout);
}

#[tokio::test]
async fn half_open_connection_is_detected_by_timeout() {
    let config = BackpressureConfig {
        send_queue_size: 1,
        inflight_window: 1,
        send_timeout: Duration::from_millis(10),
    };
    let sender = PeerSession::with_backpressure(
        Arc::new(PeerMessageIdAllocator::default()),
        config,
        tokio::runtime::Handle::current(),
    );

    let (_message, waiter) = sender
        .create_outgoing(1, 2, 1, 0, Bytes::from_static(b"half-open"))
        .await
        .expect("enqueue outgoing");

    let err = sender
        .wait_for_ack(waiter)
        .await
        .expect_err("expected send timeout");
    assert_eq!(err.kind, ErrorId::SendTimeout);
}
