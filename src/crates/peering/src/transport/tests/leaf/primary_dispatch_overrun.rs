// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::config::DomusConfigBuilder;
use crate::observability::{new_observability_with_capacity, DomusReportingEvent};

const PRIMARY_DISPATCH_TEST_TIMEOUT: Duration = Duration::from_millis(500);

fn message(peer_msg_id: PeerMessageId, msg_type: MessageType) -> PeerMessage {
    PeerMessage {
        peer_msg_id,
        src_taberna: 1,
        dst_taberna: 2,
        msg_type,
        flags: 0,
        payload: Bytes::from_static(b"overrun"),
    }
}

fn queue_with_reporting(
    send_queue_size: usize,
) -> (
    Arc<PrimaryDispatchManager>,
    tokio::sync::broadcast::Receiver<DomusReportingEvent>,
) {
    let config = DomusConfigBuilder::new()
        .send_queue_size(send_queue_size)
        .build()
        .expect("config");
    let config = DomusConfigAccess::from_config(config);
    let (reporting, observability) =
        new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5010));
    let events = reporting.subscribe_events();
    let (queue, _tasks) = PrimaryDispatchManager::new(PrimaryDispatchManagerContext {
        initial_send_queue_size: send_queue_size,
        overrun_reporter: Some(OutboundQueueOverrunReporter {
            peer: Arc::new(Mutex::new(Some(peer))),
            config,
            observability,
        }),
    });
    (queue, events)
}

async fn expect_overrun_event(
    events: &mut tokio::sync::broadcast::Receiver<DomusReportingEvent>,
    tier: OutboundQueueTierReport,
    msg_type: MessageType,
) {
    let expected_tier = match tier {
        OutboundQueueTierReport::A1 => "a1",
        OutboundQueueTierReport::A2 => "a2",
        OutboundQueueTierReport::A3 => "a3",
    };
    let event = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event");
    assert!(matches!(
        &event,
        DomusReportingEvent::OutboundQueueOverrunEvent {
            tier: event_tier,
            msg_type: event_msg_type,
            ..
        } if *event_tier == expected_tier && *event_msg_type == msg_type
    ));
}

#[tokio::test]
async fn initial_capacity_uses_context_send_queue_size() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let (queue, mut events) = queue_with_reporting(2);
        let deadline = Instant::now() + Duration::from_secs(1);
        queue
            .enqueue_new(message(1, crate::a3_message_type(0)), deadline)
            .await
            .expect("first enqueue");
        queue
            .enqueue_new(message(2, crate::a3_message_type(1)), deadline)
            .await
            .expect("second enqueue");
        let err = queue
            .enqueue_new(message(3, crate::a3_message_type(2)), deadline)
            .await
            .expect_err("third enqueue overrun");
        assert_eq!(err.kind, ErrorId::LocalQueueFull);

        expect_overrun_event(
            &mut events,
            OutboundQueueTierReport::A3,
            crate::a3_message_type(2),
        )
        .await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn a3_admission_overrun_reports_observability() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let (queue, mut events) = queue_with_reporting(1);
        let deadline = Instant::now() + Duration::from_secs(1);
        queue
            .enqueue_new(message(1, crate::a3_message_type(0)), deadline)
            .await
            .expect("first enqueue");
        let err = queue
            .enqueue_new(message(2, crate::a3_message_type(1)), deadline)
            .await
            .expect_err("second enqueue overrun");
        assert_eq!(err.kind, ErrorId::LocalQueueFull);

        expect_overrun_event(
            &mut events,
            OutboundQueueTierReport::A3,
            crate::a3_message_type(1),
        )
        .await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn a1_bare_frame_overrun_reports_observability() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let (queue, mut events) = queue_with_reporting(1);
        for peer_msg_id in 1..=16 {
            queue
                .enqueue_a1_frame(OutboundFrame::Ack { peer_msg_id })
                .await;
        }
        queue
            .enqueue_a1_frame(OutboundFrame::Ack { peer_msg_id: 17 })
            .await;

        expect_overrun_event(&mut events, OutboundQueueTierReport::A1, MSG_ACK).await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn retained_write_failure_replays_without_extra_admission() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let (queue, _events) = queue_with_reporting(1);
        let deadline = Instant::now() + Duration::from_secs(1);
        queue
            .enqueue_new(message(1, crate::a3_message_type(0)), deadline)
            .await
            .expect("first enqueue");
        let claim = queue.claim_next(1).await.expect("dispatch item");
        queue
            .complete_claim(claim, Err(AureliaError::new(ErrorId::ConnectionLost)))
            .await;

        let replay = queue.claim_next(2).await.expect("replay item");
        assert_eq!(claim_peer_msg_id(&replay), 1);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn reconnect_replay_uses_retained_slot_without_extra_admission() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let (queue, _events) = queue_with_reporting(1);
        let deadline = Instant::now() + Duration::from_secs(1);
        queue
            .enqueue_new(message(1, crate::a3_message_type(0)), deadline)
            .await
            .expect("first enqueue");
        let claim = queue.claim_next(1).await.expect("dispatch item");
        queue.complete_claim(claim, Ok(())).await;

        queue.mark_tracked_replay_ready(vec![1]).await;

        let replay = queue.claim_next(2).await.expect("replay item");
        assert_eq!(claim_peer_msg_id(&replay), 1);
    })
    .await
    .expect("async test timed out");
}
