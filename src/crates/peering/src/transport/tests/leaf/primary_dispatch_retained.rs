// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

const PRIMARY_DISPATCH_TEST_TIMEOUT: Duration = Duration::from_millis(500);

fn message(peer_msg_id: PeerMessageId, msg_type: MessageType) -> PeerMessage {
    PeerMessage {
        peer_msg_id,
        src_taberna: 1,
        dst_taberna: 2,
        msg_type,
        flags: 0,
        payload: Bytes::from_static(b"payload"),
    }
}

fn deadline_ms(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms)
}

#[tokio::test]
async fn retained_reaper_wakes_from_empty_insert_and_expires_message() {
    let queue = PrimaryOutboundStore::new(2, None);
    let reaper = tokio::spawn(queue.reaper().run());
    let mut ack_rx = queue
        .enqueue_message(message(1, crate::a3_message_type(0)), deadline_ms(5))
        .await
        .expect("queued");

    let err = tokio::time::timeout(Duration::from_millis(50), &mut ack_rx)
        .await
        .expect("reaper wakes")
        .expect("sender completes")
        .expect_err("expired");
    assert_eq!(err.kind, ErrorId::SendTimeout);

    drop(queue);
    tokio::time::timeout(Duration::from_millis(50), reaper)
        .await
        .expect("reaper exits")
        .expect("join");
}

#[tokio::test]
async fn retained_reaper_wakes_for_earlier_deadline() {
    let queue = PrimaryOutboundStore::new(4, None);
    let reaper = tokio::spawn(queue.reaper().run());
    let _late = queue
        .enqueue_message(message(1, crate::a3_message_type(0)), deadline_ms(100))
        .await
        .expect("late queued");
    tokio::task::yield_now().await;
    let mut early = queue
        .enqueue_message(message(2, crate::a3_message_type(0)), deadline_ms(5))
        .await
        .expect("early queued");

    let err = tokio::time::timeout(Duration::from_millis(50), &mut early)
        .await
        .expect("earlier deadline wakes")
        .expect("sender completes")
        .expect_err("expired");
    assert_eq!(err.kind, ErrorId::SendTimeout);

    drop(queue);
    tokio::time::timeout(Duration::from_millis(50), reaper)
        .await
        .expect("reaper exits")
        .expect("join");
}

#[tokio::test]
async fn retained_shutdown_fails_tracked_work_and_stops_reaper() {
    let queue = PrimaryOutboundStore::new(2, None);
    let reaper = tokio::spawn(queue.reaper().run());
    let mut ack_rx = queue
        .enqueue_message(message(1, crate::a3_message_type(0)), deadline_ms(100))
        .await
        .expect("queued");

    queue
        .begin_shutdown(AureliaError::new(ErrorId::PeerUnavailable))
        .await;

    let err = ack_rx
        .try_recv()
        .expect("shutdown completed")
        .expect_err("peer unavailable");
    assert_eq!(err.kind, ErrorId::PeerUnavailable);
    tokio::time::timeout(Duration::from_millis(50), reaper)
        .await
        .expect("reaper exits")
        .expect("join");
}

#[tokio::test]
async fn retained_response_duplicate_drops_second_response() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let queue = PrimaryOutboundStore::new(1, None);
        queue.enqueue_ack(7, deadline_ms(100)).await;
        queue.enqueue_error(error_frame(7), deadline_ms(100)).await;

        assert_eq!(queue.live_count_for_tests(RetainedLane::A1Ack).await, 1);
        assert_eq!(queue.live_count_for_tests(RetainedLane::A1Error).await, 0);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn retained_wait_for_a1_response_empty_wakes_after_response_write() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let queue = PrimaryOutboundStore::new(1, None);
        queue.enqueue_ack(7, deadline_ms(100)).await;
        let claim = queue.claim_next(1).await.expect("claim ack");

        queue.complete_write(claim, Ok(())).await;

        assert!(queue.wait_for_a1_response_empty(deadline_ms(10)).await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn retained_close_intent_wakes_only_targeted_callis() {
    let queue = PrimaryOutboundStore::new(1, None);
    let target = queue.close_notifier(10).await;
    let other = queue.close_notifier(11).await;
    let target_wait = target.notified();
    let other_wait = other.notified();
    tokio::pin!(target_wait);
    tokio::pin!(other_wait);

    queue.request_close(10).await;

    tokio::time::timeout(Duration::from_millis(50), &mut target_wait)
        .await
        .expect("targeted close wake");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut other_wait)
            .await
            .is_err(),
        "non-targeted callis must remain parked"
    );
    assert!(queue.take_close_intent(10).await);
    assert!(!queue.take_close_intent(11).await);
}

#[tokio::test]
async fn retained_callis_cleanup_removes_close_intent_and_waiter() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let queue = PrimaryOutboundStore::new(1, None);
        let _notify = queue.close_notifier(12).await;
        queue.request_close(12).await;
        assert_eq!(queue.close_waiter_count_for_tests().await, 1);

        queue.mark_callis_replay_ready(12).await;

        assert!(!queue.take_close_intent(12).await);
        assert_eq!(queue.close_waiter_count_for_tests().await, 0);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn retained_claim_wakes_next_transmitter_when_work_remains() {
    let queue = PrimaryOutboundStore::new(2, None);
    let _first = queue
        .enqueue_message(message(1, crate::a3_message_type(0)), deadline_ms(100))
        .await
        .expect("first queued");
    let _second = queue
        .enqueue_message(message(2, crate::a3_message_type(0)), deadline_ms(100))
        .await
        .expect("second queued");

    tokio::time::timeout(Duration::from_millis(50), queue.notifier().notified())
        .await
        .expect("initial insertion wake");

    let _claim = queue.claim_next(1).await.expect("first claim");

    tokio::time::timeout(Duration::from_millis(50), queue.notifier().notified())
        .await
        .expect("claim chained wake");
}

#[tokio::test]
async fn retained_rejects_completion_bearing_a1_message() {
    tokio::time::timeout(PRIMARY_DISPATCH_TEST_TIMEOUT, async {
        let queue = PrimaryOutboundStore::new(1, None);
        let err = queue
            .enqueue_message(message(1, 0x0000_0001), deadline_ms(100))
            .await
            .expect_err("tracked A1 is invalid");

        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert!(queue.is_empty().await);
    })
    .await
    .expect("async test timed out");
}

fn error_frame(peer_msg_id: PeerMessageId) -> OutboundFrame {
    OutboundFrame::Control {
        msg_type: MSG_ERROR,
        peer_msg_id,
        payload: Bytes::from_static(b"error"),
    }
}
