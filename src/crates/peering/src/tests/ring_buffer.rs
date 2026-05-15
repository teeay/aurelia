// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::ring_buffer::{
    ChunkWriteLease, InboundInsertOutcome, InboundRingBuffer, OutboundRingBuffer,
};
use aurelia_ids::{ErrorId, PeerMessageId};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout, Instant};

const RING_BUFFER_TEST_TIMEOUT: Duration = Duration::from_millis(500);

async fn write_next_chunk(
    ring: &OutboundRingBuffer,
    callis_id: u64,
    peer_msg_id: PeerMessageId,
) -> ChunkWriteLease {
    let lease = ring
        .lease_next_chunk_for_write(callis_id, peer_msg_id)
        .await
        .expect("chunk write lease");
    ring.mark_chunk_inflight(&lease).await;
    lease
}

#[tokio::test]
async fn ring_buffer_round_trip_preserves_data() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let outbound = OutboundRingBuffer::new(3, 3).expect("outbound");
        let inbound = InboundRingBuffer::new(3, 3).expect("inbound");

        outbound
            .push_bytes(b"abcdefg", Duration::from_secs(1))
            .await
            .expect("push");
        outbound.seal(Duration::from_secs(1)).await.expect("seal");

        let mut peer_msg_id = 10u32;
        loop {
            let sendable = outbound.wait_for_sendable().await.expect("sendable");
            if !sendable {
                break;
            }
            let chunk = outbound
                .lease_next_chunk_for_write(1, peer_msg_id)
                .await
                .expect("chunk");
            outbound.mark_chunk_inflight(&chunk).await;
            let outcome = inbound
                .insert_chunk(chunk.chunk_id, chunk.data.clone(), chunk.is_last)
                .await
                .expect("insert");
            assert!(matches!(outcome, InboundInsertOutcome::Stored { .. }));
            outbound.note_ack(peer_msg_id).await;
            peer_msg_id += 1;
            if chunk.is_last {
                break;
            }
        }

        let mut received = Vec::new();
        while let Some(chunk) = inbound.take_next().await {
            received.extend_from_slice(&chunk);
            if inbound.is_complete().await {
                break;
            }
        }

        assert_eq!(received, Bytes::from_static(b"abcdefg"));
        assert!(inbound.is_complete().await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_seal_marks_last_for_exact_chunk() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 4).expect("ring");
        ring.push_bytes(b"abc", Duration::from_secs(1))
            .await
            .expect("push");

        assert!(ring.lease_next_chunk_for_write(1, 1).await.is_none());

        ring.seal(Duration::from_secs(1)).await.expect("seal");
        assert!(ring.wait_for_sendable().await.expect("sendable"));
        let chunk = write_next_chunk(&ring, 1, 2).await;
        assert_eq!(chunk.data, Bytes::from_static(b"abc"));
        assert!(chunk.is_last);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_releases_pending_on_partial() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 4).expect("ring");
        ring.push_bytes(b"abcde", Duration::from_secs(1))
            .await
            .expect("push");

        assert!(ring.wait_for_sendable().await.expect("sendable"));
        let first = write_next_chunk(&ring, 1, 10).await;
        assert_eq!(first.data, Bytes::from_static(b"abc"));
        assert!(!first.is_last);

        ring.seal(Duration::from_secs(1)).await.expect("seal");
        let second = write_next_chunk(&ring, 1, 11).await;
        assert_eq!(second.data, Bytes::from_static(b"de"));
        assert!(second.is_last);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_waits_for_capacity_until_ack() {
    let ring = Arc::new(OutboundRingBuffer::new(2, 1).expect("ring"));
    let ring_ref = Arc::clone(&ring);
    let mut push = tokio::spawn(async move {
        ring_ref
            .push_bytes(b"abcd", Duration::from_secs(1))
            .await
            .expect("push");
    });

    assert!(ring.wait_for_sendable().await.expect("sendable"));
    let first = write_next_chunk(&ring, 1, 22).await;
    assert_eq!(first.data, Bytes::from_static(b"ab"));

    let early = timeout(Duration::from_millis(50), &mut push).await;
    assert!(early.is_err(), "push should wait for capacity");

    ring.note_ack(22).await;
    push.await.expect("push join");
}

#[tokio::test]
async fn outbound_ring_write_lease_keeps_chunk_bytes_until_ack() {
    let ring = Arc::new(OutboundRingBuffer::new(3, 1).expect("ring"));
    ring.push_bytes(b"abcd", Duration::from_secs(1))
        .await
        .expect("push");

    let lease = ring
        .lease_next_chunk_for_write(7, 100)
        .await
        .expect("lease");
    assert_eq!(lease.data, Bytes::from_static(b"abc"));
    let leased_ptr = lease.data.as_ptr();

    let blocked_ring = Arc::clone(&ring);
    let mut push = tokio::spawn(async move {
        blocked_ring
            .push_bytes(b"def", Duration::from_secs(1))
            .await
            .expect("push waits until ack");
    });
    assert!(
        timeout(Duration::from_millis(50), &mut push).await.is_err(),
        "writing chunk should still consume window capacity"
    );

    ring.mark_chunk_inflight(&lease).await;
    assert!(
        timeout(Duration::from_millis(50), &mut push).await.is_err(),
        "inflight chunk should still consume window capacity"
    );
    ring.note_ack(100).await;
    push.await.expect("push join");

    assert_eq!(lease.data.as_ptr(), leased_ptr);
}

#[tokio::test]
async fn outbound_ring_slots_stay_counted_until_terminal_completion() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 1).expect("ring");
        assert_eq!(ring.live_chunk_count().await, 0);
        assert_eq!(ring.inflight_chunk_count().await, 0);
        assert!(ring.has_window_capacity().await);

        ring.push_bytes(b"abcd", Duration::from_secs(1))
            .await
            .expect("push");
        assert_eq!(ring.live_chunk_count().await, 1);
        assert!(!ring.has_window_capacity().await);
        assert!(ring.has_dispatchable_fresh().await);

        let _first = ring
            .lease_next_chunk_for_write(7, 111)
            .await
            .expect("first lease");
        assert_eq!(ring.live_chunk_count().await, 1);
        assert!(!ring.has_window_capacity().await);
        assert!(!ring.has_dispatchable_fresh().await);

        ring.mark_callis_replay_ready(7).await;
        assert_eq!(ring.live_chunk_count().await, 1);
        assert!(ring.has_dispatchable_replay().await);
        assert!(!ring.has_window_capacity().await);

        let replay = ring
            .lease_next_chunk_for_write(8, 112)
            .await
            .expect("replay lease");
        ring.mark_chunk_inflight(&replay).await;
        assert_eq!(ring.live_chunk_count().await, 1);
        assert_eq!(ring.inflight_chunk_count().await, 1);
        assert!(!ring.has_window_capacity().await);

        ring.note_ack(112).await;
        assert_eq!(ring.live_chunk_count().await, 0);
        assert_eq!(ring.inflight_chunk_count().await, 0);
        assert!(ring.has_window_capacity().await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_replays_failed_write_from_same_slot_bytes() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 1).expect("ring");
        ring.push_bytes(b"abc", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");

        let first = ring
            .lease_next_chunk_for_write(7, 101)
            .await
            .expect("first lease");
        let first_ptr = first.data.as_ptr();
        ring.mark_callis_replay_ready(7).await;

        assert!(ring.wait_for_sendable().await.expect("sendable"));
        let replay = ring
            .lease_next_chunk_for_write(8, 102)
            .await
            .expect("replay lease");
        assert_eq!(replay.chunk_id, first.chunk_id);
        assert_eq!(replay.data.as_ptr(), first_ptr);
        assert!(replay.slot_seq > first.slot_seq);

        ring.mark_chunk_inflight(&replay).await;
        ring.note_ack(102).await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_ignores_stale_write_completion() {
    let ring = Arc::new(OutboundRingBuffer::new(3, 1).expect("ring"));
    ring.push_bytes(b"abcd", Duration::from_secs(1))
        .await
        .expect("push");

    let stale = ring
        .lease_next_chunk_for_write(7, 103)
        .await
        .expect("first lease");
    ring.mark_callis_replay_ready(7).await;
    let current = ring
        .lease_next_chunk_for_write(8, 104)
        .await
        .expect("second lease");

    ring.mark_chunk_inflight(&stale).await;
    ring.note_ack(103).await;

    let blocked_ring = Arc::clone(&ring);
    let mut push = tokio::spawn(async move {
        blocked_ring
            .push_bytes(b"def", Duration::from_secs(1))
            .await
            .expect("push waits for current ack");
    });
    assert!(
        timeout(Duration::from_millis(50), &mut push).await.is_err(),
        "stale completion must not release the current lease"
    );

    ring.mark_chunk_inflight(&current).await;
    ring.note_ack(104).await;
    push.await.expect("push join");
}

#[tokio::test]
async fn outbound_ring_marks_writing_and_inflight_chunks_replay_ready_for_callis() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 2).expect("ring");
        ring.push_bytes(b"abcdefg", Duration::from_secs(1))
            .await
            .expect("push");

        let writing = ring
            .lease_next_chunk_for_write(7, 105)
            .await
            .expect("writing lease");
        let inflight = ring
            .lease_next_chunk_for_write(7, 106)
            .await
            .expect("inflight lease");
        ring.mark_chunk_inflight(&inflight).await;

        ring.mark_callis_replay_ready(7).await;
        ring.note_ack(106).await;

        let replay_one = ring
            .lease_next_chunk_for_write(8, 107)
            .await
            .expect("replay one");
        let replay_two = ring
            .lease_next_chunk_for_write(8, 108)
            .await
            .expect("replay two");
        assert_eq!(
            [replay_one.chunk_id, replay_two.chunk_id],
            [writing.chunk_id, inflight.chunk_id]
        );
        ring.mark_chunk_inflight(&replay_one).await;
        ring.mark_chunk_inflight(&replay_two).await;
        ring.note_ack(107).await;
        ring.note_ack(108).await;
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_rejects_write_after_seal() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 4).expect("ring");
        ring.push_bytes(b"abc", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");

        let err = ring
            .push_bytes(b"d", Duration::from_secs(1))
            .await
            .expect_err("write after seal");
        assert_eq!(err.kind, ErrorId::PeerUnavailable);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_empty_stream_emits_empty_last_chunk() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 4).expect("ring");
        ring.seal(Duration::from_secs(1)).await.expect("seal");

        assert!(ring.wait_for_sendable().await.expect("sendable"));
        let chunk = write_next_chunk(&ring, 1, 44).await;
        assert_eq!(chunk.data, Bytes::new());
        assert!(chunk.is_last);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_close_wakes_sendable_waiter() {
    let ring = Arc::new(OutboundRingBuffer::new(3, 4).expect("ring"));
    let waiter_ring = Arc::clone(&ring);
    let waiter = tokio::spawn(async move { waiter_ring.wait_for_sendable().await });

    ring.close().await;

    let result = timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter timeout")
        .expect("waiter join")
        .expect("sendable result");
    assert!(!result);
}

#[tokio::test]
async fn outbound_ring_failure_wakes_complete_waiter() {
    let ring = Arc::new(OutboundRingBuffer::new(3, 4).expect("ring"));

    let complete_ring = Arc::clone(&ring);
    let complete = tokio::spawn(async move {
        complete_ring
            .wait_for_complete(Instant::now() + Duration::from_secs(1))
            .await
    });

    ring.fail(aurelia_ids::AureliaError::new(ErrorId::PeerUnavailable))
        .await;

    let complete_err = timeout(Duration::from_secs(1), complete)
        .await
        .expect("complete timeout")
        .expect("complete join")
        .expect_err("complete error");
    assert_eq!(complete_err.kind, ErrorId::PeerUnavailable);
}

#[tokio::test]
async fn outbound_ring_failure_takes_precedence_over_late_complete() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 4).expect("ring");
        ring.fail(aurelia_ids::AureliaError::new(ErrorId::PeerUnavailable))
            .await;
        ring.mark_complete().await;

        let err = ring
            .wait_for_complete(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("failure wins");
        assert_eq!(err.kind, ErrorId::PeerUnavailable);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_note_error_replaces_prior_fail() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 4).expect("ring");
        ring.push_bytes(b"abc", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");
        let _chunk = write_next_chunk(&ring, 1, 88).await;

        ring.fail(aurelia_ids::AureliaError::new(ErrorId::PeerUnavailable))
            .await;
        ring.note_error(
            88,
            aurelia_ids::AureliaError::new(ErrorId::ProtocolViolation),
        )
        .await;

        let err = ring
            .wait_for_complete(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("latest note_error should surface");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_latest_note_error_wins() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 4).expect("ring");

        ring.note_error(89, aurelia_ids::AureliaError::new(ErrorId::PeerUnavailable))
            .await;
        ring.note_error(
            90,
            aurelia_ids::AureliaError::new(ErrorId::ProtocolViolation),
        )
        .await;

        let err = ring
            .wait_for_complete(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("latest note_error should surface");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn outbound_ring_seal_is_idempotent_while_final_chunk_inflight() {
    let ring = OutboundRingBuffer::new(3, 1).expect("ring");
    ring.push_bytes(b"abc", Duration::from_secs(1))
        .await
        .expect("push");
    ring.seal(Duration::from_secs(1)).await.expect("seal");
    let _chunk = write_next_chunk(&ring, 1, 91).await;

    timeout(Duration::from_millis(50), ring.seal(Duration::from_secs(1)))
        .await
        .expect("second seal should not wait")
        .expect("second seal");
}

#[tokio::test]
async fn outbound_ring_seal_is_idempotent_after_complete() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = OutboundRingBuffer::new(3, 1).expect("ring");
        ring.push_bytes(b"abc", Duration::from_secs(1))
            .await
            .expect("push");
        ring.seal(Duration::from_secs(1)).await.expect("seal");
        let _chunk = write_next_chunk(&ring, 1, 92).await;
        ring.note_ack(92).await;
        ring.mark_complete().await;

        ring.seal(Duration::from_secs(1))
            .await
            .expect("seal after complete");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_ring_buffers_and_completes_in_order() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = InboundRingBuffer::new(3, 4).expect("ring");
        let out1 = ring
            .insert_chunk(1, Bytes::from_static(b"one"), false)
            .await
            .expect("insert 1");
        assert!(matches!(out1, InboundInsertOutcome::Stored { .. }));

        let out0 = ring
            .insert_chunk(0, Bytes::from_static(b"zer"), false)
            .await
            .expect("insert 0");
        assert!(matches!(out0, InboundInsertOutcome::Stored { .. }));

        let out2 = ring
            .insert_chunk(2, Bytes::from_static(b"two"), true)
            .await
            .expect("insert 2");
        assert!(matches!(
            out2,
            InboundInsertOutcome::Stored { complete: true, .. }
        ));

        assert_eq!(ring.take_next().await, Some(Bytes::from_static(b"zer")));
        assert_eq!(ring.take_next().await, Some(Bytes::from_static(b"one")));
        assert_eq!(ring.take_next().await, Some(Bytes::from_static(b"two")));
        assert!(ring.is_complete().await);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_ring_rejects_missing_last() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = InboundRingBuffer::new(4, 4).expect("ring");
        let err = ring
            .insert_chunk(2, Bytes::from_static(b"miss"), true)
            .await
            .expect_err("missing chunk");
        assert_eq!(err.kind, ErrorId::BlobStreamMissingChunk);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_ring_window_exceeded() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = InboundRingBuffer::new(4, 2).expect("ring");
        let err = ring
            .insert_chunk(2, Bytes::from_static(b"oops"), false)
            .await
            .expect_err("window exceeded");
        assert_eq!(err.kind, ErrorId::BlobAckWindowExceeded);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_ring_rejects_full_receiver_without_wait() {
    tokio::time::timeout(RING_BUFFER_TEST_TIMEOUT, async {
        let ring = InboundRingBuffer::new(2, 2).expect("ring");
        ring.insert_chunk(0, Bytes::from_static(b"aa"), false)
            .await
            .expect("insert 0");
        let out = ring
            .insert_chunk(1, Bytes::from_static(b"bb"), false)
            .await
            .expect("insert 1");
        assert!(matches!(
            out,
            InboundInsertOutcome::Stored {
                wait_for_space: false,
                ..
            }
        ));

        let err = ring
            .insert_chunk(2, Bytes::from_static(b"cc"), false)
            .await
            .expect_err("full receiver");
        assert_eq!(err.kind, ErrorId::BlobBufferFull);
    })
    .await
    .expect("async test timed out");
}
