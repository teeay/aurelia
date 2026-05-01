// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::ring_buffer::{InboundInsertOutcome, InboundRingBuffer, OutboundRingBuffer};
use aurelia_ids::ErrorId;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout, Instant};

#[tokio::test]
async fn outbound_ring_seal_marks_last_for_exact_chunk() {
    let ring = OutboundRingBuffer::new(3, 4).expect("ring");
    ring.push_bytes(b"abc", Duration::from_secs(1))
        .await
        .expect("push");

    assert!(ring.take_next_chunk(1).await.is_none());

    ring.seal(Duration::from_secs(1)).await.expect("seal");
    assert!(ring.wait_for_sendable().await.expect("sendable"));
    let chunk = ring.take_next_chunk(2).await.expect("chunk");
    assert_eq!(chunk.data, Bytes::from_static(b"abc"));
    assert!(chunk.is_last);
}

#[tokio::test]
async fn outbound_ring_releases_pending_on_partial() {
    let ring = OutboundRingBuffer::new(3, 4).expect("ring");
    ring.push_bytes(b"abcde", Duration::from_secs(1))
        .await
        .expect("push");

    assert!(ring.wait_for_sendable().await.expect("sendable"));
    let first = ring.take_next_chunk(10).await.expect("first chunk");
    assert_eq!(first.data, Bytes::from_static(b"abc"));
    assert!(!first.is_last);

    ring.seal(Duration::from_secs(1)).await.expect("seal");
    let second = ring.take_next_chunk(11).await.expect("second chunk");
    assert_eq!(second.data, Bytes::from_static(b"de"));
    assert!(second.is_last);
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
    let first = ring.take_next_chunk(22).await.expect("first chunk");
    assert_eq!(first.data, Bytes::from_static(b"ab"));

    let early = timeout(Duration::from_millis(50), &mut push).await;
    assert!(early.is_err(), "push should wait for capacity");

    ring.note_ack(22).await;
    push.await.expect("push join");
}

#[tokio::test]
async fn inbound_ring_buffers_and_completes_in_order() {
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
}

#[tokio::test]
async fn inbound_ring_rejects_missing_last() {
    let ring = InboundRingBuffer::new(4, 4).expect("ring");
    let err = ring
        .insert_chunk(2, Bytes::from_static(b"miss"), true)
        .await
        .expect_err("missing chunk");
    assert_eq!(err.kind, ErrorId::BlobStreamMissingChunk);
}

#[tokio::test]
async fn inbound_ring_window_exceeded() {
    let ring = InboundRingBuffer::new(4, 2).expect("ring");
    let err = ring
        .insert_chunk(2, Bytes::from_static(b"oops"), false)
        .await
        .expect_err("window exceeded");
    assert_eq!(err.kind, ErrorId::BlobAckWindowExceeded);
}

#[tokio::test]
async fn inbound_ring_waits_for_space() {
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
            wait_for_space: true,
            ..
        }
    ));

    let deadline = Instant::now() + Duration::from_millis(50);
    assert!(!ring.wait_for_space(deadline).await);

    let _ = ring.take_next().await;
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(ring.wait_for_space(deadline).await);
}
