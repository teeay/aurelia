// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use aurelia_peering::ring_buffer::{InboundInsertOutcome, InboundRingBuffer, OutboundRingBuffer};
use bytes::Bytes;
use std::time::Duration;

#[tokio::test]
async fn ring_buffer_round_trip_preserves_data() {
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
        let chunk = outbound.take_next_chunk(peer_msg_id).await.expect("chunk");
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
}
