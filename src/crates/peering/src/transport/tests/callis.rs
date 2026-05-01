// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::super::callis::{drain_accept_waiters, InboundOutcome, InboundWaiter, MessageWaiter};
use super::*;
use crate::session::CancelReason;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tokio::sync::Notify;

#[tokio::test]
async fn inbound_frame_with_reconnect_flag_is_protocol_violation() {
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let allocator = Arc::new(PeerMessageIdAllocator::default());
    let session = Arc::new(PeerSession::new(
        Arc::clone(&allocator),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    let blob = Arc::new(BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::clone(&allocator),
    ));
    let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: WireFlags::RECONNECT.bits(),
        msg_type: MSG_ACK,
        peer_msg_id: 1,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: 0,
    };

    let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
    let accept_notify = Arc::new(Notify::new());
    let err = handle_inbound_frame(
        registry,
        session,
        blob,
        config,
        events_tx,
        next_callis_id(),
        CallisKind::Primary,
        None,
        header,
        Vec::new(),
        outbound_tx,
        CancelReason::None,
        accept_notify,
        &cancel_tx,
    )
    .await
    .err()
    .expect("expected reconnect violation");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[tokio::test]
async fn inbound_waiter_emits_ack_after_session_restart() {
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let allocator = Arc::new(PeerMessageIdAllocator::default());
    let session = Arc::new(PeerSession::new(
        Arc::clone(&allocator),
        config.clone(),
        tokio::runtime::Handle::current(),
    ));
    let blob = Arc::new(BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::clone(&allocator),
    ));

    let (accept_tx, accept_rx) = oneshot::channel();
    let _ = accept_tx.send(Ok(()));
    let peer_msg_id = 10;
    let mut waiters = HashMap::new();
    waiters.insert(
        peer_msg_id,
        InboundWaiter::Message(MessageWaiter {
            dst_taberna: 1,
            accept_rx,
        }),
    );

    session.mark_restarted().await;

    let outcomes = drain_accept_waiters(&mut waiters, &session, &blob).await;
    assert_eq!(outcomes.len(), 1);
    match outcomes[0] {
        InboundOutcome::Ack(id) => assert_eq!(id, peer_msg_id),
        _ => panic!("expected ack outcome"),
    }
}
