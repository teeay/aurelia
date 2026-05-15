// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::super::callis::{
    drain_accept_waiters, spawn_callis_task, InboundOutcome, InboundWaiter, MessageWaiter,
};
use super::*;
use crate::session::CancelReason;
use crate::transport::callis::handle_inbound_frame;
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tokio::sync::Notify;

const CALLIS_TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn inbound_frame_with_reconnect_flag_is_protocol_violation() {
    tokio::time::timeout(CALLIS_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let allocator = Arc::new(PeerMessageIdAllocator::default());
        let session = Arc::new(PeerSession::new(
            Arc::clone(&allocator),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        ));
        let blob = Arc::new(BlobManager::new(
            Arc::new(BlobBufferTracker::default()),
            Arc::new(Notify::new()),
            Arc::clone(&allocator),
            128,
        ));
        let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
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
            None,
            header,
            Vec::new(),
            CancelReason::None,
            accept_notify,
            &cancel_tx,
        )
        .await
        .err()
        .expect("expected reconnect violation");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn inbound_error_with_unknown_error_id_is_protocol_violation() {
    tokio::time::timeout(CALLIS_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let allocator = Arc::new(PeerMessageIdAllocator::default());
        let session = Arc::new(PeerSession::new(
            Arc::clone(&allocator),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        ));
        let blob = Arc::new(BlobManager::new(
            Arc::new(BlobBufferTracker::default()),
            Arc::new(Notify::new()),
            Arc::clone(&allocator),
            128,
        ));
        let (events_tx, _events_rx) = mpsc::channel::<PeerStateUpdate>(1);
        let payload = Bytes::from(ErrorPayload::new(u32::MAX, "bad id").to_bytes());
        let header = WireHeader {
            version: PROTOCOL_VERSION,
            flags: 0,
            msg_type: MSG_ERROR,
            peer_msg_id: 7,
            src_taberna: 0,
            dst_taberna: 0,
            payload_len: payload.len() as u32,
        };

        let (cancel_tx, _cancel_rx) = watch::channel(CancelReason::None);
        let accept_notify = Arc::new(Notify::new());
        let err = match handle_inbound_frame(
            registry,
            session,
            blob,
            config,
            events_tx,
            next_callis_id(),
            None,
            header,
            payload.to_vec(),
            CancelReason::None,
            accept_notify,
            &cancel_tx,
        )
        .await
        {
            Ok(_) => panic!("unknown error id must be protocol violation"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert!(err.to_string().contains(&u32::MAX.to_string()));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn callis_reader_reports_established_protocol_violation() {
    let (reporting, observability) =
        crate::observability::new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5081));
    let registry = Arc::new(TabernaRegistry::new());
    let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
    let allocator = Arc::new(PeerMessageIdAllocator::default());
    let primary_dispatch = PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current());
    let session = Arc::new(PeerSession::new(
        Arc::clone(&allocator),
        config.clone(),
        tokio::runtime::Handle::current(),
        Arc::clone(&primary_dispatch),
    ));
    let blob = Arc::new(BlobManager::new(
        Arc::new(BlobBufferTracker::default()),
        Arc::new(Notify::new()),
        Arc::clone(&allocator),
        128,
    ));
    let (client, server) = tokio::io::duplex(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (state_tx, mut state_rx) = mpsc::channel::<PeerStateUpdate>(4);
    let task_set = PeerTaskSet::new(&tokio::runtime::Handle::current());
    let callis_id = next_callis_id();

    spawn_callis_task(
        config,
        session,
        blob,
        registry,
        server,
        callis_id,
        Some(primary_dispatch),
        shutdown_rx,
        state_tx,
        CallisKind::Primary,
        None,
        CallisTracker::new(),
        Some(peer.clone()),
        observability,
        task_set.spawner(),
    );

    let mut client = client;
    send_control_frame(&mut client, MSG_ACK, WireFlags::RECONNECT.bits(), 1, &[])
        .await
        .expect("write invalid frame");

    let update = timeout(Duration::from_millis(500), state_rx.recv())
        .await
        .expect("state update timeout")
        .expect("state update");
    assert!(matches!(
        update,
        PeerStateUpdate::ConnectionClosed {
            callis: CallisKind::Primary,
            id,
            ..
        } if id == callis_id
    ));

    let errors = reporting.errors_since(0, 8).await.expect("errors");
    assert!(
        errors
            .iter()
            .any(|(_, err)| err.kind == ErrorId::ProtocolViolation),
        "expected protocol violation in observability error stream"
    );
    let _ = shutdown_tx.send(true);
}

#[tokio::test]
async fn inbound_waiter_emits_ack_after_session_restart() {
    tokio::time::timeout(CALLIS_TEST_TIMEOUT, async {
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());
        let allocator = Arc::new(PeerMessageIdAllocator::default());
        let session = Arc::new(PeerSession::new(
            Arc::clone(&allocator),
            config.clone(),
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        ));
        let blob = Arc::new(BlobManager::new(
            Arc::new(BlobBufferTracker::default()),
            Arc::new(Notify::new()),
            Arc::clone(&allocator),
            128,
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
    })
    .await
    .expect("async test timed out");
}
