// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::time::timeout;

use crate::callis::CallisKind;
use crate::observability::{
    new_observability_with_capacity, BlobCallisSettingsReport, DisconnectReason,
    DomusReportingEvent, OutboundQueueTierReport,
};
use aurelia_data::DomusAddr;
use aurelia_ids::ErrorId;

const OBSERVABILITY_TEST_TIMEOUT: Duration = Duration::from_millis(500);

#[tokio::test]
async fn observability_snapshot_and_reset_tracks_deltas() {
    tokio::time::timeout(OBSERVABILITY_TEST_TIMEOUT, async {
        let (reporting, handle) =
            new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
        let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000));

        handle.dial_attempt();
        handle.dial_failed(peer.clone(), CallisKind::Primary, ErrorId::PeerUnavailable);
        handle.primary_connected(peer.clone(), 1, false);
        handle.primary_disconnected(peer.clone(), 1, DisconnectReason::LocalRequest);

        let snapshot = reporting.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.total_dial_attempts, 1);
        assert_eq!(snapshot.total_dial_failures, 1);
        assert_eq!(snapshot.total_primary_opened, 1);
        assert_eq!(snapshot.total_primary_closed, 1);

        let delta = reporting.snapshot_and_reset().await.expect("delta");
        assert_eq!(delta.total_dial_attempts, 1);
        assert_eq!(delta.total_dial_failures, 1);
        assert_eq!(delta.total_primary_opened, 1);
        assert_eq!(delta.total_primary_closed, 1);
        assert_eq!(delta.created_at, snapshot.created_at);
        assert_eq!(delta.last_reset_at, snapshot.last_reset_at);

        let delta_again = reporting.snapshot_and_reset().await.expect("second delta");
        assert_eq!(delta_again.total_dial_attempts, 0);
        assert_eq!(delta_again.total_dial_failures, 0);
        assert_eq!(delta_again.total_primary_opened, 0);
        assert_eq!(delta_again.total_primary_closed, 0);
        assert_eq!(delta_again.created_at, snapshot.created_at);
        assert_eq!(delta_again.last_reset_at, delta.last_snapshot_at);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn observability_snapshot_and_reset_tracks_interval_peaks() {
    tokio::time::timeout(OBSERVABILITY_TEST_TIMEOUT, async {
        let (reporting, handle) =
            new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
        let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5010));

        handle.primary_connected(peer.clone(), 1, false);
        handle.primary_connected(peer.clone(), 2, false);
        handle.primary_disconnected(peer.clone(), 1, DisconnectReason::LocalRequest);

        let first = reporting.snapshot_and_reset().await.expect("first delta");
        assert_eq!(first.current_primary_callis, 1);
        assert_eq!(first.peak_primary_callis, 2);

        let second = reporting.snapshot_and_reset().await.expect("second delta");
        assert_eq!(second.current_primary_callis, 1);
        assert_eq!(second.peak_primary_callis, 1);
        assert_eq!(second.total_primary_opened, 0);
        assert_eq!(second.last_reset_at, first.last_snapshot_at);

        let absolute = reporting.snapshot().await.expect("snapshot");
        assert_eq!(absolute.peak_primary_callis, 2);
        assert_eq!(absolute.last_reset_at, second.last_snapshot_at);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn observability_error_ring_is_bounded() {
    tokio::time::timeout(OBSERVABILITY_TEST_TIMEOUT, async {
        let (reporting, handle) =
            new_observability_with_capacity(tokio::runtime::Handle::current(), 2);
        let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5001));

        handle.identity_mismatch(peer.clone(), peer.clone(), peer.clone());
        handle.protocol_violation(peer.clone(), ErrorId::ProtocolViolation);
        handle.address_mismatch(peer.clone(), ErrorId::AddressMismatch);

        let errors = reporting.errors_since(0, 10).await.expect("errors");
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].0, 2);
        assert_eq!(errors[0].1.kind, ErrorId::ProtocolViolation);
        assert_eq!(errors[1].0, 3);
        assert_eq!(errors[1].1.kind, ErrorId::AddressMismatch);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn observability_reports_dial_failure_event() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5011));
    let mut events = reporting.subscribe_events();

    handle.dial_failed(peer.clone(), CallisKind::Blob, ErrorId::SendTimeout);

    let event = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    assert!(matches!(
        event,
        DomusReportingEvent::PeerDialFailedEvent {
            peer: event_peer,
            callis: "blob",
            error_id: ErrorId::SendTimeout,
            ..
        } if event_peer == peer
    ));

    let snapshot = reporting.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.total_dial_failures, 1);
}

#[tokio::test]
async fn observability_reports_fresh_session_event() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5012));
    let mut events = reporting.subscribe_events();

    handle.primary_connected(peer.clone(), 50, true);

    let first = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    let second = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    let third = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");

    assert!(matches!(
        first,
        DomusReportingEvent::PeerConnectedEvent {
            peer: event_peer,
            fresh_session: true,
            ..
        } if event_peer == peer
    ));
    assert!(matches!(
        second,
        DomusReportingEvent::PrimaryCallisConnectedEvent {
            peer: event_peer,
            callis_id: 50,
            ..
        } if event_peer == peer
    ));
    assert!(matches!(
        third,
        DomusReportingEvent::PeerSessionRestartedEvent {
            peer: event_peer,
            reason: "fresh-session",
            ..
        } if event_peer == peer
    ));
}

#[tokio::test]
async fn observability_reports_config_and_auth_reload_events() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let mut events = reporting.subscribe_events();

    handle.config_reloaded();
    handle.auth_reloaded();

    let first = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    let second = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");

    assert!(matches!(
        first,
        DomusReportingEvent::ConfigReloadedEvent { .. }
    ));
    assert!(matches!(
        second,
        DomusReportingEvent::AuthReloadedEvent { .. }
    ));
}

#[tokio::test]
async fn observability_event_is_emitted_after_metric_update() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5013));
    let mut events = reporting.subscribe_events();

    handle.primary_connected(peer, 51, false);

    let event = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    assert!(matches!(
        event,
        DomusReportingEvent::PeerConnectedEvent { .. }
    ));

    let snapshot = reporting.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.current_peers, 1);
    assert_eq!(snapshot.current_primary_callis, 1);
    assert_eq!(snapshot.total_primary_opened, 1);
}

#[tokio::test]
async fn observability_emits_events_in_order() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5002));
    let mut events = reporting.subscribe_events();

    handle.primary_connected(peer.clone(), 10, true);
    handle.blob_connected(
        peer.clone(),
        11,
        BlobCallisSettingsReport {
            chunk_size: 4,
            ack_window_chunks: 8,
        },
    );

    let first = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    let second = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");

    assert!(matches!(
        first,
        DomusReportingEvent::PeerConnectedEvent { .. }
    ));
    assert!(matches!(
        second,
        DomusReportingEvent::PrimaryCallisConnectedEvent { .. }
    ));
}

#[tokio::test]
async fn observability_duplicate_connect_keeps_peer_count_stable() {
    tokio::time::timeout(OBSERVABILITY_TEST_TIMEOUT, async {
        let (reporting, handle) =
            new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
        let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5003));

        handle.primary_connected(peer.clone(), 20, false);
        handle.primary_connected(peer, 21, false);

        let snapshot = reporting.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.current_peers, 1);
        assert_eq!(snapshot.current_primary_callis, 2);
        assert_eq!(snapshot.peak_peers, 1);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn observability_connected_peers_reports_callis_counts() {
    tokio::time::timeout(OBSERVABILITY_TEST_TIMEOUT, async {
        let (reporting, handle) =
            new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
        let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5008));

        handle.primary_connected(peer.clone(), 40, false);
        handle.primary_connected(peer.clone(), 41, false);
        handle.blob_connected(
            peer.clone(),
            42,
            BlobCallisSettingsReport {
                chunk_size: 4,
                ack_window_chunks: 8,
            },
        );

        let peers = reporting.connected_peers().await.expect("connected peers");
        let report = peers
            .into_iter()
            .find(|report| report.peer == peer)
            .expect("peer report");
        assert_eq!(report.primary_callis_count, 2);
        assert_eq!(report.blob_callis_count, 1);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn observability_duplicate_disconnect_does_not_underflow() {
    tokio::time::timeout(OBSERVABILITY_TEST_TIMEOUT, async {
        let (reporting, handle) =
            new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
        let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5004));

        handle.primary_connected(peer.clone(), 30, false);
        handle.primary_disconnected(peer.clone(), 30, DisconnectReason::LocalRequest);
        handle.primary_disconnected(peer, 30, DisconnectReason::LocalRequest);

        let snapshot = reporting.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.current_peers, 0);
        assert_eq!(snapshot.current_primary_callis, 0);
        assert_eq!(snapshot.total_primary_closed, 1);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn observability_reports_backpressure_triggered() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5009));
    let mut events = reporting.subscribe_events();

    handle.backpressure_triggered(peer.clone(), 77);

    let event = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    assert!(matches!(
        event,
        DomusReportingEvent::BackpressureTriggeredEvent {
            peer: event_peer,
            taberna_id: 77,
            ..
        } if event_peer == peer
    ));
}

#[tokio::test]
async fn observability_snapshot_reply_waits_for_prior_command_backlog() {
    tokio::time::timeout(OBSERVABILITY_TEST_TIMEOUT, async {
        let (reporting, handle) =
            new_observability_with_capacity(tokio::runtime::Handle::current(), 8);

        for _ in 0..100 {
            handle.dial_attempt();
        }

        let snapshot = reporting.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.total_dial_attempts, 100);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn observability_reports_outbound_queue_overrun() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5006));
    let mut events = reporting.subscribe_events();

    handle.outbound_queue_overrun(
        peer.clone(),
        OutboundQueueTierReport::A3,
        1,
        crate::a3_message_type(0),
    );

    let event = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    assert!(matches!(
        &event,
        DomusReportingEvent::OutboundQueueOverrunEvent {
            peer: event_peer,
            tier: "a3",
            limit: 1,
            msg_type,
            ..
        } if *event_peer == peer && *msg_type == crate::a3_message_type(0)
    ));

    let snapshot = reporting.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.total_outbound_queue_overruns, 1);
    assert_eq!(snapshot.total_a3_queue_overruns, 1);
}

#[tokio::test]
async fn observability_shutdown_events_keep_command_order() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let mut events = reporting.subscribe_events();

    handle.shutdown_started();
    handle.shutdown_complete();

    let first = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    let second = timeout(Duration::from_millis(500), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");

    assert!(matches!(
        first,
        DomusReportingEvent::ShutdownStartedEvent { .. }
    ));
    assert!(matches!(
        second,
        DomusReportingEvent::ShutdownCompleteEvent { .. }
    ));
}
