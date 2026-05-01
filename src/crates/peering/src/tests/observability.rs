// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::time::timeout;

use crate::address::DomusAddr;
use crate::callis::CallisKind;
use crate::observability::{
    new_observability_with_capacity, BlobCallisSettingsReport, DisconnectReason,
    DomusReportingEvent,
};
use aurelia_ids::ErrorId;

#[tokio::test]
async fn observability_snapshot_and_reset_tracks_deltas() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000));

    handle.dial_attempt(peer.clone(), CallisKind::Primary).await;
    handle
        .dial_failed(peer.clone(), CallisKind::Primary, ErrorId::PeerUnavailable)
        .await;
    handle.primary_connected(peer.clone(), 1, false).await;
    handle
        .primary_disconnected(peer.clone(), 1, DisconnectReason::LocalRequest)
        .await;

    let snapshot = reporting.snapshot().await;
    assert_eq!(snapshot.total_dial_attempts, 1);
    assert_eq!(snapshot.total_dial_failures, 1);
    assert_eq!(snapshot.total_primary_opened, 1);
    assert_eq!(snapshot.total_primary_closed, 1);

    let delta = reporting.snapshot_and_reset().await;
    assert_eq!(delta.total_dial_attempts, 1);
    assert_eq!(delta.total_dial_failures, 1);
    assert_eq!(delta.total_primary_opened, 1);
    assert_eq!(delta.total_primary_closed, 1);

    let delta_again = reporting.snapshot_and_reset().await;
    assert_eq!(delta_again.total_dial_attempts, 0);
    assert_eq!(delta_again.total_dial_failures, 0);
    assert_eq!(delta_again.total_primary_opened, 0);
    assert_eq!(delta_again.total_primary_closed, 0);
}

#[tokio::test]
async fn observability_error_ring_is_bounded() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 2);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5001));

    handle
        .identity_mismatch(peer.clone(), peer.clone(), peer.clone())
        .await;
    handle
        .protocol_violation(peer.clone(), ErrorId::ProtocolViolation)
        .await;
    handle
        .address_mismatch(peer.clone(), ErrorId::AddressMismatch)
        .await;

    let errors = reporting.errors_since(0, 10).await;
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].0, 2);
    assert_eq!(errors[0].1.kind, ErrorId::ProtocolViolation);
    assert_eq!(errors[1].0, 3);
    assert_eq!(errors[1].1.kind, ErrorId::AddressMismatch);
}

#[tokio::test]
async fn observability_emits_events_in_order() {
    let (reporting, handle) = new_observability_with_capacity(tokio::runtime::Handle::current(), 8);
    let peer = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5002));
    let mut events = reporting.subscribe_events();

    handle.primary_connected(peer.clone(), 10, true).await;
    handle
        .blob_connected(
            peer.clone(),
            11,
            BlobCallisSettingsReport {
                chunk_size: 4,
                ack_window_chunks: 8,
            },
        )
        .await;

    let first = timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("event timeout")
        .expect("event recv");
    let second = timeout(Duration::from_secs(2), events.recv())
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
