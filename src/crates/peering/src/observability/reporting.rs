// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::sync::{broadcast, oneshot};

use aurelia_data::DomusAddr;
use aurelia_ids::{AureliaError, ErrorId};

use super::actor::{ObservabilityCommand, ObservabilityStore};
use super::types::{DomusMetrics, DomusMetricsDelta, DomusReportingEvent, PeerIdentityReport};

/// Observability handle obtained from [`crate::Domus::reporting`]. Provides
/// snapshot access to metrics, enumeration of connected peers, and pub-sub
/// access to the live event and error streams.
#[derive(Clone)]
pub struct DomusReporting {
    pub(super) store: Arc<ObservabilityStore>,
}

impl DomusReporting {
    /// Returns a cumulative metrics snapshot.
    pub async fn snapshot(&self) -> Result<DomusMetrics, AureliaError> {
        let (tx, rx) = oneshot::channel();
        self.store
            .tx
            .send(ObservabilityCommand::Snapshot { reply: tx })
            .await
            .map_err(|_| snapshot_not_available())?;
        rx.await.map_err(|_| snapshot_not_available())
    }

    /// Returns the metrics delta since the previous call and resets counters.
    pub async fn snapshot_and_reset(&self) -> Result<DomusMetricsDelta, AureliaError> {
        let (tx, rx) = oneshot::channel();
        self.store
            .tx
            .send(ObservabilityCommand::SnapshotAndReset { reply: tx })
            .await
            .map_err(|_| snapshot_not_available())?;
        rx.await.map_err(|_| snapshot_not_available())
    }

    /// Returns the addresses of peers currently connected on at least one callis.
    pub async fn connected_peer_identities(&self) -> Result<Vec<DomusAddr>, AureliaError> {
        let (tx, rx) = oneshot::channel();
        self.store
            .tx
            .send(ObservabilityCommand::ConnectedPeerIdentities { reply: tx })
            .await
            .map_err(|_| snapshot_not_available())?;
        rx.await.map_err(|_| snapshot_not_available())
    }

    /// Returns identity reports for currently connected peers.
    pub async fn connected_peers(&self) -> Result<Vec<PeerIdentityReport>, AureliaError> {
        let (tx, rx) = oneshot::channel();
        self.store
            .tx
            .send(ObservabilityCommand::ConnectedPeers { reply: tx })
            .await
            .map_err(|_| snapshot_not_available())?;
        rx.await.map_err(|_| snapshot_not_available())
    }

    /// Subscribes to the event stream. The returned receiver yields
    /// [`DomusReportingEvent`] values until it is dropped or the domus is shut down.
    pub fn subscribe_events(&self) -> broadcast::Receiver<DomusReportingEvent> {
        self.store.events.subscribe()
    }

    /// Subscribes to the error stream. Each item is `(sequence, error)`;
    /// the sequence allows callers to detect missed errors after lag.
    pub fn subscribe_errors(&self) -> broadcast::Receiver<(u64, AureliaError)> {
        self.store.errors.subscribe()
    }

    /// Returns a [`DomusReportingFeeds`] bundling fresh subscriptions to
    /// the event and error streams.
    pub fn feeds(&self) -> DomusReportingFeeds {
        DomusReportingFeeds {
            events: self.subscribe_events(),
            errors: self.subscribe_errors(),
        }
    }

    /// Returns up to `limit` buffered errors with sequence numbers strictly
    /// greater than `seq`. Useful for catch-up after a missed broadcast.
    pub async fn errors_since(
        &self,
        seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, AureliaError)>, AureliaError> {
        let (tx, rx) = oneshot::channel();
        self.store
            .tx
            .send(ObservabilityCommand::ErrorsSince {
                seq,
                limit,
                reply: tx,
            })
            .await
            .map_err(|_| snapshot_not_available())?;
        rx.await.map_err(|_| snapshot_not_available())
    }
}

fn snapshot_not_available() -> AureliaError {
    AureliaError::new(ErrorId::SnapshotNotAvailable)
}

/// Bundled live broadcast receivers for events and errors, returned by
/// `DomusBuilder::build_with_reporting` or [`DomusReporting::feeds`].
pub struct DomusReportingFeeds {
    /// Live event receiver.
    pub events: broadcast::Receiver<DomusReportingEvent>,
    /// Live error receiver, with monotonic sequence numbers.
    pub errors: broadcast::Receiver<(u64, AureliaError)>,
}
