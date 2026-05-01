// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::address::DomusAddr;
use crate::callis::CallisKind;
use aurelia_ids::{AureliaError, ErrorId, TabernaId};

const DEFAULT_EVENT_BUFFER: usize = 256;
const DEFAULT_ERROR_BUFFER: usize = 256;
const DEFAULT_COMMAND_BUFFER: usize = 1024;

/// Stable identifier for an individual callis (a single peer connection
/// flow within a session).
pub type CallisId = u64;

/// Snapshot of blob-callis settings reported alongside connection events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobCallisSettingsReport {
    /// Effective blob chunk size in bytes.
    pub chunk_size: u32,
    /// Effective blob acknowledgment window in chunks.
    pub ack_window_chunks: u32,
}

/// Identity report for a connected peer, used when enumerating peers via
/// [`DomusReporting::connected_peers`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerIdentityReport {
    /// Address of the connected peer.
    pub peer: DomusAddr,
    /// Identifier of the peer's primary callis.
    pub primary_callis: u64,
    /// Identifier of the peer's blob callis.
    pub blob_callis: u64,
}

/// Reason a peer connection was closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DisconnectReason {
    /// The local domus initiated the close.
    LocalRequest,
    /// The remote peer cleanly closed the connection.
    RemoteClosed,
    /// The underlying transport connection dropped.
    ConnectionClosed,
    /// A protocol violation forced the connection to abort.
    ProtocolViolation,
    /// The peer restarted; sessions on the previous instance are gone.
    PeerRestarted,
    /// The local domus is shutting down.
    Shutdown,
    /// The reason is not classified.
    Unknown,
}

/// Reason a peer session was restarted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestartReason {
    /// The peer presented a fresh session identifier.
    FreshSession,
    /// The reason is not classified.
    Unknown,
}

/// Phase a handshake reached when something occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakePhase {
    /// Outbound primary callis hello phase.
    OutboundPrimaryHello,
    /// Outbound blob callis hello phase.
    OutboundBlobHello,
    /// Inbound hello reception phase.
    InboundHello,
}

/// Event surfaced on the [`DomusReporting`] event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomusReportingEvent {
    /// A peer became reachable on the named callis kind.
    PeerConnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Which callis kind connected.
        callis: CallisKind,
        /// `true` if this is a freshly negotiated session.
        fresh_session: bool,
    },
    /// A peer disconnected on the named callis kind.
    PeerDisconnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Which callis kind disconnected.
        callis: CallisKind,
        /// Disconnect reason.
        reason: DisconnectReason,
    },
    /// A dial attempt to a peer failed.
    PeerDialFailedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the dialled peer.
        peer: DomusAddr,
        /// Which callis kind was being dialled.
        callis: CallisKind,
        /// Error identifier produced by the failure.
        error_id: ErrorId,
    },
    /// A primary callis connection was established.
    PrimaryCallisConnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Identifier of the primary callis.
        callis_id: CallisId,
    },
    /// A primary callis connection was closed.
    PrimaryCallisDisconnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Identifier of the primary callis.
        callis_id: CallisId,
        /// Disconnect reason.
        reason: DisconnectReason,
    },
    /// A blob callis connection was established.
    BlobCallisConnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Identifier of the blob callis.
        callis_id: CallisId,
        /// Effective blob settings on the new callis.
        settings: BlobCallisSettingsReport,
    },
    /// A blob callis connection was closed.
    BlobCallisDisconnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Identifier of the blob callis.
        callis_id: CallisId,
        /// Disconnect reason.
        reason: DisconnectReason,
    },
    /// A peer session was restarted.
    PeerSessionRestartedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Restart reason.
        reason: RestartReason,
    },
    /// Backpressure was applied to a taberna's inbound queue.
    BackpressureTriggeredEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer whose traffic triggered backpressure.
        peer: DomusAddr,
        /// Taberna whose queue is under pressure.
        taberna_id: TabernaId,
    },
    /// The domus configuration was reloaded.
    ConfigReloadedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
    },
    /// The mTLS authentication material was reloaded.
    AuthReloadedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
    },
    /// Graceful shutdown began.
    ShutdownStartedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
    },
    /// Graceful shutdown completed.
    ShutdownCompleteEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
    },
}

/// Cumulative counters for a [`crate::Domus`], obtained via
/// [`DomusReporting::snapshot`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomusMetrics {
    /// Currently connected peers.
    pub current_peers: u64,
    /// Currently active primary callises.
    pub current_primary_callis: u64,
    /// Currently active blob callises.
    pub current_blob_callis: u64,
    /// Peak observed concurrent peers.
    pub peak_peers: u64,
    /// Peak observed concurrent primary callises.
    pub peak_primary_callis: u64,
    /// Peak observed concurrent blob callises.
    pub peak_blob_callis: u64,
    /// Total primary callises opened over the domus lifetime.
    pub total_primary_opened: u64,
    /// Total primary callises closed over the domus lifetime.
    pub total_primary_closed: u64,
    /// Total blob callises opened over the domus lifetime.
    pub total_blob_opened: u64,
    /// Total blob callises closed over the domus lifetime.
    pub total_blob_closed: u64,
    /// Total outbound dial attempts.
    pub total_dial_attempts: u64,
    /// Total outbound dial failures.
    pub total_dial_failures: u64,
    /// Total identity-mismatch rejections during handshake.
    pub total_identity_mismatch: u64,
    /// Total protocol-violation events.
    pub total_protocol_violation: u64,
    /// Wall-clock time the metrics state was created.
    pub created_at: SystemTime,
    /// Wall-clock time of the most recent snapshot.
    pub last_snapshot_at: SystemTime,
}

impl Default for DomusMetrics {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            current_peers: 0,
            current_primary_callis: 0,
            current_blob_callis: 0,
            peak_peers: 0,
            peak_primary_callis: 0,
            peak_blob_callis: 0,
            total_primary_opened: 0,
            total_primary_closed: 0,
            total_blob_opened: 0,
            total_blob_closed: 0,
            total_dial_attempts: 0,
            total_dial_failures: 0,
            total_identity_mismatch: 0,
            total_protocol_violation: 0,
            created_at: now,
            last_snapshot_at: now,
        }
    }
}

/// Counter deltas since the previous snapshot, obtained via
/// [`DomusReporting::snapshot_and_reset`]. Cumulative counters report the
/// increment over the prior snapshot interval; gauges (`current_*`,
/// `peak_*` over the interval) report current values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomusMetricsDelta {
    /// Currently connected peers.
    pub current_peers: u64,
    /// Currently active primary callises.
    pub current_primary_callis: u64,
    /// Currently active blob callises.
    pub current_blob_callis: u64,
    /// Peak peers observed during the interval.
    pub peak_peers: u64,
    /// Peak primary callises observed during the interval.
    pub peak_primary_callis: u64,
    /// Peak blob callises observed during the interval.
    pub peak_blob_callis: u64,
    /// Primary callises opened during the interval.
    pub total_primary_opened: u64,
    /// Primary callises closed during the interval.
    pub total_primary_closed: u64,
    /// Blob callises opened during the interval.
    pub total_blob_opened: u64,
    /// Blob callises closed during the interval.
    pub total_blob_closed: u64,
    /// Dial attempts during the interval.
    pub total_dial_attempts: u64,
    /// Dial failures during the interval.
    pub total_dial_failures: u64,
    /// Identity-mismatch rejections during the interval.
    pub total_identity_mismatch: u64,
    /// Protocol-violation events during the interval.
    pub total_protocol_violation: u64,
    /// Wall-clock time the previous snapshot was taken.
    pub created_at: SystemTime,
    /// Wall-clock time this snapshot was taken.
    pub last_snapshot_at: SystemTime,
}

impl Default for DomusMetricsDelta {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            current_peers: 0,
            current_primary_callis: 0,
            current_blob_callis: 0,
            peak_peers: 0,
            peak_primary_callis: 0,
            peak_blob_callis: 0,
            total_primary_opened: 0,
            total_primary_closed: 0,
            total_blob_opened: 0,
            total_blob_closed: 0,
            total_dial_attempts: 0,
            total_dial_failures: 0,
            total_identity_mismatch: 0,
            total_protocol_violation: 0,
            created_at: now,
            last_snapshot_at: now,
        }
    }
}

/// Observability handle obtained from [`crate::Domus::reporting`]. Provides
/// snapshot access to metrics, enumeration of connected peers, and pub-sub
/// access to the live event and error streams.
#[derive(Clone)]
pub struct DomusReporting {
    store: Arc<ObservabilityStore>,
}

impl DomusReporting {
    /// Returns a cumulative metrics snapshot.
    pub async fn snapshot(&self) -> DomusMetrics {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .store
            .tx
            .send(ObservabilityCommand::Snapshot { reply: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the metrics delta since the previous call and resets counters.
    pub async fn snapshot_and_reset(&self) -> DomusMetricsDelta {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .store
            .tx
            .send(ObservabilityCommand::SnapshotAndReset { reply: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns the addresses of peers currently connected on at least one callis.
    pub async fn connected_peer_identities(&self) -> Vec<DomusAddr> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .store
            .tx
            .send(ObservabilityCommand::ConnectedPeerIdentities { reply: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Returns identity reports for currently connected peers.
    pub async fn connected_peers(&self) -> Vec<PeerIdentityReport> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .store
            .tx
            .send(ObservabilityCommand::ConnectedPeers { reply: tx })
            .await;
        rx.await.unwrap_or_default()
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
    pub async fn errors_since(&self, seq: u64, limit: usize) -> Vec<(u64, AureliaError)> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .store
            .tx
            .send(ObservabilityCommand::ErrorsSince {
                seq,
                limit,
                reply: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }
}

/// Bundled live broadcast receivers for events and errors, returned by
/// `DomusBuilder::build_with_reporting` or [`DomusReporting::feeds`].
pub struct DomusReportingFeeds {
    /// Live event receiver.
    pub events: broadcast::Receiver<DomusReportingEvent>,
    /// Live error receiver, with monotonic sequence numbers.
    pub errors: broadcast::Receiver<(u64, AureliaError)>,
}

#[derive(Clone, Debug)]
pub(crate) struct ObservabilityHandle {
    tx: mpsc::Sender<ObservabilityCommand>,
}

impl ObservabilityHandle {
    pub(crate) async fn dial_attempt(&self, peer: DomusAddr, callis: CallisKind) {
        let _ = self
            .tx
            .send(ObservabilityCommand::DialAttempt { peer, callis })
            .await;
    }

    pub(crate) async fn dial_failed(&self, peer: DomusAddr, callis: CallisKind, error_id: ErrorId) {
        let _ = self
            .tx
            .send(ObservabilityCommand::DialFailed {
                peer,
                callis,
                error_id,
            })
            .await;
    }

    pub(crate) async fn primary_connected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        fresh_session: bool,
    ) {
        let _ = self
            .tx
            .send(ObservabilityCommand::PrimaryConnected {
                peer,
                callis_id,
                fresh_session,
            })
            .await;
    }

    pub(crate) async fn blob_connected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        settings: BlobCallisSettingsReport,
    ) {
        let _ = self
            .tx
            .send(ObservabilityCommand::BlobConnected {
                peer,
                callis_id,
                settings,
            })
            .await;
    }

    pub(crate) async fn primary_disconnected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    ) {
        let _ = self
            .tx
            .send(ObservabilityCommand::PrimaryDisconnected {
                peer,
                callis_id,
                reason,
            })
            .await;
    }

    pub(crate) async fn blob_disconnected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    ) {
        let _ = self
            .tx
            .send(ObservabilityCommand::BlobDisconnected {
                peer,
                callis_id,
                reason,
            })
            .await;
    }

    pub(crate) async fn identity_mismatch(
        &self,
        peer: DomusAddr,
        expected: DomusAddr,
        authenticated: DomusAddr,
    ) {
        let _ = self
            .tx
            .send(ObservabilityCommand::IdentityMismatch {
                peer,
                expected,
                authenticated,
            })
            .await;
    }

    pub(crate) async fn protocol_violation(&self, peer: DomusAddr, error_id: ErrorId) {
        let _ = self
            .tx
            .send(ObservabilityCommand::ProtocolViolation { peer, error_id })
            .await;
    }

    pub(crate) async fn address_mismatch(&self, peer: DomusAddr, error_id: ErrorId) {
        let _ = self
            .tx
            .send(ObservabilityCommand::AddressMismatch { peer, error_id })
            .await;
    }

    pub(crate) async fn handshake_timeout(&self, peer: DomusAddr, phase: HandshakePhase) {
        let _ = self
            .tx
            .send(ObservabilityCommand::HandshakeTimeout { peer, phase })
            .await;
    }

    pub(crate) async fn listener_failure(&self, local_addr: DomusAddr, error_id: ErrorId) {
        let _ = self
            .tx
            .send(ObservabilityCommand::ListenerFailure {
                local_addr,
                error_id,
            })
            .await;
    }

    pub(crate) async fn backend_failure(&self, peer: DomusAddr, error_id: ErrorId) {
        let _ = self
            .tx
            .send(ObservabilityCommand::BackendFailure { peer, error_id })
            .await;
    }

    pub(crate) async fn auth_reloaded(&self) {
        let _ = self.tx.send(ObservabilityCommand::AuthReloaded).await;
    }

    pub(crate) async fn config_reloaded(&self) {
        let _ = self.tx.send(ObservabilityCommand::ConfigReloaded).await;
    }

    pub(crate) async fn shutdown_started(&self) {
        let _ = self.tx.send(ObservabilityCommand::ShutdownStarted).await;
    }

    pub(crate) async fn shutdown_complete(&self) {
        let _ = self.tx.send(ObservabilityCommand::ShutdownComplete).await;
    }
}

pub(crate) fn new_observability(
    runtime_handle: tokio::runtime::Handle,
) -> (DomusReporting, ObservabilityHandle) {
    new_observability_with_capacity(runtime_handle, DEFAULT_ERROR_BUFFER)
}

pub(crate) fn new_observability_with_capacity(
    runtime_handle: tokio::runtime::Handle,
    error_capacity: usize,
) -> (DomusReporting, ObservabilityHandle) {
    let error_capacity = error_capacity.max(1);
    let (tx, rx) = mpsc::channel(DEFAULT_COMMAND_BUFFER);
    let (events, _events_rx) = broadcast::channel(DEFAULT_EVENT_BUFFER);
    let (errors, _errors_rx) = broadcast::channel(DEFAULT_EVENT_BUFFER);
    let store = Arc::new(ObservabilityStore {
        tx: tx.clone(),
        events: events.clone(),
        errors: errors.clone(),
    });
    let handle = ObservabilityHandle { tx };
    let created_at = SystemTime::now();
    let state = ObservabilityState::new(created_at, error_capacity);
    runtime_handle.spawn(run_observability(rx, events, errors, state));
    (DomusReporting { store }, handle)
}

struct ObservabilityStore {
    tx: mpsc::Sender<ObservabilityCommand>,
    events: broadcast::Sender<DomusReportingEvent>,
    errors: broadcast::Sender<(u64, AureliaError)>,
}

enum ObservabilityCommand {
    DialAttempt {
        peer: DomusAddr,
        callis: CallisKind,
    },
    DialFailed {
        peer: DomusAddr,
        callis: CallisKind,
        error_id: ErrorId,
    },
    PrimaryConnected {
        peer: DomusAddr,
        callis_id: CallisId,
        fresh_session: bool,
    },
    BlobConnected {
        peer: DomusAddr,
        callis_id: CallisId,
        settings: BlobCallisSettingsReport,
    },
    PrimaryDisconnected {
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    },
    BlobDisconnected {
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    },
    IdentityMismatch {
        peer: DomusAddr,
        expected: DomusAddr,
        authenticated: DomusAddr,
    },
    ProtocolViolation {
        peer: DomusAddr,
        error_id: ErrorId,
    },
    AddressMismatch {
        peer: DomusAddr,
        error_id: ErrorId,
    },
    HandshakeTimeout {
        peer: DomusAddr,
        phase: HandshakePhase,
    },
    ListenerFailure {
        local_addr: DomusAddr,
        error_id: ErrorId,
    },
    BackendFailure {
        peer: DomusAddr,
        error_id: ErrorId,
    },
    AuthReloaded,
    ConfigReloaded,
    ShutdownStarted,
    ShutdownComplete,
    Snapshot {
        reply: oneshot::Sender<DomusMetrics>,
    },
    SnapshotAndReset {
        reply: oneshot::Sender<DomusMetricsDelta>,
    },
    ConnectedPeerIdentities {
        reply: oneshot::Sender<Vec<DomusAddr>>,
    },
    ConnectedPeers {
        reply: oneshot::Sender<Vec<PeerIdentityReport>>,
    },
    ErrorsSince {
        seq: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<(u64, AureliaError)>>,
    },
}

#[derive(Clone, Debug)]
struct PeerCounts {
    primary: u64,
    blob: u64,
}

struct MetricsState {
    current_peers: u64,
    current_primary_callis: u64,
    current_blob_callis: u64,
    peak_peers: u64,
    peak_primary_callis: u64,
    peak_blob_callis: u64,
    total_primary_opened: u64,
    total_primary_closed: u64,
    total_blob_opened: u64,
    total_blob_closed: u64,
    total_dial_attempts: u64,
    total_dial_failures: u64,
    total_identity_mismatch: u64,
    total_protocol_violation: u64,
    delta_primary_opened: u64,
    delta_primary_closed: u64,
    delta_blob_opened: u64,
    delta_blob_closed: u64,
    delta_dial_attempts: u64,
    delta_dial_failures: u64,
    delta_identity_mismatch: u64,
    delta_protocol_violation: u64,
    created_at: SystemTime,
    last_snapshot_at: SystemTime,
}

impl MetricsState {
    fn new(created_at: SystemTime) -> Self {
        Self {
            current_peers: 0,
            current_primary_callis: 0,
            current_blob_callis: 0,
            peak_peers: 0,
            peak_primary_callis: 0,
            peak_blob_callis: 0,
            total_primary_opened: 0,
            total_primary_closed: 0,
            total_blob_opened: 0,
            total_blob_closed: 0,
            total_dial_attempts: 0,
            total_dial_failures: 0,
            total_identity_mismatch: 0,
            total_protocol_violation: 0,
            delta_primary_opened: 0,
            delta_primary_closed: 0,
            delta_blob_opened: 0,
            delta_blob_closed: 0,
            delta_dial_attempts: 0,
            delta_dial_failures: 0,
            delta_identity_mismatch: 0,
            delta_protocol_violation: 0,
            created_at,
            last_snapshot_at: created_at,
        }
    }

    fn snapshot(&self) -> DomusMetrics {
        DomusMetrics {
            current_peers: self.current_peers,
            current_primary_callis: self.current_primary_callis,
            current_blob_callis: self.current_blob_callis,
            peak_peers: self.peak_peers,
            peak_primary_callis: self.peak_primary_callis,
            peak_blob_callis: self.peak_blob_callis,
            total_primary_opened: self.total_primary_opened,
            total_primary_closed: self.total_primary_closed,
            total_blob_opened: self.total_blob_opened,
            total_blob_closed: self.total_blob_closed,
            total_dial_attempts: self.total_dial_attempts,
            total_dial_failures: self.total_dial_failures,
            total_identity_mismatch: self.total_identity_mismatch,
            total_protocol_violation: self.total_protocol_violation,
            created_at: self.created_at,
            last_snapshot_at: self.last_snapshot_at,
        }
    }

    fn snapshot_and_reset(&mut self) -> DomusMetricsDelta {
        let snapshot = DomusMetricsDelta {
            current_peers: self.current_peers,
            current_primary_callis: self.current_primary_callis,
            current_blob_callis: self.current_blob_callis,
            peak_peers: self.peak_peers,
            peak_primary_callis: self.peak_primary_callis,
            peak_blob_callis: self.peak_blob_callis,
            total_primary_opened: self.delta_primary_opened,
            total_primary_closed: self.delta_primary_closed,
            total_blob_opened: self.delta_blob_opened,
            total_blob_closed: self.delta_blob_closed,
            total_dial_attempts: self.delta_dial_attempts,
            total_dial_failures: self.delta_dial_failures,
            total_identity_mismatch: self.delta_identity_mismatch,
            total_protocol_violation: self.delta_protocol_violation,
            created_at: self.created_at,
            last_snapshot_at: SystemTime::now(),
        };
        self.delta_primary_opened = 0;
        self.delta_primary_closed = 0;
        self.delta_blob_opened = 0;
        self.delta_blob_closed = 0;
        self.delta_dial_attempts = 0;
        self.delta_dial_failures = 0;
        self.delta_identity_mismatch = 0;
        self.delta_protocol_violation = 0;
        self.last_snapshot_at = snapshot.last_snapshot_at;
        snapshot
    }
}

struct ObservabilityState {
    metrics: MetricsState,
    peers: HashMap<DomusAddr, PeerCounts>,
    errors: VecDeque<(u64, AureliaError)>,
    next_seq: u64,
    error_capacity: usize,
}

impl ObservabilityState {
    fn new(created_at: SystemTime, error_capacity: usize) -> Self {
        Self {
            metrics: MetricsState::new(created_at),
            peers: HashMap::new(),
            errors: VecDeque::with_capacity(error_capacity),
            next_seq: 0,
            error_capacity,
        }
    }
}

async fn run_observability(
    mut rx: mpsc::Receiver<ObservabilityCommand>,
    events: broadcast::Sender<DomusReportingEvent>,
    errors: broadcast::Sender<(u64, AureliaError)>,
    mut state: ObservabilityState,
) {
    while let Some(command) = rx.recv().await {
        match command {
            ObservabilityCommand::DialAttempt { peer, callis } => {
                state.metrics.total_dial_attempts += 1;
                state.metrics.delta_dial_attempts += 1;
                let _ = (peer, callis);
            }
            ObservabilityCommand::DialFailed {
                peer,
                callis,
                error_id,
            } => {
                state.metrics.total_dial_failures += 1;
                state.metrics.delta_dial_failures += 1;
                let event = DomusReportingEvent::PeerDialFailedEvent {
                    at: SystemTime::now(),
                    peer,
                    callis,
                    error_id,
                };
                let _ = events.send(event);
            }
            ObservabilityCommand::PrimaryConnected {
                peer,
                callis_id,
                fresh_session,
            } => {
                let was_connected = match state.peers.get(&peer) {
                    Some(counts) => counts.primary + counts.blob > 0,
                    None => false,
                };
                let counts = state.peers.entry(peer.clone()).or_insert(PeerCounts {
                    primary: 0,
                    blob: 0,
                });
                counts.primary += 1;
                state.metrics.current_primary_callis += 1;
                state.metrics.total_primary_opened += 1;
                state.metrics.delta_primary_opened += 1;
                if state.metrics.current_primary_callis > state.metrics.peak_primary_callis {
                    state.metrics.peak_primary_callis = state.metrics.current_primary_callis;
                }
                if !was_connected {
                    state.metrics.current_peers += 1;
                    if state.metrics.current_peers > state.metrics.peak_peers {
                        state.metrics.peak_peers = state.metrics.current_peers;
                    }
                    let event = DomusReportingEvent::PeerConnectedEvent {
                        at: SystemTime::now(),
                        peer: peer.clone(),
                        callis: CallisKind::Primary,
                        fresh_session,
                    };
                    let _ = events.send(event);
                }
                let event = DomusReportingEvent::PrimaryCallisConnectedEvent {
                    at: SystemTime::now(),
                    peer: peer.clone(),
                    callis_id,
                };
                let _ = events.send(event);
                if fresh_session {
                    let event = DomusReportingEvent::PeerSessionRestartedEvent {
                        at: SystemTime::now(),
                        peer,
                        reason: RestartReason::FreshSession,
                    };
                    let _ = events.send(event);
                }
            }
            ObservabilityCommand::BlobConnected {
                peer,
                callis_id,
                settings,
            } => {
                let was_connected = match state.peers.get(&peer) {
                    Some(counts) => counts.primary + counts.blob > 0,
                    None => false,
                };
                let counts = state.peers.entry(peer.clone()).or_insert(PeerCounts {
                    primary: 0,
                    blob: 0,
                });
                counts.blob += 1;
                state.metrics.current_blob_callis += 1;
                state.metrics.total_blob_opened += 1;
                state.metrics.delta_blob_opened += 1;
                if state.metrics.current_blob_callis > state.metrics.peak_blob_callis {
                    state.metrics.peak_blob_callis = state.metrics.current_blob_callis;
                }
                if !was_connected {
                    state.metrics.current_peers += 1;
                    if state.metrics.current_peers > state.metrics.peak_peers {
                        state.metrics.peak_peers = state.metrics.current_peers;
                    }
                    let event = DomusReportingEvent::PeerConnectedEvent {
                        at: SystemTime::now(),
                        peer: peer.clone(),
                        callis: CallisKind::Blob,
                        fresh_session: false,
                    };
                    let _ = events.send(event);
                }
                let event = DomusReportingEvent::BlobCallisConnectedEvent {
                    at: SystemTime::now(),
                    peer,
                    callis_id,
                    settings,
                };
                let _ = events.send(event);
            }
            ObservabilityCommand::PrimaryDisconnected {
                peer,
                callis_id,
                reason,
            } => {
                let mut was_connected = false;
                if let Some(counts) = state.peers.get_mut(&peer) {
                    was_connected = counts.primary + counts.blob > 0;
                    if counts.primary > 0 {
                        counts.primary -= 1;
                        state.metrics.current_primary_callis =
                            state.metrics.current_primary_callis.saturating_sub(1);
                        state.metrics.total_primary_closed += 1;
                        state.metrics.delta_primary_closed += 1;
                    }
                    let event = DomusReportingEvent::PrimaryCallisDisconnectedEvent {
                        at: SystemTime::now(),
                        peer: peer.clone(),
                        callis_id,
                        reason,
                    };
                    let _ = events.send(event);
                    if counts.primary + counts.blob == 0 {
                        state.peers.remove(&peer);
                    }
                }
                if was_connected {
                    let disconnected = !state.peers.contains_key(&peer);
                    if disconnected {
                        state.metrics.current_peers = state.metrics.current_peers.saturating_sub(1);
                        let event = DomusReportingEvent::PeerDisconnectedEvent {
                            at: SystemTime::now(),
                            peer,
                            callis: CallisKind::Primary,
                            reason,
                        };
                        let _ = events.send(event);
                    }
                }
            }
            ObservabilityCommand::BlobDisconnected {
                peer,
                callis_id,
                reason,
            } => {
                let mut was_connected = false;
                if let Some(counts) = state.peers.get_mut(&peer) {
                    was_connected = counts.primary + counts.blob > 0;
                    if counts.blob > 0 {
                        counts.blob -= 1;
                        state.metrics.current_blob_callis =
                            state.metrics.current_blob_callis.saturating_sub(1);
                        state.metrics.total_blob_closed += 1;
                        state.metrics.delta_blob_closed += 1;
                    }
                    let event = DomusReportingEvent::BlobCallisDisconnectedEvent {
                        at: SystemTime::now(),
                        peer: peer.clone(),
                        callis_id,
                        reason,
                    };
                    let _ = events.send(event);
                    if counts.primary + counts.blob == 0 {
                        state.peers.remove(&peer);
                    }
                }
                if was_connected {
                    let disconnected = !state.peers.contains_key(&peer);
                    if disconnected {
                        state.metrics.current_peers = state.metrics.current_peers.saturating_sub(1);
                        let event = DomusReportingEvent::PeerDisconnectedEvent {
                            at: SystemTime::now(),
                            peer,
                            callis: CallisKind::Blob,
                            reason,
                        };
                        let _ = events.send(event);
                    }
                }
            }
            ObservabilityCommand::IdentityMismatch {
                peer,
                expected,
                authenticated,
            } => {
                state.metrics.total_identity_mismatch += 1;
                state.metrics.delta_identity_mismatch += 1;
                let error = AureliaError::with_message(
                    ErrorId::AddressMismatch,
                    format!(
                        "peer={} expected={} authenticated={}",
                        peer, expected, authenticated
                    ),
                );
                push_error(&mut state, &errors, error);
            }
            ObservabilityCommand::ProtocolViolation { peer, error_id } => {
                state.metrics.total_protocol_violation += 1;
                state.metrics.delta_protocol_violation += 1;
                let error = AureliaError::with_message(error_id, format!("peer={}", peer));
                push_error(&mut state, &errors, error);
            }
            ObservabilityCommand::AddressMismatch { peer, error_id } => {
                let error = AureliaError::with_message(error_id, format!("peer={}", peer));
                push_error(&mut state, &errors, error);
            }
            ObservabilityCommand::HandshakeTimeout { peer, phase } => {
                let error = AureliaError::with_message(
                    ErrorId::PeerUnavailable,
                    format!("peer={} phase={:?}", peer, phase),
                );
                push_error(&mut state, &errors, error);
            }
            ObservabilityCommand::ListenerFailure {
                local_addr,
                error_id,
            } => {
                let error =
                    AureliaError::with_message(error_id, format!("local_addr={}", local_addr));
                push_error(&mut state, &errors, error);
            }
            ObservabilityCommand::BackendFailure { peer, error_id } => {
                let error = AureliaError::with_message(error_id, format!("peer={}", peer));
                push_error(&mut state, &errors, error);
            }
            ObservabilityCommand::AuthReloaded => {
                let event = DomusReportingEvent::AuthReloadedEvent {
                    at: SystemTime::now(),
                };
                let _ = events.send(event);
            }
            ObservabilityCommand::ConfigReloaded => {
                let event = DomusReportingEvent::ConfigReloadedEvent {
                    at: SystemTime::now(),
                };
                let _ = events.send(event);
            }
            ObservabilityCommand::ShutdownStarted => {
                let event = DomusReportingEvent::ShutdownStartedEvent {
                    at: SystemTime::now(),
                };
                let _ = events.send(event);
            }
            ObservabilityCommand::ShutdownComplete => {
                let event = DomusReportingEvent::ShutdownCompleteEvent {
                    at: SystemTime::now(),
                };
                let _ = events.send(event);
            }
            ObservabilityCommand::Snapshot { reply } => {
                let _ = reply.send(state.metrics.snapshot());
            }
            ObservabilityCommand::SnapshotAndReset { reply } => {
                let _ = reply.send(state.metrics.snapshot_and_reset());
            }
            ObservabilityCommand::ConnectedPeerIdentities { reply } => {
                let peers = state
                    .peers
                    .iter()
                    .filter_map(|(peer, counts)| {
                        if counts.primary + counts.blob > 0 {
                            Some(peer.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let _ = reply.send(peers);
            }
            ObservabilityCommand::ConnectedPeers { reply } => {
                let peers = state
                    .peers
                    .iter()
                    .filter_map(|(peer, counts)| {
                        if counts.primary + counts.blob > 0 {
                            Some(PeerIdentityReport {
                                peer: peer.clone(),
                                primary_callis: counts.primary,
                                blob_callis: counts.blob,
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                let _ = reply.send(peers);
            }
            ObservabilityCommand::ErrorsSince { seq, limit, reply } => {
                let mut collected = Vec::new();
                if limit > 0 {
                    for err in state.errors.iter() {
                        if error_seq(err) > seq {
                            collected.push(err.clone());
                            if collected.len() >= limit {
                                break;
                            }
                        }
                    }
                }
                let _ = reply.send(collected);
            }
        }
    }
}

fn push_error(
    state: &mut ObservabilityState,
    errors: &broadcast::Sender<(u64, AureliaError)>,
    error: AureliaError,
) {
    state.next_seq = state.next_seq.saturating_add(1);
    let entry = (state.next_seq, error);
    if state.errors.len() == state.error_capacity {
        state.errors.pop_front();
    }
    state.errors.push_back(entry.clone());
    let _ = errors.send(entry);
}

fn error_seq(error: &(u64, AureliaError)) -> u64 {
    error.0
}
