// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::time::SystemTime;

use aurelia_data::DomusAddr;
use aurelia_ids::ErrorId;

/// Stable identifier for an individual callis (a single peer connection
/// flow within a session).
pub(crate) type CallisId = u64;

/// Snapshot of blob-callis settings reported alongside connection events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobCallisSettingsReport {
    /// Effective blob chunk size in bytes.
    pub chunk_size: u32,
    /// Effective blob acknowledgment window in chunks.
    pub ack_window_chunks: u32,
}

/// Identity report for a connected peer, used when enumerating peers via
/// [`crate::observability::DomusReporting::connected_peers`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerIdentityReport {
    /// Address of the connected peer.
    pub peer: DomusAddr,
    /// Number of active primary callis for the peer.
    pub primary_callis_count: u64,
    /// Number of active blob callis for the peer.
    pub blob_callis_count: u64,
}

/// Reason a peer connection was closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DisconnectReason {
    /// The local domus initiated the close.
    LocalRequest,
    /// The remote peer cleanly closed the connection.
    RemoteClosed,
    /// The underlying transport connection dropped.
    ConnectionClosed,
    /// The peer restarted; sessions on the previous instance are gone.
    PeerRestarted,
    /// The local domus is shutting down.
    Shutdown,
}

pub(crate) fn disconnect_reason_label(reason: DisconnectReason) -> &'static str {
    match reason {
        DisconnectReason::LocalRequest => "local-request",
        DisconnectReason::RemoteClosed => "remote-closed",
        DisconnectReason::ConnectionClosed => "connection-closed",
        DisconnectReason::PeerRestarted => "peer-restarted",
        DisconnectReason::Shutdown => "shutdown",
    }
}

/// Reason a peer session was restarted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RestartReason {
    /// The peer presented a fresh session identifier.
    FreshSession,
}

pub(crate) fn restart_reason_label(reason: RestartReason) -> &'static str {
    match reason {
        RestartReason::FreshSession => "fresh-session",
    }
}

/// Phase a handshake reached when something occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HandshakePhase {
    /// Outbound primary callis hello phase.
    OutboundPrimary,
    /// Outbound blob callis hello phase.
    OutboundBlob,
    /// Inbound hello reception phase.
    Inbound,
}

pub(crate) fn handshake_phase_label(phase: HandshakePhase) -> &'static str {
    match phase {
        HandshakePhase::OutboundPrimary => "outbound-primary-hello",
        HandshakePhase::OutboundBlob => "outbound-blob-hello",
        HandshakePhase::Inbound => "inbound-hello",
    }
}

/// Primary outbound retained lane tier reported for admission overruns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboundQueueTierReport {
    /// A1 transport-control retained lane.
    A1,
    /// A2 Aurelia-service retained lane.
    A2,
    /// A3 application retained lane.
    A3,
}

pub(crate) fn outbound_queue_tier_label(tier: OutboundQueueTierReport) -> &'static str {
    match tier {
        OutboundQueueTierReport::A1 => "a1",
        OutboundQueueTierReport::A2 => "a2",
        OutboundQueueTierReport::A3 => "a3",
    }
}

/// Event surfaced on the [`crate::observability::DomusReporting`] event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomusReportingEvent {
    /// A peer became reachable.
    PeerConnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// `true` if this is a freshly negotiated session.
        fresh_session: bool,
    },
    /// A peer disconnected.
    PeerDisconnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Disconnect reason as a stable lower-kebab label.
        reason: &'static str,
    },
    /// A dial attempt to a peer failed.
    PeerDialFailedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the dialled peer.
        peer: DomusAddr,
        /// Which callis kind was being dialled (`"primary"` or `"blob"`).
        callis: &'static str,
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
        callis_id: u64,
    },
    /// A primary callis connection was closed.
    PrimaryCallisDisconnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Identifier of the primary callis.
        callis_id: u64,
        /// Disconnect reason as a stable lower-kebab label.
        reason: &'static str,
    },
    /// A blob callis connection was established.
    BlobCallisConnectedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Identifier of the blob callis.
        callis_id: u64,
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
        callis_id: u64,
        /// Disconnect reason as a stable lower-kebab label.
        reason: &'static str,
    },
    /// A peer session was restarted.
    PeerSessionRestartedEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer.
        peer: DomusAddr,
        /// Restart reason as a stable lower-kebab label.
        reason: &'static str,
    },
    /// Backpressure was applied to a taberna's inbound queue.
    BackpressureTriggeredEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer whose traffic triggered backpressure.
        peer: DomusAddr,
        /// Taberna whose queue is under pressure.
        taberna_id: u64,
    },
    /// An outbound retained lane rejected admission because its tier was full.
    OutboundQueueOverrunEvent {
        /// Wall-clock time of the event.
        at: SystemTime,
        /// Address of the peer whose retained lane was full.
        peer: DomusAddr,
        /// Queue tier that rejected admission (`"a1"`, `"a2"`, or `"a3"`).
        tier: &'static str,
        /// Effective queue limit at the time of rejection.
        limit: u64,
        /// Message type being admitted.
        msg_type: u32,
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
/// [`crate::DomusReporting::snapshot`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomusMetrics {
    /// Currently connected peers.
    pub current_peers: u64,
    /// Currently active primary calles.
    pub current_primary_callis: u64,
    /// Currently active blob calles.
    pub current_blob_callis: u64,
    /// Peak observed concurrent peers.
    pub peak_peers: u64,
    /// Peak observed concurrent primary calles.
    pub peak_primary_callis: u64,
    /// Peak observed concurrent blob calles.
    pub peak_blob_callis: u64,
    /// Total primary calles opened over the domus lifetime.
    pub total_primary_opened: u64,
    /// Total primary calles closed over the domus lifetime.
    pub total_primary_closed: u64,
    /// Total blob calles opened over the domus lifetime.
    pub total_blob_opened: u64,
    /// Total blob calles closed over the domus lifetime.
    pub total_blob_closed: u64,
    /// Total outbound dial attempts.
    pub total_dial_attempts: u64,
    /// Total outbound dial failures.
    pub total_dial_failures: u64,
    /// Total identity-mismatch rejections during handshake.
    pub total_identity_mismatch: u64,
    /// Total protocol-violation events.
    pub total_protocol_violation: u64,
    /// Total outbound retained-lane overruns.
    pub total_outbound_queue_overruns: u64,
    /// Total A1 retained-lane overruns.
    pub total_a1_queue_overruns: u64,
    /// Total A2 retained-lane overruns.
    pub total_a2_queue_overruns: u64,
    /// Total A3 retained-lane overruns.
    pub total_a3_queue_overruns: u64,
    /// Wall-clock time the metrics state was created.
    pub created_at: SystemTime,
    /// Wall-clock time of the most recent reset boundary.
    pub last_reset_at: SystemTime,
    /// Wall-clock time this snapshot was produced.
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
            total_outbound_queue_overruns: 0,
            total_a1_queue_overruns: 0,
            total_a2_queue_overruns: 0,
            total_a3_queue_overruns: 0,
            created_at: now,
            last_reset_at: now,
            last_snapshot_at: now,
        }
    }
}

/// Counter deltas since the previous reset, obtained via
/// [`crate::DomusReporting::snapshot_and_reset`]. Cumulative counters report the
/// increment over the prior reset interval; gauges (`current_*`,
/// `peak_*` over the interval) report current values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomusMetricsDelta {
    /// Currently connected peers.
    pub current_peers: u64,
    /// Currently active primary calles.
    pub current_primary_callis: u64,
    /// Currently active blob calles.
    pub current_blob_callis: u64,
    /// Peak peers observed during the interval.
    pub peak_peers: u64,
    /// Peak primary calles observed during the interval.
    pub peak_primary_callis: u64,
    /// Peak blob calles observed during the interval.
    pub peak_blob_callis: u64,
    /// Primary calles opened during the interval.
    pub total_primary_opened: u64,
    /// Primary calles closed during the interval.
    pub total_primary_closed: u64,
    /// Blob calles opened during the interval.
    pub total_blob_opened: u64,
    /// Blob calles closed during the interval.
    pub total_blob_closed: u64,
    /// Dial attempts during the interval.
    pub total_dial_attempts: u64,
    /// Dial failures during the interval.
    pub total_dial_failures: u64,
    /// Identity-mismatch rejections during the interval.
    pub total_identity_mismatch: u64,
    /// Protocol-violation events during the interval.
    pub total_protocol_violation: u64,
    /// Outbound retained-lane overruns during the interval.
    pub total_outbound_queue_overruns: u64,
    /// A1 retained-lane overruns during the interval.
    pub total_a1_queue_overruns: u64,
    /// A2 retained-lane overruns during the interval.
    pub total_a2_queue_overruns: u64,
    /// A3 retained-lane overruns during the interval.
    pub total_a3_queue_overruns: u64,
    /// Wall-clock time the metrics state was created.
    pub created_at: SystemTime,
    /// Wall-clock time of the reset boundary this delta starts from.
    pub last_reset_at: SystemTime,
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
            total_outbound_queue_overruns: 0,
            total_a1_queue_overruns: 0,
            total_a2_queue_overruns: 0,
            total_a3_queue_overruns: 0,
            created_at: now,
            last_reset_at: now,
            last_snapshot_at: now,
        }
    }
}
