// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::callis::{callis_kind_label, CallisKind};
use aurelia_data::DomusAddr;
use aurelia_ids::{AureliaError, ErrorId, MessageType};

use super::types::{
    disconnect_reason_label, handshake_phase_label, outbound_queue_tier_label,
    restart_reason_label, BlobCallisSettingsReport, CallisId, DisconnectReason, DomusMetrics,
    DomusMetricsDelta, DomusReportingEvent, HandshakePhase, OutboundQueueTierReport,
    PeerIdentityReport, RestartReason,
};

pub(super) struct ObservabilityStore {
    pub(super) tx: mpsc::Sender<ObservabilityCommand>,
    pub(super) events: broadcast::Sender<DomusReportingEvent>,
    pub(super) errors: broadcast::Sender<(u64, AureliaError)>,
}

pub(super) enum ObservabilityCommand {
    DialAttempt,
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
    BackpressureTriggered {
        peer: DomusAddr,
        taberna_id: u64,
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
    OutboundQueueOverrun {
        peer: DomusAddr,
        tier: OutboundQueueTierReport,
        limit: u64,
        msg_type: MessageType,
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

#[derive(Clone, Copy, Debug, Default)]
struct MetricsCounters {
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
    total_outbound_queue_overruns: u64,
    total_a1_queue_overruns: u64,
    total_a2_queue_overruns: u64,
    total_a3_queue_overruns: u64,
}

struct MetricsState {
    totals: MetricsCounters,
    baseline: MetricsCounters,
    interval_peak_peers: u64,
    interval_peak_primary_callis: u64,
    interval_peak_blob_callis: u64,
    created_at: SystemTime,
    last_reset_at: SystemTime,
}

impl MetricsState {
    fn new(created_at: SystemTime) -> Self {
        Self {
            totals: MetricsCounters::default(),
            baseline: MetricsCounters::default(),
            interval_peak_peers: 0,
            interval_peak_primary_callis: 0,
            interval_peak_blob_callis: 0,
            created_at,
            last_reset_at: created_at,
        }
    }

    fn snapshot(&self) -> DomusMetrics {
        let now = SystemTime::now();
        DomusMetrics {
            current_peers: self.totals.current_peers,
            current_primary_callis: self.totals.current_primary_callis,
            current_blob_callis: self.totals.current_blob_callis,
            peak_peers: self.totals.peak_peers,
            peak_primary_callis: self.totals.peak_primary_callis,
            peak_blob_callis: self.totals.peak_blob_callis,
            total_primary_opened: self.totals.total_primary_opened,
            total_primary_closed: self.totals.total_primary_closed,
            total_blob_opened: self.totals.total_blob_opened,
            total_blob_closed: self.totals.total_blob_closed,
            total_dial_attempts: self.totals.total_dial_attempts,
            total_dial_failures: self.totals.total_dial_failures,
            total_identity_mismatch: self.totals.total_identity_mismatch,
            total_protocol_violation: self.totals.total_protocol_violation,
            total_outbound_queue_overruns: self.totals.total_outbound_queue_overruns,
            total_a1_queue_overruns: self.totals.total_a1_queue_overruns,
            total_a2_queue_overruns: self.totals.total_a2_queue_overruns,
            total_a3_queue_overruns: self.totals.total_a3_queue_overruns,
            created_at: self.created_at,
            last_reset_at: self.last_reset_at,
            last_snapshot_at: now,
        }
    }

    fn snapshot_and_reset(&mut self) -> DomusMetricsDelta {
        let now = SystemTime::now();
        let snapshot = DomusMetricsDelta {
            current_peers: self.totals.current_peers,
            current_primary_callis: self.totals.current_primary_callis,
            current_blob_callis: self.totals.current_blob_callis,
            peak_peers: self.interval_peak_peers,
            peak_primary_callis: self.interval_peak_primary_callis,
            peak_blob_callis: self.interval_peak_blob_callis,
            total_primary_opened: self
                .totals
                .total_primary_opened
                .saturating_sub(self.baseline.total_primary_opened),
            total_primary_closed: self
                .totals
                .total_primary_closed
                .saturating_sub(self.baseline.total_primary_closed),
            total_blob_opened: self
                .totals
                .total_blob_opened
                .saturating_sub(self.baseline.total_blob_opened),
            total_blob_closed: self
                .totals
                .total_blob_closed
                .saturating_sub(self.baseline.total_blob_closed),
            total_dial_attempts: self
                .totals
                .total_dial_attempts
                .saturating_sub(self.baseline.total_dial_attempts),
            total_dial_failures: self
                .totals
                .total_dial_failures
                .saturating_sub(self.baseline.total_dial_failures),
            total_identity_mismatch: self
                .totals
                .total_identity_mismatch
                .saturating_sub(self.baseline.total_identity_mismatch),
            total_protocol_violation: self
                .totals
                .total_protocol_violation
                .saturating_sub(self.baseline.total_protocol_violation),
            total_outbound_queue_overruns: self
                .totals
                .total_outbound_queue_overruns
                .saturating_sub(self.baseline.total_outbound_queue_overruns),
            total_a1_queue_overruns: self
                .totals
                .total_a1_queue_overruns
                .saturating_sub(self.baseline.total_a1_queue_overruns),
            total_a2_queue_overruns: self
                .totals
                .total_a2_queue_overruns
                .saturating_sub(self.baseline.total_a2_queue_overruns),
            total_a3_queue_overruns: self
                .totals
                .total_a3_queue_overruns
                .saturating_sub(self.baseline.total_a3_queue_overruns),
            created_at: self.created_at,
            last_reset_at: self.last_reset_at,
            last_snapshot_at: now,
        };
        self.baseline = self.totals;
        self.interval_peak_peers = self.totals.current_peers;
        self.interval_peak_primary_callis = self.totals.current_primary_callis;
        self.interval_peak_blob_callis = self.totals.current_blob_callis;
        self.last_reset_at = now;
        snapshot
    }

    fn record_dial_attempt(&mut self) {
        self.totals.total_dial_attempts += 1;
    }

    fn record_dial_failure(&mut self) {
        self.totals.total_dial_failures += 1;
    }

    fn record_identity_mismatch(&mut self) {
        self.totals.total_identity_mismatch += 1;
    }

    fn record_protocol_violation(&mut self) {
        self.totals.total_protocol_violation += 1;
    }

    fn record_outbound_queue_overrun(&mut self, tier: OutboundQueueTierReport) {
        self.totals.total_outbound_queue_overruns += 1;
        match tier {
            OutboundQueueTierReport::A1 => self.totals.total_a1_queue_overruns += 1,
            OutboundQueueTierReport::A2 => self.totals.total_a2_queue_overruns += 1,
            OutboundQueueTierReport::A3 => self.totals.total_a3_queue_overruns += 1,
        }
    }

    fn peer_connected(&mut self) {
        self.totals.current_peers += 1;
        self.totals.peak_peers = self.totals.peak_peers.max(self.totals.current_peers);
        self.interval_peak_peers = self.interval_peak_peers.max(self.totals.current_peers);
    }

    fn peer_disconnected(&mut self) {
        self.totals.current_peers = self.totals.current_peers.saturating_sub(1);
    }

    fn primary_connected(&mut self) {
        self.totals.current_primary_callis += 1;
        self.totals.total_primary_opened += 1;
        self.totals.peak_primary_callis = self
            .totals
            .peak_primary_callis
            .max(self.totals.current_primary_callis);
        self.interval_peak_primary_callis = self
            .interval_peak_primary_callis
            .max(self.totals.current_primary_callis);
    }

    fn blob_connected(&mut self) {
        self.totals.current_blob_callis += 1;
        self.totals.total_blob_opened += 1;
        self.totals.peak_blob_callis = self
            .totals
            .peak_blob_callis
            .max(self.totals.current_blob_callis);
        self.interval_peak_blob_callis = self
            .interval_peak_blob_callis
            .max(self.totals.current_blob_callis);
    }

    fn primary_disconnected(&mut self) {
        self.totals.current_primary_callis = self.totals.current_primary_callis.saturating_sub(1);
        self.totals.total_primary_closed += 1;
    }

    fn blob_disconnected(&mut self) {
        self.totals.current_blob_callis = self.totals.current_blob_callis.saturating_sub(1);
        self.totals.total_blob_closed += 1;
    }
}

pub(super) struct ObservabilityState {
    metrics: MetricsState,
    peers: HashMap<DomusAddr, PeerCounts>,
    errors: VecDeque<(u64, AureliaError)>,
    next_seq: u64,
    error_capacity: usize,
}

impl ObservabilityState {
    pub(super) fn new(created_at: SystemTime, error_capacity: usize) -> Self {
        Self {
            metrics: MetricsState::new(created_at),
            peers: HashMap::new(),
            errors: VecDeque::with_capacity(error_capacity),
            next_seq: 0,
            error_capacity,
        }
    }
}

pub(super) async fn run_observability(
    mut rx: mpsc::Receiver<ObservabilityCommand>,
    events: broadcast::Sender<DomusReportingEvent>,
    errors: broadcast::Sender<(u64, AureliaError)>,
    mut state: ObservabilityState,
) {
    while let Some(command) = rx.recv().await {
        let effects = reduce_observability_command(&mut state, command);
        for effect in effects {
            execute_observability_effect(effect, &events, &errors);
        }
    }
}

enum ObservabilityEffect {
    Event(DomusReportingEvent),
    Error((u64, AureliaError)),
    SnapshotReply(oneshot::Sender<DomusMetrics>, DomusMetrics),
    DeltaReply(oneshot::Sender<DomusMetricsDelta>, DomusMetricsDelta),
    IdentitiesReply(oneshot::Sender<Vec<DomusAddr>>, Vec<DomusAddr>),
    PeersReply(
        oneshot::Sender<Vec<PeerIdentityReport>>,
        Vec<PeerIdentityReport>,
    ),
    ErrorsReply(
        oneshot::Sender<Vec<(u64, AureliaError)>>,
        Vec<(u64, AureliaError)>,
    ),
}

fn reduce_observability_command(
    state: &mut ObservabilityState,
    command: ObservabilityCommand,
) -> Vec<ObservabilityEffect> {
    let mut effects = Vec::new();
    match command {
        ObservabilityCommand::DialAttempt => {
            state.metrics.record_dial_attempt();
        }
        ObservabilityCommand::DialFailed {
            peer,
            callis,
            error_id,
        } => {
            state.metrics.record_dial_failure();
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::PeerDialFailedEvent {
                    at: SystemTime::now(),
                    peer,
                    callis: callis_kind_label(callis),
                    error_id,
                },
            ));
        }
        ObservabilityCommand::PrimaryConnected {
            peer,
            callis_id,
            fresh_session,
        } => reduce_connected(
            state,
            &mut effects,
            peer,
            CallisKind::Primary,
            callis_id,
            Some(fresh_session),
            None,
        ),
        ObservabilityCommand::BlobConnected {
            peer,
            callis_id,
            settings,
        } => reduce_connected(
            state,
            &mut effects,
            peer,
            CallisKind::Blob,
            callis_id,
            None,
            Some(settings),
        ),
        ObservabilityCommand::PrimaryDisconnected {
            peer,
            callis_id,
            reason,
        } => reduce_disconnected(
            state,
            &mut effects,
            peer,
            CallisKind::Primary,
            callis_id,
            reason,
        ),
        ObservabilityCommand::BlobDisconnected {
            peer,
            callis_id,
            reason,
        } => reduce_disconnected(
            state,
            &mut effects,
            peer,
            CallisKind::Blob,
            callis_id,
            reason,
        ),
        ObservabilityCommand::IdentityMismatch {
            peer,
            expected,
            authenticated,
        } => {
            state.metrics.record_identity_mismatch();
            let error = AureliaError::with_message(
                ErrorId::AddressMismatch,
                format!(
                    "peer={} expected={} authenticated={}",
                    peer, expected, authenticated
                ),
            );
            effects.push(ObservabilityEffect::Error(record_error(state, error)));
        }
        ObservabilityCommand::ProtocolViolation { peer, error_id } => {
            state.metrics.record_protocol_violation();
            let error = AureliaError::with_message(error_id, format!("peer={}", peer));
            effects.push(ObservabilityEffect::Error(record_error(state, error)));
        }
        ObservabilityCommand::BackpressureTriggered { peer, taberna_id } => {
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::BackpressureTriggeredEvent {
                    at: SystemTime::now(),
                    peer,
                    taberna_id,
                },
            ));
        }
        ObservabilityCommand::AddressMismatch { peer, error_id } => {
            let error = AureliaError::with_message(error_id, format!("peer={}", peer));
            effects.push(ObservabilityEffect::Error(record_error(state, error)));
        }
        ObservabilityCommand::HandshakeTimeout { peer, phase } => {
            let error = AureliaError::with_message(
                ErrorId::PeerUnavailable,
                format!("peer={} phase={}", peer, handshake_phase_label(phase)),
            );
            effects.push(ObservabilityEffect::Error(record_error(state, error)));
        }
        ObservabilityCommand::ListenerFailure {
            local_addr,
            error_id,
        } => {
            let error = AureliaError::with_message(error_id, format!("local_addr={}", local_addr));
            effects.push(ObservabilityEffect::Error(record_error(state, error)));
        }
        ObservabilityCommand::BackendFailure { peer, error_id } => {
            let error = AureliaError::with_message(error_id, format!("peer={}", peer));
            effects.push(ObservabilityEffect::Error(record_error(state, error)));
        }
        ObservabilityCommand::OutboundQueueOverrun {
            peer,
            tier,
            limit,
            msg_type,
        } => {
            state.metrics.record_outbound_queue_overrun(tier);
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::OutboundQueueOverrunEvent {
                    at: SystemTime::now(),
                    peer,
                    tier: outbound_queue_tier_label(tier),
                    limit,
                    msg_type,
                },
            ));
        }
        ObservabilityCommand::AuthReloaded => {
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::AuthReloadedEvent {
                    at: SystemTime::now(),
                },
            ));
        }
        ObservabilityCommand::ConfigReloaded => {
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::ConfigReloadedEvent {
                    at: SystemTime::now(),
                },
            ));
        }
        ObservabilityCommand::ShutdownStarted => {
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::ShutdownStartedEvent {
                    at: SystemTime::now(),
                },
            ));
        }
        ObservabilityCommand::ShutdownComplete => {
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::ShutdownCompleteEvent {
                    at: SystemTime::now(),
                },
            ));
        }
        ObservabilityCommand::Snapshot { reply } => {
            effects.push(ObservabilityEffect::SnapshotReply(
                reply,
                state.metrics.snapshot(),
            ));
        }
        ObservabilityCommand::SnapshotAndReset { reply } => {
            effects.push(ObservabilityEffect::DeltaReply(
                reply,
                state.metrics.snapshot_and_reset(),
            ));
        }
        ObservabilityCommand::ConnectedPeerIdentities { reply } => {
            effects.push(ObservabilityEffect::IdentitiesReply(
                reply,
                connected_peer_identities(state),
            ));
        }
        ObservabilityCommand::ConnectedPeers { reply } => {
            effects.push(ObservabilityEffect::PeersReply(
                reply,
                connected_peer_reports(state),
            ));
        }
        ObservabilityCommand::ErrorsSince { seq, limit, reply } => {
            effects.push(ObservabilityEffect::ErrorsReply(
                reply,
                errors_since(state, seq, limit),
            ));
        }
    }
    effects
}

fn execute_observability_effect(
    effect: ObservabilityEffect,
    events: &broadcast::Sender<DomusReportingEvent>,
    errors: &broadcast::Sender<(u64, AureliaError)>,
) {
    match effect {
        ObservabilityEffect::Event(event) => {
            let _ = events.send(event);
        }
        ObservabilityEffect::Error(error) => {
            let _ = errors.send(error);
        }
        ObservabilityEffect::SnapshotReply(reply, snapshot) => {
            let _ = reply.send(snapshot);
        }
        ObservabilityEffect::DeltaReply(reply, delta) => {
            let _ = reply.send(delta);
        }
        ObservabilityEffect::IdentitiesReply(reply, peers) => {
            let _ = reply.send(peers);
        }
        ObservabilityEffect::PeersReply(reply, peers) => {
            let _ = reply.send(peers);
        }
        ObservabilityEffect::ErrorsReply(reply, error_list) => {
            let _ = reply.send(error_list);
        }
    }
}

fn reduce_connected(
    state: &mut ObservabilityState,
    effects: &mut Vec<ObservabilityEffect>,
    peer: DomusAddr,
    callis: CallisKind,
    callis_id: CallisId,
    fresh_session: Option<bool>,
    blob_settings: Option<BlobCallisSettingsReport>,
) {
    let was_connected = state
        .peers
        .get(&peer)
        .map(|counts| counts.primary + counts.blob > 0)
        .unwrap_or(false);
    let counts = state.peers.entry(peer.clone()).or_insert(PeerCounts {
        primary: 0,
        blob: 0,
    });
    match callis {
        CallisKind::Primary => {
            counts.primary += 1;
            state.metrics.primary_connected();
        }
        CallisKind::Blob => {
            counts.blob += 1;
            state.metrics.blob_connected();
        }
    }
    if !was_connected {
        state.metrics.peer_connected();
        effects.push(ObservabilityEffect::Event(
            DomusReportingEvent::PeerConnectedEvent {
                at: SystemTime::now(),
                peer: peer.clone(),
                fresh_session: fresh_session.unwrap_or(false),
            },
        ));
    }
    match callis {
        CallisKind::Primary => {
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::PrimaryCallisConnectedEvent {
                    at: SystemTime::now(),
                    peer: peer.clone(),
                    callis_id,
                },
            ));
            if fresh_session.unwrap_or(false) {
                effects.push(ObservabilityEffect::Event(
                    DomusReportingEvent::PeerSessionRestartedEvent {
                        at: SystemTime::now(),
                        peer,
                        reason: restart_reason_label(RestartReason::FreshSession),
                    },
                ));
            }
        }
        CallisKind::Blob => {
            let Some(settings) = blob_settings else {
                return;
            };
            effects.push(ObservabilityEffect::Event(
                DomusReportingEvent::BlobCallisConnectedEvent {
                    at: SystemTime::now(),
                    peer,
                    callis_id,
                    settings,
                },
            ));
        }
    }
}

fn reduce_disconnected(
    state: &mut ObservabilityState,
    effects: &mut Vec<ObservabilityEffect>,
    peer: DomusAddr,
    callis: CallisKind,
    callis_id: CallisId,
    reason: DisconnectReason,
) {
    let mut was_connected = false;
    if let Some(counts) = state.peers.get_mut(&peer) {
        was_connected = counts.primary + counts.blob > 0;
        match callis {
            CallisKind::Primary => {
                if counts.primary > 0 {
                    counts.primary -= 1;
                    state.metrics.primary_disconnected();
                    effects.push(ObservabilityEffect::Event(
                        DomusReportingEvent::PrimaryCallisDisconnectedEvent {
                            at: SystemTime::now(),
                            peer: peer.clone(),
                            callis_id,
                            reason: disconnect_reason_label(reason),
                        },
                    ));
                }
            }
            CallisKind::Blob => {
                if counts.blob > 0 {
                    counts.blob -= 1;
                    state.metrics.blob_disconnected();
                    effects.push(ObservabilityEffect::Event(
                        DomusReportingEvent::BlobCallisDisconnectedEvent {
                            at: SystemTime::now(),
                            peer: peer.clone(),
                            callis_id,
                            reason: disconnect_reason_label(reason),
                        },
                    ));
                }
            }
        }
        if counts.primary + counts.blob == 0 {
            state.peers.remove(&peer);
        }
    }
    if was_connected && !state.peers.contains_key(&peer) {
        state.metrics.peer_disconnected();
        effects.push(ObservabilityEffect::Event(
            DomusReportingEvent::PeerDisconnectedEvent {
                at: SystemTime::now(),
                peer,
                reason: disconnect_reason_label(reason),
            },
        ));
    }
}

fn connected_peer_identities(state: &ObservabilityState) -> Vec<DomusAddr> {
    state
        .peers
        .iter()
        .filter_map(|(peer, counts)| {
            if counts.primary + counts.blob > 0 {
                Some(peer.clone())
            } else {
                None
            }
        })
        .collect()
}

fn connected_peer_reports(state: &ObservabilityState) -> Vec<PeerIdentityReport> {
    state
        .peers
        .iter()
        .filter_map(|(peer, counts)| {
            if counts.primary + counts.blob > 0 {
                Some(PeerIdentityReport {
                    peer: peer.clone(),
                    primary_callis_count: counts.primary,
                    blob_callis_count: counts.blob,
                })
            } else {
                None
            }
        })
        .collect()
}

fn errors_since(state: &ObservabilityState, seq: u64, limit: usize) -> Vec<(u64, AureliaError)> {
    let mut collected = Vec::new();
    if limit > 0 {
        for err in state.errors.iter() {
            if err.0 > seq {
                collected.push(err.clone());
                if collected.len() >= limit {
                    break;
                }
            }
        }
    }
    collected
}

fn record_error(state: &mut ObservabilityState, error: AureliaError) -> (u64, AureliaError) {
    state.next_seq = state.next_seq.wrapping_add(1);
    let entry = (state.next_seq, error);
    if state.errors.len() == state.error_capacity {
        state.errors.pop_front();
    }
    state.errors.push_back(entry.clone());
    entry
}
