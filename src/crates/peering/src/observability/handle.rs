// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use tokio::sync::mpsc;

use crate::callis::CallisKind;
use aurelia_data::DomusAddr;
use aurelia_ids::{ErrorId, MessageType};

use super::actor::ObservabilityCommand;
use super::types::{
    BlobCallisSettingsReport, CallisId, DisconnectReason, HandshakePhase, OutboundQueueTierReport,
};

#[derive(Clone, Debug)]
pub(crate) struct ObservabilityHandle {
    pub(super) tx: mpsc::Sender<ObservabilityCommand>,
}

impl ObservabilityHandle {
    #[cfg(test)]
    pub(crate) fn noop() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self { tx }
    }

    fn emit(&self, command: ObservabilityCommand) {
        let _ = self.tx.try_send(command);
    }

    pub(crate) fn dial_attempt(&self) {
        self.emit(ObservabilityCommand::DialAttempt);
    }

    pub(crate) fn dial_failed(&self, peer: DomusAddr, callis: CallisKind, error_id: ErrorId) {
        self.emit(ObservabilityCommand::DialFailed {
            peer,
            callis,
            error_id,
        });
    }

    pub(crate) fn primary_connected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        fresh_session: bool,
    ) {
        self.emit(ObservabilityCommand::PrimaryConnected {
            peer,
            callis_id,
            fresh_session,
        });
    }

    pub(crate) fn blob_connected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        settings: BlobCallisSettingsReport,
    ) {
        self.emit(ObservabilityCommand::BlobConnected {
            peer,
            callis_id,
            settings,
        });
    }

    pub(crate) fn primary_disconnected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    ) {
        self.emit(ObservabilityCommand::PrimaryDisconnected {
            peer,
            callis_id,
            reason,
        });
    }

    pub(crate) fn blob_disconnected(
        &self,
        peer: DomusAddr,
        callis_id: CallisId,
        reason: DisconnectReason,
    ) {
        self.emit(ObservabilityCommand::BlobDisconnected {
            peer,
            callis_id,
            reason,
        });
    }

    pub(crate) fn identity_mismatch(
        &self,
        peer: DomusAddr,
        expected: DomusAddr,
        authenticated: DomusAddr,
    ) {
        self.emit(ObservabilityCommand::IdentityMismatch {
            peer,
            expected,
            authenticated,
        });
    }

    pub(crate) fn protocol_violation(&self, peer: DomusAddr, error_id: ErrorId) {
        self.emit(ObservabilityCommand::ProtocolViolation { peer, error_id });
    }

    pub(crate) fn backpressure_triggered(&self, peer: DomusAddr, taberna_id: u64) {
        self.emit(ObservabilityCommand::BackpressureTriggered { peer, taberna_id });
    }

    pub(crate) fn address_mismatch(&self, peer: DomusAddr, error_id: ErrorId) {
        self.emit(ObservabilityCommand::AddressMismatch { peer, error_id });
    }

    pub(crate) fn handshake_timeout(&self, peer: DomusAddr, phase: HandshakePhase) {
        self.emit(ObservabilityCommand::HandshakeTimeout { peer, phase });
    }

    pub(crate) fn listener_failure(&self, local_addr: DomusAddr, error_id: ErrorId) {
        self.emit(ObservabilityCommand::ListenerFailure {
            local_addr,
            error_id,
        });
    }

    pub(crate) fn backend_failure(&self, peer: DomusAddr, error_id: ErrorId) {
        self.emit(ObservabilityCommand::BackendFailure { peer, error_id });
    }

    pub(crate) fn outbound_queue_overrun(
        &self,
        peer: DomusAddr,
        tier: OutboundQueueTierReport,
        limit: u64,
        msg_type: MessageType,
    ) {
        self.emit(ObservabilityCommand::OutboundQueueOverrun {
            peer,
            tier,
            limit,
            msg_type,
        });
    }

    pub(crate) fn auth_reloaded(&self) {
        self.emit(ObservabilityCommand::AuthReloaded);
    }

    pub(crate) fn config_reloaded(&self) {
        self.emit(ObservabilityCommand::ConfigReloaded);
    }

    pub(crate) fn shutdown_started(&self) {
        self.emit(ObservabilityCommand::ShutdownStarted);
    }

    pub(crate) fn shutdown_complete(&self) {
        self.emit(ObservabilityCommand::ShutdownComplete);
    }
}

#[cfg(test)]
#[path = "../tests/observability_handle.rs"]
mod tests;
