// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{broadcast, mpsc};

mod actor;
mod handle;
mod reporting;
mod types;

pub(crate) use handle::ObservabilityHandle;
pub use reporting::{DomusReporting, DomusReportingFeeds};
pub use types::{
    BlobCallisSettingsReport, DomusMetrics, DomusMetricsDelta, DomusReportingEvent,
    PeerIdentityReport,
};
pub(crate) use types::{DisconnectReason, HandshakePhase, OutboundQueueTierReport};

use actor::{run_observability, ObservabilityState, ObservabilityStore};

const DEFAULT_EVENT_BUFFER: usize = 256;
const DEFAULT_ERROR_BUFFER: usize = 256;
const DEFAULT_COMMAND_BUFFER: usize = 1024;

pub(crate) fn new_observability(
    runtime_handle: tokio::runtime::Handle,
) -> (DomusReporting, ObservabilityHandle) {
    new_observability_with_capacity(runtime_handle, DEFAULT_ERROR_BUFFER)
}

pub(crate) fn new_observability_with_capacity(
    runtime_handle: tokio::runtime::Handle,
    error_capacity: usize,
) -> (DomusReporting, ObservabilityHandle) {
    new_observability_with_capacities(runtime_handle, error_capacity, DEFAULT_COMMAND_BUFFER)
}

fn new_observability_with_capacities(
    runtime_handle: tokio::runtime::Handle,
    error_capacity: usize,
    command_capacity: usize,
) -> (DomusReporting, ObservabilityHandle) {
    let error_capacity = error_capacity.max(1);
    let command_capacity = command_capacity.max(1);
    let (tx, rx) = mpsc::channel(command_capacity);
    let (events, _events_rx) = broadcast::channel(DEFAULT_EVENT_BUFFER);
    let (errors, _errors_rx) = broadcast::channel(DEFAULT_ERROR_BUFFER);
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
