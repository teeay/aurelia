// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::address::TransportKind;
use crate::observability::ObservabilityHandle;
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_logging::limited;

pub(crate) const DEFAULT_TCP_BLOB_CHUNK_SIZE: u32 = 1200;
pub(crate) const DEFAULT_TCP_BLOB_ACK_WINDOW: u32 = 1024;
pub(crate) const DEFAULT_SOCKET_BLOB_CHUNK_SIZE: u32 = 128 * 1024;
pub(crate) const DEFAULT_SOCKET_BLOB_ACK_WINDOW: u32 = 32;

pub(crate) const MAX_SEND_QUEUE_SIZE: usize = 4096;
pub(crate) const MAX_INFLIGHT_WINDOW: usize = 1024;
pub(crate) const MAX_MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;
pub(crate) const MAX_INBOUND_HANDSHAKE_LIMIT_TOTAL: usize = 1024;
pub(crate) const MAX_INBOUND_HANDSHAKE_LIMIT_PER_PEER: usize = 64;
pub(crate) const MAX_PARALLEL_CALLIS_PER_PEER: usize = 64;
pub(crate) const MAX_BLOB_CHUNK_SIZE: u32 = 1024 * 1024;
pub(crate) const MAX_BLOB_ACK_WINDOW: u32 = 4096;
pub(crate) const MAX_BLOB_BUFFER_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub(crate) const MAX_RECONNECT_BACKOFF_STEPS: usize = 16;
pub(crate) const MAX_SEND_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const MAX_ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const MAX_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(300);
pub(crate) const MAX_LISTENER_DELAY: Duration = Duration::from_secs(60);
pub(crate) const MAX_LISTENER_RECONNECT_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const MAX_SOCKET_CALLBACK_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const MAX_SOCKET_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const MAX_TCP_CALLBACK_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const MAX_TCP_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const MAX_LIMITED_LOG_INTERVAL: Duration = Duration::from_secs(3600);
pub(crate) const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(300);
pub(crate) const MAX_TABERNA_ACCEPT_QUEUE_SIZE: usize = 1024;

/// Tunable parameters for a [`crate::Domus`]. Construct via [`DomusConfigBuilder`]
/// or start from [`DomusConfig::default`] and override individual fields.
///
/// All fields have sane defaults; values are validated and clamped at build
/// time, so a builder may return [`crate::AureliaError`] with
/// [`crate::ErrorId::InvalidConfig`] if a value falls outside its accepted range.
#[derive(Clone, Debug)]
pub struct DomusConfig {
    /// Maximum number of outbound messages buffered before sends back-pressure.
    pub send_queue_size: usize,
    /// Total time a send may take before failing with `SendTimeout`.
    pub send_timeout: Duration,
    /// Number of unacknowledged messages allowed in flight per peer.
    pub inflight_window: usize,
    /// Time to wait for a remote taberna to accept an inbound callis.
    pub accept_timeout: Duration,
    /// Capacity of the inbound queue presented to a [`crate::TabernaInbox`].
    pub taberna_accept_queue_size: usize,
    /// Interval between keepalive frames on an idle connection.
    pub keepalive_interval: Duration,
    /// Delay between reconnect attempts when no schedule entry applies.
    pub listener_delay: Duration,
    /// Time the listener waits for a peer to come back before giving up.
    pub listener_reconnect_timeout: Duration,
    /// Reconnect backoff schedule applied in order on repeated failures.
    pub reconnect_backoff: Vec<Duration>,
    /// Per-callback timeout for Unix-socket transport operations.
    pub socket_callback_timeout: Duration,
    /// Maximum time the Unix-socket handshake may take.
    pub socket_handshake_timeout: Duration,
    /// Per-callback timeout for TCP transport operations.
    pub tcp_callback_timeout: Duration,
    /// Maximum time the TCP/mTLS handshake may take.
    pub tcp_handshake_timeout: Duration,
    /// Minimum interval between repeated rate-limited log lines.
    pub limited_log_interval: Duration,
    /// Maximum concurrent inbound handshakes across all peers.
    pub inbound_handshake_limit_total: usize,
    /// Maximum concurrent inbound handshakes from a single peer.
    pub inbound_handshake_limit_per_peer: usize,
    /// Maximum concurrent callises a single peer may run.
    pub max_parallel_callis_per_peer: usize,
    /// Size of each blob chunk on the wire, in bytes.
    pub blob_chunk_size: u32,
    /// Number of unacknowledged blob chunks allowed in flight.
    pub blob_ack_window: u32,
    /// Soft cap on outbound blob buffer memory, in bytes.
    pub blob_outbound_buffer_bytes: u64,
    /// Soft cap on inbound blob buffer memory, in bytes.
    pub blob_inbound_buffer_bytes: u64,
    /// Maximum allowed application message payload length, in bytes.
    pub max_payload_len: usize,
}

impl Default for DomusConfig {
    fn default() -> Self {
        Self {
            send_queue_size: 128,
            send_timeout: Duration::from_secs(30),
            inflight_window: 16,
            accept_timeout: Duration::from_secs(5),
            taberna_accept_queue_size: 2,
            keepalive_interval: Duration::from_secs(15),
            listener_delay: Duration::from_secs(5),
            listener_reconnect_timeout: Duration::from_secs(20),
            reconnect_backoff: vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
            ],
            socket_callback_timeout: Duration::from_secs(2),
            socket_handshake_timeout: Duration::from_secs(5),
            tcp_callback_timeout: Duration::from_secs(10),
            tcp_handshake_timeout: Duration::from_secs(20),
            limited_log_interval: Duration::from_secs(120),
            inbound_handshake_limit_total: 64,
            inbound_handshake_limit_per_peer: 3,
            max_parallel_callis_per_peer: 8,
            blob_chunk_size: DEFAULT_TCP_BLOB_CHUNK_SIZE,
            blob_ack_window: DEFAULT_TCP_BLOB_ACK_WINDOW,
            blob_outbound_buffer_bytes: 256 * 1024 * 1024,
            blob_inbound_buffer_bytes: 256 * 1024 * 1024,
            max_payload_len: 8 * 1024 * 1024,
        }
    }
}

impl DomusConfig {
    /// Adjusts blob defaults to match the chosen transport when the caller
    /// has not customised them; a no-op if any blob field has been set away
    /// from its TCP default.
    pub fn apply_transport_defaults(&mut self, transport: TransportKind) {
        if transport == TransportKind::Socket
            && self.blob_chunk_size == DEFAULT_TCP_BLOB_CHUNK_SIZE
            && self.blob_ack_window == DEFAULT_TCP_BLOB_ACK_WINDOW
        {
            self.blob_chunk_size = DEFAULT_SOCKET_BLOB_CHUNK_SIZE;
            self.blob_ack_window = DEFAULT_SOCKET_BLOB_ACK_WINDOW;
        }
    }
}

#[derive(Clone, Debug)]
pub struct BlobBufferClamp {
    outbound_requested: u64,
    inbound_requested: u64,
    outbound_clamped: bool,
    inbound_clamped: bool,
    cap_bytes: u64,
    total_memory_bytes: Option<u64>,
}

impl BlobBufferClamp {
    fn new(
        outbound_requested: u64,
        inbound_requested: u64,
        cap_bytes: u64,
        total: Option<u64>,
    ) -> Self {
        Self {
            outbound_requested,
            inbound_requested,
            outbound_clamped: false,
            inbound_clamped: false,
            cap_bytes,
            total_memory_bytes: total,
        }
    }

    fn any(&self) -> bool {
        self.outbound_clamped || self.inbound_clamped
    }
}

fn config_range_error(field: &'static str, min: u64, max: u64, value: u64) -> AureliaError {
    AureliaError::with_message(
        ErrorId::InvalidConfig,
        format!("{field}: expected {min}..={max}, got {value}"),
    )
}

fn config_max_error(field: &'static str, max: u64, value: u64) -> AureliaError {
    AureliaError::with_message(
        ErrorId::InvalidConfig,
        format!("{field}: expected <= {max}, got {value}"),
    )
}

fn config_relation_error(field: &'static str, message: impl Into<String>) -> AureliaError {
    AureliaError::with_message(
        ErrorId::InvalidConfig,
        format!("{field}: {}", message.into()),
    )
}

/// Fluent builder for [`DomusConfig`]. Each setter overrides the matching
/// field and returns `Self` for chaining; [`DomusConfigBuilder::build`]
/// validates the final configuration.
#[derive(Clone, Debug)]
pub struct DomusConfigBuilder {
    config: DomusConfig,
}

impl DomusConfigBuilder {
    /// Starts a new builder seeded with [`DomusConfig::default`].
    pub fn new() -> Self {
        Self {
            config: DomusConfig::default(),
        }
    }

    /// Sets [`DomusConfig::send_queue_size`].
    pub fn send_queue_size(mut self, value: usize) -> Self {
        self.config.send_queue_size = value;
        self
    }

    /// Sets [`DomusConfig::send_timeout`].
    pub fn send_timeout(mut self, value: Duration) -> Self {
        self.config.send_timeout = value;
        self
    }

    /// Sets [`DomusConfig::inflight_window`].
    pub fn inflight_window(mut self, value: usize) -> Self {
        self.config.inflight_window = value;
        self
    }

    /// Sets [`DomusConfig::accept_timeout`].
    pub fn accept_timeout(mut self, value: Duration) -> Self {
        self.config.accept_timeout = value;
        self
    }

    /// Sets [`DomusConfig::taberna_accept_queue_size`].
    pub fn taberna_accept_queue_size(mut self, value: usize) -> Self {
        self.config.taberna_accept_queue_size = value;
        self
    }

    /// Sets [`DomusConfig::keepalive_interval`].
    pub fn keepalive_interval(mut self, value: Duration) -> Self {
        self.config.keepalive_interval = value;
        self
    }

    /// Sets [`DomusConfig::listener_delay`].
    pub fn listener_delay(mut self, value: Duration) -> Self {
        self.config.listener_delay = value;
        self
    }

    /// Sets [`DomusConfig::listener_reconnect_timeout`].
    pub fn listener_reconnect_timeout(mut self, value: Duration) -> Self {
        self.config.listener_reconnect_timeout = value;
        self
    }

    /// Sets [`DomusConfig::reconnect_backoff`].
    pub fn reconnect_backoff(mut self, schedule: Vec<Duration>) -> Self {
        self.config.reconnect_backoff = schedule;
        self
    }

    /// Sets [`DomusConfig::socket_callback_timeout`].
    pub fn socket_callback_timeout(mut self, value: Duration) -> Self {
        self.config.socket_callback_timeout = value;
        self
    }

    /// Sets [`DomusConfig::socket_handshake_timeout`].
    pub fn socket_handshake_timeout(mut self, value: Duration) -> Self {
        self.config.socket_handshake_timeout = value;
        self
    }

    /// Sets [`DomusConfig::tcp_callback_timeout`].
    pub fn tcp_callback_timeout(mut self, value: Duration) -> Self {
        self.config.tcp_callback_timeout = value;
        self
    }

    /// Sets [`DomusConfig::tcp_handshake_timeout`].
    pub fn tcp_handshake_timeout(mut self, value: Duration) -> Self {
        self.config.tcp_handshake_timeout = value;
        self
    }

    /// Sets [`DomusConfig::limited_log_interval`].
    pub fn limited_log_interval(mut self, value: Duration) -> Self {
        self.config.limited_log_interval = value;
        self
    }

    /// Sets [`DomusConfig::inbound_handshake_limit_total`].
    pub fn inbound_handshake_limit_total(mut self, value: usize) -> Self {
        self.config.inbound_handshake_limit_total = value;
        self
    }

    /// Sets [`DomusConfig::inbound_handshake_limit_per_peer`].
    pub fn inbound_handshake_limit_per_peer(mut self, value: usize) -> Self {
        self.config.inbound_handshake_limit_per_peer = value;
        self
    }

    /// Sets [`DomusConfig::max_parallel_callis_per_peer`].
    pub fn max_parallel_callis_per_peer(mut self, value: usize) -> Self {
        self.config.max_parallel_callis_per_peer = value;
        self
    }

    /// Sets [`DomusConfig::blob_chunk_size`].
    pub fn blob_chunk_size(mut self, value: u32) -> Self {
        self.config.blob_chunk_size = value;
        self
    }

    /// Sets [`DomusConfig::blob_ack_window`].
    pub fn blob_ack_window(mut self, value: u32) -> Self {
        self.config.blob_ack_window = value;
        self
    }

    /// Sets [`DomusConfig::blob_outbound_buffer_bytes`].
    pub fn blob_outbound_buffer_bytes(mut self, value: u64) -> Self {
        self.config.blob_outbound_buffer_bytes = value;
        self
    }

    /// Sets [`DomusConfig::blob_inbound_buffer_bytes`].
    pub fn blob_inbound_buffer_bytes(mut self, value: u64) -> Self {
        self.config.blob_inbound_buffer_bytes = value;
        self
    }

    /// Sets [`DomusConfig::max_payload_len`].
    pub fn max_payload_len(mut self, value: usize) -> Self {
        self.config.max_payload_len = value;
        self
    }

    /// Validates the configured values and returns a [`DomusConfig`].
    /// Returns [`AureliaError`] with [`ErrorId::InvalidConfig`] if any field
    /// is out of range or violates a cross-field invariant.
    pub fn build(self) -> Result<DomusConfig, AureliaError> {
        let (config, clamp) = normalize_domus_config(self.config, system_memory_bytes())?;
        if clamp.any() {
            warn!(
                outbound_requested = clamp.outbound_requested,
                inbound_requested = clamp.inbound_requested,
                outbound_clamped = clamp.outbound_clamped,
                inbound_clamped = clamp.inbound_clamped,
                cap_bytes = clamp.cap_bytes,
                total_memory_bytes = clamp.total_memory_bytes,
                "blob buffer config clamped"
            );
        }
        Ok(config)
    }
}

impl Default for DomusConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Live handle to a running [`crate::Domus`]'s configuration. Allows
/// taking a current snapshot and applying validated updates without
/// restarting the domus.
#[derive(Clone, Debug)]
pub struct DomusConfigAccess {
    inner: Arc<DomusConfigStore>,
    observability: Option<ObservabilityHandle>,
}

impl DomusConfigAccess {
    pub(crate) fn new(
        inner: Arc<DomusConfigStore>,
        observability: Option<ObservabilityHandle>,
    ) -> Self {
        Self {
            inner,
            observability,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_config(config: DomusConfig) -> Self {
        Self::new(Arc::new(DomusConfigStore::new(config)), None)
    }

    /// Returns a snapshot of the currently effective configuration.
    pub async fn snapshot(&self) -> DomusConfig {
        self.inner.snapshot().await
    }

    pub(crate) fn limited_registry(&self) -> Arc<limited::LimitedLogRegistry> {
        self.inner.limited_registry()
    }

    /// Validates `next` and atomically replaces the current configuration.
    /// Returns the (possibly clamped) configuration that was actually
    /// applied, or an [`AureliaError`] with [`ErrorId::InvalidConfig`] if
    /// validation fails.
    pub async fn update(&self, next: DomusConfig) -> Result<DomusConfig, AureliaError> {
        let (next, clamp) = normalize_domus_config(next, system_memory_bytes())?;
        if clamp.any() {
            warn!(
                outbound_requested = clamp.outbound_requested,
                inbound_requested = clamp.inbound_requested,
                outbound_clamped = clamp.outbound_clamped,
                inbound_clamped = clamp.inbound_clamped,
                cap_bytes = clamp.cap_bytes,
                total_memory_bytes = clamp.total_memory_bytes,
                "blob buffer config clamped"
            );
        }
        self.inner.update(next.clone()).await;
        if let Some(observability) = &self.observability {
            observability.config_reloaded().await;
        }
        Ok(next)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DomusConfigStore {
    inner: Arc<RwLock<DomusConfig>>,
    limited_registry: Arc<limited::LimitedLogRegistry>,
    limited_control: limited::LimitedLogControl,
}

impl DomusConfigStore {
    pub(crate) fn new(config: DomusConfig) -> Self {
        let limited = limited::init_limited_logging(
            limited::log_ids::LIMITED_LOG_IDS,
            config.limited_log_interval,
        );
        Self {
            inner: Arc::new(RwLock::new(config)),
            limited_registry: limited.registry(),
            limited_control: limited.control(),
        }
    }
}

impl DomusConfigStore {
    pub(crate) async fn snapshot(&self) -> DomusConfig {
        self.inner.read().await.clone()
    }

    pub(crate) fn limited_registry(&self) -> Arc<limited::LimitedLogRegistry> {
        Arc::clone(&self.limited_registry)
    }

    pub(crate) async fn update(&self, next: DomusConfig) {
        let mut guard = self.inner.write().await;
        *guard = next;
        self.limited_control
            .set_interval(guard.limited_log_interval);
        debug!(
            send_queue_size = guard.send_queue_size,
            inflight_window = guard.inflight_window,
            send_timeout = ?guard.send_timeout,
            accept_timeout = ?guard.accept_timeout,
            taberna_accept_queue_size = guard.taberna_accept_queue_size,
            keepalive_interval = ?guard.keepalive_interval,
            blob_outbound_buffer_bytes = guard.blob_outbound_buffer_bytes,
            blob_inbound_buffer_bytes = guard.blob_inbound_buffer_bytes,
            max_payload_len = guard.max_payload_len,
            tcp_callback_timeout = ?guard.tcp_callback_timeout,
            tcp_handshake_timeout = ?guard.tcp_handshake_timeout,
            limited_log_interval = ?guard.limited_log_interval,
            inbound_handshake_limit_total = guard.inbound_handshake_limit_total,
            inbound_handshake_limit_per_peer = guard.inbound_handshake_limit_per_peer,
            max_parallel_callis_per_peer = guard.max_parallel_callis_per_peer,
            "domus config updated"
        );
    }
}

fn normalize_domus_config(
    mut config: DomusConfig,
    total_memory_bytes: Option<u64>,
) -> Result<(DomusConfig, BlobBufferClamp), AureliaError> {
    let clamp = clamp_blob_buffers(&mut config, total_memory_bytes);
    validate_domus_config(&config, clamp.cap_bytes)?;
    Ok((config, clamp))
}

fn clamp_blob_buffers(
    config: &mut DomusConfig,
    total_memory_bytes: Option<u64>,
) -> BlobBufferClamp {
    let cap_bytes = blob_buffer_cap_bytes(total_memory_bytes);
    let mut clamp = BlobBufferClamp::new(
        config.blob_outbound_buffer_bytes,
        config.blob_inbound_buffer_bytes,
        cap_bytes,
        total_memory_bytes,
    );
    if config.blob_outbound_buffer_bytes > cap_bytes {
        config.blob_outbound_buffer_bytes = cap_bytes;
        clamp.outbound_clamped = true;
    }
    if config.blob_inbound_buffer_bytes > cap_bytes {
        config.blob_inbound_buffer_bytes = cap_bytes;
        clamp.inbound_clamped = true;
    }
    clamp
}

fn validate_domus_config(config: &DomusConfig, buffer_cap: u64) -> Result<(), AureliaError> {
    if config.send_queue_size == 0 || config.send_queue_size > MAX_SEND_QUEUE_SIZE {
        return Err(config_range_error(
            "send_queue_size",
            1,
            MAX_SEND_QUEUE_SIZE as u64,
            config.send_queue_size as u64,
        ));
    }
    if config.inflight_window == 0 || config.inflight_window > MAX_INFLIGHT_WINDOW {
        return Err(config_range_error(
            "inflight_window",
            1,
            MAX_INFLIGHT_WINDOW as u64,
            config.inflight_window as u64,
        ));
    }
    if config.taberna_accept_queue_size == 0
        || config.taberna_accept_queue_size > MAX_TABERNA_ACCEPT_QUEUE_SIZE
    {
        return Err(config_range_error(
            "taberna_accept_queue_size",
            1,
            MAX_TABERNA_ACCEPT_QUEUE_SIZE as u64,
            config.taberna_accept_queue_size as u64,
        ));
    }
    if config.max_payload_len == 0 || config.max_payload_len > MAX_MAX_PAYLOAD_LEN {
        return Err(config_range_error(
            "max_payload_len",
            1,
            MAX_MAX_PAYLOAD_LEN as u64,
            config.max_payload_len as u64,
        ));
    }
    if config.inbound_handshake_limit_total == 0
        || config.inbound_handshake_limit_total > MAX_INBOUND_HANDSHAKE_LIMIT_TOTAL
    {
        return Err(config_range_error(
            "inbound_handshake_limit_total",
            1,
            MAX_INBOUND_HANDSHAKE_LIMIT_TOTAL as u64,
            config.inbound_handshake_limit_total as u64,
        ));
    }
    if config.inbound_handshake_limit_per_peer == 0
        || config.inbound_handshake_limit_per_peer > MAX_INBOUND_HANDSHAKE_LIMIT_PER_PEER
    {
        return Err(config_range_error(
            "inbound_handshake_limit_per_peer",
            1,
            MAX_INBOUND_HANDSHAKE_LIMIT_PER_PEER as u64,
            config.inbound_handshake_limit_per_peer as u64,
        ));
    }
    if config.inbound_handshake_limit_per_peer > config.inbound_handshake_limit_total {
        return Err(config_relation_error(
            "inbound_handshake_limit_per_peer",
            "must be <= inbound_handshake_limit_total",
        ));
    }
    if config.max_parallel_callis_per_peer == 0
        || config.max_parallel_callis_per_peer > MAX_PARALLEL_CALLIS_PER_PEER
    {
        return Err(config_range_error(
            "max_parallel_callis_per_peer",
            1,
            MAX_PARALLEL_CALLIS_PER_PEER as u64,
            config.max_parallel_callis_per_peer as u64,
        ));
    }
    if config.blob_chunk_size == 0 || config.blob_chunk_size > MAX_BLOB_CHUNK_SIZE {
        return Err(config_range_error(
            "blob_chunk_size",
            1,
            MAX_BLOB_CHUNK_SIZE as u64,
            config.blob_chunk_size as u64,
        ));
    }
    if config.blob_ack_window == 0 || config.blob_ack_window > MAX_BLOB_ACK_WINDOW {
        return Err(config_range_error(
            "blob_ack_window",
            1,
            MAX_BLOB_ACK_WINDOW as u64,
            config.blob_ack_window as u64,
        ));
    }
    if config.blob_outbound_buffer_bytes > buffer_cap {
        return Err(config_max_error(
            "blob_outbound_buffer_bytes",
            buffer_cap,
            config.blob_outbound_buffer_bytes,
        ));
    }
    if config.blob_inbound_buffer_bytes > buffer_cap {
        return Err(config_max_error(
            "blob_inbound_buffer_bytes",
            buffer_cap,
            config.blob_inbound_buffer_bytes,
        ));
    }
    let required = (config.blob_chunk_size as u128) * (config.blob_ack_window as u128);
    if required > u64::MAX as u128 {
        return Err(config_relation_error(
            "blob_chunk_size",
            "chunk_size * ack_window overflows reservation size",
        ));
    }
    let required = required as u64;
    if config.blob_outbound_buffer_bytes < required {
        return Err(config_relation_error(
            "blob_outbound_buffer_bytes",
            format!("must be >= blob_chunk_size * blob_ack_window ({required})"),
        ));
    }
    if config.blob_inbound_buffer_bytes < required {
        return Err(config_relation_error(
            "blob_inbound_buffer_bytes",
            format!("must be >= blob_chunk_size * blob_ack_window ({required})"),
        ));
    }
    if config.send_timeout.is_zero() {
        return Err(config_relation_error("send_timeout", "must be > 0"));
    }
    if config.send_timeout > MAX_SEND_TIMEOUT {
        return Err(config_relation_error(
            "send_timeout",
            format!("must be <= {:?}", MAX_SEND_TIMEOUT),
        ));
    }
    if config.accept_timeout.is_zero() {
        return Err(config_relation_error("accept_timeout", "must be > 0"));
    }
    if config.accept_timeout > MAX_ACCEPT_TIMEOUT {
        return Err(config_relation_error(
            "accept_timeout",
            format!("must be <= {:?}", MAX_ACCEPT_TIMEOUT),
        ));
    }
    if config.accept_timeout > config.send_timeout {
        return Err(config_relation_error(
            "accept_timeout",
            "must be <= send_timeout",
        ));
    }
    if config.keepalive_interval > MAX_KEEPALIVE_INTERVAL {
        return Err(config_relation_error(
            "keepalive_interval",
            format!("must be <= {:?}", MAX_KEEPALIVE_INTERVAL),
        ));
    }
    if config.listener_delay > MAX_LISTENER_DELAY {
        return Err(config_relation_error(
            "listener_delay",
            format!("must be <= {:?}", MAX_LISTENER_DELAY),
        ));
    }
    if config.listener_reconnect_timeout > MAX_LISTENER_RECONNECT_TIMEOUT {
        return Err(config_relation_error(
            "listener_reconnect_timeout",
            format!("must be <= {:?}", MAX_LISTENER_RECONNECT_TIMEOUT),
        ));
    }
    if config.socket_callback_timeout > MAX_SOCKET_CALLBACK_TIMEOUT {
        return Err(config_relation_error(
            "socket_callback_timeout",
            format!("must be <= {:?}", MAX_SOCKET_CALLBACK_TIMEOUT),
        ));
    }
    if config.socket_handshake_timeout > MAX_SOCKET_HANDSHAKE_TIMEOUT {
        return Err(config_relation_error(
            "socket_handshake_timeout",
            format!("must be <= {:?}", MAX_SOCKET_HANDSHAKE_TIMEOUT),
        ));
    }
    if config.tcp_callback_timeout > MAX_TCP_CALLBACK_TIMEOUT {
        return Err(config_relation_error(
            "tcp_callback_timeout",
            format!("must be <= {:?}", MAX_TCP_CALLBACK_TIMEOUT),
        ));
    }
    if config.tcp_handshake_timeout > MAX_TCP_HANDSHAKE_TIMEOUT {
        return Err(config_relation_error(
            "tcp_handshake_timeout",
            format!("must be <= {:?}", MAX_TCP_HANDSHAKE_TIMEOUT),
        ));
    }
    if config.limited_log_interval > MAX_LIMITED_LOG_INTERVAL {
        return Err(config_relation_error(
            "limited_log_interval",
            format!("must be <= {:?}", MAX_LIMITED_LOG_INTERVAL),
        ));
    }
    if config.reconnect_backoff.len() > MAX_RECONNECT_BACKOFF_STEPS {
        return Err(config_relation_error(
            "reconnect_backoff",
            format!("must have at most {} entries", MAX_RECONNECT_BACKOFF_STEPS),
        ));
    }
    for (idx, value) in config.reconnect_backoff.iter().enumerate() {
        if *value > MAX_RECONNECT_BACKOFF {
            return Err(config_relation_error(
                "reconnect_backoff",
                format!("entry {idx} must be <= {:?}", MAX_RECONNECT_BACKOFF),
            ));
        }
    }
    Ok(())
}

fn blob_buffer_cap_bytes(total_memory_bytes: Option<u64>) -> u64 {
    match total_memory_bytes {
        Some(total) => MAX_BLOB_BUFFER_BYTES.min(total / 2),
        None => MAX_BLOB_BUFFER_BYTES,
    }
}

#[cfg(unix)]
fn system_memory_bytes() -> Option<u64> {
    // SAFETY: `sysconf` is called with valid constant selectors; we validate the returned values
    // are positive before use and clamp via `u128` to avoid overflow.
    unsafe {
        let pages = libc::sysconf(libc::_SC_PHYS_PAGES);
        let page_size = libc::sysconf(libc::_SC_PAGESIZE);
        if pages <= 0 || page_size <= 0 {
            return None;
        }
        let total = (pages as u128).saturating_mul(page_size as u128);
        u64::try_from(total).ok()
    }
}

#[cfg(not(unix))]
fn system_memory_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gib(value: u64) -> u64 {
        value * 1024 * 1024 * 1024
    }

    #[test]
    fn clamp_blob_buffers_to_half_memory_cap() {
        let config = DomusConfig {
            blob_outbound_buffer_bytes: gib(7),
            blob_inbound_buffer_bytes: gib(9),
            ..Default::default()
        };

        let (config, clamp) =
            normalize_domus_config(config, Some(gib(10))).expect("valid config with clamp");

        assert!(clamp.outbound_clamped);
        assert!(clamp.inbound_clamped);
        assert_eq!(clamp.cap_bytes, gib(5));
        assert_eq!(config.blob_outbound_buffer_bytes, gib(5));
        assert_eq!(config.blob_inbound_buffer_bytes, gib(5));
    }

    #[test]
    fn reject_blob_buffers_below_reservation() {
        let config = DomusConfig {
            blob_chunk_size: MAX_BLOB_CHUNK_SIZE,
            blob_ack_window: MAX_BLOB_ACK_WINDOW,
            blob_outbound_buffer_bytes: gib(1),
            blob_inbound_buffer_bytes: gib(1),
            ..Default::default()
        };

        let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
        assert!(err.to_string().contains("blob_outbound_buffer_bytes"));
    }

    #[test]
    fn reject_send_queue_size_over_max() {
        let config = DomusConfig {
            send_queue_size: MAX_SEND_QUEUE_SIZE + 1,
            ..Default::default()
        };
        let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
        assert!(err.to_string().contains("send_queue_size"));
    }

    #[test]
    fn reject_zero_send_timeout() {
        let config = DomusConfig {
            send_timeout: Duration::from_secs(0),
            ..Default::default()
        };
        let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
        assert!(err.to_string().contains("send_timeout"));
    }

    #[test]
    fn reject_zero_accept_timeout() {
        let config = DomusConfig {
            send_timeout: Duration::from_secs(1),
            accept_timeout: Duration::from_secs(0),
            ..Default::default()
        };
        let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
        assert!(err.to_string().contains("accept_timeout"));
    }
}
