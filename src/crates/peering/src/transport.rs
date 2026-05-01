// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, info, trace, warn};

use crate::address::{DomusAddr, TransportKind};
use crate::auth::DomusAuthConfig;
use crate::callis::CallisKind;
use crate::config::{DomusConfig, DomusConfigAccess};
use crate::message_id::PeerMessageIdAllocator;
use crate::observability::ObservabilityHandle;
use crate::reliability::InflightMessage;
use crate::send::{SendOptions, SendOutcome};
use crate::session::{PeerMessage, PeerSession, ReceiveOutcome, ReceiveSchedule};
use crate::taberna::TabernaRegistry;
use crate::wire::{
    BlobChunkFlags, BlobTransferChunkPayload, BlobTransferCompletePayload,
    BlobTransferStartPayload, ErrorPayload, HelloPayload, WireFlags, WireHeader, PROTOCOL_VERSION,
};
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_ids::{
    MessageType, PeerMessageId, TabernaId, MSG_ACK, MSG_BLOB_TRANSFER_CHUNK,
    MSG_BLOB_TRANSFER_COMPLETE, MSG_BLOB_TRANSFER_START, MSG_CLOSE, MSG_ERROR, MSG_HELLO,
    MSG_HELLO_RESPONSE, MSG_KEEPALIVE,
};

pub(crate) mod backend;
mod blob;
mod callis;
mod frame;
mod handshake;
mod limits;
mod listener;
mod peer;
mod pkcs8;
mod primary;
pub(crate) mod primary_dispatch;
mod socket_backend;
mod tcp_backend;
mod tls;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlobRequestOutcome {
    Ack,
    Skip,
}

enum BlobRequestSchedule {
    Immediate(BlobRequestOutcome),
    Pending(BlobAcceptPending),
}

struct BlobAcceptPending {
    pub(super) dst_taberna: TabernaId,
    pub(super) accept_rx: oneshot::Receiver<Result<(), AureliaError>>,
    pub(super) receiver_state: Arc<blob::BlobReceiverState>,
    pub(super) send_timeout: Duration,
    pub(super) peer_state_tx: mpsc::Sender<PeerStateUpdate>,
}

use aurelia_logging::limited::log_ids;
use aurelia_logging::LimitedLogRegistry;

pub(crate) use blob::blob_buffer_full_error;
use blob::{
    dispatch_blob, handle_blob_request, handle_blob_transfer_chunk, handle_blob_transfer_start,
    send_blob_control_and_wait_ack, BlobCallisSettings, BlobChunkOutcome, BlobManager,
    RetainedBlobKind,
};
#[cfg(test)]
use callis::handle_inbound_frame;
use callis::spawn_callis_task;
use frame::{read_frame, send_control_frame, send_outbound_frame};
use handshake::{accept_inbound, spawn_blob_dial_task, spawn_dial_task};
#[cfg(test)]
use handshake::{
    negotiate_blob_settings, validate_backend_identity, validate_blob_hello_request,
    validate_blob_hello_response,
};
use listener::run_listener;
use peer::{
    CallisHandle, ConnectionInfo, OutboundFrame, PeerHandle, PeerRole, PeerState,
    PeerStateSnapshot, PeerStateUpdate,
};
use primary::{
    compute_listener_delay, compute_reconnect_delay, current_dial_addr, remove_primary_handle,
    should_reconnect_primary,
};
use primary_dispatch::{run_primary_dispatcher, PrimaryDispatchQueue};

pub use backend::TransportBackend;
pub use socket_backend::SocketBackend;
pub use tcp_backend::TcpBackend;

pub trait TransportStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> TransportStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub enum TransportBackendImpl {
    Tcp(TcpBackend),
    Socket(Box<SocketBackend>),
}

impl TransportBackendImpl {
    pub async fn reload_auth(&self, auth: DomusAuthConfig) -> Result<(), AureliaError> {
        match self {
            TransportBackendImpl::Tcp(backend) => backend.reload_auth(auth).await,
            TransportBackendImpl::Socket(backend) => backend.reload_auth(auth).await,
        }
    }
}

pub enum TransportListener {
    Tcp(tokio::net::TcpListener),
    Socket(tokio::net::UnixListener),
}

#[async_trait::async_trait]
impl TransportBackend for TransportBackendImpl {
    type Addr = DomusAddr;
    type Listener = TransportListener;
    type Stream = Box<dyn TransportStream + 'static>;

    async fn bind(&self, local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        match self {
            TransportBackendImpl::Tcp(backend) => {
                let listener = backend.bind(local).await?;
                Ok(TransportListener::Tcp(listener))
            }
            TransportBackendImpl::Socket(backend) => {
                let listener = backend.bind(local).await?;
                Ok(TransportListener::Socket(listener))
            }
        }
    }

    async fn accept(
        &self,
        listener: &mut Self::Listener,
    ) -> Result<backend::AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        match (self, listener) {
            (TransportBackendImpl::Tcp(backend), TransportListener::Tcp(listener)) => {
                let authenticated = backend.accept(listener).await?;
                Ok(backend::AuthenticatedStream {
                    stream: Box::new(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
            (TransportBackendImpl::Socket(backend), TransportListener::Socket(listener)) => {
                let authenticated = backend.accept(listener).await?;
                Ok(backend::AuthenticatedStream {
                    stream: Box::new(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
            _ => Err(AureliaError::new(ErrorId::ProtocolViolation)),
        }
    }

    async fn dial(
        &self,
        peer: &Self::Addr,
    ) -> Result<backend::AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        match self {
            TransportBackendImpl::Tcp(backend) => {
                let authenticated = backend.dial(peer).await?;
                Ok(backend::AuthenticatedStream {
                    stream: Box::new(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
            TransportBackendImpl::Socket(backend) => {
                let authenticated = backend.dial(peer).await?;
                Ok(backend::AuthenticatedStream {
                    stream: Box::new(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
        }
    }
}

type CallisId = u64;

static CALLIS_ID: AtomicU64 = AtomicU64::new(1);

fn next_callis_id() -> CallisId {
    // Relaxed is sufficient: we only need unique, monotonically increasing IDs.
    CALLIS_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone)]
struct CallisTracker {
    open: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl CallisTracker {
    fn new() -> Self {
        Self {
            open: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    fn open(&self) {
        self.open.fetch_add(1, Ordering::SeqCst);
    }

    fn close(&self) {
        if self.open.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.notify.notify_waiters();
        }
    }

    async fn wait_for_zero(&self, timeout_duration: Duration) -> Result<(), AureliaError> {
        if self.open.load(Ordering::SeqCst) == 0 {
            return Ok(());
        }
        timeout(timeout_duration, async {
            loop {
                if self.open.load(Ordering::SeqCst) == 0 {
                    break;
                }
                self.notify.notified().await;
            }
        })
        .await
        .map_err(|_| {
            AureliaError::with_message(ErrorId::PeerUnavailable, "callis close timeout")
        })?;
        Ok(())
    }

    fn count(&self) -> usize {
        self.open.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
struct HandshakeGate {
    limited_registry: Arc<LimitedLogRegistry>,
}

impl HandshakeGate {
    fn new(limited_registry: Arc<LimitedLogRegistry>) -> Self {
        Self { limited_registry }
    }

    fn try_acquire(
        &self,
        config: &DomusConfig,
        peer_inflight: &Arc<AtomicUsize>,
    ) -> Option<HandshakePermit> {
        let per_peer_limit = config.inbound_handshake_limit_per_peer.max(1);

        let peer_next = peer_inflight.fetch_add(1, Ordering::SeqCst) + 1;
        if peer_next > per_peer_limit {
            aurelia_logging::info_limited!(
                self.limited_registry,
                log_ids::HANDSHAKE_PER_PEER_LIMIT,
                peer_inflight = peer_next,
                per_peer_limit,
                "inbound handshake rejected due to per-peer limit"
            );
            peer_inflight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }

        Some(HandshakePermit {
            peer_inflight: Arc::clone(peer_inflight),
        })
    }
}

struct HandshakePermit {
    peer_inflight: Arc<AtomicUsize>,
}

impl Drop for HandshakePermit {
    fn drop(&mut self) {
        self.peer_inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
pub(crate) struct BlobBufferTracker {
    outbound_used: AtomicU64,
    inbound_used: AtomicU64,
}

impl BlobBufferTracker {
    pub(crate) fn try_reserve_outbound(&self, bytes: u64, cap: u64) -> bool {
        Self::try_reserve(&self.outbound_used, bytes, cap)
    }

    pub(crate) fn release_outbound(&self, bytes: u64) {
        self.outbound_used.fetch_sub(bytes, Ordering::SeqCst);
    }

    pub(crate) fn try_reserve_inbound(&self, bytes: u64, cap: u64) -> bool {
        Self::try_reserve(&self.inbound_used, bytes, cap)
    }

    pub(crate) fn release_inbound(&self, bytes: u64) {
        self.inbound_used.fetch_sub(bytes, Ordering::SeqCst);
    }

    fn try_reserve(used: &AtomicU64, bytes: u64, cap: u64) -> bool {
        loop {
            let current = used.load(Ordering::SeqCst);
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > cap {
                return false;
            }
            if used
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }
}

pub struct Transport<B = TransportBackendImpl>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    inner: Arc<TransportInner<B>>,
}

struct TransportInner<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    registry: Arc<TabernaRegistry>,
    config: DomusConfigAccess,
    backend: Arc<B>,
    listener: Mutex<Option<B::Listener>>,
    local_addr: DomusAddr,
    transport_kind: TransportKind,
    peers: Mutex<HashMap<DomusAddr, Arc<PeerHandle<B>>>>,
    blob_buffers: Arc<BlobBufferTracker>,
    handshake_gate: HandshakeGate,
    shutdown_tx: watch::Sender<bool>,
    listener_shutdown_tx: watch::Sender<bool>,
    shutdown_notify: Notify,
    observability: ObservabilityHandle,
    runtime_handle: tokio::runtime::Handle,
}

impl<B> TransportInner<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    fn new(
        registry: Arc<TabernaRegistry>,
        config: DomusConfigAccess,
        backend: Arc<B>,
        listener: B::Listener,
        local_addr: DomusAddr,
        observability: ObservabilityHandle,
        runtime_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let limited_registry = config.limited_registry();
        let transport_kind = local_addr.kind();
        let (shutdown_tx, _rx) = watch::channel(false);
        let (listener_shutdown_tx, _listener_rx) = watch::channel(false);
        Arc::new(TransportInner {
            registry,
            config,
            backend,
            listener: Mutex::new(Some(listener)),
            local_addr,
            transport_kind,
            peers: Mutex::new(HashMap::new()),
            blob_buffers: Arc::new(BlobBufferTracker::default()),
            handshake_gate: HandshakeGate::new(limited_registry),
            shutdown_tx,
            listener_shutdown_tx,
            shutdown_notify: Notify::new(),
            observability,
            runtime_handle,
        })
    }
}

impl Transport {
    pub async fn bind(
        local_addr: DomusAddr,
        registry: Arc<TabernaRegistry>,
        config: DomusConfigAccess,
        observability: ObservabilityHandle,
        runtime_handle: tokio::runtime::Handle,
        auth: DomusAuthConfig,
    ) -> Result<Self, AureliaError> {
        let (backend, listener, local_addr) = match &local_addr {
            DomusAddr::Tcp(_) => {
                let backend = TcpBackend::new(auth, config.clone(), runtime_handle.clone())?;
                let listener = backend.bind(&local_addr).await?;
                let addr = listener.local_addr().map_err(|err| {
                    AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
                })?;
                (
                    TransportBackendImpl::Tcp(backend),
                    TransportListener::Tcp(listener),
                    DomusAddr::Tcp(addr),
                )
            }
            DomusAddr::Socket(path) => {
                let canonical = SocketBackend::canonicalize_socket_path(path).await?;
                let local_addr = DomusAddr::Socket(canonical);
                let backend = SocketBackend::new(auth, config.clone(), runtime_handle.clone())?;
                let listener = backend.bind(&local_addr).await?;
                (
                    TransportBackendImpl::Socket(Box::new(backend)),
                    TransportListener::Socket(listener),
                    local_addr,
                )
            }
        };
        Ok(Self {
            inner: TransportInner::new(
                registry,
                config,
                Arc::new(backend),
                listener,
                local_addr,
                observability,
                runtime_handle,
            ),
        })
    }

    /// Swap the backend's auth material atomically. Existing TLS / socket-auth sessions are
    /// unaffected; the next outbound dial and the next inbound accept use the new material.
    /// Smooth rotation: a peer presenting a different (validly authenticated) certificate on a
    /// subsequent callis is accepted at the same `peer_addr`.
    pub async fn reload_auth(&self, auth: DomusAuthConfig) -> Result<(), AureliaError> {
        self.inner.backend.reload_auth(auth).await
    }
}

impl<B> Transport<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    #[cfg(test)]
    pub async fn bind_with_backend(
        local_addr: DomusAddr,
        registry: Arc<TabernaRegistry>,
        config: DomusConfigAccess,
        observability: ObservabilityHandle,
        runtime_handle: tokio::runtime::Handle,
        backend: Arc<B>,
    ) -> Result<Self, AureliaError> {
        let listener = backend.bind(&local_addr).await?;
        Ok(Self {
            inner: TransportInner::new(
                registry,
                config,
                backend,
                listener,
                local_addr,
                observability,
                runtime_handle,
            ),
        })
    }

    pub(crate) fn blob_buffers(&self) -> Arc<BlobBufferTracker> {
        Arc::clone(&self.inner.blob_buffers)
    }

    pub fn local_addr(&self) -> DomusAddr {
        self.inner.local_addr.clone()
    }

    pub async fn start(&self) -> Result<JoinHandle<()>, AureliaError> {
        let listener = self.inner.listener.lock().await.take().ok_or_else(|| {
            AureliaError::with_message(ErrorId::PeerUnavailable, "listener already started")
        })?;
        let inner = Arc::clone(&self.inner);
        let handle = self.inner.runtime_handle.spawn(async move {
            run_listener(Arc::clone(&inner), listener).await;
        });
        Ok(handle)
    }

    pub async fn shutdown(&self) {
        let _ = self.inner.listener_shutdown_tx.send(true);

        let peers: Vec<Arc<PeerHandle<B>>> = {
            let guard = self.inner.peers.lock().await;
            guard.values().cloned().collect()
        };

        for peer in &peers {
            peer.graceful_close().await;
        }

        let send_timeout = self.inner.config.snapshot().await.send_timeout;
        let wait_timeout = send_timeout.saturating_add(send_timeout);
        for peer in &peers {
            let _ = peer.wait_for_callis_zero(wait_timeout).await;
        }

        let _ = self.inner.shutdown_tx.send(true);
        self.inner.shutdown_notify.notify_waiters();
        for peer in peers {
            peer.shutdown().await;
        }
    }

    pub async fn send_remote(
        &self,
        peer: DomusAddr,
        taberna_id: TabernaId,
        msg_type: MessageType,
        payload: Bytes,
        options: SendOptions,
    ) -> Result<SendOutcome, AureliaError> {
        let handle = self.inner.peer_handle(peer).await?;
        if options.blob {
            let sender = handle.send_blob(taberna_id, msg_type, payload).await?;
            Ok(SendOutcome::Blob { sender })
        } else {
            handle.send(taberna_id, msg_type, payload).await?;
            Ok(SendOutcome::MessageOnly)
        }
    }
}

impl<B> TransportInner<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    async fn peer_handle(
        self: &Arc<Self>,
        peer: DomusAddr,
    ) -> Result<Arc<PeerHandle<B>>, AureliaError> {
        if let Err(err) = self.ensure_peer_addr_kind(&peer) {
            self.observability
                .address_mismatch(peer.clone(), err.kind)
                .await;
            return Err(err);
        }
        let mut guard = self.peers.lock().await;
        if let Some(handle) = guard.get(&peer) {
            if !handle.session.is_closing() {
                handle.update_dial_addr(peer).await;
                return Ok(Arc::clone(handle));
            }
            // Existing handle was torn down (impaired window expired). Replace it so the
            // next send can dial fresh.
            guard.remove(&peer);
        }
        let handle = Arc::new(PeerHandle::new(
            Some(peer.clone()),
            Arc::clone(&self.registry),
            self.config.clone(),
            Arc::clone(&self.blob_buffers),
            Arc::clone(&self.backend),
            self.handshake_gate.clone(),
            self.observability.clone(),
            self.shutdown_tx.subscribe(),
            self.listener_shutdown_tx.clone(),
            self.runtime_handle.clone(),
        ));
        debug!(peer = %peer, "created peer handle");
        guard.insert(peer, Arc::clone(&handle));
        Ok(handle)
    }

    async fn peer_handle_inbound(
        self: &Arc<Self>,
        peer_addr: DomusAddr,
    ) -> Result<Arc<PeerHandle<B>>, AureliaError> {
        if let Err(err) = self.ensure_peer_addr_kind(&peer_addr) {
            self.observability
                .address_mismatch(peer_addr.clone(), err.kind)
                .await;
            return Err(err);
        }
        let mut guard = self.peers.lock().await;
        if let Some(handle) = guard.get(&peer_addr) {
            if !handle.session.is_closing() {
                return Ok(Arc::clone(handle));
            }
            guard.remove(&peer_addr);
        }
        let handle = Arc::new(PeerHandle::new(
            Some(peer_addr.clone()),
            Arc::clone(&self.registry),
            self.config.clone(),
            Arc::clone(&self.blob_buffers),
            Arc::clone(&self.backend),
            self.handshake_gate.clone(),
            self.observability.clone(),
            self.shutdown_tx.subscribe(),
            self.listener_shutdown_tx.clone(),
            self.runtime_handle.clone(),
        ));
        debug!(peer = %peer_addr, "created inbound peer handle");
        guard.insert(peer_addr, Arc::clone(&handle));
        Ok(handle)
    }

    fn ensure_peer_addr_kind(&self, peer: &DomusAddr) -> Result<(), AureliaError> {
        if peer.kind() == self.transport_kind {
            Ok(())
        } else {
            Err(AureliaError::with_message(
                ErrorId::AddressMismatch,
                format!(
                    "peer transport mismatch: local={} peer={}",
                    self.transport_kind, peer
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests;
