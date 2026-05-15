// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};
use tokio_rustls::TlsStream;
use tracing::{debug, info, trace, warn};

use crate::auth::Pkcs8AuthConfig;
use crate::callis::CallisKind;
use crate::config::{DomusConfig, DomusConfigAccess};
use crate::message_id::PeerMessageIdAllocator;
use crate::observability::{ObservabilityHandle, OutboundQueueTierReport};
use crate::send::{SendOptions, SendOutcome};
use crate::session::{DedupeDecision, PeerMessage, PeerSession, ReceiveOutcome, ReceiveSchedule};
use crate::taberna::TabernaRegistry;
use crate::wire::{
    BlobChunkFlags, BlobTransferChunkPayload, BlobTransferCompletePayload, ErrorPayload,
    HelloPayload, WireFlags, WireHeader, PROTOCOL_VERSION,
};
use aurelia_data::{DomusAddr, TransportKind};
use aurelia_ids::{AureliaError, ErrorId};
use aurelia_ids::{
    MessageType, PeerMessageId, TabernaId, MSG_ACK, MSG_BLOB_TRANSFER_CHUNK,
    MSG_BLOB_TRANSFER_COMPLETE, MSG_CLOSE, MSG_ERROR, MSG_HELLO, MSG_HELLO_RESPONSE, MSG_KEEPALIVE,
    MSG_RESERVED_7,
};

mod accept;
pub(crate) mod backend;
mod blob;
mod callback_rendezvous;
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
    PendingDuplicate(oneshot::Receiver<DedupeDecision>),
}

#[derive(Clone)]
pub(super) struct PeerTaskSpawner {
    runtime_handle: tokio::runtime::Handle,
    tx: mpsc::UnboundedSender<tokio::task::AbortHandle>,
}

pub(super) struct PeerTaskSet {
    spawner: PeerTaskSpawner,
    shutdown_tx: watch::Sender<bool>,
}

impl PeerTaskSet {
    pub(super) fn new(runtime_handle: &tokio::runtime::Handle) -> Arc<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<tokio::task::AbortHandle>();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let manager = runtime_handle.spawn(async move {
            let mut abort_handles = Vec::new();
            loop {
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    handle = rx.recv() => {
                        match handle {
                            Some(handle) => abort_handles.push(handle),
                            None => break,
                        }
                    }
                }
            }
            while let Ok(handle) = rx.try_recv() {
                abort_handles.push(handle);
            }
            for handle in abort_handles {
                handle.abort();
            }
        });
        std::mem::drop(manager);
        Arc::new(Self {
            spawner: PeerTaskSpawner {
                runtime_handle: runtime_handle.clone(),
                tx,
            },
            shutdown_tx,
        })
    }

    pub(super) fn spawner(&self) -> PeerTaskSpawner {
        self.spawner.clone()
    }
}

impl Drop for PeerTaskSet {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl PeerTaskSpawner {
    pub(super) fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        std::mem::drop(self.spawn_join(future));
    }

    pub(super) fn spawn_join<F>(&self, future: F) -> JoinHandle<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = self.runtime_handle.spawn(future);
        let abort_handle = handle.abort_handle();
        if let Err(err) = self.tx.send(abort_handle) {
            err.0.abort();
        }
        handle
    }
}

#[cfg(test)]
#[path = "tests/leaf/peer_task.rs"]
mod peer_task_tests;

struct BlobAcceptPending {
    dst_taberna: TabernaId,
    accept_rx: oneshot::Receiver<Result<(), AureliaError>>,
    receiver_state: Arc<blob::BlobReceiverState>,
    send_timeout: Duration,
    peer_state_tx: mpsc::Sender<PeerStateUpdate>,
}

use aurelia_logging::LimitedLogRegistry;

pub(crate) use blob::blob_buffer_full_error;
use blob::{
    handle_blob_request, handle_blob_transfer_chunk, BlobCallisSettings, BlobChunkOutcome,
    BlobManager,
};
use callis::spawn_callis_task;
use frame::{read_frame, send_blob_chunk_frame, send_control_frame, send_outbound_frame};
use handshake::{accept_inbound_with_observability, spawn_blob_dial_task, spawn_dial_task};
use listener::run_listener;
use peer::{
    CallisHandle, CallisTx, ConnectionInfo, OutboundFrame, PeerHandle, PeerRole, PeerStateUpdate,
};
use primary::{
    compute_listener_delay, compute_reconnect_delay, current_dial_addr, remove_primary_handle,
};
use primary_dispatch::{
    OutboundQueueOverrunReporter, PrimaryDispatchManager, PrimaryDispatchManagerContext,
};

fn try_signal_peer_state_ensure(
    peer_state_tx: &mpsc::Sender<PeerStateUpdate>,
    update: PeerStateUpdate,
) -> Result<(), AureliaError> {
    debug_assert!(matches!(
        update,
        PeerStateUpdate::EnsurePrimaryDial | PeerStateUpdate::EnsureBlobDial
    ));
    match peer_state_tx.try_send(update) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            trace!("peer-state ensure signal coalesced behind queued work");
            Ok(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            Err(AureliaError::new(ErrorId::PeerUnavailable))
        }
    }
}

pub use backend::TransportBackend;
pub use socket_backend::SocketBackend;
pub use tcp_backend::TcpBackend;

pub struct TransportStreamImpl {
    inner: TransportStreamInner,
}

// One stream enum value is owned per callis task. Keeping the concrete streams inline preserves
// static dispatch and avoids heap indirection on the production callis I/O path.
#[allow(clippy::large_enum_variant)]
enum TransportStreamInner {
    Tcp(TlsStream<TcpStream>),
    Socket(UnixStream),
}

impl TransportStreamImpl {
    fn tcp(stream: TlsStream<TcpStream>) -> Self {
        Self {
            inner: TransportStreamInner::Tcp(stream),
        }
    }

    fn socket(stream: UnixStream) -> Self {
        Self {
            inner: TransportStreamInner::Socket(stream),
        }
    }
}

impl AsyncRead for TransportStreamImpl {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            TransportStreamInner::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            TransportStreamInner::Socket(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStreamImpl {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            TransportStreamInner::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            TransportStreamInner::Socket(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            TransportStreamInner::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            TransportStreamInner::Socket(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            TransportStreamInner::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            TransportStreamInner::Socket(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

pub enum TransportBackendImpl {
    Tcp(Arc<TcpBackend>),
    Socket(Arc<SocketBackend>),
}

impl TransportBackendImpl {
    pub async fn reload_auth(&self, auth: Pkcs8AuthConfig) -> Result<(), AureliaError> {
        match self {
            TransportBackendImpl::Tcp(backend) => backend.reload_auth(auth).await,
            TransportBackendImpl::Socket(backend) => backend.reload_auth(auth).await,
        }
    }
}

pub enum TransportListener {
    Tcp {
        backend: Arc<TcpBackend>,
        listener: tokio::net::TcpListener,
    },
    Socket {
        backend: Arc<SocketBackend>,
        listener: tokio::net::UnixListener,
    },
}

#[async_trait::async_trait]
impl TransportBackend for TransportBackendImpl {
    type Addr = DomusAddr;
    type Listener = TransportListener;
    type Stream = TransportStreamImpl;

    async fn bind(&self, local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        match self {
            TransportBackendImpl::Tcp(backend) => {
                let listener = backend.bind(local).await?;
                Ok(TransportListener::Tcp {
                    backend: Arc::clone(backend),
                    listener,
                })
            }
            TransportBackendImpl::Socket(backend) => {
                let listener = backend.bind(local).await?;
                Ok(TransportListener::Socket {
                    backend: Arc::clone(backend),
                    listener,
                })
            }
        }
    }

    async fn accept(
        &self,
        listener: &mut Self::Listener,
    ) -> Result<backend::AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        match listener {
            TransportListener::Tcp { backend, listener } => {
                let authenticated = backend.accept(listener).await?;
                Ok(backend::AuthenticatedStream {
                    stream: TransportStreamImpl::tcp(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
            TransportListener::Socket { backend, listener } => {
                let authenticated = backend.accept(listener).await?;
                Ok(backend::AuthenticatedStream {
                    stream: TransportStreamImpl::socket(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
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
                    stream: TransportStreamImpl::tcp(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
            TransportBackendImpl::Socket(backend) => {
                let authenticated = backend.dial(peer).await?;
                Ok(backend::AuthenticatedStream {
                    stream: TransportStreamImpl::socket(authenticated.stream),
                    peer_addr: authenticated.peer_addr,
                })
            }
        }
    }
}

type CallisId = u64;

static CALLIS_ID: AtomicU64 = AtomicU64::new(1);

fn next_callis_id() -> CallisId {
    // Relaxed is sufficient: callis IDs only need to be unique.
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
        // docs/concurrency.md Pattern 1: arm the notified() future BEFORE the state check
        // so any close() bump fired between the check and the await is observed.
        timeout(timeout_duration, async {
            loop {
                let waiter = self.notify.notified();
                tokio::pin!(waiter);
                if self.open.load(Ordering::SeqCst) == 0 {
                    break;
                }
                waiter.await;
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
            aurelia_logging::warn_limited!(
                self.limited_registry,
                aurelia_ids::LogId::HandshakePerPeerLimit,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlobBufferSide {
    Outbound,
    Inbound,
}

pub(crate) enum BlobBufferReservationFailure {
    Outbound,
    Inbound,
}

impl BlobBufferTracker {
    pub(crate) fn try_reserve_outbound(&self, bytes: u64, cap: u64) -> bool {
        self.try_reserve_side(BlobBufferSide::Outbound, bytes, cap)
    }

    pub(crate) fn release_outbound(&self, bytes: u64) {
        self.release_side(BlobBufferSide::Outbound, bytes);
    }

    pub(crate) fn try_reserve_inbound(&self, bytes: u64, cap: u64) -> bool {
        self.try_reserve_side(BlobBufferSide::Inbound, bytes, cap)
    }

    pub(crate) fn release_inbound(&self, bytes: u64) {
        self.release_side(BlobBufferSide::Inbound, bytes);
    }

    pub(crate) fn try_reserve_pair(
        &self,
        outbound_bytes: u64,
        outbound_cap: u64,
        inbound_bytes: u64,
        inbound_cap: u64,
    ) -> Result<(), BlobBufferReservationFailure> {
        if !self.try_reserve_outbound(outbound_bytes, outbound_cap) {
            return Err(BlobBufferReservationFailure::Outbound);
        }
        if !self.try_reserve_inbound(inbound_bytes, inbound_cap) {
            self.release_outbound(outbound_bytes);
            return Err(BlobBufferReservationFailure::Inbound);
        }
        Ok(())
    }

    fn try_reserve_side(&self, side: BlobBufferSide, bytes: u64, cap: u64) -> bool {
        let used = self.used(side);
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

    fn release_side(&self, side: BlobBufferSide, bytes: u64) {
        let used = self.used(side);
        let mut underflow = false;
        let _ = used.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            if current < bytes {
                underflow = true;
                Some(0)
            } else {
                Some(current - bytes)
            }
        });
        debug_assert!(
            !underflow,
            "blob buffer release underflow: side={side:?} bytes={bytes}"
        );
    }

    fn used(&self, side: BlobBufferSide) -> &AtomicU64 {
        match side {
            BlobBufferSide::Outbound => &self.outbound_used,
            BlobBufferSide::Inbound => &self.inbound_used,
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
        auth: Pkcs8AuthConfig,
    ) -> Result<Self, AureliaError> {
        let (backend, listener, local_addr) = match &local_addr {
            DomusAddr::Tcp(_) => {
                let backend = Arc::new(TcpBackend::new(
                    auth,
                    config.clone(),
                    runtime_handle.clone(),
                )?);
                let listener = backend.bind(&local_addr).await?;
                let addr = listener.local_addr().map_err(|err| {
                    AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string())
                })?;
                (
                    TransportBackendImpl::Tcp(Arc::clone(&backend)),
                    TransportListener::Tcp { backend, listener },
                    DomusAddr::Tcp(addr),
                )
            }
            DomusAddr::Socket(path) => {
                let canonical = SocketBackend::canonicalize_socket_path(path).await?;
                let local_addr = DomusAddr::Socket(canonical);
                let backend = Arc::new(SocketBackend::new(
                    auth,
                    config.clone(),
                    runtime_handle.clone(),
                )?);
                let listener = backend.bind(&local_addr).await?;
                (
                    TransportBackendImpl::Socket(Arc::clone(&backend)),
                    TransportListener::Socket { backend, listener },
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
    pub async fn reload_auth(&self, auth: Pkcs8AuthConfig) -> Result<(), AureliaError> {
        self.inner.backend.reload_auth(auth).await
    }
}

impl<B> Transport<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
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
        join_all(
            peers
                .iter()
                .map(|peer| peer.wait_for_callis_zero(wait_timeout)),
        )
        .await;

        self.inner.shutdown_tx.send_replace(true);
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
        if *self.shutdown_tx.borrow() {
            return Err(AureliaError::new(ErrorId::PeerUnavailable));
        }
        self.peer_handle_for(peer.clone(), Some(peer)).await
    }

    async fn peer_handle_inbound(
        self: &Arc<Self>,
        peer_addr: DomusAddr,
    ) -> Result<Arc<PeerHandle<B>>, AureliaError> {
        self.peer_handle_for(peer_addr, None).await
    }

    async fn peer_handle_for(
        self: &Arc<Self>,
        peer_addr: DomusAddr,
        dial_addr: Option<DomusAddr>,
    ) -> Result<Arc<PeerHandle<B>>, AureliaError> {
        if let Err(err) = self.ensure_peer_addr_kind(&peer_addr) {
            self.observability
                .address_mismatch(peer_addr.clone(), err.kind);
            return Err(err);
        }
        let is_outbound = dial_addr.is_some();
        let mut guard = self.peers.lock().await;
        if let Some(handle) = guard.get(&peer_addr) {
            if !handle.session.is_closing() {
                if let Some(dial_addr) = dial_addr {
                    handle.update_dial_addr(dial_addr).await;
                }
                return Ok(Arc::clone(handle));
            }
            // Existing handle was torn down (impaired window expired). Replace it so the
            // next send or inbound callis can establish fresh state.
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
        if is_outbound {
            debug!(peer = %peer_addr, "created peer handle");
        } else {
            debug!(peer = %peer_addr, "created inbound peer handle");
        }
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
impl<B> Transport<B>
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
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
}

#[cfg(test)]
mod tests;
