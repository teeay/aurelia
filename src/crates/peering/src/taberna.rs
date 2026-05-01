// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Notify, RwLock};
use tracing::{debug, trace, warn};

use crate::codec::MessageCodec;
use crate::config::DomusConfigAccess;
use crate::BlobReceiver;
use aurelia_ids::{AureliaError, ErrorId, MessageType, TabernaId};
use caducus::{CaducusError, CaducusErrorKind, MpscSender, Receiver, ReportChannel};

const DEFAULT_NEXT_TIMEOUT: Duration = Duration::from_secs(1);

/// Object-safe trait implemented by every taberna's inbox so the transport
/// can deliver inbound messages without naming the concrete codec type.
#[async_trait::async_trait]
pub trait TabernaInbox: Send + Sync {
    /// Enqueues a decoded message for application consumption. Returns a
    /// oneshot the transport awaits to learn whether the application
    /// accepted, rejected, or timed out the message.
    async fn enqueue(
        &self,
        msg_type: MessageType,
        payload: bytes::Bytes,
        blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError>;

    /// Signals the inbox to stop accepting new messages and release resources.
    async fn shutdown(&self) {}
}

#[derive(Default)]
pub(crate) struct TabernaRegistry {
    inner: RwLock<HashMap<TabernaId, Arc<dyn TabernaInbox>>>,
}

impl TabernaRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn register(
        &self,
        taberna_id: TabernaId,
        inbox: Arc<dyn TabernaInbox>,
    ) -> Result<(), AureliaError> {
        let mut guard = self.inner.write().await;
        if guard.contains_key(&taberna_id) {
            warn!(taberna_id, "taberna already registered");
            return Err(AureliaError::new(ErrorId::TabernaAlreadyRegistered));
        }
        guard.insert(taberna_id, inbox);
        debug!(taberna_id, "taberna registered");
        Ok(())
    }

    pub(crate) async fn unregister(&self, taberna_id: TabernaId) {
        let mut guard = self.inner.write().await;
        guard.remove(&taberna_id);
        debug!(taberna_id, "taberna unregistered");
    }

    pub(crate) async fn shutdown(&self) {
        let mut guard = self.inner.write().await;
        let inboxes = std::mem::take(&mut *guard);
        drop(guard);
        for (taberna_id, inbox) in inboxes {
            debug!(taberna_id, "taberna shutdown requested");
            inbox.shutdown().await;
        }
    }

    pub(crate) async fn resolve_local(
        &self,
        taberna_id: TabernaId,
    ) -> Option<Arc<dyn TabernaInbox>> {
        let guard = self.inner.read().await;
        let inbox = guard.get(&taberna_id).cloned();
        trace!(taberna_id, found = inbox.is_some(), "taberna resolved");
        inbox
    }
}

struct TabernaRegistration {
    taberna_id: TabernaId,
    registry: Arc<TabernaRegistry>,
    runtime_handle: tokio::runtime::Handle,
}

impl Drop for TabernaRegistration {
    fn drop(&mut self) {
        let taberna_id = self.taberna_id;
        let registry = Arc::clone(&self.registry);
        let runtime_handle = self.runtime_handle.clone();
        runtime_handle.spawn(async move {
            registry.unregister(taberna_id).await;
        });
    }
}

/// A single inbound delivery handed to the application. The application
/// must acknowledge the request by calling either [`TabernaRequest::accept`]
/// or [`TabernaRequest::reject`]; dropping the request without a decision
/// is treated as rejection.
pub struct TabernaRequest<Codec: MessageCodec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    /// The decoded application message.
    pub message: Codec::AppMessage,
    /// Inbound blob stream attached to this message, if the sender used
    /// [`crate::SendOptions::BLOB`].
    pub blob_receiver: Option<BlobReceiver>,
    response: Option<oneshot::Sender<Result<(), AureliaError>>>,
    notify: Option<Arc<Notify>>,
}

impl<Codec: MessageCodec> TabernaRequest<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    /// Acknowledges this request, signalling success back to the sender.
    pub async fn accept(mut self) -> Result<(), AureliaError> {
        if let Some(response) = self.response.take() {
            let result = response
                .send(Ok(()))
                .map_err(|_| AureliaError::new(ErrorId::RemoteTabernaRejected));
            self.notify();
            return result;
        }
        self.notify();
        Err(AureliaError::new(ErrorId::RemoteTabernaRejected))
    }

    /// Rejects this request with `err`, propagated to the sender.
    pub async fn reject(mut self, err: AureliaError) {
        if let Some(response) = self.response.take() {
            let _ = response.send(Err(err));
        }
        self.notify();
    }

    fn expire(mut self) {
        if let Some(response) = self.response.take() {
            let _ = response.send(Err(AureliaError::new(ErrorId::TabernaBusy)));
        }
        self.notify();
    }

    fn shutdown(mut self) {
        if let Some(response) = self.response.take() {
            let _ = response.send(Err(AureliaError::new(ErrorId::DomusClosed)));
        }
        self.notify();
    }

    fn notify(&self) {
        if let Some(notify) = self.notify.as_ref() {
            notify.notify_one();
        }
    }
}

impl<Codec: MessageCodec> Drop for TabernaRequest<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(response) = self.response.take() {
            let _ = response.send(Err(AureliaError::new(ErrorId::RemoteTabernaRejected)));
        }
        if let Some(notify) = self.notify.as_ref() {
            notify.notify_one();
        }
    }
}

/// A registered inbound endpoint on a `Domus`. Each taberna has a
/// stable [`crate::TabernaId`] and a typed codec; received messages are
/// decoded with that codec and surfaced as [`TabernaRequest`]s via
/// [`Taberna::next`].
///
/// Tabernas are constructed with `Domus::taberna`; dropping a
/// [`Taberna`] unregisters it from the parent domus.
///
/// # Example
///
/// ```no_run
/// # use aurelia::{AureliaError, MessageCodec, EncodedMessage};
/// # struct MyCodec;
/// # impl MessageCodec for MyCodec {
/// #     type AppMessage = ();
/// #     fn encode_app(&self, _: &()) -> Result<EncodedMessage, AureliaError> { unimplemented!() }
/// #     fn decode_app(&self, _: u32, _: &[u8]) -> Result<(), AureliaError> { unimplemented!() }
/// # }
/// # async fn example<RR: aurelia::RouteResolver>(domus: aurelia::Domus<RR>) -> Result<(), AureliaError> {
/// let taberna = domus.taberna(42, MyCodec).await?;
/// let request = taberna.next(None).await?;
/// request.accept().await?;
/// # Ok(()) }
/// ```
pub struct Taberna<Codec: MessageCodec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    inbox: Receiver<TabernaRequest<Codec>>,
    _registration: Arc<TabernaRegistration>,
}

impl<Codec: MessageCodec> Taberna<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    pub(crate) fn new(
        taberna_id: TabernaId,
        inbox: Receiver<TabernaRequest<Codec>>,
        registry: Arc<TabernaRegistry>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            inbox,
            _registration: Arc::new(TabernaRegistration {
                taberna_id,
                registry,
                runtime_handle,
            }),
        }
    }

    /// Awaits the next [`TabernaRequest`]. `timeout_override` overrides the
    /// default per-call timeout; `None` uses the default. Returns
    /// [`AureliaError`] with [`ErrorId::ReceiveTimeout`] if no message
    /// arrives in time, or [`ErrorId::DomusClosed`] if the parent
    /// `Domus` has shut down.
    pub async fn next(
        &self,
        timeout_override: Option<Duration>,
    ) -> Result<TabernaRequest<Codec>, AureliaError> {
        let timeout_duration = timeout_override.unwrap_or(DEFAULT_NEXT_TIMEOUT);
        let deadline = Instant::now() + timeout_duration;
        match self.inbox.next(Some(deadline)).await {
            Ok(request) => Ok(request),
            Err(CaducusError {
                kind: CaducusErrorKind::Timeout,
            }) => Err(AureliaError::new(ErrorId::ReceiveTimeout)),
            Err(CaducusError {
                kind: CaducusErrorKind::Shutdown(_),
            }) => Err(AureliaError::new(ErrorId::DomusClosed)),
            Err(err) => Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                err.to_string(),
            )),
        }
    }
}

pub(crate) struct TabernaInboxHandle<Codec: MessageCodec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    codec: Codec,
    sender: MpscSender<TabernaRequest<Codec>>,
    config: DomusConfigAccess,
    last_queue_size: AtomicUsize,
    last_ttl_nanos: AtomicU64,
}

impl<Codec: MessageCodec> TabernaInboxHandle<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    pub(crate) fn new(
        codec: Codec,
        sender: MpscSender<TabernaRequest<Codec>>,
        config: DomusConfigAccess,
        initial_queue_size: usize,
        initial_ttl: Duration,
    ) -> Self {
        sender.set_expiry_channel(Some(Arc::new(TabernaExpiryReport::<Codec>::new())));
        Self {
            codec,
            sender,
            config,
            last_queue_size: AtomicUsize::new(initial_queue_size),
            last_ttl_nanos: AtomicU64::new(duration_to_nanos(initial_ttl)),
        }
    }

    async fn refresh_limits(&self) {
        let config = self.config.snapshot().await;
        let next_size = config.taberna_accept_queue_size;
        let next_ttl = config.accept_timeout;
        if self.last_queue_size.load(Ordering::SeqCst) != next_size {
            self.sender.update_capacity(next_size);
            self.last_queue_size.store(next_size, Ordering::SeqCst);
        }
        let ttl_nanos = duration_to_nanos(next_ttl);
        if self.last_ttl_nanos.load(Ordering::SeqCst) != ttl_nanos {
            let _ = self.sender.update_ttl(next_ttl);
            self.last_ttl_nanos.store(ttl_nanos, Ordering::SeqCst);
        }
    }
}

#[async_trait::async_trait]
impl<Codec: MessageCodec> TabernaInbox for TabernaInboxHandle<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    async fn enqueue(
        &self,
        msg_type: MessageType,
        payload: bytes::Bytes,
        blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        self.refresh_limits().await;
        let message = self
            .codec
            .decode_app(msg_type, payload.as_ref())
            .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))?;
        let (response_tx, response_rx) = oneshot::channel();
        let request = TabernaRequest {
            message,
            blob_receiver,
            response: Some(response_tx),
            notify,
        };
        match self.sender.send(request) {
            Ok(()) => Ok(response_rx),
            Err(CaducusError {
                kind: CaducusErrorKind::Full(_),
            }) => Err(AureliaError::new(ErrorId::TabernaBusy)),
            Err(CaducusError {
                kind: CaducusErrorKind::Shutdown(_),
            }) => Err(AureliaError::new(ErrorId::DomusClosed)),
            Err(err) => Err(AureliaError::with_message(
                ErrorId::ProtocolViolation,
                err.to_string(),
            )),
        }
    }

    async fn shutdown(&self) {
        self.sender.shutdown();
    }
}

pub(crate) struct TabernaExpiryReport<Codec: MessageCodec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    _marker: PhantomData<Codec>,
}

impl<Codec: MessageCodec> TabernaExpiryReport<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<Codec: MessageCodec> ReportChannel<TabernaRequest<Codec>> for TabernaExpiryReport<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    fn send(&self, item: TabernaRequest<Codec>) -> Result<(), TabernaRequest<Codec>> {
        item.expire();
        Ok(())
    }
}

pub(crate) struct TabernaShutdownReport<Codec: MessageCodec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    _marker: PhantomData<Codec>,
}

impl<Codec: MessageCodec> TabernaShutdownReport<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<Codec: MessageCodec> ReportChannel<TabernaRequest<Codec>> for TabernaShutdownReport<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    fn send(&self, item: TabernaRequest<Codec>) -> Result<(), TabernaRequest<Codec>> {
        item.shutdown();
        Ok(())
    }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration
        .as_nanos()
        .min(u64::MAX as u128)
        .try_into()
        .unwrap_or(u64::MAX)
}
