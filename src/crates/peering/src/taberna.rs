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
pub(crate) trait TabernaInbox: Send + Sync {
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

pub(crate) struct TabernaRegistration {
    taberna_id: TabernaId,
    registry: Arc<TabernaRegistry>,
    runtime_handle: tokio::runtime::Handle,
}

impl TabernaRegistration {
    pub(crate) fn new(
        taberna_id: TabernaId,
        registry: Arc<TabernaRegistry>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            taberna_id,
            registry,
            runtime_handle,
        }
    }
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
    completion: TabernaCompletion,
}

/// Application-owned pieces of a split [`TabernaRequest`].
///
/// Splitting is useful when an adapter or application needs to move the decoded payload and
/// complete the Aurelia delivery decision at a different boundary. Public callers can only accept
/// or reject through [`TabernaCompletion`]; internal A1 paths own transport-specific outcomes.
pub struct TabernaRequestParts<Codec: MessageCodec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    /// The decoded application message.
    pub message: Codec::AppMessage,
    /// Inbound blob stream attached to this message, if present.
    pub blob_receiver: Option<BlobReceiver>,
    /// Completion guard for the Aurelia delivery decision.
    pub completion: TabernaCompletion,
}

/// Completion guard for a taberna request.
///
/// Dropping the guard without an explicit decision rejects the request with
/// [`ErrorId::RemoteTabernaRejected`].
pub struct TabernaCompletion {
    response: Option<oneshot::Sender<Result<(), AureliaError>>>,
    notify: Option<Arc<Notify>>,
}

impl TabernaCompletion {
    /// Acknowledges the request as accepted by the local application boundary.
    ///
    /// This is a fire-and-complete operation. Sender-side wait, timeout, shutdown, and
    /// cancellation state is owned by A1 and is not surfaced to the accepting application.
    pub fn accept(self) {
        self.complete(Ok(()));
    }

    /// Rejects the request with [`ErrorId::RemoteTabernaRejected`].
    ///
    /// Rejection does not return a result because there is no useful caller action if the
    /// sender-side waiter is no longer present.
    pub fn reject(self) {
        self.complete(Err(AureliaError::new(ErrorId::RemoteTabernaRejected)));
    }

    pub(crate) fn busy(self) {
        self.complete(Err(AureliaError::new(ErrorId::TabernaBusy)));
    }

    pub(crate) fn domus_closed(self) {
        self.complete(Err(AureliaError::new(ErrorId::DomusClosed)));
    }

    #[cfg(feature = "actix")]
    pub(crate) fn taberna_shutdown(self) {
        self.complete(Err(AureliaError::new(ErrorId::TabernaShutdown)));
    }

    fn complete(mut self, result: Result<(), AureliaError>) {
        if let Some(response) = self.response.take() {
            let _ = response.send(result);
        }
        self.notify();
    }

    fn notify(&self) {
        if let Some(notify) = self.notify.as_ref() {
            notify.notify_one();
        }
    }
}

impl Drop for TabernaCompletion {
    fn drop(&mut self) {
        if let Some(response) = self.response.take() {
            let _ = response.send(Err(AureliaError::new(ErrorId::RemoteTabernaRejected)));
            self.notify();
        }
    }
}

impl<Codec: MessageCodec> TabernaRequest<Codec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    pub(crate) fn new(
        message: Codec::AppMessage,
        blob_receiver: Option<BlobReceiver>,
        response: oneshot::Sender<Result<(), AureliaError>>,
        notify: Option<Arc<Notify>>,
    ) -> Self {
        Self {
            message,
            blob_receiver,
            completion: TabernaCompletion {
                response: Some(response),
                notify,
            },
        }
    }

    /// Acknowledges this request as accepted at the local application boundary.
    ///
    /// This is a fire-and-complete operation. Sender-side wait, timeout, shutdown, and
    /// cancellation state is owned by A1 and is not surfaced to the accepting application.
    pub fn accept(self) {
        let Self { completion, .. } = self;
        completion.accept();
    }

    /// Rejects this request, signalling [`ErrorId::RemoteTabernaRejected`]
    /// back to the sender. Application-specific failure details should travel
    /// in application-level response payloads, not A1 error codes.
    ///
    /// Rejection does not return a result because there is no useful caller action if the
    /// sender-side waiter is no longer present.
    pub fn reject(self) {
        let Self { completion, .. } = self;
        completion.reject();
    }

    fn expire(self) {
        let Self { completion, .. } = self;
        completion.busy();
    }

    fn shutdown(self) {
        let Self { completion, .. } = self;
        completion.domus_closed();
    }

    /// Splits the request into application payload and completion guard without copying.
    pub fn into_parts(self) -> TabernaRequestParts<Codec> {
        let Self {
            message,
            blob_receiver,
            completion,
        } = self;
        TabernaRequestParts {
            message,
            blob_receiver,
            completion,
        }
    }
}

/// A registered inbound endpoint on a `Domus`. Each taberna has a
/// stable [`crate::TabernaId`] and a typed codec; received messages are
/// decoded with that codec and surfaced as [`TabernaRequest`]s via
/// [`Taberna::next`].
///
/// Tabernas are constructed with `Domus::taberna`; dropping a [`Taberna`] schedules
/// deregistration from the parent domus. The taberna ID becomes available for reuse once the
/// spawned deregistration task completes.
///
/// # Examples
///
/// Usage examples live in the main `aurelia` crate docs to ensure doctests
/// compile at the library API level.
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
            _registration: Arc::new(TabernaRegistration::new(
                taberna_id,
                registry,
                runtime_handle,
            )),
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

    fn refresh_limits(&self) {
        let limits = self.config.taberna_limits();
        let next_size = limits.accept_queue_size;
        let next_ttl = limits.accept_timeout;
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
        self.refresh_limits();
        let message = self
            .codec
            .decode_app(msg_type, payload.as_ref())
            .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))?;
        let (response_tx, response_rx) = oneshot::channel();
        let request = TabernaRequest::new(message, blob_receiver, response_tx, notify);
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

pub(crate) trait TabernaTermination<Codec: MessageCodec>
where
    Codec::AppMessage: Send + Sync + 'static,
{
    fn terminate(item: TabernaRequest<Codec>);
}

pub(crate) struct ExpireTabernaRequest;
pub(crate) struct ShutdownTabernaRequest;

impl<Codec: MessageCodec> TabernaTermination<Codec> for ExpireTabernaRequest
where
    Codec::AppMessage: Send + Sync + 'static,
{
    fn terminate(item: TabernaRequest<Codec>) {
        item.expire();
    }
}

impl<Codec: MessageCodec> TabernaTermination<Codec> for ShutdownTabernaRequest
where
    Codec::AppMessage: Send + Sync + 'static,
{
    fn terminate(item: TabernaRequest<Codec>) {
        item.shutdown();
    }
}

pub(crate) struct TabernaTerminationReport<Codec, F>
where
    Codec: MessageCodec,
    Codec::AppMessage: Send + Sync + 'static,
    F: TabernaTermination<Codec> + Send + Sync + 'static,
{
    _marker: PhantomData<(Codec, F)>,
}

impl<Codec, F> TabernaTerminationReport<Codec, F>
where
    Codec: MessageCodec,
    Codec::AppMessage: Send + Sync + 'static,
    F: TabernaTermination<Codec> + Send + Sync + 'static,
{
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<Codec, F> ReportChannel<TabernaRequest<Codec>> for TabernaTerminationReport<Codec, F>
where
    Codec: MessageCodec,
    Codec::AppMessage: Send + Sync + 'static,
    F: TabernaTermination<Codec> + Send + Sync + 'static,
{
    fn send(&self, item: TabernaRequest<Codec>) -> Result<(), TabernaRequest<Codec>> {
        F::terminate(item);
        Ok(())
    }
}

pub(crate) type TabernaExpiryReport<Codec> = TabernaTerminationReport<Codec, ExpireTabernaRequest>;
pub(crate) type TabernaShutdownReport<Codec> =
    TabernaTerminationReport<Codec, ShutdownTabernaRequest>;

fn duration_to_nanos(duration: Duration) -> u64 {
    duration
        .as_nanos()
        .min(u64::MAX as u128)
        .try_into()
        .unwrap_or(u64::MAX)
}
