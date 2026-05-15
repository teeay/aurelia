// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::auth::Pkcs8AuthConfig;
use crate::codec::MessageCodec;
use crate::config::{
    normalize_domus_config_for_transport, DomusConfig, DomusConfigAccess, DomusConfigStore,
};
use crate::observability::{
    new_observability, DomusReporting, DomusReportingFeeds, ObservabilityHandle,
};
use crate::peering::RouteLocalRemote;
use crate::send::{SendOptions, SendOutcome};
use crate::taberna::{Taberna, TabernaInboxHandle, TabernaRegistry, TabernaShutdownReport};
use crate::transport::Transport;
use aurelia_data::DomusAddr;
use aurelia_data::RouteResolver;
use aurelia_ids::{AureliaError, ErrorId, TabernaId};
use caducus::MpscBuilder;

#[cfg(feature = "actix")]
use crate::actix_adapter::{ActixTaberna, ActixTabernaDelivery};

/// Builder for a [`Domus`].
///
/// This is the low-level peering builder; most applications should construct
/// a [`Domus`] via `Aurelia::domus_builder` instead.
///
/// A [`DomusBuilder`] consumes a [`DomusConfig`], a local [`DomusAddr`],
/// authentication material, and a [`RouteResolver`], then yields either a
/// plain [`Domus`] via [`DomusBuilder::build`] or a `(Domus, DomusReportingFeeds)`
/// pair via [`DomusBuilder::build_with_reporting`] when observability feeds
/// are required.
pub struct DomusBuilder<RR>
where
    RR: RouteResolver + 'static,
{
    config: DomusConfig,
    local_addr: DomusAddr,
    auth: Pkcs8AuthConfig,
    resolver: Arc<RR>,
    runtime_handle: tokio::runtime::Handle,
}

impl<RR> DomusBuilder<RR>
where
    RR: RouteResolver + 'static,
{
    /// Constructs a new builder. Callers normally reach this through
    /// `Aurelia::domus_builder` rather than directly.
    pub fn new(
        config: DomusConfig,
        local_addr: DomusAddr,
        auth: Pkcs8AuthConfig,
        resolver: Arc<RR>,
    ) -> Self {
        Self {
            config,
            local_addr,
            auth,
            resolver,
            runtime_handle: aurelia_platform::runtime::handle(),
        }
    }

    /// Builds a [`Domus`] without observability feeds. The transport is
    /// bound and started before this future resolves.
    pub async fn build(self) -> Result<Domus<RR>, AureliaError> {
        let (tx, rx) = oneshot::channel();
        let runtime_handle = self.runtime_handle.clone();
        runtime_handle.spawn(async move {
            let result = self.build_internal(false).await.map(|(domus, _)| domus);
            let _ = tx.send(result);
        });
        rx.await.unwrap_or_else(|_| {
            Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "aurelia runtime unavailable",
            ))
        })
    }

    /// Builds a [`Domus`] together with [`DomusReportingFeeds`] for
    /// streaming observability events out to an external sink.
    pub async fn build_with_reporting(
        self,
    ) -> Result<(Domus<RR>, DomusReportingFeeds), AureliaError> {
        let (tx, rx) = oneshot::channel();
        let runtime_handle = self.runtime_handle.clone();
        runtime_handle.spawn(async move {
            let result = self.build_internal(true).await.and_then(|(domus, feeds)| {
                let feeds = feeds.ok_or_else(|| {
                    AureliaError::with_message(
                        ErrorId::ProtocolViolation,
                        "reporting feeds missing",
                    )
                })?;
                Ok((domus, feeds))
            });
            let _ = tx.send(result);
        });
        rx.await.unwrap_or_else(|_| {
            Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "aurelia runtime unavailable",
            ))
        })
    }

    async fn build_internal(
        self,
        with_feeds: bool,
    ) -> Result<(Domus<RR>, Option<DomusReportingFeeds>), AureliaError> {
        let registry = Arc::new(TabernaRegistry::new());
        let runtime_handle = self.runtime_handle.clone();
        let (reporting, observability) = new_observability(runtime_handle.clone());
        let feeds = if with_feeds {
            Some(reporting.feeds())
        } else {
            None
        };
        let config = normalize_domus_config_for_transport(self.config, self.local_addr.kind())?;
        let config = Arc::new(DomusConfigStore::new(config));
        let config_access =
            DomusConfigAccess::new(Arc::clone(&config), Some(observability.clone()));
        let transport = Arc::new(
            Transport::bind(
                self.local_addr,
                Arc::clone(&registry),
                config_access.clone(),
                observability.clone(),
                runtime_handle.clone(),
                self.auth,
            )
            .await?,
        );
        let peering = RouteLocalRemote::new(
            config_access.clone(),
            Arc::clone(&registry),
            Arc::clone(&self.resolver),
            Arc::clone(&transport),
        );
        let transport_handle = transport.start().await?;
        let domus = Domus {
            config: config_access,
            registry,
            peering,
            transport,
            transport_handle: Mutex::new(Some(transport_handle)),
            reporting,
            observability,
            runtime_handle,
        };
        Ok((domus, feeds))
    }
}

/// A running Aurelia domus: the local peer's representation in the mesh.
///
/// A [`Domus`] owns a single bound transport (TCP+mTLS or Unix socket)
/// and the registry of [`Taberna`]s hosted on this peer. Outbound traffic is
/// dispatched through the configured [`RouteResolver`]; inbound traffic is
/// delivered to the relevant taberna's inbox.
///
/// Construct one via `Aurelia::domus_builder` (recommended) or
/// directly via `DomusBuilder`.
///
/// # Examples
///
/// Usage examples live in the main `aurelia` crate docs to ensure doctests
/// compile at the library API level.
pub struct Domus<RR>
where
    RR: RouteResolver,
{
    config: DomusConfigAccess,
    registry: Arc<TabernaRegistry>,
    peering: RouteLocalRemote<RR>,
    transport: Arc<Transport>,
    transport_handle: Mutex<Option<JoinHandle<()>>>,
    reporting: DomusReporting,
    observability: ObservabilityHandle,
    runtime_handle: tokio::runtime::Handle,
}

impl<RR> Domus<RR>
where
    RR: RouteResolver,
{
    /// Returns a live handle to this domus's configuration; see [`DomusConfigAccess`].
    pub fn config(&self) -> DomusConfigAccess {
        self.config.clone()
    }

    /// Returns the bound local address.
    pub fn local_addr(&self) -> DomusAddr {
        self.transport.local_addr()
    }

    /// Returns a [`DomusReporting`] handle for subscribing to observability events.
    pub fn reporting(&self) -> DomusReporting {
        self.reporting.clone()
    }

    /// Registers a new [`Taberna`] on this domus under `id`, bound to the
    /// supplied codec. Returns [`AureliaError`] with
    /// [`ErrorId::TabernaAlreadyRegistered`] if the id is already in use.
    pub async fn taberna<Codec: MessageCodec>(
        &self,
        id: TabernaId,
        codec: Codec,
    ) -> Result<Taberna<Codec>, AureliaError>
    where
        Codec::AppMessage: Send + Sync + 'static,
    {
        let config = self.config.snapshot().await;
        let (sender, receiver) =
            MpscBuilder::new(config.taberna_accept_queue_size, config.accept_timeout)
                .shutdown_channel(Arc::new(TabernaShutdownReport::<Codec>::new()))
                .runtime(self.runtime_handle.clone())
                .build()
                .map_err(|err| {
                    AureliaError::with_message(ErrorId::ProtocolViolation, err.to_string())
                })?;
        let inbox = Arc::new(TabernaInboxHandle::new(
            codec,
            sender,
            self.config.clone(),
            config.taberna_accept_queue_size,
            config.accept_timeout,
        ));
        self.registry.register(id, inbox).await?;
        Ok(Taberna::new(
            id,
            receiver,
            Arc::clone(&self.registry),
            self.runtime_handle.clone(),
        ))
    }

    /// Registers an Actix-backed taberna on this domus.
    ///
    /// Requires the `actix` feature. This convenience path registers a normal
    /// [`Taberna`] and runs an Actix-side bridge that forwards accepted taberna
    /// requests to `recipient`.
    #[cfg(feature = "actix")]
    pub async fn actix_taberna<Codec>(
        &self,
        id: TabernaId,
        codec: Codec,
        recipient: actix::prelude::Recipient<ActixTabernaDelivery<Codec::AppMessage>>,
    ) -> Result<ActixTaberna, AureliaError>
    where
        Codec: MessageCodec + 'static,
        Codec::AppMessage: Send + Sync + 'static,
    {
        let taberna = self.taberna(id, codec).await?;
        Ok(ActixTaberna::new(taberna, recipient))
    }

    /// Sends `message` to the peer hosting `taberna_id`. Resolves the peer
    /// address through the configured [`RouteResolver`], then encodes the
    /// message via the supplied codec. With [`SendOptions::BLOB`] the
    /// returned [`SendOutcome::Blob`] yields a [`crate::BlobSender`] for
    /// streaming bytes alongside the message.
    pub async fn send<Codec: MessageCodec>(
        &self,
        codec: &Codec,
        taberna_id: TabernaId,
        message: &Codec::AppMessage,
        options: SendOptions,
    ) -> Result<SendOutcome, AureliaError> {
        self.peering.send(codec, taberna_id, message, options).await
    }

    /// Replaces the in-memory mTLS authentication material without
    /// restarting the domus. Useful for certificate rotation.
    pub async fn reload_auth(&self, auth: Pkcs8AuthConfig) -> Result<(), AureliaError> {
        self.transport.reload_auth(auth).await?;
        self.observability.auth_reloaded();
        Ok(())
    }

    /// Initiates a graceful shutdown: drains tabernas, closes the transport,
    /// and aborts the transport task.
    pub async fn shutdown(&self) {
        self.observability.shutdown_started();
        self.registry.shutdown().await;
        self.transport.shutdown().await;
        let handle = self.transport_handle.lock().await.take();
        if let Some(handle) = handle {
            handle.abort();
        }
        self.observability.shutdown_complete();
    }
}
