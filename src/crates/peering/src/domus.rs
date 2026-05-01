// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::address::DomusAddr;
use crate::auth::DomusAuthConfig;
use crate::codec::MessageCodec;
use crate::config::{DomusConfig, DomusConfigAccess, DomusConfigStore};
use crate::observability::{
    new_observability, DomusReporting, DomusReportingFeeds, ObservabilityHandle,
};
use crate::peering::{RouteLocalRemote, RouteLocalRemoteBuilder};
use crate::routing::RouteResolver;
use crate::send::{SendOptions, SendOutcome};
use crate::taberna::{Taberna, TabernaInboxHandle, TabernaRegistry, TabernaShutdownReport};
use crate::transport::Transport;
use aurelia_ids::{AureliaError, ErrorId, TabernaId};
use caducus::MpscBuilder;

type DomusPeering<RR> = RouteLocalRemote<RR>;
type DomusTransport = Arc<Transport>;

/// Builder for a [`Domus`].
///
/// This is the low-level peering builder; most applications should construct
/// a [`Domus`] via `Aurelia::domus_builder` instead, which wires the
/// builder to Aurelia's owned Tokio runtime.
///
/// A [`DomusBuilder`] consumes a [`DomusConfig`], a local [`DomusAddr`],
/// authentication material, and a [`RouteResolver`], then yields either a
/// plain [`Domus`] via [`DomusBuilder::build`] or a `(Domus, DomusReportingFeeds)`
/// pair via [`DomusBuilder::build_with_reporting`] when observability feeds
/// are required.
pub struct DomusBuilder<RR>
where
    RR: RouteResolver,
{
    config: DomusConfig,
    local_addr: DomusAddr,
    auth: DomusAuthConfig,
    resolver: Arc<RR>,
    runtime_handle: tokio::runtime::Handle,
}

impl<RR> DomusBuilder<RR>
where
    RR: RouteResolver,
{
    /// Constructs a new builder. Callers normally reach this through
    /// `Aurelia::domus_builder` rather than directly.
    pub fn new(
        config: DomusConfig,
        local_addr: DomusAddr,
        auth: DomusAuthConfig,
        resolver: Arc<RR>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            config,
            local_addr,
            auth,
            resolver,
            runtime_handle,
        }
    }

    /// Builds a [`Domus`] without observability feeds. The transport is
    /// bound and started before this future resolves.
    pub async fn build(self) -> Result<Domus<RR>, AureliaError> {
        let (domus, _) = self.build_internal(false).await?;
        Ok(domus)
    }

    /// Builds a [`Domus`] together with [`DomusReportingFeeds`] for
    /// streaming observability events out to an external sink.
    pub async fn build_with_reporting(
        self,
    ) -> Result<(Domus<RR>, DomusReportingFeeds), AureliaError> {
        let (domus, feeds) = self.build_internal(true).await?;
        let feeds = feeds.ok_or_else(|| {
            AureliaError::with_message(ErrorId::ProtocolViolation, "reporting feeds missing")
        })?;
        Ok((domus, feeds))
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
        let mut config = self.config;
        config.apply_transport_defaults(self.local_addr.kind());
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
        let peering = RouteLocalRemoteBuilder::new(
            config_access.clone(),
            Arc::clone(&registry),
            Arc::clone(&self.resolver),
            Arc::clone(&transport),
        )
        .build();
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
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use aurelia::{Aurelia, DomusConfigBuilder, DomusAddr, DomusAuthConfig,
///     Pkcs8AuthConfig, Pkcs8PemConfig, SimpleResolver};
///
/// # async fn example() -> Result<(), aurelia::AureliaError> {
/// let aurelia = Aurelia::new();
/// let config = DomusConfigBuilder::new().build()?;
/// let auth = DomusAuthConfig::Pkcs8(Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
///     ca_pem: vec![], cert_pem: vec![], pkcs8_key_pem: vec![],
/// }));
/// let domus = aurelia
///     .domus_builder(
///         config,
///         DomusAddr::Tcp("127.0.0.1:7000".parse().unwrap()),
///         auth,
///         Arc::new(SimpleResolver::new()),
///     )
///     .build()
///     .await?;
/// # let _ = domus.local_addr();
/// # Ok(()) }
/// ```
pub struct Domus<RR>
where
    RR: RouteResolver,
{
    config: DomusConfigAccess,
    registry: Arc<TabernaRegistry>,
    peering: DomusPeering<RR>,
    transport: DomusTransport,
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
    pub async fn reload_auth(&self, auth: DomusAuthConfig) -> Result<(), AureliaError> {
        self.transport.reload_auth(auth).await?;
        self.observability.auth_reloaded().await;
        Ok(())
    }

    /// Initiates a graceful shutdown: drains tabernas, closes the transport,
    /// and aborts the transport task.
    pub async fn shutdown(&self) {
        self.observability.shutdown_started().await;
        self.registry.shutdown().await;
        self.transport.shutdown().await;
        let handle = self.transport_handle.lock().await.take();
        if let Some(handle) = handle {
            handle.abort();
        }
        self.observability.shutdown_complete().await;
    }
}
