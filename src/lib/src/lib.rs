// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]

//! # Aurelia
//!
//! An embeddable service mesh for Rust applications. Aurelia gives a Rust
//! process a built-in, authenticated peer-to-peer fabric — no sidecar, no
//! control plane, no extra runtime to deploy.
//!
//! ## Layer model
//!
//! - **A0 — Transport authentication.** mTLS over TCP, or PKCS#8 certificate-backed
//!   authentication over Unix domain sockets. A0 completes before any A1 frames are exchanged.
//! - **A1 — Message and blob transfer.** Delivery, callis (per-peer
//!   connection flow), and taberna (named inbound endpoint) management.
//! - **A2 — Aurelia services.** Higher-level capabilities built on A1 (in
//!   progress; the current release ships A0 and A1 with the wrapper API).
//! - **A3 — Application.** Your code. All A3-to-A3 traffic transits A1.
//!
//! ## Quick start
//!
//! Initialise the Aurelia runtime and build a [`Domus`] (the local peer)
//! bound to a TCP address with PKCS#8 mTLS material:
//!
//! ```no_run
//! use std::sync::Arc;
//! use aurelia::{Aurelia, DomusAddr, DomusConfigBuilder,
//!     Pkcs8AuthConfig, Pkcs8PemConfig, SimpleResolver};
//!
//! # async fn run() -> Result<(), aurelia::AureliaError> {
//! let aurelia = Aurelia::new();
//!
//! let config = DomusConfigBuilder::new().build()?;
//! let auth = Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
//!     ca_pem: std::fs::read("ca.pem").unwrap(),
//!     cert_pem: std::fs::read("cert.pem").unwrap(),
//!     pkcs8_key_pem: std::fs::read("key.pem").unwrap().into(),
//! });
//!
//! let domus = aurelia
//!     .domus_builder(
//!         config,
//!         DomusAddr::Tcp("127.0.0.1:7000".parse().unwrap()),
//!         auth,
//!         Arc::new(SimpleResolver::new()),
//!     )
//!     .build()
//!     .await?;
//!
//! // Use `domus.taberna(...)` to register inbound endpoints, and
//! // `domus.send(...)` to dispatch messages to peers.
//! # Ok(()) }
//! ```
//!
//! ## Where to look next
//!
//! - [`Aurelia`] — runtime initializer and entry point.
//! - [`DomusBuilder`] — configures and builds a [`Domus`].
//! - [`Domus`] — the running local peer.
//! - [`Taberna`] — a named inbound endpoint on a domus.
//! - [`DomusConfig`] / [`DomusConfigBuilder`] — tuning knobs and validation.
//! - [`AureliaError`] / [`ErrorId`] — the single error type used across the API.
//! - [`DomusReporting`] / [`DomusReportingEvent`] — observability streams.
//! - [`a3_message_type`] — derives application message-type IDs in the A3 range.

use std::sync::Arc;

pub use aurelia_data::{DomusAddr, RouteResolver, TransportKind};
pub use aurelia_ids::{
    a3_message_type, classify_message_priority, try_a3_message_type, AureliaError, ErrorId,
    MessagePriorityClass, MessageType, TabernaId, A1_MESSAGE_TYPE_MAX, A2_MESSAGE_TYPE_BASE,
    A2_MESSAGE_TYPE_MAX, A3_MESSAGE_TYPE_BASE, A3_MESSAGE_TYPE_MAX_OFFSET,
};
#[cfg(feature = "actix")]
/// Registration handle for an Actix-backed taberna.
///
/// Dropping the handle schedules deregistration from its parent domus. The taberna ID becomes
/// available for reuse once the spawned deregistration task completes.
///
/// # Example
///
/// ```no_run
/// use actix::{Actor, Context, Handler};
/// use aurelia::{
///     a3_message_type, ActixTabernaDelivery, AureliaError, Domus, EncodedMessage, ErrorId,
///     MessageCodec, MessageType, RouteResolver,
/// };
///
/// struct TextCodec;
///
/// impl MessageCodec for TextCodec {
///     type AppMessage = String;
///
///     fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
///         Ok(EncodedMessage::new(
///             a3_message_type(0),
///             msg.as_bytes().to_vec().into(),
///         ))
///     }
///
///     fn decode_app(
///         &self,
///         msg_type: MessageType,
///         payload: &[u8],
///     ) -> Result<Self::AppMessage, AureliaError> {
///         if msg_type != a3_message_type(0) {
///             return Err(AureliaError::new(ErrorId::DecodeFailure));
///         }
///         String::from_utf8(payload.to_vec())
///             .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))
///     }
/// }
///
/// struct TextActor;
///
/// impl Actor for TextActor {
///     type Context = Context<Self>;
/// }
///
/// impl Handler<ActixTabernaDelivery<String>> for TextActor {
///     type Result = ();
///
///     fn handle(
///         &mut self,
///         delivery: ActixTabernaDelivery<String>,
///         _ctx: &mut Self::Context,
///     ) -> Self::Result {
///         let _message = delivery.message;
///     }
/// }
///
/// # async fn example<RR: RouteResolver>(domus: &Domus<RR>) -> Result<(), AureliaError> {
/// let recipient = TextActor.start().recipient();
/// let _taberna = domus.actix_taberna(42, TextCodec, recipient).await?;
/// # Ok(()) }
/// ```
pub use aurelia_peering::ActixTaberna;
#[cfg(feature = "actix")]
pub use aurelia_peering::ActixTabernaDelivery;
pub use aurelia_peering::PeerIdentityReport;
pub use aurelia_peering::{BlobCallisSettingsReport, DomusMetrics, DomusMetricsDelta};
pub use aurelia_peering::{
    BlobReceiver, BlobSender, BlobWindowConfig, DomusConfig, DomusConfigAccess, DomusConfigBuilder,
    EncodedMessage, MessageCodec, Pkcs8AuthConfig, Pkcs8DerConfig, Pkcs8PemConfig, Pkcs8PrivateKey,
    SendOptions, SendOutcome, TabernaCompletion, TabernaRequest, TabernaRequestParts,
};
pub use aurelia_peering::{DomusReporting, DomusReportingEvent, DomusReportingFeeds};
pub use aurelia_resolver::SimpleResolver;

/// A running Aurelia domus: the local peer's representation in the mesh.
///
/// A [`Domus`] owns a single bound transport (TCP+mTLS or Unix socket)
/// and the registry of [`Taberna`]s hosted on this peer. Outbound traffic is
/// dispatched through the configured [`RouteResolver`]; inbound traffic is
/// delivered to the relevant taberna's inbox.
///
/// Construct one via [`Aurelia::domus_builder`] (recommended) or directly via
/// [`DomusBuilder`].
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use aurelia::{
///     Aurelia, DomusAddr, DomusConfigBuilder, Pkcs8AuthConfig, Pkcs8PemConfig,
///     SimpleResolver,
/// };
///
/// # async fn example() -> Result<(), aurelia::AureliaError> {
/// let aurelia = Aurelia::new();
/// let config = DomusConfigBuilder::new().build()?;
/// let auth = Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
///     ca_pem: vec![],
///     cert_pem: vec![],
///     pkcs8_key_pem: vec![].into(),
/// });
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
///
/// Sending a typed application message:
///
/// ```no_run
/// use aurelia::{
///     a3_message_type, AureliaError, Domus, EncodedMessage, ErrorId, MessageCodec, MessageType,
///     RouteResolver, SendOptions, SendOutcome,
/// };
///
/// struct TextCodec;
///
/// impl MessageCodec for TextCodec {
///     type AppMessage = String;
///
///     fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
///         Ok(EncodedMessage::new(
///             a3_message_type(0),
///             msg.as_bytes().to_vec().into(),
///         ))
///     }
///
///     fn decode_app(
///         &self,
///         msg_type: MessageType,
///         payload: &[u8],
///     ) -> Result<Self::AppMessage, AureliaError> {
///         if msg_type != a3_message_type(0) {
///             return Err(AureliaError::new(ErrorId::DecodeFailure));
///         }
///         String::from_utf8(payload.to_vec())
///             .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))
///     }
/// }
///
/// # async fn example<RR: RouteResolver>(domus: &Domus<RR>) -> Result<(), AureliaError> {
/// let outcome = domus
///     .send(
///         &TextCodec,
///         42,
///         &"ping".to_owned(),
///         SendOptions::MESSAGE_ONLY,
///     )
///     .await?;
/// assert!(matches!(outcome, SendOutcome::MessageOnly));
/// # Ok(()) }
/// ```
///
/// Starting a blob transfer:
///
/// ```no_run
/// use tokio::io::AsyncWriteExt;
/// use aurelia::{
///     a3_message_type, AureliaError, Domus, EncodedMessage, ErrorId, MessageCodec, MessageType,
///     RouteResolver, SendOptions, SendOutcome,
/// };
///
/// struct TextCodec;
///
/// impl MessageCodec for TextCodec {
///     type AppMessage = String;
///
///     fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
///         Ok(EncodedMessage::new(
///             a3_message_type(0),
///             msg.as_bytes().to_vec().into(),
///         ))
///     }
///
///     fn decode_app(
///         &self,
///         msg_type: MessageType,
///         payload: &[u8],
///     ) -> Result<Self::AppMessage, AureliaError> {
///         if msg_type != a3_message_type(0) {
///             return Err(AureliaError::new(ErrorId::DecodeFailure));
///         }
///         String::from_utf8(payload.to_vec())
///             .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))
///     }
/// }
///
/// # async fn example<RR: RouteResolver>(domus: &Domus<RR>) -> Result<(), AureliaError> {
/// let outcome = domus
///     .send(&TextCodec, 42, &"blob incoming".to_owned(), SendOptions::BLOB)
///     .await?;
/// let SendOutcome::Blob { mut sender } = outcome else {
///     unreachable!("BLOB send option returns a blob sender");
/// };
/// sender
///     .write_all(b"blob bytes")
///     .await
///     .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
/// sender
///     .shutdown()
///     .await
///     .map_err(|err| AureliaError::with_message(ErrorId::PeerUnavailable, err.to_string()))?;
/// # Ok(()) }
/// ```
pub use aurelia_peering::Domus;

/// A registered inbound endpoint on a [`Domus`].
///
/// Each taberna has a stable [`TabernaId`] and a typed [`MessageCodec`]; received
/// messages are decoded with that codec and surfaced as [`TabernaRequest`]s via
/// [`Taberna::next`].
///
/// Tabernas are constructed with [`Domus::taberna`]; dropping a [`Taberna`]
/// unregisters it from the parent domus.
///
/// # Example
///
/// ```no_run
/// use aurelia::{
///     a3_message_type, AureliaError, EncodedMessage, ErrorId, MessageCodec, MessageType,
/// };
///
/// struct TextCodec;
///
/// impl MessageCodec for TextCodec {
///     type AppMessage = String;
///
///     fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
///         Ok(EncodedMessage::new(
///             a3_message_type(0),
///             msg.as_bytes().to_vec().into(),
///         ))
///     }
///
///     fn decode_app(
///         &self,
///         msg_type: MessageType,
///         payload: &[u8],
///     ) -> Result<Self::AppMessage, AureliaError> {
///         if msg_type != a3_message_type(0) {
///             return Err(AureliaError::new(ErrorId::DecodeFailure));
///         }
///         String::from_utf8(payload.to_vec())
///             .map_err(|err| AureliaError::with_message(ErrorId::DecodeFailure, err.to_string()))
///     }
/// }
///
/// # async fn example<RR: aurelia::RouteResolver>(
/// #     domus: aurelia::Domus<RR>,
/// # ) -> Result<(), AureliaError> {
/// let taberna = domus.taberna(42, TextCodec).await?;
/// let request = taberna.next(None).await?;
/// request.accept();
/// # Ok(()) }
/// ```
pub use aurelia_peering::Taberna;

/// Runtime owner and entry point for the Aurelia library.
///
/// `Aurelia` initialises and owns the internal Tokio runtime that all
/// Aurelia background work runs on. The runtime handle is intentionally
/// not exposed: applications keep their own runtime for their own work and
/// interact with Aurelia only through this wrapper. Construct one via
/// [`Aurelia::new`] at program start, then build a [`Domus`] via
/// [`Aurelia::domus_builder`].
///
/// # Example
///
/// ```
/// use aurelia::Aurelia;
///
/// let aurelia = Aurelia::new();
/// // The Aurelia runtime is now ready; use `aurelia.domus_builder(...)`
/// // to construct domuses.
/// # let _ = aurelia;
/// ```
pub struct Aurelia {
    _private: (),
}

impl Aurelia {
    /// Initialises the Aurelia runtime if it is not already running and
    /// returns a fresh wrapper.
    pub fn new() -> Self {
        aurelia_platform::runtime::ensure();
        Self { _private: () }
    }

    /// Returns a [`DomusBuilder`] wired to the Aurelia runtime. The builder
    /// validates its inputs at [`DomusBuilder::build`] time; this method
    /// itself never fails.
    pub fn domus_builder<RR>(
        &self,
        config: DomusConfig,
        local_addr: DomusAddr,
        auth: Pkcs8AuthConfig,
        resolver: Arc<RR>,
    ) -> DomusBuilder<RR>
    where
        RR: RouteResolver + 'static,
    {
        DomusBuilder::new(config, local_addr, auth, resolver)
    }
}

impl Default for Aurelia {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for a [`Domus`] wired to the Aurelia runtime.
///
/// Obtain one via [`Aurelia::domus_builder`]. Calling [`DomusBuilder::build`]
/// validates the supplied [`DomusConfig`], performs the A0 bind (mTLS or
/// Unix socket), and resolves to a running [`Domus`]. Use
/// [`DomusBuilder::build_with_reporting`] to receive observability feeds
/// alongside the built domus.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use aurelia::{Aurelia, DomusAddr, DomusConfigBuilder,
///     Pkcs8AuthConfig, Pkcs8PemConfig, SimpleResolver};
///
/// # async fn run() -> Result<(), aurelia::AureliaError> {
/// let aurelia = Aurelia::new();
/// let domus = aurelia
///     .domus_builder(
///         DomusConfigBuilder::new().build()?,
///         DomusAddr::Tcp("127.0.0.1:7001".parse().unwrap()),
///         Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
///             ca_pem: vec![], cert_pem: vec![], pkcs8_key_pem: vec![].into(),
///         }),
///         Arc::new(SimpleResolver::new()),
///     )
///     .build()
///     .await?;
/// # let _ = domus;
/// # Ok(()) }
/// ```
pub struct DomusBuilder<RR>
where
    RR: RouteResolver + 'static,
{
    inner: aurelia_peering::DomusBuilder<RR>,
}

#[cfg(test)]
mod tests;

impl<RR> DomusBuilder<RR>
where
    RR: RouteResolver + 'static,
{
    pub(crate) fn new(
        config: DomusConfig,
        local_addr: DomusAddr,
        auth: Pkcs8AuthConfig,
        resolver: Arc<RR>,
    ) -> Self {
        let inner = aurelia_peering::DomusBuilder::new(config, local_addr, auth, resolver);
        Self { inner }
    }

    /// Builds the [`Domus`] on the Aurelia runtime. Returns
    /// [`AureliaError`] with [`ErrorId::PeerUnavailable`] if the Aurelia
    /// runtime is shutting down, or any error produced by the underlying
    /// transport bind.
    pub async fn build(self) -> Result<Domus<RR>, AureliaError> {
        self.inner.build().await
    }

    /// Builds the [`Domus`] together with its [`DomusReportingFeeds`] for
    /// streaming observability events out to an external sink.
    pub async fn build_with_reporting(
        self,
    ) -> Result<(Domus<RR>, DomusReportingFeeds), AureliaError> {
        self.inner.build_with_reporting().await
    }
}
