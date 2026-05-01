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
//! - **A0 — Transport authentication.** mTLS over TCP, or peer-credential
//!   authentication over Unix domain sockets. A0 completes before any A1
//!   frames are exchanged.
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
//! use aurelia::{Aurelia, DomusAddr, DomusAuthConfig, DomusConfigBuilder,
//!     Pkcs8AuthConfig, Pkcs8PemConfig, SimpleResolver};
//!
//! # async fn run() -> Result<(), aurelia::AureliaError> {
//! let aurelia = Aurelia::new();
//!
//! let config = DomusConfigBuilder::new().build()?;
//! let auth = DomusAuthConfig::Pkcs8(Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
//!     ca_pem: std::fs::read("ca.pem").unwrap(),
//!     cert_pem: std::fs::read("cert.pem").unwrap(),
//!     pkcs8_key_pem: std::fs::read("key.pem").unwrap(),
//! }));
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
//! - [`Aurelia`] — runtime owner and entry point.
//! - [`DomusBuilder`] — configures and builds a [`Domus`].
//! - [`Domus`] — the running local peer.
//! - [`Taberna`] — a named inbound endpoint on a domus.
//! - [`DomusConfig`] / [`DomusConfigBuilder`] — tuning knobs and validation.
//! - [`AureliaError`] / [`ErrorId`] — the single error type used across the API.
//! - [`DomusReporting`] / [`DomusReportingEvent`] — observability streams.

mod runtime;

use std::sync::Arc;

use tokio::sync::oneshot;

pub use aurelia_peering::{
    decode_error, encode_error, AureliaError, BlobReceiver, BlobSender, Domus, DomusAddr,
    DomusAuthConfig, DomusConfig, DomusConfigAccess, DomusConfigBuilder, EncodedMessage, ErrorId,
    MessageCodec, MessageType, Pkcs8AuthConfig, Pkcs8DerConfig, Pkcs8PemConfig, SendOptions,
    SendOutcome, Taberna, TabernaId, TabernaInbox, TabernaRequest, TransportKind,
};
pub use aurelia_peering::{
    DomusMetrics, DomusMetricsDelta, HandshakePhase, PeerIdentityReport, RestartReason,
};
pub use aurelia_peering::{DomusReporting, DomusReportingEvent, DomusReportingFeeds};
pub use aurelia_peering::{RouteResolver, SimpleResolver};

/// Runtime owner and entry point for the Aurelia library.
///
/// `Aurelia` initialises and owns the internal Tokio runtime that all
/// Aurelia background work runs on. The runtime handle is intentionally
/// not exposed: applications keep their own runtime for their own work and
/// interact with Aurelia only through this wrapper. Construct one via
/// [`Aurelia::new`] (or its alias [`Aurelia::init`]) at program start, then
/// build a [`Domus`] via [`Aurelia::domus_builder`].
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
    /// returns a fresh wrapper. Equivalent to [`Aurelia::init`].
    pub fn new() -> Self {
        Self::init()
    }

    /// Initialises the Aurelia runtime if it is not already running and
    /// returns a fresh wrapper.
    pub fn init() -> Self {
        runtime::ensure();
        Self { _private: () }
    }

    /// Returns a [`DomusBuilder`] wired to the Aurelia runtime. The builder
    /// validates its inputs at [`DomusBuilder::build`] time; this method
    /// itself never fails.
    pub fn domus_builder<RR>(
        &self,
        config: DomusConfig,
        local_addr: DomusAddr,
        auth: DomusAuthConfig,
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
/// use aurelia::{Aurelia, DomusAddr, DomusAuthConfig, DomusConfigBuilder,
///     Pkcs8AuthConfig, Pkcs8PemConfig, SimpleResolver};
///
/// # async fn run() -> Result<(), aurelia::AureliaError> {
/// let aurelia = Aurelia::new();
/// let domus = aurelia
///     .domus_builder(
///         DomusConfigBuilder::new().build()?,
///         DomusAddr::Tcp("127.0.0.1:7001".parse().unwrap()),
///         DomusAuthConfig::Pkcs8(Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
///             ca_pem: vec![], cert_pem: vec![], pkcs8_key_pem: vec![],
///         })),
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
    runtime: tokio::runtime::Handle,
}

#[cfg(test)]
mod tests;

impl<RR> DomusBuilder<RR>
where
    RR: RouteResolver + 'static,
{
    /// Constructs a builder wired to the Aurelia runtime. Applications
    /// normally reach this through [`Aurelia::domus_builder`] rather than
    /// constructing it directly.
    pub fn new(
        config: DomusConfig,
        local_addr: DomusAddr,
        auth: DomusAuthConfig,
        resolver: Arc<RR>,
    ) -> Self {
        let runtime = runtime::handle();
        let inner =
            aurelia_peering::DomusBuilder::new(config, local_addr, auth, resolver, runtime.clone());
        Self { inner, runtime }
    }

    /// Builds the [`Domus`] on the Aurelia runtime. Returns
    /// [`AureliaError`] with [`ErrorId::PeerUnavailable`] if the Aurelia
    /// runtime is shutting down, or any error produced by the underlying
    /// transport bind.
    pub async fn build(self) -> Result<Domus<RR>, AureliaError> {
        let (tx, rx) = oneshot::channel();
        let runtime = self.runtime.clone();
        runtime.spawn(async move {
            let result = self.inner.build().await;
            let _ = tx.send(result);
        });
        rx.await.unwrap_or_else(|_| {
            Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "aurelia runtime unavailable",
            ))
        })
    }

    /// Builds the [`Domus`] together with its [`DomusReportingFeeds`] for
    /// streaming observability events out to an external sink.
    pub async fn build_with_reporting(
        self,
    ) -> Result<(Domus<RR>, DomusReportingFeeds), AureliaError> {
        let (tx, rx) = oneshot::channel();
        let runtime = self.runtime.clone();
        runtime.spawn(async move {
            let result = self.inner.build_with_reporting().await;
            let _ = tx.send(result);
        });
        rx.await.unwrap_or_else(|_| {
            Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "aurelia runtime unavailable",
            ))
        })
    }
}
