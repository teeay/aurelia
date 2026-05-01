// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

//! Aurelia peering transport.

mod address;
mod auth;
mod blob_io;
mod callis;
mod codec;
mod config;
mod delivery;
mod domus;
mod limiter;
mod message_id;
mod observability;
mod peering;
mod reliability;
pub mod ring_buffer;
mod routing;
mod send;
mod session;
mod simpleresolver;
mod taberna;
mod transport;
mod wire;

#[cfg(feature = "actix")]
mod actix_adapter;

pub use address::{DomusAddr, TransportKind};
pub use aurelia_ids::{
    log_ids, AureliaError, ErrorId, LogId, MessageType, PeerMessageId, TabernaId, MSG_ACK,
    MSG_BLOB_TRANSFER_CHUNK, MSG_BLOB_TRANSFER_COMPLETE, MSG_BLOB_TRANSFER_START, MSG_CLOSE,
    MSG_ERROR, MSG_HELLO, MSG_HELLO_RESPONSE, MSG_KEEPALIVE,
};
pub use auth::{DomusAuthConfig, Pkcs8AuthConfig, Pkcs8DerConfig, Pkcs8PemConfig};
pub use blob_io::{BlobReceiver, BlobSender};
pub use codec::{decode_error, encode_error, EncodedMessage, MessageCodec};
pub use config::{DomusConfig, DomusConfigAccess, DomusConfigBuilder};
pub use domus::{Domus, DomusBuilder};
pub use observability::{
    BlobCallisSettingsReport, CallisId, DisconnectReason, DomusMetrics, DomusMetricsDelta,
    DomusReporting, DomusReportingEvent, DomusReportingFeeds, HandshakePhase, PeerIdentityReport,
    RestartReason,
};
pub use routing::RouteResolver;
pub use send::{SendOptions, SendOutcome};
pub use simpleresolver::SimpleResolver;
pub use taberna::{Taberna, TabernaInbox, TabernaRequest};

#[cfg(feature = "actix")]
pub use actix_adapter::ActixTabernaSink;

#[cfg(test)]
mod tests;
