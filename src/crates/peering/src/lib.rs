// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

//! Aurelia peering transport.

mod auth;
mod blob_io;
mod callis;
mod codec;
mod config;
mod delivery;
mod domus;
mod message_id;
mod observability;
mod peering;
pub(crate) mod ring_buffer;
mod send;
mod session;
mod taberna;
mod transport;
mod wire;

#[cfg(feature = "actix")]
mod actix_adapter;

pub use aurelia_ids::{
    a3_message_type, classify_message_priority, try_a3_message_type, AureliaError, ErrorId,
    MessagePriorityClass, MessageType, TabernaId, A1_MESSAGE_TYPE_MAX, A2_MESSAGE_TYPE_BASE,
    A2_MESSAGE_TYPE_MAX, A3_MESSAGE_TYPE_BASE, A3_MESSAGE_TYPE_MAX_OFFSET,
};
pub use auth::{Pkcs8AuthConfig, Pkcs8DerConfig, Pkcs8PemConfig, Pkcs8PrivateKey};
pub use blob_io::{BlobReceiver, BlobSender};
pub use codec::{EncodedMessage, MessageCodec};
pub use config::{BlobWindowConfig, DomusConfig, DomusConfigAccess, DomusConfigBuilder};
pub use domus::{Domus, DomusBuilder};
pub use observability::{
    BlobCallisSettingsReport, DomusMetrics, DomusMetricsDelta, DomusReporting, DomusReportingEvent,
    DomusReportingFeeds, PeerIdentityReport,
};
pub use send::{SendOptions, SendOutcome};
pub use taberna::{Taberna, TabernaCompletion, TabernaRequest, TabernaRequestParts};

#[cfg(feature = "actix")]
pub use actix_adapter::{ActixTaberna, ActixTabernaDelivery};

#[cfg(test)]
mod tests;
